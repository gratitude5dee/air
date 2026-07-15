import { describe, expect, it } from "vitest";
import { createQueueCipher } from "../../lib/crypto.js";
import {
  DurableInbox,
  inboxRecordKey,
  type VerifiedInboxInput,
} from "../../lib/durable-inbox.js";
import { DurableOutbox } from "../../lib/durable-outbox.js";
import {
  acquireRedisLease,
  COMPARE_AND_SET_WITH_PX_LUA,
  MemoryRedis,
  PERSIST_INBOX_NEW_RECORD_LUA,
  PERSIST_OUTBOX_NEW_RECORD_LUA,
  WRITE_INBOX_RECORD_LUA,
  WRITE_OUTBOX_RECORD_LUA,
} from "../../lib/redis.js";
import { hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";

const identitySecret = "test-identity-secret";
const queueKey = Buffer.alloc(32, 9).toString("base64");

class FailAfterInboxPublishRedis extends MemoryRedis {
  private failOnce = true;

  override async eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown> {
    const result = await super.eval(script, options);
    if (this.failOnce && script === PERSIST_INBOX_NEW_RECORD_LUA) {
      this.failOnce = false;
      throw new Error("simulated process loss after inbox publish commit");
    }
    return result;
  }
}

/** Models an invocation dying after Redis committed its atomic response. */
class FailAfterOutboxCommitRedis extends MemoryRedis {
  private failPublishOnce = true;
  private failWriteOnce = false;

  failAfterNextWrite(): void {
    this.failWriteOnce = true;
  }

  override async eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown> {
    const result = await super.eval(script, options);
    if (
      (this.failPublishOnce && script === PERSIST_OUTBOX_NEW_RECORD_LUA) ||
      (this.failWriteOnce && script === WRITE_OUTBOX_RECORD_LUA)
    ) {
      this.failPublishOnce = false;
      this.failWriteOnce = false;
      throw new Error("simulated disconnect after atomic outbox commit");
    }
    return result;
  }
}

/** Models a lost client response after an atomic inbox state transition. */
class FailAfterInboxStateCommitRedis extends MemoryRedis {
  private failWriteOnce = false;

  failAfterNextWrite(): void {
    this.failWriteOnce = true;
  }

  override async eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown> {
    const result = await super.eval(script, options);
    if (this.failWriteOnce && script === WRITE_INBOX_RECORD_LUA) {
      this.failWriteOnce = false;
      throw new Error("simulated disconnect after atomic inbox commit");
    }
    return result;
  }
}

class TrackingMemoryRedis extends MemoryRedis {
  readonly readKeys: string[] = [];

  override async get(key: string): Promise<string | null> {
    this.readKeys.push(key);
    return super.get(key);
  }
}

function createInboxInput(
  messageId: string,
  text: string,
  receivedAtMs: number,
  webhookId = `delivery-${messageId}`,
): VerifiedInboxInput {
  const senderId = "+14155551234";
  const chatGuid = "iMessage;-;+14155551234";
  return {
    projectId: "project-a",
    messageId,
    webhookId,
    senderHash: hashSenderIdentifier(identitySecret, senderId),
    threadHash: hashThreadIdentifier(identitySecret, chatGuid),
    contentType: "text",
    isGroup: false,
    receivedAtMs,
    envelope: {
      projectId: "project-a",
      messageId,
      webhookId,
      chatGuid,
      sendingLine: "+15555550123",
      senderId,
      direction: "inbound",
      content: { type: "text", text },
      isGroup: false,
      receivedAtMs,
    },
  };
}

describe("durable inbox", () => {
  it("fences active-record replacement with an exact-value PX compare-and-set", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    await redis.set("active", "first", { px: 1_000 });
    expect(await redis.get("active")).toBe("first");
    expect(await redis.eval(COMPARE_AND_SET_WITH_PX_LUA, {
      keys: ["active"],
      arguments: ["first", "second", "2000"],
    })).toBe(1);
    expect(await redis.get("active")).toBe("second");
    expect(await redis.eval(COMPARE_AND_SET_WITH_PX_LUA, {
      keys: ["active"],
      arguments: ["first", "stale", "2000"],
    })).toBe(0);
    expect(await redis.get("active")).toBe("second");
    now += 2_001;
    expect(await redis.get("active")).toBeNull();
  });

  it("persists encrypted content once, records duplicate delivery metadata, and deletes payload on sent", async () => {
    let now = 1_000;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const input = createInboxInput("message-1", "private music question", now, "delivery-1");

    const first = await inbox.persistVerified(input);
    const duplicate = await inbox.persistVerified({
      ...input,
      webhookId: "delivery-2",
      envelope: { ...input.envelope, webhookId: "delivery-2" },
    });
    const raw = await redis.get(first.record.immutable.recordKey);
    const metadata = await redis.get(`${first.record.immutable.recordKey}:metadata`);

    expect(first.created).toBe(true);
    expect(duplicate.created).toBe(false);
    expect(duplicate.record.delivery.deliveryCount).toBe(2);
    expect(duplicate.record.delivery.webhookIdHashes).toHaveLength(2);
    expect(raw).not.toContain("private music question");
    expect(raw).not.toContain("+14155551234");
    expect(metadata).not.toContain("private music question");
    expect(metadata).not.toContain("+14155551234");

    const lease = await inbox.beginProcessing(first.record.immutable.recordKey);
    expect(lease).not.toBeNull();
    if (!lease) throw new Error("expected inbox processing lease");
    expect(lease.envelope.content.text).toBe("private music question");
    await lease.transition("session_started");
    await lease.transition("response_ready");
    await lease.transition("sending");
    const sent = await lease.transition("sent");
    await lease.release();

    expect(sent?.state).toBe("sent");
    expect(sent?.encryptedEnvelope).toBeUndefined();
    expect((await inbox.getRecord(first.record.immutable.recordKey))?.encryptedEnvelope).toBeUndefined();
  });

  it("does not turn a committed sent transition into a false dead letter after a lost response", async () => {
    let now = 1_000;
    const redis = new FailAfterInboxStateCommitRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const persisted = await inbox.persistVerified(createInboxInput("sent-atomic", "private question", now));
    const lease = await inbox.beginProcessing(persisted.record.immutable.recordKey);
    if (!lease) throw new Error("expected inbox processing lease");
    await lease.transition("session_started");
    await lease.transition("response_ready");
    await lease.transition("sending");
    redis.failAfterNextWrite();

    await expect(lease.transition("sent")).rejects.toThrow(/simulated disconnect/);
    const recovered = await inbox.getRecord(persisted.record.immutable.recordKey);
    expect(recovered?.state).toBe("sent");
    expect(recovered?.encryptedEnvelope).toBeUndefined();
    expect(await inbox.listDue()).toEqual([]);
  });

  it("serializes each thread and turns stranded encrypted content into redacted dead-letter metadata", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
      payloadRetentionMs: 1_000,
      metadataRetentionMs: 5_000,
    });
    const first = await inbox.persistVerified(createInboxInput("message-a", "first", now));
    now += 1;
    const second = await inbox.persistVerified(createInboxInput("message-b", "second", now));

    const firstLease = await inbox.beginProcessing(first.record.immutable.recordKey);
    expect(firstLease).not.toBeNull();
    expect(await inbox.beginProcessing(second.record.immutable.recordKey)).toBeNull();
    if (!firstLease) throw new Error("expected first lease");
    await firstLease.transition("dead_letter", { failureCategory: "test" });
    await firstLease.release();

    const secondLease = await inbox.beginProcessing(second.record.immutable.recordKey);
    expect(secondLease).not.toBeNull();
    if (!secondLease) throw new Error("expected second lease");
    await secondLease.transition("dead_letter", { failureCategory: "test" });
    await secondLease.release();

    const expiring = await inbox.persistVerified(createInboxInput("message-expiry", "must disappear", now));
    now += 1_001;
    expect(await redis.get(expiring.record.immutable.recordKey)).toBeNull();
    expect(await redis.get(`${expiring.record.immutable.recordKey}:metadata`)).not.toBeNull();
    const expired = await inbox.getRecord(expiring.record.immutable.recordKey);
    expect(expired?.state).toBe("dead_letter");
    expect(expired?.encryptedEnvelope).toBeUndefined();
    expect(expired?.status.failureCategory).toBe("payload_expired");
  });

  it("uses an atomic per-thread sequence when arrivals share a millisecond", async () => {
    const now = 1_000;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    // Base64 key order would place `a-message` before `z-message`; receive
    // order must still win for one thread when Date.now() ties.
    const [first, second] = await Promise.all([
      inbox.persistVerified(createInboxInput("z-message", "first", now)),
      inbox.persistVerified(createInboxInput("a-message", "second", now)),
    ]);
    expect(first.record.immutable.threadSequence).toBeLessThan(second.record.immutable.threadSequence);

    const firstLease = await inbox.beginProcessing(first.record.immutable.recordKey);
    expect(firstLease).not.toBeNull();
    expect(await inbox.beginProcessing(second.record.immutable.recordKey)).toBeNull();
    if (!firstLease) throw new Error("expected first equal-millisecond message to lead the queue");
    await firstLease.transition("dead_letter", { failureCategory: "test" });
    await firstLease.release();
  });

  it("keeps the processing capability's physical thread lease through heartbeat and release", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
      threadLeaseTtlMs: 1_000,
    });
    const stored = await inbox.persistVerified(createInboxInput("message-lease-owner", "owner", now));
    const active = await inbox.beginProcessing(stored.record.immutable.recordKey);
    expect(active).not.toBeNull();
    if (!active) throw new Error("expected processing capability");

    // A successful heartbeat proves the returned capability still owns the
    // physical lock; a second worker cannot acquire that same thread lease.
    expect(await active.heartbeat()).toBe(true);
    const threadLeaseKey = `air:inbox:v1:lease:thread:${stored.record.immutable.threadHash}`;
    expect(await acquireRedisLease(redis, threadLeaseKey, { ttlMs: 1_000, now: () => now })).toBeNull();

    expect(await active.release()).toBe(true);
    const successor = await acquireRedisLease(redis, threadLeaseKey, { ttlMs: 1_000, now: () => now });
    expect(successor).not.toBeNull();
    await successor?.release();
  });

  it("renews and releases an attempt-owned thread lock across durable callbacks", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
      threadLeaseTtlMs: 1_000,
    });
    const stored = await inbox.persistVerified(createInboxInput("message-durable-owner", "owner", now));
    const active = await inbox.beginProcessing(stored.record.immutable.recordKey);
    expect(active).not.toBeNull();
    if (!active) throw new Error("expected processing capability");

    // Simulate an Eve lifecycle callback after the original worker request
    // has handed off. It owns only the durable attempt capability, not the
    // process-local RedisLease closure returned by beginProcessing().
    now += 500;
    expect(await inbox.heartbeatAttempt(stored.record.immutable.recordKey, active.attemptToken)).toBe(true);
    expect((await inbox.getRecord(stored.record.immutable.recordKey))?.status.processingLeaseExpiresAtMs)
      .toBe(1_500);

    await active.transition("failed_retryable", {
      failureCategory: "test_handoff_failure",
      nextAttemptAtMs: now,
    });
    const threadLeaseKey = `air:inbox:v1:lease:thread:${stored.record.immutable.threadHash}`;
    // Retry/terminal transitions release the exact attempt owner immediately;
    // a stale later release cannot affect a successor because its token differs.
    const successor = await acquireRedisLease(redis, threadLeaseKey, { ttlMs: 1_000, now: () => now });
    expect(successor).not.toBeNull();
    expect(await inbox.heartbeatAttempt(stored.record.immutable.recordKey, active.attemptToken)).toBe(false);
    await successor?.release();
  });

  it("commits the sequence and queue indexes atomically across a post-commit crash", async () => {
    const now = 1_000;
    const redis = new FailAfterInboxPublishRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const firstInput = createInboxInput("message-atomic-first", "first", now);

    // The Redis script commits before the request process disappears. A retry
    // response is never produced, but the first record must already be a queue
    // head when a later delivery arrives.
    await expect(inbox.persistVerified(firstInput)).rejects.toThrow(/process loss/);
    const firstKey = inboxRecordKey(firstInput.projectId, firstInput.messageId);
    expect((await inbox.getRecord(firstKey))?.immutable.threadSequence).toBe(1);

    const second = await inbox.persistVerified(createInboxInput("message-atomic-second", "second", now));
    expect(second.record.immutable.threadSequence).toBe(2);
    expect(await inbox.beginProcessing(second.record.immutable.recordKey)).toBeNull();

    const first = await inbox.beginProcessing(firstKey);
    expect(first).not.toBeNull();
    if (!first) throw new Error("expected atomically committed first queue head");
    await first.transition("dead_letter", { failureCategory: "test" });
    await first.release();
  });

  it("dead-letters after the durable processing-attempt cap", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
      maxProcessingAttempts: 3,
    });
    const stored = await inbox.persistVerified(createInboxInput("message-attempt-cap", "retry me", now));

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const lease = await inbox.beginProcessing(stored.record.immutable.recordKey);
      expect(lease?.record.status.attempts).toBe(attempt);
      if (!lease) throw new Error("expected bounded processing attempt");
      await lease.transition("failed_retryable", {
        failureCategory: "test_failure",
        nextAttemptAtMs: now,
      });
      await lease.release();
    }

    expect(await inbox.beginProcessing(stored.record.immutable.recordKey)).toBeNull();
    const exhausted = await inbox.getRecord(stored.record.immutable.recordKey);
    expect(exhausted?.state).toBe("dead_letter");
    expect(exhausted?.status.attempts).toBe(3);
    expect(exhausted?.status.failureCategory).toBe("processing_retries_exhausted");
  });

  it("pages past a busy thread tail so another thread head is not starved", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const busyThread: Awaited<ReturnType<typeof inbox.persistVerified>>[] = [];
    for (let index = 0; index < 100; index += 1) {
      busyThread.push(await inbox.persistVerified(
        createInboxInput(`busy-${String(index).padStart(3, "0")}`, `busy ${index}`, now),
      ));
      now += 1;
    }
    const active = await inbox.beginProcessing(busyThread[0]!.record.immutable.recordKey);
    expect(active).not.toBeNull();
    if (!active) throw new Error("expected busy thread head");

    const otherSender = "+14155559999";
    const otherChatGuid = "iMessage;-;+14155559999";
    const otherInput = createInboxInput("other-thread", "other question", now);
    const other = await inbox.persistVerified({
      ...otherInput,
      senderHash: hashSenderIdentifier(identitySecret, otherSender),
      threadHash: hashThreadIdentifier(identitySecret, otherChatGuid),
      envelope: {
        ...otherInput.envelope,
        chatGuid: otherChatGuid,
        senderId: otherSender,
      },
    });

    const due = await inbox.listDue(24);
    expect(due.map((record) => record.immutable.recordKey)).toContain(other.record.immutable.recordKey);
    await active.transition("dead_letter", { failureCategory: "test" });
    await active.release();
  });

  it("does not skip a live due record while clearing a stale pending page", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
      payloadRetentionMs: 1_000,
      metadataRetentionMs: 5_000,
    });

    // Fifty stale members fill the old page size. Removing them while advancing
    // a ZRANGEBYSCORE offset used to skip the following live entry.
    for (let index = 0; index < 50; index += 1) {
      await inbox.persistVerified(createInboxInput(`stale-${String(index).padStart(2, "0")}`, "stale", now));
    }
    now = 1_001;
    const live = await inbox.persistVerified(createInboxInput("live-after-stale", "live", now));

    expect((await inbox.listDue(1)).map((record) => record.immutable.recordKey))
      .toContain(live.record.immutable.recordKey);
  });

  it("dead-letters tampered ciphertext with the minted processing capability", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const stored = await inbox.persistVerified(createInboxInput("message-corrupt", "sensitive", now));
    const raw = await redis.get(stored.record.immutable.recordKey);
    if (!raw) throw new Error("expected encrypted inbox payload");
    const corrupt = JSON.parse(raw) as { encryptedEnvelope: { ciphertext: string } };
    corrupt.encryptedEnvelope.ciphertext = "AAAA";
    await redis.set(stored.record.immutable.recordKey, JSON.stringify(corrupt), { ex: 86_400 });

    expect(await inbox.beginProcessing(stored.record.immutable.recordKey)).toBeNull();
    const dead = await inbox.getRecord(stored.record.immutable.recordKey);
    expect(dead?.state).toBe("dead_letter");
    expect(dead?.encryptedEnvelope).toBeUndefined();
    expect(dead?.status.failureCategory).toBe("payload_authentication_failed");
  });

  it("rejects a late worker after an expired processing attempt is recovered", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
      threadLeaseTtlMs: 1_000,
    });
    const stored = await inbox.persistVerified(createInboxInput("message-takeover", "first owner", now));
    const first = await inbox.beginProcessing(stored.record.immutable.recordKey);
    expect(first).not.toBeNull();
    if (!first) throw new Error("expected first processing attempt");

    // Simulate a worker that dies without releasing its physical lock. The
    // recovery scan must only reclaim it after its durable attempt expiry.
    now += 1_001;
    const due = await inbox.listDue();
    expect(due).toHaveLength(1);
    expect(due[0]?.state).toBe("failed_retryable");

    const second = await inbox.beginProcessing(stored.record.immutable.recordKey);
    expect(second).not.toBeNull();
    if (!second) throw new Error("expected recovered processing attempt");
    expect(second.attemptToken).not.toBe(first.attemptToken);

    // The first worker's late Eve callback cannot advance the recovered turn.
    expect(await first.transition("session_started")).toBeNull();
    expect(await first.heartbeat()).toBe(false);
    expect((await inbox.getRecord(stored.record.immutable.recordKey))?.state).toBe("processing");

    const started = await inbox.transition(stored.record.immutable.recordKey, "session_started", {
      expectedStates: ["processing"],
      expectedAttemptToken: second.attemptToken,
    });
    expect(started?.state).toBe("session_started");
    await second.transition("dead_letter", { failureCategory: "test" });
    await second.release();
  });

  it("never repairs a partial duplicate payload with a mismatched verified identity", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const inbox = new DurableInbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const original = createInboxInput("message-identity", "original", now);
    const persisted = await inbox.persistVerified(original);

    // Mimic the recoverable gap after metadata is written but before payload
    // persistence; a same project/message id still cannot swap its identity.
    await redis.del(persisted.record.immutable.recordKey);
    const otherSender = "+14155559999";
    await expect(inbox.persistVerified({
      ...original,
      webhookId: "delivery-other",
      senderHash: hashSenderIdentifier(identitySecret, otherSender),
      envelope: {
        ...original.envelope,
        webhookId: "delivery-other",
        senderId: otherSender,
      },
    })).rejects.toThrow(/identity does not match/);
    expect(await redis.get(persisted.record.immutable.recordKey)).toBeNull();
  });
});

describe("durable outbox", () => {
  it("encrypts the validated response, retries only proven pre-send failures, and never retries unknown sends", async () => {
    let now = 10_000;
    const redis = new MemoryRedis(() => now);
    const outbox = new DurableOutbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const correlationId = "b".repeat(64);
    const stored = await outbox.persistValidatedResponse({
      correlationId,
      renderedResponse: "Grounded answer [S1]",
    });
    const raw = await redis.get(stored.record.immutable.recordKey);
    expect(raw).not.toContain("Grounded answer");

    const first = await outbox.beginSend(stored.record.immutable.recordKey);
    expect(first?.renderedResponse).toBe("Grounded answer [S1]");
    if (!first) throw new Error("expected outbox send attempt");
    expect(await outbox.beginSend(stored.record.immutable.recordKey)).toBeNull();
    await first.markPreSendFailure("adapter_not_ready", now + 1_000);

    now += 1_000;
    const retry = await outbox.beginSend(stored.record.immutable.recordKey);
    expect(retry).not.toBeNull();
    if (!retry) throw new Error("expected retry attempt");
    const unknown = await retry.markSendUnknown("provider_timeout");

    expect(unknown?.state).toBe("send_unknown");
    expect(unknown?.encryptedResponse).toBeDefined();
    expect(await outbox.listDue()).toEqual([]);

    now += 86_400_001;
    expect(await redis.get(stored.record.immutable.recordKey)).toBeNull();
    expect(await redis.get(`${stored.record.immutable.recordKey}:metadata`)).not.toBeNull();
  });

  it("reports send_unknown solely from redacted metadata", async () => {
    let now = 10_000;
    const redis = new TrackingMemoryRedis(() => now);
    const outbox = new DurableOutbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const stored = await outbox.persistValidatedResponse({
      correlationId: "e".repeat(64),
      renderedResponse: "A response operators must never read",
    });
    const attempt = await outbox.beginSend(stored.record.immutable.recordKey);
    if (!attempt) throw new Error("expected send attempt");
    await attempt.markSendUnknown("provider_timeout");
    expect(await redis.get(stored.record.immutable.recordKey)).not.toBeNull();

    now += 2_500;
    redis.readKeys.length = 0;
    const reports = await outbox.listSendUnknownReports();

    expect(reports).toEqual([{
      correlationId: "e".repeat(64),
      age: 2_500,
      attempt: 1,
      errorCategory: "provider_timeout",
    }]);
    expect(Object.keys(reports[0] ?? {})).toEqual([
      "correlationId",
      "age",
      "attempt",
      "errorCategory",
    ]);
    expect(redis.readKeys).toEqual([`${stored.record.immutable.recordKey}:metadata`]);
    await expect(outbox.listSendUnknownReports(0)).rejects.toThrow(/between 1 and 500/);

    // The operator index scores records by metadata expiry, so expired members
    // never consume the bounded report scan or trigger a payload lookup.
    now = stored.record.immutable.metadataExpiresAtMs + 1;
    redis.readKeys.length = 0;
    expect(await outbox.listSendUnknownReports()).toEqual([]);
    expect(redis.readKeys).toEqual([]);
  });

  it("repairs a partial response write and restores its pending index on an idempotent retry", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const outbox = new DurableOutbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const correlationId = "c".repeat(64);
    const first = await outbox.persistValidatedResponse({
      correlationId,
      renderedResponse: "Durable response [S1]",
    });

    // Simulate a process interruption after the metadata authority write.
    await redis.del(first.record.immutable.recordKey);
    const retried = await outbox.persistValidatedResponse({
      correlationId,
      renderedResponse: "Durable response [S1]",
    });

    expect(retried.created).toBe(false);
    expect(retried.record.encryptedResponse).toBeDefined();
    expect((await outbox.listDue()).map((record) => record.immutable.recordKey))
      .toContain(first.record.immutable.recordKey);
  });

  it("survives a lost create response without a metadata-only or undiscoverable outbox", async () => {
    let now = 0;
    const redis = new FailAfterOutboxCommitRedis(() => now);
    const outbox = new DurableOutbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const correlationId = "7".repeat(64);

    await expect(outbox.persistValidatedResponse({
      correlationId,
      renderedResponse: "Atomically published response [S1]",
    })).rejects.toThrow(/simulated disconnect/);

    const recordKey = `air:outbox:v1:record:${correlationId}`;
    expect(await redis.get(recordKey)).not.toBeNull();
    expect(await redis.get(`${recordKey}:metadata`)).not.toBeNull();
    expect((await outbox.listDue()).map((record) => record.immutable.recordKey)).toContain(recordKey);

    const retried = await outbox.persistValidatedResponse({
      correlationId,
      renderedResponse: "Atomically published response [S1]",
    });
    expect(retried.created).toBe(false);
    expect(retried.record.encryptedResponse).toBeDefined();
  });

  it("commits a confirmed sent transition atomically after a lost client response", async () => {
    let now = 0;
    const redis = new FailAfterOutboxCommitRedis(() => now);
    const outbox = new DurableOutbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const stored = await outbox.persistValidatedResponse({
      correlationId: "8".repeat(64),
      renderedResponse: "Confirmed delivery [S1]",
    }).catch(async () => outbox.persistValidatedResponse({
      correlationId: "8".repeat(64),
      renderedResponse: "Confirmed delivery [S1]",
    }));
    const attempt = await outbox.beginSend(stored.record.immutable.recordKey);
    if (!attempt) throw new Error("expected send attempt");
    redis.failAfterNextWrite();

    await expect(attempt.markSent()).rejects.toThrow(/simulated disconnect/);
    expect((await outbox.getRecord(stored.record.immutable.recordKey))?.state).toBe("sent");
    expect(await outbox.listDue()).toEqual([]);
  });

  it("fails stale send completion closed after a newer attempt takes ownership", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const outbox = new DurableOutbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const stored = await outbox.persistValidatedResponse({
      correlationId: "d".repeat(64),
      renderedResponse: "Safe response [S1]",
    });
    const first = await outbox.beginSend(stored.record.immutable.recordKey);
    if (!first) throw new Error("expected initial send attempt");
    await first.markPreSendFailure("adapter_not_ready", now);

    const second = await outbox.beginSend(stored.record.immutable.recordKey);
    if (!second) throw new Error("expected replacement send attempt");
    expect(await first.markSent()).toBeNull();
    const current = await outbox.getRecord(stored.record.immutable.recordKey);
    expect(current?.state).toBe("sending");
    expect(current?.status.activeAttemptToken).toBe(second.attemptToken);
    await second.markSendUnknown("test_cleanup");
  });

  it("keeps heartbeated sending records out of the ready-work scan", async () => {
    let now = 0;
    const redis = new MemoryRedis(() => now);
    const outbox = new DurableOutbox({
      redis,
      cipher: createQueueCipher(queueKey),
      identityHashSecret: identitySecret,
      clock: () => now,
    });
    const activeAttempts = [] as NonNullable<Awaited<ReturnType<typeof outbox.beginSend>>>[];

    // `listDue(24)` scans at most 72 index members. If these sending records
    // retain their original score after a heartbeat, they crowd a ready record
    // out of that bounded scan exactly when their old leases would expire.
    for (let index = 0; index < 72; index += 1) {
      const correlationId = `${"a".repeat(62)}${index.toString(16).padStart(2, "0")}`;
      const stored = await outbox.persistValidatedResponse({
        correlationId,
        renderedResponse: `response ${String(index)}`,
      });
      const attempt = await outbox.beginSend(stored.record.immutable.recordKey);
      if (!attempt) throw new Error("expected sending attempt");
      activeAttempts.push(attempt);
    }

    now = 74_000;
    for (const attempt of activeAttempts) {
      expect(await attempt.heartbeat()).toBe(true);
    }
    now = 75_000;
    const ready = await outbox.persistValidatedResponse({
      correlationId: "f".repeat(64),
      renderedResponse: "ready response",
    });

    expect((await outbox.listDue(24)).map((record) => record.immutable.recordKey))
      .toEqual([ready.record.immutable.recordKey]);
  });
});
