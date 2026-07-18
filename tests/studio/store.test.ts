import { describe, expect, it } from "vitest";

import { createQueueCipher, type QueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { hashSenderIdentifier, hashThreadIdentifier, type SenderHash, type ThreadHash } from "../../lib/security.js";
import {
  STUDIO_MAX_OUTBOX_SEND_ATTEMPTS,
  STUDIO_CRON_FRESHNESS_MS,
  STUDIO_REDIS_PREFIX,
  StudioStore,
  canTransitionJob,
  canTransitionOutbox,
  createStudioJob,
  type StudioInboundEnvelope,
  type StudioInboxRecord,
  type StudioJobRecord,
  type StudioJobRecordWithPayload,
} from "../../lib/studio/store.js";

const identitySecret = "studio-store-test-identity";
const queueKey = Buffer.alloc(32, 41).toString("base64");

function fixture(start = Date.UTC(2026, 6, 18, 12, 0, 0)): {
  readonly redis: MemoryRedis;
  readonly store: StudioStore;
  readonly cipher: QueueCipher;
  readonly prefix: string;
  readonly now: () => number;
  readonly advance: (milliseconds: number) => void;
} {
  let now = start;
  const prefix = `${STUDIO_REDIS_PREFIX}:test`;
  const redis = new MemoryRedis(() => now);
  const cipher = createQueueCipher(queueKey);
  return {
    redis,
    cipher,
    prefix,
    now: () => now,
    advance(milliseconds: number): void {
      now += milliseconds;
    },
    store: new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: () => now,
    }),
  };
}

function identities(): { readonly senderHash: SenderHash; readonly threadHash: ThreadHash } {
  return {
    senderHash: hashSenderIdentifier(identitySecret, "+14155550199"),
    threadHash: hashThreadIdentifier(identitySecret, "iMessage;-;+14155550199\u0000+14155550000"),
  };
}

function inbound(overrides: Partial<StudioInboundEnvelope> = {}): StudioInboundEnvelope {
  return {
    chatGuid: "iMessage;-;+14155550199",
    sendingLine: "+14155550000",
    text: "private studio prompt",
    attachments: [],
    replyTargetId: "reply-target-guid",
    ...overrides,
  };
}

async function persistScopeRecord(
  store: StudioStore,
  senderHash: SenderHash,
  threadHash: ThreadHash,
  messageId: string,
): Promise<StudioInboxRecord> {
  const persisted = await store.persistInbox({
    receiptId: store.receiptId("scope-project", messageId),
    senderHash,
    threadHash,
    envelope: inbound({ replyTargetId: `reply-${messageId}` }),
  });
  if (!persisted.created) throw new Error("expected a new scoped receipt");
  return persisted.record;
}

function job(
  cipher: QueueCipher,
  senderHash: SenderHash,
  threadHash: ThreadHash,
  jobId: string,
  now: number,
): StudioJobRecordWithPayload {
  return createStudioJob({
    jobId,
    senderHash,
    threadHash,
    mode: "imagine",
    modelSlug: "verified-model",
    provider: "gmi",
    outputCount: 1,
    reservedCents: 25,
    payload: {
      prompt: "a private prompt",
      chatGuid: "iMessage;-;+14155550199",
      sendingLine: "+14155550000",
      replyTargetId: "reply-target-guid",
    },
    cipher,
    now,
  });
}

describe("Air v2 studio durable store", () => {
  it("keeps Cron heartbeats scoped by deployment and classifies stale or malformed markers", async () => {
    const { redis, store, prefix, now, advance } = fixture();

    await store.recordCronHeartbeat("preview");
    expect(await store.getCronFreshness("preview")).toBe("fresh");
    expect(await store.getCronFreshness("production")).toBe("missing");

    advance(STUDIO_CRON_FRESHNESS_MS + 1);
    expect(await store.getCronFreshness("preview")).toBe("stale");

    await redis.set(`${prefix}:operations:cron-heartbeat:production`, "not-a-timestamp");
    expect(await store.getCronFreshness("production")).toBe("invalid");
    await redis.set(`${prefix}:operations:cron-heartbeat:development`, String(now() + 1));
    expect(await store.getCronFreshness("development")).toBe("invalid");

    advance(24 * 60 * 60 * 1_000);
    expect(await store.getCronFreshness("preview")).toBe("missing");
  });

  it("persists a verified inbound receipt before acknowledgement, dedupes it, and keeps private data encrypted", async () => {
    const { redis, store, prefix, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const receiptId = store.receiptId("photon-private-project", "photon-inbound-message-guid");

    const first = await store.persistInbox({ receiptId, senderHash, threadHash, envelope: inbound() });
    expect(first.created).toBe(true);
    const raw = await redis.get(`${prefix}:inbox:${receiptId}`);
    expect(raw).not.toBeNull();
    for (const secret of ["private studio prompt", "+14155550199", "iMessage;-;+14155550199", "photon-inbound-message-guid"]) {
      expect(raw ?? "").not.toContain(secret);
    }
    expect(store.decryptInboxEnvelope(first.record)).toEqual(inbound());

    const duplicate = await store.persistInbox({
      receiptId,
      senderHash,
      threadHash,
      envelope: inbound({ text: "replacement must never overwrite the durable receipt" }),
    });
    expect(duplicate).toMatchObject({ created: false });
    // A duplicate needs only opaque dedupe metadata; it must not rehydrate
    // the original encrypted content outside a leased worker.
    expect(duplicate.record).not.toHaveProperty("encryptedEnvelope");

    const claimed = await store.claimNextInbox();
    expect(claimed?.record).toMatchObject({ receiptId, state: "claimed", stateVersion: 2 });
    if (!claimed) throw new Error("expected a claim");
    expect(await store.finishInbox(receiptId, claimed.leaseToken, "processed")).toBe(true);
    const terminal = await store.readInbox(receiptId);
    if (!terminal) throw new Error("expected terminal inbox receipt");
    expect(terminal).toMatchObject({ receiptId, state: "processed", stateVersion: 3 });
    expect(terminal).not.toHaveProperty("encryptedEnvelope");
    expect(() => store.decryptInboxEnvelope(terminal)).toThrow("inbox envelope is unavailable");
    const scrubbed = await redis.get(`${prefix}:inbox:${receiptId}`);
    for (const privateValue of ["private studio prompt", "+14155550199", "iMessage;-;+14155550199", "reply-target-guid"]) {
      expect(scrubbed ?? "").not.toContain(privateValue);
    }
    expect(await store.claimNextInbox()).toBeNull();
    advance(7 * 24 * 60 * 60 * 1_000 - 1);
    expect(await store.readInbox(receiptId)).not.toBeNull();
    advance(1);
    expect(await store.readInbox(receiptId)).toBeNull();
  });

  it("atomically initializes first-contact settings without duplicate onboarding", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash } = identities();
    const left = { onboardingVersion: 1, preferredMode: "imagine" };
    const right = { onboardingVersion: 1, preferredMode: "animate" };

    const [first, second] = await Promise.all([
      store.initializeSettingsIfAbsent(senderHash, left),
      store.initializeSettingsIfAbsent(senderHash, right),
    ]);

    expect([first, second].filter((result) => result.created)).toHaveLength(1);
    expect(first.value).toEqual(second.value);
    expect(await store.getSettings<typeof left>(senderHash)).toEqual(first.value);
    const raw = await redis.get(`${prefix}:settings:${senderHash}`);
    expect(raw ?? "").not.toContain("onboardingVersion");
  });

  it("assigns receipt order once and rejects A → B → retry-A without wall-clock ordering", async () => {
    const { store } = fixture();
    const { senderHash, threadHash } = identities();
    const first = await persistScopeRecord(store, senderHash, threadHash, "pending-a");
    const second = await persistScopeRecord(store, senderHash, threadHash, "pending-b");
    const duplicate = await store.persistInbox({
      receiptId: first.receiptId,
      senderHash,
      threadHash,
      envelope: inbound({ text: "duplicate must retain its first scope sequence" }),
    });
    expect(first.scopeSequence).toBe(1);
    expect(second.scopeSequence).toBe(2);
    expect(duplicate).toMatchObject({ created: false, record: { scopeSequence: 1 } });

    const firstWrite = await store.putPendingWithGuard(first, { attachmentId: "attachment-a" });
    const secondWrite = await store.putPendingWithGuard(second, { attachmentId: "attachment-b" });
    expect(firstWrite.kind).toBe("active");
    expect(secondWrite.kind).toBe("active");
    await expect(store.putPendingWithGuard(first, { attachmentId: "attachment-a" }))
      .resolves.toEqual({ kind: "superseded" });
    await expect(store.getPending<{ attachmentId: string }>(senderHash, threadHash))
      .resolves.toEqual({ attachmentId: "attachment-b" });
  });

  it("uses tombstones for cancel and maintenance while allowing a genuinely later receipt", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash, threadHash } = identities();
    const first = await persistScopeRecord(store, senderHash, threadHash, "cancel-a");
    const cancel = await persistScopeRecord(store, senderHash, threadHash, "cancel-c");
    const later = await persistScopeRecord(store, senderHash, threadHash, "cancel-d");
    const firstWrite = await store.putPendingWithGuard(first, { attachmentId: "attachment-a" });
    expect(firstWrite.kind).toBe("active");
    await expect(store.clearPending(cancel)).resolves.toEqual({ kind: "canceled" });
    await expect(store.putPendingWithGuard(first, { attachmentId: "attachment-a" }))
      .resolves.toEqual({ kind: "cleared" });
    await expect(store.putPending(later, { attachmentId: "attachment-d" })).resolves.toBe(true);

    const maintenance = await persistScopeRecord(store, senderHash, threadHash, "maintenance-e");
    await expect(store.blockPendingForMaintenance(maintenance)).resolves.toEqual({ kind: "canceled" });
    await expect(store.putPendingWithGuard(later, { attachmentId: "attachment-d" }))
      .resolves.toEqual({ kind: "maintenance" });
    const control = await redis.get(`${prefix}:pending:v3:control:${senderHash}:${threadHash}`);
    for (const privateValue of ["attachment-a", "attachment-d", "private studio prompt", "reply-target-guid"]) {
      expect(control ?? "").not.toContain(privateValue);
    }
  });

  it("keeps same-receipt retries idempotent and never extends the ten-minute payload lifetime", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const source = await persistScopeRecord(store, senderHash, threadHash, "expiry-a");
    const first = await store.putPendingWithGuard(source, { attachmentId: "expiry-media" });
    if (first.kind !== "active") throw new Error("expected active pending guard");
    advance(10 * 60 * 1_000 - 1);
    const retried = await store.putPendingWithGuard(source, { attachmentId: "expiry-media" });
    expect(retried).toEqual(first);
    expect(await store.getPending<{ attachmentId: string }>(senderHash, threadHash)).toEqual({ attachmentId: "expiry-media" });

    const acknowledgement = await store.createPendingGuardedOutbox(first.guard, {
      outboxId: "expiry-media-acknowledgement",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "I saved that one attachment for 10 minutes.", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    expect(acknowledgement.kind).toBe("persisted");
    advance(1);
    await expect(store.putPendingWithGuard(source, { attachmentId: "expiry-media" }))
      .resolves.toEqual({ kind: "expired" });
    await expect(store.getPending(senderHash, threadHash)).resolves.toBeNull();
    expect(await store.claimNextOutbox()).toBeNull();
    await expect(store.getOutbox("expiry-media-acknowledgement"))
      .resolves.toMatchObject({ state: "canceled", attempts: 0 });
  });

  it("revokes guarded media acknowledgement publication and claim after a maintenance fence", async () => {
    const { store } = fixture();
    const { senderHash, threadHash } = identities();
    const source = await persistScopeRecord(store, senderHash, threadHash, "guard-a");
    const written = await store.putPendingWithGuard(source, { attachmentId: "private-attachment-before-maintenance" });
    if (written.kind !== "active") throw new Error("expected an active pending guard");
    const acknowledgement = await store.createPendingGuardedOutbox(written.guard, {
      outboxId: "pending-media-acknowledgement",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "I saved that one attachment for 10 minutes.", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    expect(acknowledgement).toMatchObject({ kind: "persisted", record: { state: "intent", pendingGuard: written.guard } });
    const maintenance = await persistScopeRecord(store, senderHash, threadHash, "guard-maintenance");
    await expect(store.blockPendingForMaintenance(maintenance)).resolves.toEqual({ kind: "canceled" });
    await expect(store.createPendingGuardedOutbox(written.guard, {
      outboxId: "stale-pending-media-acknowledgement",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "This stale acknowledgement must not persist.", thread: { chatGuid: "chat", sendingLine: "line" } },
    })).resolves.toEqual({ kind: "maintenance" });
    expect(await store.claimNextOutbox()).toBeNull();
    expect(await store.getOutbox("pending-media-acknowledgement"))
      .toMatchObject({ state: "canceled", attempts: 0 });
  });

  it("stores the media-first correction as a durable 500ms dueAt instead of holding a worker open", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const source = await persistScopeRecord(store, senderHash, threadHash, "debounce-media");
    const written = await store.putPendingWithGuard(source, { attachmentId: "debounce-media" });
    if (written.kind !== "active") throw new Error("expected active pending media");
    await expect(store.createPendingGuardedOutbox(written.guard, {
      outboxId: "debounced-media-ack",
      senderHash,
      lane: "request-response",
      sequence: 1,
      dueAtMs: source.receivedAtMs + 500,
      payload: { kind: "text", text: "I saved that attachment.", thread: { chatGuid: "chat", sendingLine: "line" } },
    })).resolves.toMatchObject({ kind: "persisted", record: { dueAtMs: source.receivedAtMs + 500 } });
    expect(await store.claimNextOutbox()).toBeNull();
    advance(499);
    expect(await store.claimNextOutbox()).toBeNull();
    advance(1);
    expect(await store.claimNextOutbox()).toMatchObject({ record: { outboxId: "debounced-media-ack", state: "sending" } });
  });

  it("cancels a debounced media correction when a later receipt is durable before it is processed", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const media = await persistScopeRecord(store, senderHash, threadHash, "adjacent-media");
    const written = await store.putPendingWithGuard(media, { attachmentId: "adjacent-media" });
    if (written.kind !== "active") throw new Error("expected active pending media");
    await expect(store.createPendingGuardedOutbox(written.guard, {
      outboxId: "adjacent-media-ack",
      senderHash,
      lane: "request-response",
      sequence: 1,
      dueAtMs: media.receivedAtMs + 500,
      payload: { kind: "text", text: "I saved that attachment.", thread: { chatGuid: "chat", sendingLine: "line" } },
    })).resolves.toMatchObject({ kind: "persisted" });

    // This receipt deliberately stays unprocessed. Its ingress sequence alone
    // must make the older correction ineligible for delivery.
    await persistScopeRecord(store, senderHash, threadHash, "adjacent-command");
    advance(500);
    expect(await store.claimNextOutbox()).toBeNull();
    await expect(store.getOutbox("adjacent-media-ack"))
      .resolves.toMatchObject({ state: "canceled", attempts: 0 });
  });

  it("separates nonterminal recovery metadata from 24-hour encrypted content", async () => {
    const { redis, store, prefix, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const receiptId = store.receiptId("project", "privacy-expired-inbox");
    await store.persistInbox({ receiptId, senderHash, threadHash, envelope: inbound({ attachments: [{ id: "short-lived-media", mimeType: "image/png", sizeBytes: 42 }] }) });

    advance(24 * 60 * 60 * 1_000);
    expect(await redis.get(`${prefix}:inbox:envelope:${receiptId}`)).toBeNull();
    await expect(store.readInbox(receiptId)).resolves.toMatchObject({ state: "received" });
    const claim = await store.claimNextInbox();
    if (!claim) throw new Error("expected expired-envelope receipt claim");
    expect(claim.record).not.toHaveProperty("encryptedEnvelope");
    expect(await store.finishInbox(receiptId, claim.leaseToken, "failed")).toBe(true);
    await expect(store.readInbox(receiptId)).resolves.toMatchObject({ state: "failed" });

    advance(7 * 24 * 60 * 60 * 1_000);
    await expect(store.readInbox(receiptId)).resolves.toBeNull();
  });

  it("keeps ambiguous outbox metadata through reconciliation while expiring the encrypted payload", async () => {
    const { store, advance } = fixture();
    const { senderHash } = identities();
    await store.createOutbox({
      outboxId: "outbox-reconciliation-retention",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "short-lived encrypted delivery content", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    const claim = await store.claimNextOutbox();
    if (!claim) throw new Error("expected outbox claim");
    expect(await store.finishOutbox(claim.record.outboxId, claim.leaseToken, "send_unknown")).toBe(true);

    advance(24 * 60 * 60 * 1_000);
    await expect(store.getOutbox(claim.record.outboxId))
      .resolves.toMatchObject({ state: "send_unknown" });
    const retained = await store.getOutbox(claim.record.outboxId);
    expect(retained).not.toHaveProperty("encryptedPayload");

    advance(7 * 24 * 60 * 60 * 1_000);
    await expect(store.getOutbox(claim.record.outboxId))
      .resolves.toMatchObject({ state: "send_unknown" });
  });

  it("requires an explicit monotonic release fence before an active generation can publish", async () => {
    const { redis, cipher, prefix, now } = fixture();
    const { senderHash } = identities();
    const active = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });

    await expect(active.createOutbox({
      outboxId: "unarmed-active-outbox",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "must not publish before the fence is armed", thread: { chatGuid: "chat", sendingLine: "line" } },
    })).rejects.toThrow("unfenced outbox");
  });

  it("fails closed when a shared release fence is malformed instead of coercing its generation", async () => {
    const { redis, store: baseline, cipher, prefix, now } = fixture();
    const { senderHash } = identities();
    expect(await baseline.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    const active = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });
    expect(await active.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    await active.createOutbox({
      outboxId: "corrupt-fence-outbox",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "must pause on malformed fence", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    // A JSON string generation must not be coerced to a valid numeric epoch.
    await redis.set(`${prefix}:operations:release-fence`, JSON.stringify({ schemaVersion: 1, generation: "1", phase: "canary" }));

    await expect(active.claimNextOutbox()).resolves.toBeNull();
    await expect(active.getOutbox("corrupt-fence-outbox"))
      .resolves.toMatchObject({ state: "intent", attempts: 0 });
    await expect(active.createOutbox({
      outboxId: "corrupt-fence-new-outbox",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "must not publish through corruption", thread: { chatGuid: "chat", sendingLine: "line" } },
    })).rejects.toThrow("unfenced outbox");
  });

  it("cancels a generic dark intent after a later active release fence advances", async () => {
    const { redis, store: dark, cipher, prefix, now } = fixture();
    const { senderHash } = identities();
    await dark.createOutbox({
      outboxId: "dark-generic-before-active",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "a dark maintenance response", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    expect(await dark.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    const active = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });
    expect(await active.advanceReleaseFence(0)).toEqual({ kind: "advanced" });

    expect(await active.claimNextOutbox()).toBeNull();
    await expect(active.getOutbox("dark-generic-before-active"))
      .resolves.toMatchObject({ state: "canceled", attempts: 0 });
  });

  it("revokes a claimed generic intent at final dispatch after a dark rollback advances the fence", async () => {
    const { redis, store: baseline, cipher, prefix, now } = fixture();
    const { senderHash } = identities();
    expect(await baseline.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    const active = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });
    expect(await active.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    await active.createOutbox({
      outboxId: "active-generic-before-rollback",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload: { kind: "text", text: "a canary response", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    const claimed = await active.claimNextOutbox();
    if (!claimed) throw new Error("expected active outbox claim");

    const rollback = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 2, phase: "dark" },
    });
    expect(await rollback.advanceReleaseFence(1)).toEqual({ kind: "advanced" });

    expect(await active.authorizeOutboxDelivery(claimed.record, claimed.leaseToken)).toBe(false);
    await expect(active.getOutbox("active-generic-before-rollback"))
      .resolves.toMatchObject({ state: "canceled", attempts: 1 });
  });

  it("enforces release-fence CAS and preserves its non-expiring generation record", async () => {
    const { store, advance } = fixture();
    expect(await store.advanceReleaseFence(0)).toEqual({ kind: "conflict" });
    expect(await store.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    expect(await store.advanceReleaseFence(-1)).toEqual({ kind: "current" });
    expect(await store.advanceReleaseFence(0)).toEqual({ kind: "current" });
    advance(8 * 24 * 60 * 60 * 1_000);
    expect(await store.advanceReleaseFence(0)).toEqual({ kind: "current" });
  });

  it("rejects a nonterminal inbox finish state without extending its encrypted envelope", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash, threadHash } = identities();
    const receiptId = store.receiptId("project", "invalid-terminal-state");
    await store.persistInbox({ receiptId, senderHash, threadHash, envelope: inbound() });
    const claim = await store.claimNextInbox();
    if (!claim) throw new Error("expected inbox claim");

    expect(await store.finishInbox(receiptId, claim.leaseToken, "received" as never)).toBe(false);
    const retained = await store.readInbox(receiptId);
    if (!retained) throw new Error("expected retained claimed inbox receipt");
    expect(retained).toMatchObject({ state: "claimed" });
    expect(retained).not.toHaveProperty("encryptedEnvelope");
    expect(store.decryptInboxEnvelope(claim.record)).toEqual(inbound());
    expect(await redis.get(`${prefix}:inbox:envelope:${receiptId}`)).not.toBeNull();
  });

  it("recovers a crashed inbox claim and permits an explicit, side-effect-free retry", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const receiptId = store.receiptId("project", "message-for-crash-recovery");
    await store.persistInbox({ receiptId, senderHash, threadHash, envelope: inbound({ replyTargetId: "message-for-crash-recovery" }) });

    const firstClaim = await store.claimNextInbox();
    if (!firstClaim) throw new Error("expected first inbox claim");
    advance(90_001);
    const recoveredClaim = await store.claimNextInbox();
    if (!recoveredClaim) throw new Error("expected reclaimed inbox receipt");
    expect(recoveredClaim.record).toMatchObject({ state: "claimed", stateVersion: 3 });
    expect(await store.finishInbox(receiptId, firstClaim.leaseToken, "processed")).toBe(false);

    expect(await store.retryInbox(receiptId, recoveredClaim.leaseToken, 5_000)).toBe(true);
    expect(await store.claimNextInbox()).toBeNull();
    advance(5_000);
    const retryClaim = await store.claimNextInbox();
    if (!retryClaim) throw new Error("expected delayed retry claim");
    expect(await store.finishInbox(receiptId, retryClaim.leaseToken, "processed")).toBe(true);
    expect(await store.claimNextInbox()).toBeNull();
  });

  it("admits jobs atomically, keeps an active job through midnight, and only clears its own active marker", async () => {
    const start = Date.UTC(2026, 6, 18, 23, 59, 30);
    const { redis, store, cipher, prefix, now, advance } = fixture(start);
    const { senderHash, threadHash } = identities();
    const first = job(cipher, senderHash, threadHash, "job-1", now());
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    } as const;

    expect(await store.admitJob({ job: first, ...limits })).toMatchObject({ admitted: true, job: { jobId: "job-1" } });
    // Replaying the same admission is idempotent; it must not consume another budget slot.
    expect(await store.admitJob({ job: first, ...limits })).toMatchObject({ admitted: true, job: { jobId: "job-1" } });
    advance(60_000);
    expect(await store.admitJob({ job: job(cipher, senderHash, threadHash, "job-2", now()), ...limits }))
      .toEqual({ admitted: false, reason: "active_job" });

    expect(await store.transitionJob("job-1", "admitted", "executing")).toMatchObject({ state: "executing" });
    expect(await store.transitionJob("job-1", "executing", "failed")).toMatchObject({ state: "failed" });
    expect(await store.admitJob({ job: job(cipher, senderHash, threadHash, "job-3", now()), ...limits }))
      .toMatchObject({ admitted: true, job: { jobId: "job-3" } });

    const secondIdentity = hashSenderIdentifier(identitySecret, "+14155550200");
    const secondThread = hashThreadIdentifier(identitySecret, "iMessage;-;+14155550200\u0000+14155550000");
    const guardedForSecondSender = job(cipher, secondIdentity, secondThread, "job-guarded", now());
    expect(await store.admitJob({ job: guardedForSecondSender, ...limits })).toMatchObject({ admitted: true });
    await redis.set(`${prefix}:active:${secondIdentity}`, "newer-job", { ex: 300 });
    expect(await store.transitionJob(guardedForSecondSender.jobId, "admitted", "canceled")).toMatchObject({ state: "canceled" });
    expect(await redis.get(`${prefix}:active:${secondIdentity}`)).toBe("newer-job");

    expect(await store.admitJob({
      job: job(cipher, senderHash, threadHash, "too-expensive", now()),
      ...limits,
      perJobCapCents: 10,
    })).toEqual({ admitted: false, reason: "per_job_cap" });
  });

  it("keeps unresolved job metadata durable while bounding its encrypted snapshot to 24 hours", async () => {
    const { redis, store, cipher, prefix, now, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "job-retention", now());
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    } as const;

    expect(await store.admitJob({ job: created, ...limits })).toMatchObject({ admitted: true });
    const metadataKey = `${prefix}:job:job-retention`;
    const payloadKey = `${prefix}:job:payload:job-retention`;
    const metadata = JSON.parse((await redis.get(metadataKey)) ?? "{}") as Record<string, unknown>;
    expect(metadata).not.toHaveProperty("encryptedPayload");
    expect(JSON.stringify(metadata)).not.toContain("a private prompt");
    expect(await redis.pttl(metadataKey)).toBe(-1);
    expect(await redis.pttl(payloadKey)).toBe(24 * 60 * 60 * 1_000);

    advance(24 * 60 * 60 * 1_000 + 1);
    const expired = await store.getJob("job-retention");
    expect(expired).toMatchObject({ state: "admitted" });
    expect(expired).not.toHaveProperty("encryptedPayload");
    expect(() => store.decryptJobPayload(expired as NonNullable<typeof expired>)).toThrow("job payload is unavailable");
    expect(await redis.pttl(metadataKey)).toBe(-1);

    expect(await store.transitionJob("job-retention", "admitted", "failed")).toMatchObject({ state: "failed" });
    await expect(store.getJob("job-retention")).resolves.toMatchObject({ state: "failed" });
    expect(await redis.get(`${prefix}:active:${senderHash}`)).toBeNull();
    expect(await redis.pttl(metadataKey)).toBe(7 * 24 * 60 * 60 * 1_000);
    expect(await redis.get(payloadKey)).toBeNull();

    advance(7 * 24 * 60 * 60 * 1_000 + 1);
    await expect(store.getJob("job-retention")).resolves.toBeNull();
  });

  it("refuses malformed job admissions before creating any durable record", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    } as const;
    const missingPayload = {
      ...job(cipher, senderHash, threadHash, "job-without-payload", now()),
      encryptedPayload: undefined,
    } as unknown as StudioJobRecordWithPayload;
    const oldSchema = {
      ...job(cipher, senderHash, threadHash, "job-old-schema", now()),
      schemaVersion: 2,
    } as unknown as StudioJobRecordWithPayload;

    expect(await store.admitJob({ job: missingPayload, ...limits })).toEqual({ admitted: false, reason: "storage_error" });
    expect(await store.admitJob({ job: oldSchema, ...limits })).toEqual({ admitted: false, reason: "storage_error" });
    expect(await redis.get(`${prefix}:job:job-without-payload`)).toBeNull();
    expect(await redis.get(`${prefix}:job:job-old-schema`)).toBeNull();
  });

  it("uses durable intents, enforces a lane head, and never retries an ambiguous outbox send", async () => {
    const { redis, store, prefix, advance } = fixture();
    const { senderHash } = identities();
    const payload = {
      kind: "text" as const,
      text: "private outbound result",
      thread: {
        chatGuid: "iMessage;-;+14155550199",
        sendingLine: "+14155550000",
        replyTargetId: "reply-target-guid",
      },
    };
    const first = await store.createOutbox({
      outboxId: "outbox-1",
      senderHash,
      lane: "onboarding",
      sequence: 1,
      payload,
    });
    const second = await store.createOutbox({
      outboxId: "outbox-2",
      senderHash,
      lane: "onboarding",
      sequence: 2,
      payload: { ...payload, text: "later private outbound result" },
    });
    expect(first).toMatchObject({ state: "intent", attempts: 0 });
    const raw = await redis.get(`${prefix}:outbox:outbox-1`);
    expect(raw ?? "").not.toContain("private outbound result");

    const claimed = await store.claimNextOutbox();
    if (!claimed) throw new Error("expected first outbox claim");
    expect(claimed.record).toMatchObject({ outboxId: "outbox-1", state: "sending", attempts: 1 });
    // The later message is due globally but cannot cross its lane head.
    expect(await store.claimNextOutbox()).toBeNull();

    advance(90_001);
    // A lost worker after it might have called iMessage becomes a reconciliation
    // record, not a fresh physical send.
    expect(await store.claimNextOutbox()).toBeNull();
    expect(await store.getOutbox("outbox-1")).toMatchObject({ state: "send_unknown", attempts: 1 });
    expect(await store.finishOutbox("outbox-1", claimed.leaseToken, "sent")).toBe(false);
    advance(24 * 60 * 60 * 1_000);
    expect(await store.claimNextOutbox()).toBeNull();
    expect(await store.getOutbox(second.outboxId)).toMatchObject({ state: "intent", attempts: 0 });

    // Every request-response is intentionally its own lane, so independent
    // inbound receipts can use a local sequence of 1 without colliding.
    await expect(store.createOutbox({
      outboxId: "request-one",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload,
    })).resolves.toMatchObject({ state: "intent" });
    await expect(store.createOutbox({
      outboxId: "request-two",
      senderHash,
      lane: "request-response",
      sequence: 1,
      payload,
    })).resolves.toMatchObject({ state: "intent" });
  });

  it("backs off only definitive pre-send failures and makes the maximum-attempt state terminal", async () => {
    const { store, advance } = fixture();
    const { senderHash } = identities();
    await store.createOutbox({
      outboxId: "outbox-retry",
      senderHash,
      lane: "job-result",
      sequence: 1,
      payload: {
        kind: "text",
        text: "safe pre-send retry fixture",
        thread: { chatGuid: "chat", sendingLine: "line" },
      },
    });

    for (let attempt = 1; attempt <= STUDIO_MAX_OUTBOX_SEND_ATTEMPTS; attempt += 1) {
      const claim = await store.claimNextOutbox();
      if (!claim) throw new Error(`expected retry claim ${String(attempt)}`);
      expect(claim.record.attempts).toBe(attempt);
      expect(await store.finishOutbox(claim.record.outboxId, claim.leaseToken, "retryable_failure")).toBe(true);
      if (attempt < STUDIO_MAX_OUTBOX_SEND_ATTEMPTS) {
        expect(await store.claimNextOutbox()).toBeNull();
        advance(attempt * 30_000);
      }
    }
    expect(await store.getOutbox("outbox-retry")).toMatchObject({ state: "failed", attempts: STUDIO_MAX_OUTBOX_SEND_ATTEMPTS });
    advance(24 * 60 * 60 * 1_000);
    expect(await store.claimNextOutbox()).toBeNull();
  });

  it("exposes only legal state edges", () => {
    expect(canTransitionJob("admitted", "ready")).toBe(false);
    expect(canTransitionJob("executing", "reconciliation_required")).toBe(true);
    expect(canTransitionOutbox("sending", "send_unknown")).toBe(true);
    expect(canTransitionOutbox("send_unknown", "intent")).toBe(false);
  });
});
