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
  type StudioJobRecord,
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
    senderIdentifier: "+14155550199",
    messageId: "photon-inbound-message-guid",
    text: "private studio prompt",
    attachments: [],
    replyTargetId: "reply-target-guid",
    ...overrides,
  };
}

function job(
  cipher: QueueCipher,
  senderHash: SenderHash,
  threadHash: ThreadHash,
  jobId: string,
  now: number,
): StudioJobRecord {
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
    const { redis, store, prefix } = fixture();
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
    expect(store.decryptInboxEnvelope(duplicate.record).text).toBe("private studio prompt");

    const claimed = await store.claimNextInbox();
    expect(claimed?.record).toMatchObject({ receiptId, state: "claimed", stateVersion: 2 });
    if (!claimed) throw new Error("expected a claim");
    expect(await store.finishInbox(receiptId, claimed.leaseToken, "processed")).toBe(true);
    expect(await store.claimNextInbox()).toBeNull();
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

  it("recovers a crashed inbox claim and permits an explicit, side-effect-free retry", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const receiptId = store.receiptId("project", "message-for-crash-recovery");
    await store.persistInbox({ receiptId, senderHash, threadHash, envelope: inbound({ messageId: "message-for-crash-recovery" }) });

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
