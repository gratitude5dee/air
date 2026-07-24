import { describe, expect, it, vi } from "vitest";

import { createQueueCipher, type QueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { deriveOpaqueId, hashSenderIdentifier, hashThreadIdentifier, type SenderHash, type ThreadHash } from "../../lib/security.js";
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
  type StudioExecutionSnapshot,
  type StudioJobRecord,
  type StudioJobRecordWithPayload,
  type StudioReleaseStamp,
} from "../../lib/studio/store.js";

const identitySecret = "studio-store-test-identity";
const queueKey = Buffer.alloc(32, 41).toString("base64");
const darkReleaseStamp: StudioReleaseStamp = Object.freeze({ generation: 0, phase: "dark" });
const SNAPSHOT_FINGERPRINT = `sha256:${"a".repeat(64)}`;

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

function fifoScopeId(senderHash: SenderHash, threadHash: ThreadHash): string {
  return deriveOpaqueId(identitySecret, "studio-inbox-fifo-scope", `${senderHash}\u0000${threadHash}`);
}

async function activateInboxFifo(store: StudioStore): Promise<void> {
  await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "holding" });
  const prepared = await store.prepareInboxFifoHoldingScopes();
  expect(prepared).toMatchObject({ kind: "prepared", remainingScopes: 0 });
  await expect(store.activateInboxFifoMigration()).resolves.toEqual({ kind: "activated" });
}

function job(
  cipher: QueueCipher,
  senderHash: SenderHash,
  threadHash: ThreadHash,
  jobId: string,
  now: number,
  releaseStamp: StudioReleaseStamp = darkReleaseStamp,
  outputCount = 1,
  provider: "gmi" | "fal" = "gmi",
  pollingOverrides: Partial<StudioExecutionSnapshot["polling"]> = {},
): StudioJobRecordWithPayload {
  return createStudioJob({
    jobId,
    senderHash,
    threadHash,
    mode: "imagine",
    modelSlug: "verified-model",
    provider,
    outputCount,
    reservedCents: 25,
    releaseStamp,
    payload: {
      prompt: "a private prompt",
      chatGuid: "iMessage;-;+14155550199",
      sendingLine: "+14155550000",
      replyTargetId: "reply-target-guid",
      execution: {
        schemaVersion: 2,
        workflow: {
          workflowId: "air:imagine:v1",
          workflowVersion: 1,
          stepId: "provider-generation",
        },
        model: {
          slug: "verified-model",
          provider,
          providerModelId: "verified-provider-model",
          adapterKey: `${provider}.test-adapter.v1`,
          catalogFingerprint: SNAPSHOT_FINGERPRINT,
          requestSchemaFingerprint: SNAPSHOT_FINGERPRINT,
          resultSchemaFingerprint: SNAPSHOT_FINGERPRINT,
          pricingEvidenceFingerprint: SNAPSHOT_FINGERPRINT,
        },
        output: { type: "image", count: outputCount },
        maximumQuotedCostCents: 25,
        polling: {
          requestTimeoutMs: 5_000,
          pollIntervalMs: 1_000,
          maxPollAttempts: 3,
          deadlineMs: 15_000,
          ...pollingOverrides,
        },
        releaseStamp,
      },
    },
    cipher,
    now,
  });
}

function initialNotifications(options: { readonly tapback?: boolean } = {}): {
  readonly acknowledgement: {
    readonly outboxId: string;
    readonly payload: {
      readonly kind: "text";
      readonly text: string;
      readonly thread: {
        readonly chatGuid: string;
        readonly sendingLine: string;
        readonly replyTargetId: string;
      };
    };
  };
  readonly tapback?: {
    readonly outboxId: string;
    readonly payload: {
      readonly kind: "tapback";
      readonly targetMessageId: string;
      readonly thread: {
        readonly chatGuid: string;
        readonly sendingLine: string;
        readonly replyTargetId: string;
      };
    };
  };
} {
  const thread = Object.freeze({
    chatGuid: "iMessage;-;+14155550199",
    sendingLine: "+14155550000",
    replyTargetId: "reply-target-guid",
  });
  return Object.freeze({
    acknowledgement: Object.freeze({
      outboxId: "initial-job-ack",
      payload: Object.freeze({ kind: "text" as const, text: "Working on it.", thread }),
    }),
    ...(options.tapback === true
      ? {
        tapback: Object.freeze({
          outboxId: "initial-job-tapback",
          payload: Object.freeze({ kind: "tapback" as const, targetMessageId: "reply-target-guid", thread }),
        }),
      }
      : {}),
  });
}

const studioLimits = Object.freeze({
  activeJobLimit: 1,
  dailyUserJobLimit: 3,
  perJobCapCents: 50,
  dailySpendCapCents: 100,
});

const canaryBudget = Object.freeze({
  budgetId: "store-test-canary-budget",
  totalCapCents: 100,
  falCapCents: 0,
});

async function admitAndExecute(store: StudioStore, created: StudioJobRecordWithPayload): Promise<StudioJobRecord> {
  const admitted = await store.admitJob({
    job: created,
    ...studioLimits,
    ...(created.releaseStamp.phase === "canary" ? { canaryBudget } : {}),
  });
  if (!admitted.admitted) throw new Error("expected admission");
  const executing = await store.transitionJob(created.jobId, "admitted", "executing");
  if (!executing) throw new Error("expected executing job");
  return executing;
}

async function activeCanaryFixture() {
  const base = fixture();
  expect(await base.store.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
  const releaseStamp: StudioReleaseStamp = Object.freeze({ generation: 1, phase: "canary" });
  const store = new StudioStore({
    redis: base.redis,
    cipher: base.cipher,
    identityHashSecret: identitySecret,
    prefix: base.prefix,
    clock: base.now,
    releaseStamp,
  });
  expect(await store.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
  return Object.freeze({ ...base, store, releaseStamp });
}

async function submittedProviderSubrun(
  setup: Awaited<ReturnType<typeof activeCanaryFixture>>,
  jobId: string,
  pollingOverrides: Partial<StudioExecutionSnapshot["polling"]> = {},
) {
  const { store, cipher, now, releaseStamp } = setup;
  const { senderHash, threadHash } = identities();
  const created = job(cipher, senderHash, threadHash, jobId, now(), releaseStamp, 1, "gmi", pollingOverrides);
  const executing = await admitAndExecute(store, created);
  const payload = store.decryptJobPayload(executing);
  if (!payload.execution) throw new Error("expected execution snapshot");
  const reserved = await store.ensureProviderSubrun(executing, payload.execution);
  const lease = await store.acquireProviderSubrunLease(reserved.subrunId);
  if (!lease) throw new Error("expected provider submit lease");
  await expect(store.beginProviderSubmission(reserved.subrunId, lease.token, executing)).resolves.toMatchObject({ kind: "ready" });
  const submitted = await store.finishProviderSubmission(reserved.subrunId, lease.token, executing, {
    kind: "accepted",
    providerRequestId: `private-provider-request-${jobId}`,
  });
  if (!submitted || submitted.state !== "submitted") throw new Error("expected submitted provider subrun");
  return Object.freeze({ created, executing, snapshot: payload.execution, subrun: submitted });
}

function resultAttachmentPayload(outputIndex: number) {
  return Object.freeze({
    kind: "prepared-output" as const,
    outputIndex,
    output: Object.freeze({ transport: "photon" as const, attachmentGuid: `uploaded-output-${String(outputIndex)}` }),
    thread: Object.freeze({
      chatGuid: "iMessage;-;+14155550199",
      sendingLine: "+14155550000",
      replyTargetId: "reply-target-guid",
    }),
  });
}

function onboardingBundleInput(
  store: StudioStore,
  senderHash: SenderHash,
  messageId: string,
  text: string,
) {
  const receiptId = store.receiptId("onboarding-project", messageId);
  return Object.freeze({
    senderHash,
    receiptId,
    onboardingVersion: 1,
    settings: Object.freeze({ onboardingVersion: 1, preferredMode: "imagine" }),
    bubbles: Object.freeze([
      Object.freeze({
        outboxId: deriveOpaqueId(identitySecret, "onboarding-outbox", `${receiptId}\u0000one`),
        payload: Object.freeze({
          kind: "text" as const,
          text: `First ${text}`,
          thread: Object.freeze({
            chatGuid: "iMessage;-;+14155550199",
            sendingLine: "+14155550000",
            replyTargetId: "reply-target-guid",
          }),
        }),
      }),
      Object.freeze({
        outboxId: deriveOpaqueId(identitySecret, "onboarding-outbox", `${receiptId}\u0000two`),
        payload: Object.freeze({
          kind: "text" as const,
          text: `Second ${text}`,
          thread: Object.freeze({
            chatGuid: "iMessage;-;+14155550199",
            sendingLine: "+14155550000",
            replyTargetId: "reply-target-guid",
          }),
        }),
      }),
    ] as const),
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

  it("atomically commits one receipt-idempotent onboarding pair across concurrent first contacts", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash } = identities();
    const first = onboardingBundleInput(store, senderHash, "onboarding-a", "private first route");
    const competing = onboardingBundleInput(store, senderHash, "onboarding-b", "private competing route");

    const results = await Promise.all([
      store.ensureOnboardingBundle(first),
      store.ensureOnboardingBundle(first),
      store.ensureOnboardingBundle(competing),
    ]);
    expect(results.filter((result) => result.kind === "created")).toHaveLength(1);
    expect(results.filter((result) => result.kind === "existing")).toHaveLength(2);

    const markerKey = `${prefix}:onboarding:marker:${senderHash}:v1`;
    const markerRaw = await redis.get(markerKey);
    expect(markerRaw).not.toBeNull();
    for (const privateValue of ["private first route", "private competing route", "+14155550199", "iMessage;-;+14155550199"]) {
      expect(markerRaw ?? "").not.toContain(privateValue);
    }
    const marker = JSON.parse(markerRaw ?? "{}") as { firstOutboxId?: string; secondOutboxId?: string };
    expect(marker.firstOutboxId).toBeDefined();
    expect(marker.secondOutboxId).toBeDefined();
    const records = await Promise.all([
      store.getOutbox(marker.firstOutboxId ?? "missing"),
      store.getOutbox(marker.secondOutboxId ?? "missing"),
    ]);
    expect(records).toEqual([
      expect.objectContaining({ lane: "onboarding", sequence: 1, state: "intent" }),
      expect.objectContaining({ lane: "onboarding", sequence: 2, state: "intent" }),
    ]);
    expect(await store.getSettings<typeof first.settings>(senderHash)).toEqual(first.settings);

    const bubbleOne = await store.claimNextOutbox();
    expect(bubbleOne?.record.outboxId).toBe(marker.firstOutboxId);
    if (!bubbleOne) throw new Error("expected first onboarding bubble");
    await expect(store.finishOutbox(bubbleOne.record.outboxId, bubbleOne.leaseToken, "send_unknown")).resolves.toBe(true);
    expect(await store.claimNextOutbox()).toBeNull();
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

  it("refuses to begin the cutover while a legacy claim remains recoverable", async () => {
    const { store } = fixture();
    const { senderHash, threadHash } = identities();
    const legacy = await persistScopeRecord(store, senderHash, threadHash, "fifo-legacy-leased");
    const claim = await store.claimNextInbox();
    expect(claim?.record.receiptId).toBe(legacy.receiptId);
    if (!claim) throw new Error("expected legacy receipt to be leased");

    // A claimed legacy receipt remains in the global recovery index. Starting
    // the migration here would otherwise split one scope across schedulers.
    await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "legacy_not_drained" });
    await expect(store.getInboxFifoMigrationStatus()).resolves.toEqual({
      kind: "current",
      phase: "legacy",
      holdingScopes: 0,
    });

    expect(await store.finishInbox(legacy.receiptId, claim.leaseToken, "processed")).toBe(true);
    await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "holding" });
  });

  it("drains a valid-scope legacy receipt with no durable sequence before FIFO cutover", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash, threadHash } = identities();
    const legacy = await persistScopeRecord(store, senderHash, threadHash, "unsequenced-legacy-receipt");
    const metadataKey = `${prefix}:inbox:${legacy.receiptId}`;
    const raw = await redis.get(metadataKey);
    if (!raw) throw new Error("expected legacy metadata");
    const unsequenced = { ...JSON.parse(raw) as Record<string, unknown>, scopeSequence: 0 };
    await redis.set(metadataKey, JSON.stringify(unsequenced));

    // Older receipts cannot be ordered by timestamp, but they must not be
    // silently removed from the ready index. Lease and terminalize them in
    // legacy mode, then the cutover can prove the old scheduler is drained.
    const claim = await store.claimNextInbox();
    expect(claim?.record).toMatchObject({ receiptId: legacy.receiptId, scopeSequence: 0 });
    if (!claim) throw new Error("expected unsequenced legacy claim");
    expect(await store.finishInbox(legacy.receiptId, claim.leaseToken, "maintenance_rejected")).toBe(true);
    await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "holding" });
  });

  it("holds a later same-scope receipt behind a claimed or delayed FIFO head", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    await activateInboxFifo(store);
    const first = await persistScopeRecord(store, senderHash, threadHash, "fifo-claimed-first");
    const second = await persistScopeRecord(store, senderHash, threadHash, "fifo-claimed-second");

    const firstClaim = await store.claimNextInbox();
    expect(firstClaim?.record.receiptId).toBe(first.receiptId);
    if (!firstClaim) throw new Error("expected first FIFO head");
    await expect(store.claimNextInbox()).resolves.toBeNull();

    expect(await store.retryInbox(first.receiptId, firstClaim.leaseToken, 5_000)).toBe(true);
    await expect(store.claimNextInbox()).resolves.toBeNull();
    advance(5_000);
    const recovered = await store.claimNextInbox();
    expect(recovered?.record.receiptId).toBe(first.receiptId);
    if (!recovered) throw new Error("expected delayed FIFO head recovery");
    expect(await store.finishInbox(first.receiptId, recovered.leaseToken, "processed")).toBe(true);

    const secondClaim = await store.claimNextInbox();
    expect(secondClaim?.record.receiptId).toBe(second.receiptId);
  });

  it("keeps a same-scope follower blocked through lease recovery and a later retry", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    await activateInboxFifo(store);
    const first = await persistScopeRecord(store, senderHash, threadHash, "fifo-crash-first");
    const second = await persistScopeRecord(store, senderHash, threadHash, "fifo-crash-second");

    const claimed = await store.claimNextInbox();
    expect(claimed?.record.receiptId).toBe(first.receiptId);
    if (!claimed) throw new Error("expected FIFO head claim");

    // The worker crashes without calling retry. The expired head is the only
    // member that can be recovered; B must not become independently ready.
    advance(90_001);
    const recovered = await store.claimNextInbox();
    expect(recovered?.record.receiptId).toBe(first.receiptId);
    if (!recovered) throw new Error("expected FIFO head lease recovery");
    await expect(store.claimNextInbox()).resolves.toBeNull();

    expect(await store.retryInbox(first.receiptId, recovered.leaseToken, 1_000)).toBe(true);
    await expect(store.claimNextInbox()).resolves.toBeNull();
    advance(1_000);
    const retried = await store.claimNextInbox();
    expect(retried?.record.receiptId).toBe(first.receiptId);
    if (!retried) throw new Error("expected retried FIFO head");
    expect(await store.finishInbox(first.receiptId, retried.leaseToken, "processed")).toBe(true);

    const follower = await store.claimNextInbox();
    expect(follower?.record.receiptId).toBe(second.receiptId);
  });

  it("permits a different scope while a claimed FIFO head blocks only its own scope", async () => {
    const { store, advance } = fixture();
    const { senderHash, threadHash } = identities();
    await activateInboxFifo(store);
    const firstScopeFirst = await persistScopeRecord(store, senderHash, threadHash, "fifo-scope-a-first");
    advance(1);
    const secondThreadHash = hashThreadIdentifier(identitySecret, "iMessage;-;+14155550199\u0000+14155550001");
    const otherScope = await persistScopeRecord(store, senderHash, secondThreadHash, "fifo-scope-b-first");
    const firstScopeSecond = await persistScopeRecord(store, senderHash, threadHash, "fifo-scope-a-second");

    const firstClaim = await store.claimNextInbox();
    expect(firstClaim?.record.receiptId).toBe(firstScopeFirst.receiptId);
    if (!firstClaim) throw new Error("expected first scope head");
    const otherClaim = await store.claimNextInbox();
    expect(otherClaim?.record.receiptId).toBe(otherScope.receiptId);
    expect(otherClaim?.record.receiptId).not.toBe(firstScopeSecond.receiptId);
  });

  it("never promotes a later FIFO receipt over a missing predecessor", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash, threadHash } = identities();
    await activateInboxFifo(store);
    const first = await persistScopeRecord(store, senderHash, threadHash, "fifo-gap-first");
    const missing = await persistScopeRecord(store, senderHash, threadHash, "fifo-gap-missing");
    const later = await persistScopeRecord(store, senderHash, threadHash, "fifo-gap-later");
    const scopeQueueKey = `${prefix}:inbox:fifo:queue:${fifoScopeId(senderHash, threadHash)}`;
    // Simulate a durable-store integrity loss after accepted receipt B. The
    // scheduler must hold C rather than manufacture an order around B.
    await redis.zRem(scopeQueueKey, missing.receiptId);
    await redis.del(`${prefix}:inbox:${missing.receiptId}`);

    const firstClaim = await store.claimNextInbox();
    expect(firstClaim?.record.receiptId).toBe(first.receiptId);
    if (!firstClaim) throw new Error("expected FIFO head before gap");
    expect(await store.finishInbox(first.receiptId, firstClaim.leaseToken, "processed")).toBe(true);
    await expect(store.claimNextInbox()).resolves.toBeNull();
    await expect(store.readInbox(later.receiptId)).resolves.toMatchObject({ state: "received", scopeSequence: 3 });
  });

  it("fails closed when a corrupted global-ready index names a FIFO follower before its head", async () => {
    const { redis, store, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    await activateInboxFifo(store);
    const first = await persistScopeRecord(store, senderHash, threadHash, "fifo-global-gap-first");
    const second = await persistScopeRecord(store, senderHash, threadHash, "fifo-global-gap-second");
    const readyKey = `${prefix}:inbox:ready`;

    // Simulate an index corruption that makes B look schedulable while A is
    // still the durable scope head. The scheduler must stop, not claim B.
    await redis.zRem(readyKey, first.receiptId);
    await redis.zAdd(readyKey, [{ score: now(), value: second.receiptId }]);

    await expect(store.claimNextInbox()).resolves.toBeNull();
    await expect(store.readInbox(first.receiptId)).resolves.toMatchObject({ state: "received", scopeSequence: 1 });
    await expect(store.readInbox(second.receiptId)).resolves.toMatchObject({ state: "received", scopeSequence: 2 });
  });

  it("fails closed on duplicate or malformed FIFO head scores instead of using member sort order", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash, threadHash } = identities();
    await activateInboxFifo(store);
    const first = await persistScopeRecord(store, senderHash, threadHash, "fifo-duplicate-score-first");
    const second = await persistScopeRecord(store, senderHash, threadHash, "fifo-duplicate-score-second");
    const scopeQueueKey = `${prefix}:inbox:fifo:queue:${fifoScopeId(senderHash, threadHash)}`;

    // Redis ZSET ties sort by member. That must never become an ordering
    // decision, so a duplicated receipt sequence blocks the scope entirely.
    await redis.zAdd(scopeQueueKey, [{ score: first.scopeSequence, value: second.receiptId }]);
    await expect(store.claimNextInbox()).resolves.toBeNull();
    await expect(store.readInbox(first.receiptId)).resolves.toMatchObject({ state: "received" });

    // Restore the second score only to isolate malformed-head behavior.
    await redis.zAdd(scopeQueueKey, [{ score: second.scopeSequence, value: second.receiptId }]);
    await redis.zRem(scopeQueueKey, first.receiptId);
    await redis.zAdd(scopeQueueKey, [{ score: first.scopeSequence, value: "not-an-opaque-receipt-id" }]);
    await expect(store.claimNextInbox()).resolves.toBeNull();
  });

  it("rejects FIFO persistence at the JavaScript safe-integer sequence boundary", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash, threadHash } = identities();
    await activateInboxFifo(store);
    await redis.set(
      `${prefix}:inbox:scope-sequence:${senderHash}:${threadHash}`,
      String(Number.MAX_SAFE_INTEGER),
    );

    await expect(persistScopeRecord(store, senderHash, threadHash, "fifo-safe-integer-exhausted"))
      .rejects.toThrow("durable inbox publication could not be confirmed");
  });

  it("uses holding as a no-claim migration barrier and refuses activation on an invalid head", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash, threadHash } = identities();
    const legacy = await persistScopeRecord(store, senderHash, threadHash, "fifo-legacy-not-drained");
    await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "legacy_not_drained" });
    const legacyClaim = await store.claimNextInbox();
    if (!legacyClaim) throw new Error("expected legacy receipt");
    expect(await store.finishInbox(legacy.receiptId, legacyClaim.leaseToken, "processed")).toBe(true);

    await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "holding" });
    const held = await persistScopeRecord(store, senderHash, threadHash, "fifo-held-receipt");
    await expect(store.claimNextInbox()).resolves.toBeNull();
    await expect(store.getInboxFifoMigrationStatus()).resolves.toEqual({
      kind: "current",
      phase: "holding",
      holdingScopes: 1,
    });
    await redis.del(`${prefix}:inbox:${held.receiptId}`);
    await expect(store.prepareInboxFifoHoldingScopes()).resolves.toEqual({ kind: "blocked", preparedScopes: 0 });
    await expect(store.activateInboxFifoMigration()).resolves.toEqual({ kind: "holding_not_prepared" });
  });

  it("publishes a validated holding head only after the barrier is prepared and activated", async () => {
    const { store } = fixture();
    const { senderHash, threadHash } = identities();
    await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "holding" });
    const first = await persistScopeRecord(store, senderHash, threadHash, "fifo-holding-first");
    const second = await persistScopeRecord(store, senderHash, threadHash, "fifo-holding-second");
    await expect(store.claimNextInbox()).resolves.toBeNull();
    await expect(store.prepareInboxFifoHoldingScopes()).resolves.toEqual({
      kind: "prepared",
      preparedScopes: 1,
      remainingScopes: 0,
    });
    // Preparation may populate the global scheduler, but holding remains a
    // complete execution barrier until the one-way activation succeeds.
    await expect(store.claimNextInbox()).resolves.toBeNull();
    await expect(store.activateInboxFifoMigration()).resolves.toEqual({ kind: "activated" });

    const firstClaim = await store.claimNextInbox();
    expect(firstClaim?.record.receiptId).toBe(first.receiptId);
    if (!firstClaim) throw new Error("expected prepared FIFO head");
    expect(await store.finishInbox(first.receiptId, firstClaim.leaseToken, "processed")).toBe(true);
    const secondClaim = await store.claimNextInbox();
    expect(secondClaim?.record.receiptId).toBe(second.receiptId);
  });

  it("requires a newly received holding scope to be prepared before activation", async () => {
    const { store } = fixture();
    const { senderHash, threadHash } = identities();
    const secondThreadHash = hashThreadIdentifier(identitySecret, "iMessage;-;+14155550199\u0000+14155550001");
    await expect(store.beginInboxFifoMigration()).resolves.toEqual({ kind: "holding" });

    const firstScope = await persistScopeRecord(store, senderHash, threadHash, "fifo-holding-before-prepare");
    await expect(store.prepareInboxFifoHoldingScopes()).resolves.toEqual({
      kind: "prepared",
      preparedScopes: 1,
      remainingScopes: 0,
    });

    // Webhooks continue to be receipt-durable during the operator cutover.
    // A new scope must reopen preparation rather than racing into FIFO.
    const secondScope = await persistScopeRecord(store, senderHash, secondThreadHash, "fifo-holding-after-prepare");
    await expect(store.activateInboxFifoMigration()).resolves.toEqual({ kind: "holding_not_prepared" });
    await expect(store.claimNextInbox()).resolves.toBeNull();
    await expect(store.prepareInboxFifoHoldingScopes()).resolves.toEqual({
      kind: "prepared",
      preparedScopes: 1,
      remainingScopes: 0,
    });
    await expect(store.activateInboxFifoMigration()).resolves.toEqual({ kind: "activated" });

    const firstClaim = await store.claimNextInbox();
    if (!firstClaim) throw new Error("expected a prepared holding head");
    expect([firstScope.receiptId, secondScope.receiptId]).toContain(firstClaim.record.receiptId);
    expect(await store.finishInbox(firstClaim.record.receiptId, firstClaim.leaseToken, "processed")).toBe(true);

    const secondClaim = await store.claimNextInbox();
    if (!secondClaim) throw new Error("expected the other prepared holding head");
    expect([firstScope.receiptId, secondScope.receiptId]).toContain(secondClaim.record.receiptId);
    expect(secondClaim.record.receiptId).not.toBe(firstClaim.record.receiptId);
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

  it("reconciles a confirmed-absent send once, preserves its lane, and carries only the remaining payload lifetime forward", async () => {
    const { redis, store, prefix } = fixture();
    const { senderHash } = identities();
    const sourceOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", "source");
    const laterOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", "later");
    const operatorHash = store.operatorAuditHash("on-call-operator");
    const payload = {
      kind: "text" as const,
      text: "private reconciliation delivery",
      thread: { chatGuid: "chat", sendingLine: "line", replyTargetId: "reply-guid" },
    };
    await store.createOutbox({
      outboxId: sourceOutboxId,
      senderHash,
      lane: "onboarding",
      sequence: 1,
      payload,
    });
    await store.createOutbox({
      outboxId: laterOutboxId,
      senderHash,
      lane: "onboarding",
      sequence: 2,
      payload: { ...payload, text: "later private reconciliation delivery" },
    });
    const claim = await store.claimNextOutbox();
    if (!claim) throw new Error("expected source outbox claim");
    expect(claim.record.outboxId).toBe(sourceOutboxId);
    expect(await store.finishOutbox(sourceOutboxId, claim.leaseToken, "send_unknown")).toBe(true);
    const unknown = await store.getOutbox(sourceOutboxId);
    if (!unknown) throw new Error("expected unknown source outbox");

    const absent = await store.resolveOutboxSendForReconciliation({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: unknown.stateVersion,
      resolution: "confirmed_absent",
      operatorHash,
    });
    if (absent.kind !== "applied") throw new Error("expected confirmed absence");
    await expect(store.getOutbox(sourceOutboxId)).resolves.toMatchObject({
      state: "confirmed_absent",
      stateVersion: 4,
    });
    // Outstanding reconciliation records are durable even though their
    // encrypted payload keeps its original 24-hour privacy deadline.
    expect(await redis.pttl(`${prefix}:outbox:${sourceOutboxId}`)).toBe(-1);
    await expect(store.claimNextOutbox()).resolves.toBeNull();
    const remainingPayloadTtl = await redis.pttl(`${prefix}:outbox:payload:${sourceOutboxId}`);
    expect(remainingPayloadTtl).toBeGreaterThan(0);

    const requeued = await store.requeueConfirmedAbsentOutbox({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: 4,
      operatorHash,
    });
    if (requeued.kind !== "applied" || !requeued.replacementOutboxId) {
      throw new Error("expected one audited replacement outbox");
    }
    await expect(store.getOutbox(sourceOutboxId)).resolves.toMatchObject({ state: "requeued", stateVersion: 5 });
    const sourceAfter = await store.getOutbox(sourceOutboxId);
    expect(sourceAfter).not.toHaveProperty("encryptedPayload");
    expect(await redis.get(`${prefix}:outbox:payload:${sourceOutboxId}`)).toBeNull();
    expect(await redis.pttl(`${prefix}:outbox:${sourceOutboxId}`)).toBe(7 * 24 * 60 * 60 * 1_000);
    const replacement = await store.getOutbox(requeued.replacementOutboxId);
    if (!replacement) throw new Error("expected replacement outbox");
    expect(replacement).toMatchObject({ state: "intent", stateVersion: 1, sequence: 1 });
    expect(store.decryptOutboxPayload(replacement)).toEqual(payload);
    expect(await redis.pttl(`${prefix}:outbox:payload:${requeued.replacementOutboxId}`)).toBeLessThanOrEqual(remainingPayloadTtl);
    await expect(store.getReconciliationAudit(requeued.auditId)).resolves.toMatchObject({
      action: "outbox_requeued",
      subjectId: sourceOutboxId,
      relatedId: requeued.replacementOutboxId,
      actorHash: operatorHash,
    });
    const rawAudit = await redis.get(`${prefix}:reconciliation:audit:${requeued.auditId}`);
    expect(rawAudit ?? "").not.toContain("private reconciliation delivery");
    expect(rawAudit ?? "").not.toContain("reply-guid");

    // The replacement atomically takes the unresolved lane position, so the
    // later onboarding bubble cannot overtake it.
    const replacementClaim = await store.claimNextOutbox();
    expect(replacementClaim?.record.outboxId).toBe(requeued.replacementOutboxId);
    await expect(store.requeueConfirmedAbsentOutbox({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: 4,
      operatorHash,
    })).resolves.toEqual({ kind: "conflict" });
  });

  it("terminalizes a confirmed-absent send as artifact_expired when its private payload has elapsed", async () => {
    const { redis, store, prefix, advance } = fixture();
    const { senderHash } = identities();
    const sourceOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", "expired-source");
    const operatorHash = store.operatorAuditHash("on-call-operator");
    await store.createOutbox({
      outboxId: sourceOutboxId,
      senderHash,
      lane: "onboarding",
      sequence: 1,
      payload: { kind: "text", text: "short lived result", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    const claim = await store.claimNextOutbox();
    if (!claim) throw new Error("expected source outbox claim");
    expect(await store.finishOutbox(sourceOutboxId, claim.leaseToken, "send_unknown")).toBe(true);
    const unknown = await store.getOutbox(sourceOutboxId);
    if (!unknown) throw new Error("expected unknown source outbox");
    const absent = await store.resolveOutboxSendForReconciliation({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: unknown.stateVersion,
      resolution: "confirmed_absent",
      operatorHash,
    });
    if (absent.kind !== "applied") throw new Error("expected confirmed absence");

    advance(24 * 60 * 60 * 1_000 + 1);
    const expired = await store.requeueConfirmedAbsentOutbox({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: 4,
      operatorHash,
    });
    if (expired.kind !== "payload_expired" || !expired.auditId) {
      throw new Error("expected artifact-expiry terminalization");
    }
    await expect(store.getOutbox(sourceOutboxId)).resolves.toMatchObject({
      state: "failed",
      terminalReason: "artifact_expired",
      stateVersion: 5,
    });
    expect(await redis.get(`${prefix}:outbox:payload:${sourceOutboxId}`)).toBeNull();
    await expect(store.getReconciliationAudit(expired.auditId)).resolves.toMatchObject({
      action: "outbox_artifact_expired",
      subjectId: sourceOutboxId,
      actorHash: operatorHash,
    });
  });

  it("terminalizes the expiry race between requeue payload read and atomic copy", async () => {
    const { redis, store, cipher, prefix, now, advance } = fixture();
    const { senderHash } = identities();
    const sourceOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", "copy-expiry-race");
    const operatorHash = store.operatorAuditHash("on-call-operator");
    await store.createOutbox({
      outboxId: sourceOutboxId,
      senderHash,
      lane: "onboarding",
      sequence: 1,
      payload: { kind: "text", text: "expire at copy boundary", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    const claim = await store.claimNextOutbox();
    if (!claim) throw new Error("expected source outbox claim");
    expect(await store.finishOutbox(sourceOutboxId, claim.leaseToken, "send_unknown")).toBe(true);
    const unknown = await store.getOutbox(sourceOutboxId);
    if (!unknown) throw new Error("expected unknown source outbox");
    const absent = await store.resolveOutboxSendForReconciliation({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: unknown.stateVersion,
      resolution: "confirmed_absent",
      operatorHash,
    });
    if (absent.kind !== "applied") throw new Error("expected confirmed absence");

    advance(24 * 60 * 60 * 1_000 - 1);
    // The canary fence is intentionally unarmed. Privacy expiry must still
    // win over a release-blocked requeue rather than stranding the lane.
    const active = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });
    const evaluate = redis.eval.bind(redis);
    const evaluator = vi.spyOn(redis, "eval").mockImplementation(async (script, options) => {
      if (script.includes("AIR_STUDIO_REQUEUE_CONFIRMED_ABSENT_OUTBOX_V1")) advance(2);
      return evaluate(script, options);
    });
    try {
      const result = await active.requeueConfirmedAbsentOutbox({
        outboxId: sourceOutboxId,
        expectedOutboxVersion: 4,
        operatorHash: active.operatorAuditHash("on-call-operator"),
      });
      expect(result.kind).toBe("payload_expired");
    } finally {
      evaluator.mockRestore();
    }
    await expect(store.getOutbox(sourceOutboxId)).resolves.toMatchObject({
      state: "failed",
      terminalReason: "artifact_expired",
      stateVersion: 5,
    });
  });

  it("keeps a requeued attempt in its original isolated or fallback lane", async () => {
    const { store } = fixture();
    const { senderHash } = identities();
    const operatorHash = store.operatorAuditHash("on-call-operator");
    const lanes = ["request-response", "tapback", "job-result"] as const;

    for (const lane of lanes) {
      const sourceOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", `lane-${lane}`);
      const payload = lane === "tapback"
        ? { kind: "tapback" as const, targetMessageId: "target-message", thread: { chatGuid: "chat", sendingLine: "line" } }
        : { kind: "text" as const, text: `private ${lane} result`, thread: { chatGuid: "chat", sendingLine: "line" } };
      await store.createOutbox({
        outboxId: sourceOutboxId,
        senderHash,
        lane,
        sequence: 0,
        payload,
      });
      const sourceClaim = await store.claimNextOutbox();
      if (!sourceClaim) throw new Error(`expected ${lane} source claim`);
      expect(sourceClaim.record.outboxId).toBe(sourceOutboxId);
      expect(await store.finishOutbox(sourceOutboxId, sourceClaim.leaseToken, "send_unknown")).toBe(true);
      const unknown = await store.getOutbox(sourceOutboxId);
      if (!unknown) throw new Error(`expected ${lane} unknown state`);
      const absent = await store.resolveOutboxSendForReconciliation({
        outboxId: sourceOutboxId,
        expectedOutboxVersion: unknown.stateVersion,
        resolution: "confirmed_absent",
        operatorHash,
      });
      if (absent.kind !== "applied") throw new Error(`expected ${lane} absence confirmation`);
      const requeued = await store.requeueConfirmedAbsentOutbox({
        outboxId: sourceOutboxId,
        expectedOutboxVersion: 4,
        operatorHash,
      });
      if (requeued.kind !== "applied" || !requeued.replacementOutboxId) {
        throw new Error(`expected ${lane} replacement`);
      }
      await expect(store.getOutbox(requeued.replacementOutboxId)).resolves.toMatchObject({
        laneIdentity: sourceOutboxId,
        sequence: 0,
      });
      const replacementClaim = await store.claimNextOutbox();
      if (!replacementClaim) throw new Error(`expected ${lane} replacement claim`);
      expect(replacementClaim.record.outboxId).toBe(requeued.replacementOutboxId);
      expect(await store.finishOutbox(replacementClaim.record.outboxId, replacementClaim.leaseToken, "sent")).toBe(true);
    }
  });

  it("carries a media-first pending guard into a requeue so a revoked acknowledgement cannot send", async () => {
    const { store } = fixture();
    const { senderHash, threadHash } = identities();
    const source = await persistScopeRecord(store, senderHash, threadHash, "guarded-reconciliation-source");
    const pending = await store.putPendingWithGuard(source, { attachmentId: "private-media-reference" });
    if (pending.kind !== "active") throw new Error("expected active pending guard");
    const sourceOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", "guarded-source");
    const created = await store.createPendingGuardedOutbox(pending.guard, {
      outboxId: sourceOutboxId,
      senderHash,
      lane: "request-response",
      sequence: 0,
      payload: { kind: "text", text: "I saved that attachment.", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    if (created.kind !== "persisted") throw new Error("expected guarded outbox");
    const sourceClaim = await store.claimNextOutbox();
    if (!sourceClaim) throw new Error("expected guarded source claim");
    expect(await store.finishOutbox(sourceOutboxId, sourceClaim.leaseToken, "send_unknown")).toBe(true);
    const unknown = await store.getOutbox(sourceOutboxId);
    if (!unknown) throw new Error("expected guarded unknown state");
    const operatorHash = store.operatorAuditHash("on-call-operator");
    const absent = await store.resolveOutboxSendForReconciliation({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: unknown.stateVersion,
      resolution: "confirmed_absent",
      operatorHash,
    });
    if (absent.kind !== "applied") throw new Error("expected confirmed absence");
    const cancel = await persistScopeRecord(store, senderHash, threadHash, "guarded-reconciliation-cancel");
    await expect(store.clearPending(cancel)).resolves.toEqual({ kind: "canceled" });

    const requeued = await store.requeueConfirmedAbsentOutbox({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: 4,
      operatorHash,
    });
    if (requeued.kind !== "applied" || !requeued.replacementOutboxId) {
      throw new Error("expected guarded replacement outbox");
    }
    await expect(store.getOutbox(requeued.replacementOutboxId)).resolves.toMatchObject({ pendingGuard: pending.guard });
    await expect(store.claimNextOutbox()).resolves.toBeNull();
    await expect(store.getOutbox(requeued.replacementOutboxId)).resolves.toMatchObject({ state: "canceled" });
  });

  it("terminalizes an already-expired payload while confirming absence instead of leaving a blocked lane", async () => {
    const { store, advance } = fixture();
    const { senderHash } = identities();
    const sourceOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", "expired-before-absence");
    await store.createOutbox({
      outboxId: sourceOutboxId,
      senderHash,
      lane: "onboarding",
      sequence: 1,
      payload: { kind: "text", text: "expired before operator evidence", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    const claim = await store.claimNextOutbox();
    if (!claim) throw new Error("expected source outbox claim");
    expect(await store.finishOutbox(sourceOutboxId, claim.leaseToken, "send_unknown")).toBe(true);
    const unknown = await store.getOutbox(sourceOutboxId);
    if (!unknown) throw new Error("expected unknown source outbox");

    advance(24 * 60 * 60 * 1_000 + 1);
    const resolved = await store.resolveOutboxSendForReconciliation({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: unknown.stateVersion,
      resolution: "confirmed_absent",
      operatorHash: store.operatorAuditHash("on-call-operator"),
    });
    expect(resolved.kind).toBe("applied");
    await expect(store.getOutbox(sourceOutboxId)).resolves.toMatchObject({
      state: "failed",
      terminalReason: "artifact_expired",
      stateVersion: 4,
    });
  });

  it("refuses a requeue when the replacement release is not fenced current", async () => {
    const { redis, store: dark, cipher, prefix, now } = fixture();
    const { senderHash } = identities();
    const sourceOutboxId = deriveOpaqueId(identitySecret, "reconciliation-outbox", "unarmed-active");
    const operatorHash = dark.operatorAuditHash("on-call-operator");
    await dark.createOutbox({
      outboxId: sourceOutboxId,
      senderHash,
      lane: "onboarding",
      sequence: 1,
      payload: { kind: "text", text: "do not release through an unarmed fence", thread: { chatGuid: "chat", sendingLine: "line" } },
    });
    const claim = await dark.claimNextOutbox();
    if (!claim) throw new Error("expected source outbox claim");
    expect(await dark.finishOutbox(sourceOutboxId, claim.leaseToken, "send_unknown")).toBe(true);
    const unknown = await dark.getOutbox(sourceOutboxId);
    if (!unknown) throw new Error("expected unknown source outbox");
    const absent = await dark.resolveOutboxSendForReconciliation({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: unknown.stateVersion,
      resolution: "confirmed_absent",
      operatorHash,
    });
    if (absent.kind !== "applied") throw new Error("expected confirmed absence");
    const active = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });

    await expect(active.requeueConfirmedAbsentOutbox({
      outboxId: sourceOutboxId,
      expectedOutboxVersion: 4,
      operatorHash: active.operatorAuditHash("on-call-operator"),
    })).resolves.toEqual({ kind: "release_blocked" });
    await expect(dark.getOutbox(sourceOutboxId)).resolves.toMatchObject({ state: "confirmed_absent", stateVersion: 4 });
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

  it("reports only the global release epoch needed for an exact operator CAS", async () => {
    const { redis, store, prefix } = fixture();
    await expect(store.getReleaseFenceStatus()).resolves.toEqual({ kind: "missing" });

    expect(await store.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    await expect(store.getReleaseFenceStatus()).resolves.toEqual({
      kind: "current",
      stamp: { generation: 0, phase: "dark" },
    });

    await redis.set(`${prefix}:operations:release-fence`, JSON.stringify({ schemaVersion: 1, generation: "0", phase: "dark" }));
    await expect(store.getReleaseFenceStatus()).resolves.toEqual({ kind: "invalid" });
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

  it("atomically admits an iMessage job with its acknowledgement and optional tapback before scheduling", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "initial-bundle-job", now());
    const notifications = initialNotifications({ tapback: true });
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    } as const;

    await expect(store.admitJobWithInitialNotifications({
      job: candidate,
      ...limits,
      ...notifications,
    })).resolves.toMatchObject({
      admitted: true,
      job: {
        jobId: candidate.jobId,
        initialNotifications: {
          acknowledgementOutboxId: notifications.acknowledgement.outboxId,
          tapbackOutboxId: notifications.tapback?.outboxId,
        },
      },
    });
    const acknowledgement = await store.getOutbox(notifications.acknowledgement.outboxId);
    const tapback = await store.getOutbox(notifications.tapback?.outboxId ?? "missing-tapback");
    expect(acknowledgement).toMatchObject({
      jobId: candidate.jobId,
      lane: "job-acknowledgement",
      state: "intent",
    });
    expect(tapback).toMatchObject({ jobId: candidate.jobId, lane: "tapback", state: "intent" });
    expect(await redis.zCard(`${prefix}:outbox:ready`)).toBe(2);
    const scheduled = await store.claimNextJob();
    expect(scheduled?.record).toMatchObject({ jobId: candidate.jobId, state: "admitted" });
    await scheduled?.lease.release();
  });

  it("returns only a sender-owned most recent terminal job and never lets a retry rewrite it", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const first = job(cipher, senderHash, threadHash, "recent-terminal-first", now());
    const notifications = initialNotifications({ tapback: true });
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    } as const;

    await expect(store.admitJobWithInitialNotifications({
      job: first,
      ...limits,
      ...notifications,
    })).resolves.toMatchObject({ admitted: true });
    // An active admission is intentionally not returned through the terminal
    // status lookup; `/status` reads the active pointer first.
    await expect(store.getMostRecentTerminalJob(senderHash)).resolves.toBeNull();

    await expect(store.transitionJob(first.jobId, "admitted", "failed"))
      .resolves.toMatchObject({ state: "failed" });
    await expect(store.getMostRecentTerminalJob(senderHash))
      .resolves.toMatchObject({ jobId: first.jobId, senderHash, state: "failed" });
    expect(await redis.get(`${prefix}:recent-job:${senderHash}`)).toBe(first.jobId);

    const later = job(cipher, senderHash, threadHash, "recent-terminal-later", now());
    await expect(store.admitJob({ job: later, ...limits })).resolves.toMatchObject({ admitted: true });
    await expect(store.transitionJob(later.jobId, "admitted", "executing"))
      .resolves.toMatchObject({ state: "executing" });
    await expect(store.transitionJob(later.jobId, "executing", "ready"))
      .resolves.toMatchObject({ state: "ready" });
    await expect(store.transitionJob(later.jobId, "ready", "delivering"))
      .resolves.toMatchObject({ state: "delivering" });
    await expect(store.transitionJob(later.jobId, "delivering", "delivered"))
      .resolves.toMatchObject({ state: "delivered" });
    await expect(store.getMostRecentTerminalJob(senderHash))
      .resolves.toMatchObject({ jobId: later.jobId, senderHash, state: "delivered" });

    // A delayed duplicate for the earlier receipt must recover its original
    // admission without changing which request is most recent.
    await expect(store.admitJobWithInitialNotifications({
      job: first,
      ...limits,
      ...notifications,
    })).resolves.toMatchObject({ admitted: true, job: { jobId: first.jobId } });
    await expect(store.getMostRecentTerminalJob(senderHash))
      .resolves.toMatchObject({ jobId: later.jobId, state: "delivered" });

    const otherSender = hashSenderIdentifier(identitySecret, "+14155550200");
    // Even a corrupt cross-sender opaque pointer cannot disclose another
    // caller's terminal metadata because the record owner is rechecked.
    await redis.set(`${prefix}:recent-job:${otherSender}`, later.jobId);
    await expect(store.getMostRecentTerminalJob(otherSender)).resolves.toBeNull();
  });

  it("atomically cancels an admitted job with one same-thread notification and no schedulable work", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "cancel-admitted-job", now());
    await expect(store.admitJob({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    })).resolves.toMatchObject({ admitted: true });
    const active = await store.getActiveJob(senderHash);
    if (!active) throw new Error("expected active admitted job");
    const notification = {
      outboxId: "cancel-admitted-notice",
      payload: {
        kind: "text" as const,
        text: "Canceled your queued request. No generation was submitted.",
        thread: {
          chatGuid: "iMessage;-;+14155550199",
          sendingLine: "+14155550000",
          replyTargetId: "cancel-command-guid",
        },
      },
    };

    await expect(store.cancelAdmittedJobWithNotification({ job: active, notification })).resolves.toBe("canceled");
    await expect(store.getJob(candidate.jobId)).resolves.toMatchObject({ state: "canceled" });
    await expect(store.getActiveJob(senderHash)).resolves.toBeNull();
    await expect(store.claimNextJob()).resolves.toBeNull();
    await expect(redis.get(`${prefix}:job:payload:${candidate.jobId}`)).resolves.toBeNull();
    const outbox = await store.getOutbox(notification.outboxId);
    expect(outbox).toMatchObject({
      jobId: candidate.jobId,
      lane: "request-response",
      state: "intent",
      attempts: 0,
    });
    expect(store.decryptOutboxPayload(outbox!)).toMatchObject({ text: notification.payload.text });

    // A worker crash before the inbox receipt is terminal may replay the same
    // cancellation. The pre-existing atomic notice is the idempotent proof;
    // it must not create another bubble or affect a later job.
    await expect(store.cancelAdmittedJobWithNotification({ job: active, notification })).resolves.toBe("canceled");
    expect(await redis.zCard(`${prefix}:outbox:ready`)).toBe(1);
  });

  it("refuses a cancellation notice routed outside the encrypted job conversation", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "cancel-cross-thread-job", now());
    await store.admitJob({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    });
    const active = await store.getActiveJob(senderHash);
    if (!active) throw new Error("expected active admitted job");

    await expect(store.cancelAdmittedJobWithNotification({
      job: active,
      notification: {
        outboxId: "cancel-cross-thread-notice",
        payload: {
          kind: "text",
          text: "Canceled your queued request.",
          thread: {
            chatGuid: "iMessage;-;+14155550998",
            sendingLine: "+14155550000",
            replyTargetId: "cancel-command-guid",
          },
        },
      },
    })).resolves.toBe("unavailable");
    await expect(store.getJob(candidate.jobId)).resolves.toMatchObject({ state: "admitted" });
    await expect(redis.get(`${prefix}:outbox:cancel-cross-thread-notice`)).resolves.toBeNull();
  });

  it("orders an initial Like before its acknowledgement and lets an ambiguous Like release the text", async () => {
    const { store, cipher, now } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "initial-ordering-job", now());
    const notifications = initialNotifications({ tapback: true });
    await expect(store.admitJobWithInitialNotifications({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      ...notifications,
    })).resolves.toMatchObject({ admitted: true });

    const tapback = await store.claimNextOutbox();
    expect(tapback?.record).toMatchObject({
      outboxId: notifications.tapback?.outboxId,
      lane: "tapback",
      sequence: 1,
    });
    if (!tapback) throw new Error("expected initial tapback claim");
    await expect(store.finishOutbox(tapback.record.outboxId, tapback.leaseToken, "send_unknown")).resolves.toBe(true);

    const acknowledgement = await store.claimNextOutbox();
    expect(acknowledgement?.record).toMatchObject({
      outboxId: notifications.acknowledgement.outboxId,
      lane: "job-acknowledgement",
      sequence: 2,
    });
    if (!acknowledgement) throw new Error("expected acknowledgement claim after tapback outcome");
    await expect(store.finishOutbox(acknowledgement.record.outboxId, acknowledgement.leaseToken, "sent")).resolves.toBe(true);
    await expect(store.getOutbox(tapback.record.outboxId)).resolves.toMatchObject({ state: "send_unknown" });
  });

  it("keeps an admitted initial-notification bundle idempotent after a release advance", async () => {
    const { redis, store: darkStore, cipher, prefix, now, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "bundle-retry-job", now());
    const notifications = initialNotifications({ tapback: true });
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    } as const;
    await expect(darkStore.admitJobWithInitialNotifications({
      job: candidate,
      ...limits,
      ...notifications,
    })).resolves.toMatchObject({ admitted: true });
    advance(1_000);
    const acknowledgementPayloadKey = `${prefix}:outbox:payload:${notifications.acknowledgement.outboxId}`;
    const ttlBeforeRetry = await redis.pttl(acknowledgementPayloadKey);

    expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    const activeStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });
    expect(await activeStore.advanceReleaseFence(0)).toEqual({ kind: "advanced" });

    await expect(activeStore.admitJobWithInitialNotifications({
      job: candidate,
      ...limits,
      ...notifications,
    })).resolves.toMatchObject({ admitted: true, job: { jobId: candidate.jobId } });
    expect(await redis.get(`${prefix}:rate:2026-07-18:${senderHash}`)).toBe("1");
    expect(await redis.get(`${prefix}:spend:2026-07-18`)).toBe("25");
    expect(await redis.pttl(acknowledgementPayloadKey)).toBe(ttlBeforeRetry);
  });

  it("does not create any part of an initial bundle when an active release fence is unarmed", async () => {
    const { redis, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const activeStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 1, phase: "canary" },
    });
    const candidate = job(cipher, senderHash, threadHash, "unarmed-initial-bundle", now(), { generation: 1, phase: "canary" });
    const notifications = initialNotifications({ tapback: true });

    await expect(activeStore.admitJobWithInitialNotifications({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      ...notifications,
    })).resolves.toEqual({ admitted: false, reason: "release_blocked" });
    await expect(redis.get(`${prefix}:job:${candidate.jobId}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:job:payload:${candidate.jobId}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:outbox:${notifications.acknowledgement.outboxId}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:outbox:${notifications.tapback?.outboxId ?? "missing"}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:active:${senderHash}`)).resolves.toBeNull();
  });

  it("fails closed without a reservation when an initial notification record already exists", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "orphan-notification-job", now());
    const notifications = initialNotifications();
    await redis.set(`${prefix}:outbox:${notifications.acknowledgement.outboxId}`, "orphan-metadata");

    await expect(store.admitJobWithInitialNotifications({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      ...notifications,
    })).resolves.toEqual({ admitted: false, reason: "storage_error" });
    await expect(redis.get(`${prefix}:job:${candidate.jobId}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:job:payload:${candidate.jobId}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:active:${senderHash}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:rate:2026-07-18:${senderHash}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:spend:2026-07-18`)).resolves.toBeNull();
  });

  it("refuses an acknowledgement or tapback routed outside the encrypted job conversation", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "cross-thread-initial-bundle", now());
    const notifications = initialNotifications({ tapback: true });
    const crossThreadAcknowledgement = {
      ...notifications.acknowledgement,
      payload: {
        ...notifications.acknowledgement.payload,
        thread: {
          ...notifications.acknowledgement.payload.thread,
          chatGuid: "iMessage;-;+14155550998",
        },
      },
    };

    await expect(store.admitJobWithInitialNotifications({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      acknowledgement: crossThreadAcknowledgement,
      tapback: notifications.tapback,
    })).resolves.toEqual({ admitted: false, reason: "storage_error" });
    await expect(redis.get(`${prefix}:job:${candidate.jobId}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:outbox:${notifications.acknowledgement.outboxId}`)).resolves.toBeNull();
  });

  it("omits a tapback cleanly when the inbound message has no stable reply target", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const candidate = job(cipher, senderHash, threadHash, "ack-only-job", now());
    const notifications = initialNotifications();
    await expect(store.admitJobWithInitialNotifications({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      ...notifications,
    })).resolves.toMatchObject({ admitted: true });
    expect(await redis.zCard(`${prefix}:outbox:ready`)).toBe(1);
    await expect(store.getOutbox(notifications.acknowledgement.outboxId)).resolves.toMatchObject({
      lane: "job-acknowledgement",
    });
  });

  it("fences new active admissions but preserves an existing retry after a later release change", async () => {
    const { redis, store: darkStore, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const activeStamp: StudioReleaseStamp = { generation: 1, phase: "canary" };
    const activeStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: activeStamp,
    });
    const candidate = job(cipher, senderHash, threadHash, "active-fenced-job", now(), activeStamp);
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      canaryBudget,
    } as const;

    await expect(activeStore.admitJob({ job: candidate, ...limits }))
      .resolves.toEqual({ admitted: false, reason: "release_blocked" });
    await expect(redis.get(`${prefix}:job:${candidate.jobId}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:rate:2026-07-18:${senderHash}`)).resolves.toBeNull();
    await expect(redis.get(`${prefix}:spend:2026-07-18`)).resolves.toBeNull();

    expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    expect(await activeStore.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    await expect(activeStore.admitJob({ job: candidate, ...limits })).resolves.toMatchObject({ admitted: true });

    const rollbackStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: { generation: 2, phase: "dark" },
    });
    expect(await rollbackStore.advanceReleaseFence(1)).toEqual({ kind: "advanced" });

    // The original stable receipt/job ID wins before current-fence checks;
    // replaying it must not consume a second reservation or strand the job.
    await expect(rollbackStore.admitJob({ job: candidate, ...limits })).resolves.toMatchObject({
      admitted: true,
      job: { jobId: candidate.jobId },
    });
    await expect(redis.get(`${prefix}:rate:2026-07-18:${senderHash}`)).resolves.toBe("1");
    await expect(redis.get(`${prefix}:spend:2026-07-18`)).resolves.toBe("25");
    const claim = await rollbackStore.claimNextJob();
    expect(claim?.record).toMatchObject({ jobId: candidate.jobId, state: "admitted" });
    await claim?.lease.release();
  });

  it("atomically exhausts one opaque canary budget across independent senders", async () => {
    const { redis, store: darkStore, cipher, prefix, now } = fixture();
    const firstIdentity = identities();
    const secondSenderHash = hashSenderIdentifier(identitySecret, "+14155550200");
    const secondThreadHash = hashThreadIdentifier(identitySecret, "iMessage;-;+14155550200\u0000+14155550000");
    const activeStamp: StudioReleaseStamp = { generation: 1, phase: "canary" };
    const activeStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: activeStamp,
    });
    expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    expect(await activeStore.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    const policy = Object.freeze({
      budgetId: "approved-shared-canary-budget",
      totalCapCents: 40,
      falCapCents: 0,
    });
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      canaryBudget: policy,
    } as const;
    const first = job(cipher, firstIdentity.senderHash, firstIdentity.threadHash, "canary-shared-first", now(), activeStamp);
    const second = job(cipher, secondSenderHash, secondThreadHash, "canary-shared-second", now(), activeStamp);

    const results = await Promise.all([
      activeStore.admitJob({ job: first, ...limits }),
      activeStore.admitJob({ job: second, ...limits }),
    ]);
    expect(results.filter((result) => result.admitted)).toHaveLength(1);
    expect(results.find((result) => !result.admitted)).toEqual({ admitted: false, reason: "canary_total_cap" });

    const budgetKey = `${prefix}:canary:budget:${deriveOpaqueId(identitySecret, "studio-canary-budget", policy.budgetId)}`;
    const budgetRaw = await redis.get(budgetKey);
    expect(budgetRaw).not.toBeNull();
    expect(JSON.parse(budgetRaw ?? "{}")).toEqual({
      schemaVersion: 1,
      totalReservedCents: 25,
      falReservedCents: 0,
    });
    expect(await redis.pttl(budgetKey)).toBe(-1);
    expect(await redis.get(`${prefix}:canary:budget:${policy.budgetId}`)).toBeNull();
    expect(budgetRaw).not.toContain(policy.budgetId);
  });

  it("does not double-reserve a deterministic canary job retry", async () => {
    const { redis, store: darkStore, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const activeStamp: StudioReleaseStamp = { generation: 1, phase: "canary" };
    const activeStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: activeStamp,
    });
    expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    expect(await activeStore.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    const policy = Object.freeze({ budgetId: "idempotent-canary-budget", totalCapCents: 25, falCapCents: 0 });
    const candidate = job(cipher, senderHash, threadHash, "canary-idempotent-job", now(), activeStamp);
    const input = {
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      canaryBudget: policy,
    } as const;

    await expect(activeStore.admitJob(input)).resolves.toMatchObject({ admitted: true });
    await expect(activeStore.admitJob(input)).resolves.toMatchObject({ admitted: true, job: { jobId: candidate.jobId } });
    const budgetKey = `${prefix}:canary:budget:${deriveOpaqueId(identitySecret, "studio-canary-budget", policy.budgetId)}`;
    expect(JSON.parse((await redis.get(budgetKey)) ?? "{}")).toMatchObject({ totalReservedCents: 25, falReservedCents: 0 });
  });

  it("blocks FAL at its canary subcap before publishing an initial-notification bundle", async () => {
    const { redis, store: darkStore, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const activeStamp: StudioReleaseStamp = { generation: 1, phase: "canary" };
    const activeStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: activeStamp,
    });
    expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    expect(await activeStore.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    await activateInboxFifo(activeStore);
    const policy = Object.freeze({ budgetId: "fal-zero-canary-budget", totalCapCents: 100, falCapCents: 0 });
    const candidate = job(cipher, senderHash, threadHash, "canary-fal-subcap", now(), activeStamp, 1, "fal");
    const notifications = initialNotifications({ tapback: true });

    await expect(activeStore.admitJobWithInitialNotifications({
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      canaryBudget: policy,
      ...notifications,
    })).resolves.toEqual({ admitted: false, reason: "canary_fal_cap" });
    expect(await redis.get(`${prefix}:job:${candidate.jobId}`)).toBeNull();
    expect(await redis.get(`${prefix}:outbox:${notifications.acknowledgement.outboxId}`)).toBeNull();
    const budgetKey = `${prefix}:canary:budget:${deriveOpaqueId(identitySecret, "studio-canary-budget", policy.budgetId)}`;
    expect(await redis.get(budgetKey)).toBeNull();
  });

  it("atomically requires durable FIFO for a new active iMessage admission", async () => {
    const { redis, store: darkStore, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const activeStamp: StudioReleaseStamp = { generation: 1, phase: "canary" };
    const activeStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: activeStamp,
    });
    expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    expect(await activeStore.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    const candidate = job(cipher, senderHash, threadHash, "fifo-admission-gate", now(), activeStamp);
    const notifications = initialNotifications({ tapback: true });
    const input = {
      job: candidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      canaryBudget: { budgetId: "fifo-admission-gate-budget", totalCapCents: 100, falCapCents: 0 },
      ...notifications,
    } as const;

    await expect(activeStore.admitJobWithInitialNotifications(input))
      .resolves.toEqual({ admitted: false, reason: "inbox_fifo_required" });
    expect(await redis.get(`${prefix}:job:${candidate.jobId}`)).toBeNull();
    expect(await redis.get(`${prefix}:outbox:${notifications.acknowledgement.outboxId}`)).toBeNull();

    await activateInboxFifo(activeStore);
    await expect(activeStore.admitJobWithInitialNotifications(input))
      .resolves.toMatchObject({ admitted: true, job: { jobId: candidate.jobId } });
  });

  it("never reserves a canary budget from dark or fence-blocked admission", async () => {
    const { redis, store: darkStore, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const policy = Object.freeze({ budgetId: "blocked-canary-budget", totalCapCents: 100, falCapCents: 0 });
    const darkCandidate = job(cipher, senderHash, threadHash, "dark-no-canary-reservation", now());
    await expect(darkStore.admitJob({
      job: darkCandidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      canaryBudget: policy,
    })).resolves.toMatchObject({ admitted: true });
    const budgetKey = `${prefix}:canary:budget:${deriveOpaqueId(identitySecret, "studio-canary-budget", policy.budgetId)}`;
    expect(await redis.get(budgetKey)).toBeNull();

    const blockedStamp: StudioReleaseStamp = { generation: 1, phase: "canary" };
    const blockedStore = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: blockedStamp,
    });
    const blockedCandidate = job(cipher, senderHash, threadHash, "blocked-no-canary-reservation", now(), blockedStamp);
    await expect(blockedStore.admitJob({
      job: blockedCandidate,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
      canaryBudget: policy,
    })).resolves.toEqual({ admitted: false, reason: "release_blocked" });
    expect(await redis.get(budgetKey)).toBeNull();
  });

  it("claims only matching recoverable job records and fences rescheduling by state version", async () => {
    const { redis, store, cipher, prefix, now, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const limits = {
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 50,
      dailySpendCapCents: 100,
    } as const;
    const legacy = job(cipher, senderHash, threadHash, "legacy-scheduled-job", now());
    await expect(store.admitJob({ job: legacy, ...limits })).resolves.toMatchObject({ admitted: true });

    const first = await store.claimNextJob();
    if (!first) throw new Error("expected scheduled legacy job");
    expect(first.record.jobId).toBe(legacy.jobId);
    await expect(store.rescheduleClaimedJob(
      { ...first.record, stateVersion: first.record.stateVersion + 1 },
      first.lease.token,
      1_000,
    )).resolves.toBe("conflict");
    await expect(store.rescheduleClaimedJob(first.record, first.lease.token, 1_000)).resolves.toBe("scheduled");
    await first.lease.release();

    advance(999);
    await expect(store.claimNextJob()).resolves.toBeNull();
    advance(1);
    const resumed = await store.claimNextJob();
    if (!resumed) throw new Error("expected rescheduled job");
    await expect(store.transitionJob(legacy.jobId, "admitted", "executing")).resolves.toMatchObject({ state: "executing" });
    await expect(store.transitionJob(legacy.jobId, "executing", "failed")).resolves.toMatchObject({ state: "failed" });
    await resumed.lease.release();
    await expect(store.claimNextJob()).resolves.toBeNull();

    const corrupt = job(cipher, senderHash, threadHash, "corrupt-scheduled-job", now());
    await expect(store.admitJob({ job: corrupt, ...limits })).resolves.toMatchObject({ admitted: true });
    const corruptedMetadata = JSON.parse((await redis.get(`${prefix}:job:${corrupt.jobId}`)) ?? "{}") as StudioJobRecord;
    await redis.set(`${prefix}:job:${corrupt.jobId}`, JSON.stringify({ ...corruptedMetadata, jobId: "wrong-job-id" }));
    await expect(store.claimNextJob()).resolves.toBeNull();
    await expect(redis.zCard(`${prefix}:job:ready`)).resolves.toBe(0);
  });

  it("uses distinct acknowledgement and result lanes for the same job", async () => {
    const { store } = fixture();
    const { senderHash } = identities();
    const payload = {
      kind: "text" as const,
      text: "private job response",
      thread: {
        chatGuid: "iMessage;-;+14155550199",
        sendingLine: "+14155550000",
        replyTargetId: "reply-target-guid",
      },
    };

    await expect(store.createOutbox({
      outboxId: "job-ack-lane",
      jobId: "same-job",
      senderHash,
      lane: "job-acknowledgement",
      sequence: 1,
      payload,
    })).resolves.toMatchObject({ state: "intent" });
    await expect(store.createOutbox({
      outboxId: "job-result-lane",
      jobId: "same-job",
      senderHash,
      lane: "job-result",
      sequence: 1,
      payload,
    })).resolves.toMatchObject({ state: "intent" });
  });

  it("publishes provider outputs once in one-based order and retains their cursor after terminal delivery", async () => {
    const { redis, store, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "ordered-result-job", now(), darkReleaseStamp, 2);
    await admitAndExecute(store, created);

    await expect(store.publishJobResult({
      jobId: created.jobId,
      outputIndex: 2,
      payload: resultAttachmentPayload(2),
    })).resolves.toEqual({ kind: "out_of_order", expectedOutputIndex: 1 });

    const first = await store.publishJobResult({
      jobId: created.jobId,
      outputIndex: 1,
      payload: resultAttachmentPayload(1),
    });
    if (first.kind !== "published") throw new Error("expected first output publication");
    expect(first.cursor).toMatchObject({ nextOutputIndex: 2, outputs: [{ outputIndex: 1, state: "intent" }] });

    const repeated = await store.publishJobResult({
      jobId: created.jobId,
      outputIndex: 1,
      payload: resultAttachmentPayload(1),
    });
    expect(repeated).toMatchObject({ kind: "existing", outbox: { outboxId: first.outbox.outboxId } });

    const second = await store.publishJobResult({
      jobId: created.jobId,
      outputIndex: 2,
      payload: resultAttachmentPayload(2),
    });
    if (second.kind !== "published") throw new Error("expected second output publication");
    expect(second.cursor).toMatchObject({ nextOutputIndex: 3, outputs: [
      { outputIndex: 1, outboxId: first.outbox.outboxId },
      { outputIndex: 2, outboxId: second.outbox.outboxId },
    ] });
    expect(await store.transitionJob(created.jobId, "executing", "ready")).toMatchObject({ state: "ready" });
    expect(await store.transitionJob(created.jobId, "ready", "delivering")).toMatchObject({ state: "delivering" });
    expect(await store.transitionJob(created.jobId, "delivering", "delivered")).toMatchObject({ state: "delivered" });
    expect(await redis.pttl(prefix + ":job:result-cursor:" + created.jobId)).toBe(7 * 24 * 60 * 60 * 1_000);
  });

  it("hydrates each result cursor slot from definitive retry and send state", async () => {
    const { store, cipher, now, advance } = fixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "retry-result-job", now());
    await admitAndExecute(store, created);
    const published = await store.publishJobResult({
      jobId: created.jobId,
      outputIndex: 1,
      payload: resultAttachmentPayload(1),
    });
    if (published.kind !== "published") throw new Error("expected result publication");
    const firstClaim = await store.claimNextOutbox();
    if (!firstClaim) throw new Error("expected first delivery claim");
    expect(await store.finishOutbox(firstClaim.record.outboxId, firstClaim.leaseToken, "retryable_failure")).toBe(true);
    await expect(store.getJobResultCursor(created.jobId)).resolves.toMatchObject({
      outputs: [{ outputIndex: 1, state: "retryable_failure", attempts: 1 }],
    });
    advance(30_000);
    const retry = await store.claimNextOutbox();
    if (!retry) throw new Error("expected retry delivery claim");
    expect(await store.finishOutbox(retry.record.outboxId, retry.leaseToken, "sent")).toBe(true);
    await expect(store.getJobResultCursor(created.jobId)).resolves.toMatchObject({
      outputs: [{ outputIndex: 1, state: "sent", attempts: 2 }],
    });
  });

  it("keeps the admitted stamp immutable while a fenced rollback deployment stamps result delivery", async () => {
    const { redis, store: dark, cipher, prefix, now } = fixture();
    const { senderHash, threadHash } = identities();
    expect(await dark.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    const admittedRelease: StudioReleaseStamp = { generation: 1, phase: "canary" };
    const active = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: admittedRelease,
    });
    expect(await active.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
    const created = job(cipher, senderHash, threadHash, "rollback-result-job", now(), admittedRelease);
    await admitAndExecute(active, created);

    const rollbackStamp: StudioReleaseStamp = { generation: 2, phase: "dark" };
    const rollback = new StudioStore({
      redis,
      cipher,
      identityHashSecret: identitySecret,
      prefix,
      clock: now,
      releaseStamp: rollbackStamp,
    });
    expect(await rollback.advanceReleaseFence(1)).toEqual({ kind: "advanced" });
    const published = await rollback.publishJobResult({
      jobId: created.jobId,
      outputIndex: 1,
      payload: resultAttachmentPayload(1),
    });
    if (published.kind !== "published") throw new Error("expected rollback delivery publication");
    expect(published.cursor.releaseStamp).toEqual(admittedRelease);
    expect(published.outbox.releaseStamp).toEqual(rollbackStamp);
  });

  it("atomically rebinds a confirmed-absent result cursor to its audited replacement intent", async () => {
    const { store, cipher, now } = fixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "requeue-result-job", now());
    await admitAndExecute(store, created);
    const published = await store.publishJobResult({
      jobId: created.jobId,
      outputIndex: 1,
      payload: resultAttachmentPayload(1),
    });
    if (published.kind !== "published") throw new Error("expected result publication");
    const claim = await store.claimNextOutbox();
    if (!claim) throw new Error("expected result delivery claim");
    expect(await store.finishOutbox(claim.record.outboxId, claim.leaseToken, "send_unknown")).toBe(true);
    const operatorHash = store.operatorAuditHash("cursor-recovery-operator");
    const unknown = await store.getOutbox(claim.record.outboxId);
    if (!unknown) throw new Error("expected unknown result outbox");
    expect(await store.resolveOutboxSendForReconciliation({
      outboxId: unknown.outboxId,
      expectedOutboxVersion: unknown.stateVersion,
      resolution: "confirmed_absent",
      operatorHash,
    })).toMatchObject({ kind: "applied" });
    const absent = await store.getOutbox(claim.record.outboxId);
    if (!absent) throw new Error("expected confirmed-absent outbox");
    const requeued = await store.requeueConfirmedAbsentOutbox({
      outboxId: absent.outboxId,
      expectedOutboxVersion: absent.stateVersion,
      operatorHash,
    });
    if (requeued.kind !== "applied" || !requeued.replacementOutboxId) throw new Error("expected replacement intent");
    const cursor = await store.getJobResultCursor(created.jobId);
    expect(cursor).toMatchObject({
      outputs: [{ outputIndex: 1, outboxId: requeued.replacementOutboxId, state: "intent" }],
    });
    await expect(store.getOutbox(absent.outboxId)).resolves.toMatchObject({ state: "requeued" });
    await expect(store.getReconciliationAudit(requeued.auditId)).resolves.toMatchObject({
      action: "outbox_requeued",
      subjectId: absent.outboxId,
      relatedId: requeued.replacementOutboxId,
    });
  });

  it("treats an expired Photon upload lease as reconciliation, never a second upload", async () => {
    const { store, cipher, now, advance, releaseStamp } = await activeCanaryFixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "upload-crash-job", now(), releaseStamp);
    const executing = await admitAndExecute(store, created);

    await expect(store.ensureJobResultPreparation(created.jobId, 1)).resolves.toMatchObject({ state: "reserved" });
    const firstLease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!firstLease) throw new Error("expected first upload lease");
    await expect(store.beginJobResultPreparation(executing, 1, firstLease.token))
      .resolves.toMatchObject({ kind: "ready", preparation: { state: "uploading" } });

    // Simulates a worker dying after Photon may have accepted bytes but before
    // it committed the returned attachment GUID to Redis.
    advance(90_001);
    const recoveryLease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!recoveryLease) throw new Error("expected recovered upload lease");
    await expect(store.recoverExpiredJobResultPreparation(created.jobId, 1, recoveryLease.token))
      .resolves.toMatchObject({ kind: "upload_unknown", preparation: { state: "upload_unknown" } });

    await expect(store.getPreparedJobResultOutput(created.jobId, 1)).resolves.toBeNull();
    await expect(store.getJobResultPreparation(created.jobId, 1)).resolves.toMatchObject({ state: "upload_unknown" });
    await expect(store.getJob(created.jobId)).resolves.toMatchObject({ state: "reconciliation_required" });
    await expect(store.publishPreparedJobResult(created.jobId, 1)).resolves.toEqual({ kind: "unavailable" });
  });

  it("keeps reserved and prepared Photon preparations inert in recovery-only mode", async () => {
    const { store, cipher, now, releaseStamp } = await activeCanaryFixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "recovery-only-preparation", now(), releaseStamp);
    const executing = await admitAndExecute(store, created);

    await expect(store.ensureJobResultPreparation(created.jobId, 1)).resolves.toMatchObject({ state: "reserved" });
    const reservedLease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!reservedLease) throw new Error("expected reserved recovery lease");
    try {
      await expect(store.recoverExpiredJobResultPreparation(created.jobId, 1, reservedLease.token))
        .resolves.toMatchObject({ kind: "reserved", preparation: { state: "reserved", stateVersion: 1 } });
      await expect(store.getJob(created.jobId)).resolves.toMatchObject({ state: "executing" });
    } finally {
      await reservedLease.release();
    }

    const uploadLease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!uploadLease) throw new Error("expected upload lease");
    await expect(store.beginJobResultPreparation(executing, 1, uploadLease.token)).resolves.toMatchObject({ kind: "ready" });
    await expect(store.finishJobResultPreparation(executing, 1, uploadLease.token, {
      kind: "prepared",
      output: { transport: "photon", attachmentGuid: "recovery-only-prepared-guid" },
    })).resolves.toMatchObject({ kind: "prepared" });

    const preparedLease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!preparedLease) throw new Error("expected prepared recovery lease");
    try {
      await expect(store.recoverExpiredJobResultPreparation(created.jobId, 1, preparedLease.token))
        .resolves.toMatchObject({ kind: "prepared", preparation: { state: "prepared" } });
      await expect(store.getPreparedJobResultOutput(created.jobId, 1)).resolves.toEqual({
        transport: "photon",
        attachmentGuid: "recovery-only-prepared-guid",
      });
      await expect(store.getJob(created.jobId)).resolves.toMatchObject({ state: "executing" });
    } finally {
      await preparedLease.release();
    }
  });

  it("persists an uploaded Photon GUID before cursor publication, then recovers without another upload", async () => {
    const { store, redis, cipher, prefix, now, releaseStamp } = await activeCanaryFixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "prepared-upload-job", now(), releaseStamp);
    const executing = await admitAndExecute(store, created);
    await store.ensureJobResultPreparation(created.jobId, 1);
    const lease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!lease) throw new Error("expected upload lease");
    await expect(store.beginJobResultPreparation(executing, 1, lease.token)).resolves.toMatchObject({ kind: "ready" });

    const attachmentGuid = "photon-guid-persisted-before-outbox";
    await expect(store.finishJobResultPreparation(executing, 1, lease.token, {
      kind: "prepared",
      output: { transport: "photon", attachmentGuid },
    })).resolves.toMatchObject({ kind: "prepared", preparation: { state: "prepared" } });

    // This is the crash window that used to lose the only GUID. The encrypted
    // preparation reference is now sufficient to create the result cursor on
    // a later recovery, with no provider bytes or second Photon upload.
    const preparation = await store.getJobResultPreparation(created.jobId, 1);
    if (!preparation) throw new Error("expected prepared upload record");
    const rawReference = await redis.get(`${prefix}:job:result-preparation:reference:${preparation.preparationId}`);
    expect(rawReference ?? "").not.toContain(attachmentGuid);
    await expect(store.getPreparedJobResultOutput(created.jobId, 1)).resolves.toEqual({
      transport: "photon",
      attachmentGuid,
    });

    const published = await store.publishPreparedJobResult(created.jobId, 1);
    if (published.kind !== "published") throw new Error("expected durable result publication");
    await expect(store.decryptOutboxPayload(published.outbox)).toEqual({
      kind: "prepared-output",
      outputIndex: 1,
      output: { transport: "photon", attachmentGuid },
      thread: {
        chatGuid: "iMessage;-;+14155550199",
        sendingLine: "+14155550000",
        replyTargetId: "reply-target-guid",
      },
    });
    await expect(store.publishPreparedJobResult(created.jobId, 1)).resolves.toMatchObject({
      kind: "existing",
      outbox: { outboxId: published.outbox.outboxId },
    });
  });

  it("returns a preparation to reserved only for a known pre-upload retryable failure", async () => {
    const { store, cipher, now, releaseStamp } = await activeCanaryFixture();
    const { senderHash, threadHash } = identities();
    const created = job(cipher, senderHash, threadHash, "retryable-upload-job", now(), releaseStamp);
    const executing = await admitAndExecute(store, created);
    await store.ensureJobResultPreparation(created.jobId, 1);
    const firstLease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!firstLease) throw new Error("expected first upload lease");
    await expect(store.beginJobResultPreparation(executing, 1, firstLease.token)).resolves.toMatchObject({ kind: "ready" });
    await expect(store.finishJobResultPreparation(executing, 1, firstLease.token, { kind: "retryable_failure" }))
      .resolves.toMatchObject({ kind: "retryable_failure", preparation: { state: "reserved" } });

    const retryLease = await store.acquireJobResultPreparationLease(created.jobId, 1);
    if (!retryLease) throw new Error("expected retry upload lease");
    await expect(store.beginJobResultPreparation(executing, 1, retryLease.token))
      .resolves.toMatchObject({ kind: "ready", preparation: { state: "uploading" } });
  });

  it("creates a deterministic content-free observation only after a known provider submission", async () => {
    const setup = await activeCanaryFixture();
    const { store, redis, prefix } = setup;
    const { snapshot, subrun } = await submittedProviderSubrun(setup, "observation-known-submission");

    const first = await store.ensureProviderObservation(subrun, snapshot);
    if (!first) throw new Error("expected provider observation");
    const second = await store.ensureProviderObservation(subrun, snapshot);
    expect(second).toEqual(first);
    expect(first).toMatchObject({
      subrunId: subrun.subrunId,
      jobId: subrun.jobId,
      state: "scheduled",
      pollAttempts: 0,
      maxPollAttempts: snapshot.polling.maxPollAttempts,
      requestTimeoutMs: snapshot.polling.requestTimeoutMs,
      pollIntervalMs: snapshot.polling.pollIntervalMs,
      deadlineAtMs: subrun.updatedAtMs + snapshot.polling.deadlineMs,
      nextDueAtMs: subrun.updatedAtMs,
    });
    const raw = await redis.get(`${prefix}:provider-observation:${first.observationId}`);
    expect(raw ?? "").not.toContain("private-provider-request-observation-known-submission");
    expect(raw ?? "").not.toContain("a private prompt");
    expect(raw ?? "").not.toContain("providerRequestId");
    await expect(store.getProviderObservation(subrun.subrunId)).resolves.toEqual(first);
  });

  it("retries only bounded safe provider polls after a crashed observation lease", async () => {
    const setup = await activeCanaryFixture();
    const { store, advance, now } = setup;
    const { snapshot, subrun } = await submittedProviderSubrun(setup, "observation-safe-retry", {
      deadlineMs: 300_000,
      maxPollAttempts: 3,
      pollIntervalMs: 1_000,
    });
    const observation = await store.ensureProviderObservation(subrun, snapshot);
    if (!observation) throw new Error("expected provider observation");

    const firstLease = await store.acquireProviderObservationLease(subrun.subrunId);
    if (!firstLease) throw new Error("expected first observation lease");
    await expect(store.beginProviderObservation(subrun.subrunId, firstLease.token))
      .resolves.toMatchObject({ kind: "ready", observation: { state: "observing", pollAttempts: 1 } });

    // A provider GET is read-only, so the recovery is allowed to make one
    // bounded new poll. Unlike submission recovery, it never changes the
    // subrun back to `reserved` or calls a submit primitive.
    advance(90_001);
    const recoveryLease = await store.acquireProviderObservationLease(subrun.subrunId);
    if (!recoveryLease) throw new Error("expected recovered observation lease");
    await expect(store.beginProviderObservation(subrun.subrunId, recoveryLease.token))
      .resolves.toMatchObject({ kind: "ready", observation: { state: "observing", pollAttempts: 2 } });
    await expect(store.finishProviderObservation(subrun.subrunId, firstLease.token, { kind: "succeeded" }))
      .resolves.toEqual({ kind: "lease_lost" });
    await expect(store.finishProviderObservation(subrun.subrunId, recoveryLease.token, { kind: "pending" }))
      .resolves.toMatchObject({
        kind: "pending",
        observation: { state: "scheduled", pollAttempts: 2, nextDueAtMs: now() + 1_000 },
      });

    const earlyLease = await store.acquireProviderObservationLease(subrun.subrunId);
    if (!earlyLease) throw new Error("expected early observation lease");
    await expect(store.beginProviderObservation(subrun.subrunId, earlyLease.token))
      .resolves.toMatchObject({ kind: "not_due", observation: { state: "scheduled", pollAttempts: 2 } });
    advance(1_000);
    const finalLease = await store.acquireProviderObservationLease(subrun.subrunId);
    if (!finalLease) throw new Error("expected final observation lease");
    await expect(store.beginProviderObservation(subrun.subrunId, finalLease.token))
      .resolves.toMatchObject({ kind: "ready", observation: { state: "observing", pollAttempts: 3 } });
    await expect(store.finishProviderObservation(subrun.subrunId, finalLease.token, { kind: "pending" }))
      .resolves.toMatchObject({ kind: "attempts_exhausted", observation: { state: "attempts_exhausted", pollAttempts: 3 } });
    await expect(store.getProviderSubrun(subrun.subrunId)).resolves.toMatchObject({ state: "submitted" });
  });

  it("fences a completed poll when its submitted subrun or executing job is no longer current", async () => {
    const setup = await activeCanaryFixture();
    const { store } = setup;
    const { executing, snapshot, subrun } = await submittedProviderSubrun(setup, "observation-fenced-finish");
    const observation = await store.ensureProviderObservation(subrun, snapshot);
    if (!observation) throw new Error("expected provider observation");
    const lease = await store.acquireProviderObservationLease(subrun.subrunId);
    if (!lease) throw new Error("expected observation lease");
    await expect(store.beginProviderObservation(subrun.subrunId, lease.token)).resolves.toMatchObject({ kind: "ready" });
    await expect(store.transitionJob(executing.jobId, "executing", "failed")).resolves.toMatchObject({ state: "failed" });
    await expect(store.finishProviderObservation(subrun.subrunId, lease.token, { kind: "succeeded" }))
      .resolves.toMatchObject({ kind: "canceled", observation: { state: "canceled" } });
    await expect(store.getProviderSubrun(subrun.subrunId)).resolves.toMatchObject({ state: "submitted" });
  });

  it("persists an opaque provider failure disposition atomically with its terminal observation", async () => {
    const reconciliationSetup = await activeCanaryFixture();
    const { store, redis, prefix } = reconciliationSetup;
    const { snapshot, subrun } = await submittedProviderSubrun(reconciliationSetup, "observation-reconciliation-disposition");
    await expect(store.ensureProviderObservation(subrun, snapshot)).resolves.toBeTruthy();
    const lease = await store.acquireProviderObservationLease(subrun.subrunId);
    if (!lease) throw new Error("expected reconciliation observation lease");
    await expect(store.beginProviderObservation(subrun.subrunId, lease.token)).resolves.toMatchObject({ kind: "ready" });
    await expect(store.finishProviderObservation(subrun.subrunId, lease.token, {
      kind: "failed",
      failureDisposition: "reconciliation_required",
    })).resolves.toMatchObject({
      kind: "failed",
      observation: { state: "failed", failureDisposition: "reconciliation_required" },
    });
    await expect(store.getProviderObservation(subrun.subrunId)).resolves.toMatchObject({
      state: "failed",
      failureDisposition: "reconciliation_required",
    });
    const observation = await store.getProviderObservation(subrun.subrunId);
    if (!observation) throw new Error("expected terminal reconciliation observation");
    const raw = await redis.get(`${prefix}:provider-observation:${observation.observationId}`);
    expect(raw ?? "").toContain("reconciliation_required");
    expect(raw ?? "").not.toContain("private-provider-request-observation-reconciliation-disposition");

    const defaultSetup = await activeCanaryFixture();
    const defaultSubrun = await submittedProviderSubrun(defaultSetup, "observation-default-failure-disposition");
    await expect(defaultSetup.store.ensureProviderObservation(defaultSubrun.subrun, defaultSubrun.snapshot)).resolves.toBeTruthy();
    const defaultLease = await defaultSetup.store.acquireProviderObservationLease(defaultSubrun.subrun.subrunId);
    if (!defaultLease) throw new Error("expected default observation lease");
    await expect(defaultSetup.store.beginProviderObservation(defaultSubrun.subrun.subrunId, defaultLease.token))
      .resolves.toMatchObject({ kind: "ready" });
    await expect(defaultSetup.store.finishProviderObservation(defaultSubrun.subrun.subrunId, defaultLease.token, { kind: "failed" }))
      .resolves.toMatchObject({ kind: "failed", observation: { failureDisposition: "failed" } });
  });

  it("expires a due observation at its immutable deadline without starting a provider GET", async () => {
    const setup = await activeCanaryFixture();
    const { store, advance } = setup;
    const { snapshot, subrun } = await submittedProviderSubrun(setup, "observation-deadline", { deadlineMs: 6_000 });
    const observation = await store.ensureProviderObservation(subrun, snapshot);
    if (!observation) throw new Error("expected provider observation");
    advance(6_000);
    const lease = await store.acquireProviderObservationLease(subrun.subrunId);
    if (!lease) throw new Error("expected deadline observation lease");
    await expect(store.beginProviderObservation(subrun.subrunId, lease.token))
      .resolves.toMatchObject({ kind: "deadline_exceeded", observation: { state: "deadline_exceeded", pollAttempts: 0 } });
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
    const legacySnapshotBase = job(cipher, senderHash, threadHash, "job-legacy-execution-snapshot", now());
    const legacySnapshotPayload = store.decryptJobPayload(legacySnapshotBase);
    const legacyExecutionSnapshot = {
      ...legacySnapshotBase,
      encryptedPayload: cipher.encrypt({
        ...legacySnapshotPayload,
        execution: { ...legacySnapshotPayload.execution, schemaVersion: 1 },
      }, `job:${legacySnapshotBase.jobId}`),
    } as unknown as StudioJobRecordWithPayload;
    const mismatchedBindingBase = job(cipher, senderHash, threadHash, "job-mismatched-execution-binding", now());
    const mismatchedBindingPayload = store.decryptJobPayload(mismatchedBindingBase);
    const mismatchedExecutionBinding = {
      ...mismatchedBindingBase,
      encryptedPayload: cipher.encrypt({
        ...mismatchedBindingPayload,
        execution: {
          ...mismatchedBindingPayload.execution,
          model: { ...mismatchedBindingPayload.execution?.model, adapterKey: "fal.test-adapter.v1" },
        },
      }, `job:${mismatchedBindingBase.jobId}`),
    } as unknown as StudioJobRecordWithPayload;

    expect(await store.admitJob({ job: missingPayload, ...limits })).toEqual({ admitted: false, reason: "storage_error" });
    expect(await store.admitJob({ job: oldSchema, ...limits })).toEqual({ admitted: false, reason: "storage_error" });
    expect(await store.admitJob({ job: legacyExecutionSnapshot, ...limits })).toEqual({ admitted: false, reason: "storage_error" });
    expect(await store.admitJob({ job: mismatchedExecutionBinding, ...limits })).toEqual({ admitted: false, reason: "storage_error" });
    expect(await redis.get(`${prefix}:job:job-without-payload`)).toBeNull();
    expect(await redis.get(`${prefix}:job:job-old-schema`)).toBeNull();
    expect(await redis.get(`${prefix}:job:job-legacy-execution-snapshot`)).toBeNull();
    expect(await redis.get(`${prefix}:job:job-mismatched-execution-binding`)).toBeNull();
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

  it("does not spend the physical-send budget on deadline-only pre-Photon claim deferrals", async () => {
    const { store, advance } = fixture();
    const { senderHash } = identities();
    await store.createOutbox({
      outboxId: "outbox-deadline-only-deferral",
      senderHash,
      lane: "job-result",
      sequence: 1,
      payload: {
        kind: "text",
        text: "A shared Cron deadline must not exhaust this delivery.",
        thread: { chatGuid: "chat", sendingLine: "line" },
      },
    });

    // More than the normal three-Cron retry limit: every claim is returned
    // before any Photon RPC, so each next Cron still starts at attempt one.
    for (let cron = 0; cron <= STUDIO_MAX_OUTBOX_SEND_ATTEMPTS; cron += 1) {
      const claim = await store.claimNextOutbox();
      if (!claim) throw new Error(`expected deadline deferral claim ${String(cron)}`);
      expect(claim.record).toMatchObject({ state: "sending", attempts: 1 });
      expect(await store.deferOutboxBeforePhoton(claim.record.outboxId, claim.leaseToken)).toBe(true);
      await expect(store.getOutbox(claim.record.outboxId)).resolves.toMatchObject({
        state: "intent",
        attempts: 0,
      });
    }

    // A real, known pre-send transport failure still consumes an attempt and
    // retains its normal retryable state/backoff.
    const physicalFailure = await store.claimNextOutbox();
    if (!physicalFailure) throw new Error("expected a claim after deadline deferrals");
    expect(await store.finishOutbox(physicalFailure.record.outboxId, physicalFailure.leaseToken, "retryable_failure"))
      .toBe(true);
    await expect(store.getOutbox(physicalFailure.record.outboxId)).resolves.toMatchObject({
      state: "retryable_failure",
      attempts: 1,
    });

    advance(30_000);
    const deferredRetry = await store.claimNextOutbox();
    if (!deferredRetry) throw new Error("expected retryable claim after its normal backoff");
    expect(deferredRetry.record).toMatchObject({ state: "sending", attempts: 2 });
    expect(await store.deferOutboxBeforePhoton(deferredRetry.record.outboxId, deferredRetry.leaseToken)).toBe(true);
    await expect(store.getOutbox(deferredRetry.record.outboxId)).resolves.toMatchObject({
      state: "retryable_failure",
      attempts: 1,
    });
  });

  it("exposes only legal state edges", () => {
    expect(canTransitionJob("admitted", "ready")).toBe(false);
    expect(canTransitionJob("executing", "reconciliation_required")).toBe(true);
    expect(canTransitionJob("ready", "reconciliation_required")).toBe(true);
    expect(canTransitionOutbox("sending", "intent")).toBe(true);
    expect(canTransitionOutbox("sending", "send_unknown")).toBe(true);
    expect(canTransitionOutbox("send_unknown", "intent")).toBe(false);
  });
});
