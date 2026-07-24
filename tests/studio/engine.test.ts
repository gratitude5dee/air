import { describe, expect, it, vi } from "vitest";

import { createQueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";
import type { StudioEnvironment, StudioReleasePhase } from "../../lib/studio/config.js";
import {
  createStudioDrainBudget,
  drainStudioJobs,
  drainStudioOutbox,
  drainStudioWithStats,
} from "../../lib/studio/engine.js";
import type { IMessageThreadReference, StudioIMessageTransport, TransportOutcome } from "../../lib/studio/imessage-transport.js";
import { validateModelRegistry, type AirModelRegistry } from "../../lib/studio/model-registry.js";
import { UnprovisionedStudioProvider, type StudioProviderPort } from "../../lib/studio/provider-port.js";
import { getStudioWorkflow, isSinglePaidStepWorkflow } from "../../lib/studio/workflows.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";
import {
  STUDIO_REDIS_PREFIX,
  StudioStore,
  type StudioAttachmentReference,
  type StudioInboundEnvelope,
  type StudioInboxRecord,
  type StudioOutboxPayload,
  type StudioOutboxRecord,
} from "../../lib/studio/store.js";

const IDENTITY_SECRET = "studio-test-identity-secret";
const SENDER_IDENTIFIER = "+14155550123";
const CHAT_GUID = "iMessage;-;+14155550999";
const SENDING_LINE = "+14155550999";

function createDarkFixture(options: {
  readonly acceptNewJobs?: boolean;
  readonly allowProviderSubmit?: boolean;
  readonly deliveryEnabled?: boolean;
  readonly releasePhase?: StudioReleasePhase;
  readonly transportVerified?: boolean;
  readonly provider?: StudioProviderPort;
  readonly registry?: AirModelRegistry;
  readonly perJobCents?: number;
  readonly dailyCents?: number;
  readonly canaryTotalCents?: number;
  readonly canaryFalCents?: number;
  readonly canaryBudgetId?: string;
  readonly modelRegistryVersion?: string | undefined;
  readonly getDeliveryTransport?: (() => Promise<StudioIMessageTransport>) | undefined;
} = {}) {
  const now = 1_784_990_400_000;
  const cipher = createQueueCipher(Buffer.alloc(32, 23).toString("base64"));
  const senderHash = hashSenderIdentifier(IDENTITY_SECRET, SENDER_IDENTIFIER);
  const threadHash = hashThreadIdentifier(IDENTITY_SECRET, CHAT_GUID);
  const releasePhase = options.releasePhase ?? "dark";
  const releaseGeneration = releasePhase === "dark" ? 0 : 1;
  const redis = new MemoryRedis(() => now);
  const store = new StudioStore({
    redis,
    cipher,
    identityHashSecret: IDENTITY_SECRET,
    clock: () => now,
    releaseStamp: { generation: releaseGeneration, phase: releasePhase },
  });
  const transport = {
    verified: options.transportVerified ?? false,
    showTyping: vi.fn(async (_thread: IMessageThreadReference): Promise<void> => undefined),
    stopTyping: vi.fn(async (_thread: IMessageThreadReference): Promise<void> => undefined),
    sendText: vi.fn(async (
      _thread: IMessageThreadReference,
      _text: string,
      _idempotencyKey: string,
    ): Promise<TransportOutcome> => ({ kind: "sent" })),
    sendAttachment: vi.fn(async (
      _thread: IMessageThreadReference,
      _attachment: StudioAttachmentReference,
      _idempotencyKey: string,
    ): Promise<TransportOutcome> => ({ kind: "sent" })),
    addLikeTapback: vi.fn(async (
      _thread: IMessageThreadReference,
      _messageId: string,
      _idempotencyKey: string,
    ): Promise<TransportOutcome> => ({ kind: "sent" })),
    fetchAttachment: vi.fn(async (
      _thread: IMessageThreadReference,
      _attachmentId: string,
    ): Promise<Uint8Array> => new Uint8Array()),
  } satisfies StudioIMessageTransport;
  const environment: StudioEnvironment = {
    deploymentMode: "preview",
    releasePhase,
    releaseGeneration,
    ...(releasePhase === "dark" ? {} : { releaseFenceSecret: "test-release-fence-secret" }),
    storageTransport: "rest",
    imessageProjectId: "test-project",
    imessageProjectSecret: "test-project-secret",
    imessageWebhookSecret: "test-webhook-secret",
    upstashRedisRestUrl: "https://redis.example.test",
    upstashRedisRestToken: "test-redis-token",
    idHashSecret: IDENTITY_SECRET,
    queueEncryptionKey: Buffer.alloc(32, 23).toString("base64"),
    allowedSenderHashes: [senderHash],
    cronSecret: "test-cron-secret",
    publicBaseUrl: "https://air.example.test",
    modelRegistryVersion: options.modelRegistryVersion ?? "2.1.0",
    dailyUserJobLimit: 3,
    activeJobLimit: 1,
    ...(releasePhase === "canary"
      ? { canaryBudgetId: options.canaryBudgetId ?? "engine-test-canary-budget" }
      : {}),
    policy: {
      acceptNewJobs: options.acceptNewJobs ?? false,
      allowProviderSubmit: options.allowProviderSubmit ?? false,
      allowProviderRecovery: false,
      deliveryEnabled: options.deliveryEnabled ?? false,
    },
    policyExplicit: true,
    spendCaps: {
      perJobCents: options.perJobCents ?? 0,
      dailyCents: options.dailyCents ?? 0,
      canaryTotalCents: options.canaryTotalCents ?? (releasePhase === "canary" ? 100 : 0),
      canaryFalCents: options.canaryFalCents ?? 0,
    },
  };
  const runtime: StudioRuntime = {
    environment,
    cipher,
    store,
    transport,
    ...(options.getDeliveryTransport === undefined ? {} : { getDeliveryTransport: options.getDeliveryTransport }),
    provider: options.provider ?? new UnprovisionedStudioProvider(),
    ...(options.registry === undefined ? {} : { registry: options.registry }),
    storageTransport: "rest",
    readiness: (_expectedRegistryVersion: string): readonly string[] => [],
    close: async (): Promise<void> => undefined,
  };

  return { runtime, store, transport, senderHash, threadHash, redis, cipher };
}

async function armFixtureReleaseFence(
  fixture: ReturnType<typeof createDarkFixture>,
  options: { readonly activateInboxFifo?: boolean } = {},
): Promise<void> {
  if (fixture.runtime.environment.releasePhase === "dark") return;
  const baseline = new StudioStore({
    redis: fixture.redis,
    cipher: fixture.cipher,
    identityHashSecret: IDENTITY_SECRET,
    releaseStamp: { generation: 0, phase: "dark" },
  });
  expect(await baseline.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
  expect(await fixture.store.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
  if (options.activateInboxFifo ?? true) {
    await expect(fixture.store.beginInboxFifoMigration()).resolves.toEqual({ kind: "holding" });
    await expect(fixture.store.prepareInboxFifoHoldingScopes()).resolves.toEqual({
      kind: "prepared",
      preparedScopes: 0,
      remainingScopes: 0,
    });
    await expect(fixture.store.activateInboxFifoMigration()).resolves.toEqual({ kind: "activated" });
  }
}

function reviewedImagineRegistry(options: {
  readonly hostedPublicInput?: boolean;
  readonly includeIncompatibleImagine?: boolean;
} = {}): AirModelRegistry {
  const reviewedModel = {
    displayName: "Reviewed GMI Imagine",
    slug: "reviewed-gmi-imagine",
    mode: "imagine",
    provider: "gmi",
    providerModelId: "reviewed-provider-image-model",
    providerBinding: {
      adapterKey: "gmi.test-adapter.v1",
      requestSchemaFingerprint: "sha256:15c9c4c639d7a17e24f635771f2d8f39840e769eec92e9d5a2f6a5b864f74c9f",
      resultSchemaFingerprint: "sha256:7c00f55d2774e6212e2c14f035d5096df824ad2f2430cf4960899258a893c5c7",
    },
    verification: {
      verifiedAt: "2026-07-18T12:00:00Z",
      catalogFingerprint: "sha256:0e1fc0e407a81cce262b73cb17d6e8709b12a038aee452ff77c11eaf5f01a4e0",
      schemaFingerprint: "sha256:572f6d4c2e2da341c2f9465d3fd8c834a85f4a533e8bf452ec1c89d4a862fa82",
    },
    input: { prompt: "required", image: false, video: false, mediaOnly: false },
    inputHosting: options.hostedPublicInput
      ? { mode: "hosted-public", host: "inputs.example.test", pathPrefix: "/reviewed-air-input/" }
      : { mode: "data-uri" },
    output: {
      type: "image",
      maximumOutputCount: 1,
      supportedOutputCounts: [1],
      supportedDurationSeconds: [],
      lockedAspectRatio: "1:1",
    },
    outputRetrieval: {
      host: "storage.example.test",
      pathPrefix: "/air-output/",
      access: "signed-url",
      publiclyAccessible: false,
      verifiedAt: "2026-07-18T12:00:00Z",
      evidenceFingerprint: "sha256:4ba1aa35208012ff19979eb847e0202d89d5e72634ff7bc8d97ed3370461ed4b",
    },
    requiredPayloadFields: [{ name: "prompt", declaredType: "string", required: true }],
    pricing: {
      source: "https://pricing.example.test/reviewed-gmi-imagine",
      currency: "USD",
      verifiedAt: "2026-07-18T12:00:00Z",
      evidenceFingerprint: "sha256:69b7d0c3f7a681c18f1b8590ab3276d1c081ed510916e094d604284a7475e350",
      quotes: [{ count: 1, durationSeconds: null, maximumQuotedCostCents: 25 }],
    },
    polling: { requestTimeoutMs: 5_000, pollIntervalMs: 1_000, maxPollAttempts: 3, deadlineMs: 15_000 },
    status: "enabled",
    hostedPublicInputAcknowledged: options.hostedPublicInput ?? false,
  };
  return validateModelRegistry({
    schemaVersion: "air-model-registry/v1",
    registryVersion: "2.1.0",
    models: [
      reviewedModel,
      ...(options.includeIncompatibleImagine ? [{
        ...reviewedModel,
        displayName: "Two Output GMI Imagine",
        slug: "two-output-gmi-imagine",
        output: {
          ...reviewedModel.output,
          maximumOutputCount: 2,
          supportedOutputCounts: [2],
        },
        pricing: {
          ...reviewedModel.pricing,
          quotes: [{ count: 2, durationSeconds: null, maximumQuotedCostCents: 50 }],
        },
      }] : []),
    ],
    defaults: { imagine: "reviewed-gmi-imagine" },
  });
}

async function persistInbound(
  fixture: ReturnType<typeof createDarkFixture>,
  messageId: string,
  text: string | null,
  attachments: readonly StudioAttachmentReference[] = [],
): Promise<string> {
  const receiptId = fixture.store.receiptId("test-project", messageId);
  const envelope: StudioInboundEnvelope = {
    chatGuid: CHAT_GUID,
    sendingLine: SENDING_LINE,
    text,
    attachments,
    replyTargetId: `reply-${messageId}`,
  };
  const persisted = await fixture.store.persistInbox({
    receiptId,
    senderHash: fixture.senderHash,
    threadHash: fixture.threadHash,
    envelope,
  });
  expect(persisted.created).toBe(true);
  return receiptId;
}

async function terminalPendingSource(
  fixture: ReturnType<typeof createDarkFixture>,
  messageId: string,
): Promise<StudioInboxRecord> {
  const receiptId = fixture.store.receiptId("test-project", messageId);
  const persisted = await fixture.store.persistInbox({
    receiptId,
    senderHash: fixture.senderHash,
    threadHash: fixture.threadHash,
    envelope: {
      chatGuid: CHAT_GUID,
      sendingLine: SENDING_LINE,
      text: null,
      attachments: [],
      replyTargetId: `reply-${messageId}`,
    },
  });
  if (!persisted.created) throw new Error("expected pending source receipt");
  const claim = await fixture.store.claimNextInbox();
  if (!claim || claim.record.receiptId !== receiptId) throw new Error("expected source receipt claim");
  if (!await fixture.store.finishInbox(receiptId, claim.leaseToken, "processed")) {
    throw new Error("expected terminal source receipt");
  }
  return persisted.record;
}

async function durableOutboxRecords(
  fixture: ReturnType<typeof createDarkFixture>,
  outboxIds: readonly string[],
): Promise<readonly StudioOutboxRecord[]> {
  const records = await Promise.all(outboxIds.map((outboxId) => fixture.store.getOutbox(outboxId)));
  if (records.some((record) => record === null)) throw new Error("expected durable outbox intent");
  return records as StudioOutboxRecord[];
}

function textFromOutboxPayload(payload: StudioOutboxPayload): string | undefined {
  return payload.kind === "text" ? payload.text : undefined;
}

describe("Air studio dark engine", () => {
  it("drains an allowlisted persisted creation into maintenance_rejected with onboarding and a durable response", async () => {
    const fixture = createDarkFixture();
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    const admitJob = vi.spyOn(fixture.store, "admitJob");
    const receiptId = await persistInbound(fixture, "creation-001", "/imagine a moonlit glass terrarium");

    expect(fixture.runtime.environment.allowedSenderHashes).toEqual([fixture.senderHash]);
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({
      receiptId,
      state: "maintenance_rejected",
    });
    expect(admitJob).not.toHaveBeenCalled();
    // The welcome pair is published by one atomic onboarding transaction;
    // the separate generic maintenance response is the only direct
    // `createOutbox` call made by this receipt.
    expect(createOutbox).toHaveBeenCalledTimes(1);

    const outboxIds = await fixture.redis.zRangeByScore(
      `${STUDIO_REDIS_PREFIX}:outbox:ready`,
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
    );
    expect(outboxIds).toHaveLength(3);
    const records = await durableOutboxRecords(fixture, outboxIds);
    expect(records.every((record) => record.state === "intent" && record.attempts === 0)).toBe(true);

    const texts = records.map((record) => textFromOutboxPayload(fixture.store.decryptOutboxPayload(record)));
    expect(texts).toEqual(expect.arrayContaining([
      expect.stringContaining("I’m Air by WZRD"),
      expect.stringContaining("Create with /imagine"),
      expect.stringContaining("maintenance mode"),
    ]));
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    expect(fixture.transport.sendAttachment).not.toHaveBeenCalled();
  });

  it("keeps a first-contact receipt recoverable when its atomic onboarding bundle cannot commit", async () => {
    const fixture = createDarkFixture();
    const ensureOnboardingBundle = vi.spyOn(fixture.store, "ensureOnboardingBundle")
      .mockResolvedValueOnce({ kind: "unavailable" });
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    const receiptId = await persistInbound(fixture, "onboarding-bundle-unavailable-001", "/help");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 0, outbox: 0 });

    // The inbound envelope remains leased/recoverable and neither a settings
    // cursor nor a one-bubble partial has been written by the engine.
    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({
      receiptId,
      state: "claimed",
    });
    await expect(fixture.store.getSettings<unknown>(fixture.senderHash)).resolves.toBeNull();
    expect(createOutbox).not.toHaveBeenCalled();
    expect(ensureOnboardingBundle).toHaveBeenCalledTimes(1);
  });

  it("does not claim or invoke a verified transport while delivery is disabled", async () => {
    const fixture = createDarkFixture({ transportVerified: true });
    const outboxId = "delivery-disabled-intent";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "This must stay durable until the delivery gate is explicitly enabled.",
      },
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);
    await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({
      state: "intent",
      attempts: 0,
    });
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.stopTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    expect(fixture.transport.sendAttachment).not.toHaveBeenCalled();
    expect(fixture.transport.addLikeTapback).not.toHaveBeenCalled();
  });

  it("does not claim or invoke a verified transport in the dark phase even when delivery is enabled", async () => {
    const fixture = createDarkFixture({ transportVerified: true, deliveryEnabled: true });
    const outboxId = "dark-phase-delivery-intent";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "The release phase must be an independent delivery gate.",
      },
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);
    await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({ state: "intent", attempts: 0 });
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
  });

  it("keeps a durable outbox intent unclaimed when deferred Photon setup is unavailable", async () => {
    const getDeliveryTransport = vi.fn(async (): Promise<StudioIMessageTransport> => {
      throw new Error("photon-token-service-unavailable");
    });
    const fixture = createDarkFixture({
      releasePhase: "canary",
      deliveryEnabled: true,
      transportVerified: false,
      getDeliveryTransport,
    });
    await armFixtureReleaseFence(fixture);
    const outboxId = "lazy-transport-unavailable-intent";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "Delivery must wait for a later Photon recovery drain.",
      },
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);

    expect(getDeliveryTransport).toHaveBeenCalledTimes(1);
    await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({
      state: "intent",
      attempts: 0,
    });
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
  });

  it("delivers a prepared provider result only through the Photon GUID capability", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    const sendPreparedOutput = vi.fn(async (): Promise<TransportOutcome> => ({ kind: "sent" }));
    Object.assign(fixture.transport, {
      preparedOutputTransport: "photon" as const,
      prepareValidatedProviderArtifact: vi.fn(),
      sendPreparedOutput,
    });
    await fixture.store.createOutbox({
      outboxId: "prepared-output-intent",
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "prepared-output",
        outputIndex: 1,
        output: { transport: "photon", attachmentGuid: "already-uploaded-photon-guid" },
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
      },
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(1);
    expect(sendPreparedOutput).toHaveBeenCalledWith(
      { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
      { transport: "photon", attachmentGuid: "already-uploaded-photon-guid" },
      expect.stringContaining("prepared-output-intent"),
      expect.objectContaining({
        deadlineAtMs: expect.any(Number),
        minimumPhysicalActionRemainingMs: expect.any(Number),
        minimumDeliveryPreparationRemainingMs: expect.any(Number),
      }),
    );
    expect(fixture.transport.sendAttachment).not.toHaveBeenCalled();
    await expect(fixture.store.getOutbox("prepared-output-intent")).resolves.toMatchObject({ state: "sent" });
  });

  it("fails an outbox whose encrypted payload disappears after claim without treating it as a send", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    const outboxId = "payload-expired-after-claim";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "This payload must never reach the transport after expiry.",
      },
    });
    const claimNextOutbox = fixture.store.claimNextOutbox.bind(fixture.store);
    vi.spyOn(fixture.store, "claimNextOutbox").mockImplementation(async () => {
      const claim = await claimNextOutbox();
      if (claim) await fixture.redis.del(`${STUDIO_REDIS_PREFIX}:outbox:payload:${outboxId}`);
      return claim;
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);

    await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({
      state: "failed",
      attempts: 1,
    });
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
  });

  it("returns repeated post-claim deadline deferrals without exhausting an outbox", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    const outboxId = "post-claim-deadline-deferral";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "No Photon operation should begin after this shared deadline closes.",
      },
    });

    const claimNextOutbox = fixture.store.claimNextOutbox.bind(fixture.store);
    let budgetNow = 1_000;
    let activeDeadline = 37_000;
    vi.spyOn(fixture.store, "claimNextOutbox").mockImplementation(async () => {
      const claim = await claimNextOutbox();
      if (claim) budgetNow = activeDeadline - 21_999;
      return claim;
    });

    // Each loop starts with the 36s admission window, then deterministically
    // closes it just after Redis claims the intent. Four passes prove the
    // normal three-attempt terminalization cannot be reached without a
    // physical Photon action.
    for (let cron = 0; cron < 4; cron += 1) {
      budgetNow = 1_000;
      activeDeadline = budgetNow + 36_000;
      const budget = createStudioDrainBudget({ now: () => budgetNow, deadlineAtMs: activeDeadline });

      await expect(drainStudioOutbox(fixture.runtime, budget)).resolves.toBe(0);
      await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({
        state: "intent",
        attempts: 0,
      });
    }

    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    expect(fixture.transport.stopTyping).not.toHaveBeenCalled();
  });

  it("does not charge an attempt when Redis authorization consumes the Photon admission window", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    const outboxId = "pre-typing-authorization-deadline";
    await fixture.store.createOutbox({
      outboxId,
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
        text: "Redis-only authorization must not spend a Photon retry.",
      },
    });

    const authorizeOutboxDelivery = fixture.store.authorizeOutboxDelivery.bind(fixture.store);
    let budgetNow = 1_000;
    let activeDeadline = 37_000;
    vi.spyOn(fixture.store, "authorizeOutboxDelivery").mockImplementation(async (...args) => {
      const allowed = await authorizeOutboxDelivery(...args);
      // The authorization has completed, but it consumed the full 22s
      // physical-action reserve before `showTyping` can be called.
      budgetNow = activeDeadline - 21_999;
      return allowed;
    });

    for (let cron = 0; cron < 4; cron += 1) {
      budgetNow = 1_000;
      activeDeadline = budgetNow + 36_000;
      const budget = createStudioDrainBudget({ now: () => budgetNow, deadlineAtMs: activeDeadline });

      await expect(drainStudioOutbox(fixture.runtime, budget)).resolves.toBe(0);
      await expect(fixture.store.getOutbox(outboxId)).resolves.toMatchObject({
        state: "intent",
        attempts: 0,
      });
    }

    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
  });

  it("returns a verified Photon tapback preflight deadline without charging a send attempt", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    fixture.transport.addLikeTapback.mockResolvedValueOnce({
      kind: "retryable_failure",
      code: "photon_tapback_deadline_exhausted",
    });
    await fixture.store.createOutbox({
      outboxId: "tapback-preflight-deadline",
      senderHash: fixture.senderHash,
      lane: "tapback",
      sequence: 1,
      payload: {
        kind: "tapback",
        targetMessageId: "target-message-guid",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
      },
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);

    expect(fixture.transport.addLikeTapback).toHaveBeenCalledTimes(1);
    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    await expect(fixture.store.getOutbox("tapback-preflight-deadline")).resolves.toMatchObject({
      state: "intent",
      attempts: 0,
    });
  });

  it("defines every launch workflow as exactly one provider-generation paid step", () => {
    for (const mode of ["imagine", "animate", "edit-image", "edit-video"] as const) {
      const workflow = getStudioWorkflow(mode);
      expect(isSinglePaidStepWorkflow(workflow)).toBe(true);
      expect(workflow.workflowId).toBe(`air:${mode}:v1`);
      expect(workflow.paidSteps).toEqual([{ stepId: "provider-generation", kind: "provider-generation" }]);
    }
  });

  it("cancels a pre-existing pending intent before a dark media-first receipt", async () => {
    const fixture = createDarkFixture();
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    const source = await terminalPendingSource(fixture, "older-pending-source");
    await fixture.store.putPending(source, {
      schemaVersion: 1,
      kind: "media",
      attachment: { id: "older-attachment", mimeType: "image/png", sizeBytes: 42 },
      createdAtMs: source.receivedAtMs,
    });
    const receiptId = await persistInbound(
      fixture,
      "media-only-dark-001",
      null,
      [{ id: "new-attachment", mimeType: "image/png", sizeBytes: 42 }],
    );

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "maintenance_rejected" });
    await expect(fixture.store.getPending(fixture.senderHash, fixture.threadHash)).resolves.toBeNull();
    const records = await durableOutboxRecords(
      fixture,
      createOutbox.mock.calls.map(([input]) => input.outboxId),
    );
    expect(records.map((record) => textFromOutboxPayload(fixture.store.decryptOutboxPayload(record)))).toEqual(expect.arrayContaining([
      "Air is in maintenance mode; I canceled the pending request. No generation was submitted.",
    ]));
  });

  it("does not retain a new media-first attachment in the dark phase", async () => {
    const fixture = createDarkFixture();
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    const receiptId = await persistInbound(
      fixture,
      "media-only-dark-no-pending-001",
      null,
      [{ id: "new-attachment", mimeType: "image/png", sizeBytes: 42 }],
    );

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "maintenance_rejected" });
    await expect(fixture.store.getPending(fixture.senderHash, fixture.threadHash)).resolves.toBeNull();
    const records = await durableOutboxRecords(
      fixture,
      createOutbox.mock.calls.map(([input]) => input.outboxId),
    );
    expect(records.map((record) => textFromOutboxPayload(fixture.store.decryptOutboxPayload(record)))).toEqual(expect.arrayContaining([
      "Air is in maintenance mode; I didn’t retain that attachment. No generation was submitted.",
    ]));
  });

  it("does not publish or deliver a saved-media claim when pending acknowledgement persistence is revoked", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    // Keep the interleaving focused on the media acknowledgement rather than
    // first-contact onboarding messages.
    await fixture.store.putSettings(fixture.senderHash, {
      schemaVersion: 1,
      onboardingVersion: 1,
      modes: {
        imagine: { outputCount: 1 },
        animate: { outputCount: 1, durationSeconds: 5 },
        "edit-image": { outputCount: 1 },
        "edit-video": { outputCount: 1, durationSeconds: 5 },
      },
    });
    vi.spyOn(fixture.store, "createPendingGuardedOutbox").mockImplementation(async (guard, input) => {
      // Model an intervening dark deployment. The storage primitive itself is
      // exercised in store tests; this keeps the engine assertion focused on
      // truthful copy for a revoked acknowledgement.
      void guard;
      void input;
      return { kind: "maintenance" };
    });
    const receiptId = await persistInbound(
      fixture,
      "media-rollback-between-pending-and-ack-001",
      null,
      [{ id: "race-attachment", mimeType: "image/png", sizeBytes: 42 }],
    );

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 1 });

    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "maintenance_rejected" });
    const deliveredTexts = fixture.transport.sendText.mock.calls.map(([, text]) => text);
    expect(deliveredTexts).toEqual([
      "That pending attachment is no longer active. Send it again with an explicit command.",
    ]);
    expect(deliveredTexts.join("\n")).not.toContain("I saved that one attachment");
  });

  it("never delivers a queued media acknowledgement after maintenance revokes its guard", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    const source = await terminalPendingSource(fixture, "queued-before-rollback-source");
    const pending = await fixture.store.putPendingWithGuard(source, {
      attachmentId: "queued-before-rollback",
    });
    if (pending.kind !== "active") throw new Error("expected a pending guard");
    const acknowledgement = await fixture.store.createPendingGuardedOutbox(pending.guard, {
      outboxId: "queued-media-ack-before-rollback",
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        text: "I saved that one attachment for 10 minutes.",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
      },
    });
    expect(acknowledgement.kind).toBe("persisted");

    const maintenance = await terminalPendingSource(fixture, "queued-before-rollback-maintenance");
    await fixture.store.blockPendingForMaintenance(maintenance);
    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);

    expect(fixture.transport.showTyping).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    await expect(fixture.store.getOutbox("queued-media-ack-before-rollback"))
      .resolves.toMatchObject({ state: "canceled" });
  });

  it("rechecks a claimed media acknowledgement after typing before sending it", async () => {
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      deliveryEnabled: true,
      transportVerified: true,
    });
    await armFixtureReleaseFence(fixture);
    const source = await terminalPendingSource(fixture, "claimed-before-rollback-source");
    const pending = await fixture.store.putPendingWithGuard(source, {
      attachmentId: "claimed-before-rollback",
    });
    if (pending.kind !== "active") throw new Error("expected a pending guard");
    await fixture.store.createPendingGuardedOutbox(pending.guard, {
      outboxId: "claimed-media-ack-before-rollback",
      senderHash: fixture.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: {
        kind: "text",
        text: "I saved that one attachment for 10 minutes.",
        thread: { chatGuid: CHAT_GUID, sendingLine: SENDING_LINE },
      },
    });
    fixture.transport.showTyping.mockImplementationOnce(async () => {
      // This is after the queue claim and first authorization, but before the
      // dwell-complete authorization directly preceding sendText.
      const maintenance = await terminalPendingSource(fixture, "claimed-before-rollback-maintenance");
      await fixture.store.blockPendingForMaintenance(maintenance);
    });

    await expect(drainStudioOutbox(fixture.runtime)).resolves.toBe(0);

    expect(fixture.transport.showTyping).toHaveBeenCalledTimes(1);
    expect(fixture.transport.stopTyping).toHaveBeenCalledTimes(1);
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    await expect(fixture.store.getOutbox("claimed-media-ack-before-rollback"))
      .resolves.toMatchObject({ state: "canceled" });
  });

  it("cancels a pending intent before dark utility and multi-attachment branches", async () => {
    for (const input of [
      { id: "pending-help-dark-001", text: "/help", attachments: [] },
      {
        id: "pending-multi-dark-001",
        text: null,
        attachments: [
          { id: "first-attachment", mimeType: "image/png", sizeBytes: 42 },
          { id: "second-attachment", mimeType: "image/png", sizeBytes: 42 },
        ],
      },
    ] as const) {
      const fixture = createDarkFixture();
      const source = await terminalPendingSource(fixture, `older-pending-${input.id}`);
      await fixture.store.putPending(source, {
        schemaVersion: 1,
        kind: "media",
        attachment: { id: "older-attachment", mimeType: "image/png", sizeBytes: 42 },
        createdAtMs: source.receivedAtMs,
      });
      const receiptId = await persistInbound(fixture, input.id, input.text, input.attachments);

      await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
      await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "maintenance_rejected" });
      await expect(fixture.store.getPending(fixture.senderHash, fixture.threadHash)).resolves.toBeNull();
    }
  });

  it("never admits a creation in the dark phase even when admission and provider switches are true", async () => {
    // The release phase remains dark. Both switches are intentionally true to
    // prove that phase is an independent gate.
    const fixture = createDarkFixture({ acceptNewJobs: true, allowProviderSubmit: true });
    const admitJob = vi.spyOn(fixture.store, "admitJob");
    const creationReceipt = await persistInbound(fixture, "creation-002", "/imagine a cobalt paper lantern");
    const bareTextReceipt = await persistInbound(fixture, "bare-text-001", "make it feel more cinematic");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 2, outbox: 0 });

    await expect(fixture.store.readInbox(creationReceipt)).resolves.toMatchObject({ state: "maintenance_rejected" });
    await expect(fixture.store.readInbox(bareTextReceipt)).resolves.toMatchObject({ state: "processed" });
    expect(admitJob).not.toHaveBeenCalled();
    expect(fixture.transport.sendText).not.toHaveBeenCalled();
    expect(fixture.transport.sendAttachment).not.toHaveBeenCalled();
  });

  it("requires durable FIFO before active media or creation handling while /help remains available", async () => {
    const provider = {
      verified: true,
      submit: vi.fn(async () => Object.freeze({ kind: "accepted" as const, providerRequestId: "must-not-submit" })),
    } satisfies StudioProviderPort;
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      allowProviderSubmit: true,
      provider,
      registry: reviewedImagineRegistry(),
      perJobCents: 100,
      dailyCents: 100,
    });
    await armFixtureReleaseFence(fixture, { activateInboxFifo: false });
    await fixture.store.putSettings(fixture.senderHash, {
      schemaVersion: 1,
      onboardingVersion: 1,
      modes: {
        imagine: { outputCount: 1 },
        animate: { outputCount: 1, durationSeconds: 5 },
        "edit-image": { outputCount: 1 },
        "edit-video": { outputCount: 1, durationSeconds: 5 },
      },
    });
    const admit = vi.spyOn(fixture.store, "admitJobWithInitialNotifications");
    const putPending = vi.spyOn(fixture.store, "putPendingWithGuard");
    const fifoStatus = vi.spyOn(fixture.store, "getInboxFifoMigrationStatus");
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");

    const creationReceipt = await persistInbound(fixture, "fifo-required-create-001", "/imagine a paper lantern");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    await expect(fixture.store.readInbox(creationReceipt)).resolves.toMatchObject({ state: "maintenance_rejected" });
    expect(admit).not.toHaveBeenCalled();

    const mediaReceipt = await persistInbound(
      fixture,
      "fifo-required-media-001",
      null,
      [{ id: "fifo-required-media", mimeType: "image/png", sizeBytes: 42 }],
    );
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    await expect(fixture.store.readInbox(mediaReceipt)).resolves.toMatchObject({ state: "maintenance_rejected" });
    await expect(fixture.store.getPending(fixture.senderHash, fixture.threadHash)).resolves.toBeNull();
    expect(putPending).not.toHaveBeenCalled();

    const fifoReadsBeforeHelp = fifoStatus.mock.calls.length;
    const helpReceipt = await persistInbound(fixture, "fifo-required-help-001", "/help");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    await expect(fixture.store.readInbox(helpReceipt)).resolves.toMatchObject({ state: "processed" });
    // Claiming and finishing every receipt inspect the durable migration
    // marker in the store. `/help` itself must not add the engine's paid
    // admission check, so only those two scheduler reads are expected.
    expect(fifoStatus).toHaveBeenCalledTimes(fifoReadsBeforeHelp + 2);
    expect(createOutbox.mock.calls.map(([input]) => input.payload))
      .toContainEqual(expect.objectContaining({ text: expect.stringContaining("Air commands:") }));
    expect(createOutbox.mock.calls.map(([input]) => input.payload))
      .toContainEqual(expect.objectContaining({ text: expect.stringContaining("ordering-safety migration") }));
    expect(provider.submit).not.toHaveBeenCalled();
  });

  it("fails closed for an invalid FIFO migration status even after a FIFO cutover", async () => {
    const provider = {
      verified: true,
      submit: vi.fn(async () => Object.freeze({ kind: "accepted" as const, providerRequestId: "must-not-submit" })),
    } satisfies StudioProviderPort;
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      allowProviderSubmit: true,
      provider,
      registry: reviewedImagineRegistry(),
      perJobCents: 100,
      dailyCents: 100,
    });
    await armFixtureReleaseFence(fixture);
    // Let the store's scheduler claim the already-persisted FIFO head, then
    // make the engine's new-admission read fail closed. The real marker stays
    // FIFO so the Lua finish primitive can terminalize the claimed receipt.
    vi.spyOn(fixture.store, "getInboxFifoMigrationStatus")
      .mockResolvedValueOnce({ kind: "current", phase: "fifo", holdingScopes: 0 })
      .mockResolvedValueOnce({ kind: "invalid" })
      .mockResolvedValue({ kind: "current", phase: "fifo", holdingScopes: 0 });
    const admit = vi.spyOn(fixture.store, "admitJobWithInitialNotifications");
    const receiptId = await persistInbound(fixture, "fifo-invalid-create-001", "/imagine a paper lantern");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "maintenance_rejected" });
    expect(admit).not.toHaveBeenCalled();
    expect(provider.submit).not.toHaveBeenCalled();
  });

  it("treats zero spend caps as a no-paid-generation kill switch even if provider switches are true", async () => {
    const provider = {
      verified: true,
      submit: vi.fn(async () => Object.freeze({ kind: "accepted" as const, providerRequestId: "must-not-submit" })),
    } satisfies StudioProviderPort;
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      allowProviderSubmit: true,
      provider,
      registry: reviewedImagineRegistry(),
      perJobCents: 0,
      dailyCents: 0,
    });
    await armFixtureReleaseFence(fixture);
    const admit = vi.spyOn(fixture.store, "admitJobWithInitialNotifications");
    const receiptId = await persistInbound(fixture, "zero-spend-paid-kill-switch-001", "/imagine a paper lantern");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "processed" });
    await expect(fixture.store.getActiveJob(fixture.senderHash)).resolves.toBeNull();
    expect(admit).not.toHaveBeenCalled();
    await expect(drainStudioJobs(fixture.runtime)).resolves.toBe(0);
    expect(provider.submit).not.toHaveBeenCalled();

    const outboxIds = await fixture.redis.zRangeByScore(
      `${STUDIO_REDIS_PREFIX}:outbox:ready`,
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
    );
    const payloads = await Promise.all(outboxIds.map(async (outboxId) => {
      const outbox = await fixture.store.getOutbox(outboxId);
      return outbox ? textFromOutboxPayload(fixture.store.decryptOutboxPayload(outbox)) : undefined;
    }));
    expect(payloads).toContain("Air has no verified paid execution path enabled in this deployment. No generation was submitted.");
  });

  it("refuses paid admission and provider work when the configured registry version mismatches", async () => {
    const provider = {
      verified: true,
      submit: vi.fn(async () => Object.freeze({ kind: "accepted" as const, providerRequestId: "must-not-submit" })),
    } satisfies StudioProviderPort;
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      allowProviderSubmit: true,
      provider,
      registry: reviewedImagineRegistry(),
      modelRegistryVersion: "2.1.1",
      perJobCents: 100,
      dailyCents: 100,
    });
    await armFixtureReleaseFence(fixture);
    const admit = vi.spyOn(fixture.store, "admitJobWithInitialNotifications");
    const receiptId = await persistInbound(fixture, "registry-mismatch-no-admission-001", "/imagine a glass lantern");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "processed" });
    await expect(fixture.store.getActiveJob(fixture.senderHash)).resolves.toBeNull();
    expect(admit).not.toHaveBeenCalled();
    await expect(drainStudioJobs(fixture.runtime)).resolves.toBe(0);
    expect(provider.submit).not.toHaveBeenCalled();

    const outboxIds = await fixture.redis.zRangeByScore(
      `${STUDIO_REDIS_PREFIX}:outbox:ready`,
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
    );
    const payloads = await Promise.all(outboxIds.map(async (outboxId) => {
      const outbox = await fixture.store.getOutbox(outboxId);
      return outbox ? textFromOutboxPayload(fixture.store.decryptOutboxPayload(outbox)) : undefined;
    }));
    expect(payloads).toContain("Air’s verified model configuration is unavailable right now. No generation was submitted.");
  });

  it("reports a shared canary-budget reservation refusal without submitting", async () => {
    const provider = {
      verified: true,
      submit: vi.fn(async () => Object.freeze({ kind: "accepted" as const, providerRequestId: "must-not-submit" })),
    } satisfies StudioProviderPort;
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      allowProviderSubmit: true,
      provider,
      registry: reviewedImagineRegistry(),
      perJobCents: 100,
      dailyCents: 100,
      canaryTotalCents: 24,
      canaryFalCents: 0,
    });
    await armFixtureReleaseFence(fixture);
    const receiptId = await persistInbound(fixture, "shared-canary-cap-001", "/imagine a brushed-steel hummingbird");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "processed" });
    await expect(fixture.store.getActiveJob(fixture.senderHash)).resolves.toBeNull();
    expect(provider.submit).not.toHaveBeenCalled();
    const outboxIds = await fixture.redis.zRangeByScore(
      `${STUDIO_REDIS_PREFIX}:outbox:ready`,
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
    );
    const payloads = await Promise.all(outboxIds.map(async (outboxId) => {
      const outbox = await fixture.store.getOutbox(outboxId);
      return outbox ? textFromOutboxPayload(fixture.store.decryptOutboxPayload(outbox)) : undefined;
    }));
    expect(payloads).toContain("Air’s shared launch budget is fully reserved. No generation was submitted.");
  });

  it("persists an acknowledgement and tapback before the verified scheduler submits a text-only job", async () => {
    let fixture: ReturnType<typeof createDarkFixture>;
    let observedInitialEffects: readonly StudioOutboxRecord[] = [];
    const provider = {
      verified: true,
      submit: vi.fn(async (input) => {
        const readyIds = await fixture.redis.zRangeByScore(
          `${STUDIO_REDIS_PREFIX}:outbox:ready`,
          Number.NEGATIVE_INFINITY,
          Number.POSITIVE_INFINITY,
        );
        const records = await Promise.all(readyIds.map((id) => fixture.store.getOutbox(id)));
        observedInitialEffects = records.filter(
          (record): record is StudioOutboxRecord => record !== null && record.jobId === input.job.jobId,
        );
        return Object.freeze({ kind: "accepted" as const, providerRequestId: "reviewed-fake-request" });
      }),
    } satisfies StudioProviderPort;
    fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      allowProviderSubmit: true,
      provider,
      registry: reviewedImagineRegistry(),
      perJobCents: 100,
      dailyCents: 100,
    });
    await armFixtureReleaseFence(fixture);
    const receiptId = await persistInbound(fixture, "active-direct-create-001", "/imagine a brushed-steel hummingbird");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    await expect(fixture.store.readInbox(receiptId)).resolves.toMatchObject({ state: "processed" });
    expect(provider.submit).not.toHaveBeenCalled();

    await expect(drainStudioJobs(fixture.runtime)).resolves.toBe(1);
    expect(provider.submit).toHaveBeenCalledTimes(1);
    expect(observedInitialEffects).toHaveLength(2);
    expect(observedInitialEffects.map((record) => record.lane).sort()).toEqual([
      "job-acknowledgement",
      "tapback",
    ]);
    expect(observedInitialEffects.every((record) => record.state === "intent" && record.attempts === 0)).toBe(true);
  });

  it("reports and atomically cancels an admitted job before provider submission without clearing saved media", async () => {
    const provider = {
      verified: true,
      submit: vi.fn(async () => Object.freeze({ kind: "accepted" as const, providerRequestId: "should-not-submit" })),
    } satisfies StudioProviderPort;
    const fixture = createDarkFixture({
      releasePhase: "canary",
      acceptNewJobs: true,
      allowProviderSubmit: true,
      provider,
      registry: reviewedImagineRegistry(),
      perJobCents: 100,
      dailyCents: 100,
    });
    await armFixtureReleaseFence(fixture);
    await persistInbound(fixture, "cancelable-create-001", "/imagine an illuminated paper kite");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    const active = await fixture.store.getActiveJob(fixture.senderHash);
    if (!active) throw new Error("expected an admitted active job");

    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    await persistInbound(fixture, "status-active-001", "/status");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    expect(createOutbox).toHaveBeenLastCalledWith(expect.objectContaining({
      payload: expect.objectContaining({ text: "Your request is queued. I’ll send it here when it’s ready." }),
    }));

    const pendingSource = await terminalPendingSource(fixture, "pending-media-before-cancel");
    await fixture.store.putPending(pendingSource, {
      schemaVersion: 1,
      kind: "media",
      attachment: { id: "saved-before-cancel", mimeType: "image/png", sizeBytes: 42 },
      createdAtMs: pendingSource.receivedAtMs,
    });
    await persistInbound(fixture, "cancel-active-001", "/cancel");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    await expect(fixture.store.getJob(active.jobId)).resolves.toMatchObject({ state: "canceled" });
    await expect(fixture.store.getActiveJob(fixture.senderHash)).resolves.toBeNull();
    await expect(fixture.store.getPending(fixture.senderHash, fixture.threadHash)).resolves.toMatchObject({
      kind: "media",
      attachment: { id: "saved-before-cancel" },
    });
    await expect(drainStudioJobs(fixture.runtime)).resolves.toBe(0);
    expect(provider.submit).not.toHaveBeenCalled();

    const readyOutboxIds = await fixture.redis.zRangeByScore(
      `${STUDIO_REDIS_PREFIX}:outbox:ready`,
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
    );
    const readyPayloads = await Promise.all(readyOutboxIds.map(async (outboxId) => {
      const outbox = await fixture.store.getOutbox(outboxId);
      return outbox ? fixture.store.decryptOutboxPayload(outbox) : undefined;
    }));
    expect(readyPayloads).toContainEqual(expect.objectContaining({
      text: "Canceled your queued request. No generation was submitted.",
    }));

    await persistInbound(fixture, "status-recent-canceled-001", "/status");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    expect(createOutbox).toHaveBeenLastCalledWith(expect.objectContaining({
      payload: expect.objectContaining({ text: "Your most recent request was canceled." }),
    }));
    // Utility commands cannot consume a saved media reference while they read
    // the terminal job history.
    await expect(fixture.store.getPending(fixture.senderHash, fixture.threadHash)).resolves.toMatchObject({
      kind: "media",
      attachment: { id: "saved-before-cancel" },
    });
  });

  it("shows only setting-compatible models with exact change examples", async () => {
    const fixture = createDarkFixture({ registry: reviewedImagineRegistry({ includeIncompatibleImagine: true }) });
    await fixture.store.putSettings(fixture.senderHash, {
      schemaVersion: 1,
      onboardingVersion: 1,
      modes: {
        imagine: { outputCount: 1 },
        animate: { outputCount: 1, durationSeconds: 5 },
        "edit-image": { outputCount: 1 },
        "edit-video": { outputCount: 1, durationSeconds: 5 },
      },
    });
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    await persistInbound(fixture, "settings-command-reference-001", "/model-settings imagine");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    const settingText = createOutbox.mock.calls
      .map(([input]) => input.payload.kind === "text" ? input.payload.text : undefined)
      .find((text) => text?.startsWith("imagine:"));
    expect(settingText).toContain("Compatible models: reviewed-gmi-imagine (counts 1; aspect 1:1).");
    expect(settingText).toContain("/model-settings imagine reviewed-gmi-imagine");
    expect(settingText).toContain("/model-settings imagine count 1");
    expect(settingText).toContain("/model-settings reset imagine");
    expect(settingText).not.toContain("two-output-gmi-imagine");

    await persistInbound(fixture, "models-compatible-only-001", "/models imagine");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    const modelsText = createOutbox.mock.calls
      .map(([input]) => input.payload.kind === "text" ? input.payload.text : undefined)
      .find((text) => text === "reviewed-gmi-imagine — Reviewed GMI Imagine");
    expect(modelsText).toBe("reviewed-gmi-imagine — Reviewed GMI Imagine");
    expect(createOutbox.mock.calls
      .map(([input]) => input.payload.kind === "text" ? input.payload.text : "")
      .join("\n")).not.toContain("two-output-gmi-imagine");
  });

  it("explains all-mode model incompatibility without claiming no models are enabled", async () => {
    const fixture = createDarkFixture({ registry: reviewedImagineRegistry() });
    await fixture.store.putSettings(fixture.senderHash, {
      schemaVersion: 1,
      onboardingVersion: 1,
      modes: {
        imagine: { outputCount: 3 },
        animate: { outputCount: 1, durationSeconds: 5 },
        "edit-image": { outputCount: 1 },
        "edit-video": { outputCount: 1, durationSeconds: 5 },
      },
    });
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    await persistInbound(fixture, "models-all-incompatible-001", "/models");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    const reply = createOutbox.mock.calls
      .map(([input]) => input.payload.kind === "text" ? input.payload.text : undefined)
      .find((text) => text?.startsWith("No enabled model is compatible"));
    expect(reply).toBe(
      "No enabled model is compatible with your saved settings. Use /model-settings imagine to review the exact supported values.",
    );
    expect(reply).not.toContain("No verified models are enabled");

    await persistInbound(fixture, "models-mode-incompatible-001", "/models imagine");
    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });
    expect(createOutbox).toHaveBeenLastCalledWith(expect.objectContaining({
      payload: expect.objectContaining({
        text: "No enabled imagine model is compatible with your saved settings. Use /model-settings imagine to review the exact supported values.",
      }),
    }));
  });

  it("discloses an enabled hosted-public input path without claiming provider deletion", async () => {
    const fixture = createDarkFixture({ registry: reviewedImagineRegistry({ hostedPublicInput: true }) });
    await fixture.store.putSettings(fixture.senderHash, {
      schemaVersion: 1,
      onboardingVersion: 1,
      modes: {
        imagine: { outputCount: 1 },
        animate: { outputCount: 1, durationSeconds: 5 },
        "edit-image": { outputCount: 1 },
        "edit-video": { outputCount: 1, durationSeconds: 5 },
      },
    });
    const createOutbox = vi.spyOn(fixture.store, "createOutbox");
    await persistInbound(fixture, "privacy-hosted-public-001", "/privacy");

    await expect(drainStudioWithStats(fixture.runtime)).resolves.toEqual({ inbox: 1, outbox: 0 });

    const privacy = createOutbox.mock.calls
      .map(([input]) => input.payload.kind === "text" ? input.payload.text : undefined)
      .find((text) => text?.startsWith("Air is private and allowlist-only."));
    expect(privacy).toContain("Air does not promise provider-side deletion.");
    expect(privacy).toContain("reviewed-gmi-imagine: inputs.example.test/reviewed-air-input/");
    expect(privacy).toContain("publicly accessible path during processing");
  });
});
