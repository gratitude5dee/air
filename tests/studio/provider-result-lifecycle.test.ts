import { describe, expect, it, vi } from "vitest";

import { createQueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";
import type { StudioEnvironment } from "../../lib/studio/config.js";
import {
  advanceStudioProviderResultLifecycle,
} from "../../lib/studio/provider-result-lifecycle.js";
import type {
  IMessageThreadReference,
  StudioIMessageTransport,
  StudioPrepareOutputOutcome,
  StudioPreparedOutputTransport,
  TransportOutcome,
} from "../../lib/studio/imessage-transport.js";
import type {
  StudioProviderObservationPort,
  StudioProviderObservationResult,
  StudioProviderSubmitResult,
} from "../../lib/studio/provider-port.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";
import {
  STUDIO_REDIS_PREFIX,
  STUDIO_RESULT_PREAMBLE_TEXT,
  StudioStore,
  createStudioJob,
  type StudioExecutionSnapshot,
  type StudioJobRecord,
  type StudioJobRecordWithPayload,
  type StudioPreparedOutput,
} from "../../lib/studio/store.js";
import { drainStudio, drainStudioJobs, drainStudioOutbox } from "../../lib/studio/engine.js";
import { executeStudioProviderSubrun } from "../../lib/studio/workflow-executor.js";

const IDENTITY_SECRET = "provider-result-lifecycle-test-identity";
const QUEUE_KEY = Buffer.alloc(32, 113).toString("base64");
const SENDER = "+14155550991";
const THREAD = "iMessage;-;+14155550991";
const SNAPSHOT_FINGERPRINT = `sha256:${"c".repeat(64)}`;
const PROVIDER_REQUEST_ID = "private-provider-request-lifecycle-001";
const PRIVATE_RESULT_URL = "https://private-provider.example.test/outputs/never-store-this.jpg?signature=opaque";
const PREPARED_OUTPUT: StudioPreparedOutput = Object.freeze({
  transport: "photon",
  attachmentGuid: "private-photon-guid-lifecycle-001",
});
const ONE_PIXEL_PNG = Uint8Array.from(Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
  "base64",
));

type RecordingProvider = StudioProviderObservationPort & {
  readonly submit: ReturnType<typeof vi.fn>;
  readonly observe: ReturnType<typeof vi.fn>;
};

type RecordingTransport = StudioIMessageTransport & StudioPreparedOutputTransport & {
  readonly prepareValidatedProviderArtifact: ReturnType<typeof vi.fn>;
  readonly sendText: ReturnType<typeof vi.fn>;
  readonly sendPreparedOutput: ReturnType<typeof vi.fn>;
  readonly events: string[];
};

function frozen<T>(value: T): Readonly<T> {
  return Object.freeze(value);
}

async function* bytesStream(bytes: Uint8Array): AsyncGenerator<Uint8Array> {
  yield Uint8Array.from(bytes);
}

/**
 * The provider port deliberately does not expose a result URL. The extra
 * field simulates a future adapter accidentally retaining one: the lifecycle
 * must still neither serialize nor deliver it.
 */
function readyPrivateImage(url = PRIVATE_RESULT_URL): StudioProviderObservationResult {
  return {
    kind: "ready",
    artifacts: [
      {
        outputIndex: 1,
        mediaType: "image",
        declaredMimeType: "image/png",
        stream: bytesStream(ONE_PIXEL_PNG),
        providerOutputUrl: url,
      },
    ],
  } as unknown as StudioProviderObservationResult;
}

function malformedPrivateImage(): StudioProviderObservationResult {
  return frozen({
    kind: "ready" as const,
    artifacts: [frozen({
      outputIndex: 1 as const,
      mediaType: "image" as const,
      declaredMimeType: "image/png",
      stream: bytesStream(Buffer.from("not a provider image")),
    })],
  });
}

function environment(policy: Partial<StudioEnvironment["policy"]> = {}): StudioEnvironment {
  const senderHash = hashSenderIdentifier(IDENTITY_SECRET, SENDER);
  return frozen({
    deploymentMode: "preview",
    releasePhase: "canary",
    releaseGeneration: 1,
    releaseFenceSecret: "lifecycle-release-fence-secret",
    storageTransport: "rest",
    imessageProjectId: "test-project",
    imessageProjectSecret: "test-project-secret",
    imessageWebhookSecret: "test-webhook-secret",
    upstashRedisRestUrl: "https://redis.example.test",
    upstashRedisRestToken: "test-redis-token",
    idHashSecret: IDENTITY_SECRET,
    queueEncryptionKey: QUEUE_KEY,
    allowedSenderHashes: [senderHash],
    cronSecret: "test-cron-secret",
    publicBaseUrl: "https://air.example.test",
    modelRegistryVersion: "2.1.0",
    dailyUserJobLimit: 3,
    activeJobLimit: 1,
    canaryBudgetId: "provider-result-lifecycle-budget",
    policy: frozen({
      acceptNewJobs: true,
      allowProviderSubmit: true,
      allowProviderRecovery: true,
      deliveryEnabled: true,
      ...policy,
    }),
    policyExplicit: true,
    spendCaps: frozen({
      perJobCents: 10_000,
      dailyCents: 10_000,
      canaryTotalCents: 10_000,
      canaryFalCents: 0,
    }),
  });
}

function fakeProvider(results: readonly StudioProviderObservationResult[]): RecordingProvider {
  const pending = [...results];
  return {
    verified: true,
    observationSupported: true,
    submit: vi.fn(async (): Promise<StudioProviderSubmitResult> => frozen({
      kind: "accepted" as const,
      providerRequestId: PROVIDER_REQUEST_ID,
    })),
    observe: vi.fn(async (): Promise<StudioProviderObservationResult> => {
      const next = pending.shift();
      if (!next) throw new Error("unexpected provider observation");
      return next;
    }),
  } as RecordingProvider;
}

function fakeTransport(
  prepareOutcome: StudioPrepareOutputOutcome = frozen({ kind: "prepared" as const, output: PREPARED_OUTPUT }),
): RecordingTransport {
  const events: string[] = [];
  return {
    verified: true,
    preparedOutputTransport: "photon",
    events,
    showTyping: vi.fn(async (_thread: IMessageThreadReference): Promise<void> => undefined),
    stopTyping: vi.fn(async (_thread: IMessageThreadReference): Promise<void> => undefined),
    sendText: vi.fn(async (_thread: IMessageThreadReference, text: string, _idempotencyKey: string): Promise<TransportOutcome> => {
      events.push(`text:${text}`);
      return frozen({ kind: "sent" as const });
    }),
    sendAttachment: vi.fn(async (): Promise<TransportOutcome> => frozen({ kind: "failed" as const, code: "legacy_path_must_not_run" })),
    addLikeTapback: vi.fn(async (): Promise<TransportOutcome> => frozen({ kind: "failed" as const, code: "tapback_not_used" })),
    fetchAttachment: vi.fn(async (): Promise<Uint8Array> => {
      throw new Error("input_attachment_fetch_must_not_run");
    }),
    prepareValidatedProviderArtifact: vi.fn(async (): Promise<StudioPrepareOutputOutcome> => prepareOutcome),
    sendPreparedOutput: vi.fn(async (_thread: IMessageThreadReference, output: StudioPreparedOutput, _idempotencyKey: string): Promise<TransportOutcome> => {
      events.push(`attachment:${output.attachmentGuid}`);
      return frozen({ kind: "sent" as const });
    }),
  } as RecordingTransport;
}

async function fixture(options: {
  readonly observations?: readonly StudioProviderObservationResult[];
  readonly prepareOutcome?: StudioPrepareOutputOutcome;
} = {}) {
  let now = Date.UTC(2026, 6, 23, 12, 0, 0);
  const redis = new MemoryRedis(() => now);
  const cipher = createQueueCipher(QUEUE_KEY);
  const prefix = `${STUDIO_REDIS_PREFIX}:provider-result-lifecycle-test`;
  const releaseStamp = frozen({ generation: 1, phase: "canary" as const });
  const darkStore = new StudioStore({
    redis,
    cipher,
    identityHashSecret: IDENTITY_SECRET,
    prefix,
    clock: () => now,
    releaseStamp: frozen({ generation: 0, phase: "dark" as const }),
  });
  expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });

  const store = new StudioStore({
    redis,
    cipher,
    identityHashSecret: IDENTITY_SECRET,
    prefix,
    clock: () => now,
    releaseStamp,
  });
  expect(await store.advanceReleaseFence(0)).toEqual({ kind: "advanced" });

  const provider = fakeProvider(options.observations ?? []);
  const transport = fakeTransport(options.prepareOutcome);
  const runtime: StudioRuntime = {
    environment: environment(),
    cipher,
    store,
    transport,
    provider,
    storageTransport: "rest",
    readiness: (): readonly string[] => [],
    close: async (): Promise<void> => undefined,
  };

  const senderHash = hashSenderIdentifier(IDENTITY_SECRET, SENDER);
  const threadHash = hashThreadIdentifier(IDENTITY_SECRET, THREAD);
  const snapshot: StudioExecutionSnapshot = frozen({
    schemaVersion: 2,
    workflow: frozen({ workflowId: "air:imagine:v1", workflowVersion: 1, stepId: "provider-generation" }),
    model: frozen({
      slug: "verified-model",
      provider: "gmi",
      providerModelId: "flux-kontext-pro",
      adapterKey: "gmi.flux-kontext-pro.v1",
      catalogFingerprint: SNAPSHOT_FINGERPRINT,
      requestSchemaFingerprint: SNAPSHOT_FINGERPRINT,
      resultSchemaFingerprint: SNAPSHOT_FINGERPRINT,
      pricingEvidenceFingerprint: SNAPSHOT_FINGERPRINT,
    }),
    output: frozen({ type: "image" as const, count: 1 }),
    maximumQuotedCostCents: 25,
    polling: frozen({ requestTimeoutMs: 5_000, pollIntervalMs: 1_000, maxPollAttempts: 4, deadlineMs: 15_000 }),
    releaseStamp,
  });

  function job(jobId: string): StudioJobRecordWithPayload {
    return createStudioJob({
      jobId,
      senderHash,
      threadHash,
      mode: "imagine",
      modelSlug: "verified-model",
      provider: "gmi",
      outputCount: 1,
      reservedCents: 25,
      releaseStamp,
      payload: frozen({
        prompt: "private prompt not retained in lifecycle records",
        chatGuid: THREAD,
        sendingLine: SENDER,
        replyTargetId: "incoming-guid-lifecycle",
        execution: snapshot,
      }),
      cipher,
      now,
    });
  }

  async function admit(created: StudioJobRecordWithPayload): Promise<void> {
    await expect(store.admitJob({
      job: created,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 10_000,
      dailySpendCapCents: 10_000,
      canaryBudget: {
        budgetId: "provider-result-lifecycle-budget",
        totalCapCents: 10_000,
        falCapCents: 0,
      },
    })).resolves.toMatchObject({ admitted: true });
  }

  function runtimeWithPolicy(policy: Partial<StudioEnvironment["policy"]>): StudioRuntime {
    return {
      ...runtime,
      environment: environment(policy),
    };
  }

  return {
    runtime,
    runtimeWithPolicy,
    store,
    redis,
    cipher,
    prefix,
    provider,
    transport,
    snapshot,
    job,
    admit,
    now: (): number => now,
    advance(milliseconds: number): void { now += milliseconds; },
  };
}

async function submitAccepted(
  setup: Awaited<ReturnType<typeof fixture>>,
  jobId: string,
): Promise<StudioJobRecord> {
  const created = setup.job(jobId);
  await setup.admit(created);
  await expect(executeStudioProviderSubrun(setup.runtime, jobId)).resolves.toEqual({ kind: "submitted" });
  const executing = await setup.store.getJob(jobId);
  if (!executing || executing.state !== "executing") throw new Error("expected an executing job");
  return executing;
}

async function seedPreparedOutput(
  setup: Awaited<ReturnType<typeof fixture>>,
  job: StudioJobRecord,
): Promise<void> {
  await expect(setup.store.ensureJobResultPreparation(job.jobId, 1)).resolves.toMatchObject({ state: "reserved" });
  const lease = await setup.store.acquireJobResultPreparationLease(job.jobId, 1);
  if (!lease) throw new Error("expected result-preparation lease");
  try {
    await expect(setup.store.beginJobResultPreparation(job, 1, lease.token)).resolves.toMatchObject({ kind: "ready" });
    await expect(setup.store.finishJobResultPreparation(job, 1, lease.token, {
      kind: "prepared",
      output: PREPARED_OUTPUT,
    })).resolves.toMatchObject({ kind: "prepared" });
  } finally {
    await lease.release();
  }
}

function durableRawStrings(redis: MemoryRedis): readonly string[] {
  const values = (redis as unknown as {
    readonly values: ReadonlyMap<string, { readonly kind: string; readonly value: unknown }>;
  }).values;
  return Object.freeze([...values.values()]
    .filter((entry): entry is { readonly kind: "string"; readonly value: string } =>
      entry.kind === "string" && typeof entry.value === "string")
    .map((entry) => entry.value));
}

function expectProviderSecretsAbsent(setup: Awaited<ReturnType<typeof fixture>>): void {
  const raw = durableRawStrings(setup.redis).join("\n");
  expect(raw).not.toContain(PROVIDER_REQUEST_ID);
  expect(raw).not.toContain(PRIVATE_RESULT_URL);
}

describe("private provider result lifecycle", () => {
  it("defers a deadline-bound executing recovery before provider or Photon work", async () => {
    const setup = await fixture({ observations: [readyPrivateImage()] });
    const job = await submitAccepted(setup, "deadline-bound-result-recovery");
    const getDeliveryTransport = vi.fn(async () => setup.transport);
    Object.assign(setup.runtime, { getDeliveryTransport });

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId, {
      deadlineAtMs: Date.now() + 50_000,
      minimumDeliveryPreparationRemainingMs: 36_000,
      minimumPhysicalActionRemainingMs: 22_000,
    })).resolves.toEqual({ kind: "deadline_deferred" });

    expect(getDeliveryTransport).not.toHaveBeenCalled();
    expect(setup.provider.observe).not.toHaveBeenCalled();
    expect(setup.transport.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "executing" });
    const subrun = await setup.store.getProviderSubrunForJob(job.jobId);
    if (!subrun) throw new Error("expected submitted subrun");
    await expect(setup.store.getProviderObservation(subrun.subrunId)).resolves.toBeNull();
  });

  it("never resubmits an accepted request across bounded poll retries", async () => {
    const setup = await fixture({
      observations: [
        frozen({ kind: "pending" as const }),
        frozen({ kind: "retryable_failure" as const }),
      ],
    });
    const job = await submitAccepted(setup, "poll-retries-no-resubmit");

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId))
      .resolves.toMatchObject({ kind: "pending" });
    expect(setup.provider.submit).toHaveBeenCalledTimes(1);
    expect(setup.provider.observe).toHaveBeenCalledTimes(1);

    // Re-entering the submission executor after a poll is a no-op at the
    // physical provider boundary, even before the next poll is due.
    await expect(executeStudioProviderSubrun(setup.runtime, job.jobId)).resolves.toEqual({ kind: "submitted" });
    expect(setup.provider.submit).toHaveBeenCalledTimes(1);

    setup.advance(1_000);
    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId))
      .resolves.toMatchObject({ kind: "pending" });
    await expect(executeStudioProviderSubrun(setup.runtime, job.jobId)).resolves.toEqual({ kind: "submitted" });

    expect(setup.provider.submit).toHaveBeenCalledTimes(1);
    expect(setup.provider.observe).toHaveBeenCalledTimes(2);
    await expect(setup.store.getProviderSubrunForJob(job.jobId)).resolves.toMatchObject({ state: "submitted" });
    await expect(setup.store.getProviderObservation((await setup.store.getProviderSubrunForJob(job.jobId))!.subrunId))
      .resolves.toMatchObject({ state: "scheduled", pollAttempts: 2 });
  });

  it("persists an observation's exact due time even when the job timestamp is stale", async () => {
    const setup = await fixture();
    const job = await submitAccepted(setup, "scheduler-uses-observation-due-time");
    setup.provider.observe.mockImplementationOnce(async () => {
      // The provider GET consumed time after the executing job was written.
      // Deriving a delay from that stale job timestamp would make the next
      // claim 7 seconds late; the store's absolute observation due time must
      // be preserved instead.
      setup.advance(7_000);
      return frozen({ kind: "pending" as const });
    });

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId))
      .resolves.toMatchObject({ kind: "pending" });

    const subrun = await setup.store.getProviderSubrunForJob(job.jobId);
    if (!subrun) throw new Error("expected submitted subrun");
    const observation = await setup.store.getProviderObservation(subrun.subrunId);
    if (!observation) throw new Error("expected pending observation");
    expect(observation.nextDueAtMs).toBe(setup.now() + 1_000);
    expect(setup.provider.submit).toHaveBeenCalledTimes(1);
    expect(setup.provider.observe).toHaveBeenCalledTimes(1);
  });

  it("validates a ready image and atomically publishes a text preamble before its native attachment", async () => {
    const setup = await fixture({ observations: [readyPrivateImage()] });
    const job = await submitAccepted(setup, "ready-private-image");

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId)).resolves.toEqual({ kind: "ready" });
    expect(setup.provider.submit).toHaveBeenCalledTimes(1);
    expect(setup.provider.observe).toHaveBeenCalledTimes(1);
    expect(setup.transport.prepareValidatedProviderArtifact).toHaveBeenCalledTimes(1);

    const [persistedJob, subrun, preamble, cursor] = await Promise.all([
      setup.store.getJob(job.jobId),
      setup.store.getProviderSubrunForJob(job.jobId),
      setup.store.getJobResultPreamble(job.jobId),
      setup.store.getJobResultCursor(job.jobId),
    ]);
    expect(persistedJob).toMatchObject({ state: "ready" });
    expect(subrun).toMatchObject({ state: "submitted" });
    expect(preamble).toMatchObject({ lane: "job-result", sequence: 0 });
    expect(preamble?.resultOutputIndex).toBeUndefined();
    expect(cursor).toMatchObject({ nextOutputIndex: 2, outputs: [{ outputIndex: 1 }] });
    if (!preamble || !cursor) throw new Error("expected finalized result records");
    const attachment = await setup.store.getOutbox(cursor.outputs[0]!.outboxId);
    expect(attachment).toMatchObject({ lane: "job-result", sequence: 1, resultOutputIndex: 1 });
    const fullPreamble = await setup.store.getOutbox(preamble.outboxId);
    if (!fullPreamble || !attachment) throw new Error("expected result outboxes");
    expect(setup.store.decryptOutboxPayload(fullPreamble)).toMatchObject({
      kind: "text",
      text: STUDIO_RESULT_PREAMBLE_TEXT,
    });
    expect(setup.store.decryptOutboxPayload(attachment)).toEqual({
      kind: "prepared-output",
      outputIndex: 1,
      output: PREPARED_OUTPUT,
      thread: { chatGuid: THREAD, sendingLine: SENDER, replyTargetId: "incoming-guid-lifecycle" },
    });

    // The lane head enforces the exact text-before-media dispatch sequence.
    const first = await setup.store.claimNextOutbox();
    expect(first?.record.outboxId).toBe(preamble.outboxId);
    expect(first?.record.sequence).toBe(0);
    if (!first) throw new Error("expected preamble claim");
    await expect(setup.store.finishOutbox(first.record.outboxId, first.leaseToken, "sent")).resolves.toBe(true);
    const second = await setup.store.claimNextOutbox();
    expect(second?.record.outboxId).toBe(attachment.outboxId);
    expect(second?.record.sequence).toBe(1);

    expectProviderSecretsAbsent(setup);
    expect(JSON.stringify(setup.store.decryptOutboxPayload(fullPreamble))).not.toContain(PROVIDER_REQUEST_ID);
    expect(JSON.stringify(setup.store.decryptOutboxPayload(attachment))).not.toContain(PRIVATE_RESULT_URL);
  });

  it("never dispatches a result attachment after a failed preamble survives a worker restart", async () => {
    const setup = await fixture({ observations: [readyPrivateImage()] });
    const job = await submitAccepted(setup, "failed-preamble-blocks-restarted-attachment");
    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId)).resolves.toEqual({ kind: "ready" });

    const [preamble, cursor] = await Promise.all([
      setup.store.getJobResultPreamble(job.jobId),
      setup.store.getJobResultCursor(job.jobId),
    ]);
    if (!preamble || !cursor) throw new Error("expected finalized result records");
    const attachmentId = cursor.outputs[0]?.outboxId;
    if (!attachmentId) throw new Error("expected result attachment");

    const preambleClaim = await setup.store.claimNextOutbox();
    if (!preambleClaim || preambleClaim.record.outboxId !== preamble.outboxId) {
      throw new Error("expected result preamble claim");
    }
    // Simulate a process death after the known preamble failure committed but
    // before the best-effort job reconciliation in the original worker.
    await expect(setup.store.finishOutbox(preamble.outboxId, preambleClaim.leaseToken, "failed"))
      .resolves.toBe(true);

    await expect(drainStudioOutbox(setup.runtime)).resolves.toBe(1);

    expect(setup.transport.sendPreparedOutput).not.toHaveBeenCalled();
    expect(setup.transport.events).toEqual([]);
    await expect(setup.store.getOutbox(attachmentId)).resolves.toMatchObject({ state: "failed" });
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "reconciliation_required" });
  });

  it.each([
    ["a provider private-policy rejection", frozen({ kind: "reconciliation_required" as const })],
    ["malformed result bytes", malformedPrivateImage()],
  ])("fences %s into reconciliation without uploading", async (_label, observation) => {
    const setup = await fixture({ observations: [observation] });
    const job = await submitAccepted(setup, `reconcile-${String(_label).replace(/[^a-z]/giu, "-")}`);

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId))
      .resolves.toEqual({ kind: "reconciliation_required" });

    expect(setup.provider.submit).toHaveBeenCalledTimes(1);
    expect(setup.provider.observe).toHaveBeenCalledTimes(1);
    expect(setup.transport.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "reconciliation_required" });
    await expect(setup.store.getJobResultCursor(job.jobId)).resolves.toBeNull();
  });

  it("treats an ambiguous Photon upload as reconciliation, never a second upload", async () => {
    const setup = await fixture({
      observations: [readyPrivateImage()],
      prepareOutcome: frozen({ kind: "send_unknown" as const, code: "photon_upload_ambiguous" }),
    });
    const job = await submitAccepted(setup, "upload-unknown-reconciles");

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId))
      .resolves.toEqual({ kind: "reconciliation_required" });
    expect(setup.transport.prepareValidatedProviderArtifact).toHaveBeenCalledTimes(1);
    await expect(setup.store.getJobResultPreparation(job.jobId, 1)).resolves.toMatchObject({ state: "upload_unknown" });
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "reconciliation_required" });
    await expect(setup.store.getJobResultCursor(job.jobId)).resolves.toBeNull();
  });

  it("leaves a live Photon upload lease alone instead of reconciling it as ambiguous", async () => {
    const setup = await fixture();
    const job = await submitAccepted(setup, "live-upload-lease-is-busy");
    await expect(setup.store.ensureJobResultPreparation(job.jobId, 1)).resolves.toMatchObject({ state: "reserved" });
    const preparationLease = await setup.store.acquireJobResultPreparationLease(job.jobId, 1);
    if (!preparationLease) throw new Error("expected live upload lease");
    try {
      await expect(setup.store.beginJobResultPreparation(job, 1, preparationLease.token))
        .resolves.toMatchObject({ kind: "ready" });
      await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId)).resolves.toEqual({ kind: "busy" });
      expect(setup.provider.observe).not.toHaveBeenCalled();
      expect(setup.transport.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
      await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "executing" });
      await expect(setup.store.getJobResultPreparation(job.jobId, 1)).resolves.toMatchObject({ state: "uploading" });
    } finally {
      await preparationLease.release();
    }
  });

  it("recovers an expired Photon upload marker into reconciliation without another provider or Photon call", async () => {
    const setup = await fixture();
    const job = await submitAccepted(setup, "expired-upload-marker-reconciles");
    await expect(setup.store.ensureJobResultPreparation(job.jobId, 1)).resolves.toMatchObject({ state: "reserved" });
    const uploadLease = await setup.store.acquireJobResultPreparationLease(job.jobId, 1);
    if (!uploadLease) throw new Error("expected upload lease");
    await expect(setup.store.beginJobResultPreparation(job, 1, uploadLease.token))
      .resolves.toMatchObject({ kind: "ready", preparation: { state: "uploading" } });

    // The original uploader died after the durable upload-start marker. A
    // recovered preparation lease must classify that side effect as ambiguous,
    // not leave the job `busy` forever and not re-open either physical path.
    setup.advance(90_001);
    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId))
      .resolves.toEqual({ kind: "reconciliation_required" });

    expect(setup.provider.observe).not.toHaveBeenCalled();
    expect(setup.transport.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    await expect(setup.store.getJobResultPreparation(job.jobId, 1))
      .resolves.toMatchObject({ state: "upload_unknown" });
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "reconciliation_required" });
  });

  it("maps a persisted reconciliation failure disposition to reconciliation, not job failure", async () => {
    const setup = await fixture();
    const job = await submitAccepted(setup, "persisted-reconciliation-failure");
    const subrun = await setup.store.getProviderSubrunForJob(job.jobId);
    if (!subrun) throw new Error("expected submitted subrun");
    const payload = setup.store.decryptJobPayload(job);
    if (!payload.execution) throw new Error("expected execution snapshot");
    await expect(setup.store.ensureProviderObservation(subrun, payload.execution)).resolves.toBeTruthy();
    const observationLease = await setup.store.acquireProviderObservationLease(subrun.subrunId);
    if (!observationLease) throw new Error("expected observation lease");
    await expect(setup.store.beginProviderObservation(subrun.subrunId, observationLease.token))
      .resolves.toMatchObject({ kind: "ready" });
    await expect(setup.store.finishProviderObservation(subrun.subrunId, observationLease.token, {
      kind: "failed",
      failureDisposition: "reconciliation_required",
    })).resolves.toMatchObject({
      kind: "failed",
      observation: { state: "failed", failureDisposition: "reconciliation_required" },
    });

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId))
      .resolves.toEqual({ kind: "reconciliation_required" });
    expect(setup.provider.observe).not.toHaveBeenCalled();
    expect(setup.transport.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "reconciliation_required" });
  });

  it("does not terminalize a job when a terminal provider result loses its observation lease", async () => {
    const setup = await fixture();
    const job = await submitAccepted(setup, "lost-observation-lease-terminal-result");
    setup.provider.observe.mockImplementationOnce(async () => {
      // Exceed the observation lease while the GET is in flight. The result is
      // terminal, but this worker no longer owns the durable terminal write.
      setup.advance(91_000);
      return frozen({ kind: "definitive_failure" as const });
    });

    await expect(advanceStudioProviderResultLifecycle(setup.runtime, job.jobId)).resolves.toEqual({ kind: "busy" });
    expect(setup.provider.observe).toHaveBeenCalledTimes(1);
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "executing" });
    const subrun = await setup.store.getProviderSubrunForJob(job.jobId);
    if (!subrun) throw new Error("expected submitted subrun");
    await expect(setup.store.getProviderObservation(subrun.subrunId)).resolves.toMatchObject({ state: "observing" });
  });

  it("keeps a committed GUID durable instead of finalizing it in a bounded Cron", async () => {
    const setup = await fixture({ observations: [readyPrivateImage()] });
    const job = await submitAccepted(setup, "drain-recovery-prepared-guid");
    await seedPreparedOutput(setup, job);

    // This simulates a worker crash after the original provider result was
    // uploaded and its Photon GUID committed, but before atomic publication.
    // Recovery must not reopen the provider or upload boundary when submit is
    // rolled back while recovery/delivery remain intentionally active.
    const recoveryRuntime = setup.runtimeWithPolicy({
      acceptNewJobs: false,
      allowProviderSubmit: false,
      allowProviderRecovery: true,
      deliveryEnabled: true,
    });
    await expect(drainStudio(recoveryRuntime)).resolves.toBeUndefined();

    // A bounded Cron must not enter the executing provider-result lifecycle,
    // even when the upload itself happened before the worker crash. It leaves
    // the encrypted GUID and job intact for the future reviewed durable split.
    await expect(drainStudio(recoveryRuntime)).resolves.toBeUndefined();

    expect(setup.provider.submit).toHaveBeenCalledTimes(1);
    expect(setup.provider.observe).not.toHaveBeenCalled();
    expect(setup.transport.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    expect(setup.transport.events).toEqual([]);
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "executing" });

    const [preamble, cursor] = await Promise.all([
      setup.store.getJobResultPreamble(job.jobId),
      setup.store.getJobResultCursor(job.jobId),
    ]);
    expect(preamble).toBeNull();
    expect(cursor).toBeNull();
    expectProviderSecretsAbsent(setup);
  });

  it("fails finalization closed for a stale observation lease and a later release fence", async () => {
    const setup = await fixture();
    const job = await submitAccepted(setup, "stale-finalizer-fence");
    await seedPreparedOutput(setup, job);
    const subrun = await setup.store.getProviderSubrunForJob(job.jobId);
    if (!subrun) throw new Error("expected submitted subrun");
    const payload = setup.store.decryptJobPayload(job);
    if (!payload.execution) throw new Error("expected execution snapshot");
    await expect(setup.store.ensureProviderObservation(subrun, payload.execution)).resolves.toBeTruthy();

    const staleLease = await setup.store.acquireProviderObservationLease(subrun.subrunId);
    if (!staleLease) throw new Error("expected observation lease");
    await staleLease.release();
    await expect(setup.store.finalizePreparedProviderResult(subrun.subrunId, staleLease.token))
      .resolves.toEqual({ kind: "lease_lost" });

    const currentLease = await setup.store.acquireProviderObservationLease(subrun.subrunId);
    if (!currentLease) throw new Error("expected current observation lease");
    const rollback = new StudioStore({
      redis: setup.redis,
      cipher: setup.cipher,
      identityHashSecret: IDENTITY_SECRET,
      prefix: setup.prefix,
      clock: setup.now,
      releaseStamp: frozen({ generation: 2, phase: "dark" as const }),
    });
    await expect(rollback.advanceReleaseFence(1)).resolves.toEqual({ kind: "advanced" });
    await expect(setup.store.finalizePreparedProviderResult(subrun.subrunId, currentLease.token))
      .resolves.toEqual({ kind: "release_blocked" });
    await currentLease.release();

    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "executing" });
    await expect(setup.store.getJobResultCursor(job.jobId)).resolves.toBeNull();
  });

  it("does not enter native Photon preparation from the shared Cron", async () => {
    const setup = await fixture({ observations: [readyPrivateImage()] });
    const job = await submitAccepted(setup, "shared-deadline-native-preparation");

    await expect(drainStudioJobs(setup.runtime)).resolves.toBe(1);

    expect(setup.provider.observe).not.toHaveBeenCalled();
    expect(setup.transport.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    await expect(setup.store.getJob(job.jobId)).resolves.toMatchObject({ state: "executing" });
  });
});
