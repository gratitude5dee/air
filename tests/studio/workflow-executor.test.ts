import { describe, expect, it, vi } from "vitest";

import { createQueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { deriveOpaqueId, hashSenderIdentifier, hashThreadIdentifier } from "../../lib/security.js";
import type { StudioEnvironment, StudioReleasePhase } from "../../lib/studio/config.js";
import { UnverifiedPhotonTransport } from "../../lib/studio/imessage-transport.js";
import type { StudioProviderPort, StudioProviderSubmitResult } from "../../lib/studio/provider-port.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";
import {
  STUDIO_REDIS_PREFIX,
  STUDIO_PROVIDER_REFERENCE_COMMIT_WINDOW_MS,
  StudioStore,
  createStudioJob,
  type StudioExecutionSnapshot,
  type StudioJobRecordWithPayload,
} from "../../lib/studio/store.js";
import { executeStudioProviderSubrun } from "../../lib/studio/workflow-executor.js";

const IDENTITY_SECRET = "workflow-executor-test-identity-secret";
const QUEUE_KEY = Buffer.alloc(32, 67).toString("base64");
const SENDER = "+14155550777";
const THREAD = "iMessage;-;+14155550777";

function fixture(options: {
  readonly phase?: StudioReleasePhase;
  readonly provider?: StudioProviderPort;
} = {}) {
  let now = Date.UTC(2026, 6, 18, 12, 0, 0);
  const phase = options.phase ?? "canary";
  const generation = phase === "dark" ? 0 : 1;
  const redis = new MemoryRedis(() => now);
  const cipher = createQueueCipher(QUEUE_KEY);
  const senderHash = hashSenderIdentifier(IDENTITY_SECRET, SENDER);
  const threadHash = hashThreadIdentifier(IDENTITY_SECRET, THREAD);
  const store = new StudioStore({
    redis,
    cipher,
    identityHashSecret: IDENTITY_SECRET,
    prefix: `${STUDIO_REDIS_PREFIX}:workflow-executor-test`,
    clock: () => now,
    releaseStamp: { generation, phase },
  });
  const provider = options.provider ?? fakeProvider({ kind: "accepted", providerRequestId: "fake-request-default" });
  const environment: StudioEnvironment = {
    deploymentMode: "preview",
    releasePhase: phase,
    releaseGeneration: generation,
    ...(phase === "dark" ? {} : { releaseFenceSecret: "test-fence-secret" }),
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
    policy: {
      acceptNewJobs: phase !== "dark",
      allowProviderSubmit: phase !== "dark",
      allowProviderRecovery: false,
      deliveryEnabled: false,
    },
    policyExplicit: true,
    spendCaps: {
      perJobCents: 100,
      dailyCents: 100,
      canaryTotalCents: 100,
      canaryFalCents: 0,
    },
  };
  const runtime: StudioRuntime = {
    environment,
    cipher,
    store,
    transport: new UnverifiedPhotonTransport(),
    provider,
    storageTransport: "rest",
    readiness: (): readonly string[] => [],
    close: async (): Promise<void> => undefined,
  };

  const snapshot: StudioExecutionSnapshot = {
    schemaVersion: 1,
    workflow: {
      workflowId: "air:imagine:v1",
      workflowVersion: 1,
      stepId: "provider-generation",
    },
    model: {
      slug: "verified-model",
      provider: "gmi",
      providerModelId: "verified-provider-model",
    },
    output: { count: 1 },
    releaseStamp: { generation, phase },
  };

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
      payload: {
        prompt: "a private prompt that must remain encrypted",
        chatGuid: THREAD,
        sendingLine: "+14155550777",
        replyTargetId: "reply-guid",
        execution: snapshot,
      },
      cipher,
      now,
    });
  }

  async function admit(record: StudioJobRecordWithPayload): Promise<void> {
    await expect(store.admitJob({
      job: record,
      activeJobLimit: 1,
      dailyUserJobLimit: 3,
      perJobCapCents: 100,
      dailySpendCapCents: 100,
    })).resolves.toMatchObject({ admitted: true });
  }

  async function arm(): Promise<void> {
    if (phase === "dark") return;
    const dark = new StudioStore({
      redis,
      cipher,
      identityHashSecret: IDENTITY_SECRET,
      prefix: `${STUDIO_REDIS_PREFIX}:workflow-executor-test`,
      clock: () => now,
      releaseStamp: { generation: 0, phase: "dark" },
    });
    expect(await dark.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
    expect(await store.advanceReleaseFence(0)).toEqual({ kind: "advanced" });
  }

  return {
    runtime,
    store,
    redis,
    cipher,
    snapshot,
    job,
    admit,
    arm,
    advance(milliseconds: number): void { now += milliseconds; },
  };
}

function fakeProvider(result: StudioProviderSubmitResult): StudioProviderPort & { readonly submit: ReturnType<typeof vi.fn> } {
  return {
    verified: true,
    submit: vi.fn(async () => result),
  };
}

describe("Air provider submission executor", () => {
  it("does not even claim a job when dark, despite a verified fake provider", async () => {
    const provider = fakeProvider({ kind: "accepted", providerRequestId: "dark-should-not-run" });
    const setup = fixture({ phase: "dark", provider });
    const admitted = setup.job("dark-submission");
    await setup.admit(admitted);

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "disabled" });
    expect(provider.submit).not.toHaveBeenCalled();
    await expect(setup.store.getJob(admitted.jobId)).resolves.toMatchObject({ state: "admitted" });
  });

  it("writes an intent before a fake provider accepts and never resubmits an accepted request", async () => {
    const provider = fakeProvider({ kind: "accepted", providerRequestId: "request-accepted-001" });
    const setup = fixture({ provider });
    const admitted = setup.job("accepted-submission");
    const subrunId = deriveOpaqueId(IDENTITY_SECRET, "studio-provider-subrun", admitted.jobId);
    provider.submit.mockImplementation(async () => {
      await expect(setup.store.getProviderSubrun(subrunId)).resolves.toMatchObject({
        state: "submitting",
        submissionIntentId: expect.any(String),
      });
      await expect(setup.store.getProviderSubrunReference(subrunId)).resolves.toBeNull();
      return { kind: "accepted", providerRequestId: "request-accepted-001" };
    });
    await setup.admit(admitted);
    await setup.arm();

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "submitted" });
    expect(provider.submit).toHaveBeenCalledTimes(1);
    await expect(setup.store.getProviderSubrun(subrunId)).resolves.toMatchObject({ state: "submitted" });
    await expect(setup.store.getProviderSubrunReference(subrunId)).resolves.toEqual({ providerRequestId: "request-accepted-001" });
    await expect(setup.store.getJob(admitted.jobId)).resolves.toMatchObject({ state: "executing" });

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "submitted" });
    expect(provider.submit).toHaveBeenCalledTimes(1);
  });

  it("terminalizes definitive rejection without retaining a provider request reference", async () => {
    const provider = fakeProvider({ kind: "definitive_rejection" });
    const setup = fixture({ provider });
    const admitted = setup.job("rejected-submission");
    const subrunId = deriveOpaqueId(IDENTITY_SECRET, "studio-provider-subrun", admitted.jobId);
    await setup.admit(admitted);
    await setup.arm();

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "definitive_rejection" });
    await expect(setup.store.getProviderSubrun(subrunId)).resolves.toMatchObject({ state: "definitive_rejection" });
    await expect(setup.store.getProviderSubrunReference(subrunId)).resolves.toBeNull();
    await expect(setup.store.getJob(admitted.jobId)).resolves.toMatchObject({ state: "failed" });
  });

  it("marks an ambiguous provider result for reconciliation and never retries it", async () => {
    const provider = fakeProvider({ kind: "ambiguous" });
    const setup = fixture({ provider });
    const admitted = setup.job("ambiguous-submission");
    const subrunId = deriveOpaqueId(IDENTITY_SECRET, "studio-provider-subrun", admitted.jobId);
    await setup.admit(admitted);
    await setup.arm();

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "reconciliation_required" });
    await expect(setup.store.getProviderSubrun(subrunId)).resolves.toMatchObject({ state: "submission_unknown" });
    await expect(setup.store.getJob(admitted.jobId)).resolves.toMatchObject({ state: "reconciliation_required" });
    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "reconciliation_required" });
    expect(provider.submit).toHaveBeenCalledTimes(1);
  });

  it("turns a crashed submitting lease into submission_unknown before any second call", async () => {
    const provider = fakeProvider({ kind: "accepted", providerRequestId: "must-not-be-called" });
    const setup = fixture({ provider });
    const admitted = setup.job("orphaned-submission");
    await setup.admit(admitted);
    await setup.arm();
    const executing = await setup.store.transitionJob(admitted.jobId, "admitted", "executing");
    if (!executing?.encryptedPayload) throw new Error("expected encrypted job payload");
    const payload = setup.store.decryptJobPayload(executing);
    if (!payload.execution) throw new Error("expected execution snapshot");
    const subrun = await setup.store.ensureProviderSubrun(executing, payload.execution);
    const firstLease = await setup.store.acquireProviderSubrunLease(subrun.subrunId);
    if (!firstLease) throw new Error("expected first lease");
    await expect(setup.store.beginProviderSubmission(subrun.subrunId, firstLease.token, executing)).resolves.toMatchObject({ kind: "ready" });
    await firstLease.release();

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "reconciliation_required" });
    expect(provider.submit).not.toHaveBeenCalled();
    await expect(setup.store.getProviderSubrun(subrun.subrunId)).resolves.toMatchObject({ state: "submission_unknown" });
  });

  it("blocks a stale release before fake-provider invocation and fails closed after snapshot expiry", async () => {
    const provider = fakeProvider({ kind: "accepted", providerRequestId: "must-not-be-called" });
    const setup = fixture({ provider });
    const admitted = setup.job("stale-release-submission");
    await setup.admit(admitted);
    await setup.arm();
    const rollback = new StudioStore({
      redis: setup.redis,
      cipher: setup.cipher,
      identityHashSecret: IDENTITY_SECRET,
      prefix: `${STUDIO_REDIS_PREFIX}:workflow-executor-test`,
      releaseStamp: { generation: 2, phase: "dark" },
    });
    expect(await rollback.advanceReleaseFence(1)).toEqual({ kind: "advanced" });

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "release_blocked" });
    expect(provider.submit).not.toHaveBeenCalled();
    await expect(setup.store.getJob(admitted.jobId)).resolves.toMatchObject({ state: "canceled" });

    const fresh = fixture({ provider: fakeProvider({ kind: "accepted", providerRequestId: "expired-should-not-run" }) });
    const expired = fresh.job("expired-snapshot-submission");
    await fresh.admit(expired);
    await fresh.arm();
    fresh.advance(24 * 60 * 60 * 1_000 + 1);
    await expect(executeStudioProviderSubrun(fresh.runtime, expired.jobId)).resolves.toEqual({ kind: "snapshot_unavailable" });
    await expect(fresh.store.getJob(expired.jobId)).resolves.toMatchObject({ state: "failed" });
  });

  it("lets a cancellation that wins before the durable pre-call boundary prevent submission", async () => {
    const provider = fakeProvider({ kind: "accepted", providerRequestId: "must-not-be-called" });
    const setup = fixture({ provider });
    const admitted = setup.job("canceled-before-provider-boundary");
    await setup.admit(admitted);
    await setup.arm();
    const acquire = setup.store.acquireProviderSubrunLease.bind(setup.store);
    vi.spyOn(setup.store, "acquireProviderSubrunLease").mockImplementation(async (subrunId) => {
      const lease = await acquire(subrunId);
      await setup.store.transitionJob(admitted.jobId, "executing", "canceled");
      return lease;
    });

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "already_terminal" });
    expect(provider.submit).not.toHaveBeenCalled();
    const subrunId = deriveOpaqueId(IDENTITY_SECRET, "studio-provider-subrun", admitted.jobId);
    await expect(setup.store.getProviderSubrun(subrunId)).resolves.toMatchObject({ state: "canceled" });
  });

  it("never extends a provider reference past the original job privacy deadline", async () => {
    const setup = fixture();
    const admitted = setup.job("provider-reference-hard-deadline");
    await setup.admit(admitted);
    await setup.arm();
    const executing = await setup.store.transitionJob(admitted.jobId, "admitted", "executing");
    if (!executing?.encryptedPayload) throw new Error("expected encrypted job payload");
    const payload = setup.store.decryptJobPayload(executing);
    if (!payload.execution) throw new Error("expected execution snapshot");
    const subrun = await setup.store.ensureProviderSubrun(executing, payload.execution);
    setup.advance(24 * 60 * 60 * 1_000 - 2_000);
    const lease = await setup.store.acquireProviderSubrunLease(subrun.subrunId);
    if (!lease) throw new Error("expected late lease");
    await expect(setup.store.beginProviderSubmission(subrun.subrunId, lease.token, executing)).resolves.toMatchObject({ kind: "ready" });
    await expect(setup.store.finishProviderSubmission(subrun.subrunId, lease.token, {
      kind: "accepted",
      providerRequestId: "expires-with-job",
    })).resolves.toMatchObject({ state: "submitted" });
    await expect(setup.store.getProviderSubrunReference(subrun.subrunId)).resolves.toEqual({ providerRequestId: "expires-with-job" });
    setup.advance(2_001);
    await expect(setup.store.getProviderSubrunReference(subrun.subrunId)).resolves.toBeNull();
  });

  it("does not start a provider call without the bounded reference-commit window", async () => {
    const provider = fakeProvider({ kind: "accepted", providerRequestId: "must-not-be-called" });
    const setup = fixture({ provider });
    const admitted = setup.job("provider-reference-commit-window");
    await setup.admit(admitted);
    await setup.arm();
    setup.advance(24 * 60 * 60 * 1_000 - STUDIO_PROVIDER_REFERENCE_COMMIT_WINDOW_MS);

    await expect(executeStudioProviderSubrun(setup.runtime, admitted.jobId)).resolves.toEqual({ kind: "snapshot_expiring" });
    expect(provider.submit).not.toHaveBeenCalled();
    await expect(setup.store.getJob(admitted.jobId)).resolves.toMatchObject({ state: "failed" });
  });
});
