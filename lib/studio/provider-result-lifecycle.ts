import {
  validateStudioProviderArtifact,
  type StudioValidatedProviderArtifact,
} from "./artifact-validation.js";
import {
  canDeliverStudioMessages,
  canRecoverPaidStudioProvider,
} from "./config.js";
import { isStudioPreparedOutputTransport } from "./imessage-transport.js";
import {
  isStudioProviderObservationPort,
  type StudioProviderObservationResult,
} from "./provider-port.js";
import type { StudioRuntime } from "./runtime.js";
import {
  canTransitionJob,
  type StudioExecutionSnapshot,
  type StudioJobRecord,
  type StudioOutboxRecord,
  type StudioProviderObservationRecord,
  type StudioProviderSubrunRecord,
} from "./store.js";

/**
 * Advances only the post-submit path for a known provider request. It has no
 * submission capability: every physical provider call here is a bounded GET
 * or a restricted result fetch owned by the observation record.
 */
export type StudioProviderResultLifecycleResult =
  | { readonly kind: "disabled" }
  | { readonly kind: "missing_job" }
  | { readonly kind: "not_observable" }
  | { readonly kind: "awaiting_submission" }
  | { readonly kind: "busy" }
  | { readonly kind: "pending"; readonly nextDueAtMs: number }
  /** Outbox owns its exact retry score; job recovery receives a bounded revisit. */
  | { readonly kind: "delivery_pending" }
  | { readonly kind: "ready" }
  | { readonly kind: "failed" }
  | { readonly kind: "reconciliation_required" };

export type StudioJobResultDispatchAuthorization = "allowed" | "defer" | "blocked";

/**
 * Observes a durably accepted request, validates an in-memory result, and
 * prepares a native Photon attachment exactly once. The final store call
 * atomically publishes the preamble + attachment and moves the job to ready.
 */
export async function advanceStudioProviderResultLifecycle(
  runtime: StudioRuntime,
  jobId: string,
): Promise<StudioProviderResultLifecycleResult> {
  const job = await runtime.store.getJob(jobId);
  if (!job) return frozen({ kind: "missing_job" });
  if (job.state === "reconciliation_required") return frozen({ kind: "reconciliation_required" });
  if (job.state !== "executing") return frozen({ kind: "not_observable" });

  // Observation currently includes a private result fetch. Do not consume
  // provider output unless a reviewed native delivery path can retain the
  // resulting GUID and send it without exposing provider URLs or bytes.
  if (
    !canRecoverPaidStudioProvider(runtime.environment)
    || !runtime.provider.verified
    || !isStudioProviderObservationPort(runtime.provider)
    || !canDeliverStudioMessages(runtime.environment)
    || !runtime.transport.verified
    || !isStudioPreparedOutputTransport(runtime.transport)
  ) return frozen({ kind: "disabled" });

  let payload: ReturnType<StudioRuntime["store"]["decryptJobPayload"]>;
  try {
    payload = runtime.store.decryptJobPayload(job);
  } catch {
    await transitionToReconciliation(runtime, job.jobId);
    return frozen({ kind: "reconciliation_required" });
  }
  if (!payload.execution) {
    await transitionToReconciliation(runtime, job.jobId);
    return frozen({ kind: "reconciliation_required" });
  }

  const subrun = await runtime.store.getProviderSubrunForJob(job.jobId);
  if (!subrun) return frozen({ kind: "awaiting_submission" });
  if (subrun.state === "definitive_rejection") {
    await transitionToFailed(runtime, job.jobId);
    return frozen({ kind: "failed" });
  }
  if (subrun.state !== "submitted") {
    if (subrun.state === "submission_unknown") await transitionToReconciliation(runtime, job.jobId);
    return subrun.state === "submission_unknown"
      ? frozen({ kind: "reconciliation_required" })
      : frozen({ kind: "awaiting_submission" });
  }

  const existingPreparation = await runtime.store.getJobResultPreparation(job.jobId, 1);
  if (existingPreparation?.state === "upload_unknown") {
    await transitionToReconciliation(runtime, job.jobId);
    return frozen({ kind: "reconciliation_required" });
  }
  // Do not classify `uploading` from this unfenced read. After we own the
  // observation lease, a fresh preparation lease distinguishes a live Photon
  // upload (busy) from an expired one (irreversibly reconciliation-required).

  const observation = await runtime.store.ensureProviderObservation(subrun, payload.execution);
  if (!observation) {
    await transitionToReconciliation(runtime, job.jobId);
    return frozen({ kind: "reconciliation_required" });
  }

  const terminal = await resolveTerminalObservation(runtime, job, observation);
  if (terminal !== null) return terminal;

  const observationLease = await runtime.store.acquireProviderObservationLease(subrun.subrunId);
  if (!observationLease) return frozen({ kind: "busy" });
  try {
    // A committed Photon GUID is a stronger recovery boundary than a second
    // provider GET. The atomic finalizer intentionally accepts scheduled or
    // stale-observing metadata under this fresh lease, so no result refetch or
    // attachment re-upload is needed after a post-upload crash.
    const preparation = await runtime.store.getJobResultPreparation(job.jobId, 1);
    if (preparation?.state === "prepared") {
      return await finalizePreparedResult(runtime, job.jobId, subrun.subrunId, observationLease.token);
    }
    if (preparation?.state === "upload_unknown") {
      await transitionToReconciliation(runtime, job.jobId);
      return frozen({ kind: "reconciliation_required" });
    }
    if (preparation?.state === "uploading") {
      return await recoverExpiredUploadingPreparation(
        runtime,
        job.jobId,
        subrun.subrunId,
        observationLease.token,
      );
    }
    if (preparation?.state === "failed" || preparation?.state === "canceled") {
      return await finishObservedFailure(runtime, job.jobId, subrun.subrunId, observationLease.token, "failed");
    }

    const begin = await runtime.store.beginProviderObservation(subrun.subrunId, observationLease.token);
    if (begin.kind !== "ready") return await mapObservationBegin(runtime, job, begin);

    const reference = await runtime.store.getProviderSubrunReference(subrun.subrunId);
    if (!reference) {
      return await finishObservedFailure(
        runtime,
        job.jobId,
        subrun.subrunId,
        observationLease.token,
        "reconciliation_required",
      );
    }

    let providerResult: StudioProviderObservationResult;
    try {
      providerResult = await runtime.provider.observe({
        job: providerJobView(job),
        snapshot: payload.execution,
        providerRequestId: reference.providerRequestId,
      });
    } catch {
      providerResult = frozen({ kind: "retryable_failure" });
    }

    switch (providerResult.kind) {
      case "pending":
      case "retryable_failure":
        return await persistPendingObservation(runtime, subrun.subrunId, observationLease.token);
      case "definitive_failure":
        return await finishObservedFailure(runtime, job.jobId, subrun.subrunId, observationLease.token, "failed");
      case "reconciliation_required":
        return await finishObservedFailure(
          runtime,
          job.jobId,
          subrun.subrunId,
          observationLease.token,
          "reconciliation_required",
        );
      case "ready":
        return await prepareReadyProviderResult({
          runtime,
          job,
          snapshot: payload.execution,
          thread: Object.freeze({
            chatGuid: payload.chatGuid,
            sendingLine: payload.sendingLine,
            ...(payload.replyTargetId === undefined ? {} : { replyTargetId: payload.replyTargetId }),
          }),
          artifacts: providerResult.artifacts,
          subrun,
          observationLeaseToken: observationLease.token,
        });
    }
  } finally {
    await observationLease.release();
  }
}

/**
 * Gates job-result intents before their physical iMessage action. Preamble
 * text can leave only after the atomic finalizer made the result ready; the
 * attachment advances the state to `delivering` before Photon is contacted.
 */
export async function authorizeStudioJobResultDispatch(
  runtime: StudioRuntime,
  record: StudioOutboxRecord,
): Promise<StudioJobResultDispatchAuthorization> {
  if (record.lane !== "job-result" || record.jobId === undefined) return "allowed";
  const job = await runtime.store.getJob(record.jobId);
  if (!job) return "blocked";

  if (record.resultOutputIndex === undefined) {
    return job.state === "ready" || job.state === "delivering" ? "allowed" : "blocked";
  }

  if (job.state === "delivering") return "allowed";
  if (job.state !== "ready") return job.state === "executing" ? "defer" : "blocked";

  const transitioned = await runtime.store.transitionJob(job.jobId, "ready", "delivering");
  if (transitioned?.state === "delivering") return "allowed";
  const current = await runtime.store.getJob(job.jobId);
  return current?.state === "delivering" ? "allowed" : current?.state === "executing" ? "defer" : "blocked";
}

/**
 * Reconciles the durable preamble/attachment cursor without sending anything.
 * It is invoked after a result outbox outcome and by the minute recovery job
 * queue, so a worker crash after a physical send cannot strand the active job.
 */
export async function reconcileStudioJobResultDelivery(
  runtime: StudioRuntime,
  jobId: string,
): Promise<StudioProviderResultLifecycleResult> {
  const job = await runtime.store.getJob(jobId);
  if (!job) return frozen({ kind: "missing_job" });
  if (job.state === "reconciliation_required") return frozen({ kind: "reconciliation_required" });
  if (job.state !== "ready" && job.state !== "delivering") return frozen({ kind: "not_observable" });

  const [preamble, cursor] = await Promise.all([
    runtime.store.getJobResultPreamble(job.jobId),
    runtime.store.getJobResultCursor(job.jobId),
  ]);
  if (!preamble || !cursor || cursor.outputs.length !== job.outputCount) {
    await transitionToReconciliation(runtime, job.jobId);
    return frozen({ kind: "reconciliation_required" });
  }

  if (preamble.state === "send_unknown" || preamble.state === "canceled" || preamble.state === "failed") {
    await transitionToReconciliation(runtime, job.jobId);
    return frozen({ kind: "reconciliation_required" });
  }
  // The outbox owns its exact retry score. Reusing a historical metadata
  // timestamp here would make this job immediately claimable forever and
  // could starve provider polling work in the bounded job drain.
  if (preamble.state !== "sent") return frozen({ kind: "delivery_pending" });

  let hasPending = false;
  for (const output of cursor.outputs) {
    switch (output.state) {
      case "sent":
        break;
      case "failed":
        await transitionToFailed(runtime, job.jobId);
        return frozen({ kind: "failed" });
      case "send_unknown":
      case "confirmed_absent":
      case "requeued":
      case "canceled":
        await transitionToReconciliation(runtime, job.jobId);
        return frozen({ kind: "reconciliation_required" });
      default:
        hasPending = true;
        break;
    }
  }
  if (hasPending) return frozen({ kind: "delivery_pending" });

  let current = job;
  if (current.state === "ready") {
    const moved = await runtime.store.transitionJob(current.jobId, "ready", "delivering");
    if (!moved) return frozen({ kind: "busy" });
    current = moved;
  }
  if (current.state !== "delivering") return frozen({ kind: "busy" });
  const delivered = await runtime.store.transitionJob(current.jobId, "delivering", "delivered");
  return delivered ? frozen({ kind: "ready" }) : frozen({ kind: "busy" });
}

async function prepareReadyProviderResult(input: {
  readonly runtime: StudioRuntime;
  readonly job: StudioJobRecord;
  readonly snapshot: StudioExecutionSnapshot;
  readonly thread: {
    readonly chatGuid: string;
    readonly sendingLine: string;
    readonly replyTargetId?: string | undefined;
  };
  readonly artifacts: readonly {
    readonly outputIndex: 1 | 2 | 3 | 4;
    readonly mediaType: "image" | "video";
    readonly declaredMimeType?: string | null | undefined;
    readonly stream: ReadableStream<Uint8Array> | AsyncIterable<Uint8Array>;
  }[];
  readonly subrun: StudioProviderSubrunRecord;
  readonly observationLeaseToken: string;
}): Promise<StudioProviderResultLifecycleResult> {
  const { runtime, job, snapshot, thread, artifacts, subrun, observationLeaseToken } = input;
  if (!isStudioPreparedOutputTransport(runtime.transport)) return frozen({ kind: "disabled" });
  // The first enabled adapter is intentionally fixed to Flux's one-image
  // contract. Future multi-output modes need their own atomic multi-output
  // finalizer rather than treating an array as independently retryable.
  if (
    job.outputCount !== 1
    || snapshot.output.count !== 1
    || artifacts.length !== 1
    || artifacts[0]?.outputIndex !== 1
    || artifacts[0].mediaType !== snapshot.output.type
  ) return await failObservationForReconciliation(runtime, job.jobId, subrun.subrunId, observationLeaseToken);

  let artifact: StudioValidatedProviderArtifact;
  try {
    artifact = await validateStudioProviderArtifact({
      expectedMediaType: snapshot.output.type,
      declaredMimeType: artifacts[0].declaredMimeType,
      stream: artifacts[0].stream,
      outputIndex: 1,
    });
  } catch {
    return await failObservationForReconciliation(runtime, job.jobId, subrun.subrunId, observationLeaseToken);
  }

  const preparation = await runtime.store.ensureJobResultPreparation(job.jobId, 1);
  if (!preparation) return await failObservationForReconciliation(runtime, job.jobId, subrun.subrunId, observationLeaseToken);
  if (preparation.state === "prepared") {
    return await finalizePreparedResult(runtime, job.jobId, subrun.subrunId, observationLeaseToken);
  }
  if (preparation.state === "upload_unknown") {
    await transitionToReconciliation(runtime, job.jobId);
    return frozen({ kind: "reconciliation_required" });
  }
  if (preparation.state === "uploading") {
    return await recoverExpiredUploadingPreparation(
      runtime,
      job.jobId,
      subrun.subrunId,
      observationLeaseToken,
    );
  }
  if (preparation.state === "failed" || preparation.state === "canceled") {
    return await finishObservedFailure(runtime, job.jobId, subrun.subrunId, observationLeaseToken, "failed");
  }

  const preparationLease = await runtime.store.acquireJobResultPreparationLease(job.jobId, 1);
  if (!preparationLease) return frozen({ kind: "busy" });
  try {
    const latestJob = await runtime.store.getJob(job.jobId);
    if (!latestJob || latestJob.state !== "executing") return frozen({ kind: "busy" });
    const begin = await runtime.store.beginJobResultPreparation(latestJob, 1, preparationLease.token);
    switch (begin.kind) {
      case "prepared":
        return await finalizePreparedResult(runtime, job.jobId, subrun.subrunId, observationLeaseToken);
      case "upload_unknown":
      case "release_blocked":
      case "payload_expiring":
      case "canceled":
        return await finishObservedFailure(
          runtime,
          job.jobId,
          subrun.subrunId,
          observationLeaseToken,
          "reconciliation_required",
        );
      case "failed":
        return await finishObservedFailure(runtime, job.jobId, subrun.subrunId, observationLeaseToken, "failed");
      case "lease_lost":
      case "unavailable":
        return frozen({ kind: "busy" });
      case "ready":
        break;
    }

    let prepared;
    try {
      prepared = await runtime.transport.prepareValidatedProviderArtifact(thread, artifact);
    } catch {
      prepared = frozen({ kind: "send_unknown" as const, code: "prepared_output_upload_unknown" });
    }

    const preparationOutcome = prepared.kind === "prepared"
      ? prepared
      : prepared.kind === "retryable_failure"
        ? frozen({ kind: "retryable_failure" as const })
        : prepared.kind === "failed"
          ? frozen({ kind: "failed" as const })
          : frozen({ kind: "upload_unknown" as const });
    const completed = await runtime.store.finishJobResultPreparation(
      latestJob,
      1,
      preparationLease.token,
      preparationOutcome,
    );
    switch (completed.kind) {
      case "prepared":
        return await finalizePreparedResult(runtime, job.jobId, subrun.subrunId, observationLeaseToken);
      case "retryable_failure":
        return await persistPendingObservation(runtime, subrun.subrunId, observationLeaseToken);
      case "failed":
        return await finishObservedFailure(runtime, job.jobId, subrun.subrunId, observationLeaseToken, "failed");
      case "upload_unknown":
        return frozen({ kind: "reconciliation_required" });
      case "lease_lost":
      case "unavailable":
        return frozen({ kind: "busy" });
    }
  } finally {
    await preparationLease.release();
  }
}

/**
 * An `uploading` marker is a physical Photon boundary. It may mean a live
 * worker is still uploading, or that the former worker died after beginning
 * an upload and before recording the attachment GUID. Only a newly acquired
 * preparation lease can tell those cases apart. The store primitive never
 * uploads or retries the physical action: an expired marker becomes durable
 * reconciliation instead of risking a duplicate attachment.
 */
async function recoverExpiredUploadingPreparation(
  runtime: StudioRuntime,
  jobId: string,
  subrunId: string,
  observationLeaseToken: string,
): Promise<StudioProviderResultLifecycleResult> {
  const preparationLease = await runtime.store.acquireJobResultPreparationLease(jobId, 1);
  if (!preparationLease) return frozen({ kind: "busy" });
  try {
    const recovered = await runtime.store.recoverExpiredJobResultPreparation(
      jobId,
      1,
      preparationLease.token,
    );
    switch (recovered.kind) {
      case "prepared":
        return await finalizePreparedResult(runtime, jobId, subrunId, observationLeaseToken);
      case "upload_unknown":
        // The store atomically set the active job to reconciliation-required
        // before returning. Do not make a provider GET or Photon retry here.
        // The best-effort transition also covers a concurrently changed or
        // legacy job record that the store rightly refused to mutate.
        await transitionToReconciliation(runtime, jobId);
        return frozen({ kind: "reconciliation_required" });
      case "failed":
        await transitionToFailed(runtime, jobId);
        return frozen({ kind: "failed" });
      case "canceled":
        await transitionToReconciliation(runtime, jobId);
        return frozen({ kind: "reconciliation_required" });
      case "reserved":
      case "lease_lost":
      case "unavailable":
        return frozen({ kind: "busy" });
    }
  } finally {
    await preparationLease.release();
  }
}

async function finalizePreparedResult(
  runtime: StudioRuntime,
  jobId: string,
  subrunId: string,
  observationLeaseToken: string,
): Promise<StudioProviderResultLifecycleResult> {
  const finalized = await runtime.store.finalizePreparedProviderResult(subrunId, observationLeaseToken);
  switch (finalized.kind) {
    case "finalized":
    case "existing":
      return frozen({ kind: "ready" });
    case "lease_lost":
      return frozen({ kind: "busy" });
    case "release_blocked":
    case "payload_expiring":
    case "unavailable":
      await transitionToReconciliation(runtime, jobId);
      return frozen({ kind: "reconciliation_required" });
  }
}

async function persistPendingObservation(
  runtime: StudioRuntime,
  subrunId: string,
  observationLeaseToken: string,
): Promise<StudioProviderResultLifecycleResult> {
  const completed = await runtime.store.finishProviderObservation(subrunId, observationLeaseToken, frozen({ kind: "pending" }));
  switch (completed.kind) {
    case "pending":
      return frozen({ kind: "pending", nextDueAtMs: completed.observation.nextDueAtMs });
    case "deadline_exceeded":
    case "attempts_exhausted":
      await transitionToReconciliation(runtime, completed.observation.jobId);
      return frozen({ kind: "reconciliation_required" });
    case "failed":
      return await mapFailedObservation(runtime, completed.observation);
    case "succeeded":
    case "canceled":
      return frozen({ kind: "busy" });
    case "lease_lost":
    case "unavailable":
      return frozen({ kind: "busy" });
  }
}

async function mapObservationBegin(
  runtime: StudioRuntime,
  job: StudioJobRecord,
  begin: Exclude<Awaited<ReturnType<StudioRuntime["store"]["beginProviderObservation"]>>, { readonly kind: "ready" }>,
): Promise<StudioProviderResultLifecycleResult> {
  switch (begin.kind) {
    case "not_due":
      return frozen({ kind: "pending", nextDueAtMs: begin.observation.nextDueAtMs });
    case "failed":
      return await mapFailedObservation(runtime, begin.observation);
    case "deadline_exceeded":
    case "attempts_exhausted":
    case "reference_expired":
      await transitionToReconciliation(runtime, job.jobId);
      return frozen({ kind: "reconciliation_required" });
    case "succeeded":
      // The finalizer commits `succeeded` and `executing → ready` together.
      // Seeing success on an executing job therefore proves a corrupt or old
      // partial state, which must never be guessed into delivery.
      await transitionToReconciliation(runtime, job.jobId);
      return frozen({ kind: "reconciliation_required" });
    case "canceled":
    case "lease_lost":
    case "unavailable":
      return frozen({ kind: "busy" });
  }
}

async function resolveTerminalObservation(
  runtime: StudioRuntime,
  job: StudioJobRecord,
  observation: StudioProviderObservationRecord,
): Promise<StudioProviderResultLifecycleResult | null> {
  switch (observation.state) {
    case "scheduled":
    case "observing":
      return null;
    case "failed":
      return await mapFailedObservation(runtime, observation);
    case "deadline_exceeded":
    case "attempts_exhausted":
    case "reference_expired":
      await transitionToReconciliation(runtime, job.jobId);
      return frozen({ kind: "reconciliation_required" });
    case "succeeded":
      await transitionToReconciliation(runtime, job.jobId);
      return frozen({ kind: "reconciliation_required" });
    case "canceled":
      return frozen({ kind: "busy" });
  }
}

/**
 * A failed observation is not always a definitive provider rejection. The
 * durable disposition is committed under the same observation lease as the
 * terminal state, so a stale worker cannot turn an ambiguous private result
 * into a clean failed paid job.
 */
async function mapFailedObservation(
  runtime: StudioRuntime,
  observation: StudioProviderObservationRecord,
): Promise<StudioProviderResultLifecycleResult> {
  if (observation.failureDisposition === "reconciliation_required") {
    await transitionToReconciliation(runtime, observation.jobId);
    return frozen({ kind: "reconciliation_required" });
  }
  await transitionToFailed(runtime, observation.jobId);
  return frozen({ kind: "failed" });
}

async function failObservationForReconciliation(
  runtime: StudioRuntime,
  jobId: string,
  subrunId: string,
  observationLeaseToken: string,
): Promise<StudioProviderResultLifecycleResult> {
  return await finishObservedFailure(runtime, jobId, subrunId, observationLeaseToken, "reconciliation_required");
}

/**
 * A worker may terminalize a job only after its own observation lease has
 * atomically committed the corresponding provider-observation terminal state.
 * This prevents a late GET/fetch worker from defeating a newer worker that is
 * finishing a valid result after the old lease expired.
 */
async function finishObservedFailure(
  runtime: StudioRuntime,
  jobId: string,
  subrunId: string,
  observationLeaseToken: string,
  target: "failed" | "reconciliation_required",
): Promise<StudioProviderResultLifecycleResult> {
  const completed = await runtime.store.finishProviderObservation(
    subrunId,
    observationLeaseToken,
    frozen({ kind: "failed", failureDisposition: target }),
  );
  switch (completed.kind) {
    case "failed":
      // Trust the atomically persisted disposition, not the local caller's
      // intention. A future store migration or lease race must stay
      // fail-closed rather than silently changing reconciliation into failure.
      return await mapFailedObservation(runtime, completed.observation);
    case "deadline_exceeded":
    case "attempts_exhausted":
      await transitionToReconciliation(runtime, completed.observation.jobId);
      return frozen({ kind: "reconciliation_required" });
    case "pending":
    case "succeeded":
    case "canceled":
    case "lease_lost":
    case "unavailable":
      return frozen({ kind: "busy" });
  }
}

async function transitionToReconciliation(runtime: StudioRuntime, jobId: string): Promise<void> {
  const current = await runtime.store.getJob(jobId);
  if (!current || current.state === "reconciliation_required" || !canTransitionJob(current.state, "reconciliation_required")) return;
  await runtime.store.transitionJob(current.jobId, current.state, "reconciliation_required");
}

async function transitionToFailed(runtime: StudioRuntime, jobId: string): Promise<void> {
  const current = await runtime.store.getJob(jobId);
  if (!current || current.state === "failed" || !canTransitionJob(current.state, "failed")) {
    if (current && current.state !== "failed") await transitionToReconciliation(runtime, jobId);
    return;
  }
  await runtime.store.transitionJob(current.jobId, current.state, "failed");
}

function providerJobView(job: StudioJobRecord): Pick<StudioJobRecord,
  "jobId" | "mode" | "modelSlug" | "provider" | "outputCount" | "durationSeconds" | "reservedCents"
> {
  return Object.freeze({
    jobId: job.jobId,
    mode: job.mode,
    modelSlug: job.modelSlug,
    provider: job.provider,
    outputCount: job.outputCount,
    ...(job.durationSeconds === undefined ? {} : { durationSeconds: job.durationSeconds }),
    reservedCents: job.reservedCents,
  });
}

function frozen<T>(value: T): Readonly<T> {
  return Object.freeze(value);
}
