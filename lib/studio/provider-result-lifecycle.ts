import {
  validateStudioProviderArtifact,
  type StudioValidatedProviderArtifact,
} from "./artifact-validation.js";
import {
  canDeliverStudioMessages,
  canRecoverPaidStudioProvider,
} from "./config.js";
import {
  isStudioPreparedOutputTransport,
  type StudioIMessageTransport,
  type StudioTransportOperationOptions,
} from "./imessage-transport.js";
import {
  isStudioProviderObservationPort,
  isStudioProviderResultReferencePort,
  type StudioProviderObservationResult,
  type StudioProviderResultReferencePort,
} from "./provider-port.js";
import { getStudioDeliveryTransport, type StudioRuntime } from "./runtime.js";
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
  /**
   * A Vercel-bounded worker deliberately did not begin a result poll/fetch or
   * Photon upload. Those operations cannot yet be completed as one provably
   * bounded chain without a durable encrypted result-reference phase.
   */
  | { readonly kind: "deadline_deferred" }
  | { readonly kind: "missing_job" }
  | { readonly kind: "not_observable" }
  | { readonly kind: "awaiting_submission" }
  | { readonly kind: "busy" }
  | {
      readonly kind: "pending";
      readonly nextDueAtMs: number;
      /** Stop this job loop so an immediately-due sealed result waits for the next Cron. */
      readonly deferToNextDrain?: boolean | undefined;
    }
  /** Outbox owns its exact retry score; job recovery receives a bounded revisit. */
  | { readonly kind: "delivery_pending" }
  | { readonly kind: "ready" }
  | { readonly kind: "failed" }
  | { readonly kind: "reconciliation_required" };

export type StudioJobResultDispatchAuthorization = "allowed" | "defer" | "blocked";

/**
 * A single bounded Cron has 50 seconds before Vercel's 60-second hard stop.
 * The reviewed GMI artifact read has a 12-second maximum. Before first-use
 * Photon token minting, the transport keeps its 36-second delivery reserve;
 * once that token is ready, the upload path needs its 22-second physical-RPC
 * reserve. A worker only starts the private artifact read when the applicable
 * later phases still fit, leaving a small persistence margin.
 */
const DURABLE_RESULT_MATERIALIZATION_START_RESERVE_MS = 48_000;
const DURABLE_RESULT_PRIVATE_READ_RESERVE_MS = 12_000;
const DURABLE_RESULT_PHYSICAL_UPLOAD_RESERVE_MS = 22_000;
/** Leaves a small wall-clock margin between stage admission and its re-check. */
const DURABLE_RESULT_MATERIALIZATION_RECHECK_MARGIN_MS = 250;

/**
 * Observes a durably accepted request, validates an in-memory result, and
 * prepares a native Photon attachment exactly once. The final store call
 * atomically publishes the preamble + attachment and moves the job to ready.
 *
 * A deadline-bound Vercel Cron may advance this lifecycle only through a
 * reviewed split provider: it persists a sealed result capability in one
 * invocation before private bytes. It then starts materialization only when
 * the current or a later worker still reserves the full Photon preparation
 * window. Legacy combined poll/fetch/upload adapters remain unavailable to
 * the bounded worker.
 */
export async function advanceStudioProviderResultLifecycle(
  runtime: StudioRuntime,
  jobId: string,
  deliveryOptions?: StudioTransportOperationOptions,
): Promise<StudioProviderResultLifecycleResult> {
  const job = await runtime.store.getJob(jobId);
  if (!job) return frozen({ kind: "missing_job" });
  if (job.state === "reconciliation_required") return frozen({ kind: "reconciliation_required" });
  if (job.state !== "executing") return frozen({ kind: "not_observable" });

  if (!canRecoverPaidStudioProvider(runtime.environment) || !runtime.provider.verified) {
    return frozen({ kind: "disabled" });
  }

  // A reviewed split-capability provider is the only path that may advance
  // inside the bounded Cron. Its status GET persists an encrypted retrieval
  // capability first; a later invocation downloads/validates/uploads through
  // the existing native Photon preparation fence.
  if (isStudioProviderResultReferencePort(runtime.provider)) {
    return await advanceDurableProviderResultLifecycle(runtime, job, deliveryOptions);
  }

  // Legacy adapters combine a status poll and private artifact materialization
  // in one call. Retain their unbounded review/test behavior, but keep them
  // fail-closed for the production Cron until they implement the durable split.
  if (!isStudioProviderObservationPort(runtime.provider) || !canDeliverStudioMessages(runtime.environment)) {
    return frozen({ kind: "disabled" });
  }

  // Do not start an executing-result recovery inside the bounded Vercel Cron.
  // A complete path would serially include a provider status GET, private
  // artifact fetch/validation, Photon token setup, native upload, and durable
  // post-upload commits. Until that path is split through a separately
  // encrypted result reference and backed by measured bounds, declining it
  // before *any* content/provider/transport operation is the only dependable
  // outcome. Ready/delivering jobs are reconciled by the caller before this
  // function is reached and remain eligible for their non-provider path.
  if (isDeadlineBoundDeliveryAttempt(deliveryOptions)) return frozen({ kind: "deadline_deferred" });

  // Private provider output must not be fetched unless a verified Photon
  // delivery binding is available to retain its opaque prepared GUID. Resolve
  // that binding only here, after durable job recovery reaches the outbound
  // boundary; a failed token mint leaves the known provider request untouched
  // for a later recovery drain.
  let transport: StudioIMessageTransport;
  try {
    transport = await getStudioDeliveryTransport(runtime, deliveryOptions);
  } catch {
    return frozen({ kind: "disabled" });
  }
  if (!transport.verified || !isStudioPreparedOutputTransport(transport)) {
    return frozen({ kind: "disabled" });
  }

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
          transport,
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
          deliveryOptions,
        });
    }
  } finally {
    await observationLease.release();
  }
}

function isDeadlineBoundDeliveryAttempt(options?: StudioTransportOperationOptions): boolean {
  // Malformed caller options must fail closed the same way as a real absolute
  // deadline. The only route that may start this combined chain today is the
  // explicit unbounded review/test path, which omits options entirely.
  return options?.deadlineAtMs !== undefined;
}

/**
 * The durable path deliberately persists the no-bytes status result before
 * the private download/upload phase. The second phase requires the full
 * reviewed time reservation; if the current invocation still has it, the
 * already-committed capability may proceed immediately, otherwise it is left
 * untouched for the next Cron.
 */
async function advanceDurableProviderResultLifecycle(
  runtime: StudioRuntime,
  job: StudioJobRecord,
  deliveryOptions?: StudioTransportOperationOptions,
): Promise<StudioProviderResultLifecycleResult> {
  const provider = runtime.provider;
  if (!isStudioProviderResultReferencePort(provider)) return frozen({ kind: "disabled" });

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
    // A previously committed Photon GUID is always stronger than a provider
    // read or media fetch. The finalizer is a pure durable state transition.
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
    if (begin.kind === "result_ready") {
      if (!hasDurableMaterializationStartWindow(deliveryOptions, begin.observation.deadlineAtMs)) {
        return deferSealedProviderResultToNextDrain(begin.observation.nextDueAtMs);
      }
      return await materializeDurableProviderResult({
        runtime,
        provider,
        job,
        snapshot: payload.execution,
        subrun,
        observationLeaseToken: observationLease.token,
        observationDeadlineAtMs: begin.observation.deadlineAtMs,
        observationNextDueAtMs: begin.observation.nextDueAtMs,
        deliveryOptions,
      });
    }
    if (begin.kind !== "ready") return await mapObservationBegin(runtime, job, begin);

    const requestReference = await runtime.store.getProviderSubrunReference(subrun.subrunId);
    if (!requestReference) {
      return await finishObservedFailure(
        runtime,
        job.jobId,
        subrun.subrunId,
        observationLease.token,
        "reconciliation_required",
      );
    }

    let polled: Awaited<ReturnType<StudioProviderResultReferencePort["pollResult"]>>;
    try {
      polled = await provider.pollResult({
        job: providerJobView(job),
        snapshot: payload.execution,
        providerRequestId: requestReference.providerRequestId,
      });
    } catch {
      polled = frozen({ kind: "retryable_failure" });
    }

    switch (polled.kind) {
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
      case "ready": {
        const committed = await runtime.store.commitProviderResultReference(
          subrun.subrunId,
          observationLease.token,
          polled.reference,
        );
        switch (committed.kind) {
          case "result_ready":
          case "existing":
            // The result reference is now durable before any private bytes
            // leave the provider. A status response arriving late in its own
            // long polling window should not be forced to wait a minute and
            // miss an otherwise safe current Cron materialization window.
            if (
              isDeadlineBoundDeliveryAttempt(deliveryOptions)
              && hasDurableMaterializationStartWindow(deliveryOptions, committed.observation.deadlineAtMs)
            ) {
              return await materializeDurableProviderResult({
                runtime,
                provider,
                job,
                snapshot: payload.execution,
                subrun,
                observationLeaseToken: observationLease.token,
                observationDeadlineAtMs: committed.observation.deadlineAtMs,
                observationNextDueAtMs: committed.observation.nextDueAtMs,
                deliveryOptions,
              });
            }
            return frozen({
              kind: "pending",
              nextDueAtMs: committed.observation?.nextDueAtMs ?? Date.now() + 60_000,
              deferToNextDrain: true,
            });
          case "lease_lost":
            return frozen({ kind: "busy" });
          case "payload_expiring":
          case "unavailable":
            return await finishObservedFailure(
              runtime,
              job.jobId,
              subrun.subrunId,
              observationLease.token,
              "reconciliation_required",
            );
        }
      }
    }
  } finally {
    await observationLease.release();
  }
}

async function materializeDurableProviderResult(input: {
  readonly runtime: StudioRuntime;
  readonly provider: StudioProviderResultReferencePort;
  readonly job: StudioJobRecord;
  readonly snapshot: StudioExecutionSnapshot;
  readonly subrun: StudioProviderSubrunRecord;
  readonly observationLeaseToken: string;
  readonly observationDeadlineAtMs: number;
  readonly observationNextDueAtMs: number;
  readonly deliveryOptions?: StudioTransportOperationOptions | undefined;
}): Promise<StudioProviderResultLifecycleResult> {
  const {
    runtime,
    provider,
    job,
    snapshot,
    subrun,
    observationLeaseToken,
    observationDeadlineAtMs,
    observationNextDueAtMs,
    deliveryOptions,
  } = input;
  if (!canDeliverStudioMessages(runtime.environment)) return frozen({ kind: "disabled" });
  if (!hasDurableMaterializationStartWindow(deliveryOptions, observationDeadlineAtMs)) {
    return deferSealedProviderResultToNextDrain(observationNextDueAtMs);
  }

  const reference = await runtime.store.getProviderResultReference(subrun.subrunId);
  if (!reference) {
    return await finishObservedFailure(
      runtime,
      job.jobId,
      subrun.subrunId,
      observationLeaseToken,
      "reconciliation_required",
    );
  }

  // Mint the Photon capability before consuming private provider bytes. A
  // rejected token leaves the sealed provider result alone for later bounded
  // recovery instead of forcing a result refetch or an ambiguous upload.
  let transport: StudioIMessageTransport;
  try {
    transport = await getStudioDeliveryTransport(runtime, deliveryOptions);
  } catch {
    return frozen({ kind: "disabled" });
  }
  if (!transport.verified || !isStudioPreparedOutputTransport(transport)) {
    return frozen({ kind: "disabled" });
  }
  if (!hasDurablePostTokenMaterializationWindow(deliveryOptions, observationDeadlineAtMs)) {
    return deferSealedProviderResultToNextDrain(observationNextDueAtMs);
  }

  // Count only the work that can now reach the provider's private artifact
  // endpoint. Credential/token or preflight failures above leave the sealed
  // capability retryable without consuming the finite materialization budget.
  const materializationStart = await runtime.store.beginProviderObservation(
    subrun.subrunId,
    observationLeaseToken,
    { startResultMaterialization: true },
  );
  if (materializationStart.kind !== "result_ready") {
    if (materializationStart.kind === "ready") {
      await transitionToReconciliation(runtime, job.jobId);
      return frozen({ kind: "reconciliation_required" });
    }
    return await mapObservationBegin(runtime, job, materializationStart);
  }

  let materialized: StudioProviderObservationResult;
  try {
    materialized = await provider.materializeResult({
      job: providerJobView(job),
      snapshot,
      reference,
    });
  } catch {
    materialized = frozen({ kind: "retryable_failure" });
  }

  switch (materialized.kind) {
    case "pending":
    case "retryable_failure": {
      const pending = await persistPendingObservation(runtime, subrun.subrunId, observationLeaseToken);
      return pending.kind === "pending"
        ? deferSealedProviderResultToNextDrain(pending.nextDueAtMs)
        : pending;
    }
    case "definitive_failure":
      return await finishObservedFailure(runtime, job.jobId, subrun.subrunId, observationLeaseToken, "failed");
    case "reconciliation_required":
      return await finishObservedFailure(
        runtime,
        job.jobId,
        subrun.subrunId,
        observationLeaseToken,
        "reconciliation_required",
      );
    case "ready":
      // Validation and the sole physical Photon upload retain the existing
      // preparation lease/finalizer semantics. Re-check after result fetch so
      // a slow private read can never start an upload in a stale worker.
      if (!hasDurableDeliveryPreparationWindow(deliveryOptions, observationDeadlineAtMs)) {
        discardUnconsumedProviderArtifacts(materialized.artifacts);
        return deferSealedProviderResultToNextDrain(observationNextDueAtMs);
      }
      let payload: ReturnType<StudioRuntime["store"]["decryptJobPayload"]>;
      try {
        payload = runtime.store.decryptJobPayload(job);
      } catch {
        discardUnconsumedProviderArtifacts(materialized.artifacts);
        return await finishObservedFailure(
          runtime,
          job.jobId,
          subrun.subrunId,
          observationLeaseToken,
          "reconciliation_required",
        );
      }
      return await prepareReadyProviderResult({
        runtime,
        transport,
        job,
        snapshot,
        thread: Object.freeze({
          chatGuid: payload.chatGuid,
          sendingLine: payload.sendingLine,
          ...(payload.replyTargetId === undefined ? {} : { replyTargetId: payload.replyTargetId }),
        }),
        artifacts: materialized.artifacts,
        subrun,
        observationLeaseToken,
        deliveryOptions,
      });
  }
}

/**
 * A sealed result is already due; a fixed one-minute retry can fall between
 * Vercel's minute Cron ticks and let a short-lived signed capability expire.
 * Leave its queue score immediately due, but stop this worker so a fresh
 * invocation receives the whole private-read/Photon budget.
 */
function deferSealedProviderResultToNextDrain(nextDueAtMs: number): StudioProviderResultLifecycleResult {
  return frozen({
    kind: "pending" as const,
    nextDueAtMs: Number.isSafeInteger(nextDueAtMs) && nextDueAtMs >= 0 ? nextDueAtMs : Date.now(),
    deferToNextDrain: true,
  });
}

function hasDurableMaterializationStartWindow(
  options?: StudioTransportOperationOptions,
  observationDeadlineAtMs?: number,
): boolean {
  return hasDurableResultWindow(
    options,
    observationDeadlineAtMs,
    Math.max(
      DURABLE_RESULT_MATERIALIZATION_START_RESERVE_MS,
      options?.minimumDeliveryPreparationRemainingMs ?? 0,
    ),
  );
}

/**
 * A fresh private read starts only after Photon token minting has completed.
 * The remaining chain is the 12-second bounded read plus the physical upload
 * reserve; the token-mint reserve was already consumed before this check.
 */
function hasDurablePostTokenMaterializationWindow(
  options?: StudioTransportOperationOptions,
  observationDeadlineAtMs?: number,
): boolean {
  return hasDurableResultWindow(
    options,
    observationDeadlineAtMs,
    DURABLE_RESULT_PRIVATE_READ_RESERVE_MS + physicalUploadReserve(options),
  );
}

/**
 * Once the private provider read has completed, retain only the independently
 * reviewed physical-upload/persistence reserve before validating bytes and
 * opening the non-idempotent Photon upload boundary.
 */
function hasDurableDeliveryPreparationWindow(
  options?: StudioTransportOperationOptions,
  observationDeadlineAtMs?: number,
): boolean {
  return hasDurableResultWindow(options, observationDeadlineAtMs, physicalUploadReserve(options));
}

function physicalUploadReserve(options?: StudioTransportOperationOptions): number {
  return Math.max(
    DURABLE_RESULT_PHYSICAL_UPLOAD_RESERVE_MS,
    options?.minimumPhysicalActionRemainingMs ?? 0,
  );
}

function hasDurableResultWindow(
  options: StudioTransportOperationOptions | undefined,
  observationDeadlineAtMs: number | undefined,
  reserveMs: number,
): boolean {
  if (options?.deadlineAtMs === undefined) return true;
  const required = Math.max(reserveMs, DURABLE_RESULT_PHYSICAL_UPLOAD_RESERVE_MS)
    + DURABLE_RESULT_MATERIALIZATION_RECHECK_MARGIN_MS;
  const now = Date.now();
  if (!Number.isSafeInteger(options.deadlineAtMs) || options.deadlineAtMs - now < required) return false;
  return observationDeadlineAtMs === undefined
    || (Number.isSafeInteger(observationDeadlineAtMs) && observationDeadlineAtMs - now >= required);
}

/**
 * A late worker must not leave an unread fetch body active after it declines
 * to begin Photon preparation. Async iterables have no universal cancellation
 * protocol; close WHATWG streams best-effort without retaining their bytes or
 * provider-owned errors.
 */
function discardUnconsumedProviderArtifacts(
  artifacts: readonly { readonly stream: unknown }[],
): void {
  for (const artifact of artifacts) {
    const stream = artifact.stream;
    if (typeof stream !== "object" || stream === null || !("cancel" in stream)) continue;
    const cancel = (stream as { readonly cancel?: unknown }).cancel;
    if (typeof cancel !== "function") continue;
    try {
      void Promise.resolve(cancel.call(stream)).catch(() => undefined);
    } catch {
      // Resource cleanup must not leak provider details or alter the durable
      // reconciliation decision.
    }
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

  // The lane keeps the normal preamble → attachment sequence, but a known
  // preamble failure deliberately releases that lane so it cannot block all
  // recovery forever. A worker can then restart between terminalizing the
  // preamble and reconciling the job. Never let that restarted worker infer
  // attachment authorization from the job state alone: the exact durable
  // preamble must have physically completed first.
  const preamble = await runtime.store.getJobResultPreamble(job.jobId);
  if (!preamble || preamble.state !== "sent") return "blocked";

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
  readonly transport: StudioIMessageTransport;
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
  readonly deliveryOptions?: StudioTransportOperationOptions | undefined;
}): Promise<StudioProviderResultLifecycleResult> {
  const { runtime, transport, job, snapshot, thread, artifacts, subrun, observationLeaseToken, deliveryOptions } = input;
  if (!isStudioPreparedOutputTransport(transport)) {
    discardUnconsumedProviderArtifacts(artifacts);
    return frozen({ kind: "disabled" });
  }
  // The first enabled adapter is intentionally fixed to Flux's one-image
  // contract. Future multi-output modes need their own atomic multi-output
  // finalizer rather than treating an array as independently retryable.
  if (
    job.outputCount !== 1
    || snapshot.output.count !== 1
    || artifacts.length !== 1
    || artifacts[0]?.outputIndex !== 1
    || artifacts[0].mediaType !== snapshot.output.type
  ) {
    discardUnconsumedProviderArtifacts(artifacts);
    return await failObservationForReconciliation(runtime, job.jobId, subrun.subrunId, observationLeaseToken);
  }

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
      prepared = await transport.prepareValidatedProviderArtifact(thread, artifact, deliveryOptions);
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
    case "reference_expired":
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
    case "result_ready":
      // Only the split-provider branch may consume a sealed result reference.
      // If an adapter changed between invocations, preserve the record for an
      // explicit reconciliation rather than letting a legacy observer poll it.
      await transitionToReconciliation(runtime, job.jobId);
      return frozen({ kind: "reconciliation_required" });
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
    case "result_ready":
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
    case "reference_expired":
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
