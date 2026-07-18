import { canSubmitStudioProvider } from "./config.js";
import type { StudioProviderSubmitResult } from "./provider-port.js";
import type { StudioRuntime } from "./runtime.js";
import {
  canTransitionJob,
  STUDIO_PROVIDER_REFERENCE_COMMIT_WINDOW_MS,
  type StudioJobRecord,
  type StudioProviderSubmissionBeginResult,
} from "./store.js";

/**
 * Runs one already-admitted provider subrun. Admission, model enablement,
 * pricing, media normalization, polling, and artifact delivery remain
 * separate stages; this foundation owns only the no-retry submission boundary.
 */
export type StudioProviderExecutionResult =
  | { readonly kind: "disabled" }
  | { readonly kind: "missing_job" }
  | { readonly kind: "not_executable" }
  | { readonly kind: "snapshot_missing" }
  | { readonly kind: "snapshot_unavailable" }
  | { readonly kind: "snapshot_expiring" }
  | { readonly kind: "snapshot_invalid" }
  | { readonly kind: "busy" }
  | { readonly kind: "lease_lost" }
  | { readonly kind: "release_blocked" }
  | { readonly kind: "submitted" }
  | { readonly kind: "definitive_rejection" }
  | { readonly kind: "reconciliation_required" }
  | { readonly kind: "already_terminal" };

/**
 * This function is intentionally not wired into the inbox drain yet. The
 * deployed registry is empty and the runtime provider is unprovisioned; tests
 * inject a fake port to prove the persistence and ambiguity behavior without
 * contacting any paid service.
 */
export async function executeStudioProviderSubrun(
  runtime: StudioRuntime,
  jobId: string,
): Promise<StudioProviderExecutionResult> {
  // Check before any job/subrun claim so a dark deployment cannot mutate a
  // future active queue simply because a worker route was invoked.
  if (!canSubmitStudioProvider(runtime.environment) || !runtime.provider.verified) {
    return Object.freeze({ kind: "disabled" });
  }

  let job = await runtime.store.getJob(jobId);
  if (!job) return Object.freeze({ kind: "missing_job" });
  if (job.state === "reconciliation_required") return Object.freeze({ kind: "reconciliation_required" });
  if (isTerminal(job)) return Object.freeze({ kind: "already_terminal" });
  if (job.state !== "admitted" && job.state !== "executing") {
    return Object.freeze({ kind: "not_executable" });
  }

  if (!job.encryptedPayload) {
    await transitionIfLegal(runtime, job, "failed");
    return Object.freeze({ kind: "snapshot_unavailable" });
  }
  // A submit must have enough time to commit an encrypted provider reference
  // under the original job privacy deadline. Refuse before decrypting or
  // creating a subrun rather than extending that deadline after acceptance.
  if (runtime.store.jobPayloadRemainingMs(job) <= STUDIO_PROVIDER_REFERENCE_COMMIT_WINDOW_MS) {
    await transitionIfLegal(runtime, job, "failed");
    return Object.freeze({ kind: "snapshot_expiring" });
  }

  let payload: ReturnType<StudioRuntime["store"]["decryptJobPayload"]>;
  try {
    payload = runtime.store.decryptJobPayload(job);
  } catch {
    await transitionIfLegal(runtime, job, "failed");
    return Object.freeze({ kind: "snapshot_unavailable" });
  }
  if (!payload.execution) {
    await transitionIfLegal(runtime, job, "failed");
    return Object.freeze({ kind: "snapshot_missing" });
  }

  if (job.state === "admitted") {
    const transitioned = await runtime.store.transitionJob(job.jobId, "admitted", "executing");
    if (!transitioned) {
      const current = await runtime.store.getJob(job.jobId);
      if (!current) return Object.freeze({ kind: "missing_job" });
      if (current.state === "reconciliation_required") return Object.freeze({ kind: "reconciliation_required" });
      if (isTerminal(current)) return Object.freeze({ kind: "already_terminal" });
      if (current.state !== "executing") return Object.freeze({ kind: "not_executable" });
      job = current;
    } else {
      job = transitioned;
    }
  }

  let subrun;
  try {
    subrun = await runtime.store.ensureProviderSubrun(job, payload.execution);
  } catch {
    await transitionIfLegal(runtime, job, "failed");
    return Object.freeze({ kind: "snapshot_invalid" });
  }

  const lease = await runtime.store.acquireProviderSubrunLease(subrun.subrunId);
  if (!lease) return Object.freeze({ kind: "busy" });
  try {
    const begin = await runtime.store.beginProviderSubmission(subrun.subrunId, lease.token, job);
    // Await inside the try so the finally block cannot release the ownership
    // token before the fenced completion record is committed.
    return await completeProviderSubmission(runtime, job, payload, subrun.subrunId, lease.token, begin);
  } finally {
    await lease.release();
  }
}

async function completeProviderSubmission(
  runtime: StudioRuntime,
  job: StudioJobRecord,
  payload: ReturnType<StudioRuntime["store"]["decryptJobPayload"]>,
  subrunId: string,
  leaseToken: string,
  begin: StudioProviderSubmissionBeginResult,
): Promise<StudioProviderExecutionResult> {
  switch (begin.kind) {
    case "lease_lost":
    case "unavailable":
      return Object.freeze({ kind: "lease_lost" });
    case "release_blocked":
      await transitionIfLegal(runtime, job, "canceled");
      return Object.freeze({ kind: "release_blocked" });
    case "submission_unknown":
      await transitionIfLegal(runtime, job, "reconciliation_required");
      return Object.freeze({ kind: "reconciliation_required" });
    case "already_submitted":
      return Object.freeze({ kind: "submitted" });
    case "definitive_rejection":
      await transitionIfLegal(runtime, job, "failed");
      return Object.freeze({ kind: "definitive_rejection" });
    case "canceled":
      await transitionIfLegal(runtime, job, "canceled");
      return Object.freeze({ kind: "already_terminal" });
    case "ready":
      break;
  }

  const providerOutcome = await submitOnce(runtime, job, payload, begin.subrun.submissionIntentId);
  const committed = await runtime.store.finishProviderSubmission(subrunId, leaseToken, job, providerOutcome);
  // If the lease expired after the physical provider call, do not infer that
  // the call failed. A later worker will convert the surviving `submitting`
  // intent into `submission_unknown` instead of replaying it.
  if (!committed) return Object.freeze({ kind: "lease_lost" });

  if (providerOutcome.kind === "accepted") {
    // A cancellation or fence transition might win after the durable begin
    // boundary. The finish Lua transition detects races that occur before its
    // commit; this reread catches a change immediately after it. Neither case
    // may be reported as an uncomplicated successful submission.
    const current = await runtime.store.getJob(job.jobId);
    if (
      committed.state !== "submitted"
      || !current
      || current.state !== "executing"
      || current.stateVersion !== job.stateVersion
    ) {
      await transitionIfLegal(runtime, job, "reconciliation_required");
      return Object.freeze({ kind: "reconciliation_required" });
    }
    return Object.freeze({ kind: "submitted" });
  }

  switch (providerOutcome.kind) {
    case "definitive_rejection":
      await transitionIfLegal(runtime, job, "failed");
      return Object.freeze({ kind: "definitive_rejection" });
    case "ambiguous":
      await transitionIfLegal(runtime, job, "reconciliation_required");
      return Object.freeze({ kind: "reconciliation_required" });
  }
}

async function submitOnce(
  runtime: StudioRuntime,
  job: StudioJobRecord,
  payload: ReturnType<StudioRuntime["store"]["decryptJobPayload"]>,
  submissionIntentId: string,
): Promise<StudioProviderSubmitResult> {
  try {
    // `execution` was checked by the caller immediately before the durable
    // intent was written. The provider never receives a request without it.
    return await runtime.provider.submit({
      job,
      snapshot: payload.execution as NonNullable<typeof payload.execution>,
      submissionIntentId,
      request: Object.freeze({
        prompt: payload.prompt,
        ...(payload.input === undefined ? {} : { input: payload.input }),
      }),
    });
  } catch {
    return Object.freeze({ kind: "ambiguous" });
  }
}

function isTerminal(job: StudioJobRecord): boolean {
  return job.state === "delivered"
    || job.state === "failed"
    || job.state === "canceled"
    || job.state === "maintenance_rejected";
}

async function transitionIfLegal(
  runtime: StudioRuntime,
  job: StudioJobRecord,
  target: "failed" | "canceled" | "reconciliation_required",
): Promise<void> {
  const current = await runtime.store.getJob(job.jobId);
  if (!current || current.state === target || !canTransitionJob(current.state, target)) return;
  await runtime.store.transitionJob(current.jobId, current.state, target);
}
