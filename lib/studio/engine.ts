import { createHash } from "node:crypto";

import {
  parseStudioCommand,
  type StudioCommand,
  type StudioMode,
  type StudioOutputCount,
  type StudioVideoDurationSeconds,
} from "./commands.js";
import {
  canAcceptStudioJobs,
  canDeliverStudioMessages,
  canRecoverPaidStudioProvider,
  canSubmitPaidStudioProvider,
} from "./config.js";
import {
  AIR_MODEL_REGISTRY,
  checkModelCompatibility,
  listEnabledModels,
  resolveDefaultModel,
  resolveEnabledModel,
  validateRegistryVersion,
  type AirModelRegistry,
  type StudioModel,
} from "./model-registry.js";
import {
  isStudioPreparedOutputTransport,
  isSafeTransportText,
  type IMessageThreadReference,
  type StudioIMessageTransport,
  type StudioTransportOperationOptions,
  type TransportOutcome,
} from "./imessage-transport.js";
import { getStudioDeliveryTransport, type StudioRuntime } from "./runtime.js";
import type {
  StudioAttachmentReference,
  StudioInboxRecord,
  StudioInboxRecord as InboxRecord,
  StudioOutboxPayload,
  StudioOutboxRecord,
  PendingWriteGuard,
  StudioCanaryBudgetPolicy,
  StudioJobRecord,
  StudioExecutionSnapshot,
} from "./store.js";
import { createStudioJob, isJobQueueEligibleState } from "./store.js";
import {
  authorizeStudioJobResultDispatch,
  reconcileStudioJobResultDelivery,
  type StudioProviderResultLifecycleResult,
} from "./provider-result-lifecycle.js";
import { executeStudioProviderSubrun } from "./workflow-executor.js";
import { getStudioWorkflow, isSinglePaidStepWorkflow } from "./workflows.js";

const ONBOARDING_VERSION = 1;
const DEFAULT_OUTPUT_COUNT: StudioOutputCount = 1;
const DEFAULT_VIDEO_DURATION_SECONDS: StudioVideoDurationSeconds = 5;
const MAX_INBOX_PER_DRAIN = 12;
const MAX_OUTBOX_PER_DRAIN = 16;
/** Keep one Cron invocation bounded even with many recoverable jobs. */
const MAX_JOBS_PER_DRAIN = 8;
/** Default recovery cadence when no provider observation supplies a due time. */
const JOB_RECHECK_DELAY_MS = 60_000;
const TYPING_DWELL_MS = 300;
const MEDIA_FIRST_CORRECTION_DELAY_MS = 500;
/** Vercel hard-stops the route at 60 seconds; retain room for persistence. */
const DRAIN_INVOCATION_BUDGET_MS = 50_000;
/** Never begin a physical Photon message without its full bounded window. */
const OUTBOX_START_RESERVE_MS = 36_000;
/** After bounded token setup, a send still needs RPC + typing cleanup room. */
const OUTBOX_PHYSICAL_ACTION_MARGIN_MS = 22_000;
const INBOX_START_RESERVE_MS = 1_000;
/** GMI's direct queue operation is bounded at 12s; leave persistence headroom. */
const JOB_START_RESERVE_MS = 20_000;

const HELP_TEXT = [
  "Air commands:",
  "/imagine <prompt>",
  "/animate <prompt>",
  "/edit-image <prompt>",
  "/edit-video <prompt>",
  "/models [mode] • /model-settings • /status • /cancel • /privacy",
].join("\n");

const ONBOARDING_BUBBLES = Object.freeze([
  "Hey — I’m Air by WZRD ⚡ Your image + video studio in iMessage.",
  "Create with /imagine, /animate, /edit-image, or /edit-video. Try: /imagine a chrome hummingbird at sunrise. Settings: /model-settings • Help: /help. Generation is currently unavailable while Air completes provider review; any later approved path will disclose its media-retention terms.",
] as const);

type StoredModeSettings = {
  readonly modelSlug?: string | undefined;
  readonly outputCount: StudioOutputCount;
  readonly durationSeconds?: StudioVideoDurationSeconds | undefined;
};

export interface StudioUserSettings {
  readonly schemaVersion: 1;
  readonly onboardingVersion: number;
  readonly modes: Readonly<Record<StudioMode, StoredModeSettings>>;
}

type PendingIntent =
  | {
      readonly schemaVersion: 1;
      readonly kind: "media";
      readonly attachment: StudioAttachmentReference;
      readonly createdAtMs: number;
    }
  | {
      readonly schemaVersion: 1;
      readonly kind: "awaiting_prompt";
      readonly mode: StudioMode;
      readonly attachment?: StudioAttachmentReference | undefined;
      readonly createdAtMs: number;
    }
  | {
      readonly schemaVersion: 1;
      readonly kind: "awaiting_media";
      readonly mode: "edit-image" | "edit-video";
      readonly prompt: string;
      readonly expectedMedia: "image" | "video";
      readonly createdAtMs: number;
    };

type InboxDisposition = "processed" | "maintenance_rejected" | "failed";

export interface StudioDrainStats {
  readonly inbox: number;
  readonly outbox: number;
}

export interface StudioDrainBudgetOptions {
  /** Test-only clock injection; production uses the current wall clock. */
  readonly now?: (() => number) | undefined;
  /** Test-only explicit absolute cutoff. */
  readonly deadlineAtMs?: number | undefined;
}

/**
 * A single deadline shared by inbox, outbox, and job phases.  It is not a
 * queue deadline: stopping here leaves every unstarted durable record for the
 * following minute Cron.
 */
export interface StudioDrainBudget {
  readonly deadlineAtMs: number;
  remainingMs(): number;
  canStart(minimumRemainingMs: number): boolean;
  transportOptions(): StudioTransportOperationOptions;
}

export function createStudioDrainBudget(options: StudioDrainBudgetOptions = {}): StudioDrainBudget {
  const now = options.now ?? Date.now;
  const startedAtMs = now();
  const deadlineAtMs = options.deadlineAtMs ?? startedAtMs + DRAIN_INVOCATION_BUDGET_MS;
  if (!Number.isSafeInteger(startedAtMs) || !Number.isSafeInteger(deadlineAtMs)) {
    throw new Error("invalid_studio_drain_deadline");
  }
  const remainingMs = (): number => Math.max(0, deadlineAtMs - now());
  return Object.freeze({
    deadlineAtMs,
    remainingMs,
    canStart(minimumRemainingMs: number): boolean {
      return Number.isSafeInteger(minimumRemainingMs) && minimumRemainingMs > 0 && remainingMs() >= minimumRemainingMs;
    },
    transportOptions(): StudioTransportOperationOptions {
      return Object.freeze({
        deadlineAtMs,
        minimumPhysicalActionRemainingMs: OUTBOX_PHYSICAL_ACTION_MARGIN_MS,
        minimumDeliveryPreparationRemainingMs: OUTBOX_START_RESERVE_MS,
      });
    },
  });
}

/**
 * Authoritative bounded worker entrypoint. The minute Vercel Cron invokes it;
 * webhook-triggered work is only a latency hint. It is deliberately safe to
 * run more than once because receipts and outbox IDs are stable.
 */
export async function drainStudio(runtime: StudioRuntime, budget = createStudioDrainBudget()): Promise<void> {
  await drainStudioWithStats(runtime, budget);
  if (!budget.canStart(JOB_START_RESERVE_MS)) return;
  await drainStudioJobs(runtime, budget);
}

export async function drainStudioWithStats(
  runtime: StudioRuntime,
  budget = createStudioDrainBudget(),
): Promise<StudioDrainStats> {
  let inbox = 0;
  for (let index = 0; index < MAX_INBOX_PER_DRAIN; index += 1) {
    if (!budget.canStart(INBOX_START_RESERVE_MS)) break;
    const claim = await runtime.store.claimNextInbox();
    if (!claim) break;

    try {
      const disposition = await processInbox(runtime, claim.record);
      // A stale worker may have lost its lease while it was processing. In
      // that case the CAS-backed store refuses this finish and the new owner
      // remains the only worker permitted to advance the receipt.
      if (await runtime.store.finishInbox(claim.record.receiptId, claim.leaseToken, disposition)) {
        inbox += 1;
      }
    } catch {
      // Do not mark a partially processed receipt terminal. Its durable
      // outbox IDs make a later fenced retry safe; store recovery reclaims the
      // expired lease. No error detail is logged because envelopes may carry
      // prompts, GUIDs, and identifiers.
      break;
    }
  }

  return Object.freeze({ inbox, outbox: await drainStudioOutbox(runtime, budget) });
}

/**
 * Advances durable admitted/executing jobs. Owning the job lease only grants
 * bounded scheduling work; the provider-subrun lease and its Lua commit stay
 * the final authority immediately before a provider side effect.
 *
 * The default runtime cannot enter this loop: both the release policy and a
 * reviewed provider adapter must be explicitly active first.
 */
export async function drainStudioJobs(
  runtime: StudioRuntime,
  budget = createStudioDrainBudget(),
): Promise<number> {
  // The version is a release binding, not a display-only readiness signal.
  // Do not submit, poll, or fetch a provider result through an injected or
  // stale runtime when its configured registry version differs from the
  // immutable registry in use.
  const registryVersionCurrent = validateRegistryVersion(
    runtime.environment.modelRegistryVersion,
    studioRegistry(runtime),
  ).ok;
  const maySubmit = registryVersionCurrent && canSubmitPaidStudioProvider(runtime.environment) && runtime.provider.verified;
  const mayRecover = registryVersionCurrent && canRecoverPaidStudioProvider(runtime.environment) && runtime.provider.verified;
  // A ready/delivering result contains no provider side effect: retain its
  // durable recovery loop even during a submit rollback so an already-sent
  // Photon attachment can reach a truthful terminal job state.
  if (!maySubmit && !mayRecover && !canDeliverStudioMessages(runtime.environment)) return 0;

  let advanced = 0;
  for (let index = 0; index < MAX_JOBS_PER_DRAIN; index += 1) {
    if (!budget.canStart(JOB_START_RESERVE_MS)) break;
    const claim = await runtime.store.claimNextJob();
    if (!claim) break;
    advanced += 1;

    try {
      const lifecycle = await advanceClaimedStudioJob(runtime, claim.record, maySubmit, mayRecover);
      await rescheduleCurrentJob(runtime, claim.record, claim.lease.token, lifecycle);
    } catch {
      // An internal fault before a physical provider-call outcome is not a
      // reason to terminalize paid work. Leave the durable queue entry in
      // place and best-effort delay its freshly read state; a lost CAS/lease
      // is recovered later without reopening the physical submit boundary.
      try {
        await rescheduleCurrentJob(runtime, claim.record, claim.lease.token);
      } catch {
        // The existing ready entry and expiring lease remain recovery-safe.
      }
    } finally {
      await claim.lease.release();
    }
  }
  return advanced;
}

async function rescheduleCurrentJob(
  runtime: StudioRuntime,
  claimed: StudioJobRecord,
  leaseToken: string,
  lifecycle?: StudioProviderResultLifecycleResult | undefined,
): Promise<void> {
  // Provider submission and terminal transitions change state/version, so a
  // stale pre-execution record must never decide queue ownership afterward.
  const current = await runtime.store.getJob(claimed.jobId);
  if (!current || !isJobQueueEligibleState(current.state)) {
    await runtime.store.removeJobFromQueue(claimed.jobId);
    return;
  }
  if (lifecycle?.kind === "pending") {
    await runtime.store.rescheduleClaimedJobAt(current, leaseToken, lifecycle.nextDueAtMs);
    return;
  }
  await runtime.store.rescheduleClaimedJob(current, leaseToken, JOB_RECHECK_DELAY_MS);
}

/**
 * Submission and recovery deliberately use different gates. A rollback may
 * close new paid work while retaining a known accepted request's bounded GET
 * / private-download path, so this dispatcher never turns recovery into a
 * second submit.
 */
async function advanceClaimedStudioJob(
  runtime: StudioRuntime,
  claimed: StudioJobRecord,
  maySubmit: boolean,
  mayRecover: boolean,
): Promise<StudioProviderResultLifecycleResult | undefined> {
  if (claimed.state === "ready" || claimed.state === "delivering") {
    return await reconcileStudioJobResultDelivery(runtime, claimed.jobId);
  }

  if (claimed.state === "admitted") {
    if (!maySubmit) return undefined;
    await executeStudioProviderSubrun(runtime, claimed.jobId);
  } else if (claimed.state === "executing" && maySubmit) {
    // This is still needed for a stale `submitting` boundary: the executor
    // converts it to reconciliation rather than risking a blind re-submit.
    await executeStudioProviderSubrun(runtime, claimed.jobId);
  }

  const current = await runtime.store.getJob(claimed.jobId);
  if (!current) return undefined;
  if (current.state === "ready" || current.state === "delivering") {
    return await reconcileStudioJobResultDelivery(runtime, current.jobId);
  }
  if (current.state !== "executing" || !mayRecover) return undefined;
  // A fresh provider-result observation may serially perform a status GET,
  // private artifact read/validation, Photon token mint, upload, and Redis
  // finalization. That does not fit a proven Vercel Cron deadline yet. Leave
  // the executing job durably scheduled rather than starting any part of that
  // chain; `advanceStudioProviderResultLifecycle` carries the same defensive
  // no-side-effect rule if it is ever called with deadline options directly.
  return undefined;
}

/**
 * Dispatches at most one physical action per outbox entry. In a dark release
 * it exits before claiming any intent, which preserves every durable intent
 * for the later verified transport/release gate.
 */
export async function drainStudioOutbox(
  runtime: StudioRuntime,
  budget = createStudioDrainBudget(),
): Promise<number> {
  if (!canDeliverStudioMessages(runtime.environment)) return 0;
  // Do not start a token mint (or claim an intent) when there is no complete
  // bounded message window left in this Vercel invocation.
  if (!budget.canStart(OUTBOX_START_RESERVE_MS)) return 0;

  // Photon credential issuance is intentionally deferred until a durable
  // outbox drain. A failed mint happens before any physical message RPC, so
  // leave every intent untouched for the next bounded recovery attempt.
  let transport: StudioIMessageTransport;
  try {
    const resolved = await awaitWithinDrainBudget(
      () => getStudioDeliveryTransport(runtime, budget.transportOptions()),
      budget,
    );
    if (resolved.kind === "expired") return 0;
    transport = resolved.value;
  } catch {
    return 0;
  }
  if (!transport.verified) return 0;

  let delivered = 0;
  for (let index = 0; index < MAX_OUTBOX_PER_DRAIN; index += 1) {
    if (!budget.canStart(OUTBOX_PHYSICAL_ACTION_MARGIN_MS)) break;
    const claim = await runtime.store.claimNextOutbox();
    if (!claim) break;

    // A slow Redis claim may consume the last window. No Photon method has
    // been invoked, so return the intent to its normal retry-safe state.
    if (!budget.canStart(OUTBOX_PHYSICAL_ACTION_MARGIN_MS)) {
      try {
        await runtime.store.deferOutboxBeforePhoton(claim.record.outboxId, claim.leaseToken);
      } catch {
        // If this best-effort rollback cannot persist before the invocation
        // cutoff, the existing lease-expiry reconciliation stays
        // conservative rather than guessing a delivery outcome.
      }
      break;
    }

    let payload: StudioOutboxPayload;
    try {
      // The claim verifies that a payload exists, but its independent 24-hour
      // privacy TTL can still elapse (or a ciphertext can be found corrupt)
      // between that atomic check and local decryption. No physical transport
      // action has begun at this point, so terminalize it as a known failure
      // rather than letting a later lease-expiry reconciliation mislabel it as
      // an ambiguous send.
      payload = runtime.store.decryptOutboxPayload(claim.record);
    } catch {
      await runtime.store.finishOutbox(claim.record.outboxId, claim.leaseToken, "failed");
      continue;
    }
    const outcome = await dispatchOutboxEntry(
      runtime,
      transport,
      claim.record,
      claim.leaseToken,
      payload,
      budget,
    );
    // A dark/rollback guard can revoke a media acknowledgement after its
    // queue claim but before the physical iMessage send. The store has already
    // terminalized that intent as canceled, so there is nothing to finish.
    if (outcome === null) continue;
    if (outcome.kind === "deferred_before_photon") {
      try {
        await runtime.store.deferOutboxBeforePhoton(claim.record.outboxId, claim.leaseToken);
      } catch {
        // No Photon operation was started. If Redis cannot persist this
        // narrow rollback, leave the lease to the existing conservative
        // recovery path rather than guessing a delivery outcome.
      }
      break;
    }
    const finished = await runtime.store.finishOutbox(claim.record.outboxId, claim.leaseToken, outcome.kind);
    if (finished) delivered += 1;
    if (finished && claim.record.lane === "job-result" && claim.record.jobId !== undefined) {
      // This does not send anything. It records the durable attachment state
      // after every terminal/retryable outbox outcome so Cron can recover a
      // worker crash between a Photon call and its job transition.
      try {
        await reconcileStudioJobResultDelivery(runtime, claim.record.jobId);
      } catch {
        // The linked job remains in its own durable recovery queue. Never
        // alter a physical iMessage outcome because this bookkeeping read
        // failed after the outbox result was already committed.
      }
    }

    // An uncertain send pauses all further dispatch in this invocation. The
    // store additionally fences the affected lane; this conservative global
    // pause is safer than accidentally passing a dependent attachment.
    if (outcome.kind === "send_unknown") break;
  }
  return delivered;
}

async function processInbox(runtime: StudioRuntime, record: StudioInboxRecord): Promise<InboxDisposition> {
  // A record written by an older, unsequenced deployment cannot safely take
  // part in pending assembly. Terminalize it without side effects rather than
  // fabricating order from its wall-clock timestamp.
  if (!Number.isSafeInteger(record.scopeSequence) || record.scopeSequence < 1) {
    return "maintenance_rejected";
  }
  // The opaque receipt deliberately outlives its encrypted envelope. If the
  // immutable 24-hour privacy window elapsed before recovery, terminalize
  // safely rather than retaining or reconstructing a media reference.
  if (!record.encryptedEnvelope) return "failed";
  const envelope = runtime.store.decryptInboxEnvelope(record);
  // The onboarding cursor must never be advanced independently of the two
  // first-contact delivery intents. `ensureOnboardingBundle` writes the
  // settings, both ordered bubbles, and a non-expiring opaque marker in one
  // Redis transaction. A crash or a concurrent first receipt therefore
  // either retries the same bundle or observes the completed one—never a
  // settings-only "onboarded" user with a missing welcome message.
  const storedSettings = await runtime.store.getSettings<unknown>(record.senderHash);
  let settings = normalizeSettings(storedSettings ?? defaultSettings());
  if (storedSettings === null || settings.onboardingVersion < ONBOARDING_VERSION) {
    const candidate = Object.freeze({ ...settings, onboardingVersion: ONBOARDING_VERSION });
    const onboarding = await runtime.store.ensureOnboardingBundle({
      senderHash: record.senderHash,
      receiptId: record.receiptId,
      onboardingVersion: ONBOARDING_VERSION,
      settings: candidate,
      bubbles: [
        {
          outboxId: stableRecordId(record.receiptId, "onboarding-1"),
          payload: responsePayload(envelope, ONBOARDING_BUBBLES[0]),
        },
        {
          outboxId: stableRecordId(record.receiptId, "onboarding-2"),
          payload: responsePayload(envelope, ONBOARDING_BUBBLES[1]),
        },
      ],
    });
    if (onboarding.kind === "unavailable") {
      // Leave the encrypted receipt leased/recoverable. The transaction
      // either made no change or committed the whole idempotent bundle, so a
      // later retry cannot create a partial first-contact sequence.
      throw new Error("studio_onboarding_bundle_unavailable");
    }
    if (onboarding.kind === "created") {
      settings = candidate;
    } else {
      // A concurrent receipt may have won enrollment. Read its committed
      // settings rather than reusing this receipt's local defaults. A marker
      // intentionally outlives settings TTL; falling back to `candidate` in
      // that case still skips duplicate onboarding safely.
      settings = normalizeSettings((await runtime.store.getSettings<unknown>(record.senderHash)) ?? candidate);
    }
  }

  // Fence pending assembly before branching on message shape. A dark/rollback
  // receipt cancels any older intent and prevents a stale active-phase worker
  // from recreating it after this receipt has completed.
  if (!canAcceptStudioJobs(runtime.environment)) {
    // `/cancel` has one exception: when a pre-submission job is active it
    // targets that job and must leave an unrelated pending media reference
    // untouched. Do this before the rollback maintenance fence can clear it.
    if (envelope.text !== null && envelope.attachments.length === 0) {
      const parsed = parseStudioCommand(envelope.text);
      if (parsed.kind === "command" && parsed.command.kind === "cancel") {
        const activeJob = await runtime.store.getActiveJob(record.senderHash);
        if (activeJob) return handleCancelCommand(runtime, record, envelope, activeJob);
      }
    }
    const pendingFence = await runtime.store.blockPendingForMaintenance(record);
    if (pendingFence.kind === "canceled") {
      await enqueueResponse(
        runtime,
        record,
        envelope,
        "pending-canceled-maintenance",
        "Air is in maintenance mode; I canceled the pending request. No generation was submitted.",
      );
      return "maintenance_rejected";
    }
  }

  if (envelope.attachments.length > 1) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      "multiple-inputs",
      "Send exactly one image or video with a creation command. Air does not choose between multiple inputs.",
    );
    return "processed";
  }

  const attachment = envelope.attachments[0];
  if (envelope.text === null) {
    if (attachment) {
      // A dark/rollback release must not carry a media-first intent into a
      // later active phase. The maintenance fence above makes a stale write
      // impossible before this truthful response is persisted.
      if (!canAcceptStudioJobs(runtime.environment)) {
        await enqueueResponse(
          runtime,
          record,
          envelope,
          "media-first-maintenance",
          "Air is in maintenance mode; I didn’t retain that attachment. No generation was submitted.",
        );
        return "maintenance_rejected";
      }
      const pendingWrite = await runtime.store.putPendingWithGuard<PendingIntent>(record, Object.freeze({
        schemaVersion: 1,
        kind: "media",
        attachment,
        createdAtMs: record.receivedAtMs,
      }));
      if (pendingWrite.kind !== "active") {
        const superseded = pendingWrite.kind === "superseded";
        const maintenance = pendingWrite.kind === "maintenance";
        await enqueueResponse(
          runtime,
          record,
          envelope,
          superseded ? "media-first-superseded" : "media-first-inactive",
          superseded
            ? "A newer attachment replaced that pending request. Send it again with an explicit command if you still want to use it."
            : "That pending attachment is no longer active. Send it again with an explicit command.",
        );
        return maintenance ? "maintenance_rejected" : "processed";
      }
      const acknowledgement = await enqueuePendingMediaAcknowledgement(
        runtime,
        record,
        envelope,
        pendingWrite.guard,
        "I saved that one attachment for 10 minutes. Send an explicit command such as /animate a slow orbit, /edit-image brighten the sky, or /edit-video trim the opening.",
      );
      if (acknowledgement.kind !== "persisted") {
        const superseded = acknowledgement.kind === "superseded";
        const maintenance = acknowledgement.kind === "maintenance";
        await enqueueResponse(
          runtime,
          record,
          envelope,
          superseded ? "media-first-superseded" : "media-first-inactive",
          superseded
            ? "A newer attachment replaced that pending request. Send it again with an explicit command if you still want to use it."
            : "That pending attachment is no longer active. Send it again with an explicit command.",
        );
        return maintenance ? "maintenance_rejected" : "processed";
      }
      return "processed";
    }
    await enqueueResponse(runtime, record, envelope, "empty-message", HELP_TEXT);
    return "processed";
  }

  const parsed = parseStudioCommand(envelope.text);
  if (parsed.kind === "help") {
    await enqueueResponse(runtime, record, envelope, `help-${parsed.reason}`, HELP_TEXT);
    return "processed";
  }
  if (parsed.kind === "invalid") {
    await enqueueResponse(runtime, record, envelope, `invalid-${parsed.error.code}`, `${parsed.error.message}\n${HELP_TEXT}`);
    return "processed";
  }
  if (parsed.kind === "bare-text") {
    return handleBareText(runtime, record, envelope, settings, attachment, parsed.text);
  }

  return handleCommand(runtime, record, envelope, settings, attachment, parsed.command);
}

async function handleBareText(
  runtime: StudioRuntime,
  record: InboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  _settings: StudioUserSettings,
  attachment: StudioAttachmentReference | undefined,
  text: string,
): Promise<InboxDisposition> {
  if (attachment) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      "bare-text-with-media",
      "Use an explicit command with one attachment, for example /imagine a watercolor version or /animate a gentle camera move.",
    );
    return "processed";
  }
  const pending = await runtime.store.getPending<PendingIntent>(record.senderHash, record.threadHash);
  if (pending?.kind !== "awaiting_prompt") {
    await enqueueResponse(runtime, record, envelope, "bare-text-help", HELP_TEXT);
    return "processed";
  }

  // A pending prompt is only actionable after a separately approved release
  // enables admissions. In dark mode do not turn a follow-up bubble into a
  // paid job or silently preserve an intent that looks accepted.
  if (!canAcceptStudioJobs(runtime.environment)) {
    await runtime.store.clearPending(record);
    await enqueueMaintenanceNotice(runtime, record, envelope, "pending-prompt-maintenance");
    return "maintenance_rejected";
  }

  await enqueueResponse(
    runtime,
    record,
    envelope,
    "pending-prompt-unavailable",
    "Air cannot submit generation work until a verified model and paid-release approval are in place.",
  );
  return "processed";
}

async function handleCommand(
  runtime: StudioRuntime,
  record: InboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  settings: StudioUserSettings,
  attachment: StudioAttachmentReference | undefined,
  command: StudioCommand,
): Promise<InboxDisposition> {
  switch (command.kind) {
    case "help":
      await enqueueResponse(runtime, record, envelope, "help", HELP_TEXT);
      return "processed";
    case "privacy":
      await enqueueResponse(runtime, record, envelope, "privacy", privacyText(studioRegistry(runtime)));
      return "processed";
    case "status":
      {
        const activeJob = await runtime.store.getActiveJob(record.senderHash);
        const recentTerminalJob = activeJob === null
          ? await runtime.store.getMostRecentTerminalJob(record.senderHash)
          : null;
        const text = activeJob
          ? activeJobStatusText(activeJob)
          : recentTerminalJob
            ? recentTerminalJobStatusText(recentTerminalJob)
            : "No active or recent jobs are available in this deployment. Use /help for the command reference.";
        await enqueueResponse(
          runtime,
          record,
          envelope,
          "status",
          text,
        );
        return "processed";
      }
    case "cancel": {
      return handleCancelCommand(runtime, record, envelope);
    }
    case "models":
      await enqueueResponse(
        runtime,
        record,
        envelope,
        `models-${command.mode ?? "all"}`,
        modelsText(settings, command.mode, studioRegistry(runtime)),
      );
      return "processed";
    case "model-settings":
      return handleModelSettings(runtime, record, envelope, settings, command);
    case "create":
      return handleCreation(runtime, record, envelope, settings, attachment, command.mode, command.prompt);
  }
  // `parseStudioCommand` supplies a closed union, but keep the ingress worker
  // fail-closed if a future command reaches this dispatch without an explicit
  // durable handler. Silently completing its inbox receipt would lose a user
  // request and make its response non-recoverable.
  throw new Error("unknown_studio_command");
}

/**
 * Cancels a durable job only before the executor wins `admitted → executing`.
 * The store commits the terminal transition and the cancellation outbox intent
 * together. Once execution has started, reporting a confirmed cancellation
 * would risk lying about a provider submission that may already exist.
 */
async function handleCancelCommand(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  activeJobHint?: StudioJobRecord,
): Promise<InboxDisposition> {
  const cancellationOutboxId = stableRecordId(record.receiptId, "cancel-active-job");
  const existingCancellation = await runtime.store.getOutbox(cancellationOutboxId);
  if (
    existingCancellation?.outboxId === cancellationOutboxId
    && existingCancellation.lane === "request-response"
    && existingCancellation.jobId !== undefined
  ) return "processed";

  let activeJob = activeJobHint ?? await runtime.store.getActiveJob(record.senderHash);
  for (let attempt = 0; activeJob?.state === "admitted" && attempt < 2; attempt += 1) {
    const canceled = await runtime.store.cancelAdmittedJobWithNotification({
      job: activeJob,
      notification: Object.freeze({
        outboxId: cancellationOutboxId,
        payload: responsePayload(envelope, "Canceled your queued request. No generation was submitted."),
      }),
    });
    if (canceled === "canceled") return "processed";
    if (canceled === "release_blocked" || canceled === "unavailable") {
      await enqueueResponse(
        runtime,
        record,
        envelope,
        "cancel-active-unavailable",
        "I couldn’t safely cancel that request. Send /status and try /cancel again in this conversation.",
      );
      return "processed";
    }
    activeJob = await runtime.store.getActiveJob(record.senderHash);
  }

  if (activeJob) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      "cancel-active-started",
      "Your request has already started, so I can’t confirm a cancellation. I’ll send a truthful update here when its outcome is known.",
    );
    return "processed";
  }

  const canceledPending = await runtime.store.clearPending(record);
  if (canceledPending.kind === "canceled") {
    await enqueueResponse(runtime, record, envelope, "cancel-pending", "Canceled the pending request. No generation was submitted.");
  } else if (canceledPending.kind === "stale") {
    await enqueueResponse(runtime, record, envelope, "cancel-stale", "That cancellation command is no longer current. Send /cancel again to cancel the latest pending request.");
  } else {
    await enqueueResponse(runtime, record, envelope, "cancel-none", "There is no pending request to cancel.");
  }
  return "processed";
}

async function handleCreation(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  settings: StudioUserSettings,
  attachment: StudioAttachmentReference | undefined,
  mode: StudioMode,
  prompt: string | null,
): Promise<InboxDisposition> {
  // This is the explicit Section 16 rollback/dark gate. It is deliberately
  // checked before pending assembly, model selection, reservation, or any
  // provider code so a dark release cannot create a later paid surprise.
  if (!canAcceptStudioJobs(runtime.environment)) {
    await enqueueMaintenanceNotice(runtime, record, envelope, `maintenance-${mode}`);
    return "maintenance_rejected";
  }

  const registry = studioRegistry(runtime);
  // Never reserve spend or create an executable job from a registry whose
  // configured release version does not exactly match the committed data.
  // This remains a user-safe, non-provider response rather than revealing
  // configuration details or attempting a best-effort fallback.
  if (!validateRegistryVersion(runtime.environment.modelRegistryVersion, registry).ok) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      `registry-unavailable-${mode}`,
      "Air’s verified model configuration is unavailable right now. No generation was submitted.",
    );
    return "processed";
  }
  const selection = selectedModel(settings, mode, registry);
  if (!selection.model) {
    await enqueueResponse(runtime, record, envelope, `missing-model-${mode}`, noModelText(mode));
    return "processed";
  }
  const media = attachment ? mediaKindFromAttachment(attachment) : "none";
  const durationSeconds = selection.model.output.type === "video"
    ? settings.modes[mode].durationSeconds ?? DEFAULT_VIDEO_DURATION_SECONDS
    : null;
  const compatibility = checkModelCompatibility(selection.model, {
    mode,
    count: settings.modes[mode].outputCount,
    durationSeconds,
    hasPrompt: prompt !== null,
    media,
  });
  if (!compatibility.ok) {
    await enqueueResponse(runtime, record, envelope, `incompatible-${mode}-${compatibility.code}`, compatibilityText(mode, compatibility.code));
    return "processed";
  }

  // Attachment bytes are still deliberately outside this admission slice:
  // before accepting them, the provider adapter needs a reviewed fetch/upload
  // implementation with MIME/size validation and retention evidence. Never
  // reserve paid work that we cannot submit exactly once.
  if (attachment !== undefined) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      `media-input-not-enabled-${mode}`,
      "Media-backed generation is not enabled in this deployment. No generation was submitted.",
    );
    return "processed";
  }

  // The committed workflow declaration protects future registry changes from
  // accidentally introducing a multi-step paid plan. The deployed runtime
  // keeps this false through both the dark policy and the unprovisioned
  // provider port; a reviewed adapter must be injected explicitly.
  if (
    !isSinglePaidStepWorkflow(getStudioWorkflow(mode))
    || !canSubmitPaidStudioProvider(runtime.environment)
    || !runtime.provider.verified
  ) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      `provider-disabled-${mode}`,
      "Air has no verified paid execution path enabled in this deployment. No generation was submitted.",
    );
    return "processed";
  }

  const workflow = getStudioWorkflow(mode);
  const outputCount = settings.modes[mode].outputCount;
  const releaseStamp = Object.freeze({
    generation: runtime.environment.releaseGeneration,
    phase: runtime.environment.releasePhase,
  } as const);
  const execution: StudioExecutionSnapshot = Object.freeze({
    schemaVersion: 2,
    workflow: Object.freeze({
      workflowId: workflow.workflowId,
      workflowVersion: workflow.version,
      stepId: "provider-generation",
    }),
    model: Object.freeze({
      slug: selection.model.slug,
      provider: selection.model.provider,
      providerModelId: selection.model.providerModelId,
      adapterKey: selection.model.providerBinding.adapterKey,
      catalogFingerprint: selection.model.verification.catalogFingerprint,
      requestSchemaFingerprint: selection.model.providerBinding.requestSchemaFingerprint,
      resultSchemaFingerprint: selection.model.providerBinding.resultSchemaFingerprint,
      pricingEvidenceFingerprint: selection.model.pricing.evidenceFingerprint,
    }),
    output: Object.freeze({
      type: selection.model.output.type,
      count: outputCount,
      ...(durationSeconds === null ? {} : { durationSeconds }),
    }),
    maximumQuotedCostCents: compatibility.maximumQuotedCost.maximumQuotedCostCents,
    polling: Object.freeze({ ...selection.model.polling }),
    releaseStamp,
  });
  const admittedJob = createStudioJob({
    jobId: stableJobId(record.receiptId),
    senderHash: record.senderHash,
    threadHash: record.threadHash,
    mode,
    modelSlug: selection.model.slug,
    provider: selection.model.provider,
    outputCount,
    ...(durationSeconds === null ? {} : { durationSeconds }),
    reservedCents: compatibility.maximumQuotedCost.maximumQuotedCostCents,
    releaseStamp,
    payload: Object.freeze({
      prompt,
      chatGuid: envelope.chatGuid,
      sendingLine: envelope.sendingLine,
      ...(envelope.replyTargetId === undefined ? {} : { replyTargetId: envelope.replyTargetId }),
      execution,
    }),
    cipher: runtime.cipher,
    now: record.receivedAtMs,
  });
  const acknowledgementText = "✨ Working my magic — I’ll send it here when it’s ready.";
  const acknowledgement = Object.freeze({
    outboxId: stableRecordId(record.receiptId, "job-acknowledgement"),
    payload: responsePayload(envelope, acknowledgementText),
  });
  const tapback = envelope.replyTargetId === undefined
    ? undefined
    : Object.freeze({
      outboxId: stableRecordId(record.receiptId, "job-tapback"),
      payload: Object.freeze({
        kind: "tapback" as const,
        thread: Object.freeze({
          chatGuid: envelope.chatGuid,
          sendingLine: envelope.sendingLine,
          replyTargetId: envelope.replyTargetId,
        }),
        targetMessageId: envelope.replyTargetId,
      }),
    });
  const canaryBudget = canaryAdmissionBudget(runtime);
  const admission = await runtime.store.admitJobWithInitialNotifications({
    job: admittedJob,
    activeJobLimit: runtime.environment.activeJobLimit,
    dailyUserJobLimit: runtime.environment.dailyUserJobLimit,
    perJobCapCents: runtime.environment.spendCaps.perJobCents,
    dailySpendCapCents: runtime.environment.spendCaps.dailyCents,
    ...(canaryBudget === undefined ? {} : { canaryBudget }),
    acknowledgement,
    ...(tapback === undefined ? {} : { tapback }),
  });
  if (admission.admitted) return "processed";
  switch (admission.reason) {
    case "active_job":
      await enqueueResponse(runtime, record, envelope, `active-job-${mode}`, "You already have a request in progress. I’ll send it here when it’s ready.");
      return "processed";
    case "user_limit":
      await enqueueResponse(runtime, record, envelope, `user-limit-${mode}`, "You’ve reached today’s request limit. No generation was submitted.");
      return "processed";
    case "per_job_cap":
    case "daily_cap":
      await enqueueResponse(runtime, record, envelope, `spend-cap-${mode}`, "This request exceeds the current studio spend limit. No generation was submitted.");
      return "processed";
    case "canary_total_cap":
      await enqueueResponse(runtime, record, envelope, `canary-total-cap-${mode}`, "Air’s shared launch budget is fully reserved. No generation was submitted.");
      return "processed";
    case "canary_fal_cap":
      await enqueueResponse(runtime, record, envelope, `canary-fal-cap-${mode}`, "Air’s launch allocation for this provider is fully reserved. No generation was submitted.");
      return "processed";
    case "canary_budget_required":
      await enqueueResponse(runtime, record, envelope, `canary-budget-required-${mode}`, "Air’s launch budget controls are unavailable right now. No generation was submitted.");
      return "processed";
    case "release_blocked":
      // A fence change means this worker is stale. Creating a second generic
      // response could cross the same rollback boundary, so leave no effect.
      return "maintenance_rejected";
    case "storage_error":
      // Preserve the encrypted receipt and let its bounded lease recovery
      // retry the atomic boundary. A partial acknowledgement/job bundle is
      // impossible, so a transient store failure cannot spend or send.
      throw new Error("studio_atomic_admission_unavailable");
  }
}

/** Pass the opaque budget policy only at the canary admission boundary. */
function canaryAdmissionBudget(runtime: StudioRuntime): StudioCanaryBudgetPolicy | undefined {
  const environment = runtime.environment;
  if (environment.releasePhase !== "canary" || environment.canaryBudgetId === undefined) return undefined;
  return Object.freeze({
    budgetId: environment.canaryBudgetId,
    totalCapCents: environment.spendCaps.canaryTotalCents,
    falCapCents: environment.spendCaps.canaryFalCents,
  });
}

async function handleModelSettings(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  settings: StudioUserSettings,
  command: Extract<StudioCommand, { readonly kind: "model-settings" }>,
): Promise<InboxDisposition> {
  if (command.action === "show-all") {
    await enqueueResponse(runtime, record, envelope, "settings-all", allSettingsText(settings, studioRegistry(runtime)));
    return "processed";
  }
  if (command.action === "show-mode") {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      `settings-${command.mode}`,
      modeSettingsText(settings, command.mode, studioRegistry(runtime)),
    );
    return "processed";
  }
  if (command.action === "reset") {
    const next = resetSettings(settings, command.target);
    await runtime.store.putSettings(record.senderHash, next);
    const message = command.target === "all"
      ? "Reset all model settings to the committed defaults. " + allSettingsText(next, studioRegistry(runtime))
      : `Reset ${command.target} to its committed default. ${modeSettingsText(next, command.target, studioRegistry(runtime))}`;
    await enqueueResponse(runtime, record, envelope, `settings-reset-${command.target}`, message);
    return "processed";
  }

  const current = settings.modes[command.mode];
  if (command.action === "select-model") {
    const resolved = resolveEnabledModel(studioRegistry(runtime), command.mode, command.modelSlug);
    if (!resolved.ok) {
      await enqueueResponse(runtime, record, envelope, `settings-model-${command.mode}`, noModelText(command.mode));
      return "processed";
    }
    if (!modelSupportsStoredValues(resolved.model, current)) {
      await enqueueResponse(
        runtime,
        record,
        envelope,
        `settings-model-incompatible-${command.mode}`,
        supportedValuesText(resolved.model),
      );
      return "processed";
    }
    const next = replaceModeSettings(settings, command.mode, Object.freeze({ ...current, modelSlug: resolved.model.slug }));
    await runtime.store.putSettings(record.senderHash, next);
    await enqueueResponse(
      runtime,
      record,
      envelope,
      `settings-model-saved-${command.mode}`,
      `Selected ${resolved.model.displayName}. ${modeSettingsText(next, command.mode, studioRegistry(runtime))}`,
    );
    return "processed";
  }

  const selected = selectedModel(settings, command.mode, studioRegistry(runtime)).model;
  if (!selected) {
    await enqueueResponse(runtime, record, envelope, `settings-no-model-${command.mode}`, noModelText(command.mode));
    return "processed";
  }
  if (command.action === "set-count") {
    if (!selected.output.supportedOutputCounts.includes(command.count)) {
      await enqueueResponse(runtime, record, envelope, `settings-count-invalid-${command.mode}`, supportedValuesText(selected));
      return "processed";
    }
    const next = replaceModeSettings(settings, command.mode, Object.freeze({ ...current, outputCount: command.count }));
    await runtime.store.putSettings(record.senderHash, next);
    await enqueueResponse(runtime, record, envelope, `settings-count-saved-${command.mode}`, `Set ${command.mode} output count to ${command.count}.`);
    return "processed";
  }

  if (selected.output.type !== "video" || !selected.output.supportedDurationSeconds.includes(command.durationSeconds)) {
    await enqueueResponse(runtime, record, envelope, `settings-duration-invalid-${command.mode}`, supportedValuesText(selected));
    return "processed";
  }
  const next = replaceModeSettings(settings, command.mode, Object.freeze({ ...current, durationSeconds: command.durationSeconds }));
  await runtime.store.putSettings(record.senderHash, next);
  await enqueueResponse(runtime, record, envelope, `settings-duration-saved-${command.mode}`, `Set ${command.mode} duration to ${command.durationSeconds} seconds.`);
  return "processed";
}

async function loadSettings(runtime: StudioRuntime, senderHash: StudioInboxRecord["senderHash"]): Promise<StudioUserSettings> {
  const stored = await runtime.store.getSettings<unknown>(senderHash);
  return normalizeSettings(stored);
}

function normalizeSettings(value: unknown): StudioUserSettings {
  const fallback = defaultSettings();
  if (!value || typeof value !== "object" || Array.isArray(value)) return fallback;
  const source = value as { schemaVersion?: unknown; onboardingVersion?: unknown; modes?: unknown };
  if (source.schemaVersion !== 1 || !source.modes || typeof source.modes !== "object" || Array.isArray(source.modes)) {
    return fallback;
  }
  const modeSource = source.modes as Record<string, unknown>;
  return Object.freeze({
    schemaVersion: 1,
    onboardingVersion: safeOnboardingVersion(source.onboardingVersion),
    modes: Object.freeze({
      imagine: normalizeModeSettings(modeSource.imagine, false),
      animate: normalizeModeSettings(modeSource.animate, true),
      "edit-image": normalizeModeSettings(modeSource["edit-image"], false),
      "edit-video": normalizeModeSettings(modeSource["edit-video"], true),
    }),
  });
}

function defaultSettings(onboardingVersion = 0): StudioUserSettings {
  return Object.freeze({
    schemaVersion: 1,
    onboardingVersion,
    modes: Object.freeze({
      imagine: Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT }),
      animate: Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT, durationSeconds: DEFAULT_VIDEO_DURATION_SECONDS }),
      "edit-image": Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT }),
      "edit-video": Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT, durationSeconds: DEFAULT_VIDEO_DURATION_SECONDS }),
    }),
  });
}

function normalizeModeSettings(value: unknown, video: boolean): StoredModeSettings {
  const fallback = video
    ? { outputCount: DEFAULT_OUTPUT_COUNT, durationSeconds: DEFAULT_VIDEO_DURATION_SECONDS }
    : { outputCount: DEFAULT_OUTPUT_COUNT };
  if (!value || typeof value !== "object" || Array.isArray(value)) return Object.freeze(fallback);
  const source = value as { modelSlug?: unknown; outputCount?: unknown; durationSeconds?: unknown };
  const modelSlug = typeof source.modelSlug === "string" && /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(source.modelSlug)
    ? source.modelSlug
    : undefined;
  const outputCount = isOutputCount(source.outputCount) ? source.outputCount : DEFAULT_OUTPUT_COUNT;
  const durationSeconds = video && isVideoDuration(source.durationSeconds)
    ? source.durationSeconds
    : DEFAULT_VIDEO_DURATION_SECONDS;
  return Object.freeze({
    ...(modelSlug === undefined ? {} : { modelSlug }),
    outputCount,
    ...(video ? { durationSeconds } : {}),
  });
}

function safeOnboardingVersion(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= 100
    ? value
    : 0;
}

function isOutputCount(value: unknown): value is StudioOutputCount {
  return value === 1 || value === 2 || value === 3 || value === 4;
}

function isVideoDuration(value: unknown): value is StudioVideoDurationSeconds {
  return value === 5 || value === 10 || value === 15;
}

function resetSettings(settings: StudioUserSettings, target: StudioMode | "all"): StudioUserSettings {
  const defaults = defaultSettings();
  return Object.freeze({
    ...defaults,
    onboardingVersion: settings.onboardingVersion,
    modes: target === "all"
      ? defaults.modes
      : Object.freeze({ ...settings.modes, [target]: defaults.modes[target] }),
  });
}

function replaceModeSettings(settings: StudioUserSettings, mode: StudioMode, next: StoredModeSettings): StudioUserSettings {
  return Object.freeze({ ...settings, modes: Object.freeze({ ...settings.modes, [mode]: next }) });
}

function studioRegistry(runtime: StudioRuntime): AirModelRegistry {
  return runtime.registry ?? AIR_MODEL_REGISTRY;
}

function selectedModel(
  settings: StudioUserSettings,
  mode: StudioMode,
  registry: AirModelRegistry = AIR_MODEL_REGISTRY,
): { readonly model?: StudioModel | undefined } {
  const explicit = settings.modes[mode].modelSlug;
  if (explicit !== undefined) {
    const resolved = resolveEnabledModel(registry, mode, explicit);
    return resolved.ok ? { model: resolved.model } : {};
  }
  const fallback = resolveDefaultModel(registry, mode);
  return fallback.ok ? { model: fallback.model } : {};
}

function modelSupportsStoredValues(model: StudioModel, settings: StoredModeSettings): boolean {
  if (!model.output.supportedOutputCounts.includes(settings.outputCount)) return false;
  if (model.output.type !== "video") return settings.durationSeconds === undefined;
  return settings.durationSeconds !== undefined && model.output.supportedDurationSeconds.includes(settings.durationSeconds);
}

/**
 * A model that cannot honor the caller's already-saved count/duration is not
 * selectable without an explicit settings change, so do not advertise it as
 * compatible in `/models`. This preserves the no-implicit-reset contract.
 */
function compatibleModelsForSettings(
  settings: StudioUserSettings,
  mode: StudioMode,
  registry: AirModelRegistry = AIR_MODEL_REGISTRY,
): readonly StudioModel[] {
  return listEnabledModels(registry, mode).filter((model) => modelSupportsStoredValues(model, settings.modes[mode]));
}

function modelsText(
  settings: StudioUserSettings,
  mode: StudioMode | null,
  registry: AirModelRegistry = AIR_MODEL_REGISTRY,
): string {
  const modes = mode === null ? (['imagine', 'animate', 'edit-image', 'edit-video'] as const) : [mode];
  const models = modes.flatMap((candidateMode) => compatibleModelsForSettings(settings, candidateMode, registry));
  if (models.length === 0) {
    const enabled = listEnabledModels(registry, mode);
    if (enabled.length > 0) {
      if (mode !== null) {
        return `No enabled ${mode} model is compatible with your saved settings. Use /model-settings ${mode} to review the exact supported values.`;
      }
      const enabledModes = modes.filter((candidateMode) => listEnabledModels(registry, candidateMode).length > 0);
      const commands = enabledModes.map((candidateMode) => `/model-settings ${candidateMode}`);
      return `No enabled model is compatible with your saved settings. Use ${commands.join(", ")} to review the exact supported values.`;
    }
    return mode === null
      ? "No verified models are enabled in this deployment. Candidate names are not selectable until catalog, schema, price, and privacy review complete."
      : noModelText(mode);
  }
  return models.map((model) => `${model.slug} — ${model.displayName}`).join("\n");
}

function noModelText(mode: StudioMode): string {
  return `No verified ${mode} model is enabled yet. /models lists only reviewed models; Air will not substitute a different model.`;
}

function allSettingsText(settings: StudioUserSettings, registry: AirModelRegistry = AIR_MODEL_REGISTRY): string {
  return (["imagine", "animate", "edit-image", "edit-video"] as const)
    .map((mode) => modeSettingsText(settings, mode, registry))
    .join("\n");
}

function modeSettingsText(
  settings: StudioUserSettings,
  mode: StudioMode,
  registry: AirModelRegistry = AIR_MODEL_REGISTRY,
): string {
  const values = settings.modes[mode];
  const selected = selectedModel(settings, mode, registry).model;
  const model = selected ? selected.displayName : "No verified model selected";
  const duration = mode === "animate" || mode === "edit-video"
    ? `, duration ${values.durationSeconds ?? DEFAULT_VIDEO_DURATION_SECONDS}s`
    : "";
  const aspectRatio = selected ? `, aspect ${selected.output.lockedAspectRatio}` : "";
  const enabled = listEnabledModels(registry, mode);
  const compatible = compatibleModelsForSettings(settings, mode, registry);
  const choiceText = compatible.length > 0
    ? ` Compatible models: ${compatible.map(modelChoiceSummary).join(" • ")}.`
    : enabled.length > 0
      ? ` No enabled model is compatible with the saved count/duration. Use the exact values shown by /model-settings ${mode}.`
      : " No verified models enabled.";
  return `${mode}: ${model}; count ${values.outputCount}${duration}${aspectRatio}. ${choiceText}\n${settingsExamplesText(settings, mode, compatible)}`;
}

function modelChoiceSummary(model: StudioModel): string {
  const duration = model.output.type === "video"
    ? `; durations ${model.output.supportedDurationSeconds.join(", ")}s`
    : "";
  return `${model.slug} (counts ${model.output.supportedOutputCounts.join(", ")}${duration}; aspect ${model.output.lockedAspectRatio})`;
}

/**
 * Keep examples literal and directly parsable. They use an already-compatible
 * model slug, never a placeholder or a model which would trigger a hidden
 * count/duration reset.
 */
function settingsExamplesText(
  settings: StudioUserSettings,
  mode: StudioMode,
  compatible: readonly StudioModel[],
): string {
  const values = settings.modes[mode];
  const examples: string[] = [];
  const model = compatible[0];
  if (model !== undefined) examples.push(`/model-settings ${mode} ${model.slug}`);
  examples.push(`/model-settings ${mode} count ${values.outputCount}`);
  if (mode === "animate" || mode === "edit-video") {
    examples.push(`/model-settings ${mode} duration ${values.durationSeconds ?? DEFAULT_VIDEO_DURATION_SECONDS}`);
  }
  examples.push(`/model-settings reset ${mode}`);
  return `Examples: ${examples.join(" • ")}`;
}

function supportedValuesText(model: StudioModel): string {
  const counts = model.output.supportedOutputCounts.join(", ");
  const durations = model.output.type === "video"
    ? ` Durations: ${model.output.supportedDurationSeconds.join(", ")} seconds.`
    : " Duration is not available for this image model.";
  return `${model.displayName} supports output counts: ${counts}.${durations}`;
}

function compatibilityText(mode: StudioMode, code: string): string {
  switch (code) {
    case "prompt_required":
      return `/${mode} needs a prompt for the selected model.`;
    case "media_required":
      return `/${mode} needs one compatible attachment for the selected model.`;
    case "image_not_accepted":
    case "video_not_accepted":
      return `That attachment type is not supported by the selected ${mode} model.`;
    case "unsupported_count":
    case "duration_not_supported":
    case "duration_required":
      return `Your saved ${mode} settings are not compatible with the selected model. Use /model-settings ${mode}.`;
    default:
      return `That ${mode} request is not compatible with the selected model. No generation was submitted.`;
  }
}

function mediaKindFromAttachment(attachment: StudioAttachmentReference): "none" | "image" | "video" {
  const mime = attachment.mimeType?.toLowerCase() ?? "";
  if (mime.startsWith("image/")) return "image";
  if (mime.startsWith("video/")) return "video";
  // Unknown declared types are never treated as a compatible media input.
  return "none";
}

/** Never expose a provider ID, model ID, cost, or transport claim through `/status`. */
function activeJobStatusText(job: StudioJobRecord): string {
  switch (job.state) {
    case "admitted":
      return "Your request is queued. I’ll send it here when it’s ready.";
    case "executing":
      return "Your request is being generated. I’ll send it here when its outcome is known.";
    case "ready":
      return "Your result is ready and awaiting delivery.";
    case "delivering":
      return "Your result is being delivered.";
    case "reconciliation_required":
      return "Your request needs a manual safety check. Air will not submit or send it again automatically.";
    default:
      return "Your request is changing state. Send /status again shortly for a truthful update.";
  }
}

/** `/status` may report a caller's terminal job, but never provider internals. */
function recentTerminalJobStatusText(job: StudioJobRecord): string {
  switch (job.state) {
    case "delivered":
      return "Your most recent request was delivered.";
    case "failed":
      return "Your most recent request ended without a delivered result.";
    case "canceled":
      return "Your most recent request was canceled.";
    case "maintenance_rejected":
      return "Your most recent request was not accepted. No generation was submitted.";
    default:
      // `getMostRecentTerminalJob` filters the state, but do not make a
      // stale/corrupt record look like a known terminal outcome.
      return "Your most recent request is changing state. Send /status again shortly for a truthful update.";
  }
}

function privacyText(registry: AirModelRegistry = AIR_MODEL_REGISTRY): string {
  const enabled = listEnabledModels(registry);
  if (enabled.length === 0) {
    return "Air is private and allowlist-only. This deployment accepts no generation work and is not sending media to a generation provider. Air keeps its own accepted-work media references for up to 24 hours; any later approved path must disclose provider retention and hosted-public input terms. Avoid sending sensitive media.";
  }

  const hostedPublic = enabled.filter((model) => model.inputHosting.mode === "hosted-public");
  const common = "Air is private and allowlist-only. Air keeps its own accepted-work media references for up to 24 hours. Providers process media under their own retention terms; Air does not promise provider-side deletion.";
  if (hostedPublic.length === 0) {
    return `${common} The enabled models do not use a stable publicly accessible provider-hosted input path. Avoid sending sensitive media.`;
  }

  const paths = hostedPublic.map((model) => {
    if (model.inputHosting.mode !== "hosted-public") return "";
    return `${model.slug}: ${model.inputHosting.host}${model.inputHosting.pathPrefix}`;
  }).filter((path) => path.length > 0);
  return `${common} The following enabled models can place input media at a provider-hosted publicly accessible path during processing: ${paths.join("; ")}. Do not send sensitive media.`;
}

async function enqueueMaintenanceNotice(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  label: string,
): Promise<void> {
  await enqueueResponse(
    runtime,
    record,
    envelope,
    label,
    "Air is in maintenance mode: new generation requests are not being accepted. No generation was submitted.",
  );
}

async function enqueueResponse(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  label: string,
  text: string,
): Promise<void> {
  await enqueueText(runtime, record, envelope, `response-${label}`, "request-response", 1, text);
}

/**
 * The acknowledgement for a media-first message is the sole response that
 * asserts retained pending state. It is therefore coupled to the opaque
 * pending guard instead of a generic durable response.
 */
async function enqueuePendingMediaAcknowledgement(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  pendingGuard: PendingWriteGuard,
  text: string,
): ReturnType<StudioRuntime["store"]["createPendingGuardedOutbox"]> {
  if (!isSafeTransportText(text)) throw new Error("unsafe studio response text");
  const payload = responsePayload(envelope, text);
  return runtime.store.createPendingGuardedOutbox(pendingGuard, {
    outboxId: stableRecordId(record.receiptId, "response-media-first"),
    senderHash: record.senderHash,
    lane: "request-response",
    sequence: 1,
    state: "intent",
    attempts: 0,
    dueAtMs: record.receivedAtMs + MEDIA_FIRST_CORRECTION_DELAY_MS,
    payload,
  });
}

async function enqueueText(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  label: string,
  lane: StudioOutboxRecord["lane"],
  sequence: number,
  text: string,
): Promise<void> {
  if (!isSafeTransportText(text)) throw new Error("unsafe studio response text");
  const payload = responsePayload(envelope, text);
  await runtime.store.createOutbox({
    outboxId: stableRecordId(record.receiptId, label),
    senderHash: record.senderHash,
    lane,
    sequence,
    state: "intent",
    attempts: 0,
    payload,
  });
}

function responsePayload(
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  text: string,
): StudioOutboxPayload {
  return Object.freeze({
    kind: "text",
    thread: Object.freeze({
      chatGuid: envelope.chatGuid,
      sendingLine: envelope.sendingLine,
      ...(envelope.replyTargetId === undefined ? {} : { replyTargetId: envelope.replyTargetId }),
    }),
    text,
  });
}

function stableRecordId(receiptId: string, purpose: string): string {
  return createHash("sha256").update(`air:v2:outbox:${receiptId}:${purpose}`, "utf8").digest("hex");
}

/** Separate opaque namespace so a job can never collide with an outbox ID. */
function stableJobId(receiptId: string): string {
  return createHash("sha256").update(`air:v2:job:${receiptId}`, "utf8").digest("hex");
}

type DrainDeadlineResult<T> =
  | { readonly kind: "completed"; readonly value: T }
  | { readonly kind: "expired" };

/**
 * A strictly local worker result: shared-Cron time expired before any Photon
 * RPC started, so its claim may be safely returned without consuming a
 * physical-send attempt. This is not a transport outcome and is never used
 * for a real retryable Photon failure.
 */
type PrePhotonDeadlineDeferral = {
  readonly kind: "deferred_before_photon";
  readonly code: string;
};

type OutboxDispatchOutcome = TransportOutcome | PrePhotonDeadlineDeferral | null;

// Text and prepared-output flows invoke best-effort typing first, so even a
// later send preflight result cannot prove that no Photon RPC happened. A
// tapback has no earlier Photon boundary and its direct binding emits this
// code only when it refused to start the reaction RPC.
const PRE_PHOTON_DEADLINE_CODES = new Set([
  "photon_tapback_deadline_exhausted",
]);

function deadlineDeferral(code: string): PrePhotonDeadlineDeferral {
  return Object.freeze({ kind: "deferred_before_photon", code });
}

function classifyPrePhotonDeadlineOutcome(outcome: TransportOutcome): TransportOutcome | PrePhotonDeadlineDeferral {
  if (outcome.kind === "retryable_failure" && PRE_PHOTON_DEADLINE_CODES.has(outcome.code)) {
    return deadlineDeferral(outcome.code);
  }
  return outcome;
}

/**
 * A generic injected transport may not understand Air's optional deadline.
 * Race it here as a final worker guard. The operation is supplied as a thunk
 * so an elapsed budget never even invokes a physical transport method.
 */
async function awaitWithinDrainBudget<T>(
  operation: () => Promise<T>,
  budget: StudioDrainBudget,
): Promise<DrainDeadlineResult<T>> {
  const remainingMs = budget.remainingMs();
  if (remainingMs < 1) return Object.freeze({ kind: "expired" });

  return await new Promise<DrainDeadlineResult<T>>((resolve, reject) => {
    const timeout = setTimeout(() => resolve(Object.freeze({ kind: "expired" })), remainingMs);
    let operationPromise: Promise<T>;
    try {
      operationPromise = Promise.resolve().then(operation);
    } catch (error) {
      clearTimeout(timeout);
      reject(error);
      return;
    }
    void operationPromise.then(
      (value) => {
        clearTimeout(timeout);
        resolve(Object.freeze({ kind: "completed", value }));
      },
      (error: unknown) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

async function dispatchOutboxEntry(
  runtime: StudioRuntime,
  transport: StudioIMessageTransport,
  record: StudioOutboxRecord,
  leaseToken: string,
  payload: StudioOutboxPayload,
  budget: StudioDrainBudget,
): Promise<OutboxDispatchOutcome> {
  if (record.lane === "job-result") {
    const authorization = await authorizeStudioJobResultDispatch(runtime, record);
    if (authorization === "defer") {
      return Object.freeze({ kind: "retryable_failure", code: "job_result_not_ready" });
    }
    if (authorization === "blocked") {
      return Object.freeze({ kind: "failed", code: "job_result_delivery_blocked" });
    }
  }
  const thread: IMessageThreadReference = payload.thread;
  const idempotencyKey = `air-v2-outbox-${record.outboxId}`;

  if (payload.kind === "tapback") {
    if (!payload.targetMessageId) return Object.freeze({ kind: "failed", code: "missing_tapback_target" });
    const authorization = await awaitWithinDrainBudget(
      () => runtime.store.authorizeOutboxDelivery(record, leaseToken),
      budget,
    );
    if (authorization.kind === "expired") {
      return deadlineDeferral("outbox_deadline_before_photon");
    }
    if (!authorization.value) return null;
    if (!budget.canStart(OUTBOX_PHYSICAL_ACTION_MARGIN_MS)) {
      return deadlineDeferral("outbox_deadline_before_photon");
    }
    try {
      const outcome = await awaitWithinDrainBudget(
        () => transport.addLikeTapback(thread, payload.targetMessageId, idempotencyKey, budget.transportOptions()),
        budget,
      );
      return outcome.kind === "expired"
        ? Object.freeze({ kind: "send_unknown", code: "tapback_dispatch_deadline_unknown" })
        : classifyPrePhotonDeadlineOutcome(outcome.value);
    } catch {
      return Object.freeze({ kind: "send_unknown", code: "tapback_dispatch_unknown" });
    }
  }

  let typingStarted = false;
  try {
    const initialAuthorization = await awaitWithinDrainBudget(
      () => runtime.store.authorizeOutboxDelivery(record, leaseToken),
      budget,
    );
    if (initialAuthorization.kind === "expired") {
      return deadlineDeferral("outbox_deadline_before_photon");
    }
    if (!initialAuthorization.value) return null;
    // Authorization is Redis-only. It can consume the shared window, but no
    // Photon method has run yet, so return the claim without charging a
    // physical-delivery attempt.
    if (!budget.canStart(OUTBOX_PHYSICAL_ACTION_MARGIN_MS)) {
      return deadlineDeferral("outbox_deadline_before_photon");
    }
    try {
      const typing = await awaitWithinDrainBudget(
        () => transport.showTyping(thread, budget.transportOptions()),
        budget,
      );
      typingStarted = typing.kind === "completed";
    } catch {
      // Typing is strictly best-effort and never blocks a durable message.
    }
    const dwellResult = await awaitWithinDrainBudget(() => dwell(TYPING_DWELL_MS), budget);
    if (dwellResult.kind === "expired") {
      return Object.freeze({ kind: "retryable_failure", code: "outbox_deadline_before_send" });
    }
    // This second check closes the typing-dwell interval. The successful
    // authorization immediately below is the durable commitment point for
    // the following non-transactional transport call.
    const sendAuthorization = await awaitWithinDrainBudget(
      () => runtime.store.authorizeOutboxDelivery(record, leaseToken),
      budget,
    );
    if (sendAuthorization.kind === "expired") {
      return Object.freeze({ kind: "retryable_failure", code: "outbox_deadline_before_send" });
    }
    if (!sendAuthorization.value) return null;
    if (!budget.canStart(OUTBOX_PHYSICAL_ACTION_MARGIN_MS)) {
      return Object.freeze({ kind: "retryable_failure", code: "outbox_deadline_before_send" });
    }
    if (payload.kind === "text") {
      if (!payload.text || !isSafeTransportText(payload.text)) {
        return Object.freeze({ kind: "failed", code: "invalid_text_payload" });
      }
      const outcome = await awaitWithinDrainBudget(
        () => transport.sendText(thread, payload.text, idempotencyKey, budget.transportOptions()),
        budget,
      );
      return outcome.kind === "expired"
        ? Object.freeze({ kind: "send_unknown", code: "transport_dispatch_deadline_unknown" })
        : classifyPrePhotonDeadlineOutcome(outcome.value);
    }
    if (payload.kind !== "prepared-output" || !isStudioPreparedOutputTransport(transport)) {
      return Object.freeze({ kind: "failed", code: "prepared_output_transport_unavailable" });
    }
    const outcome = await awaitWithinDrainBudget(
      () => transport.sendPreparedOutput(thread, payload.output, idempotencyKey, budget.transportOptions()),
      budget,
    );
    return outcome.kind === "expired"
      ? Object.freeze({ kind: "send_unknown", code: "transport_dispatch_deadline_unknown" })
      : classifyPrePhotonDeadlineOutcome(outcome.value);
  } catch {
    // A rejected send promise can occur after the transport accepted bytes, so
    // it is intentionally unknown rather than a retryable failure.
    return Object.freeze({ kind: "send_unknown", code: "transport_dispatch_unknown" });
  } finally {
    if (typingStarted && budget.canStart(1)) {
      try {
        await awaitWithinDrainBudget(() => transport.stopTyping(thread, budget.transportOptions()), budget);
      } catch {
        // No action: a failed stop must not alter the message outcome.
      }
    }
  }
}

function dwell(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
