import { randomBytes } from "node:crypto";
import type { EncryptedPayload, QueueCipher } from "../crypto.js";
import { acquireRedisLease, type RedisLease, type RedisLike } from "../redis.js";
import { deriveOpaqueId, type SenderHash, type ThreadHash } from "../security.js";

/** All new records live under this namespace. Legacy Air records stay untouched. */
export const STUDIO_REDIS_PREFIX = "air:v2";

export const STUDIO_JOB_STATES = Object.freeze([
  "received",
  "validating",
  "admitted",
  "executing",
  "ready",
  "delivering",
  "delivered",
  "failed",
  "canceled",
  "maintenance_rejected",
  "reconciliation_required",
] as const);
export type StudioJobState = (typeof STUDIO_JOB_STATES)[number];

export const STUDIO_OUTBOX_STATES = Object.freeze([
  "intent",
  "sending",
  "sent",
  "retryable_failure",
  "failed",
  "canceled",
  "send_unknown",
  "confirmed_absent",
  "requeued",
] as const);
export type StudioOutboxState = (typeof STUDIO_OUTBOX_STATES)[number];

/** An opaque deployment capability, advanced only by the protected operator route. */
export type StudioReleasePhase = "dark" | "canary" | "production";

export interface StudioReleaseStamp {
  readonly generation: number;
  readonly phase: StudioReleasePhase;
}

export type StudioReleaseFenceAdvanceResult =
  | { readonly kind: "advanced" }
  | { readonly kind: "current" }
  | { readonly kind: "conflict" }
  | { readonly kind: "invalid" };

/** Secret-free operator view used to choose an exact release-fence CAS value. */
export type StudioReleaseFenceStatus =
  | { readonly kind: "missing" }
  | { readonly kind: "current"; readonly stamp: StudioReleaseStamp }
  | { readonly kind: "invalid" };

/**
 * A delivery attempt is only retried when the worker has established that no
 * iMessage side effect occurred.  An interrupted `sending` attempt is always
 * reconciled as `send_unknown`, never replayed.
 */
export const STUDIO_MAX_OUTBOX_SEND_ATTEMPTS = 3;

export interface StudioAttachmentReference {
  readonly id: string;
  readonly mimeType: string | null;
  readonly sizeBytes: number | null;
}

/**
 * A provider result after exactly one successful Photon upload. This compact
 * capability is encrypted at rest; it carries no provider URL, filename,
 * bytes, or inbound attachment locator.
 */
export interface StudioPreparedOutput {
  readonly transport: "photon";
  readonly attachmentGuid: string;
}

/**
 * The upload of a provider artifact is a separate physical Photon side
 * effect.  It is deliberately tracked before bytes leave Air so a worker
 * crash between Photon accepting an upload and Redis receiving its GUID can
 * never look safe to retry.
 */
export const STUDIO_JOB_RESULT_PREPARATION_STATES = Object.freeze([
  "reserved",
  "uploading",
  "prepared",
  "upload_unknown",
  "failed",
  "canceled",
] as const);
export type StudioJobResultPreparationState = (typeof STUDIO_JOB_RESULT_PREPARATION_STATES)[number];

/**
 * Content-free, durable ownership record for one output's Photon upload.
 * The small Photon GUID itself is separately encrypted and expires with the
 * immutable job payload; this metadata intentionally contains no provider
 * artifact reference, filename, route, or user content.
 */
export interface StudioJobResultPreparationRecord {
  readonly schemaVersion: 1;
  readonly preparationId: string;
  readonly jobId: string;
  readonly outputIndex: number;
  readonly state: StudioJobResultPreparationState;
  readonly stateVersion: number;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
}

/** The encrypted reference stores only an already-uploaded native Photon GUID. */
interface StudioJobResultPreparationReference {
  readonly output: StudioPreparedOutput;
}

export type StudioJobResultPreparationBeginResult =
  | { readonly kind: "ready"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "prepared"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "upload_unknown"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "failed" | "canceled"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "release_blocked" | "payload_expiring"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "lease_lost" | "unavailable" };

export type StudioJobResultPreparationFinishOutcome =
  | { readonly kind: "prepared"; readonly output: StudioPreparedOutput }
  | { readonly kind: "retryable_failure" }
  | { readonly kind: "failed" }
  | { readonly kind: "upload_unknown" };

export type StudioJobResultPreparationFinishResult =
  | { readonly kind: "prepared"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "retryable_failure"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "failed"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "upload_unknown"; readonly preparation: StudioJobResultPreparationRecord }
  | { readonly kind: "lease_lost" | "unavailable" };

/**
 * A recovery worker may own a newly acquired preparation lease after an
 * uploader died. This primitive deliberately never starts an upload: it only
 * turns the pre-existing ambiguous `uploading` boundary into reconciliation.
 */
export type StudioJobResultPreparationRecoveryResult =
  | {
      readonly kind: "reserved" | "prepared" | "upload_unknown" | "failed" | "canceled";
      readonly preparation: StudioJobResultPreparationRecord;
    }
  | { readonly kind: "lease_lost" | "unavailable" };

/** Stored encrypted. No prompt, phone, GUID, or media bytes is visible in Redis metadata. */
export interface StudioInboundEnvelope {
  readonly chatGuid: string;
  readonly sendingLine: string;
  readonly text: string | null;
  readonly attachments: readonly StudioAttachmentReference[];
  readonly replyTargetId?: string | undefined;
}

export interface StudioInboxRecord {
  readonly schemaVersion: 2;
  readonly receiptId: string;
  readonly senderHash: SenderHash;
  readonly threadHash: ThreadHash;
  /**
   * Monotonic durable receipt order for one `{project, sender, chat, line}`
   * scope. It is allocated at ingress, never inferred from wall-clock time.
   */
  readonly scopeSequence: number;
  readonly receivedAtMs: number;
  readonly state: "received" | "claimed" | "processed" | "maintenance_rejected" | "failed";
  readonly stateVersion: number;
  /** Scrubbed atomically as soon as the receipt reaches a terminal state. */
  readonly encryptedEnvelope?: EncryptedPayload | undefined;
}

/**
 * The inbox queue moves forward exactly once.  `legacy` deliberately keeps
 * the pre-FIFO global-ready behavior while a compatibility deployment is
 * drained; `holding` durably accepts new receipts without claiming them; and
 * `fifo` permits only the exact per-scope head to be claimed.
 */
export type StudioInboxFifoPhase = "legacy" | "holding" | "fifo";

/** Secret-free migration view for the protected operator control plane. */
export type StudioInboxFifoStatus =
  | { readonly kind: "current"; readonly phase: StudioInboxFifoPhase; readonly holdingScopes: number }
  | { readonly kind: "invalid" };

export type StudioInboxFifoBeginResult =
  | { readonly kind: "holding" }
  | { readonly kind: "already_holding" }
  | { readonly kind: "legacy_not_drained" }
  | { readonly kind: "already_fifo" }
  | { readonly kind: "invalid" };

export type StudioInboxFifoPrepareResult =
  | { readonly kind: "prepared"; readonly preparedScopes: number; readonly remainingScopes: number }
  | { readonly kind: "blocked"; readonly preparedScopes: number }
  | { readonly kind: "not_holding" }
  | { readonly kind: "invalid" };

export type StudioInboxFifoActivateResult =
  | { readonly kind: "activated" }
  | { readonly kind: "already_fifo" }
  | { readonly kind: "holding_not_prepared" }
  | { readonly kind: "not_holding" }
  | { readonly kind: "invalid" };

export interface StudioJobPayload {
  readonly prompt: string | null;
  readonly input?: StudioAttachmentReference | undefined;
  readonly chatGuid: string;
  readonly sendingLine: string;
  readonly replyTargetId?: string | undefined;
  /**
   * Immutable, encrypted admission evidence for the one allowed provider
   * operation. Historical dark jobs intentionally omit this field and can
   * never be sent through the executor by accident.
   */
  readonly execution?: StudioExecutionSnapshot | undefined;
}

/**
 * A reviewed model/workflow snapshot carried only inside the job's encrypted
 * 24-hour payload. It is deliberately narrower than a provider request: no
 * prompt, chat, attachment reference, provider request ID, or artifact URL
 * belongs in durable plaintext metadata.
 */
export interface StudioExecutionSnapshot {
  /**
   * Version two pins every code-facing provider contract used at admission.
   * A legacy snapshot deliberately cannot cross the paid-provider boundary:
   * its model name alone is not enough to reconstruct a safe request later.
   */
  readonly schemaVersion: 2;
  readonly workflow: {
    readonly workflowId: string;
    readonly workflowVersion: 1;
    readonly stepId: "provider-generation";
  };
  readonly model: {
    readonly slug: string;
    readonly provider: "gmi" | "fal";
    readonly providerModelId: string;
    /** Exact reviewed adapter key; never infer an adapter from provider name. */
    readonly adapterKey: string;
    /** Canonical fingerprints captured from the exact live model review. */
    readonly catalogFingerprint: string;
    readonly requestSchemaFingerprint: string;
    readonly resultSchemaFingerprint: string;
    readonly pricingEvidenceFingerprint: string;
  };
  readonly output: {
    readonly type: "image" | "video";
    readonly count: number;
    readonly durationSeconds?: number | undefined;
  };
  /** The quoted ceiling that was atomically reserved as `job.reservedCents`. */
  readonly maximumQuotedCostCents: number;
  /** Exact bounded observation policy from the reviewed registry entry. */
  readonly polling: {
    readonly requestTimeoutMs: number;
    readonly pollIntervalMs: number;
    readonly maxPollAttempts: number;
    readonly deadlineMs: number;
  };
  /** The release generation that was allowed to admit this exact work. */
  readonly releaseStamp: StudioReleaseStamp;
}

/**
 * Immutable evidence that the two initial iMessage effects were created in
 * the same admission transaction as this job. It contains only opaque IDs,
 * never message content or addressing data.
 */
export interface StudioInitialNotificationManifest {
  readonly schemaVersion: 1;
  readonly acknowledgementOutboxId: string;
  readonly tapbackOutboxId?: string | undefined;
}

export interface StudioJobRecord {
  readonly schemaVersion: 3;
  readonly jobId: string;
  readonly senderHash: SenderHash;
  readonly threadHash: ThreadHash;
  readonly mode: "imagine" | "animate" | "edit-image" | "edit-video";
  readonly modelSlug: string;
  readonly provider: "gmi" | "fal";
  readonly outputCount: number;
  readonly durationSeconds?: number | undefined;
  readonly reservedCents: number;
  /**
   * Non-secret activation stamp duplicated from the encrypted execution
   * snapshot so admission can reject a stale deployment before it reserves
   * spend or publishes a recoverable job.
   */
  readonly releaseStamp: StudioReleaseStamp;
  /**
   * Present for jobs admitted through the iMessage creation path. A retry
   * verifies this bundle before it may republish the job to the scheduler.
   * Older recovery records intentionally lack it and cannot be mistaken for
   * a newly accepted iMessage creation request.
   */
  readonly initialNotifications?: StudioInitialNotificationManifest | undefined;
  readonly state: StudioJobState;
  readonly stateVersion: number;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
  /** Hydrated only while its separately stored 24-hour payload remains available. */
  readonly encryptedPayload?: EncryptedPayload | undefined;
}

/**
 * A short-lived worker ownership token for one durable job. This is distinct
 * from the provider-subrun lease: owning a job makes it eligible for bounded
 * recovery work, while the subrun lease remains the final pre-provider-call
 * fence.
 */
export interface StudioJobClaim {
  readonly record: StudioJobRecord;
  readonly lease: RedisLease;
}

export type StudioJobRescheduleResult = "scheduled" | "removed" | "conflict";

/** New job admission always needs an encrypted snapshot to publish atomically. */
export type StudioJobRecordWithPayload = StudioJobRecord & {
  readonly encryptedPayload: EncryptedPayload;
};

/** A single native provider submission; Air v2 workflows contain exactly one. */
export const STUDIO_PROVIDER_SUBRUN_STATES = Object.freeze([
  "reserved",
  "submitting",
  "submitted",
  "definitive_rejection",
  "submission_unknown",
  "reconciliation_terminalized",
  "canceled",
] as const);
export type StudioProviderSubrunState = (typeof STUDIO_PROVIDER_SUBRUN_STATES)[number];

/**
 * Opaque durable metadata only. Provider request IDs and all provider output
 * references live in the separately encrypted short-lived reference record.
 */
export interface StudioProviderSubrunRecord {
  readonly schemaVersion: 1;
  readonly subrunId: string;
  readonly jobId: string;
  readonly provider: "gmi" | "fal";
  readonly modelSlug: string;
  readonly workflowId: string;
  readonly stepId: "provider-generation";
  readonly releaseStamp: StudioReleaseStamp;
  /** Stable opaque intent identity assigned before the provider call. */
  readonly submissionIntentId: string;
  readonly state: StudioProviderSubrunState;
  readonly stateVersion: number;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
}

/** Stored encrypted for 24 hours at most, and only after provider acceptance. */
export interface StudioProviderSubrunReference {
  readonly providerRequestId: string;
}

/**
 * A read-only provider-request observation. Unlike submission, observing is
 * safe to retry after a crashed worker because it performs only a bounded
 * provider GET/poll. The record deliberately contains no provider request
 * ID, URL, output reference, prompt, or media data.
 */
export const STUDIO_PROVIDER_OBSERVATION_STATES = Object.freeze([
  "scheduled",
  "observing",
  "succeeded",
  "failed",
  "deadline_exceeded",
  "attempts_exhausted",
  "reference_expired",
  "canceled",
] as const);
export type StudioProviderObservationState = (typeof STUDIO_PROVIDER_OBSERVATION_STATES)[number];

/**
 * Opaque terminal intent for an observed provider failure. It distinguishes a
 * definitive provider rejection from a result that must be audited instead of
 * being silently collapsed into a failed paid job.
 */
export const STUDIO_PROVIDER_OBSERVATION_FAILURE_DISPOSITIONS = Object.freeze([
  "failed",
  "reconciliation_required",
] as const);
export type StudioProviderObservationFailureDisposition =
  (typeof STUDIO_PROVIDER_OBSERVATION_FAILURE_DISPOSITIONS)[number];

/**
 * Content-free durable polling control plane for one known provider request.
 * The immutable state-version fences bind this record to the exact submitted
 * subrun and executing job it was created for. The provider request ID stays
 * only in the existing encrypted, short-lived subrun reference.
 */
export interface StudioProviderObservationRecord {
  readonly schemaVersion: 1;
  readonly observationId: string;
  readonly subrunId: string;
  readonly jobId: string;
  /** The exact known-submitted subrun state this poller may observe. */
  readonly subrunStateVersion: number;
  /** The exact executing job state this poller may observe. */
  readonly jobStateVersion: number;
  readonly state: StudioProviderObservationState;
  /** Present only for `state: "failed"`; legacy missing values mean `failed`. */
  readonly failureDisposition?: StudioProviderObservationFailureDisposition | undefined;
  /** Counted before each physical provider GET, including a crashed attempt. */
  readonly pollAttempts: number;
  readonly maxPollAttempts: number;
  readonly requestTimeoutMs: number;
  readonly pollIntervalMs: number;
  /** Absolute, immutable deadline derived from the reviewed job snapshot. */
  readonly deadlineAtMs: number;
  /** Earliest time a scheduled safe GET may begin. */
  readonly nextDueAtMs: number;
  readonly stateVersion: number;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
}

export type StudioProviderObservationBeginResult =
  | { readonly kind: "ready"; readonly observation: StudioProviderObservationRecord }
  | {
      readonly kind:
        | "not_due"
        | "succeeded"
        | "failed"
        | "deadline_exceeded"
        | "attempts_exhausted"
        | "reference_expired"
        | "canceled";
      readonly observation: StudioProviderObservationRecord;
    }
  | { readonly kind: "lease_lost" | "unavailable" };

/**
 * `pending` represents an authoritative non-terminal provider GET response
 * or a known safe GET failure. It may be retried under the persisted policy;
 * it never authorizes another provider submission.
 */
export type StudioProviderObservationFinishOutcome =
  | { readonly kind: "pending" }
  | { readonly kind: "succeeded" }
  | {
      readonly kind: "failed";
      /** Omitted by older callers and normalized durably to `failed`. */
      readonly failureDisposition?: StudioProviderObservationFailureDisposition | undefined;
    };

export type StudioProviderObservationFinishResult =
  | {
      readonly kind:
        | "pending"
        | "succeeded"
        | "failed"
        | "deadline_exceeded"
        | "attempts_exhausted"
        | "canceled";
      readonly observation: StudioProviderObservationRecord;
    }
  | { readonly kind: "lease_lost" | "unavailable" };

export type StudioProviderSubmissionBeginResult =
  | { readonly kind: "ready"; readonly subrun: StudioProviderSubrunRecord }
  | { readonly kind: "already_submitted"; readonly subrun: StudioProviderSubrunRecord }
  | { readonly kind: "definitive_rejection"; readonly subrun: StudioProviderSubrunRecord }
  | { readonly kind: "submission_unknown"; readonly subrun: StudioProviderSubrunRecord }
  | { readonly kind: "release_blocked"; readonly subrun: StudioProviderSubrunRecord }
  | { readonly kind: "canceled"; readonly subrun: StudioProviderSubrunRecord }
  | { readonly kind: "lease_lost" | "unavailable" };

export type StudioProviderSubmissionOutcome =
  | { readonly kind: "accepted"; readonly providerRequestId: string }
  | { readonly kind: "definitive_rejection" }
  | { readonly kind: "ambiguous" };

/**
 * Opaque, append-only reconciliation evidence.  It intentionally contains no
 * prompt, phone, GUID, provider URL, provider request ID, or artifact data.
 */
export const STUDIO_RECONCILIATION_AUDIT_ACTIONS = Object.freeze([
  "provider_request_attached",
  "provider_terminalized",
  "outbox_sent_confirmed",
  "outbox_absence_confirmed",
  "outbox_requeued",
  "outbox_artifact_expired",
] as const);
export type StudioReconciliationAuditAction = (typeof STUDIO_RECONCILIATION_AUDIT_ACTIONS)[number];

export type StudioProviderTerminalizationReason =
  | "provider_not_found"
  | "provider_unrecoverable"
  | "operator_canceled";

export interface StudioReconciliationAuditRecord {
  readonly schemaVersion: 1;
  readonly auditId: string;
  /** HMAC-derived actor identity; callers must never persist the raw value. */
  readonly actorHash: string;
  readonly action: StudioReconciliationAuditAction;
  /** Opaque provider-subrun or outbox identifier. */
  readonly subjectId: string;
  /** Opaque replacement outbox ID for a confirmed-absent requeue. */
  readonly relatedId?: string | undefined;
  readonly reason?: StudioProviderTerminalizationReason | undefined;
  readonly occurredAtMs: number;
}

export type StudioReconciliationResult =
  | { readonly kind: "applied"; readonly auditId: string; readonly replacementOutboxId?: string | undefined }
  | { readonly kind: "conflict" }
  | { readonly kind: "payload_expired"; readonly auditId?: string | undefined }
  | { readonly kind: "release_blocked" }
  | { readonly kind: "unavailable" };

export interface AttachProviderRequestForReconciliationInput {
  readonly subrunId: string;
  readonly expectedSubrunVersion: number;
  readonly expectedJobVersion: number;
  readonly providerRequestId: string;
  readonly operatorHash: string;
}

export interface TerminalizeProviderSubmissionForReconciliationInput {
  readonly subrunId: string;
  readonly expectedSubrunVersion: number;
  readonly expectedJobVersion: number;
  readonly reason: StudioProviderTerminalizationReason;
  readonly operatorHash: string;
}

export interface ResolveOutboxSendForReconciliationInput {
  readonly outboxId: string;
  readonly expectedOutboxVersion: number;
  readonly resolution: "sent" | "confirmed_absent";
  readonly operatorHash: string;
}

export interface RequeueConfirmedAbsentOutboxInput {
  readonly outboxId: string;
  readonly expectedOutboxVersion: number;
  readonly operatorHash: string;
}

export interface StudioOutboxThread {
  readonly thread: {
    readonly chatGuid: string;
    readonly sendingLine: string;
    readonly replyTargetId?: string | undefined;
  };
}

export type StudioOutboxPayload =
  | (StudioOutboxThread & {
      readonly kind: "text";
      readonly text: string;
    })
  | (StudioOutboxThread & {
      /** Never an inbound reference: only an already-uploaded Photon GUID. */
      readonly kind: "prepared-output";
      readonly outputIndex: number;
      readonly output: StudioPreparedOutput;
    })
  | (StudioOutboxThread & {
      readonly kind: "tapback";
      readonly targetMessageId: string;
    });

export interface StudioOutboxRecord {
  readonly schemaVersion: 3;
  readonly outboxId: string;
  readonly jobId?: string | undefined;
  /**
   * Present only on a result intent that is owned by the durable per-job
   * delivery cursor.  Generic `job-result` records created before that cursor
   * foundation intentionally omit it and remain readable during migration.
   */
  readonly resultOutputIndex?: number | undefined;
  readonly senderHash: SenderHash;
  readonly lane: "onboarding" | "tapback" | "job-acknowledgement" | "job-result" | "request-response";
  /** Stable identity for an isolated lane across an explicitly requeued send. */
  readonly laneIdentity?: string | undefined;
  readonly sequence: number;
  readonly state: StudioOutboxState;
  /** A known privacy-expiry failure is terminal and never triggers a refetch. */
  readonly terminalReason?: "artifact_expired" | undefined;
  readonly attempts: number;
  readonly stateVersion: number;
  /** Every user-visible intent is bound to one shared release generation. */
  readonly releaseStamp: StudioReleaseStamp;
  /**
   * Present only for a media-first acknowledgement. The random guard ties the
   * acknowledgement to the still-live pending intent so maintenance can
   * revoke it before delivery without retaining any user content.
   */
  readonly pendingGuard?: PendingWriteGuard | undefined;
  /**
   * Present only while the encrypted delivery payload remains inside its
   * immutable 24-hour privacy lifetime. Terminal/reconciliation metadata
   * deliberately outlives this short-lived reference.
   */
  readonly encryptedPayload?: EncryptedPayload | undefined;
  readonly createdAtMs: number;
  /** Durable not-before time. Media-first corrections use this for debounce. */
  readonly dueAtMs: number;
  readonly updatedAtMs: number;
}

/**
 * Content-free, durable pointer for one provider output. The actual artifact
 * reference remains encrypted in the outbox payload, never in this cursor.
 */
interface StudioJobResultCursorOutput {
  readonly outputIndex: number;
  readonly outboxId: string;
}

interface StudioJobResultCursorMetadata {
  readonly schemaVersion: 1;
  readonly jobId: string;
  readonly senderHash: SenderHash;
  readonly threadHash: ThreadHash;
  readonly outputCount: number;
  /** Immutable copy of the release stamp at paid-job admission. */
  readonly releaseStamp: StudioReleaseStamp;
  /** The next one-based provider output index allowed to publish. */
  readonly nextOutputIndex: number;
  readonly outputs: readonly StudioJobResultCursorOutput[];
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
}

/** The authoritative delivery state is hydrated from the linked outbox. */
export interface StudioJobResultOutputStatus extends StudioJobResultCursorOutput {
  readonly state: StudioOutboxState;
  readonly attempts: number;
  readonly stateVersion: number;
  readonly updatedAtMs: number;
}

/**
 * A read-safe view of the result cursor. It binds every one-based provider
 * output to its current physical iMessage intent without exposing media,
 * route, prompt, or provider references.
 */
export interface StudioJobResultCursor extends Omit<StudioJobResultCursorMetadata, "outputs"> {
  readonly outputs: readonly StudioJobResultOutputStatus[];
}

export interface PublishStudioJobResultInput {
  readonly jobId: string;
  readonly outputIndex: number;
  /** Must be a native attachment result bound to the job's encrypted route. */
  readonly payload: StudioOutboxPayload;
}

export type StudioJobResultPublishResult =
  | { readonly kind: "published"; readonly cursor: StudioJobResultCursor; readonly outbox: StudioOutboxRecord }
  | { readonly kind: "existing"; readonly cursor: StudioJobResultCursor; readonly outbox: StudioOutboxRecord }
  | { readonly kind: "out_of_order"; readonly expectedOutputIndex: number }
  | { readonly kind: "release_blocked" }
  | { readonly kind: "unavailable" };

/**
 * The single text bubble which is durably ordered immediately before Air's
 * first native result attachment. It is encrypted in the outbox payload;
 * the plaintext never belongs in Redis metadata.
 */
export const STUDIO_RESULT_PREAMBLE_TEXT = "Here’s your result:";

/**
 * The coherent state committed by the provider-result finalizer. Both
 * outboxes are persistent intents, so callers must still let the existing
 * delivery worker perform the physical iMessage sends in sequence.
 */
export interface StudioPreparedProviderResultFinalization {
  readonly job: StudioJobRecord;
  readonly observation: StudioProviderObservationRecord;
  readonly preamble: StudioOutboxRecord;
  readonly attachment: StudioOutboxRecord;
  readonly cursor: StudioJobResultCursor;
}

/**
 * A provider result is only final once its native Photon GUID has already
 * crossed the upload-to-Redis boundary. The finalizer never accepts provider
 * bytes, URLs, or request IDs from a caller.
 */
export type StudioPreparedProviderResultFinalizeResult =
  | ({ readonly kind: "finalized" | "existing" } & StudioPreparedProviderResultFinalization)
  | { readonly kind: "lease_lost" | "release_blocked" | "payload_expiring" | "unavailable" };

/** Opaque capability bound to one pending-assembly fencing transition. */
export interface PendingWriteGuard {
  readonly threadHash: ThreadHash;
  readonly receiptId: string;
  readonly scopeSequence: number;
  readonly revision: number;
  readonly token: string;
  readonly expiresAtMs: number;
}

export type PendingWriteResult =
  | { readonly kind: "active"; readonly guard: PendingWriteGuard }
  | { readonly kind: "superseded" }
  | { readonly kind: "maintenance" }
  | { readonly kind: "cleared" }
  | { readonly kind: "expired" }
  | { readonly kind: "unavailable" };

export type PendingFenceResult =
  | { readonly kind: "canceled" }
  | { readonly kind: "empty" }
  | { readonly kind: "stale" };

export type PendingGuardedOutboxResult =
  | { readonly kind: "persisted"; readonly record: StudioOutboxRecord }
  | { readonly kind: "maintenance" }
  | { readonly kind: "superseded" }
  | { readonly kind: "cleared" }
  | { readonly kind: "expired" }
  | { readonly kind: "unavailable" };

/**
 * The two text payloads which make up a first-contact onboarding sequence.
 * Their IDs are derived from the inbound receipt by the caller, so a retry
 * cannot create a second physical-message intent for the same receipt.
 */
export interface StudioOnboardingBubbleInput {
  readonly outboxId: string;
  readonly payload: StudioOutboxPayload;
}

export interface EnsureStudioOnboardingBundleInput<T> {
  readonly senderHash: SenderHash;
  readonly receiptId: string;
  readonly onboardingVersion: number;
  /** The settings value encrypted and committed with the enrollment marker. */
  readonly settings: T;
  readonly bubbles: readonly [StudioOnboardingBubbleInput, StudioOnboardingBubbleInput];
}

/** A failed bundle never advances the settings-only onboarding marker. */
export type StudioOnboardingBundleResult =
  | { readonly kind: "created" }
  | { readonly kind: "existing" }
  | { readonly kind: "unavailable" };

/**
 * Opaque, non-expiring proof that settings and both onboarding intents were
 * committed together. It deliberately contains no thread or message content.
 */
interface StudioOnboardingMarker {
  readonly schemaVersion: 1;
  readonly onboardingVersion: number;
  readonly receiptId: string;
  readonly firstOutboxId: string;
  readonly secondOutboxId: string;
}

type PendingControlState = "active" | "cleared" | "maintenance" | "expired";

/**
 * Deliberately unencrypted: every value is an opaque HMAC-derived ID or a
 * monotonic counter. The encrypted payload remains the only pending record
 * containing an attachment reference or user content.
 */
interface PendingControl {
  readonly schemaVersion: 1;
  readonly state: PendingControlState;
  readonly receiptId: string;
  readonly scopeSequence: number;
  readonly revision: number;
  readonly token?: string | undefined;
  readonly expiresAtMs?: number | undefined;
}

/** Non-secret global migration record; its absence is the legacy baseline. */
interface InboxFifoMigrationRecord {
  readonly schemaVersion: 1;
  readonly phase: Exclude<StudioInboxFifoPhase, "legacy">;
}

/** Internal, secret-free arguments for the atomic canary admission scripts. */
interface CanaryBudgetAdmission {
  readonly key: string;
  readonly enabled: "0" | "1";
  readonly totalCapCents: number;
  readonly falCapCents: number;
}

type CreateStudioOutboxInput = Omit<StudioOutboxRecord,
  "schemaVersion" | "releaseStamp" | "encryptedPayload" | "createdAtMs" | "dueAtMs" | "updatedAtMs" | "stateVersion" | "state" | "attempts" | "pendingGuard"
> & {
  readonly payload: StudioOutboxPayload;
  /** Defaults to immediate dispatch; media-first corrections set a durable debounce. */
  readonly dueAtMs?: number | undefined;
  /** Accepted for backward-compatible callers, but always normalized to v3. */
  readonly schemaVersion?: 3 | undefined;
  readonly state?: "intent" | undefined;
  readonly attempts?: 0 | undefined;
};

export type AdmissionResult =
  | { readonly admitted: true; readonly job: StudioJobRecord }
  | {
      readonly admitted: false;
      readonly reason:
        | "active_job"
        | "user_limit"
        | "per_job_cap"
        | "daily_cap"
        | "canary_budget_required"
        | "canary_total_cap"
        | "canary_fal_cap"
        | "inbox_fifo_required"
        | "release_blocked"
        | "storage_error";
    };

/**
 * A pre-submission cancellation is one atomic state transition plus one
 * durable user-notice intent.  `not_admitted` is deliberately distinct from
 * a storage error: the executor may have crossed its own submit boundary, so
 * the caller must re-read and describe that state truthfully rather than
 * pretending the request was canceled.
 */
export type StudioAdmittedJobCancellationResult =
  | "canceled"
  | "not_admitted"
  | "release_blocked"
  | "unavailable";

export interface CancelAdmittedJobWithNotificationInput {
  readonly job: Pick<StudioJobRecord, "jobId" | "senderHash" | "state" | "stateVersion" | "encryptedPayload">;
  readonly notification: StudioInitialJobNotification;
}

export interface StudioStoreOptions {
  readonly redis: RedisLike;
  readonly cipher: QueueCipher;
  readonly identityHashSecret: string;
  readonly prefix?: string | undefined;
  readonly clock?: (() => number) | undefined;
  /** Defaults only to the non-delivering dark baseline. */
  readonly releaseStamp?: StudioReleaseStamp | undefined;
}

/**
 * Ephemeral canary-admission policy. Its budget identifier is used only to
 * derive an HMAC-scoped Redis key; it is never serialized into a job,
 * argument, log, or budget record.
 */
export interface StudioCanaryBudgetPolicy {
  readonly budgetId: string;
  readonly totalCapCents: number;
  readonly falCapCents: number;
}

export interface AdmitStudioJobInput {
  readonly job: StudioJobRecordWithPayload;
  readonly activeJobLimit: number;
  readonly dailyUserJobLimit: number;
  readonly perJobCapCents: number;
  readonly dailySpendCapCents: number;
  /** Required for a new canary reservation; ignored outside canary. */
  readonly canaryBudget?: StudioCanaryBudgetPolicy | undefined;
}

/**
 * The two user-visible effects that must exist before a newly admitted job is
 * eligible for provider submission. Their content stays encrypted in their
 * own outbox payloads; this type carries it only until the atomic admission
 * script persists it.
 */
export interface StudioInitialJobNotification {
  readonly outboxId: string;
  readonly payload: StudioOutboxPayload;
}

export interface AdmitStudioJobWithInitialNotificationsInput extends AdmitStudioJobInput {
  readonly acknowledgement: StudioInitialJobNotification;
  /** Best-effort like reaction when the inbound event has a stable target. */
  readonly tapback?: StudioInitialJobNotification | undefined;
}

/**
 * The configured Vercel recovery route runs every minute. Three missed beats
 * is enough to surface an outage while allowing a single slow function or
 * scheduler delay without declaring a false incident.
 */
export const STUDIO_CRON_FRESHNESS_MS = 3 * 60 * 1_000;

export type StudioCronFreshness = "fresh" | "stale" | "missing" | "invalid";
export type StudioCronHeartbeatScope = "development" | "preview" | "production";

/** Terminal metadata is retained for idempotency/audit after terminalization. */
const TERMINAL_METADATA_TTL_SECONDS = 7 * 24 * 60 * 60;
const INBOX_TTL_SECONDS = TERMINAL_METADATA_TTL_SECONDS;
/** Encrypted inbound content may include media references and never outlives one day. */
const INBOX_ENVELOPE_TTL_SECONDS = 24 * 60 * 60;
/** Prompts, media references, and thread data in a job payload never outlive one day. */
export const STUDIO_JOB_PAYLOAD_TTL_MS = 24 * 60 * 60 * 1_000;
const JOB_PAYLOAD_TTL_SECONDS = STUDIO_JOB_PAYLOAD_TTL_MS / 1_000;
const OUTBOX_PAYLOAD_TTL_SECONDS = 24 * 60 * 60;
const SETTINGS_TTL_SECONDS = 90 * 24 * 60 * 60;
const PENDING_TTL_SECONDS = 10 * 60;
const PENDING_TTL_MS = PENDING_TTL_SECONDS * 1_000;
const LEASE_TTL_MS = 90_000;
/** Scan a bounded set of due entries when a stale lease occupies the head. */
const JOB_CLAIM_SCAN_LIMIT = 16;
/**
 * A provider submit needs its full bounded lease plus a small persistence
 * margin before the immutable encrypted job payload expires.  This is a
 * fail-closed admission-to-submit deadline, not a promise that an external
 * provider call can be rolled back after it has begun.
 */
export const STUDIO_PROVIDER_REFERENCE_COMMIT_WINDOW_MS = LEASE_TTL_MS + 5_000;
/**
 * A Photon upload must leave the same bounded persistence margin as a
 * provider submission. If the immutable job payload cannot survive that
 * window, Air does not start a physical upload it could not durably bind.
 */
export const STUDIO_RESULT_PREPARATION_COMMIT_WINDOW_MS = LEASE_TTL_MS + 5_000;
const MAX_INBOX_RETRY_DELAY_MS = 24 * 60 * 60 * 1_000;
const OUTBOX_RETRY_BACKOFF_MS = 30_000;
const CRON_HEARTBEAT_TTL_SECONDS = 24 * 60 * 60;

/**
 * Compact encrypted control-plane store. It intentionally models only Air v2
 * records and leaves v1 keys entirely alone for safe drain-mode rollback.
 */
export class StudioStore {
  private readonly redis: RedisLike;
  private readonly cipher: QueueCipher;
  private readonly secret: string;
  private readonly prefix: string;
  private readonly now: () => number;
  private readonly releaseStamp: StudioReleaseStamp;

  public constructor(options: StudioStoreOptions) {
    if (!options.identityHashSecret.trim()) throw new Error("identity hash secret is required");
    this.redis = options.redis;
    this.cipher = options.cipher;
    this.secret = options.identityHashSecret;
    this.prefix = options.prefix ?? STUDIO_REDIS_PREFIX;
    this.now = options.clock ?? Date.now;
    this.releaseStamp = normalizeReleaseStamp(options.releaseStamp ?? { generation: 0, phase: "dark" });
  }

  public receiptId(projectId: string, messageId: string): string {
    return deriveOpaqueId(this.secret, "studio-receipt", `${projectId}\u0000${messageId}`);
  }

  /**
   * A successful GET drain records only its completion time. It contains no
   * user data and doubles as a lightweight Redis reachability check for the
   * protected readiness route.
   */
  public async recordCronHeartbeat(scope: StudioCronHeartbeatScope): Promise<void> {
    const result = await this.redis.set(this.cronHeartbeatKey(scope), String(this.now()), {
      ex: CRON_HEARTBEAT_TTL_SECONDS,
    });
    if (result !== "OK") throw new Error("cron heartbeat could not be persisted");
  }

  /**
   * Reading this fixed operational key is deliberately the readiness Redis
   * probe. It never opens or decrypts a tenant record.
   */
  public async getCronFreshness(
    scope: StudioCronHeartbeatScope,
    maxAgeMs = STUDIO_CRON_FRESHNESS_MS,
  ): Promise<StudioCronFreshness> {
    if (!Number.isSafeInteger(maxAgeMs) || maxAgeMs < 1) {
      throw new Error("cron freshness window is invalid");
    }
    const raw = await this.redis.get(this.cronHeartbeatKey(scope));
    if (raw === null) return "missing";
    if (!/^(?:0|[1-9]\d*)$/u.test(raw)) return "invalid";
    const recordedAtMs = Number(raw);
    const now = this.now();
    if (!Number.isSafeInteger(recordedAtMs) || recordedAtMs > now) return "invalid";
    return now - recordedAtMs <= maxAgeMs ? "fresh" : "stale";
  }

  /**
   * Explicitly advances the non-expiring shared release generation. Runtime
   * construction and drains never call this: deployment is not activation.
   */
  public async advanceReleaseFence(previousGeneration: number): Promise<StudioReleaseFenceAdvanceResult> {
    if (!Number.isSafeInteger(previousGeneration) || previousGeneration < -1) {
      return Object.freeze({ kind: "invalid" });
    }
    const result = await this.redis.eval(ADVANCE_RELEASE_FENCE_LUA, {
      keys: [this.releaseFenceKey()],
      arguments: [String(previousGeneration), String(this.releaseStamp.generation), this.releaseStamp.phase],
    });
    if (result === 1 || result === "1") return Object.freeze({ kind: "advanced" });
    if (result === 2 || result === "2") return Object.freeze({ kind: "current" });
    if (result === -1 || result === "-1") return Object.freeze({ kind: "conflict" });
    return Object.freeze({ kind: "invalid" });
  }

  /**
   * Reads only the global release epoch. It never opens a tenant record or
   * decrypts data, and is exposed solely through the dedicated operator route.
   */
  public async getReleaseFenceStatus(): Promise<StudioReleaseFenceStatus> {
    const raw = await this.redis.get(this.releaseFenceKey());
    if (raw === null) return Object.freeze({ kind: "missing" });
    const parsed = parseStored<unknown>(raw);
    if (!isReleaseFenceRecord(parsed)) return Object.freeze({ kind: "invalid" });
    return Object.freeze({
      kind: "current",
      stamp: Object.freeze({ generation: parsed.generation, phase: parsed.phase }),
    });
  }

  /**
   * Returns only the durable inbox migration phase. It never reads an inbox
   * record, scope identifier, prompt, media reference, or queue payload.
   */
  public async getInboxFifoMigrationStatus(): Promise<StudioInboxFifoStatus> {
    const phase = parseInboxFifoPhase(await this.redis.get(this.inboxFifoMigrationKey()));
    if (phase === "invalid") return Object.freeze({ kind: "invalid" });
    const holdingScopes = phase === "holding"
      ? await this.redis.zCard(this.inboxFifoHoldingScopesKey())
      : 0;
    return Object.freeze({ kind: "current", phase, holdingScopes });
  }

  /**
   * Begins the one-way legacy -> holding -> fifo cutover. A nonempty legacy
   * ready index is an explicit safety stop: old receipts are never guessed
   * into the new queue or allowed to overtake a missing predecessor.
   */
  public async beginInboxFifoMigration(): Promise<StudioInboxFifoBeginResult> {
    const result = await this.redis.eval(BEGIN_INBOX_FIFO_HOLDING_LUA, {
      keys: [this.inboxFifoMigrationKey(), this.readyInboxKey(), this.inboxFifoHoldingScopesKey()],
      arguments: [],
    });
    if (result === 1 || result === "1") return Object.freeze({ kind: "holding" });
    if (result === 2 || result === "2") return Object.freeze({ kind: "already_holding" });
    if (result === -1 || result === "-1") return Object.freeze({ kind: "legacy_not_drained" });
    if (result === -2 || result === "-2") return Object.freeze({ kind: "already_fifo" });
    return Object.freeze({ kind: "invalid" });
  }

  /**
   * Publishes validated holding heads into the global scheduler in bounded
   * batches. It intentionally never advances over a missing sequence; a
   * malformed/missing head leaves the migration in holding for reconciliation.
   */
  public async prepareInboxFifoHoldingScopes(limit = 32): Promise<StudioInboxFifoPrepareResult> {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
      throw new Error("inbox FIFO preparation limit is invalid");
    }
    const status = await this.getInboxFifoMigrationStatus();
    if (status.kind === "invalid") return Object.freeze({ kind: "invalid" });
    if (status.phase !== "holding") return Object.freeze({ kind: "not_holding" });

    const scopeIds = await this.redis.zRangeByScore(
      this.inboxFifoHoldingScopesKey(),
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
      { limit },
    );
    let preparedScopes = 0;
    for (const scopeId of scopeIds) {
      if (!isOpaqueStudioIdentifier(scopeId)) return Object.freeze({ kind: "blocked", preparedScopes });
      const result = await this.redis.eval(PREPARE_INBOX_FIFO_HOLDING_SCOPE_LUA, {
        keys: [
          this.inboxFifoMigrationKey(),
          this.inboxFifoHoldingScopesKey(),
          this.inboxFifoScopeQueueKey(scopeId),
          this.inboxFifoScopeCursorKey(scopeId),
          this.readyInboxKey(),
        ],
        arguments: [scopeId, this.inboxKeyPrefix()],
      });
      if (result === 1 || result === "1") {
        preparedScopes += 1;
        continue;
      }
      // Another bounded operator invocation may have prepared this exact
      // scope after this call selected it. The Lua primitive returns `2` only
      // after proving that its validated head is already globally published.
      if (result === 2 || result === "2") continue;
      if (result === -1 || result === "-1") return Object.freeze({ kind: "blocked", preparedScopes });
      if (result === -2 || result === "-2") return Object.freeze({ kind: "not_holding" });
      return Object.freeze({ kind: "invalid" });
    }
    return Object.freeze({
      kind: "prepared",
      preparedScopes,
      remainingScopes: await this.redis.zCard(this.inboxFifoHoldingScopesKey()),
    });
  }

  /** Promotes a fully prepared holding set to FIFO; this transition is one-way. */
  public async activateInboxFifoMigration(): Promise<StudioInboxFifoActivateResult> {
    const result = await this.redis.eval(ACTIVATE_INBOX_FIFO_LUA, {
      keys: [this.inboxFifoMigrationKey(), this.inboxFifoHoldingScopesKey()],
      arguments: [],
    });
    if (result === 1 || result === "1") return Object.freeze({ kind: "activated" });
    if (result === 2 || result === "2") return Object.freeze({ kind: "already_fifo" });
    if (result === -1 || result === "-1") return Object.freeze({ kind: "holding_not_prepared" });
    if (result === -2 || result === "-2") return Object.freeze({ kind: "not_holding" });
    return Object.freeze({ kind: "invalid" });
  }

  public async persistInbox(input: {
    readonly receiptId: string;
    readonly senderHash: SenderHash;
    readonly threadHash: ThreadHash;
    readonly envelope: StudioInboundEnvelope;
  }): Promise<{ readonly created: boolean; readonly record: StudioInboxRecord }> {
    const now = this.now();
    const encryptedEnvelope = this.cipher.encrypt(input.envelope, `inbox:${input.receiptId}`);
    // Metadata deliberately never embeds user content. It remains durable
    // while recovery is outstanding; the separately stored encrypted envelope
    // has an immutable 24-hour privacy lifetime.
    const metadata: StudioInboxRecord = Object.freeze({
      schemaVersion: 2,
      receiptId: input.receiptId,
      senderHash: input.senderHash,
      threadHash: input.threadHash,
      // The Lua publication primitive assigns the durable source sequence
      // atomically with dedupe and queue publication.
      scopeSequence: 0,
      receivedAtMs: now,
      state: "received",
      stateVersion: 1,
    });
    const record = Object.freeze({ ...metadata, encryptedEnvelope });
    const key = this.inboxKey(input.receiptId);
    const fifoScopeId = this.inboxFifoScopeId(input.senderHash, input.threadHash);
    // Receipt + ready index must commit together. A separate SET followed by
    // ZADD could acknowledge a webhook and strand work after a crash.
    const result = await this.redis.eval(PERSIST_INBOX_LUA, {
      keys: [
        key,
        this.inboxEnvelopeKey(input.receiptId),
        this.readyInboxKey(),
        // This is the existing latest-ingress fence used by pending-media
        // guards. FIFO progress deliberately uses the separate cursor below.
        this.inboxScopeSequenceKey(input.senderHash, input.threadHash),
        this.inboxFifoMigrationKey(),
        this.inboxFifoScopeQueueKey(fifoScopeId),
        this.inboxFifoScopeCursorKey(fifoScopeId),
        this.inboxFifoHoldingScopesKey(),
      ],
      arguments: [
        JSON.stringify(metadata),
        JSON.stringify(encryptedEnvelope),
        String(INBOX_ENVELOPE_TTL_SECONDS),
        String(now),
        input.receiptId,
        fifoScopeId,
      ],
    });
    const persisted = parseStored<StudioInboxRecord>(typeof result === "string" ? result : null);
    if (persisted && persisted.receiptId === input.receiptId && isPositiveSafeInteger(persisted.scopeSequence)) {
      return Object.freeze({ created: true, record: Object.freeze({ ...persisted, encryptedEnvelope }) });
    }
    if (result === 0 || result === "0") {
      const existing = await this.readInbox(input.receiptId);
      if (existing && isPositiveSafeInteger(existing.scopeSequence)) {
        return Object.freeze({ created: false, record: existing });
      }
    }
    throw new Error("durable inbox publication could not be confirmed");
  }

  public async readInbox(receiptId: string): Promise<StudioInboxRecord | null> {
    return parseStored<StudioInboxRecord>(await this.redis.get(this.inboxKey(receiptId)));
  }

  public decryptInboxEnvelope(record: StudioInboxRecord): StudioInboundEnvelope {
    if (!record.encryptedEnvelope) throw new Error("inbox envelope is unavailable");
    return this.cipher.decrypt<StudioInboundEnvelope>(record.encryptedEnvelope, `inbox:${record.receiptId}`);
  }

  /** Claiming a receipt is fenced by a short Redis lease and never replays an already processed receipt. */
  public async claimNextInbox(): Promise<{ readonly record: StudioInboxRecord; readonly leaseToken: string } | null> {
    const migration = await this.getInboxFifoMigrationStatus();
    if (migration.kind === "invalid") throw new Error("inbox FIFO migration state is invalid");
    // New receipts continue to commit in holding, but no worker may run them
    // until every scope head has been audited and prepared atomically.
    if (migration.phase === "holding") return null;
    const due = await this.redis.zRangeByScore(this.readyInboxKey(), Number.NEGATIVE_INFINITY, this.now(), { limit: 50 });
    for (const receiptId of due) {
      const token = randomBytes(16).toString("base64url");
      const beforeClaim = await this.readInbox(receiptId);
      if (!beforeClaim) {
        // Legacy behavior may clean an orphaned global member. Once FIFO is
        // active an orphan could be an unknown predecessor, so halt rather
        // than letting another same-scope receipt pass it.
        if (migration.phase === "fifo") return null;
        await this.redis.zRem(this.readyInboxKey(), receiptId);
        continue;
      }
      const validFifoIdentity = isOpaqueStudioIdentifier(beforeClaim.senderHash)
        && isOpaqueStudioIdentifier(beforeClaim.threadHash);
      if (!validFifoIdentity) {
        // A nonterminal receipt without a trustworthy scope cannot be safely
        // assigned order or terminalized. Leaving it indexed is an explicit
        // legacy cutover stop; removing it would make `begin` mistake a
        // stranded encrypted receipt for a drained scheduler.
        if (migration.phase === "fifo") return null;
        if (beforeClaim.state !== "received" && beforeClaim.state !== "claimed") {
          await this.redis.zRem(this.readyInboxKey(), receiptId);
          continue;
        }
        return null;
      }
      // Older deployed receipts can have a valid opaque scope but no durable
      // sequence. Legacy mode must still lease them so the engine can
      // terminalize them as maintenance-rejected; FIFO mode never guesses an
      // order for such a record.
      if (!isPositiveSafeInteger(beforeClaim.scopeSequence) && migration.phase === "fifo") return null;
      const claimed = await this.claimInbox(receiptId, token, beforeClaim);
      if (claimed) return Object.freeze({ record: claimed, leaseToken: token });

      // The FIFO claim script has already checked the head/cursor/migration
      // relationship. A failed claim can be a live head lease, a migration
      // race, or integrity trouble; none is safe to bypass by inspecting a
      // later global member.
      if (migration.phase === "fifo") return null;

      // Keep an in-flight claimed receipt indexed until its lease expires so a
      // crashed worker can be recovered. Only terminal/corrupt records are
      // safe to clean from the ready index here.
      const current = await this.readInbox(receiptId);
      if (!current || (current.state !== "received" && current.state !== "claimed")) {
        await this.redis.zRem(this.readyInboxKey(), receiptId);
      }
    }
    return null;
  }

  public async finishInbox(receiptId: string, leaseToken: string, state: "processed" | "maintenance_rejected" | "failed"): Promise<boolean> {
    const record = await this.readInbox(receiptId);
    if (!record || !isOpaqueStudioIdentifier(record.senderHash) || !isOpaqueStudioIdentifier(record.threadHash)) return false;
    const fifoScopeId = this.inboxFifoScopeId(record.senderHash, record.threadHash);
    const result = await this.redis.eval(FINISH_INBOX_LUA, {
      keys: [
        this.inboxKey(receiptId),
        this.inboxEnvelopeKey(receiptId),
        this.inboxLeaseKey(receiptId),
        this.readyInboxKey(),
        this.inboxFifoMigrationKey(),
        this.inboxFifoScopeQueueKey(fifoScopeId),
        this.inboxFifoScopeCursorKey(fifoScopeId),
      ],
      arguments: [leaseToken, state, String(this.now()), String(INBOX_TTL_SECONDS), receiptId, this.inboxKeyPrefix()],
    });
    // `2` means the just-finished record was safely terminalized but a later
    // same-scope receipt was deliberately held behind an integrity gap.
    return result === 1 || result === "1" || result === 2 || result === "2";
  }

  /**
   * Releases a claimed receipt back to the durable queue only when the caller
   * knows it has not created an irreversible provider or iMessage side effect.
   * A worker crash needs no explicit release: the expired lease is reclaimable.
   */
  public async retryInbox(receiptId: string, leaseToken: string, retryAfterMs = 0): Promise<boolean> {
    if (!Number.isSafeInteger(retryAfterMs) || retryAfterMs < 0 || retryAfterMs > MAX_INBOX_RETRY_DELAY_MS) {
      throw new Error("inbox retry delay is invalid");
    }
    const record = await this.readInbox(receiptId);
    if (!record || !isOpaqueStudioIdentifier(record.senderHash) || !isOpaqueStudioIdentifier(record.threadHash)) return false;
    const fifoScopeId = this.inboxFifoScopeId(record.senderHash, record.threadHash);
    const result = await this.redis.eval(RETRY_INBOX_LUA, {
      keys: [
        this.inboxKey(receiptId),
        this.inboxEnvelopeKey(receiptId),
        this.inboxLeaseKey(receiptId),
        this.readyInboxKey(),
        this.inboxFifoMigrationKey(),
        this.inboxFifoScopeQueueKey(fifoScopeId),
        this.inboxFifoScopeCursorKey(fifoScopeId),
      ],
      arguments: [leaseToken, String(this.now()), String(retryAfterMs), String(INBOX_TTL_SECONDS), receiptId, this.inboxKeyPrefix()],
    });
    return result === 1 || result === "1";
  }

  public async getSettings<T>(senderHash: SenderHash): Promise<T | null> {
    const raw = await this.redis.get(this.settingsKey(senderHash));
    const encrypted = parseStored<EncryptedPayload>(raw);
    return encrypted ? this.cipher.decrypt<T>(encrypted, `settings:${senderHash}`) : null;
  }

  public async putSettings<T>(senderHash: SenderHash, value: T): Promise<void> {
    const encrypted = this.cipher.encrypt(value, `settings:${senderHash}`);
    await this.redis.set(this.settingsKey(senderHash), JSON.stringify(encrypted), { ex: SETTINGS_TTL_SECONDS });
  }

  /**
   * Atomically installs first-contact settings.  This prevents two concurrent
   * inbound retries from both deciding that onboarding has not happened yet.
   */
  public async initializeSettingsIfAbsent<T>(senderHash: SenderHash, value: T): Promise<{ readonly created: boolean; readonly value: T }> {
    const key = this.settingsKey(senderHash);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const encrypted = this.cipher.encrypt(value, `settings:${senderHash}`);
      if (await this.redis.set(key, JSON.stringify(encrypted), { nx: true, ex: SETTINGS_TTL_SECONDS }) === "OK") {
        return Object.freeze({ created: true, value });
      }
      const existing = await this.getSettings<T>(senderHash);
      if (existing !== null) return Object.freeze({ created: false, value: existing });
      // The existing key expired between SET NX and GET. Retry once rather
      // than replacing a concurrent first-contact write blindly.
    }
    throw new Error("settings initialization could not be confirmed");
  }

  /**
   * Commits first-contact settings and the complete two-bubble onboarding
   * sequence as one Redis transaction. A settings record is therefore never
   * evidence of onboarding unless both durable outbox intents exist too.
   *
   * The marker is intentionally non-expiring: terminal outbox metadata and
   * encrypted settings can expire independently, but neither event should
   * make an already-onboarded sender receive a duplicate sequence.
   */
  public async ensureOnboardingBundle<T>(
    input: EnsureStudioOnboardingBundleInput<T>,
  ): Promise<StudioOnboardingBundleResult> {
    if (
      !isOpaqueStudioIdentifier(input.senderHash)
      || !isOpaqueStudioIdentifier(input.receiptId)
      || !isPositiveSafeInteger(input.onboardingVersion)
      || input.bubbles.length !== 2
    ) return Object.freeze({ kind: "unavailable" });

    const [firstInput, secondInput] = input.bubbles;
    if (
      !firstInput
      || !secondInput
      || !isOpaqueStudioIdentifier(firstInput.outboxId)
      || !isOpaqueStudioIdentifier(secondInput.outboxId)
      || firstInput.outboxId === secondInput.outboxId
      || firstInput.payload.kind !== "text"
      || secondInput.payload.kind !== "text"
      || typeof firstInput.payload.text !== "string"
      || !firstInput.payload.text
      || typeof secondInput.payload.text !== "string"
      || !secondInput.payload.text
    ) return Object.freeze({ kind: "unavailable" });

    const first = this.buildOutboxRecord({
      outboxId: firstInput.outboxId,
      senderHash: input.senderHash,
      lane: "onboarding",
      sequence: 1,
      payload: firstInput.payload,
    });
    const second = this.buildOutboxRecord({
      outboxId: secondInput.outboxId,
      senderHash: input.senderHash,
      lane: "onboarding",
      sequence: 2,
      payload: secondInput.payload,
    });
    const marker: StudioOnboardingMarker = Object.freeze({
      schemaVersion: 1,
      onboardingVersion: input.onboardingVersion,
      receiptId: input.receiptId,
      firstOutboxId: first.outboxId,
      secondOutboxId: second.outboxId,
    });
    const encryptedSettings = this.cipher.encrypt(input.settings, `settings:${input.senderHash}`);
    const result = await this.redis.eval(ENSURE_ONBOARDING_BUNDLE_LUA, {
      keys: [
        this.settingsKey(input.senderHash),
        this.onboardingMarkerKey(input.senderHash, input.onboardingVersion),
        this.outboxKey(first.outboxId),
        this.outboxPayloadKey(first.outboxId),
        this.outboxKey(second.outboxId),
        this.outboxPayloadKey(second.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(first),
        this.releaseFenceKey(),
      ],
      arguments: [
        JSON.stringify(encryptedSettings),
        String(SETTINGS_TTL_SECONDS),
        JSON.stringify(marker),
        JSON.stringify(outboxMetadata(first)),
        JSON.stringify(requiredOutboxPayload(first)),
        first.outboxId,
        String(first.sequence),
        String(first.dueAtMs),
        JSON.stringify(outboxMetadata(second)),
        JSON.stringify(requiredOutboxPayload(second)),
        second.outboxId,
        String(second.sequence),
        String(second.dueAtMs),
        String(OUTBOX_PAYLOAD_TTL_SECONDS),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
      ],
    });
    if (result === 1 || result === "1") return Object.freeze({ kind: "created" });
    if (result === 2 || result === "2") return Object.freeze({ kind: "existing" });
    return Object.freeze({ kind: "unavailable" });
  }

  public async getPending<T>(senderHash: SenderHash, threadHash: ThreadHash): Promise<T | null> {
    // Read control and encrypted payload in one primitive. Two independent
    // GETs could pair an older control record with a newer payload after an
    // adjacent inbound receipt replaced the pending assembly.
    const result = await this.redis.eval(READ_PENDING_LUA, {
      keys: [this.pendingKey(senderHash, threadHash), this.pendingControlKey(senderHash, threadHash)],
      arguments: [String(this.now())],
    });
    const encrypted = parseStored<EncryptedPayload>(typeof result === "string" ? result : null);
    return encrypted ? this.cipher.decrypt<T>(encrypted, `pending:${senderHash}:${threadHash}`) : null;
  }

  public async putPending<T>(record: StudioInboxRecord, value: T): Promise<boolean> {
    return (await this.putPendingWithGuard(record, value)).kind === "active";
  }

  /**
   * Writes a pending intent alongside an opaque, short-lived guard. Only the
   * media-first acknowledgement needs the guard, but every pending write gets
   * one so a later overwrite or clear revokes any older acknowledgement.
   */
  public async putPendingWithGuard<T>(
    record: StudioInboxRecord,
    value: T,
  ): Promise<PendingWriteResult> {
    assertPendingSource(record);
    const expiresAtMs = record.receivedAtMs + PENDING_TTL_MS;
    if (!Number.isSafeInteger(expiresAtMs)) throw new Error("pending expiry is invalid");
    const encrypted = this.cipher.encrypt(value, `pending:${record.senderHash}:${record.threadHash}`);
    // The guard is derived solely from the durable source receipt. A crash
    // retry sees the same capability and therefore cannot extend retention or
    // revoke its own acknowledgement.
    const token = deriveOpaqueId(
      this.secret,
      "studio-pending-guard",
      `${record.senderHash}\u0000${record.threadHash}\u0000${record.receiptId}\u0000${record.scopeSequence}`,
    );
    const result = await this.redis.eval(PERSIST_PENDING_LUA, {
      keys: [
        this.pendingKey(record.senderHash, record.threadHash),
        this.pendingControlKey(record.senderHash, record.threadHash),
      ],
      arguments: [
        JSON.stringify(encrypted),
        record.receiptId,
        String(record.scopeSequence),
        String(expiresAtMs),
        token,
        String(this.now()),
      ],
    });
    if (result === 1 || result === "1" || result === 2 || result === "2") {
      // The Lua control record increments revision only for a newer durable
      // source. On idempotent retry it returns the already-stored revision.
      const control = await this.readPendingControl(record.senderHash, record.threadHash);
      if (
        control?.state === "active"
        && control.receiptId === record.receiptId
        && control.scopeSequence === record.scopeSequence
        && control.token === token
        && control.expiresAtMs === expiresAtMs
      ) {
        return Object.freeze({
          kind: "active",
          guard: Object.freeze({
            threadHash: record.threadHash,
            receiptId: record.receiptId,
            scopeSequence: record.scopeSequence,
            revision: control.revision,
            token,
            expiresAtMs,
          }),
        });
      }
      return Object.freeze({ kind: "unavailable" });
    }
    if (result === -1 || result === "-1") return Object.freeze({ kind: "superseded" });
    if (result === -2 || result === "-2") return Object.freeze({ kind: "maintenance" });
    if (result === -3 || result === "-3") return Object.freeze({ kind: "cleared" });
    if (result === -4 || result === "-4") return Object.freeze({ kind: "expired" });
    return Object.freeze({ kind: "unavailable" });
  }

  public async clearPending(record: StudioInboxRecord): Promise<PendingFenceResult> {
    return this.fencePending(record, "cleared");
  }

  /**
   * Dark/rollback handling atomically removes pending media and advances the
   * durable scope fence. A stale active-phase worker cannot repopulate the
   * intent after a maintenance receipt has cleared it.
   */
  public async blockPendingForMaintenance(record: StudioInboxRecord): Promise<PendingFenceResult> {
    return this.fencePending(record, "maintenance");
  }

  private async fencePending(
    record: StudioInboxRecord,
    state: "cleared" | "maintenance",
  ): Promise<PendingFenceResult> {
    assertPendingSource(record);
    const result = await this.redis.eval(FENCE_PENDING_LUA, {
      keys: [
        this.pendingKey(record.senderHash, record.threadHash),
        this.pendingControlKey(record.senderHash, record.threadHash),
      ],
      arguments: [record.receiptId, String(record.scopeSequence), state, String(this.now())],
    });
    if (result === 1 || result === "1") return Object.freeze({ kind: "canceled" });
    if (result === 2 || result === "2") return Object.freeze({ kind: "empty" });
    return Object.freeze({ kind: "stale" });
  }

  public async admitJob(input: AdmitStudioJobInput): Promise<AdmissionResult> {
    if (!hasValidAdmissionBounds(input)) {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    const now = this.now();
    const jobKey = this.jobKey(input.job.jobId);

    // A deterministic webhook retry must recover the original reservation
    // even if a later release has changed the live fence or policy caps. Do
    // not make the retry re-prove mutable admission policy. The sender/thread
    // match prevents a guessed opaque job ID from exposing another scope.
    const existing = parseStored<StudioJobRecord>(await this.redis.get(jobKey));
    if (existing) {
      if (
        existing.jobId !== input.job.jobId
        || existing.senderHash !== input.job.senderHash
        || existing.threadHash !== input.job.threadHash
      ) {
        return Object.freeze({ admitted: false, reason: "storage_error" });
      }
      if (isJobQueueEligibleState(existing.state)) {
        await this.redis.zAdd(this.readyJobKey(), [{ score: now, value: existing.jobId }]);
      }
      return Object.freeze({ admitted: true, job: await this.hydrateJobPayload(existing) });
    }

    if (!sameReleaseStamp(input.job.releaseStamp, this.releaseStamp)) {
      return Object.freeze({ admitted: false, reason: "release_blocked" });
    }
    try {
      const payload = this.decryptJobPayload(input.job);
      if (!payload.execution || !isExecutionSnapshotForJob(payload.execution, input.job)) {
        return Object.freeze({ admitted: false, reason: "storage_error" });
      }
    } catch {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    if (input.job.reservedCents > input.perJobCapCents) {
      return Object.freeze({ admitted: false, reason: "per_job_cap" });
    }
    const day = utcDay(now);
    const activeKey = this.activeJobKey(input.job.senderHash);
    const userCountKey = this.userDailyKey(input.job.senderHash, day);
    const dailySpendKey = this.dailySpendKey(day);
    const jobPayloadKey = this.jobPayloadKey(input.job.jobId);
    const ttl = secondsToUtcMidnight(now);
    const canaryBudget = this.canaryBudgetAdmission(input.canaryBudget);
    const result = await this.redis.eval(ADMIT_JOB_LUA, {
      keys: [
        activeKey,
        userCountKey,
        dailySpendKey,
        jobKey,
        jobPayloadKey,
        this.readyJobKey(),
        this.releaseFenceKey(),
        canaryBudget.key,
        this.recentJobKey(input.job.senderHash),
      ],
      arguments: [
        input.job.jobId,
        JSON.stringify(jobMetadata(input.job)),
        JSON.stringify(requiredJobPayload(input.job)),
        String(ttl),
        String(JOB_PAYLOAD_TTL_SECONDS),
        String(input.dailyUserJobLimit),
        String(input.dailySpendCapCents),
        String(input.job.reservedCents),
        String(now),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        canaryBudget.enabled,
        String(canaryBudget.totalCapCents),
        String(canaryBudget.falCapCents),
      ],
    });
    const code = Array.isArray(result) ? result[0] : result;
    if (code === "admitted") return Object.freeze({ admitted: true, job: input.job });
    if (code === "existing") {
      const existing = parseStored<StudioJobRecord>(Array.isArray(result) && typeof result[1] === "string" ? result[1] : null);
      if (!existing || existing.jobId !== input.job.jobId || existing.senderHash !== input.job.senderHash) {
        return Object.freeze({ admitted: false, reason: "storage_error" });
      }
      return Object.freeze({ admitted: true, job: await this.hydrateJobPayload(existing) });
    }
    if (
      code === "active_job"
      || code === "user_limit"
      || code === "daily_cap"
      || code === "canary_budget_required"
      || code === "canary_total_cap"
      || code === "canary_fal_cap"
      || code === "release_blocked"
    ) {
      return Object.freeze({ admitted: false, reason: code });
    }
    return Object.freeze({ admitted: false, reason: "storage_error" });
  }

  /**
   * The creation-path admission boundary. A job reservation, its encrypted
   * snapshot, the durable scheduler entry, and the acknowledgement/tapback
   * intents are committed by one Lua transaction. No worker can observe a
   * newly ready job before those initial user-visible intents exist.
   *
   * `admitJob()` remains for legacy/reconciliation fixtures. New inbound
   * creation code must use this method so it cannot crash between reservation
   * and acknowledgement publication.
   */
  public async admitJobWithInitialNotifications(
    input: AdmitStudioJobWithInitialNotificationsInput,
  ): Promise<AdmissionResult> {
    if (!hasValidAdmissionBounds(input)) {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    if (!isValidInitialJobNotification(input.acknowledgement, "text")) {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    if (input.tapback !== undefined && !isValidInitialJobNotification(input.tapback, "tapback")) {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    if (input.tapback?.outboxId === input.acknowledgement.outboxId) {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    let jobPayload: StudioJobPayload;
    try {
      const payload = this.decryptJobPayload(input.job);
      if (!payload.execution || !isExecutionSnapshotForJob(payload.execution, input.job)) {
        return Object.freeze({ admitted: false, reason: "storage_error" });
      }
      jobPayload = payload;
    } catch {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    // The caller may choose only the human-visible acknowledgement text. Its
    // routing must be the exact encrypted job route; otherwise an internal
    // misuse could acknowledge one conversation while charging another.
    if (
      !isInitialNotificationBoundToJob(input.acknowledgement, "text", jobPayload)
      || (input.tapback !== undefined && !isInitialNotificationBoundToJob(input.tapback, "tapback", jobPayload))
    ) {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }

    const acknowledgement = this.buildOutboxRecord({
      outboxId: input.acknowledgement.outboxId,
      jobId: input.job.jobId,
      senderHash: input.job.senderHash,
      lane: "job-acknowledgement",
      // A job-scoped tapback shares this lane at sequence one, so the text
      // acknowledgement cannot physically cross it in another worker.
      sequence: input.tapback === undefined ? 1 : 2,
      payload: input.acknowledgement.payload,
    });
    const tapback = input.tapback === undefined
      ? undefined
      : this.buildOutboxRecord({
        outboxId: input.tapback.outboxId,
        jobId: input.job.jobId,
        senderHash: input.job.senderHash,
        lane: "tapback",
        sequence: 1,
        payload: input.tapback.payload,
      });
    const initialNotifications: StudioInitialNotificationManifest = Object.freeze({
      schemaVersion: 1,
      acknowledgementOutboxId: acknowledgement.outboxId,
      ...(tapback === undefined ? {} : { tapbackOutboxId: tapback.outboxId }),
    });
    // The manifest is part of the durable job metadata, not caller-provided
    // data. It binds retry-time validation to exactly the two intents this
    // store transaction is about to create.
    const job: StudioJobRecordWithPayload = Object.freeze({
      ...input.job,
      initialNotifications,
    });
    const now = this.now();
    const ttl = secondsToUtcMidnight(now);
    const day = utcDay(now);
    const canaryBudget = this.canaryBudgetAdmission(input.canaryBudget);
    const result = await this.redis.eval(ADMIT_JOB_WITH_INITIAL_NOTIFICATIONS_LUA, {
      keys: [
        this.activeJobKey(job.senderHash),
        this.userDailyKey(job.senderHash, day),
        this.dailySpendKey(day),
        this.jobKey(job.jobId),
        this.jobPayloadKey(job.jobId),
        this.readyJobKey(),
        this.releaseFenceKey(),
        this.readyOutboxKey(),
        this.outboxKey(acknowledgement.outboxId),
        this.outboxPayloadKey(acknowledgement.outboxId),
        this.outboxLaneKey(acknowledgement),
        tapback === undefined ? this.unusedInitialAdmissionOutboxKey() : this.outboxKey(tapback.outboxId),
        tapback === undefined ? this.unusedInitialAdmissionOutboxPayloadKey() : this.outboxPayloadKey(tapback.outboxId),
        tapback === undefined ? this.unusedInitialAdmissionOutboxLaneKey() : this.outboxLaneKey(tapback),
        canaryBudget.key,
        this.recentJobKey(job.senderHash),
        this.inboxFifoMigrationKey(),
      ],
      arguments: [
        job.jobId,
        JSON.stringify(jobMetadata(job)),
        JSON.stringify(requiredJobPayload(job)),
        String(ttl),
        String(JOB_PAYLOAD_TTL_SECONDS),
        String(input.dailyUserJobLimit),
        String(input.dailySpendCapCents),
        String(job.reservedCents),
        String(now),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        String(input.perJobCapCents),
        JSON.stringify(outboxMetadata(acknowledgement)),
        JSON.stringify(requiredOutboxPayload(acknowledgement)),
        acknowledgement.outboxId,
        String(acknowledgement.sequence),
        String(acknowledgement.dueAtMs),
        tapback === undefined ? "" : JSON.stringify(outboxMetadata(tapback)),
        tapback === undefined ? "" : JSON.stringify(requiredOutboxPayload(tapback)),
        tapback?.outboxId ?? "",
        tapback === undefined ? "0" : String(tapback.sequence),
        tapback === undefined ? "0" : String(tapback.dueAtMs),
        tapback === undefined ? "0" : "1",
        String(OUTBOX_PAYLOAD_TTL_SECONDS),
        canaryBudget.enabled,
        String(canaryBudget.totalCapCents),
        String(canaryBudget.falCapCents),
      ],
    });
    const code = Array.isArray(result) ? result[0] : result;
    if (code === "admitted") return Object.freeze({ admitted: true, job });
    if (code === "existing") {
      const existing = parseStored<StudioJobRecord>(Array.isArray(result) && typeof result[1] === "string" ? result[1] : null);
      if (
        !existing
        || existing.jobId !== job.jobId
        || existing.senderHash !== job.senderHash
        || existing.threadHash !== job.threadHash
      ) {
        return Object.freeze({ admitted: false, reason: "storage_error" });
      }
      return Object.freeze({ admitted: true, job: await this.hydrateJobPayload(existing) });
    }
    if (
      code === "active_job"
      || code === "user_limit"
      || code === "daily_cap"
      || code === "per_job_cap"
      || code === "canary_budget_required"
      || code === "canary_total_cap"
      || code === "canary_fal_cap"
      || code === "inbox_fifo_required"
      || code === "release_blocked"
    ) {
      return Object.freeze({ admitted: false, reason: code });
    }
    return Object.freeze({ admitted: false, reason: "storage_error" });
  }

  public async getJob(jobId: string): Promise<StudioJobRecord | null> {
    const metadata = parseStored<StudioJobRecord>(await this.redis.get(this.jobKey(jobId)));
    return metadata ? this.hydrateJobPayload(metadata) : null;
  }

  /**
   * Returns only the caller-owned, nonterminal active job selected by the
   * admission fence.  The pointer is intentionally not trusted on its own:
   * a corrupt or stale value must never expose another sender's record or
   * become a reason to mutate Redis from a read-only status request.
   */
  public async getActiveJob(senderHash: SenderHash): Promise<StudioJobRecord | null> {
    const jobId = await this.redis.get(this.activeJobKey(senderHash));
    if (!isSafeRecordString(jobId, 160)) return null;
    const job = await this.getJob(jobId);
    if (!job || job.senderHash !== senderHash || isTerminalJobState(job.state)) return null;
    return job;
  }

  /**
   * Returns the caller-owned most recently admitted job only after it reaches
   * a terminal state. The pointer contains an opaque job ID and is committed
   * in the same transaction as a new admission; it is never trusted without
   * re-checking both the record owner and terminal state.
   *
   * A retry of an older inbound receipt intentionally does not rewrite this
   * pointer, so a delayed duplicate can never make an older job appear newer
   * than a later completed request.
   */
  public async getMostRecentTerminalJob(senderHash: SenderHash): Promise<StudioJobRecord | null> {
    const jobId = await this.redis.get(this.recentJobKey(senderHash));
    if (!isSafeRecordString(jobId, 160)) return null;
    const job = await this.getJob(jobId);
    if (!job || job.senderHash !== senderHash || !isTerminalJobState(job.state)) return null;
    return job;
  }

  /**
   * Claims one due admitted/executing job from the durable recovery index.
   * The queue head is only a scheduling hint: a separate token lease prevents
   * two serverless invocations from advancing the same job concurrently.
   */
  public async claimNextJob(): Promise<StudioJobClaim | null> {
    const candidates = await this.redis.zRangeByScore(
      this.readyJobKey(),
      Number.NEGATIVE_INFINITY,
      this.now(),
      { limit: JOB_CLAIM_SCAN_LIMIT },
    );
    for (const jobId of candidates) {
      // Existing v2 jobs predate the opaque deterministic receipt IDs. Keep
      // their bounded identifiers recoverable instead of stranding an already
      // reserved job during the migration.
      if (!isSafeRecordString(jobId, 160)) {
        await this.redis.zRem(this.readyJobKey(), jobId);
        continue;
      }
      const lease = await acquireRedisLease(this.redis, this.jobLeaseKey(jobId), {
        now: this.now,
        ttlMs: LEASE_TTL_MS,
      });
      if (!lease) continue;

      let job: StudioJobRecord | null;
      try {
        job = await this.getJob(jobId);
      } catch {
        job = null;
      }
      if (!job || job.jobId !== jobId || !isJobQueueEligibleState(job.state)) {
        await this.redis.zRem(this.readyJobKey(), jobId);
        await lease.release();
        continue;
      }
      return Object.freeze({ record: job, lease });
    }
    return null;
  }

  /**
   * Requeues a claimed nonterminal job under the same ownership token. A stale
   * worker cannot reintroduce a terminal or newer-version job after a later
   * transition has won.
   */
  public async rescheduleClaimedJob(
    job: Pick<StudioJobRecord, "jobId" | "stateVersion">,
    leaseToken: string,
    delayMs: number,
  ): Promise<StudioJobRescheduleResult> {
    if (!Number.isSafeInteger(delayMs) || delayMs < 0 || delayMs > 24 * 60 * 60 * 1_000) return "conflict";
    const dueAtMs = this.now() + delayMs;
    if (!Number.isSafeInteger(dueAtMs)) return "conflict";
    return this.rescheduleClaimedJobAt(job, leaseToken, dueAtMs);
  }

  /**
   * Requeues a claimed job at an authoritative provider-supplied due time.
   * Observation records have their own update clock, so deriving a delay from
   * `job.updatedAtMs` could postpone a poll far beyond its reviewed cadence.
   * A past due time is safely made eligible immediately using the store clock.
   */
  public async rescheduleClaimedJobAt(
    job: Pick<StudioJobRecord, "jobId" | "stateVersion">,
    leaseToken: string,
    dueAtMs: number,
  ): Promise<StudioJobRescheduleResult> {
    if (
      !isSafeRecordString(job.jobId, 160)
      || !isSafeRecordString(leaseToken, 256)
      || !Number.isSafeInteger(job.stateVersion)
      || job.stateVersion < 1
      || !Number.isSafeInteger(dueAtMs)
      || dueAtMs < 0
    ) return "conflict";
    const now = this.now();
    const effectiveDueAtMs = Math.max(now, dueAtMs);
    if (!Number.isSafeInteger(effectiveDueAtMs)) return "conflict";
    const result = await this.redis.eval(RESCHEDULE_CLAIMED_JOB_LUA, {
      keys: [this.jobKey(job.jobId), this.jobLeaseKey(job.jobId), this.readyJobKey()],
      arguments: [leaseToken, String(job.stateVersion), String(now), String(effectiveDueAtMs), job.jobId],
    });
    if (result === 1 || result === "1") return "scheduled";
    if (result === 2 || result === "2") return "removed";
    return "conflict";
  }

  /** Removes a known terminal or reconciliation-only job from automatic work. */
  public async removeJobFromQueue(jobId: string): Promise<void> {
    if (!isSafeRecordString(jobId, 160)) return;
    await this.redis.zRem(this.readyJobKey(), jobId);
  }

  public decryptJobPayload(job: StudioJobRecord): StudioJobPayload {
    if (!job.encryptedPayload) throw new Error("job payload is unavailable");
    return this.cipher.decrypt<StudioJobPayload>(job.encryptedPayload, `job:${job.jobId}`);
  }

  /**
   * The encrypted admission snapshot has an absolute wall-clock deadline. A
 * provider submit must leave a bounded window to persist its encrypted
 * request reference without extending that privacy lifetime. The final
 * reference TTL is nevertheless always read from Redis atomically at commit;
 * this local clock calculation is only an earlier fail-closed gate.
   */
  public jobPayloadRemainingMs(job: Pick<StudioJobRecord, "createdAtMs">): number {
    const expiresAtMs = job.createdAtMs + STUDIO_JOB_PAYLOAD_TTL_MS;
    if (!Number.isSafeInteger(expiresAtMs)) return 0;
    return Math.max(0, expiresAtMs - this.now());
  }

  public async transitionJob(jobId: string, from: StudioJobState, to: StudioJobState): Promise<StudioJobRecord | null> {
    const job = await this.getJob(jobId);
    if (!job || job.state !== from || !canTransitionJob(from, to)) return null;
    const result = await this.redis.eval(COMPARE_AND_TRANSITION_JOB_LUA, {
      keys: [
        this.jobKey(jobId),
        this.jobPayloadKey(jobId),
        this.activeJobKey(job.senderHash),
        this.readyJobKey(),
        this.jobResultCursorKey(jobId),
      ],
      arguments: [from, String(job.stateVersion), to, String(this.now()), String(TERMINAL_METADATA_TTL_SECONDS), job.jobId, isTerminalJobState(to) ? "1" : "0"],
    });
    const next = parseStored<StudioJobRecord>(typeof result === "string" ? result : null);
    if (!next) return null;
    return this.hydrateJobPayload(next);
  }

  /**
   * Cancels only an `admitted` job.  The job payload, active marker, scheduler
   * entry, and cancellation notice commit together, so a crash cannot leave a
   * user-visible cancellation without stopping the pre-submit work (or stop
   * it silently without a durable notification intent).
   */
  public async cancelAdmittedJobWithNotification(
    input: CancelAdmittedJobWithNotificationInput,
  ): Promise<StudioAdmittedJobCancellationResult> {
    if (
      input.job.state !== "admitted"
      || !isSafeRecordString(input.job.jobId, 160)
      || !isPositiveSafeInteger(input.job.stateVersion)
      || !isSafeRecordString(input.notification.outboxId, 160)
      || !isValidInitialJobNotification(input.notification, "text")
    ) return "unavailable";

    let jobPayload: StudioJobPayload;
    try {
      jobPayload = this.decryptJobPayload(input.job as StudioJobRecord);
    } catch {
      return "unavailable";
    }
    // `/cancel` is allowed to reply to its own triggering bubble, but it must
    // stay on the job's original chat and sending line. A same-sender command
    // from another conversation must never reroute or cancel paid work.
    if (!isCancellationNotificationBoundToJob(input.notification, jobPayload)) return "unavailable";

    const notification = this.buildOutboxRecord({
      outboxId: input.notification.outboxId,
      jobId: input.job.jobId,
      senderHash: input.job.senderHash,
      lane: "request-response",
      sequence: 1,
      payload: input.notification.payload,
    });
    const result = await this.redis.eval(CANCEL_ADMITTED_JOB_WITH_NOTIFICATION_LUA, {
      keys: [
        this.activeJobKey(input.job.senderHash),
        this.jobKey(input.job.jobId),
        this.jobPayloadKey(input.job.jobId),
        this.readyJobKey(),
        this.outboxKey(notification.outboxId),
        this.outboxPayloadKey(notification.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(notification),
        this.releaseFenceKey(),
      ],
      arguments: [
        input.job.jobId,
        input.job.senderHash,
        String(input.job.stateVersion),
        JSON.stringify(outboxMetadata(notification)),
        JSON.stringify(requiredOutboxPayload(notification)),
        String(OUTBOX_PAYLOAD_TTL_SECONDS),
        String(notification.dueAtMs),
        notification.outboxId,
        String(notification.sequence),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
      ],
    });
    if (result === 1 || result === "1" || result === 2 || result === "2") return "canceled";
    if (result === 0 || result === "0") return "not_admitted";
    if (result === -1 || result === "-1") return "release_blocked";
    return "unavailable";
  }

  /**
   * Creates the single provider subrun from immutable encrypted admission
   * evidence. This operation is deliberately idempotent: a worker crash after
   * changing a job to `executing` cannot cause a second paid subrun to exist.
   */
  public async ensureProviderSubrun(
    job: StudioJobRecord,
    snapshot: StudioExecutionSnapshot,
  ): Promise<StudioProviderSubrunRecord> {
    if (!isExecutionSnapshotForJob(snapshot, job)) {
      throw new Error("provider execution snapshot is invalid");
    }
    const subrunId = deriveOpaqueId(this.secret, "studio-provider-subrun", job.jobId);
    const template: StudioProviderSubrunRecord = Object.freeze({
      schemaVersion: 1,
      subrunId,
      jobId: job.jobId,
      provider: snapshot.model.provider,
      modelSlug: snapshot.model.slug,
      workflowId: snapshot.workflow.workflowId,
      stepId: snapshot.workflow.stepId,
      releaseStamp: normalizeReleaseStamp(snapshot.releaseStamp),
      submissionIntentId: deriveOpaqueId(this.secret, "studio-provider-intent", subrunId),
      state: "reserved",
      stateVersion: 1,
      createdAtMs: this.now(),
      updatedAtMs: this.now(),
    });
    const created = await this.redis.set(this.providerSubrunKey(subrunId), JSON.stringify(template), { nx: true });
    if (created === "OK") return template;

    const existing = await this.getProviderSubrun(subrunId);
    if (!existing || !sameProviderSubrunIdentity(existing, template)) {
      throw new Error("provider subrun collision");
    }
    return existing;
  }

  public async getProviderSubrun(subrunId: string): Promise<StudioProviderSubrunRecord | null> {
    if (!isOpaqueStudioIdentifier(subrunId)) return null;
    const record = parseStored<StudioProviderSubrunRecord>(await this.redis.get(this.providerSubrunKey(subrunId)));
    return record && isProviderSubrunRecord(record) ? Object.freeze(record) : null;
  }

  /**
   * Looks up Air's one deterministic provider subrun for a job without ever
   * opening its encrypted provider-request reference. A mismatched/corrupt
   * record is unavailable rather than being treated as a different job's
   * provider capability.
   */
  public async getProviderSubrunForJob(jobId: string): Promise<StudioProviderSubrunRecord | null> {
    if (!isSafeRecordString(jobId, 160)) return null;
    const subrunId = deriveOpaqueId(this.secret, "studio-provider-subrun", jobId);
    const subrun = await this.getProviderSubrun(subrunId);
    return subrun?.jobId === jobId ? subrun : null;
  }

  /**
   * A lease spans only the bounded provider call. The final write verifies the
   * token atomically, so a late worker can never overwrite a newer recovery
   * decision after its lease has expired.
   */
  public async acquireProviderSubrunLease(subrunId: string): Promise<RedisLease | null> {
    if (!isOpaqueStudioIdentifier(subrunId)) return null;
    return acquireRedisLease(this.redis, this.providerSubrunLeaseKey(subrunId), {
      now: this.now,
      ttlMs: LEASE_TTL_MS,
    });
  }

  /**
   * Persists the provider submission intent and performs the final durable
   * pre-call release/job-state check. A recovered `submitting` record is made
   * `submission_unknown`, never retried. An external provider call cannot be
   * atomically coupled to this Redis transition, so a rollback after `ready`
   * is handled as reconciliation rather than a claimed prevention.
   */
  public async beginProviderSubmission(
    subrunId: string,
    leaseToken: string,
    job: Pick<StudioJobRecord, "jobId" | "state" | "stateVersion">,
  ): Promise<StudioProviderSubmissionBeginResult> {
    if (job.state !== "executing") return Object.freeze({ kind: "unavailable" });
    const result = await this.redis.eval(BEGIN_PROVIDER_SUBMISSION_LUA, {
      keys: [
        this.providerSubrunKey(subrunId),
        this.providerSubrunLeaseKey(subrunId),
        this.releaseFenceKey(),
        this.jobKey(job.jobId),
      ],
      arguments: [
        leaseToken,
        String(this.now()),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        String(TERMINAL_METADATA_TTL_SECONDS),
        job.jobId,
        String(job.stateVersion),
      ],
    });
    return parseProviderSubmissionBeginResult(typeof result === "string" ? result : null);
  }

  /**
   * Commits one provider outcome under the same lease that authorized the
   * physical call. Accepted request IDs remain encrypted and expire with the
   * immutable 24-hour job privacy horizon; opaque ambiguity metadata remains
   * recoverable for operator reconciliation.
   */
  public async finishProviderSubmission(
    subrunId: string,
    leaseToken: string,
    job: Pick<StudioJobRecord, "jobId" | "state" | "stateVersion">,
    outcome: StudioProviderSubmissionOutcome,
  ): Promise<StudioProviderSubrunRecord | null> {
    if (!isOpaqueStudioIdentifier(subrunId)) return null;
    if (outcome.kind === "accepted" && !isSafeProviderRequestId(outcome.providerRequestId)) {
      // A malformed "accepted" response cannot safely become a known
      // submission. Preserve the durable intent as unknown rather than
      // throwing and leaving a replay-shaped record behind.
      return this.finishProviderSubmission(subrunId, leaseToken, job, { kind: "ambiguous" });
    }
    const subrun = await this.getProviderSubrun(subrunId);
    if (!subrun || subrun.jobId !== job.jobId) return null;
    const encryptedReference = outcome.kind === "accepted"
      ? this.cipher.encrypt({ providerRequestId: outcome.providerRequestId }, `provider-subrun:${subrunId}`)
      : undefined;
    const result = await this.redis.eval(FINISH_PROVIDER_SUBMISSION_LUA, {
      keys: [
        this.providerSubrunKey(subrunId),
        this.providerSubrunReferenceKey(subrunId),
        this.providerSubrunLeaseKey(subrunId),
        this.jobPayloadKey(subrun.jobId),
        this.jobKey(subrun.jobId),
        this.releaseFenceKey(),
      ],
      arguments: [
        leaseToken,
        outcome.kind,
        encryptedReference === undefined ? "" : JSON.stringify(encryptedReference),
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        job.jobId,
        String(job.stateVersion),
      ],
    });
    const record = parseStored<StudioProviderSubrunRecord>(typeof result === "string" ? result : null);
    return record && isProviderSubrunRecord(record) ? Object.freeze(record) : null;
  }

  /**
   * Creates the deterministic, content-free observation record only after a
   * provider submission is durably known. The reviewed polling policy is
   * copied once and can never be loosened by a later worker or deployment.
   */
  public async ensureProviderObservation(
    subrun: StudioProviderSubrunRecord,
    snapshot: StudioExecutionSnapshot,
  ): Promise<StudioProviderObservationRecord | null> {
    if (!isProviderSubrunRecord(subrun) || subrun.state !== "submitted") return null;
    const currentSubrun = await this.getProviderSubrun(subrun.subrunId);
    if (
      !currentSubrun
      || currentSubrun.state !== "submitted"
      || currentSubrun.stateVersion !== subrun.stateVersion
      || !sameProviderSubrunIdentity(currentSubrun, subrun)
    ) return null;

    const job = await this.getJob(currentSubrun.jobId);
    if (
      !job
      || job.jobId !== currentSubrun.jobId
      || job.state !== "executing"
      || !job.encryptedPayload
      || !isExecutionSnapshotForJob(snapshot, job)
      || currentSubrun.provider !== snapshot.model.provider
      || currentSubrun.modelSlug !== snapshot.model.slug
      || currentSubrun.workflowId !== snapshot.workflow.workflowId
      || currentSubrun.stepId !== snapshot.workflow.stepId
      || !sameReleaseStamp(currentSubrun.releaseStamp, snapshot.releaseStamp)
    ) return null;

    // A known submission is observable only while its encrypted provider
    // request reference remains available. This never decrypts or copies the
    // provider ID into observation metadata.
    if (await this.redis.pttl(this.providerSubrunReferenceKey(currentSubrun.subrunId)) < 1) return null;
    const deadlineAtMs = addSafeMilliseconds(currentSubrun.updatedAtMs, snapshot.polling.deadlineMs);
    if (deadlineAtMs === null) return null;

    const observationId = this.providerObservationId(currentSubrun.subrunId);
    const now = this.now();
    const template: StudioProviderObservationRecord = Object.freeze({
      schemaVersion: 1,
      observationId,
      subrunId: currentSubrun.subrunId,
      jobId: job.jobId,
      subrunStateVersion: currentSubrun.stateVersion,
      jobStateVersion: job.stateVersion,
      state: "scheduled",
      pollAttempts: 0,
      maxPollAttempts: snapshot.polling.maxPollAttempts,
      requestTimeoutMs: snapshot.polling.requestTimeoutMs,
      pollIntervalMs: snapshot.polling.pollIntervalMs,
      deadlineAtMs,
      nextDueAtMs: currentSubrun.updatedAtMs,
      stateVersion: 1,
      createdAtMs: now,
      updatedAtMs: now,
    });
    const created = await this.redis.set(this.providerObservationKey(observationId), JSON.stringify(template), { nx: true });
    if (created === "OK") return template;
    const existing = await this.getProviderObservation(currentSubrun.subrunId);
    if (!existing || !sameProviderObservationIdentity(existing, template)) return null;
    return existing;
  }

  /** Reads the durable, content-free polling state for one provider subrun. */
  public async getProviderObservation(subrunId: string): Promise<StudioProviderObservationRecord | null> {
    if (!isOpaqueStudioIdentifier(subrunId)) return null;
    const observationId = this.providerObservationId(subrunId);
    const record = parseStored<StudioProviderObservationRecord>(await this.redis.get(this.providerObservationKey(observationId)));
    if (!record || !isProviderObservationRecord(record)) return null;
    if (record.observationId !== observationId || record.subrunId !== subrunId) return null;
    return Object.freeze(record);
  }

  /** One lease spans at most one safe provider GET/poll. */
  public async acquireProviderObservationLease(subrunId: string): Promise<RedisLease | null> {
    if (!isOpaqueStudioIdentifier(subrunId)) return null;
    return acquireRedisLease(this.redis, this.providerObservationLeaseKey(this.providerObservationId(subrunId)), {
      now: this.now,
      ttlMs: LEASE_TTL_MS,
    });
  }

  /**
   * Fences and records a physical provider GET before it begins. A stale
   * `observing` state is intentionally safe to re-enter after lease expiry:
   * it counts a new bounded read attempt, never a provider submission.
   */
  public async beginProviderObservation(
    subrunId: string,
    leaseToken: string,
  ): Promise<StudioProviderObservationBeginResult> {
    if (!isOpaqueStudioIdentifier(subrunId) || !isSafeRecordString(leaseToken, 256)) {
      return Object.freeze({ kind: "unavailable" });
    }
    const observationId = this.providerObservationId(subrunId);
    const observation = await this.getProviderObservation(subrunId);
    if (!observation) return Object.freeze({ kind: "unavailable" });
    const result = await this.redis.eval(BEGIN_PROVIDER_OBSERVATION_LUA, {
      keys: [
        this.providerObservationKey(observationId),
        this.providerObservationLeaseKey(observationId),
        this.providerSubrunKey(subrunId),
        this.providerSubrunReferenceKey(subrunId),
        this.jobKey(observation.jobId),
      ],
      arguments: [
        leaseToken,
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
        observationId,
        subrunId,
        observation.jobId,
      ],
    });
    return parseProviderObservationBeginResult(typeof result === "string" ? result : null);
  }

  /**
   * Persists the outcome of one already-authorized provider GET. `pending`
   * schedules another safe read under the original bounds; this method never
   * transitions a subrun back toward submission or accepts provider content.
   */
  public async finishProviderObservation(
    subrunId: string,
    leaseToken: string,
    outcome: StudioProviderObservationFinishOutcome,
  ): Promise<StudioProviderObservationFinishResult> {
    if (
      !isOpaqueStudioIdentifier(subrunId)
      || !isSafeRecordString(leaseToken, 256)
      || !isProviderObservationFinishOutcome(outcome)
    ) return Object.freeze({ kind: "unavailable" });
    const observationId = this.providerObservationId(subrunId);
    const observation = await this.getProviderObservation(subrunId);
    if (!observation) return Object.freeze({ kind: "unavailable" });
    const result = await this.redis.eval(FINISH_PROVIDER_OBSERVATION_LUA, {
      keys: [
        this.providerObservationKey(observationId),
        this.providerObservationLeaseKey(observationId),
        this.providerSubrunKey(subrunId),
        this.jobKey(observation.jobId),
      ],
      arguments: [
        leaseToken,
        outcome.kind,
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
        observationId,
        subrunId,
        observation.jobId,
        outcome.kind === "failed" ? outcome.failureDisposition ?? "failed" : "",
      ],
    });
    return parseProviderObservationFinishResult(typeof result === "string" ? result : null);
  }

  /**
   * Creates the opaque upload record before a Photon upload is allowed to
   * start. The ID is deterministic for one `{job, output}` pair, so a retry
   * can only observe the same preparation state; it can never allocate a
   * second physical-upload intent.
   */
  public async ensureJobResultPreparation(
    jobId: string,
    outputIndex: number,
  ): Promise<StudioJobResultPreparationRecord | null> {
    if (!isSafeRecordString(jobId, 160) || !isPositiveSafeInteger(outputIndex) || outputIndex > 4) return null;
    const job = await this.getJob(jobId);
    // A fresh Photon upload is only meaningful while the provider result is
    // still being observed. Existing prepared records remain readable after
    // this state changes through `getJobResultPreparation` below.
    if (
      !job
      || job.jobId !== jobId
      || job.state !== "executing"
      || outputIndex > job.outputCount
      || !job.encryptedPayload
    ) return null;

    const preparationId = this.jobResultPreparationId(jobId, outputIndex);
    const now = this.now();
    const template: StudioJobResultPreparationRecord = Object.freeze({
      schemaVersion: 1,
      preparationId,
      jobId,
      outputIndex,
      state: "reserved",
      stateVersion: 1,
      createdAtMs: now,
      updatedAtMs: now,
    });
    const created = await this.redis.set(this.jobResultPreparationKey(preparationId), JSON.stringify(template), { nx: true });
    if (created === "OK") return template;
    const existing = await this.getJobResultPreparation(jobId, outputIndex);
    if (!existing || !sameJobResultPreparationIdentity(existing, template)) return null;
    return existing;
  }

  /** Reads content-free upload state for one deterministic provider output. */
  public async getJobResultPreparation(
    jobId: string,
    outputIndex: number,
  ): Promise<StudioJobResultPreparationRecord | null> {
    if (!isSafeRecordString(jobId, 160) || !isPositiveSafeInteger(outputIndex) || outputIndex > 4) return null;
    const preparationId = this.jobResultPreparationId(jobId, outputIndex);
    const record = parseStored<StudioJobResultPreparationRecord>(await this.redis.get(this.jobResultPreparationKey(preparationId)));
    if (!record || !isJobResultPreparationRecord(record)) return null;
    if (record.preparationId !== preparationId || record.jobId !== jobId || record.outputIndex !== outputIndex) return null;
    return Object.freeze(record);
  }

  /** One lease spans only the upload-to-durable-GUID boundary. */
  public async acquireJobResultPreparationLease(
    jobId: string,
    outputIndex: number,
  ): Promise<RedisLease | null> {
    if (!isSafeRecordString(jobId, 160) || !isPositiveSafeInteger(outputIndex) || outputIndex > 4) return null;
    return acquireRedisLease(this.redis, this.jobResultPreparationLeaseKey(this.jobResultPreparationId(jobId, outputIndex)), {
      now: this.now,
      ttlMs: LEASE_TTL_MS,
    });
  }

  /**
   * Resolves only the stale pre-upload crash boundary under a preparation
   * lease that the caller has already acquired. A live uploader keeps that
   * lease, so this method can never turn `reserved` into `uploading` or make a
   * Photon call. If it finds an old `uploading` marker, the physical upload is
   * ambiguous and the active job is atomically fenced into reconciliation.
   */
  public async recoverExpiredJobResultPreparation(
    jobId: string,
    outputIndex: number,
    leaseToken: string,
  ): Promise<StudioJobResultPreparationRecoveryResult> {
    if (
      !isSafeRecordString(jobId, 160)
      || !isPositiveSafeInteger(outputIndex)
      || outputIndex > 4
      || !isSafeRecordString(leaseToken, 256)
    ) return Object.freeze({ kind: "unavailable" });
    const preparationId = this.jobResultPreparationId(jobId, outputIndex);
    const result = await this.redis.eval(RECOVER_EXPIRED_JOB_RESULT_PREPARATION_LUA, {
      keys: [
        this.jobResultPreparationKey(preparationId),
        this.jobResultPreparationReferenceKey(preparationId),
        this.jobResultPreparationLeaseKey(preparationId),
        this.jobKey(jobId),
        this.readyJobKey(),
      ],
      arguments: [leaseToken, String(this.now()), jobId, String(outputIndex)],
    });
    return parseJobResultPreparationRecoveryResult(typeof result === "string" ? result : null);
  }

  /**
   * Writes `uploading` before the Photon RPC. If a predecessor's lease has
   * expired while that state is present, the operation is irrevocably marked
   * `upload_unknown` and the job is fenced into reconciliation instead of
   * uploading the provider artifact a second time.
   */
  public async beginJobResultPreparation(
    job: Pick<StudioJobRecord, "jobId" | "state" | "stateVersion">,
    outputIndex: number,
    leaseToken: string,
  ): Promise<StudioJobResultPreparationBeginResult> {
    if (
      !isSafeRecordString(job.jobId, 160)
      || !isPositiveSafeInteger(outputIndex)
      || outputIndex > 4
      || !isSafeRecordString(leaseToken, 256)
    ) return Object.freeze({ kind: "unavailable" });
    const preparationId = this.jobResultPreparationId(job.jobId, outputIndex);
    const result = await this.redis.eval(BEGIN_JOB_RESULT_PREPARATION_LUA, {
      keys: [
        this.jobResultPreparationKey(preparationId),
        this.jobResultPreparationReferenceKey(preparationId),
        this.jobResultPreparationLeaseKey(preparationId),
        this.releaseFenceKey(),
        this.jobKey(job.jobId),
        this.jobPayloadKey(job.jobId),
        this.readyJobKey(),
        this.jobResultCursorKey(job.jobId),
      ],
      arguments: [
        leaseToken,
        String(this.now()),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        String(TERMINAL_METADATA_TTL_SECONDS),
        job.jobId,
        String(job.stateVersion),
        String(STUDIO_RESULT_PREPARATION_COMMIT_WINDOW_MS),
        String(outputIndex),
      ],
    });
    return parseJobResultPreparationBeginResult(typeof result === "string" ? result : null);
  }

  /**
   * Commits the local outcome after the upload RPC. A successful GUID is
   * encrypted under the preparation ID and receives the job payload's
   * remaining TTL. The result cursor/outbox is intentionally a later,
   * recoverable step: a crash after this commit can publish without another
   * Photon upload.
   */
  public async finishJobResultPreparation(
    job: Pick<StudioJobRecord, "jobId" | "state" | "stateVersion">,
    outputIndex: number,
    leaseToken: string,
    outcome: StudioJobResultPreparationFinishOutcome,
  ): Promise<StudioJobResultPreparationFinishResult> {
    if (
      !isSafeRecordString(job.jobId, 160)
      || !isPositiveSafeInteger(outputIndex)
      || outputIndex > 4
      || !isSafeRecordString(leaseToken, 256)
    ) return Object.freeze({ kind: "unavailable" });
    const preparationId = this.jobResultPreparationId(job.jobId, outputIndex);
    const normalizedOutcome = outcome.kind === "prepared" && !isStudioPreparedOutput(outcome.output)
      ? ({ kind: "upload_unknown" } as const)
      : outcome;
    const encryptedReference = normalizedOutcome.kind === "prepared"
      ? this.cipher.encrypt({ output: normalizedOutcome.output }, `job-result-preparation:${preparationId}`)
      : undefined;
    const result = await this.redis.eval(FINISH_JOB_RESULT_PREPARATION_LUA, {
      keys: [
        this.jobResultPreparationKey(preparationId),
        this.jobResultPreparationReferenceKey(preparationId),
        this.jobResultPreparationLeaseKey(preparationId),
        this.jobKey(job.jobId),
        this.jobPayloadKey(job.jobId),
        this.readyJobKey(),
      ],
      arguments: [
        leaseToken,
        normalizedOutcome.kind,
        encryptedReference === undefined ? "" : JSON.stringify(encryptedReference),
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
        job.jobId,
        String(outputIndex),
      ],
    });
    return parseJobResultPreparationFinishResult(typeof result === "string" ? result : null);
  }

  /**
   * Returns the GUID only from a completed encrypted preparation record. A
   * caller cannot manufacture this shape from an inbound attachment or a
   * provider URL; the durable reference must be present and decrypt cleanly.
   */
  public async getPreparedJobResultOutput(
    jobId: string,
    outputIndex: number,
  ): Promise<StudioPreparedOutput | null> {
    const preparation = await this.getJobResultPreparation(jobId, outputIndex);
    if (!preparation || preparation.state !== "prepared") return null;
    const encrypted = parseStored<EncryptedPayload>(
      await this.redis.get(this.jobResultPreparationReferenceKey(preparation.preparationId)),
    );
    if (!encrypted) return null;
    try {
      const reference = this.cipher.decrypt<StudioJobResultPreparationReference>(
        encrypted,
        `job-result-preparation:${preparation.preparationId}`,
      );
      return isStudioPreparedOutput(reference.output) ? Object.freeze(reference.output) : null;
    } catch {
      return null;
    }
  }

  /**
   * Publishes an already-durable Photon GUID through the existing sequential
   * result cursor. It never receives provider bytes or URLs and therefore
   * cannot re-run the upload side effect after a recovery.
   */
  public async publishPreparedJobResult(
    jobId: string,
    outputIndex: number,
  ): Promise<StudioJobResultPublishResult> {
    if (!isSafeRecordString(jobId, 160) || !isPositiveSafeInteger(outputIndex) || outputIndex > 4) {
      return Object.freeze({ kind: "unavailable" });
    }
    // Once the cursor exists it is the durable publication authority, even
    // after the short-lived encrypted Photon reference has expired. This is
    // the recovery path for a crash after cursor publication but before a
    // caller observed its response.
    const existingCursor = await this.getJobResultCursor(jobId);
    const existingOutput = existingCursor?.outputs.find((output) => output.outputIndex === outputIndex);
    if (existingCursor && existingOutput) {
      const outbox = await this.getOutbox(existingOutput.outboxId);
      if (outbox) return Object.freeze({ kind: "existing", cursor: existingCursor, outbox });
    }
    const output = await this.getPreparedJobResultOutput(jobId, outputIndex);
    if (!output) return Object.freeze({ kind: "unavailable" });
    const job = await this.getJob(jobId);
    if (!job) return Object.freeze({ kind: "unavailable" });
    let payload: StudioJobPayload;
    try {
      payload = this.decryptJobPayload(job);
    } catch {
      return Object.freeze({ kind: "unavailable" });
    }
    return this.publishJobResult({
      jobId,
      outputIndex,
      payload: Object.freeze({
        kind: "prepared-output" as const,
        outputIndex,
        output,
        thread: Object.freeze({
          chatGuid: payload.chatGuid,
          sendingLine: payload.sendingLine,
          ...(payload.replyTargetId === undefined ? {} : { replyTargetId: payload.replyTargetId }),
        }),
      }),
    });
  }

  /**
   * Atomically turns one already-prepared native Photon output into Air's
   * first result-delivery bundle. This is deliberately the only success path
   * which advances a provider observation: it proves the same submitted
   * subrun/executing-job fences that authorized the GET, records the text
   * preamble and attachment together, then marks the job ready.
   *
   * The method receives no provider material. It opens only the encrypted
   * Photon GUID that a prior upload transaction has already committed, and
   * writes that GUID back only inside the attachment's encrypted outbox
   * payload. A crash after that upload but before this call can therefore
   * recover from a `scheduled` or `observing` observation without a second
   * provider request, poll, or upload.
   */
  public async finalizePreparedProviderResult(
    subrunId: string,
    observationLeaseToken: string,
  ): Promise<StudioPreparedProviderResultFinalizeResult> {
    if (!isOpaqueStudioIdentifier(subrunId) || !isSafeRecordString(observationLeaseToken, 256)) {
      return Object.freeze({ kind: "unavailable" });
    }

    const existing = await this.readPreparedProviderResultFinalization(subrunId);
    const subrun = await this.getProviderSubrun(subrunId);
    if (!subrun) return Object.freeze({ kind: "unavailable" });
    const job = await this.getJob(subrun.jobId);
    if (!job || job.jobId !== subrun.jobId) return Object.freeze({ kind: "unavailable" });
    const observation = await this.getProviderObservation(subrunId);
    if (!observation || observation.jobId !== subrun.jobId) return Object.freeze({ kind: "unavailable" });

    const preambleId = this.jobResultPreambleOutboxId(subrun.jobId);
    const attachmentId = this.jobResultOutboxId(subrun.jobId, 1);
    let preambleMetadata = "";
    let preamblePayload = "";
    let attachmentMetadata = "";
    let attachmentPayload = "";

    if (!existing) {
      if (
        job.state !== "executing"
        || job.outputCount !== 1
        || !job.encryptedPayload
      ) return Object.freeze({ kind: "unavailable" });

      const preparation = await this.getJobResultPreparation(job.jobId, 1);
      const output = await this.getPreparedJobResultOutput(job.jobId, 1);
      if (!output) {
        // Distinguish a known privacy-lifetime expiry from a malformed or
        // unavailable prepared reference without exposing either value.
        if (preparation?.state === "prepared") {
          const [jobPayloadTtl, preparedReferenceTtl] = await Promise.all([
            this.redis.pttl(this.jobPayloadKey(job.jobId)),
            this.redis.pttl(this.jobResultPreparationReferenceKey(preparation.preparationId)),
          ]);
          if (jobPayloadTtl < 1 || preparedReferenceTtl < 1) {
            return Object.freeze({ kind: "payload_expiring" });
          }
        }
        return Object.freeze({ kind: "unavailable" });
      }

      let payload: StudioJobPayload;
      try {
        payload = this.decryptJobPayload(job);
      } catch {
        return Object.freeze({ kind: "payload_expiring" });
      }
      const thread = Object.freeze({
        chatGuid: payload.chatGuid,
        sendingLine: payload.sendingLine,
        ...(payload.replyTargetId === undefined ? {} : { replyTargetId: payload.replyTargetId }),
      });
      try {
        const preamble = this.buildOutboxRecord({
          outboxId: preambleId,
          jobId: job.jobId,
          senderHash: job.senderHash,
          lane: "job-result",
          sequence: 0,
          payload: Object.freeze({
            kind: "text" as const,
            text: STUDIO_RESULT_PREAMBLE_TEXT,
            thread,
          }),
        });
        const attachment = this.buildOutboxRecord({
          outboxId: attachmentId,
          jobId: job.jobId,
          resultOutputIndex: 1,
          senderHash: job.senderHash,
          lane: "job-result",
          sequence: 1,
          payload: Object.freeze({
            kind: "prepared-output" as const,
            outputIndex: 1,
            output,
            thread,
          }),
        }, undefined, undefined, true);
        preambleMetadata = JSON.stringify(outboxMetadata(preamble));
        preamblePayload = JSON.stringify(requiredOutboxPayload(preamble));
        attachmentMetadata = JSON.stringify(outboxMetadata(attachment));
        attachmentPayload = JSON.stringify(requiredOutboxPayload(attachment));
      } catch {
        return Object.freeze({ kind: "unavailable" });
      }
    }

    const result = await this.redis.eval(FINALIZE_PREPARED_PROVIDER_RESULT_LUA, {
      keys: [
        this.providerObservationKey(observation.observationId),
        this.providerObservationLeaseKey(observation.observationId),
        this.providerSubrunKey(subrunId),
        this.jobKey(subrun.jobId),
        this.jobPayloadKey(subrun.jobId),
        this.jobResultPreparationKey(this.jobResultPreparationId(subrun.jobId, 1)),
        this.jobResultPreparationReferenceKey(this.jobResultPreparationId(subrun.jobId, 1)),
        this.jobResultCursorKey(subrun.jobId),
        this.outboxKey(preambleId),
        this.outboxPayloadKey(preambleId),
        this.outboxKey(attachmentId),
        this.outboxPayloadKey(attachmentId),
        this.readyOutboxKey(),
        this.outboxLaneKey({
          outboxId: preambleId,
          jobId: subrun.jobId,
          senderHash: job.senderHash,
          lane: "job-result",
        }),
        this.readyJobKey(),
        this.releaseFenceKey(),
      ],
      arguments: [
        observationLeaseToken,
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
        observation.observationId,
        subrunId,
        subrun.jobId,
        preambleId,
        attachmentId,
        preambleMetadata,
        preamblePayload,
        attachmentMetadata,
        attachmentPayload,
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
      ],
    });
    const outcome = parsePreparedProviderResultFinalizeResult(typeof result === "string" ? result : null);
    if (outcome.kind !== "finalized" && outcome.kind !== "existing") return outcome;
    const finalization = await this.readPreparedProviderResultFinalization(subrunId);
    return finalization
      ? Object.freeze({ kind: outcome.kind, ...finalization })
      : Object.freeze({ kind: "unavailable" });
  }

  /** Read the encrypted provider reference only when a bounded recovery needs it. */
  public async getProviderSubrunReference(subrunId: string): Promise<StudioProviderSubrunReference | null> {
    if (!isOpaqueStudioIdentifier(subrunId)) return null;
    const encrypted = parseStored<EncryptedPayload>(await this.redis.get(this.providerSubrunReferenceKey(subrunId)));
    if (!encrypted) return null;
    try {
      return Object.freeze(this.cipher.decrypt<StudioProviderSubrunReference>(encrypted, `provider-subrun:${subrunId}`));
    } catch {
      return null;
    }
  }

  /**
   * Derives a non-reversible operator audit identity from an operator-supplied
   * label.  The raw label is never accepted by any durable record, log, or
   * reconciliation primitive.
   */
  public operatorAuditHash(operatorIdentifier: string): string {
    if (!isSafeRecordString(operatorIdentifier, 160)) {
      throw new Error("operator identity is invalid");
    }
    return deriveOpaqueId(this.secret, "studio-reconciliation-operator", operatorIdentifier);
  }

  /** Read one opaque reconciliation audit record. It contains no user or provider content. */
  public async getReconciliationAudit(auditId: string): Promise<StudioReconciliationAuditRecord | null> {
    if (!isOpaqueStudioIdentifier(auditId)) return null;
    const record = parseStored<StudioReconciliationAuditRecord>(await this.redis.get(this.reconciliationAuditKey(auditId)));
    return record && isReconciliationAuditRecord(record) ? Object.freeze(record) : null;
  }

  /**
   * Attaches a verified provider request ID to an ambiguous subrun without
   * submitting anything.  Both the job and subrun versions are compared in
   * one Redis transaction, then the job returns only to observation-ready
   * `executing`; the executor sees `submitted` and cannot resubmit it.
   */
  public async attachProviderRequestForReconciliation(
    input: AttachProviderRequestForReconciliationInput,
  ): Promise<StudioReconciliationResult> {
    if (
      !isOpaqueStudioIdentifier(input.subrunId)
      || !isPositiveSafeInteger(input.expectedSubrunVersion)
      || !isPositiveSafeInteger(input.expectedJobVersion)
      || !isSafeProviderRequestId(input.providerRequestId)
      || !isOpaqueStudioIdentifier(input.operatorHash)
    ) return Object.freeze({ kind: "unavailable" });

    const subrun = await this.getProviderSubrun(input.subrunId);
    const job = subrun ? await this.getJob(subrun.jobId) : null;
    if (!subrun || !job || subrun.subrunId !== input.subrunId) return Object.freeze({ kind: "unavailable" });
    const audit = this.buildReconciliationAudit({
      actorHash: input.operatorHash,
      action: "provider_request_attached",
      subjectId: subrun.subrunId,
    });
    const encryptedReference = this.cipher.encrypt(
      { providerRequestId: input.providerRequestId },
      `provider-subrun:${subrun.subrunId}`,
    );
    const result = await this.redis.eval(ATTACH_PROVIDER_REQUEST_RECONCILIATION_LUA, {
      keys: [
        this.providerSubrunKey(input.subrunId),
        this.providerSubrunReferenceKey(input.subrunId),
        this.providerSubrunLeaseKey(input.subrunId),
        this.jobKey(subrun.jobId),
        this.jobPayloadKey(subrun.jobId),
        this.reconciliationAuditKey(audit.auditId),
        this.readyJobKey(),
      ],
      arguments: [
        String(input.expectedSubrunVersion),
        String(input.expectedJobVersion),
        input.subrunId,
        subrun.jobId,
        JSON.stringify(encryptedReference),
        JSON.stringify(audit),
        String(this.now()),
      ],
    });
    return reconciliationResult(result, audit.auditId, undefined, { payloadExpired: true });
  }

  /**
   * Terminalizes an unreconciled provider submission without creating another
   * provider request.  The reservation remains in immutable job metadata;
   * only the short-lived execution payload/reference is removed.
   */
  public async terminalizeProviderSubmissionForReconciliation(
    input: TerminalizeProviderSubmissionForReconciliationInput,
  ): Promise<StudioReconciliationResult> {
    if (
      !isOpaqueStudioIdentifier(input.subrunId)
      || !isPositiveSafeInteger(input.expectedSubrunVersion)
      || !isPositiveSafeInteger(input.expectedJobVersion)
      || !isProviderTerminalizationReason(input.reason)
      || !isOpaqueStudioIdentifier(input.operatorHash)
    ) return Object.freeze({ kind: "unavailable" });

    const subrun = await this.getProviderSubrun(input.subrunId);
    const job = subrun ? await this.getJob(subrun.jobId) : null;
    if (!subrun || !job || subrun.subrunId !== input.subrunId) return Object.freeze({ kind: "unavailable" });
    const audit = this.buildReconciliationAudit({
      actorHash: input.operatorHash,
      action: "provider_terminalized",
      subjectId: subrun.subrunId,
      reason: input.reason,
    });
    const result = await this.redis.eval(TERMINALIZE_PROVIDER_RECONCILIATION_LUA, {
      keys: [
        this.providerSubrunKey(input.subrunId),
        this.providerSubrunReferenceKey(input.subrunId),
        this.providerSubrunLeaseKey(input.subrunId),
        this.jobKey(subrun.jobId),
        this.jobPayloadKey(subrun.jobId),
        this.activeJobKey(job.senderHash),
        this.reconciliationAuditKey(audit.auditId),
        this.readyJobKey(),
      ],
      arguments: [
        String(input.expectedSubrunVersion),
        String(input.expectedJobVersion),
        input.subrunId,
        subrun.jobId,
        JSON.stringify(audit),
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
      ],
    });
    return reconciliationResult(result, audit.auditId);
  }

  /**
   * Reads the compact result cursor and hydrates each individual delivery
   * status from its linked outbox. A malformed or missing linked intent fails
   * closed rather than making a caller guess whether an artifact was sent.
   */
  public async getJobResultCursor(jobId: string): Promise<StudioJobResultCursor | null> {
    if (!isSafeRecordString(jobId, 160)) return null;
    const metadata = await this.getJobResultCursorMetadata(jobId);
    if (!metadata) return null;
    const outputs: StudioJobResultOutputStatus[] = [];
    for (const output of metadata.outputs) {
      const outbox = await this.getOutbox(output.outboxId);
      if (
        !outbox
        || outbox.jobId !== metadata.jobId
        || outbox.senderHash !== metadata.senderHash
        || outbox.lane !== "job-result"
        || outbox.resultOutputIndex !== output.outputIndex
      ) return null;
      outputs.push(Object.freeze({
        outputIndex: output.outputIndex,
        outboxId: output.outboxId,
        state: outbox.state,
        attempts: outbox.attempts,
        stateVersion: outbox.stateVersion,
        updatedAtMs: outbox.updatedAtMs,
      }));
    }
    return Object.freeze({
      schemaVersion: metadata.schemaVersion,
      jobId: metadata.jobId,
      senderHash: metadata.senderHash,
      threadHash: metadata.threadHash,
      outputCount: metadata.outputCount,
      releaseStamp: metadata.releaseStamp,
      nextOutputIndex: metadata.nextOutputIndex,
      outputs: Object.freeze(outputs),
      createdAtMs: metadata.createdAtMs,
      updatedAtMs: metadata.updatedAtMs,
    });
  }

  /**
   * Reads only the sequence-zero result preamble metadata. This deliberately
   * strips the encrypted payload so delivery reconciliation can inspect the
   * physical-send state without opening a route, message text, or media GUID.
   */
  public async getJobResultPreamble(jobId: string): Promise<StudioOutboxRecord | null> {
    if (!isSafeRecordString(jobId, 160)) return null;
    const job = await this.getJob(jobId);
    if (!job || job.jobId !== jobId) return null;
    const record = await this.getOutbox(this.jobResultPreambleOutboxId(jobId));
    if (!isPreparedProviderResultPreamble(record, job, this.jobResultPreambleOutboxId(jobId))) return null;
    const { encryptedPayload: _encryptedPayload, ...metadata } = record;
    return Object.freeze(metadata);
  }

  /**
   * Atomically appends exactly one one-based provider output to the job's
   * result cursor and publishes its attachment outbox intent. The cursor keeps
   * the immutable admission stamp; the physical intent deliberately receives
   * this deployment's fenced stamp so an approved recovery deployment can
   * finish delivery after a rollback without reopening provider admission.
   */
  public async publishJobResult(
    input: PublishStudioJobResultInput,
  ): Promise<StudioJobResultPublishResult> {
    if (!isSafeRecordString(input.jobId, 160) || !isPositiveSafeInteger(input.outputIndex) || input.outputIndex > 4) {
      return Object.freeze({ kind: "unavailable" });
    }
    const job = await this.getJob(input.jobId);
    if (!job || job.jobId !== input.jobId || input.outputIndex > job.outputCount) {
      return Object.freeze({ kind: "unavailable" });
    }

    // A delivery cursor is authoritative after the first publication. This
    // makes a provider-poll retry idempotent even after the job's private
    // admission payload has expired or the job has terminalized.
    const existingCursor = await this.getJobResultCursor(job.jobId);
    const existingOutput = existingCursor?.outputs.find((output) => output.outputIndex === input.outputIndex);
    if (existingCursor && existingOutput) {
      const outbox = await this.getOutbox(existingOutput.outboxId);
      if (!outbox) return Object.freeze({ kind: "unavailable" });
      return Object.freeze({ kind: "existing", cursor: existingCursor, outbox });
    }

    let jobPayload: StudioJobPayload;
    try {
      jobPayload = this.decryptJobPayload(job);
    } catch {
      return Object.freeze({ kind: "unavailable" });
    }
    if (!isJobResultPayloadBoundToJob(input.payload, input.outputIndex, jobPayload)) {
      return Object.freeze({ kind: "unavailable" });
    }

    const outboxId = this.jobResultOutboxId(job.jobId, input.outputIndex);
    let outbox: StudioOutboxRecord;
    try {
      outbox = this.buildOutboxRecord({
        outboxId,
        jobId: job.jobId,
        resultOutputIndex: input.outputIndex,
        senderHash: job.senderHash,
        lane: "job-result",
        sequence: input.outputIndex,
        payload: input.payload,
      }, undefined, undefined, true);
    } catch {
      return Object.freeze({ kind: "unavailable" });
    }

    const result = await this.redis.eval(PUBLISH_JOB_RESULT_LUA, {
      keys: [
        this.jobKey(job.jobId),
        this.jobResultCursorKey(job.jobId),
        this.outboxKey(outbox.outboxId),
        this.outboxPayloadKey(outbox.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(outbox),
        this.releaseFenceKey(),
      ],
      arguments: [
        job.jobId,
        String(job.stateVersion),
        String(job.releaseStamp.generation),
        job.releaseStamp.phase,
        String(input.outputIndex),
        outbox.outboxId,
        JSON.stringify(outboxMetadata(outbox)),
        JSON.stringify(requiredOutboxPayload(outbox)),
        String(OUTBOX_PAYLOAD_TTL_SECONDS),
        String(outbox.dueAtMs),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        String(this.now()),
      ],
    });
    const outcome = parseJobResultPublication(result);
    if (outcome.kind === "out_of_order") {
      return Object.freeze({ kind: "out_of_order", expectedOutputIndex: outcome.expectedOutputIndex });
    }
    if (outcome.kind === "release_blocked") return Object.freeze({ kind: "release_blocked" });
    if (outcome.kind !== "created" && outcome.kind !== "existing") {
      return Object.freeze({ kind: "unavailable" });
    }
    const cursor = await this.getJobResultCursor(job.jobId);
    const currentOutput = cursor?.outputs.find((output) => output.outputIndex === input.outputIndex);
    const persistedOutbox = currentOutput ? await this.getOutbox(currentOutput.outboxId) : null;
    if (!cursor || !persistedOutbox) return Object.freeze({ kind: "unavailable" });
    return Object.freeze({
      kind: outcome.kind === "created" ? "published" : "existing",
      cursor,
      outbox: persistedOutbox,
    });
  }

  public async createOutbox(input: CreateStudioOutboxInput): Promise<StudioOutboxRecord> {
    if (input.resultOutputIndex !== undefined) {
      throw new Error("job result outboxes require the durable result cursor");
    }
    const record = this.buildOutboxRecord(input);
    const result = await this.persistOutbox(record);
    if (result === "created") return record;
    if (result === "blocked") throw new Error("unfenced outbox was unexpectedly blocked");
    return this.existingOutbox(record.outboxId);
  }

  /**
   * Persists the only acknowledgement that claims a media-first attachment
   * was saved. Both publication and later claim require the same live guard,
   * so a dark/rollback receipt wins over a stale active worker.
   */
  public async createPendingGuardedOutbox(
    guard: PendingWriteGuard,
    input: CreateStudioOutboxInput,
  ): Promise<PendingGuardedOutboxResult> {
    const record = this.buildOutboxRecord(input, guard);
    const result = await this.redis.eval(PERSIST_PENDING_GUARDED_OUTBOX_LUA, {
      keys: [
        this.pendingKey(record.senderHash, guard.threadHash),
        this.pendingControlKey(record.senderHash, guard.threadHash),
        this.outboxKey(record.outboxId),
        this.outboxPayloadKey(record.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(record),
        this.inboxScopeSequenceKey(record.senderHash, guard.threadHash),
        this.releaseFenceKey(),
      ],
      arguments: [
        guard.receiptId,
        String(guard.scopeSequence),
        String(guard.revision),
        guard.token,
        String(guard.expiresAtMs),
        JSON.stringify(outboxMetadata(record)),
        JSON.stringify(requiredOutboxPayload(record)),
        String(OUTBOX_PAYLOAD_TTL_SECONDS),
        String(this.now()),
        record.outboxId,
        String(record.sequence),
        String(record.dueAtMs),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
      ],
    });
    if (result === -2 || result === "-2") return Object.freeze({ kind: "maintenance" });
    if (result === -3 || result === "-3") return Object.freeze({ kind: "cleared" });
    if (result === -4 || result === "-4") return Object.freeze({ kind: "expired" });
    if (result === -5 || result === "-5") return Object.freeze({ kind: "superseded" });
    if (result === -6 || result === "-6" || result === -7 || result === "-7" || result === -8 || result === "-8") {
      return Object.freeze({ kind: "unavailable" });
    }
    if (result === 1 || result === "1") return Object.freeze({ kind: "persisted", record });
    if (result === -1 || result === "-1") {
      throw new Error("outbox sequence is already in use for this sender lane");
    }
    const existing = await this.existingOutbox(record.outboxId);
    return samePendingGuard(existing.pendingGuard, guard)
      ? Object.freeze({ kind: "persisted", record: existing })
      : Object.freeze({ kind: "unavailable" });
  }

  private buildOutboxRecord(
    input: CreateStudioOutboxInput,
    pendingGuard?: PendingWriteGuard,
    laneIdentity?: string,
    allowResultOutput = false,
  ): StudioOutboxRecord {
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 0) {
      throw new Error("outbox sequence is invalid");
    }
    if (!isStudioOutboxPayload(input.payload)) {
      throw new Error("outbox payload is invalid");
    }
    const now = this.now();
    const dueAtMs = input.dueAtMs ?? now;
    if (!Number.isSafeInteger(dueAtMs) || dueAtMs < 0) {
      throw new Error("outbox due time is invalid");
    }
    if (laneIdentity !== undefined && !isSafeRecordString(laneIdentity, 160)) {
      throw new Error("outbox lane identity is invalid");
    }
    if (input.resultOutputIndex !== undefined) {
      if (
        !allowResultOutput
        || input.lane !== "job-result"
        || input.jobId === undefined
        || !isPositiveSafeInteger(input.resultOutputIndex)
        || input.sequence !== input.resultOutputIndex
      ) {
        throw new Error("outbox result cursor binding is invalid");
      }
    }
    const record: StudioOutboxRecord = Object.freeze({
      schemaVersion: 3,
      outboxId: input.outboxId,
      ...(input.jobId === undefined ? {} : { jobId: input.jobId }),
      ...(input.resultOutputIndex === undefined ? {} : { resultOutputIndex: input.resultOutputIndex }),
      senderHash: input.senderHash,
      lane: input.lane,
      ...(laneIdentity === undefined ? {} : { laneIdentity }),
      sequence: input.sequence,
      // Every physical send starts as a durable intent. Callers cannot create
      // a pre-sent/retryable record and accidentally bypass this invariant.
      state: "intent",
      attempts: 0,
      stateVersion: 1,
      releaseStamp: this.releaseStamp,
      ...(pendingGuard === undefined ? {} : { pendingGuard }),
      encryptedPayload: this.cipher.encrypt(input.payload, `outbox:${input.outboxId}`),
      createdAtMs: now,
      dueAtMs,
      updatedAtMs: now,
    });
    return record;
  }

  private async persistOutbox(record: StudioOutboxRecord): Promise<"created" | "existing" | "blocked"> {
    const result = await this.redis.eval(PERSIST_OUTBOX_LUA, {
      keys: [this.outboxKey(record.outboxId), this.outboxPayloadKey(record.outboxId), this.readyOutboxKey(), this.outboxLaneKey(record), this.releaseFenceKey()],
      arguments: [
        JSON.stringify(outboxMetadata(record)),
        JSON.stringify(requiredOutboxPayload(record)),
        String(OUTBOX_PAYLOAD_TTL_SECONDS),
        String(record.dueAtMs),
        record.outboxId,
        String(record.sequence),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
      ],
    });
    if (result === -1 || result === "-1") {
      throw new Error("outbox sequence is already in use for this sender lane");
    }
    if (result === -2 || result === "-2" || result === -3 || result === "-3") return "blocked";
    return result === 1 || result === "1" ? "created" : "existing";
  }

  private async existingOutbox(outboxId: string): Promise<StudioOutboxRecord> {
    const existing = await this.getOutbox(outboxId);
    if (!existing) throw new Error("outbox publication could not be confirmed");
    return existing;
  }

  private async getJobResultCursorMetadata(jobId: string): Promise<StudioJobResultCursorMetadata | null> {
    const metadata = parseStored<StudioJobResultCursorMetadata>(await this.redis.get(this.jobResultCursorKey(jobId)));
    return metadata && isJobResultCursorMetadata(metadata) ? metadata : null;
  }

  /**
   * Re-reads a completed finalization from durable metadata only. The helper
   * intentionally does not decrypt either delivery payload, so an idempotent
   * recovery cannot surface or extend the prepared Photon reference.
   */
  private async readPreparedProviderResultFinalization(
    subrunId: string,
  ): Promise<StudioPreparedProviderResultFinalization | null> {
    const subrun = await this.getProviderSubrun(subrunId);
    if (!subrun || subrun.state !== "submitted") return null;
    const [job, observation, cursor] = await Promise.all([
      this.getJob(subrun.jobId),
      this.getProviderObservation(subrunId),
      this.getJobResultCursor(subrun.jobId),
    ]);
    if (
      !job
      || job.jobId !== subrun.jobId
      || job.state !== "ready"
      || job.outputCount !== 1
      || !isValidReleaseStamp(job.releaseStamp)
      || !observation
      || observation.subrunId !== subrunId
      || observation.jobId !== subrun.jobId
      || observation.state !== "succeeded"
      || observation.subrunStateVersion !== subrun.stateVersion
      || observation.jobStateVersion >= Number.MAX_SAFE_INTEGER
      || job.stateVersion !== observation.jobStateVersion + 1
      || !cursor
      || cursor.jobId !== job.jobId
      || cursor.outputCount !== 1
      || cursor.nextOutputIndex !== 2
      || cursor.outputs.length !== 1
      || cursor.outputs[0]?.outputIndex !== 1
      || !sameReleaseStamp(cursor.releaseStamp, job.releaseStamp)
    ) return null;

    const preambleId = this.jobResultPreambleOutboxId(job.jobId);
    const attachment = cursor.outputs[0];
    if (!attachment) return null;
    const [preamble, attachmentOutbox] = await Promise.all([
      this.getOutbox(preambleId),
      this.getOutbox(attachment.outboxId),
    ]);
    if (
      !isPreparedProviderResultPreamble(preamble, job, preambleId)
      || !isPreparedProviderResultAttachment(attachmentOutbox, job, attachment.outboxId)
      || !sameReleaseStamp(preamble.releaseStamp, attachmentOutbox.releaseStamp)
    ) return null;
    return Object.freeze({ job, observation, preamble, attachment: attachmentOutbox, cursor });
  }

  private buildReconciliationAudit(input: {
    readonly actorHash: string;
    readonly action: StudioReconciliationAuditAction;
    readonly subjectId: string;
    readonly relatedId?: string | undefined;
    readonly reason?: StudioProviderTerminalizationReason | undefined;
  }): StudioReconciliationAuditRecord {
    return Object.freeze({
      schemaVersion: 1,
      auditId: randomBytes(32).toString("hex"),
      actorHash: input.actorHash,
      action: input.action,
      subjectId: input.subjectId,
      ...(input.relatedId === undefined ? {} : { relatedId: input.relatedId }),
      ...(input.reason === undefined ? {} : { reason: input.reason }),
      occurredAtMs: this.now(),
    });
  }

  private async terminalizeExpiredConfirmedAbsentOutbox(
    input: RequeueConfirmedAbsentOutboxInput,
    source: Pick<StudioOutboxRecord, "outboxId" | "senderHash" | "lane" | "jobId">,
  ): Promise<StudioReconciliationResult> {
    const audit = this.buildReconciliationAudit({
      actorHash: input.operatorHash,
      action: "outbox_artifact_expired",
      subjectId: input.outboxId,
    });
    const expired = await this.redis.eval(TERMINALIZE_EXPIRED_OUTBOX_RECONCILIATION_LUA, {
      keys: [
        this.outboxKey(input.outboxId),
        this.outboxPayloadKey(input.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(source),
        this.reconciliationAuditKey(audit.auditId),
      ],
      arguments: [
        String(input.expectedOutboxVersion),
        input.outboxId,
        JSON.stringify(audit),
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
      ],
    });
    if (expired === 1 || expired === "1") {
      return Object.freeze({ kind: "payload_expired", auditId: audit.auditId });
    }
    if (expired === -1 || expired === "-1") return Object.freeze({ kind: "conflict" });
    return Object.freeze({ kind: "unavailable" });
  }

  private async hydrateJobPayload(metadata: StudioJobRecord): Promise<StudioJobRecord> {
    // New job metadata is deliberately content-free. Ignore any legacy inline
    // field so a read cannot extend or re-expose an older retained snapshot.
    const { encryptedPayload: _legacyEncryptedPayload, ...safeMetadata } = metadata;
    const encryptedPayload = parseStored<EncryptedPayload>(await this.redis.get(this.jobPayloadKey(metadata.jobId)));
    return encryptedPayload ? Object.freeze({ ...safeMetadata, encryptedPayload }) : Object.freeze(safeMetadata);
  }

  public async getOutbox(outboxId: string): Promise<StudioOutboxRecord | null> {
    const metadata = parseStored<StudioOutboxRecord>(await this.redis.get(this.outboxKey(outboxId)));
    if (!metadata) return null;
    const encryptedPayload = parseStored<EncryptedPayload>(await this.redis.get(this.outboxPayloadKey(outboxId)));
    return encryptedPayload ? Object.freeze({ ...metadata, encryptedPayload }) : metadata;
  }

  public decryptOutboxPayload(record: StudioOutboxRecord): StudioOutboxPayload {
    if (!record.encryptedPayload) throw new Error("outbox payload is unavailable");
    return this.cipher.decrypt<StudioOutboxPayload>(record.encryptedPayload, `outbox:${record.outboxId}`);
  }

  public async claimNextOutbox(): Promise<{ readonly record: StudioOutboxRecord; readonly leaseToken: string } | null> {
    const due = await this.redis.zRangeByScore(this.readyOutboxKey(), Number.NEGATIVE_INFINITY, this.now(), { limit: 50 });
    for (const outboxId of due) {
      const beforeClaim = await this.getOutbox(outboxId);
      if (!beforeClaim) {
        await this.redis.zRem(this.readyOutboxKey(), outboxId);
        continue;
      }
      const token = randomBytes(16).toString("base64url");
      const claimed = await this.claimOutbox(outboxId, token, beforeClaim);
      if (claimed) return Object.freeze({ record: claimed, leaseToken: token });

      const current = await this.getOutbox(outboxId);
      if (!current) {
        await this.redis.zRem(this.readyOutboxKey(), outboxId);
      } else if (current.state === "sending") {
        // The worker may have reached the physical iMessage call before it
        // crashed. Treat the lease expiry as ambiguous and stop this lane.
        await this.reconcileExpiredOutboxSend(outboxId, current);
      } else if (current.state !== "intent" && current.state !== "retryable_failure") {
        await this.redis.zRem(this.readyOutboxKey(), outboxId);
      }
    }
    return null;
  }

  public async finishOutbox(
    outboxId: string,
    leaseToken: string,
    state: Exclude<StudioOutboxState, "intent" | "sending" | "canceled" | "confirmed_absent" | "requeued">,
  ): Promise<boolean> {
    const record = await this.getOutbox(outboxId);
    if (!record) return false;
    const result = await this.redis.eval(FINISH_OUTBOX_LUA, {
      keys: [this.outboxKey(outboxId), this.outboxPayloadKey(outboxId), this.outboxLeaseKey(outboxId), this.readyOutboxKey(), this.outboxLaneKey(record)],
      arguments: [leaseToken, state, String(this.now()), String(TERMINAL_METADATA_TTL_SECONDS), outboxId, String(STUDIO_MAX_OUTBOX_SEND_ATTEMPTS), String(OUTBOX_RETRY_BACKOFF_MS)],
    });
    return typeof result === "string";
  }

  /**
   * Returns a claimed intent to the durable queue when this worker knows that
   * its shared invocation budget expired before it started any Photon RPC.
   *
   * Claiming deliberately increments the attempt counter so a crashed worker
   * cannot replay a possibly-started send. That accounting must be undone
   * only on this narrow, fenced pre-Photon path; ordinary retryable transport
   * failures continue through `finishOutbox` and consume their retry budget.
   */
  public async deferOutboxBeforePhoton(outboxId: string, leaseToken: string): Promise<boolean> {
    const record = await this.getOutbox(outboxId);
    if (!record) return false;
    const result = await this.redis.eval(DEFER_OUTBOX_BEFORE_PHOTON_LUA, {
      keys: [this.outboxKey(outboxId), this.outboxLeaseKey(outboxId), this.readyOutboxKey(), this.outboxLaneKey(record)],
      arguments: [leaseToken, String(this.now()), outboxId],
    });
    return typeof result === "string";
  }

  /**
   * Resolves a physical iMessage attempt only after an operator has confirmed
   * its outcome.  No send occurs here; a confirmed absence merely makes an
   * explicit later requeue possible.
   */
  public async resolveOutboxSendForReconciliation(
    input: ResolveOutboxSendForReconciliationInput,
  ): Promise<StudioReconciliationResult> {
    if (
      !isOpaqueStudioIdentifier(input.outboxId)
      || !isPositiveSafeInteger(input.expectedOutboxVersion)
      || (input.resolution !== "sent" && input.resolution !== "confirmed_absent")
      || !isOpaqueStudioIdentifier(input.operatorHash)
    ) return Object.freeze({ kind: "unavailable" });

    const outbox = await this.getOutbox(input.outboxId);
    if (!outbox || outbox.outboxId !== input.outboxId) return Object.freeze({ kind: "unavailable" });
    const audit = this.buildReconciliationAudit({
      actorHash: input.operatorHash,
      action: input.resolution === "sent" ? "outbox_sent_confirmed" : "outbox_absence_confirmed",
      subjectId: outbox.outboxId,
    });
    const result = await this.redis.eval(RESOLVE_OUTBOX_SEND_RECONCILIATION_LUA, {
      keys: [
        this.outboxKey(input.outboxId),
        this.outboxPayloadKey(input.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(outbox),
        this.reconciliationAuditKey(audit.auditId),
      ],
      arguments: [
        String(input.expectedOutboxVersion),
        input.resolution,
        JSON.stringify(audit),
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
        input.outboxId,
      ],
    });
    return reconciliationResult(result, audit.auditId);
  }

  /**
   * Creates one fresh, explicitly audited attempt from an operator-confirmed
   * absent send. The ciphertext is re-encrypted under a new opaque outbox ID
   * and inherits the source payload's remaining Redis TTL, never a fresh day.
   */
  public async requeueConfirmedAbsentOutbox(
    input: RequeueConfirmedAbsentOutboxInput,
  ): Promise<StudioReconciliationResult> {
    if (
      !isOpaqueStudioIdentifier(input.outboxId)
      || !isPositiveSafeInteger(input.expectedOutboxVersion)
      || !isOpaqueStudioIdentifier(input.operatorHash)
    ) return Object.freeze({ kind: "unavailable" });

    const source = await this.getOutbox(input.outboxId);
    if (!source || source.outboxId !== input.outboxId) return Object.freeze({ kind: "unavailable" });
    if (!source.encryptedPayload) return this.terminalizeExpiredConfirmedAbsentOutbox(input, source);
    let payload: StudioOutboxPayload;
    try {
      payload = this.decryptOutboxPayload(source);
    } catch {
      return Object.freeze({ kind: "unavailable" });
    }
    const replacementOutboxId = randomBytes(32).toString("hex");
    const laneIdentity = source.lane === "request-response"
      || source.lane === "tapback"
      || ((source.lane === "job-acknowledgement" || source.lane === "job-result") && source.jobId === undefined)
      ? (source.laneIdentity ?? source.outboxId)
      : undefined;
    const replacement = this.buildOutboxRecord({
      outboxId: replacementOutboxId,
      ...(source.jobId === undefined ? {} : { jobId: source.jobId }),
      ...(source.resultOutputIndex === undefined ? {} : { resultOutputIndex: source.resultOutputIndex }),
      senderHash: source.senderHash,
      lane: source.lane,
      sequence: source.sequence,
      payload,
    }, source.pendingGuard, laneIdentity, source.resultOutputIndex !== undefined);
    const audit = this.buildReconciliationAudit({
      actorHash: input.operatorHash,
      action: "outbox_requeued",
      subjectId: source.outboxId,
      relatedId: replacementOutboxId,
    });
    const result = await this.redis.eval(REQUEUE_CONFIRMED_ABSENT_OUTBOX_LUA, {
      keys: [
        this.outboxKey(input.outboxId),
        this.outboxPayloadKey(input.outboxId),
        this.outboxKey(replacement.outboxId),
        this.outboxPayloadKey(replacement.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(replacement),
        this.releaseFenceKey(),
        this.reconciliationAuditKey(audit.auditId),
        source.resultOutputIndex === undefined || source.jobId === undefined
          ? this.unusedJobResultCursorKey()
          : this.jobResultCursorKey(source.jobId),
      ],
      arguments: [
        String(input.expectedOutboxVersion),
        input.outboxId,
        JSON.stringify(outboxMetadata(replacement)),
        JSON.stringify(requiredOutboxPayload(replacement)),
        String(replacement.dueAtMs),
        replacement.outboxId,
        String(replacement.sequence),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        JSON.stringify(audit),
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
      ],
    });
    // The source payload can expire after the read/decrypt above but before
    // the atomic copy. Make that privacy expiry a durable known failure
    // rather than leaving reconciliation outstanding.
    if (result === -2 || result === "-2") {
      return this.terminalizeExpiredConfirmedAbsentOutbox(input, source);
    }
    return reconciliationResult(result, audit.auditId, replacementOutboxId, { releaseBlocked: true });
  }

  /**
   * The delivery worker calls this immediately before any physical transport
   * action. It gives a dark/rollback guard one last atomic chance to cancel a
   * media acknowledgement that was claimed before the release changed.
   */
  public async authorizeOutboxDelivery(record: StudioOutboxRecord, leaseToken: string): Promise<boolean> {
    const pendingGuard = record.pendingGuard;
    const result = await this.redis.eval(AUTHORIZE_OUTBOX_DELIVERY_LUA, {
      keys: [
        this.outboxKey(record.outboxId),
        this.outboxPayloadKey(record.outboxId),
        this.outboxLeaseKey(record.outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(record),
        pendingGuard === undefined
          ? this.unusedPendingControlKey()
          : this.pendingControlKey(record.senderHash, pendingGuard.threadHash),
        pendingGuard === undefined
          ? this.unusedPendingKey()
          : this.pendingKey(record.senderHash, pendingGuard.threadHash),
        pendingGuard === undefined
          ? this.unusedInboxScopeSequenceKey()
          : this.inboxScopeSequenceKey(record.senderHash, pendingGuard.threadHash),
        this.releaseFenceKey(),
      ],
      arguments: [
        leaseToken,
        String(this.now()),
        String(TERMINAL_METADATA_TTL_SECONDS),
        record.outboxId,
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
        String(OUTBOX_RETRY_BACKOFF_MS),
      ],
    });
    return result === 1 || result === "1";
  }

  private async claimInbox(
    receiptId: string,
    leaseToken: string,
    record: Pick<StudioInboxRecord, "senderHash" | "threadHash">,
  ): Promise<StudioInboxRecord | null> {
    const now = this.now();
    const fifoScopeId = this.inboxFifoScopeId(record.senderHash, record.threadHash);
    const result = await this.redis.eval(CLAIM_INBOX_LUA, {
      keys: [
        this.inboxKey(receiptId),
        this.inboxEnvelopeKey(receiptId),
        this.inboxLeaseKey(receiptId),
        this.readyInboxKey(),
        this.inboxFifoMigrationKey(),
        this.inboxFifoScopeQueueKey(fifoScopeId),
        this.inboxFifoScopeCursorKey(fifoScopeId),
      ],
      arguments: [leaseToken, String(LEASE_TTL_MS), String(now), receiptId],
    });
    return parseStored<StudioInboxRecord>(typeof result === "string" ? result : null);
  }

  private async claimOutbox(outboxId: string, leaseToken: string, record: StudioOutboxRecord): Promise<StudioOutboxRecord | null> {
    const now = this.now();
    const pendingGuard = record.pendingGuard;
    const result = await this.redis.eval(CLAIM_OUTBOX_LUA, {
      keys: [
        this.outboxKey(outboxId),
        this.outboxPayloadKey(outboxId),
        this.outboxLeaseKey(outboxId),
        this.readyOutboxKey(),
        this.outboxLaneKey(record),
        pendingGuard === undefined
          ? this.unusedPendingControlKey()
          : this.pendingControlKey(record.senderHash, pendingGuard.threadHash),
        pendingGuard === undefined
          ? this.unusedPendingKey()
          : this.pendingKey(record.senderHash, pendingGuard.threadHash),
        pendingGuard === undefined
          ? this.unusedInboxScopeSequenceKey()
          : this.inboxScopeSequenceKey(record.senderHash, pendingGuard.threadHash),
        this.releaseFenceKey(),
      ],
      arguments: [
        leaseToken,
        String(LEASE_TTL_MS),
        String(now),
        String(TERMINAL_METADATA_TTL_SECONDS),
        outboxId,
        String(STUDIO_MAX_OUTBOX_SEND_ATTEMPTS),
        String(this.releaseStamp.generation),
        this.releaseStamp.phase,
      ],
    });
    return parseStored<StudioOutboxRecord>(typeof result === "string" ? result : null);
  }

  private async reconcileExpiredOutboxSend(outboxId: string, record: StudioOutboxRecord): Promise<void> {
    await this.redis.eval(RECONCILE_EXPIRED_OUTBOX_LUA, {
      keys: [this.outboxKey(outboxId), this.outboxPayloadKey(outboxId), this.outboxLeaseKey(outboxId), this.readyOutboxKey(), this.outboxLaneKey(record)],
      arguments: [String(this.now()), String(TERMINAL_METADATA_TTL_SECONDS), outboxId],
    });
  }

  private async readPendingControl(senderHash: SenderHash, threadHash: ThreadHash): Promise<PendingControl | null> {
    const control = parseStored<PendingControl>(await this.redis.get(this.pendingControlKey(senderHash, threadHash)));
    return control && isPendingControl(control) ? control : null;
  }

  private inboxKey(receiptId: string): string { return `${this.prefix}:inbox:${receiptId}`; }
  private inboxKeyPrefix(): string { return `${this.prefix}:inbox:`; }
  private inboxEnvelopeKey(receiptId: string): string { return `${this.prefix}:inbox:envelope:${receiptId}`; }
  private readyInboxKey(): string { return `${this.prefix}:inbox:ready`; }
  private inboxLeaseKey(receiptId: string): string { return `${this.prefix}:inbox:lease:${receiptId}`; }
  private inboxScopeSequenceKey(senderHash: SenderHash, threadHash: ThreadHash): string {
    return `${this.prefix}:inbox:scope-sequence:${senderHash}:${threadHash}`;
  }
  /**
   * A separate opaque scope namespace avoids turning the existing ingress
   * sequence fence into processing progress. The original counter remains the
   * authority for pending-media/guard revocation.
   */
  private inboxFifoScopeId(senderHash: SenderHash, threadHash: ThreadHash): string {
    return deriveOpaqueId(this.secret, "studio-inbox-fifo-scope", `${senderHash}\u0000${threadHash}`);
  }
  private inboxFifoMigrationKey(): string { return `${this.prefix}:inbox:fifo:migration`; }
  private inboxFifoHoldingScopesKey(): string { return `${this.prefix}:inbox:fifo:holding-scopes`; }
  private inboxFifoScopeQueueKey(scopeId: string): string { return `${this.prefix}:inbox:fifo:queue:${scopeId}`; }
  private inboxFifoScopeCursorKey(scopeId: string): string { return `${this.prefix}:inbox:fifo:cursor:${scopeId}`; }
  private cronHeartbeatKey(scope: StudioCronHeartbeatScope): string {
    return `${this.prefix}:operations:cron-heartbeat:${scope}`;
  }
  private releaseFenceKey(): string { return `${this.prefix}:operations:release-fence`; }
  private canaryBudgetAdmission(policy: StudioCanaryBudgetPolicy | undefined): CanaryBudgetAdmission {
    if (this.releaseStamp.phase !== "canary" || !isValidCanaryBudgetPolicy(policy)) {
      return Object.freeze({
        key: this.unusedCanaryBudgetKey(),
        enabled: "0" as const,
        totalCapCents: 0,
        falCapCents: 0,
      });
    }
    return Object.freeze({
      key: this.canaryBudgetKey(policy.budgetId),
      enabled: "1" as const,
      totalCapCents: policy.totalCapCents,
      falCapCents: policy.falCapCents,
    });
  }
  /** HMAC-derived so the operator budget identifier never appears in Redis. */
  private canaryBudgetKey(budgetId: string): string {
    return `${this.prefix}:canary:budget:${deriveOpaqueId(this.secret, "studio-canary-budget", budgetId)}`;
  }
  /** Never written; keeps fixed Lua key arity when a canary policy is absent. */
  private unusedCanaryBudgetKey(): string { return `${this.prefix}:canary:budget:unused`; }
  private settingsKey(senderHash: SenderHash): string { return `${this.prefix}:settings:${senderHash}`; }
  private onboardingMarkerKey(senderHash: SenderHash, onboardingVersion: number): string {
    return `${this.prefix}:onboarding:marker:${senderHash}:v${onboardingVersion}`;
  }
  private pendingKey(senderHash: SenderHash, threadHash: ThreadHash): string {
    return `${this.prefix}:pending:v3:payload:${senderHash}:${threadHash}`;
  }
  private pendingControlKey(senderHash: SenderHash, threadHash: ThreadHash): string {
    return `${this.prefix}:pending:v3:control:${senderHash}:${threadHash}`;
  }
  /** Never written; keeps generic guarded-outbox Lua keys fixed without a wildcard. */
  private unusedPendingKey(): string { return `${this.prefix}:pending:v3:payload:unused`; }
  private unusedPendingControlKey(): string { return `${this.prefix}:pending:v3:control:unused`; }
  /** Never written; keeps guarded-outbox Lua key arity fixed without a wildcard. */
  private unusedInboxScopeSequenceKey(): string { return `${this.prefix}:inbox:scope-sequence:unused`; }
  private activeJobKey(senderHash: SenderHash): string { return `${this.prefix}:active:${senderHash}`; }
  /** Opaque sender-scoped pointer only; never stores a prompt, route, or media reference. */
  private recentJobKey(senderHash: SenderHash): string { return `${this.prefix}:recent-job:${senderHash}`; }
  private userDailyKey(senderHash: SenderHash, day: string): string { return `${this.prefix}:rate:${day}:${senderHash}`; }
  private dailySpendKey(day: string): string { return `${this.prefix}:spend:${day}`; }
  private jobKey(jobId: string): string { return `${this.prefix}:job:${jobId}`; }
  private jobPayloadKey(jobId: string): string { return `${this.prefix}:job:payload:${jobId}`; }
  private readyJobKey(): string { return `${this.prefix}:job:ready`; }
  private jobLeaseKey(jobId: string): string { return `${this.prefix}:job:lease:${jobId}`; }
  private jobResultCursorKey(jobId: string): string { return `${this.prefix}:job:result-cursor:${jobId}`; }
  private jobResultPreparationId(jobId: string, outputIndex: number): string {
    return deriveOpaqueId(this.secret, "studio-job-result-preparation", `${jobId}\u0000${String(outputIndex)}`);
  }
  private jobResultPreparationKey(preparationId: string): string {
    return `${this.prefix}:job:result-preparation:${preparationId}`;
  }
  private jobResultPreparationReferenceKey(preparationId: string): string {
    return `${this.prefix}:job:result-preparation:reference:${preparationId}`;
  }
  private jobResultPreparationLeaseKey(preparationId: string): string {
    return `${this.prefix}:job:result-preparation:lease:${preparationId}`;
  }
  private jobResultPreambleOutboxId(jobId: string): string {
    return deriveOpaqueId(this.secret, "studio-job-result-preamble", jobId);
  }
  private jobResultOutboxId(jobId: string, outputIndex: number): string {
    return deriveOpaqueId(this.secret, "studio-job-result-outbox", `${jobId}\u0000${String(outputIndex)}`);
  }
  private unusedJobResultCursorKey(): string { return `${this.prefix}:job:result-cursor:unused`; }
  private providerSubrunKey(subrunId: string): string { return `${this.prefix}:provider-subrun:${subrunId}`; }
  private providerSubrunReferenceKey(subrunId: string): string { return `${this.prefix}:provider-subrun:reference:${subrunId}`; }
  private providerSubrunLeaseKey(subrunId: string): string { return `${this.prefix}:provider-subrun:lease:${subrunId}`; }
  private providerObservationId(subrunId: string): string {
    return deriveOpaqueId(this.secret, "studio-provider-observation", subrunId);
  }
  private providerObservationKey(observationId: string): string {
    return `${this.prefix}:provider-observation:${observationId}`;
  }
  private providerObservationLeaseKey(observationId: string): string {
    return `${this.prefix}:provider-observation:lease:${observationId}`;
  }
  private reconciliationAuditKey(auditId: string): string { return `${this.prefix}:reconciliation:audit:${auditId}`; }
  private outboxKey(outboxId: string): string { return `${this.prefix}:outbox:${outboxId}`; }
  private outboxPayloadKey(outboxId: string): string { return `${this.prefix}:outbox:payload:${outboxId}`; }
  private readyOutboxKey(): string { return `${this.prefix}:outbox:ready`; }
  private outboxLeaseKey(outboxId: string): string { return `${this.prefix}:outbox:lease:${outboxId}`; }
  /** Fixed unused keys keep the optional initial-tapback Lua invocation bounded. */
  private unusedInitialAdmissionOutboxKey(): string { return `${this.prefix}:outbox:initial-admission:unused`; }
  private unusedInitialAdmissionOutboxPayloadKey(): string { return `${this.prefix}:outbox:payload:initial-admission:unused`; }
  private unusedInitialAdmissionOutboxLaneKey(): string { return `${this.prefix}:outbox:lane:initial-admission:unused`; }
  /**
   * A lane is only shared where user-visible ordering is meaningful. Independent
   * request/response and generic tapback effects must never block each other.
   * The one exception is a job's initial tapback + acknowledgement pair: they
   * share a lane so Like → text remains ordered across serverless workers.
   */
  private outboxLaneKey(record: Pick<StudioOutboxRecord, "outboxId" | "senderHash" | "lane" | "laneIdentity" | "jobId">): string {
    const base = `${this.prefix}:outbox:lane:${record.senderHash}`;
    switch (record.lane) {
      case "onboarding":
        return `${base}:onboarding`;
      case "job-acknowledgement":
        return `${base}:job-initial:${record.jobId ?? record.laneIdentity ?? record.outboxId}`;
      case "job-result":
        return `${base}:job-result:${record.jobId ?? record.laneIdentity ?? record.outboxId}`;
      case "request-response":
        return `${base}:request:${record.laneIdentity ?? record.outboxId}`;
      case "tapback":
        return record.jobId === undefined
          ? `${base}:tapback:${record.laneIdentity ?? record.outboxId}`
          : `${base}:job-initial:${record.jobId}`;
    }
  }
}

export function createStudioJob(input: Omit<StudioJobRecord, "schemaVersion" | "state" | "stateVersion" | "createdAtMs" | "updatedAtMs" | "encryptedPayload"> & { readonly payload: StudioJobPayload; readonly cipher: QueueCipher; readonly now?: number | undefined }): StudioJobRecordWithPayload {
  const now = input.now ?? Date.now();
  return Object.freeze({
    schemaVersion: 3,
    jobId: input.jobId,
    senderHash: input.senderHash,
    threadHash: input.threadHash,
    mode: input.mode,
    modelSlug: input.modelSlug,
    provider: input.provider,
    outputCount: input.outputCount,
    ...(input.durationSeconds === undefined ? {} : { durationSeconds: input.durationSeconds }),
    reservedCents: input.reservedCents,
    releaseStamp: normalizeReleaseStamp(input.releaseStamp),
    state: "admitted",
    stateVersion: 1,
    createdAtMs: now,
    updatedAtMs: now,
    encryptedPayload: input.cipher.encrypt(input.payload, `job:${input.jobId}`),
  });
}

export function canTransitionJob(from: StudioJobState, to: StudioJobState): boolean {
  return (JOB_TRANSITIONS[from] ?? []).includes(to);
}

export function canTransitionOutbox(from: StudioOutboxState, to: StudioOutboxState): boolean {
  return (OUTBOX_TRANSITIONS[from] ?? []).includes(to);
}

/** Only these states may advance without an explicit operator reconciliation. */
export function isJobQueueEligibleState(state: StudioJobState): boolean {
  // Provider execution and result delivery have separate durable lanes, but
  // they share the job scheduler for bounded recovery. A finalizer moves a
  // job to `ready` only after both result intents exist; retaining ready and
  // delivering jobs lets the engine reconcile their ordered iMessage sends.
  return state === "admitted" || state === "executing" || state === "ready" || state === "delivering";
}

const JOB_TRANSITIONS: Readonly<Record<StudioJobState, readonly StudioJobState[]>> = Object.freeze({
  received: ["validating", "maintenance_rejected", "failed"],
  validating: ["admitted", "failed", "maintenance_rejected"],
  // A recovered admission whose immutable encrypted snapshot has expired has
  // not reached a provider. It may terminalize as a known privacy failure.
  admitted: ["executing", "canceled", "failed"],
  executing: ["executing", "ready", "failed", "canceled", "reconciliation_required"],
  // An expired Photon-upload lease means the remote attachment may exist but
  // its GUID was never committed. A ready result must therefore be able to
  // enter the same no-retry reconciliation state as an executing one.
  ready: ["delivering", "canceled", "reconciliation_required"],
  delivering: ["delivered", "failed", "reconciliation_required"],
  delivered: [],
  failed: [],
  // A provider may have accepted just as cancellation won the shared-state
  // race. Escalate only to a no-retry audit state; this never restores the
  // scrubbed payload, active-job marker, or delivery eligibility.
  canceled: ["reconciliation_required"],
  maintenance_rejected: [],
  reconciliation_required: ["executing", "delivering", "delivered", "failed", "canceled"],
});

const OUTBOX_TRANSITIONS: Readonly<Record<StudioOutboxState, readonly StudioOutboxState[]>> = Object.freeze({
  intent: ["sending", "canceled"],
  // A bounded worker can return a claim to intent only after proving that no
  // Photon RPC began; every other interrupted sending lease stays unknown.
  sending: ["intent", "sent", "retryable_failure", "failed", "canceled", "send_unknown"],
  retryable_failure: ["sending", "failed", "canceled"],
  send_unknown: ["sent", "confirmed_absent"],
  // A verified absence can spawn one separately audited replacement. The
  // source becomes terminal `requeued`, never a fresh physical attempt.
  confirmed_absent: ["requeued", "failed"],
  requeued: [],
  sent: [],
  failed: [],
  canceled: [],
});

const PERSIST_PENDING_LUA = `
-- AIR_STUDIO_PERSIST_PENDING_V3
-- KEYS: encrypted payload, opaque durable control record
-- ARGV: payload, receiptId, scopeSequence, expiresAtMs, guard token, now
local incomingSequence = tonumber(ARGV[3])
local expiresAtMs = tonumber(ARGV[4])
local now = tonumber(ARGV[6])
if not incomingSequence or incomingSequence < 1 or not expiresAtMs or not now then return -6 end

local raw = redis.call('GET', KEYS[2])
local control = nil
if raw then
  control = cjson.decode(raw)
  local currentSequence = tonumber(control.scopeSequence)
  local currentRevision = tonumber(control.revision)
  if not currentSequence or currentSequence < 1 or not currentRevision or currentRevision < 1 then return -6 end
  if control.state == 'active' and (
    not tonumber(control.expiresAtMs) or tonumber(control.expiresAtMs) <= now or redis.call('EXISTS', KEYS[1]) ~= 1
  ) then
    control.state = 'expired'
    control.token = nil
    redis.call('DEL', KEYS[1])
    redis.call('SET', KEYS[2], cjson.encode(control))
  end
  if incomingSequence < currentSequence then
    if control.state == 'maintenance' then return -2 end
    if control.state == 'cleared' then return -3 end
    if control.state == 'expired' then return -4 end
    return -1
  end
  if incomingSequence == currentSequence then
    if control.state == 'maintenance' then return -2 end
    if control.state == 'cleared' then return -3 end
    if control.state == 'expired' then return -4 end
    if control.state ~= 'active' or control.receiptId ~= ARGV[2] or control.token ~= ARGV[5] or tonumber(control.expiresAtMs) ~= expiresAtMs then return -6 end
    return 2
  end
end

local revision = control and (tonumber(control.revision) + 1) or 1
if expiresAtMs <= now then
  redis.call('DEL', KEYS[1])
  redis.call('SET', KEYS[2], cjson.encode({
    schemaVersion = 1,
    state = 'expired',
    receiptId = ARGV[2],
    scopeSequence = incomingSequence,
    revision = revision,
    expiresAtMs = expiresAtMs
  }))
  return -4
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', expiresAtMs - now)
redis.call('SET', KEYS[2], cjson.encode({
  schemaVersion = 1,
  state = 'active',
  receiptId = ARGV[2],
  scopeSequence = incomingSequence,
  revision = revision,
  token = ARGV[5],
  expiresAtMs = expiresAtMs
}))
return 1
`;

const FENCE_PENDING_LUA = `
-- AIR_STUDIO_FENCE_PENDING_V3
-- KEYS: encrypted payload, opaque durable control record
-- ARGV: receiptId, scopeSequence, state(cleared|maintenance), now
local incomingSequence = tonumber(ARGV[2])
local now = tonumber(ARGV[4])
if not incomingSequence or incomingSequence < 1 or not now or (ARGV[3] ~= 'cleared' and ARGV[3] ~= 'maintenance') then return 0 end
local raw = redis.call('GET', KEYS[2])
local control = nil
local revision = 1
local canceled = false
if raw then
  control = cjson.decode(raw)
  local currentSequence = tonumber(control.scopeSequence)
  local currentRevision = tonumber(control.revision)
  if not currentSequence or currentSequence < 1 or not currentRevision or currentRevision < 1 then return 0 end
  if incomingSequence < currentSequence then return 0 end
  if incomingSequence == currentSequence then
    if control.receiptId == ARGV[1] and control.state == ARGV[3] then return 2 end
    return 0
  end
  revision = currentRevision + 1
  canceled = control.state == 'active'
    and tonumber(control.expiresAtMs) ~= nil
    and tonumber(control.expiresAtMs) > now
    and redis.call('EXISTS', KEYS[1]) == 1
end
redis.call('DEL', KEYS[1])
redis.call('SET', KEYS[2], cjson.encode({
  schemaVersion = 1,
  state = ARGV[3],
  receiptId = ARGV[1],
  scopeSequence = incomingSequence,
  revision = revision
}))
return canceled and 1 or 2
`;

const READ_PENDING_LUA = `
-- AIR_STUDIO_READ_PENDING_V3
-- KEYS: encrypted payload, opaque durable control record
-- ARGV: now
local now = tonumber(ARGV[1])
if not now then return false end
local raw = redis.call('GET', KEYS[2])
if not raw then return false end
local control = cjson.decode(raw)
if control.state ~= 'active' then return false end
if not tonumber(control.expiresAtMs) or tonumber(control.expiresAtMs) <= now then
  control.state = 'expired'
  control.token = nil
  redis.call('DEL', KEYS[1])
  redis.call('SET', KEYS[2], cjson.encode(control))
  return false
end
local payload = redis.call('GET', KEYS[1])
if not payload then
  control.state = 'expired'
  control.token = nil
  redis.call('SET', KEYS[2], cjson.encode(control))
  return false
end
return payload
`;

const ADMIT_JOB_LUA = `
-- AIR_STUDIO_ADMIT_JOB_V5
-- KEYS: active sender fence, daily user count, daily spend, job metadata,
--       encrypted job payload, durable job-ready index, release fence,
--       opaque non-expiring canary budget reservation, sender recent-job pointer
-- ARGV: job id, metadata, payload, daily ttl, payload ttl, user limit,
--       spend cap, reserved cents, now, release generation, release phase,
--       canary policy enabled, total cap, fal subcap
local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end

local function keyType(key)
  local response = redis.call('TYPE', key)
  return response and response.ok or ''
end

local function isStringOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'string'
end

local existing = redis.call('GET', KEYS[4])
-- A retry must preserve the original admission even if a later deployment has
-- advanced the shared fence. The current fence governs only a new reservation.
if existing then
  local ok, record = pcall(cjson.decode, existing)
  if ok and type(record) == 'table'
    and (record.state == 'admitted' or record.state == 'executing') then
    redis.call('ZADD', KEYS[6], tonumber(ARGV[9]), ARGV[1])
  end
  return {'existing', existing}
end
if ARGV[11] ~= 'dark' and ARGV[11] ~= 'canary' and ARGV[11] ~= 'production' then
  return {'release_blocked'}
end
local metadata = cjson.decode(ARGV[2])
local stamp = metadata.releaseStamp
if type(stamp) ~= 'table'
  or type(stamp.generation) ~= 'number' or stamp.generation < 0
  or stamp.generation ~= math.floor(stamp.generation)
  or stamp.generation ~= tonumber(ARGV[10])
  or stamp.phase ~= ARGV[11] then return {'release_blocked'} end
local fenceRaw = redis.call('GET', KEYS[7])
if fenceRaw then
  local ok, fence = pcall(cjson.decode, fenceRaw)
  if not ok or type(fence) ~= 'table' or fence.schemaVersion ~= 1
    or type(fence.generation) ~= 'number' or fence.generation < 0
    or fence.generation ~= math.floor(fence.generation)
    or (fence.phase ~= 'dark' and fence.phase ~= 'canary' and fence.phase ~= 'production') then return {'release_blocked'} end
  if fence.generation ~= tonumber(ARGV[10]) or fence.phase ~= ARGV[11] then return {'release_blocked'} end
elseif ARGV[11] ~= 'dark' then
  return {'release_blocked'}
end
if not isStringOrMissing(KEYS[9]) then return {'storage_error'} end

-- The canary record is shared by every sender/mode/provider for this opaque
-- approved budget. It is touched only after the live release fence accepts
-- this exact admission, so dark/rollback/stale workers cannot reserve money.
local canaryBudget = nil
if ARGV[11] == 'canary' then
  if ARGV[12] ~= '1' then return {'canary_budget_required'} end
  local totalCap = tonumber(ARGV[13])
  local falCap = tonumber(ARGV[14])
  local cost = tonumber(ARGV[8])
  if not nonnegativeInteger(totalCap) or not nonnegativeInteger(falCap)
    or not nonnegativeInteger(cost)
    or (metadata.provider ~= 'gmi' and metadata.provider ~= 'fal')
    or not isStringOrMissing(KEYS[8]) then return {'storage_error'} end
  local totalReserved = 0
  local falReserved = 0
  local budgetRaw = redis.call('GET', KEYS[8])
  if budgetRaw then
    local okBudget, budget = pcall(cjson.decode, budgetRaw)
    if not okBudget or type(budget) ~= 'table' or budget.schemaVersion ~= 1
      or not nonnegativeInteger(budget.totalReservedCents)
      or not nonnegativeInteger(budget.falReservedCents) then return {'storage_error'} end
    totalReserved = budget.totalReservedCents
    falReserved = budget.falReservedCents
  end
  if totalReserved + cost > totalCap then return {'canary_total_cap'} end
  if metadata.provider == 'fal' and falReserved + cost > falCap then return {'canary_fal_cap'} end
  canaryBudget = {
    schemaVersion = 1,
    totalReservedCents = totalReserved + cost,
    falReservedCents = metadata.provider == 'fal' and falReserved + cost or falReserved
  }
end

if redis.call('EXISTS', KEYS[5]) == 1 then return {'storage_error'} end
local active = redis.call('GET', KEYS[1])
if active then return {'active_job'} end
local userCount = tonumber(redis.call('GET', KEYS[2]) or '0')
if userCount >= tonumber(ARGV[6]) then return {'user_limit'} end
local spend = tonumber(redis.call('GET', KEYS[3]) or '0')
local cost = tonumber(ARGV[8])
if spend + cost > tonumber(ARGV[7]) then return {'daily_cap'} end
-- A nonterminal job never loses its active fence solely because time passed.
-- Recovery/operator reconciliation must resolve it explicitly.
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], tostring(userCount + 1), 'EX', ARGV[4])
redis.call('SET', KEYS[3], tostring(spend + cost), 'EX', ARGV[4])
-- Keep opaque admission metadata indefinitely while nonterminal. The separate
-- encrypted snapshot contains prompt/media/thread references and is never
-- refreshed beyond its hard 24-hour privacy deadline.
redis.call('SET', KEYS[4], ARGV[2])
redis.call('SET', KEYS[5], ARGV[3], 'EX', ARGV[5])
redis.call('SET', KEYS[9], ARGV[1])
redis.call('ZADD', KEYS[6], tonumber(ARGV[9]), ARGV[1])
if canaryBudget then redis.call('SET', KEYS[8], cjson.encode(canaryBudget)) end
return {'admitted'}
`;

const ADMIT_JOB_WITH_INITIAL_NOTIFICATIONS_LUA = `
-- AIR_STUDIO_ADMIT_JOB_WITH_INITIAL_NOTIFICATIONS_V2
-- KEYS: active sender fence, daily user count, daily spend, job metadata,
--       encrypted job payload, durable job-ready index, release fence,
--       outbox-ready index, acknowledgement metadata/payload/lane,
--       optional tapback metadata/payload/lane, opaque canary budget,
--       sender recent-job pointer, inbox FIFO migration marker
-- ARGV: job id, metadata, payload, daily ttl, job payload ttl, user limit,
--       daily spend cap, reserved cents, now, release generation/phase,
--       per-job cap, acknowledgement metadata/payload/id/sequence/due,
--       optional tapback metadata/payload/id/sequence/due/flag, outbox ttl,
--       canary policy enabled, total cap, fal subcap
local function keyType(key)
  local response = redis.call('TYPE', key)
  return response and response.ok or ''
end

local function isStringOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'string'
end

local function isZsetOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'zset'
end

local function validStamp(stamp)
  return type(stamp) == 'table'
    and type(stamp.generation) == 'number'
    and stamp.generation >= 0
    and stamp.generation == math.floor(stamp.generation)
    and (stamp.phase == 'dark' or stamp.phase == 'canary' or stamp.phase == 'production')
    and (stamp.phase == 'dark' or stamp.generation >= 1)
end

local function sameStamp(left, right)
  return validStamp(left) and validStamp(right)
    and left.generation == right.generation and left.phase == right.phase
end

local function validNotificationManifest(manifest, acknowledgementId, tapbackId, hasTapback)
  if type(manifest) ~= 'table' or manifest.schemaVersion ~= 1
    or type(manifest.acknowledgementOutboxId) ~= 'string'
    or manifest.acknowledgementOutboxId ~= acknowledgementId then return false end
  if hasTapback then
    return type(manifest.tapbackOutboxId) == 'string' and manifest.tapbackOutboxId == tapbackId
  end
  return manifest.tapbackOutboxId == nil
end

-- Existing acknowledgements may already be sent and have their encrypted
-- payload scrubbed. Their immutable metadata is sufficient to prove the
-- bundle was created before the job was allowed back onto the scheduler.
local function validExistingOutbox(record, outboxId, jobId, senderHash, lane, stamp, sequence)
  return type(record) == 'table'
    and record.outboxId == outboxId
    and record.jobId == jobId
    and record.senderHash == senderHash
    and record.lane == lane
    and type(record.sequence) == 'number' and record.sequence == sequence
    and sameStamp(record.releaseStamp, stamp)
end

local function validNewOutbox(record, outboxId, jobId, senderHash, lane, stamp, sequence, dueAt)
  return validExistingOutbox(record, outboxId, jobId, senderHash, lane, stamp, sequence)
    and record.state == 'intent'
    and record.attempts == 0
    and record.stateVersion == 1
    and record.dueAtMs == dueAt
end

local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end

local function positiveInteger(value)
  return nonnegativeInteger(value) and value >= 1
end

-- An active release may create a billable job only after the one-way inbox
-- ordering migration has reached FIFO. Missing, malformed, legacy, and
-- holding markers are all a no-spend stop rather than a best-effort state.
local function inboxFifoActive()
  local raw = redis.call('GET', KEYS[17])
  if not raw then return false end
  local ok, marker = pcall(cjson.decode, raw)
  return ok and type(marker) == 'table'
    and marker.schemaVersion == 1 and marker.phase == 'fifo'
end

local jobId = ARGV[1]
local okCandidate, candidate = pcall(cjson.decode, ARGV[2])
if not okCandidate or type(candidate) ~= 'table'
  or candidate.jobId ~= jobId
  or type(candidate.senderHash) ~= 'string'
  or type(candidate.threadHash) ~= 'string' then return {'storage_error'} end

local hasTapback = ARGV[23] == '1'
if not hasTapback and ARGV[23] ~= '0' then return {'storage_error'} end
local expectedAcknowledgementSequence = hasTapback and 2 or 1
local acknowledgementId = ARGV[15]
local tapbackId = ARGV[20]

-- The existing branch is deliberately before every release/cap/fence check:
-- a deterministic webhook retry must recover its original reservation even
-- after policy or deployment state has changed.
if not isStringOrMissing(KEYS[4]) then return {'storage_error'} end
local existing = redis.call('GET', KEYS[4])
if existing then
  local okExisting, existingRecord = pcall(cjson.decode, existing)
  if not okExisting or type(existingRecord) ~= 'table'
    or existingRecord.jobId ~= jobId
    or existingRecord.senderHash ~= candidate.senderHash
    or existingRecord.threadHash ~= candidate.threadHash
    or not validStamp(existingRecord.releaseStamp)
    or not validNotificationManifest(existingRecord.initialNotifications, acknowledgementId, tapbackId, hasTapback) then
    return {'notification_incomplete'}
  end
  if not isStringOrMissing(KEYS[9])
    or (hasTapback and not isStringOrMissing(KEYS[12])) then return {'notification_incomplete'} end
  local acknowledgementRaw = redis.call('GET', KEYS[9])
  local okAcknowledgement, acknowledgement = pcall(cjson.decode, acknowledgementRaw or '')
  if not acknowledgementRaw or not okAcknowledgement
    or not validExistingOutbox(
      acknowledgement,
      acknowledgementId,
      jobId,
      candidate.senderHash,
      'job-acknowledgement',
      existingRecord.releaseStamp,
      expectedAcknowledgementSequence
    ) then return {'notification_incomplete'} end
  if hasTapback then
    local tapbackRaw = redis.call('GET', KEYS[12])
    local okTapback, tapback = pcall(cjson.decode, tapbackRaw or '')
    if not tapbackRaw or not okTapback
      or not validExistingOutbox(
        tapback,
        tapbackId,
        jobId,
        candidate.senderHash,
        'tapback',
        existingRecord.releaseStamp,
        1
      ) then return {'notification_incomplete'} end
  end
  if (existingRecord.state == 'admitted' or existingRecord.state == 'executing') then
    if not isZsetOrMissing(KEYS[6]) then return {'notification_incomplete'} end
    redis.call('ZADD', KEYS[6], tonumber(ARGV[9]), jobId)
  end
  return {'existing', existing}
end

local dailyTtl = tonumber(ARGV[4])
local jobPayloadTtl = tonumber(ARGV[5])
local userLimit = tonumber(ARGV[6])
local dailySpendCap = tonumber(ARGV[7])
local reservedCents = tonumber(ARGV[8])
local now = tonumber(ARGV[9])
local releaseGeneration = tonumber(ARGV[10])
local perJobCap = tonumber(ARGV[12])
local acknowledgementSequence = tonumber(ARGV[16])
local acknowledgementDueAt = tonumber(ARGV[17])
local tapbackSequence = tonumber(ARGV[21])
local tapbackDueAt = tonumber(ARGV[22])
local outboxPayloadTtl = tonumber(ARGV[24])
if not positiveInteger(dailyTtl) or not positiveInteger(jobPayloadTtl)
  or not positiveInteger(userLimit) or not nonnegativeInteger(dailySpendCap)
  or not nonnegativeInteger(reservedCents) or not nonnegativeInteger(now)
  or not nonnegativeInteger(releaseGeneration) or not nonnegativeInteger(perJobCap)
  or acknowledgementSequence ~= expectedAcknowledgementSequence or not nonnegativeInteger(acknowledgementDueAt)
  or (hasTapback and (tapbackSequence ~= 1 or not nonnegativeInteger(tapbackDueAt)))
  or not positiveInteger(outboxPayloadTtl) then return {'storage_error'} end
if ARGV[11] ~= 'dark' and ARGV[11] ~= 'canary' and ARGV[11] ~= 'production' then return {'release_blocked'} end
if not validStamp(candidate.releaseStamp)
  or candidate.releaseStamp.generation ~= releaseGeneration
  or candidate.releaseStamp.phase ~= ARGV[11] then return {'release_blocked'} end

-- Validate every participating record and index before the first mutation.
if not isStringOrMissing(KEYS[1]) or not isStringOrMissing(KEYS[2])
  or not isStringOrMissing(KEYS[3]) or not isStringOrMissing(KEYS[5])
  or not isZsetOrMissing(KEYS[6]) or not isStringOrMissing(KEYS[7])
  or not isZsetOrMissing(KEYS[8]) or not isStringOrMissing(KEYS[9])
  or not isStringOrMissing(KEYS[10]) or not isZsetOrMissing(KEYS[11])
  or (ARGV[11] == 'canary' and not isStringOrMissing(KEYS[15]))
  or not isStringOrMissing(KEYS[16])
  or not isStringOrMissing(KEYS[17])
  or (hasTapback and (
    not isStringOrMissing(KEYS[12]) or not isStringOrMissing(KEYS[13]) or not isZsetOrMissing(KEYS[14])
  )) then return {'storage_error'} end

local fenceRaw = redis.call('GET', KEYS[7])
if fenceRaw then
  local okFence, fence = pcall(cjson.decode, fenceRaw)
  if not okFence or type(fence) ~= 'table' or fence.schemaVersion ~= 1
    or not validStamp(fence)
    or fence.generation ~= releaseGeneration or fence.phase ~= ARGV[11] then return {'release_blocked'} end
elseif ARGV[11] ~= 'dark' then
  return {'release_blocked'}
end
if ARGV[11] ~= 'dark' and not inboxFifoActive() then
  return {'inbox_fifo_required'}
end
-- A known release revocation wins over a cost response: this stale worker
-- must not publish any user-visible effect while a rollback is in force.
if reservedCents > perJobCap then return {'per_job_cap'} end

-- The reservation record is shared by the opaque approved canary budget and
-- intentionally has no expiry. It is evaluated only after the current fence
-- accepts this exact release, so dark/rollback/stale workers cannot mutate it.
local canaryBudget = nil
if ARGV[11] == 'canary' then
  if ARGV[25] ~= '1' then return {'canary_budget_required'} end
  local canaryTotalCap = tonumber(ARGV[26])
  local canaryFalCap = tonumber(ARGV[27])
  if not nonnegativeInteger(canaryTotalCap) or not nonnegativeInteger(canaryFalCap)
    or (candidate.provider ~= 'gmi' and candidate.provider ~= 'fal') then return {'storage_error'} end
  local totalReserved = 0
  local falReserved = 0
  local budgetRaw = redis.call('GET', KEYS[15])
  if budgetRaw then
    local okBudget, budget = pcall(cjson.decode, budgetRaw)
    if not okBudget or type(budget) ~= 'table' or budget.schemaVersion ~= 1
      or not nonnegativeInteger(budget.totalReservedCents)
      or not nonnegativeInteger(budget.falReservedCents) then return {'storage_error'} end
    totalReserved = budget.totalReservedCents
    falReserved = budget.falReservedCents
  end
  if totalReserved + reservedCents > canaryTotalCap then return {'canary_total_cap'} end
  if candidate.provider == 'fal' and falReserved + reservedCents > canaryFalCap then return {'canary_fal_cap'} end
  canaryBudget = {
    schemaVersion = 1,
    totalReservedCents = totalReserved + reservedCents,
    falReservedCents = candidate.provider == 'fal' and falReserved + reservedCents or falReserved
  }
end

local okPayload, encryptedJobPayload = pcall(cjson.decode, ARGV[3])
local okAcknowledgement, acknowledgement = pcall(cjson.decode, ARGV[13])
local okAcknowledgementPayload, acknowledgementPayload = pcall(cjson.decode, ARGV[14])
if not okPayload or type(encryptedJobPayload) ~= 'table'
  or not okAcknowledgement or type(acknowledgement) ~= 'table'
  or not okAcknowledgementPayload or type(acknowledgementPayload) ~= 'table'
  or not validNotificationManifest(candidate.initialNotifications, acknowledgementId, tapbackId, hasTapback)
  or not validNewOutbox(
    acknowledgement,
    acknowledgementId,
    jobId,
    candidate.senderHash,
    'job-acknowledgement',
    candidate.releaseStamp,
    acknowledgementSequence,
    acknowledgementDueAt
  ) then return {'storage_error'} end

local tapback = nil
if hasTapback then
  local okTapback, parsedTapback = pcall(cjson.decode, ARGV[18])
  local okTapbackPayload, parsedTapbackPayload = pcall(cjson.decode, ARGV[19])
  if not okTapback or type(parsedTapback) ~= 'table'
    or not okTapbackPayload or type(parsedTapbackPayload) ~= 'table'
    or not validNewOutbox(
      parsedTapback,
      tapbackId,
      jobId,
      candidate.senderHash,
      'tapback',
      candidate.releaseStamp,
      tapbackSequence,
      tapbackDueAt
    ) then return {'storage_error'} end
  tapback = parsedTapback
end

if redis.call('EXISTS', KEYS[5]) == 1
  or redis.call('EXISTS', KEYS[9]) == 1 or redis.call('EXISTS', KEYS[10]) == 1
  or (hasTapback and (redis.call('EXISTS', KEYS[12]) == 1 or redis.call('EXISTS', KEYS[13]) == 1)) then
  return {'storage_error'}
end
if redis.call('ZSCORE', KEYS[6], jobId) then return {'storage_error'} end
if #redis.call('ZRANGEBYSCORE', KEYS[11], acknowledgementSequence, acknowledgementSequence, 'LIMIT', 0, 1) > 0 then
  return {'storage_error'}
end
if hasTapback and #redis.call('ZRANGEBYSCORE', KEYS[14], tapbackSequence, tapbackSequence, 'LIMIT', 0, 1) > 0 then
  return {'storage_error'}
end

local active = redis.call('GET', KEYS[1])
if active then return {'active_job'} end
local userCount = tonumber(redis.call('GET', KEYS[2]) or '0')
local spend = tonumber(redis.call('GET', KEYS[3]) or '0')
if not nonnegativeInteger(userCount) or not nonnegativeInteger(spend) then return {'storage_error'} end
if userCount >= userLimit then return {'user_limit'} end
if spend + reservedCents > dailySpendCap then return {'daily_cap'} end

redis.call('SET', KEYS[1], jobId)
redis.call('SET', KEYS[2], tostring(userCount + 1), 'EX', dailyTtl)
redis.call('SET', KEYS[3], tostring(spend + reservedCents), 'EX', dailyTtl)
redis.call('SET', KEYS[4], ARGV[2])
redis.call('SET', KEYS[5], ARGV[3], 'EX', jobPayloadTtl)
redis.call('SET', KEYS[16], jobId)
redis.call('SET', KEYS[9], ARGV[13])
redis.call('SET', KEYS[10], ARGV[14], 'EX', outboxPayloadTtl)
redis.call('ZADD', KEYS[8], acknowledgementDueAt, acknowledgementId)
redis.call('ZADD', KEYS[11], acknowledgementSequence, acknowledgementId)
if hasTapback then
  redis.call('SET', KEYS[12], ARGV[18])
  redis.call('SET', KEYS[13], ARGV[19], 'EX', outboxPayloadTtl)
  redis.call('ZADD', KEYS[8], tapbackDueAt, tapbackId)
  redis.call('ZADD', KEYS[14], tapbackSequence, tapbackId)
end
-- Publication is last: all user-visible durable intents exist first.
redis.call('ZADD', KEYS[6], now, jobId)
if canaryBudget then redis.call('SET', KEYS[15], cjson.encode(canaryBudget)) end
return {'admitted'}
`;

// Kept as source text and injected into every inbox FIFO Lua primitive so the
// production scripts fail closed exactly like MemoryRedis. In particular, a
// tied ZSET score, unsafe integer, or malformed receipt member can never be
// selected by Redis's lexical tie-break and treated as a valid predecessor.
const INBOX_FIFO_LUA_VALIDATED_HEAD_HELPER = `
local function fifoSafePositiveInteger(value)
  if type(value) ~= 'string' or not string.match(value, '^[1-9][0-9]*$') then return false end
  local length = string.len(value)
  return length < 16 or (length == 16 and value <= '9007199254740991')
end

local function fifoSafeSequence(value)
  return type(value) == 'number'
    and value >= 1 and value <= 9007199254740991
    and value == math.floor(value)
end

-- Returns the sole valid head at one exact durable sequence, or false. A
-- duplicate score is never resolved by Redis member ordering.
local function fifoValidatedQueueHead(queueKey, expectedSequence, expectedReceipt)
  if not fifoSafeSequence(expectedSequence) then return false end
  local head = redis.call('ZRANGE', queueKey, 0, 0, 'WITHSCORES')
  if #head ~= 2
    or not string.match(head[1], '^[a-f0-9]+$') or string.len(head[1]) ~= 64 then return false end
  local score = tonumber(head[2])
  if not fifoSafeSequence(score) or score ~= expectedSequence then return false end
  local exact = redis.call('ZRANGEBYSCORE', queueKey, expectedSequence, expectedSequence, 'LIMIT', 0, 2)
  if #exact ~= 1 or exact[1] ~= head[1] then return false end
  if expectedReceipt and head[1] ~= expectedReceipt then return false end
  return head[1]
end
`;

const PERSIST_INBOX_LUA = `
-- AIR_STUDIO_PERSIST_INBOX_V5
-- KEYS: metadata, encrypted envelope, global ready, latest-ingress sequence,
--       FIFO migration, per-scope FIFO queue, per-scope FIFO cursor,
--       holding-scope index.
-- ARGV: metadata, envelope, envelope ttl, received-at, receipt id, scope id.
-- The existing latest-ingress counter remains the pending-media fence. FIFO
-- progress has a distinct cursor so a later received bubble may revoke an
-- older correction without permitting that later bubble to process first.
local function fifoPhase()
  local raw = redis.call('GET', KEYS[5])
  if not raw then return 'legacy' end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' or value.schemaVersion ~= 1
    or (value.phase ~= 'holding' and value.phase ~= 'fifo') then return nil end
  return value.phase
end
${INBOX_FIFO_LUA_VALIDATED_HEAD_HELPER}
local function nonnegativeInteger(value)
  return value == '0' or fifoSafePositiveInteger(value)
end
local function typeOrMissing(key, expected)
  local actual = redis.call('TYPE', key).ok
  return actual == 'none' or actual == expected
end

if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local phase = fifoPhase()
if not phase
  or not fifoSafePositiveInteger(ARGV[3])
  or not nonnegativeInteger(ARGV[4])
  or not ARGV[5] or ARGV[5] == ''
  or not ARGV[6] or not string.match(ARGV[6], '^[a-f0-9]+$') or string.len(ARGV[6]) ~= 64
  or not typeOrMissing(KEYS[1], 'string')
  or not typeOrMissing(KEYS[2], 'string')
  or not typeOrMissing(KEYS[3], 'zset')
  or not typeOrMissing(KEYS[4], 'string')
  or not typeOrMissing(KEYS[5], 'string') then return -2 end
local decoded, record = pcall(cjson.decode, ARGV[1])
if not decoded or type(record) ~= 'table' then return -2 end
local counter = redis.call('GET', KEYS[4])
if counter and not nonnegativeInteger(counter) then return -2 end
local nextSequence = (counter and tonumber(counter) or 0) + 1
if not fifoSafeSequence(nextSequence) then return -2 end

local cursor = nil
if phase ~= 'legacy' then
  if not typeOrMissing(KEYS[6], 'zset')
    or not typeOrMissing(KEYS[7], 'string')
    or not typeOrMissing(KEYS[8], 'zset') then return -2 end
  local cursorRaw = redis.call('GET', KEYS[7])
  if cursorRaw then
    if not fifoSafePositiveInteger(cursorRaw) then return -2 end
    cursor = tonumber(cursorRaw)
    if cursor > nextSequence then return -2 end
  else
    cursor = nextSequence
  end
  -- A counter rollback or corrupted queue must not create a second record at
  -- one receipt sequence and rely on Redis's lexical member tie-break.
  if #redis.call('ZRANGEBYSCORE', KEYS[6], nextSequence, nextSequence, 'LIMIT', 0, 1) ~= 0 then return -2 end
end

local allocated = redis.call('INCR', KEYS[4])
if allocated ~= nextSequence then return -2 end
record.encryptedEnvelope = nil
record.scopeSequence = allocated
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
if phase == 'legacy' then
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[5])
  return cjson.encode(record)
end

if not redis.call('GET', KEYS[7]) then redis.call('SET', KEYS[7], tostring(cursor)) end
redis.call('ZADD', KEYS[6], allocated, ARGV[5])
if phase == 'holding' then
  redis.call('ZADD', KEYS[8], 0, ARGV[6])
elseif cursor == allocated then
  -- Only the durable per-scope head is globally schedulable.
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[5])
end
return cjson.encode(record)
`;

const CLAIM_INBOX_LUA = `
-- AIR_STUDIO_CLAIM_INBOX_V4
-- KEYS: metadata, envelope, lease, global ready, FIFO migration, scope queue,
--       scope cursor. ARGV: lease token, lease ttl, now, receipt id.
local function fifoPhase()
  local raw = redis.call('GET', KEYS[5])
  if not raw then return 'legacy' end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' or value.schemaVersion ~= 1
    or (value.phase ~= 'holding' and value.phase ~= 'fifo') then return nil end
  return value.phase
end
${INBOX_FIFO_LUA_VALIDATED_HEAD_HELPER}
local function validRecord(record)
  return type(record) == 'table'
    and record.receiptId == ARGV[4]
    and type(record.scopeSequence) == 'number'
    and record.scopeSequence >= 1
    and record.scopeSequence <= 9007199254740991
    and record.scopeSequence == math.floor(record.scopeSequence)
end

local phase = fifoPhase()
if not phase or phase == 'holding' then return false end
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local decoded, record = pcall(cjson.decode, raw)
if not decoded or type(record) ~= 'table' then return false end
if record.state ~= 'received' and record.state ~= 'claimed' then return false end

if phase == 'fifo' then
  if not validRecord(record) then return false end
  local cursorRaw = redis.call('GET', KEYS[7])
  if not fifoSafePositiveInteger(cursorRaw) or tonumber(cursorRaw) ~= record.scopeSequence then return false end
  if not fifoValidatedQueueHead(KEYS[6], record.scopeSequence, ARGV[4]) then return false end
  local dueAt = redis.call('ZSCORE', KEYS[4], ARGV[4])
  if not dueAt or tonumber(dueAt) > tonumber(ARGV[3]) then return false end
end

if redis.call('SET', KEYS[3], ARGV[1], 'NX', 'PX', ARGV[2]) ~= 'OK' then return false end
record.state = 'claimed'
record.stateVersion = record.stateVersion + 1
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[4], tonumber(ARGV[3]) + tonumber(ARGV[2]), ARGV[4])
local envelope = redis.call('GET', KEYS[2])
if envelope then
  local envelopeDecoded, value = pcall(cjson.decode, envelope)
  if envelopeDecoded then record.encryptedEnvelope = value end
end
return cjson.encode(record)
`;

const FINISH_INBOX_LUA = `
-- AIR_STUDIO_FINISH_INBOX_V4
-- KEYS: metadata, envelope, lease, global ready, FIFO migration, scope queue,
--       scope cursor. ARGV: lease, terminal state, now, metadata ttl, receipt,
--       inbox metadata-key prefix.
local function fifoPhase()
  local raw = redis.call('GET', KEYS[5])
  if not raw then return 'legacy' end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' or value.schemaVersion ~= 1
    or (value.phase ~= 'holding' and value.phase ~= 'fifo') then return nil end
  return value.phase
end
${INBOX_FIFO_LUA_VALIDATED_HEAD_HELPER}
local function validCurrent(record)
  return type(record) == 'table'
    and record.receiptId == ARGV[5]
    and type(record.scopeSequence) == 'number'
    and record.scopeSequence >= 1
    and record.scopeSequence <= 9007199254740991
    and record.scopeSequence == math.floor(record.scopeSequence)
end
local function promoteExactNext(nextSequence)
  local first = redis.call('ZRANGE', KEYS[6], 0, 0)
  if #first == 0 then return 1 end
  local nextReceipt = fifoValidatedQueueHead(KEYS[6], nextSequence, nil)
  if not nextReceipt then return 2 end
  local nextRaw = redis.call('GET', ARGV[6] .. nextReceipt)
  if not nextRaw then return 2 end
  local decoded, nextRecord = pcall(cjson.decode, nextRaw)
  if not decoded or type(nextRecord) ~= 'table'
    or nextRecord.receiptId ~= nextReceipt
    or nextRecord.scopeSequence ~= nextSequence
    or nextRecord.state ~= 'received'
    or type(nextRecord.receivedAtMs) ~= 'number'
    or nextRecord.receivedAtMs < 0 or nextRecord.receivedAtMs > 9007199254740991
    or nextRecord.receivedAtMs ~= math.floor(nextRecord.receivedAtMs) then return 2 end
  redis.call('ZADD', KEYS[4], nextRecord.receivedAtMs, nextReceipt)
  return 1
end

if ARGV[2] ~= 'processed' and ARGV[2] ~= 'maintenance_rejected' and ARGV[2] ~= 'failed' then return 0 end
local phase = fifoPhase()
if not phase or phase == 'holding' then return 0 end
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local decoded, record = pcall(cjson.decode, raw)
if not decoded or record.state ~= 'claimed' then return 0 end

if phase == 'fifo' then
  if not validCurrent(record) then return 0 end
  local cursorRaw = redis.call('GET', KEYS[7])
  if not fifoSafePositiveInteger(cursorRaw) or tonumber(cursorRaw) ~= record.scopeSequence then return 0 end
  if not fifoValidatedQueueHead(KEYS[6], record.scopeSequence, ARGV[5]) then return 0 end
end

record.state = ARGV[2]
record.stateVersion = record.stateVersion + 1
record.encryptedEnvelope = nil
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[4], ARGV[5])
redis.call('DEL', KEYS[3])
if phase == 'legacy' then return 1 end

redis.call('ZREM', KEYS[6], ARGV[5])
local nextSequence = record.scopeSequence + 1
redis.call('SET', KEYS[7], tostring(nextSequence))
return promoteExactNext(nextSequence)
`;

const RETRY_INBOX_LUA = `
-- AIR_STUDIO_RETRY_INBOX_V4
-- KEYS and ARGV match FINISH except ARGV[2] is now and ARGV[3] retry delay.
local function fifoPhase()
  local raw = redis.call('GET', KEYS[5])
  if not raw then return 'legacy' end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' or value.schemaVersion ~= 1
    or (value.phase ~= 'holding' and value.phase ~= 'fifo') then return nil end
  return value.phase
end
${INBOX_FIFO_LUA_VALIDATED_HEAD_HELPER}
local function validCurrent(record)
  return type(record) == 'table'
    and record.receiptId == ARGV[5]
    and type(record.scopeSequence) == 'number'
    and record.scopeSequence >= 1
    and record.scopeSequence <= 9007199254740991
    and record.scopeSequence == math.floor(record.scopeSequence)
end
local function promoteExactNext(nextSequence)
  local first = redis.call('ZRANGE', KEYS[6], 0, 0)
  if #first == 0 then return 2 end
  local nextReceipt = fifoValidatedQueueHead(KEYS[6], nextSequence, nil)
  if not nextReceipt then return 3 end
  local nextRaw = redis.call('GET', ARGV[6] .. nextReceipt)
  if not nextRaw then return 3 end
  local decoded, nextRecord = pcall(cjson.decode, nextRaw)
  if not decoded or type(nextRecord) ~= 'table'
    or nextRecord.receiptId ~= nextReceipt
    or nextRecord.scopeSequence ~= nextSequence
    or nextRecord.state ~= 'received'
    or type(nextRecord.receivedAtMs) ~= 'number'
    or nextRecord.receivedAtMs < 0 or nextRecord.receivedAtMs > 9007199254740991
    or nextRecord.receivedAtMs ~= math.floor(nextRecord.receivedAtMs) then return 3 end
  redis.call('ZADD', KEYS[4], nextRecord.receivedAtMs, nextReceipt)
  return 2
end

local phase = fifoPhase()
if not phase or phase == 'holding' then return 0 end
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local decoded, record = pcall(cjson.decode, raw)
if not decoded or record.state ~= 'claimed' then return 0 end
if phase == 'fifo' then
  if not validCurrent(record) then return 0 end
  local cursorRaw = redis.call('GET', KEYS[7])
  if not fifoSafePositiveInteger(cursorRaw) or tonumber(cursorRaw) ~= record.scopeSequence then return 0 end
  if not fifoValidatedQueueHead(KEYS[6], record.scopeSequence, ARGV[5]) then return 0 end
end

-- Privacy wins once the immutable encrypted envelope lifetime is over.
if redis.call('EXISTS', KEYS[2]) ~= 1 then
  record.state = 'failed'
  record.stateVersion = record.stateVersion + 1
  record.encryptedEnvelope = nil
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('ZREM', KEYS[4], ARGV[5])
  redis.call('DEL', KEYS[3])
  if phase == 'legacy' then return 2 end
  redis.call('ZREM', KEYS[6], ARGV[5])
  local nextSequence = record.scopeSequence + 1
  redis.call('SET', KEYS[7], tostring(nextSequence))
  return promoteExactNext(nextSequence)
end
record.state = 'received'
record.stateVersion = record.stateVersion + 1
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[4], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[5])
redis.call('DEL', KEYS[3])
return 1
`;

const BEGIN_INBOX_FIFO_HOLDING_LUA = `
-- AIR_STUDIO_BEGIN_INBOX_FIFO_HOLDING_V1
-- KEYS: migration record, legacy/global ready index, holding-scope index.
local raw = redis.call('GET', KEYS[1])
if raw then
  local decoded, value = pcall(cjson.decode, raw)
  if not decoded or type(value) ~= 'table' or value.schemaVersion ~= 1
    or (value.phase ~= 'holding' and value.phase ~= 'fifo') then return -3 end
  if value.phase == 'holding' then return 2 end
  return -2
end
-- Do not infer an old scope queue from metadata that may have expired. A
-- claimed V4 receipt remains in global ready, so this proves no legacy owner
-- can cross the cutover boundary.
if redis.call('ZCARD', KEYS[2]) ~= 0 then return -1 end
if redis.call('ZCARD', KEYS[3]) ~= 0 then return -3 end
redis.call('SET', KEYS[1], cjson.encode({ schemaVersion = 1, phase = 'holding' }))
return 1
`;

const PREPARE_INBOX_FIFO_HOLDING_SCOPE_LUA = `
-- AIR_STUDIO_PREPARE_INBOX_FIFO_HOLDING_SCOPE_V1
-- KEYS: migration, holding scopes, scope queue, scope cursor, global ready.
-- ARGV: scope id, inbox metadata-key prefix.
${INBOX_FIFO_LUA_VALIDATED_HEAD_HELPER}
local raw = redis.call('GET', KEYS[1])
if not raw then return -2 end
local decoded, migration = pcall(cjson.decode, raw)
if not decoded or type(migration) ~= 'table' or migration.schemaVersion ~= 1
  or migration.phase ~= 'holding' then return -2 end
if not ARGV[1] or not string.match(ARGV[1], '^[a-f0-9]+$') or string.len(ARGV[1]) ~= 64
  or not ARGV[2] or ARGV[2] == '' then return -1 end
local cursorRaw = redis.call('GET', KEYS[4])
if not fifoSafePositiveInteger(cursorRaw) then return -1 end
local cursor = tonumber(cursorRaw)
-- An empty scope while holding means a record vanished after acceptance. It
-- is intentionally not cleaned up or skipped.
local headReceipt = fifoValidatedQueueHead(KEYS[3], cursor, nil)
if not headReceipt then return -1 end
local nextRaw = redis.call('GET', ARGV[2] .. headReceipt)
if not nextRaw then return -1 end
local ok, record = pcall(cjson.decode, nextRaw)
if not ok or type(record) ~= 'table'
  or record.receiptId ~= headReceipt
  or record.scopeSequence ~= cursor
  or record.state ~= 'received'
  or type(record.receivedAtMs) ~= 'number'
  or record.receivedAtMs < 0 or record.receivedAtMs > 9007199254740991
  or record.receivedAtMs ~= math.floor(record.receivedAtMs) then return -1 end
local held = redis.call('ZSCORE', KEYS[2], ARGV[1])
local published = redis.call('ZSCORE', KEYS[5], headReceipt)
-- A second operator can observe the scope after the first one atomically
-- published it. Treat that as idempotent only after proving the exact valid
-- head is already present at its immutable receipt-time score. Anything else
-- is an integrity failure, not a reason to infer or overwrite queue state.
if not held then
  if published and tonumber(published) == record.receivedAtMs then return 2 end
  return -1
end
if published then
  if tonumber(published) ~= record.receivedAtMs then return -1 end
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 2
end
redis.call('ZADD', KEYS[5], record.receivedAtMs, headReceipt)
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`;

const ACTIVATE_INBOX_FIFO_LUA = `
-- AIR_STUDIO_ACTIVATE_INBOX_FIFO_V1
-- KEYS: migration record, holding-scope index.
local raw = redis.call('GET', KEYS[1])
if not raw then return -2 end
local decoded, migration = pcall(cjson.decode, raw)
if not decoded or type(migration) ~= 'table' or migration.schemaVersion ~= 1
  or (migration.phase ~= 'holding' and migration.phase ~= 'fifo') then return -3 end
if migration.phase == 'fifo' then return 2 end
if redis.call('ZCARD', KEYS[2]) ~= 0 then return -1 end
redis.call('SET', KEYS[1], cjson.encode({ schemaVersion = 1, phase = 'fifo' }))
return 1
`;

const ADVANCE_RELEASE_FENCE_LUA = `
-- AIR_STUDIO_ADVANCE_RELEASE_FENCE_V1
-- KEYS: one non-expiring opaque global release record
-- ARGV: expected previous generation, target generation, target phase
local previous = tonumber(ARGV[1])
local target = tonumber(ARGV[2])
if not previous or not target or previous < -1 or target < 0
  or previous ~= math.floor(previous) or target ~= math.floor(target)
  or (ARGV[3] ~= 'dark' and ARGV[3] ~= 'canary' and ARGV[3] ~= 'production') then return -2 end
local raw = redis.call('GET', KEYS[1])
if not raw then
  if previous ~= -1 or target ~= 0 or ARGV[3] ~= 'dark' then return -1 end
  redis.call('SET', KEYS[1], cjson.encode({ schemaVersion = 1, generation = target, phase = ARGV[3] }))
  return 1
end
local ok, current = pcall(cjson.decode, raw)
if not ok or type(current) ~= 'table' or current.schemaVersion ~= 1
  or type(current.generation) ~= 'number' or current.generation < 0
  or current.generation ~= math.floor(current.generation)
  or (current.phase ~= 'dark' and current.phase ~= 'canary' and current.phase ~= 'production') then return -2 end
if current.generation == target and current.phase == ARGV[3]
  and (previous == target or previous == target - 1) then return 2 end
if current.generation ~= previous or target ~= previous + 1 then return -1 end
redis.call('SET', KEYS[1], cjson.encode({ schemaVersion = 1, generation = target, phase = ARGV[3] }))
return 1
`;

const PUBLISH_JOB_RESULT_LUA = `
-- AIR_STUDIO_PUBLISH_JOB_RESULT_V1
-- KEYS: job metadata, result cursor, outbox metadata/payload, ready index,
--       result lane, current release fence
-- ARGV: job id/version/admission generation/admission phase, one-based output
--       index, deterministic outbox id, outbox metadata/payload/payload ttl/
--       due time, current outbox generation/phase, now
local function keyType(key)
  local response = redis.call('TYPE', key)
  return response and response.ok or ''
end

local function isStringOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'string'
end

local function isZsetOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'zset'
end

local function opaque(value)
  return type(value) == 'string' and #value == 64 and string.match(value, '^[a-f0-9]+$') ~= nil
end

local function safeText(value, maximum)
  return type(value) == 'string' and #value > 0 and #value <= maximum
    and string.match(value, '[\x00\r\n]') == nil
end

local function positiveInteger(value)
  return type(value) == 'number' and value >= 1 and value == math.floor(value)
end

local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end

local function validStamp(stamp)
  return type(stamp) == 'table'
    and nonnegativeInteger(stamp.generation)
    and (stamp.phase == 'dark' or stamp.phase == 'canary' or stamp.phase == 'production')
    and (stamp.phase == 'dark' or stamp.generation >= 1)
end

local function sameStamp(left, right)
  return validStamp(left) and validStamp(right)
    and left.generation == right.generation and left.phase == right.phase
end

local function encrypted(value)
  return type(value) == 'table'
    and value.version == 1
    and value.algorithm == 'aes-256-gcm'
    and type(value.iv) == 'string'
    and type(value.ciphertext) == 'string'
    and type(value.tag) == 'string'
end

local function result(kind, expected)
  local value = { kind = kind }
  if expected ~= nil then value.expectedOutputIndex = expected end
  return cjson.encode(value)
end

local expectedJobVersion = tonumber(ARGV[2])
local admissionGeneration = tonumber(ARGV[3])
local outputIndex = tonumber(ARGV[5])
local payloadTtl = tonumber(ARGV[9])
local dueAt = tonumber(ARGV[10])
local currentGeneration = tonumber(ARGV[11])
local now = tonumber(ARGV[13])
if not positiveInteger(expectedJobVersion)
  or not nonnegativeInteger(admissionGeneration)
  or not positiveInteger(outputIndex)
  or not positiveInteger(payloadTtl)
  or not nonnegativeInteger(dueAt)
  or not nonnegativeInteger(currentGeneration)
  or not nonnegativeInteger(now)
  or not safeText(ARGV[1], 160)
  or not opaque(ARGV[6])
  or (ARGV[4] ~= 'dark' and ARGV[4] ~= 'canary' and ARGV[4] ~= 'production')
  or (ARGV[12] ~= 'dark' and ARGV[12] ~= 'canary' and ARGV[12] ~= 'production')
  or (ARGV[12] ~= 'dark' and currentGeneration < 1) then return result('unavailable') end

if not isStringOrMissing(KEYS[1])
  or not isStringOrMissing(KEYS[2])
  or not isStringOrMissing(KEYS[3])
  or not isStringOrMissing(KEYS[4])
  or not isZsetOrMissing(KEYS[5])
  or not isZsetOrMissing(KEYS[6])
  or not isStringOrMissing(KEYS[7]) then return result('unavailable') end

local jobRaw = redis.call('GET', KEYS[1])
if not jobRaw then return result('unavailable') end
local jobOk, job = pcall(cjson.decode, jobRaw)
if not jobOk or type(job) ~= 'table'
  or job.jobId ~= ARGV[1]
  or not opaque(job.senderHash)
  or not opaque(job.threadHash)
  or not positiveInteger(job.outputCount)
  or job.outputCount > 4
  or outputIndex > job.outputCount
  or not validStamp(job.releaseStamp)
  or job.releaseStamp.generation ~= admissionGeneration
  or job.releaseStamp.phase ~= ARGV[4] then return result('unavailable') end

local cursorRaw = redis.call('GET', KEYS[2])
local cursor = nil
if cursorRaw then
  local cursorOk
  cursorOk, cursor = pcall(cjson.decode, cursorRaw)
  if not cursorOk or type(cursor) ~= 'table'
    or cursor.schemaVersion ~= 1
    or cursor.jobId ~= job.jobId
    or cursor.senderHash ~= job.senderHash
    or cursor.threadHash ~= job.threadHash
    or cursor.outputCount ~= job.outputCount
    or not sameStamp(cursor.releaseStamp, job.releaseStamp)
    or not positiveInteger(cursor.nextOutputIndex)
    or cursor.nextOutputIndex > job.outputCount + 1
    or type(cursor.outputs) ~= 'table'
    or not nonnegativeInteger(cursor.createdAtMs)
    or not nonnegativeInteger(cursor.updatedAtMs) then return result('unavailable') end
  if #cursor.outputs ~= cursor.nextOutputIndex - 1 then return result('unavailable') end
  for position, output in ipairs(cursor.outputs) do
    if type(output) ~= 'table'
      or output.outputIndex ~= position
      or not opaque(output.outboxId) then return result('unavailable') end
  end
  local prior = cursor.outputs[outputIndex]
  if prior ~= nil then
    -- A confirmed-absent requeue may already have rebound this output to a
    -- new opaque outbox. The cursor remains the idempotency authority.
    return result('existing')
  end
  if outputIndex ~= cursor.nextOutputIndex then
    return result('out_of_order', cursor.nextOutputIndex)
  end
else
  if outputIndex ~= 1 then return result('out_of_order', 1) end
  cursor = {
    schemaVersion = 1,
    jobId = job.jobId,
    senderHash = job.senderHash,
    threadHash = job.threadHash,
    outputCount = job.outputCount,
    releaseStamp = job.releaseStamp,
    nextOutputIndex = 1,
    outputs = {},
    createdAtMs = now,
    updatedAtMs = now
  }
end

-- Existing outputs above remain idempotent after terminalization. A new
-- output must still bind to the exact live nonterminal job version.
if (job.state ~= 'executing' and job.state ~= 'ready' and job.state ~= 'delivering')
  or job.stateVersion ~= expectedJobVersion then return result('unavailable') end

local outboxOk, outbox = pcall(cjson.decode, ARGV[7])
local encryptedOk, outboxPayload = pcall(cjson.decode, ARGV[8])
local currentStamp = { generation = currentGeneration, phase = ARGV[12] }
if not outboxOk or type(outbox) ~= 'table'
  or not encryptedOk or not encrypted(outboxPayload)
  or outbox.schemaVersion ~= 3
  or outbox.outboxId ~= ARGV[6]
  or outbox.jobId ~= job.jobId
  or outbox.senderHash ~= job.senderHash
  or outbox.lane ~= 'job-result'
  or outbox.resultOutputIndex ~= outputIndex
  or outbox.sequence ~= outputIndex
  or outbox.state ~= 'intent'
  or outbox.attempts ~= 0
  or outbox.stateVersion ~= 1
  or outbox.laneIdentity ~= nil
  or outbox.pendingGuard ~= nil
  or not nonnegativeInteger(outbox.createdAtMs)
  or outbox.dueAtMs ~= dueAt
  or not nonnegativeInteger(outbox.updatedAtMs)
  or not sameStamp(outbox.releaseStamp, currentStamp) then return result('unavailable') end

local fenceRaw = redis.call('GET', KEYS[7])
if fenceRaw then
  local fenceOk, fence = pcall(cjson.decode, fenceRaw)
  if not fenceOk or not sameStamp(fence, currentStamp) then return result('release_blocked') end
elseif ARGV[12] ~= 'dark' then
  return result('release_blocked')
end

if redis.call('EXISTS', KEYS[3]) == 1
  or redis.call('EXISTS', KEYS[4]) == 1
  or #redis.call('ZRANGEBYSCORE', KEYS[6], outputIndex, outputIndex, 'LIMIT', 0, 1) > 0 then
  return result('unavailable')
end

table.insert(cursor.outputs, { outputIndex = outputIndex, outboxId = ARGV[6] })
cursor.nextOutputIndex = outputIndex + 1
cursor.updatedAtMs = now
redis.call('SET', KEYS[2], cjson.encode(cursor))
redis.call('SET', KEYS[3], ARGV[7])
redis.call('SET', KEYS[4], ARGV[8], 'EX', payloadTtl)
redis.call('ZADD', KEYS[5], dueAt, ARGV[6])
redis.call('ZADD', KEYS[6], outputIndex, ARGV[6])
return result('created')
`;

const PERSIST_OUTBOX_LUA = `
-- AIR_STUDIO_PERSIST_OUTBOX_V4
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local record = cjson.decode(ARGV[1])
local stamp = record.releaseStamp
if type(stamp) ~= 'table'
  or type(stamp.generation) ~= 'number' or stamp.generation < 0
  or stamp.generation ~= math.floor(stamp.generation)
  or stamp.generation ~= tonumber(ARGV[7])
  or stamp.phase ~= ARGV[8] then return -2 end
local fenceRaw = redis.call('GET', KEYS[5])
if fenceRaw then
  local ok, fence = pcall(cjson.decode, fenceRaw)
  if not ok or type(fence) ~= 'table' or type(fence.generation) ~= 'number'
    or fence.generation < 0 or fence.generation ~= math.floor(fence.generation)
    or (fence.phase ~= 'dark' and fence.phase ~= 'canary' and fence.phase ~= 'production') then return -3 end
  if fence.generation ~= tonumber(ARGV[7]) or fence.phase ~= ARGV[8] then return -2 end
elseif ARGV[8] ~= 'dark' then
  return -3
end
if #redis.call('ZRANGEBYSCORE', KEYS[4], ARGV[6], ARGV[6]) > 0 then return -1 end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[5])
redis.call('ZADD', KEYS[4], ARGV[6], ARGV[5])
return 1
`;

const ENSURE_ONBOARDING_BUNDLE_LUA = `
-- AIR_STUDIO_ENSURE_ONBOARDING_BUNDLE_V1
-- KEYS: encrypted settings, opaque enrollment marker, first metadata/payload,
--       second metadata/payload, ready-outbox index, onboarding lane, fence
-- ARGV: encrypted settings, settings ttl, marker, first metadata/payload/id/
--       sequence/due, second metadata/payload/id/sequence/due, payload ttl,
--       release generation/phase
local function keyType(key)
  local response = redis.call('TYPE', key)
  return response and response.ok or ''
end

local function isStringOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'string'
end

local function isZsetOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'zset'
end

local function opaqueId(value)
  return type(value) == 'string' and #value == 64 and string.match(value, '^[a-f0-9]+$') ~= nil
end

local function positiveInteger(value)
  return type(value) == 'number' and value >= 1 and value == math.floor(value)
end

local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end

local function validStamp(stamp)
  return type(stamp) == 'table'
    and nonnegativeInteger(stamp.generation)
    and (stamp.phase == 'dark' or stamp.phase == 'canary' or stamp.phase == 'production')
    and (stamp.phase == 'dark' or stamp.generation >= 1)
end

local function sameStamp(left, right)
  return validStamp(left) and validStamp(right)
    and left.generation == right.generation and left.phase == right.phase
end

local function validMarker(marker, expectedVersion)
  return type(marker) == 'table'
    and marker.schemaVersion == 1
    and marker.onboardingVersion == expectedVersion
    and positiveInteger(marker.onboardingVersion)
    and opaqueId(marker.receiptId)
    and opaqueId(marker.firstOutboxId)
    and opaqueId(marker.secondOutboxId)
    and marker.firstOutboxId ~= marker.secondOutboxId
end

local function validNewOutbox(record, outboxId, senderHash, sequence, stamp, dueAt)
  return type(record) == 'table'
    and record.schemaVersion == 3
    and record.outboxId == outboxId
    and record.senderHash == senderHash
    and record.lane == 'onboarding'
    and record.jobId == nil
    and record.laneIdentity == nil
    and record.pendingGuard == nil
    and record.encryptedPayload == nil
    and record.sequence == sequence
    and record.state == 'intent'
    and record.attempts == 0
    and record.stateVersion == 1
    and nonnegativeInteger(record.createdAtMs)
    and record.dueAtMs == dueAt
    and nonnegativeInteger(record.updatedAtMs)
    and sameStamp(record.releaseStamp, stamp)
end

local function validEncrypted(value)
  return type(value) == 'table'
    and value.version == 1
    and value.algorithm == 'aes-256-gcm'
    and type(value.iv) == 'string'
    and type(value.ciphertext) == 'string'
    and type(value.tag) == 'string'
end

local settingsTtl = tonumber(ARGV[2])
local firstSequence = tonumber(ARGV[7])
local firstDueAt = tonumber(ARGV[8])
local secondSequence = tonumber(ARGV[12])
local secondDueAt = tonumber(ARGV[13])
local outboxPayloadTtl = tonumber(ARGV[14])
local generation = tonumber(ARGV[15])
local phase = ARGV[16]
if not positiveInteger(settingsTtl)
  or firstSequence ~= 1 or not nonnegativeInteger(firstDueAt)
  or secondSequence ~= 2 or not nonnegativeInteger(secondDueAt)
  or not positiveInteger(outboxPayloadTtl)
  or not nonnegativeInteger(generation)
  or (phase ~= 'dark' and phase ~= 'canary' and phase ~= 'production')
  or (phase ~= 'dark' and generation < 1) then return -3 end

local markerOk, candidateMarker = pcall(cjson.decode, ARGV[3])
if not markerOk or type(candidateMarker) ~= 'table' then return -3 end
local candidateVersion = candidateMarker.onboardingVersion
if not validMarker(candidateMarker, candidateVersion)
  or candidateMarker.firstOutboxId ~= ARGV[6]
  or candidateMarker.secondOutboxId ~= ARGV[11] then return -3 end

-- A prior atomic enrollment is authoritative even if a later deployment has
-- changed the release fence. Retrying it must never recreate stale bubbles.
if not isStringOrMissing(KEYS[2]) then return -3 end
local existingMarkerRaw = redis.call('GET', KEYS[2])
if existingMarkerRaw then
  local existingMarkerOk, existingMarker = pcall(cjson.decode, existingMarkerRaw)
  if not existingMarkerOk or not validMarker(existingMarker, candidateMarker.onboardingVersion) then return -3 end
  return 2
end

-- Validate every key and payload before the first mutation. Redis does not
-- roll back a Lua script after a runtime error, so partial enrollment is not
-- an acceptable fallback path.
if not isStringOrMissing(KEYS[1])
  or not isStringOrMissing(KEYS[3]) or not isStringOrMissing(KEYS[4])
  or not isStringOrMissing(KEYS[5]) or not isStringOrMissing(KEYS[6])
  or not isStringOrMissing(KEYS[9])
  or not isZsetOrMissing(KEYS[7]) or not isZsetOrMissing(KEYS[8]) then return -3 end

local settingsOk, settings = pcall(cjson.decode, ARGV[1])
local firstOk, first = pcall(cjson.decode, ARGV[4])
local firstPayloadOk, firstPayload = pcall(cjson.decode, ARGV[5])
local secondOk, second = pcall(cjson.decode, ARGV[9])
local secondPayloadOk, secondPayload = pcall(cjson.decode, ARGV[10])
if not settingsOk or not validEncrypted(settings)
  or not firstOk or type(first) ~= 'table'
  or not secondOk or type(second) ~= 'table'
  or not firstPayloadOk or not validEncrypted(firstPayload)
  or not secondPayloadOk or not validEncrypted(secondPayload) then return -3 end

if not opaqueId(ARGV[6]) or not opaqueId(ARGV[11]) or ARGV[6] == ARGV[11] then return -3 end
local senderHash = first.senderHash
if not opaqueId(senderHash) or senderHash ~= second.senderHash then return -3 end
local stamp = { generation = generation, phase = phase }
if not validNewOutbox(first, ARGV[6], senderHash, firstSequence, stamp, firstDueAt)
  or not validNewOutbox(second, ARGV[11], senderHash, secondSequence, stamp, secondDueAt) then return -3 end

local fenceRaw = redis.call('GET', KEYS[9])
if fenceRaw then
  local fenceOk, fence = pcall(cjson.decode, fenceRaw)
  if not fenceOk or not validStamp(fence) then return -3 end
  if not sameStamp(fence, stamp) then return -2 end
elseif phase ~= 'dark' then
  return -3
end

if redis.call('EXISTS', KEYS[3]) == 1 or redis.call('EXISTS', KEYS[4]) == 1
  or redis.call('EXISTS', KEYS[5]) == 1 or redis.call('EXISTS', KEYS[6]) == 1 then return -3 end
if #redis.call('ZRANGEBYSCORE', KEYS[8], firstSequence, firstSequence, 'LIMIT', 0, 1) > 0
  or #redis.call('ZRANGEBYSCORE', KEYS[8], secondSequence, secondSequence, 'LIMIT', 0, 1) > 0 then return -3 end

redis.call('SET', KEYS[1], ARGV[1], 'EX', settingsTtl)
redis.call('SET', KEYS[3], ARGV[4])
redis.call('SET', KEYS[4], ARGV[5], 'EX', outboxPayloadTtl)
redis.call('SET', KEYS[5], ARGV[9])
redis.call('SET', KEYS[6], ARGV[10], 'EX', outboxPayloadTtl)
redis.call('ZADD', KEYS[7], firstDueAt, ARGV[6])
redis.call('ZADD', KEYS[7], secondDueAt, ARGV[11])
redis.call('ZADD', KEYS[8], firstSequence, ARGV[6])
redis.call('ZADD', KEYS[8], secondSequence, ARGV[11])
-- A valid marker is written last; it proves the whole enrollment bundle.
redis.call('SET', KEYS[2], ARGV[3])
return 1
`;

const PERSIST_PENDING_GUARDED_OUTBOX_LUA = `
-- AIR_STUDIO_PERSIST_PENDING_GUARDED_OUTBOX_V5
-- KEYS: pending payload, pending control, outbox metadata, outbox payload,
--       ready index, lane index, durable inbound scope counter, release fence
-- ARGV: receipt, sequence, revision, token, expires, metadata, payload,
--       payload ttl, now, outboxId, laneSequence, dueAt
local now = tonumber(ARGV[9])
local guardSequence = tonumber(ARGV[2])
local guardRevision = tonumber(ARGV[3])
local guardExpiry = tonumber(ARGV[5])
if not now or not guardSequence or not guardRevision or not guardExpiry then return -6 end
local outbox = cjson.decode(ARGV[6])
local stamp = outbox.releaseStamp
if type(stamp) ~= 'table'
  or type(stamp.generation) ~= 'number' or stamp.generation < 0
  or stamp.generation ~= math.floor(stamp.generation)
  or stamp.generation ~= tonumber(ARGV[13])
  or stamp.phase ~= ARGV[14] then return -7 end
local fenceRaw = redis.call('GET', KEYS[8])
if fenceRaw then
  local ok, fence = pcall(cjson.decode, fenceRaw)
  if not ok or type(fence) ~= 'table' or type(fence.generation) ~= 'number'
    or fence.generation < 0 or fence.generation ~= math.floor(fence.generation)
    or (fence.phase ~= 'dark' and fence.phase ~= 'canary' and fence.phase ~= 'production') then return -8 end
  if fence.generation ~= tonumber(ARGV[13]) or fence.phase ~= ARGV[14] then return -7 end
elseif ARGV[14] ~= 'dark' then
  return -8
end
local raw = redis.call('GET', KEYS[2])
if not raw then return -6 end
local control = cjson.decode(raw)
if control.state == 'maintenance' then return -2 end
if control.state == 'cleared' then return -3 end
if control.state == 'expired' then return -4 end
if control.state ~= 'active' then return -6 end
if tonumber(control.expiresAtMs) == nil or tonumber(control.expiresAtMs) <= now or redis.call('EXISTS', KEYS[1]) ~= 1 then
  control.state = 'expired'
  control.token = nil
  redis.call('DEL', KEYS[1])
  redis.call('SET', KEYS[2], cjson.encode(control))
  return -4
end
if tonumber(control.scopeSequence) > guardSequence then return -5 end
if tonumber(control.scopeSequence) ~= guardSequence
  or tonumber(control.revision) ~= guardRevision
  or control.receiptId ~= ARGV[1]
  or control.token ~= ARGV[4]
  or tonumber(control.expiresAtMs) ~= guardExpiry then return -6 end
if tonumber(redis.call('GET', KEYS[7])) ~= guardSequence then return -5 end
if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
if #redis.call('ZRANGEBYSCORE', KEYS[6], ARGV[11], ARGV[11]) > 0 then return -1 end
redis.call('SET', KEYS[3], ARGV[6])
redis.call('SET', KEYS[4], ARGV[7], 'EX', ARGV[8])
redis.call('ZADD', KEYS[5], ARGV[12], ARGV[10])
redis.call('ZADD', KEYS[6], ARGV[11], ARGV[10])
return 1
`;

const CLAIM_OUTBOX_LUA = `
-- AIR_STUDIO_CLAIM_OUTBOX_V5
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= 'intent' and record.state ~= 'retryable_failure' then return false end
local function releaseFenceStatus()
  local stamp = record.releaseStamp
  if type(stamp) ~= 'table'
    or type(stamp.generation) ~= 'number' or stamp.generation < 0
    or stamp.generation ~= math.floor(stamp.generation)
    or stamp.generation ~= tonumber(ARGV[7])
    or stamp.phase ~= ARGV[8] then return -1 end
  local fenceRaw = redis.call('GET', KEYS[9])
  if not fenceRaw then return ARGV[8] == 'dark' and 1 or 0 end
  local ok, fence = pcall(cjson.decode, fenceRaw)
  if not ok or type(fence) ~= 'table' or type(fence.generation) ~= 'number'
    or fence.generation < 0 or fence.generation ~= math.floor(fence.generation)
    or (fence.phase ~= 'dark' and fence.phase ~= 'canary' and fence.phase ~= 'production') then return 0 end
  if fence.generation == tonumber(ARGV[7]) and fence.phase == ARGV[8] then return 1 end
  return -1
end
local releaseStatus = releaseFenceStatus()
local laneHead = redis.call('ZRANGE', KEYS[5], 0, 0)
if #laneHead ~= 1 or laneHead[1] ~= ARGV[5] then return false end
if releaseStatus == 0 then return false end
if releaseStatus == -1 then
  record.state = 'canceled'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[3])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('DEL', KEYS[2])
  redis.call('ZREM', KEYS[4], ARGV[5])
  redis.call('ZREM', KEYS[5], ARGV[5])
  return false
end
if redis.call('EXISTS', KEYS[2]) ~= 1 then
  record.state = 'failed'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[3])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('ZREM', KEYS[4], ARGV[5])
  redis.call('ZREM', KEYS[5], ARGV[5])
  return false
end
local guardIsLive = true
if record.pendingGuard ~= nil then
  local guard = record.pendingGuard
  guardIsLive = type(guard) == 'table'
    and type(guard.receiptId) == 'string'
    and type(guard.scopeSequence) == 'number'
    and type(guard.revision) == 'number'
    and type(guard.token) == 'string'
    and type(guard.expiresAtMs) == 'number'
  if guardIsLive then
    local controlRaw = redis.call('GET', KEYS[6])
    if not controlRaw then
      guardIsLive = false
    else
      local control = cjson.decode(controlRaw)
      guardIsLive = control.state == 'active'
        and control.receiptId == guard.receiptId
        and tonumber(control.scopeSequence) == guard.scopeSequence
        and tonumber(control.revision) == guard.revision
        and control.token == guard.token
        and tonumber(control.expiresAtMs) == guard.expiresAtMs
      if guardIsLive and (guard.expiresAtMs <= tonumber(ARGV[3]) or redis.call('EXISTS', KEYS[7]) ~= 1) then
        control.state = 'expired'
        control.token = nil
        redis.call('DEL', KEYS[7])
        redis.call('SET', KEYS[6], cjson.encode(control))
        guardIsLive = false
      end
    end
  end
  if guardIsLive and tonumber(redis.call('GET', KEYS[8])) ~= guard.scopeSequence then guardIsLive = false end
end
if not guardIsLive then
  record.state = 'canceled'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[3])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('DEL', KEYS[2])
  redis.call('ZREM', KEYS[4], ARGV[5])
  redis.call('ZREM', KEYS[5], ARGV[5])
  return false
end
if record.state == 'retryable_failure' and record.attempts >= tonumber(ARGV[6]) then
  record.state = 'failed'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[3])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('DEL', KEYS[2])
  redis.call('ZREM', KEYS[4], ARGV[5])
  redis.call('ZREM', KEYS[5], ARGV[5])
  return false
end
if redis.call('SET', KEYS[3], ARGV[1], 'NX', 'PX', ARGV[2]) ~= 'OK' then return false end
record.state = 'sending'
record.attempts = record.attempts + 1
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[3])
redis.call('SET', KEYS[1], cjson.encode(record))
record.encryptedPayload = cjson.decode(redis.call('GET', KEYS[2]))
redis.call('ZADD', KEYS[4], tonumber(ARGV[3]) + tonumber(ARGV[2]), ARGV[5])
return cjson.encode(record)
`;

const AUTHORIZE_OUTBOX_DELIVERY_LUA = `
-- AIR_STUDIO_AUTHORIZE_OUTBOX_DELIVERY_V5
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local record = cjson.decode(raw)
if record.state ~= 'sending' then return 0 end
local function releaseFenceStatus()
  local stamp = record.releaseStamp
  if type(stamp) ~= 'table'
    or type(stamp.generation) ~= 'number' or stamp.generation < 0
    or stamp.generation ~= math.floor(stamp.generation)
    or stamp.generation ~= tonumber(ARGV[5])
    or stamp.phase ~= ARGV[6] then return -1 end
  local fenceRaw = redis.call('GET', KEYS[9])
  if not fenceRaw then return ARGV[6] == 'dark' and 1 or 0 end
  local ok, fence = pcall(cjson.decode, fenceRaw)
  if not ok or type(fence) ~= 'table' or type(fence.generation) ~= 'number'
    or fence.generation < 0 or fence.generation ~= math.floor(fence.generation)
    or (fence.phase ~= 'dark' and fence.phase ~= 'canary' and fence.phase ~= 'production') then return 0 end
  if fence.generation == tonumber(ARGV[5]) and fence.phase == ARGV[6] then return 1 end
  return -1
end
local releaseStatus = releaseFenceStatus()
if releaseStatus == 0 then
  -- No physical dispatch occurred. Requeue without consuming an attempt so a
  -- temporary fence read failure cannot become send_unknown or failed.
  record.state = 'intent'
  record.attempts = math.max(0, tonumber(record.attempts) - 1)
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], cjson.encode(record))
  redis.call('DEL', KEYS[3])
  redis.call('ZADD', KEYS[4], tonumber(ARGV[2]) + tonumber(ARGV[7]), ARGV[4])
  return 0
end
if releaseStatus == -1 then
  record.state = 'canceled'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[3])
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  redis.call('ZREM', KEYS[4], ARGV[4])
  redis.call('ZREM', KEYS[5], ARGV[4])
  return 0
end
if redis.call('EXISTS', KEYS[2]) ~= 1 then
  record.state = 'failed'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[3])
  redis.call('DEL', KEYS[3])
  redis.call('ZREM', KEYS[4], ARGV[4])
  redis.call('ZREM', KEYS[5], ARGV[4])
  return 0
end
local guardIsLive = true
if record.pendingGuard ~= nil then
  local guard = record.pendingGuard
  guardIsLive = type(guard) == 'table'
    and type(guard.receiptId) == 'string'
    and type(guard.scopeSequence) == 'number'
    and type(guard.revision) == 'number'
    and type(guard.token) == 'string'
    and type(guard.expiresAtMs) == 'number'
  if guardIsLive then
    local controlRaw = redis.call('GET', KEYS[6])
    if not controlRaw then
      guardIsLive = false
    else
      local control = cjson.decode(controlRaw)
      guardIsLive = control.state == 'active'
        and control.receiptId == guard.receiptId
        and tonumber(control.scopeSequence) == guard.scopeSequence
        and tonumber(control.revision) == guard.revision
        and control.token == guard.token
        and tonumber(control.expiresAtMs) == guard.expiresAtMs
      if guardIsLive and (guard.expiresAtMs <= tonumber(ARGV[2]) or redis.call('EXISTS', KEYS[7]) ~= 1) then
        control.state = 'expired'
        control.token = nil
        redis.call('DEL', KEYS[7])
        redis.call('SET', KEYS[6], cjson.encode(control))
        guardIsLive = false
      end
    end
  end
  if guardIsLive and tonumber(redis.call('GET', KEYS[8])) ~= guard.scopeSequence then guardIsLive = false end
end
if not guardIsLive then
  record.state = 'canceled'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[3])
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  redis.call('ZREM', KEYS[4], ARGV[4])
  redis.call('ZREM', KEYS[5], ARGV[4])
  return 0
end
return 1
`;

const FINISH_OUTBOX_LUA = `
-- AIR_STUDIO_FINISH_OUTBOX_V4
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return false end
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= 'sending' then return false end
local nextState = ARGV[2]
if nextState ~= 'sent' and nextState ~= 'retryable_failure' and nextState ~= 'failed' and nextState ~= 'send_unknown' then return false end
if nextState == 'retryable_failure' and record.attempts >= tonumber(ARGV[6]) then nextState = 'failed' end
record.state = nextState
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[3])
if nextState == 'sent' or nextState == 'failed' then
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('DEL', KEYS[2])
else
  redis.call('SET', KEYS[1], cjson.encode(record))
end
redis.call('DEL', KEYS[3])
if nextState == 'retryable_failure' then
  redis.call('ZADD', KEYS[4], tonumber(ARGV[3]) + (record.attempts * tonumber(ARGV[7])), ARGV[5])
else
  redis.call('ZREM', KEYS[4], ARGV[5])
  -- A job's Like is deliberately best-effort. If its physical outcome is
  -- unknown, retain that audit state but release the paired acknowledgement
  -- behind it; otherwise a transient tapback outage could stall paid work.
  if nextState ~= 'send_unknown' or (record.lane == 'tapback' and record.jobId ~= nil) then
    redis.call('ZREM', KEYS[5], ARGV[5])
  end
end
return cjson.encode(record)
`;

const DEFER_OUTBOX_BEFORE_PHOTON_LUA = `
-- AIR_STUDIO_DEFER_OUTBOX_BEFORE_PHOTON_V1
-- This is intentionally narrower than a retryable delivery failure. The
-- caller has established that no Photon RPC has begun, so restore the exact
-- prior retry accounting instead of allowing a short Cron deadline to spend
-- the durable physical-send budget.
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return false end
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= 'sending' then return false end
if not redis.call('ZSCORE', KEYS[4], ARGV[3]) then return false end
local attempts = tonumber(record.attempts)
if attempts == nil or attempts < 1 or attempts ~= math.floor(attempts) then return false end
attempts = attempts - 1
record.attempts = attempts
-- A fresh intent has no prior physical retry. Any nonzero count came from a
-- real earlier retryable failure, which this worker must preserve.
record.state = attempts == 0 and 'intent' or 'retryable_failure'
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('DEL', KEYS[2])
redis.call('ZADD', KEYS[3], tonumber(ARGV[2]), ARGV[3])
return cjson.encode(record)
`;

const RECONCILE_EXPIRED_OUTBOX_LUA = `
-- AIR_STUDIO_RECONCILE_EXPIRED_OUTBOX_V4
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
if redis.call('GET', KEYS[3]) then return 0 end
local record = cjson.decode(raw)
if record.state ~= 'sending' then return 0 end
record.state = 'send_unknown'
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[1])
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZREM', KEYS[4], ARGV[3])
-- A crashed job-scoped Like is still an ambiguous physical effect, but it is
-- best-effort rather than a reason to indefinitely block its acknowledgement.
if record.lane == 'tapback' and record.jobId ~= nil then
  redis.call('ZREM', KEYS[5], ARGV[3])
end
return 1
`;

const COMPARE_AND_TRANSITION_JOB_LUA = `
-- AIR_STUDIO_TRANSITION_JOB_V5
-- KEYS: job metadata, encrypted job payload, active sender fence, job-ready
--       index, durable job-result cursor
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= ARGV[1] or record.stateVersion ~= tonumber(ARGV[2]) then return false end
record.state = ARGV[3]
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[4])
record.encryptedPayload = nil
if ARGV[7] == '1' then
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[5])
  redis.call('DEL', KEYS[2])
  -- Never delete a newer job's active marker after an old job's terminal
  -- transition. The value is the fencing identity for this particular job.
  if redis.call('GET', KEYS[3]) == ARGV[6] then redis.call('DEL', KEYS[3]) end
  redis.call('ZREM', KEYS[4], ARGV[6])
  -- This content-free cursor remains useful for delivery audit and an
  -- operator-confirmed requeue, but never outlives terminal metadata.
  if redis.call('EXISTS', KEYS[5]) == 1 then
    redis.call('EXPIRE', KEYS[5], ARGV[5])
  end
else
  redis.call('SET', KEYS[1], cjson.encode(record))
end
return cjson.encode(record)
`;

const CANCEL_ADMITTED_JOB_WITH_NOTIFICATION_LUA = `
-- AIR_STUDIO_CANCEL_ADMITTED_JOB_WITH_NOTIFICATION_V1
-- KEYS: active sender fence, job metadata, encrypted job payload, job-ready index,
--       cancellation outbox metadata, encrypted outbox payload, outbox-ready index,
--       cancellation lane index, shared release fence
-- ARGV: expected job id, expected sender hash, expected job version, outbox metadata,
--       encrypted outbox payload, payload ttl, outbox due time, outbox id, sequence,
--       release generation, release phase, now, terminal metadata ttl
local existingOutboxRaw = redis.call('GET', KEYS[5])
if existingOutboxRaw then
  local existingOk, existing = pcall(cjson.decode, existingOutboxRaw)
  if existingOk and type(existing) == 'table'
    and existing.outboxId == ARGV[8]
    and existing.jobId == ARGV[1]
    and existing.senderHash == ARGV[2]
    and existing.lane == 'request-response' then
    -- A recovered inbox may replay after the job transition committed but
    -- before its receipt was marked terminal. The original atomic notice is
    -- sufficient; never cancel a newer job or create another bubble.
    return 2
  end
  return -2
end

local outboxOk, outbox = pcall(cjson.decode, ARGV[4])
if not outboxOk or type(outbox) ~= 'table'
  or outbox.outboxId ~= ARGV[8]
  or outbox.jobId ~= ARGV[1]
  or outbox.senderHash ~= ARGV[2]
  or outbox.lane ~= 'request-response'
  or outbox.state ~= 'intent'
  or tonumber(outbox.stateVersion) ~= 1
  or tonumber(outbox.sequence) ~= tonumber(ARGV[9]) then return -2 end
local stamp = outbox.releaseStamp
if type(stamp) ~= 'table'
  or type(stamp.generation) ~= 'number' or stamp.generation < 0
  or stamp.generation ~= math.floor(stamp.generation)
  or stamp.generation ~= tonumber(ARGV[10]) or stamp.phase ~= ARGV[11] then return -1 end
local fenceRaw = redis.call('GET', KEYS[9])
if fenceRaw then
  local fenceOk, fence = pcall(cjson.decode, fenceRaw)
  if not fenceOk or type(fence) ~= 'table'
    or type(fence.generation) ~= 'number' or fence.generation < 0
    or fence.generation ~= math.floor(fence.generation)
    or (fence.phase ~= 'dark' and fence.phase ~= 'canary' and fence.phase ~= 'production') then return -1 end
  if fence.generation ~= tonumber(ARGV[10]) or fence.phase ~= ARGV[11] then return -1 end
elseif ARGV[11] ~= 'dark' then
  return -1
end

local active = redis.call('GET', KEYS[1])
if active ~= ARGV[1] then return 0 end
local jobRaw = redis.call('GET', KEYS[2])
if not jobRaw then return 0 end
local jobOk, job = pcall(cjson.decode, jobRaw)
if not jobOk or type(job) ~= 'table'
  or job.jobId ~= ARGV[1]
  or job.senderHash ~= ARGV[2]
  or job.state ~= 'admitted'
  or tonumber(job.stateVersion) ~= tonumber(ARGV[3]) then return 0 end
if redis.call('EXISTS', KEYS[6]) == 1 then return -2 end
if #redis.call('ZRANGEBYSCORE', KEYS[8], ARGV[9], ARGV[9], 'LIMIT', 0, 1) > 0 then return -2 end

job.state = 'canceled'
job.stateVersion = tonumber(job.stateVersion) + 1
job.updatedAtMs = tonumber(ARGV[12])
job.encryptedPayload = nil
redis.call('SET', KEYS[2], cjson.encode(job), 'EX', ARGV[13])
redis.call('DEL', KEYS[3])
if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) end
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('SET', KEYS[5], ARGV[4])
redis.call('SET', KEYS[6], ARGV[5], 'EX', ARGV[6])
redis.call('ZADD', KEYS[7], tonumber(ARGV[7]), ARGV[8])
redis.call('ZADD', KEYS[8], tonumber(ARGV[9]), ARGV[8])
return 1
`;

const RESCHEDULE_CLAIMED_JOB_LUA = `
-- AIR_STUDIO_RESCHEDULE_CLAIMED_JOB_V1
-- KEYS: job metadata, short worker lease, durable job-ready index
-- ARGV: lease token, expected state version, now, dueAt, job id
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then
  redis.call('ZREM', KEYS[3], ARGV[5])
  redis.call('DEL', KEYS[2])
  return 2
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' or record.stateVersion ~= tonumber(ARGV[2]) then return 0 end
if record.state ~= 'admitted' and record.state ~= 'executing'
  and record.state ~= 'ready' and record.state ~= 'delivering' then
  redis.call('ZREM', KEYS[3], ARGV[5])
  redis.call('DEL', KEYS[2])
  return 2
end
local now = tonumber(ARGV[3])
local dueAt = tonumber(ARGV[4])
if not now or not dueAt or dueAt < now then return 0 end
redis.call('ZADD', KEYS[3], dueAt, ARGV[5])
redis.call('DEL', KEYS[2])
return 1
`;

const BEGIN_PROVIDER_SUBMISSION_LUA = `
-- AIR_STUDIO_BEGIN_PROVIDER_SUBMISSION_V2
-- KEYS: provider subrun metadata, short execution lease, shared release fence, job metadata
-- ARGV: lease token, now, release generation, release phase, terminal metadata ttl,
--       expected job id, expected job state version
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return cjson.encode({ kind = 'lease_lost' }) end
local raw = redis.call('GET', KEYS[1])
if not raw then return cjson.encode({ kind = 'unavailable' }) end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then return cjson.encode({ kind = 'unavailable' }) end
local function persistPersistent()
  redis.call('SET', KEYS[1], cjson.encode(record))
end
local function persistTerminal()
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[5])
end
local function response(kind)
  return cjson.encode({ kind = kind, subrun = record })
end
if record.state == 'submitting' then
  -- A prior owner may have crossed the physical-call point before crashing.
  -- It is never safe to decide that a second submit is a retry.
  record.state = 'submission_unknown'
  record.stateVersion = tonumber(record.stateVersion or 0) + 1
  record.updatedAtMs = tonumber(ARGV[2])
  persistPersistent()
  return response('submission_unknown')
end
if record.state == 'submitted' then return response('already_submitted') end
if record.state == 'definitive_rejection' then return response('definitive_rejection') end
if record.state == 'submission_unknown' then return response('submission_unknown') end
if record.state == 'canceled' then return response('canceled') end
if record.state ~= 'reserved' then return cjson.encode({ kind = 'unavailable' }) end

-- A cancellation, failure, or state transition that completes before this
-- final durable authorization must win over a provider submit. We cannot make
-- an external provider call atomic with Redis after this script returns.
local jobRaw = redis.call('GET', KEYS[4])
local job = nil
if jobRaw then
  local decoded, parsed = pcall(cjson.decode, jobRaw)
  if decoded and type(parsed) == 'table' then job = parsed end
end
local jobValid = job ~= nil
  and job.jobId == ARGV[6]
  and record.jobId == ARGV[6]
  and job.state == 'executing'
  and type(job.stateVersion) == 'number'
  and job.stateVersion == math.floor(job.stateVersion)
  and job.stateVersion == tonumber(ARGV[7])
if not jobValid then
  record.state = 'canceled'
  record.stateVersion = tonumber(record.stateVersion or 0) + 1
  record.updatedAtMs = tonumber(ARGV[2])
  persistTerminal()
  return response('canceled')
end

local stamp = record.releaseStamp
local current = nil
local fenceRaw = redis.call('GET', KEYS[3])
if fenceRaw then
  local decoded, fence = pcall(cjson.decode, fenceRaw)
  if decoded and type(fence) == 'table' then current = fence end
end
local stampValid = type(stamp) == 'table'
  and type(stamp.generation) == 'number' and stamp.generation >= 1
  and stamp.generation == math.floor(stamp.generation)
  and (stamp.phase == 'canary' or stamp.phase == 'production')
  and stamp.generation == tonumber(ARGV[3]) and stamp.phase == ARGV[4]
local fenceValid = current ~= nil and current.schemaVersion == 1
  and type(current.generation) == 'number' and current.generation >= 1
  and current.generation == math.floor(current.generation)
  and (current.phase == 'canary' or current.phase == 'production')
  and current.generation == tonumber(ARGV[3]) and current.phase == ARGV[4]
if not stampValid or not fenceValid then
  record.state = 'canceled'
  record.stateVersion = tonumber(record.stateVersion or 0) + 1
  record.updatedAtMs = tonumber(ARGV[2])
  persistTerminal()
  return response('release_blocked')
end
record.state = 'submitting'
record.stateVersion = tonumber(record.stateVersion or 0) + 1
record.updatedAtMs = tonumber(ARGV[2])
persistPersistent()
return response('ready')
`;

const FINISH_PROVIDER_SUBMISSION_LUA = `
-- AIR_STUDIO_FINISH_PROVIDER_SUBMISSION_V2
-- KEYS: provider subrun metadata, encrypted provider reference, short execution lease,
--       encrypted job payload, job metadata, shared release fence
-- ARGV: lease token, accepted|definitive_rejection|ambiguous, encrypted reference, now,
--       terminal metadata ttl, release generation, release phase, expected job id,
--       expected job state version
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return false end
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' or record.state ~= 'submitting' then return false end
if ARGV[2] == 'accepted' then
  if ARGV[3] == '' then return false end
  -- The source payload's actual Redis TTL is the only privacy deadline that
  -- matters here. Do not trust a worker clock or create a fresh 24-hour TTL.
  local payloadTtl = redis.call('PTTL', KEYS[4])
  record.stateVersion = tonumber(record.stateVersion or 0) + 1
  record.updatedAtMs = tonumber(ARGV[4])
  if not payloadTtl or payloadTtl < 1 then
    record.state = 'submission_unknown'
    redis.call('DEL', KEYS[2])
    redis.call('SET', KEYS[1], cjson.encode(record))
    redis.call('DEL', KEYS[3])
    return cjson.encode(record)
  end

  local job = nil
  local jobRaw = redis.call('GET', KEYS[5])
  if jobRaw then
    local decoded, parsed = pcall(cjson.decode, jobRaw)
    if decoded and type(parsed) == 'table' then job = parsed end
  end
  local jobValid = job ~= nil
    and job.jobId == ARGV[8]
    and record.jobId == ARGV[8]
    and job.state == 'executing'
    and type(job.stateVersion) == 'number'
    and job.stateVersion == math.floor(job.stateVersion)
    and job.stateVersion == tonumber(ARGV[9])

  local current = nil
  local fenceRaw = redis.call('GET', KEYS[6])
  if fenceRaw then
    local decoded, fence = pcall(cjson.decode, fenceRaw)
    if decoded and type(fence) == 'table' then current = fence end
  end
  local stamp = record.releaseStamp
  local stampValid = type(stamp) == 'table'
    and type(stamp.generation) == 'number' and stamp.generation >= 1
    and stamp.generation == math.floor(stamp.generation)
    and (stamp.phase == 'canary' or stamp.phase == 'production')
    and stamp.generation == tonumber(ARGV[6]) and stamp.phase == ARGV[7]
  local fenceValid = current ~= nil and current.schemaVersion == 1
    and type(current.generation) == 'number' and current.generation >= 1
    and current.generation == math.floor(current.generation)
    and (current.phase == 'canary' or current.phase == 'production')
    and current.generation == tonumber(ARGV[6]) and current.phase == ARGV[7]

  -- Keep the provider reference only while the source payload is live. If a
  -- post-begin cancellation or rollback won the race, retain that bounded
  -- reference for reconciliation but never claim a clean submitted state.
  redis.call('SET', KEYS[2], ARGV[3], 'PX', payloadTtl)
  if jobValid and stampValid and fenceValid then
    record.state = 'submitted'
  else
    record.state = 'submission_unknown'
  end
  redis.call('SET', KEYS[1], cjson.encode(record))
elseif ARGV[2] == 'definitive_rejection' then
  record.state = 'definitive_rejection'
  record.stateVersion = tonumber(record.stateVersion or 0) + 1
  record.updatedAtMs = tonumber(ARGV[4])
  redis.call('DEL', KEYS[2])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[5])
elseif ARGV[2] == 'ambiguous' then
  record.state = 'submission_unknown'
  record.stateVersion = tonumber(record.stateVersion or 0) + 1
  record.updatedAtMs = tonumber(ARGV[4])
  redis.call('SET', KEYS[1], cjson.encode(record))
else
  return false
end
redis.call('DEL', KEYS[3])
return cjson.encode(record)
`;

const BEGIN_PROVIDER_OBSERVATION_LUA = `
-- AIR_STUDIO_BEGIN_PROVIDER_OBSERVATION_V1
-- KEYS: opaque observation metadata, short observation lease, provider subrun
--       metadata, encrypted provider request reference, job metadata
-- ARGV: lease token, now, terminal metadata ttl, expected observation id,
--       expected subrun id, expected job id
local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end
local function positiveInteger(value)
  return nonnegativeInteger(value) and value >= 1
end
local function opaque(value)
  return type(value) == 'string' and string.match(value, '^[a-f0-9]+$') and string.len(value) == 64
end
local function safeText(value, maximum)
  return type(value) == 'string' and string.len(value) >= 1 and string.len(value) <= maximum
    and string.match(value, '^%s') == nil and string.match(value, '%s$') == nil
    and string.find(value, '[%z\r\n]') == nil
end
local function terminalState(state)
  return state == 'succeeded' or state == 'failed' or state == 'deadline_exceeded'
    or state == 'attempts_exhausted' or state == 'reference_expired' or state == 'canceled'
end
local function validObservation(record)
  return type(record) == 'table'
    and record.schemaVersion == 1
    and opaque(record.observationId)
    and opaque(record.subrunId)
    and safeText(record.jobId, 160)
    and positiveInteger(record.subrunStateVersion)
    and positiveInteger(record.jobStateVersion)
    and (record.state == 'scheduled' or record.state == 'observing' or terminalState(record.state))
    and nonnegativeInteger(record.pollAttempts)
    and positiveInteger(record.maxPollAttempts) and record.maxPollAttempts <= 10000
    and record.pollAttempts <= record.maxPollAttempts
    and positiveInteger(record.requestTimeoutMs) and record.requestTimeoutMs >= 1000 and record.requestTimeoutMs <= 900000
    and positiveInteger(record.pollIntervalMs) and record.pollIntervalMs >= 250 and record.pollIntervalMs <= 300000
    and nonnegativeInteger(record.deadlineAtMs)
    and nonnegativeInteger(record.nextDueAtMs) and record.nextDueAtMs <= record.deadlineAtMs
    and positiveInteger(record.stateVersion)
    and nonnegativeInteger(record.createdAtMs)
    and nonnegativeInteger(record.updatedAtMs)
    and ((record.state == 'failed' and (record.failureDisposition == nil
      or record.failureDisposition == 'failed' or record.failureDisposition == 'reconciliation_required'))
      or (record.state ~= 'failed' and record.failureDisposition == nil))
end
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return cjson.encode({ kind = 'lease_lost' }) end
local raw = redis.call('GET', KEYS[1])
if not raw then return cjson.encode({ kind = 'unavailable' }) end
local ok, observation = pcall(cjson.decode, raw)
if not ok or not validObservation(observation)
  or observation.observationId ~= ARGV[4]
  or observation.subrunId ~= ARGV[5]
  or observation.jobId ~= ARGV[6] then return cjson.encode({ kind = 'unavailable' }) end
local function response(kind)
  return cjson.encode({ kind = kind, observation = observation })
end
local function persistPersistent()
  redis.call('SET', KEYS[1], cjson.encode(observation))
end
local function terminal(kind, state)
  observation.state = state
  observation.failureDisposition = nil
  observation.stateVersion = observation.stateVersion + 1
  observation.updatedAtMs = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], cjson.encode(observation), 'EX', ARGV[3])
  redis.call('DEL', KEYS[2])
  return response(kind)
end
if terminalState(observation.state) then
  redis.call('DEL', KEYS[2])
  return response(observation.state)
end

-- The observation never transitions a subrun toward submission. It is valid
-- only for the exact durable submitted state and executing job that created
-- it, so a stale worker cannot write a later provider-read outcome.
local subrunRaw = redis.call('GET', KEYS[3])
local subrun = nil
if subrunRaw then
  local decoded, parsed = pcall(cjson.decode, subrunRaw)
  if decoded and type(parsed) == 'table' then subrun = parsed end
end
local jobRaw = redis.call('GET', KEYS[5])
local job = nil
if jobRaw then
  local decoded, parsed = pcall(cjson.decode, jobRaw)
  if decoded and type(parsed) == 'table' then job = parsed end
end
local subrunValid = subrun ~= nil
  and subrun.subrunId == observation.subrunId
  and subrun.jobId == observation.jobId
  and subrun.state == 'submitted'
  and type(subrun.stateVersion) == 'number'
  and subrun.stateVersion == math.floor(subrun.stateVersion)
  and subrun.stateVersion == observation.subrunStateVersion
local jobValid = job ~= nil
  and job.jobId == observation.jobId
  and job.state == 'executing'
  and type(job.stateVersion) == 'number'
  and job.stateVersion == math.floor(job.stateVersion)
  and job.stateVersion == observation.jobStateVersion
if not subrunValid or not jobValid then return terminal('canceled', 'canceled') end

local now = tonumber(ARGV[2])
if observation.state == 'scheduled' and now < observation.nextDueAtMs then
  redis.call('DEL', KEYS[2])
  return response('not_due')
end
if observation.state ~= 'scheduled' and observation.state ~= 'observing' then
  return cjson.encode({ kind = 'unavailable' })
end
if now >= observation.deadlineAtMs then return terminal('deadline_exceeded', 'deadline_exceeded') end
if observation.pollAttempts >= observation.maxPollAttempts then return terminal('attempts_exhausted', 'attempts_exhausted') end

-- A GET must not start when the encrypted request ID cannot survive its
-- reviewed request timeout. We never renew this privacy-bound reference.
local referenceTtl = redis.call('PTTL', KEYS[4])
if not referenceTtl or referenceTtl <= observation.requestTimeoutMs then
  return terminal('reference_expired', 'reference_expired')
end
observation.state = 'observing'
observation.pollAttempts = observation.pollAttempts + 1
observation.nextDueAtMs = now
observation.stateVersion = observation.stateVersion + 1
observation.updatedAtMs = now
persistPersistent()
return response('ready')
`;

const FINISH_PROVIDER_OBSERVATION_LUA = `
-- AIR_STUDIO_FINISH_PROVIDER_OBSERVATION_V2
-- KEYS: opaque observation metadata, short observation lease, provider subrun
--       metadata, job metadata
-- ARGV: lease token, pending|succeeded|failed, now, terminal metadata ttl,
--       expected observation id, expected subrun id, expected job id,
--       failed|reconciliation_required disposition (only for failed)
local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end
local function positiveInteger(value)
  return nonnegativeInteger(value) and value >= 1
end
local function opaque(value)
  return type(value) == 'string' and string.match(value, '^[a-f0-9]+$') and string.len(value) == 64
end
local function safeText(value, maximum)
  return type(value) == 'string' and string.len(value) >= 1 and string.len(value) <= maximum
    and string.match(value, '^%s') == nil and string.match(value, '%s$') == nil
    and string.find(value, '[%z\r\n]') == nil
end
local function terminalState(state)
  return state == 'succeeded' or state == 'failed' or state == 'deadline_exceeded'
    or state == 'attempts_exhausted' or state == 'reference_expired' or state == 'canceled'
end
local function validObservation(record)
  return type(record) == 'table'
    and record.schemaVersion == 1
    and opaque(record.observationId)
    and opaque(record.subrunId)
    and safeText(record.jobId, 160)
    and positiveInteger(record.subrunStateVersion)
    and positiveInteger(record.jobStateVersion)
    and (record.state == 'scheduled' or record.state == 'observing' or terminalState(record.state))
    and nonnegativeInteger(record.pollAttempts)
    and positiveInteger(record.maxPollAttempts) and record.maxPollAttempts <= 10000
    and record.pollAttempts <= record.maxPollAttempts
    and positiveInteger(record.requestTimeoutMs) and record.requestTimeoutMs >= 1000 and record.requestTimeoutMs <= 900000
    and positiveInteger(record.pollIntervalMs) and record.pollIntervalMs >= 250 and record.pollIntervalMs <= 300000
    and nonnegativeInteger(record.deadlineAtMs)
    and nonnegativeInteger(record.nextDueAtMs) and record.nextDueAtMs <= record.deadlineAtMs
    and positiveInteger(record.stateVersion)
    and nonnegativeInteger(record.createdAtMs)
    and nonnegativeInteger(record.updatedAtMs)
    and ((record.state == 'failed' and (record.failureDisposition == nil
      or record.failureDisposition == 'failed' or record.failureDisposition == 'reconciliation_required'))
      or (record.state ~= 'failed' and record.failureDisposition == nil))
end
if (ARGV[2] == 'failed' and ARGV[8] ~= 'failed' and ARGV[8] ~= 'reconciliation_required')
  or (ARGV[2] ~= 'failed' and ARGV[8] ~= '') then
  return cjson.encode({ kind = 'unavailable' })
end
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return cjson.encode({ kind = 'lease_lost' }) end
local raw = redis.call('GET', KEYS[1])
if not raw then return cjson.encode({ kind = 'unavailable' }) end
local ok, observation = pcall(cjson.decode, raw)
if not ok or not validObservation(observation)
  or observation.observationId ~= ARGV[5]
  or observation.subrunId ~= ARGV[6]
  or observation.jobId ~= ARGV[7]
  or observation.state ~= 'observing' then return cjson.encode({ kind = 'unavailable' }) end
local function response(kind)
  return cjson.encode({ kind = kind, observation = observation })
end
local function persistPersistent()
  redis.call('SET', KEYS[1], cjson.encode(observation))
end
local function terminal(kind, state, failureDisposition)
  observation.state = state
  if state == 'failed' then
    observation.failureDisposition = failureDisposition
  else
    observation.failureDisposition = nil
  end
  observation.stateVersion = observation.stateVersion + 1
  observation.updatedAtMs = tonumber(ARGV[3])
  redis.call('SET', KEYS[1], cjson.encode(observation), 'EX', ARGV[4])
  redis.call('DEL', KEYS[2])
  return response(kind)
end

local subrunRaw = redis.call('GET', KEYS[3])
local subrun = nil
if subrunRaw then
  local decoded, parsed = pcall(cjson.decode, subrunRaw)
  if decoded and type(parsed) == 'table' then subrun = parsed end
end
local jobRaw = redis.call('GET', KEYS[4])
local job = nil
if jobRaw then
  local decoded, parsed = pcall(cjson.decode, jobRaw)
  if decoded and type(parsed) == 'table' then job = parsed end
end
local subrunValid = subrun ~= nil
  and subrun.subrunId == observation.subrunId
  and subrun.jobId == observation.jobId
  and subrun.state == 'submitted'
  and type(subrun.stateVersion) == 'number'
  and subrun.stateVersion == math.floor(subrun.stateVersion)
  and subrun.stateVersion == observation.subrunStateVersion
local jobValid = job ~= nil
  and job.jobId == observation.jobId
  and job.state == 'executing'
  and type(job.stateVersion) == 'number'
  and job.stateVersion == math.floor(job.stateVersion)
  and job.stateVersion == observation.jobStateVersion
if not subrunValid or not jobValid then return terminal('canceled', 'canceled') end

if ARGV[2] == 'succeeded' then return terminal('succeeded', 'succeeded') end
if ARGV[2] == 'failed' then return terminal('failed', 'failed', ARGV[8]) end
if ARGV[2] ~= 'pending' then return cjson.encode({ kind = 'unavailable' }) end

local now = tonumber(ARGV[3])
if now >= observation.deadlineAtMs then return terminal('deadline_exceeded', 'deadline_exceeded') end
if observation.pollAttempts >= observation.maxPollAttempts then return terminal('attempts_exhausted', 'attempts_exhausted') end
local nextDue = now + observation.pollIntervalMs
if nextDue > observation.deadlineAtMs then nextDue = observation.deadlineAtMs end
observation.state = 'scheduled'
observation.nextDueAtMs = nextDue
observation.stateVersion = observation.stateVersion + 1
observation.updatedAtMs = now
persistPersistent()
redis.call('DEL', KEYS[2])
return response('pending')
`;

const FINALIZE_PREPARED_PROVIDER_RESULT_LUA = `
-- AIR_STUDIO_FINALIZE_PREPARED_PROVIDER_RESULT_V1
-- KEYS: observation/lease, submitted subrun, job/payload, prepared output
--       metadata/reference, result cursor, preamble metadata/payload,
--       attachment metadata/payload, ready outbox, result lane, ready job,
--       shared release fence
-- ARGV: lease token, now, terminal ttl, observation/subrun/job/preamble/
--       attachment ids, proposed encrypted preamble/attachment, release stamp
local function keyType(key)
  local response = redis.call('TYPE', key)
  return response and response.ok or ''
end
local function stringOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'string'
end
local function zsetOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'zset'
end
local function positive(value)
  return type(value) == 'number' and value >= 1 and value == math.floor(value)
end
local function nonnegative(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end
local function opaque(value)
  return type(value) == 'string' and #value == 64 and string.match(value, '^[a-f0-9]+$') ~= nil
end
local function text(value, maximum)
  return type(value) == 'string' and #value >= 1 and #value <= maximum
    and string.match(value, '^%s') == nil and string.match(value, '%s$') == nil
    and string.find(value, '[%z\r\n]') == nil
end
local function stamp(value)
  return type(value) == 'table' and nonnegative(value.generation)
    and (value.phase == 'dark' or value.phase == 'canary' or value.phase == 'production')
    and (value.phase == 'dark' or value.generation >= 1)
end
local function sameStamp(left, right)
  return stamp(left) and stamp(right)
    and left.generation == right.generation and left.phase == right.phase
end
local function encrypted(value)
  return type(value) == 'table' and value.version == 1 and value.algorithm == 'aes-256-gcm'
    and type(value.iv) == 'string' and type(value.ciphertext) == 'string' and type(value.tag) == 'string'
end
local function parse(key)
  local raw = redis.call('GET', key)
  if not raw then return nil end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' then return false end
  return value
end
local function response(kind) return cjson.encode({ kind = kind }) end
local function validSubrun(value)
  return type(value) == 'table' and value.schemaVersion == 1 and opaque(value.subrunId)
    and text(value.jobId, 160) and (value.provider == 'gmi' or value.provider == 'fal')
    and text(value.modelSlug, 80) and text(value.workflowId, 160)
    and value.stepId == 'provider-generation' and stamp(value.releaseStamp)
    and opaque(value.submissionIntentId) and value.state == 'submitted'
    and positive(value.stateVersion) and nonnegative(value.createdAtMs) and nonnegative(value.updatedAtMs)
end
local function validJob(value)
  return type(value) == 'table' and value.schemaVersion == 3 and text(value.jobId, 160)
    and opaque(value.senderHash) and opaque(value.threadHash)
    and (value.mode == 'imagine' or value.mode == 'animate' or value.mode == 'edit-image' or value.mode == 'edit-video')
    and text(value.modelSlug, 80) and (value.provider == 'gmi' or value.provider == 'fal')
    and value.outputCount == 1 and nonnegative(value.reservedCents) and stamp(value.releaseStamp)
    and positive(value.stateVersion) and nonnegative(value.createdAtMs) and nonnegative(value.updatedAtMs)
end
local function validObservation(value)
  return type(value) == 'table' and value.schemaVersion == 1 and opaque(value.observationId)
    and opaque(value.subrunId) and text(value.jobId, 160)
    and positive(value.subrunStateVersion) and positive(value.jobStateVersion)
    and (value.state == 'scheduled' or value.state == 'observing' or value.state == 'succeeded'
      or value.state == 'failed' or value.state == 'deadline_exceeded' or value.state == 'attempts_exhausted'
      or value.state == 'reference_expired' or value.state == 'canceled')
    and nonnegative(value.pollAttempts) and positive(value.maxPollAttempts) and value.maxPollAttempts <= 10000
    and value.pollAttempts <= value.maxPollAttempts
    and positive(value.requestTimeoutMs) and value.requestTimeoutMs >= 1000 and value.requestTimeoutMs <= 900000
    and positive(value.pollIntervalMs) and value.pollIntervalMs >= 250 and value.pollIntervalMs <= 300000
    and nonnegative(value.deadlineAtMs) and nonnegative(value.nextDueAtMs) and value.nextDueAtMs <= value.deadlineAtMs
    and positive(value.stateVersion) and nonnegative(value.createdAtMs) and nonnegative(value.updatedAtMs)
    and ((value.state == 'failed' and (value.failureDisposition == nil
      or value.failureDisposition == 'failed' or value.failureDisposition == 'reconciliation_required'))
      or (value.state ~= 'failed' and value.failureDisposition == nil))
end
local function validPreparation(value, jobId)
  return type(value) == 'table' and value.schemaVersion == 1 and opaque(value.preparationId)
    and value.jobId == jobId and value.outputIndex == 1 and value.state == 'prepared'
    and positive(value.stateVersion) and nonnegative(value.createdAtMs) and nonnegative(value.updatedAtMs)
end
local function validOutbox(value, job, outboxId, sequence, resultIndex, currentStamp, existing)
  if type(value) ~= 'table' or value.schemaVersion ~= 3 or value.outboxId ~= outboxId
    or value.jobId ~= job.jobId or value.senderHash ~= job.senderHash or value.lane ~= 'job-result'
    or value.sequence ~= sequence or not nonnegative(value.attempts) or not positive(value.stateVersion)
    or not stamp(value.releaseStamp) or value.laneIdentity ~= nil or value.pendingGuard ~= nil
    or not nonnegative(value.createdAtMs) or not nonnegative(value.dueAtMs) or not nonnegative(value.updatedAtMs) then
    return false
  end
  if resultIndex == nil then
    if value.resultOutputIndex ~= nil then return false end
  elseif value.resultOutputIndex ~= resultIndex then return false end
  if existing then
    return value.state == 'intent' or value.state == 'sending' or value.state == 'sent'
      or value.state == 'retryable_failure' or value.state == 'failed' or value.state == 'canceled'
      or value.state == 'send_unknown' or value.state == 'confirmed_absent' or value.state == 'requeued'
  end
  return value.state == 'intent' and value.attempts == 0 and value.stateVersion == 1
    and value.terminalReason == nil and sameStamp(value.releaseStamp, currentStamp)
end
local function validCursor(value, job, attachmentId)
  return type(value) == 'table' and value.schemaVersion == 1 and value.jobId == job.jobId
    and value.senderHash == job.senderHash and value.threadHash == job.threadHash
    and value.outputCount == 1 and sameStamp(value.releaseStamp, job.releaseStamp)
    and value.nextOutputIndex == 2 and type(value.outputs) == 'table' and #value.outputs == 1
    and type(value.outputs[1]) == 'table' and value.outputs[1].outputIndex == 1
    and value.outputs[1].outboxId == attachmentId
    and nonnegative(value.createdAtMs) and nonnegative(value.updatedAtMs)
end

local now = tonumber(ARGV[2])
local terminalTtl = tonumber(ARGV[3])
local generation = tonumber(ARGV[13])
if not text(ARGV[1], 256) or not nonnegative(now) or not positive(terminalTtl)
  or not opaque(ARGV[4]) or not opaque(ARGV[5]) or not text(ARGV[6], 160)
  or not opaque(ARGV[7]) or not opaque(ARGV[8]) or not nonnegative(generation)
  or (ARGV[14] ~= 'dark' and ARGV[14] ~= 'canary' and ARGV[14] ~= 'production') then
  return response('unavailable')
end
if not stringOrMissing(KEYS[1]) or not stringOrMissing(KEYS[2]) or not stringOrMissing(KEYS[3])
  or not stringOrMissing(KEYS[4]) or not stringOrMissing(KEYS[5]) or not stringOrMissing(KEYS[6])
  or not stringOrMissing(KEYS[7]) or not stringOrMissing(KEYS[8]) or not stringOrMissing(KEYS[9])
  or not stringOrMissing(KEYS[10]) or not stringOrMissing(KEYS[11]) or not stringOrMissing(KEYS[12])
  or not zsetOrMissing(KEYS[13]) or not zsetOrMissing(KEYS[14]) or not zsetOrMissing(KEYS[15])
  or not stringOrMissing(KEYS[16]) then return response('unavailable') end
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return response('lease_lost') end

local observation = parse(KEYS[1])
local subrun = parse(KEYS[3])
local job = parse(KEYS[4])
if observation == false or subrun == false or job == false or not validObservation(observation)
  or not validSubrun(subrun) or not validJob(job)
  or observation.observationId ~= ARGV[4] or observation.subrunId ~= ARGV[5] or observation.jobId ~= ARGV[6]
  or subrun.subrunId ~= ARGV[5] or subrun.jobId ~= ARGV[6] or job.jobId ~= ARGV[6]
  or observation.subrunStateVersion ~= subrun.stateVersion
  or not sameStamp(subrun.releaseStamp, job.releaseStamp) then return response('unavailable') end

-- A full prior atomic commit is the only condition under which this method
-- returns existing. The re-acquired observation lease is still checked above.
if observation.state == 'succeeded' and job.state == 'ready'
  and job.stateVersion == observation.jobStateVersion + 1 then
  local cursor = parse(KEYS[8])
  local preamble = parse(KEYS[9])
  local attachment = parse(KEYS[11])
  if cursor ~= false and preamble ~= false and attachment ~= false
    and validCursor(cursor, job, ARGV[8])
    and validOutbox(preamble, job, ARGV[7], 0, nil, nil, true)
    and validOutbox(attachment, job, ARGV[8], 1, 1, nil, true)
    and sameStamp(preamble.releaseStamp, attachment.releaseStamp) then
    return response('existing')
  end
  return response('unavailable')
end

if (observation.state ~= 'scheduled' and observation.state ~= 'observing')
  or job.state ~= 'executing' or job.stateVersion ~= observation.jobStateVersion then
  return response('unavailable')
end
local preparation = parse(KEYS[6])
local preparedReference = parse(KEYS[7])
local jobPayload = parse(KEYS[5])
if preparation == false or preparedReference == false or jobPayload == false
  or not validPreparation(preparation, job.jobId) or not encrypted(preparedReference)
  or not encrypted(jobPayload) then return response('unavailable') end
local payloadTtl = redis.call('PTTL', KEYS[5])
local preparedTtl = redis.call('PTTL', KEYS[7])
if not payloadTtl or not preparedTtl or payloadTtl < 1 or preparedTtl < 1 then
  return response('payload_expiring')
end
if ARGV[14] ~= 'canary' and ARGV[14] ~= 'production' or generation < 1 then
  return response('release_blocked')
end
local currentStamp = { generation = generation, phase = ARGV[14] }
local fence = parse(KEYS[16])
if fence == false or not fence or fence.schemaVersion ~= 1 or not sameStamp(fence, currentStamp) then
  return response('release_blocked')
end
if redis.call('GET', KEYS[8])
  or redis.call('EXISTS', KEYS[9]) == 1 or redis.call('EXISTS', KEYS[10]) == 1
  or redis.call('EXISTS', KEYS[11]) == 1 or redis.call('EXISTS', KEYS[12]) == 1
  or #redis.call('ZRANGEBYSCORE', KEYS[14], 0, 0, 'LIMIT', 0, 1) > 0
  or #redis.call('ZRANGEBYSCORE', KEYS[14], 1, 1, 'LIMIT', 0, 1) > 0 then
  return response('unavailable')
end
local preambleOk, preamble = pcall(cjson.decode, ARGV[9])
local preamblePayloadOk, preamblePayload = pcall(cjson.decode, ARGV[10])
local attachmentOk, attachment = pcall(cjson.decode, ARGV[11])
local attachmentPayloadOk, attachmentPayload = pcall(cjson.decode, ARGV[12])
if not preambleOk or not preamblePayloadOk or not attachmentOk or not attachmentPayloadOk
  or not validOutbox(preamble, job, ARGV[7], 0, nil, currentStamp, false)
  or not validOutbox(attachment, job, ARGV[8], 1, 1, currentStamp, false)
  or not encrypted(preamblePayload) or not encrypted(attachmentPayload) then
  return response('unavailable')
end

local deliveryTtl = math.min(payloadTtl, preparedTtl)
local cursor = {
  schemaVersion = 1, jobId = job.jobId, senderHash = job.senderHash, threadHash = job.threadHash,
  outputCount = 1, releaseStamp = job.releaseStamp, nextOutputIndex = 2,
  outputs = { { outputIndex = 1, outboxId = ARGV[8] } }, createdAtMs = now, updatedAtMs = now
}
observation.state = 'succeeded'
observation.stateVersion = observation.stateVersion + 1
observation.updatedAtMs = now
job.state = 'ready'
job.stateVersion = job.stateVersion + 1
job.updatedAtMs = now
redis.call('SET', KEYS[8], cjson.encode(cursor))
redis.call('SET', KEYS[9], ARGV[9])
redis.call('SET', KEYS[10], ARGV[10], 'PX', deliveryTtl)
redis.call('SET', KEYS[11], ARGV[11])
redis.call('SET', KEYS[12], ARGV[12], 'PX', deliveryTtl)
redis.call('ZADD', KEYS[13], preamble.dueAtMs, ARGV[7])
redis.call('ZADD', KEYS[13], attachment.dueAtMs, ARGV[8])
redis.call('ZADD', KEYS[14], 0, ARGV[7])
redis.call('ZADD', KEYS[14], 1, ARGV[8])
redis.call('SET', KEYS[1], cjson.encode(observation), 'EX', terminalTtl)
redis.call('DEL', KEYS[2])
redis.call('SET', KEYS[4], cjson.encode(job))
-- Result delivery remains its own durable recovery phase. Keep the job due
-- so a crash after finalization cannot strand the ordered outbox lifecycle.
redis.call('ZADD', KEYS[15], now, job.jobId)
return response('finalized')
`;

const RECOVER_EXPIRED_JOB_RESULT_PREPARATION_LUA = `
-- AIR_STUDIO_RECOVER_EXPIRED_JOB_RESULT_PREPARATION_V1
-- KEYS: opaque preparation metadata/reference/lease, job metadata, ready jobs
-- ARGV: recovered lease token, now, job id, output index
local function keyType(key)
  local response = redis.call('TYPE', key)
  return response and response.ok or ''
end
local function stringOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'string'
end
local function zsetOrMissing(key)
  local kind = keyType(key)
  return kind == 'none' or kind == 'zset'
end
local function positive(value)
  return type(value) == 'number' and value >= 1 and value == math.floor(value)
end
local function nonnegative(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end
local function opaque(value)
  return type(value) == 'string' and #value == 64 and string.match(value, '^[a-f0-9]+$') ~= nil
end
local function text(value, maximum)
  return type(value) == 'string' and #value >= 1 and #value <= maximum
    and string.match(value, '^%s') == nil and string.match(value, '%s$') == nil
    and string.find(value, '[%z\r\n]') == nil
end
local function parse(key)
  local raw = redis.call('GET', key)
  if not raw then return nil end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' then return false end
  return value
end
local function response(kind, preparation)
  return cjson.encode({ kind = kind, preparation = preparation })
end
local function validPreparation(value, jobId, outputIndex)
  return type(value) == 'table' and value.schemaVersion == 1 and opaque(value.preparationId)
    and value.jobId == jobId and value.outputIndex == outputIndex
    and (value.state == 'reserved' or value.state == 'uploading' or value.state == 'prepared'
      or value.state == 'upload_unknown' or value.state == 'failed' or value.state == 'canceled')
    and positive(value.stateVersion) and nonnegative(value.createdAtMs) and nonnegative(value.updatedAtMs)
end

local now = tonumber(ARGV[2])
local outputIndex = tonumber(ARGV[4])
if not text(ARGV[1], 256) or not nonnegative(now) or not text(ARGV[3], 160)
  or not positive(outputIndex) or outputIndex > 4 then
  return cjson.encode({ kind = 'unavailable' })
end
if not stringOrMissing(KEYS[1]) or not stringOrMissing(KEYS[2]) or not stringOrMissing(KEYS[3])
  or not stringOrMissing(KEYS[4]) or not zsetOrMissing(KEYS[5]) then
  return cjson.encode({ kind = 'unavailable' })
end
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return cjson.encode({ kind = 'lease_lost' }) end

local preparation = parse(KEYS[1])
if preparation == false or not validPreparation(preparation, ARGV[3], outputIndex) then
  return cjson.encode({ kind = 'unavailable' })
end

-- This recovery primitive has no upload authorization branch. In particular,
-- 'reserved' remains reserved; only an already-recorded 'uploading' state can
-- become terminally ambiguous after its former lease expired.
if preparation.state ~= 'uploading' then
  return response(preparation.state, preparation)
end

preparation.state = 'upload_unknown'
preparation.stateVersion = preparation.stateVersion + 1
preparation.updatedAtMs = now
redis.call('SET', KEYS[1], cjson.encode(preparation))
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[3])

local job = parse(KEYS[4])
if job ~= false and type(job) == 'table' and job.jobId == ARGV[3]
  and (job.state == 'executing' or job.state == 'ready' or job.state == 'delivering')
  and positive(job.stateVersion) then
  job.state = 'reconciliation_required'
  job.stateVersion = job.stateVersion + 1
  job.updatedAtMs = now
  redis.call('SET', KEYS[4], cjson.encode(job))
  redis.call('ZREM', KEYS[5], ARGV[3])
end
return response('upload_unknown', preparation)
`;

const BEGIN_JOB_RESULT_PREPARATION_LUA = `
-- AIR_STUDIO_BEGIN_JOB_RESULT_PREPARATION_V1
-- KEYS: opaque preparation metadata, encrypted prepared Photon GUID, upload
--       lease, shared release fence, job metadata, encrypted job payload,
--       durable job-ready index, durable result cursor
-- ARGV: lease token, now, release generation, release phase, terminal ttl,
--       job id, expected job version, minimum payload remaining ms, output index
local function positiveInteger(value)
  return type(value) == 'number' and value >= 1 and value == math.floor(value)
end

local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end

local function opaque(value)
  return type(value) == 'string' and #value == 64 and string.match(value, '^[a-f0-9]+$') ~= nil
end

local function validPreparation(record)
  return type(record) == 'table'
    and record.schemaVersion == 1
    and opaque(record.preparationId)
    and type(record.jobId) == 'string' and #record.jobId > 0 and #record.jobId <= 160
    and positiveInteger(record.outputIndex) and record.outputIndex <= 4
    and (record.state == 'reserved' or record.state == 'uploading' or record.state == 'prepared'
      or record.state == 'upload_unknown' or record.state == 'failed' or record.state == 'canceled')
    and positiveInteger(record.stateVersion)
    and nonnegativeInteger(record.createdAtMs)
    and nonnegativeInteger(record.updatedAtMs)
end

local function response(kind, preparation)
  return cjson.encode({ kind = kind, preparation = preparation })
end

local function activeRelease()
  if ARGV[4] ~= 'canary' and ARGV[4] ~= 'production' then return false end
  local generation = tonumber(ARGV[3])
  if not positiveInteger(generation) then return false end
  local raw = redis.call('GET', KEYS[4])
  if not raw then return false end
  local ok, fence = pcall(cjson.decode, raw)
  return ok and type(fence) == 'table'
    and fence.schemaVersion == 1
    and fence.generation == generation
    and fence.phase == ARGV[4]
end

local raw = redis.call('GET', KEYS[1])
if not raw then return cjson.encode({ kind = 'unavailable' }) end
local ok, preparation = pcall(cjson.decode, raw)
if not ok or not validPreparation(preparation)
  or preparation.jobId ~= ARGV[6]
  or preparation.outputIndex ~= tonumber(ARGV[9]) then return cjson.encode({ kind = 'unavailable' }) end
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return cjson.encode({ kind = 'lease_lost' }) end

local function reconcileUnknown()
  preparation.state = 'upload_unknown'
  preparation.stateVersion = preparation.stateVersion + 1
  preparation.updatedAtMs = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], cjson.encode(preparation))
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  local jobRaw = redis.call('GET', KEYS[5])
  if jobRaw then
    local jobOk, job = pcall(cjson.decode, jobRaw)
    if jobOk and type(job) == 'table'
      and job.jobId == ARGV[6]
      and (job.state == 'executing' or job.state == 'ready' or job.state == 'delivering') then
      job.state = 'reconciliation_required'
      job.stateVersion = tonumber(job.stateVersion or 0) + 1
      job.updatedAtMs = tonumber(ARGV[2])
      redis.call('SET', KEYS[5], cjson.encode(job))
      redis.call('ZREM', KEYS[7], ARGV[6])
    end
  end
  return response('upload_unknown', preparation)
end

if preparation.state == 'uploading' then return reconcileUnknown() end
if preparation.state == 'prepared' then
  if redis.call('PTTL', KEYS[2]) < 1 then
    -- A cursor/outbox may have committed immediately before a worker crash.
    -- Its short-lived preparation reference can later expire, but that is not
    -- an unknown upload and must not taint an already-durable delivery.
    local cursorRaw = redis.call('GET', KEYS[8])
    local cursorOk, cursor = pcall(cjson.decode, cursorRaw or '')
    local published = cursorOk and type(cursor) == 'table'
      and cursor.jobId == ARGV[6] and type(cursor.outputs) == 'table'
      and type(cursor.outputs[tonumber(ARGV[9])]) == 'table'
      and cursor.outputs[tonumber(ARGV[9])].outputIndex == tonumber(ARGV[9])
    if not published then return reconcileUnknown() end
  end
  return response('prepared', preparation)
end
if preparation.state == 'upload_unknown' or preparation.state == 'failed' or preparation.state == 'canceled' then
  return response(preparation.state, preparation)
end
if preparation.state ~= 'reserved' then return cjson.encode({ kind = 'unavailable' }) end

local jobRaw = redis.call('GET', KEYS[5])
local jobOk, job = pcall(cjson.decode, jobRaw or '')
if not jobOk or type(job) ~= 'table' or job.jobId ~= ARGV[6] then
  preparation.state = 'canceled'
  preparation.stateVersion = preparation.stateVersion + 1
  preparation.updatedAtMs = tonumber(ARGV[2])
  redis.call('SET', KEYS[1], cjson.encode(preparation), 'EX', tonumber(ARGV[5]))
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  return response('canceled', preparation)
end
if job.state ~= 'executing' or tonumber(job.stateVersion) ~= tonumber(ARGV[7]) then
  return cjson.encode({ kind = 'unavailable' })
end
if redis.call('PTTL', KEYS[6]) <= tonumber(ARGV[8]) then
  return response('payload_expiring', preparation)
end
if not activeRelease() then return response('release_blocked', preparation) end

preparation.state = 'uploading'
preparation.stateVersion = preparation.stateVersion + 1
preparation.updatedAtMs = tonumber(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode(preparation))
return response('ready', preparation)
`;

const FINISH_JOB_RESULT_PREPARATION_LUA = `
-- AIR_STUDIO_FINISH_JOB_RESULT_PREPARATION_V1
-- KEYS: opaque preparation metadata, encrypted prepared Photon GUID, upload
--       lease, job metadata, encrypted job payload, durable job-ready index
-- ARGV: lease token, outcome, encrypted prepared reference, now, terminal ttl,
--       job id, output index
local function positiveInteger(value)
  return type(value) == 'number' and value >= 1 and value == math.floor(value)
end

local function nonnegativeInteger(value)
  return type(value) == 'number' and value >= 0 and value == math.floor(value)
end

local function opaque(value)
  return type(value) == 'string' and #value == 64 and string.match(value, '^[a-f0-9]+$') ~= nil
end

local function validPreparation(record)
  return type(record) == 'table'
    and record.schemaVersion == 1
    and opaque(record.preparationId)
    and type(record.jobId) == 'string' and #record.jobId > 0 and #record.jobId <= 160
    and positiveInteger(record.outputIndex) and record.outputIndex <= 4
    and (record.state == 'reserved' or record.state == 'uploading' or record.state == 'prepared'
      or record.state == 'upload_unknown' or record.state == 'failed' or record.state == 'canceled')
    and positiveInteger(record.stateVersion)
    and nonnegativeInteger(record.createdAtMs)
    and nonnegativeInteger(record.updatedAtMs)
end

local function encrypted(value)
  return type(value) == 'table'
    and value.version == 1
    and value.algorithm == 'aes-256-gcm'
    and type(value.iv) == 'string'
    and type(value.ciphertext) == 'string'
    and type(value.tag) == 'string'
end

local function response(kind, preparation)
  return cjson.encode({ kind = kind, preparation = preparation })
end

local raw = redis.call('GET', KEYS[1])
if not raw then return cjson.encode({ kind = 'unavailable' }) end
local ok, preparation = pcall(cjson.decode, raw)
if not ok or not validPreparation(preparation)
  or preparation.jobId ~= ARGV[6]
  or preparation.outputIndex ~= tonumber(ARGV[7]) then return cjson.encode({ kind = 'unavailable' }) end
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return cjson.encode({ kind = 'lease_lost' }) end
if preparation.state ~= 'uploading' then return cjson.encode({ kind = 'unavailable' }) end

local function reconcileUnknown()
  preparation.state = 'upload_unknown'
  preparation.stateVersion = preparation.stateVersion + 1
  preparation.updatedAtMs = tonumber(ARGV[4])
  redis.call('SET', KEYS[1], cjson.encode(preparation))
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  local jobRaw = redis.call('GET', KEYS[4])
  if jobRaw then
    local jobOk, job = pcall(cjson.decode, jobRaw)
    if jobOk and type(job) == 'table'
      and job.jobId == ARGV[6]
      and (job.state == 'executing' or job.state == 'ready' or job.state == 'delivering') then
      job.state = 'reconciliation_required'
      job.stateVersion = tonumber(job.stateVersion or 0) + 1
      job.updatedAtMs = tonumber(ARGV[4])
      redis.call('SET', KEYS[4], cjson.encode(job))
      redis.call('ZREM', KEYS[6], ARGV[6])
    end
  end
  return response('upload_unknown', preparation)
end

if ARGV[2] == 'prepared' then
  local refOk, reference = pcall(cjson.decode, ARGV[3])
  local payloadTtl = redis.call('PTTL', KEYS[5])
  if not refOk or not encrypted(reference) or payloadTtl < 1 then return reconcileUnknown() end
  preparation.state = 'prepared'
  preparation.stateVersion = preparation.stateVersion + 1
  preparation.updatedAtMs = tonumber(ARGV[4])
  redis.call('SET', KEYS[1], cjson.encode(preparation))
  redis.call('SET', KEYS[2], ARGV[3], 'PX', payloadTtl)
  redis.call('DEL', KEYS[3])
  return response('prepared', preparation)
end

if ARGV[2] == 'retryable_failure' then
  preparation.state = 'reserved'
  preparation.stateVersion = preparation.stateVersion + 1
  preparation.updatedAtMs = tonumber(ARGV[4])
  redis.call('SET', KEYS[1], cjson.encode(preparation))
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  return response('retryable_failure', preparation)
end

if ARGV[2] == 'failed' then
  preparation.state = 'failed'
  preparation.stateVersion = preparation.stateVersion + 1
  preparation.updatedAtMs = tonumber(ARGV[4])
  redis.call('SET', KEYS[1], cjson.encode(preparation), 'EX', tonumber(ARGV[5]))
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  return response('failed', preparation)
end

if ARGV[2] == 'upload_unknown' then return reconcileUnknown() end
return cjson.encode({ kind = 'unavailable' })
`;

const ATTACH_PROVIDER_REQUEST_RECONCILIATION_LUA = `
-- AIR_STUDIO_ATTACH_PROVIDER_REQUEST_RECONCILIATION_V2
-- KEYS: subrun metadata, encrypted provider reference, execution lease, job metadata,
--       encrypted job payload, append-only audit record, durable job-ready index
-- ARGV: expected subrun version, expected job version, expected subrun id,
--       expected job id, encrypted provider reference, audit record, now
local subrunRaw = redis.call('GET', KEYS[1])
local jobRaw = redis.call('GET', KEYS[4])
if not subrunRaw or not jobRaw then return -3 end
local subrunOk, subrun = pcall(cjson.decode, subrunRaw)
local jobOk, job = pcall(cjson.decode, jobRaw)
if not subrunOk or type(subrun) ~= 'table' or not jobOk or type(job) ~= 'table' then return -3 end
if subrun.state ~= 'submission_unknown'
  or tonumber(subrun.stateVersion) ~= tonumber(ARGV[1])
  or subrun.subrunId ~= ARGV[3]
  or subrun.jobId ~= ARGV[4]
  or job.jobId ~= ARGV[4]
  or job.state ~= 'reconciliation_required'
  or tonumber(job.stateVersion) ~= tonumber(ARGV[2]) then return -1 end
local payloadTtl = redis.call('PTTL', KEYS[5])
if not payloadTtl or payloadTtl < 1 then return -2 end
if not redis.call('SET', KEYS[6], ARGV[6], 'NX') then return -3 end
subrun.state = 'submitted'
subrun.stateVersion = tonumber(subrun.stateVersion) + 1
subrun.updatedAtMs = tonumber(ARGV[7])
job.state = 'executing'
job.stateVersion = tonumber(job.stateVersion) + 1
job.updatedAtMs = tonumber(ARGV[7])
redis.call('SET', KEYS[1], cjson.encode(subrun))
redis.call('SET', KEYS[2], ARGV[5], 'PX', payloadTtl)
redis.call('DEL', KEYS[3])
redis.call('SET', KEYS[4], cjson.encode(job))
redis.call('ZADD', KEYS[7], tonumber(ARGV[7]), ARGV[4])
return 1
`;

const TERMINALIZE_PROVIDER_RECONCILIATION_LUA = `
-- AIR_STUDIO_TERMINALIZE_PROVIDER_RECONCILIATION_V2
-- KEYS: subrun metadata, encrypted provider reference, execution lease, job metadata,
--       encrypted job payload, active-job marker, append-only audit record,
--       durable job-ready index
-- ARGV: expected subrun version, expected job version, expected subrun id, expected job id,
--       audit record, now, terminal metadata ttl
local subrunRaw = redis.call('GET', KEYS[1])
local jobRaw = redis.call('GET', KEYS[4])
if not subrunRaw or not jobRaw then return -3 end
local subrunOk, subrun = pcall(cjson.decode, subrunRaw)
local jobOk, job = pcall(cjson.decode, jobRaw)
if not subrunOk or type(subrun) ~= 'table' or not jobOk or type(job) ~= 'table' then return -3 end
if subrun.state ~= 'submission_unknown'
  or tonumber(subrun.stateVersion) ~= tonumber(ARGV[1])
  or subrun.subrunId ~= ARGV[3]
  or subrun.jobId ~= ARGV[4]
  or job.jobId ~= ARGV[4]
  or job.state ~= 'reconciliation_required'
  or tonumber(job.stateVersion) ~= tonumber(ARGV[2]) then return -1 end
if not redis.call('SET', KEYS[7], ARGV[5], 'NX') then return -3 end
subrun.state = 'reconciliation_terminalized'
subrun.stateVersion = tonumber(subrun.stateVersion) + 1
subrun.updatedAtMs = tonumber(ARGV[6])
job.state = 'failed'
job.stateVersion = tonumber(job.stateVersion) + 1
job.updatedAtMs = tonumber(ARGV[6])
job.encryptedPayload = nil
redis.call('SET', KEYS[1], cjson.encode(subrun), 'EX', ARGV[7])
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[3])
redis.call('SET', KEYS[4], cjson.encode(job), 'EX', ARGV[7])
redis.call('DEL', KEYS[5])
if redis.call('GET', KEYS[6]) == ARGV[4] then redis.call('DEL', KEYS[6]) end
redis.call('ZREM', KEYS[8], ARGV[4])
return 1
`;

const RESOLVE_OUTBOX_SEND_RECONCILIATION_LUA = `
-- AIR_STUDIO_RESOLVE_OUTBOX_SEND_RECONCILIATION_V1
-- KEYS: outbox metadata, encrypted outbox payload, ready index, lane index, audit record
-- ARGV: expected outbox version, sent|confirmed_absent, audit record, now,
--       terminal metadata ttl, outbox id
local raw = redis.call('GET', KEYS[1])
if not raw then return -3 end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then return -3 end
if record.outboxId ~= ARGV[6]
  or record.state ~= 'send_unknown'
  or tonumber(record.stateVersion) ~= tonumber(ARGV[1]) then return -1 end
if ARGV[2] ~= 'sent' and ARGV[2] ~= 'confirmed_absent' then return -3 end
if not redis.call('SET', KEYS[5], ARGV[3], 'NX') then return -3 end
if ARGV[2] == 'sent' then
  record.state = 'sent'
  record.stateVersion = tonumber(record.stateVersion) + 1
  record.updatedAtMs = tonumber(ARGV[4])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[5])
  redis.call('DEL', KEYS[2])
  redis.call('ZREM', KEYS[3], ARGV[6])
  redis.call('ZREM', KEYS[4], ARGV[6])
else
  local payloadTtl = redis.call('PTTL', KEYS[2])
  if not payloadTtl or payloadTtl < 1 then
    -- A known absence cannot be safely requeued once the Air-owned private
    -- reference has elapsed. Terminalize rather than refetching/regenerating.
    record.state = 'failed'
    record.terminalReason = 'artifact_expired'
    record.stateVersion = tonumber(record.stateVersion) + 1
    record.updatedAtMs = tonumber(ARGV[4])
    redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[5])
    redis.call('DEL', KEYS[2])
    redis.call('ZREM', KEYS[3], ARGV[6])
    redis.call('ZREM', KEYS[4], ARGV[6])
  else
    -- Outstanding reconciliation remains durable; the encrypted payload
    -- itself retains its original privacy deadline independently.
    record.state = 'confirmed_absent'
    record.stateVersion = tonumber(record.stateVersion) + 1
    record.updatedAtMs = tonumber(ARGV[4])
    redis.call('SET', KEYS[1], cjson.encode(record))
    redis.call('ZREM', KEYS[3], ARGV[6])
  end
end
return 1
`;

const REQUEUE_CONFIRMED_ABSENT_OUTBOX_LUA = `
-- AIR_STUDIO_REQUEUE_CONFIRMED_ABSENT_OUTBOX_V1
-- KEYS: source metadata, source payload, replacement metadata, replacement payload,
--       ready index, replacement lane index, shared release fence, audit record,
--       optional job-result cursor
-- ARGV: expected source version, expected source outbox id, replacement metadata,
--       replacement encrypted payload, replacement due time, replacement id,
--       replacement sequence, release generation, release phase, audit record,
--       now, terminal metadata ttl
local sourceRaw = redis.call('GET', KEYS[1])
if not sourceRaw then return -4 end
local sourceOk, source = pcall(cjson.decode, sourceRaw)
if not sourceOk or type(source) ~= 'table' then return -4 end
if source.outboxId ~= ARGV[2]
  or source.state ~= 'confirmed_absent'
  or tonumber(source.stateVersion) ~= tonumber(ARGV[1]) then return -1 end
if redis.call('EXISTS', KEYS[3]) == 1 then return -1 end
local replacementOk, replacement = pcall(cjson.decode, ARGV[3])
if not replacementOk or type(replacement) ~= 'table'
  or replacement.outboxId ~= ARGV[6]
  or replacement.state ~= 'intent'
  or tonumber(replacement.stateVersion) ~= 1 then return -4 end
local resultCursor = nil
local resultCursorTtl = nil
if source.resultOutputIndex ~= nil then
  if source.lane ~= 'job-result'
    or type(source.jobId) ~= 'string'
    or type(source.resultOutputIndex) ~= 'number'
    or source.resultOutputIndex < 1
    or source.resultOutputIndex ~= math.floor(source.resultOutputIndex)
    or replacement.jobId ~= source.jobId
    or replacement.lane ~= 'job-result'
    or replacement.resultOutputIndex ~= source.resultOutputIndex
    or replacement.sequence ~= source.resultOutputIndex then return -4 end
  local cursorRaw = redis.call('GET', KEYS[9])
  local cursorOk
  cursorOk, resultCursor = pcall(cjson.decode, cursorRaw or '')
  if not cursorOk or type(resultCursor) ~= 'table'
    or resultCursor.schemaVersion ~= 1
    or resultCursor.jobId ~= source.jobId
    or type(resultCursor.outputs) ~= 'table' then return -4 end
  local cursorOutput = resultCursor.outputs[source.resultOutputIndex]
  if type(cursorOutput) ~= 'table'
    or cursorOutput.outputIndex ~= source.resultOutputIndex
    or cursorOutput.outboxId ~= source.outboxId then return -1 end
  resultCursorTtl = redis.call('PTTL', KEYS[9])
  if resultCursorTtl == -2 then return -4 end
elseif replacement.resultOutputIndex ~= nil then
  return -4
end
local payloadTtl = redis.call('PTTL', KEYS[2])
if not payloadTtl or payloadTtl < 1 then return -2 end
local stamp = replacement.releaseStamp
if type(stamp) ~= 'table'
  or type(stamp.generation) ~= 'number' or stamp.generation < 0
  or stamp.generation ~= math.floor(stamp.generation)
  or stamp.generation ~= tonumber(ARGV[8]) or stamp.phase ~= ARGV[9] then return -3 end
local fenceRaw = redis.call('GET', KEYS[7])
if fenceRaw then
  local fenceOk, fence = pcall(cjson.decode, fenceRaw)
  if not fenceOk or type(fence) ~= 'table'
    or type(fence.generation) ~= 'number' or fence.generation < 0
    or fence.generation ~= math.floor(fence.generation)
    or (fence.phase ~= 'dark' and fence.phase ~= 'canary' and fence.phase ~= 'production') then return -3 end
  if fence.generation ~= tonumber(ARGV[8]) or fence.phase ~= ARGV[9] then return -3 end
elseif ARGV[9] ~= 'dark' then
  return -3
end
local laneHead = redis.call('ZRANGE', KEYS[6], 0, 0)
if #laneHead ~= 1 or laneHead[1] ~= ARGV[2] then return -1 end
local laneSequence = redis.call('ZRANGEBYSCORE', KEYS[6], ARGV[7], ARGV[7])
if #laneSequence ~= 1 or laneSequence[1] ~= ARGV[2] then return -1 end
if not redis.call('SET', KEYS[8], ARGV[10], 'NX') then return -5 end
source.state = 'requeued'
source.stateVersion = tonumber(source.stateVersion) + 1
source.updatedAtMs = tonumber(ARGV[11])
source.terminalReason = nil
redis.call('SET', KEYS[1], cjson.encode(source), 'EX', ARGV[12])
redis.call('DEL', KEYS[2])
redis.call('SET', KEYS[3], ARGV[3])
redis.call('SET', KEYS[4], ARGV[4], 'PX', payloadTtl)
redis.call('ZADD', KEYS[5], tonumber(ARGV[5]), ARGV[6])
redis.call('ZREM', KEYS[6], ARGV[2])
redis.call('ZADD', KEYS[6], tonumber(ARGV[7]), ARGV[6])
if resultCursor ~= nil then
  resultCursor.outputs[source.resultOutputIndex].outboxId = ARGV[6]
  resultCursor.updatedAtMs = tonumber(ARGV[11])
  if resultCursorTtl and resultCursorTtl > 0 then
    redis.call('SET', KEYS[9], cjson.encode(resultCursor), 'PX', resultCursorTtl)
  else
    redis.call('SET', KEYS[9], cjson.encode(resultCursor))
  end
end
return 1
`;

const TERMINALIZE_EXPIRED_OUTBOX_RECONCILIATION_LUA = `
-- AIR_STUDIO_TERMINALIZE_EXPIRED_OUTBOX_RECONCILIATION_V1
-- KEYS: outbox metadata, encrypted outbox payload, ready index, lane index, audit record
-- ARGV: expected outbox version, expected outbox id, audit record, now, terminal metadata ttl
local raw = redis.call('GET', KEYS[1])
if not raw then return -3 end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then return -3 end
if record.outboxId ~= ARGV[2]
  or record.state ~= 'confirmed_absent'
  or tonumber(record.stateVersion) ~= tonumber(ARGV[1]) then return -1 end
local payloadTtl = redis.call('PTTL', KEYS[2])
if payloadTtl and payloadTtl >= 1 then return -2 end
if not redis.call('SET', KEYS[5], ARGV[3], 'NX') then return -3 end
record.state = 'failed'
record.terminalReason = 'artifact_expired'
record.stateVersion = tonumber(record.stateVersion) + 1
record.updatedAtMs = tonumber(ARGV[4])
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[5])
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('ZREM', KEYS[4], ARGV[2])
return 1
`;

function parseStored<T>(raw: string | null): T | null {
  if (raw === null) return null;
  try {
    const value: unknown = JSON.parse(raw);
    return value !== null && typeof value === "object" ? value as T : null;
  } catch {
    return null;
  }
}

function reconciliationResult(
  raw: unknown,
  auditId: string,
  replacementOutboxId?: string,
  options: { readonly payloadExpired?: boolean; readonly releaseBlocked?: boolean } = {},
): StudioReconciliationResult {
  if (raw === 1 || raw === "1") {
    return Object.freeze({
      kind: "applied",
      auditId,
      ...(replacementOutboxId === undefined ? {} : { replacementOutboxId }),
    });
  }
  if (raw === -1 || raw === "-1") return Object.freeze({ kind: "conflict" });
  if (options.payloadExpired && (raw === -2 || raw === "-2")) {
    return Object.freeze({ kind: "payload_expired" });
  }
  if (options.releaseBlocked && (raw === -3 || raw === "-3")) {
    return Object.freeze({ kind: "release_blocked" });
  }
  return Object.freeze({ kind: "unavailable" });
}

function parseProviderSubmissionBeginResult(raw: string | null): StudioProviderSubmissionBeginResult {
  const parsed = parseStored<{ readonly kind?: unknown; readonly subrun?: unknown }>(raw);
  if (!parsed || typeof parsed.kind !== "string") return Object.freeze({ kind: "unavailable" });
  if (parsed.kind === "lease_lost" || parsed.kind === "unavailable") return Object.freeze({ kind: parsed.kind });
  if (!isProviderSubrunRecord(parsed.subrun)) return Object.freeze({ kind: "unavailable" });
  const subrun = Object.freeze(parsed.subrun);
  switch (parsed.kind) {
    case "ready":
    case "already_submitted":
    case "definitive_rejection":
    case "submission_unknown":
    case "release_blocked":
    case "canceled":
      return Object.freeze({ kind: parsed.kind, subrun });
    default:
      return Object.freeze({ kind: "unavailable" });
  }
}

function parseProviderObservationBeginResult(raw: string | null): StudioProviderObservationBeginResult {
  const parsed = parseStored<{ readonly kind?: unknown; readonly observation?: unknown }>(raw);
  if (!parsed || typeof parsed.kind !== "string") return Object.freeze({ kind: "unavailable" });
  if (parsed.kind === "lease_lost" || parsed.kind === "unavailable") return Object.freeze({ kind: parsed.kind });
  if (!isProviderObservationRecord(parsed.observation)) return Object.freeze({ kind: "unavailable" });
  const observation = Object.freeze(parsed.observation);
  switch (parsed.kind) {
    case "ready":
    case "not_due":
    case "succeeded":
    case "failed":
    case "deadline_exceeded":
    case "attempts_exhausted":
    case "reference_expired":
    case "canceled":
      return Object.freeze({ kind: parsed.kind, observation });
    default:
      return Object.freeze({ kind: "unavailable" });
  }
}

function parseProviderObservationFinishResult(raw: string | null): StudioProviderObservationFinishResult {
  const parsed = parseStored<{ readonly kind?: unknown; readonly observation?: unknown }>(raw);
  if (!parsed || typeof parsed.kind !== "string") return Object.freeze({ kind: "unavailable" });
  if (parsed.kind === "lease_lost" || parsed.kind === "unavailable") return Object.freeze({ kind: parsed.kind });
  if (!isProviderObservationRecord(parsed.observation)) return Object.freeze({ kind: "unavailable" });
  const observation = Object.freeze(parsed.observation);
  switch (parsed.kind) {
    case "pending":
    case "succeeded":
    case "failed":
    case "deadline_exceeded":
    case "attempts_exhausted":
    case "canceled":
      return Object.freeze({ kind: parsed.kind, observation });
    default:
      return Object.freeze({ kind: "unavailable" });
  }
}

function parseJobResultPreparationRecoveryResult(
  raw: string | null,
): StudioJobResultPreparationRecoveryResult {
  const parsed = parseStored<{ readonly kind?: unknown; readonly preparation?: unknown }>(raw);
  if (!parsed || typeof parsed.kind !== "string") return Object.freeze({ kind: "unavailable" });
  if (parsed.kind === "lease_lost" || parsed.kind === "unavailable") return Object.freeze({ kind: parsed.kind });
  if (!isJobResultPreparationRecord(parsed.preparation)) return Object.freeze({ kind: "unavailable" });
  const preparation = Object.freeze(parsed.preparation);
  switch (parsed.kind) {
    case "reserved":
    case "prepared":
    case "upload_unknown":
    case "failed":
    case "canceled":
      return Object.freeze({ kind: parsed.kind, preparation });
    default:
      return Object.freeze({ kind: "unavailable" });
  }
}

function parsePreparedProviderResultFinalizeResult(
  raw: string | null,
): Extract<StudioPreparedProviderResultFinalizeResult, { readonly kind: string }> {
  const parsed = parseStored<{ readonly kind?: unknown }>(raw);
  if (!parsed || typeof parsed.kind !== "string") return Object.freeze({ kind: "unavailable" });
  switch (parsed.kind) {
    case "finalized":
    case "existing":
    case "lease_lost":
    case "release_blocked":
    case "payload_expiring":
    case "unavailable":
      // Successful outcomes are hydrated immediately by the caller from the
      // authoritative records, never from Lua-returned payload material.
      return Object.freeze({ kind: parsed.kind }) as Extract<StudioPreparedProviderResultFinalizeResult, { readonly kind: string }>;
    default:
      return Object.freeze({ kind: "unavailable" });
  }
}

function parseJobResultPreparationBeginResult(raw: string | null): StudioJobResultPreparationBeginResult {
  const parsed = parseStored<{ readonly kind?: unknown; readonly preparation?: unknown }>(raw);
  if (!parsed || typeof parsed.kind !== "string") return Object.freeze({ kind: "unavailable" });
  if (parsed.kind === "lease_lost" || parsed.kind === "unavailable") return Object.freeze({ kind: parsed.kind });
  if (!isJobResultPreparationRecord(parsed.preparation)) return Object.freeze({ kind: "unavailable" });
  const preparation = Object.freeze(parsed.preparation);
  switch (parsed.kind) {
    case "ready":
    case "prepared":
    case "upload_unknown":
    case "failed":
    case "canceled":
    case "release_blocked":
    case "payload_expiring":
      return Object.freeze({ kind: parsed.kind, preparation });
    default:
      return Object.freeze({ kind: "unavailable" });
  }
}

function parseJobResultPreparationFinishResult(raw: string | null): StudioJobResultPreparationFinishResult {
  const parsed = parseStored<{ readonly kind?: unknown; readonly preparation?: unknown }>(raw);
  if (!parsed || typeof parsed.kind !== "string") return Object.freeze({ kind: "unavailable" });
  if (parsed.kind === "lease_lost" || parsed.kind === "unavailable") return Object.freeze({ kind: parsed.kind });
  if (!isJobResultPreparationRecord(parsed.preparation)) return Object.freeze({ kind: "unavailable" });
  const preparation = Object.freeze(parsed.preparation);
  switch (parsed.kind) {
    case "prepared":
    case "retryable_failure":
    case "failed":
    case "upload_unknown":
      return Object.freeze({ kind: parsed.kind, preparation });
    default:
      return Object.freeze({ kind: "unavailable" });
  }
}

function isExecutionSnapshotForJob(snapshot: StudioExecutionSnapshot, job: StudioJobRecord): boolean {
  return snapshot.schemaVersion === 2
    && snapshot.workflow.workflowVersion === 1
    && snapshot.workflow.stepId === "provider-generation"
    && isSafeRecordString(snapshot.workflow.workflowId, 160)
    && snapshot.model.slug === job.modelSlug
    && snapshot.model.provider === job.provider
    && isSafeRecordString(snapshot.model.providerModelId, 512)
    && isSafeProviderAdapterKey(snapshot.model.adapterKey, job.provider)
    && isCanonicalSha256Fingerprint(snapshot.model.catalogFingerprint)
    && isCanonicalSha256Fingerprint(snapshot.model.requestSchemaFingerprint)
    && isCanonicalSha256Fingerprint(snapshot.model.resultSchemaFingerprint)
    && isCanonicalSha256Fingerprint(snapshot.model.pricingEvidenceFingerprint)
    && isSnapshotOutputCompatibleWithJob(snapshot.output, job)
    && snapshot.maximumQuotedCostCents === job.reservedCents
    && isSnapshotPollingPolicy(snapshot.polling)
    && isValidReleaseStamp(snapshot.releaseStamp)
    && isValidReleaseStamp(job.releaseStamp)
    && sameReleaseStamp(snapshot.releaseStamp, job.releaseStamp);
}

const CANONICAL_SHA256_FINGERPRINT_PATTERN = /^sha256:[a-f0-9]{64}$/;
const SNAPSHOT_PROVIDER_ADAPTER_KEY_PATTERN = /^(?:gmi|fal)\.[a-z0-9]+(?:[.-][a-z0-9]+)*\.v[1-9][0-9]*$/;

function isCanonicalSha256Fingerprint(value: unknown): value is string {
  return typeof value === "string" && CANONICAL_SHA256_FINGERPRINT_PATTERN.test(value);
}

function isSafeProviderAdapterKey(value: unknown, provider: StudioJobRecord["provider"]): value is string {
  return typeof value === "string"
    && SNAPSHOT_PROVIDER_ADAPTER_KEY_PATTERN.test(value)
    && value.startsWith(`${provider}.`);
}

function isSnapshotOutputCompatibleWithJob(
  output: StudioExecutionSnapshot["output"],
  job: StudioJobRecord,
): boolean {
  if (!isPositiveSafeInteger(output.count) || output.count > 4 || output.count !== job.outputCount) return false;
  const expectedType = job.mode === "imagine" || job.mode === "edit-image" ? "image" : "video";
  if (output.type !== expectedType) return false;
  if (expectedType === "image") return output.durationSeconds === undefined && job.durationSeconds === undefined;
  return isPositiveSafeInteger(output.durationSeconds ?? 0)
    && output.durationSeconds === job.durationSeconds;
}

function isSnapshotPollingPolicy(value: StudioExecutionSnapshot["polling"]): boolean {
  return isPositiveSafeInteger(value.requestTimeoutMs)
    && value.requestTimeoutMs >= 1_000
    && value.requestTimeoutMs <= 900_000
    && isPositiveSafeInteger(value.pollIntervalMs)
    && value.pollIntervalMs >= 250
    && value.pollIntervalMs <= 300_000
    && isPositiveSafeInteger(value.maxPollAttempts)
    && value.maxPollAttempts <= 10_000
    && isPositiveSafeInteger(value.deadlineMs)
    && value.deadlineMs <= 86_400_000
    && value.deadlineMs >= value.requestTimeoutMs;
}

type JobResultPublicationOutcome =
  | { readonly kind: "created" | "existing" }
  | { readonly kind: "out_of_order"; readonly expectedOutputIndex: number }
  | { readonly kind: "release_blocked" | "unavailable" };

function parseJobResultPublication(raw: unknown): JobResultPublicationOutcome {
  const value = parseStored<{ readonly kind?: unknown; readonly expectedOutputIndex?: unknown }>(
    typeof raw === "string" ? raw : null,
  );
  if (!value || typeof value.kind !== "string") return Object.freeze({ kind: "unavailable" });
  if (value.kind === "created" || value.kind === "existing" || value.kind === "release_blocked") {
    return Object.freeze({ kind: value.kind });
  }
  if (value.kind === "out_of_order" && typeof value.expectedOutputIndex === "number" && isPositiveSafeInteger(value.expectedOutputIndex)) {
    return Object.freeze({ kind: "out_of_order", expectedOutputIndex: value.expectedOutputIndex });
  }
  return Object.freeze({ kind: "unavailable" });
}

function isJobResultCursorMetadata(value: unknown): value is StudioJobResultCursorMetadata {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const candidate = value as Partial<StudioJobResultCursorMetadata>;
  if (
    candidate.schemaVersion !== 1
    || !isSafeRecordString(candidate.jobId, 160)
    || !isOpaqueStudioIdentifier(candidate.senderHash)
    || !isOpaqueStudioIdentifier(candidate.threadHash)
    || typeof candidate.outputCount !== "number"
    || !isPositiveSafeInteger(candidate.outputCount)
    || candidate.outputCount > 4
    || !isValidReleaseStamp(candidate.releaseStamp)
    || typeof candidate.nextOutputIndex !== "number"
    || !isPositiveSafeInteger(candidate.nextOutputIndex)
    || candidate.nextOutputIndex > candidate.outputCount + 1
    || !Array.isArray(candidate.outputs)
    || candidate.outputs.length !== candidate.nextOutputIndex - 1
    || typeof candidate.createdAtMs !== "number"
    || !isNonnegativeSafeInteger(candidate.createdAtMs)
    || typeof candidate.updatedAtMs !== "number"
    || !isNonnegativeSafeInteger(candidate.updatedAtMs)
  ) return false;
  return candidate.outputs.every((output, offset) => {
    if (!output || typeof output !== "object" || Array.isArray(output)) return false;
    const result = output as Partial<StudioJobResultCursorOutput>;
    return result.outputIndex === offset + 1 && isOpaqueStudioIdentifier(result.outboxId);
  });
}

function isJobResultPayloadBoundToJob(
  payload: StudioOutboxPayload,
  outputIndex: number,
  jobPayload: StudioJobPayload,
): boolean {
  if (
    payload.kind !== "prepared-output"
    || payload.outputIndex !== outputIndex
    || !isStudioPreparedOutput(payload.output)
  ) return false;
  const thread = payload.thread;
  return thread.chatGuid === jobPayload.chatGuid
    && thread.sendingLine === jobPayload.sendingLine
    && thread.replyTargetId === jobPayload.replyTargetId;
}

function isPreparedProviderResultPreamble(
  record: StudioOutboxRecord | null,
  job: StudioJobRecord,
  preambleId: string,
): record is StudioOutboxRecord {
  return isPreparedProviderResultOutbox(record, job, preambleId, 0)
    && record.resultOutputIndex === undefined;
}

function isPreparedProviderResultAttachment(
  record: StudioOutboxRecord | null,
  job: StudioJobRecord,
  attachmentId: string,
): record is StudioOutboxRecord {
  return isPreparedProviderResultOutbox(record, job, attachmentId, 1)
    && record.resultOutputIndex === 1;
}

/**
 * Metadata-only validation for an already-finalized result bundle. The
 * encrypted payload is intentionally optional here: a durable terminal audit
 * must stay readable after its privacy TTL has elapsed.
 */
function isPreparedProviderResultOutbox(
  record: StudioOutboxRecord | null,
  job: StudioJobRecord,
  outboxId: string,
  sequence: number,
): record is StudioOutboxRecord {
  return record !== null
    && record.schemaVersion === 3
    && record.outboxId === outboxId
    && record.jobId === job.jobId
    && record.senderHash === job.senderHash
    && record.lane === "job-result"
    && record.sequence === sequence
    && STUDIO_OUTBOX_STATES.includes(record.state)
    && isNonnegativeSafeInteger(record.attempts)
    && isPositiveSafeInteger(record.stateVersion)
    && isValidReleaseStamp(record.releaseStamp)
    && record.laneIdentity === undefined
    && record.pendingGuard === undefined
    && isNonnegativeSafeInteger(record.createdAtMs)
    && isNonnegativeSafeInteger(record.dueAtMs)
    && isNonnegativeSafeInteger(record.updatedAtMs);
}

/**
 * Reject unknown payload fields at every encrypted-outbox write. This keeps a
 * prepared Photon output from becoming a generic bag that could retain a
 * provider URL, filename, byte buffer, or inbound attachment locator.
 */
function isStudioOutboxPayload(value: unknown): value is StudioOutboxPayload {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const payload = value as Record<string, unknown>;
  if (!isStudioOutboxThread(payload.thread)) return false;
  switch (payload.kind) {
    case "text":
      return hasExactObjectKeys(payload, ["kind", "thread", "text"])
        && typeof payload.text === "string"
        && payload.text.length > 0
        && payload.text.length <= 4_000
        && !/[\u0000\r]/u.test(payload.text);
    case "tapback":
      return hasExactObjectKeys(payload, ["kind", "thread", "targetMessageId"])
        && isSafeRecordString(payload.targetMessageId, 512);
    case "prepared-output":
      return hasExactObjectKeys(payload, ["kind", "thread", "outputIndex", "output"])
        && typeof payload.outputIndex === "number"
        && isPositiveSafeInteger(payload.outputIndex)
        && payload.outputIndex <= 4
        && isStudioPreparedOutput(payload.output);
    default:
      return false;
  }
}

function isStudioOutboxThread(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const thread = value as Record<string, unknown>;
  return hasExactObjectKeys(thread, ["chatGuid", "sendingLine"], ["replyTargetId"])
    && isSafeRecordString(thread.chatGuid, 512)
    && isSafeRecordString(thread.sendingLine, 160)
    && (thread.replyTargetId === undefined || isSafeRecordString(thread.replyTargetId, 512));
}

function hasExactObjectKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[] = [],
): boolean {
  const keys = Object.keys(value);
  return required.every((key) => Object.hasOwn(value, key))
    && keys.every((key) => required.includes(key) || optional.includes(key));
}

function isStudioPreparedOutput(value: unknown): value is StudioPreparedOutput {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const output = value as Record<string, unknown>;
  const keys = Object.keys(output);
  return keys.length === 2
    && Object.hasOwn(output, "transport")
    && Object.hasOwn(output, "attachmentGuid")
    && output.transport === "photon"
    && isSafeRecordString(output.attachmentGuid, 4_096);
}

function isJobResultPreparationRecord(value: unknown): value is StudioJobResultPreparationRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Partial<StudioJobResultPreparationRecord>;
  return record.schemaVersion === 1
    && isOpaqueStudioIdentifier(record.preparationId)
    && isSafeRecordString(record.jobId, 160)
    && typeof record.outputIndex === "number"
    && isPositiveSafeInteger(record.outputIndex)
    && record.outputIndex <= 4
    && STUDIO_JOB_RESULT_PREPARATION_STATES.includes(record.state as StudioJobResultPreparationState)
    && typeof record.stateVersion === "number"
    && isPositiveSafeInteger(record.stateVersion)
    && typeof record.createdAtMs === "number"
    && isNonnegativeSafeInteger(record.createdAtMs)
    && typeof record.updatedAtMs === "number"
    && isNonnegativeSafeInteger(record.updatedAtMs);
}

function isProviderSubrunRecord(value: unknown): value is StudioProviderSubrunRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const candidate = value as Partial<StudioProviderSubrunRecord>;
  return candidate.schemaVersion === 1
    && isOpaqueStudioIdentifier(candidate.subrunId)
    // Existing Air job IDs are opaque in production, but accepting the
    // bounded legacy shape here keeps reconciliation readable across the
    // already-admitted v2 records created before this executor foundation.
    && isSafeRecordString(candidate.jobId, 160)
    && (candidate.provider === "gmi" || candidate.provider === "fal")
    && isSafeRecordString(candidate.modelSlug, 80)
    && isSafeRecordString(candidate.workflowId, 160)
    && candidate.stepId === "provider-generation"
    && isValidReleaseStamp(candidate.releaseStamp)
    && isOpaqueStudioIdentifier(candidate.submissionIntentId)
    && STUDIO_PROVIDER_SUBRUN_STATES.includes(candidate.state as StudioProviderSubrunState)
    && typeof candidate.stateVersion === "number" && isPositiveSafeInteger(candidate.stateVersion)
    && typeof candidate.createdAtMs === "number" && isNonnegativeSafeInteger(candidate.createdAtMs)
    && typeof candidate.updatedAtMs === "number" && isNonnegativeSafeInteger(candidate.updatedAtMs);
}

function isProviderObservationRecord(value: unknown): value is StudioProviderObservationRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Partial<StudioProviderObservationRecord>;
  const hasValidFailureDisposition = record.failureDisposition === undefined
    || STUDIO_PROVIDER_OBSERVATION_FAILURE_DISPOSITIONS.includes(
      record.failureDisposition as StudioProviderObservationFailureDisposition,
    );
  return record.schemaVersion === 1
    && isOpaqueStudioIdentifier(record.observationId)
    && isOpaqueStudioIdentifier(record.subrunId)
    && isSafeRecordString(record.jobId, 160)
    && typeof record.subrunStateVersion === "number" && isPositiveSafeInteger(record.subrunStateVersion)
    && typeof record.jobStateVersion === "number" && isPositiveSafeInteger(record.jobStateVersion)
    && STUDIO_PROVIDER_OBSERVATION_STATES.includes(record.state as StudioProviderObservationState)
    && typeof record.pollAttempts === "number"
    && isNonnegativeSafeInteger(record.pollAttempts)
    && typeof record.maxPollAttempts === "number"
    && isPositiveSafeInteger(record.maxPollAttempts)
    && record.maxPollAttempts <= 10_000
    && record.pollAttempts <= record.maxPollAttempts
    && typeof record.requestTimeoutMs === "number"
    && isPositiveSafeInteger(record.requestTimeoutMs)
    && record.requestTimeoutMs >= 1_000
    && record.requestTimeoutMs <= 900_000
    && typeof record.pollIntervalMs === "number"
    && isPositiveSafeInteger(record.pollIntervalMs)
    && record.pollIntervalMs >= 250
    && record.pollIntervalMs <= 300_000
    && typeof record.deadlineAtMs === "number"
    && isNonnegativeSafeInteger(record.deadlineAtMs)
    && typeof record.nextDueAtMs === "number"
    && isNonnegativeSafeInteger(record.nextDueAtMs)
    && record.nextDueAtMs <= record.deadlineAtMs
    && typeof record.stateVersion === "number"
    && isPositiveSafeInteger(record.stateVersion)
    && typeof record.createdAtMs === "number"
    && isNonnegativeSafeInteger(record.createdAtMs)
    && typeof record.updatedAtMs === "number"
    && isNonnegativeSafeInteger(record.updatedAtMs)
    && hasValidFailureDisposition
    && (record.state === "failed" || record.failureDisposition === undefined);
}

function isProviderObservationFinishOutcome(value: unknown): value is StudioProviderObservationFinishOutcome {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const outcome = value as {
    readonly kind?: unknown;
    readonly failureDisposition?: unknown;
  };
  if (outcome.kind === "failed") {
    return outcome.failureDisposition === undefined
      || STUDIO_PROVIDER_OBSERVATION_FAILURE_DISPOSITIONS.includes(
        outcome.failureDisposition as StudioProviderObservationFailureDisposition,
      );
  }
  return (outcome.kind === "pending" || outcome.kind === "succeeded")
    && outcome.failureDisposition === undefined;
}

function isReconciliationAuditRecord(value: unknown): value is StudioReconciliationAuditRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const candidate = value as Partial<StudioReconciliationAuditRecord>;
  return candidate.schemaVersion === 1
    && isOpaqueStudioIdentifier(candidate.auditId)
    && isOpaqueStudioIdentifier(candidate.actorHash)
    && STUDIO_RECONCILIATION_AUDIT_ACTIONS.includes(candidate.action as StudioReconciliationAuditAction)
    && isOpaqueStudioIdentifier(candidate.subjectId)
    && (candidate.relatedId === undefined || isOpaqueStudioIdentifier(candidate.relatedId))
    && (candidate.reason === undefined || isProviderTerminalizationReason(candidate.reason))
    && typeof candidate.occurredAtMs === "number"
    && isNonnegativeSafeInteger(candidate.occurredAtMs);
}

function sameProviderSubrunIdentity(
  left: StudioProviderSubrunRecord,
  right: StudioProviderSubrunRecord,
): boolean {
  return left.subrunId === right.subrunId
    && left.jobId === right.jobId
    && left.provider === right.provider
    && left.modelSlug === right.modelSlug
    && left.workflowId === right.workflowId
    && left.stepId === right.stepId
    && left.submissionIntentId === right.submissionIntentId
    && left.releaseStamp.generation === right.releaseStamp.generation
    && left.releaseStamp.phase === right.releaseStamp.phase;
}

function sameProviderObservationIdentity(
  left: StudioProviderObservationRecord,
  right: StudioProviderObservationRecord,
): boolean {
  return left.observationId === right.observationId
    && left.subrunId === right.subrunId
    && left.jobId === right.jobId
    && left.subrunStateVersion === right.subrunStateVersion
    && left.jobStateVersion === right.jobStateVersion
    && left.maxPollAttempts === right.maxPollAttempts
    && left.requestTimeoutMs === right.requestTimeoutMs
    && left.pollIntervalMs === right.pollIntervalMs
    && left.deadlineAtMs === right.deadlineAtMs;
}

function sameJobResultPreparationIdentity(
  left: StudioJobResultPreparationRecord,
  right: StudioJobResultPreparationRecord,
): boolean {
  return left.preparationId === right.preparationId
    && left.jobId === right.jobId
    && left.outputIndex === right.outputIndex;
}

function isOpaqueStudioIdentifier(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/u.test(value);
}

function isSafeProviderRequestId(value: string): boolean {
  return isSafeRecordString(value, 512);
}

function addSafeMilliseconds(start: number, duration: number): number | null {
  if (!isNonnegativeSafeInteger(start) || !isPositiveSafeInteger(duration)) return null;
  const result = start + duration;
  return isNonnegativeSafeInteger(result) ? result : null;
}

function isProviderTerminalizationReason(value: unknown): value is StudioProviderTerminalizationReason {
  return value === "provider_not_found" || value === "provider_unrecoverable" || value === "operator_canceled";
}

function isSafeRecordString(value: unknown, maximumLength: number): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= maximumLength
    && value.trim() === value
    && !/[\u0000\r\n]/u.test(value);
}

function isValidInitialJobNotification(
  notification: unknown,
  expectedKind: "text" | "tapback",
): boolean {
  if (!notification || typeof notification !== "object" || Array.isArray(notification)) return false;
  const candidate = notification as { outboxId?: unknown; payload?: unknown };
  if (!isSafeRecordString(candidate.outboxId, 160)) return false;
  const payload = candidate.payload;
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const message = payload as {
    kind?: unknown;
    thread?: unknown;
    text?: unknown;
    targetMessageId?: unknown;
  };
  if (message.kind !== expectedKind) return false;
  const thread = message.thread;
  if (!thread || typeof thread !== "object" || Array.isArray(thread)) return false;
  const destination = thread as { chatGuid?: unknown; sendingLine?: unknown; replyTargetId?: unknown };
  if (
    !isSafeRecordString(destination.chatGuid, 512)
    || !isSafeRecordString(destination.sendingLine, 160)
    || (destination.replyTargetId !== undefined && !isSafeRecordString(destination.replyTargetId, 512))
  ) return false;
  if (expectedKind === "text") {
    return typeof message.text === "string"
      && message.text.length > 0
      && message.text.length <= 4_000
      && !/[\u0000\r]/u.test(message.text);
  }
  return isSafeRecordString(message.targetMessageId, 512);
}

/**
 * The request payload is encrypted with the job, while an outbox payload is
 * encrypted independently. Verify their route fields before the atomic write
 * so an internal caller cannot cross-wire acknowledgements between chats.
 */
function isInitialNotificationBoundToJob(
  notification: unknown,
  expectedKind: "text" | "tapback",
  jobPayload: StudioJobPayload,
): boolean {
  if (!isValidInitialJobNotification(notification, expectedKind)) return false;
  const message = (notification as { readonly payload: StudioOutboxPayload }).payload;
  const thread = message.thread;
  if (
    thread.chatGuid !== jobPayload.chatGuid
    || thread.sendingLine !== jobPayload.sendingLine
    || thread.replyTargetId !== jobPayload.replyTargetId
  ) return false;
  return expectedKind === "text"
    || (message.kind === "tapback" && message.targetMessageId === jobPayload.replyTargetId);
}

/**
 * A cancellation replies to its own command bubble, so its reply target is
 * intentionally different from the creation message. The chat and concrete
 * sending line are still immutable job-routing facts.
 */
function isCancellationNotificationBoundToJob(
  notification: unknown,
  jobPayload: StudioJobPayload,
): boolean {
  if (!isValidInitialJobNotification(notification, "text")) return false;
  const message = (notification as { readonly payload: StudioOutboxPayload }).payload;
  return message.kind === "text"
    && message.thread.chatGuid === jobPayload.chatGuid
    && message.thread.sendingLine === jobPayload.sendingLine;
}

function isValidReleaseStamp(value: unknown): value is StudioReleaseStamp {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const stamp = value as Partial<StudioReleaseStamp>;
  return Number.isSafeInteger(stamp.generation)
    && (stamp.generation as number) >= 0
    && (stamp.phase === "dark" || stamp.phase === "canary" || stamp.phase === "production")
    && (stamp.phase === "dark" || (stamp.generation as number) >= 1);
}

function isReleaseFenceRecord(value: unknown): value is { readonly schemaVersion: 1 } & StudioReleaseStamp {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as { readonly schemaVersion?: unknown };
  return record.schemaVersion === 1 && isValidReleaseStamp(value);
}

function parseInboxFifoPhase(raw: string | null): StudioInboxFifoPhase | "invalid" {
  if (raw === null) return "legacy";
  const parsed = parseStored<unknown>(raw);
  if (!isInboxFifoMigrationRecord(parsed)) return "invalid";
  return parsed.phase;
}

function isInboxFifoMigrationRecord(value: unknown): value is InboxFifoMigrationRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const candidate = value as Partial<InboxFifoMigrationRecord>;
  return candidate.schemaVersion === 1 && (candidate.phase === "holding" || candidate.phase === "fifo");
}

function sameReleaseStamp(left: unknown, right: unknown): boolean {
  return isValidReleaseStamp(left)
    && isValidReleaseStamp(right)
    && left.generation === right.generation
    && left.phase === right.phase;
}

function outboxMetadata(record: StudioOutboxRecord): Omit<StudioOutboxRecord, "encryptedPayload"> {
  const { encryptedPayload: _encryptedPayload, ...metadata } = record;
  return metadata;
}

function jobMetadata(record: StudioJobRecordWithPayload): Omit<StudioJobRecord, "encryptedPayload"> {
  const { encryptedPayload: _encryptedPayload, ...metadata } = record;
  return metadata;
}

function requiredJobPayload(record: StudioJobRecordWithPayload): EncryptedPayload {
  return record.encryptedPayload;
}

function requiredOutboxPayload(record: StudioOutboxRecord): EncryptedPayload {
  if (!record.encryptedPayload) throw new Error("outbox payload is unavailable");
  return record.encryptedPayload;
}

function normalizeReleaseStamp(stamp: StudioReleaseStamp): StudioReleaseStamp {
  if (!Number.isSafeInteger(stamp.generation) || stamp.generation < 0) {
    throw new Error("release generation is invalid");
  }
  if (stamp.phase !== "dark" && stamp.phase !== "canary" && stamp.phase !== "production") {
    throw new Error("release phase is invalid");
  }
  if (stamp.phase !== "dark" && stamp.generation < 1) {
    throw new Error("active release generation is invalid");
  }
  return Object.freeze({ generation: stamp.generation, phase: stamp.phase });
}

function assertPendingSource(record: StudioInboxRecord): void {
  if (
    !record.receiptId
    || !record.senderHash
    || !record.threadHash
    || !isPositiveSafeInteger(record.scopeSequence)
    || !Number.isSafeInteger(record.receivedAtMs)
    || record.receivedAtMs < 0
  ) {
    throw new Error("pending source record is invalid");
  }
}

function isPendingControl(value: PendingControl): boolean {
  if (
    value.schemaVersion !== 1
    || !["active", "cleared", "maintenance", "expired"].includes(value.state)
    || !value.receiptId
    || !isPositiveSafeInteger(value.scopeSequence)
    || !isPositiveSafeInteger(value.revision)
  ) return false;
  if (value.state === "active") {
    return Boolean(value.token)
      && Number.isSafeInteger(value.expiresAtMs)
      && (value.expiresAtMs as number) > 0;
  }
  return value.token === undefined && (value.expiresAtMs === undefined || (Number.isSafeInteger(value.expiresAtMs) && value.expiresAtMs > 0));
}

function samePendingGuard(left: PendingWriteGuard | undefined, right: PendingWriteGuard): boolean {
  return left !== undefined
    && left.threadHash === right.threadHash
    && left.receiptId === right.receiptId
    && left.scopeSequence === right.scopeSequence
    && left.revision === right.revision
    && left.token === right.token
    && left.expiresAtMs === right.expiresAtMs;
}

function hasValidAdmissionBounds(input: AdmitStudioJobInput): boolean {
  const { job } = input;
  return (
    input.activeJobLimit === 1
    && isPositiveSafeInteger(input.dailyUserJobLimit)
    && isNonnegativeSafeInteger(input.perJobCapCents)
    && isNonnegativeSafeInteger(input.dailySpendCapCents)
    && job.schemaVersion === 3
    && job.state === "admitted"
    && job.stateVersion === 1
    && Boolean(job.encryptedPayload)
    && isValidReleaseStamp(job.releaseStamp)
    && isNonnegativeSafeInteger(job.reservedCents)
    && isPositiveSafeInteger(job.outputCount)
    && (job.durationSeconds === undefined || isPositiveSafeInteger(job.durationSeconds))
  );
}

function isValidCanaryBudgetPolicy(
  value: StudioCanaryBudgetPolicy | undefined,
): value is StudioCanaryBudgetPolicy {
  return value !== undefined
    && value.budgetId.trim().length > 0
    && value.budgetId.length <= 4_096
    && !/[\u0000\r\n]/u.test(value.budgetId)
    && isNonnegativeSafeInteger(value.totalCapCents)
    && isNonnegativeSafeInteger(value.falCapCents);
}

function isTerminalJobState(state: StudioJobState): boolean {
  return state === "delivered" || state === "failed" || state === "canceled" || state === "maintenance_rejected";
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

function isNonnegativeSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function utcDay(now: number): string {
  return new Date(now).toISOString().slice(0, 10);
}

function secondsToUtcMidnight(now: number): number {
  const date = new Date(now);
  const midnight = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() + 1);
  return Math.max(1, Math.ceil((midnight - now) / 1_000));
}
