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
  readonly schemaVersion: 1;
  readonly workflow: {
    readonly workflowId: string;
    readonly workflowVersion: 1;
    readonly stepId: "provider-generation";
  };
  readonly model: {
    readonly slug: string;
    readonly provider: "gmi" | "fal";
    readonly providerModelId: string;
  };
  readonly output: {
    readonly count: number;
    readonly durationSeconds?: number | undefined;
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

export interface StudioOutboxPayload {
  readonly kind: "text" | "attachment" | "tapback";
  readonly thread: {
    readonly chatGuid: string;
    readonly sendingLine: string;
    readonly replyTargetId?: string | undefined;
  };
  readonly text?: string | undefined;
  readonly attachment?: StudioAttachmentReference | undefined;
  readonly targetMessageId?: string | undefined;
  readonly outputIndex?: number | undefined;
}

export interface StudioOutboxRecord {
  readonly schemaVersion: 3;
  readonly outboxId: string;
  readonly jobId?: string | undefined;
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
      readonly reason: "active_job" | "user_limit" | "per_job_cap" | "daily_cap" | "release_blocked" | "storage_error";
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

export interface AdmitStudioJobInput {
  readonly job: StudioJobRecordWithPayload;
  readonly activeJobLimit: number;
  readonly dailyUserJobLimit: number;
  readonly perJobCapCents: number;
  readonly dailySpendCapCents: number;
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
    // Receipt + ready index must commit together. A separate SET followed by
    // ZADD could acknowledge a webhook and strand work after a crash.
    const result = await this.redis.eval(PERSIST_INBOX_LUA, {
      keys: [key, this.inboxEnvelopeKey(input.receiptId), this.readyInboxKey(), this.inboxScopeSequenceKey(input.senderHash, input.threadHash)],
      arguments: [JSON.stringify(metadata), JSON.stringify(encryptedEnvelope), String(INBOX_ENVELOPE_TTL_SECONDS), String(now), input.receiptId],
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
    const due = await this.redis.zRangeByScore(this.readyInboxKey(), Number.NEGATIVE_INFINITY, this.now(), { limit: 50 });
    for (const receiptId of due) {
      const token = randomBytes(16).toString("base64url");
      const claimed = await this.claimInbox(receiptId, token);
      if (claimed) return Object.freeze({ record: claimed, leaseToken: token });

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
    const result = await this.redis.eval(FINISH_INBOX_LUA, {
      keys: [this.inboxKey(receiptId), this.inboxEnvelopeKey(receiptId), this.inboxLeaseKey(receiptId), this.readyInboxKey()],
      arguments: [leaseToken, state, String(this.now()), String(INBOX_TTL_SECONDS), receiptId],
    });
    return result === 1 || result === "1";
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
    const result = await this.redis.eval(RETRY_INBOX_LUA, {
      keys: [this.inboxKey(receiptId), this.inboxEnvelopeKey(receiptId), this.inboxLeaseKey(receiptId), this.readyInboxKey()],
      arguments: [leaseToken, String(this.now()), String(retryAfterMs), String(INBOX_TTL_SECONDS), receiptId],
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
    const result = await this.redis.eval(ADMIT_JOB_LUA, {
      keys: [
        activeKey,
        userCountKey,
        dailySpendKey,
        jobKey,
        jobPayloadKey,
        this.readyJobKey(),
        this.releaseFenceKey(),
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
    if (code === "active_job" || code === "user_limit" || code === "daily_cap" || code === "release_blocked") {
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
    if (code === "active_job" || code === "user_limit" || code === "daily_cap" || code === "per_job_cap" || code === "release_blocked") {
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
    if (
      !isSafeRecordString(job.jobId, 160)
      || !isSafeRecordString(leaseToken, 256)
      || !Number.isSafeInteger(job.stateVersion)
      || job.stateVersion < 1
      || !Number.isSafeInteger(delayMs)
      || delayMs < 0
      || delayMs > 24 * 60 * 60 * 1_000
    ) return "conflict";
    const now = this.now();
    const dueAtMs = now + delayMs;
    if (!Number.isSafeInteger(dueAtMs)) return "conflict";
    const result = await this.redis.eval(RESCHEDULE_CLAIMED_JOB_LUA, {
      keys: [this.jobKey(job.jobId), this.jobLeaseKey(job.jobId), this.readyJobKey()],
      arguments: [leaseToken, String(job.stateVersion), String(now), String(dueAtMs), job.jobId],
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

  public async createOutbox(input: CreateStudioOutboxInput): Promise<StudioOutboxRecord> {
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
  ): StudioOutboxRecord {
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 0) {
      throw new Error("outbox sequence is invalid");
    }
    const now = this.now();
    const dueAtMs = input.dueAtMs ?? now;
    if (!Number.isSafeInteger(dueAtMs) || dueAtMs < 0) {
      throw new Error("outbox due time is invalid");
    }
    if (laneIdentity !== undefined && !isSafeRecordString(laneIdentity, 160)) {
      throw new Error("outbox lane identity is invalid");
    }
    const record: StudioOutboxRecord = Object.freeze({
      schemaVersion: 3,
      outboxId: input.outboxId,
      ...(input.jobId === undefined ? {} : { jobId: input.jobId }),
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
      senderHash: source.senderHash,
      lane: source.lane,
      sequence: source.sequence,
      payload,
    }, source.pendingGuard, laneIdentity);
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

  private async claimInbox(receiptId: string, leaseToken: string): Promise<StudioInboxRecord | null> {
    const now = this.now();
    const result = await this.redis.eval(CLAIM_INBOX_LUA, {
      keys: [this.inboxKey(receiptId), this.inboxEnvelopeKey(receiptId), this.inboxLeaseKey(receiptId), this.readyInboxKey()],
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
  private inboxEnvelopeKey(receiptId: string): string { return `${this.prefix}:inbox:envelope:${receiptId}`; }
  private readyInboxKey(): string { return `${this.prefix}:inbox:ready`; }
  private inboxLeaseKey(receiptId: string): string { return `${this.prefix}:inbox:lease:${receiptId}`; }
  private inboxScopeSequenceKey(senderHash: SenderHash, threadHash: ThreadHash): string {
    return `${this.prefix}:inbox:scope-sequence:${senderHash}:${threadHash}`;
  }
  private cronHeartbeatKey(scope: StudioCronHeartbeatScope): string {
    return `${this.prefix}:operations:cron-heartbeat:${scope}`;
  }
  private releaseFenceKey(): string { return `${this.prefix}:operations:release-fence`; }
  private settingsKey(senderHash: SenderHash): string { return `${this.prefix}:settings:${senderHash}`; }
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
  private userDailyKey(senderHash: SenderHash, day: string): string { return `${this.prefix}:rate:${day}:${senderHash}`; }
  private dailySpendKey(day: string): string { return `${this.prefix}:spend:${day}`; }
  private jobKey(jobId: string): string { return `${this.prefix}:job:${jobId}`; }
  private jobPayloadKey(jobId: string): string { return `${this.prefix}:job:payload:${jobId}`; }
  private readyJobKey(): string { return `${this.prefix}:job:ready`; }
  private jobLeaseKey(jobId: string): string { return `${this.prefix}:job:lease:${jobId}`; }
  private providerSubrunKey(subrunId: string): string { return `${this.prefix}:provider-subrun:${subrunId}`; }
  private providerSubrunReferenceKey(subrunId: string): string { return `${this.prefix}:provider-subrun:reference:${subrunId}`; }
  private providerSubrunLeaseKey(subrunId: string): string { return `${this.prefix}:provider-subrun:lease:${subrunId}`; }
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
  return state === "admitted" || state === "executing";
}

const JOB_TRANSITIONS: Readonly<Record<StudioJobState, readonly StudioJobState[]>> = Object.freeze({
  received: ["validating", "maintenance_rejected", "failed"],
  validating: ["admitted", "failed", "maintenance_rejected"],
  // A recovered admission whose immutable encrypted snapshot has expired has
  // not reached a provider. It may terminalize as a known privacy failure.
  admitted: ["executing", "canceled", "failed"],
  executing: ["executing", "ready", "failed", "canceled", "reconciliation_required"],
  ready: ["delivering", "canceled"],
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
  sending: ["sent", "retryable_failure", "failed", "canceled", "send_unknown"],
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
-- AIR_STUDIO_ADMIT_JOB_V4
-- KEYS: active sender fence, daily user count, daily spend, job metadata,
--       encrypted job payload, durable job-ready index, release fence
-- ARGV: job id, metadata, payload, daily ttl, payload ttl, user limit,
--       spend cap, reserved cents, now, release generation, release phase
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
redis.call('ZADD', KEYS[6], tonumber(ARGV[9]), ARGV[1])
return {'admitted'}
`;

const ADMIT_JOB_WITH_INITIAL_NOTIFICATIONS_LUA = `
-- AIR_STUDIO_ADMIT_JOB_WITH_INITIAL_NOTIFICATIONS_V1
-- KEYS: active sender fence, daily user count, daily spend, job metadata,
--       encrypted job payload, durable job-ready index, release fence,
--       outbox-ready index, acknowledgement metadata/payload/lane,
--       optional tapback metadata/payload/lane
-- ARGV: job id, metadata, payload, daily ttl, job payload ttl, user limit,
--       daily spend cap, reserved cents, now, release generation/phase,
--       per-job cap, acknowledgement metadata/payload/id/sequence/due,
--       optional tapback metadata/payload/id/sequence/due/flag, outbox ttl
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
-- A known release revocation wins over a cost response: this stale worker
-- must not publish any user-visible effect while a rollback is in force.
if reservedCents > perJobCap then return {'per_job_cap'} end

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
return {'admitted'}
`;

const PERSIST_INBOX_LUA = `
-- AIR_STUDIO_PERSIST_INBOX_V4
-- The scope counter deliberately has no expiry: it is a compact opaque
-- fencing record and must outlive any recoverable inbox receipt.
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local record = cjson.decode(ARGV[1])
record.encryptedEnvelope = nil
record.scopeSequence = redis.call('INCR', KEYS[4])
-- A nonterminal receipt must remain recoverable until it is explicitly
-- terminalized. Its encrypted envelope is kept separately with a hard,
-- immutable 24-hour lifetime because it can contain media references.
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[5])
return cjson.encode(record)
`;

const CLAIM_INBOX_LUA = `
-- AIR_STUDIO_CLAIM_INBOX_V3
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= 'received' and record.state ~= 'claimed' then return false end
if redis.call('SET', KEYS[3], ARGV[1], 'NX', 'PX', ARGV[2]) ~= 'OK' then return false end
record.state = 'claimed'
record.stateVersion = record.stateVersion + 1
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[4], tonumber(ARGV[3]) + tonumber(ARGV[2]), ARGV[4])
local envelope = redis.call('GET', KEYS[2])
if envelope then record.encryptedEnvelope = cjson.decode(envelope) end
return cjson.encode(record)
`;

const FINISH_INBOX_LUA = `
-- AIR_STUDIO_FINISH_INBOX_V3
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local record = cjson.decode(raw)
if record.state ~= 'claimed' then return 0 end
if ARGV[2] ~= 'processed' and ARGV[2] ~= 'maintenance_rejected' and ARGV[2] ~= 'failed' then return 0 end
record.state = ARGV[2]
record.stateVersion = record.stateVersion + 1
-- Terminal receipt metadata is enough for idempotency and audit. Do not keep
-- prompts, attachment GUIDs, or raw thread identifiers after processing.
record.encryptedEnvelope = nil
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[4], ARGV[5])
redis.call('DEL', KEYS[3])
return 1
`;

const RETRY_INBOX_LUA = `
-- AIR_STUDIO_RETRY_INBOX_V3
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local record = cjson.decode(raw)
if record.state ~= 'claimed' then return 0 end
-- Privacy wins once the immutable encrypted envelope lifetime is over. The
-- opaque receipt remains terminal for seven days rather than looping forever.
if redis.call('EXISTS', KEYS[2]) ~= 1 then
  record.state = 'failed'
  record.stateVersion = record.stateVersion + 1
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('ZREM', KEYS[4], ARGV[5])
  redis.call('DEL', KEYS[3])
  return 2
end
record.state = 'received'
record.stateVersion = record.stateVersion + 1
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[4], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[5])
redis.call('DEL', KEYS[3])
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
-- KEYS: job metadata, encrypted job payload, active sender fence, job-ready index
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
if record.state ~= 'admitted' and record.state ~= 'executing' then
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
--       ready index, replacement lane index, shared release fence, audit record
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

function isExecutionSnapshotForJob(snapshot: StudioExecutionSnapshot, job: StudioJobRecord): boolean {
  return snapshot.schemaVersion === 1
    && snapshot.workflow.workflowVersion === 1
    && snapshot.workflow.stepId === "provider-generation"
    && isSafeRecordString(snapshot.workflow.workflowId, 160)
    && snapshot.model.slug === job.modelSlug
    && snapshot.model.provider === job.provider
    && isSafeRecordString(snapshot.model.providerModelId, 512)
    && snapshot.output.count === job.outputCount
    && (snapshot.output.durationSeconds ?? undefined) === (job.durationSeconds ?? undefined)
    && isValidReleaseStamp(snapshot.releaseStamp)
    && isValidReleaseStamp(job.releaseStamp)
    && sameReleaseStamp(snapshot.releaseStamp, job.releaseStamp);
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

function isOpaqueStudioIdentifier(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/u.test(value);
}

function isSafeProviderRequestId(value: string): boolean {
  return isSafeRecordString(value, 512);
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
  return expectedKind === "text" || message.targetMessageId === jobPayload.replyTargetId;
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
