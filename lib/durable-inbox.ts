import { randomBytes } from "node:crypto";
import type { EncryptedPayload, QueueCipher } from "./crypto.js";
import {
  acquireRedisLease,
  assertPseudonymousHash,
  COMPARE_AND_DELETE_LUA,
  COMPARE_AND_PEXPIRE_LUA,
  PERSIST_INBOX_NEW_RECORD_LUA,
  WRITE_INBOX_RECORD_LUA,
  type RedisLease,
  type RedisLike,
} from "./redis.js";
import {
  DEFAULT_RATE_LIMIT_PREFIX,
  type RateLimitIngressReservation,
} from "./rate-limit.js";
import {
  deriveCorrelationId,
  deriveOpaqueId,
  type CorrelationId,
  type SenderHash,
  type ThreadHash,
} from "./security.js";

export const INBOX_STATES = Object.freeze([
  "received",
  "processing",
  "session_started",
  "response_ready",
  "sending",
  "sent",
  /** A durable video job owns all remaining recovery; no inbox replay is safe. */
  "handed_off",
  "failed_retryable",
  "send_unknown",
  "dead_letter",
] as const);

export type InboxState = (typeof INBOX_STATES)[number];
export type InboxContentType = "text" | "attachment" | "reaction" | "unknown";

export interface InboxEnvelope {
  /** Values in this envelope are encrypted before any Redis write. */
  readonly projectId: string;
  readonly messageId: string;
  readonly webhookId?: string | undefined;
  readonly chatGuid: string;
  readonly sendingLine: string;
  readonly senderId: string;
  readonly direction: "inbound" | "outbound";
  readonly content: {
    readonly type: InboxContentType;
    readonly text: string | null;
    /** Photon attachment references only; bytes are fetched just-in-time. */
    readonly attachments?: readonly {
      readonly id: string;
      readonly name: string | null;
      readonly mimeType: string | null;
      readonly size: number | null;
    }[] | undefined;
  };
  readonly isGroup: boolean;
  readonly receivedAtMs: number;
}

/**
 * Ingress provides only verified, normalized values. The raw sender/chat/text
 * are kept exclusively inside `envelope`, which is AES-GCM encrypted here.
 */
export interface VerifiedInboxInput {
  readonly projectId: string;
  readonly messageId: string;
  readonly webhookId?: string | undefined;
  readonly senderHash: SenderHash | string;
  readonly threadHash: ThreadHash | string;
  readonly contentType: InboxContentType;
  readonly isGroup: boolean;
  readonly receivedAtMs?: number | undefined;
  /**
   * Present only for an allowed, substantive direct-message receipt. The
   * first-publication Redis script reserves this correlation atomically with
   * the durable inbox record, so later queue recovery cannot re-consume it.
   */
  readonly rateLimitReservation?: RateLimitIngressReservation | undefined;
  readonly envelope: InboxEnvelope;
}

export interface ImmutableInboxMetadata {
  readonly recordKey: string;
  readonly correlationId: CorrelationId;
  readonly senderHash: SenderHash;
  readonly threadHash: ThreadHash;
  readonly contentType: InboxContentType;
  readonly isGroup: boolean;
  readonly receivedAtMs: number;
  /** Monotonic Redis-issued sequence; orders equal-millisecond thread arrivals. */
  readonly threadSequence: number;
  readonly payloadExpiresAtMs: number;
  readonly metadataExpiresAtMs: number;
}

export interface InboxDeliveryMetadata {
  readonly deliveryCount: number;
  /** HMACs of X-Spectrum-Webhook-Id values, never provider IDs themselves. */
  readonly webhookIdHashes: readonly string[];
  readonly firstReceivedAtMs: number;
  readonly lastReceivedAtMs: number;
}

export interface InboxStatusMetadata {
  readonly attempts: number;
  readonly updatedAtMs: number;
  readonly nextAttemptAtMs?: number | undefined;
  readonly processingLeaseExpiresAtMs?: number | undefined;
  /** HMAC of the active worker/session capability; the raw token is never metadata. */
  readonly processingAttemptHash?: string | undefined;
  readonly failureCategory?: string | undefined;
}

/** Plaintext Redis record. It intentionally has no message text, handle, URL, or signature. */
export interface InboxRecord {
  readonly schemaVersion: 1;
  readonly immutable: ImmutableInboxMetadata;
  readonly delivery: InboxDeliveryMetadata;
  readonly state: InboxState;
  readonly stateVersion: number;
  readonly status: InboxStatusMetadata;
  readonly encryptedEnvelope?: EncryptedPayload | undefined;
}

export interface PersistedInbox {
  readonly created: boolean;
  readonly record: InboxRecord;
}

/**
 * Privacy-safe queue observability. These values derive only from private
 * Redis indexes and immutable receipt timestamps; no envelope data is read
 * into the returned metric.
 */
export interface InboxQueueMetrics {
  /** Number of nonterminal records currently in the durable inbox. */
  readonly depth: number;
  /**
   * Age of the oldest queued receipt. Undefined only while a bounded
   * migration fallback cannot establish an exact value.
   */
  readonly oldestInboxAgeMs?: number | undefined;
}

interface ThreadQueueHeadLookup {
  readonly recordKey: string | null;
  /** A bounded stale-record repair pass has more members left to clean. */
  readonly repairIncomplete: boolean;
}

type InboxPublication =
  | { readonly created: true; readonly threadSequence: number }
  | { readonly created: false };

export interface InboxTransitionOptions {
  readonly expectedStates?: readonly InboxState[] | undefined;
  /**
   * Required to advance an active processing attempt. Async Eve event handlers
   * must retain the token from `InboxProcessingLease.attemptToken` and supply
   * it with their expected state, so an event from a superseded worker fails
   * closed instead of advancing a recovered attempt.
   */
  readonly expectedAttemptToken?: string | undefined;
  readonly failureCategory?: string | undefined;
  /** Required when moving to failed_retryable. */
  readonly nextAttemptAtMs?: number | undefined;
  /** Set by `beginProcessing` so crashed workers can be recovered safely. */
  readonly processingLeaseExpiresAtMs?: number | undefined;
}

/** Private extension used only while minting an attempt in `beginProcessing`. */
interface InternalInboxTransitionOptions extends InboxTransitionOptions {
  readonly processingAttemptHash?: string | undefined;
}

export interface InboxProcessingLease {
  readonly record: InboxRecord;
  /** Decrypted only after the worker owns the per-thread lease. */
  readonly envelope: InboxEnvelope;
  /** Persist this with the active Eve turn and use it for async transitions. */
  readonly attemptToken: string;
  heartbeat(): Promise<boolean>;
  transition(state: InboxState, options?: InboxTransitionOptions): Promise<InboxRecord | null>;
  release(): Promise<boolean>;
}

export interface DurableInboxOptions {
  readonly redis: RedisLike;
  readonly cipher: QueueCipher;
  readonly identityHashSecret: string;
  readonly prefix?: string | undefined;
  readonly clock?: (() => number) | undefined;
  readonly payloadRetentionMs?: number | undefined;
  readonly metadataRetentionMs?: number | undefined;
  readonly threadLeaseTtlMs?: number | undefined;
  /** Upper bound on durable processing attempts before a redacted dead letter. */
  readonly maxProcessingAttempts?: number | undefined;
  /** Must match the RateLimiter namespace used to mint ingress reservations. */
  readonly rateLimitPrefix?: string | undefined;
}

const TERMINAL_STATES = new Set<InboxState>(["sent", "handed_off", "send_unknown", "dead_letter"]);
const TRANSITIONS: Readonly<Record<InboxState, readonly InboxState[]>> = Object.freeze({
  received: ["processing", "dead_letter"],
  processing: ["session_started", "response_ready", "handed_off", "failed_retryable", "dead_letter"],
  session_started: ["response_ready", "failed_retryable", "dead_letter"],
  response_ready: ["sending", "failed_retryable", "dead_letter"],
  sending: ["sent", "failed_retryable", "send_unknown", "dead_letter"],
  sent: [],
  handed_off: [],
  failed_retryable: ["processing", "dead_letter"],
  send_unknown: [],
  dead_letter: [],
});

const MAX_RECORDED_DELIVERIES = 16;
const DEFAULT_MAX_PROCESSING_ATTEMPTS = 3;
/** Legacy records created before the received-at index are scanned only in a bounded maintenance window. */
const QUEUE_METRICS_FALLBACK_SCAN_LIMIT = 500;
const QUEUE_METRICS_STALE_INDEX_REPAIRS = 16;
/** Normal scheduling reads one ready/recovery head per thread, never every tail. */
const READY_THREAD_SCAN_LIMIT = 500;
const THREAD_HEAD_STALE_REPAIR_LIMIT = 500;
const THREAD_HEAD_STALE_REPAIR_PAGE_SIZE = 32;
const READY_THREADS_BACKFILL_REQUIRED_ERROR = "inbox ready-thread index backfill required";
/**
 * A duplicate Photon delivery can arrive while the first request is still
 * committing its idempotency record.  Wait only a small, fixed fraction of
 * the webhook's one-second acknowledgement budget before asking Photon to
 * retry; a second valid concurrent delivery should normally be recorded as
 * duplicate metadata rather than be surfaced as a spurious 503.
 */
const INGRESS_RECORD_LEASE_WAIT_MS = 250;
const RECORD_LEASE_RETRY_INTERVAL_MS = 5;

/**
 * Redis key names are HMAC-only: provider project/message identifiers remain
 * exclusively in the encrypted receipt envelope.
 */
export function inboxRecordKey(correlationId: CorrelationId | string, prefix = "air:inbox:v1"): string {
  assertPseudonymousHash(correlationId, "inbox correlation id");
  return `${prefix}:record:${correlationId}`;
}

export function canTransitionInboxState(from: InboxState, to: InboxState): boolean {
  return TRANSITIONS[from].includes(to);
}

/**
 * Durable encrypted inbox with project+message idempotency and per-thread
 * ordered leases. No operation needs live credentials beyond the caller's
 * Redis adapter and the already-configured queue cipher.
 */
export class DurableInbox {
  private readonly redis: RedisLike;
  private readonly cipher: QueueCipher;
  private readonly identityHashSecret: string;
  private readonly prefix: string;
  private readonly clock: () => number;
  private readonly payloadRetentionMs: number;
  private readonly metadataRetentionMs: number;
  private readonly threadLeaseTtlMs: number;
  private readonly maxProcessingAttempts: number;
  private readonly rateLimitPrefix: string;

  public constructor(options: DurableInboxOptions) {
    if (!options.identityHashSecret?.trim()) {
      throw new Error("identity hash secret is required for durable inbox");
    }
    this.redis = options.redis;
    this.cipher = options.cipher;
    this.identityHashSecret = options.identityHashSecret;
    this.prefix = options.prefix ?? "air:inbox:v1";
    this.clock = options.clock ?? Date.now;
    this.payloadRetentionMs = options.payloadRetentionMs ?? 86_400_000;
    this.metadataRetentionMs = options.metadataRetentionMs ?? 604_800_000;
    this.threadLeaseTtlMs = options.threadLeaseTtlMs ?? 75_000;
    this.maxProcessingAttempts = options.maxProcessingAttempts ?? DEFAULT_MAX_PROCESSING_ATTEMPTS;
    this.rateLimitPrefix = options.rateLimitPrefix ?? DEFAULT_RATE_LIMIT_PREFIX;
    for (const value of [this.payloadRetentionMs, this.metadataRetentionMs, this.threadLeaseTtlMs]) {
      if (!Number.isInteger(value) || value < 1_000) throw new Error("invalid inbox retention or lease setting");
    }
    if (this.metadataRetentionMs < this.payloadRetentionMs) {
      throw new Error("metadata retention must outlive encrypted payload retention");
    }
    if (!Number.isInteger(this.maxProcessingAttempts) || this.maxProcessingAttempts < 1) {
      throw new Error("max processing attempts must be a positive integer");
    }
    if (!/^[A-Za-z0-9:_-]{1,128}$/.test(this.rateLimitPrefix)) {
      throw new Error("rate-limit prefix is invalid");
    }
  }

  /**
   * Persists the verified delivery before HTTP acknowledgement. `SET NX` makes
   * project+message retries idempotent; duplicate Spectrum webhook IDs become
   * bounded, hashed delivery metadata only.
   */
  async persistVerified(input: VerifiedInboxInput): Promise<PersistedInbox> {
    assertProviderIdentifier(input.projectId, "project id");
    assertProviderIdentifier(input.messageId, "message id");
    assertPseudonymousHash(input.senderHash, "sender hash");
    assertPseudonymousHash(input.threadHash, "thread hash");
    validateVerifiedEnvelope(input);

    const now = input.receivedAtMs ?? this.clock();
    if (!Number.isSafeInteger(now) || now < 0) throw new Error("received time is invalid");
    // Receipt time is generated by Air after signature validation. Do not let
    // a provider-provided event timestamp choose queue order or retention.
    const normalizedEnvelope: InboxEnvelope = Object.freeze({
      ...input.envelope,
      webhookId: input.webhookId ?? input.envelope.webhookId,
      receivedAtMs: now,
    });
    const correlationId = deriveCorrelationId(this.identityHashSecret, input.projectId, input.messageId);
    const recordKey = inboxRecordKey(correlationId, this.prefix);
    const rateLimitReservation = validateRateLimitReservation(
      input.rateLimitReservation,
      correlationId,
      input.senderHash,
      this.rateLimitPrefix,
    );
    const threadHash = input.threadHash as ThreadHash;
    const webhookIdHash = input.webhookId
      ? deriveOpaqueId(this.identityHashSecret, "spectrum-webhook", input.webhookId)
      : undefined;
    const verifiedIdentity = {
      senderHash: input.senderHash as SenderHash,
      threadHash,
      contentType: input.contentType,
      isGroup: input.isGroup,
    } as const;

    // Serialize first creation, duplicate repair, and logical-expiry cleanup
    // for this project/message idempotency generation. Sequence allocation and
    // queue publication themselves happen in one Redis script below, so a
    // lease expiry or process crash cannot expose a durable record without its
    // earlier per-thread queue entry.
    const recordLease = await this.acquireRecordLease(recordKey, INGRESS_RECORD_LEASE_WAIT_MS);
    if (!recordLease) throw new Error("inbox record is busy");
    try {
      // Avoid extending sequence retention for a redelivery. The original
      // record already owns both its sequence and its seven-day TTL.
      if (await this.redis.get(this.metadataRecordKey(recordKey))) {
        const duplicate = await this.recordDuplicateDeliveryUnderRecordLease(
          recordKey,
          webhookIdHash,
          now,
          normalizedEnvelope,
          verifiedIdentity,
        );
        if (!duplicate) throw new Error("inbox record disappeared during duplicate handling");
        return Object.freeze({ created: false, record: duplicate });
      }

      // The script replaces this marker with its atomically minted sequence in
      // both stored documents before making either queue index visible.
      const unpublishedRecord = freezeInboxRecord({
        schemaVersion: 1,
        immutable: {
          recordKey,
          correlationId,
          senderHash: verifiedIdentity.senderHash,
          threadHash,
          contentType: input.contentType,
          isGroup: input.isGroup,
          receivedAtMs: now,
          threadSequence: -1,
          payloadExpiresAtMs: now + this.payloadRetentionMs,
          metadataExpiresAtMs: now + this.metadataRetentionMs,
        },
        delivery: {
          deliveryCount: 1,
          webhookIdHashes: webhookIdHash ? [webhookIdHash] : [],
          firstReceivedAtMs: now,
          lastReceivedAtMs: now,
        },
        state: "received",
        stateVersion: 1,
        status: { attempts: 0, updatedAtMs: now, nextAttemptAtMs: now },
        encryptedEnvelope: this.cipher.encrypt(normalizedEnvelope, recordKey),
      });
      const publication = await this.publishNewRecord(unpublishedRecord, now, rateLimitReservation);
      if (publication.created) {
        const record = withThreadSequence(unpublishedRecord, publication.threadSequence);
        return Object.freeze({ created: true, record });
      }

      // Another process may have won the metadata key after the initial read.
      // Repair its durable payload/indexes before acknowledging this duplicate.
      const duplicate = await this.recordDuplicateDeliveryUnderRecordLease(
        recordKey,
        webhookIdHash,
        now,
        normalizedEnvelope,
        verifiedIdentity,
      );
      if (!duplicate) {
        // A status record may have expired between the script's duplicate
        // result and this read. Ask Spectrum to retry rather than acknowledging
        // no durable record.
        throw new Error("inbox record disappeared during duplicate handling");
      }
      return Object.freeze({ created: false, record: duplicate });
    } finally {
      await recordLease.release();
    }
  }

  async getRecord(recordKey: string): Promise<InboxRecord | null> {
    const record = await this.readStoredRecord(recordKey);
    if (!record) return null;
    const now = this.clock();
    if (record.immutable.metadataExpiresAtMs <= now) return this.expireMetadata(recordKey);
    if (
      (!TERMINAL_STATES.has(record.state) && !record.encryptedEnvelope) ||
      (record.encryptedEnvelope && record.immutable.payloadExpiresAtMs <= now)
    ) {
      return this.expirePayload(recordKey, "payload_expired");
    }
    return record;
  }

  /**
   * Returns records that are safe to attempt now. A stale worker lease is
   * converted to retryable work; no in-flight state is blindly replayed.
   */
  async listDue(limit = 50): Promise<readonly InboxRecord[]> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 500) {
      throw new Error("inbox due limit must be between 1 and 500");
    }
    const now = this.clock();
    const due: InboxRecord[] = [];
    await this.assertReadyThreadIndexPresent();
    const threadHashes = await this.redis.zRangeByScore(
      this.readyThreadsIndexKey(),
      Number.NEGATIVE_INFINITY,
      now,
      { limit: Math.min(READY_THREAD_SCAN_LIMIT, Math.max(limit * 4, limit)) },
    );

    // The ready index has exactly one member per thread: its current head.
    // Publication, state writes, heartbeats, and terminal promotion update it
    // in the same Redis scripts as the durable queue. That makes scheduling
    // independent of how many later records a busy thread has accumulated.
    for (const threadHashValue of threadHashes) {
      if (!isPseudonymousHash(threadHashValue)) {
        await this.redis.zRem(this.readyThreadsIndexKey(), threadHashValue);
        continue;
      }
      const threadHash = threadHashValue as ThreadHash;
      const head = await this.readThreadQueueHead(threadHash);
      if (head.repairIncomplete) continue;
      if (!head.recordKey) {
        await this.redis.zRem(this.readyThreadsIndexKey(), threadHash);
        continue;
      }

      let record = await this.getRecord(head.recordKey);
      if (!record || TERMINAL_STATES.has(record.state)) {
        await this.reconcileReadyThread(threadHash);
        continue;
      }
      if (
        record.state !== "received" &&
        record.state !== "failed_retryable" &&
        record.status.processingLeaseExpiresAtMs !== undefined &&
        record.status.processingLeaseExpiresAtMs <= now
      ) {
        // Re-check the expiration while holding the record mutex. A heartbeat
        // can refresh the processing lease between the scan above and a state
        // write; treating that live worker as stale would create two owners.
        record = (await this.recoverExpiredProcessingLease(head.recordKey)) ?? record;
      }
      if (!isDispatchable(record, now)) {
        // A legacy/stale index member must not make a live worker look due.
        // Re-score it to the active lease expiry (or retry time) for later.
        await this.reconcileReadyThread(threadHash);
        continue;
      }

      // The head can change only through an atomic queue write, but re-check
      // it before handing work to a caller after an asynchronous record read.
      const latestHead = await this.readThreadQueueHead(threadHash);
      if (latestHead.repairIncomplete || latestHead.recordKey !== head.recordKey) {
        await this.reconcileReadyThread(threadHash);
        continue;
      }
      due.push(record);
    }
    // Redis uses the ready-thread hash only as a membership key. Restore the
    // durable head's due-time/record-key order before applying the worker
    // limit, so equal-millisecond arrivals remain deterministic across chats.
    due.sort(compareDueInboxRecords);
    return Object.freeze(due.slice(0, limit));
  }

  /**
   * Returns queue depth plus an exact oldest-receipt age without exposing a
   * Redis key, provider identifier, sender, thread, or encrypted envelope.
   *
   * The scheduling index is deliberately re-scored when a retry is deferred,
   * so it cannot identify the oldest receipt. A second, lockstep received-at
   * index preserves that ordering. Deployments with pre-index records fall
   * back to a bounded complete scan; if the queue is larger than that bounded
   * window, the age is withheld rather than reported inaccurately.
   */
  async getQueueMetrics(): Promise<InboxQueueMetrics> {
    const [depth, receivedAtIndexDepth] = await Promise.all([
      this.redis.zCard(this.pendingIndexKey()),
      this.redis.zCard(this.receivedAtIndexKey()),
    ]);
    if (depth === 0) return Object.freeze({ depth: 0, oldestInboxAgeMs: 0 });

    if (depth === receivedAtIndexDepth) {
      const oldest = await this.readOldestReceivedAtRecord();
      if (oldest) {
        const [reconciledDepth, reconciledReceivedAtIndexDepth] = await Promise.all([
          this.redis.zCard(this.pendingIndexKey()),
          this.redis.zCard(this.receivedAtIndexKey()),
        ]);
        if (reconciledDepth === depth && reconciledReceivedAtIndexDepth === depth) {
          return Object.freeze({
            depth,
            oldestInboxAgeMs: Math.max(0, this.clock() - oldest.immutable.receivedAtMs),
          });
        }
        return this.getLegacyQueueMetrics(reconciledDepth);
      }
    }

    return this.getLegacyQueueMetrics(depth);
  }

  /**
   * Acquires the per-thread lease and only starts the queue head, preventing
   * rapid messages from producing interleaved sessions. The caller must hold
   * this lease until its terminal inbox transition, and heartbeat long turns.
   */
  async beginProcessing(recordKey: string): Promise<InboxProcessingLease | null> {
    const initial = await this.getRecord(recordKey);
    if (!initial || !isDispatchable(initial, this.clock())) return null;

    // New ingress records publish their sequence and both queue indexes in one
    // Redis script, so a thread head is never observable before an earlier
    // committed record. Mint the attempt before the physical lock so that
    // later, stateless Eve lifecycle callbacks can prove ownership of this
    // exact lock without retaining a process-local RedisLease object.
    const attemptToken = createAttemptToken();
    const threadLease = await acquireRedisLease(
      this.redis,
      this.threadLeaseKey(initial.immutable.threadHash),
      {
        ttlMs: this.threadLeaseTtlMs,
        now: this.clock,
        token: this.attemptThreadLeaseToken(attemptToken),
      },
    );
    if (!threadLease) return null;
    let keepThreadLease = false;
    try {
      const head = await this.nextThreadQueueHead(initial.immutable.threadHash);
      if (head !== recordKey) return null;
      const processing = await this.transitionInternal(recordKey, "processing", {
        expectedStates: ["received", "failed_retryable"],
        processingLeaseExpiresAtMs: threadLease.expiresAt,
        processingAttemptHash: this.attemptHash(attemptToken),
      });
      if (
        !processing?.encryptedEnvelope ||
        processing.state !== "processing" ||
        processing.status.processingLeaseExpiresAtMs !== threadLease.expiresAt ||
        processing.status.processingAttemptHash !== this.attemptHash(attemptToken)
      ) {
        return null;
      }

      let envelope: InboxEnvelope;
      try {
        envelope = this.cipher.decrypt<InboxEnvelope>(processing.encryptedEnvelope, recordKey);
      } catch {
        await this.transition(recordKey, "dead_letter", {
          expectedStates: ["processing"],
          expectedAttemptToken: attemptToken,
          failureCategory: "payload_authentication_failed",
        });
        return null;
      }

      let released = false;
      const capability = Object.freeze({
        record: processing,
        envelope: Object.freeze(envelope),
        attemptToken,
        heartbeat: async (): Promise<boolean> => {
          if (released) return false;
          return this.heartbeatAttempt(recordKey, attemptToken);
        },
        transition: (state: InboxState, options?: InboxTransitionOptions) =>
          this.transition(recordKey, state, {
            ...options,
            expectedAttemptToken: attemptToken,
          }),
        release: async (): Promise<boolean> => {
          if (released) return false;
          released = true;
          return this.releaseThreadLease(initial.immutable.threadHash, attemptToken);
        },
      });
      // Do not release the physical per-thread lease in this setup `finally`:
      // the returned capability owns it until caller release (or TTL expiry).
      keepThreadLease = true;
      return capability;
    } finally {
      if (!keepThreadLease) await threadLease.release();
    }
  }

  /**
   * Renew an attempt-owned physical thread lock and its durable expiry from a
   * later serverless invocation. A callback may never revive an attempt once
   * its logical lease has expired or its state has become terminal.
   */
  async heartbeatAttempt(recordKey: string, attemptToken: string): Promise<boolean> {
    if (!isAttemptToken(attemptToken)) return false;
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) return false;
    try {
      const current = await this.readStoredRecord(recordKey);
      const now = this.clock();
      if (
        !current ||
        TERMINAL_STATES.has(current.state) ||
        current.status.processingAttemptHash !== this.attemptHash(attemptToken) ||
        current.status.processingLeaseExpiresAtMs === undefined ||
        current.status.processingLeaseExpiresAtMs <= now
      ) {
        return false;
      }
      const renewed = await this.redis.eval(COMPARE_AND_PEXPIRE_LUA, {
        keys: [this.threadLeaseKey(current.immutable.threadHash)],
        arguments: [this.attemptThreadLeaseToken(attemptToken), String(this.threadLeaseTtlMs)],
      });
      if (Number(renewed) !== 1) return false;
      const next = freezeInboxRecord({
        ...current,
        status: {
          ...current.status,
          updatedAtMs: now,
          processingLeaseExpiresAtMs: now + this.threadLeaseTtlMs,
        },
      });
      await this.writeStoredRecord(next, now);
      return true;
    } finally {
      await lease.release();
    }
  }

  /**
   * Release the physical thread lock for an attempt after its durable terminal
   * transition. Token comparison makes a late attempt unable to release a
   * recovered successor's lock.
   */
  async releaseAttempt(recordKey: string, attemptToken: string): Promise<boolean> {
    if (!isAttemptToken(attemptToken)) return false;
    const current = await this.readStoredRecord(recordKey);
    if (!current) return false;
    return this.releaseThreadLease(current.immutable.threadHash, attemptToken);
  }

  /**
   * Conditionally advance an inbox record. Once an attempt is active, callers
   * must provide its raw `expectedAttemptToken`; `beginProcessing` is the only
   * public way to enter `processing` and mint that capability.
   */
  async transition(
    recordKey: string,
    targetState: InboxState,
    options: InboxTransitionOptions = {},
  ): Promise<InboxRecord | null> {
    if (targetState === "processing") {
      throw new Error("inbox processing attempts must start via beginProcessing");
    }
    return this.transitionInternal(recordKey, targetState, options);
  }

  /** State transition implementation with a private processing-attempt hash. */
  private async transitionInternal(
    recordKey: string,
    targetState: InboxState,
    options: InternalInboxTransitionOptions = {},
  ): Promise<InboxRecord | null> {
    if (!INBOX_STATES.includes(targetState)) throw new Error("unknown inbox state");
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) throw new Error("inbox record is busy");
    let next: InboxRecord | null = null;
    try {
      let current = await this.readStoredRecord(recordKey);
      if (!current) return null;
      const now = this.clock();
      if (
        (!TERMINAL_STATES.has(current.state) && !current.encryptedEnvelope) ||
        (current.encryptedEnvelope && current.immutable.payloadExpiresAtMs <= now)
      ) {
        next = makePayloadExpiredRecord(current, now, "payload_expired");
      } else {
        if (options.expectedStates && !options.expectedStates.includes(current.state)) {
          return current;
        }
        if (!this.hasExpectedAttemptOwnership(current, options.expectedAttemptToken)) {
          // Ownership mismatches are intentionally indistinguishable from a
          // missing record to callers. A stale event must not learn or mutate
          // a successor attempt's state.
          return null;
        }
        if (targetState === "processing" && current.status.attempts >= this.maxProcessingAttempts) {
          // The durable queue, rather than a particular runtime retry loop,
          // is the authority for this bound. Every retry path must re-enter
          // through beginProcessing, so a fourth attempt cannot restart work.
          next = makeTransitionRecord(current, "dead_letter", now, {
            failureCategory: "processing_retries_exhausted",
          });
        } else {
          if (!canTransitionInboxState(current.state, targetState)) {
            throw new Error(`invalid inbox state transition: ${current.state} -> ${targetState}`);
          }
          next = makeTransitionRecord(current, targetState, now, options);
        }
      }
      await this.writeStoredRecord(next, now);
    } finally {
      await lease.release();
    }
    if (next) {
      try {
        // `writeStoredRecord` commits payload, redacted metadata, and queue
        // indexes in one Lua transaction before we release the attempt.
      } finally {
        // A durable retry/terminal transition ends this attempt's ownership.
        // Do this after status and indexes have been committed so a later
        // thread head cannot start against an uncommitted predecessor.
        if (
          options.expectedAttemptToken &&
          (next.state === "failed_retryable" || TERMINAL_STATES.has(next.state))
        ) {
          await this.releaseAttempt(recordKey, options.expectedAttemptToken).catch(() => false);
        }
      }
    }
    return next;
  }

  /** Remove only encrypted content after its 24-hour recovery deadline. */
  async expirePayload(recordKey: string, category = "payload_expired"): Promise<InboxRecord | null> {
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) throw new Error("inbox record is busy");
    let next: InboxRecord | null = null;
    try {
      let current = await this.readStoredRecord(recordKey);
      if (!current) return null;
      const now = this.clock();
      if (
        (TERMINAL_STATES.has(current.state) && !current.encryptedEnvelope) ||
        (current.encryptedEnvelope && current.immutable.payloadExpiresAtMs > now)
      ) return current;
      next = makePayloadExpiredRecord(current, now, category);
      await this.writeStoredRecord(next, now);
    } finally {
      await lease.release();
    }
    return next;
  }

  /** Remove a fully expired record only while creation for this key is fenced. */
  private async expireMetadata(recordKey: string): Promise<InboxRecord | null> {
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) throw new Error("inbox record is busy");
    try {
      const current = await this.readStoredRecord(recordKey);
      if (!current) return null;
      if (current.immutable.metadataExpiresAtMs > this.clock()) return current;
      await this.redis.del(recordKey, this.metadataRecordKey(recordKey));
      await this.removeFromIndexes(current);
      return null;
    } finally {
      await lease.release();
    }
  }

  private async recordDuplicateDeliveryUnderRecordLease(
    recordKey: string,
    webhookIdHash: string | undefined,
    now: number,
    repairEnvelope: InboxEnvelope,
    verifiedIdentity: Pick<ImmutableInboxMetadata, "senderHash" | "threadHash" | "contentType" | "isGroup">,
  ): Promise<InboxRecord | null> {
      let current = await this.readStoredRecord(recordKey);
      if (!current) return null;
      if (
        current.immutable.senderHash !== verifiedIdentity.senderHash ||
        current.immutable.threadHash !== verifiedIdentity.threadHash ||
        current.immutable.contentType !== verifiedIdentity.contentType ||
        current.immutable.isGroup !== verifiedIdentity.isGroup
      ) {
        // A project/message id is the idempotency key, never permission to
        // attach a different verified sender or chat envelope to its record.
        throw new Error("duplicate inbox delivery identity does not match the original record");
      }
      if (current.encryptedEnvelope && current.immutable.payloadExpiresAtMs <= now) {
        const expired = makePayloadExpiredRecord(current, now, "payload_expired");
        await this.writeStoredRecord(expired, now);
        return expired;
      }
      if (!current.encryptedEnvelope && !TERMINAL_STATES.has(current.state)) {
        if (current.immutable.payloadExpiresAtMs <= now) {
          const expired = makePayloadExpiredRecord(current, now, "payload_expired");
          await this.writeStoredRecord(expired, now);
          return expired;
        }
        current = freezeInboxRecord({
          ...current,
          encryptedEnvelope: this.cipher.encrypt(repairEnvelope, recordKey),
        });
      }
      const hashes = webhookIdHash && !current.delivery.webhookIdHashes.includes(webhookIdHash)
        ? [...current.delivery.webhookIdHashes, webhookIdHash].slice(-MAX_RECORDED_DELIVERIES)
        : [...current.delivery.webhookIdHashes];
      const next = freezeInboxRecord({
        ...current,
        delivery: {
          ...current.delivery,
          deliveryCount: current.delivery.deliveryCount + 1,
          webhookIdHashes: hashes,
          lastReceivedAtMs: now,
        },
        status: { ...current.status, updatedAtMs: now },
      });
      await this.writeStoredRecord(next, now);
      return next;
  }

  /**
   * Atomically turns a truly abandoned active attempt back into retryable work.
   * This deliberately bypasses public transition ownership because expiration
   * is the recovery authority, but it performs the expiration check inside the
   * same record mutex as the write.
   */
  private async recoverExpiredProcessingLease(recordKey: string): Promise<InboxRecord | null> {
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) return null;
    let next: InboxRecord | null = null;
    try {
      const current = await this.readStoredRecord(recordKey);
      if (!current) return null;
      const now = this.clock();
      if (
        current.state === "received" ||
        current.state === "failed_retryable" ||
        TERMINAL_STATES.has(current.state) ||
        current.status.processingAttemptHash === undefined ||
        current.status.processingLeaseExpiresAtMs === undefined ||
        current.status.processingLeaseExpiresAtMs > now
      ) {
        return current;
      }
      // A static reply may have reached Photon immediately before this worker
      // vanished. Its delivery outcome is unknowable, so never replay it.
      next = current.state === "sending"
        ? makeTransitionRecord(current, "send_unknown", now, {
          failureCategory: "photon_static_send_unknown",
        })
        : makeTransitionRecord(current, "failed_retryable", now, {
          failureCategory: "worker_lease_expired",
          nextAttemptAtMs: now,
        });
      await this.writeStoredRecord(next, now);
    } finally {
      await lease.release();
    }
    return next;
  }

  private async readStoredRecord(recordKey: string): Promise<InboxRecord | null> {
    const serialized = await this.redis.get(recordKey) ?? await this.redis.get(this.metadataRecordKey(recordKey));
    if (!serialized) return null;
    return parseInboxRecord(serialized);
  }

  private async writeStoredRecord(record: InboxRecord, now: number): Promise<void> {
    const index = inboxIndexIntent(record);
    const result = await this.redis.eval(WRITE_INBOX_RECORD_LUA, {
      keys: [
        this.metadataRecordKey(record.immutable.recordKey),
        record.immutable.recordKey,
        this.pendingIndexKey(),
        this.threadQueueKey(record.immutable.threadHash),
        this.receivedAtIndexKey(),
        this.readyThreadsIndexKey(),
      ],
      arguments: [
        JSON.stringify(redactInboxRecord(record)),
        String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        record.encryptedEnvelope ? JSON.stringify(record) : "",
        record.encryptedEnvelope ? String(ttlSeconds(record.immutable.payloadExpiresAtMs, now)) : "0",
        record.encryptedEnvelope ? "1" : "0",
        index.mode,
        String(index.pendingScore),
        record.immutable.recordKey,
        String(record.immutable.threadSequence),
        String(record.immutable.receivedAtMs),
        record.immutable.threadHash,
      ],
    });
    if (Number(result) !== 1) throw new Error("inbox record could not be updated");
  }

  /**
   * Redis runs this as one transaction: it allocates the sequence, commits
   * both durable documents, and publishes the record to both queue indexes.
   * A worker that dies after the script commits can therefore be retried
   * without making a later same-thread record look like the queue head.
   */
  private async publishNewRecord(
    record: InboxRecord,
    now: number,
    rateLimitReservation?: RateLimitIngressReservation,
  ): Promise<InboxPublication> {
    if (record.immutable.threadSequence !== -1) {
      throw new Error("new inbox record must use the unpublished sequence marker");
    }
    const result = await this.redis.eval(PERSIST_INBOX_NEW_RECORD_LUA, {
      keys: [
        this.metadataRecordKey(record.immutable.recordKey),
        record.immutable.recordKey,
        this.pendingIndexKey(),
        this.threadQueueKey(record.immutable.threadHash),
        this.threadSequenceKey(record.immutable.threadHash),
        this.receivedAtIndexKey(),
        this.readyThreadsIndexKey(),
        ...(rateLimitReservation
          ? [
              rateLimitReservation.minuteKey,
              rateLimitReservation.dayKey,
              rateLimitReservation.minuteNoticeKey,
              rateLimitReservation.dayNoticeKey,
              rateLimitReservation.decisionKey,
              rateLimitReservation.receiptWatermarkKey,
            ]
          : []),
      ],
      arguments: [
        JSON.stringify(redactInboxRecord(record)),
        String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        JSON.stringify(record),
        String(ttlSeconds(record.immutable.payloadExpiresAtMs, now)),
        String(record.immutable.receivedAtMs),
        record.immutable.recordKey,
        String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        String(record.immutable.receivedAtMs),
        record.immutable.threadHash,
        ...(rateLimitReservation
          ? [
              "1",
              String(rateLimitReservation.minuteWindowMs),
              String(rateLimitReservation.dayWindowMs),
              String(rateLimitReservation.perMinute),
              String(rateLimitReservation.perRollingDay),
              // A decision is needed until this encrypted payload can no
              // longer be processed. A custom inbox retention must never
              // let a queued rejected receipt become newly eligible later.
              String(Math.max(rateLimitReservation.decisionTtlMs, this.payloadRetentionMs)),
              rateLimitReservation.correlationId,
            ]
          : ["0", "0", "0", "0", "0", "0", ""]),
      ],
    });
    if (!Array.isArray(result) || result.length < 2) {
      throw new Error("inbox publish script returned an invalid response");
    }
    const created = Number(result[0]);
    if (created === 0) return Object.freeze({ created: false });
    const threadSequence = Number(result[1]);
    if (created !== 1 || !Number.isSafeInteger(threadSequence) || threadSequence < 1) {
      throw new Error("inbox publish script returned an invalid response");
    }
    return Object.freeze({ created: true, threadSequence });
  }

  private async acquireRecordLease(
    recordKey: string,
    waitForContentionMs = 0,
  ): Promise<RedisLease | null> {
    const deadline = Date.now() + waitForContentionMs;
    for (;;) {
      const lease = await acquireRedisLease(this.redis, this.recordLeaseKey(recordKey), {
        ttlMs: 15_000,
        now: this.clock,
      });
      if (lease || Date.now() >= deadline) return lease;
      await new Promise<void>((resolve) => setTimeout(resolve, RECORD_LEASE_RETRY_INTERVAL_MS));
    }
  }

  private async removeFromIndexes(record: InboxRecord): Promise<void> {
    await Promise.all([
      this.redis.zRem(this.pendingIndexKey(), record.immutable.recordKey),
      this.redis.zRem(this.receivedAtIndexKey(), record.immutable.recordKey),
      this.redis.zRem(this.threadQueueKey(record.immutable.threadHash), record.immutable.recordKey),
    ]);
    await this.reconcileReadyThread(record.immutable.threadHash);
  }

  private async removeUnknownRecordFromIndexes(recordKey: string): Promise<void> {
    await Promise.all([
      this.redis.zRem(this.pendingIndexKey(), recordKey),
      this.redis.zRem(this.receivedAtIndexKey(), recordKey),
    ]);
  }

  private async readOldestReceivedAtRecord(): Promise<InboxRecord | null> {
    for (let repair = 0; repair < QUEUE_METRICS_STALE_INDEX_REPAIRS; repair += 1) {
      const [recordKey] = await this.redis.zRangeByScore(
        this.receivedAtIndexKey(),
        Number.NEGATIVE_INFINITY,
        Number.POSITIVE_INFINITY,
        { limit: 1 },
      );
      if (!recordKey) return null;

      const record = await this.getRecord(recordKey);
      if (record && !TERMINAL_STATES.has(record.state)) return record;
      if (record) {
        await this.removeFromIndexes(record);
      } else {
        await this.removeUnknownRecordFromIndexes(recordKey);
      }
    }
    return null;
  }

  private async getLegacyQueueMetrics(depth: number): Promise<InboxQueueMetrics> {
    if (depth === 0) return Object.freeze({ depth: 0, oldestInboxAgeMs: 0 });
    const scanLimit = Math.min(depth, QUEUE_METRICS_FALLBACK_SCAN_LIMIT);
    const recordKeys = await this.redis.zRangeByScore(
      this.pendingIndexKey(),
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
      { limit: scanLimit },
    );
    let oldestReceivedAtMs: number | undefined;
    for (const recordKey of recordKeys) {
      const record = await this.getRecord(recordKey);
      if (record && !TERMINAL_STATES.has(record.state)) {
        oldestReceivedAtMs = oldestReceivedAtMs === undefined
          ? record.immutable.receivedAtMs
          : Math.min(oldestReceivedAtMs, record.immutable.receivedAtMs);
      } else if (record) {
        await this.removeFromIndexes(record);
      } else {
        await this.removeUnknownRecordFromIndexes(recordKey);
      }
    }

    const reconciledDepth = await this.redis.zCard(this.pendingIndexKey());
    if (reconciledDepth === 0) return Object.freeze({ depth: 0, oldestInboxAgeMs: 0 });
    if (
      reconciledDepth > scanLimit ||
      recordKeys.length < reconciledDepth ||
      oldestReceivedAtMs === undefined
    ) {
      return Object.freeze({ depth: reconciledDepth });
    }
    return Object.freeze({
      depth: reconciledDepth,
      oldestInboxAgeMs: Math.max(0, this.clock() - oldestReceivedAtMs),
    });
  }

  private async nextThreadQueueHead(threadHash: ThreadHash): Promise<string | null> {
    return (await this.readThreadQueueHead(threadHash)).recordKey;
  }

  /**
   * Cleans stale queue members from the prefix without scanning a busy live
   * thread's tail. If a legacy/corrupt queue needs more repair, callers leave
   * its ready-thread member in place for the next bounded scheduler pass.
   */
  private async readThreadQueueHead(threadHash: ThreadHash): Promise<ThreadQueueHeadLookup> {
    const queueKey = this.threadQueueKey(threadHash);
    let repaired = 0;
    while (repaired < THREAD_HEAD_STALE_REPAIR_LIMIT) {
      const candidates = await this.redis.zRangeByScore(
        queueKey,
        Number.NEGATIVE_INFINITY,
        Number.POSITIVE_INFINITY,
        { limit: Math.min(THREAD_HEAD_STALE_REPAIR_PAGE_SIZE, THREAD_HEAD_STALE_REPAIR_LIMIT - repaired) },
      );
      if (candidates.length === 0) {
        return Object.freeze({ recordKey: null, repairIncomplete: false });
      }
      for (const key of candidates) {
        const record = await this.getRecord(key);
        if (!record || TERMINAL_STATES.has(record.state)) {
          await this.redis.zRem(queueKey, key);
          repaired += 1;
          continue;
        }
        return Object.freeze({ recordKey: key, repairIncomplete: false });
      }
      // Every member in this prefix was stale and has been removed. Fetch the
      // next prefix at offset zero so removal cannot skip the live head.
    }
    const [remaining] = await this.redis.zRangeByScore(
      queueKey,
      Number.NEGATIVE_INFINITY,
      Number.POSITIVE_INFINITY,
      { limit: 1 },
    );
    return Object.freeze({ recordKey: null, repairIncomplete: remaining !== undefined });
  }

  /**
   * Best-effort repair for legacy or manually-corrupted indexes. Normal queue
   * publication and state transitions do this atomically in Redis Lua.
   */
  private async reconcileReadyThread(threadHash: ThreadHash): Promise<void> {
    for (let remaining = 0; remaining < 32; remaining += 1) {
      const head = await this.readThreadQueueHead(threadHash);
      if (head.repairIncomplete) return;
      if (!head.recordKey) {
        await this.redis.zRem(this.readyThreadsIndexKey(), threadHash);
        return;
      }
      const record = await this.getRecord(head.recordKey);
      if (!record || TERMINAL_STATES.has(record.state)) continue;
      const score = inboxReadyThreadScore(record);
      if (score === undefined) {
        await this.redis.zRem(this.readyThreadsIndexKey(), threadHash);
      } else {
        await this.redis.zAdd(this.readyThreadsIndexKey(), [{ score, value: threadHash }]);
      }
      return;
    }
    await this.redis.zRem(this.readyThreadsIndexKey(), threadHash);
  }

  /**
   * This release intentionally has no automatic global-pending fallback: an
   * offset scan can again let one thread's tail hide another thread forever.
   * Existing queues must be drained or backfilled while quiescent before this
   * ready-head index is enabled. The double read avoids falsely flagging an
   * atomic ingress publication that races this scheduler invocation.
   */
  private async assertReadyThreadIndexPresent(): Promise<void> {
    if (await this.redis.zCard(this.pendingIndexKey()) === 0) return;
    if (await this.redis.zCard(this.readyThreadsIndexKey()) > 0) return;
    const [pendingDepth, readyThreadDepth] = await Promise.all([
      this.redis.zCard(this.pendingIndexKey()),
      this.redis.zCard(this.readyThreadsIndexKey()),
    ]);
    if (pendingDepth > 0 && readyThreadDepth === 0) {
      throw new Error(READY_THREADS_BACKFILL_REQUIRED_ERROR);
    }
  }

  private pendingIndexKey(): string {
    return `${this.prefix}:pending`;
  }

  private readyThreadsIndexKey(): string {
    return `${this.prefix}:ready-threads`;
  }

  private receivedAtIndexKey(): string {
    return `${this.prefix}:pending:received-at`;
  }

  private metadataRecordKey(recordKey: string): string {
    return `${recordKey}:metadata`;
  }

  private threadQueueKey(threadHash: ThreadHash): string {
    assertPseudonymousHash(threadHash, "thread hash");
    return `${this.prefix}:thread:${threadHash}:queue`;
  }

  private threadLeaseKey(threadHash: ThreadHash): string {
    assertPseudonymousHash(threadHash, "thread hash");
    return `${this.prefix}:lease:thread:${threadHash}`;
  }

  private threadSequenceKey(threadHash: ThreadHash): string {
    assertPseudonymousHash(threadHash, "thread hash");
    return `${this.prefix}:thread:${threadHash}:sequence`;
  }

  private recordLeaseKey(recordKey: string): string {
    return `${this.prefix}:lease:record:${deriveOpaqueId(this.identityHashSecret, "inbox-record", recordKey)}`;
  }

  private attemptHash(attemptToken: string): string {
    if (!isAttemptToken(attemptToken)) throw new Error("inbox attempt token is invalid");
    return deriveOpaqueId(this.identityHashSecret, "inbox-attempt", attemptToken);
  }

  private attemptThreadLeaseToken(attemptToken: string): string {
    if (!isAttemptToken(attemptToken)) throw new Error("inbox attempt token is invalid");
    return deriveOpaqueId(this.identityHashSecret, "inbox-thread-lease", attemptToken);
  }

  private async releaseThreadLease(threadHash: ThreadHash, attemptToken: string): Promise<boolean> {
    const released = await this.redis.eval(COMPARE_AND_DELETE_LUA, {
      keys: [this.threadLeaseKey(threadHash)],
      arguments: [this.attemptThreadLeaseToken(attemptToken)],
    });
    return Number(released) === 1;
  }

  private hasExpectedAttemptOwnership(record: InboxRecord, expectedToken: string | undefined): boolean {
    const activeHash = record.status.processingAttemptHash;
    if (activeHash === undefined) return expectedToken === undefined;
    return isAttemptToken(expectedToken) && this.attemptHash(expectedToken) === activeHash;
  }
}

function makeTransitionRecord(
  current: InboxRecord,
  targetState: InboxState,
  now: number,
  options: InternalInboxTransitionOptions,
): InboxRecord {
  if (targetState === "failed_retryable" && options.nextAttemptAtMs === undefined) {
    throw new Error("retryable inbox state requires a next attempt time");
  }
  if (targetState === "processing" && !isPseudonymousHash(options.processingAttemptHash)) {
    throw new Error("processing inbox state requires an attempt hash");
  }
  const terminal = TERMINAL_STATES.has(targetState);
  const clearPayload = targetState === "sent" || targetState === "handed_off" || targetState === "dead_letter";
  const processingLeaseExpiresAtMs = targetState === "processing"
    ? options.processingLeaseExpiresAtMs
    : terminal || targetState === "failed_retryable"
      ? undefined
      : current.status.processingLeaseExpiresAtMs;
  const processingAttemptHash = targetState === "processing"
    ? options.processingAttemptHash
    : terminal || targetState === "failed_retryable"
      ? undefined
      : current.status.processingAttemptHash;
  const failureCategory = options.failureCategory
    ? sanitizeFailureCategory(options.failureCategory)
    : targetState === "failed_retryable" || targetState === "dead_letter" || targetState === "send_unknown"
      ? current.status.failureCategory
      : undefined;
  return freezeInboxRecord({
    ...current,
    state: targetState,
    stateVersion: current.stateVersion + 1,
    encryptedEnvelope: clearPayload ? undefined : current.encryptedEnvelope,
    status: {
      attempts: targetState === "processing" ? current.status.attempts + 1 : current.status.attempts,
      updatedAtMs: now,
      nextAttemptAtMs: targetState === "failed_retryable" ? options.nextAttemptAtMs : undefined,
      processingLeaseExpiresAtMs,
      processingAttemptHash,
      failureCategory,
    },
  });
}

function makePayloadExpiredRecord(current: InboxRecord, now: number, category: string): InboxRecord {
  if (TERMINAL_STATES.has(current.state) && !current.encryptedEnvelope) return current;
  return freezeInboxRecord({
    ...current,
    state: "dead_letter",
    stateVersion: current.stateVersion + 1,
    encryptedEnvelope: undefined,
    status: {
      attempts: current.status.attempts,
      updatedAtMs: now,
      processingAttemptHash: undefined,
      failureCategory: sanitizeFailureCategory(category),
    },
  });
}

function isDispatchable(record: InboxRecord, now: number): boolean {
  if (record.state === "received") return true;
  return record.state === "failed_retryable" && (record.status.nextAttemptAtMs ?? Number.POSITIVE_INFINITY) <= now;
}

function inboxIndexIntent(record: InboxRecord): {
  readonly mode: "queued" | "terminal";
  readonly pendingScore: number;
} {
  if (TERMINAL_STATES.has(record.state)) {
    return Object.freeze({ mode: "terminal", pendingScore: 0 });
  }
  return Object.freeze({
    mode: "queued",
    pendingScore: record.state === "failed_retryable"
      ? (record.status.nextAttemptAtMs ?? record.status.updatedAtMs)
      : record.immutable.receivedAtMs,
  });
}

/**
 * The ready-thread index carries one current head per thread. Active heads
 * stay scheduled at their durable lease expiry so a crashed worker is still
 * recoverable without scanning every later record in that thread.
 */
function inboxReadyThreadScore(record: InboxRecord): number | undefined {
  if (record.state === "received" || record.state === "failed_retryable") {
    return inboxIndexIntent(record).pendingScore;
  }
  const leaseExpiry = record.status.processingLeaseExpiresAtMs;
  return typeof leaseExpiry === "number" && Number.isSafeInteger(leaseExpiry) && leaseExpiry >= 0
    ? leaseExpiry
    : undefined;
}

function compareDueInboxRecords(first: InboxRecord, second: InboxRecord): number {
  const firstScore = inboxReadyThreadScore(first) ?? Number.POSITIVE_INFINITY;
  const secondScore = inboxReadyThreadScore(second) ?? Number.POSITIVE_INFINITY;
  if (firstScore !== secondScore) return firstScore - secondScore;
  if (first.immutable.recordKey < second.immutable.recordKey) return -1;
  if (first.immutable.recordKey > second.immutable.recordKey) return 1;
  return 0;
}

function createAttemptToken(): string {
  return randomBytes(24).toString("base64url");
}

function isAttemptToken(value: string | undefined): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_-]{32,128}$/.test(value);
}

/** Active records are writable only by the matching durable attempt owner. */
function isPseudonymousHash(value: string | undefined): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}

function ttlSeconds(expiresAtMs: number, now: number): number {
  const remaining = Math.ceil((expiresAtMs - now) / 1_000);
  if (!Number.isInteger(remaining) || remaining < 1) throw new Error("inbox metadata retention has expired");
  return remaining;
}

function validateVerifiedEnvelope(input: VerifiedInboxInput): void {
  const envelope = input.envelope;
  if (envelope.projectId !== input.projectId || envelope.messageId !== input.messageId) {
    throw new Error("verified envelope identifiers do not match inbox identifiers");
  }
  if (envelope.direction !== "inbound") throw new Error("only inbound deliveries enter the durable inbox");
  if (envelope.isGroup !== input.isGroup || envelope.content.type !== input.contentType) {
    throw new Error("verified envelope metadata does not match inbox metadata");
  }
  if (envelope.content.text !== null && typeof envelope.content.text !== "string") {
    throw new Error("verified envelope text is invalid");
  }
  const attachments = envelope.content.attachments;
  if (attachments !== undefined) {
    if (!Array.isArray(attachments) || attachments.length > 16) {
      throw new Error("verified envelope attachments are invalid");
    }
    for (const attachment of attachments) {
      if (
        !attachment ||
        typeof attachment.id !== "string" ||
        !attachment.id ||
        attachment.id.length > 4_096 ||
        (attachment.name !== null && typeof attachment.name !== "string") ||
        (attachment.mimeType !== null && typeof attachment.mimeType !== "string") ||
        (attachment.size !== null && (!Number.isSafeInteger(attachment.size) || attachment.size < 0))
      ) {
        throw new Error("verified envelope attachments are invalid");
      }
    }
  }
}

/**
 * The ingress runtime builds these fields solely from HMACs and fixed policy
 * constants. Validate them at the durable boundary so an accidental caller
 * cannot turn a generic inbox publication into a write against arbitrary
 * Redis keys or reserve a different correlation.
 */
function validateRateLimitReservation(
  value: RateLimitIngressReservation | undefined,
  correlationId: CorrelationId,
  senderHash: SenderHash | string,
  rateLimitPrefix: string,
): RateLimitIngressReservation | undefined {
  if (!value) return undefined;
  assertPseudonymousHash(value.correlationId, "rate reservation correlation id");
  if (value.correlationId !== correlationId) {
    throw new Error("rate reservation correlation does not match inbox record");
  }
  const base = `${rateLimitPrefix}:${senderHash}`;
  const expectedKeys = {
    minuteKey: `${base}:minute`,
    dayKey: `${base}:day`,
    minuteNoticeKey: `${base}:notice:minute`,
    dayNoticeKey: `${base}:notice:day`,
    decisionKey: `${base}:reservation:${correlationId}`,
    receiptWatermarkKey: `${base}:receipt-watermark`,
  } as const;
  for (const [field, expected] of Object.entries(expectedKeys)) {
    if (value[field as keyof typeof expectedKeys] !== expected) {
      throw new Error("rate reservation keys do not match the inbox rate namespace");
    }
  }
  for (const number of [
    value.minuteWindowMs,
    value.dayWindowMs,
    value.perMinute,
    value.perRollingDay,
    value.decisionTtlMs,
  ]) {
    if (!Number.isSafeInteger(number) || number < 1) {
      throw new Error("rate reservation policy is invalid");
    }
  }
  if (value.decisionTtlMs < value.minuteWindowMs || value.decisionTtlMs < value.dayWindowMs) {
    throw new Error("rate reservation retention is invalid");
  }
  return Object.freeze({ ...value });
}

function assertProviderIdentifier(value: string, label: string): void {
  if (!value?.trim() || value.length > 512 || value.includes("\u0000")) {
    throw new Error(`${label} is invalid`);
  }
}

function sanitizeFailureCategory(value: string): string {
  return /^[a-z0-9_.-]{1,80}$/i.test(value) ? value.toLowerCase() : "unknown_failure";
}

function parseInboxRecord(serialized: string): InboxRecord {
  try {
    const parsed = JSON.parse(serialized) as InboxRecord;
    if (
      parsed.schemaVersion !== 1 ||
      !parsed.immutable ||
      !parsed.delivery ||
      !parsed.status ||
      !INBOX_STATES.includes(parsed.state) ||
      typeof parsed.immutable.recordKey !== "string" ||
      !Number.isSafeInteger(parsed.immutable.threadSequence) ||
      parsed.immutable.threadSequence < 1 ||
      typeof parsed.immutable.metadataExpiresAtMs !== "number" ||
      (parsed.status.processingAttemptHash !== undefined &&
        !isPseudonymousHash(parsed.status.processingAttemptHash))
    ) {
      throw new Error("invalid inbox record");
    }
    return freezeInboxRecord(parsed);
  } catch {
    throw new Error("durable inbox record is corrupt");
  }
}

/** Replace the never-persisted Lua sequence marker in a newly published record. */
function withThreadSequence(record: InboxRecord, threadSequence: number): InboxRecord {
  if (
    record.immutable.threadSequence !== -1 ||
    !Number.isSafeInteger(threadSequence) ||
    threadSequence < 1
  ) {
    throw new Error("inbox thread sequence is invalid");
  }
  return freezeInboxRecord({
    ...record,
    immutable: { ...record.immutable, threadSequence },
  });
}

function freezeInboxRecord(record: InboxRecord): InboxRecord {
  const payload = record.encryptedEnvelope
    ? Object.freeze({ ...record.encryptedEnvelope })
    : undefined;
  return Object.freeze({
    ...record,
    immutable: Object.freeze({ ...record.immutable }),
    delivery: Object.freeze({
      ...record.delivery,
      webhookIdHashes: Object.freeze([...record.delivery.webhookIdHashes]),
    }),
    status: Object.freeze({ ...record.status }),
    encryptedEnvelope: payload,
  });
}

/** The seven-day idempotency/status record must never contain ciphertext. */
function redactInboxRecord(record: InboxRecord): InboxRecord {
  return freezeInboxRecord({ ...record, encryptedEnvelope: undefined });
}
