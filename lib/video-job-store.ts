import type { EncryptedPayload, QueueCipher } from "./crypto.js";
import {
  acquireRedisLease,
  assertPseudonymousHash,
  type RedisLease,
  type RedisLike,
} from "./redis.js";
import { deriveOpaqueId, type CorrelationId } from "./security.js";

/**
 * Air's local projection of a Zap video run.  Redis metadata stays redacted;
 * prompt text, uploaded-image URL, Zap run ID, and output URL live only in the
 * AES-GCM encrypted payload key.
 */
export const VIDEO_JOB_STATES = Object.freeze([
  "submission_pending",
  "submission_unknown",
  "queued",
  "running",
  "video_ready",
  "sending",
  "send_unknown",
  "retryable",
  "dead_letter",
  "delivered",
] as const);

export type VideoJobState = (typeof VIDEO_JOB_STATES)[number];

export interface VideoJobImmutableMetadata {
  readonly recordKey: string;
  /** HMAC correlation id derived from the inbound Photon event; never a provider ID. */
  readonly inboundEventHash: CorrelationId;
  /** HMAC-only sender identity used for the one-active-video-job fence. */
  readonly senderHash: string;
  /** HMAC of normalized prompt + opaque first-frame identity for duplicate checks. */
  readonly requestDigest: string;
  readonly createdAtMs: number;
  readonly payloadExpiresAtMs: number;
  readonly metadataExpiresAtMs: number;
}

export interface VideoJobStatusMetadata {
  readonly attempts: number;
  readonly updatedAtMs: number;
  /** Next safe submission/poll attempt. Only automatic states carry this value. */
  readonly nextAttemptAtMs?: number | undefined;
  /** HMAC only; the raw Zap run ID is encrypted in the payload. */
  readonly zapRunIdHash?: string | undefined;
  /** HMAC only; the raw video URL is encrypted in the payload. */
  readonly videoUrlHash?: string | undefined;
  readonly progress?: number | undefined;
  /** Fixed safe category, never provider response text. */
  readonly failureCategory?: string | undefined;
  /** Persisted before the static acknowledgement is attempted. */
  readonly acknowledgementAttemptedAtMs?: number | undefined;
}

/** Redacted record safe for indexes and operator metadata. */
export interface VideoJobRecord {
  readonly schemaVersion: 1;
  readonly immutable: VideoJobImmutableMetadata;
  readonly state: VideoJobState;
  readonly stateVersion: number;
  readonly status: VideoJobStatusMetadata;
}

/** Sensitive data encrypted before every Redis write. Never log this object. */
export interface VideoJobPayload {
  readonly schemaVersion: 1;
  readonly prompt: string;
  readonly firstFrameUrl?: string | undefined;
  readonly zapRunId?: string | undefined;
  readonly video?: VideoJobVideo | undefined;
  /** Raw iMessage addressing remains inside the encrypted payload. */
  readonly delivery: VideoJobDeliveryTarget;
}

export interface VideoJobDeliveryTarget {
  readonly chatGuid: string;
  readonly phone?: string | undefined;
}

export interface VideoJobVideo {
  readonly url: string;
  readonly mimeType: "video/mp4";
  readonly expiresAt?: string | undefined;
}

export interface CreateVideoJobInput {
  /** Opaque HMAC of the Photon project/message id pair. */
  readonly correlationId: CorrelationId | string;
  /** Opaque HMAC sender identity. A sender can have only one active generation. */
  readonly senderHash: string;
  readonly prompt: string;
  readonly firstFrameUrl?: string | undefined;
  /** Stable HMAC attachment identity; never the ephemeral signed/public URL. */
  readonly firstFrameIdentity?: string | undefined;
  readonly delivery: VideoJobDeliveryTarget;
  readonly createdAtMs?: number | undefined;
}

export interface PersistedVideoJob {
  readonly created: boolean;
  readonly record: VideoJobRecord;
}

export interface VideoJobTransitionInput {
  readonly expectedStates: readonly VideoJobState[];
  readonly state: VideoJobState;
  readonly nextAttemptAtMs?: number | undefined;
  readonly failureCategory?: string | undefined;
  readonly progress?: number | undefined;
  /** Raw value remains encrypted; metadata receives an HMAC only. */
  readonly zapRunId?: string | undefined;
  /** Raw value remains encrypted; metadata receives an HMAC only. */
  readonly video?: VideoJobVideo | undefined;
  /** Acknowledgements are never retried after a transport ambiguity. */
  readonly acknowledgementAttempted?: boolean | undefined;
}

export interface VideoJobAttempt {
  readonly record: VideoJobRecord;
  readonly payload: VideoJobPayload | null;
  /** A submission intent is durably marked unknown before the paid Zap call. */
  readonly operation: "submit" | "poll" | "send";
  /** Always reuse this opaque inbound-event HMAC for Zap's idempotency header. */
  readonly idempotencyKey: string;
  heartbeat(): Promise<boolean>;
  transition(input: VideoJobTransitionInput): Promise<VideoJobRecord | null>;
  release(): Promise<boolean>;
}

export interface VideoJobStoreOptions {
  readonly redis: RedisLike;
  readonly cipher: QueueCipher;
  readonly identityHashSecret: string;
  readonly prefix?: string | undefined;
  readonly clock?: (() => number) | undefined;
  readonly payloadRetentionMs?: number | undefined;
  readonly metadataRetentionMs?: number | undefined;
  readonly workLeaseTtlMs?: number | undefined;
}

export type VideoJobStoreErrorCode =
  | "invalid_input"
  | "busy"
  | "sender_busy"
  | "conflict"
  | "corrupt_record"
  | "invalid_transition";

/** No sensitive payload is ever included in this error's message or fields. */
export class VideoJobStoreError extends Error {
  public constructor(readonly code: VideoJobStoreErrorCode) {
    super(`video_job_${code}`);
    this.name = "VideoJobStoreError";
  }
}

const DEFAULT_PREFIX = "air:video:v1";
const DEFAULT_PAYLOAD_RETENTION_MS = 86_400_000;
const DEFAULT_METADATA_RETENTION_MS = 604_800_000;
const DEFAULT_WORK_LEASE_TTL_MS = 55_000;
const MAX_PROMPT_CHARS = 2_000;
const MAX_URL_CHARS = 8_192;
const MAX_RUN_ID_CHARS = 512;
const MAX_FAILURE_CATEGORY_CHARS = 120;
const MAX_ATTEMPTS = 100;
const HASH_PATTERN = /^[a-f0-9]{64}$/;
const RUN_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$/;
const FAILURE_CATEGORY_PATTERN = /^[a-z0-9][a-z0-9_:-]{0,119}$/;
const AUTOMATIC_STATES = new Set<VideoJobState>([
  "submission_pending",
  "queued",
  "running",
  "video_ready",
  "retryable",
]);

/** Generation occupies a sender slot; delivery does not. */
const GENERATION_ACTIVE_STATES = new Set<VideoJobState>([
  "submission_pending",
  "submission_unknown",
  "queued",
  "running",
  "retryable",
]);

const TRANSITIONS: Readonly<Record<VideoJobState, readonly VideoJobState[]>> = Object.freeze({
  submission_pending: ["queued", "running", "retryable", "submission_unknown", "dead_letter"],
  // Never picked up automatically. An operator or an idempotent Zap replay can
  // reconcile the durable intent once the provider-side result is known.
  submission_unknown: ["queued", "running", "video_ready", "retryable", "dead_letter"],
  queued: ["queued", "running", "video_ready", "retryable", "dead_letter"],
  running: ["running", "video_ready", "retryable", "dead_letter"],
  // A delivery worker first durably enters `sending`; an uncertain Photon
  // result must go to `send_unknown`, never back to automatic delivery.
  video_ready: ["sending", "dead_letter"],
  sending: ["video_ready", "send_unknown", "delivered", "dead_letter"],
  send_unknown: ["delivered", "dead_letter"],
  retryable: ["submission_pending", "queued", "running", "video_ready", "retryable", "submission_unknown", "dead_letter"],
  dead_letter: [],
  delivered: [],
});

/**
 * Atomically publish metadata, encrypted payload, and the recovery index.
 * A duplicate result has an unambiguous zero result; a dangling payload is a
 * corruption signal rather than permission to overwrite a prior event.
 */
export const PERSIST_VIDEO_JOB_LUA = `
-- AIR_PERSIST_VIDEO_JOB_V1
if #KEYS ~= 4 or #ARGV ~= 7 then
  return redis.error_reply("invalid video job publish arguments")
end
local function positiveInteger(value)
  return value and string.match(value, "^[1-9][0-9]*$") and string.len(value) <= 16
end
local function nonnegativeInteger(value)
  return value == "0" or (value and string.match(value, "^[1-9][0-9]*$") and string.len(value) <= 16)
end
if redis.call("TYPE", KEYS[1]).ok ~= "none" and redis.call("TYPE", KEYS[1]).ok ~= "string" then
  return redis.error_reply("invalid video metadata key type")
end
if redis.call("TYPE", KEYS[2]).ok ~= "none" and redis.call("TYPE", KEYS[2]).ok ~= "string" then
  return redis.error_reply("invalid video payload key type")
end
if redis.call("TYPE", KEYS[3]).ok ~= "none" and redis.call("TYPE", KEYS[3]).ok ~= "zset" then
  return redis.error_reply("invalid video due key type")
end
if redis.call("TYPE", KEYS[4]).ok ~= "none" and redis.call("TYPE", KEYS[4]).ok ~= "string" then
  return redis.error_reply("invalid video active sender key type")
end
if not positiveInteger(ARGV[2]) or not positiveInteger(ARGV[4]) or not nonnegativeInteger(ARGV[5]) or not ARGV[6] or not positiveInteger(ARGV[7]) then
  return redis.error_reply("invalid video job publish arguments")
end
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return -1
end
if redis.call("EXISTS", KEYS[4]) == 1 then
  return -2
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
redis.call("SET", KEYS[2], ARGV[3], "EX", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
redis.call("SET", KEYS[4], ARGV[6], "EX", ARGV[7])
return 1
`;

/** Atomically replaces a redacted record, its encrypted payload, and due membership. */
export const WRITE_VIDEO_JOB_LUA = `
-- AIR_WRITE_VIDEO_JOB_V1
if #KEYS ~= 6 or #ARGV ~= 13 then
  return redis.error_reply("invalid video job write arguments")
end
local function positiveInteger(value)
  return value and string.match(value, "^[1-9][0-9]*$") and string.len(value) <= 16
end
local function nonnegativeInteger(value)
  return value == "0" or (value and string.match(value, "^[1-9][0-9]*$") and string.len(value) <= 16)
end
if not positiveInteger(ARGV[9]) or not ARGV[10] or #ARGV[10] < 32 or #ARGV[10] > 128 or not string.match(ARGV[10], "^[A-Za-z0-9_-]+$") then
  return redis.error_reply("invalid video job write fence")
end
-- Fence every metadata/payload/index mutation with the distributed lease.
-- A paused worker that loses its lease can therefore never overwrite a newer
-- worker's state after it resumes.
if redis.call("GET", KEYS[4]) ~= ARGV[10] then
  return -2
end
if redis.call("TYPE", KEYS[1]).ok ~= "string" then
  return 0
end
local currentRaw = redis.call("GET", KEYS[1])
if not currentRaw then
  return 0
end
local decoded, current = pcall(cjson.decode, currentRaw)
if not decoded or type(current) ~= "table" or type(current.stateVersion) ~= "number" then
  return redis.error_reply("invalid stored video metadata")
end
if current.stateVersion ~= tonumber(ARGV[9]) then
  return -3
end
if redis.call("TYPE", KEYS[2]).ok ~= "none" and redis.call("TYPE", KEYS[2]).ok ~= "string" then
  return redis.error_reply("invalid video payload key type")
end
if redis.call("TYPE", KEYS[3]).ok ~= "none" and redis.call("TYPE", KEYS[3]).ok ~= "zset" then
  return redis.error_reply("invalid video due key type")
end
if redis.call("TYPE", KEYS[5]).ok ~= "none" and redis.call("TYPE", KEYS[5]).ok ~= "string" then
  return redis.error_reply("invalid video active sender key type")
end
if redis.call("TYPE", KEYS[6]).ok ~= "none" and redis.call("TYPE", KEYS[6]).ok ~= "zset" then
  return redis.error_reply("invalid video manual-review key type")
end
if not positiveInteger(ARGV[2]) or (ARGV[5] ~= "0" and ARGV[5] ~= "1") or (ARGV[6] ~= "due" and ARGV[6] ~= "terminal") or not nonnegativeInteger(ARGV[7]) or not ARGV[8] or (ARGV[11] ~= "retain" and ARGV[11] ~= "release") or not positiveInteger(ARGV[12]) or (ARGV[13] ~= "manual" and ARGV[13] ~= "remove") then
  return redis.error_reply("invalid video job write arguments")
end
if ARGV[5] == "1" and not positiveInteger(ARGV[4]) then
  return redis.error_reply("invalid video payload TTL")
end
local activeRecord = redis.call("GET", KEYS[5])
if ARGV[11] == "retain" and activeRecord and activeRecord ~= ARGV[8] then
  return -4
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
if ARGV[5] == "1" then
  redis.call("SET", KEYS[2], ARGV[3], "EX", ARGV[4])
else
  redis.call("DEL", KEYS[2])
end
if ARGV[6] == "due" then
  redis.call("ZADD", KEYS[3], ARGV[7], ARGV[8])
else
  redis.call("ZREM", KEYS[3], ARGV[8])
end
if ARGV[13] == "manual" then
  redis.call("ZADD", KEYS[6], ARGV[7], ARGV[8])
else
  redis.call("ZREM", KEYS[6], ARGV[8])
end
if ARGV[11] == "release" and redis.call("GET", KEYS[5]) == ARGV[8] then
  redis.call("DEL", KEYS[5])
elseif ARGV[11] == "retain" then
  redis.call("SET", KEYS[5], ARGV[8], "EX", ARGV[12])
end
return 1
`;

export function videoJobRecordKey(
  correlationId: CorrelationId | string,
  prefix = DEFAULT_PREFIX,
): string {
  assertPseudonymousHash(correlationId, "video correlation id");
  assertPrefix(prefix);
  return `${prefix}:record:${correlationId}`;
}

export function videoJobDueIndexKey(prefix = DEFAULT_PREFIX): string {
  assertPrefix(prefix);
  return `${prefix}:due`;
}

/** Metadata-only index for states that require deliberate operator reconciliation. */
export function videoJobManualReviewIndexKey(prefix = DEFAULT_PREFIX): string {
  assertPrefix(prefix);
  return `${prefix}:manual-review`;
}

/** Pseudonymous sender-scoped fence; it never contains a phone number or email. */
export function videoJobActiveSenderKey(senderHash: string, prefix = DEFAULT_PREFIX): string {
  assertPseudonymousHash(senderHash, "video sender hash");
  assertPrefix(prefix);
  return `${prefix}:sender:${senderHash}:active`;
}

export class VideoJobStore {
  private readonly redis: RedisLike;
  private readonly cipher: QueueCipher;
  private readonly identityHashSecret: string;
  private readonly prefix: string;
  private readonly clock: () => number;
  private readonly payloadRetentionMs: number;
  private readonly metadataRetentionMs: number;
  private readonly workLeaseTtlMs: number;

  public constructor(options: VideoJobStoreOptions) {
    if (!options.identityHashSecret?.trim()) throw new VideoJobStoreError("invalid_input");
    assertPrefix(options.prefix ?? DEFAULT_PREFIX);
    this.redis = options.redis;
    this.cipher = options.cipher;
    this.identityHashSecret = options.identityHashSecret;
    this.prefix = options.prefix ?? DEFAULT_PREFIX;
    this.clock = options.clock ?? Date.now;
    this.payloadRetentionMs = options.payloadRetentionMs ?? DEFAULT_PAYLOAD_RETENTION_MS;
    this.metadataRetentionMs = options.metadataRetentionMs ?? DEFAULT_METADATA_RETENTION_MS;
    this.workLeaseTtlMs = options.workLeaseTtlMs ?? DEFAULT_WORK_LEASE_TTL_MS;
    for (const value of [this.payloadRetentionMs, this.metadataRetentionMs, this.workLeaseTtlMs]) {
      if (!Number.isInteger(value) || value < 1) throw new VideoJobStoreError("invalid_input");
    }
    if (this.metadataRetentionMs < this.payloadRetentionMs || this.workLeaseTtlMs < 1_000) {
      throw new VideoJobStoreError("invalid_input");
    }
  }

  /** Create exactly one job per inbound correlation ID, or return its existing projection. */
  public async createOrGet(input: CreateVideoJobInput): Promise<PersistedVideoJob> {
    const prepared = this.prepareCreateInput(input);
    const lease = await acquireRedisLease(this.redis, this.leaseKey(prepared.recordKey), {
      ttlMs: this.workLeaseTtlMs,
      now: this.clock,
    });
    if (!lease) {
      const existing = await this.readRecord(prepared.recordKey);
      if (existing && existing.immutable.requestDigest === prepared.requestDigest) {
        return Object.freeze({ created: false, record: existing });
      }
      if (await this.redis.get(this.activeSenderKey(prepared.senderHash))) {
        throw new VideoJobStoreError("sender_busy");
      }
      throw new VideoJobStoreError("busy");
    }

    try {
      const existing = await this.readRecord(prepared.recordKey);
      if (existing) {
        if (existing.immutable.requestDigest !== prepared.requestDigest) {
          throw new VideoJobStoreError("conflict");
        }
        const repaired = await this.repairInitialPayloadIfSafe(existing, prepared.payload, lease);
        return Object.freeze({ created: false, record: repaired });
      }

      const now = prepared.createdAtMs;
      const record = freezeRecord({
        schemaVersion: 1,
        immutable: {
          recordKey: prepared.recordKey,
          inboundEventHash: prepared.correlationId as CorrelationId,
          senderHash: prepared.senderHash,
          requestDigest: prepared.requestDigest,
          createdAtMs: now,
          payloadExpiresAtMs: now + this.payloadRetentionMs,
          metadataExpiresAtMs: now + this.metadataRetentionMs,
        },
        state: "submission_pending",
        stateVersion: 1,
        status: { attempts: 0, updatedAtMs: now, nextAttemptAtMs: now },
      });
      const encryptedPayload = this.cipher.encrypt(prepared.payload, this.payloadKey(prepared.recordKey));
      const published = await this.redis.eval(PERSIST_VIDEO_JOB_LUA, {
        keys: [
          this.metadataKey(prepared.recordKey),
          this.payloadKey(prepared.recordKey),
          this.dueIndexKey(),
          this.activeSenderKey(prepared.senderHash),
        ],
        arguments: [
          JSON.stringify(record),
          String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
          JSON.stringify(encryptedPayload),
          String(ttlSeconds(record.immutable.payloadExpiresAtMs, now)),
          String(now),
          prepared.recordKey,
          String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        ],
      });
      if (Number(published) === 1) return Object.freeze({ created: true, record });
      if (Number(published) === -2) throw new VideoJobStoreError("sender_busy");

      const recovered = await this.readRecord(prepared.recordKey);
      if (!recovered) throw new VideoJobStoreError("corrupt_record");
      if (recovered.immutable.requestDigest !== prepared.requestDigest) {
        throw new VideoJobStoreError("conflict");
      }
      const repaired = await this.repairInitialPayloadIfSafe(recovered, prepared.payload, lease);
      return Object.freeze({ created: false, record: repaired });
    } finally {
      await lease.release();
    }
  }

  /** Returns only redacted metadata. */
  public async get(recordKey: string): Promise<VideoJobRecord | null> {
    return this.readRecord(this.assertRecordKey(recordKey));
  }

  /** Sensitive payload access is explicit so callers cannot accidentally log it with metadata. */
  public async getPayload(recordKey: string): Promise<VideoJobPayload | null> {
    const validated = this.assertRecordKey(recordKey);
    const record = await this.readRecord(validated);
    if (!record) return null;
    return this.readPayload(validated);
  }

  /** Lists only job metadata whose automatic submit, poll, or delivery time is due. */
  public async listDue(limit = 50): Promise<readonly VideoJobRecord[]> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new VideoJobStoreError("invalid_input");
    const now = this.clock();
    const keys = await this.redis.zRangeByScore(this.dueIndexKey(), 0, now, { limit });
    const due: VideoJobRecord[] = [];
    for (const recordKey of keys) {
      try {
        const record = await this.readRecord(recordKey);
        if (!record || !AUTOMATIC_STATES.has(record.state)) {
          await this.redis.zRem(this.dueIndexKey(), recordKey);
          continue;
        }
        const nextAttemptAtMs = record.status.nextAttemptAtMs ?? record.status.updatedAtMs;
        if (nextAttemptAtMs > now) {
          await this.redis.zAdd(this.dueIndexKey(), [{ score: nextAttemptAtMs, value: recordKey }]);
          continue;
        }
        due.push(record);
      } catch {
        // A malformed/stale index member cannot block unrelated work. The
        // source record remains untouched for manual forensic recovery.
        await this.redis.zRem(this.dueIndexKey(), recordKey).catch(() => undefined);
      }
    }
    return Object.freeze(due);
  }

  /**
   * Acquire exclusive ownership before an external side effect. Paid Zap
   * submissions and Photon media sends are first persisted as unknown intent;
   * a crash can therefore never turn into an automatic duplicate side effect.
   */
  public async beginWork(recordKey: string): Promise<VideoJobAttempt | null> {
    const validated = this.assertRecordKey(recordKey);
    const lease = await acquireRedisLease(this.redis, this.leaseKey(validated), {
      ttlMs: this.workLeaseTtlMs,
      now: this.clock,
    });
    if (!lease) return null;

    let retainLease = false;
    try {
      let record = await this.readRecord(validated);
      if (!record || !AUTOMATIC_STATES.has(record.state)) return null;
      const now = this.clock();
      const nextAttemptAtMs = record.status.nextAttemptAtMs ?? record.status.updatedAtMs;
      if (nextAttemptAtMs > now) return null;

      let payload = await this.readPayload(validated);
      if (!payload || record.immutable.payloadExpiresAtMs <= now) {
        await this.transitionUnderLease(lease, record, payload, {
          expectedStates: [record.state],
          state: "dead_letter",
          failureCategory: "payload_expired",
        });
        return null;
      }

      if (record.status.attempts >= MAX_ATTEMPTS) {
        await this.transitionUnderLease(lease, record, payload, {
          expectedStates: [record.state],
          state: "dead_letter",
          failureCategory: "attempts_exhausted",
        });
        return null;
      }

      let operation: VideoJobAttempt["operation"];
      const needsSubmissionIntent =
        record.state === "submission_pending" ||
        (record.state === "retryable" && payload.zapRunId === undefined);
      if (needsSubmissionIntent) {
        const intent = await this.transitionUnderLease(lease, record, payload, {
          expectedStates: [record.state],
          state: "submission_unknown",
          failureCategory: "submission_intent",
        });
        if (!intent) return null;
        record = intent;
        operation = "submit";
      } else if (record.state === "video_ready") {
        if (!payload.zapRunId || !payload.video) throw new VideoJobStoreError("corrupt_record");
        const intent = await this.transitionUnderLease(lease, record, payload, {
          expectedStates: ["video_ready"],
          state: "sending",
        });
        if (!intent) return null;
        record = intent;
        operation = "send";
      } else {
        if (!payload.zapRunId || payload.video) throw new VideoJobStoreError("corrupt_record");
        operation = "poll";
      }

      retainLease = true;
      return this.createAttempt(lease, record, payload, operation);
    } finally {
      if (!retainLease) await lease.release();
    }
  }

  /** Convenience for state changes made outside a long-lived worker lease. */
  public async transition(recordKey: string, input: VideoJobTransitionInput): Promise<VideoJobRecord | null> {
    const validated = this.assertRecordKey(recordKey);
    const lease = await acquireRedisLease(this.redis, this.leaseKey(validated), {
      ttlMs: this.workLeaseTtlMs,
      now: this.clock,
    });
    if (!lease) return null;
    try {
      const record = await this.readRecord(validated);
      if (!record) return null;
      const payload = await this.readPayload(validated);
      // Await inside the try so the lease remains held through the fenced Lua
      // write. Returning the promise directly would run `finally` first.
      return await this.transitionUnderLease(lease, record, payload, input);
    } finally {
      await lease.release();
    }
  }

  private createAttempt(
    lease: RedisLease,
    initialRecord: VideoJobRecord,
    initialPayload: VideoJobPayload,
    operation: VideoJobAttempt["operation"],
  ): VideoJobAttempt {
    let record = initialRecord;
    let payload: VideoJobPayload | null = initialPayload;
    let released = false;
    const release = async (): Promise<boolean> => {
      if (released) return false;
      released = true;
      return lease.release();
    };

    return Object.freeze({
      get record() {
        return record;
      },
      get payload() {
        return payload;
      },
      operation,
      get idempotencyKey() {
        return record.immutable.inboundEventHash;
      },
      heartbeat: () => lease.refresh(this.workLeaseTtlMs),
      transition: async (input: VideoJobTransitionInput): Promise<VideoJobRecord | null> => {
        if (released) return null;
        const transitioned = await this.transitionUnderLease(lease, record, payload, input);
        if (!transitioned) return null;
        record = transitioned;
        payload = input.state === "dead_letter" || input.state === "delivered"
          ? null
          : await this.readPayload(record.immutable.recordKey);
        return record;
      },
      release,
    });
  }

  private async transitionUnderLease(
    lease: RedisLease,
    current: VideoJobRecord,
    currentPayload: VideoJobPayload | null,
    input: VideoJobTransitionInput,
  ): Promise<VideoJobRecord | null> {
    if (!(await lease.refresh(this.workLeaseTtlMs))) return null;
    if (!input.expectedStates.includes(current.state) || !isVideoJobState(input.state)) {
      throw new VideoJobStoreError("invalid_transition");
    }
    if (input.state !== current.state && !TRANSITIONS[current.state].includes(input.state)) {
      throw new VideoJobStoreError("invalid_transition");
    }
    validateTransitionInput(input);

    const now = this.clock();
    let nextPayload = currentPayload;
    if (input.state !== "dead_letter" && input.state !== "delivered") {
      if (!nextPayload || current.immutable.payloadExpiresAtMs <= now) {
        throw new VideoJobStoreError("corrupt_record");
      }
      nextPayload = freezePayload({
        ...nextPayload,
        ...(input.zapRunId === undefined ? {} : { zapRunId: validateRunId(input.zapRunId) }),
        ...(input.video === undefined ? {} : { video: validateVideo(input.video) }),
      });
      if (requiresZapRunId(input.state) && !nextPayload.zapRunId) {
        throw new VideoJobStoreError("invalid_transition");
      }
      if (requiresVideoPayload(input.state) && !nextPayload.video) {
        throw new VideoJobStoreError("invalid_transition");
      }
    } else {
      nextPayload = null;
    }

    const automatic = AUTOMATIC_STATES.has(input.state);
    if (!automatic && input.nextAttemptAtMs !== undefined) {
      throw new VideoJobStoreError("invalid_transition");
    }
    const nextAttemptAtMs = automatic
      ? input.nextAttemptAtMs ?? now
      : undefined;
    if (nextAttemptAtMs !== undefined && (!Number.isSafeInteger(nextAttemptAtMs) || nextAttemptAtMs < now)) {
      throw new VideoJobStoreError("invalid_transition");
    }

    const failureCategory = input.failureCategory;
    const status: VideoJobStatusMetadata = {
      attempts: input.state === "retryable" ? Math.min(MAX_ATTEMPTS, current.status.attempts + 1) : current.status.attempts,
      updatedAtMs: now,
      ...(nextAttemptAtMs === undefined ? {} : { nextAttemptAtMs }),
      ...(nextPayload?.zapRunId === undefined
        ? {}
        : { zapRunIdHash: deriveOpaqueId(this.identityHashSecret, "zap-video-run", nextPayload.zapRunId) }),
      ...(nextPayload?.video === undefined
        ? {}
        : { videoUrlHash: deriveOpaqueId(this.identityHashSecret, "zap-video-output", nextPayload.video.url) }),
      ...(input.progress === undefined ? {} : { progress: input.progress }),
      ...(failureCategory === undefined ? {} : { failureCategory }),
      ...(input.acknowledgementAttempted
        ? { acknowledgementAttemptedAtMs: now }
        : current.status.acknowledgementAttemptedAtMs === undefined
          ? {}
          : { acknowledgementAttemptedAtMs: current.status.acknowledgementAttemptedAtMs }),
    };
    const next = freezeRecord({
      schemaVersion: 1,
      immutable: current.immutable,
      state: input.state,
      stateVersion: current.stateVersion + 1,
      status,
    });

    const written = await this.writeRecord(next, nextPayload, now, lease, current.stateVersion);
    return written ? next : null;
  }

  private async repairInitialPayloadIfSafe(
    record: VideoJobRecord,
    initialPayload: VideoJobPayload,
    lease: RedisLease,
  ): Promise<VideoJobRecord> {
    const existingPayload = await this.readPayload(record.immutable.recordKey);
    if (existingPayload) return record;
    const safeToRepair =
      (record.state === "submission_pending" || record.state === "retryable") &&
      record.status.zapRunIdHash === undefined &&
      record.immutable.payloadExpiresAtMs > this.clock();
    if (!safeToRepair) {
      const terminal = await this.transitionUnderLease(lease, record, null, {
        expectedStates: [record.state],
        state: "dead_letter",
        failureCategory: "payload_missing",
      });
      if (!terminal) throw new VideoJobStoreError("corrupt_record");
      return terminal;
    }
    const repaired = await this.writeRecord(record, initialPayload, this.clock(), lease, record.stateVersion);
    if (!repaired) throw new VideoJobStoreError("corrupt_record");
    return record;
  }

  private prepareCreateInput(input: CreateVideoJobInput): {
    readonly correlationId: string;
    readonly senderHash: string;
    readonly recordKey: string;
    readonly requestDigest: string;
    readonly payload: VideoJobPayload;
    readonly createdAtMs: number;
  } {
    assertPseudonymousHash(input.correlationId, "video correlation id");
    assertPseudonymousHash(input.senderHash, "video sender hash");
    const prompt = normalizePrompt(input.prompt);
    const firstFrameUrl = input.firstFrameUrl === undefined ? undefined : normalizeHttpsUrl(input.firstFrameUrl);
    const firstFrameIdentity = input.firstFrameIdentity === undefined ? undefined : input.firstFrameIdentity;
    if (firstFrameUrl !== undefined) {
      if (firstFrameIdentity === undefined) throw new VideoJobStoreError("invalid_input");
      assertPseudonymousHash(firstFrameIdentity, "video first-frame identity");
    } else if (firstFrameIdentity !== undefined) {
      throw new VideoJobStoreError("invalid_input");
    }
    const delivery = validateDeliveryTarget(input.delivery);
    const createdAtMs = input.createdAtMs ?? this.clock();
    if (!Number.isSafeInteger(createdAtMs) || createdAtMs < 0) throw new VideoJobStoreError("invalid_input");
    const requestDigest = deriveOpaqueId(
      this.identityHashSecret,
      "video-job-request",
      JSON.stringify({
        senderHash: input.senderHash,
        prompt,
        ...(firstFrameIdentity === undefined ? {} : { firstFrameIdentity }),
        delivery,
      }),
    );
    return Object.freeze({
      correlationId: input.correlationId,
      senderHash: input.senderHash,
      recordKey: videoJobRecordKey(input.correlationId, this.prefix),
      requestDigest,
      payload: freezePayload({
        schemaVersion: 1,
        prompt,
        ...(firstFrameUrl === undefined ? {} : { firstFrameUrl }),
        delivery,
      }),
      createdAtMs,
    });
  }

  private async readRecord(recordKey: string): Promise<VideoJobRecord | null> {
    const raw = await this.redis.get(this.metadataKey(recordKey));
    if (!raw) return null;
    return parseRecord(raw, recordKey);
  }

  private async readPayload(recordKey: string): Promise<VideoJobPayload | null> {
    const raw = await this.redis.get(this.payloadKey(recordKey));
    if (!raw) return null;
    let encrypted: EncryptedPayload;
    try {
      encrypted = JSON.parse(raw) as EncryptedPayload;
      return parsePayload(this.cipher.decrypt<VideoJobPayload>(encrypted, this.payloadKey(recordKey)));
    } catch {
      throw new VideoJobStoreError("corrupt_record");
    }
  }

  private async writeRecord(
    record: VideoJobRecord,
    payload: VideoJobPayload | null,
    now: number,
    lease: RedisLease,
    expectedStateVersion: number,
  ): Promise<boolean> {
    const payloadPresent = payload !== null;
    const encryptedPayload = payloadPresent
      ? JSON.stringify(this.cipher.encrypt(payload, this.payloadKey(record.immutable.recordKey)))
      : "";
    const indexMode = AUTOMATIC_STATES.has(record.state) ? "due" : "terminal";
    const dueAt = record.status.nextAttemptAtMs ?? record.status.updatedAtMs;
    const response = await this.redis.eval(WRITE_VIDEO_JOB_LUA, {
      keys: [
        this.metadataKey(record.immutable.recordKey),
        this.payloadKey(record.immutable.recordKey),
        this.dueIndexKey(),
        this.leaseKey(record.immutable.recordKey),
        this.activeSenderKey(record.immutable.senderHash),
        this.manualReviewIndexKey(),
      ],
      arguments: [
        JSON.stringify(record),
        String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        encryptedPayload,
        payloadPresent ? String(ttlSeconds(record.immutable.payloadExpiresAtMs, now)) : "0",
        payloadPresent ? "1" : "0",
        indexMode,
        String(dueAt),
        record.immutable.recordKey,
        String(expectedStateVersion),
        lease.token,
        GENERATION_ACTIVE_STATES.has(record.state) ? "retain" : "release",
        String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        needsManualReview(record.state) ? "manual" : "remove",
      ],
    });
    return Number(response) === 1;
  }

  private metadataKey(recordKey: string): string {
    return `${recordKey}:metadata`;
  }

  private payloadKey(recordKey: string): string {
    return `${recordKey}:payload`;
  }

  private leaseKey(recordKey: string): string {
    return `${recordKey}:lease`;
  }

  private activeSenderKey(senderHash: string): string {
    return videoJobActiveSenderKey(senderHash, this.prefix);
  }

  private dueIndexKey(): string {
    return videoJobDueIndexKey(this.prefix);
  }

  private manualReviewIndexKey(): string {
    return videoJobManualReviewIndexKey(this.prefix);
  }

  private assertRecordKey(recordKey: string): string {
    const expectedPrefix = `${this.prefix}:record:`;
    if (!recordKey.startsWith(expectedPrefix)) throw new VideoJobStoreError("invalid_input");
    assertPseudonymousHash(recordKey.slice(expectedPrefix.length), "video correlation id");
    return recordKey;
  }
}

function isVideoJobState(value: unknown): value is VideoJobState {
  return typeof value === "string" && (VIDEO_JOB_STATES as readonly string[]).includes(value);
}

function needsManualReview(state: VideoJobState): boolean {
  // `sending` is already a durable outbound intent. A process failure after
  // it is written must be operator-visible rather than silently stranded.
  return state === "submission_unknown" || state === "sending" || state === "send_unknown";
}

function requiresZapRunId(state: VideoJobState): boolean {
  return state === "queued" || state === "running" || state === "video_ready" || state === "sending" || state === "send_unknown";
}

function requiresVideoPayload(state: VideoJobState): boolean {
  return state === "video_ready" || state === "sending" || state === "send_unknown";
}

function validateTransitionInput(input: VideoJobTransitionInput): void {
  if (!Array.isArray(input.expectedStates) || input.expectedStates.length === 0 || input.expectedStates.some((state) => !isVideoJobState(state))) {
    throw new VideoJobStoreError("invalid_transition");
  }
  if (input.failureCategory !== undefined) validateFailureCategory(input.failureCategory);
  if (input.progress !== undefined && (!Number.isInteger(input.progress) || input.progress < 0 || input.progress > 100)) {
    throw new VideoJobStoreError("invalid_transition");
  }
  if (input.zapRunId !== undefined) validateRunId(input.zapRunId);
  if (input.video !== undefined) validateVideo(input.video);
  if (input.acknowledgementAttempted !== undefined && input.acknowledgementAttempted !== true) {
    throw new VideoJobStoreError("invalid_transition");
  }
  if (
    (input.state === "retryable" ||
      input.state === "submission_unknown" ||
      input.state === "send_unknown" ||
      input.state === "dead_letter") &&
    input.failureCategory === undefined
  ) {
    throw new VideoJobStoreError("invalid_transition");
  }
  if (
    (input.state === "dead_letter" || input.state === "delivered") &&
    (input.zapRunId !== undefined || input.video !== undefined)
  ) {
    throw new VideoJobStoreError("invalid_transition");
  }
}

function parseRecord(raw: string, expectedRecordKey: string): VideoJobRecord {
  let value: unknown;
  try {
    value = JSON.parse(raw) as unknown;
  } catch {
    throw new VideoJobStoreError("corrupt_record");
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new VideoJobStoreError("corrupt_record");
  const record = value as Record<string, unknown>;
  const immutable = record.immutable;
  const status = record.status;
  if (record.schemaVersion !== 1 || !immutable || typeof immutable !== "object" || Array.isArray(immutable) || !status || typeof status !== "object" || Array.isArray(status) || !isVideoJobState(record.state) || !Number.isSafeInteger(record.stateVersion) || (record.stateVersion as number) < 1) {
    throw new VideoJobStoreError("corrupt_record");
  }
  const immutableValue = immutable as Record<string, unknown>;
  const statusValue = status as Record<string, unknown>;
  if (
    immutableValue.recordKey !== expectedRecordKey ||
    typeof immutableValue.inboundEventHash !== "string" ||
    !HASH_PATTERN.test(immutableValue.inboundEventHash) ||
    immutableValue.inboundEventHash !== expectedRecordKey.slice(expectedRecordKey.lastIndexOf(":") + 1) ||
    typeof immutableValue.senderHash !== "string" ||
    !HASH_PATTERN.test(immutableValue.senderHash) ||
    typeof immutableValue.requestDigest !== "string" ||
    !HASH_PATTERN.test(immutableValue.requestDigest) ||
    !isNonnegativeSafeInteger(immutableValue.createdAtMs) ||
    !isNonnegativeSafeInteger(immutableValue.payloadExpiresAtMs) ||
    !isNonnegativeSafeInteger(immutableValue.metadataExpiresAtMs) ||
    (immutableValue.payloadExpiresAtMs as number) < (immutableValue.createdAtMs as number) ||
    (immutableValue.metadataExpiresAtMs as number) < (immutableValue.payloadExpiresAtMs as number) ||
    !Number.isInteger(statusValue.attempts) ||
    (statusValue.attempts as number) < 0 ||
    (statusValue.attempts as number) > MAX_ATTEMPTS ||
    !isNonnegativeSafeInteger(statusValue.updatedAtMs)
  ) {
    throw new VideoJobStoreError("corrupt_record");
  }
  validateOptionalMetadata(statusValue);
  validateRecordStateConsistency(record.state, statusValue);
  return freezeRecord({
    schemaVersion: 1,
    immutable: {
      recordKey: immutableValue.recordKey as string,
      inboundEventHash: immutableValue.inboundEventHash as CorrelationId,
      senderHash: immutableValue.senderHash as string,
      requestDigest: immutableValue.requestDigest as string,
      createdAtMs: immutableValue.createdAtMs as number,
      payloadExpiresAtMs: immutableValue.payloadExpiresAtMs as number,
      metadataExpiresAtMs: immutableValue.metadataExpiresAtMs as number,
    },
    state: record.state,
    stateVersion: record.stateVersion as number,
    status: {
      attempts: statusValue.attempts as number,
      updatedAtMs: statusValue.updatedAtMs as number,
      ...(statusValue.nextAttemptAtMs === undefined ? {} : { nextAttemptAtMs: statusValue.nextAttemptAtMs as number }),
      ...(statusValue.zapRunIdHash === undefined ? {} : { zapRunIdHash: statusValue.zapRunIdHash as string }),
      ...(statusValue.videoUrlHash === undefined ? {} : { videoUrlHash: statusValue.videoUrlHash as string }),
      ...(statusValue.progress === undefined ? {} : { progress: statusValue.progress as number }),
      ...(statusValue.failureCategory === undefined ? {} : { failureCategory: statusValue.failureCategory as string }),
      ...(statusValue.acknowledgementAttemptedAtMs === undefined
        ? {}
        : { acknowledgementAttemptedAtMs: statusValue.acknowledgementAttemptedAtMs as number }),
    },
  });
}

function validateOptionalMetadata(status: Record<string, unknown>): void {
  if (
    status.nextAttemptAtMs !== undefined &&
    (!isNonnegativeSafeInteger(status.nextAttemptAtMs) ||
      (status.nextAttemptAtMs as number) < (status.updatedAtMs as number))
  ) {
    throw new VideoJobStoreError("corrupt_record");
  }
  for (const field of ["zapRunIdHash", "videoUrlHash"] as const) {
    if (status[field] !== undefined && (typeof status[field] !== "string" || !HASH_PATTERN.test(status[field] as string))) {
      throw new VideoJobStoreError("corrupt_record");
    }
  }
  if (status.progress !== undefined && (!Number.isInteger(status.progress) || (status.progress as number) < 0 || (status.progress as number) > 100)) {
    throw new VideoJobStoreError("corrupt_record");
  }
  if (status.failureCategory !== undefined) {
    try {
      validateFailureCategory(status.failureCategory);
    } catch {
      throw new VideoJobStoreError("corrupt_record");
    }
  }
  if (status.acknowledgementAttemptedAtMs !== undefined && !isNonnegativeSafeInteger(status.acknowledgementAttemptedAtMs)) {
    throw new VideoJobStoreError("corrupt_record");
  }
}

function validateRecordStateConsistency(state: VideoJobState, status: Record<string, unknown>): void {
  const hasNextAttempt = status.nextAttemptAtMs !== undefined;
  const hasRunId = status.zapRunIdHash !== undefined;
  const hasVideo = status.videoUrlHash !== undefined;
  const requiresFailure = state === "submission_unknown" || state === "retryable" || state === "send_unknown" || state === "dead_letter";
  if (
    AUTOMATIC_STATES.has(state) !== hasNextAttempt ||
    (requiresZapRunId(state) && !hasRunId) ||
    (requiresVideoPayload(state) && !hasVideo) ||
    (state === "submission_pending" && (hasRunId || hasVideo)) ||
    ((state === "submission_unknown" || state === "queued" || state === "running" || state === "retryable") && hasVideo) ||
    ((state === "dead_letter" || state === "delivered") && (hasRunId || hasVideo)) ||
    (requiresFailure && status.failureCategory === undefined)
  ) {
    throw new VideoJobStoreError("corrupt_record");
  }
}

function parsePayload(value: unknown): VideoJobPayload {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new VideoJobStoreError("corrupt_record");
  const payload = value as Record<string, unknown>;
  if (payload.schemaVersion !== 1 || typeof payload.prompt !== "string") throw new VideoJobStoreError("corrupt_record");
  let prompt: string;
  let firstFrameUrl: string | undefined;
  let zapRunId: string | undefined;
  let video: VideoJobVideo | undefined;
  let delivery: VideoJobDeliveryTarget;
  try {
    prompt = normalizePrompt(payload.prompt);
    firstFrameUrl = payload.firstFrameUrl === undefined ? undefined : normalizeHttpsUrl(payload.firstFrameUrl as string);
    zapRunId = payload.zapRunId === undefined ? undefined : validateRunId(payload.zapRunId);
    video = payload.video === undefined ? undefined : validateVideo(payload.video as VideoJobVideo);
    delivery = validateDeliveryTarget(payload.delivery);
  } catch {
    throw new VideoJobStoreError("corrupt_record");
  }
  return freezePayload({
    schemaVersion: 1,
    prompt,
    ...(firstFrameUrl === undefined ? {} : { firstFrameUrl }),
    ...(zapRunId === undefined ? {} : { zapRunId }),
    ...(video === undefined ? {} : { video }),
    delivery,
  });
}

function freezeRecord(record: VideoJobRecord): VideoJobRecord {
  return Object.freeze({
    ...record,
    immutable: Object.freeze({ ...record.immutable }),
    status: Object.freeze({ ...record.status }),
  });
}

function freezePayload(payload: VideoJobPayload): VideoJobPayload {
  return Object.freeze({
    ...payload,
    ...(payload.video === undefined ? {} : { video: Object.freeze({ ...payload.video }) }),
    delivery: Object.freeze({ ...payload.delivery }),
  });
}

function validateDeliveryTarget(value: unknown): VideoJobDeliveryTarget {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new VideoJobStoreError("invalid_input");
  }
  const delivery = value as Record<string, unknown>;
  const chatGuid = delivery.chatGuid;
  const phone = delivery.phone;
  if (
    typeof chatGuid !== "string" ||
    !chatGuid.trim() ||
    chatGuid.length > 4_096 ||
    /[\u0000\r\n]/.test(chatGuid) ||
    (phone !== undefined &&
      (typeof phone !== "string" || !phone.trim() || phone.length > 320 || /[\u0000\r\n]/.test(phone)))
  ) {
    throw new VideoJobStoreError("invalid_input");
  }
  return Object.freeze({
    chatGuid: chatGuid.trim(),
    ...(phone === undefined ? {} : { phone: (phone as string).trim() }),
  });
}

function normalizePrompt(value: string): string {
  const prompt = value?.trim();
  if (!prompt || prompt.length > MAX_PROMPT_CHARS || prompt.includes("\u0000")) {
    throw new VideoJobStoreError("invalid_input");
  }
  return prompt;
}

function normalizeHttpsUrl(value: string): string {
  if (!value || value.length > MAX_URL_CHARS) throw new VideoJobStoreError("invalid_input");
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new VideoJobStoreError("invalid_input");
  }
  if (url.protocol !== "https:" || !url.hostname || url.username || url.password) {
    throw new VideoJobStoreError("invalid_input");
  }
  return url.toString();
}

function validateRunId(value: unknown): string {
  if (typeof value !== "string" || value.length > MAX_RUN_ID_CHARS || !RUN_ID_PATTERN.test(value)) {
    throw new VideoJobStoreError("invalid_transition");
  }
  return value;
}

function validateVideo(value: VideoJobVideo): VideoJobVideo {
  if (!value || typeof value !== "object" || value.mimeType !== "video/mp4") {
    throw new VideoJobStoreError("invalid_transition");
  }
  const url = normalizeHttpsUrl(value.url);
  const expiresAt = value.expiresAt;
  if (expiresAt !== undefined && (typeof expiresAt !== "string" || expiresAt.length > 128 || !Number.isFinite(Date.parse(expiresAt)))) {
    throw new VideoJobStoreError("invalid_transition");
  }
  return Object.freeze({ url, mimeType: "video/mp4", ...(expiresAt === undefined ? {} : { expiresAt }) });
}

function validateFailureCategory(value: unknown): asserts value is string {
  if (typeof value !== "string" || value.length > MAX_FAILURE_CATEGORY_CHARS || !FAILURE_CATEGORY_PATTERN.test(value)) {
    throw new VideoJobStoreError("invalid_transition");
  }
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function ttlSeconds(expiresAtMs: number, now: number): number {
  const remaining = expiresAtMs - now;
  if (!Number.isSafeInteger(remaining) || remaining < 1) throw new VideoJobStoreError("corrupt_record");
  return Math.max(1, Math.ceil(remaining / 1_000));
}

function assertPrefix(prefix: string): void {
  if (!/^[A-Za-z0-9:_-]{1,128}$/.test(prefix)) throw new VideoJobStoreError("invalid_input");
}
