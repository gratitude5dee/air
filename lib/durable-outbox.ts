import { randomBytes } from "node:crypto";
import type { EncryptedPayload, QueueCipher } from "./crypto.js";
import {
  acquireRedisLease,
  assertPseudonymousHash,
  PERSIST_OUTBOX_NEW_RECORD_LUA,
  WRITE_OUTBOX_RECORD_LUA,
  type RedisLease,
  type RedisLike,
} from "./redis.js";
import { deriveOpaqueId, type CorrelationId } from "./security.js";

export const OUTBOX_STATES = Object.freeze([
  "ready",
  "sending",
  "sent",
  "failed_retryable",
  "send_unknown",
  "dead_letter",
] as const);

export type OutboxState = (typeof OUTBOX_STATES)[number];

export interface OutboxImmutableMetadata {
  readonly recordKey: string;
  readonly correlationId: CorrelationId;
  /** HMAC of the rendered response; no plaintext response is retained here. */
  readonly responseHash: string;
  readonly createdAtMs: number;
  readonly payloadExpiresAtMs: number;
  readonly metadataExpiresAtMs: number;
}

export interface OutboxStatusMetadata {
  readonly attempts: number;
  readonly updatedAtMs: number;
  readonly nextAttemptAtMs?: number | undefined;
  readonly sendingLeaseExpiresAtMs?: number | undefined;
  readonly activeAttemptToken?: string | undefined;
  readonly failureCategory?: string | undefined;
}

/** Plaintext status metadata only; the actual rendered iMessage response is encrypted. */
export interface OutboxRecord {
  readonly schemaVersion: 1;
  readonly immutable: OutboxImmutableMetadata;
  readonly state: OutboxState;
  readonly stateVersion: number;
  readonly status: OutboxStatusMetadata;
  readonly encryptedResponse?: EncryptedPayload | undefined;
}

export interface PersistOutboxInput {
  readonly correlationId: CorrelationId | string;
  readonly renderedResponse: string;
  readonly createdAtMs?: number | undefined;
}

export interface PersistedOutbox {
  readonly created: boolean;
  readonly record: OutboxRecord;
}

/**
 * Metadata-only reconciliation data for an ambiguous provider send. These
 * fields are intentionally the complete public surface of operator reports:
 * no response body, sender, thread, or provider identifier is available.
 */
export interface SendUnknownOutboxReport {
  readonly correlationId: CorrelationId;
  /** Milliseconds since this record entered `send_unknown`. */
  readonly age: number;
  readonly attempt: number;
  readonly errorCategory: string;
}

export interface OutboxSendAttempt {
  readonly record: OutboxRecord;
  readonly renderedResponse: string;
  readonly attemptToken: string;
  heartbeat(): Promise<boolean>;
  /** Use only after a confirmed provider success. Deletes encrypted response immediately. */
  markSent(): Promise<OutboxRecord | null>;
  /**
   * Use only when the provider was definitely not called (e.g. local adapter
   * construction/network preflight failure). A send timeout is not retryable.
   */
  markPreSendFailure(category: string, nextAttemptAtMs: number): Promise<OutboxRecord | null>;
  /** Ambiguous provider outcome: preserve encrypted response up to 24h, never auto-retry. */
  markSendUnknown(category: string): Promise<OutboxRecord | null>;
  release(): Promise<boolean>;
}

export interface DurableOutboxOptions {
  readonly redis: RedisLike;
  readonly cipher: QueueCipher;
  readonly identityHashSecret: string;
  readonly prefix?: string | undefined;
  readonly clock?: (() => number) | undefined;
  readonly payloadRetentionMs?: number | undefined;
  readonly metadataRetentionMs?: number | undefined;
  readonly sendLeaseTtlMs?: number | undefined;
  readonly maxSendAttempts?: number | undefined;
}

const TERMINAL_STATES = new Set<OutboxState>(["sent", "send_unknown", "dead_letter"]);
const MAX_RESPONSE_LENGTH = 24_000;

export function outboxRecordKey(correlationId: CorrelationId | string, prefix = "air:outbox:v1"): string {
  assertPseudonymousHash(correlationId, "correlation id");
  return `${prefix}:record:${correlationId}`;
}

export function outboxSendUnknownIndexKey(prefix = "air:outbox:v1"): string {
  return `${prefix}:send_unknown`;
}

/**
 * Durable, idempotent outbound state. The sender calls `persistValidatedResponse`
 * before touching Photon, then obtains exactly one `beginSend` attempt.
 */
export class DurableOutbox {
  private readonly redis: RedisLike;
  private readonly cipher: QueueCipher;
  private readonly identityHashSecret: string;
  private readonly prefix: string;
  private readonly clock: () => number;
  private readonly payloadRetentionMs: number;
  private readonly metadataRetentionMs: number;
  private readonly sendLeaseTtlMs: number;
  private readonly maxSendAttempts: number;

  public constructor(options: DurableOutboxOptions) {
    if (!options.identityHashSecret?.trim()) throw new Error("identity hash secret is required for durable outbox");
    this.redis = options.redis;
    this.cipher = options.cipher;
    this.identityHashSecret = options.identityHashSecret;
    this.prefix = options.prefix ?? "air:outbox:v1";
    this.clock = options.clock ?? Date.now;
    this.payloadRetentionMs = options.payloadRetentionMs ?? 86_400_000;
    this.metadataRetentionMs = options.metadataRetentionMs ?? 604_800_000;
    this.sendLeaseTtlMs = options.sendLeaseTtlMs ?? 75_000;
    this.maxSendAttempts = options.maxSendAttempts ?? 3;
    for (const value of [
      this.payloadRetentionMs,
      this.metadataRetentionMs,
      this.sendLeaseTtlMs,
      this.maxSendAttempts,
    ]) {
      if (!Number.isInteger(value) || value < 1) throw new Error("invalid outbox retention or attempt setting");
    }
    if (this.metadataRetentionMs < this.payloadRetentionMs) {
      throw new Error("outbox metadata retention must outlive encrypted payload retention");
    }
  }

  /**
   * Stores only the first validated response for a correlation ID. A different
   * later model result is rejected rather than replacing a response in flight.
   */
  async persistValidatedResponse(input: PersistOutboxInput): Promise<PersistedOutbox> {
    assertPseudonymousHash(input.correlationId, "correlation id");
    if (!input.renderedResponse || input.renderedResponse.length > MAX_RESPONSE_LENGTH) {
      throw new Error("validated response is empty or too large");
    }
    const now = input.createdAtMs ?? this.clock();
    if (!Number.isSafeInteger(now) || now < 0) throw new Error("outbox creation time is invalid");
    const correlationId = input.correlationId as CorrelationId;
    const recordKey = outboxRecordKey(correlationId, this.prefix);
    const responseHash = deriveOpaqueId(this.identityHashSecret, "rendered-response", input.renderedResponse);
    // Serialize first-write/idempotency repair with logical-expiry cleanup so
    // an old reader cannot erase a fresh response generation for this turn.
    const recordLease = await this.acquireRecordLease(recordKey);
    if (!recordLease) throw new Error("outbox record is busy");
    try {
    const record = freezeOutboxRecord({
      schemaVersion: 1,
      immutable: {
        recordKey,
        correlationId,
        responseHash,
        createdAtMs: now,
        payloadExpiresAtMs: now + this.payloadRetentionMs,
        metadataExpiresAtMs: now + this.metadataRetentionMs,
      },
      state: "ready",
      stateVersion: 1,
      status: { attempts: 0, updatedAtMs: now, nextAttemptAtMs: now },
      encryptedResponse: this.cipher.encrypt({ renderedResponse: input.renderedResponse }, recordKey),
    });
    // The seven-day status key owns idempotency; ciphertext has its own hard
    // 24-hour TTL in the payload key and therefore cannot linger in metadata.
    // The metadata, ciphertext, and discovery indexes must be one Redis
    // transaction: a client can disconnect after a committed Lua result, but
    // it can never strand a metadata-only response or an undiscoverable one.
    const published = await this.redis.eval(PERSIST_OUTBOX_NEW_RECORD_LUA, {
      keys: [
        this.metadataRecordKey(recordKey),
        recordKey,
        this.pendingIndexKey(),
        this.sendUnknownIndexKey(),
      ],
      arguments: [
        JSON.stringify(redactOutboxRecord(record)),
        String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        JSON.stringify(record),
        String(ttlSeconds(record.immutable.payloadExpiresAtMs, now)),
        String(record.immutable.createdAtMs),
        recordKey,
      ],
    });
    if (Number(published) === 1) {
      return Object.freeze({ created: true, record });
    }
    if (Number(published) !== 0) throw new Error("outbox payload authority is inconsistent");

    let existing = await this.readStoredRecord(recordKey);
    if (!existing) throw new Error("outbox record disappeared during idempotency check");
    if (existing.immutable.responseHash !== responseHash) {
      throw new Error("a different validated response already exists for this turn");
    }
    if (!existing.encryptedResponse && !TERMINAL_STATES.has(existing.state)) {
      if (existing.immutable.payloadExpiresAtMs <= now) {
        existing = makeTerminalRecord(existing, "dead_letter", now, "payload_expired");
        await this.writeStoredRecord(existing, now);
      } else {
        existing = freezeOutboxRecord({
          ...existing,
          encryptedResponse: this.cipher.encrypt({ renderedResponse: input.renderedResponse }, recordKey),
        });
        await this.writeStoredRecord(existing, now);
      }
    }
    return Object.freeze({ created: false, record: existing });
    } finally {
      await recordLease.release();
    }
  }

  async getRecord(recordKey: string): Promise<OutboxRecord | null> {
    const record = await this.readStoredRecord(recordKey);
    if (!record) return null;
    const now = this.clock();
    if (record.immutable.metadataExpiresAtMs <= now) return this.expireMetadata(recordKey);
    if (
      (!TERMINAL_STATES.has(record.state) && !record.encryptedResponse) ||
      (record.encryptedResponse && record.immutable.payloadExpiresAtMs <= now)
    ) {
      return this.expirePayload(recordKey, "payload_expired");
    }
    return record;
  }

  /**
   * Lists ambiguous sends from redacted metadata only. It deliberately never
   * calls `getRecord` or reads the encrypted payload key, so operator tooling
   * cannot accidentally decrypt or expose the response text.
   */
  async listSendUnknownReports(limit = 100): Promise<readonly SendUnknownOutboxReport[]> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 500) {
      throw new Error("send-unknown report limit must be between 1 and 500");
    }
    const now = this.clock();
    const recordKeys = await this.redis.zRangeByScore(
      this.sendUnknownIndexKey(),
      now,
      Number.POSITIVE_INFINITY,
      { limit },
    );
    const reports: SendUnknownOutboxReport[] = [];
    for (const recordKey of recordKeys) {
      let record: OutboxRecord | null;
      try {
        record = await this.readMetadataRecord(recordKey);
      } catch {
        // A malformed redacted record is not actionable by this report. Do
        // not surface any of its fields or fall back to its payload key.
        continue;
      }
      if (
        !record ||
        record.encryptedResponse ||
        record.state !== "send_unknown" ||
        record.immutable.recordKey !== recordKey ||
        !Number.isSafeInteger(record.immutable.metadataExpiresAtMs) ||
        record.immutable.metadataExpiresAtMs <= now ||
        !isPseudonymousHash(record.immutable.correlationId) ||
        !Number.isSafeInteger(record.status.updatedAtMs) ||
        !Number.isSafeInteger(record.status.attempts) ||
        record.status.attempts < 0
      ) {
        continue;
      }
      reports.push(Object.freeze({
        correlationId: record.immutable.correlationId,
        age: Math.max(0, now - record.status.updatedAtMs),
        attempt: record.status.attempts,
        errorCategory: sanitizeFailureCategory(record.status.failureCategory ?? "unknown_failure"),
      }));
    }
    return Object.freeze(reports);
  }

  /** Returns only safe-to-send work; stale sending states become send_unknown. */
  async listDue(limit = 50): Promise<readonly OutboxRecord[]> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 500) {
      throw new Error("outbox due limit must be between 1 and 500");
    }
    const now = this.clock();
    const keys = await this.redis.zRangeByScore(this.pendingIndexKey(), Number.NEGATIVE_INFINITY, now, {
      limit: limit * 3,
    });
    const due: OutboxRecord[] = [];
    for (const recordKey of keys) {
      if (due.length >= limit) break;
      let record = await this.getRecord(recordKey);
      if (!record) {
        await this.redis.zRem(this.pendingIndexKey(), recordKey);
        continue;
      }
      if (TERMINAL_STATES.has(record.state)) {
        // New writes update indexes atomically. This is only a best-effort
        // repair for records written by an older schema; a repair failure must
        // never turn a known terminal result into a failed send operation.
        await this.reconcileIndex(record).catch(() => undefined);
        continue;
      }
      if (record.state === "sending" && record.status.sendingLeaseExpiresAtMs !== undefined && record.status.sendingLeaseExpiresAtMs <= now) {
        record = (await this.finishAttempt(recordKey, record.status.activeAttemptToken, "send_unknown", "send_lease_expired")) ?? record;
      }
      if (
        record.state === "ready" ||
        (record.state === "failed_retryable" && (record.status.nextAttemptAtMs ?? Number.POSITIVE_INFINITY) <= now)
      ) {
        due.push(record);
      }
    }
    return Object.freeze(due);
  }

  /**
   * Reserves an outbound attempt. This is intentionally separate from the
   * actual Photon call so the encrypted response exists before any send.
   */
  async beginSend(recordKey: string): Promise<OutboxSendAttempt | null> {
    const candidate = await this.getRecord(recordKey);
    const now = this.clock();
    if (!candidate || !isSendable(candidate, now)) return null;
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) return null;

    try {
      const current = await this.readStoredRecord(recordKey);
      if (!current || !isSendable(current, this.clock()) || !current.encryptedResponse) {
        await lease.release();
        return null;
      }
      if (current.status.attempts >= this.maxSendAttempts) {
        const dead = makeTerminalRecord(current, "dead_letter", this.clock(), "pre_send_retries_exhausted");
        await this.writeStoredRecord(dead, this.clock());
        await lease.release();
        return null;
      }

      const attemptToken = randomBytes(24).toString("base64url");
      const sending = freezeOutboxRecord({
        ...current,
        state: "sending",
        stateVersion: current.stateVersion + 1,
        status: {
          attempts: current.status.attempts + 1,
          updatedAtMs: now,
          sendingLeaseExpiresAtMs: now + this.sendLeaseTtlMs,
          activeAttemptToken: attemptToken,
        },
      });
      await this.writeStoredRecord(sending, now);

      const encryptedResponse = sending.encryptedResponse;
      if (!encryptedResponse) {
        const dead = makeTerminalRecord(sending, "dead_letter", this.clock(), "payload_missing");
        await this.writeStoredRecord(dead, this.clock());
        return null;
      }

      let renderedResponse: string;
      try {
        const payload = this.cipher.decrypt<{ renderedResponse: string }>(encryptedResponse, recordKey);
        if (!payload.renderedResponse || payload.renderedResponse.length > MAX_RESPONSE_LENGTH) {
          throw new Error("invalid encrypted response");
        }
        renderedResponse = payload.renderedResponse;
      } catch {
        const dead = makeTerminalRecord(sending, "dead_letter", this.clock(), "payload_authentication_failed");
        await this.writeStoredRecord(dead, this.clock());
        return null;
      }

      // The record mutex only protects the state transition. `sending` plus
      // its attempt token is the durable ownership marker while Photon runs.
      await lease.release();
      return Object.freeze({
        record: sending,
        renderedResponse,
        attemptToken,
        heartbeat: async (): Promise<boolean> => {
          return this.touchSendingLease(
            recordKey,
            attemptToken,
            this.clock() + this.sendLeaseTtlMs,
          );
        },
        markSent: () => this.finishAttempt(recordKey, attemptToken, "sent"),
        markPreSendFailure: (category: string, nextAttemptAtMs: number) =>
          this.markPreSendFailure(recordKey, attemptToken, category, nextAttemptAtMs),
        markSendUnknown: (category: string) =>
          this.finishAttempt(recordKey, attemptToken, "send_unknown", category),
        // State ownership is recorded durably, so there is no process-local
        // lock to release after the provider call. Keep this method for a
        // symmetric caller contract with inbox processing leases.
        release: async (): Promise<boolean> => true,
      });
    } catch (error) {
      throw error;
    } finally {
      await lease.release();
    }
  }

  private async markPreSendFailure(
    recordKey: string,
    attemptToken: string,
    category: string,
    nextAttemptAtMs: number,
  ): Promise<OutboxRecord | null> {
    if (!Number.isSafeInteger(nextAttemptAtMs) || nextAttemptAtMs < this.clock()) {
      throw new Error("retry time is invalid");
    }
    const current = await this.getRecord(recordKey);
    if (!current) return null;
    if (current.status.attempts >= this.maxSendAttempts) {
      return this.finishAttempt(recordKey, attemptToken, "dead_letter", "pre_send_retries_exhausted");
    }
    return this.finishAttempt(recordKey, attemptToken, "failed_retryable", category, nextAttemptAtMs);
  }

  private async finishAttempt(
    recordKey: string,
    attemptToken: string | undefined,
    target: "sent" | "failed_retryable" | "send_unknown" | "dead_letter",
    category?: string,
    nextAttemptAtMs?: number,
  ): Promise<OutboxRecord | null> {
    if (!attemptToken) return null;
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) throw new Error("outbox record is busy");
    let next: OutboxRecord | null = null;
    try {
      const current = await this.readStoredRecord(recordKey);
      if (!current || current.state !== "sending" || current.status.activeAttemptToken !== attemptToken) return null;
      const now = this.clock();
      if (target === "failed_retryable" && nextAttemptAtMs === undefined) {
        throw new Error("retryable outbox state requires a next attempt time");
      }
      next = target === "failed_retryable"
        ? freezeOutboxRecord({
          ...current,
          state: "failed_retryable",
          stateVersion: current.stateVersion + 1,
          status: {
            attempts: current.status.attempts,
            updatedAtMs: now,
            nextAttemptAtMs,
            failureCategory: sanitizeFailureCategory(category ?? "pre_send_failure"),
          },
        })
        : makeTerminalRecord(current, target, now, category);
      await this.writeStoredRecord(next, now);
    } finally {
      await lease.release();
    }
    return next;
  }

  private async expirePayload(recordKey: string, category: string): Promise<OutboxRecord | null> {
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) throw new Error("outbox record is busy");
    let next: OutboxRecord | null = null;
    try {
      const current = await this.readStoredRecord(recordKey);
      if (!current) return null;
      const now = this.clock();
      if (
        (TERMINAL_STATES.has(current.state) && !current.encryptedResponse) ||
        (current.encryptedResponse && current.immutable.payloadExpiresAtMs > now)
      ) return current;
      next = makeTerminalRecord(current, "dead_letter", now, category);
      await this.writeStoredRecord(next, now);
    } finally {
      await lease.release();
    }
    return next;
  }

  /** Remove a fully expired turn only while new creation is fenced for its key. */
  private async expireMetadata(recordKey: string): Promise<OutboxRecord | null> {
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) throw new Error("outbox record is busy");
    try {
      const current = await this.readStoredRecord(recordKey);
      if (!current) return null;
      if (current.immutable.metadataExpiresAtMs > this.clock()) return current;
      await this.redis.del(recordKey, this.metadataRecordKey(recordKey));
      await Promise.all([
        this.redis.zRem(this.pendingIndexKey(), recordKey),
        this.redis.zRem(this.sendUnknownIndexKey(), recordKey),
      ]);
      return null;
    } finally {
      await lease.release();
    }
  }

  private async touchSendingLease(recordKey: string, attemptToken: string, expiresAtMs: number): Promise<boolean> {
    const lease = await this.acquireRecordLease(recordKey);
    if (!lease) return false;
    let next: OutboxRecord | null = null;
    try {
      const current = await this.readStoredRecord(recordKey);
      if (!current || current.state !== "sending" || current.status.activeAttemptToken !== attemptToken) return false;
      const now = this.clock();
      next = freezeOutboxRecord({
        ...current,
        status: { ...current.status, updatedAtMs: now, sendingLeaseExpiresAtMs: expiresAtMs },
      });
      await this.writeStoredRecord(next, now);
    } finally {
      await lease.release();
    }
    if (!next) return false;
    return true;
  }

  private async readStoredRecord(recordKey: string): Promise<OutboxRecord | null> {
    const serialized = await this.redis.get(recordKey) ?? await this.redis.get(this.metadataRecordKey(recordKey));
    return serialized ? parseOutboxRecord(serialized) : null;
  }

  /** Operator reports must use this metadata-only reader, never the payload-first reader above. */
  private async readMetadataRecord(recordKey: string): Promise<OutboxRecord | null> {
    const serialized = await this.redis.get(this.metadataRecordKey(recordKey));
    return serialized ? parseOutboxRecord(serialized) : null;
  }

  private async writeStoredRecord(record: OutboxRecord, now: number): Promise<void> {
    const index = outboxIndexIntent(record);
    const result = await this.redis.eval(WRITE_OUTBOX_RECORD_LUA, {
      keys: [
        this.metadataRecordKey(record.immutable.recordKey),
        record.immutable.recordKey,
        this.pendingIndexKey(),
        this.sendUnknownIndexKey(),
      ],
      arguments: [
        JSON.stringify(redactOutboxRecord(record)),
        String(ttlSeconds(record.immutable.metadataExpiresAtMs, now)),
        record.encryptedResponse ? JSON.stringify(record) : "",
        record.encryptedResponse ? String(ttlSeconds(record.immutable.payloadExpiresAtMs, now)) : "0",
        record.encryptedResponse ? "1" : "0",
        index.mode,
        String(index.score),
        record.immutable.recordKey,
      ],
    });
    if (Number(result) !== 1) throw new Error("outbox record could not be updated");
  }

  private async reconcileIndex(record: OutboxRecord): Promise<void> {
    if (record.state === "send_unknown") {
      await Promise.all([
        this.redis.zRem(this.pendingIndexKey(), record.immutable.recordKey),
        this.redis.zAdd(this.sendUnknownIndexKey(), [{
          value: record.immutable.recordKey,
          // Score by metadata expiry so bounded reports naturally skip stale
          // ZSET members after Redis expires their status key.
          score: record.immutable.metadataExpiresAtMs,
        }]),
      ]);
      return;
    }
    if (TERMINAL_STATES.has(record.state)) {
      await Promise.all([
        this.redis.zRem(this.pendingIndexKey(), record.immutable.recordKey),
        this.redis.zRem(this.sendUnknownIndexKey(), record.immutable.recordKey),
      ]);
      return;
    }
    const dueAt = record.state === "failed_retryable"
      ? (record.status.nextAttemptAtMs ?? record.status.updatedAtMs)
      : record.state === "sending"
        ? (record.status.sendingLeaseExpiresAtMs ?? record.status.updatedAtMs)
        : record.immutable.createdAtMs;
    await Promise.all([
      this.redis.zAdd(this.pendingIndexKey(), [{ value: record.immutable.recordKey, score: dueAt }]),
      this.redis.zRem(this.sendUnknownIndexKey(), record.immutable.recordKey),
    ]);
  }

  private async acquireRecordLease(recordKey: string): Promise<RedisLease | null> {
    return acquireRedisLease(this.redis, `${this.prefix}:lease:${deriveOpaqueId(this.identityHashSecret, "outbox-record", recordKey)}`, {
      ttlMs: 15_000,
      now: this.clock,
    });
  }

  private pendingIndexKey(): string {
    return `${this.prefix}:pending`;
  }

  private sendUnknownIndexKey(): string {
    return outboxSendUnknownIndexKey(this.prefix);
  }

  private metadataRecordKey(recordKey: string): string {
    return `${recordKey}:metadata`;
  }
}

function isSendable(record: OutboxRecord, now: number): boolean {
  return record.state === "ready" ||
    (record.state === "failed_retryable" && (record.status.nextAttemptAtMs ?? Number.POSITIVE_INFINITY) <= now);
}

function outboxIndexIntent(record: OutboxRecord): {
  readonly mode: "pending" | "send_unknown" | "terminal";
  readonly score: number;
} {
  if (record.state === "send_unknown") {
    return Object.freeze({ mode: "send_unknown", score: record.immutable.metadataExpiresAtMs });
  }
  if (TERMINAL_STATES.has(record.state)) return Object.freeze({ mode: "terminal", score: 0 });
  const score = record.state === "failed_retryable"
    ? (record.status.nextAttemptAtMs ?? record.status.updatedAtMs)
    : record.state === "sending"
      ? (record.status.sendingLeaseExpiresAtMs ?? record.status.updatedAtMs)
      : record.immutable.createdAtMs;
  return Object.freeze({ mode: "pending", score });
}

function makeTerminalRecord(
  current: OutboxRecord,
  target: "sent" | "send_unknown" | "dead_letter",
  now: number,
  category?: string,
): OutboxRecord {
  const clearPayload = target === "sent" || target === "dead_letter";
  return freezeOutboxRecord({
    ...current,
    state: target,
    stateVersion: current.stateVersion + 1,
    encryptedResponse: clearPayload ? undefined : current.encryptedResponse,
    status: {
      attempts: current.status.attempts,
      updatedAtMs: now,
      failureCategory: category ? sanitizeFailureCategory(category) : undefined,
    },
  });
}

function ttlSeconds(expiresAtMs: number, now: number): number {
  const remaining = Math.ceil((expiresAtMs - now) / 1_000);
  if (!Number.isInteger(remaining) || remaining < 1) throw new Error("outbox metadata retention has expired");
  return remaining;
}

function sanitizeFailureCategory(value: string): string {
  return /^[a-z0-9_.-]{1,80}$/i.test(value) ? value.toLowerCase() : "unknown_failure";
}

function isPseudonymousHash(value: unknown): value is CorrelationId {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}

function parseOutboxRecord(serialized: string): OutboxRecord {
  try {
    const parsed = JSON.parse(serialized) as OutboxRecord;
    if (
      parsed.schemaVersion !== 1 ||
      !parsed.immutable ||
      !parsed.status ||
      !OUTBOX_STATES.includes(parsed.state) ||
      typeof parsed.immutable.recordKey !== "string" ||
      typeof parsed.immutable.metadataExpiresAtMs !== "number"
    ) {
      throw new Error("invalid outbox record");
    }
    return freezeOutboxRecord(parsed);
  } catch {
    throw new Error("durable outbox record is corrupt");
  }
}

function freezeOutboxRecord(record: OutboxRecord): OutboxRecord {
  const encryptedResponse = record.encryptedResponse
    ? Object.freeze({ ...record.encryptedResponse })
    : undefined;
  return Object.freeze({
    ...record,
    immutable: Object.freeze({ ...record.immutable }),
    status: Object.freeze({ ...record.status }),
    encryptedResponse,
  });
}

/** Seven-day outbox metadata has no encrypted response field by construction. */
function redactOutboxRecord(record: OutboxRecord): OutboxRecord {
  return freezeOutboxRecord({ ...record, encryptedResponse: undefined });
}
