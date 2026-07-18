import { randomBytes } from "node:crypto";
import type { EncryptedPayload, QueueCipher } from "../crypto.js";
import type { RedisLike } from "../redis.js";
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
  "send_unknown",
  "confirmed_absent",
] as const);
export type StudioOutboxState = (typeof STUDIO_OUTBOX_STATES)[number];

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
  readonly senderIdentifier: string;
  readonly messageId: string;
  readonly text: string | null;
  readonly attachments: readonly StudioAttachmentReference[];
  readonly replyTargetId?: string | undefined;
}

export interface StudioInboxRecord {
  readonly schemaVersion: 2;
  readonly receiptId: string;
  readonly senderHash: SenderHash;
  readonly threadHash: ThreadHash;
  readonly receivedAtMs: number;
  readonly state: "received" | "claimed" | "processed" | "maintenance_rejected" | "failed";
  readonly stateVersion: number;
  readonly encryptedEnvelope: EncryptedPayload;
}

export interface StudioJobPayload {
  readonly prompt: string | null;
  readonly input?: StudioAttachmentReference | undefined;
  readonly chatGuid: string;
  readonly sendingLine: string;
  readonly replyTargetId?: string | undefined;
}

export interface StudioJobRecord {
  readonly schemaVersion: 2;
  readonly jobId: string;
  readonly senderHash: SenderHash;
  readonly threadHash: ThreadHash;
  readonly mode: "imagine" | "animate" | "edit-image" | "edit-video";
  readonly modelSlug: string;
  readonly provider: "gmi" | "fal";
  readonly outputCount: number;
  readonly durationSeconds?: number | undefined;
  readonly reservedCents: number;
  readonly state: StudioJobState;
  readonly stateVersion: number;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
  readonly encryptedPayload: EncryptedPayload;
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
  readonly schemaVersion: 2;
  readonly outboxId: string;
  readonly jobId?: string | undefined;
  readonly senderHash: SenderHash;
  readonly lane: "onboarding" | "tapback" | "job-acknowledgement" | "job-result" | "request-response";
  readonly sequence: number;
  readonly state: StudioOutboxState;
  readonly attempts: number;
  readonly stateVersion: number;
  readonly encryptedPayload: EncryptedPayload;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
}

export type AdmissionResult =
  | { readonly admitted: true; readonly job: StudioJobRecord }
  | { readonly admitted: false; readonly reason: "active_job" | "user_limit" | "per_job_cap" | "daily_cap" | "storage_error" };

export interface StudioStoreOptions {
  readonly redis: RedisLike;
  readonly cipher: QueueCipher;
  readonly identityHashSecret: string;
  readonly prefix?: string | undefined;
  readonly clock?: (() => number) | undefined;
}

export interface AdmitStudioJobInput {
  readonly job: StudioJobRecord;
  readonly activeJobLimit: number;
  readonly dailyUserJobLimit: number;
  readonly perJobCapCents: number;
  readonly dailySpendCapCents: number;
}

/**
 * The configured Vercel recovery route runs every minute. Three missed beats
 * is enough to surface an outage while allowing a single slow function or
 * scheduler delay without declaring a false incident.
 */
export const STUDIO_CRON_FRESHNESS_MS = 3 * 60 * 1_000;

export type StudioCronFreshness = "fresh" | "stale" | "missing" | "invalid";
export type StudioCronHeartbeatScope = "development" | "preview" | "production";

const INBOX_TTL_SECONDS = 7 * 24 * 60 * 60;
const JOB_TTL_SECONDS = 7 * 24 * 60 * 60;
const SETTINGS_TTL_SECONDS = 90 * 24 * 60 * 60;
const PENDING_TTL_SECONDS = 10 * 60;
const LEASE_TTL_MS = 90_000;
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

  public constructor(options: StudioStoreOptions) {
    if (!options.identityHashSecret.trim()) throw new Error("identity hash secret is required");
    this.redis = options.redis;
    this.cipher = options.cipher;
    this.secret = options.identityHashSecret;
    this.prefix = options.prefix ?? STUDIO_REDIS_PREFIX;
    this.now = options.clock ?? Date.now;
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

  public async persistInbox(input: {
    readonly receiptId: string;
    readonly senderHash: SenderHash;
    readonly threadHash: ThreadHash;
    readonly envelope: StudioInboundEnvelope;
  }): Promise<{ readonly created: boolean; readonly record: StudioInboxRecord }> {
    const now = this.now();
    const record: StudioInboxRecord = Object.freeze({
      schemaVersion: 2,
      receiptId: input.receiptId,
      senderHash: input.senderHash,
      threadHash: input.threadHash,
      receivedAtMs: now,
      state: "received",
      stateVersion: 1,
      encryptedEnvelope: this.cipher.encrypt(input.envelope, `inbox:${input.receiptId}`),
    });
    const key = this.inboxKey(input.receiptId);
    // Receipt + ready index must commit together. A separate SET followed by
    // ZADD could acknowledge a webhook and strand work after a crash.
    const result = await this.redis.eval(PERSIST_INBOX_LUA, {
      keys: [key, this.readyInboxKey()],
      arguments: [JSON.stringify(record), String(INBOX_TTL_SECONDS), String(now), input.receiptId],
    });
    if (result === 1 || result === "1") {
      return Object.freeze({ created: true, record });
    }
    const existing = await this.readInbox(input.receiptId);
    if (!existing) throw new Error("durable inbox publication could not be confirmed");
    return Object.freeze({ created: false, record: existing });
  }

  public async readInbox(receiptId: string): Promise<StudioInboxRecord | null> {
    return parseStored<StudioInboxRecord>(await this.redis.get(this.inboxKey(receiptId)));
  }

  public decryptInboxEnvelope(record: StudioInboxRecord): StudioInboundEnvelope {
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
      keys: [this.inboxKey(receiptId), this.inboxLeaseKey(receiptId), this.readyInboxKey()],
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
      keys: [this.inboxKey(receiptId), this.inboxLeaseKey(receiptId), this.readyInboxKey()],
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
    const key = this.pendingKey(senderHash, threadHash);
    const encrypted = parseStored<EncryptedPayload>(await this.redis.get(key));
    return encrypted ? this.cipher.decrypt<T>(encrypted, `pending:${senderHash}:${threadHash}`) : null;
  }

  public async putPending<T>(senderHash: SenderHash, threadHash: ThreadHash, value: T): Promise<void> {
    const encrypted = this.cipher.encrypt(value, `pending:${senderHash}:${threadHash}`);
    await this.redis.set(this.pendingKey(senderHash, threadHash), JSON.stringify(encrypted), { ex: PENDING_TTL_SECONDS });
  }

  public async clearPending(senderHash: SenderHash, threadHash: ThreadHash): Promise<void> {
    await this.redis.del(this.pendingKey(senderHash, threadHash));
  }

  public async admitJob(input: AdmitStudioJobInput): Promise<AdmissionResult> {
    if (!hasValidAdmissionBounds(input)) {
      return Object.freeze({ admitted: false, reason: "storage_error" });
    }
    if (input.job.reservedCents > input.perJobCapCents) {
      return Object.freeze({ admitted: false, reason: "per_job_cap" });
    }
    const day = utcDay(this.now());
    const activeKey = this.activeJobKey(input.job.senderHash);
    const userCountKey = this.userDailyKey(input.job.senderHash, day);
    const dailySpendKey = this.dailySpendKey(day);
    const jobKey = this.jobKey(input.job.jobId);
    const ttl = secondsToUtcMidnight(this.now());
    const result = await this.redis.eval(ADMIT_JOB_LUA, {
      keys: [activeKey, userCountKey, dailySpendKey, jobKey],
      arguments: [
        input.job.jobId,
        JSON.stringify(input.job),
        String(ttl),
        String(JOB_TTL_SECONDS),
        String(input.dailyUserJobLimit),
        String(input.dailySpendCapCents),
        String(input.job.reservedCents),
        String(JOB_TTL_SECONDS),
      ],
    });
    const code = Array.isArray(result) ? result[0] : result;
    if (code === "admitted") return Object.freeze({ admitted: true, job: input.job });
    if (code === "existing") {
      const existing = parseStored<StudioJobRecord>(Array.isArray(result) && typeof result[1] === "string" ? result[1] : null);
      if (!existing || existing.jobId !== input.job.jobId || existing.senderHash !== input.job.senderHash) {
        return Object.freeze({ admitted: false, reason: "storage_error" });
      }
      return Object.freeze({ admitted: true, job: existing });
    }
    if (code === "active_job" || code === "user_limit" || code === "daily_cap") {
      return Object.freeze({ admitted: false, reason: code });
    }
    return Object.freeze({ admitted: false, reason: "storage_error" });
  }

  public async getJob(jobId: string): Promise<StudioJobRecord | null> {
    return parseStored<StudioJobRecord>(await this.redis.get(this.jobKey(jobId)));
  }

  public decryptJobPayload(job: StudioJobRecord): StudioJobPayload {
    return this.cipher.decrypt<StudioJobPayload>(job.encryptedPayload, `job:${job.jobId}`);
  }

  public async transitionJob(jobId: string, from: StudioJobState, to: StudioJobState): Promise<StudioJobRecord | null> {
    const job = await this.getJob(jobId);
    if (!job || job.state !== from || !canTransitionJob(from, to)) return null;
    const result = await this.redis.eval(COMPARE_AND_TRANSITION_JOB_LUA, {
      keys: [this.jobKey(jobId), this.activeJobKey(job.senderHash)],
      arguments: [from, String(job.stateVersion), to, String(this.now()), String(JOB_TTL_SECONDS), job.jobId, isTerminalJobState(to) ? "1" : "0"],
    });
    const next = parseStored<StudioJobRecord>(typeof result === "string" ? result : null);
    if (!next) return null;
    return next;
  }

  public async createOutbox(input: Omit<StudioOutboxRecord, "schemaVersion" | "encryptedPayload" | "createdAtMs" | "updatedAtMs" | "stateVersion" | "state" | "attempts"> & {
    readonly payload: StudioOutboxPayload;
    /** Accepted for backward-compatible callers, but always normalized to v2. */
    readonly schemaVersion?: 2 | undefined;
    readonly state?: "intent" | undefined;
    readonly attempts?: 0 | undefined;
  }): Promise<StudioOutboxRecord> {
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 0) {
      throw new Error("outbox sequence is invalid");
    }
    const now = this.now();
    const record: StudioOutboxRecord = Object.freeze({
      schemaVersion: 2,
      outboxId: input.outboxId,
      ...(input.jobId === undefined ? {} : { jobId: input.jobId }),
      senderHash: input.senderHash,
      lane: input.lane,
      sequence: input.sequence,
      // Every physical send starts as a durable intent. Callers cannot create
      // a pre-sent/retryable record and accidentally bypass this invariant.
      state: "intent",
      attempts: 0,
      stateVersion: 1,
      encryptedPayload: this.cipher.encrypt(input.payload, `outbox:${input.outboxId}`),
      createdAtMs: now,
      updatedAtMs: now,
    });
    const result = await this.redis.eval(PERSIST_OUTBOX_LUA, {
      keys: [this.outboxKey(record.outboxId), this.readyOutboxKey(), this.outboxLaneKey(record)],
      arguments: [JSON.stringify(record), String(JOB_TTL_SECONDS), String(now), record.outboxId, String(record.sequence)],
    });
    if (result === -1 || result === "-1") {
      throw new Error("outbox sequence is already in use for this sender lane");
    }
    if (!(result === 1 || result === "1")) {
      const existing = await this.getOutbox(record.outboxId);
      if (!existing) throw new Error("outbox publication could not be confirmed");
      return existing;
    }
    return record;
  }

  public async getOutbox(outboxId: string): Promise<StudioOutboxRecord | null> {
    return parseStored<StudioOutboxRecord>(await this.redis.get(this.outboxKey(outboxId)));
  }

  public decryptOutboxPayload(record: StudioOutboxRecord): StudioOutboxPayload {
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
    state: Exclude<StudioOutboxState, "intent" | "sending" | "confirmed_absent">,
  ): Promise<boolean> {
    const record = await this.getOutbox(outboxId);
    if (!record) return false;
    const result = await this.redis.eval(FINISH_OUTBOX_LUA, {
      keys: [this.outboxKey(outboxId), this.outboxLeaseKey(outboxId), this.readyOutboxKey(), this.outboxLaneKey(record)],
      arguments: [leaseToken, state, String(this.now()), String(JOB_TTL_SECONDS), outboxId, String(STUDIO_MAX_OUTBOX_SEND_ATTEMPTS), String(OUTBOX_RETRY_BACKOFF_MS)],
    });
    return typeof result === "string";
  }

  private async claimInbox(receiptId: string, leaseToken: string): Promise<StudioInboxRecord | null> {
    const now = this.now();
    const result = await this.redis.eval(CLAIM_INBOX_LUA, {
      keys: [this.inboxKey(receiptId), this.inboxLeaseKey(receiptId), this.readyInboxKey()],
      arguments: [leaseToken, String(LEASE_TTL_MS), String(now), String(INBOX_TTL_SECONDS), receiptId],
    });
    return parseStored<StudioInboxRecord>(typeof result === "string" ? result : null);
  }

  private async claimOutbox(outboxId: string, leaseToken: string, record: StudioOutboxRecord): Promise<StudioOutboxRecord | null> {
    const now = this.now();
    const result = await this.redis.eval(CLAIM_OUTBOX_LUA, {
      keys: [this.outboxKey(outboxId), this.outboxLeaseKey(outboxId), this.readyOutboxKey(), this.outboxLaneKey(record)],
      arguments: [leaseToken, String(LEASE_TTL_MS), String(now), String(JOB_TTL_SECONDS), outboxId, String(STUDIO_MAX_OUTBOX_SEND_ATTEMPTS)],
    });
    return parseStored<StudioOutboxRecord>(typeof result === "string" ? result : null);
  }

  private async reconcileExpiredOutboxSend(outboxId: string, record: StudioOutboxRecord): Promise<void> {
    await this.redis.eval(RECONCILE_EXPIRED_OUTBOX_LUA, {
      keys: [this.outboxKey(outboxId), this.outboxLeaseKey(outboxId), this.readyOutboxKey(), this.outboxLaneKey(record)],
      arguments: [String(this.now()), String(JOB_TTL_SECONDS), outboxId],
    });
  }

  private inboxKey(receiptId: string): string { return `${this.prefix}:inbox:${receiptId}`; }
  private readyInboxKey(): string { return `${this.prefix}:inbox:ready`; }
  private inboxLeaseKey(receiptId: string): string { return `${this.prefix}:inbox:lease:${receiptId}`; }
  private cronHeartbeatKey(scope: StudioCronHeartbeatScope): string {
    return `${this.prefix}:operations:cron-heartbeat:${scope}`;
  }
  private settingsKey(senderHash: SenderHash): string { return `${this.prefix}:settings:${senderHash}`; }
  private pendingKey(senderHash: SenderHash, threadHash: ThreadHash): string { return `${this.prefix}:pending:${senderHash}:${threadHash}`; }
  private activeJobKey(senderHash: SenderHash): string { return `${this.prefix}:active:${senderHash}`; }
  private userDailyKey(senderHash: SenderHash, day: string): string { return `${this.prefix}:rate:${day}:${senderHash}`; }
  private dailySpendKey(day: string): string { return `${this.prefix}:spend:${day}`; }
  private jobKey(jobId: string): string { return `${this.prefix}:job:${jobId}`; }
  private outboxKey(outboxId: string): string { return `${this.prefix}:outbox:${outboxId}`; }
  private readyOutboxKey(): string { return `${this.prefix}:outbox:ready`; }
  private outboxLeaseKey(outboxId: string): string { return `${this.prefix}:outbox:lease:${outboxId}`; }
  /**
   * A lane is only shared where user-visible ordering is meaningful. Independent
   * request/response and tapback effects must never block each other, while a
   * job acknowledgement/result pair and onboarding bubbles remain serialized.
   */
  private outboxLaneKey(record: Pick<StudioOutboxRecord, "outboxId" | "senderHash" | "lane" | "jobId">): string {
    const base = `${this.prefix}:outbox:lane:${record.senderHash}`;
    switch (record.lane) {
      case "onboarding":
        return `${base}:onboarding`;
      case "job-acknowledgement":
      case "job-result":
        return `${base}:job:${record.jobId ?? record.outboxId}`;
      case "request-response":
        return `${base}:request:${record.outboxId}`;
      case "tapback":
        return `${base}:tapback:${record.outboxId}`;
    }
  }
}

export function createStudioJob(input: Omit<StudioJobRecord, "schemaVersion" | "state" | "stateVersion" | "createdAtMs" | "updatedAtMs" | "encryptedPayload"> & { readonly payload: StudioJobPayload; readonly cipher: QueueCipher; readonly now?: number | undefined }): StudioJobRecord {
  const now = input.now ?? Date.now();
  return Object.freeze({
    schemaVersion: 2,
    jobId: input.jobId,
    senderHash: input.senderHash,
    threadHash: input.threadHash,
    mode: input.mode,
    modelSlug: input.modelSlug,
    provider: input.provider,
    outputCount: input.outputCount,
    ...(input.durationSeconds === undefined ? {} : { durationSeconds: input.durationSeconds }),
    reservedCents: input.reservedCents,
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

const JOB_TRANSITIONS: Readonly<Record<StudioJobState, readonly StudioJobState[]>> = Object.freeze({
  received: ["validating", "maintenance_rejected", "failed"],
  validating: ["admitted", "failed", "maintenance_rejected"],
  admitted: ["executing", "canceled"],
  executing: ["executing", "ready", "failed", "canceled", "reconciliation_required"],
  ready: ["delivering", "canceled"],
  delivering: ["delivered", "failed", "reconciliation_required"],
  delivered: [],
  failed: [],
  canceled: [],
  maintenance_rejected: [],
  reconciliation_required: ["executing", "delivering", "delivered", "failed", "canceled"],
});

const OUTBOX_TRANSITIONS: Readonly<Record<StudioOutboxState, readonly StudioOutboxState[]>> = Object.freeze({
  intent: ["sending"],
  sending: ["sent", "retryable_failure", "failed", "send_unknown"],
  retryable_failure: ["sending", "failed"],
  send_unknown: ["sent", "confirmed_absent"],
  confirmed_absent: ["intent", "failed"],
  sent: [],
  failed: [],
});

const ADMIT_JOB_LUA = `
-- AIR_STUDIO_ADMIT_JOB_V2
local existing = redis.call('GET', KEYS[4])
if existing then return {'existing', existing} end
local active = redis.call('GET', KEYS[1])
if active then return {'active_job'} end
local userCount = tonumber(redis.call('GET', KEYS[2]) or '0')
if userCount >= tonumber(ARGV[5]) then return {'user_limit'} end
local spend = tonumber(redis.call('GET', KEYS[3]) or '0')
local cost = tonumber(ARGV[7])
if spend + cost > tonumber(ARGV[6]) then return {'daily_cap'} end
-- An active-job lease must outlive the daily accounting window. Otherwise a
-- job that crosses UTC midnight could admit a second concurrent job.
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[8])
redis.call('SET', KEYS[2], tostring(userCount + 1), 'EX', ARGV[3])
redis.call('SET', KEYS[3], tostring(spend + cost), 'EX', ARGV[3])
redis.call('SET', KEYS[4], ARGV[2], 'EX', ARGV[4])
return {'admitted'}
`;

const PERSIST_INBOX_LUA = `
-- AIR_STUDIO_PERSIST_INBOX_V2
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
`;

const CLAIM_INBOX_LUA = `
-- AIR_STUDIO_CLAIM_INBOX_V2
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= 'received' and record.state ~= 'claimed' then return false end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]) ~= 'OK' then return false end
record.state = 'claimed'
record.stateVersion = record.stateVersion + 1
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + tonumber(ARGV[2]), ARGV[5])
return cjson.encode(record)
`;

const FINISH_INBOX_LUA = `
-- AIR_STUDIO_FINISH_INBOX_V2
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local record = cjson.decode(raw)
if record.state ~= 'claimed' then return 0 end
record.state = ARGV[2]
record.stateVersion = record.stateVersion + 1
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
redis.call('ZREM', KEYS[3], ARGV[5])
redis.call('DEL', KEYS[2])
return 1
`;

const RETRY_INBOX_LUA = `
-- AIR_STUDIO_RETRY_INBOX_V2
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local record = cjson.decode(raw)
if record.state ~= 'claimed' then return 0 end
record.state = 'received'
record.stateVersion = record.stateVersion + 1
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
redis.call('ZADD', KEYS[3], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[5])
redis.call('DEL', KEYS[2])
return 1
`;

const PERSIST_OUTBOX_LUA = `
-- AIR_STUDIO_PERSIST_OUTBOX_V2
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
-- Sequence numbers are per sender/lane. Reject an ambiguous duplicate rather
-- than silently choosing an arbitrary order for two user-visible bubbles.
if #redis.call('ZRANGEBYSCORE', KEYS[3], ARGV[5], ARGV[5]) > 0 then return -1 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('ZADD', KEYS[3], ARGV[5], ARGV[4])
return 1
`;

const CLAIM_OUTBOX_LUA = `
-- AIR_STUDIO_CLAIM_OUTBOX_V2
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= 'intent' and record.state ~= 'retryable_failure' then return false end
local laneHead = redis.call('ZRANGE', KEYS[4], 0, 0)
if #laneHead ~= 1 or laneHead[1] ~= ARGV[5] then return false end
if record.state == 'retryable_failure' and record.attempts >= tonumber(ARGV[6]) then
  record.state = 'failed'
  record.stateVersion = record.stateVersion + 1
  record.updatedAtMs = tonumber(ARGV[3])
  redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
  redis.call('ZREM', KEYS[3], ARGV[5])
  redis.call('ZREM', KEYS[4], ARGV[5])
  return false
end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]) ~= 'OK' then return false end
record.state = 'sending'
record.attempts = record.attempts + 1
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[3])
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + tonumber(ARGV[2]), ARGV[5])
return cjson.encode(record)
`;

const FINISH_OUTBOX_LUA = `
-- AIR_STUDIO_FINISH_OUTBOX_V2
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return false end
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
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[4])
redis.call('DEL', KEYS[2])
if nextState == 'retryable_failure' then
  redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + (record.attempts * tonumber(ARGV[7])), ARGV[5])
else
  redis.call('ZREM', KEYS[3], ARGV[5])
  -- An unknown send deliberately remains the lane head: allowing the next
  -- bubble through would reorder a conversation around an ambiguous delivery.
  if nextState ~= 'send_unknown' then redis.call('ZREM', KEYS[4], ARGV[5]) end
end
return cjson.encode(record)
`;

const RECONCILE_EXPIRED_OUTBOX_LUA = `
-- AIR_STUDIO_RECONCILE_EXPIRED_OUTBOX_V2
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
if redis.call('GET', KEYS[2]) then return 0 end
local record = cjson.decode(raw)
if record.state ~= 'sending' then return 0 end
record.state = 'send_unknown'
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[1])
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[3])
-- Keep KEYS[4]'s lane head as a deliberate fail-closed pause.
return 1
`;

const COMPARE_AND_TRANSITION_JOB_LUA = `
-- AIR_STUDIO_TRANSITION_JOB_V2
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local record = cjson.decode(raw)
if record.state ~= ARGV[1] or record.stateVersion ~= tonumber(ARGV[2]) then return false end
record.state = ARGV[3]
record.stateVersion = record.stateVersion + 1
record.updatedAtMs = tonumber(ARGV[4])
redis.call('SET', KEYS[1], cjson.encode(record), 'EX', ARGV[5])
-- Never delete a newer job's active marker after an old job's terminal
-- transition. The value is the fencing identity for this particular job.
if ARGV[7] == '1' and redis.call('GET', KEYS[2]) == ARGV[6] then redis.call('DEL', KEYS[2]) end
return cjson.encode(record)
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

function hasValidAdmissionBounds(input: AdmitStudioJobInput): boolean {
  const { job } = input;
  return (
    input.activeJobLimit === 1
    && isPositiveSafeInteger(input.dailyUserJobLimit)
    && isNonnegativeSafeInteger(input.perJobCapCents)
    && isNonnegativeSafeInteger(input.dailySpendCapCents)
    && job.state === "admitted"
    && job.stateVersion === 1
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
