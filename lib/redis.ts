import { randomBytes } from "node:crypto";

/**
 * The small Redis surface Air needs.  Application code depends on this shape,
 * not on a particular Redis client.  A production adapter should normalize its
 * client to these return values and preserve the atomic `eval` calls.
 */
export interface RedisLike {
  get(key: string): Promise<string | null>;
  set(
    key: string,
    value: string,
    options?: RedisSetOptions,
  ): Promise<"OK" | null>;
  del(...keys: string[]): Promise<number>;
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<boolean>;
  pttl(key: string): Promise<number>;
  zAdd(key: string, entries: readonly RedisSortedSetEntry[]): Promise<number>;
  zRangeByScore(
    key: string,
    min: number,
    max: number,
    options?: { limit?: number; offset?: number },
  ): Promise<string[]>;
  zRem(key: string, ...members: string[]): Promise<number>;
  zRemRangeByScore(key: string, min: number, max: number): Promise<number>;
  eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown>;
}

export interface RedisSetOptions {
  /** Set only if the key does not already exist. */
  nx?: boolean;
  /** Set only if the key already exists. */
  xx?: boolean;
  /** Expiry in seconds. */
  ex?: number;
  /** Expiry in milliseconds. */
  px?: number;
}

export interface RedisSortedSetEntry {
  score: number;
  value: string;
}

/** The subset of node-redis v5 used by the generic Air adapter below. */
export interface NodeRedisCompatibleClient {
  get(key: string): Promise<string | null>;
  set(
    key: string,
    value: string,
    options?: { NX?: true; XX?: true; EX?: number; PX?: number },
  ): Promise<string | null>;
  del(keys: string | string[]): Promise<number>;
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<number | boolean>;
  pTTL(key: string): Promise<number>;
  zAdd(key: string, entries: readonly RedisSortedSetEntry[]): Promise<number>;
  zRangeByScore(
    key: string,
    min: number,
    max: number,
    options?: { LIMIT?: { offset: number; count: number } },
  ): Promise<string[]>;
  zRem(key: string, members: string | string[]): Promise<number>;
  zRemRangeByScore(key: string, min: number, max: number): Promise<number>;
  eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown>;
}

/**
 * Wrap a connected `redis` (node-redis v5) client. This keeps application code
 * vendor-neutral while still using server-side Lua for the security-sensitive
 * lease and rate-limit operations.
 */
export function createNodeRedisAdapter(client: NodeRedisCompatibleClient): RedisLike {
  const adapter: RedisLike = {
    get: (key) => client.get(key),
    async set(key, value, options = {}) {
      const nodeOptions: { NX?: true; XX?: true; EX?: number; PX?: number } = {};
      if (options.nx) nodeOptions.NX = true;
      if (options.xx) nodeOptions.XX = true;
      if (options.ex !== undefined) nodeOptions.EX = options.ex;
      if (options.px !== undefined) nodeOptions.PX = options.px;
      const result = await client.set(
        key,
        value,
        Object.keys(nodeOptions).length > 0 ? nodeOptions : undefined,
      );
      return result === "OK" ? "OK" : null;
    },
    del: (...keys) => client.del(keys),
    incr: (key) => client.incr(key),
    async expire(key, seconds) {
      const result = await client.expire(key, seconds);
      return result === true || result === 1;
    },
    pttl: (key) => client.pTTL(key),
    zAdd: (key, entries) => client.zAdd(key, entries),
    zRangeByScore: (key, min, max, options) =>
      client.zRangeByScore(
        key,
        min,
        max,
        options?.limit === undefined
          ? undefined
          : { LIMIT: { offset: options.offset ?? 0, count: options.limit } },
      ),
    zRem: (key, ...members) => client.zRem(key, members),
    zRemRangeByScore: (key, min, max) => client.zRemRangeByScore(key, min, max),
    eval: (script, options) => client.eval(script, options),
  };
  return Object.freeze(adapter);
}

export interface ConnectedRedis {
  readonly redis: RedisLike;
  /** Fires after a connected client's transport fails, without exposing the error. */
  onOperationalError(listener: () => void): () => void;
  /** Immediate teardown for an expired inbound durability budget. */
  destroy(): void;
  close(): Promise<void>;
}

/** Cold webhook setup must leave enough time to return a signed 503 within one second. */
export const REDIS_INITIAL_CONNECT_TIMEOUT_MS = 500;

interface NodeRedisConnectionClient extends NodeRedisCompatibleClient {
  readonly isOpen: boolean;
  connect(): Promise<unknown>;
  close(): Promise<void>;
  destroy(): void;
  on(event: "error", listener: (error: Error) => void): unknown;
}

export interface RedisConnectionOptions {
  /** Test-only override; production callers use the fixed fail-fast budget. */
  readonly connectTimeoutMs?: number | undefined;
  /** Test-only factory so timeout/error handling is independently verifiable. */
  readonly clientFactory?: ((options: {
    readonly url: string;
    readonly socket: {
      readonly connectTimeout: number;
      readonly reconnectStrategy: false;
    };
  }) => NodeRedisConnectionClient) | undefined;
}

/**
 * Production convenience factory. Call once from a module-level lazy promise,
 * not inside every Vercel request handler. It performs no connection at import
 * time, so unit tests stay credential-free.
 */
export async function connectRedisFromUrl(
  redisUrl: string,
  options: RedisConnectionOptions = {},
): Promise<ConnectedRedis> {
  let url: URL;
  try {
    url = new URL(redisUrl);
  } catch {
    throw new Error("REDIS_URL is invalid");
  }
  if (url.protocol !== "redis:" && url.protocol !== "rediss:") {
    throw new Error("REDIS_URL must use redis:// or rediss://");
  }

  const connectTimeoutMs = options.connectTimeoutMs ?? REDIS_INITIAL_CONNECT_TIMEOUT_MS;
  if (!Number.isInteger(connectTimeoutMs) || connectTimeoutMs < 1 || connectTimeoutMs > 1_000) {
    throw new Error("Redis connection timeout is invalid");
  }
  const connectionOptions = Object.freeze({
    url: redisUrl,
    socket: Object.freeze({ connectTimeout: connectTimeoutMs, reconnectStrategy: false as const }),
  });
  const client = options.clientFactory
    ? options.clientFactory(connectionOptions)
    : (await import("redis")).createClient(connectionOptions) as unknown as NodeRedisConnectionClient;
  const operationalErrorListeners = new Set<() => void>();
  // node-redis emits EventEmitter errors for transport failures. Always attach
  // a redacted listener before connect so a transient socket fault cannot crash
  // a serverless invocation; runtime consumers decide whether to reinitialize.
  client.on("error", () => {
    for (const listener of operationalErrorListeners) {
      try {
        listener();
      } catch {
        // Error reporting must never rethrow through node-redis's EventEmitter.
      }
    }
  });
  try {
    await settleWithin(client.connect(), connectTimeoutMs);
  } catch {
    // A promise that outlives the webhook budget must not leave a reconnecting
    // socket behind in a warm function. `destroy` is safe before and after a
    // successful open and rejects queued commands without retrying them.
    client.destroy();
    // A caller can log the fixed error category without exposing URI/credentials.
    throw new Error("Redis connection failed");
  }
  const compatible = client as unknown as NodeRedisCompatibleClient;
  return Object.freeze({
    redis: createNodeRedisAdapter(compatible),
    onOperationalError(listener: () => void): () => void {
      operationalErrorListeners.add(listener);
      return () => operationalErrorListeners.delete(listener);
    },
    destroy(): void {
      client.destroy();
    },
    async close(): Promise<void> {
      if (client.isOpen) await client.close();
    },
  });
}

/** Attach handlers to the connection promise so a timed-out attempt is never unhandled later. */
function settleWithin<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error("Redis connection timed out"));
    }, timeoutMs);
    void operation.then(
      (value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

/**
 * Encodes externally supplied identifiers before they become part of a Redis
 * key. It is deliberately reversible only for provider IDs (never sender
 * handles); sender and thread values must be HMAC hashes before this point.
 */
export function encodeRedisKeyPart(value: string): string {
  if (!value || value.length > 512) {
    throw new Error("invalid Redis key component");
  }
  return Buffer.from(value, "utf8").toString("base64url");
}

/** SHA-256 HMAC values used as sender/thread pseudonyms are 64 lowercase hex characters. */
export function assertPseudonymousHash(value: string, label = "hash"): void {
  if (!/^[a-f0-9]{64}$/.test(value)) {
    throw new Error(`${label} must be a SHA-256 HMAC hex digest`);
  }
}

export const COMPARE_AND_DELETE_LUA = `
-- AIR_COMPARE_AND_DELETE_V1
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`;

export const COMPARE_AND_PEXPIRE_LUA = `
-- AIR_COMPARE_AND_PEXPIRE_V1
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`;

/**
 * Fenced replacement for encrypted active-turn records. A plain GET followed
 * by SET can overwrite a newer attempt after a serverless retry; this script
 * changes the value and its lease together only when the exact prior bytes
 * still match.
 */
export const COMPARE_AND_SET_WITH_PX_LUA = `
-- AIR_COMPARE_AND_SET_WITH_PX_V1
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
  return 1
end
return 0
`;

/**
 * Atomically publishes a newly verified inbox delivery.  Keeping sequence
 * allocation, the two durable records, and both queue indexes in one Redis
 * script means a process can never leave an earlier durable delivery absent
 * from its thread queue for a later delivery to overtake.
 *
 * KEYS: metadata record, encrypted payload record, global pending index,
 *       per-thread queue, per-thread sequence counter.
 * ARGV: redacted metadata JSON template, metadata TTL seconds, encrypted
 *       record JSON template, payload TTL seconds, pending score, record key,
 *       sequence-counter TTL seconds.
 *
 * Both JSON templates must contain exactly one `"threadSequence":-1` marker.
 * Redis replaces it with the atomically minted sequence before storing them.
 */
export const PERSIST_INBOX_NEW_RECORD_LUA = `
-- AIR_PERSIST_INBOX_NEW_RECORD_V1
if redis.call("EXISTS", KEYS[1]) == 1 then
  return { 0, 0 }
end

local markerPattern = '"threadSequence":%-1'
local _, metadataMarkers = string.gsub(ARGV[1], markerPattern, "")
local _, payloadMarkers = string.gsub(ARGV[3], markerPattern, "")
if metadataMarkers ~= 1 or payloadMarkers ~= 1 then
  return redis.error_reply("invalid inbox sequence template")
end

local sequence = redis.call("INCR", KEYS[5])
redis.call("EXPIRE", KEYS[5], ARGV[7])
local replacement = '"threadSequence":' .. sequence
local metadataJson = string.gsub(ARGV[1], markerPattern, replacement, 1)
local payloadJson = string.gsub(ARGV[3], markerPattern, replacement, 1)

redis.call("SET", KEYS[1], metadataJson, "NX", "EX", ARGV[2])
redis.call("SET", KEYS[2], payloadJson, "EX", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
redis.call("ZADD", KEYS[4], sequence, ARGV[6])
return { 1, sequence }
`;

/**
 * Atomically creates the two physical records and the pending-work index for
 * an outbound response. Redis Lua has no externally visible command boundary,
 * so a process can lose the response to this call without leaving a
 * metadata-only response that a later recovery would dead-letter.
 *
 * KEYS: redacted metadata record, encrypted payload record, pending index,
 *       send-unknown operator index.
 * ARGV: redacted metadata JSON, metadata TTL seconds, encrypted record JSON,
 *       payload TTL seconds, pending score, record key.
 * Returns 1 when created, 0 when the metadata authority already exists, and
 * -1 when an impossible orphan payload exists without metadata.
 */
export const PERSIST_OUTBOX_NEW_RECORD_LUA = `
-- AIR_PERSIST_OUTBOX_NEW_RECORD_V1
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return -1
end

redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
redis.call("SET", KEYS[2], ARGV[3], "NX", "EX", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
redis.call("ZREM", KEYS[4], ARGV[6])
return 1
`;

/**
 * Atomically replaces an outbox state, its encrypted payload (or deliberate
 * deletion), and both discovery indexes. This protects the ambiguous
 * provider-success path: no caller can observe a missing payload paired with
 * stale `sending` metadata and incorrectly turn a confirmed send into a
 * dead-letter.
 *
 * KEYS: redacted metadata record, encrypted payload record, pending index,
 *       send-unknown operator index.
 * ARGV: redacted metadata JSON, metadata TTL seconds, encrypted payload JSON,
 *       payload TTL seconds, payload-present flag (1/0), index mode
 *       (pending/send_unknown/terminal), index score, record key.
 */
export const WRITE_OUTBOX_RECORD_LUA = `
-- AIR_WRITE_OUTBOX_RECORD_V1
local payloadPresent = ARGV[5]
local indexMode = ARGV[6]
if (payloadPresent ~= "1" and payloadPresent ~= "0") then
  return redis.error_reply("invalid outbox payload flag")
end
if (indexMode ~= "pending" and indexMode ~= "send_unknown" and indexMode ~= "terminal") then
  return redis.error_reply("invalid outbox index mode")
end

if payloadPresent == "1" then
  redis.call("SET", KEYS[2], ARGV[3], "EX", ARGV[4])
else
  redis.call("DEL", KEYS[2])
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])

if indexMode == "pending" then
  redis.call("ZADD", KEYS[3], ARGV[7], ARGV[8])
  redis.call("ZREM", KEYS[4], ARGV[8])
elseif indexMode == "send_unknown" then
  redis.call("ZREM", KEYS[3], ARGV[8])
  redis.call("ZADD", KEYS[4], ARGV[7], ARGV[8])
else
  redis.call("ZREM", KEYS[3], ARGV[8])
  redis.call("ZREM", KEYS[4], ARGV[8])
end
return 1
`;

/**
 * Atomically replaces an inbox state, encrypted envelope (or deliberate
 * deletion), and both discovery indexes. In particular, a confirmed
 * `sending -> sent` transition cannot expose stale `sending` metadata without
 * its envelope and be mislabeled as a dead letter after a process crash.
 *
 * KEYS: redacted metadata record, encrypted payload record, global pending
 *       index, per-thread queue.
 * ARGV: redacted metadata JSON, metadata TTL seconds, encrypted payload JSON,
 *       payload TTL seconds, payload-present flag (1/0), index mode
 *       (queued/terminal), pending score, record key, thread sequence.
 */
export const WRITE_INBOX_RECORD_LUA = `
-- AIR_WRITE_INBOX_RECORD_V1
local payloadPresent = ARGV[5]
local indexMode = ARGV[6]
if (payloadPresent ~= "1" and payloadPresent ~= "0") then
  return redis.error_reply("invalid inbox payload flag")
end
if (indexMode ~= "queued" and indexMode ~= "terminal") then
  return redis.error_reply("invalid inbox index mode")
end

if payloadPresent == "1" then
  redis.call("SET", KEYS[2], ARGV[3], "EX", ARGV[4])
else
  redis.call("DEL", KEYS[2])
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])

if indexMode == "queued" then
  redis.call("ZADD", KEYS[3], ARGV[7], ARGV[8])
  redis.call("ZADD", KEYS[4], ARGV[9], ARGV[8])
else
  redis.call("ZREM", KEYS[3], ARGV[8])
  redis.call("ZREM", KEYS[4], ARGV[8])
end
return 1
`;

export interface RedisLease {
  readonly key: string;
  readonly token: string;
  readonly acquiredAt: number;
  expiresAt: number;
  refresh(ttlMs?: number): Promise<boolean>;
  release(): Promise<boolean>;
}

export interface AcquireLeaseOptions {
  /** The Vercel function limit is 60 seconds; 75 seconds leaves a heartbeat margin. */
  ttlMs?: number;
  now?: () => number;
  /**
   * Optional durable owner capability. Callers that must renew a lease from a
   * later invocation can supply an opaque, high-entropy token; ordinary
   * short-lived leases continue to receive a fresh random token.
   */
  token?: string;
}

/**
 * Acquires a distributed lease with token-checked refresh and release.  Never
 * implement release as GET then DEL: that can erase another worker's renewed
 * lease after an expiry race.
 */
export async function acquireRedisLease(
  redis: RedisLike,
  key: string,
  options: AcquireLeaseOptions = {},
): Promise<RedisLease | null> {
  const ttlMs = options.ttlMs ?? 75_000;
  if (!Number.isInteger(ttlMs) || ttlMs < 1_000) {
    throw new Error("lease ttl must be at least one second");
  }

  const now = options.now ?? Date.now;
  if (options.token !== undefined && !isLeaseToken(options.token)) {
    throw new Error("lease token is invalid");
  }
  const token = options.token ?? randomBytes(32).toString("base64url");
  const acquiredAt = now();
  const result = await redis.set(key, token, { nx: true, px: ttlMs });
  if (result !== "OK") return null;

  let released = false;
  let expiresAt = acquiredAt + ttlMs;

  return {
    key,
    token,
    acquiredAt,
    get expiresAt() {
      return expiresAt;
    },
    set expiresAt(value: number) {
      expiresAt = value;
    },
    async refresh(nextTtlMs = ttlMs): Promise<boolean> {
      if (released || !Number.isInteger(nextTtlMs) || nextTtlMs < 1_000) {
        return false;
      }
      const response = await redis.eval(COMPARE_AND_PEXPIRE_LUA, {
        keys: [key],
        arguments: [token, String(nextTtlMs)],
      });
      const renewed = Number(response) === 1;
      if (renewed) expiresAt = now() + nextTtlMs;
      return renewed;
    },
    async release(): Promise<boolean> {
      if (released) return false;
      released = true;
      const response = await redis.eval(COMPARE_AND_DELETE_LUA, {
        keys: [key],
        arguments: [token],
      });
      return Number(response) === 1;
    },
  };
}

function isLeaseToken(value: string): boolean {
  // Random base64url tokens and HMAC-derived opaque identifiers are both
  // accepted. Keeping this strict prevents accidental provider values from
  // becoming Redis compare-and-delete capabilities.
  return /^[A-Za-z0-9_-]{32,128}$/.test(value);
}

export async function withRedisLease<T>(
  redis: RedisLike,
  key: string,
  work: (lease: RedisLease) => Promise<T>,
  options?: AcquireLeaseOptions,
): Promise<T | null> {
  const lease = await acquireRedisLease(redis, key, options);
  if (!lease) return null;
  try {
    return await work(lease);
  } finally {
    await lease.release();
  }
}

type MemoryValue =
  | { kind: "string"; value: string; expiresAt: number | null }
  | { kind: "zset"; value: Map<string, number>; expiresAt: number | null };

/**
 * Deterministic in-memory implementation for isolated tests.  It is not a
 * production fallback: deployed Air must provide a real Redis adapter.
 */
export class MemoryRedis implements RedisLike {
  private readonly values = new Map<string, MemoryValue>();

  public constructor(private readonly now: () => number = Date.now) {}

  async get(key: string): Promise<string | null> {
    const entry = this.getEntry(key);
    return entry?.kind === "string" ? entry.value : null;
  }

  async set(
    key: string,
    value: string,
    options: RedisSetOptions = {},
  ): Promise<"OK" | null> {
    if (options.nx && options.xx) throw new Error("NX and XX cannot be combined");
    if (options.ex !== undefined && options.px !== undefined) {
      throw new Error("EX and PX cannot be combined");
    }
    const existing = this.getEntry(key);
    if (options.nx && existing) return null;
    if (options.xx && !existing) return null;

    const expiresAt = this.expiryFromOptions(options);
    this.values.set(key, { kind: "string", value, expiresAt });
    return "OK";
  }

  async del(...keys: string[]): Promise<number> {
    let deleted = 0;
    for (const key of keys) {
      if (this.getEntry(key)) {
        this.values.delete(key);
        deleted += 1;
      }
    }
    return deleted;
  }

  async incr(key: string): Promise<number> {
    const entry = this.getEntry(key);
    const previous = entry?.kind === "string" ? Number(entry.value) : 0;
    if (!Number.isSafeInteger(previous)) throw new Error("value is not an integer");
    const next = previous + 1;
    this.values.set(key, {
      kind: "string",
      value: String(next),
      expiresAt: entry?.expiresAt ?? null,
    });
    return next;
  }

  async expire(key: string, seconds: number): Promise<boolean> {
    if (!Number.isInteger(seconds) || seconds < 1) throw new Error("invalid expiry");
    const entry = this.getEntry(key);
    if (!entry) return false;
    entry.expiresAt = this.now() + seconds * 1_000;
    return true;
  }

  async pttl(key: string): Promise<number> {
    const entry = this.getEntry(key);
    if (!entry) return -2;
    if (entry.expiresAt === null) return -1;
    return Math.max(0, entry.expiresAt - this.now());
  }

  async zAdd(key: string, entries: readonly RedisSortedSetEntry[]): Promise<number> {
    const entry = this.getZSet(key, true);
    let added = 0;
    for (const item of entries) {
      if (!Number.isFinite(item.score)) throw new Error("sorted-set score must be finite");
      if (!entry.value.has(item.value)) added += 1;
      entry.value.set(item.value, item.score);
    }
    return added;
  }

  async zRangeByScore(
    key: string,
    min: number,
    max: number,
    options: { limit?: number; offset?: number } = {},
  ): Promise<string[]> {
    const entry = this.getZSet(key, false);
    if (!entry) return [];
    const values = [...entry.value.entries()]
      .filter(([, score]) => score >= min && score <= max)
      .sort(([firstValue, firstScore], [secondValue, secondScore]) =>
        firstScore === secondScore
          ? firstValue.localeCompare(secondValue)
          : firstScore - secondScore,
      )
      .map(([value]) => value);
    const offset = options.offset ?? 0;
    if (!Number.isInteger(offset) || offset < 0) throw new Error("sorted-set offset must be a non-negative integer");
    return options.limit === undefined ? values.slice(offset) : values.slice(offset, offset + options.limit);
  }

  async zRem(key: string, ...members: string[]): Promise<number> {
    const entry = this.getZSet(key, false);
    if (!entry) return 0;
    let removed = 0;
    for (const member of members) {
      if (entry.value.delete(member)) removed += 1;
    }
    if (entry.value.size === 0) this.values.delete(key);
    return removed;
  }

  async zRemRangeByScore(key: string, min: number, max: number): Promise<number> {
    const entry = this.getZSet(key, false);
    if (!entry) return 0;
    let removed = 0;
    for (const [member, score] of entry.value) {
      if (score >= min && score <= max) {
        entry.value.delete(member);
        removed += 1;
      }
    }
    if (entry.value.size === 0) this.values.delete(key);
    return removed;
  }

  async eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown> {
    if (script.includes("AIR_COMPARE_AND_DELETE_V1")) {
      const key = options.keys[0];
      const token = options.arguments[0];
      if (!key || !token) throw new Error("invalid compare-and-delete arguments");
      return (await this.get(key)) === token ? this.del(key) : 0;
    }
    if (script.includes("AIR_COMPARE_AND_PEXPIRE_V1")) {
      const key = options.keys[0];
      const token = options.arguments[0];
      const ttl = options.arguments[1];
      if (!key || !token || !ttl) throw new Error("invalid compare-and-expire arguments");
      if ((await this.get(key)) !== token) return 0;
      const entry = this.getEntry(key);
      if (!entry) return 0;
      const ttlMs = Number(ttl);
      if (!Number.isInteger(ttlMs) || ttlMs < 1) return 0;
      entry.expiresAt = this.now() + ttlMs;
      return 1;
    }
    if (script.includes("AIR_COMPARE_AND_SET_WITH_PX_V1")) {
      const key = options.keys[0];
      const expected = options.arguments[0];
      const next = options.arguments[1];
      const ttl = options.arguments[2];
      if (!key || expected === undefined || next === undefined || !ttl) {
        throw new Error("invalid compare-and-set arguments");
      }
      if ((await this.get(key)) !== expected) return 0;
      const ttlMs = Number(ttl);
      if (!Number.isInteger(ttlMs) || ttlMs < 1) return 0;
      this.values.set(key, {
        kind: "string",
        value: next,
        expiresAt: this.now() + ttlMs,
      });
      return 1;
    }
    if (script.includes("AIR_PERSIST_INBOX_NEW_RECORD_V1")) {
      return this.evalPersistInboxNewRecord(options.keys, options.arguments);
    }
    if (script.includes("AIR_PERSIST_OUTBOX_NEW_RECORD_V1")) {
      return this.evalPersistOutboxNewRecord(options.keys, options.arguments);
    }
    if (script.includes("AIR_WRITE_OUTBOX_RECORD_V1")) {
      return this.evalWriteOutboxRecord(options.keys, options.arguments);
    }
    if (script.includes("AIR_WRITE_INBOX_RECORD_V1")) {
      return this.evalWriteInboxRecord(options.keys, options.arguments);
    }
    if (script.includes("AIR_RATE_LIMIT_V1")) {
      return this.evalRateLimit(options.keys, options.arguments);
    }
    throw new Error("MemoryRedis does not implement this Lua script");
  }

  private evalPersistInboxNewRecord(
    keys: readonly string[],
    args: readonly string[],
  ): readonly [number, number] {
    const metadataKey = keys[0];
    const payloadKey = keys[1];
    const pendingKey = keys[2];
    const threadQueueKey = keys[3];
    const sequenceKey = keys[4];
    const metadataTemplate = args[0];
    const metadataTtlValue = args[1];
    const payloadTemplate = args[2];
    const payloadTtlValue = args[3];
    const pendingScoreValue = args[4];
    const recordKey = args[5];
    const sequenceTtlValue = args[6];
    if (
      !metadataKey ||
      !payloadKey ||
      !pendingKey ||
      !threadQueueKey ||
      !sequenceKey ||
      metadataTemplate === undefined ||
      metadataTtlValue === undefined ||
      payloadTemplate === undefined ||
      payloadTtlValue === undefined ||
      pendingScoreValue === undefined ||
      !recordKey ||
      sequenceTtlValue === undefined
    ) {
      throw new Error("invalid inbox publish script arguments");
    }
    if (this.getEntry(metadataKey)) return [0, 0];

    const marker = '"threadSequence":-1';
    if (
      metadataTemplate.split(marker).length !== 2 ||
      payloadTemplate.split(marker).length !== 2
    ) {
      throw new Error("invalid inbox sequence template");
    }
    const metadataTtlSeconds = Number(metadataTtlValue);
    const payloadTtlSeconds = Number(payloadTtlValue);
    const sequenceTtlSeconds = Number(sequenceTtlValue);
    const pendingScore = Number(pendingScoreValue);
    if (
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      !Number.isSafeInteger(payloadTtlSeconds) ||
      payloadTtlSeconds < 1 ||
      !Number.isSafeInteger(sequenceTtlSeconds) ||
      sequenceTtlSeconds < 1 ||
      !Number.isFinite(pendingScore)
    ) {
      throw new Error("invalid inbox publish script arguments");
    }

    const sequenceEntry = this.getEntry(sequenceKey);
    const priorSequence = sequenceEntry?.kind === "string" ? Number(sequenceEntry.value) : 0;
    if (!Number.isSafeInteger(priorSequence) || priorSequence < 0) {
      throw new Error("inbox thread sequence is invalid");
    }
    const sequence = priorSequence + 1;
    if (!Number.isSafeInteger(sequence)) throw new Error("inbox thread sequence is invalid");

    const now = this.now();
    const replacement = `"threadSequence":${String(sequence)}`;
    this.values.set(metadataKey, {
      kind: "string",
      value: metadataTemplate.replace(marker, replacement),
      expiresAt: now + metadataTtlSeconds * 1_000,
    });
    this.values.set(payloadKey, {
      kind: "string",
      value: payloadTemplate.replace(marker, replacement),
      expiresAt: now + payloadTtlSeconds * 1_000,
    });
    this.values.set(sequenceKey, {
      kind: "string",
      value: String(sequence),
      expiresAt: now + sequenceTtlSeconds * 1_000,
    });
    this.getZSet(pendingKey, true).value.set(recordKey, pendingScore);
    this.getZSet(threadQueueKey, true).value.set(recordKey, sequence);
    return [1, sequence];
  }

  private evalPersistOutboxNewRecord(
    keys: readonly string[],
    args: readonly string[],
  ): number {
    const metadataKey = keys[0];
    const payloadKey = keys[1];
    const pendingKey = keys[2];
    const sendUnknownKey = keys[3];
    const metadata = args[0];
    const metadataTtlValue = args[1];
    const payload = args[2];
    const payloadTtlValue = args[3];
    const pendingScoreValue = args[4];
    const recordKey = args[5];
    if (
      !metadataKey ||
      !payloadKey ||
      !pendingKey ||
      !sendUnknownKey ||
      metadata === undefined ||
      metadataTtlValue === undefined ||
      payload === undefined ||
      payloadTtlValue === undefined ||
      pendingScoreValue === undefined ||
      !recordKey
    ) {
      throw new Error("invalid outbox publish script arguments");
    }
    if (this.getEntry(metadataKey)) return 0;
    if (this.getEntry(payloadKey)) return -1;
    const metadataTtlSeconds = Number(metadataTtlValue);
    const payloadTtlSeconds = Number(payloadTtlValue);
    const pendingScore = Number(pendingScoreValue);
    if (
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      !Number.isSafeInteger(payloadTtlSeconds) ||
      payloadTtlSeconds < 1 ||
      !Number.isFinite(pendingScore)
    ) {
      throw new Error("invalid outbox publish script arguments");
    }
    const now = this.now();
    this.values.set(metadataKey, {
      kind: "string",
      value: metadata,
      expiresAt: now + metadataTtlSeconds * 1_000,
    });
    this.values.set(payloadKey, {
      kind: "string",
      value: payload,
      expiresAt: now + payloadTtlSeconds * 1_000,
    });
    this.getZSet(pendingKey, true).value.set(recordKey, pendingScore);
    const sendUnknown = this.getZSet(sendUnknownKey, false);
    if (sendUnknown?.value.delete(recordKey) && sendUnknown.value.size === 0) {
      this.values.delete(sendUnknownKey);
    }
    return 1;
  }

  private evalWriteOutboxRecord(
    keys: readonly string[],
    args: readonly string[],
  ): number {
    const metadataKey = keys[0];
    const payloadKey = keys[1];
    const pendingKey = keys[2];
    const sendUnknownKey = keys[3];
    const metadata = args[0];
    const metadataTtlValue = args[1];
    const payload = args[2];
    const payloadTtlValue = args[3];
    const payloadPresent = args[4];
    const indexMode = args[5];
    const indexScoreValue = args[6];
    const recordKey = args[7];
    if (
      !metadataKey ||
      !payloadKey ||
      !pendingKey ||
      !sendUnknownKey ||
      metadata === undefined ||
      metadataTtlValue === undefined ||
      payload === undefined ||
      payloadTtlValue === undefined ||
      (payloadPresent !== "0" && payloadPresent !== "1") ||
      (indexMode !== "pending" && indexMode !== "send_unknown" && indexMode !== "terminal") ||
      indexScoreValue === undefined ||
      !recordKey
    ) {
      throw new Error("invalid outbox write script arguments");
    }
    const metadataTtlSeconds = Number(metadataTtlValue);
    const payloadTtlSeconds = Number(payloadTtlValue);
    const indexScore = Number(indexScoreValue);
    if (
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      (payloadPresent === "1" && (!Number.isSafeInteger(payloadTtlSeconds) || payloadTtlSeconds < 1)) ||
      !Number.isFinite(indexScore)
    ) {
      throw new Error("invalid outbox write script arguments");
    }
    const now = this.now();
    if (payloadPresent === "1") {
      this.values.set(payloadKey, {
        kind: "string",
        value: payload,
        expiresAt: now + payloadTtlSeconds * 1_000,
      });
    } else {
      this.values.delete(payloadKey);
    }
    this.values.set(metadataKey, {
      kind: "string",
      value: metadata,
      expiresAt: now + metadataTtlSeconds * 1_000,
    });

    const pending = this.getZSet(pendingKey, false);
    const sendUnknown = this.getZSet(sendUnknownKey, false);
    if (indexMode === "pending") {
      this.getZSet(pendingKey, true).value.set(recordKey, indexScore);
      if (sendUnknown?.value.delete(recordKey) && sendUnknown.value.size === 0) {
        this.values.delete(sendUnknownKey);
      }
    } else if (indexMode === "send_unknown") {
      if (pending?.value.delete(recordKey) && pending.value.size === 0) this.values.delete(pendingKey);
      this.getZSet(sendUnknownKey, true).value.set(recordKey, indexScore);
    } else {
      if (pending?.value.delete(recordKey) && pending.value.size === 0) this.values.delete(pendingKey);
      if (sendUnknown?.value.delete(recordKey) && sendUnknown.value.size === 0) {
        this.values.delete(sendUnknownKey);
      }
    }
    return 1;
  }

  private evalWriteInboxRecord(
    keys: readonly string[],
    args: readonly string[],
  ): number {
    const metadataKey = keys[0];
    const payloadKey = keys[1];
    const pendingKey = keys[2];
    const threadQueueKey = keys[3];
    const metadata = args[0];
    const metadataTtlValue = args[1];
    const payload = args[2];
    const payloadTtlValue = args[3];
    const payloadPresent = args[4];
    const indexMode = args[5];
    const pendingScoreValue = args[6];
    const recordKey = args[7];
    const sequenceValue = args[8];
    if (
      !metadataKey ||
      !payloadKey ||
      !pendingKey ||
      !threadQueueKey ||
      metadata === undefined ||
      metadataTtlValue === undefined ||
      payload === undefined ||
      payloadTtlValue === undefined ||
      (payloadPresent !== "0" && payloadPresent !== "1") ||
      (indexMode !== "queued" && indexMode !== "terminal") ||
      pendingScoreValue === undefined ||
      !recordKey ||
      sequenceValue === undefined
    ) {
      throw new Error("invalid inbox write script arguments");
    }
    const metadataTtlSeconds = Number(metadataTtlValue);
    const payloadTtlSeconds = Number(payloadTtlValue);
    const pendingScore = Number(pendingScoreValue);
    const threadSequence = Number(sequenceValue);
    if (
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      (payloadPresent === "1" && (!Number.isSafeInteger(payloadTtlSeconds) || payloadTtlSeconds < 1)) ||
      !Number.isFinite(pendingScore) ||
      !Number.isSafeInteger(threadSequence) ||
      threadSequence < 1
    ) {
      throw new Error("invalid inbox write script arguments");
    }
    const now = this.now();
    if (payloadPresent === "1") {
      this.values.set(payloadKey, {
        kind: "string",
        value: payload,
        expiresAt: now + payloadTtlSeconds * 1_000,
      });
    } else {
      this.values.delete(payloadKey);
    }
    this.values.set(metadataKey, {
      kind: "string",
      value: metadata,
      expiresAt: now + metadataTtlSeconds * 1_000,
    });
    const pending = this.getZSet(pendingKey, false);
    const threadQueue = this.getZSet(threadQueueKey, false);
    if (indexMode === "queued") {
      this.getZSet(pendingKey, true).value.set(recordKey, pendingScore);
      this.getZSet(threadQueueKey, true).value.set(recordKey, threadSequence);
    } else {
      if (pending?.value.delete(recordKey) && pending.value.size === 0) this.values.delete(pendingKey);
      if (threadQueue?.value.delete(recordKey) && threadQueue.value.size === 0) {
        this.values.delete(threadQueueKey);
      }
    }
    return 1;
  }

  private evalRateLimit(
    keys: readonly string[],
    args: readonly string[],
  ): readonly [number, string, number, number, number, number, number] {
    const minuteKey = keys[0];
    const dayKey = keys[1];
    const minuteNoticeKey = keys[2];
    const dayNoticeKey = keys[3];
    const nowValue = args[0];
    const minuteWindowValue = args[1];
    const dayWindowValue = args[2];
    const minuteLimitValue = args[3];
    const dayLimitValue = args[4];
    const member = args[5];
    if (
      !minuteKey ||
      !dayKey ||
      !minuteNoticeKey ||
      !dayNoticeKey ||
      !nowValue ||
      !minuteWindowValue ||
      !dayWindowValue ||
      !minuteLimitValue ||
      !dayLimitValue ||
      !member
    ) {
      throw new Error("invalid rate-limit script arguments");
    }
    const now = Number(nowValue);
    const minuteWindow = Number(minuteWindowValue);
    const dayWindow = Number(dayWindowValue);
    const minuteLimit = Number(minuteLimitValue);
    const dayLimit = Number(dayLimitValue);
    if (![now, minuteWindow, dayWindow, minuteLimit, dayLimit].every(Number.isFinite)) {
      throw new Error("invalid rate-limit script arguments");
    }

    const minute = this.getZSet(minuteKey, true);
    const day = this.getZSet(dayKey, true);
    this.removeBefore(minute, now - minuteWindow);
    this.removeBefore(day, now - dayWindow);
    const minuteCount = minute.value.size;
    const dayCount = day.value.size;

    if (minute.value.has(member) || day.value.has(member)) {
      return [1, "", minuteCount, dayCount, 0, 0, 1];
    }

    const limitedByMinute = minuteCount >= minuteLimit;
    const limitedByDay = !limitedByMinute && dayCount >= dayLimit;
    if (limitedByMinute || limitedByDay) {
      const reason = limitedByMinute ? "minute" : "day";
      const set = limitedByMinute ? minute : day;
      const window = limitedByMinute ? minuteWindow : dayWindow;
      const noticeKey = limitedByMinute ? minuteNoticeKey : dayNoticeKey;
      const oldest = Math.min(...set.value.values());
      if (!Number.isFinite(oldest)) throw new Error("rate-limit set has no entries");
      const retryAfter = Math.max(1, Math.ceil((oldest + window - now) / 1_000));
      const firstNotice = !this.getEntry(noticeKey);
      if (firstNotice) {
        this.values.set(noticeKey, {
          kind: "string",
          value: "1",
          expiresAt: now + retryAfter * 1_000,
        });
      }
      return [0, reason, minuteCount, dayCount, retryAfter, firstNotice ? 1 : 0, 0];
    }

    minute.value.set(member, now);
    day.value.set(member, now);
    minute.expiresAt = now + minuteWindow;
    day.expiresAt = now + dayWindow;
    return [1, "", minuteCount + 1, dayCount + 1, 0, 0, 0];
  }

  private removeBefore(entry: Extract<MemoryValue, { kind: "zset" }>, cutoff: number): void {
    for (const [member, score] of entry.value) {
      if (score <= cutoff) entry.value.delete(member);
    }
  }

  private expiryFromOptions(options: RedisSetOptions): number | null {
    if (options.ex !== undefined) {
      if (!Number.isInteger(options.ex) || options.ex < 1) throw new Error("invalid EX expiry");
      return this.now() + options.ex * 1_000;
    }
    if (options.px !== undefined) {
      if (!Number.isInteger(options.px) || options.px < 1) throw new Error("invalid PX expiry");
      return this.now() + options.px;
    }
    return null;
  }

  private getEntry(key: string): MemoryValue | undefined {
    const entry = this.values.get(key);
    if (entry && entry.expiresAt !== null && entry.expiresAt <= this.now()) {
      this.values.delete(key);
      return undefined;
    }
    return entry;
  }

  private getZSet(
    key: string,
    create: true,
  ): Extract<MemoryValue, { kind: "zset" }>;
  private getZSet(
    key: string,
    create: false,
  ): Extract<MemoryValue, { kind: "zset" }> | undefined;
  private getZSet(
    key: string,
    create: boolean,
  ): Extract<MemoryValue, { kind: "zset" }> | undefined {
    const entry = this.getEntry(key);
    if (entry?.kind === "zset") return entry;
    if (entry) throw new Error("Redis key has the wrong type");
    if (!create) return undefined;
    const zset: Extract<MemoryValue, { kind: "zset" }> = {
      kind: "zset",
      value: new Map(),
      expiresAt: null,
    };
    this.values.set(key, zset);
    return zset;
  }
}
