import { randomBytes } from "node:crypto";
import { Redis } from "@upstash/redis";

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
  zCard(key: string): Promise<number>;
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
  zCard(key: string): Promise<number>;
  zRem(key: string, members: string | string[]): Promise<number>;
  zRemRangeByScore(key: string, min: number, max: number): Promise<number>;
  eval(
    script: string,
    options: { keys: readonly string[]; arguments: readonly string[] },
  ): Promise<unknown>;
}

/**
 * The narrow `@upstash/redis` REST surface Air uses in Vercel.  Keeping it
 * separate from the node-redis shape lets the durable queue use HTTPS without
 * leaking a transport dependency into workflow code.
 */
export interface UpstashRedisCompatibleClient {
  get<T = string>(key: string): Promise<T | null>;
  set<T = string>(
    key: string,
    value: T,
    options?: { nx?: true; xx?: true; ex?: number; px?: number },
  ): Promise<"OK" | T | null>;
  del(...keys: string[]): Promise<number>;
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<number>;
  pttl(key: string): Promise<number>;
  zadd<T = string>(
    key: string,
    entry: { score: number; member: T },
    ...entries: readonly { score: number; member: T }[]
  ): Promise<number | null>;
  zrange<T extends unknown[] = string[]>(
    key: string,
    min: number | "-inf" | "+inf",
    max: number | "-inf" | "+inf",
    options: { byScore: true; offset?: number; count?: number },
  ): Promise<T>;
  zcard(key: string): Promise<number>;
  zrem<T = string>(key: string, ...members: T[]): Promise<number>;
  zremrangebyscore(
    key: string,
    min: number | "-inf" | "+inf",
    max: number | "-inf" | "+inf",
  ): Promise<number>;
  eval<T = unknown>(script: string, keys: string[], arguments_: string[]): Promise<T>;
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
    zCard: (key) => client.zCard(key),
    zRem: (key, ...members) => client.zRem(key, members),
    zRemRangeByScore: (key, min, max) => client.zRemRangeByScore(key, min, max),
    eval: (script, options) => client.eval(script, options),
  };
  return Object.freeze(adapter);
}

/**
 * REST adapter for Upstash. Automatic deserialization stays disabled because
 * Air deliberately stores opaque encrypted JSON strings and Redis Lua payloads
 * as exact bytes, not application objects.
 */
export function createUpstashRedisAdapter(client: UpstashRedisCompatibleClient): RedisLike {
  const adapter: RedisLike = {
    async get(key) {
      const value = await client.get<string>(key);
      if (value === null || typeof value === "string") return value;
      throw new Error("Upstash Redis returned a non-string queue value");
    },
    async set(key, value, options = {}) {
      const upstashOptions: { nx?: true; xx?: true; ex?: number; px?: number } = {};
      if (options.nx) upstashOptions.nx = true;
      if (options.xx) upstashOptions.xx = true;
      if (options.ex !== undefined) upstashOptions.ex = options.ex;
      if (options.px !== undefined) upstashOptions.px = options.px;
      const result = await client.set(
        key,
        value,
        Object.keys(upstashOptions).length > 0 ? upstashOptions : undefined,
      );
      return result === "OK" ? "OK" : null;
    },
    del: (...keys) => keys.length === 0 ? Promise.resolve(0) : client.del(...keys),
    incr: (key) => client.incr(key),
    async expire(key, seconds) {
      return (await client.expire(key, seconds)) === 1;
    },
    pttl: (key) => client.pttl(key),
    async zAdd(key, entries) {
      const mapped = entries.map((entry) => ({ score: entry.score, member: entry.value }));
      const first = mapped[0];
      if (!first) return 0;
      return (await client.zadd(key, first, ...mapped.slice(1))) ?? 0;
    },
    zRangeByScore: async (key, min, max, options) => {
      const rangeOptions = options?.limit === undefined
        ? { byScore: true as const }
        : { byScore: true as const, offset: options.offset ?? 0, count: options.limit };
      return client.zrange<string[]>(
        key,
        upstashScoreBound(min),
        upstashScoreBound(max),
        rangeOptions,
      );
    },
    zCard: (key) => client.zcard(key),
    zRem: (key, ...members) => members.length === 0 ? Promise.resolve(0) : client.zrem(key, ...members),
    zRemRangeByScore: (key, min, max) =>
      client.zremrangebyscore(key, upstashScoreBound(min), upstashScoreBound(max)),
    eval: (script, options) => client.eval(script, [...options.keys], [...options.arguments]),
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
  /**
   * Preferred serverless connection for Upstash. These values are supplied
   * separately from the legacy TCP REDIS_URL and never appear in logs.
   */
  readonly upstashRestUrl?: string | undefined;
  readonly upstashRestToken?: string | undefined;
  /** Test-only factory for the serverless Upstash REST client. */
  readonly upstashClientFactory?: ((options: UpstashRedisRestOptions) => UpstashRedisCompatibleClient) | undefined;
}

export interface UpstashRedisRestOptions {
  readonly url: string;
  readonly token: string;
  /** Explicit zero retries: v1.38's `false` still issues a second request. */
  readonly retry: { readonly retries: 0 };
  readonly automaticDeserialization: false;
  readonly signal: () => AbortSignal;
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
  const hasUpstashRestUrl = options.upstashRestUrl !== undefined;
  const hasUpstashRestToken = options.upstashRestToken !== undefined;
  if (hasUpstashRestUrl || hasUpstashRestToken) {
    if (!options.upstashRestUrl?.trim() || !options.upstashRestToken?.trim()) {
      throw new Error("Upstash Redis REST configuration is incomplete");
    }
    return connectUpstashRedis(options);
  }

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

/** Prefer Upstash HTTPS in Vercel: it needs no persistent TCP socket. */
function connectUpstashRedis(options: RedisConnectionOptions): ConnectedRedis {
  const restUrl = options.upstashRestUrl as string;
  const restToken = options.upstashRestToken as string;
  let url: URL;
  try {
    url = new URL(restUrl);
  } catch {
    throw new Error("Upstash Redis REST URL is invalid");
  }
  if (
    url.protocol !== "https:" ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash ||
    restToken.length > 4_096 ||
    /[\r\n]/.test(restToken)
  ) {
    throw new Error("Upstash Redis REST configuration is invalid");
  }
  const requestTimeoutMs = REDIS_INITIAL_CONNECT_TIMEOUT_MS;
  const restOptions: UpstashRedisRestOptions = Object.freeze({
    url: restUrl,
    token: restToken,
    retry: Object.freeze({ retries: 0 as const }),
    automaticDeserialization: false,
    signal: () => AbortSignal.timeout(requestTimeoutMs),
  });
  let client: UpstashRedisCompatibleClient;
  try {
    client = options.upstashClientFactory
      ? options.upstashClientFactory(restOptions)
      : new Redis(restOptions) as unknown as UpstashRedisCompatibleClient;
  } catch {
    throw new Error("Upstash Redis REST configuration is invalid");
  }
  return Object.freeze({
    redis: createUpstashRedisAdapter(client),
    // REST failures are request-scoped; the next command receives a fresh
    // HTTPS request, so there is no socket event stream to subscribe to.
    onOperationalError(): () => void {
      return () => undefined;
    },
    destroy(): void {
      // The Upstash client owns no persistent connection to close.
    },
    async close(): Promise<void> {
      // The Upstash client owns no persistent connection to close.
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

function upstashScoreBound(value: number): number | "-inf" | "+inf" {
  if (value === Number.NEGATIVE_INFINITY) return "-inf";
  if (value === Number.POSITIVE_INFINITY) return "+inf";
  if (!Number.isFinite(value)) throw new Error("sorted-set score must be finite or infinite");
  return value;
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
 *       per-thread queue, per-thread sequence counter, received-at index,
 *       ready-thread-head index, optional rate minute/day indexes, optional
 *       rate minute/day notice keys, optional correlation decision key, and
 *       optional per-sender receipt watermark key.
 * ARGV: redacted metadata JSON template, metadata TTL seconds, encrypted
 *       record JSON template, payload TTL seconds, pending score, record key,
 *       sequence-counter TTL seconds, received-at score, thread hash, rate
 *       reservation enabled (0/1), minute/day windows, minute/day limits,
 *       and decision TTL milliseconds, then the correlation-HMAC member.
 *
 * Both JSON templates must contain exactly one `"threadSequence":-1` marker.
 * Redis replaces it with the atomically minted sequence before storing them.
 */
export const PERSIST_INBOX_NEW_RECORD_LUA = `
-- AIR_PERSIST_INBOX_NEW_RECORD_V1
if #KEYS < 7 or #ARGV < 16 then
  return redis.error_reply("invalid inbox publish script arity")
end
if redis.call("EXISTS", KEYS[1]) == 1 then
  return { 0, 0 }
end

local markerPattern = '"threadSequence":%-1'
local _, metadataMarkers = string.gsub(ARGV[1], markerPattern, "")
local _, payloadMarkers = string.gsub(ARGV[3], markerPattern, "")
if metadataMarkers ~= 1 or payloadMarkers ~= 1 then
  return redis.error_reply("invalid inbox sequence template")
end

local function isTypeOrMissing(key, expected)
  local actual = redis.call("TYPE", key).ok
  return actual == "none" or actual == expected
end

local function positiveInteger(value)
  return value and string.match(value, "^[1-9][0-9]*$") and string.len(value) <= 16
end

local function nonnegativeInteger(value)
  return value == "0" or (value and string.match(value, "^[1-9][0-9]*$") and string.len(value) <= 16)
end

local function validSequence(value)
  if not value or value == "0" then return true end
  if not string.match(value, "^[1-9][0-9]*$") then return false end
  if string.len(value) < 19 then return true end
  if string.len(value) > 19 then return false end
  return value < "9223372036854775807"
end

-- Redis Lua does not roll back prior writes when a later command errors.
-- Validate every queue write and its key type before touching rate counters.
if not positiveInteger(ARGV[2])
  or not positiveInteger(ARGV[4])
  or not positiveInteger(ARGV[7])
  or not nonnegativeInteger(ARGV[5])
  or not nonnegativeInteger(ARGV[8])
  or not ARGV[6] or ARGV[6] == ""
  or not ARGV[9] or ARGV[9] == ""
  or not isTypeOrMissing(KEYS[3], "zset")
  or not isTypeOrMissing(KEYS[4], "zset")
  or not isTypeOrMissing(KEYS[5], "string")
  or not isTypeOrMissing(KEYS[6], "zset")
  or not isTypeOrMissing(KEYS[7], "zset")
  or not validSequence(redis.call("GET", KEYS[5])) then
  return redis.error_reply("invalid inbox publish preflight")
end

-- For an eligible substantive DM, the receipt's quota decision is made in
-- this same atomic publication as the encrypted inbox record. The resulting
-- correlation decision is what a recovered worker reads later; it must never
-- consume quota after waiting behind an earlier same-thread video job.
if ARGV[10] == "1" then
  if #KEYS ~= 13 then
    return redis.error_reply("missing inbox rate reservation keys")
  end
  local now = tonumber(ARGV[8])
  local minuteWindow = tonumber(ARGV[11])
  local dayWindow = tonumber(ARGV[12])
  local minuteLimit = tonumber(ARGV[13])
  local dayLimit = tonumber(ARGV[14])
  local decisionTtl = tonumber(ARGV[15])
  if not nonnegativeInteger(ARGV[8])
    or not positiveInteger(ARGV[11])
    or not positiveInteger(ARGV[12])
    or not positiveInteger(ARGV[13])
    or not positiveInteger(ARGV[14])
    or not positiveInteger(ARGV[15])
    or decisionTtl < dayWindow
    or not ARGV[16]
    or not string.match(ARGV[16], "^[a-f0-9][a-f0-9]*$")
    or string.len(ARGV[16]) ~= 64
    or not isTypeOrMissing(KEYS[8], "zset")
    or not isTypeOrMissing(KEYS[9], "zset")
    or not isTypeOrMissing(KEYS[10], "string")
    or not isTypeOrMissing(KEYS[11], "string")
    or not isTypeOrMissing(KEYS[12], "string")
    or not isTypeOrMissing(KEYS[13], "string") then
    return redis.error_reply("invalid inbox rate reservation")
  end

  local existingDecision = redis.call("GET", KEYS[12])
  if existingDecision then
    if existingDecision ~= "a" and not string.match(existingDecision, "^[md]:[01]:%d+$") then
      return redis.error_reply("invalid inbox rate reservation decision")
    end
  else
    local watermark = redis.call("GET", KEYS[13])
    if watermark then
      if not nonnegativeInteger(watermark) then
        return redis.error_reply("invalid inbox rate receipt watermark")
      end
      now = math.max(now, tonumber(watermark))
    end
    redis.call("ZREMRANGEBYSCORE", KEYS[8], "-inf", now - minuteWindow)
    redis.call("ZREMRANGEBYSCORE", KEYS[9], "-inf", now - dayWindow)
    if redis.call("ZSCORE", KEYS[8], ARGV[16]) or redis.call("ZSCORE", KEYS[9], ARGV[16]) then
      -- Upgrade a legacy counter-only member to a durable decision without
      -- charging it again; this mirrors RATE_LIMIT_LUA's recovery branch.
      redis.call("SET", KEYS[12], "a", "PX", decisionTtl, "NX")
    else
      local minuteCount = redis.call("ZCARD", KEYS[8])
      local dayCount = redis.call("ZCARD", KEYS[9])
      local limitedByMinute = minuteCount >= minuteLimit
      local limitedByDay = (not limitedByMinute) and dayCount >= dayLimit
      if limitedByMinute or limitedByDay then
        local targetKey = limitedByMinute and KEYS[8] or KEYS[9]
        local noticeKey = limitedByMinute and KEYS[10] or KEYS[11]
        local window = limitedByMinute and minuteWindow or dayWindow
        local first = redis.call("ZRANGE", targetKey, 0, 0, "WITHSCORES")
        local retryUntil = tonumber(first[2]) + window
        local shouldNotify = redis.call("SET", noticeKey, "1", "PX", math.max(1, retryUntil - now), "NX") and 1 or 0
        local decision = (limitedByMinute and "m:" or "d:") .. shouldNotify .. ":" .. retryUntil
        redis.call("SET", KEYS[12], decision, "PX", decisionTtl, "NX")
      else
        redis.call("ZADD", KEYS[8], now, ARGV[16])
        redis.call("ZADD", KEYS[9], now, ARGV[16])
        redis.call("PEXPIRE", KEYS[8], minuteWindow)
        redis.call("PEXPIRE", KEYS[9], dayWindow)
        redis.call("SET", KEYS[12], "a", "PX", decisionTtl, "NX")
      end
    end
    redis.call("SET", KEYS[13], tostring(now), "PX", dayWindow)
  end
elseif ARGV[10] ~= "0" or #KEYS ~= 7 then
  return redis.error_reply("invalid inbox rate reservation mode")
end

local sequence = redis.call("INCR", KEYS[5])
redis.call("EXPIRE", KEYS[5], ARGV[7])
local threadWasEmpty = redis.call("ZCARD", KEYS[4]) == 0
local replacement = '"threadSequence":' .. sequence
local metadataJson = string.gsub(ARGV[1], markerPattern, replacement, 1)
local payloadJson = string.gsub(ARGV[3], markerPattern, replacement, 1)

redis.call("SET", KEYS[1], metadataJson, "NX", "EX", ARGV[2])
redis.call("SET", KEYS[2], payloadJson, "EX", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
redis.call("ZADD", KEYS[4], sequence, ARGV[6])
redis.call("ZADD", KEYS[6], ARGV[8], ARGV[6])
if threadWasEmpty then
  redis.call("ZADD", KEYS[7], ARGV[5], ARGV[9])
end
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
 *       index, per-thread queue, received-at index, ready-thread-head index.
 * ARGV: redacted metadata JSON, metadata TTL seconds, encrypted payload JSON,
 *       payload TTL seconds, payload-present flag (1/0), index mode
 *       (queued/terminal), pending score, record key, thread sequence,
 *       received-at score, thread hash.
 */
export const WRITE_INBOX_RECORD_LUA = `
-- AIR_WRITE_INBOX_RECORD_V1
local payloadPresent = ARGV[5]
local indexMode = ARGV[6]
local threadHash = ARGV[11]
if (payloadPresent ~= "1" and payloadPresent ~= "0") then
  return redis.error_reply("invalid inbox payload flag")
end
if (indexMode ~= "queued" and indexMode ~= "terminal") then
  return redis.error_reply("invalid inbox index mode")
end
if not threadHash or threadHash == "" then
  return redis.error_reply("invalid inbox thread hash")
end

local function readyScoreForThreadHead(recordKey)
  local metadata = redis.call("GET", recordKey .. ":metadata")
  if not metadata then return nil end
  local decodedOk, decoded = pcall(cjson.decode, metadata)
  if not decodedOk or type(decoded) ~= "table" then return nil end
  if decoded.state == "received" or decoded.state == "failed_retryable" then
    return redis.call("ZSCORE", KEYS[3], recordKey)
  end
  local status = decoded.status
  if type(status) ~= "table" then return nil end
  local leaseExpiry = status.processingLeaseExpiresAtMs
  if type(leaseExpiry) == "number" and leaseExpiry >= 0 then
    return tostring(leaseExpiry)
  end
  return nil
end

local function promoteReadyThreadHead()
  local heads = redis.call("ZRANGE", KEYS[4], 0, 0)
  if #heads == 0 then
    redis.call("ZREM", KEYS[6], threadHash)
    return
  end
  local score = readyScoreForThreadHead(heads[1])
  if score then
    redis.call("ZADD", KEYS[6], score, threadHash)
  else
    redis.call("ZREM", KEYS[6], threadHash)
  end
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
  redis.call("ZADD", KEYS[5], ARGV[10], ARGV[8])
else
  redis.call("ZREM", KEYS[3], ARGV[8])
  redis.call("ZREM", KEYS[4], ARGV[8])
  redis.call("ZREM", KEYS[5], ARGV[8])
end
promoteReadyThreadHead()
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

type MemoryStudioPendingState = "active" | "cleared" | "maintenance" | "expired";

interface MemoryStudioPendingControl {
  readonly schemaVersion: 1;
  readonly state: MemoryStudioPendingState;
  readonly receiptId: string;
  readonly scopeSequence: number;
  readonly revision: number;
  readonly token?: string | undefined;
  readonly expiresAtMs?: number | undefined;
}

interface MemoryStudioPendingGuard {
  readonly threadHash: string;
  readonly receiptId: string;
  readonly scopeSequence: number;
  readonly revision: number;
  readonly token: string;
  readonly expiresAtMs: number;
}

interface MemoryStudioReleaseFence {
  readonly schemaVersion: 1;
  readonly generation: number;
  readonly phase: "dark" | "canary" | "production";
}

interface MemoryStudioOnboardingMarker {
  readonly schemaVersion: 1;
  readonly onboardingVersion: number;
  readonly receiptId: string;
  readonly firstOutboxId: string;
  readonly secondOutboxId: string;
}

/** Opaque shared reservation record for one canary budget key. */
interface MemoryStudioCanaryBudget {
  readonly schemaVersion: 1;
  readonly totalReservedCents: number;
  readonly falReservedCents: number;
}

interface MemoryStudioResultCursor {
  schemaVersion: 1;
  jobId: string;
  senderHash: string;
  threadHash: string;
  outputCount: number;
  releaseStamp: Record<string, unknown>;
  nextOutputIndex: number;
  outputs: Array<{ outputIndex: number; outboxId: string }>;
  createdAtMs: number;
  updatedAtMs: number;
}

function isMemoryOpaqueStudioIdentifier(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/u.test(value);
}

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

  async zCard(key: string): Promise<number> {
    return this.getZSet(key, false)?.value.size ?? 0;
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
    if (script.includes("AIR_STUDIO_PERSIST_PENDING_V3")) {
      return this.evalStudioPersistPending(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_FENCE_PENDING_V3")) {
      return this.evalStudioFencePending(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_READ_PENDING_V3")) {
      return this.evalStudioReadPending(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_PERSIST_PENDING_GUARDED_OUTBOX_V5")) {
      return this.evalStudioPersistPendingGuardedOutbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_ADVANCE_RELEASE_FENCE_V1")) {
      return this.evalStudioAdvanceReleaseFence(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_BEGIN_JOB_RESULT_PREPARATION_V1")) {
      return this.evalStudioBeginJobResultPreparation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_RECOVER_EXPIRED_JOB_RESULT_PREPARATION_V1")) {
      return this.evalStudioRecoverExpiredJobResultPreparation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_FINISH_JOB_RESULT_PREPARATION_V1")) {
      return this.evalStudioFinishJobResultPreparation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_PUBLISH_JOB_RESULT_V1")) {
      return this.evalStudioPublishJobResult(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_ADMIT_JOB_WITH_INITIAL_NOTIFICATIONS_V2")) {
      return this.evalStudioAdmitJobWithInitialNotifications(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_ADMIT_JOB_V5")) {
      return this.evalStudioAdmitJob(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_BEGIN_INBOX_FIFO_HOLDING_V1")) {
      return this.evalStudioBeginInboxFifoHolding(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_PREPARE_INBOX_FIFO_HOLDING_SCOPE_V1")) {
      return this.evalStudioPrepareInboxFifoHoldingScope(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_ACTIVATE_INBOX_FIFO_V1")) {
      return this.evalStudioActivateInboxFifo(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_PERSIST_INBOX_V5")) {
      return this.evalStudioPersistInbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_ENSURE_ONBOARDING_BUNDLE_V1")) {
      return this.evalStudioEnsureOnboardingBundle(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_PERSIST_OUTBOX_V4")) {
      return this.evalStudioPersistOutbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_CLAIM_INBOX_V4")) {
      return this.evalStudioClaimInbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_FINISH_INBOX_V4")) {
      return this.evalStudioFinishInbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_RETRY_INBOX_V4")) {
      return this.evalStudioRetryInbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_CLAIM_OUTBOX_V5")) {
      return this.evalStudioClaimOutbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_AUTHORIZE_OUTBOX_DELIVERY_V5")) {
      return this.evalStudioAuthorizeOutboxDelivery(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_FINISH_OUTBOX_V4")) {
      return this.evalStudioFinishOutbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_DEFER_OUTBOX_BEFORE_PHOTON_V1")) {
      return this.evalStudioDeferOutboxBeforePhoton(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_RECONCILE_EXPIRED_OUTBOX_V4")) {
      return this.evalStudioReconcileExpiredOutbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_TRANSITION_JOB_V5")) {
      return this.evalStudioTransitionJob(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_CANCEL_ADMITTED_JOB_WITH_NOTIFICATION_V1")) {
      return this.evalStudioCancelAdmittedJobWithNotification(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_RESCHEDULE_CLAIMED_JOB_V1")) {
      return this.evalStudioRescheduleClaimedJob(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_BEGIN_PROVIDER_SUBMISSION_V2")) {
      return this.evalStudioBeginProviderSubmission(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_FINISH_PROVIDER_SUBMISSION_V2")) {
      return this.evalStudioFinishProviderSubmission(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_BEGIN_PROVIDER_OBSERVATION_V1")) {
      return this.evalStudioBeginProviderObservation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_FINISH_PROVIDER_OBSERVATION_V2")) {
      return this.evalStudioFinishProviderObservation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_COMMIT_PROVIDER_RESULT_REFERENCE_V1")) {
      return this.evalStudioCommitProviderResultReference(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_FINALIZE_PREPARED_PROVIDER_RESULT_V1")) {
      return this.evalStudioFinalizePreparedProviderResult(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_ATTACH_PROVIDER_REQUEST_RECONCILIATION_V2")) {
      return this.evalStudioAttachProviderRequestReconciliation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_TERMINALIZE_PROVIDER_RECONCILIATION_V2")) {
      return this.evalStudioTerminalizeProviderReconciliation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_RESOLVE_OUTBOX_SEND_RECONCILIATION_V1")) {
      return this.evalStudioResolveOutboxSendReconciliation(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_REQUEUE_CONFIRMED_ABSENT_OUTBOX_V1")) {
      return this.evalStudioRequeueConfirmedAbsentOutbox(options.keys, options.arguments);
    }
    if (script.includes("AIR_STUDIO_TERMINALIZE_EXPIRED_OUTBOX_RECONCILIATION_V1")) {
      return this.evalStudioTerminalizeExpiredOutboxReconciliation(options.keys, options.arguments);
    }
    throw new Error("MemoryRedis does not implement this Lua script");
  }

  /** Deterministic mirrors for the Air v2 studio Lua primitives. */
  private evalStudioPersistPending(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 2, 6);
    const payloadKey = this.studioKey(keys, 0);
    const controlKey = this.studioKey(keys, 1);
    const encrypted = this.studioArgument(args, 0);
    const receiptId = this.studioArgument(args, 1);
    const scopeSequence = this.studioPositiveInteger(this.studioArgument(args, 2));
    const expiresAtMs = this.studioPositiveInteger(this.studioArgument(args, 3));
    const token = this.studioArgument(args, 4);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 5));
    this.assertStringOrMissing(payloadKey);
    this.assertStringOrMissing(controlKey);
    let control = this.studioPendingControl(controlKey);
    if (control === "invalid") return -6;
    if (control?.state === "active" && (!control.expiresAtMs || control.expiresAtMs <= now || this.studioString(payloadKey) === null)) {
      control = this.studioExpirePendingControl(payloadKey, controlKey, control);
    }
    if (control) {
      if (scopeSequence < control.scopeSequence) return this.studioPendingOutcome(control.state);
      if (scopeSequence === control.scopeSequence) {
        if (control.state !== "active") return this.studioPendingOutcome(control.state);
        if (
          control.receiptId !== receiptId
          || control.token !== token
          || control.expiresAtMs !== expiresAtMs
        ) return -6;
        return 2;
      }
    }
    const revision = control ? control.revision + 1 : 1;
    if (expiresAtMs <= now) {
      this.values.delete(payloadKey);
      this.studioSetPendingControl(controlKey, {
        schemaVersion: 1,
        state: "expired",
        receiptId,
        scopeSequence,
        revision,
        expiresAtMs,
      });
      return -4;
    }
    this.studioSet(payloadKey, encrypted, expiresAtMs - now);
    this.studioSetPendingControl(controlKey, {
      schemaVersion: 1,
      state: "active",
      receiptId,
      scopeSequence,
      revision,
      token,
      expiresAtMs,
    });
    return 1;
  }

  private evalStudioFencePending(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 2, 4);
    const payloadKey = this.studioKey(keys, 0);
    const controlKey = this.studioKey(keys, 1);
    const receiptId = this.studioArgument(args, 0);
    const scopeSequence = this.studioPositiveInteger(this.studioArgument(args, 1));
    const state = this.studioArgument(args, 2);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    if (state !== "cleared" && state !== "maintenance") return 0;
    this.assertStringOrMissing(payloadKey);
    this.assertStringOrMissing(controlKey);
    const control = this.studioPendingControl(controlKey);
    if (control === "invalid") return 0;
    if (control) {
      if (scopeSequence < control.scopeSequence) return 0;
      if (scopeSequence === control.scopeSequence) {
        return control.receiptId === receiptId && control.state === state ? 2 : 0;
      }
    }
    const canceled = Boolean(
      control?.state === "active"
      && control.expiresAtMs !== undefined
      && control.expiresAtMs > now
      && this.studioString(payloadKey) !== null,
    );
    this.values.delete(payloadKey);
    this.studioSetPendingControl(controlKey, {
      schemaVersion: 1,
      state,
      receiptId,
      scopeSequence,
      revision: control ? control.revision + 1 : 1,
    });
    return canceled ? 1 : 2;
  }

  private evalStudioReadPending(keys: readonly string[], args: readonly string[]): string | false {
    this.assertStudioArity(keys, args, 2, 1);
    const payloadKey = this.studioKey(keys, 0);
    const controlKey = this.studioKey(keys, 1);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 0));
    const control = this.studioPendingControl(controlKey);
    if (control === null || control === "invalid" || control.state !== "active") return false;
    if (!control.expiresAtMs || control.expiresAtMs <= now || this.studioString(payloadKey) === null) {
      this.studioExpirePendingControl(payloadKey, controlKey, control);
      return false;
    }
    return this.studioString(payloadKey) ?? false;
  }

  private evalStudioAdvanceReleaseFence(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 1, 3);
    const fenceKey = this.studioKey(keys, 0);
    const previous = this.studioArgument(args, 0);
    const target = this.studioArgument(args, 1);
    const phase = this.studioArgument(args, 2);
    if (!/^-?(?:0|[1-9]\d*)$/.test(previous) || !/^(?:0|[1-9]\d*)$/.test(target)) return -2;
    const previousGeneration = Number(previous);
    const targetGeneration = Number(target);
    if (
      !Number.isSafeInteger(previousGeneration) || previousGeneration < -1
      || !Number.isSafeInteger(targetGeneration) || targetGeneration < 0
      || (phase !== "dark" && phase !== "canary" && phase !== "production")
    ) return -2;
    const current = this.studioReleaseFence(fenceKey);
    if (current === "invalid") return -2;
    if (current === null) {
      if (previousGeneration !== -1 || targetGeneration !== 0 || phase !== "dark") return -1;
      this.values.set(fenceKey, {
        kind: "string",
        value: JSON.stringify({ schemaVersion: 1, generation: targetGeneration, phase }),
        expiresAt: null,
      });
      return 1;
    }
    if (
      current.generation === targetGeneration
      && current.phase === phase
      && (previousGeneration === targetGeneration || previousGeneration === targetGeneration - 1)
    ) return 2;
    if (current.generation !== previousGeneration || targetGeneration !== previousGeneration + 1) return -1;
    this.values.set(fenceKey, {
      kind: "string",
      value: JSON.stringify({ schemaVersion: 1, generation: targetGeneration, phase }),
      expiresAt: null,
    });
    return 1;
  }

  /** Mirrors the atomic one-based provider-output cursor + outbox publication. */
  private evalStudioPublishJobResult(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    const outOfOrder = (expectedOutputIndex: number): string =>
      JSON.stringify({ kind: "out_of_order", expectedOutputIndex });
    try {
      this.assertStudioArity(keys, args, 7, 13);
      const jobKey = this.studioKey(keys, 0);
      const cursorKey = this.studioKey(keys, 1);
      const outboxKey = this.studioKey(keys, 2);
      const outboxPayloadKey = this.studioKey(keys, 3);
      const readyKey = this.studioKey(keys, 4);
      const laneKey = this.studioKey(keys, 5);
      const releaseFenceKey = this.studioKey(keys, 6);
      const jobId = this.studioArgument(args, 0);
      const expectedJobVersion = this.studioPositiveInteger(this.studioArgument(args, 1));
      const admissionGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 2));
      const admissionPhase = this.studioArgument(args, 3);
      const outputIndex = this.studioPositiveInteger(this.studioArgument(args, 4));
      const outboxId = this.studioArgument(args, 5);
      const serializedOutbox = this.studioArgument(args, 6);
      const serializedOutboxPayload = this.studioArgument(args, 7);
      const outboxPayloadTtl = this.studioPositiveInteger(this.studioArgument(args, 8));
      const dueAtMs = this.studioNonnegativeInteger(this.studioArgument(args, 9));
      const currentGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 10));
      const currentPhase = this.studioArgument(args, 11);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 12));
      if (
        !this.studioSafeText(jobId, 160)
        || !isMemoryOpaqueStudioIdentifier(outboxId)
        || (admissionPhase !== "dark" && admissionPhase !== "canary" && admissionPhase !== "production")
        || (currentPhase !== "dark" && currentPhase !== "canary" && currentPhase !== "production")
        || (currentPhase !== "dark" && currentGeneration < 1)
      ) return unavailable();
      this.assertStringOrMissing(jobKey);
      this.assertStringOrMissing(cursorKey);
      this.assertStringOrMissing(outboxKey);
      this.assertStringOrMissing(outboxPayloadKey);
      this.assertZSetOrMissing(readyKey);
      this.assertZSetOrMissing(laneKey);
      this.assertStringOrMissing(releaseFenceKey);

      const job = this.studioRecordFailClosed(jobKey);
      if (
        !job
        || job.jobId !== jobId
        || !isMemoryOpaqueStudioIdentifier(job.senderHash)
        || !isMemoryOpaqueStudioIdentifier(job.threadHash)
        || typeof job.outputCount !== "number"
        || !Number.isSafeInteger(job.outputCount)
        || job.outputCount < 1
        || outputIndex > job.outputCount
        || !this.studioSameReleaseStamp(job.releaseStamp, { generation: admissionGeneration, phase: admissionPhase })
      ) return unavailable();

      const cursor = this.studioResultCursor(cursorKey);
      if (cursor === "invalid") return unavailable();
      if (cursor !== null) {
        if (
          cursor.jobId !== jobId
          || cursor.senderHash !== job.senderHash
          || cursor.threadHash !== job.threadHash
          || cursor.outputCount !== job.outputCount
          || !this.studioSameReleaseStamp(cursor.releaseStamp, job.releaseStamp)
        ) return unavailable();
        const output = cursor.outputs[outputIndex - 1];
        if (output !== undefined) return JSON.stringify({ kind: "existing" });
        if (outputIndex !== cursor.nextOutputIndex) return outOfOrder(cursor.nextOutputIndex);
      } else if (outputIndex !== 1) {
        return outOfOrder(1);
      }

      if (
        (job.state !== "executing" && job.state !== "ready" && job.state !== "delivering")
        || this.studioStateVersion(job) !== expectedJobVersion
      ) return unavailable();
      const outbox = this.studioRecordFailClosedFromRaw(serializedOutbox);
      if (
        !outbox
        || !this.studioEncryptedPayloadFromJson(serializedOutboxPayload)
        || outbox.schemaVersion !== 3
        || outbox.outboxId !== outboxId
        || outbox.jobId !== jobId
        || outbox.senderHash !== job.senderHash
        || outbox.lane !== "job-result"
        || outbox.resultOutputIndex !== outputIndex
        || outbox.sequence !== outputIndex
        || outbox.state !== "intent"
        || outbox.attempts !== 0
        || this.studioStateVersion(outbox) !== 1
        || outbox.laneIdentity !== undefined
        || outbox.pendingGuard !== undefined
        || outbox.dueAtMs !== dueAtMs
        || typeof outbox.createdAtMs !== "number"
        || !Number.isSafeInteger(outbox.createdAtMs)
        || outbox.createdAtMs < 0
        || typeof outbox.updatedAtMs !== "number"
        || !Number.isSafeInteger(outbox.updatedAtMs)
        || outbox.updatedAtMs < 0
      ) return unavailable();
      const release = this.studioReleasePersistenceStatus(
        outbox,
        releaseFenceKey,
        currentGeneration,
        currentPhase,
      );
      if (release !== 1) return JSON.stringify({ kind: "release_blocked" });
      if (this.getEntry(outboxKey) || this.getEntry(outboxPayloadKey)) return unavailable();
      if ([...(this.getZSet(laneKey, false)?.value.values() ?? [])].some((score) => score === outputIndex)) {
        return unavailable();
      }

      const nextCursor = cursor === null
        ? {
          schemaVersion: 1,
          jobId,
          senderHash: job.senderHash,
          threadHash: job.threadHash,
          outputCount: job.outputCount,
          releaseStamp: job.releaseStamp,
          nextOutputIndex: 1,
          outputs: [] as Array<{ outputIndex: number; outboxId: string }>,
          createdAtMs: now,
          updatedAtMs: now,
        }
        : cursor;
      nextCursor.outputs.push({ outputIndex, outboxId });
      nextCursor.nextOutputIndex = outputIndex + 1;
      nextCursor.updatedAtMs = now;
      this.studioSetPersistentRecord(cursorKey, nextCursor);
      this.studioSetPersistentRecord(outboxKey, outbox);
      this.studioSet(outboxPayloadKey, serializedOutboxPayload, outboxPayloadTtl * 1_000);
      this.getZSet(readyKey, true).value.set(outboxId, dueAtMs);
      this.getZSet(laneKey, true).value.set(outboxId, outputIndex);
      return JSON.stringify({ kind: "created" });
    } catch {
      return unavailable();
    }
  }

  /** Mirrors the pre-upload crash fence for direct Photon result delivery. */
  private evalStudioBeginJobResultPreparation(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    try {
      this.assertStudioArity(keys, args, 8, 9);
      const preparationKey = this.studioKey(keys, 0);
      const referenceKey = this.studioKey(keys, 1);
      const leaseKey = this.studioKey(keys, 2);
      const releaseFenceKey = this.studioKey(keys, 3);
      const jobKey = this.studioKey(keys, 4);
      const payloadKey = this.studioKey(keys, 5);
      const readyJobKey = this.studioKey(keys, 6);
      const cursorKey = this.studioKey(keys, 7);
      const token = this.studioArgument(args, 0);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
      const generation = this.studioNonnegativeInteger(this.studioArgument(args, 2));
      const phase = this.studioArgument(args, 3);
      const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 4));
      const jobId = this.studioArgument(args, 5);
      const expectedJobVersion = this.studioPositiveInteger(this.studioArgument(args, 6));
      const minimumPayloadRemainingMs = this.studioPositiveInteger(this.studioArgument(args, 7));
      const outputIndex = this.studioPositiveInteger(this.studioArgument(args, 8));
      if (!this.studioSafeText(jobId, 160) || outputIndex > 4) return unavailable();
      const preparation = this.studioJobResultPreparation(preparationKey);
      if (
        preparation === null
        || preparation === "invalid"
        || preparation.jobId !== jobId
        || preparation.outputIndex !== outputIndex
      ) return unavailable();
      const response = (kind: string): string => JSON.stringify({ kind, preparation });
      if (this.studioString(leaseKey) !== token) return JSON.stringify({ kind: "lease_lost" });

      const reconcileUnknown = (): string => {
        preparation.state = "upload_unknown";
        preparation.stateVersion = this.studioStateVersion(preparation) + 1;
        preparation.updatedAtMs = now;
        this.studioSetPersistentRecord(preparationKey, preparation);
        this.values.delete(referenceKey);
        this.values.delete(leaseKey);
        const job = this.studioRecordFailClosed(jobKey);
        if (
          job
          && job.jobId === jobId
          && (job.state === "executing" || job.state === "ready" || job.state === "delivering")
        ) {
          job.state = "reconciliation_required";
          job.stateVersion = this.studioStateVersion(job) + 1;
          job.updatedAtMs = now;
          this.studioSetPersistentRecord(jobKey, job);
          this.studioRemoveMember(readyJobKey, jobId);
        }
        return response("upload_unknown");
      };

      if (preparation.state === "uploading") return reconcileUnknown();
      if (preparation.state === "prepared") {
        if (this.studioRemainingTtl(referenceKey) < 1) {
          const cursor = this.studioResultCursor(cursorKey);
          const published = cursor !== null
            && cursor !== "invalid"
            && cursor.jobId === jobId
            && cursor.outputs[outputIndex - 1]?.outputIndex === outputIndex;
          if (!published) return reconcileUnknown();
        }
        return response("prepared");
      }
      if (
        preparation.state === "upload_unknown"
        || preparation.state === "failed"
        || preparation.state === "canceled"
      ) return response(preparation.state);
      if (preparation.state !== "reserved") return unavailable();

      const job = this.studioRecordFailClosed(jobKey);
      if (!job || job.jobId !== jobId) {
        preparation.state = "canceled";
        preparation.stateVersion = this.studioStateVersion(preparation) + 1;
        preparation.updatedAtMs = now;
        this.studioSetRecord(preparationKey, preparation, terminalTtl);
        this.values.delete(referenceKey);
        this.values.delete(leaseKey);
        return response("canceled");
      }
      if (job.state !== "executing" || this.studioStateVersion(job) !== expectedJobVersion) return unavailable();
      if (this.studioRemainingTtl(payloadKey) <= minimumPayloadRemainingMs) return response("payload_expiring");
      const fence = this.studioReleaseFence(releaseFenceKey);
      if (
        (phase !== "canary" && phase !== "production")
        || generation < 1
        || fence === null
        || fence === "invalid"
        || fence.generation !== generation
        || fence.phase !== phase
      ) return response("release_blocked");

      preparation.state = "uploading";
      preparation.stateVersion = this.studioStateVersion(preparation) + 1;
      preparation.updatedAtMs = now;
      this.studioSetPersistentRecord(preparationKey, preparation);
      return response("ready");
    } catch {
      return unavailable();
    }
  }

  /**
   * Mirrors the recovery-only stale-upload fence. It intentionally has no
   * authorization path from `reserved` to `uploading` and never touches a
   * provider artifact or Photon output.
   */
  private evalStudioRecoverExpiredJobResultPreparation(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    try {
      this.assertStudioArity(keys, args, 5, 4);
      const preparationKey = this.studioKey(keys, 0);
      const referenceKey = this.studioKey(keys, 1);
      const leaseKey = this.studioKey(keys, 2);
      const jobKey = this.studioKey(keys, 3);
      const readyJobKey = this.studioKey(keys, 4);
      const token = this.studioArgument(args, 0);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
      const jobId = this.studioArgument(args, 2);
      const outputIndex = this.studioPositiveInteger(this.studioArgument(args, 3));
      if (!this.studioSafeText(jobId, 160) || outputIndex > 4) return unavailable();
      this.assertStringOrMissing(preparationKey);
      this.assertStringOrMissing(referenceKey);
      this.assertStringOrMissing(leaseKey);
      this.assertStringOrMissing(jobKey);
      this.assertZSetOrMissing(readyJobKey);
      if (this.studioString(leaseKey) !== token) return JSON.stringify({ kind: "lease_lost" });

      const preparation = this.studioJobResultPreparation(preparationKey);
      if (
        preparation === null
        || preparation === "invalid"
        || preparation.jobId !== jobId
        || preparation.outputIndex !== outputIndex
      ) return unavailable();
      const response = (kind: string): string => JSON.stringify({ kind, preparation });
      if (preparation.state !== "uploading") return response(preparation.state as string);

      const job = this.studioRecordFailClosed(jobKey);
      const jobCanReconcile = Boolean(
        job
        && job.jobId === jobId
        && (job.state === "executing" || job.state === "ready" || job.state === "delivering")
        && typeof job.stateVersion === "number"
        && Number.isSafeInteger(job.stateVersion)
        && job.stateVersion >= 1,
      );
      preparation.state = "upload_unknown";
      preparation.stateVersion = this.studioStateVersion(preparation) + 1;
      preparation.updatedAtMs = now;
      this.studioSetPersistentRecord(preparationKey, preparation);
      this.values.delete(referenceKey);
      this.values.delete(leaseKey);
      if (jobCanReconcile && job) {
        job.state = "reconciliation_required";
        job.stateVersion = (job.stateVersion as number) + 1;
        job.updatedAtMs = now;
        this.studioSetPersistentRecord(jobKey, job);
        this.studioRemoveMember(readyJobKey, jobId);
      }
      return response("upload_unknown");
    } catch {
      return unavailable();
    }
  }

  /** Mirrors the write-after-upload boundary without extending reference TTLs. */
  private evalStudioFinishJobResultPreparation(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    try {
      this.assertStudioArity(keys, args, 6, 7);
      const preparationKey = this.studioKey(keys, 0);
      const referenceKey = this.studioKey(keys, 1);
      const leaseKey = this.studioKey(keys, 2);
      const jobKey = this.studioKey(keys, 3);
      const payloadKey = this.studioKey(keys, 4);
      const readyJobKey = this.studioKey(keys, 5);
      const token = this.studioArgument(args, 0);
      const outcome = this.studioArgument(args, 1);
      const encryptedReference = this.studioArgument(args, 2);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 3));
      const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 4));
      const jobId = this.studioArgument(args, 5);
      const outputIndex = this.studioPositiveInteger(this.studioArgument(args, 6));
      if (!this.studioSafeText(jobId, 160) || outputIndex > 4) return unavailable();
      const preparation = this.studioJobResultPreparation(preparationKey);
      if (
        preparation === null
        || preparation === "invalid"
        || preparation.jobId !== jobId
        || preparation.outputIndex !== outputIndex
      ) return unavailable();
      const response = (kind: string): string => JSON.stringify({ kind, preparation });
      if (this.studioString(leaseKey) !== token) return JSON.stringify({ kind: "lease_lost" });
      if (preparation.state !== "uploading") return unavailable();

      const reconcileUnknown = (): string => {
        preparation.state = "upload_unknown";
        preparation.stateVersion = this.studioStateVersion(preparation) + 1;
        preparation.updatedAtMs = now;
        this.studioSetPersistentRecord(preparationKey, preparation);
        this.values.delete(referenceKey);
        this.values.delete(leaseKey);
        const job = this.studioRecordFailClosed(jobKey);
        if (
          job
          && job.jobId === jobId
          && (job.state === "executing" || job.state === "ready" || job.state === "delivering")
        ) {
          job.state = "reconciliation_required";
          job.stateVersion = this.studioStateVersion(job) + 1;
          job.updatedAtMs = now;
          this.studioSetPersistentRecord(jobKey, job);
          this.studioRemoveMember(readyJobKey, jobId);
        }
        return response("upload_unknown");
      };

      if (outcome === "prepared") {
        if (!this.studioEncryptedPayloadFromJson(encryptedReference)) return reconcileUnknown();
        const payloadTtl = this.studioRemainingTtl(payloadKey);
        if (payloadTtl < 1) return reconcileUnknown();
        preparation.state = "prepared";
        preparation.stateVersion = this.studioStateVersion(preparation) + 1;
        preparation.updatedAtMs = now;
        this.studioSetPersistentRecord(preparationKey, preparation);
        this.studioSet(referenceKey, encryptedReference, payloadTtl);
        this.values.delete(leaseKey);
        return response("prepared");
      }
      if (outcome === "retryable_failure") {
        preparation.state = "reserved";
        preparation.stateVersion = this.studioStateVersion(preparation) + 1;
        preparation.updatedAtMs = now;
        this.studioSetPersistentRecord(preparationKey, preparation);
        this.values.delete(referenceKey);
        this.values.delete(leaseKey);
        return response("retryable_failure");
      }
      if (outcome === "failed") {
        preparation.state = "failed";
        preparation.stateVersion = this.studioStateVersion(preparation) + 1;
        preparation.updatedAtMs = now;
        this.studioSetRecord(preparationKey, preparation, terminalTtl);
        this.values.delete(referenceKey);
        this.values.delete(leaseKey);
        return response("failed");
      }
      if (outcome === "upload_unknown") return reconcileUnknown();
      return unavailable();
    } catch {
      return unavailable();
    }
  }

  private evalStudioPersistPendingGuardedOutbox(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 8, 14);
    const payloadKey = this.studioKey(keys, 0);
    const controlKey = this.studioKey(keys, 1);
    const recordKey = this.studioKey(keys, 2);
    const outboxPayloadKey = this.studioKey(keys, 3);
    const readyKey = this.studioKey(keys, 4);
    const laneKey = this.studioKey(keys, 5);
    const scopeCounterKey = this.studioKey(keys, 6);
    const releaseFenceKey = this.studioKey(keys, 7);
    const receiptId = this.studioArgument(args, 0);
    const scopeSequence = this.studioPositiveInteger(this.studioArgument(args, 1));
    const revision = this.studioPositiveInteger(this.studioArgument(args, 2));
    const token = this.studioArgument(args, 3);
    const expiresAtMs = this.studioPositiveInteger(this.studioArgument(args, 4));
    const record = this.studioArgument(args, 5);
    const outboxPayload = this.studioArgument(args, 6);
    const outboxPayloadTtl = this.studioPositiveInteger(this.studioArgument(args, 7));
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 8));
    const member = this.studioArgument(args, 9);
    const sequence = this.studioNonnegativeInteger(this.studioArgument(args, 10));
    const dueAtMs = this.studioNonnegativeInteger(this.studioArgument(args, 11));
    const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 12));
    const releasePhase = this.studioArgument(args, 13);
    if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") return -8;
    const release = this.studioReleasePersistenceStatus(
      this.studioRecordFromJson(record),
      releaseFenceKey,
      releaseGeneration,
      releasePhase,
    );
    if (release === -1) return -7;
    if (release === 0) return -8;
    const control = this.studioPendingControl(controlKey);
    if (control === null || control === "invalid") return -6;
    if (control.state === "maintenance") return -2;
    if (control.state === "cleared") return -3;
    if (control.state === "expired") return -4;
    if (!control.expiresAtMs || control.expiresAtMs <= now || this.studioString(payloadKey) === null) {
      this.studioExpirePendingControl(payloadKey, controlKey, control);
      return -4;
    }
    if (control.scopeSequence > scopeSequence) return -5;
    if (
      control.scopeSequence !== scopeSequence
      || control.revision !== revision
      || control.receiptId !== receiptId
      || control.token !== token
      || control.expiresAtMs !== expiresAtMs
    ) return -6;
    if (this.studioCounter(scopeCounterKey) !== scopeSequence) return -5;
    if (this.getEntry(recordKey)) return 0;
    const existingSequence = [...(this.getZSet(laneKey, false)?.value.entries() ?? [])]
      .some(([, scoreAtSequence]) => scoreAtSequence === sequence);
    if (existingSequence) return -1;
    this.values.set(recordKey, { kind: "string", value: record, expiresAt: null });
    this.values.set(outboxPayloadKey, { kind: "string", value: outboxPayload, expiresAt: this.now() + outboxPayloadTtl * 1_000 });
    this.getZSet(readyKey, true).value.set(member, dueAtMs);
    this.getZSet(laneKey, true).value.set(member, sequence);
    return 1;
  }

  private evalStudioPersistInbox(keys: readonly string[], args: readonly string[]): string | number {
    this.assertStudioArity(keys, args, 8, 6);
    const recordKey = this.studioKey(keys, 0);
    const envelopeKey = this.studioKey(keys, 1);
    const readyKey = this.studioKey(keys, 2);
    const scopeCounterKey = this.studioKey(keys, 3);
    const migrationKey = this.studioKey(keys, 4);
    const scopeQueueKey = this.studioKey(keys, 5);
    const scopeCursorKey = this.studioKey(keys, 6);
    const holdingScopesKey = this.studioKey(keys, 7);
    const serializedRecord = this.studioArgument(args, 0);
    const serializedEnvelope = this.studioArgument(args, 1);
    const envelopeTtl = this.studioPositiveInteger(this.studioArgument(args, 2));
    const score = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    const member = this.studioArgument(args, 4);
    const scopeId = this.studioArgument(args, 5);
    if (!/^[a-f0-9]{64}$/.test(scopeId)) return -2;
    this.assertStringOrMissing(recordKey);
    this.assertStringOrMissing(envelopeKey);
    this.assertStringOrMissing(scopeCounterKey);
    this.assertStringOrMissing(migrationKey);
    this.assertZSetOrMissing(readyKey);
    if (this.getEntry(recordKey)) return 0;
    const phase = this.studioInboxFifoPhase(migrationKey);
    if (phase === "invalid") return -2;
    const record = this.studioRecordFromJson(serializedRecord);
    const nextSequence = this.studioCounter(scopeCounterKey) + 1;
    if (!Number.isSafeInteger(nextSequence) || nextSequence < 1) return -2;

    let cursor: number | undefined;
    if (phase !== "legacy") {
      this.assertZSetOrMissing(scopeQueueKey);
      this.assertStringOrMissing(scopeCursorKey);
      this.assertZSetOrMissing(holdingScopesKey);
      const rawCursor = this.studioString(scopeCursorKey);
      if (rawCursor === null) {
        cursor = nextSequence;
      } else {
        try {
          cursor = this.studioPositiveInteger(rawCursor);
        } catch {
          return -2;
        }
        if (cursor > nextSequence) return -2;
      }
      // Match the production Lua's counter-rollback/corruption guard: a
      // second member at one sequence must never be resolved by member sort.
      if ([...(this.getZSet(scopeQueueKey, false)?.value.values() ?? [])]
        .some((scopeSequence) => scopeSequence === nextSequence)) return -2;
    }

    this.values.set(scopeCounterKey, { kind: "string", value: String(nextSequence), expiresAt: null });
    record.scopeSequence = nextSequence;
    delete record.encryptedEnvelope;
    const stored = JSON.stringify(record);
    this.values.set(recordKey, { kind: "string", value: stored, expiresAt: null });
    this.values.set(envelopeKey, { kind: "string", value: serializedEnvelope, expiresAt: this.now() + envelopeTtl * 1_000 });
    if (phase === "legacy") {
      this.getZSet(readyKey, true).value.set(member, score);
      return stored;
    }

    if (this.studioString(scopeCursorKey) === null) {
      this.studioSetPersistent(scopeCursorKey, String(cursor));
    }
    this.getZSet(scopeQueueKey, true).value.set(member, nextSequence);
    if (phase === "holding") {
      this.getZSet(holdingScopesKey, true).value.set(scopeId, 0);
    } else if (cursor === nextSequence) {
      this.getZSet(readyKey, true).value.set(member, score);
    }
    return stored;
  }

  private evalStudioBeginInboxFifoHolding(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 3, 0);
    const migrationKey = this.studioKey(keys, 0);
    const readyKey = this.studioKey(keys, 1);
    const holdingScopesKey = this.studioKey(keys, 2);
    this.assertStringOrMissing(migrationKey);
    this.assertZSetOrMissing(readyKey);
    this.assertZSetOrMissing(holdingScopesKey);
    const phase = this.studioInboxFifoPhase(migrationKey);
    if (phase === "invalid") return -3;
    if (phase === "holding") return 2;
    if (phase === "fifo") return -2;
    if (this.getZSet(readyKey, false)?.value.size ?? 0) return -1;
    if (this.getZSet(holdingScopesKey, false)?.value.size ?? 0) return -3;
    this.studioSetPersistent(migrationKey, JSON.stringify({ schemaVersion: 1, phase: "holding" }));
    return 1;
  }

  private evalStudioPrepareInboxFifoHoldingScope(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 5, 2);
    const migrationKey = this.studioKey(keys, 0);
    const holdingScopesKey = this.studioKey(keys, 1);
    const scopeQueueKey = this.studioKey(keys, 2);
    const scopeCursorKey = this.studioKey(keys, 3);
    const readyKey = this.studioKey(keys, 4);
    const scopeId = this.studioArgument(args, 0);
    const recordKeyPrefix = this.studioArgument(args, 1);
    if (!/^[a-f0-9]{64}$/.test(scopeId) || !recordKeyPrefix) return -1;
    this.assertStringOrMissing(migrationKey);
    this.assertZSetOrMissing(holdingScopesKey);
    this.assertZSetOrMissing(scopeQueueKey);
    this.assertStringOrMissing(scopeCursorKey);
    this.assertZSetOrMissing(readyKey);
    if (this.studioInboxFifoPhase(migrationKey) !== "holding") return -2;
    let cursor: number;
    try {
      const raw = this.studioString(scopeCursorKey);
      if (raw === null) return -1;
      cursor = this.studioPositiveInteger(raw);
    } catch {
      return -1;
    }
    const head = this.studioInboxFifoHead(scopeQueueKey);
    if (head === "invalid" || head === null || head.scopeSequence !== cursor) return -1;
    const record = this.studioRecordFailClosed(`${recordKeyPrefix}${head.receiptId}`);
    if (!this.studioInboxFifoReadyRecord(record, head.receiptId, cursor)) return -1;
    const held = this.getZSet(holdingScopesKey, false)?.value.has(scopeId) === true;
    const published = this.getZSet(readyKey, false)?.value.get(head.receiptId);
    if (!held) return published === record.receivedAtMs ? 2 : -1;
    if (published !== undefined) {
      if (published !== record.receivedAtMs) return -1;
      this.studioRemoveMember(holdingScopesKey, scopeId);
      return 2;
    }
    this.getZSet(readyKey, true).value.set(head.receiptId, record.receivedAtMs);
    this.studioRemoveMember(holdingScopesKey, scopeId);
    return 1;
  }

  private evalStudioActivateInboxFifo(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 2, 0);
    const migrationKey = this.studioKey(keys, 0);
    const holdingScopesKey = this.studioKey(keys, 1);
    this.assertStringOrMissing(migrationKey);
    this.assertZSetOrMissing(holdingScopesKey);
    const phase = this.studioInboxFifoPhase(migrationKey);
    if (phase === "invalid") return -3;
    if (phase === "legacy") return -2;
    if (phase === "fifo") return 2;
    if (this.getZSet(holdingScopesKey, false)?.value.size ?? 0) return -1;
    this.studioSetPersistent(migrationKey, JSON.stringify({ schemaVersion: 1, phase: "fifo" }));
    return 1;
  }

  private evalStudioPersistOutbox(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 5, 8);
    const recordKey = this.studioKey(keys, 0);
    const outboxPayloadKey = this.studioKey(keys, 1);
    const readyKey = this.studioKey(keys, 2);
    const laneKey = this.studioKey(keys, 3);
    const releaseFenceKey = this.studioKey(keys, 4);
    const record = this.studioArgument(args, 0);
    const outboxPayload = this.studioArgument(args, 1);
    const outboxPayloadTtl = this.studioPositiveInteger(this.studioArgument(args, 2));
    const score = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    const member = this.studioArgument(args, 4);
    const sequence = this.studioNonnegativeInteger(this.studioArgument(args, 5));
    const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 6));
    const releasePhase = this.studioArgument(args, 7);
    if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") return -3;
    this.assertStringOrMissing(recordKey);
    this.assertStringOrMissing(outboxPayloadKey);
    this.assertZSetOrMissing(readyKey);
    this.assertZSetOrMissing(laneKey);
    if (this.getEntry(recordKey)) return 0;
    const release = this.studioReleasePersistenceStatus(
      this.studioRecordFromJson(record),
      releaseFenceKey,
      releaseGeneration,
      releasePhase,
    );
    if (release === -1) return -2;
    if (release === 0) return -3;
    const existingSequence = [...(this.getZSet(laneKey, false)?.value.entries() ?? [])]
      .some(([, scoreAtSequence]) => scoreAtSequence === sequence);
    if (existingSequence) return -1;
    this.values.set(recordKey, { kind: "string", value: record, expiresAt: null });
    this.values.set(outboxPayloadKey, { kind: "string", value: outboxPayload, expiresAt: this.now() + outboxPayloadTtl * 1_000 });
    this.getZSet(readyKey, true).value.set(member, score);
    this.getZSet(laneKey, true).value.set(member, sequence);
    return 1;
  }

  /**
   * Mirror the enrollment Lua as one unit. Do not compose the generic outbox
   * primitive here: tests must reject the same partial-write states Redis
   * rejects when a concurrent first contact races the onboarding lane.
   */
  private evalStudioEnsureOnboardingBundle(keys: readonly string[], args: readonly string[]): number {
    try {
      this.assertStudioArity(keys, args, 9, 16);
      const settingsKey = this.studioKey(keys, 0);
      const markerKey = this.studioKey(keys, 1);
      const firstKey = this.studioKey(keys, 2);
      const firstPayloadKey = this.studioKey(keys, 3);
      const secondKey = this.studioKey(keys, 4);
      const secondPayloadKey = this.studioKey(keys, 5);
      const readyKey = this.studioKey(keys, 6);
      const laneKey = this.studioKey(keys, 7);
      const releaseFenceKey = this.studioKey(keys, 8);
      const settings = this.studioArgument(args, 0);
      const settingsTtl = this.studioPositiveInteger(this.studioArgument(args, 1));
      const marker = this.studioArgument(args, 2);
      const firstRaw = this.studioArgument(args, 3);
      const firstPayload = this.studioArgument(args, 4);
      const firstId = this.studioArgument(args, 5);
      const firstSequence = this.studioNonnegativeInteger(this.studioArgument(args, 6));
      const firstDueAt = this.studioNonnegativeInteger(this.studioArgument(args, 7));
      const secondRaw = this.studioArgument(args, 8);
      const secondPayload = this.studioArgument(args, 9);
      const secondId = this.studioArgument(args, 10);
      const secondSequence = this.studioNonnegativeInteger(this.studioArgument(args, 11));
      const secondDueAt = this.studioNonnegativeInteger(this.studioArgument(args, 12));
      const outboxPayloadTtl = this.studioPositiveInteger(this.studioArgument(args, 13));
      const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 14));
      const releasePhase = this.studioArgument(args, 15);
      if (
        firstSequence !== 1
        || secondSequence !== 2
        || (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production")
        || (releasePhase !== "dark" && releaseGeneration < 1)
      ) return -3;

      const candidateMarker = this.studioOnboardingMarkerFromJson(marker);
      if (
        !candidateMarker
        || candidateMarker.firstOutboxId !== firstId
        || candidateMarker.secondOutboxId !== secondId
      ) return -3;

      // A marker is proof of the whole atomic bundle and wins before current
      // release checks, just as an existing job admission wins before a later
      // rollout changes policy.
      this.assertStringOrMissing(markerKey);
      const existingMarkerRaw = this.studioString(markerKey);
      if (existingMarkerRaw !== null) {
        const existingMarker = this.studioOnboardingMarkerFromJson(existingMarkerRaw);
        return existingMarker && existingMarker.onboardingVersion === candidateMarker.onboardingVersion ? 2 : -3;
      }

      // Validate all source values and key kinds before creating any zset or
      // writing a string. This preserves Redis Lua's all-or-nothing boundary.
      this.assertStringOrMissing(settingsKey);
      this.assertStringOrMissing(firstKey);
      this.assertStringOrMissing(firstPayloadKey);
      this.assertStringOrMissing(secondKey);
      this.assertStringOrMissing(secondPayloadKey);
      this.assertStringOrMissing(releaseFenceKey);
      this.assertZSetOrMissing(readyKey);
      this.assertZSetOrMissing(laneKey);
      if (
        !this.studioEncryptedPayloadFromJson(settings)
        || !this.studioEncryptedPayloadFromJson(firstPayload)
        || !this.studioEncryptedPayloadFromJson(secondPayload)
        || !isMemoryOpaqueStudioIdentifier(firstId)
        || !isMemoryOpaqueStudioIdentifier(secondId)
        || firstId === secondId
      ) return -3;

      const first = this.studioRecordFromJson(firstRaw);
      const second = this.studioRecordFromJson(secondRaw);
      const senderHash = first.senderHash;
      if (
        !isMemoryOpaqueStudioIdentifier(senderHash)
        || senderHash !== second.senderHash
        || !this.studioValidNewOnboardingOutbox(
          first,
          firstId,
          senderHash,
          firstSequence,
          firstDueAt,
          releaseGeneration,
          releasePhase,
        )
        || !this.studioValidNewOnboardingOutbox(
          second,
          secondId,
          senderHash,
          secondSequence,
          secondDueAt,
          releaseGeneration,
          releasePhase,
        )
      ) return -3;

      const release = this.studioReleasePersistenceStatus(
        first,
        releaseFenceKey,
        releaseGeneration,
        releasePhase,
      );
      if (release === -1) return -2;
      if (release === 0) return -3;

      if (
        this.getEntry(firstKey)
        || this.getEntry(firstPayloadKey)
        || this.getEntry(secondKey)
        || this.getEntry(secondPayloadKey)
      ) return -3;
      const lane = this.getZSet(laneKey, false);
      if ([...(lane?.value.values() ?? [])].some((sequence) => sequence === firstSequence || sequence === secondSequence)) {
        return -3;
      }

      const now = this.now();
      this.values.set(settingsKey, {
        kind: "string",
        value: settings,
        expiresAt: now + settingsTtl * 1_000,
      });
      this.values.set(firstKey, { kind: "string", value: firstRaw, expiresAt: null });
      this.values.set(firstPayloadKey, {
        kind: "string",
        value: firstPayload,
        expiresAt: now + outboxPayloadTtl * 1_000,
      });
      this.values.set(secondKey, { kind: "string", value: secondRaw, expiresAt: null });
      this.values.set(secondPayloadKey, {
        kind: "string",
        value: secondPayload,
        expiresAt: now + outboxPayloadTtl * 1_000,
      });
      const ready = this.getZSet(readyKey, true);
      ready.value.set(firstId, firstDueAt);
      ready.value.set(secondId, secondDueAt);
      const laneAfterCommit = this.getZSet(laneKey, true);
      laneAfterCommit.value.set(firstId, firstSequence);
      laneAfterCommit.value.set(secondId, secondSequence);
      this.values.set(markerKey, { kind: "string", value: marker, expiresAt: null });
      return 1;
    } catch {
      return -3;
    }
  }

  private evalStudioAdmitJob(keys: readonly string[], args: readonly string[]): readonly [string, string?] {
    this.assertStudioArity(keys, args, 9, 14);
    const activeKey = this.studioKey(keys, 0);
    const userCountKey = this.studioKey(keys, 1);
    const spendKey = this.studioKey(keys, 2);
    const jobKey = this.studioKey(keys, 3);
    const jobPayloadKey = this.studioKey(keys, 4);
    const readyKey = this.studioKey(keys, 5);
    const releaseFenceKey = this.studioKey(keys, 6);
    const canaryBudgetKey = this.studioKey(keys, 7);
    const recentJobKey = this.studioKey(keys, 8);
    const jobId = this.studioArgument(args, 0);
    const serializedMetadata = this.studioArgument(args, 1);
    const serializedPayload = this.studioArgument(args, 2);
    const dailyTtl = this.studioPositiveInteger(this.studioArgument(args, 3));
    const payloadTtl = this.studioPositiveInteger(this.studioArgument(args, 4));
    const userLimit = this.studioPositiveInteger(this.studioArgument(args, 5));
    const spendCap = this.studioNonnegativeInteger(this.studioArgument(args, 6));
    const cost = this.studioNonnegativeInteger(this.studioArgument(args, 7));
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 8));
    const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 9));
    const releasePhase = this.studioArgument(args, 10);
    const canaryBudgetEnabled = this.studioArgument(args, 11);
    const canaryTotalCapRaw = this.studioArgument(args, 12);
    const canaryFalCapRaw = this.studioArgument(args, 13);
    this.assertStringOrMissing(activeKey);
    this.assertStringOrMissing(userCountKey);
    this.assertStringOrMissing(spendKey);
    this.assertStringOrMissing(jobKey);
    this.assertStringOrMissing(jobPayloadKey);
    this.assertZSetOrMissing(readyKey);

    const existing = this.studioString(jobKey);
    if (existing !== null) {
      const existingRecord = this.studioRecordFailClosed(jobKey);
      if (existingRecord && (existingRecord.state === "admitted" || existingRecord.state === "executing")) {
        this.getZSet(readyKey, true).value.set(jobId, now);
      }
      return ["existing", existing];
    }

    if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") {
      return ["release_blocked"];
    }

    let metadata: Record<string, unknown>;
    try {
      metadata = this.studioRecordFromJson(serializedMetadata);
    } catch {
      return ["release_blocked"];
    }
    if (this.studioReleasePersistenceStatus(metadata, releaseFenceKey, releaseGeneration, releasePhase) !== 1) {
      return ["release_blocked"];
    }
    this.assertStringOrMissing(recentJobKey);

    let nextCanaryBudget: MemoryStudioCanaryBudget | null = null;
    if (releasePhase === "canary") {
      if (canaryBudgetEnabled !== "1") return ["canary_budget_required"];
      let canaryTotalCap: number;
      let canaryFalCap: number;
      try {
        canaryTotalCap = this.studioNonnegativeInteger(canaryTotalCapRaw);
        canaryFalCap = this.studioNonnegativeInteger(canaryFalCapRaw);
        this.assertStringOrMissing(canaryBudgetKey);
      } catch {
        return ["storage_error"];
      }
      if (metadata.provider !== "gmi" && metadata.provider !== "fal") return ["storage_error"];
      const currentBudget = this.studioCanaryBudget(canaryBudgetKey);
      if (currentBudget === "invalid") return ["storage_error"];
      const totalReservedCents = currentBudget?.totalReservedCents ?? 0;
      const falReservedCents = currentBudget?.falReservedCents ?? 0;
      if (totalReservedCents + cost > canaryTotalCap) return ["canary_total_cap"];
      if (metadata.provider === "fal" && falReservedCents + cost > canaryFalCap) return ["canary_fal_cap"];
      nextCanaryBudget = {
        schemaVersion: 1,
        totalReservedCents: totalReservedCents + cost,
        falReservedCents: metadata.provider === "fal" ? falReservedCents + cost : falReservedCents,
      };
    }

    if (this.getEntry(jobPayloadKey)) return ["storage_error"];
    if (this.studioString(activeKey) !== null) return ["active_job"];
    const userCount = this.studioCounter(userCountKey);
    if (userCount >= userLimit) return ["user_limit"];
    const spend = this.studioCounter(spendKey);
    if (spend + cost > spendCap) return ["daily_cap"];

    this.values.set(activeKey, { kind: "string", value: jobId, expiresAt: null });
    this.values.set(userCountKey, { kind: "string", value: String(userCount + 1), expiresAt: this.now() + dailyTtl * 1_000 });
    this.values.set(spendKey, { kind: "string", value: String(spend + cost), expiresAt: this.now() + dailyTtl * 1_000 });
    this.values.set(jobKey, { kind: "string", value: serializedMetadata, expiresAt: null });
    this.values.set(jobPayloadKey, { kind: "string", value: serializedPayload, expiresAt: this.now() + payloadTtl * 1_000 });
    this.values.set(recentJobKey, { kind: "string", value: jobId, expiresAt: null });
    this.getZSet(readyKey, true).value.set(jobId, now);
    if (nextCanaryBudget) {
      this.values.set(canaryBudgetKey, {
        kind: "string",
        value: JSON.stringify(nextCanaryBudget),
        expiresAt: null,
      });
    }
    return ["admitted"];
  }

  /**
   * In-memory mirror of the one-shot iMessage admission Lua transaction. Do
   * not compose the individual job/outbox evaluators here: that would make
   * tests accept a partial-write sequence that Redis never permits.
   */
  private evalStudioAdmitJobWithInitialNotifications(
    keys: readonly string[],
    args: readonly string[],
  ): readonly [string, string?] {
    try {
      this.assertStudioArity(keys, args, 17, 27);
      const activeKey = this.studioKey(keys, 0);
      const userCountKey = this.studioKey(keys, 1);
      const spendKey = this.studioKey(keys, 2);
      const jobKey = this.studioKey(keys, 3);
      const jobPayloadKey = this.studioKey(keys, 4);
      const readyJobKey = this.studioKey(keys, 5);
      const releaseFenceKey = this.studioKey(keys, 6);
      const readyOutboxKey = this.studioKey(keys, 7);
      const acknowledgementKey = this.studioKey(keys, 8);
      const acknowledgementPayloadKey = this.studioKey(keys, 9);
      const acknowledgementLaneKey = this.studioKey(keys, 10);
      const tapbackKey = this.studioKey(keys, 11);
      const tapbackPayloadKey = this.studioKey(keys, 12);
      const tapbackLaneKey = this.studioKey(keys, 13);
      const canaryBudgetKey = this.studioKey(keys, 14);
      const recentJobKey = this.studioKey(keys, 15);
      const inboxFifoMigrationKey = this.studioKey(keys, 16);
      const jobId = this.studioArgument(args, 0);
      const serializedMetadata = this.studioArgument(args, 1);
      const serializedPayload = this.studioArgument(args, 2);
      const dailyTtl = this.studioPositiveInteger(this.studioArgument(args, 3));
      const jobPayloadTtl = this.studioPositiveInteger(this.studioArgument(args, 4));
      const userLimit = this.studioPositiveInteger(this.studioArgument(args, 5));
      const dailySpendCap = this.studioNonnegativeInteger(this.studioArgument(args, 6));
      const reservedCents = this.studioNonnegativeInteger(this.studioArgument(args, 7));
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 8));
      const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 9));
      const releasePhase = this.studioArgument(args, 10);
      const perJobCap = this.studioNonnegativeInteger(this.studioArgument(args, 11));
      const serializedAcknowledgement = this.studioArgument(args, 12);
      const serializedAcknowledgementPayload = this.studioArgument(args, 13);
      const acknowledgementId = this.studioArgument(args, 14);
      const acknowledgementSequence = this.studioNonnegativeInteger(this.studioArgument(args, 15));
      const acknowledgementDueAt = this.studioNonnegativeInteger(this.studioArgument(args, 16));
      const serializedTapback = this.studioArgument(args, 17);
      const serializedTapbackPayload = this.studioArgument(args, 18);
      const tapbackId = this.studioArgument(args, 19);
      const tapbackSequence = this.studioNonnegativeInteger(this.studioArgument(args, 20));
      const tapbackDueAt = this.studioNonnegativeInteger(this.studioArgument(args, 21));
      const hasTapbackRaw = this.studioArgument(args, 22);
      const outboxPayloadTtl = this.studioPositiveInteger(this.studioArgument(args, 23));
      const canaryBudgetEnabled = this.studioArgument(args, 24);
      const canaryTotalCapRaw = this.studioArgument(args, 25);
      const canaryFalCapRaw = this.studioArgument(args, 26);
      if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") {
        return ["release_blocked"];
      }
      if (hasTapbackRaw !== "0" && hasTapbackRaw !== "1") return ["storage_error"];
      const hasTapback = hasTapbackRaw === "1";
      const expectedAcknowledgementSequence = hasTapback ? 2 : 1;
      if (acknowledgementSequence !== expectedAcknowledgementSequence || (hasTapback && tapbackSequence !== 1)) {
        return ["storage_error"];
      }

      const candidate = this.studioRecordFromJson(serializedMetadata);
      if (
        candidate.jobId !== jobId
        || typeof candidate.senderHash !== "string"
        || typeof candidate.threadHash !== "string"
      ) return ["storage_error"];

      // Existing job state wins before current release/cap checks, exactly as
      // the Redis script does for a retry after a release transition.
      this.assertStringOrMissing(jobKey);
      const existing = this.studioString(jobKey);
      if (existing !== null) {
        const record = this.studioRecordFailClosed(jobKey);
        if (
          !record
          || record.jobId !== jobId
          || record.senderHash !== candidate.senderHash
          || record.threadHash !== candidate.threadHash
          || !this.studioValidReleaseStamp(record.releaseStamp)
          || !this.studioInitialNotificationManifestMatches(
            record.initialNotifications,
            acknowledgementId,
            tapbackId,
            hasTapback,
          )
        ) return ["notification_incomplete"];
        this.assertStringOrMissing(acknowledgementKey);
        const acknowledgement = this.studioRecordFailClosed(acknowledgementKey);
        if (!this.studioInitialAdmissionOutboxMatches(
          acknowledgement,
          acknowledgementId,
          jobId,
          candidate.senderHash,
          "job-acknowledgement",
          record.releaseStamp,
          expectedAcknowledgementSequence,
        )) return ["notification_incomplete"];
        if (hasTapback) {
          this.assertStringOrMissing(tapbackKey);
          const tapback = this.studioRecordFailClosed(tapbackKey);
          if (!this.studioInitialAdmissionOutboxMatches(
            tapback,
            tapbackId,
            jobId,
            candidate.senderHash,
            "tapback",
            record.releaseStamp,
            1,
          )) return ["notification_incomplete"];
        }
        if (record.state === "admitted" || record.state === "executing") {
          this.assertZSetOrMissing(readyJobKey);
          this.getZSet(readyJobKey, true).value.set(jobId, now);
        }
        return ["existing", existing];
      }

      if (
        !this.studioValidReleaseStamp(candidate.releaseStamp)
        || candidate.releaseStamp.generation !== releaseGeneration
        || candidate.releaseStamp.phase !== releasePhase
      ) {
        return ["release_blocked"];
      }

      // Validate every key and every serialised record before the first
      // mutation, preserving the all-or-nothing Lua boundary in tests.
      this.assertStringOrMissing(activeKey);
      this.assertStringOrMissing(userCountKey);
      this.assertStringOrMissing(spendKey);
      this.assertStringOrMissing(jobPayloadKey);
      this.assertZSetOrMissing(readyJobKey);
      this.assertStringOrMissing(releaseFenceKey);
      this.assertZSetOrMissing(readyOutboxKey);
      this.assertStringOrMissing(acknowledgementKey);
      this.assertStringOrMissing(acknowledgementPayloadKey);
      this.assertZSetOrMissing(acknowledgementLaneKey);
      this.assertStringOrMissing(recentJobKey);
      this.assertStringOrMissing(inboxFifoMigrationKey);
      if (releasePhase === "canary") this.assertStringOrMissing(canaryBudgetKey);
      if (hasTapback) {
        this.assertStringOrMissing(tapbackKey);
        this.assertStringOrMissing(tapbackPayloadKey);
        this.assertZSetOrMissing(tapbackLaneKey);
      }
      if (this.studioReleasePersistenceStatus(candidate, releaseFenceKey, releaseGeneration, releasePhase) !== 1) {
        return ["release_blocked"];
      }
      // New iMessage job creation is impossible until the one-way inbox
      // migration reaches FIFO. This check lives in the same transaction as
      // reservation so an active config flag cannot race a cutover state.
      if (releasePhase !== "dark" && this.studioInboxFifoPhase(inboxFifoMigrationKey) !== "fifo") {
        return ["inbox_fifo_required"];
      }
      if (reservedCents > perJobCap) return ["per_job_cap"];

      let nextCanaryBudget: MemoryStudioCanaryBudget | null = null;
      if (releasePhase === "canary") {
        if (canaryBudgetEnabled !== "1") return ["canary_budget_required"];
        const canaryTotalCap = this.studioNonnegativeInteger(canaryTotalCapRaw);
        const canaryFalCap = this.studioNonnegativeInteger(canaryFalCapRaw);
        if (candidate.provider !== "gmi" && candidate.provider !== "fal") return ["storage_error"];
        const currentBudget = this.studioCanaryBudget(canaryBudgetKey);
        if (currentBudget === "invalid") return ["storage_error"];
        const totalReservedCents = currentBudget?.totalReservedCents ?? 0;
        const falReservedCents = currentBudget?.falReservedCents ?? 0;
        if (totalReservedCents + reservedCents > canaryTotalCap) return ["canary_total_cap"];
        if (candidate.provider === "fal" && falReservedCents + reservedCents > canaryFalCap) {
          return ["canary_fal_cap"];
        }
        nextCanaryBudget = {
          schemaVersion: 1,
          totalReservedCents: totalReservedCents + reservedCents,
          falReservedCents: candidate.provider === "fal" ? falReservedCents + reservedCents : falReservedCents,
        };
      }
      this.studioRecordFromJson(serializedPayload);
      const acknowledgement = this.studioRecordFromJson(serializedAcknowledgement);
      this.studioRecordFromJson(serializedAcknowledgementPayload);
      if (
        !this.studioInitialNotificationManifestMatches(
          candidate.initialNotifications,
          acknowledgementId,
          tapbackId,
          hasTapback,
        )
        || !this.studioInitialAdmissionOutboxMatches(
          acknowledgement,
          acknowledgementId,
          jobId,
          candidate.senderHash,
          "job-acknowledgement",
          candidate.releaseStamp,
          acknowledgementSequence,
          acknowledgementDueAt,
          true,
        )
      ) return ["storage_error"];
      if (hasTapback) {
        const tapback = this.studioRecordFromJson(serializedTapback);
        this.studioRecordFromJson(serializedTapbackPayload);
        if (!this.studioInitialAdmissionOutboxMatches(
          tapback,
          tapbackId,
          jobId,
          candidate.senderHash,
          "tapback",
          candidate.releaseStamp,
          tapbackSequence,
          tapbackDueAt,
          true,
        )) return ["storage_error"];
      }
      if (
        this.getEntry(jobPayloadKey)
        || this.getEntry(acknowledgementKey)
        || this.getEntry(acknowledgementPayloadKey)
        || (hasTapback && (this.getEntry(tapbackKey) || this.getEntry(tapbackPayloadKey)))
      ) return ["storage_error"];
      if (this.getZSet(readyJobKey, false)?.value.has(jobId)) return ["storage_error"];
      if ([...(this.getZSet(acknowledgementLaneKey, false)?.value.values() ?? [])]
        .some((score) => score === acknowledgementSequence)) return ["storage_error"];
      if (hasTapback && [...(this.getZSet(tapbackLaneKey, false)?.value.values() ?? [])]
        .some((score) => score === tapbackSequence)) return ["storage_error"];

      if (this.studioString(activeKey) !== null) return ["active_job"];
      const userCount = this.studioCounter(userCountKey);
      if (userCount >= userLimit) return ["user_limit"];
      const spend = this.studioCounter(spendKey);
      if (spend + reservedCents > dailySpendCap) return ["daily_cap"];

      this.values.set(activeKey, { kind: "string", value: jobId, expiresAt: null });
      this.values.set(userCountKey, {
        kind: "string",
        value: String(userCount + 1),
        expiresAt: this.now() + dailyTtl * 1_000,
      });
      this.values.set(spendKey, {
        kind: "string",
        value: String(spend + reservedCents),
        expiresAt: this.now() + dailyTtl * 1_000,
      });
      this.values.set(jobKey, { kind: "string", value: serializedMetadata, expiresAt: null });
      this.values.set(jobPayloadKey, {
        kind: "string",
        value: serializedPayload,
        expiresAt: this.now() + jobPayloadTtl * 1_000,
      });
      this.values.set(recentJobKey, { kind: "string", value: jobId, expiresAt: null });
      this.values.set(acknowledgementKey, { kind: "string", value: serializedAcknowledgement, expiresAt: null });
      this.values.set(acknowledgementPayloadKey, {
        kind: "string",
        value: serializedAcknowledgementPayload,
        expiresAt: this.now() + outboxPayloadTtl * 1_000,
      });
      this.getZSet(readyOutboxKey, true).value.set(acknowledgementId, acknowledgementDueAt);
      this.getZSet(acknowledgementLaneKey, true).value.set(acknowledgementId, acknowledgementSequence);
      if (hasTapback) {
        this.values.set(tapbackKey, { kind: "string", value: serializedTapback, expiresAt: null });
        this.values.set(tapbackPayloadKey, {
          kind: "string",
          value: serializedTapbackPayload,
          expiresAt: this.now() + outboxPayloadTtl * 1_000,
        });
        this.getZSet(readyOutboxKey, true).value.set(tapbackId, tapbackDueAt);
        this.getZSet(tapbackLaneKey, true).value.set(tapbackId, tapbackSequence);
      }
      // Scheduler publication is intentionally last, mirroring Lua.
      this.getZSet(readyJobKey, true).value.set(jobId, now);
      if (nextCanaryBudget) {
        this.values.set(canaryBudgetKey, {
          kind: "string",
          value: JSON.stringify(nextCanaryBudget),
          expiresAt: null,
        });
      }
      return ["admitted"];
    } catch {
      return ["storage_error"];
    }
  }

  private evalStudioClaimInbox(keys: readonly string[], args: readonly string[]): string | false {
    this.assertStudioArity(keys, args, 7, 4);
    const recordKey = this.studioKey(keys, 0);
    const envelopeKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const migrationKey = this.studioKey(keys, 4);
    const scopeQueueKey = this.studioKey(keys, 5);
    const scopeCursorKey = this.studioKey(keys, 6);
    const token = this.studioArgument(args, 0);
    const leaseTtl = this.studioPositiveInteger(this.studioArgument(args, 1));
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 2));
    const member = this.studioArgument(args, 3);
    this.assertStringOrMissing(migrationKey);
    this.assertZSetOrMissing(readyKey);
    const phase = this.studioInboxFifoPhase(migrationKey);
    if (phase === "invalid" || phase === "holding") return false;
    const record = this.studioRecordFailClosed(recordKey);
    if (!record || (record.state !== "received" && record.state !== "claimed")) return false;
    if (phase === "fifo") {
      this.assertZSetOrMissing(scopeQueueKey);
      this.assertStringOrMissing(scopeCursorKey);
      if (!this.studioInboxFifoCurrentHead(record, member, scopeQueueKey, scopeCursorKey)) return false;
      const dueAt = this.getZSet(readyKey, false)?.value.get(member);
      if (dueAt === undefined || dueAt > now) return false;
    }
    if (this.studioString(leaseKey) !== null) return false;
    this.studioSet(leaseKey, token, leaseTtl);
    record.state = "claimed";
    record.stateVersion = this.studioStateVersion(record) + 1;
    this.studioSetPersistentRecord(recordKey, record);
    this.getZSet(readyKey, true).value.set(member, now + leaseTtl);
    const envelope = this.studioString(envelopeKey);
    if (envelope !== null) {
      try {
        record.encryptedEnvelope = this.studioRecordFromJson(envelope);
      } catch {
        // Match the deployed Lua's privacy-safe outcome: a bad payload is
        // returned without an envelope and terminalized by the worker.
      }
    }
    return JSON.stringify(record);
  }

  private evalStudioFinishInbox(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 7, 6);
    const recordKey = this.studioKey(keys, 0);
    const envelopeKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const migrationKey = this.studioKey(keys, 4);
    const scopeQueueKey = this.studioKey(keys, 5);
    const scopeCursorKey = this.studioKey(keys, 6);
    const token = this.studioArgument(args, 0);
    const nextState = this.studioArgument(args, 1);
    const recordTtl = this.studioPositiveInteger(this.studioArgument(args, 3));
    const member = this.studioArgument(args, 4);
    const recordKeyPrefix = this.studioArgument(args, 5);
    if (nextState !== "processed" && nextState !== "maintenance_rejected" && nextState !== "failed") return 0;
    this.assertStringOrMissing(migrationKey);
    this.assertZSetOrMissing(readyKey);
    const phase = this.studioInboxFifoPhase(migrationKey);
    if (phase === "invalid" || phase === "holding") return 0;
    if (this.studioString(leaseKey) !== token) return 0;
    const record = this.studioRecordFailClosed(recordKey);
    if (!record || record.state !== "claimed") return 0;
    if (phase === "fifo") {
      this.assertZSetOrMissing(scopeQueueKey);
      this.assertStringOrMissing(scopeCursorKey);
      if (!this.studioInboxFifoCurrentHead(record, member, scopeQueueKey, scopeCursorKey)) return 0;
    }
    record.state = nextState;
    record.stateVersion = this.studioStateVersion(record) + 1;
    // Mirror the production Lua terminal scrub: receipt metadata remains for
    // idempotency, but the encrypted user envelope does not.
    delete record.encryptedEnvelope;
    this.studioSetRecord(recordKey, record, recordTtl);
    this.values.delete(envelopeKey);
    this.studioRemoveMember(readyKey, member);
    this.values.delete(leaseKey);
    if (phase === "legacy") return 1;
    this.studioRemoveMember(scopeQueueKey, member);
    const nextSequence = (record.scopeSequence as number) + 1;
    this.studioSetPersistent(scopeCursorKey, String(nextSequence));
    return this.studioPromoteInboxFifoNext(scopeQueueKey, readyKey, nextSequence, recordKeyPrefix);
  }

  private evalStudioRetryInbox(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 7, 6);
    const recordKey = this.studioKey(keys, 0);
    const envelopeKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const migrationKey = this.studioKey(keys, 4);
    const scopeQueueKey = this.studioKey(keys, 5);
    const scopeCursorKey = this.studioKey(keys, 6);
    const token = this.studioArgument(args, 0);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
    const retryAfter = this.studioNonnegativeInteger(this.studioArgument(args, 2));
    const recordTtl = this.studioPositiveInteger(this.studioArgument(args, 3));
    const member = this.studioArgument(args, 4);
    const recordKeyPrefix = this.studioArgument(args, 5);
    this.assertStringOrMissing(migrationKey);
    this.assertZSetOrMissing(readyKey);
    const phase = this.studioInboxFifoPhase(migrationKey);
    if (phase === "invalid" || phase === "holding") return 0;
    if (this.studioString(leaseKey) !== token) return 0;
    const record = this.studioRecordFailClosed(recordKey);
    if (!record || record.state !== "claimed") return 0;
    if (phase === "fifo") {
      this.assertZSetOrMissing(scopeQueueKey);
      this.assertStringOrMissing(scopeCursorKey);
      if (!this.studioInboxFifoCurrentHead(record, member, scopeQueueKey, scopeCursorKey)) return 0;
    }
    if (this.studioString(envelopeKey) === null) {
      record.state = "failed";
      record.stateVersion = this.studioStateVersion(record) + 1;
      delete record.encryptedEnvelope;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.studioRemoveMember(readyKey, member);
      this.values.delete(leaseKey);
      if (phase === "fifo") {
        this.studioRemoveMember(scopeQueueKey, member);
        const nextSequence = (record.scopeSequence as number) + 1;
        this.studioSetPersistent(scopeCursorKey, String(nextSequence));
        this.studioPromoteInboxFifoNext(scopeQueueKey, readyKey, nextSequence, recordKeyPrefix);
      }
      return 2;
    }
    record.state = "received";
    record.stateVersion = this.studioStateVersion(record) + 1;
    this.studioSetPersistentRecord(recordKey, record);
    this.getZSet(readyKey, true).value.set(member, now + retryAfter);
    this.values.delete(leaseKey);
    return 1;
  }

  private evalStudioClaimOutbox(keys: readonly string[], args: readonly string[]): string | false {
    this.assertStudioArity(keys, args, 9, 8);
    const recordKey = this.studioKey(keys, 0);
    const outboxPayloadKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const laneKey = this.studioKey(keys, 4);
    const controlKey = this.studioKey(keys, 5);
    const payloadKey = this.studioKey(keys, 6);
    const scopeCounterKey = this.studioKey(keys, 7);
    const releaseFenceKey = this.studioKey(keys, 8);
    const token = this.studioArgument(args, 0);
    const leaseTtl = this.studioPositiveInteger(this.studioArgument(args, 1));
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 2));
    const recordTtl = this.studioPositiveInteger(this.studioArgument(args, 3));
    const member = this.studioArgument(args, 4);
    const maxAttempts = this.studioPositiveInteger(this.studioArgument(args, 5));
    const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 6));
    const releasePhase = this.studioArgument(args, 7);
    if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") return false;
    const record = this.studioRecord(recordKey);
    if (!record || (record.state !== "intent" && record.state !== "retryable_failure")) return false;
    const laneHead = [...(this.getZSet(laneKey, false)?.value.entries() ?? [])]
      .sort(([firstMember, firstSequence], [secondMember, secondSequence]) =>
        firstSequence === secondSequence ? firstMember.localeCompare(secondMember) : firstSequence - secondSequence,
      )[0];
    if (!laneHead || laneHead[0] !== member) return false;
    const release = this.studioReleaseDispatchStatus(record, releaseFenceKey, releaseGeneration, releasePhase);
    if (release === 0) return false;
    if (release === -1) {
      record.state = "canceled";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.values.delete(outboxPayloadKey);
      this.studioRemoveMember(readyKey, member);
      this.studioRemoveMember(laneKey, member);
      return false;
    }
    const outboxPayload = this.studioString(outboxPayloadKey);
    if (outboxPayload === null) {
      record.state = "failed";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.studioRemoveMember(readyKey, member);
      this.studioRemoveMember(laneKey, member);
      return false;
    }
    if (!this.studioPendingGuardIsLive(record, controlKey, payloadKey, scopeCounterKey, now)) {
      record.state = "canceled";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.values.delete(outboxPayloadKey);
      this.studioRemoveMember(readyKey, member);
      this.studioRemoveMember(laneKey, member);
      return false;
    }
    const attempts = this.studioAttempts(record);
    if (record.state === "retryable_failure" && attempts >= maxAttempts) {
      record.state = "failed";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.values.delete(outboxPayloadKey);
      this.studioRemoveMember(readyKey, member);
      this.studioRemoveMember(laneKey, member);
      return false;
    }
    if (this.studioString(leaseKey) !== null) return false;
    this.studioSet(leaseKey, token, leaseTtl);
    record.state = "sending";
    record.attempts = attempts + 1;
    record.stateVersion = this.studioStateVersion(record) + 1;
    record.updatedAtMs = now;
    this.studioSetPersistentRecord(recordKey, record);
    this.getZSet(readyKey, true).value.set(member, now + leaseTtl);
    record.encryptedPayload = this.studioRecordFromJson(outboxPayload);
    return JSON.stringify(record);
  }

  private evalStudioAuthorizeOutboxDelivery(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 9, 7);
    const recordKey = this.studioKey(keys, 0);
    const outboxPayloadKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const laneKey = this.studioKey(keys, 4);
    const controlKey = this.studioKey(keys, 5);
    const payloadKey = this.studioKey(keys, 6);
    const scopeCounterKey = this.studioKey(keys, 7);
    const releaseFenceKey = this.studioKey(keys, 8);
    const token = this.studioArgument(args, 0);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
    const recordTtl = this.studioPositiveInteger(this.studioArgument(args, 2));
    const member = this.studioArgument(args, 3);
    const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 4));
    const releasePhase = this.studioArgument(args, 5);
    const backoff = this.studioNonnegativeInteger(this.studioArgument(args, 6));
    if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") return 0;
    if (this.studioString(leaseKey) !== token) return 0;
    const record = this.studioRecord(recordKey);
    if (!record || record.state !== "sending") return 0;
    const release = this.studioReleaseDispatchStatus(record, releaseFenceKey, releaseGeneration, releasePhase);
    if (release === 0) {
      record.state = "intent";
      record.attempts = Math.max(0, this.studioAttempts(record) - 1);
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetPersistentRecord(recordKey, record);
      this.values.delete(leaseKey);
      this.getZSet(readyKey, true).value.set(member, now + backoff);
      return 0;
    }
    if (release === -1) {
      record.state = "canceled";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.values.delete(outboxPayloadKey);
      this.values.delete(leaseKey);
      this.studioRemoveMember(readyKey, member);
      this.studioRemoveMember(laneKey, member);
      return 0;
    }
    if (this.studioString(outboxPayloadKey) === null) {
      record.state = "failed";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.values.delete(leaseKey);
      this.studioRemoveMember(readyKey, member);
      this.studioRemoveMember(laneKey, member);
      return 0;
    }
    if (!this.studioPendingGuardIsLive(record, controlKey, payloadKey, scopeCounterKey, now)) {
      record.state = "canceled";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, recordTtl);
      this.values.delete(outboxPayloadKey);
      this.values.delete(leaseKey);
      this.studioRemoveMember(readyKey, member);
      this.studioRemoveMember(laneKey, member);
      return 0;
    }
    return 1;
  }

  private evalStudioFinishOutbox(keys: readonly string[], args: readonly string[]): string | false {
    this.assertStudioArity(keys, args, 5, 7);
    const recordKey = this.studioKey(keys, 0);
    const outboxPayloadKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const laneKey = this.studioKey(keys, 4);
    const token = this.studioArgument(args, 0);
    let nextState = this.studioArgument(args, 1);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 2));
    const recordTtl = this.studioPositiveInteger(this.studioArgument(args, 3));
    const member = this.studioArgument(args, 4);
    const maxAttempts = this.studioPositiveInteger(this.studioArgument(args, 5));
    const backoff = this.studioNonnegativeInteger(this.studioArgument(args, 6));
    if (nextState !== "sent" && nextState !== "retryable_failure" && nextState !== "failed" && nextState !== "send_unknown") return false;
    if (this.studioString(leaseKey) !== token) return false;
    const record = this.studioRecord(recordKey);
    if (!record || record.state !== "sending") return false;
    const attempts = this.studioAttempts(record);
    if (nextState === "retryable_failure" && attempts >= maxAttempts) nextState = "failed";
    record.state = nextState;
    record.stateVersion = this.studioStateVersion(record) + 1;
    record.updatedAtMs = now;
    if (nextState === "sent" || nextState === "failed") {
      this.studioSetRecord(recordKey, record, recordTtl);
      this.values.delete(outboxPayloadKey);
    } else {
      this.studioSetPersistentRecord(recordKey, record);
    }
    this.values.delete(leaseKey);
    if (nextState === "retryable_failure") {
      this.getZSet(readyKey, true).value.set(member, now + attempts * backoff);
    } else {
      this.studioRemoveMember(readyKey, member);
      // A job-scoped Like is best-effort. Preserve its ambiguous audit state,
      // but allow the paired acknowledgement behind it to proceed.
      if (nextState !== "send_unknown" || (record.lane === "tapback" && record.jobId !== undefined)) {
        this.studioRemoveMember(laneKey, member);
      }
    }
    return JSON.stringify(record);
  }

  private evalStudioDeferOutboxBeforePhoton(keys: readonly string[], args: readonly string[]): string | false {
    this.assertStudioArity(keys, args, 4, 3);
    const recordKey = this.studioKey(keys, 0);
    const leaseKey = this.studioKey(keys, 1);
    const readyKey = this.studioKey(keys, 2);
    const laneKey = this.studioKey(keys, 3);
    const token = this.studioArgument(args, 0);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
    const member = this.studioArgument(args, 2);
    if (this.studioString(leaseKey) !== token) return false;
    const record = this.studioRecord(recordKey);
    if (!record || record.state !== "sending") return false;
    if (!this.getZSet(laneKey, false)?.value.has(member)) return false;
    const attempts = this.studioAttempts(record);
    if (attempts < 1) return false;
    const restoredAttempts = attempts - 1;
    record.attempts = restoredAttempts;
    // Match the Lua rollback: preserve a prior genuine retryable failure
    // instead of turning it into a fresh intent with zero accounting.
    record.state = restoredAttempts === 0 ? "intent" : "retryable_failure";
    record.stateVersion = this.studioStateVersion(record) + 1;
    record.updatedAtMs = now;
    this.studioSetPersistentRecord(recordKey, record);
    this.values.delete(leaseKey);
    this.getZSet(readyKey, true).value.set(member, now);
    return JSON.stringify(record);
  }

  private evalStudioReconcileExpiredOutbox(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 5, 3);
    const recordKey = this.studioKey(keys, 0);
    const _outboxPayloadKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 0));
    const recordTtl = this.studioPositiveInteger(this.studioArgument(args, 1));
    const member = this.studioArgument(args, 2);
    if (this.studioString(leaseKey) !== null) return 0;
    const record = this.studioRecord(recordKey);
    if (!record || record.state !== "sending") return 0;
    record.state = "send_unknown";
    record.stateVersion = this.studioStateVersion(record) + 1;
    record.updatedAtMs = now;
    this.studioSetPersistentRecord(recordKey, record);
    this.studioRemoveMember(readyKey, member);
    if (record.lane === "tapback" && record.jobId !== undefined) {
      this.studioRemoveMember(this.studioKey(keys, 4), member);
    }
    return 1;
  }

  private evalStudioTransitionJob(keys: readonly string[], args: readonly string[]): string | false {
    this.assertStudioArity(keys, args, 5, 7);
    const jobKey = this.studioKey(keys, 0);
    const jobPayloadKey = this.studioKey(keys, 1);
    const activeKey = this.studioKey(keys, 2);
    const readyKey = this.studioKey(keys, 3);
    const cursorKey = this.studioKey(keys, 4);
    const from = this.studioArgument(args, 0);
    const expectedVersion = this.studioPositiveInteger(this.studioArgument(args, 1));
    const to = this.studioArgument(args, 2);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    const ttl = this.studioPositiveInteger(this.studioArgument(args, 4));
    const jobId = this.studioArgument(args, 5);
    const terminal = this.studioArgument(args, 6);
    const record = this.studioRecord(jobKey);
    if (!record || record.state !== from || this.studioStateVersion(record) !== expectedVersion) return false;
    record.state = to;
    record.stateVersion = expectedVersion + 1;
    record.updatedAtMs = now;
    delete record.encryptedPayload;
    if (terminal === "1") {
      this.studioSetRecord(jobKey, record, ttl);
      this.values.delete(jobPayloadKey);
      if (this.studioString(activeKey) === jobId) this.values.delete(activeKey);
      this.studioRemoveMember(readyKey, jobId);
      const cursor = this.getEntry(cursorKey);
      if (cursor) cursor.expiresAt = this.now() + ttl * 1_000;
    } else {
      this.studioSetPersistentRecord(jobKey, record);
    }
    return JSON.stringify(record);
  }

  /** Exact in-memory mirror of the atomic admitted-job cancellation primitive. */
  private evalStudioCancelAdmittedJobWithNotification(keys: readonly string[], args: readonly string[]): number {
    try {
      this.assertStudioArity(keys, args, 9, 13);
      const activeKey = this.studioKey(keys, 0);
      const jobKey = this.studioKey(keys, 1);
      const jobPayloadKey = this.studioKey(keys, 2);
      const readyJobKey = this.studioKey(keys, 3);
      const outboxKey = this.studioKey(keys, 4);
      const outboxPayloadKey = this.studioKey(keys, 5);
      const readyOutboxKey = this.studioKey(keys, 6);
      const outboxLaneKey = this.studioKey(keys, 7);
      const releaseFenceKey = this.studioKey(keys, 8);
      const jobId = this.studioArgument(args, 0);
      const senderHash = this.studioArgument(args, 1);
      const expectedJobVersion = this.studioPositiveInteger(this.studioArgument(args, 2));
      const serializedOutbox = this.studioArgument(args, 3);
      const serializedOutboxPayload = this.studioArgument(args, 4);
      const outboxPayloadTtl = this.studioPositiveInteger(this.studioArgument(args, 5));
      const outboxDueAt = this.studioNonnegativeInteger(this.studioArgument(args, 6));
      const outboxId = this.studioArgument(args, 7);
      const outboxSequence = this.studioNonnegativeInteger(this.studioArgument(args, 8));
      const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 9));
      const releasePhase = this.studioArgument(args, 10);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 11));
      const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 12));
      this.assertStringOrMissing(activeKey);
      this.assertStringOrMissing(jobKey);
      this.assertStringOrMissing(jobPayloadKey);
      this.assertStringOrMissing(outboxKey);
      this.assertStringOrMissing(outboxPayloadKey);
      this.assertZSetOrMissing(readyJobKey);
      this.assertZSetOrMissing(readyOutboxKey);
      this.assertZSetOrMissing(outboxLaneKey);

      const existing = this.getEntry(outboxKey);
      if (existing) {
        const record = this.studioRecordFailClosed(outboxKey);
        return record
          && record.outboxId === outboxId
          && record.jobId === jobId
          && record.senderHash === senderHash
          && record.lane === "request-response"
          ? 2
          : -2;
      }

      if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") return -1;
      const outbox = this.studioRecordFromJson(serializedOutbox);
      if (
        outbox.outboxId !== outboxId
        || outbox.jobId !== jobId
        || outbox.senderHash !== senderHash
        || outbox.lane !== "request-response"
        || outbox.state !== "intent"
        || this.studioStateVersion(outbox) !== 1
        || outbox.sequence !== outboxSequence
      ) return -2;
      if (
        this.studioReleasePersistenceStatus(outbox, releaseFenceKey, releaseGeneration, releasePhase) !== 1
      ) return -1;

      if (this.studioString(activeKey) !== jobId) return 0;
      const job = this.studioRecordFailClosed(jobKey);
      if (
        !job
        || job.jobId !== jobId
        || job.senderHash !== senderHash
        || job.state !== "admitted"
        || this.studioStateVersion(job) !== expectedJobVersion
      ) return 0;
      if (this.getEntry(outboxPayloadKey)) return -2;
      const laneHasSequence = [...(this.getZSet(outboxLaneKey, false)?.value.entries() ?? [])]
        .some(([, score]) => score === outboxSequence);
      if (laneHasSequence) return -2;

      job.state = "canceled";
      job.stateVersion = expectedJobVersion + 1;
      job.updatedAtMs = now;
      delete job.encryptedPayload;
      this.studioSetRecord(jobKey, job, terminalTtl);
      this.values.delete(jobPayloadKey);
      if (this.studioString(activeKey) === jobId) this.values.delete(activeKey);
      this.studioRemoveMember(readyJobKey, jobId);
      this.studioSetPersistentRecord(outboxKey, outbox);
      this.studioSet(outboxPayloadKey, serializedOutboxPayload, outboxPayloadTtl * 1_000);
      this.getZSet(readyOutboxKey, true).value.set(outboxId, outboxDueAt);
      this.getZSet(outboxLaneKey, true).value.set(outboxId, outboxSequence);
      return 1;
    } catch {
      return -2;
    }
  }

  private evalStudioRescheduleClaimedJob(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 3, 5);
    const jobKey = this.studioKey(keys, 0);
    const leaseKey = this.studioKey(keys, 1);
    const readyKey = this.studioKey(keys, 2);
    const leaseToken = this.studioArgument(args, 0);
    const expectedVersion = this.studioPositiveInteger(this.studioArgument(args, 1));
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 2));
    const dueAt = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    const jobId = this.studioArgument(args, 4);
    if (this.studioString(leaseKey) !== leaseToken) return 0;
    const record = this.studioRecordFailClosed(jobKey);
    if (!record) {
      this.studioRemoveMember(readyKey, jobId);
      this.values.delete(leaseKey);
      return 2;
    }
    if (this.studioStateVersion(record) !== expectedVersion) return 0;
    if (
      record.state !== "admitted"
      && record.state !== "executing"
      && record.state !== "ready"
      && record.state !== "delivering"
    ) {
      this.studioRemoveMember(readyKey, jobId);
      this.values.delete(leaseKey);
      return 2;
    }
    if (dueAt < now) return 0;
    this.getZSet(readyKey, true).value.set(jobId, dueAt);
    this.values.delete(leaseKey);
    return 1;
  }

  private evalStudioBeginProviderSubmission(keys: readonly string[], args: readonly string[]): string {
    this.assertStudioArity(keys, args, 4, 7);
    const recordKey = this.studioKey(keys, 0);
    const leaseKey = this.studioKey(keys, 1);
    const releaseFenceKey = this.studioKey(keys, 2);
    const jobKey = this.studioKey(keys, 3);
    const token = this.studioArgument(args, 0);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
    const releaseGeneration = this.studioPositiveInteger(this.studioArgument(args, 2));
    const releasePhase = this.studioArgument(args, 3);
    const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 4));
    const expectedJobId = this.studioArgument(args, 5);
    const expectedJobVersion = this.studioPositiveInteger(this.studioArgument(args, 6));
    if (releasePhase !== "canary" && releasePhase !== "production") {
      return JSON.stringify({ kind: "unavailable" });
    }
    if (this.studioString(leaseKey) !== token) return JSON.stringify({ kind: "lease_lost" });
    const record = this.studioRecordFailClosed(recordKey);
    if (!record) return JSON.stringify({ kind: "unavailable" });
    const response = (kind: string): string => JSON.stringify({ kind, subrun: record });
    const state = record.state;
    if (state === "submitting") {
      record.state = "submission_unknown";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetPersistentRecord(recordKey, record);
      return response("submission_unknown");
    }
    if (state === "submitted") return response("already_submitted");
    if (state === "definitive_rejection") return response("definitive_rejection");
    if (state === "submission_unknown") return response("submission_unknown");
    if (state === "canceled") return response("canceled");
    if (state !== "reserved") return JSON.stringify({ kind: "unavailable" });

    const job = this.studioRecordFailClosed(jobKey);
    if (
      !job
      || record.jobId !== expectedJobId
      || job.jobId !== expectedJobId
      || job.state !== "executing"
      || typeof job.stateVersion !== "number"
      || !Number.isSafeInteger(job.stateVersion)
      || job.stateVersion < 1
      || job.stateVersion !== expectedJobVersion
    ) {
      record.state = "canceled";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, terminalTtl);
      return response("canceled");
    }

    const release = this.studioReleaseDispatchStatus(
      record,
      releaseFenceKey,
      releaseGeneration,
      releasePhase,
    );
    if (release !== 1) {
      record.state = "canceled";
      record.stateVersion = this.studioStateVersion(record) + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(recordKey, record, terminalTtl);
      return response("release_blocked");
    }
    record.state = "submitting";
    record.stateVersion = this.studioStateVersion(record) + 1;
    record.updatedAtMs = now;
    this.studioSetPersistentRecord(recordKey, record);
    return response("ready");
  }

  private evalStudioFinishProviderSubmission(keys: readonly string[], args: readonly string[]): string | false {
    this.assertStudioArity(keys, args, 6, 9);
    const recordKey = this.studioKey(keys, 0);
    const referenceKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const payloadKey = this.studioKey(keys, 3);
    const jobKey = this.studioKey(keys, 4);
    const releaseFenceKey = this.studioKey(keys, 5);
    const token = this.studioArgument(args, 0);
    const outcome = this.studioArgument(args, 1);
    const encryptedReference = this.studioArgument(args, 2);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 4));
    const releaseGeneration = this.studioPositiveInteger(this.studioArgument(args, 5));
    const releasePhase = this.studioArgument(args, 6);
    const expectedJobId = this.studioArgument(args, 7);
    const expectedJobVersion = this.studioPositiveInteger(this.studioArgument(args, 8));
    if (releasePhase !== "canary" && releasePhase !== "production") return false;
    if (this.studioString(leaseKey) !== token) return false;
    const record = this.studioRecordFailClosed(recordKey);
    if (!record || record.state !== "submitting") return false;
    record.stateVersion = this.studioStateVersion(record) + 1;
    record.updatedAtMs = now;
    switch (outcome) {
      case "accepted":
        if (!encryptedReference) return false;
        // Mirror Redis PTTL inside the same finish transition. A local worker
        // clock must never extend an encrypted provider reference beyond the
        // source job payload's actual remaining lifetime.
        const payload = this.getEntry(payloadKey);
        const payloadTtl = !payload || payload.expiresAt === null
          ? -1
          : Math.max(0, payload.expiresAt - this.now());
        if (payloadTtl < 1) {
          record.state = "submission_unknown";
          this.values.delete(referenceKey);
          this.studioSetPersistentRecord(recordKey, record);
          break;
        }
        const job = this.studioRecordFailClosed(jobKey);
        const jobIsCurrent = Boolean(
          job
          && record.jobId === expectedJobId
          && job.jobId === expectedJobId
          && job.state === "executing"
          && typeof job.stateVersion === "number"
          && Number.isSafeInteger(job.stateVersion)
          && job.stateVersion > 0
          && job.stateVersion === expectedJobVersion,
        );
        const releaseIsCurrent = this.studioReleaseDispatchStatus(
          record,
          releaseFenceKey,
          releaseGeneration,
          releasePhase,
        ) === 1;
        record.state = jobIsCurrent && releaseIsCurrent ? "submitted" : "submission_unknown";
        this.studioSet(referenceKey, encryptedReference, payloadTtl);
        this.studioSetPersistentRecord(recordKey, record);
        break;
      case "definitive_rejection":
        record.state = "definitive_rejection";
        this.values.delete(referenceKey);
        this.studioSetRecord(recordKey, record, terminalTtl);
        break;
      case "ambiguous":
        record.state = "submission_unknown";
        this.studioSetPersistentRecord(recordKey, record);
        break;
      default:
        return false;
    }
    this.values.delete(leaseKey);
    return JSON.stringify(record);
  }

  /** Mirrors Air's safe GET/poll fence; it never opens a submission path. */
  private evalStudioBeginProviderObservation(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    try {
      this.assertStudioArity(keys, args, 6, 8);
      const observationKey = this.studioKey(keys, 0);
      const leaseKey = this.studioKey(keys, 1);
      const subrunKey = this.studioKey(keys, 2);
      const referenceKey = this.studioKey(keys, 3);
      const jobKey = this.studioKey(keys, 4);
      const resultReferenceKey = this.studioKey(keys, 5);
      const token = this.studioArgument(args, 0);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
      const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 2));
      const observationId = this.studioArgument(args, 3);
      const subrunId = this.studioArgument(args, 4);
      const jobId = this.studioArgument(args, 5);
      const maxMaterializationAttempts = this.studioPositiveInteger(this.studioArgument(args, 6));
      const startResultMaterialization = this.studioArgument(args, 7);
      if (startResultMaterialization !== "0" && startResultMaterialization !== "1") return unavailable();
      this.assertStringOrMissing(resultReferenceKey);
      if (this.studioString(leaseKey) !== token) return JSON.stringify({ kind: "lease_lost" });
      const observation = this.studioProviderObservation(observationKey);
      if (
        observation === null
        || observation === "invalid"
        || observation.observationId !== observationId
        || observation.subrunId !== subrunId
        || observation.jobId !== jobId
      ) return unavailable();
      const response = (kind: string): string => JSON.stringify({ kind, observation });
      const terminal = (kind: string, state: string): string => {
        observation.state = state;
        delete observation.failureDisposition;
        observation.stateVersion = this.studioStateVersion(observation) + 1;
        observation.updatedAtMs = now;
        this.studioSetRecord(observationKey, observation, terminalTtl);
        this.values.delete(resultReferenceKey);
        this.values.delete(leaseKey);
        return response(kind);
      };
      if (this.studioProviderObservationTerminalState(observation.state)) {
        this.values.delete(resultReferenceKey);
        this.values.delete(leaseKey);
        return response(observation.state as string);
      }

      const subrun = this.studioRecordFailClosed(subrunKey);
      const job = this.studioRecordFailClosed(jobKey);
      const subrunValid = Boolean(
        subrun
        && subrun.subrunId === observation.subrunId
        && subrun.jobId === observation.jobId
        && subrun.state === "submitted"
        && this.studioStateVersion(subrun) === observation.subrunStateVersion,
      );
      const jobValid = Boolean(
        job
        && job.jobId === observation.jobId
        && job.state === "executing"
        && this.studioStateVersion(job) === observation.jobStateVersion,
      );
      if (!subrunValid || !jobValid) return terminal("canceled", "canceled");
      if (observation.state === "result_ready") {
        if (now >= (observation.deadlineAtMs as number)) {
          return terminal("deadline_exceeded", "deadline_exceeded");
        }
        if (now < (observation.nextDueAtMs as number)) {
          this.values.delete(leaseKey);
          return response("not_due");
        }
        if (this.studioRemainingTtl(resultReferenceKey) <= (observation.requestTimeoutMs as number)) {
          return terminal("reference_expired", "reference_expired");
        }
        if (startResultMaterialization === "1") {
          const materializationAttempts = observation.materializationAttempts ?? 0;
          if (
            typeof materializationAttempts !== "number"
            || !Number.isSafeInteger(materializationAttempts)
            || materializationAttempts < 0
          ) return unavailable();
          if (materializationAttempts >= maxMaterializationAttempts) {
            return terminal("attempts_exhausted", "attempts_exhausted");
          }
          observation.materializationAttempts = materializationAttempts + 1;
          observation.stateVersion = this.studioStateVersion(observation) + 1;
          observation.updatedAtMs = now;
          this.studioSetPersistentRecord(observationKey, observation);
        }
        return response("result_ready");
      }
      if (observation.state === "scheduled" && now < (observation.nextDueAtMs as number)) {
        this.values.delete(leaseKey);
        return response("not_due");
      }
      if (observation.state !== "scheduled" && observation.state !== "observing") return unavailable();
      if (now >= (observation.deadlineAtMs as number)) return terminal("deadline_exceeded", "deadline_exceeded");
      if ((observation.pollAttempts as number) >= (observation.maxPollAttempts as number)) {
        return terminal("attempts_exhausted", "attempts_exhausted");
      }
      if (this.studioRemainingTtl(referenceKey) <= (observation.requestTimeoutMs as number)) {
        return terminal("reference_expired", "reference_expired");
      }
      observation.state = "observing";
      observation.pollAttempts = (observation.pollAttempts as number) + 1;
      observation.nextDueAtMs = now;
      observation.stateVersion = this.studioStateVersion(observation) + 1;
      observation.updatedAtMs = now;
      this.studioSetPersistentRecord(observationKey, observation);
      return response("ready");
    } catch {
      return unavailable();
    }
  }

  /** Mirrors the state-version fence after one already-authorized provider GET. */
  private evalStudioFinishProviderObservation(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    try {
      this.assertStudioArity(keys, args, 5, 8);
      const observationKey = this.studioKey(keys, 0);
      const leaseKey = this.studioKey(keys, 1);
      const subrunKey = this.studioKey(keys, 2);
      const jobKey = this.studioKey(keys, 3);
      const resultReferenceKey = this.studioKey(keys, 4);
      const token = this.studioArgument(args, 0);
      const outcome = this.studioArgument(args, 1);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 2));
      const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 3));
      const observationId = this.studioArgument(args, 4);
      const subrunId = this.studioArgument(args, 5);
      const jobId = this.studioArgument(args, 6);
      const failureDisposition = this.studioArgument(args, 7);
      this.assertStringOrMissing(resultReferenceKey);
      if (
        (outcome === "failed" && failureDisposition !== "failed" && failureDisposition !== "reconciliation_required")
        || (outcome !== "failed" && failureDisposition !== "")
      ) return unavailable();
      if (this.studioString(leaseKey) !== token) return JSON.stringify({ kind: "lease_lost" });
      const observation = this.studioProviderObservation(observationKey);
      if (
        observation === null
        || observation === "invalid"
        || observation.observationId !== observationId
        || observation.subrunId !== subrunId
        || observation.jobId !== jobId
        || (observation.state !== "observing" && observation.state !== "result_ready")
      ) return unavailable();
      const response = (kind: string): string => JSON.stringify({ kind, observation });
      const terminal = (kind: string, state: string, disposition?: string): string => {
        observation.state = state;
        if (state === "failed") observation.failureDisposition = disposition;
        else delete observation.failureDisposition;
        observation.stateVersion = this.studioStateVersion(observation) + 1;
        observation.updatedAtMs = now;
        this.studioSetRecord(observationKey, observation, terminalTtl);
        this.values.delete(resultReferenceKey);
        this.values.delete(leaseKey);
        return response(kind);
      };

      const subrun = this.studioRecordFailClosed(subrunKey);
      const job = this.studioRecordFailClosed(jobKey);
      const subrunValid = Boolean(
        subrun
        && subrun.subrunId === observation.subrunId
        && subrun.jobId === observation.jobId
        && subrun.state === "submitted"
        && this.studioStateVersion(subrun) === observation.subrunStateVersion,
      );
      const jobValid = Boolean(
        job
        && job.jobId === observation.jobId
        && job.state === "executing"
        && this.studioStateVersion(job) === observation.jobStateVersion,
      );
      if (!subrunValid || !jobValid) return terminal("canceled", "canceled");
      if (outcome === "succeeded") {
        return observation.state === "observing"
          ? terminal("succeeded", "succeeded")
          : unavailable();
      }
      if (outcome === "failed") return terminal("failed", "failed", failureDisposition);
      if (outcome !== "pending") return unavailable();
      if (now >= (observation.deadlineAtMs as number)) return terminal("deadline_exceeded", "deadline_exceeded");
      if (
        observation.state === "observing"
        && (observation.pollAttempts as number) >= (observation.maxPollAttempts as number)
      ) {
        return terminal("attempts_exhausted", "attempts_exhausted");
      }
      if (
        observation.state === "result_ready"
        && this.studioRemainingTtl(resultReferenceKey) <= (observation.requestTimeoutMs as number)
      ) return terminal("reference_expired", "reference_expired");
      const deadlineAtMs = observation.deadlineAtMs as number;
      const interval = observation.pollIntervalMs as number;
      const nextDueAtMs = now > Number.MAX_SAFE_INTEGER - interval
        ? deadlineAtMs
        : Math.min(deadlineAtMs, now + interval);
      if (observation.state === "observing") observation.state = "scheduled";
      observation.nextDueAtMs = nextDueAtMs;
      observation.stateVersion = this.studioStateVersion(observation) + 1;
      observation.updatedAtMs = now;
      this.studioSetPersistentRecord(observationKey, observation);
      this.values.delete(leaseKey);
      return response("pending");
    } catch {
      return unavailable();
    }
  }

  /** Mirrors the private result-reference persistence boundary after one GET. */
  private evalStudioCommitProviderResultReference(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    const response = (
      kind: "result_ready" | "existing" | "lease_lost" | "payload_expiring" | "unavailable",
      observation?: Record<string, unknown>,
    ): string => JSON.stringify(observation === undefined ? { kind } : { kind, observation });
    try {
      this.assertStudioArity(keys, args, 7, 7);
      const observationKey = this.studioKey(keys, 0);
      const leaseKey = this.studioKey(keys, 1);
      const subrunKey = this.studioKey(keys, 2);
      const requestReferenceKey = this.studioKey(keys, 3);
      const jobKey = this.studioKey(keys, 4);
      const jobPayloadKey = this.studioKey(keys, 5);
      const resultReferenceKey = this.studioKey(keys, 6);
      const leaseToken = this.studioArgument(args, 0);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
      const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 2));
      const observationId = this.studioArgument(args, 3);
      const subrunId = this.studioArgument(args, 4);
      const jobId = this.studioArgument(args, 5);
      const encryptedReference = this.studioArgument(args, 6);
      if (
        !this.studioSafeText(leaseToken, 256)
        || !isMemoryOpaqueStudioIdentifier(observationId)
        || !isMemoryOpaqueStudioIdentifier(subrunId)
        || !this.studioSafeText(jobId, 160)
        || !this.studioEncryptedPayloadFromJson(encryptedReference)
      ) return unavailable();
      this.assertStringOrMissing(observationKey);
      this.assertStringOrMissing(leaseKey);
      this.assertStringOrMissing(subrunKey);
      this.assertStringOrMissing(requestReferenceKey);
      this.assertStringOrMissing(jobKey);
      this.assertStringOrMissing(jobPayloadKey);
      this.assertStringOrMissing(resultReferenceKey);
      if (this.studioString(leaseKey) !== leaseToken) return response("lease_lost");

      const observation = this.studioProviderObservation(observationKey);
      const subrun = this.studioRecordFailClosed(subrunKey);
      const job = this.studioRecordFailClosed(jobKey);
      if (
        observation === null
        || observation === "invalid"
        || !subrun
        || !job
        || observation.observationId !== observationId
        || observation.subrunId !== subrunId
        || observation.jobId !== jobId
        || subrun.schemaVersion !== 1
        || subrun.subrunId !== subrunId
        || subrun.jobId !== jobId
        || subrun.state !== "submitted"
        || this.studioStateVersion(subrun) !== observation.subrunStateVersion
        || job.schemaVersion !== 3
        || job.jobId !== jobId
        || job.state !== "executing"
        || this.studioStateVersion(job) !== observation.jobStateVersion
        || !this.studioSameReleaseStamp(subrun.releaseStamp, job.releaseStamp)
      ) return unavailable();

      if (observation.state === "result_ready") {
        const persisted = this.studioString(resultReferenceKey);
        const ttl = this.studioRemainingTtl(resultReferenceKey);
        if (persisted !== null && this.studioEncryptedPayloadFromJson(persisted) && ttl >= 1) {
          return response("existing", observation);
        }
        if (ttl < 1) return response("payload_expiring");
        return unavailable();
      }
      if (observation.state !== "observing") return unavailable();
      if (now >= (observation.deadlineAtMs as number)) return unavailable();

      const jobPayload = this.studioString(jobPayloadKey);
      if (jobPayload === null || !this.studioEncryptedPayloadFromJson(jobPayload)) return unavailable();
      const payloadTtl = this.studioRemainingTtl(jobPayloadKey);
      if (payloadTtl < 1) return response("payload_expiring");

      observation.state = "result_ready";
      delete observation.failureDisposition;
      observation.nextDueAtMs = now;
      observation.stateVersion = this.studioStateVersion(observation) + 1;
      observation.updatedAtMs = now;
      this.studioSetPersistentRecord(observationKey, observation);
      this.studioSet(resultReferenceKey, encryptedReference, payloadTtl);
      this.values.delete(requestReferenceKey);
      return response("result_ready", observation);
    } catch {
      return unavailable();
    }
  }

  /** Mirrors the all-or-nothing provider success → result-delivery commit. */
  private evalStudioFinalizePreparedProviderResult(keys: readonly string[], args: readonly string[]): string {
    const unavailable = (): string => JSON.stringify({ kind: "unavailable" });
    const response = (kind: "finalized" | "existing" | "lease_lost" | "release_blocked" | "payload_expiring" | "unavailable"): string =>
      JSON.stringify({ kind });
    try {
      this.assertStudioArity(keys, args, 17, 14);
      const observationKey = this.studioKey(keys, 0);
      const observationLeaseKey = this.studioKey(keys, 1);
      const subrunKey = this.studioKey(keys, 2);
      const jobKey = this.studioKey(keys, 3);
      const jobPayloadKey = this.studioKey(keys, 4);
      const preparationKey = this.studioKey(keys, 5);
      const preparationReferenceKey = this.studioKey(keys, 6);
      const cursorKey = this.studioKey(keys, 7);
      const preambleKey = this.studioKey(keys, 8);
      const preamblePayloadKey = this.studioKey(keys, 9);
      const attachmentKey = this.studioKey(keys, 10);
      const attachmentPayloadKey = this.studioKey(keys, 11);
      const readyOutboxKey = this.studioKey(keys, 12);
      const resultLaneKey = this.studioKey(keys, 13);
      const readyJobKey = this.studioKey(keys, 14);
      const releaseFenceKey = this.studioKey(keys, 15);
      const resultReferenceKey = this.studioKey(keys, 16);
      const leaseToken = this.studioArgument(args, 0);
      const now = this.studioNonnegativeInteger(this.studioArgument(args, 1));
      const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 2));
      const observationId = this.studioArgument(args, 3);
      const subrunId = this.studioArgument(args, 4);
      const jobId = this.studioArgument(args, 5);
      const preambleId = this.studioArgument(args, 6);
      const attachmentId = this.studioArgument(args, 7);
      const preambleMetadata = this.studioArgument(args, 8);
      const preamblePayload = this.studioArgument(args, 9);
      const attachmentMetadata = this.studioArgument(args, 10);
      const attachmentPayload = this.studioArgument(args, 11);
      const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 12));
      const releasePhase = this.studioArgument(args, 13);
      if (
        !this.studioSafeText(leaseToken, 256)
        || !isMemoryOpaqueStudioIdentifier(observationId)
        || !isMemoryOpaqueStudioIdentifier(subrunId)
        || !this.studioSafeText(jobId, 160)
        || !isMemoryOpaqueStudioIdentifier(preambleId)
        || !isMemoryOpaqueStudioIdentifier(attachmentId)
        || (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production")
      ) return unavailable();
      this.assertStringOrMissing(observationKey);
      this.assertStringOrMissing(observationLeaseKey);
      this.assertStringOrMissing(subrunKey);
      this.assertStringOrMissing(jobKey);
      this.assertStringOrMissing(jobPayloadKey);
      this.assertStringOrMissing(preparationKey);
      this.assertStringOrMissing(preparationReferenceKey);
      this.assertStringOrMissing(cursorKey);
      this.assertStringOrMissing(preambleKey);
      this.assertStringOrMissing(preamblePayloadKey);
      this.assertStringOrMissing(attachmentKey);
      this.assertStringOrMissing(attachmentPayloadKey);
      this.assertZSetOrMissing(readyOutboxKey);
      this.assertZSetOrMissing(resultLaneKey);
      this.assertZSetOrMissing(readyJobKey);
      this.assertStringOrMissing(releaseFenceKey);
      this.assertStringOrMissing(resultReferenceKey);
      if (this.studioString(observationLeaseKey) !== leaseToken) return response("lease_lost");

      const observation = this.studioProviderObservation(observationKey);
      const subrun = this.studioRecordFailClosed(subrunKey);
      const job = this.studioRecordFailClosed(jobKey);
      if (
        observation === null
        || observation === "invalid"
        || !subrun
        || !job
        || observation.observationId !== observationId
        || observation.subrunId !== subrunId
        || observation.jobId !== jobId
        || subrun.subrunId !== subrunId
        || subrun.jobId !== jobId
        || subrun.state !== "submitted"
        || job.jobId !== jobId
        || job.outputCount !== 1
        || !this.studioValidReleaseStamp(job.releaseStamp)
        || this.studioStateVersion(subrun) !== observation.subrunStateVersion
        || !this.studioSameReleaseStamp(subrun.releaseStamp, job.releaseStamp)
      ) return unavailable();
      const observedJobStateVersion = observation.jobStateVersion;
      if (
        typeof observedJobStateVersion !== "number"
        || !Number.isSafeInteger(observedJobStateVersion)
        || observedJobStateVersion < 1
      ) return unavailable();

      const validOutbox = (
        value: Record<string, unknown> | null,
        outboxId: string,
        sequence: number,
        resultOutputIndex: number | undefined,
        requireNew: boolean,
      ): boolean => {
        if (
          !value
          || value.schemaVersion !== 3
          || value.outboxId !== outboxId
          || value.jobId !== jobId
          || value.senderHash !== job.senderHash
          || value.lane !== "job-result"
          || value.sequence !== sequence
          || typeof value.attempts !== "number"
          || !Number.isSafeInteger(value.attempts)
          || value.attempts < 0
          || typeof value.stateVersion !== "number"
          || !Number.isSafeInteger(value.stateVersion)
          || value.stateVersion < 1
          || value.laneIdentity !== undefined
          || value.pendingGuard !== undefined
          || typeof value.createdAtMs !== "number"
          || !Number.isSafeInteger(value.createdAtMs)
          || value.createdAtMs < 0
          || typeof value.dueAtMs !== "number"
          || !Number.isSafeInteger(value.dueAtMs)
          || value.dueAtMs < 0
          || typeof value.updatedAtMs !== "number"
          || !Number.isSafeInteger(value.updatedAtMs)
          || value.updatedAtMs < 0
          || !this.studioValidReleaseStamp(value.releaseStamp)
        ) return false;
        if (resultOutputIndex === undefined) {
          if (value.resultOutputIndex !== undefined) return false;
        } else if (value.resultOutputIndex !== resultOutputIndex) return false;
        if (!requireNew) {
          return value.state === "intent" || value.state === "sending" || value.state === "sent"
            || value.state === "retryable_failure" || value.state === "failed" || value.state === "canceled"
            || value.state === "send_unknown" || value.state === "confirmed_absent" || value.state === "requeued";
        }
        if (
          value.state !== "intent"
          || value.attempts !== 0
          || value.stateVersion !== 1
          || value.terminalReason !== undefined
          || !this.studioSameReleaseStamp(value.releaseStamp, { generation: releaseGeneration, phase: releasePhase })
        ) return false;
        const allowed = resultOutputIndex === undefined
          ? new Set([
            "schemaVersion", "outboxId", "jobId", "senderHash", "lane", "sequence", "state",
            "attempts", "stateVersion", "releaseStamp", "createdAtMs", "dueAtMs", "updatedAtMs",
          ])
          : new Set([
            "schemaVersion", "outboxId", "jobId", "resultOutputIndex", "senderHash", "lane",
            "sequence", "state", "attempts", "stateVersion", "releaseStamp", "createdAtMs",
            "dueAtMs", "updatedAtMs",
          ]);
        return Object.keys(value).every((key) => allowed.has(key));
      };

      // Existing is deliberately strict: no half-written cursor, preamble, or
      // observation may be mistaken for a successful result after a crash.
      if (
        observation.state === "succeeded"
        && job.state === "ready"
        && this.studioStateVersion(job) === observedJobStateVersion + 1
      ) {
        const cursor = this.studioResultCursor(cursorKey);
        const preamble = this.studioRecordFailClosed(preambleKey);
        const attachment = this.studioRecordFailClosed(attachmentKey);
        const cursorMatches = cursor !== null
          && cursor !== "invalid"
          && cursor.jobId === jobId
          && cursor.senderHash === job.senderHash
          && cursor.threadHash === job.threadHash
          && cursor.outputCount === 1
          && cursor.nextOutputIndex === 2
          && cursor.outputs.length === 1
          && cursor.outputs[0]?.outputIndex === 1
          && cursor.outputs[0]?.outboxId === attachmentId
          && this.studioSameReleaseStamp(cursor.releaseStamp, job.releaseStamp);
        if (
          cursorMatches
          && validOutbox(preamble, preambleId, 0, undefined, false)
          && validOutbox(attachment, attachmentId, 1, 1, false)
          && this.studioSameReleaseStamp(preamble?.releaseStamp, attachment?.releaseStamp)
        ) return response("existing");
        return unavailable();
      }

      if (
        (observation.state !== "scheduled" && observation.state !== "observing" && observation.state !== "result_ready")
        || job.state !== "executing"
        || this.studioStateVersion(job) !== observedJobStateVersion
      ) return unavailable();
      const preparation = this.studioJobResultPreparation(preparationKey);
      const preparedReference = this.studioString(preparationReferenceKey);
      const jobPayload = this.studioString(jobPayloadKey);
      if (
        preparation === null
        || preparation === "invalid"
        || preparation.jobId !== jobId
        || preparation.outputIndex !== 1
        || preparation.state !== "prepared"
        || preparedReference === null
        || jobPayload === null
        || !this.studioEncryptedPayloadFromJson(preparedReference)
        || !this.studioEncryptedPayloadFromJson(jobPayload)
      ) return unavailable();
      const jobPayloadTtl = this.studioRemainingTtl(jobPayloadKey);
      const preparedReferenceTtl = this.studioRemainingTtl(preparationReferenceKey);
      if (jobPayloadTtl < 1 || preparedReferenceTtl < 1) return response("payload_expiring");
      if ((releasePhase !== "canary" && releasePhase !== "production") || releaseGeneration < 1) {
        return response("release_blocked");
      }
      const fence = this.studioReleaseFence(releaseFenceKey);
      if (
        fence === null
        || fence === "invalid"
        || fence.generation !== releaseGeneration
        || fence.phase !== releasePhase
      ) return response("release_blocked");
      if (
        this.getEntry(cursorKey)
        || this.getEntry(preambleKey)
        || this.getEntry(preamblePayloadKey)
        || this.getEntry(attachmentKey)
        || this.getEntry(attachmentPayloadKey)
        || [...(this.getZSet(resultLaneKey, false)?.value.values() ?? [])].some((score) => score === 0 || score === 1)
      ) return unavailable();
      const preamble = this.studioRecordFailClosedFromRaw(preambleMetadata);
      const attachment = this.studioRecordFailClosedFromRaw(attachmentMetadata);
      if (
        !preamble
        || !attachment
        || !this.studioEncryptedPayloadFromJson(preamblePayload)
        || !this.studioEncryptedPayloadFromJson(attachmentPayload)
        || !validOutbox(preamble, preambleId, 0, undefined, true)
        || !validOutbox(attachment, attachmentId, 1, 1, true)
      ) return unavailable();

      const deliveryTtl = Math.min(jobPayloadTtl, preparedReferenceTtl);
      const cursor = {
        schemaVersion: 1,
        jobId,
        senderHash: job.senderHash,
        threadHash: job.threadHash,
        outputCount: 1,
        releaseStamp: job.releaseStamp,
        nextOutputIndex: 2,
        outputs: [{ outputIndex: 1, outboxId: attachmentId }],
        createdAtMs: now,
        updatedAtMs: now,
      };
      observation.state = "succeeded";
      observation.stateVersion = this.studioStateVersion(observation) + 1;
      observation.updatedAtMs = now;
      job.state = "ready";
      job.stateVersion = this.studioStateVersion(job) + 1;
      job.updatedAtMs = now;
      this.studioSetPersistentRecord(cursorKey, cursor);
      this.studioSetPersistentRecord(preambleKey, preamble);
      this.studioSet(preamblePayloadKey, preamblePayload, deliveryTtl);
      this.studioSetPersistentRecord(attachmentKey, attachment);
      this.studioSet(attachmentPayloadKey, attachmentPayload, deliveryTtl);
      this.getZSet(readyOutboxKey, true).value.set(preambleId, preamble.dueAtMs as number);
      this.getZSet(readyOutboxKey, true).value.set(attachmentId, attachment.dueAtMs as number);
      this.getZSet(resultLaneKey, true).value.set(preambleId, 0);
      this.getZSet(resultLaneKey, true).value.set(attachmentId, 1);
      this.studioSetRecord(observationKey, observation, terminalTtl);
      this.values.delete(observationLeaseKey);
      this.values.delete(resultReferenceKey);
      this.studioSetPersistentRecord(jobKey, job);
      this.getZSet(readyJobKey, true).value.set(jobId, now);
      return response("finalized");
    } catch {
      return unavailable();
    }
  }

  private evalStudioAttachProviderRequestReconciliation(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 7, 7);
    const subrunKey = this.studioKey(keys, 0);
    const referenceKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const jobKey = this.studioKey(keys, 3);
    const payloadKey = this.studioKey(keys, 4);
    const auditKey = this.studioKey(keys, 5);
    const readyKey = this.studioKey(keys, 6);
    const expectedSubrunVersion = this.studioPositiveInteger(this.studioArgument(args, 0));
    const expectedJobVersion = this.studioPositiveInteger(this.studioArgument(args, 1));
    const expectedSubrunId = this.studioArgument(args, 2);
    const expectedJobId = this.studioArgument(args, 3);
    const encryptedReference = this.studioArgument(args, 4);
    const audit = this.studioArgument(args, 5);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 6));

    const subrun = this.studioRecordFailClosed(subrunKey);
    const job = this.studioRecordFailClosed(jobKey);
    if (!subrun || !job) return -3;
    if (
      subrun.state !== "submission_unknown"
      || typeof subrun.stateVersion !== "number"
      || !Number.isSafeInteger(subrun.stateVersion)
      || subrun.stateVersion !== expectedSubrunVersion
      || subrun.subrunId !== expectedSubrunId
      || subrun.jobId !== expectedJobId
      || job.jobId !== expectedJobId
      || job.state !== "reconciliation_required"
      || typeof job.stateVersion !== "number"
      || !Number.isSafeInteger(job.stateVersion)
      || job.stateVersion !== expectedJobVersion
    ) return -1;

    // Keep the new encrypted provider reference bounded by the original job
    // snapshot's remaining lifetime. Reconciliation must never extend it.
    const payload = this.getEntry(payloadKey);
    const payloadTtl = !payload || payload.expiresAt === null
      ? -1
      : Math.max(0, payload.expiresAt - this.now());
    if (payloadTtl < 1) return -2;
    if (this.getEntry(auditKey)) return -3;

    subrun.state = "submitted";
    subrun.stateVersion = expectedSubrunVersion + 1;
    subrun.updatedAtMs = now;
    job.state = "executing";
    job.stateVersion = expectedJobVersion + 1;
    job.updatedAtMs = now;
    this.studioSetPersistentRecord(subrunKey, subrun);
    this.studioSet(referenceKey, encryptedReference, payloadTtl);
    this.values.delete(leaseKey);
    this.studioSetPersistentRecord(jobKey, job);
    this.getZSet(readyKey, true).value.set(expectedJobId, now);
    this.studioSetPersistent(auditKey, audit);
    return 1;
  }

  private evalStudioTerminalizeProviderReconciliation(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 8, 7);
    const subrunKey = this.studioKey(keys, 0);
    const referenceKey = this.studioKey(keys, 1);
    const leaseKey = this.studioKey(keys, 2);
    const jobKey = this.studioKey(keys, 3);
    const payloadKey = this.studioKey(keys, 4);
    const activeJobKey = this.studioKey(keys, 5);
    const auditKey = this.studioKey(keys, 6);
    const readyKey = this.studioKey(keys, 7);
    const expectedSubrunVersion = this.studioPositiveInteger(this.studioArgument(args, 0));
    const expectedJobVersion = this.studioPositiveInteger(this.studioArgument(args, 1));
    const expectedSubrunId = this.studioArgument(args, 2);
    const expectedJobId = this.studioArgument(args, 3);
    const audit = this.studioArgument(args, 4);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 5));
    const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 6));

    const subrun = this.studioRecordFailClosed(subrunKey);
    const job = this.studioRecordFailClosed(jobKey);
    if (!subrun || !job) return -3;
    if (
      subrun.state !== "submission_unknown"
      || typeof subrun.stateVersion !== "number"
      || !Number.isSafeInteger(subrun.stateVersion)
      || subrun.stateVersion !== expectedSubrunVersion
      || subrun.subrunId !== expectedSubrunId
      || subrun.jobId !== expectedJobId
      || job.jobId !== expectedJobId
      || job.state !== "reconciliation_required"
      || typeof job.stateVersion !== "number"
      || !Number.isSafeInteger(job.stateVersion)
      || job.stateVersion !== expectedJobVersion
    ) return -1;
    if (this.getEntry(auditKey)) return -3;

    subrun.state = "reconciliation_terminalized";
    subrun.stateVersion = expectedSubrunVersion + 1;
    subrun.updatedAtMs = now;
    job.state = "failed";
    job.stateVersion = expectedJobVersion + 1;
    job.updatedAtMs = now;
    delete job.encryptedPayload;
    this.studioSetRecord(subrunKey, subrun, terminalTtl);
    this.values.delete(referenceKey);
    this.values.delete(leaseKey);
    this.studioSetRecord(jobKey, job, terminalTtl);
    this.values.delete(payloadKey);
    if (this.studioString(activeJobKey) === expectedJobId) this.values.delete(activeJobKey);
    this.studioRemoveMember(readyKey, expectedJobId);
    this.studioSetPersistent(auditKey, audit);
    return 1;
  }

  private evalStudioResolveOutboxSendReconciliation(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 5, 6);
    const outboxKey = this.studioKey(keys, 0);
    const payloadKey = this.studioKey(keys, 1);
    const readyKey = this.studioKey(keys, 2);
    const laneKey = this.studioKey(keys, 3);
    const auditKey = this.studioKey(keys, 4);
    const expectedVersion = this.studioPositiveInteger(this.studioArgument(args, 0));
    const resolution = this.studioArgument(args, 1);
    const audit = this.studioArgument(args, 2);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 4));
    const outboxId = this.studioArgument(args, 5);

    const record = this.studioRecordFailClosed(outboxKey);
    if (!record) return -3;
    if (
      record.outboxId !== outboxId
      || record.state !== "send_unknown"
      || typeof record.stateVersion !== "number"
      || !Number.isSafeInteger(record.stateVersion)
      || record.stateVersion !== expectedVersion
    ) return -1;
    if (resolution !== "sent" && resolution !== "confirmed_absent") return -3;
    if (this.getEntry(auditKey)) return -3;

    if (resolution === "sent") {
      record.state = "sent";
      record.stateVersion = expectedVersion + 1;
      record.updatedAtMs = now;
      this.studioSetRecord(outboxKey, record, terminalTtl);
      this.values.delete(payloadKey);
      this.studioRemoveMember(readyKey, outboxId);
      this.studioRemoveMember(laneKey, outboxId);
    } else {
      const payload = this.getEntry(payloadKey);
      const payloadTtl = !payload || payload.expiresAt === null
        ? -1
        : Math.max(0, payload.expiresAt - this.now());
      if (payloadTtl < 1) {
        record.state = "failed";
        record.terminalReason = "artifact_expired";
        record.stateVersion = expectedVersion + 1;
        record.updatedAtMs = now;
        this.studioSetRecord(outboxKey, record, terminalTtl);
        this.values.delete(payloadKey);
        this.studioRemoveMember(readyKey, outboxId);
        this.studioRemoveMember(laneKey, outboxId);
      } else {
        record.state = "confirmed_absent";
        record.stateVersion = expectedVersion + 1;
        record.updatedAtMs = now;
        this.studioSetPersistentRecord(outboxKey, record);
        this.studioRemoveMember(readyKey, outboxId);
      }
    }
    this.studioSetPersistent(auditKey, audit);
    return 1;
  }

  private evalStudioRequeueConfirmedAbsentOutbox(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 9, 12);
    const sourceKey = this.studioKey(keys, 0);
    const sourcePayloadKey = this.studioKey(keys, 1);
    const replacementKey = this.studioKey(keys, 2);
    const replacementPayloadKey = this.studioKey(keys, 3);
    const readyKey = this.studioKey(keys, 4);
    const laneKey = this.studioKey(keys, 5);
    const releaseFenceKey = this.studioKey(keys, 6);
    const auditKey = this.studioKey(keys, 7);
    const cursorKey = this.studioKey(keys, 8);
    const expectedSourceVersion = this.studioPositiveInteger(this.studioArgument(args, 0));
    const expectedSourceId = this.studioArgument(args, 1);
    const replacementRaw = this.studioArgument(args, 2);
    const replacementPayload = this.studioArgument(args, 3);
    const dueAtMs = this.studioNonnegativeInteger(this.studioArgument(args, 4));
    const replacementId = this.studioArgument(args, 5);
    const replacementSequence = this.studioNonnegativeInteger(this.studioArgument(args, 6));
    const releaseGeneration = this.studioNonnegativeInteger(this.studioArgument(args, 7));
    const releasePhase = this.studioArgument(args, 8);
    const audit = this.studioArgument(args, 9);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 10));
    const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 11));

    const source = this.studioRecordFailClosed(sourceKey);
    if (!source) return -4;
    if (
      source.outboxId !== expectedSourceId
      || source.state !== "confirmed_absent"
      || typeof source.stateVersion !== "number"
      || !Number.isSafeInteger(source.stateVersion)
      || source.stateVersion !== expectedSourceVersion
    ) return -1;
    if (this.getEntry(replacementKey)) return -1;

    let replacement: Record<string, unknown>;
    try {
      replacement = this.studioRecordFromJson(replacementRaw);
    } catch {
      return -4;
    }
    if (
      replacement.outboxId !== replacementId
      || replacement.state !== "intent"
      || replacement.stateVersion !== 1
    ) return -4;
    let cursor: MemoryStudioResultCursor | null = null;
    let cursorExpiresAt: number | null = null;
    if (source.resultOutputIndex !== undefined) {
      if (
        source.lane !== "job-result"
        || typeof source.jobId !== "string"
        || typeof source.resultOutputIndex !== "number"
        || !Number.isSafeInteger(source.resultOutputIndex)
        || source.resultOutputIndex < 1
        || replacement.jobId !== source.jobId
        || replacement.lane !== "job-result"
        || replacement.resultOutputIndex !== source.resultOutputIndex
        || replacement.sequence !== source.resultOutputIndex
      ) return -4;
      const parsedCursor = this.studioResultCursor(cursorKey);
      if (parsedCursor === null || parsedCursor === "invalid" || parsedCursor.jobId !== source.jobId) return -4;
      const cursorOutput = parsedCursor.outputs[source.resultOutputIndex - 1];
      if (
        !cursorOutput
        || cursorOutput.outputIndex !== source.resultOutputIndex
        || cursorOutput.outboxId !== source.outboxId
      ) return -1;
      const cursorEntry = this.getEntry(cursorKey);
      if (!cursorEntry || cursorEntry.kind !== "string") return -4;
      cursor = parsedCursor;
      cursorExpiresAt = cursorEntry.expiresAt;
    } else if (replacement.resultOutputIndex !== undefined) {
      return -4;
    }
    const sourcePayload = this.getEntry(sourcePayloadKey);
    const payloadTtl = !sourcePayload || sourcePayload.expiresAt === null
      ? -1
      : Math.max(0, sourcePayload.expiresAt - this.now());
    if (payloadTtl < 1) return -2;
    if (releasePhase !== "dark" && releasePhase !== "canary" && releasePhase !== "production") return -3;
    const stamp = replacement.releaseStamp;
    if (!stamp || typeof stamp !== "object" || Array.isArray(stamp)) return -3;
    const replacementStamp = stamp as Record<string, unknown>;
    if (
      typeof replacementStamp.generation !== "number"
      || !Number.isSafeInteger(replacementStamp.generation)
      || replacementStamp.generation < 0
      || replacementStamp.generation !== releaseGeneration
      || replacementStamp.phase !== releasePhase
    ) return -3;
    if (this.studioReleasePersistenceStatus(replacement, releaseFenceKey, releaseGeneration, releasePhase) !== 1) {
      return -3;
    }

    const lane = this.getZSet(laneKey, false);
    const laneEntries = [...(lane?.value.entries() ?? [])]
      .sort(([firstMember, firstSequence], [secondMember, secondSequence]) =>
        firstSequence === secondSequence ? firstMember.localeCompare(secondMember) : firstSequence - secondSequence,
      );
    if (laneEntries.length === 0 || laneEntries[0]?.[0] !== expectedSourceId) return -1;
    const matchingSequence = laneEntries.filter(([, score]) => score === replacementSequence);
    if (matchingSequence.length !== 1 || matchingSequence[0]?.[0] !== expectedSourceId) return -1;
    if (this.getEntry(auditKey)) return -5;

    source.state = "requeued";
    source.stateVersion = expectedSourceVersion + 1;
    source.updatedAtMs = now;
    delete source.terminalReason;
    this.studioSetRecord(sourceKey, source, terminalTtl);
    this.values.delete(sourcePayloadKey);
    this.studioSetPersistentRecord(replacementKey, replacement);
    this.studioSet(replacementPayloadKey, replacementPayload, payloadTtl);
    const ready = this.getZSet(readyKey, true);
    ready.value.set(replacementId, dueAtMs);
    const replacementLane = this.getZSet(laneKey, true);
    replacementLane.value.delete(expectedSourceId);
    replacementLane.value.set(replacementId, replacementSequence);
    if (cursor !== null) {
      // Preflight above proves this cursor slot is present and bound to source.
      const output = cursor.outputs[(source.resultOutputIndex as number) - 1] as {
        outputIndex: number;
        outboxId: string;
      };
      output.outboxId = replacementId;
      cursor.updatedAtMs = now;
      this.values.set(cursorKey, {
        kind: "string",
        value: JSON.stringify(cursor),
        expiresAt: cursorExpiresAt,
      });
    }
    this.studioSetPersistent(auditKey, audit);
    return 1;
  }

  private evalStudioTerminalizeExpiredOutboxReconciliation(keys: readonly string[], args: readonly string[]): number {
    this.assertStudioArity(keys, args, 5, 5);
    const outboxKey = this.studioKey(keys, 0);
    const payloadKey = this.studioKey(keys, 1);
    const readyKey = this.studioKey(keys, 2);
    const laneKey = this.studioKey(keys, 3);
    const auditKey = this.studioKey(keys, 4);
    const expectedVersion = this.studioPositiveInteger(this.studioArgument(args, 0));
    const expectedOutboxId = this.studioArgument(args, 1);
    const audit = this.studioArgument(args, 2);
    const now = this.studioNonnegativeInteger(this.studioArgument(args, 3));
    const terminalTtl = this.studioPositiveInteger(this.studioArgument(args, 4));

    const record = this.studioRecordFailClosed(outboxKey);
    if (!record) return -3;
    if (
      record.outboxId !== expectedOutboxId
      || record.state !== "confirmed_absent"
      || typeof record.stateVersion !== "number"
      || !Number.isSafeInteger(record.stateVersion)
      || record.stateVersion !== expectedVersion
    ) return -1;
    const payload = this.getEntry(payloadKey);
    const payloadTtl = !payload || payload.expiresAt === null
      ? -1
      : Math.max(0, payload.expiresAt - this.now());
    if (payloadTtl >= 1) return -2;
    if (this.getEntry(auditKey)) return -3;

    record.state = "failed";
    record.terminalReason = "artifact_expired";
    record.stateVersion = expectedVersion + 1;
    record.updatedAtMs = now;
    this.studioSetRecord(outboxKey, record, terminalTtl);
    this.values.delete(payloadKey);
    this.studioRemoveMember(readyKey, expectedOutboxId);
    this.studioRemoveMember(laneKey, expectedOutboxId);
    this.studioSetPersistent(auditKey, audit);
    return 1;
  }

  private assertStudioArity(keys: readonly string[], args: readonly string[], keyCount: number, argCount: number): void {
    if (keys.length !== keyCount || args.length !== argCount) throw new Error("invalid studio Lua arguments");
  }

  private studioKey(keys: readonly string[], index: number): string {
    const value = keys[index];
    if (!value) throw new Error("invalid studio Redis key");
    return value;
  }

  private studioArgument(args: readonly string[], index: number): string {
    const value = args[index];
    if (value === undefined) throw new Error("invalid studio Lua argument");
    return value;
  }

  private studioPositiveInteger(value: string): number {
    if (!isCanonicalPositiveInteger(value)) throw new Error("invalid studio integer");
    const number = Number(value);
    if (!Number.isSafeInteger(number)) throw new Error("invalid studio integer");
    return number;
  }

  private studioNonnegativeInteger(value: string): number {
    if (!isCanonicalNonnegativeInteger(value)) throw new Error("invalid studio integer");
    const number = Number(value);
    if (!Number.isSafeInteger(number)) throw new Error("invalid studio integer");
    return number;
  }

  private studioString(key: string): string | null {
    const entry = this.getEntry(key);
    if (!entry) return null;
    if (entry.kind !== "string") throw new Error("Redis key has the wrong type");
    return entry.value;
  }

  private studioInboxFifoPhase(key: string): "legacy" | "holding" | "fifo" | "invalid" {
    const raw = this.studioString(key);
    if (raw === null) return "legacy";
    let record: Record<string, unknown>;
    try {
      record = this.studioRecordFromJson(raw);
    } catch {
      return "invalid";
    }
    if (record.schemaVersion !== 1) return "invalid";
    if (record.phase === "holding" || record.phase === "fifo") return record.phase;
    return "invalid";
  }

  private studioInboxFifoHead(
    queueKey: string,
  ): { readonly receiptId: string; readonly scopeSequence: number } | "invalid" | null {
    const queue = this.getZSet(queueKey, false);
    if (!queue || queue.value.size === 0) return null;
    const ordered = [...queue.value.entries()].sort(([firstMember, firstSequence], [secondMember, secondSequence]) =>
      firstSequence === secondSequence ? firstMember.localeCompare(secondMember) : firstSequence - secondSequence,
    );
    const [receiptId, scopeSequence] = ordered[0] ?? [];
    if (
      typeof receiptId !== "string"
      || !/^[a-f0-9]{64}$/.test(receiptId)
      || typeof scopeSequence !== "number"
      || !Number.isSafeInteger(scopeSequence)
      || scopeSequence < 1
      || ordered.filter(([, sequence]) => sequence === scopeSequence).length !== 1
    ) return "invalid";
    return { receiptId, scopeSequence };
  }

  private studioInboxFifoCurrentHead(
    record: Record<string, unknown>,
    member: string,
    queueKey: string,
    cursorKey: string,
  ): boolean {
    const scopeSequence = record.scopeSequence;
    if (
      record.receiptId !== member
      || typeof scopeSequence !== "number"
      || !Number.isSafeInteger(scopeSequence)
      || scopeSequence < 1
    ) return false;
    let cursor: number;
    try {
      const raw = this.studioString(cursorKey);
      if (raw === null) return false;
      cursor = this.studioPositiveInteger(raw);
    } catch {
      return false;
    }
    const head = this.studioInboxFifoHead(queueKey);
    return head !== null
      && head !== "invalid"
      && cursor === scopeSequence
      && head.receiptId === member
      && head.scopeSequence === scopeSequence;
  }

  private studioInboxFifoReadyRecord(
    record: Record<string, unknown> | null,
    receiptId: string,
    scopeSequence: number,
  ): record is Record<string, unknown> & { readonly receivedAtMs: number } {
    return record !== null
      && record.receiptId === receiptId
      && record.scopeSequence === scopeSequence
      && record.state === "received"
      && typeof record.receivedAtMs === "number"
      && Number.isSafeInteger(record.receivedAtMs)
      && record.receivedAtMs >= 0;
  }

  /**
   * A missing/corrupt/gapped successor stays unpromoted. Returning `2` is a
   * deliberate integrity hold, not permission to skip it.
   */
  private studioPromoteInboxFifoNext(
    queueKey: string,
    readyKey: string,
    nextSequence: number,
    recordKeyPrefix: string,
  ): 1 | 2 {
    const head = this.studioInboxFifoHead(queueKey);
    if (head === null) return 1;
    if (head === "invalid" || head.scopeSequence !== nextSequence || !recordKeyPrefix) return 2;
    const record = this.studioRecordFailClosed(`${recordKeyPrefix}${head.receiptId}`);
    if (!this.studioInboxFifoReadyRecord(record, head.receiptId, nextSequence)) return 2;
    this.getZSet(readyKey, true).value.set(head.receiptId, record.receivedAtMs);
    return 1;
  }

  private studioCounter(key: string): number {
    const raw = this.studioString(key);
    if (raw === null) return 0;
    return this.studioNonnegativeInteger(raw);
  }

  private studioRecord(key: string): Record<string, unknown> | null {
    const raw = this.studioString(key);
    if (raw === null) return null;
    return this.studioRecordFromJson(raw);
  }

  /** Provider-side corruption must fail closed like Lua's guarded cjson decode. */
  private studioRecordFailClosed(key: string): Record<string, unknown> | null {
    try {
      return this.studioRecord(key);
    } catch {
      return null;
    }
  }

  private studioRecordFromJson(raw: string): Record<string, unknown> {
    try {
      const parsed: unknown = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("invalid studio record");
      return parsed as Record<string, unknown>;
    } catch {
      throw new Error("invalid studio record");
    }
  }

  private studioRecordFailClosedFromRaw(raw: string): Record<string, unknown> | null {
    try {
      return this.studioRecordFromJson(raw);
    } catch {
      return null;
    }
  }

  private studioSafeText(value: unknown, maximumLength: number): value is string {
    return typeof value === "string"
      && value.length > 0
      && value.length <= maximumLength
      && value.trim() === value
      && !/[\u0000\r\n]/u.test(value);
  }

  private studioResultCursor(key: string): MemoryStudioResultCursor | "invalid" | null {
    const record = this.studioRecordFailClosed(key);
    if (record === null) return null;
    const outputCount = record.outputCount;
    const nextOutputIndex = record.nextOutputIndex;
    const outputs = record.outputs;
    if (
      record.schemaVersion !== 1
      || !this.studioSafeText(record.jobId, 160)
      || !isMemoryOpaqueStudioIdentifier(record.senderHash)
      || !isMemoryOpaqueStudioIdentifier(record.threadHash)
      || typeof outputCount !== "number"
      || !Number.isSafeInteger(outputCount)
      || outputCount < 1
      || !this.studioValidReleaseStamp(record.releaseStamp)
      || typeof nextOutputIndex !== "number"
      || !Number.isSafeInteger(nextOutputIndex)
      || nextOutputIndex < 1
      || nextOutputIndex > outputCount + 1
      || !Array.isArray(outputs)
      || outputs.length !== nextOutputIndex - 1
      || typeof record.createdAtMs !== "number"
      || !Number.isSafeInteger(record.createdAtMs)
      || record.createdAtMs < 0
      || typeof record.updatedAtMs !== "number"
      || !Number.isSafeInteger(record.updatedAtMs)
      || record.updatedAtMs < 0
    ) return "invalid";
    const normalized: Array<{ outputIndex: number; outboxId: string }> = [];
    for (const [index, output] of outputs.entries()) {
      if (!output || typeof output !== "object" || Array.isArray(output)) return "invalid";
      const candidate = output as Record<string, unknown>;
      if (candidate.outputIndex !== index + 1 || !isMemoryOpaqueStudioIdentifier(candidate.outboxId)) return "invalid";
      normalized.push({ outputIndex: index + 1, outboxId: candidate.outboxId });
    }
    return {
      schemaVersion: 1,
      jobId: record.jobId as string,
      senderHash: record.senderHash as string,
      threadHash: record.threadHash as string,
      outputCount,
      releaseStamp: record.releaseStamp as Record<string, unknown>,
      nextOutputIndex,
      outputs: normalized,
      createdAtMs: record.createdAtMs as number,
      updatedAtMs: record.updatedAtMs as number,
    };
  }

  private studioJobResultPreparation(key: string): Record<string, unknown> | "invalid" | null {
    const record = this.studioRecordFailClosed(key);
    if (record === null) return null;
    const outputIndex = record.outputIndex;
    const state = record.state;
    if (
      record.schemaVersion !== 1
      || !isMemoryOpaqueStudioIdentifier(record.preparationId)
      || !this.studioSafeText(record.jobId, 160)
      || typeof outputIndex !== "number"
      || !Number.isSafeInteger(outputIndex)
      || outputIndex < 1
      || outputIndex > 4
      || (state !== "reserved"
        && state !== "uploading"
        && state !== "prepared"
        && state !== "upload_unknown"
        && state !== "failed"
        && state !== "canceled")
      || typeof record.stateVersion !== "number"
      || !Number.isSafeInteger(record.stateVersion)
      || record.stateVersion < 1
      || typeof record.createdAtMs !== "number"
      || !Number.isSafeInteger(record.createdAtMs)
      || record.createdAtMs < 0
      || typeof record.updatedAtMs !== "number"
      || !Number.isSafeInteger(record.updatedAtMs)
      || record.updatedAtMs < 0
    ) return "invalid";
    return record;
  }

  private studioProviderObservation(key: string): Record<string, unknown> | "invalid" | null {
    const record = this.studioRecordFailClosed(key);
    if (record === null) return null;
    const state = record.state;
    const failureDisposition = record.failureDisposition;
    const pollAttempts = record.pollAttempts;
    const materializationAttempts = record.materializationAttempts;
    const maxPollAttempts = record.maxPollAttempts;
    const requestTimeoutMs = record.requestTimeoutMs;
    const pollIntervalMs = record.pollIntervalMs;
    const deadlineAtMs = record.deadlineAtMs;
    const nextDueAtMs = record.nextDueAtMs;
    if (
      record.schemaVersion !== 1
      || !isMemoryOpaqueStudioIdentifier(record.observationId)
      || !isMemoryOpaqueStudioIdentifier(record.subrunId)
      || !this.studioSafeText(record.jobId, 160)
      || typeof record.subrunStateVersion !== "number"
      || !Number.isSafeInteger(record.subrunStateVersion)
      || record.subrunStateVersion < 1
      || typeof record.jobStateVersion !== "number"
      || !Number.isSafeInteger(record.jobStateVersion)
      || record.jobStateVersion < 1
      || (state !== "scheduled"
        && state !== "observing"
        && state !== "result_ready"
        && state !== "succeeded"
        && state !== "failed"
        && state !== "deadline_exceeded"
        && state !== "attempts_exhausted"
        && state !== "reference_expired"
        && state !== "canceled")
      || (state === "failed"
        ? (failureDisposition !== undefined
          && failureDisposition !== "failed"
          && failureDisposition !== "reconciliation_required")
        : failureDisposition !== undefined)
      || typeof pollAttempts !== "number"
      || !Number.isSafeInteger(pollAttempts)
      || pollAttempts < 0
      || (materializationAttempts !== undefined
        && (typeof materializationAttempts !== "number"
          || !Number.isSafeInteger(materializationAttempts)
          || materializationAttempts < 0
          || materializationAttempts > 10_000))
      || typeof maxPollAttempts !== "number"
      || !Number.isSafeInteger(maxPollAttempts)
      || maxPollAttempts < 1
      || maxPollAttempts > 10_000
      || pollAttempts > maxPollAttempts
      || typeof requestTimeoutMs !== "number"
      || !Number.isSafeInteger(requestTimeoutMs)
      || requestTimeoutMs < 1_000
      || requestTimeoutMs > 900_000
      || typeof pollIntervalMs !== "number"
      || !Number.isSafeInteger(pollIntervalMs)
      || pollIntervalMs < 250
      || pollIntervalMs > 300_000
      || typeof deadlineAtMs !== "number"
      || !Number.isSafeInteger(deadlineAtMs)
      || deadlineAtMs < 0
      || typeof nextDueAtMs !== "number"
      || !Number.isSafeInteger(nextDueAtMs)
      || nextDueAtMs < 0
      || nextDueAtMs > deadlineAtMs
      || typeof record.stateVersion !== "number"
      || !Number.isSafeInteger(record.stateVersion)
      || record.stateVersion < 1
      || typeof record.createdAtMs !== "number"
      || !Number.isSafeInteger(record.createdAtMs)
      || record.createdAtMs < 0
      || typeof record.updatedAtMs !== "number"
      || !Number.isSafeInteger(record.updatedAtMs)
      || record.updatedAtMs < 0
    ) return "invalid";
    return record;
  }

  private studioProviderObservationTerminalState(value: unknown): boolean {
    return value === "succeeded"
      || value === "failed"
      || value === "deadline_exceeded"
      || value === "attempts_exhausted"
      || value === "reference_expired"
      || value === "canceled";
  }

  /** Redis PTTL shape: -2 missing, -1 persistent, otherwise milliseconds. */
  private studioRemainingTtl(key: string): number {
    const entry = this.getEntry(key);
    if (!entry) return -2;
    return entry.expiresAt === null ? -1 : Math.max(0, entry.expiresAt - this.now());
  }

  private studioValidReleaseStamp(value: unknown): value is Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const stamp = value as Record<string, unknown>;
    return typeof stamp.generation === "number"
      && Number.isSafeInteger(stamp.generation)
      && stamp.generation >= 0
      && (stamp.phase === "dark" || stamp.phase === "canary" || stamp.phase === "production")
      && (stamp.phase === "dark" || stamp.generation >= 1);
  }

  private studioSameReleaseStamp(left: unknown, right: unknown): boolean {
    return this.studioValidReleaseStamp(left)
      && this.studioValidReleaseStamp(right)
      && left.generation === right.generation
      && left.phase === right.phase;
  }

  private studioOnboardingMarkerFromJson(raw: string): MemoryStudioOnboardingMarker | null {
    let value: Record<string, unknown>;
    try {
      value = this.studioRecordFromJson(raw);
    } catch {
      return null;
    }
    const onboardingVersion = value.onboardingVersion;
    if (
      value.schemaVersion !== 1
      || typeof onboardingVersion !== "number"
      || !Number.isSafeInteger(onboardingVersion)
      || onboardingVersion < 1
      || !isMemoryOpaqueStudioIdentifier(value.receiptId)
      || !isMemoryOpaqueStudioIdentifier(value.firstOutboxId)
      || !isMemoryOpaqueStudioIdentifier(value.secondOutboxId)
      || value.firstOutboxId === value.secondOutboxId
    ) return null;
    return {
      schemaVersion: 1,
      onboardingVersion,
      receiptId: value.receiptId,
      firstOutboxId: value.firstOutboxId,
      secondOutboxId: value.secondOutboxId,
    };
  }

  private studioCanaryBudget(key: string): MemoryStudioCanaryBudget | "invalid" | null {
    const record = this.studioRecordFailClosed(key);
    if (record === null) return null;
    const totalReservedCents = record.totalReservedCents;
    const falReservedCents = record.falReservedCents;
    if (
      record.schemaVersion !== 1
      || typeof totalReservedCents !== "number"
      || !Number.isSafeInteger(totalReservedCents)
      || totalReservedCents < 0
      || typeof falReservedCents !== "number"
      || !Number.isSafeInteger(falReservedCents)
      || falReservedCents < 0
    ) return "invalid";
    return { schemaVersion: 1, totalReservedCents, falReservedCents };
  }

  private studioEncryptedPayloadFromJson(raw: string): boolean {
    let value: Record<string, unknown>;
    try {
      value = this.studioRecordFromJson(raw);
    } catch {
      return false;
    }
    return value.version === 1
      && value.algorithm === "aes-256-gcm"
      && typeof value.iv === "string"
      && typeof value.ciphertext === "string"
      && typeof value.tag === "string";
  }

  private studioValidNewOnboardingOutbox(
    record: Record<string, unknown>,
    outboxId: string,
    senderHash: string,
    sequence: number,
    dueAtMs: number,
    releaseGeneration: number,
    releasePhase: MemoryStudioReleaseFence["phase"],
  ): boolean {
    return record.schemaVersion === 3
      && record.outboxId === outboxId
      && record.senderHash === senderHash
      && record.lane === "onboarding"
      && record.jobId === undefined
      && record.laneIdentity === undefined
      && record.pendingGuard === undefined
      && record.encryptedPayload === undefined
      && record.sequence === sequence
      && record.state === "intent"
      && record.attempts === 0
      && record.stateVersion === 1
      && typeof record.createdAtMs === "number"
      && Number.isSafeInteger(record.createdAtMs)
      && record.createdAtMs >= 0
      && record.dueAtMs === dueAtMs
      && typeof record.updatedAtMs === "number"
      && Number.isSafeInteger(record.updatedAtMs)
      && record.updatedAtMs >= 0
      && this.studioSameReleaseStamp(record.releaseStamp, {
        generation: releaseGeneration,
        phase: releasePhase,
      });
  }

  private studioInitialNotificationManifestMatches(
    value: unknown,
    acknowledgementId: string,
    tapbackId: string,
    hasTapback: boolean,
  ): boolean {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const manifest = value as Record<string, unknown>;
    if (
      manifest.schemaVersion !== 1
      || manifest.acknowledgementOutboxId !== acknowledgementId
    ) return false;
    return hasTapback
      ? manifest.tapbackOutboxId === tapbackId
      : manifest.tapbackOutboxId === undefined;
  }

  private studioInitialAdmissionOutboxMatches(
    record: Record<string, unknown> | null,
    outboxId: string,
    jobId: string,
    senderHash: unknown,
    lane: "job-acknowledgement" | "tapback",
    releaseStamp: unknown,
    sequence: number,
    dueAtMs?: number,
    requireIntent = false,
  ): boolean {
    if (!record) return false;
    if (
      record.outboxId !== outboxId
      || record.jobId !== jobId
      || record.senderHash !== senderHash
      || record.lane !== lane
      || record.sequence !== sequence
      || !this.studioSameReleaseStamp(record.releaseStamp, releaseStamp)
    ) return false;
    if (!requireIntent) return true;
    return record.state === "intent"
      && record.attempts === 0
      && record.stateVersion === 1
      && record.dueAtMs === dueAtMs;
  }

  private studioReleaseFence(key: string): MemoryStudioReleaseFence | "invalid" | null {
    const raw = this.studioString(key);
    if (raw === null) return null;
    let record: Record<string, unknown>;
    try {
      record = this.studioRecordFromJson(raw);
    } catch {
      // Fence corruption must be an operationally unavailable state, never a
      // test-only exception that hides the deployed fail-closed behavior.
      return "invalid";
    }
    const generation = record.generation;
    const phase = record.phase;
    if (
      record.schemaVersion !== 1
      || typeof generation !== "number" || !Number.isSafeInteger(generation) || generation < 0
      || (phase !== "dark" && phase !== "canary" && phase !== "production")
    ) return "invalid";
    return { schemaVersion: 1, generation, phase };
  }

  /** -1 known stale/malformed, 0 unavailable/corrupt, 1 exact current. */
  private studioReleaseDispatchStatus(
    record: Record<string, unknown>,
    fenceKey: string,
    expectedGeneration: number,
    expectedPhase: MemoryStudioReleaseFence["phase"],
  ): -1 | 0 | 1 {
    const stamp = record.releaseStamp;
    if (!stamp || typeof stamp !== "object" || Array.isArray(stamp)) return -1;
    const candidate = stamp as Record<string, unknown>;
    if (candidate.generation !== expectedGeneration || candidate.phase !== expectedPhase) return -1;
    const fence = this.studioReleaseFence(fenceKey);
    if (fence === null) return expectedPhase === "dark" ? 1 : 0;
    if (fence === "invalid") return 0;
    return fence.generation === expectedGeneration && fence.phase === expectedPhase ? 1 : -1;
  }

  /** -1 known mismatch, 0 unavailable, 1 allowed (unarmed dark baseline). */
  private studioReleasePersistenceStatus(
    record: Record<string, unknown>,
    fenceKey: string,
    expectedGeneration: number,
    expectedPhase: MemoryStudioReleaseFence["phase"],
  ): -1 | 0 | 1 {
    const stamp = record.releaseStamp;
    if (!stamp || typeof stamp !== "object" || Array.isArray(stamp)) return -1;
    const candidate = stamp as Record<string, unknown>;
    if (candidate.generation !== expectedGeneration || candidate.phase !== expectedPhase) return -1;
    const fence = this.studioReleaseFence(fenceKey);
    if (fence === "invalid") return 0;
    if (fence === null) return expectedPhase === "dark" ? 1 : 0;
    return fence.generation === expectedGeneration && fence.phase === expectedPhase ? 1 : -1;
  }

  private studioPendingControl(key: string): MemoryStudioPendingControl | "invalid" | null {
    const record = this.studioRecord(key);
    if (record === null) return null;
    const state = record.state;
    const receiptId = record.receiptId;
    const scopeSequence = record.scopeSequence;
    const revision = record.revision;
    const token = record.token;
    const expiresAtMs = record.expiresAtMs;
    if (
      record.schemaVersion !== 1
      || (state !== "active" && state !== "cleared" && state !== "maintenance" && state !== "expired")
      || typeof receiptId !== "string" || receiptId.length < 1
      || typeof scopeSequence !== "number" || !Number.isSafeInteger(scopeSequence) || scopeSequence < 1
      || typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1
    ) return "invalid";
    if (state === "active") {
      if (
        typeof token !== "string" || token.length < 1
        || typeof expiresAtMs !== "number" || !Number.isSafeInteger(expiresAtMs) || expiresAtMs < 1
      ) return "invalid";
      return { schemaVersion: 1, state, receiptId, scopeSequence, revision, token, expiresAtMs };
    }
    if (
      token !== undefined
      || (expiresAtMs !== undefined && (typeof expiresAtMs !== "number" || !Number.isSafeInteger(expiresAtMs) || expiresAtMs < 1))
    ) return "invalid";
    return {
      schemaVersion: 1,
      state,
      receiptId,
      scopeSequence,
      revision,
      ...(expiresAtMs === undefined ? {} : { expiresAtMs }),
    };
  }

  private studioSetPendingControl(key: string, control: MemoryStudioPendingControl): void {
    this.assertStringOrMissing(key);
    this.values.set(key, { kind: "string", value: JSON.stringify(control), expiresAt: null });
  }

  private studioExpirePendingControl(
    payloadKey: string,
    controlKey: string,
    control: MemoryStudioPendingControl,
  ): MemoryStudioPendingControl {
    const expired: MemoryStudioPendingControl = {
      schemaVersion: 1,
      state: "expired",
      receiptId: control.receiptId,
      scopeSequence: control.scopeSequence,
      revision: control.revision,
      ...(control.expiresAtMs === undefined ? {} : { expiresAtMs: control.expiresAtMs }),
    };
    this.values.delete(payloadKey);
    this.studioSetPendingControl(controlKey, expired);
    return expired;
  }

  private studioPendingOutcome(state: MemoryStudioPendingState): -1 | -2 | -3 | -4 {
    switch (state) {
      case "active": return -1;
      case "maintenance": return -2;
      case "cleared": return -3;
      case "expired": return -4;
    }
  }

  private studioPendingGuard(record: Record<string, unknown>): MemoryStudioPendingGuard | "invalid" | null {
    if (!("pendingGuard" in record)) return null;
    const value = record.pendingGuard;
    if (!value || typeof value !== "object" || Array.isArray(value)) return "invalid";
    const candidate = value as Record<string, unknown>;
    const threadHash = candidate.threadHash;
    const receiptId = candidate.receiptId;
    const scopeSequence = candidate.scopeSequence;
    const revision = candidate.revision;
    const token = candidate.token;
    const expiresAtMs = candidate.expiresAtMs;
    if (
      typeof threadHash !== "string" || threadHash.length < 1
      || typeof receiptId !== "string" || receiptId.length < 1
      || typeof scopeSequence !== "number" || !Number.isSafeInteger(scopeSequence) || scopeSequence < 1
      || typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1
      || typeof token !== "string" || token.length < 1
      || typeof expiresAtMs !== "number" || !Number.isSafeInteger(expiresAtMs) || expiresAtMs < 1
    ) return "invalid";
    return { threadHash, receiptId, scopeSequence, revision, token, expiresAtMs };
  }

  private studioPendingGuardIsLive(
    record: Record<string, unknown>,
    controlKey: string,
    payloadKey: string,
    scopeCounterKey: string,
    now: number,
  ): boolean {
    const guard = this.studioPendingGuard(record);
    if (guard === null) return true;
    if (guard === "invalid") return false;
    if (this.studioCounter(scopeCounterKey) !== guard.scopeSequence) return false;
    const control = this.studioPendingControl(controlKey);
    if (control === null || control === "invalid" || control.state !== "active") return false;
    if (control.expiresAtMs === undefined || control.expiresAtMs <= now || this.studioString(payloadKey) === null) {
      this.studioExpirePendingControl(payloadKey, controlKey, control);
      return false;
    }
    return control.receiptId === guard.receiptId
      && control.scopeSequence === guard.scopeSequence
      && control.revision === guard.revision
      && control.token === guard.token
      && control.expiresAtMs === guard.expiresAtMs;
  }

  private studioSet(key: string, value: string, ttlMs: number): void {
    if (!Number.isSafeInteger(ttlMs) || ttlMs < 1) throw new Error("invalid studio expiry");
    this.assertStringOrMissing(key);
    this.values.set(key, { kind: "string", value, expiresAt: this.now() + ttlMs });
  }

  private studioSetRecord(key: string, record: Record<string, unknown>, ttlSeconds: number): void {
    this.studioSet(key, JSON.stringify(record), ttlSeconds * 1_000);
  }

  private studioSetPersistent(key: string, value: string): void {
    this.assertStringOrMissing(key);
    this.values.set(key, { kind: "string", value, expiresAt: null });
  }

  private studioSetPersistentRecord(key: string, record: Record<string, unknown>): void {
    this.studioSetPersistent(key, JSON.stringify(record));
  }

  private studioStateVersion(record: Record<string, unknown>): number {
    const version = record.stateVersion;
    if (typeof version !== "number" || !Number.isSafeInteger(version) || version < 1) {
      throw new Error("invalid studio record version");
    }
    return version;
  }

  private studioAttempts(record: Record<string, unknown>): number {
    const attempts = record.attempts;
    if (typeof attempts !== "number" || !Number.isSafeInteger(attempts) || attempts < 0) {
      throw new Error("invalid studio outbox attempts");
    }
    return attempts;
  }

  private studioRemoveMember(key: string, member: string): void {
    const entry = this.getZSet(key, false);
    if (!entry) return;
    entry.value.delete(member);
    if (entry.value.size === 0) this.values.delete(key);
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
    const receivedAtIndexKey = keys[5];
    const readyThreadsIndexKey = keys[6];
    const metadataTemplate = args[0];
    const metadataTtlValue = args[1];
    const payloadTemplate = args[2];
    const payloadTtlValue = args[3];
    const pendingScoreValue = args[4];
    const recordKey = args[5];
    const sequenceTtlValue = args[6];
    const receivedAtScoreValue = args[7];
    const threadHash = args[8];
    const rateReservationEnabled = args[9] ?? "0";
    const minuteWindowValue = args[10] ?? "0";
    const dayWindowValue = args[11] ?? "0";
    const minuteLimitValue = args[12] ?? "0";
    const dayLimitValue = args[13] ?? "0";
    const decisionTtlValue = args[14] ?? "0";
    const rateMember = args[15] ?? "";
    if (
      !metadataKey ||
      !payloadKey ||
      !pendingKey ||
      !threadQueueKey ||
      !sequenceKey ||
      !receivedAtIndexKey ||
      !readyThreadsIndexKey ||
      metadataTemplate === undefined ||
      metadataTtlValue === undefined ||
      payloadTemplate === undefined ||
      payloadTtlValue === undefined ||
      pendingScoreValue === undefined ||
      !recordKey ||
      sequenceTtlValue === undefined ||
      receivedAtScoreValue === undefined ||
      !threadHash
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
    const receivedAtScore = Number(receivedAtScoreValue);
    if (
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      !Number.isSafeInteger(payloadTtlSeconds) ||
      payloadTtlSeconds < 1 ||
      !Number.isSafeInteger(sequenceTtlSeconds) ||
      sequenceTtlSeconds < 1 ||
      !Number.isSafeInteger(pendingScore) ||
      pendingScore < 0 ||
      !Number.isSafeInteger(receivedAtScore) ||
      receivedAtScore < 0
    ) {
      throw new Error("invalid inbox publish script arguments");
    }
    if (rateReservationEnabled !== "0" && rateReservationEnabled !== "1") {
      throw new Error("invalid inbox rate reservation arguments");
    }
    if (
      (rateReservationEnabled === "1" && keys.length !== 13) ||
      (rateReservationEnabled === "0" && keys.length !== 7)
    ) {
      throw new Error("invalid inbox rate reservation arguments");
    }
    const minuteWindow = Number(minuteWindowValue);
    const dayWindow = Number(dayWindowValue);
    const minuteLimit = Number(minuteLimitValue);
    const dayLimit = Number(dayLimitValue);
    const decisionTtl = Number(decisionTtlValue);
    if (rateReservationEnabled === "1") {
      const minuteKey = keys[7];
      const dayKey = keys[8];
      const minuteNoticeKey = keys[9];
      const dayNoticeKey = keys[10];
      const decisionKey = keys[11];
      const receiptWatermarkKey = keys[12];
      if (
        !minuteKey ||
        !dayKey ||
        !minuteNoticeKey ||
        !dayNoticeKey ||
        !decisionKey ||
        !receiptWatermarkKey ||
        !/^[a-f0-9]{64}$/.test(rateMember) ||
        ![minuteWindow, dayWindow, minuteLimit, dayLimit, decisionTtl].every(
          (value) => Number.isSafeInteger(value) && value >= 1,
        ) ||
        decisionTtl < dayWindow
      ) {
        throw new Error("invalid inbox rate reservation arguments");
      }
    }

    // Match Lua's no-partial-write preflight. In particular, do not reserve a
    // rate slot if a corrupt queue/index key would make publication fail.
    this.assertZSetOrMissing(pendingKey);
    this.assertZSetOrMissing(threadQueueKey);
    this.assertZSetOrMissing(receivedAtIndexKey);
    this.assertZSetOrMissing(readyThreadsIndexKey);
    const sequenceEntry = this.getEntry(sequenceKey);
    if (sequenceEntry && sequenceEntry.kind !== "string") {
      throw new Error("inbox thread sequence is invalid");
    }
    if (sequenceEntry && !/^(?:0|[1-9][0-9]*)$/.test(sequenceEntry.value)) {
      throw new Error("inbox thread sequence is invalid");
    }
    const priorSequence = sequenceEntry ? Number(sequenceEntry.value) : 0;
    if (!Number.isSafeInteger(priorSequence) || priorSequence < 0) {
      throw new Error("inbox thread sequence is invalid");
    }
    const sequence = priorSequence + 1;
    if (!Number.isSafeInteger(sequence)) throw new Error("inbox thread sequence is invalid");

    if (rateReservationEnabled === "1") {
      this.reserveRateDecision({
        minuteKey: keys[7] as string,
        dayKey: keys[8] as string,
        minuteNoticeKey: keys[9] as string,
        dayNoticeKey: keys[10] as string,
        decisionKey: keys[11] as string,
        receiptWatermarkKey: keys[12] as string,
        now: receivedAtScore,
        minuteWindow,
        dayWindow,
        minuteLimit,
        dayLimit,
        member: rateMember,
        decisionTtl,
      });
    }

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
    const threadQueue = this.getZSet(threadQueueKey, true);
    const threadWasEmpty = threadQueue.value.size === 0;
    threadQueue.value.set(recordKey, sequence);
    this.getZSet(receivedAtIndexKey, true).value.set(recordKey, receivedAtScore);
    if (threadWasEmpty) this.getZSet(readyThreadsIndexKey, true).value.set(threadHash, pendingScore);
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
    const receivedAtIndexKey = keys[4];
    const readyThreadsIndexKey = keys[5];
    const metadata = args[0];
    const metadataTtlValue = args[1];
    const payload = args[2];
    const payloadTtlValue = args[3];
    const payloadPresent = args[4];
    const indexMode = args[5];
    const pendingScoreValue = args[6];
    const recordKey = args[7];
    const sequenceValue = args[8];
    const receivedAtScoreValue = args[9];
    const threadHash = args[10];
    if (
      !metadataKey ||
      !payloadKey ||
      !pendingKey ||
      !threadQueueKey ||
      !receivedAtIndexKey ||
      !readyThreadsIndexKey ||
      metadata === undefined ||
      metadataTtlValue === undefined ||
      payload === undefined ||
      payloadTtlValue === undefined ||
      (payloadPresent !== "0" && payloadPresent !== "1") ||
      (indexMode !== "queued" && indexMode !== "terminal") ||
      pendingScoreValue === undefined ||
      !recordKey ||
      sequenceValue === undefined ||
      receivedAtScoreValue === undefined ||
      !threadHash
    ) {
      throw new Error("invalid inbox write script arguments");
    }
    const metadataTtlSeconds = Number(metadataTtlValue);
    const payloadTtlSeconds = Number(payloadTtlValue);
    const pendingScore = Number(pendingScoreValue);
    const threadSequence = Number(sequenceValue);
    const receivedAtScore = Number(receivedAtScoreValue);
    if (
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      (payloadPresent === "1" && (!Number.isSafeInteger(payloadTtlSeconds) || payloadTtlSeconds < 1)) ||
      !Number.isFinite(pendingScore) ||
      !Number.isSafeInteger(threadSequence) ||
      threadSequence < 1 ||
      !Number.isSafeInteger(receivedAtScore) ||
      receivedAtScore < 0
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
    const receivedAtIndex = this.getZSet(receivedAtIndexKey, false);
    if (indexMode === "queued") {
      this.getZSet(pendingKey, true).value.set(recordKey, pendingScore);
      this.getZSet(threadQueueKey, true).value.set(recordKey, threadSequence);
      this.getZSet(receivedAtIndexKey, true).value.set(recordKey, receivedAtScore);
    } else {
      if (pending?.value.delete(recordKey) && pending.value.size === 0) this.values.delete(pendingKey);
      if (threadQueue?.value.delete(recordKey) && threadQueue.value.size === 0) {
        this.values.delete(threadQueueKey);
      }
      if (receivedAtIndex?.value.delete(recordKey) && receivedAtIndex.value.size === 0) {
        this.values.delete(receivedAtIndexKey);
      }
    }
    this.reconcileInboxReadyThread(
      pendingKey,
      threadQueueKey,
      readyThreadsIndexKey,
      threadHash,
    );
    return 1;
  }

  private reconcileInboxReadyThread(
    pendingKey: string,
    threadQueueKey: string,
    readyThreadsIndexKey: string,
    threadHash: string,
  ): void {
    const readyThreads = this.getZSet(readyThreadsIndexKey, false);
    const threadQueue = this.getZSet(threadQueueKey, false);
    if (!threadQueue || threadQueue.value.size === 0) {
      if (readyThreads?.value.delete(threadHash) && readyThreads.value.size === 0) {
        this.values.delete(readyThreadsIndexKey);
      }
      return;
    }
    const [headRecordKey] = [...threadQueue.value.entries()]
      .sort(([firstValue, firstScore], [secondValue, secondScore]) =>
        firstScore === secondScore ? firstValue.localeCompare(secondValue) : firstScore - secondScore,
      )[0] ?? [];
    if (!headRecordKey) return;

    const metadataEntry = this.getEntry(`${headRecordKey}:metadata`);
    if (metadataEntry?.kind !== "string") {
      if (readyThreads?.value.delete(threadHash) && readyThreads.value.size === 0) {
        this.values.delete(readyThreadsIndexKey);
      }
      return;
    }
    let metadata: {
      state?: unknown;
      status?: { processingLeaseExpiresAtMs?: unknown } | undefined;
    };
    try {
      metadata = JSON.parse(metadataEntry.value) as {
        state?: unknown;
        status?: { processingLeaseExpiresAtMs?: unknown } | undefined;
      };
    } catch {
      if (readyThreads?.value.delete(threadHash) && readyThreads.value.size === 0) {
        this.values.delete(readyThreadsIndexKey);
      }
      return;
    }

    let score: number | undefined;
    if (metadata.state === "received" || metadata.state === "failed_retryable") {
      score = this.getZSet(pendingKey, false)?.value.get(headRecordKey);
    } else if (
      typeof metadata.status?.processingLeaseExpiresAtMs === "number" &&
      Number.isFinite(metadata.status.processingLeaseExpiresAtMs) &&
      metadata.status.processingLeaseExpiresAtMs >= 0
    ) {
      score = metadata.status.processingLeaseExpiresAtMs;
    }
    if (score === undefined) {
      if (readyThreads?.value.delete(threadHash) && readyThreads.value.size === 0) {
        this.values.delete(readyThreadsIndexKey);
      }
      return;
    }
    this.getZSet(readyThreadsIndexKey, true).value.set(threadHash, score);
  }

  private evalRateLimit(
    keys: readonly string[],
    args: readonly string[],
  ): readonly [number, string, number, number, number, number, number] {
    const minuteKey = keys[0];
    const dayKey = keys[1];
    const minuteNoticeKey = keys[2];
    const dayNoticeKey = keys[3];
    const decisionKey = keys[4];
    const receiptWatermarkKey = keys[5];
    const nowValue = args[0];
    const minuteWindowValue = args[1];
    const dayWindowValue = args[2];
    const minuteLimitValue = args[3];
    const dayLimitValue = args[4];
    const member = args[5];
    const decisionTtlValue = args[6];
    if (
      keys.length !== 6 ||
      args.length !== 7 ||
      !minuteKey ||
      !dayKey ||
      !minuteNoticeKey ||
      !dayNoticeKey ||
      !decisionKey ||
      !receiptWatermarkKey ||
      !nowValue ||
      !minuteWindowValue ||
      !dayWindowValue ||
      !minuteLimitValue ||
      !dayLimitValue ||
      !member ||
      !decisionTtlValue
    ) {
      throw new Error("invalid rate-limit script arguments");
    }
    const now = Number(nowValue);
    const minuteWindow = Number(minuteWindowValue);
    const dayWindow = Number(dayWindowValue);
    const minuteLimit = Number(minuteLimitValue);
    const dayLimit = Number(dayLimitValue);
    const decisionTtl = Number(decisionTtlValue);
    if (
      ![now, minuteWindow, dayWindow, minuteLimit, dayLimit, decisionTtl].every(Number.isFinite) ||
      !Number.isSafeInteger(now) ||
      now < 0 ||
      !/^[a-f0-9]{64}$/.test(member) ||
      ![minuteWindow, dayWindow, minuteLimit, dayLimit, decisionTtl].every(
        (value) => Number.isSafeInteger(value) && value >= 1,
      ) ||
      decisionTtl < dayWindow
    ) {
      throw new Error("invalid rate-limit script arguments");
    }

    const result = this.reserveRateDecision({
      minuteKey,
      dayKey,
      minuteNoticeKey,
      dayNoticeKey,
      decisionKey,
      receiptWatermarkKey,
      now,
      minuteWindow,
      dayWindow,
      minuteLimit,
      dayLimit,
      member,
      decisionTtl,
    });
    return [
      result.accepted ? 1 : 0,
      result.reason ?? "",
      result.minuteCount,
      result.dayCount,
      result.retryAfterSeconds,
      result.shouldNotify ? 1 : 0,
      result.duplicate ? 1 : 0,
    ];
  }

  private reserveRateDecision(input: {
    readonly minuteKey: string;
    readonly dayKey: string;
    readonly minuteNoticeKey: string;
    readonly dayNoticeKey: string;
    readonly decisionKey: string;
    readonly receiptWatermarkKey: string;
    readonly now: number;
    readonly minuteWindow: number;
    readonly dayWindow: number;
    readonly minuteLimit: number;
    readonly dayLimit: number;
    readonly member: string;
    readonly decisionTtl: number;
  }): {
    readonly accepted: boolean;
    readonly reason: "minute" | "day" | null;
    readonly minuteCount: number;
    readonly dayCount: number;
    readonly retryAfterSeconds: number;
    readonly shouldNotify: boolean;
    readonly duplicate: boolean;
  } {
    // Keep direct rate calls and the combined inbox Lua path equally
    // fail-closed: detect every wrong Redis type before touching a counter.
    this.assertZSetOrMissing(input.minuteKey);
    this.assertZSetOrMissing(input.dayKey);
    this.assertStringOrMissing(input.minuteNoticeKey);
    this.assertStringOrMissing(input.dayNoticeKey);
    this.assertStringOrMissing(input.decisionKey);
    this.assertStringOrMissing(input.receiptWatermarkKey);
    const existing = this.getEntry(input.decisionKey);
    if (existing) {
      if (existing.kind !== "string") throw new Error("invalid rate reservation decision");
      const parsed = parseMemoryRateDecision(existing.value, input.now);
      return {
        ...parsed,
        minuteCount: this.getZSet(input.minuteKey, false)?.value.size ?? 0,
        dayCount: this.getZSet(input.dayKey, false)?.value.size ?? 0,
        duplicate: true,
      };
    }

    const watermark = this.getEntry(input.receiptWatermarkKey);
    if (watermark && watermark.kind !== "string") throw new Error("invalid rate receipt watermark");
    const watermarkMs = watermark ? Number(watermark.value) : 0;
    if (watermark && (!Number.isSafeInteger(watermarkMs) || watermarkMs < 0)) {
      throw new Error("invalid rate receipt watermark");
    }
    const effectiveNow = Math.max(input.now, watermarkMs);
    const minute = this.getZSet(input.minuteKey, true);
    const day = this.getZSet(input.dayKey, true);

    this.removeBefore(minute, effectiveNow - input.minuteWindow);
    this.removeBefore(day, effectiveNow - input.dayWindow);
    const minuteCount = minute.value.size;
    const dayCount = day.value.size;
    const limitedByMinute = minuteCount >= input.minuteLimit;
    const limitedByDay = !limitedByMinute && dayCount >= input.dayLimit;
    const now = this.now();

    const persistWatermark = () => {
      this.values.set(input.receiptWatermarkKey, {
        kind: "string",
        value: String(effectiveNow),
        expiresAt: now + input.dayWindow,
      });
    };

    if (minute.value.has(input.member) || day.value.has(input.member)) {
      this.values.set(input.decisionKey, {
        kind: "string",
        value: "a",
        expiresAt: now + input.decisionTtl,
      });
      persistWatermark();
      return {
        accepted: true,
        reason: null,
        minuteCount,
        dayCount,
        retryAfterSeconds: 0,
        shouldNotify: false,
        duplicate: true,
      };
    }

    if (limitedByMinute || limitedByDay) {
      const reason = limitedByMinute ? "minute" : "day";
      const set = limitedByMinute ? minute : day;
      const window = limitedByMinute ? input.minuteWindow : input.dayWindow;
      const noticeKey = limitedByMinute ? input.minuteNoticeKey : input.dayNoticeKey;
      const oldest = Math.min(...set.value.values());
      if (!Number.isFinite(oldest)) throw new Error("rate-limit set has no entries");
      const retryUntilMs = oldest + window;
      const retryAfterSeconds = Math.max(1, Math.ceil((retryUntilMs - effectiveNow) / 1_000));
      const shouldNotify = !this.getEntry(noticeKey);
      if (shouldNotify) {
        this.values.set(noticeKey, {
          kind: "string",
          value: "1",
          expiresAt: now + Math.max(1, retryUntilMs - effectiveNow),
        });
      }
      this.values.set(input.decisionKey, {
        kind: "string",
        value: `${limitedByMinute ? "m" : "d"}:${shouldNotify ? "1" : "0"}:${retryUntilMs}`,
        expiresAt: now + input.decisionTtl,
      });
      persistWatermark();
      return {
        accepted: false,
        reason,
        minuteCount,
        dayCount,
        retryAfterSeconds,
        shouldNotify,
        duplicate: false,
      };
    }

    minute.value.set(input.member, effectiveNow);
    day.value.set(input.member, effectiveNow);
    minute.expiresAt = now + input.minuteWindow;
    day.expiresAt = now + input.dayWindow;
    this.values.set(input.decisionKey, {
      kind: "string",
      value: "a",
      expiresAt: now + input.decisionTtl,
    });
    persistWatermark();
    return {
      accepted: true,
      reason: null,
      minuteCount: minuteCount + 1,
      dayCount: dayCount + 1,
      retryAfterSeconds: 0,
      shouldNotify: false,
      duplicate: false,
    };
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

  private assertZSetOrMissing(key: string): void {
    const entry = this.getEntry(key);
    if (entry && entry.kind !== "zset") throw new Error("Redis key has the wrong type");
  }

  private assertStringOrMissing(key: string): void {
    const entry = this.getEntry(key);
    if (entry && entry.kind !== "string") throw new Error("Redis key has the wrong type");
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

/** Keep the deterministic emulator aligned with the Lua scripts' decimal-only inputs. */
function isCanonicalPositiveInteger(value: string): boolean {
  return /^[1-9][0-9]*$/.test(value) && value.length <= 16;
}

function isCanonicalNonnegativeInteger(value: string): boolean {
  return value === "0" || isCanonicalPositiveInteger(value);
}

function parseMemoryRateDecision(
  value: string,
  now: number,
): {
  readonly accepted: boolean;
  readonly reason: "minute" | "day" | null;
  readonly retryAfterSeconds: number;
  readonly shouldNotify: boolean;
} {
  if (value === "a") {
    return {
      accepted: true,
      reason: null,
      retryAfterSeconds: 0,
      shouldNotify: false,
    };
  }
  const match = /^([md]):([01]):(\d+)$/.exec(value);
  if (!match) throw new Error("invalid rate reservation decision");
  const retryUntilMs = Number(match[3]);
  if (!Number.isSafeInteger(retryUntilMs) || retryUntilMs < 0) {
    throw new Error("invalid rate reservation decision");
  }
  return {
    accepted: false,
    reason: match[1] === "m" ? "minute" : "day",
    retryAfterSeconds: Math.max(0, Math.ceil((retryUntilMs - now) / 1_000)),
    shouldNotify: match[2] === "1",
  };
}
