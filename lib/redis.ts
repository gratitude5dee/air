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

/** The subset of `@upstash/redis` used by the Workers adapter below. */
export interface UpstashRedisCompatibleClient {
  get(key: string): Promise<unknown>;
  set(
    key: string,
    value: string,
    options?: { nx?: boolean; xx?: boolean; ex?: number; px?: number },
  ): Promise<unknown>;
  del(...keys: string[]): Promise<number>;
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<number | boolean>;
  pttl(key: string): Promise<number>;
  zadd(key: string, entries: readonly { score: number; member: string }[]): Promise<number | null>;
  zrangebyscore(
    key: string,
    min: number,
    max: number,
    options?: { offset?: number; count?: number },
  ): Promise<string[]>;
  zcard(key: string): Promise<number>;
  zrem(key: string, ...members: string[]): Promise<number>;
  zremrangebyscore(key: string, min: number, max: number): Promise<number>;
  eval(
    script: string,
    keys: readonly string[],
    args: readonly string[],
  ): Promise<unknown>;
}

/**
 * Upstash REST adapter for Cloudflare Workers, where node-redis' TCP socket
 * cannot connect. `RedisLike` is unchanged — the same server-side Lua `eval`
 * path runs over Upstash's HTTP API.
 */
export function createUpstashRedisAdapter(client: UpstashRedisCompatibleClient): RedisLike {
  const adapter: RedisLike = {
    get: async (key) => {
      const value = await client.get(key);
      return value === null || value === undefined ? null : String(value);
    },
    async set(key, value, options = {}) {
      const upstashOptions: { nx?: boolean; xx?: boolean; ex?: number; px?: number } = {};
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
    del: (...keys) => client.del(...keys),
    incr: (key) => client.incr(key),
    async expire(key, seconds) {
      const result = await client.expire(key, seconds);
      return result === true || result === 1;
    },
    pttl: (key) => client.pttl(key),
    async zAdd(key, entries) {
      if (entries.length === 0) return 0;
      const added = await client.zadd(
        key,
        entries.map((entry) => ({ score: entry.score, member: entry.value })),
      );
      return added ?? 0;
    },
    zRangeByScore: (key, min, max, options) =>
      client.zrangebyscore(
        key,
        min,
        max,
        options?.limit === undefined
          ? undefined
          : { offset: options.offset ?? 0, count: options.limit },
      ),
    zCard: (key) => client.zcard(key),
    zRem: (key, ...members) => client.zrem(key, ...members),
    zRemRangeByScore: (key, min, max) => client.zremrangebyscore(key, min, max),
    eval: (script, options) => client.eval(script, [...options.keys], [...options.arguments]),
  };
  return Object.freeze(adapter);
}

export interface UpstashRedisConnectionOptions {
  readonly url: string;
  readonly token: string;
  /** Test-only factory so the REST adapter is independently verifiable. */
  readonly clientFactory?:
    | ((options: { url: string; token: string }) => UpstashRedisCompatibleClient)
    | undefined;
}

/**
 * Cloudflare Workers path: Upstash REST has no socket to connect or destroy,
 * so `ConnectedRedis` lifecycle methods are no-ops. Every command is wrapped
 * so a failed REST call still fires `onOperationalError` and invalidates the
 * cached runtime the same way a dropped TCP connection does.
 */
export async function connectUpstashRedis(
  options: UpstashRedisConnectionOptions,
): Promise<ConnectedRedis> {
  const client = options.clientFactory
    ? options.clientFactory({ url: options.url, token: options.token })
    : (new ((await import("@upstash/redis")).Redis)({
        url: options.url,
        token: options.token,
      }) as unknown as UpstashRedisCompatibleClient);
  const operationalErrorListeners = new Set<() => void>();
  const notify = () => {
    for (const listener of operationalErrorListeners) {
      try {
        listener();
      } catch {
        // Error reporting must never rethrow through a command failure.
      }
    }
  };
  const adapter = createUpstashRedisAdapter(client);
  const guarded = {} as Record<keyof RedisLike, (...args: unknown[]) => Promise<unknown>>;
  for (const name of Object.keys(adapter) as (keyof RedisLike)[]) {
    const fn = adapter[name] as (...args: unknown[]) => Promise<unknown>;
    guarded[name] = async (...args: unknown[]) => {
      try {
        return await fn(...args);
      } catch (error) {
        notify();
        throw error;
      }
    };
  }
  return Object.freeze({
    redis: guarded as RedisLike,
    onOperationalError(listener: () => void): () => void {
      operationalErrorListeners.add(listener);
      return () => operationalErrorListeners.delete(listener);
    },
    destroy(): void {},
    async close(): Promise<void> {},
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
    if (script.includes("AIR_PERSIST_VIDEO_JOB_V1")) {
      return this.evalPersistVideoJob(options.keys, options.arguments);
    }
    if (script.includes("AIR_WRITE_VIDEO_JOB_V1")) {
      return this.evalWriteVideoJob(options.keys, options.arguments);
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

  /** Mirrors the narrow atomic create primitive used by Air's Zap video jobs. */
  private evalPersistVideoJob(
    keys: readonly string[],
    args: readonly string[],
  ): number {
    const metadataKey = keys[0];
    const payloadKey = keys[1];
    const dueKey = keys[2];
    const activeSenderKey = keys[3];
    const metadata = args[0];
    const metadataTtlValue = args[1];
    const payload = args[2];
    const payloadTtlValue = args[3];
    const dueScoreValue = args[4];
    const recordKey = args[5];
    const activeTtlValue = args[6];
    if (
      keys.length !== 4 ||
      args.length !== 7 ||
      !metadataKey ||
      !payloadKey ||
      !dueKey ||
      !activeSenderKey ||
      !metadata ||
      !payload ||
      metadataTtlValue === undefined ||
      payloadTtlValue === undefined ||
      dueScoreValue === undefined ||
      !recordKey ||
      activeTtlValue === undefined
    ) {
      throw new Error("invalid video job publish script arguments");
    }
    this.assertStringOrMissing(metadataKey);
    this.assertStringOrMissing(payloadKey);
    this.assertZSetOrMissing(dueKey);
    this.assertStringOrMissing(activeSenderKey);
    if (this.getEntry(metadataKey)) return 0;
    if (this.getEntry(payloadKey)) return -1;
    if (this.getEntry(activeSenderKey)) return -2;
    const metadataTtlSeconds = Number(metadataTtlValue);
    const payloadTtlSeconds = Number(payloadTtlValue);
    const dueScore = Number(dueScoreValue);
    const activeTtlSeconds = Number(activeTtlValue);
    if (
      !isCanonicalPositiveInteger(metadataTtlValue) ||
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      !isCanonicalPositiveInteger(payloadTtlValue) ||
      !Number.isSafeInteger(payloadTtlSeconds) ||
      payloadTtlSeconds < 1 ||
      !isCanonicalNonnegativeInteger(dueScoreValue) ||
      !Number.isSafeInteger(dueScore) ||
      dueScore < 0 ||
      !isCanonicalPositiveInteger(activeTtlValue) ||
      !Number.isSafeInteger(activeTtlSeconds) ||
      activeTtlSeconds < 1
    ) {
      throw new Error("invalid video job publish script arguments");
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
    this.getZSet(dueKey, true).value.set(recordKey, dueScore);
    this.values.set(activeSenderKey, {
      kind: "string",
      value: recordKey,
      expiresAt: now + activeTtlSeconds * 1_000,
    });
    return 1;
  }

  /** Mirrors the atomic metadata/payload/index replacement for a video job state transition. */
  private evalWriteVideoJob(
    keys: readonly string[],
    args: readonly string[],
  ): number {
    const metadataKey = keys[0];
    const payloadKey = keys[1];
    const dueKey = keys[2];
    const leaseKey = keys[3];
    const activeSenderKey = keys[4];
    const manualReviewKey = keys[5];
    const metadata = args[0];
    const metadataTtlValue = args[1];
    const payload = args[2];
    const payloadTtlValue = args[3];
    const payloadPresent = args[4];
    const indexMode = args[5];
    const dueScoreValue = args[6];
    const recordKey = args[7];
    const expectedStateVersionValue = args[8];
    const leaseToken = args[9];
    const activeMode = args[10];
    const activeTtlValue = args[11];
    const manualReviewMode = args[12];
    if (
      keys.length !== 6 ||
      args.length !== 13 ||
      !metadataKey ||
      !payloadKey ||
      !dueKey ||
      !leaseKey ||
      !activeSenderKey ||
      !manualReviewKey ||
      !metadata ||
      metadataTtlValue === undefined ||
      payload === undefined ||
      payloadTtlValue === undefined ||
      (payloadPresent !== "0" && payloadPresent !== "1") ||
      (indexMode !== "due" && indexMode !== "terminal") ||
      dueScoreValue === undefined ||
      !recordKey ||
      expectedStateVersionValue === undefined ||
      !leaseToken ||
      (activeMode !== "retain" && activeMode !== "release") ||
      activeTtlValue === undefined ||
      (manualReviewMode !== "manual" && manualReviewMode !== "remove")
    ) {
      throw new Error("invalid video job write script arguments");
    }
    if (
      !isCanonicalPositiveInteger(expectedStateVersionValue) ||
      !/^[A-Za-z0-9_-]{32,128}$/.test(leaseToken) ||
      !isCanonicalPositiveInteger(activeTtlValue)
    ) {
      throw new Error("invalid video job write fence");
    }
    const leaseEntry = this.getEntry(leaseKey);
    if (leaseEntry && leaseEntry.kind !== "string") throw new Error("Redis key has the wrong type");
    if (!leaseEntry || leaseEntry.value !== leaseToken) return -2;
    const metadataEntry = this.getEntry(metadataKey);
    if (metadataEntry?.kind !== "string") return 0;
    let storedStateVersion: unknown;
    try {
      storedStateVersion = (JSON.parse(metadataEntry.value) as { stateVersion?: unknown }).stateVersion;
    } catch {
      throw new Error("invalid stored video metadata");
    }
    if (storedStateVersion !== Number(expectedStateVersionValue)) return -3;
    this.assertStringOrMissing(payloadKey);
    this.assertZSetOrMissing(dueKey);
    this.assertStringOrMissing(activeSenderKey);
    this.assertZSetOrMissing(manualReviewKey);
    const metadataTtlSeconds = Number(metadataTtlValue);
    const payloadTtlSeconds = Number(payloadTtlValue);
    const dueScore = Number(dueScoreValue);
    const activeTtlSeconds = Number(activeTtlValue);
    if (
      !isCanonicalPositiveInteger(metadataTtlValue) ||
      !Number.isSafeInteger(metadataTtlSeconds) ||
      metadataTtlSeconds < 1 ||
      (payloadPresent === "1" && (
        !isCanonicalPositiveInteger(payloadTtlValue) ||
        !Number.isSafeInteger(payloadTtlSeconds) ||
        payloadTtlSeconds < 1
      )) ||
      !isCanonicalNonnegativeInteger(dueScoreValue) ||
      !Number.isSafeInteger(dueScore) ||
      dueScore < 0 ||
      !Number.isSafeInteger(activeTtlSeconds) ||
      activeTtlSeconds < 1
    ) {
      throw new Error("invalid video job write script arguments");
    }
    const activeEntry = this.getEntry(activeSenderKey);
    if (activeMode === "retain" && activeEntry && activeEntry.value !== recordKey) return -4;
    const now = this.now();
    this.values.set(metadataKey, {
      kind: "string",
      value: metadata,
      expiresAt: now + metadataTtlSeconds * 1_000,
    });
    if (payloadPresent === "1") {
      this.values.set(payloadKey, {
        kind: "string",
        value: payload,
        expiresAt: now + payloadTtlSeconds * 1_000,
      });
    } else {
      this.values.delete(payloadKey);
    }
    const due = this.getZSet(dueKey, false);
    if (indexMode === "due") {
      this.getZSet(dueKey, true).value.set(recordKey, dueScore);
    } else if (due?.value.delete(recordKey) && due.value.size === 0) {
      this.values.delete(dueKey);
    }
    const manualReview = this.getZSet(manualReviewKey, false);
    if (manualReviewMode === "manual") {
      this.getZSet(manualReviewKey, true).value.set(recordKey, dueScore);
    } else if (manualReview?.value.delete(recordKey) && manualReview.value.size === 0) {
      this.values.delete(manualReviewKey);
    }
    if (activeMode === "release") {
      if (activeEntry?.value === recordKey) this.values.delete(activeSenderKey);
    } else {
      this.values.set(activeSenderKey, {
        kind: "string",
        value: recordKey,
        expiresAt: now + activeTtlSeconds * 1_000,
      });
    }
    return 1;
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
