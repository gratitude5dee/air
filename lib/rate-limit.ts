import { assertPseudonymousHash } from "./redis.js";
import type { RedisLike } from "./redis.js";
import type { SenderHash } from "./security.js";

export const DEFAULT_RATE_LIMIT_POLICY = Object.freeze({
  perMinute: 3,
  perRollingDay: 20,
  minuteWindowMs: 60_000,
  dayWindowMs: 86_400_000,
});

export interface RateLimitPolicy {
  readonly perMinute: number;
  readonly perRollingDay: number;
  readonly minuteWindowMs: number;
  readonly dayWindowMs: number;
}

export interface RateLimitResult {
  readonly accepted: boolean;
  readonly duplicate: boolean;
  readonly reason: "minute" | "day" | null;
  readonly minuteCount: number;
  readonly dayCount: number;
  readonly retryAfterSeconds: number;
  /** Send the user-facing notice only when this is true. */
  readonly shouldNotify: boolean;
}

/**
 * Atomic rolling-window limiter. `member` is a correlation HMAC, never raw
 * text or a provider handle. The Lua script deliberately combines counter and
 * one-notice-per-window decisions to avoid concurrent webhook races.
 */
export const RATE_LIMIT_LUA = `
-- AIR_RATE_LIMIT_V1
local now = tonumber(ARGV[1])
local minuteWindow = tonumber(ARGV[2])
local dayWindow = tonumber(ARGV[3])
local minuteLimit = tonumber(ARGV[4])
local dayLimit = tonumber(ARGV[5])
local member = ARGV[6]

redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now - minuteWindow)
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now - dayWindow)

if redis.call("ZSCORE", KEYS[1], member) or redis.call("ZSCORE", KEYS[2], member) then
  return {1, "", redis.call("ZCARD", KEYS[1]), redis.call("ZCARD", KEYS[2]), 0, 0, 1}
end

local minuteCount = redis.call("ZCARD", KEYS[1])
local dayCount = redis.call("ZCARD", KEYS[2])
local limitedByMinute = minuteCount >= minuteLimit
local limitedByDay = (not limitedByMinute) and dayCount >= dayLimit

if limitedByMinute or limitedByDay then
  local targetKey = limitedByMinute and KEYS[1] or KEYS[2]
  local noticeKey = limitedByMinute and KEYS[3] or KEYS[4]
  local window = limitedByMinute and minuteWindow or dayWindow
  local first = redis.call("ZRANGE", targetKey, 0, 0, "WITHSCORES")
  local retryAfter = math.max(1, math.ceil((tonumber(first[2]) + window - now) / 1000))
  local shouldNotify = redis.call("SET", noticeKey, "1", "EX", retryAfter, "NX") and 1 or 0
  return {0, limitedByMinute and "minute" or "day", minuteCount, dayCount, retryAfter, shouldNotify, 0}
end

redis.call("ZADD", KEYS[1], now, member)
redis.call("ZADD", KEYS[2], now, member)
redis.call("PEXPIRE", KEYS[1], minuteWindow)
redis.call("PEXPIRE", KEYS[2], dayWindow)
return {1, "", minuteCount + 1, dayCount + 1, 0, 0, 0}
`;

export class RateLimiter {
  private readonly policy: RateLimitPolicy;
  private readonly prefix: string;
  private readonly now: () => number;

  public constructor(
    private readonly redis: RedisLike,
    options: {
      policy?: Partial<RateLimitPolicy>;
      prefix?: string;
      now?: () => number;
    } = {},
  ) {
    this.policy = Object.freeze({ ...DEFAULT_RATE_LIMIT_POLICY, ...options.policy });
    validatePolicy(this.policy);
    this.prefix = options.prefix ?? "air:rate:v1";
    this.now = options.now ?? Date.now;
  }

  /** Consumes one accepted-question slot, atomically across serverless instances. */
  async consume(senderHash: SenderHash | string, correlationId: string): Promise<RateLimitResult> {
    assertPseudonymousHash(senderHash, "sender hash");
    assertPseudonymousHash(correlationId, "correlation id");
    const now = this.now();
    const raw = await this.redis.eval(RATE_LIMIT_LUA, {
      keys: [
        `${this.prefix}:${senderHash}:minute`,
        `${this.prefix}:${senderHash}:day`,
        `${this.prefix}:${senderHash}:notice:minute`,
        `${this.prefix}:${senderHash}:notice:day`,
      ],
      arguments: [
        String(now),
        String(this.policy.minuteWindowMs),
        String(this.policy.dayWindowMs),
        String(this.policy.perMinute),
        String(this.policy.perRollingDay),
        correlationId,
      ],
    });
    return parseRateLimitReply(raw);
  }
}

function validatePolicy(policy: RateLimitPolicy): void {
  for (const value of [
    policy.perMinute,
    policy.perRollingDay,
    policy.minuteWindowMs,
    policy.dayWindowMs,
  ]) {
    if (!Number.isInteger(value) || value < 1) {
      throw new Error("rate-limit policy values must be positive integers");
    }
  }
}

function parseRateLimitReply(raw: unknown): RateLimitResult {
  if (!Array.isArray(raw) || raw.length < 7) {
    throw new Error("Redis rate-limit script returned an invalid response");
  }
  const accepted = Number(raw[0]) === 1;
  const reason = raw[1] === "minute" || raw[1] === "day" ? raw[1] : null;
  const minuteCount = Number(raw[2]);
  const dayCount = Number(raw[3]);
  const retryAfterSeconds = Number(raw[4]);
  const shouldNotify = Number(raw[5]) === 1;
  const duplicate = Number(raw[6]) === 1;
  if (
    !Number.isInteger(minuteCount) ||
    !Number.isInteger(dayCount) ||
    !Number.isInteger(retryAfterSeconds) ||
    (accepted && reason !== null) ||
    (!accepted && reason === null)
  ) {
    throw new Error("Redis rate-limit script returned malformed counters");
  }
  return Object.freeze({
    accepted,
    duplicate,
    reason,
    minuteCount,
    dayCount,
    retryAfterSeconds,
    shouldNotify,
  });
}
