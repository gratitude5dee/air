import { assertPseudonymousHash } from "./redis.js";
import type { RedisLike } from "./redis.js";
import type { SenderHash } from "./security.js";

export const DEFAULT_RATE_LIMIT_POLICY = Object.freeze({
  perMinute: 3,
  perRollingDay: 20,
  minuteWindowMs: 60_000,
  dayWindowMs: 86_400_000,
});

/** Shared with DurableInbox so it can reject a forged reservation key plan. */
export const DEFAULT_RATE_LIMIT_PREFIX = "air:rate:v1";

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
 * A durable decision made while a verified inbox record is first published.
 * It deliberately excludes counters: a recovered worker needs only the
 * immutable allow/deny outcome, not a new opportunity to consume quota.
 */
export interface RateLimitReservationDecision {
  readonly accepted: boolean;
  readonly reason: "minute" | "day" | null;
  /** Whether this exact receipt owns the one visible notice for its window. */
  readonly shouldNotify: boolean;
  /** Informational only; a recovered receipt can legitimately report zero. */
  readonly retryAfterSeconds: number;
}

/**
 * Safe, prevalidated Redis inputs that DurableInbox can include in its first
 * publication EVAL.  Every key component is a HMAC pseudonym; this object
 * contains no sender handle, message text, or provider identifier.
 */
export interface RateLimitIngressReservation {
  readonly correlationId: string;
  readonly minuteKey: string;
  readonly dayKey: string;
  readonly minuteNoticeKey: string;
  readonly dayNoticeKey: string;
  readonly decisionKey: string;
  /** Monotonic receipt watermark; prevents reordered EVALs from opening a fourth slot. */
  readonly receiptWatermarkKey: string;
  readonly minuteWindowMs: number;
  readonly dayWindowMs: number;
  readonly perMinute: number;
  readonly perRollingDay: number;
  /** Keep an idempotent receipt decision through the 24-hour payload window. */
  readonly decisionTtlMs: number;
}

/**
 * Atomic rolling-window limiter. `member` is a correlation HMAC, never raw
 * text or a provider handle. The Lua script deliberately combines counter and
 * one-notice-per-window decisions to avoid concurrent webhook races.
 */
export const RATE_LIMIT_LUA = `
-- AIR_RATE_LIMIT_V1
if #KEYS ~= 6 then
  return redis.error_reply("invalid rate-limit key count")
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

-- Preflight every command that could otherwise fail after the first counter
-- mutation. Redis Lua does not roll back earlier commands on an error.
if not isTypeOrMissing(KEYS[1], "zset")
  or not isTypeOrMissing(KEYS[2], "zset")
  or not isTypeOrMissing(KEYS[3], "string")
  or not isTypeOrMissing(KEYS[4], "string")
  or not isTypeOrMissing(KEYS[5], "string")
  or not isTypeOrMissing(KEYS[6], "string") then
  return redis.error_reply("invalid rate-limit key type")
end

local now = tonumber(ARGV[1])
local minuteWindow = tonumber(ARGV[2])
local dayWindow = tonumber(ARGV[3])
local minuteLimit = tonumber(ARGV[4])
local dayLimit = tonumber(ARGV[5])
local member = ARGV[6]
local decisionTtl = tonumber(ARGV[7])
if not nonnegativeInteger(ARGV[1])
  or not positiveInteger(ARGV[2])
  or not positiveInteger(ARGV[3])
  or not positiveInteger(ARGV[4])
  or not positiveInteger(ARGV[5])
  or not positiveInteger(ARGV[7])
  or decisionTtl < dayWindow
  or not member
  or not string.match(member, "^[a-f0-9][a-f0-9]*$")
  or string.len(member) ~= 64 then
  return redis.error_reply("invalid rate-limit arguments")
end

local existingDecision = redis.call("GET", KEYS[5])
if existingDecision then
  local minuteCount = redis.call("ZCARD", KEYS[1])
  local dayCount = redis.call("ZCARD", KEYS[2])
  if existingDecision == "a" then
    return {1, "", minuteCount, dayCount, 0, 0, 1}
  end
  local reason, shouldNotify, retryUntil = string.match(existingDecision, "^([md]):([01]):(%d+)$")
  if not reason or not shouldNotify or not retryUntil then
    return redis.error_reply("invalid rate reservation decision")
  end
  local retryAfter = math.max(0, math.ceil((tonumber(retryUntil) - now) / 1000))
  return {0, reason == "m" and "minute" or "day", minuteCount, dayCount, retryAfter, tonumber(shouldNotify), 1}
end

-- Incoming requests can start in one order and reach Redis in another. Pin
-- each sender to the latest committed receipt time, which is conservative:
-- a delayed older EVAL shares the newer window instead of reopening quota.
local watermark = redis.call("GET", KEYS[6])
if watermark then
  if not nonnegativeInteger(watermark) then
    return redis.error_reply("invalid rate receipt watermark")
  end
  now = math.max(now, tonumber(watermark))
end

redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now - minuteWindow)
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now - dayWindow)

local function persistWatermark()
  redis.call("SET", KEYS[6], tostring(now), "PX", dayWindow)
end

if redis.call("ZSCORE", KEYS[1], member) or redis.call("ZSCORE", KEYS[2], member) then
  redis.call("SET", KEYS[5], "a", "PX", decisionTtl, "NX")
  persistWatermark()
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
  local retryUntil = tonumber(first[2]) + window
  local retryAfter = math.max(1, math.ceil((retryUntil - now) / 1000))
  local shouldNotify = redis.call("SET", noticeKey, "1", "PX", math.max(1, retryUntil - now), "NX") and 1 or 0
  local decision = (limitedByMinute and "m:" or "d:") .. shouldNotify .. ":" .. retryUntil
  redis.call("SET", KEYS[5], decision, "PX", decisionTtl, "NX")
  persistWatermark()
  return {0, limitedByMinute and "minute" or "day", minuteCount, dayCount, retryAfter, shouldNotify, 0}
end

redis.call("ZADD", KEYS[1], now, member)
redis.call("ZADD", KEYS[2], now, member)
redis.call("PEXPIRE", KEYS[1], minuteWindow)
redis.call("PEXPIRE", KEYS[2], dayWindow)
redis.call("SET", KEYS[5], "a", "PX", decisionTtl, "NX")
persistWatermark()
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
    this.prefix = options.prefix ?? DEFAULT_RATE_LIMIT_PREFIX;
    if (!/^[A-Za-z0-9:_-]{1,128}$/.test(this.prefix)) {
      throw new Error("rate-limit prefix is invalid");
    }
    this.now = options.now ?? Date.now;
  }

  /**
   * Build the non-sensitive rate keys/configuration that must be included in
   * the same Redis EVAL as a new durable inbox publication.  Calling this
   * method alone cannot consume a slot.
   */
  reservationForIngress(
    senderHash: SenderHash | string,
    correlationId: string,
  ): RateLimitIngressReservation {
    assertPseudonymousHash(senderHash, "sender hash");
    assertPseudonymousHash(correlationId, "correlation id");
    const keys = this.keysFor(senderHash, correlationId);
    return Object.freeze({
      correlationId,
      ...keys,
      minuteWindowMs: this.policy.minuteWindowMs,
      dayWindowMs: this.policy.dayWindowMs,
      perMinute: this.policy.perMinute,
      perRollingDay: this.policy.perRollingDay,
      // A rate decision must outlive every encrypted inbox payload. The
      // default payload and rolling-day windows are both 24 hours.
      decisionTtlMs: this.policy.dayWindowMs,
    });
  }

  /** Read the immutable outcome of an ingress reservation, if one exists. */
  async getReservation(
    senderHash: SenderHash | string,
    correlationId: string,
  ): Promise<RateLimitReservationDecision | null> {
    assertPseudonymousHash(senderHash, "sender hash");
    assertPseudonymousHash(correlationId, "correlation id");
    const serialized = await this.redis.get(this.keysFor(senderHash, correlationId).decisionKey);
    if (!serialized) return null;
    return parseReservationDecision(serialized, this.now());
  }

  /**
   * Legacy/recovery-only fallback for an eligible record created before the
   * atomic publication reservation existed. It uses the original receipt
   * time and persists a correlation-scoped decision, so retries cannot
   * double-charge or become newly eligible after a window rolls over.
   */
  async reserveAtReceipt(
    senderHash: SenderHash | string,
    correlationId: string,
    receivedAtMs = this.now(),
    minimumDecisionTtlMs = this.policy.dayWindowMs,
  ): Promise<RateLimitResult> {
    assertPseudonymousHash(senderHash, "sender hash");
    assertPseudonymousHash(correlationId, "correlation id");
    if (!Number.isSafeInteger(receivedAtMs) || receivedAtMs < 0) {
      throw new Error("rate-limit receipt time is invalid");
    }
    if (!Number.isSafeInteger(minimumDecisionTtlMs) || minimumDecisionTtlMs < 1) {
      throw new Error("rate-limit decision retention is invalid");
    }
    const decisionTtlMs = Math.max(this.policy.dayWindowMs, minimumDecisionTtlMs);
    const keys = this.keysFor(senderHash, correlationId);
    const raw = await this.redis.eval(RATE_LIMIT_LUA, {
      keys: [
        keys.minuteKey,
        keys.dayKey,
        keys.minuteNoticeKey,
        keys.dayNoticeKey,
        keys.decisionKey,
        keys.receiptWatermarkKey,
      ],
      arguments: [
        String(receivedAtMs),
        String(this.policy.minuteWindowMs),
        String(this.policy.dayWindowMs),
        String(this.policy.perMinute),
        String(this.policy.perRollingDay),
        correlationId,
        String(decisionTtlMs),
      ],
    });
    return parseRateLimitReply(raw);
  }

  /** Backward-compatible direct reservation API for unit/legacy callers. */
  async consume(senderHash: SenderHash | string, correlationId: string): Promise<RateLimitResult> {
    return this.reserveAtReceipt(senderHash, correlationId, this.now());
  }

  private keysFor(senderHash: string, correlationId: string): {
    readonly minuteKey: string;
    readonly dayKey: string;
    readonly minuteNoticeKey: string;
    readonly dayNoticeKey: string;
    readonly decisionKey: string;
    readonly receiptWatermarkKey: string;
  } {
    const base = `${this.prefix}:${senderHash}`;
    return Object.freeze({
      minuteKey: `${base}:minute`,
      dayKey: `${base}:day`,
      minuteNoticeKey: `${base}:notice:minute`,
      dayNoticeKey: `${base}:notice:day`,
      decisionKey: `${base}:reservation:${correlationId}`,
      receiptWatermarkKey: `${base}:receipt-watermark`,
    });
  }
}

function parseReservationDecision(
  serialized: string,
  now: number,
): RateLimitReservationDecision {
  if (serialized === "a") {
    return Object.freeze({
      accepted: true,
      reason: null,
      shouldNotify: false,
      retryAfterSeconds: 0,
    });
  }
  const match = /^([md]):([01]):(\d+)$/.exec(serialized);
  if (!match) throw new Error("rate reservation decision is invalid");
  const retryUntilMs = Number(match[3]);
  if (!Number.isSafeInteger(retryUntilMs) || retryUntilMs < 0) {
    throw new Error("rate reservation decision is invalid");
  }
  return Object.freeze({
    accepted: false,
    reason: match[1] === "m" ? "minute" : "day",
    shouldNotify: match[2] === "1",
    retryAfterSeconds: Math.max(0, Math.ceil((retryUntilMs - now) / 1_000)),
  });
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
