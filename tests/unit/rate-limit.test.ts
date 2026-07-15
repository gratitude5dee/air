import { describe, expect, it } from "vitest";
import { MemoryRedis } from "../../lib/redis.js";
import { DEFAULT_RATE_LIMIT_POLICY, RateLimiter } from "../../lib/rate-limit.js";

const senderHash = "a".repeat(64);
const correlation = (value: number) => value.toString(16).padStart(64, "0");

describe("rolling sender limits", () => {
  it("enforces 3/minute and sends only one limit notice per window", async () => {
    let now = 1_000_000;
    const limiter = new RateLimiter(new MemoryRedis(() => now), { now: () => now });

    for (let index = 0; index < 3; index += 1) {
      await expect(limiter.consume(senderHash, correlation(index + 1))).resolves.toMatchObject({ accepted: true });
    }
    await expect(limiter.consume(senderHash, correlation(4))).resolves.toMatchObject({
      accepted: false,
      reason: "minute",
      shouldNotify: true,
    });
    await expect(limiter.consume(senderHash, correlation(5))).resolves.toMatchObject({
      accepted: false,
      reason: "minute",
      shouldNotify: false,
    });

    now += 60_001;
    await expect(limiter.consume("b".repeat(64), correlation(6))).resolves.toMatchObject({ accepted: true });
  });

  it("enforces the rolling 24-hour cap and treats a repeated correlation id idempotently", async () => {
    let now = 2_000_000;
    const limiter = new RateLimiter(new MemoryRedis(() => now), { now: () => now });

    const first = await limiter.consume(senderHash, correlation(1));
    const duplicate = await limiter.consume(senderHash, correlation(1));
    expect(first).toMatchObject({ accepted: true, minuteCount: 1, dayCount: 1 });
    expect(duplicate).toMatchObject({ accepted: true, duplicate: true, minuteCount: 1, dayCount: 1 });

    for (let index = 2; index <= 3; index += 1) {
      now += 61_000;
      await expect(limiter.consume(senderHash, correlation(index))).resolves.toMatchObject({ accepted: true });
    }
    now += 61_000;
    await expect(limiter.consume(senderHash, correlation(4))).resolves.toMatchObject({
      accepted: false,
      reason: "day",
      shouldNotify: true,
    });
  });

  it("retains a legacy receipt decision through the requested payload horizon", async () => {
    let now = 3_000_000;
    const limiter = new RateLimiter(new MemoryRedis(() => now), { now: () => now });
    const horizonMs = 2 * DEFAULT_RATE_LIMIT_POLICY.dayWindowMs;

    await expect(limiter.reserveAtReceipt(senderHash, correlation(1), now, horizonMs))
      .resolves.toMatchObject({ accepted: true });

    now += DEFAULT_RATE_LIMIT_POLICY.dayWindowMs + 1;
    await expect(limiter.getReservation(senderHash, correlation(1)))
      .resolves.toMatchObject({ accepted: true });
  });
});
