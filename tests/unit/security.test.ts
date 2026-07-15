import { describe, expect, it } from "vitest";
import { loadAirEnvironment } from "../../lib/env.js";
import {
  createSenderIdentity,
  hashSenderIdentifier,
  isSenderAllowed,
  parseAllowedSenderHashes,
} from "../../lib/security.js";

const secret = "unit-test-identity-secret";

function productionEnvironment(
  overrides: Readonly<Record<string, string | undefined>> = {},
): Record<string, string | undefined> {
  const senderHash = hashSenderIdentifier(secret, "+14155551234");
  return {
    IMESSAGE_PROJECT_ID: "project",
    IMESSAGE_PROJECT_SECRET: "project-secret",
    IMESSAGE_WEBHOOK_SECRET: "webhook-secret",
    REDIS_URL: "rediss://example.invalid:6379",
    ZAP_AIR_SERVICE_URL: "https://zap.wzrd.tech",
    ZAP_AIR_SERVICE_TOKEN: "zap-air-service-token",
    AIR_ID_HASH_SECRET: secret,
    AIR_QUEUE_ENCRYPTION_KEY: Buffer.alloc(32, 1).toString("base64"),
    AIR_ALLOWED_SENDER_HASHES: senderHash,
    AIR_PUBLIC_BASE_URL: "https://air.wzrd.tech",
    CRON_SECRET: "cron-secret",
    ...overrides,
  };
}

describe("sender privacy and Air video environment", () => {
  it("normalizes before HMAC hashing and never returns the raw identifier", () => {
    const formatted = hashSenderIdentifier(secret, "+1 (415) 555-1234");
    const canonical = hashSenderIdentifier(secret, "+14155551234");
    const email = createSenderIdentity(secret, "Artist@Example.COM");

    expect(formatted).toBe(canonical);
    expect(formatted).toMatch(/^[a-f0-9]{64}$/);
    expect(formatted).not.toContain("415");
    expect(email.kind).toBe("email");
    expect(email.senderHash).not.toContain("artist");
  });

  it("fails closed for empty or malformed allowlists", () => {
    const sender = hashSenderIdentifier(secret, "+14155551234");
    expect(isSenderAllowed(sender, [])).toBe(false);
    expect(isSenderAllowed(sender, [sender])).toBe(true);
    expect(isSenderAllowed(sender, ["not-a-hash"])).toBe(false);
    expect(() => parseAllowedSenderHashes("not-a-hash")).toThrow(/invalid hash/);
    expect(parseAllowedSenderHashes(`${sender}, ${sender}`)).toEqual([sender]);
  });

  it("fails closed in production until every private video-service value exists", () => {
    expect(() => loadAirEnvironment({}, { mode: "production" }))
      .toThrow(/missing required production environment/);
    const senderHash = hashSenderIdentifier(secret, "+14155551234");
    const loaded = loadAirEnvironment(productionEnvironment(), { mode: "production" });

    expect(loaded.allowedSenderHashes).toEqual([senderHash]);
    expect(loaded.mode).toBe("production");
  });

  it("validates the private Zap origin and bearer-token shape", () => {
    expect(() => loadAirEnvironment(productionEnvironment({
      ZAP_AIR_SERVICE_URL: "http://zap.wzrd.tech",
    }), { mode: "production" })).toThrow(/ZAP_AIR_SERVICE_URL is invalid/);
    expect(() => loadAirEnvironment(productionEnvironment({
      ZAP_AIR_SERVICE_TOKEN: "valid-prefix\ninvalid-suffix",
    }), { mode: "production" })).toThrow(/ZAP_AIR_SERVICE_TOKEN is invalid/);
  });

  it("treats Vercel production as production even without a normal NODE_ENV", () => {
    expect(() => loadAirEnvironment({
      VERCEL_ENV: "production",
      NODE_ENV: "unexpected-local-value",
    })).toThrow(/missing required production environment/);
    expect(() => loadAirEnvironment({ VERCEL_ENV: "production" }))
      .toThrow(/REDIS_URL/);
  });
});
