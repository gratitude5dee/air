import { describe, expect, it } from "vitest";

import { loadStudioEnvironment } from "../../lib/studio/config.js";
import {
  createSenderIdentity,
  hashSenderIdentifier,
  isSenderAllowed,
  parseAllowedSenderHashes,
} from "../../lib/security.js";

const secret = "studio-security-test-secret";
const encryptionKey = Buffer.alloc(32, 5).toString("base64");

describe("Air studio sender privacy and fail-closed configuration", () => {
  it("canonicalizes a sender before HMAC hashing without returning the identifier", () => {
    const formatted = hashSenderIdentifier(secret, "+1 (415) 555-1234");
    const canonical = hashSenderIdentifier(secret, "+14155551234");
    const email = createSenderIdentity(secret, "Artist@Example.COM");

    expect(formatted).toBe(canonical);
    expect(formatted).toMatch(/^[a-f0-9]{64}$/);
    expect(formatted).not.toContain("415");
    expect(email.kind).toBe("email");
    expect(email.senderHash).not.toContain("artist");
  });

  it("fails closed for empty, malformed, or non-matching allowlists", () => {
    const sender = hashSenderIdentifier(secret, "+14155551234");
    expect(isSenderAllowed(sender, [])).toBe(false);
    expect(isSenderAllowed(sender, [sender])).toBe(true);
    expect(isSenderAllowed(sender, ["not-a-hash"])).toBe(false);
    expect(() => parseAllowedSenderHashes("not-a-hash")).toThrow(/invalid hash/);
    expect(parseAllowedSenderHashes(`${sender}, ${sender}`)).toEqual([sender]);
  });

  it("keeps missing production switches and storage credentials fail-closed", () => {
    const sender = hashSenderIdentifier(secret, "+14155551234");
    const environment = loadStudioEnvironment({
      VERCEL_ENV: "production",
      IMESSAGE_PROJECT_ID: "project",
      IMESSAGE_PROJECT_SECRET: "project-secret",
      IMESSAGE_WEBHOOK_SECRET: "webhook-secret",
      REDIS_URL: "rediss://redis.example.test:6380",
      AIR_ID_HASH_SECRET: secret,
      AIR_QUEUE_ENCRYPTION_KEY: encryptionKey,
      AIR_ALLOWED_SENDER_HASHES: sender,
      CRON_SECRET: "cron-secret",
      AIR_MODEL_REGISTRY_VERSION: "2.1.0",
      // A missing flag must resolve false rather than authorize a side effect.
      AIR_ACCEPT_NEW_JOBS: undefined,
      AIR_ALLOW_PROVIDER_SUBMIT: "false",
      AIR_ALLOW_PROVIDER_RECOVERY: "false",
      AIR_DELIVERY_ENABLED: "false",
    });

    expect(environment.policy.acceptNewJobs).toBe(false);
    expect(environment.policyExplicit).toBe(false);
    expect(() => loadStudioEnvironment({
      UPSTASH_REDIS_REST_URL: "https://redis.example.test",
    })).toThrow(/pair/);
  });
});
