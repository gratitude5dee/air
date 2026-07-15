import { describe, expect, it } from "vitest";
import { loadAirEnvironment } from "../../lib/env.js";
import { redactLogFields } from "../../lib/logging.js";
import {
  createSenderIdentity,
  hashSenderIdentifier,
  isSenderAllowed,
  parseAllowedSenderHashes,
  validateAllowedSourceDeepLink,
} from "../../lib/security.js";

const secret = "unit-test-identity-secret";

describe("sender privacy", () => {
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

  it("fails closed for empty/invalid allowlists and compares valid hashes", () => {
    const sender = hashSenderIdentifier(secret, "+14155551234");
    expect(isSenderAllowed(sender, [])).toBe(false);
    expect(isSenderAllowed(sender, [sender])).toBe(true);
    expect(isSenderAllowed(sender, ["not-a-hash"])).toBe(false);
    expect(() => parseAllowedSenderHashes("not-a-hash")).toThrow(/invalid hash/);
    expect(parseAllowedSenderHashes(`${sender}, ${sender}`)).toEqual([sender]);
  });

  it("allows only approved HTTPS source hosts and removes sensitive log fields", () => {
    expect(validateAllowedSourceDeepLink("https://www.youtube.com/watch?v=abc")).not.toBeNull();
    expect(validateAllowedSourceDeepLink("https://youtube.com/watch?v=abc123&t=1m30s"))
      .toBe("https://www.youtube.com/watch?v=abc123&t=90");
    expect(validateAllowedSourceDeepLink("https://youtu.be/abc123?start=90"))
      .toBe("https://youtu.be/abc123?t=90");
    expect(validateAllowedSourceDeepLink("http://youtube.com/watch?v=abc")).toBeNull();
    expect(validateAllowedSourceDeepLink("https://evil.example/youtube.com")).toBeNull();
    expect(validateAllowedSourceDeepLink("https://evil.example@youtube.com/watch?v=abc")).toBeNull();
    expect(validateAllowedSourceDeepLink("https://youtube.com/redirect?q=https%3A%2F%2Fevil.example"))
      .toBeNull();
    expect(validateAllowedSourceDeepLink("https://www.youtube.com/watch?v=abc123&q=https%3A%2F%2Fevil.example"))
      .toBeNull();
    expect(validateAllowedSourceDeepLink("https://youtu.be/redirect?q=https%3A%2F%2Fevil.example"))
      .toBeNull();
    expect(validateAllowedSourceDeepLink("https://youtu.be/abc123?si=tracking-token")).toBeNull();
    expect(validateAllowedSourceDeepLink(`https://youtu.be/abc123?start=${"9".repeat(1_000)}`))
      .toBeNull();

    const fields = redactLogFields({
      sender: "+14155551234",
      senderId: "+14155551234",
      messageId: "provider-message-123",
      webhookId: "provider-webhook-123",
      threadId: "imessage-thread-123",
      text: "private question",
      signature: "secret-signature",
      senderHash: "a".repeat(64),
      sourceCount: 2,
      errorCategory: "timeout",
    });
    expect(fields).toEqual({
      senderHash: "a".repeat(64),
      sourceCount: 2,
      errorCategory: "timeout",
    });
  });

  it("fails closed in production until every critical secret and an approved hash exist", () => {
    expect(() => loadAirEnvironment({}, { mode: "production" })).toThrow(/missing required production environment/);
    const senderHash = hashSenderIdentifier(secret, "+14155551234");
    const loaded = loadAirEnvironment({
      IMESSAGE_PROJECT_ID: "project",
      IMESSAGE_PROJECT_SECRET: "project-secret",
      IMESSAGE_WEBHOOK_SECRET: "webhook-secret",
      REDIS_URL: "rediss://example.invalid:6379",
      MONGODB_URI: "mongodb+srv://readonly@example.invalid/music_rag",
      MONGODB_DB: "music_rag",
      MONGODB_SERVER_SELECTION_TIMEOUT_MS: "8000",
      MONGODB_CONNECT_TIMEOUT_MS: "8000",
      VOYAGE_API_KEY: "voyage-key",
      EMBED_MODEL: "voyage-4-large",
      RERANK_MODEL: "rerank-2.5",
      GENERATION_MODEL: "google/gemini-3.5-flash",
      AIR_ID_HASH_SECRET: secret,
      AIR_QUEUE_ENCRYPTION_KEY: Buffer.alloc(32, 1).toString("base64"),
      AIR_ALLOWED_SENDER_HASHES: senderHash,
      AIR_PUBLIC_BASE_URL: "https://air.wzrd.tech",
      CRON_SECRET: "cron-secret",
    }, { mode: "production" });

    expect(loaded.allowedSenderHashes).toEqual([senderHash]);
    expect(loaded.mode).toBe("production");
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
