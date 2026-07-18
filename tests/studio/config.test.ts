import { describe, expect, it } from "vitest";
import { explicitBoolean, loadStudioEnvironment, studioReadinessProblems, usdToCents } from "../../lib/studio/config.js";

const key = Buffer.alloc(32, 7).toString("base64");
const senderHash = "a".repeat(64);

function configured(overrides: Record<string, string | undefined> = {}) {
  return {
    IMESSAGE_PROJECT_ID: "project",
    IMESSAGE_PROJECT_SECRET: "secret",
    IMESSAGE_WEBHOOK_SECRET: "webhook",
    REDIS_URL: "rediss://redis.example.test:6380",
    AIR_ID_HASH_SECRET: "hash-secret",
    AIR_QUEUE_ENCRYPTION_KEY: key,
    AIR_ALLOWED_SENDER_HASHES: senderHash,
    AIR_PUBLIC_BASE_URL: "https://air.example.test",
    CRON_SECRET: "cron-secret",
    AIR_MODEL_REGISTRY_VERSION: "test-registry",
    AIR_ACCEPT_NEW_JOBS: "false",
    AIR_ALLOW_PROVIDER_SUBMIT: "false",
    AIR_ALLOW_PROVIDER_RECOVERY: "false",
    AIR_DELIVERY_ENABLED: "false",
    ...overrides,
  };
}

describe("Air studio configuration", () => {
  it("defaults malformed spend caps to zero and side effects to false", () => {
    expect(usdToCents(undefined)).toBe(0);
    expect(usdToCents("invalid")).toBe(0);
    expect(usdToCents("1.2")).toBe(120);
    expect(usdToCents("1.239")).toBe(0);
    expect(explicitBoolean("TRUE")).toBe(false);
    expect(explicitBoolean("true")).toBe(true);
  });

  it("requires a complete Upstash REST pair", () => {
    expect(() => loadStudioEnvironment(configured({ UPSTASH_REDIS_REST_URL: "https://redis.example.test" }))).toThrow(/pair/);
  });

  it("keeps a dark release closed even when all identity settings are present", () => {
    const environment = loadStudioEnvironment(configured({ VERCEL_ENV: "production" }));
    expect(environment.releasePhase).toBe("dark");
    expect(environment.policy.acceptNewJobs).toBe(false);
    expect(environment.policy.allowProviderSubmit).toBe(false);
    expect(environment.storageTransport).toBe("tcp");
    expect(studioReadinessProblems(environment, "test-registry")).toEqual([]);
  });

  it("requires explicit side-effect switches for production readiness", () => {
    const environment = loadStudioEnvironment(configured({
      VERCEL_ENV: "production",
      AIR_DELIVERY_ENABLED: undefined,
    }));
    expect(studioReadinessProblems(environment, "test-registry")).toContain("side_effect_policy_unset");
  });

  it("rejects a canary without explicit budget authorization", () => {
    expect(() => loadStudioEnvironment(configured({ AIR_RELEASE_PHASE: "canary" }))).toThrow(/canary phase/);
  });
});
