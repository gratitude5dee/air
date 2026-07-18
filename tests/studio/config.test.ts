import { describe, expect, it } from "vitest";
import {
  canAcceptStudioJobs,
  canDeliverStudioMessages,
  canRecoverStudioProvider,
  canSubmitStudioProvider,
  explicitBoolean,
  loadStudioEnvironment,
  studioReadinessProblems,
  usdToCents,
} from "../../lib/studio/config.js";

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

  it("prioritizes the explicit V2 REST pair while preserving a legacy TCP URL for rollback", () => {
    const shared = loadStudioEnvironment(configured({
      UPSTASH_REDIS_REST_URL: "https://redis.example.test",
      UPSTASH_REDIS_REST_TOKEN: "rest-token",
    }));
    expect(shared.storageTransport).toBe("rest");

    const legacy = loadStudioEnvironment(configured({
      UPSTASH_REDIS_REST_URL: "https://other-redis.example.test",
      UPSTASH_REDIS_REST_TOKEN: "rest-token",
    }));
    expect(legacy.storageTransport).toBe("rest");

    const logicalDatabase = loadStudioEnvironment(configured({
      REDIS_URL: "rediss://redis.example.test:6380/1",
      UPSTASH_REDIS_REST_URL: "https://redis.example.test",
      UPSTASH_REDIS_REST_TOKEN: "rest-token",
    }));
    expect(logicalDatabase.storageTransport).toBe("rest");
  });

  it("keeps a dark release closed even when all identity settings are present", () => {
    const environment = loadStudioEnvironment(configured({ VERCEL_ENV: "production" }));
    expect(environment.releasePhase).toBe("dark");
    expect(environment.releaseGeneration).toBe(0);
    expect(environment.policy.acceptNewJobs).toBe(false);
    expect(environment.policy.allowProviderSubmit).toBe(false);
    expect(environment.storageTransport).toBe("tcp");
    expect(studioReadinessProblems(environment, "test-registry")).toEqual([]);
  });

  it("requires an active release phase as well as every side-effect switch", () => {
    const enabledSwitches = {
      AIR_ACCEPT_NEW_JOBS: "true",
      AIR_ALLOW_PROVIDER_SUBMIT: "true",
      AIR_ALLOW_PROVIDER_RECOVERY: "true",
      AIR_DELIVERY_ENABLED: "true",
    };
    const dark = loadStudioEnvironment(configured(enabledSwitches));

    expect(canAcceptStudioJobs(dark)).toBe(false);
    expect(canSubmitStudioProvider(dark)).toBe(false);
    expect(canRecoverStudioProvider(dark)).toBe(false);
    expect(canDeliverStudioMessages(dark)).toBe(false);

    const active = loadStudioEnvironment(configured({
      ...enabledSwitches,
      VERCEL_ENV: "production",
      AIR_RELEASE_PHASE: "production",
      AIR_RELEASE_GENERATION: "1",
      AIR_RELEASE_FENCE_SECRET: "release-fence-secret",
    }));
    expect(canAcceptStudioJobs(active)).toBe(true);
    expect(canSubmitStudioProvider(active)).toBe(true);
    expect(canRecoverStudioProvider(active)).toBe(true);
    expect(canDeliverStudioMessages(active)).toBe(true);
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

  it("requires a strict, positive release generation and fence secret for active phases", () => {
    const active = {
      VERCEL_ENV: "production",
      AIR_RELEASE_PHASE: "production",
      AIR_RELEASE_GENERATION: "1",
      AIR_RELEASE_FENCE_SECRET: "release-fence-secret",
    };

    expect(() => loadStudioEnvironment(configured({
      ...active,
      AIR_RELEASE_GENERATION: undefined,
    }))).toThrow(/AIR_RELEASE_GENERATION/);
    expect(() => loadStudioEnvironment(configured({
      ...active,
      AIR_RELEASE_GENERATION: "0",
    }))).toThrow(/positive AIR_RELEASE_GENERATION/);
    for (const invalid of ["-1", "1.5", "01", "9007199254740992"]) {
      expect(() => loadStudioEnvironment(configured({
        ...active,
        AIR_RELEASE_GENERATION: invalid,
      }))).toThrow(/AIR_RELEASE_GENERATION is invalid/);
    }
    expect(() => loadStudioEnvironment(configured({
      ...active,
      AIR_RELEASE_FENCE_SECRET: " \n",
    }))).toThrow(/AIR_RELEASE_FENCE_SECRET/);

    const environment = loadStudioEnvironment(configured(active));
    expect(environment.releaseGeneration).toBe(1);
    expect(environment.releaseFenceSecret).toBe("release-fence-secret");
    expect(studioReadinessProblems({ ...environment, releaseGeneration: 0 }, "test-registry"))
      .toContain("release_generation_unavailable");
    expect(studioReadinessProblems({ ...environment, releaseFenceSecret: undefined }, "test-registry"))
      .toContain("release_fence_unavailable");
  });
});
