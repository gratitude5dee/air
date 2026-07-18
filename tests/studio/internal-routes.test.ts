import { describe, expect, it, vi } from "vitest";

import type { StudioEnvironment } from "../../lib/studio/config.js";
import { handleStudioDrain, handleStudioReadiness } from "../../lib/studio/internal-routes.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";

function darkEnvironment(): StudioEnvironment {
  return {
    deploymentMode: "preview",
    releasePhase: "dark",
    storageTransport: "rest",
    imessageProjectId: "project",
    imessageProjectSecret: "project-secret",
    imessageWebhookSecret: "webhook-secret",
    upstashRedisRestUrl: "https://redis.example.test",
    upstashRedisRestToken: "redis-token",
    idHashSecret: "identity-secret",
    queueEncryptionKey: Buffer.alloc(32, 7).toString("base64"),
    allowedSenderHashes: ["a".repeat(64) as StudioEnvironment["allowedSenderHashes"][number]],
    cronSecret: "cron-secret",
    publicBaseUrl: "https://air.example.test",
    modelRegistryVersion: "2.1.0",
    dailyUserJobLimit: 3,
    activeJobLimit: 1,
    policy: {
      acceptNewJobs: false,
      allowProviderSubmit: false,
      allowProviderRecovery: false,
      deliveryEnabled: false,
    },
    policyExplicit: true,
    spendCaps: {
      perJobCents: 0,
      dailyCents: 0,
      canaryTotalCents: 0,
      canaryFalCents: 0,
    },
  };
}

function authorizedRequest(path: string, method: "GET" | "POST" = "POST"): Request {
  return new Request(`https://air.example.test${path}`, {
    method,
    headers: { authorization: "Bearer cron-secret" },
  });
}

describe("Studio Vercel internal routes", () => {
  it("does not create a runtime or run a drain for an unauthenticated request", async () => {
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);
    const drain = vi.fn(async () => undefined);
    const response = await handleStudioDrain(
      new Request("https://air.example.test/internal/drain-inbox", { method: "POST" }),
      drain,
      { loadEnvironment: darkEnvironment, getRuntime },
    );

    expect(response.status).toBe(401);
    expect(await response.text()).toBe("");
    expect(getRuntime).not.toHaveBeenCalled();
    expect(drain).not.toHaveBeenCalled();
  });

  it("runs the same authenticated drain used by the minute cron and reports only acceptance", async () => {
    const runtime = {} as StudioRuntime;
    const getRuntime = vi.fn(async () => runtime);
    const drain = vi.fn(async (received: StudioRuntime) => {
      expect(received).toBe(runtime);
    });
    const response = await handleStudioDrain(
      authorizedRequest("/internal/drain-inbox"),
      drain,
      { loadEnvironment: darkEnvironment, getRuntime },
    );

    expect(response.status).toBe(202);
    await expect(response.json()).resolves.toEqual({ accepted: true });
    expect(getRuntime).toHaveBeenCalledTimes(1);
    expect(drain).toHaveBeenCalledTimes(1);
  });

  it("fails closed when the authenticated drain cannot create its runtime", async () => {
    const drain = vi.fn(async () => undefined);
    const response = await handleStudioDrain(
      authorizedRequest("/internal/drain-inbox"),
      drain,
      {
        loadEnvironment: darkEnvironment,
        getRuntime: async () => Promise.reject(new Error("unavailable")),
      },
    );

    expect(response.status).toBe(503);
    expect(await response.text()).toBe("");
    expect(drain).not.toHaveBeenCalled();
  });

  it("reports dark-release blockers without connecting to storage or exposing secrets", async () => {
    const response = handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: darkEnvironment,
      getRuntime: vi.fn(async () => {
        throw new Error("must not connect");
      }),
    });

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual(expect.objectContaining({
      ok: false,
      releasePhase: "dark",
      registryVersion: "2.1.0",
      enabledModels: 0,
      sideEffects: {
        acceptNewJobs: false,
        allowProviderSubmit: false,
        allowProviderRecovery: false,
        deliveryEnabled: false,
      },
      problems: expect.arrayContaining(["transport_unverified", "no_enabled_models"]),
    }));
  });

  it("allows readiness only through its bearer-protected GET contract", async () => {
    const response = handleStudioReadiness(new Request("https://air.example.test/internal/readiness", {
      method: "POST",
      headers: { authorization: "Bearer cron-secret" },
    }), { loadEnvironment: darkEnvironment });

    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET");
    expect(await response.text()).toBe("");
  });
});
