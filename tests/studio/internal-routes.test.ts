import { describe, expect, it, vi } from "vitest";

import type { StudioEnvironment } from "../../lib/studio/config.js";
import {
  handleStudioDrain,
  handleStudioReadiness,
  handleStudioReleaseFence,
} from "../../lib/studio/internal-routes.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";

function darkEnvironment(): StudioEnvironment {
  return {
    deploymentMode: "preview",
    releasePhase: "dark",
    releaseGeneration: 0,
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

function releaseFenceEnvironment(): StudioEnvironment {
  return { ...darkEnvironment(), releaseFenceSecret: "release-fence-secret" };
}

function releaseFenceRequest(ifMatch: string | undefined, method = "POST", token = "release-fence-secret"): Request {
  const headers = new Headers({ authorization: `Bearer ${token}` });
  if (ifMatch !== undefined) headers.set("if-match", ifMatch);
  return new Request("https://air.example.test/internal/release-fence", { method, headers });
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

  it("does not create a runtime for an unauthenticated readiness request", async () => {
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);
    const response = await handleStudioReadiness(
      new Request("https://air.example.test/internal/readiness", { method: "GET" }),
      { loadEnvironment: darkEnvironment, getRuntime },
    );

    expect(response.status).toBe(401);
    expect(await response.text()).toBe("");
    expect(getRuntime).not.toHaveBeenCalled();
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

  it("records an environment-scoped Cron heartbeat only after a successful authenticated GET drain", async () => {
    const recordCronHeartbeat = vi.fn(async (_scope: string) => undefined);
    const runtime = { store: { recordCronHeartbeat } } as unknown as StudioRuntime;
    const drain = vi.fn(async () => undefined);
    const manualResponse = await handleStudioDrain(
      authorizedRequest("/internal/drain-inbox", "POST"),
      drain,
      { loadEnvironment: darkEnvironment, getRuntime: async () => runtime },
    );
    const response = await handleStudioDrain(
      authorizedRequest("/internal/drain-inbox", "GET"),
      drain,
      { loadEnvironment: darkEnvironment, getRuntime: async () => runtime },
    );

    expect(manualResponse.status).toBe(202);
    expect(response.status).toBe(202);
    expect(drain).toHaveBeenCalledTimes(2);
    expect(recordCronHeartbeat).toHaveBeenCalledWith("preview");
    expect(recordCronHeartbeat).toHaveBeenCalledTimes(1);
  });

  it("never records a Cron heartbeat when the authenticated drain fails", async () => {
    const recordCronHeartbeat = vi.fn(async () => undefined);
    const runtime = { store: { recordCronHeartbeat } } as unknown as StudioRuntime;
    const response = await handleStudioDrain(
      authorizedRequest("/internal/drain-inbox", "GET"),
      async () => Promise.reject(new Error("drain failed")),
      { loadEnvironment: darkEnvironment, getRuntime: async () => runtime },
    );

    expect(response.status).toBe(503);
    expect(recordCronHeartbeat).not.toHaveBeenCalled();
  });

  it("fails closed if a successful Cron drain cannot persist its heartbeat", async () => {
    const runtime = {
      store: { recordCronHeartbeat: vi.fn(async () => Promise.reject(new Error("redis unavailable"))) },
    } as unknown as StudioRuntime;
    const drain = vi.fn(async () => undefined);
    const response = await handleStudioDrain(
      authorizedRequest("/internal/drain-inbox", "GET"),
      drain,
      { loadEnvironment: darkEnvironment, getRuntime: async () => runtime },
    );

    expect(response.status).toBe(503);
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

  it("reports dark-release blockers with a secret-safe Redis probe and preview Cron status", async () => {
    const getCronFreshness = vi.fn(async (_scope: string) => "missing" as const);
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: darkEnvironment,
      getRuntime: vi.fn(async () => ({ store: { getCronFreshness } }) as unknown as StudioRuntime),
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
      effectiveSideEffects: {
        acceptNewJobs: false,
        allowProviderSubmit: false,
        allowProviderRecovery: false,
        deliveryEnabled: false,
      },
      redis: "reachable",
      cron: "not_scheduled",
      problems: expect.arrayContaining(["transport_unverified", "no_enabled_models"]),
    }));
    expect(getCronFreshness).toHaveBeenCalledWith("preview");
  });

  it("reports dark-phase switches as ineffective even when their configured values are true", async () => {
    const environment = {
      ...darkEnvironment(),
      policy: {
        acceptNewJobs: true,
        allowProviderSubmit: true,
        allowProviderRecovery: true,
        deliveryEnabled: true,
      },
    };
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: () => environment,
      getRuntime: async () => ({
        store: { getCronFreshness: vi.fn(async () => "fresh" as const) },
      }) as unknown as StudioRuntime,
    });

    await expect(response.json()).resolves.toEqual(expect.objectContaining({
      sideEffects: environment.policy,
      effectiveSideEffects: {
        acceptNewJobs: false,
        allowProviderSubmit: false,
        allowProviderRecovery: false,
        deliveryEnabled: false,
      },
    }));
  });

  it("reports a stale production Cron heartbeat without exposing its timestamp", async () => {
    const environment = { ...darkEnvironment(), deploymentMode: "production" as const };
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: () => environment,
      getRuntime: async () => ({
        store: { getCronFreshness: vi.fn(async () => "stale" as const) },
      }) as unknown as StudioRuntime,
    });

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual(expect.objectContaining({
      redis: "reachable",
      cron: "stale",
      problems: expect.arrayContaining(["cron_stale"]),
    }));
  });

  it("distinguishes a missing or invalid production Cron heartbeat from a stale one", async () => {
    const environment = { ...darkEnvironment(), deploymentMode: "production" as const };
    for (const [freshness, expectedCron, expectedProblem] of [
      ["missing", "missing", "cron_missing"],
      ["invalid", "invalid", "cron_invalid"],
    ] as const) {
      const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
        loadEnvironment: () => environment,
        getRuntime: async () => ({
          store: { getCronFreshness: vi.fn(async () => freshness) },
        }) as unknown as StudioRuntime,
      });

      expect(response.status).toBe(503);
      await expect(response.json()).resolves.toEqual(expect.objectContaining({
        redis: "reachable",
        cron: expectedCron,
        problems: expect.arrayContaining([expectedProblem]),
      }));
    }
  });

  it("does not mislabel missing runtime prerequisites as a Redis outage", async () => {
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: () => ({
        ...darkEnvironment(),
        storageTransport: "unconfigured",
        upstashRedisRestUrl: undefined,
        upstashRedisRestToken: undefined,
        idHashSecret: undefined,
        queueEncryptionKey: undefined,
      }),
      getRuntime,
    });

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual(expect.objectContaining({
      redis: "unknown",
      cron: "unknown",
      problems: expect.arrayContaining(["redis_reachability_unknown", "cron_freshness_unknown"]),
    }));
    expect(getRuntime).not.toHaveBeenCalled();
  });

  it("reports an unavailable Redis path only after a configured runtime probe fails", async () => {
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: darkEnvironment,
      getRuntime: async () => Promise.reject(new Error("redis unavailable")),
    });

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual(expect.objectContaining({
      redis: "unreachable",
      cron: "unreachable",
      problems: expect.arrayContaining(["redis_unavailable", "cron_freshness_unknown"]),
    }));
  });

  it("allows readiness only through its bearer-protected GET contract", async () => {
    const response = await handleStudioReadiness(new Request("https://air.example.test/internal/readiness", {
      method: "POST",
      headers: { authorization: "Bearer cron-secret" },
    }), { loadEnvironment: darkEnvironment });

    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET");
    expect(await response.text()).toBe("");
  });

  it("advances or confirms the release fence only with a POST, dedicated bearer, and strict CAS precondition", async () => {
    const advanceReleaseFence = vi.fn(async (
      _previousGeneration: number,
    ): Promise<{ kind: "advanced" | "current" | "conflict" | "invalid" }> => ({ kind: "advanced" }));
    const getRuntime = vi.fn(async () => ({ store: { advanceReleaseFence } }) as unknown as StudioRuntime);
    const response = await handleStudioReleaseFence(releaseFenceRequest("-1"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime,
    });

    expect(response.status).toBe(202);
    await expect(response.json()).resolves.toEqual({ accepted: true });
    expect(advanceReleaseFence).toHaveBeenCalledWith(-1);

    advanceReleaseFence.mockResolvedValueOnce({ kind: "current" });
    const current = await handleStudioReleaseFence(releaseFenceRequest("0"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime,
    });
    expect(current.status).toBe(202);
    await expect(current.json()).resolves.toEqual({ accepted: true });
    expect(advanceReleaseFence).toHaveBeenLastCalledWith(0);
  });

  it("does not load a runtime for an unauthenticated or malformed release-fence request", async () => {
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);
    for (const request of [
      releaseFenceRequest("-1", "POST", "wrong-secret"),
      new Request("https://air.example.test/internal/release-fence", { method: "POST", headers: { "if-match": "-1" } }),
    ]) {
      const response = await handleStudioReleaseFence(request, {
        loadEnvironment: releaseFenceEnvironment,
        getRuntime,
      });
      expect(response.status).toBe(401);
      expect(await response.text()).toBe("");
    }
    for (const ifMatch of [undefined, "-2", "01", "1.0", "9007199254740992"]) {
      const response = await handleStudioReleaseFence(releaseFenceRequest(ifMatch), {
        loadEnvironment: releaseFenceEnvironment,
        getRuntime,
      });
      expect(response.status).toBe(409);
      expect(await response.text()).toBe("");
    }
    expect(getRuntime).not.toHaveBeenCalled();
  });

  it("returns only a conflict response for a mismatched or invalid persisted release fence", async () => {
    for (const kind of ["conflict", "invalid"] as const) {
      const advanceReleaseFence = vi.fn(async () => ({ kind }));
      const response = await handleStudioReleaseFence(releaseFenceRequest("1"), {
        loadEnvironment: releaseFenceEnvironment,
        getRuntime: async () => ({ store: { advanceReleaseFence } }) as unknown as StudioRuntime,
      });
      expect(response.status).toBe(409);
      expect(await response.text()).toBe("");
      expect(advanceReleaseFence).toHaveBeenCalledWith(1);
    }
  });

  it("exposes the release-fence route only through its POST contract", async () => {
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);
    const response = await handleStudioReleaseFence(releaseFenceRequest("-1", "GET"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime,
    });

    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("POST");
    expect(await response.text()).toBe("");
    expect(getRuntime).not.toHaveBeenCalled();
  });
});
