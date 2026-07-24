import { describe, expect, it, vi } from "vitest";

import type { StudioEnvironment } from "../../lib/studio/config.js";
import {
  handleStudioDrain,
  handleStudioInboxFifoMigration,
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

function productionDeliveryEnvironment(): StudioEnvironment {
  return {
    ...darkEnvironment(),
    deploymentMode: "production",
    releasePhase: "production",
    releaseGeneration: 1,
    releaseFenceSecret: "release-fence-secret",
    policy: {
      acceptNewJobs: false,
      allowProviderSubmit: false,
      allowProviderRecovery: false,
      deliveryEnabled: true,
    },
    policyExplicit: true,
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

function inboxFifoMigrationRequest(
  method: "GET" | "POST" | "PUT",
  action?: string,
  token = "release-fence-secret",
): Request {
  const headers = new Headers({ authorization: `Bearer ${token}` });
  if (action !== undefined) headers.set("x-air-fifo-migration-action", action);
  return new Request("https://air.example.test/internal/inbox-fifo-migration", { method, headers });
}

function readinessRuntime(freshness: "fresh" | "stale" | "missing" | "invalid") {
  return {
    store: { getCronFreshness: vi.fn(async () => freshness) },
    transport: { verified: false },
    provider: { verified: false },
  } as unknown as StudioRuntime;
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

  it("rejects unsupported drain methods before loading configuration or running work", async () => {
    const loadEnvironment = vi.fn(darkEnvironment);
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);
    const drain = vi.fn(async () => undefined);
    const response = await handleStudioDrain(
      new Request("https://air.example.test/internal/drain-inbox", {
        method: "PUT",
        headers: { authorization: "Bearer cron-secret" },
      }),
      drain,
      { loadEnvironment, getRuntime },
    );

    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET, POST");
    expect(await response.text()).toBe("");
    expect(loadEnvironment).not.toHaveBeenCalled();
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
      getRuntime: vi.fn(async () => ({
        store: { getCronFreshness },
        transport: { verified: false },
        provider: { verified: false },
      }) as unknown as StudioRuntime),
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
      problems: expect.arrayContaining(["transport_unverified"]),
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
      getRuntime: async () => readinessRuntime("fresh"),
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

  it("reports a production delivery-only release as ready without enabling a model or provider", async () => {
    const environment = productionDeliveryEnvironment();
    const getReleaseFenceStatus = vi.fn(async () => ({
      kind: "current" as const,
      stamp: { generation: environment.releaseGeneration, phase: environment.releasePhase },
    }));
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: () => environment,
      getRuntime: async () => ({
        store: {
          getCronFreshness: vi.fn(async () => "fresh" as const),
          getReleaseFenceStatus,
        },
        transport: { verified: true },
        provider: { verified: false },
      }) as unknown as StudioRuntime,
    });

    expect(response.status).toBe(200);
    const body = await response.json() as Record<string, unknown>;
    expect(body).toEqual(expect.objectContaining({
      ok: true,
      enabledModels: 0,
      redis: "reachable",
      cron: "fresh",
      releaseFence: "current",
      effectiveSideEffects: environment.policy,
      problems: [],
    }));
    expect(body).not.toHaveProperty("stamp");
    expect(body).not.toHaveProperty("releaseGeneration");
    expect(getReleaseFenceStatus).toHaveBeenCalledTimes(1);
  });

  it("rejects an active admission configuration until the durable inbox is FIFO-ready", async () => {
    const environment: StudioEnvironment = {
      ...productionDeliveryEnvironment(),
      policy: {
        acceptNewJobs: true,
        allowProviderSubmit: true,
        allowProviderRecovery: false,
        deliveryEnabled: false,
      },
      spendCaps: {
        perJobCents: 100,
        dailyCents: 100,
        canaryTotalCents: 0,
        canaryFalCents: 0,
      },
    };
    const cases = [
      {
        status: { kind: "current" as const, phase: "legacy" as const, holdingScopes: 0 },
        expected: "legacy",
        problem: "inbox_fifo_legacy",
      },
      {
        status: { kind: "current" as const, phase: "holding" as const, holdingScopes: 2 },
        expected: "holding",
        problem: "inbox_fifo_holding",
      },
      {
        status: { kind: "invalid" as const },
        expected: "invalid",
        problem: "inbox_fifo_invalid",
      },
    ] as const;

    for (const testCase of cases) {
      const getInboxFifoMigrationStatus = vi.fn(async () => testCase.status);
      const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
        loadEnvironment: () => environment,
        getRuntime: async () => ({
          store: {
            getCronFreshness: vi.fn(async () => "fresh" as const),
            getInboxFifoMigrationStatus,
            getReleaseFenceStatus: vi.fn(async () => ({
              kind: "current" as const,
              stamp: { generation: environment.releaseGeneration, phase: environment.releasePhase },
            })),
          },
          transport: { verified: true },
          provider: { verified: true },
        }) as unknown as StudioRuntime,
      });

      expect(response.status, testCase.expected).toBe(503);
      await expect(response.json()).resolves.toEqual(expect.objectContaining({
        inboxFifo: testCase.expected,
        effectiveSideEffects: expect.objectContaining({ acceptNewJobs: false }),
        problems: expect.arrayContaining([testCase.problem]),
      }));
      expect(getInboxFifoMigrationStatus).toHaveBeenCalledTimes(1);
    }
  });

  it("probes a deferred Photon transport only for an active delivery release", async () => {
    const environment = productionDeliveryEnvironment();
    const getDeliveryTransport = vi.fn(async () => ({ verified: false }));
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: () => environment,
      getRuntime: async () => ({
        store: {
          getCronFreshness: vi.fn(async () => "fresh" as const),
          getReleaseFenceStatus: vi.fn(async () => ({
            kind: "current" as const,
            stamp: { generation: environment.releaseGeneration, phase: environment.releasePhase },
          })),
        },
        transport: { verified: false },
        getDeliveryTransport,
        provider: { verified: false },
      }) as unknown as StudioRuntime,
    });

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual(expect.objectContaining({
      problems: expect.arrayContaining(["transport_unverified"]),
    }));
    expect(getDeliveryTransport).toHaveBeenCalledTimes(1);
  });

  it("fails closed for missing, mismatched, or invalid delivery release fences without disclosing their stamps", async () => {
    const environment = productionDeliveryEnvironment();
    const cases = [
      {
        status: { kind: "missing" as const },
        releaseFence: "missing",
        problem: "release_fence_missing",
      },
      {
        status: {
          kind: "current" as const,
          stamp: { generation: 991337, phase: "canary" as const },
        },
        releaseFence: "mismatched",
        problem: "release_fence_mismatched",
      },
      {
        status: { kind: "invalid" as const },
        releaseFence: "invalid",
        problem: "release_fence_invalid",
      },
    ] as const;

    for (const testCase of cases) {
      const getReleaseFenceStatus = vi.fn(async () => testCase.status);
      const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
        loadEnvironment: () => environment,
        getRuntime: async () => ({
          store: {
            getCronFreshness: vi.fn(async () => "fresh" as const),
            getReleaseFenceStatus,
          },
          transport: { verified: true },
          provider: { verified: false },
        }) as unknown as StudioRuntime,
      });

      expect(response.status, testCase.releaseFence).toBe(503);
      const body = await response.json() as Record<string, unknown>;
      expect(body).toEqual(expect.objectContaining({
        ok: false,
        redis: "reachable",
        cron: "fresh",
        releaseFence: testCase.releaseFence,
        problems: expect.arrayContaining([testCase.problem]),
      }));
      expect(body).not.toHaveProperty("stamp");
      expect(body).not.toHaveProperty("releaseGeneration");
      expect(body).not.toHaveProperty("releaseFenceGeneration");
      expect(JSON.stringify(body)).not.toContain("991337");
      expect(JSON.stringify(body)).not.toContain("\"canary\"");
      expect(getReleaseFenceStatus).toHaveBeenCalledTimes(1);
    }
  });

  it("fails closed when a delivery release-fence probe is unreachable without disclosing its internals", async () => {
    const environment = productionDeliveryEnvironment();
    const getReleaseFenceStatus = vi.fn(async () => Promise.reject(new Error("redis unavailable")));
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: () => environment,
      getRuntime: async () => ({
        store: {
          getCronFreshness: vi.fn(async () => "fresh" as const),
          getReleaseFenceStatus,
        },
        transport: { verified: true },
        provider: { verified: false },
      }) as unknown as StudioRuntime,
    });

    expect(response.status).toBe(503);
    const body = await response.json() as Record<string, unknown>;
    expect(body).toEqual(expect.objectContaining({
      ok: false,
      redis: "reachable",
      cron: "fresh",
      releaseFence: "unreachable",
      problems: expect.arrayContaining(["release_fence_unavailable"]),
    }));
    expect(body).not.toHaveProperty("stamp");
    expect(body).not.toHaveProperty("releaseGeneration");
    expect(body).not.toHaveProperty("releaseFenceGeneration");
    expect(getReleaseFenceStatus).toHaveBeenCalledTimes(1);
  });

  it("reports a stale production Cron heartbeat without exposing its timestamp", async () => {
    const environment = { ...darkEnvironment(), deploymentMode: "production" as const };
    const response = await handleStudioReadiness(authorizedRequest("/internal/readiness", "GET"), {
      loadEnvironment: () => environment,
      getRuntime: async () => readinessRuntime("stale"),
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
        getRuntime: async () => readinessRuntime(freshness),
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

  it("reads only the secret-free release-fence epoch with its dedicated GET contract", async () => {
    const getReleaseFenceStatus = vi.fn(async () => ({
      kind: "current" as const,
      stamp: { generation: 4, phase: "production" as const },
    }));
    const response = await handleStudioReleaseFence(releaseFenceRequest(undefined, "GET"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => ({ store: { getReleaseFenceStatus } }) as unknown as StudioRuntime,
    });

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      kind: "current",
      stamp: { generation: 4, phase: "production" },
    });
    expect(getReleaseFenceStatus).toHaveBeenCalledTimes(1);

    const unauthorized = await handleStudioReleaseFence(releaseFenceRequest(undefined, "GET", "wrong-secret"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => ({}) as StudioRuntime,
    });
    expect(unauthorized.status).toBe(401);
  });

  it("rejects unsupported release-fence methods before loading a runtime", async () => {
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);
    const response = await handleStudioReleaseFence(releaseFenceRequest(undefined, "POST"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime,
    });
    expect(response.status).toBe(409);

    const unsupported = await handleStudioReleaseFence(new Request("https://air.example.test/internal/release-fence", {
      method: "PUT",
    }), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime,
    });
    expect(unsupported.status).toBe(405);
    expect(unsupported.headers.get("allow")).toBe("GET, POST");
    expect(getRuntime).not.toHaveBeenCalled();
  });

  it("keeps FIFO migration authority independent from Cron and returns only secret-free status", async () => {
    const getInboxFifoMigrationStatus = vi.fn(async () => ({
      kind: "current" as const,
      phase: "holding" as const,
      holdingScopes: 2,
    }));
    const getRuntime = vi.fn(async () => ({
      store: { getInboxFifoMigrationStatus },
    }) as unknown as StudioRuntime);

    const cronCredential = await handleStudioInboxFifoMigration(
      inboxFifoMigrationRequest("GET", undefined, "cron-secret"),
      { loadEnvironment: releaseFenceEnvironment, getRuntime },
    );
    expect(cronCredential.status).toBe(401);
    expect(await cronCredential.text()).toBe("");
    expect(getRuntime).not.toHaveBeenCalled();

    const response = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("GET"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime,
    });
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({
      kind: "current",
      phase: "holding",
      holdingScopes: 2,
    });
    expect(getInboxFifoMigrationStatus).toHaveBeenCalledTimes(1);
  });

  it("rejects all unsupported or nonliteral FIFO migration actions before creating a runtime", async () => {
    const loadEnvironment = vi.fn(releaseFenceEnvironment);
    const getRuntime = vi.fn(async () => ({}) as StudioRuntime);

    const method = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("PUT", "begin"), {
      loadEnvironment,
      getRuntime,
    });
    expect(method.status).toBe(405);
    expect(method.headers.get("allow")).toBe("GET, POST");
    expect(loadEnvironment).not.toHaveBeenCalled();

    // Fetch's Headers constructor correctly applies HTTP OWS normalization,
    // so this checks exact post-normalization control verbs rather than
    // attempting to distinguish wire representations HTTP treats as equal.
    for (const action of [undefined, "BEGIN", "begin,prepare", "force", "skip", "reset"]) {
      const response = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("POST", action), {
        loadEnvironment,
        getRuntime,
      });
      expect(response.status, `action=${String(action)}`).toBe(400);
      expect(await response.text()).toBe("");
    }
    expect(getRuntime).not.toHaveBeenCalled();
  });

  it("dispatches each exact FIFO migration action to its one reviewed store primitive", async () => {
    const getInboxFifoMigrationStatus = vi.fn();
    const beginInboxFifoMigration = vi.fn(async () => ({ kind: "holding" as const }));
    const prepareInboxFifoHoldingScopes = vi.fn(async () => ({
      kind: "prepared" as const,
      preparedScopes: 3,
      remainingScopes: 1,
    }));
    const activateInboxFifoMigration = vi.fn(async () => ({ kind: "activated" as const }));
    const runtime = {
      store: {
        getInboxFifoMigrationStatus,
        beginInboxFifoMigration,
        prepareInboxFifoHoldingScopes,
        activateInboxFifoMigration,
      },
    } as unknown as StudioRuntime;

    const begin = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("POST", "begin"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => runtime,
    });
    expect(begin.status).toBe(202);
    await expect(begin.json()).resolves.toEqual({ kind: "holding" });

    const prepare = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("POST", "prepare"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => runtime,
    });
    expect(prepare.status).toBe(202);
    await expect(prepare.json()).resolves.toEqual({ kind: "prepared", preparedScopes: 3, remainingScopes: 1 });

    const activate = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("POST", "activate"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => runtime,
    });
    expect(activate.status).toBe(202);
    await expect(activate.json()).resolves.toEqual({ kind: "activated" });

    expect(getInboxFifoMigrationStatus).not.toHaveBeenCalled();
    expect(beginInboxFifoMigration).toHaveBeenCalledTimes(1);
    expect(prepareInboxFifoHoldingScopes).toHaveBeenCalledTimes(1);
    expect(activateInboxFifoMigration).toHaveBeenCalledTimes(1);
  });

  it("exposes only safe blocked or invalid FIFO migration outcomes", async () => {
    const beginInboxFifoMigration = vi.fn(async () => ({ kind: "legacy_not_drained" as const }));
    const prepareInboxFifoHoldingScopes = vi.fn(async () => ({ kind: "blocked" as const, preparedScopes: 0 }));
    const activateInboxFifoMigration = vi.fn(async () => ({ kind: "invalid" as const }));
    const runtime = {
      store: { beginInboxFifoMigration, prepareInboxFifoHoldingScopes, activateInboxFifoMigration },
    } as unknown as StudioRuntime;

    const blockedBegin = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("POST", "begin"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => runtime,
    });
    expect(blockedBegin.status).toBe(409);
    await expect(blockedBegin.json()).resolves.toEqual({ kind: "legacy_not_drained" });

    const blockedPrepare = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("POST", "prepare"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => runtime,
    });
    expect(blockedPrepare.status).toBe(409);
    await expect(blockedPrepare.json()).resolves.toEqual({ kind: "blocked", preparedScopes: 0 });

    const invalidActivate = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("POST", "activate"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => runtime,
    });
    expect(invalidActivate.status).toBe(503);
    await expect(invalidActivate.json()).resolves.toEqual({ kind: "invalid" });
  });

  it("returns a safe unavailable response for an invalid persisted FIFO migration status", async () => {
    const response = await handleStudioInboxFifoMigration(inboxFifoMigrationRequest("GET"), {
      loadEnvironment: releaseFenceEnvironment,
      getRuntime: async () => ({
        store: { getInboxFifoMigrationStatus: async () => ({ kind: "invalid" as const }) },
      }) as unknown as StudioRuntime,
    });

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({ kind: "invalid" });
  });
});
