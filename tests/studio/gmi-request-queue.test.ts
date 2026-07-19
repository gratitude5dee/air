import { describe, expect, it } from "vitest";
import {
  createGmiRequestQueueClient,
  createGmiRequestQueueClientFromEnv,
  GmiRequestQueueError,
} from "../../lib/studio/gmi-request-queue.js";
import type { StudioPaidProviderEnvironment } from "../../lib/studio/config.js";

const ACTIVE_PROVIDER_POLICY = {
  releasePhase: "production",
  policy: {
    acceptNewJobs: true,
    allowProviderSubmit: true,
    allowProviderRecovery: true,
    deliveryEnabled: true,
  },
  policyExplicit: true,
  spendCaps: {
    perJobCents: 100,
    dailyCents: 100,
    canaryTotalCents: 100,
    canaryFalCents: 0,
  },
} satisfies StudioPaidProviderEnvironment;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function client(
  fetch: typeof globalThis.fetch,
  options?: {
    readonly maxResponseBytes?: number;
    readonly timeoutMs?: number;
    readonly sideEffectEnvironment?: StudioPaidProviderEnvironment;
  },
): ReturnType<typeof createGmiRequestQueueClient> {
  return createGmiRequestQueueClient({
    apiKey: "gmi-test-secret",
    organizationId: "org_test",
    fetch,
    sideEffectEnvironment: options?.sideEffectEnvironment ?? ACTIVE_PROVIDER_POLICY,
    ...options,
  });
}

const storage = {
  host: "storage.googleapis.com",
  pathPrefix: "/gmi-bucket/air/",
} as const;

describe("GMI request-queue client", () => {
  it("uses only the fixed request-queue API with private bearer and optional organization headers", async () => {
    const requests: Request[] = [];
    const service = client(async (input, init) => {
      requests.push(new Request(input, init));
      return json({ models: [] });
    });

    await expect(service.listModels()).resolves.toEqual({ models: [] });

    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe("https://console.gmicloud.ai/api/v1/ie/requestqueue/apikey/models");
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer gmi-test-secret");
    expect(requests[0]?.headers.get("x-organization-id")).toBe("org_test");
  });

  it("reads a model detail through the encoded fixed endpoint without enabling it", async () => {
    let request: Request | undefined;
    const service = client(async (input, init) => {
      request = new Request(input, init);
      return json({ model_id: "provider/model" });
    });

    await expect(service.getModelDetail("provider/model")).resolves.toEqual({ model_id: "provider/model" });

    expect(request?.url).toBe("https://console.gmicloud.ai/api/v1/ie/requestqueue/apikey/models/provider%2Fmodel");
    expect(request?.method).toBe("GET");
  });

  it("submits the documented { model, payload } shape and accepts only a valid request id", async () => {
    let request: Request | undefined;
    const service = client(async (input, init) => {
      request = new Request(input, init);
      return json({ request_id: "gmi-request-123", status: "dispatched" }, 202);
    });

    await expect(service.submitRequest({
      model: "Kling-Text2Video-V2.1-Master",
      payload: { prompt: "A private fixture prompt", durationSeconds: "5" },
    })).resolves.toEqual({
      classification: "accepted",
      requestId: "gmi-request-123",
      status: "dispatched",
    });

    expect(request?.url).toBe("https://console.gmicloud.ai/api/v1/ie/requestqueue/apikey/requests");
    expect(request?.headers.get("content-type")).toBe("application/json");
    expect(await request?.json()).toEqual({
      model: "Kling-Text2Video-V2.1-Master",
      payload: { prompt: "A private fixture prompt", durationSeconds: "5" },
    });
  });

  it("requires both an active release phase and the provider recovery switch before provider work", async () => {
    let calls = 0;
    const fetch = async (): Promise<Response> => {
      calls += 1;
      return json({ request_id: "gmi-request-123", status: "success" });
    };
    const dark = client(fetch, {
      sideEffectEnvironment: {
        ...ACTIVE_PROVIDER_POLICY,
        releasePhase: "dark",
      },
    });
    const recoveryDisabled = client(fetch, {
      sideEffectEnvironment: {
        ...ACTIVE_PROVIDER_POLICY,
        policy: { ...ACTIVE_PROVIDER_POLICY.policy, allowProviderRecovery: false },
      },
    });

    await expect(dark.submit({ model: "Veo3", payload: { prompt: "fixture" } })).rejects.toMatchObject({
      code: "provider_submission_disabled",
    });
    await expect(dark.getRequestStatus("gmi-request-123")).rejects.toMatchObject({
      code: "provider_recovery_disabled",
    });
    await expect(recoveryDisabled.getRequest("gmi-request-123")).rejects.toMatchObject({
      code: "provider_recovery_disabled",
    });
    expect(calls).toBe(0);
  });

  it("keeps submission, upload preparation, and recovery off without explicit accepted positive-budget policy", async () => {
    let calls = 0;
    const fetch = async (): Promise<Response> => {
      calls += 1;
      return json({ request_id: "must-not-be-called", status: "success" });
    };
    const blockedPolicies = [
      {
        ...ACTIVE_PROVIDER_POLICY,
        policy: { ...ACTIVE_PROVIDER_POLICY.policy, acceptNewJobs: false },
      },
      {
        ...ACTIVE_PROVIDER_POLICY,
        policyExplicit: false,
      },
      {
        ...ACTIVE_PROVIDER_POLICY,
        spendCaps: { ...ACTIVE_PROVIDER_POLICY.spendCaps, perJobCents: 0 },
      },
      {
        ...ACTIVE_PROVIDER_POLICY,
        spendCaps: { ...ACTIVE_PROVIDER_POLICY.spendCaps, dailyCents: 0 },
      },
    ] as const;

    for (const sideEffectEnvironment of blockedPolicies) {
      const service = client(fetch, { sideEffectEnvironment });
      await expect(service.submit({ model: "Veo3", payload: { prompt: "fixture" } })).rejects.toMatchObject({
        code: "provider_submission_disabled",
      });
      await expect(service.createUploadUrl({ fileType: "png", storage })).rejects.toMatchObject({
        code: "provider_submission_disabled",
      });
      await expect(service.getRequestStatus("must-not-be-called")).rejects.toMatchObject({
        code: "provider_recovery_disabled",
      });
    }
    expect(calls).toBe(0);
  });

  it("classifies only provider-proven non-rate-limit 4xx responses as definitive rejections", async () => {
    const service = client(async () => new Response("do not inspect this provider body", {
      status: 422,
      headers: { "content-type": "text/plain" },
    }));

    await expect(service.submit({ model: "Veo3", payload: { prompt: "fixture" } })).resolves.toEqual({
      classification: "definitive_rejection",
      httpStatus: 422,
    });
  });

  it("keeps 429, 5xx, disconnects, timeouts, and malformed 2xx bodies ambiguous", async () => {
    const rateLimited = client(async () => json({ ignored: "provider body" }, 429));
    const unavailable = client(async () => json({ ignored: "provider body" }, 503));
    const disconnected = client(async () => {
      throw new TypeError("socket reset with private provider detail");
    });
    const malformed = client(async () => new Response("not-json-private-provider-body", {
      status: 200,
      headers: { "content-type": "text/plain" },
    }));
    const timedOut = client(async (_input, init) => new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
    }), { timeoutMs: 250 });

    const input = { model: "Veo3", payload: { prompt: "fixture" } } as const;
    await expect(rateLimited.submit(input)).resolves.toEqual({
      classification: "ambiguous",
      reason: "rate_limited",
      httpStatus: 429,
    });
    await expect(unavailable.submit(input)).resolves.toEqual({
      classification: "ambiguous",
      reason: "server_error",
      httpStatus: 503,
    });
    await expect(disconnected.submit(input)).resolves.toEqual({
      classification: "ambiguous",
      reason: "transport_failure",
    });
    await expect(malformed.submit(input)).resolves.toEqual({
      classification: "ambiguous",
      reason: "malformed_response",
    });
    await expect(timedOut.submit(input)).resolves.toEqual({
      classification: "ambiguous",
      reason: "timeout",
    });
  });

  it("treats an oversized JSON response as malformed without preserving its body", async () => {
    const privateBody = "x".repeat(2_000);
    const service = client(async () => json({ request_id: "gmi-request-123", status: "dispatched", privateBody }), {
      maxResponseBytes: 1_024,
    });

    await expect(service.submit({ model: "Veo3", payload: { prompt: "fixture" } })).resolves.toEqual({
      classification: "ambiguous",
      reason: "malformed_response",
    });
  });

  it("normalizes a request status only when its request id matches", async () => {
    const service = client(async () => json({
      request_id: "gmi-request-123",
      status: "canceled",
      output: { url: "not-logged-here" },
    }));

    await expect(service.getRequest("gmi-request-123")).resolves.toEqual({
      requestId: "gmi-request-123",
      status: "cancelled",
      response: {
        request_id: "gmi-request-123",
        status: "canceled",
        output: { url: "not-logged-here" },
      },
    });

    const mismatched = client(async () => json({ request_id: "other-request", status: "success" }));
    await expect(mismatched.getRequestStatus("gmi-request-123")).rejects.toMatchObject({
      name: "GmiRequestQueueError",
      code: "invalid_response",
    });
  });

  it("returns upload URLs only when both exact Google Storage host and path prefix match", async () => {
    let request: Request | undefined;
    const service = client(async (input, init) => {
      request = new Request(input, init);
      return json({
        upload_url: "https://storage.googleapis.com/gmi-bucket/air/uploads/frame.jpg?X-Goog-Signature=fixture",
        public_url: "https://storage.googleapis.com/gmi-bucket/air/public/frame.jpg",
      }, 201);
    });

    await expect(service.createUploadUrl({ fileType: "jpeg", storage, isPublic: false })).resolves.toEqual({
      uploadUrl: "https://storage.googleapis.com/gmi-bucket/air/uploads/frame.jpg?X-Goog-Signature=fixture",
      publicUrl: "https://storage.googleapis.com/gmi-bucket/air/public/frame.jpg",
    });
    expect(request?.url).toBe("https://console.gmicloud.ai/api/v1/ie/requestqueue/apikey/upload-url");
    expect(await request?.json()).toEqual({ file_type: "jpeg", is_public: false });
  });

  it("rejects bare Google Storage and prefix lookalike upload URLs", async () => {
    const bareDomain = client(async () => json({
      upload_url: "https://storage.googleapis.com",
      public_url: "https://storage.googleapis.com/gmi-bucket/air/public/frame.jpg",
    }));
    const lookalikePrefix = client(async () => json({
      upload_url: "https://storage.googleapis.com/gmi-bucket/air-evil/uploads/frame.jpg",
      public_url: "https://storage.googleapis.com/gmi-bucket/air/public/frame.jpg",
    }));

    await expect(bareDomain.createUploadUrl({ fileType: "jpeg", storage })).rejects.toMatchObject({
      code: "invalid_response",
    });
    await expect(lookalikePrefix.createUploadUrl({ fileType: "jpeg", storage })).rejects.toMatchObject({
      code: "invalid_response",
    });
  });

  it("reads GMI_API_KEY with a genuinely optional GMI_ORG_ID and keeps configuration errors generic", async () => {
    let request: Request | undefined;
    const noOrganization = createGmiRequestQueueClientFromEnv(
      { GMI_API_KEY: "from-environment-key", GMI_ORG_ID: "" },
      {
        fetch: async (input, init) => {
          request = new Request(input, init);
          return json({ models: [] });
        },
      },
    );

    await noOrganization.listModels();
    expect(request?.headers.get("authorization")).toBe("Bearer from-environment-key");
    expect(request?.headers.get("x-organization-id")).toBeNull();

    try {
      createGmiRequestQueueClient({ apiKey: "secret-that-must-not-leak\nattacker" });
      throw new Error("expected configuration failure");
    } catch (error) {
      expect(error).toBeInstanceOf(GmiRequestQueueError);
      expect((error as Error).message).not.toContain("secret-that-must-not-leak");
    }
  });
});
