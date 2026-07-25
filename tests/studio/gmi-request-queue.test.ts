import { describe, expect, it, vi } from "vitest";
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

  it("submits the documented { model, payload } shape and accepts only an exact model-bound request id/status", async () => {
    let request: Request | undefined;
    const service = client(async (input, init) => {
      request = new Request(input, init);
      return json({
        model: "Kling-Text2Video-V2.1-Master",
        request_id: "gmi-request-123",
        status: "dispatched",
      }, 202);
    });

    await expect(service.submitRequest({
      model: "Kling-Text2Video-V2.1-Master",
      payload: { prompt: "A private fixture prompt", durationSeconds: "5" },
    })).resolves.toEqual({
      classification: "accepted",
      model: "Kling-Text2Video-V2.1-Master",
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

  it("keeps 2xx submissions ambiguous when the response omits or mismatches the requested model", async () => {
    const input = { model: "flux-kontext-pro", payload: { prompt: "fixture" } } as const;
    const responses = [
      { request_id: "gmi-request-123", status: "dispatched" },
      { model: "other-model", request_id: "gmi-request-123", status: "dispatched" },
    ] as const;

    for (const response of responses) {
      const service = client(async () => json(response, 202));
      await expect(service.submit(input)).resolves.toEqual({
        classification: "ambiguous",
        reason: "malformed_response",
      });
    }
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

  it("allows recovery after rollback closes admissions and submissions", async () => {
    let calls = 0;
    const service = client(async () => {
      calls += 1;
      return json({ request_id: "gmi-request-123", status: "success" });
    }, {
      sideEffectEnvironment: {
        ...ACTIVE_PROVIDER_POLICY,
        policy: {
          ...ACTIVE_PROVIDER_POLICY.policy,
          acceptNewJobs: false,
          allowProviderSubmit: false,
          allowProviderRecovery: true,
        },
      },
    });

    await expect(service.submit({ model: "Veo3", payload: { prompt: "fixture" } })).rejects.toMatchObject({
      code: "provider_submission_disabled",
    });
    await expect(service.createUploadUrl({ fileType: "png", storage })).rejects.toMatchObject({
      code: "provider_submission_disabled",
    });
    await expect(service.getRequestStatus("gmi-request-123")).resolves.toMatchObject({
      requestId: "gmi-request-123",
      status: "success",
    });
    expect(calls).toBe(1);
  });

  it("keeps submission, upload preparation, and recovery off without explicit positive-budget policy", async () => {
    let calls = 0;
    const fetch = async (): Promise<Response> => {
      calls += 1;
      return json({ request_id: "must-not-be-called", status: "success" });
    };
    const blockedPolicies = [
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

  it("keeps the request budget through a trickling JSON body and cancels it without exposing provider data", async () => {
    vi.useFakeTimers();
    try {
      const encoder = new TextEncoder();
      let beginReading!: () => void;
      const reading = new Promise<void>((resolve) => {
        beginReading = resolve;
      });
      let cancelled = false;
      let cancellationReason: unknown;
      const privateProviderFragment = "private-provider-fragment-must-not-leak";
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          controller.enqueue(encoder.encode('{"models":['));
          beginReading();
          setTimeout(() => {
            controller.enqueue(encoder.encode(`"${privateProviderFragment}",`));
          }, 50);
          // The source never closes. Its slow continuation must be bounded by
          // the request timer rather than holding the worker indefinitely.
          return new Promise<void>(() => undefined);
        },
        cancel(reason) {
          cancelled = true;
          cancellationReason = reason;
        },
      }, { highWaterMark: 0 });
      const service = client(async () => new Response(body, {
        status: 200,
        headers: { "content-type": "application/json" },
      }), { timeoutMs: 250 });

      const pending = service.listModels();
      const timeout = expect(pending).rejects.toMatchObject({
        name: "GmiRequestQueueError",
        code: "request_timeout",
        retryable: true,
      });
      await reading;
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(150);

      await timeout;
      expect(cancelled).toBe(true);
      expect(String(cancellationReason)).not.toContain(privateProviderFragment);

      try {
        await pending;
      } catch (error) {
        expect((error as Error).message).toBe("gmi_request_queue_request_timeout");
        expect((error as Error).message).not.toContain(privateProviderFragment);
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not wait for an uncooperative provider body cleanup on early rejection", async () => {
    const neverSettlingBody = (chunk?: Uint8Array): ReadableStream<Uint8Array> => new ReadableStream<Uint8Array>({
      start(controller) {
        if (chunk) controller.enqueue(chunk);
      },
      cancel() {
        return new Promise<void>(() => undefined);
      },
    });
    const encoder = new TextEncoder();
    const unexpectedStatus = client(async () => new Response(neverSettlingBody(), {
      status: 503,
      headers: { "content-type": "application/json" },
    }));
    const invalidContentType = client(async () => new Response(neverSettlingBody(), {
      status: 200,
      headers: { "content-type": "text/plain" },
    }));
    const invalidContentLength = client(async () => new Response(neverSettlingBody(), {
      status: 200,
      headers: { "content-type": "application/json", "content-length": "2048" },
    }), { maxResponseBytes: 1_024 });
    const oversizedBody = client(async () => new Response(neverSettlingBody(encoder.encode("x".repeat(1_025))), {
      status: 200,
      headers: { "content-type": "application/json" },
    }), { maxResponseBytes: 1_024 });

    await expect(unexpectedStatus.listModels()).rejects.toMatchObject({
      code: "unexpected_status",
      retryable: true,
      httpStatus: 503,
    });
    await expect(invalidContentType.listModels()).rejects.toMatchObject({ code: "invalid_response" });
    await expect(invalidContentLength.listModels()).rejects.toMatchObject({ code: "invalid_response" });
    await expect(oversizedBody.listModels()).rejects.toMatchObject({ code: "invalid_response" });
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
