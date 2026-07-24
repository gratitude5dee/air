import { describe, expect, it, vi } from "vitest";

import type { GmiRequestQueueClient, GmiSubmitResult } from "../../lib/studio/gmi-request-queue.js";
import {
  GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY,
  GMI_FLUX_KONTEXT_PRO_MODEL_ID,
  GmiFluxKontextProStudioProvider,
  type GmiFluxKontextProRequestQueueClient,
} from "../../lib/studio/gmi-flux-kontext-pro-provider.js";
import type {
  StudioProviderObservationInput,
  StudioProviderSubmitInput,
} from "../../lib/studio/provider-port.js";

const FINGERPRINT = `sha256:${"a".repeat(64)}`;
const SUBMISSION_INTENT_ID = "b".repeat(64);

function submitInput(overrides: Partial<StudioProviderSubmitInput> = {}): StudioProviderSubmitInput {
  const base: StudioProviderSubmitInput = {
    job: {
      jobId: "direct-flux-job-001",
      mode: "imagine",
      modelSlug: "gmi-flux-kontext-pro",
      provider: "gmi",
      outputCount: 1,
      reservedCents: 5,
    },
    snapshot: {
      schemaVersion: 2,
      workflow: {
        workflowId: "air:imagine:v1",
        workflowVersion: 1,
        stepId: "provider-generation",
      },
      model: {
        slug: "gmi-flux-kontext-pro",
        provider: "gmi",
        providerModelId: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
        adapterKey: GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY,
        catalogFingerprint: FINGERPRINT,
        requestSchemaFingerprint: FINGERPRINT,
        resultSchemaFingerprint: FINGERPRINT,
        pricingEvidenceFingerprint: FINGERPRINT,
      },
      output: { type: "image", count: 1 },
      maximumQuotedCostCents: 5,
      polling: {
        requestTimeoutMs: 5_000,
        pollIntervalMs: 1_000,
        maxPollAttempts: 3,
        deadlineMs: 15_000,
      },
      releaseStamp: { generation: 1, phase: "canary" },
    },
    submissionIntentId: SUBMISSION_INTENT_ID,
    request: { prompt: "  a translucent glass orchard at sunrise  " },
  };

  return {
    ...base,
    ...overrides,
    job: { ...base.job, ...overrides.job },
    snapshot: {
      ...base.snapshot,
      ...overrides.snapshot,
      workflow: { ...base.snapshot.workflow, ...overrides.snapshot?.workflow },
      model: { ...base.snapshot.model, ...overrides.snapshot?.model },
      output: { ...base.snapshot.output, ...overrides.snapshot?.output },
      polling: { ...base.snapshot.polling, ...overrides.snapshot?.polling },
      releaseStamp: { ...base.snapshot.releaseStamp, ...overrides.snapshot?.releaseStamp },
    },
    request: { ...base.request, ...overrides.request },
  };
}

function observationInput(
  overrides: Partial<StudioProviderObservationInput> = {},
): StudioProviderObservationInput {
  const submit = submitInput();
  return {
    job: { ...submit.job, ...overrides.job },
    snapshot: {
      ...submit.snapshot,
      ...overrides.snapshot,
      workflow: { ...submit.snapshot.workflow, ...overrides.snapshot?.workflow },
      model: { ...submit.snapshot.model, ...overrides.snapshot?.model },
      output: { ...submit.snapshot.output, ...overrides.snapshot?.output },
      polling: { ...submit.snapshot.polling, ...overrides.snapshot?.polling },
      releaseStamp: { ...submit.snapshot.releaseStamp, ...overrides.snapshot?.releaseStamp },
    },
    providerRequestId: overrides.providerRequestId ?? "gmi-flux-observation-001",
  };
}

function queueClient(result: GmiSubmitResult): GmiFluxKontextProRequestQueueClient & {
  readonly submitRequest: ReturnType<typeof vi.fn>;
} {
  const submitRequest = vi.fn(async () => result);
  return { submitRequest } as GmiFluxKontextProRequestQueueClient & { readonly submitRequest: ReturnType<typeof vi.fn> };
}

const outputPolicy = Object.freeze({
  host: "storage.googleapis.com",
  pathPrefix: "/gmi-generated-assets/air/",
  access: "signed-url" as const,
});

const gcsV4SignedQuery = [
  "X-Goog-Algorithm=GOOG4-RSA-SHA256",
  "X-Goog-Credential=air%2F20260723%2Fauto%2Fstorage%2Fgoog4_request",
  "X-Goog-Date=20260723T000000Z",
  "X-Goog-Expires=900",
  "X-Goog-SignedHeaders=host",
  `X-Goog-Signature=${"a".repeat(64)}`,
].join("&");

function signedFluxOutputUrl(path = "opaque-output.jpg"): string {
  return `https://storage.googleapis.com/gmi-generated-assets/air/${path}?${gcsV4SignedQuery}`;
}

describe("GMI Flux Kontext Pro StudioProvider", () => {
  it("uses an injected queue client for the exact prompt-only Flux request", async () => {
    const queue = queueClient({
      classification: "accepted",
      model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
      requestId: "gmi-flux-request-001",
      status: "dispatched",
    });
    const provider = new GmiFluxKontextProStudioProvider(queue);

    await expect(provider.submit(submitInput())).resolves.toEqual({
      kind: "accepted",
      providerRequestId: "gmi-flux-request-001",
    });

    expect(provider.verified).toBe(true);
    expect(queue.submitRequest).toHaveBeenCalledTimes(1);
    expect(queue.submitRequest).toHaveBeenCalledWith({
      model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
      payload: { prompt: "a translucent glass orchard at sunrise" },
    });
    // The caller's durable reconciliation ID and all unsupported provider
    // controls are intentionally absent from the documented Queue payload.
    expect(Object.keys(queue.submitRequest.mock.calls[0]?.[0]?.payload ?? {})).toEqual(["prompt"]);
  });

  it("rejects any provider, model, adapter, mode, or output binding mismatch without a provider call", async () => {
    const queue = queueClient({
      classification: "accepted",
      model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
      requestId: "must-not-be-called",
      status: "dispatched",
    });
    const provider = new GmiFluxKontextProStudioProvider(queue);
    const base = submitInput();

    const invalidInputs: readonly StudioProviderSubmitInput[] = [
      submitInput({ job: { ...base.job, provider: "fal" } }),
      submitInput({ snapshot: { ...base.snapshot, model: { ...base.snapshot.model, providerModelId: "other-model" } } }),
      submitInput({ snapshot: { ...base.snapshot, model: { ...base.snapshot.model, adapterKey: "gmi.other.v1" } } }),
      submitInput({ job: { ...base.job, mode: "animate" } }),
      submitInput({ job: { ...base.job, outputCount: 2 } }),
      submitInput({ snapshot: { ...base.snapshot, output: { ...base.snapshot.output, count: 2 } } }),
    ];

    for (const input of invalidInputs) {
      await expect(provider.submit(input)).resolves.toEqual({ kind: "definitive_rejection" });
    }
    expect(queue.submitRequest).not.toHaveBeenCalled();
  });

  it("rejects attachment-bearing, empty, and invalid durable-intent requests before the queue boundary", async () => {
    const queue = queueClient({
      classification: "accepted",
      model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
      requestId: "must-not-be-called",
      status: "dispatched",
    });
    const provider = new GmiFluxKontextProStudioProvider(queue);
    const base = submitInput();

    const withAttachment = submitInput({
      request: {
        ...base.request,
        input: {
          id: "private-attachment",
          mimeType: "image/jpeg",
          sizeBytes: 32,
        },
      },
    });
    const emptyPrompt = submitInput({ request: { prompt: "  " } });
    const invalidIntent = submitInput({ submissionIntentId: "not-a-durable-opaque-intent" });

    for (const input of [withAttachment, emptyPrompt, invalidIntent]) {
      await expect(provider.submit(input)).resolves.toEqual({ kind: "definitive_rejection" });
    }
    expect(queue.submitRequest).not.toHaveBeenCalled();
  });

  it("maps only provider-proven rejections to definitive rejection and preserves uncertainty", async () => {
    const rejected = new GmiFluxKontextProStudioProvider(queueClient({
      classification: "definitive_rejection",
      httpStatus: 422,
    }));
    const ambiguousResult = new GmiFluxKontextProStudioProvider(queueClient({
      classification: "ambiguous",
      reason: "server_error",
      httpStatus: 503,
    }));
    const malformedAccepted = new GmiFluxKontextProStudioProvider(queueClient({
      classification: "accepted",
      model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
      requestId: "invalid request id with spaces",
      status: "dispatched",
    }));
    const missingModelAccepted = new GmiFluxKontextProStudioProvider(queueClient({
      classification: "accepted",
      requestId: "gmi-flux-request-001",
      status: "dispatched",
    } as unknown as GmiSubmitResult));
    const mismatchedModelAccepted = new GmiFluxKontextProStudioProvider(queueClient({
      classification: "accepted",
      model: "other-model",
      requestId: "gmi-flux-request-001",
      status: "dispatched",
    }));
    const clientThrows = new GmiFluxKontextProStudioProvider({
      submitRequest: async () => {
        throw new Error("private provider transport detail");
      },
    } satisfies Pick<GmiRequestQueueClient, "submitRequest">);

    await expect(rejected.submit(submitInput())).resolves.toEqual({ kind: "definitive_rejection" });
    await expect(ambiguousResult.submit(submitInput())).resolves.toEqual({ kind: "ambiguous" });
    await expect(malformedAccepted.submit(submitInput())).resolves.toEqual({ kind: "ambiguous" });
    await expect(missingModelAccepted.submit(submitInput())).resolves.toEqual({ kind: "ambiguous" });
    await expect(mismatchedModelAccepted.submit(submitInput())).resolves.toEqual({ kind: "ambiguous" });
    await expect(clientThrows.submit(submitInput())).resolves.toEqual({ kind: "ambiguous" });
  });

  it("observes a pending Flux request without fetching an artifact or re-submitting", async () => {
    const getRequestStatus = vi.fn(async () => ({
      requestId: "gmi-flux-observation-001",
      status: "processing" as const,
      response: {
        request_id: "gmi-flux-observation-001",
        model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
        status: "processing",
      },
    }));
    const submitRequest = vi.fn(async () => ({
      classification: "accepted" as const,
      model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
      requestId: "not-used",
      status: "dispatched" as const,
    }));
    const fetch = vi.fn<typeof globalThis.fetch>(async () => new Response("must not fetch"));
    const provider = new GmiFluxKontextProStudioProvider(
      { submitRequest, getRequestStatus },
      { outputPolicy, fetch },
    );

    await expect(provider.observe(observationInput())).resolves.toEqual({ kind: "pending" });
    expect(getRequestStatus).toHaveBeenCalledWith("gmi-flux-observation-001");
    expect(submitRequest).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("fetches exactly one documented private Flux result through the restricted output boundary", async () => {
    const getRequestStatus = vi.fn(async () => ({
      requestId: "gmi-flux-observation-001",
      status: "success" as const,
      response: {
        request_id: "gmi-flux-observation-001",
        model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
        status: "success",
        is_public: false,
        outcome: {
          media_urls: [{
            id: "0",
            url: signedFluxOutputUrl(),
          }],
        },
      },
    }));
    let request: Request | undefined;
    const provider = new GmiFluxKontextProStudioProvider(
      {
        submitRequest: async () => ({ classification: "definitive_rejection", httpStatus: 422 }),
        getRequestStatus,
      },
      {
        outputPolicy,
        fetch: async (input, init) => {
          request = new Request(input, init);
          return new Response(new Uint8Array([0xff, 0xd8, 0xff]), {
            status: 200,
            headers: { "content-type": "image/jpeg", "content-length": "3" },
          });
        },
      },
    );

    const result = await provider.observe(observationInput());
    expect(result.kind).toBe("ready");
    if (result.kind !== "ready") throw new Error("expected Flux output");
    expect(result.artifacts).toHaveLength(1);
    expect(result.artifacts[0]).toMatchObject({
      outputIndex: 1,
      mediaType: "image",
      declaredMimeType: "image/jpeg",
    });
    expect(request?.url).toContain("/gmi-generated-assets/air/opaque-output.jpg");
    expect(request?.credentials).toBe("omit");
    expect(request?.redirect).toBe("error");
    expect(JSON.stringify(result)).not.toContain("X-Goog-Signature");
  });

  it("fences malformed, public, and off-policy Flux outcomes instead of guessing a result", async () => {
    const baseResponse = {
      request_id: "gmi-flux-observation-001",
      model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
      status: "success",
      is_public: false,
      outcome: { media_urls: [{ id: "0", url: "https://storage.googleapis.com/gmi-generated-assets/air/output.jpg" }] },
    };
    const malformedResponses = [
      baseResponse,
      { ...baseResponse, is_public: true },
      { ...baseResponse, model: "other-model" },
      { ...baseResponse, outcome: { media_urls: [] } },
      {
        ...baseResponse,
        outcome: { media_urls: [{ id: "0", url: "https://storage.googleapis.com/other/output.jpg" }] },
      },
    ] as const;

    for (const response of malformedResponses) {
      const getRequestStatus = vi.fn(async () => ({
        requestId: "gmi-flux-observation-001",
        status: "success" as const,
        response,
      }));
      const fetch = vi.fn<typeof globalThis.fetch>(async () => new Response("must not fetch"));
      const provider = new GmiFluxKontextProStudioProvider(
        { submitRequest: async () => ({ classification: "definitive_rejection", httpStatus: 422 }), getRequestStatus },
        { outputPolicy, fetch },
      );
      await expect(provider.observe(observationInput())).resolves.toEqual({ kind: "reconciliation_required" });
      expect(fetch).not.toHaveBeenCalled();
    }
  });

  it("requires exact Flux model identity before every observation state is handled", async () => {
    const states = ["processing", "success", "failed", "cancelled"] as const;
    const modelValues = [undefined, "other-model"] as const;

    for (const status of states) {
      for (const model of modelValues) {
        const getRequestStatus = vi.fn(async () => ({
          requestId: "gmi-flux-observation-001",
          status,
          response: {
            request_id: "gmi-flux-observation-001",
            status,
            ...(model === undefined ? {} : { model }),
          },
        }));
        const fetch = vi.fn<typeof globalThis.fetch>(async () => new Response("must not fetch"));
        const provider = new GmiFluxKontextProStudioProvider(
          { submitRequest: async () => ({ classification: "definitive_rejection", httpStatus: 422 }), getRequestStatus },
          { outputPolicy, fetch },
        );

        await expect(provider.observe(observationInput())).resolves.toEqual({ kind: "reconciliation_required" });
        expect(fetch).not.toHaveBeenCalled();
      }
    }
  });

  it("keeps temporary status reads retryable but terminal provider failures definitive", async () => {
    const retryable = new GmiFluxKontextProStudioProvider(
      {
        submitRequest: async () => ({ classification: "definitive_rejection", httpStatus: 422 }),
        getRequestStatus: async () => { throw new Error("private provider transport detail"); },
      },
      { outputPolicy },
    );
    const failed = new GmiFluxKontextProStudioProvider(
      {
        submitRequest: async () => ({ classification: "definitive_rejection", httpStatus: 422 }),
        getRequestStatus: async () => ({
          requestId: "gmi-flux-observation-001",
          status: "failed" as const,
          response: {
            request_id: "gmi-flux-observation-001",
            model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
            status: "failed",
          },
        }),
      },
      { outputPolicy },
    );

    await expect(retryable.observe(observationInput())).resolves.toEqual({ kind: "retryable_failure" });
    await expect(failed.observe(observationInput())).resolves.toEqual({ kind: "definitive_failure" });
  });
});
