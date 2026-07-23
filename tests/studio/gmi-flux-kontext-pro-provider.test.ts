import { describe, expect, it, vi } from "vitest";

import type { GmiRequestQueueClient, GmiSubmitResult } from "../../lib/studio/gmi-request-queue.js";
import {
  GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY,
  GMI_FLUX_KONTEXT_PRO_MODEL_ID,
  GmiFluxKontextProStudioProvider,
  type GmiFluxKontextProRequestQueueClient,
} from "../../lib/studio/gmi-flux-kontext-pro-provider.js";
import type { StudioProviderSubmitInput } from "../../lib/studio/provider-port.js";

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

function queueClient(result: GmiSubmitResult): GmiFluxKontextProRequestQueueClient & {
  readonly submitRequest: ReturnType<typeof vi.fn>;
} {
  const submitRequest = vi.fn(async () => result);
  return { submitRequest } as GmiFluxKontextProRequestQueueClient & { readonly submitRequest: ReturnType<typeof vi.fn> };
}

describe("GMI Flux Kontext Pro StudioProvider", () => {
  it("uses an injected queue client for the exact prompt-only Flux request", async () => {
    const queue = queueClient({
      classification: "accepted",
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
      requestId: "invalid request id with spaces",
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
    await expect(clientThrows.submit(submitInput())).resolves.toEqual({ kind: "ambiguous" });
  });
});
