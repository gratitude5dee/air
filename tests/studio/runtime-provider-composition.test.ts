import { describe, expect, it, vi } from "vitest";

import type { StudioEnvironment } from "../../lib/studio/config.js";
import {
  GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY,
  GMI_FLUX_KONTEXT_PRO_MODEL_ID,
  GmiFluxKontextProStudioProvider,
} from "../../lib/studio/gmi-flux-kontext-pro-provider.js";
import { AIR_MODEL_REGISTRY, validateModelRegistry, type AirModelRegistry } from "../../lib/studio/model-registry.js";
import { UnprovisionedStudioProvider, type StudioProviderPort } from "../../lib/studio/provider-port.js";
import { createConfiguredStudioProvider } from "../../lib/studio/runtime.js";
import { parseAllowedSenderHashes } from "../../lib/security.js";

const FINGERPRINT = `sha256:${"a".repeat(64)}`;
const SENDER_HASH = parseAllowedSenderHashes("b".repeat(64))[0]!;

function environment(overrides: Partial<StudioEnvironment> = {}): StudioEnvironment {
  const base: StudioEnvironment = {
    deploymentMode: "production",
    releasePhase: "canary",
    releaseGeneration: 1,
    releaseFenceSecret: "release-fence",
    storageTransport: "rest",
    gmiApiKey: "gmi-api-key",
    idHashSecret: "identity-secret",
    queueEncryptionKey: Buffer.alloc(32, 9).toString("base64"),
    allowedSenderHashes: [SENDER_HASH],
    cronSecret: "cron-secret",
    publicBaseUrl: "https://air.example.test",
    modelRegistryVersion: "2.1.0",
    dailyUserJobLimit: 3,
    activeJobLimit: 1,
    canaryBudgetId: "approved-budget",
    policy: {
      acceptNewJobs: true,
      allowProviderSubmit: true,
      allowProviderRecovery: true,
      deliveryEnabled: true,
    },
    policyExplicit: true,
    spendCaps: {
      perJobCents: 100,
      dailyCents: 10_000,
      canaryTotalCents: 10_000,
      canaryFalCents: 0,
    },
  };
  return {
    ...base,
    ...overrides,
    policy: { ...base.policy, ...overrides.policy },
    spendCaps: { ...base.spendCaps, ...overrides.spendCaps },
  };
}

function reviewedFluxModel() {
  return {
    displayName: "Flux Kontext Pro",
    slug: "gmi-flux-kontext-pro",
    mode: "imagine",
    provider: "gmi",
    providerModelId: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
    providerBinding: {
      adapterKey: GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY,
      requestSchemaFingerprint: FINGERPRINT,
      resultSchemaFingerprint: FINGERPRINT,
    },
    verification: {
      verifiedAt: "2026-07-23T00:00:00Z",
      catalogFingerprint: FINGERPRINT,
      schemaFingerprint: FINGERPRINT,
    },
    input: {
      prompt: "required",
      image: false,
      video: false,
      mediaOnly: false,
    },
    inputHosting: { mode: "data-uri" },
    output: {
      type: "image",
      maximumOutputCount: 1,
      supportedOutputCounts: [1],
      supportedDurationSeconds: [],
      lockedAspectRatio: "provider-default",
    },
    outputRetrieval: {
      // This intentionally differs from the earlier speculative GCS path:
      // composition must use a registry-reviewed contract, not hard-code a
      // provider-storage location before the authenticated catalog audit.
      host: "private-results.example.test",
      pathPrefix: "/reviewed/air-output/",
      access: "signed-url",
      publiclyAccessible: false,
      verifiedAt: "2026-07-23T00:00:00Z",
      evidenceFingerprint: FINGERPRINT,
    },
    requiredPayloadFields: [{ name: "prompt", declaredType: "string", required: true }],
    pricing: {
      source: "https://docs.gmicloud.ai/inference-engine/billing/price",
      currency: "USD",
      verifiedAt: "2026-07-23T00:00:00Z",
      evidenceFingerprint: FINGERPRINT,
      quotes: [{ count: 1, durationSeconds: null, maximumQuotedCostCents: 100 }],
    },
    polling: {
      requestTimeoutMs: 12_000,
      pollIntervalMs: 1_000,
      maxPollAttempts: 30,
      deadlineMs: 60_000,
    },
    status: "enabled",
    hostedPublicInputAcknowledged: false,
  };
}

function reviewedFluxRegistry(
  mutate?: (model: ReturnType<typeof reviewedFluxModel>) => unknown,
  defaults: Record<string, string> = { imagine: "gmi-flux-kontext-pro" },
): AirModelRegistry {
  const model = reviewedFluxModel();
  return validateModelRegistry({
    schemaVersion: "air-model-registry/v1",
    registryVersion: "2.1.0",
    models: [mutate === undefined ? model : mutate(model)],
    defaults,
  });
}

describe("strict GMI runtime composition", () => {
  it("keeps the committed empty registry unprovisioned even when a GMI key exists", () => {
    const provider = createConfiguredStudioProvider(environment(), AIR_MODEL_REGISTRY);

    expect(provider).toBeInstanceOf(UnprovisionedStudioProvider);
    expect(provider.verified).toBe(false);
  });

  it("constructs the real adapter only for the exact reviewed Flux default and does not call a provider", () => {
    const fetch = vi.fn<typeof globalThis.fetch>();
    vi.stubGlobal("fetch", fetch);
    try {
      const provider = createConfiguredStudioProvider(environment(), reviewedFluxRegistry());

      expect(provider).toBeInstanceOf(GmiFluxKontextProStudioProvider);
      expect(provider.verified).toBe(true);
      expect(fetch).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("fails closed for a missing/mismatched registry version, missing key, absent default, and every reviewed-contract mismatch", () => {
    const noDefault = reviewedFluxRegistry(undefined, {});
    const mismatchedRegistries = [
      reviewedFluxRegistry((model) => ({
        ...model,
        provider: "fal",
        providerBinding: { ...model.providerBinding, adapterKey: "fal.flux-kontext-pro.v1" },
      })),
      reviewedFluxRegistry((model) => ({ ...model, providerModelId: "flux-kontext-pro-v2" })),
      reviewedFluxRegistry((model) => ({
        ...model,
        providerBinding: { ...model.providerBinding, adapterKey: "gmi.flux-kontext-pro.v2" },
      })),
      reviewedFluxRegistry((model) => ({
        ...model,
        input: { ...model.input, image: true },
      })),
      reviewedFluxRegistry((model) => ({
        ...model,
        output: { ...model.output, maximumOutputCount: 2, supportedOutputCounts: [1, 2] },
        pricing: {
          ...model.pricing,
          quotes: [...model.pricing.quotes, { count: 2, durationSeconds: null, maximumQuotedCostCents: 200 }],
        },
      })),
      reviewedFluxRegistry((model) => ({
        ...model,
        outputRetrieval: { ...model.outputRetrieval, access: "provider-private" },
      })),
    ];

    for (const modelRegistryVersion of [undefined, "2.1.1"] as const) {
      expect(createConfiguredStudioProvider(environment({ modelRegistryVersion }), reviewedFluxRegistry()))
        .toBeInstanceOf(UnprovisionedStudioProvider);
    }
    expect(createConfiguredStudioProvider(environment({ gmiApiKey: undefined }), reviewedFluxRegistry()))
      .toBeInstanceOf(UnprovisionedStudioProvider);
    expect(createConfiguredStudioProvider(environment(), noDefault)).toBeInstanceOf(UnprovisionedStudioProvider);
    for (const registry of mismatchedRegistries) {
      expect(createConfiguredStudioProvider(environment(), registry)).toBeInstanceOf(UnprovisionedStudioProvider);
    }
  });

  it("accepts a different validated private signed-output host and path without inventing one", () => {
    const registry = reviewedFluxRegistry((model) => ({
      ...model,
      outputRetrieval: {
        ...model.outputRetrieval,
        host: "catalog-reviewed.example.test",
        pathPrefix: "/private/flux-output/",
      },
    }));

    const provider = createConfiguredStudioProvider(environment(), registry);

    expect(provider).toBeInstanceOf(GmiFluxKontextProStudioProvider);
  });

  it("preserves an explicitly injected provider without composing GMI", () => {
    const injected = {
      verified: true,
      submit: vi.fn(async () => ({ kind: "definitive_rejection" as const })),
    } satisfies StudioProviderPort;

    const provider = createConfiguredStudioProvider(
      environment({ gmiApiKey: undefined }),
      AIR_MODEL_REGISTRY,
      injected,
    );

    expect(provider).toBe(injected);
    expect(injected.submit).not.toHaveBeenCalled();
  });
});
