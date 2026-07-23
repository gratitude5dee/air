import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  AIR_MODEL_REGISTRY,
  AIR_MODEL_REGISTRY_VERSION,
  ModelRegistryValidationError,
  checkModelCompatibility,
  getPaidRegistryReadiness,
  listCompatibleEnabledModels,
  listEnabledModels,
  resolveDefaultModel,
  resolveEnabledModel,
  validateModelRegistry,
  validateRegistryVersion,
} from "../../lib/studio/model-registry.js";

function sha256Fixture(value: string): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function verifiedAnimateModel(overrides: Record<string, unknown> = {}) {
  return {
    displayName: "Verified Test Video",
    slug: "verified-test-video",
    mode: "animate",
    provider: "gmi",
    providerModelId: "gmi/verified-test-video-v1",
    providerBinding: {
      adapterKey: "gmi.test-adapter.v1",
      requestSchemaFingerprint: sha256Fixture("verified-test-video-request-schema"),
      resultSchemaFingerprint: sha256Fixture("verified-test-video-result-schema"),
    },
    verification: {
      verifiedAt: "2026-07-18T00:00:00Z",
      catalogFingerprint: sha256Fixture("verified-test-video-catalog"),
      schemaFingerprint: sha256Fixture("verified-test-video-model-schema"),
    },
    input: {
      prompt: "optional",
      image: true,
      video: false,
      mediaOnly: true,
    },
    inputHosting: { mode: "data-uri" },
    output: {
      type: "video",
      maximumOutputCount: 2,
      supportedOutputCounts: [1, 2],
      supportedDurationSeconds: [5, 10],
      lockedAspectRatio: "provider-default",
    },
    requiredPayloadFields: [
      { name: "prompt", declaredType: "string", required: false },
      { name: "durationSeconds", declaredType: "string", required: true },
    ],
    pricing: {
      source: "https://catalog.example.test/pricing/verified-test-video",
      currency: "USD",
      verifiedAt: "2026-07-18T00:00:00Z",
      evidenceFingerprint: sha256Fixture("verified-test-video-pricing"),
      quotes: [
        { count: 1, durationSeconds: 5, maximumQuotedCostCents: 25 },
        { count: 1, durationSeconds: 10, maximumQuotedCostCents: 50 },
        { count: 2, durationSeconds: 5, maximumQuotedCostCents: 50 },
        { count: 2, durationSeconds: 10, maximumQuotedCostCents: 100 },
      ],
    },
    polling: {
      requestTimeoutMs: 30_000,
      pollIntervalMs: 2_000,
      maxPollAttempts: 90,
      deadlineMs: 300_000,
    },
    status: "enabled",
    hostedPublicInputAcknowledged: false,
    ...overrides,
  };
}

function registry(models: readonly Record<string, unknown>[], defaults: Record<string, string> = {}) {
  return validateModelRegistry({
    schemaVersion: "air-model-registry/v1",
    registryVersion: "2.1.0",
    models,
    defaults,
  });
}

describe("Air model registry", () => {
  it("starts as a frozen dark-deploy registry with no enabled model", () => {
    expect(AIR_MODEL_REGISTRY.registryVersion).toBe(AIR_MODEL_REGISTRY_VERSION);
    expect(AIR_MODEL_REGISTRY.models).toEqual([]);
    expect(Object.isFrozen(AIR_MODEL_REGISTRY)).toBe(true);
    expect(listEnabledModels(AIR_MODEL_REGISTRY)).toEqual([]);
    expect(resolveDefaultModel(AIR_MODEL_REGISTRY, "imagine")).toEqual({
      ok: false,
      code: "no_default_configured",
    });
    expect(getPaidRegistryReadiness(AIR_MODEL_REGISTRY)).toEqual({
      ready: false,
      missingEnabledModes: ["imagine", "animate", "edit-image", "edit-video"],
      missingDefaultModes: ["imagine", "animate", "edit-image", "edit-video"],
    });
  });

  it("requires exact version equality and rejects unknown registry fields", () => {
    expect(validateRegistryVersion("2.1.0", AIR_MODEL_REGISTRY)).toEqual({ ok: true });
    expect(validateRegistryVersion(undefined, AIR_MODEL_REGISTRY)).toEqual({
      ok: false,
      code: "missing",
      expectedVersion: "2.1.0",
    });
    expect(validateRegistryVersion("2.1.1", AIR_MODEL_REGISTRY)).toEqual({
      ok: false,
      code: "mismatch",
      expectedVersion: "2.1.0",
    });

    expect(() =>
      validateModelRegistry({
        schemaVersion: "air-model-registry/v1",
        registryVersion: "2.1.0",
        models: [],
        defaults: {},
        unexpected: true,
      }),
    ).toThrow(ModelRegistryValidationError);

    expect(() =>
      validateModelRegistry({
        schemaVersion: "air-model-registry/v2",
        registryVersion: "2.1.0",
        models: [],
        defaults: {},
      }),
    ).toThrow(ModelRegistryValidationError);
    expect(() =>
      validateModelRegistry({
        schemaVersion: "air-model-registry/v1",
        registryVersion: "not-a-version",
        models: [],
        defaults: {},
      }),
    ).toThrow(ModelRegistryValidationError);
  });

  it("validates all supported count/duration price shapes and exact compatibility", () => {
    const model = verifiedAnimateModel();
    const catalog = registry([model], { animate: "verified-test-video" });

    const selected = resolveEnabledModel(catalog, "animate", "verified-test-video");
    expect(selected.ok).toBe(true);
    if (!selected.ok) throw new Error("fixture should resolve");

    expect(
      checkModelCompatibility(selected.model, {
        mode: "animate",
        count: 2,
        durationSeconds: 10,
        hasPrompt: false,
        media: "image",
      }),
    ).toEqual({
      ok: true,
      maximumQuotedCost: { count: 2, durationSeconds: 10, maximumQuotedCostCents: 100 },
    });
    expect(
      checkModelCompatibility(selected.model, {
        mode: "animate",
        count: 1,
        durationSeconds: null,
        hasPrompt: true,
        media: "none",
      }),
    ).toEqual({ ok: false, code: "duration_required", acceptedDurations: [5, 10] });
    expect(
      checkModelCompatibility(selected.model, {
        mode: "animate",
        count: 1,
        durationSeconds: 15,
        hasPrompt: true,
        media: "none",
      }),
    ).toEqual({ ok: false, code: "duration_not_supported", acceptedDurations: [5, 10] });
    expect(
      listCompatibleEnabledModels(catalog, {
        mode: "animate",
        count: 2,
        durationSeconds: 10,
        hasPrompt: false,
        media: "image",
      }).map((candidate) => candidate.slug),
    ).toEqual(["verified-test-video"]);
  });

  it("does not silently pick unavailable models or a nearby model", () => {
    const unavailable = verifiedAnimateModel({ status: "unavailable" });
    const catalog = registry([unavailable]);

    expect(resolveEnabledModel(catalog, "animate", "verified-test-video")).toEqual({
      ok: false,
      code: "model_not_enabled",
    });
    expect(resolveEnabledModel(catalog, "animate", "not-in-registry")).toEqual({
      ok: false,
      code: "unknown_model",
    });
    expect(listEnabledModels(catalog, "animate")).toEqual([]);
  });

  it("fails closed on missing price evidence and public-input enablement without acknowledgement", () => {
    const withoutEveryQuote = verifiedAnimateModel({
      pricing: {
        source: "https://catalog.example.test/pricing/verified-test-video",
        currency: "USD",
        verifiedAt: "2026-07-18T00:00:00Z",
        evidenceFingerprint: sha256Fixture("verified-test-video-pricing"),
        quotes: [{ count: 1, durationSeconds: 5, maximumQuotedCostCents: 25 }],
      },
    });
    expect(() => registry([withoutEveryQuote])).toThrow(ModelRegistryValidationError);

    const publicWithoutAcknowledgement = verifiedAnimateModel({
      inputHosting: { mode: "hosted-public", host: "storage.example.test", pathPrefix: "/air-input/" },
    });
    expect(() => registry([publicWithoutAcknowledgement])).toThrow(ModelRegistryValidationError);
  });

  it("requires a reviewed, provider-bound contract and immutable pricing evidence", () => {
    const catalog = registry([verifiedAnimateModel()]);
    const model = catalog.models[0];
    if (model === undefined) throw new Error("fixture should contain one model");

    expect(model.providerBinding).toEqual({
      adapterKey: "gmi.test-adapter.v1",
      requestSchemaFingerprint: sha256Fixture("verified-test-video-request-schema"),
      resultSchemaFingerprint: sha256Fixture("verified-test-video-result-schema"),
    });
    expect(Object.isFrozen(model.providerBinding)).toBe(true);
    expect(Object.isFrozen(model.pricing)).toBe(true);
    expect(() => {
      (model.providerBinding as { adapterKey: string }).adapterKey = "gmi.other.v1";
    }).toThrow(TypeError);

    const withoutBinding = verifiedAnimateModel() as Record<string, unknown>;
    delete withoutBinding.providerBinding;
    expect(() => registry([withoutBinding])).toThrow(ModelRegistryValidationError);

    const unversionedAdapter = verifiedAnimateModel({
      providerBinding: {
        adapterKey: "gmi.test-adapter",
        requestSchemaFingerprint: sha256Fixture("verified-test-video-request-schema"),
        resultSchemaFingerprint: sha256Fixture("verified-test-video-result-schema"),
      },
    });
    expect(() => registry([unversionedAdapter])).toThrow(ModelRegistryValidationError);

    const malformedRequestFingerprint = verifiedAnimateModel({
      providerBinding: {
        adapterKey: "gmi.test-adapter.v1",
        requestSchemaFingerprint: "sha256:UPPERCASE-OR-SHORT",
        resultSchemaFingerprint: sha256Fixture("verified-test-video-result-schema"),
      },
    });
    expect(() => registry([malformedRequestFingerprint])).toThrow(ModelRegistryValidationError);

    const incompleteResultBinding = verifiedAnimateModel({
      providerBinding: {
        adapterKey: "gmi.test-adapter.v1",
        requestSchemaFingerprint: sha256Fixture("verified-test-video-request-schema"),
      },
    });
    expect(() => registry([incompleteResultBinding])).toThrow(ModelRegistryValidationError);

    const wrongProviderAdapter = verifiedAnimateModel({
      providerBinding: {
        adapterKey: "fal.test-adapter.v1",
        requestSchemaFingerprint: sha256Fixture("verified-test-video-request-schema"),
        resultSchemaFingerprint: sha256Fixture("verified-test-video-result-schema"),
      },
    });
    expect(() => registry([wrongProviderAdapter])).toThrow(ModelRegistryValidationError);

    const malformedEvidenceFingerprint = verifiedAnimateModel({
      pricing: {
        source: "https://catalog.example.test/pricing/verified-test-video",
        currency: "USD",
        verifiedAt: "2026-07-18T00:00:00Z",
        evidenceFingerprint: "sha256:not-a-canonical-digest",
        quotes: [
          { count: 1, durationSeconds: 5, maximumQuotedCostCents: 25 },
          { count: 1, durationSeconds: 10, maximumQuotedCostCents: 50 },
          { count: 2, durationSeconds: 5, maximumQuotedCostCents: 50 },
          { count: 2, durationSeconds: 10, maximumQuotedCostCents: 100 },
        ],
      },
    });
    expect(() => registry([malformedEvidenceFingerprint])).toThrow(ModelRegistryValidationError);

    const unknownBindingField = verifiedAnimateModel({
      providerBinding: {
        adapterKey: "gmi.test-adapter.v1",
        requestSchemaFingerprint: sha256Fixture("verified-test-video-request-schema"),
        resultSchemaFingerprint: sha256Fixture("verified-test-video-result-schema"),
        unreviewed: true,
      },
    });
    expect(() => registry([unknownBindingField])).toThrow(ModelRegistryValidationError);
  });
});
