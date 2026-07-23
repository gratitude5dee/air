import { describe, expect, it, vi } from "vitest";

import type { StudioEnvironment } from "../../lib/studio/config.js";
import type { GmiJsonValue } from "../../lib/studio/gmi-request-queue.js";
import {
  GMI_PROVIDER_AUDIT_MODEL_ID,
  MAX_PROVIDER_AUDIT_PARAMETERS,
  handleStudioProviderAudit,
  sanitizeProviderAudit,
  type StudioProviderAuditClient,
} from "../../lib/studio/provider-audit.js";

const PRIVATE_URL = "https://provider.example.test/private-output?signature=never-return";
const PRIVATE_PROMPT = "a private customer prompt that must never leave the provider response";
const PRIVATE_REQUEST_ID = "provider-request-id-that-must-not-leak";

function environment(overrides: Partial<StudioEnvironment> = {}): StudioEnvironment {
  const base: StudioEnvironment = {
    deploymentMode: "production",
    releasePhase: "production",
    releaseGeneration: 1,
    releaseFenceSecret: "release-fence-secret",
    storageTransport: "rest",
    gmiApiKey: "gmi-key-kept-server-side",
    gmiOrganizationId: "org-private",
    idHashSecret: "identity-secret",
    queueEncryptionKey: Buffer.alloc(32, 7).toString("base64"),
    allowedSenderHashes: [],
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
      perJobCents: 10_000,
      dailyCents: 10_000,
      canaryTotalCents: 0,
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

function request(method = "GET", token = "cron-secret"): Request {
  return new Request("https://air.example.test/internal/provider-audit", {
    method,
    headers: { authorization: `Bearer ${token}` },
  });
}

type RecordingClient = StudioProviderAuditClient & {
  readonly submit: ReturnType<typeof vi.fn>;
  readonly getRequestStatus: ReturnType<typeof vi.fn>;
  readonly createUploadUrl: ReturnType<typeof vi.fn>;
};

function clientFixture(input: {
  readonly catalog?: GmiJsonValue;
  readonly detail?: GmiJsonValue;
  readonly fail?: boolean;
} = {}): RecordingClient {
  const client = {
    listModels: vi.fn(async (): Promise<GmiJsonValue> => {
      if (input.fail) throw new Error("provider response containing private body");
      return input.catalog ?? { models: [] };
    }),
    getModelDetail: vi.fn(async (): Promise<GmiJsonValue> => {
      if (input.fail) throw new Error("provider response containing private body");
      return input.detail ?? {};
    }),
    submit: vi.fn(),
    getRequestStatus: vi.fn(),
    createUploadUrl: vi.fn(),
  };
  return client as RecordingClient;
}

describe("read-only protected GMI provider audit", () => {
  it("rejects every non-GET request before environment parsing or client construction", async () => {
    const loadEnvironment = vi.fn(() => environment());
    const createClient = vi.fn();

    const response = await handleStudioProviderAudit(request("POST"), { loadEnvironment, createClient });

    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET");
    expect(await response.text()).toBe("");
    expect(loadEnvironment).not.toHaveBeenCalled();
    expect(createClient).not.toHaveBeenCalled();
  });

  it("uses the drain's exact bearer protection before constructing a GMI client", async () => {
    const loadEnvironment = vi.fn(() => environment());
    const createClient = vi.fn();

    const response = await handleStudioProviderAudit(request("GET", "almost-cron-secret"), {
      loadEnvironment,
      createClient,
    });

    expect(response.status).toBe(401);
    expect(await response.text()).toBe("");
    expect(loadEnvironment).toHaveBeenCalledTimes(1);
    expect(createClient).not.toHaveBeenCalled();
  });

  it("keeps a missing GMI credential and every provider failure generic", async () => {
    const noKeyClient = vi.fn();
    const missingKey = await handleStudioProviderAudit(request(), {
      loadEnvironment: () => environment({ gmiApiKey: undefined }),
      createClient: noKeyClient,
    });
    expect(missingKey.status).toBe(503);
    expect(await missingKey.text()).toBe("");
    expect(noKeyClient).not.toHaveBeenCalled();

    const client = clientFixture({ fail: true });
    const providerFailure = await handleStudioProviderAudit(request(), {
      loadEnvironment: () => environment(),
      createClient: () => client,
    });
    expect(providerFailure.status).toBe(503);
    expect(await providerFailure.text()).toBe("");
    expect(client.listModels).toHaveBeenCalledTimes(1);
    expect(client.getModelDetail).not.toHaveBeenCalled();
    expect(client.submit).not.toHaveBeenCalled();
    expect(client.getRequestStatus).not.toHaveBeenCalled();
    expect(client.createUploadUrl).not.toHaveBeenCalled();
  });

  it("calls only the exact two approved read methods and returns a bounded sanitized review", async () => {
    const client = clientFixture({
      catalog: {
        model_ids: ["other-model", GMI_PROVIDER_AUDIT_MODEL_ID],
        request_id: PRIVATE_REQUEST_ID,
      },
      detail: {
        model: GMI_PROVIDER_AUDIT_MODEL_ID,
        parameters: [
          { name: "prompt", type: "string", required: true, description: PRIVATE_PROMPT },
          { name: "aspect_ratio", type: "string", default: "16:9", url: PRIVATE_URL },
          { name: "unsafe name with space", type: "string" },
          { name: "unsupported_type", type: "url", example: PRIVATE_URL },
        ],
        output: { url: PRIVATE_URL },
        request_id: PRIVATE_REQUEST_ID,
      },
    });
    const createClient = vi.fn(() => client);

    const response = await handleStudioProviderAudit(request(), {
      loadEnvironment: () => environment(),
      createClient,
    });

    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(createClient).toHaveBeenCalledWith({
      apiKey: "gmi-key-kept-server-side",
      organizationId: "org-private",
    });
    expect(client.listModels).toHaveBeenCalledTimes(1);
    expect(client.getModelDetail).toHaveBeenCalledTimes(1);
    expect(client.getModelDetail).toHaveBeenCalledWith(GMI_PROVIDER_AUDIT_MODEL_ID);
    expect(client.submit).not.toHaveBeenCalled();
    expect(client.getRequestStatus).not.toHaveBeenCalled();
    expect(client.createUploadUrl).not.toHaveBeenCalled();

    const body = await response.json() as Record<string, unknown>;
    expect(Object.keys(body).sort()).toEqual([
      "available",
      "catalogFingerprint",
      "modelId",
      "parameters",
      "pricing",
      "schemaFingerprint",
    ]);
    expect(body).toMatchObject({
      modelId: GMI_PROVIDER_AUDIT_MODEL_ID,
      available: true,
      pricing: "manual_console_required",
      parameters: [
        { name: "aspect_ratio", declaredType: "string", required: false },
        { name: "prompt", declaredType: "string", required: true },
        { name: "unsupported_type", declaredType: "unknown", required: false },
      ],
    });
    expect(body.catalogFingerprint).toMatch(/^sha256:[a-f0-9]{64}$/u);
    expect(body.schemaFingerprint).toMatch(/^sha256:[a-f0-9]{64}$/u);
    const serialized = JSON.stringify(body);
    expect(serialized).not.toContain("gmi-key-kept-server-side");
    expect(serialized).not.toContain(PRIVATE_URL);
    expect(serialized).not.toContain(PRIVATE_PROMPT);
    expect(serialized).not.toContain(PRIVATE_REQUEST_ID);
  });

  it("fails closed when the detail does not explicitly identify the exact reviewed model", async () => {
    const client = clientFixture({
      catalog: { model_ids: [GMI_PROVIDER_AUDIT_MODEL_ID] },
      detail: {
        model: "other-model",
        description: `not identity: ${GMI_PROVIDER_AUDIT_MODEL_ID}`,
        input_schema: { properties: { prompt: { type: "string" } } },
      },
    });

    const response = await handleStudioProviderAudit(request(), {
      loadEnvironment: () => environment(),
      createClient: () => client,
    });

    expect(response.status).toBe(503);
    expect(await response.text()).toBe("");
    expect(client.listModels).toHaveBeenCalledTimes(1);
    expect(client.getModelDetail).toHaveBeenCalledTimes(1);
    expect(client.getModelDetail).toHaveBeenCalledWith(GMI_PROVIDER_AUDIT_MODEL_ID);
    expect(client.submit).not.toHaveBeenCalled();
    expect(client.getRequestStatus).not.toHaveBeenCalled();
    expect(client.createUploadUrl).not.toHaveBeenCalled();
  });

  it("hashes only minimized safe evidence and bounds every allowlisted parameter projection", () => {
    const propertyEntries = Array.from({ length: MAX_PROVIDER_AUDIT_PARAMETERS + 8 }, (_, index) => [
      `field_${String(index).padStart(2, "0")}`,
      index % 2 === 0 ? "string" : "number",
    ] as const);
    const firstProperties = Object.fromEntries(propertyEntries.map(([name, type]) => [name, {
      type,
      private_text: PRIVATE_PROMPT,
      url: PRIVATE_URL,
      request_id: PRIVATE_REQUEST_ID,
    }]));
    const secondProperties = Object.fromEntries([...propertyEntries].reverse().map(([name, type]) => [name, {
      type,
      private_text: "a different private prompt",
      url: "https://provider.example.test/another-private-output?signature=also-never-return",
      request_id: "different-provider-request-id",
      timestamp: "volatile-provider-timestamp",
    }]));
    const first = sanitizeProviderAudit(
      {
        model_ids: [GMI_PROVIDER_AUDIT_MODEL_ID, "unrelated-model"],
        url: PRIVATE_URL,
        prompt: PRIVATE_PROMPT,
        request_id: PRIVATE_REQUEST_ID,
        timestamp: "first-volatile-timestamp",
      },
      {
        model: GMI_PROVIDER_AUDIT_MODEL_ID,
        input_schema: { properties: firstProperties },
        output: { url: PRIVATE_URL, prompt: PRIVATE_PROMPT },
        request_id: PRIVATE_REQUEST_ID,
        timestamp: "first-volatile-timestamp",
      },
    );
    const second = sanitizeProviderAudit(
      {
        model_ids: ["different-unrelated-model", GMI_PROVIDER_AUDIT_MODEL_ID],
        url: "https://provider.example.test/another-private-catalog-url",
        prompt: "a different private catalog prompt",
        request_id: "different-catalog-request-id",
        timestamp: "second-volatile-timestamp",
      },
      {
        model: GMI_PROVIDER_AUDIT_MODEL_ID,
        input_schema: { properties: secondProperties },
        output: { url: "https://provider.example.test/another-private-output" },
        request_id: "different-provider-request-id",
        timestamp: "second-volatile-timestamp",
      },
    );
    const unavailable = sanitizeProviderAudit(
      { model_ids: ["different-unrelated-model"] },
      { model: GMI_PROVIDER_AUDIT_MODEL_ID, input_schema: { properties: secondProperties } },
    );

    expect(first.catalogFingerprint).toBe(second.catalogFingerprint);
    expect(first.schemaFingerprint).toBe(second.schemaFingerprint);
    expect(first.catalogFingerprint).not.toBe(unavailable.catalogFingerprint);
    expect(first.parameters).toHaveLength(MAX_PROVIDER_AUDIT_PARAMETERS);
    expect(first.parameters[0]?.name).toBe("field_00");
    expect(first.parameters.at(-1)?.name).toBe("field_31");
    expect(JSON.stringify(first)).not.toContain(PRIVATE_URL);
    expect(JSON.stringify(first)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(first)).not.toContain(PRIVATE_REQUEST_ID);
  });
});
