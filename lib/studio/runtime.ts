import { createQueueCipher, type QueueCipher } from "../crypto.js";
import { connectRedisFromUrl, type ConnectedRedis } from "../redis.js";
import {
  canAcceptStudioJobs,
  canRecoverPaidStudioProvider,
  canSubmitPaidStudioProvider,
  loadStudioEnvironment,
  studioReadinessProblems,
  type StudioEnvironment,
} from "./config.js";
import { UnverifiedPhotonTransport, type StudioIMessageTransport } from "./imessage-transport.js";
import {
  AIR_MODEL_REGISTRY,
  resolveDefaultModel,
  validateRegistryVersion,
  type AirModelRegistry,
  type StudioModel,
} from "./model-registry.js";
import { assertNoPhotonTelemetryExporters } from "./photon-telemetry.js";
import { UnprovisionedStudioProvider, type StudioProviderPort } from "./provider-port.js";
import { StudioStore } from "./store.js";
import {
  GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY,
  GMI_FLUX_KONTEXT_PRO_MODEL_ID,
  GmiFluxKontextProStudioProvider,
} from "./gmi-flux-kontext-pro-provider.js";
import { GmiRequestQueueClient } from "./gmi-request-queue.js";

export const AIR_STUDIO_VERSION = "2";

export interface StudioRuntime {
  readonly environment: StudioEnvironment;
  readonly cipher: QueueCipher;
  readonly store: StudioStore;
  /**
   * The inert fallback is deliberately available for compatibility and dark
   * release checks. Call `getStudioDeliveryTransport` immediately before an
   * outbound action so receipt durability never depends on Photon token
   * issuance.
   */
  readonly transport: StudioIMessageTransport;
  /**
   * Production runtime composition creates this resolver without contacting
   * Photon. It is optional so already-constructed test and drain runtimes
   * retain their exact injected transport behavior.
   */
  readonly getDeliveryTransport?: (() => Promise<StudioIMessageTransport>) | undefined;
  /** Never live by default; the executor rejects this unverified provider. */
  readonly provider: StudioProviderPort;
  /** Committed registry; tests may inject a reviewed fixture without mutating code defaults. */
  readonly registry?: AirModelRegistry | undefined;
  readonly storageTransport: "rest" | "tcp";
  readiness(expectedRegistryVersion: string): readonly string[];
  close(): Promise<void>;
}

let runtimePromise: Promise<StudioRuntime> | undefined;
let runtimeOverride: StudioRuntime | undefined;

/** Test-only injection. Production calls always connect to the configured existing Redis resource. */
export function __setStudioRuntimeForTests(runtime: StudioRuntime | undefined): void {
  runtimeOverride = runtime;
  runtimePromise = undefined;
}

export async function getStudioRuntime(): Promise<StudioRuntime> {
  if (runtimeOverride) return runtimeOverride;
  if (!runtimePromise) {
    runtimePromise = createStudioRuntime();
    void runtimePromise.catch(() => {
      runtimePromise = undefined;
    });
  }
  return runtimePromise;
}

/**
 * Resolves the direct Photon binding only at a delivery boundary. A rejected
 * token issuance is intentionally surfaced to the caller rather than cached:
 * the durable outbox can retry later, while signed inbound receipt persistence
 * remains independent of all outbound credentials.
 */
export async function getStudioDeliveryTransport(runtime: StudioRuntime): Promise<StudioIMessageTransport> {
  return runtime.getDeliveryTransport === undefined
    ? runtime.transport
    : await runtime.getDeliveryTransport();
}

export async function createStudioRuntime(
  environment: StudioEnvironment = loadStudioEnvironment(),
  transport?: StudioIMessageTransport,
  provider?: StudioProviderPort,
): Promise<StudioRuntime> {
  if (!environment.idHashSecret || !environment.queueEncryptionKey || environment.storageTransport === "unconfigured") {
    throw new Error("studio runtime configuration is incomplete");
  }
  const configuredProvider = createConfiguredStudioProvider(environment, AIR_MODEL_REGISTRY, provider);
  const connection = await connectExistingRedis(environment);
  // Do not mint a Photon token while acknowledging an inbound webhook. The
  // receipt reaches Redis first; a later outbox/job-delivery drain resolves
  // the transport immediately before a physical Photon action.
  const configuredTransport = transport ?? new UnverifiedPhotonTransport();
  const getDeliveryTransport = transport === undefined
    ? createLazyIMessageTransportResolver(() => createConfiguredIMessageTransport(environment))
    : undefined;
  return runtimeFromConnectedRedis(
    environment,
    connection,
    configuredTransport,
    configuredProvider,
    getDeliveryTransport,
  );
}

/**
 * Composes the only reviewed first-launch provider contract. This does not
 * enable a model: the committed production registry remains empty until live
 * catalog, schema, and pricing evidence are recorded. The registry argument
 * makes this pure boundary directly testable without changing that default.
 */
export function createConfiguredStudioProvider(
  environment: StudioEnvironment,
  registry: AirModelRegistry = AIR_MODEL_REGISTRY,
  injectedProvider?: StudioProviderPort,
): StudioProviderPort {
  // A stale environment value is not merely a readiness concern. It means the
  // runtime cannot prove that the immutable registry it is about to bind is
  // the one the release was configured to run. Keep both real composition and
  // test-only injection fail-closed so a version-drift deployment cannot make
  // a paid provider reachable.
  if (!validateRegistryVersion(environment.modelRegistryVersion, registry).ok) {
    return new UnprovisionedStudioProvider();
  }
  if (injectedProvider !== undefined) return injectedProvider;

  const model = reviewedFluxKontextProImagineDefault(registry);
  if (model === undefined || environment.gmiApiKey === undefined) {
    return new UnprovisionedStudioProvider();
  }

  try {
    // Construction only validates credentials and pins an immutable client
    // configuration. It deliberately performs no provider request.
    const queue = new GmiRequestQueueClient({
      apiKey: environment.gmiApiKey,
      ...(environment.gmiOrganizationId === undefined
        ? {}
        : { organizationId: environment.gmiOrganizationId }),
      sideEffectEnvironment: environment,
    });
    return new GmiFluxKontextProStudioProvider(queue, {
      outputPolicy: Object.freeze({
        host: model.outputRetrieval.host,
        pathPrefix: model.outputRetrieval.pathPrefix,
        access: model.outputRetrieval.access,
      }),
    });
  } catch {
    // A manually constructed or malformed test environment must fail closed,
    // just like a missing credential or unreviewed registry binding.
    return new UnprovisionedStudioProvider();
  }
}

export function studioPublicStatusResponse(): Response {
  return Response.json(
    // This branded, dependency-free shape is an external compatibility
    // contract. The protected readiness route is the source of truth for the
    // current dark/active release state; do not overload this public product
    // description with deployment configuration.
    { name: "Air", status: "private allowlisted iMessage studio" },
    { headers: { "cache-control": "no-store", link: '</favicon.ico>; rel="icon"; type="image/x-icon"' } },
  );
}

export function studioHealthResponse(): Response {
  return Response.json(
    { ok: true, version: AIR_STUDIO_VERSION },
    { headers: { "cache-control": "no-store" } },
  );
}

async function connectExistingRedis(environment: StudioEnvironment): Promise<ConnectedRedis> {
  // connectRedisFromUrl ignores the TCP URL when the validated REST pair is
  // present. The placeholder is never parsed or used on that path.
  return connectRedisFromUrl(environment.redisUrl ?? "rediss://unused.invalid", {
    ...(environment.upstashRedisRestUrl && environment.upstashRedisRestToken
      ? { upstashRestUrl: environment.upstashRedisRestUrl, upstashRestToken: environment.upstashRedisRestToken }
      : {}),
  });
}

/**
 * The adapter's own final submit boundary repeats these checks. Keeping an
 * equally narrow composition gate prevents a generic GMI key plus an enabled
 * but differently shaped model from making a provider appear provisioned.
 */
function reviewedFluxKontextProImagineDefault(registry: AirModelRegistry): StudioModel | undefined {
  const resolution = resolveDefaultModel(registry, "imagine");
  if (!resolution.ok) return undefined;
  const model = resolution.model;

  const onlyPromptField = model.requiredPayloadFields.length === 1 &&
    model.requiredPayloadFields[0]?.name === "prompt" &&
    model.requiredPayloadFields[0]?.declaredType === "string" &&
    model.requiredPayloadFields[0]?.required === true;
  const exactlyOneImage = model.output.type === "image" &&
    model.output.maximumOutputCount === 1 &&
    model.output.supportedOutputCounts.length === 1 &&
    model.output.supportedOutputCounts[0] === 1 &&
    model.output.supportedDurationSeconds.length === 0;
  // Host and path are verified registry evidence, not runtime guesses. The
  // first authenticated catalog audit will commit their exact values; this
  // gate only requires a non-empty, private, signed delivery contract and
  // passes the reviewed allowlist through unchanged to the adapter.
  const privateFluxOutput = model.outputRetrieval.host.trim().length > 0 &&
    model.outputRetrieval.pathPrefix.length > 1 &&
    model.outputRetrieval.access === "signed-url" &&
    model.outputRetrieval.publiclyAccessible === false;

  return model.provider === "gmi" &&
    model.providerModelId === GMI_FLUX_KONTEXT_PRO_MODEL_ID &&
    model.providerBinding.adapterKey === GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY &&
    model.input.prompt === "required" &&
    model.input.image === false &&
    model.input.video === false &&
    model.input.mediaOnly === false &&
    model.inputHosting.mode === "data-uri" &&
    onlyPromptField &&
    exactlyOneImage &&
    privateFluxOutput
    ? model
    : undefined;
}

/**
 * Importing the Photon client is deferred until a real, fully configured
 * messaging runtime is requested. Dark previews with incomplete credentials
 * retain the dependency-free, fail-closed transport instead of initializing a
 * client that could emit unredacted telemetry.
 */
async function createConfiguredIMessageTransport(environment: StudioEnvironment): Promise<StudioIMessageTransport> {
  if (!environment.imessageProjectId || !environment.imessageProjectSecret || !environment.imessageWebhookSecret) {
    return new UnverifiedPhotonTransport();
  }
  // This module has no Photon imports, so unredacted telemetry is rejected
  // before any provider package can initialize against process-wide settings.
  assertNoPhotonTelemetryExporters();
  const { createPhotonIMessageTransport } = await import("./photon-transport.js");
  return createPhotonIMessageTransport({
    projectId: environment.imessageProjectId,
    projectSecret: environment.imessageProjectSecret,
    webhookSecret: environment.imessageWebhookSecret,
  });
}

/**
 * Retain one successfully verified transport for the lifetime of this Vercel
 * runtime, while allowing a failed initial token issuance to be retried by a
 * future durable drain. No Photon package is imported until this resolver is
 * actually invoked.
 */
export function createLazyIMessageTransportResolver(
  createTransport: () => Promise<StudioIMessageTransport>,
): () => Promise<StudioIMessageTransport> {
  let resolved: StudioIMessageTransport | undefined;
  let inFlight: Promise<StudioIMessageTransport> | undefined;

  return async (): Promise<StudioIMessageTransport> => {
    if (resolved !== undefined) return resolved;
    if (inFlight === undefined) {
      inFlight = createTransport()
        .then((transport) => {
          resolved = transport;
          return transport;
        })
        .finally(() => {
          inFlight = undefined;
        });
    }
    return await inFlight;
  };
}

function runtimeFromConnectedRedis(
  environment: StudioEnvironment,
  connection: ConnectedRedis,
  transport: StudioIMessageTransport,
  provider: StudioProviderPort,
  getDeliveryTransport?: (() => Promise<StudioIMessageTransport>) | undefined,
): StudioRuntime {
  const cipher = createQueueCipher(environment.queueEncryptionKey as string);
  const store = new StudioStore({
    redis: connection.redis,
    cipher,
    identityHashSecret: environment.idHashSecret as string,
    releaseStamp: {
      generation: environment.releaseGeneration,
      phase: environment.releasePhase,
    },
  });
  return Object.freeze({
    environment,
    cipher,
    store,
    transport,
    ...(getDeliveryTransport === undefined ? {} : { getDeliveryTransport }),
    provider,
    registry: AIR_MODEL_REGISTRY,
    storageTransport: environment.storageTransport === "rest" ? "rest" : "tcp",
    readiness(expectedRegistryVersion: string): readonly string[] {
      const problems = [...studioReadinessProblems(environment, expectedRegistryVersion)];
      if (!transport.verified) problems.push("transport_unverified");
      if (
        (canAcceptStudioJobs(environment) || canSubmitPaidStudioProvider(environment) || canRecoverPaidStudioProvider(environment))
        && !provider.verified
      ) problems.push("provider_unprovisioned");
      return Object.freeze(problems);
    },
    async close(): Promise<void> {
      await connection.close();
    },
  });
}
