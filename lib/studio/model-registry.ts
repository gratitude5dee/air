import { isIP } from "node:net";

import { z } from "zod";

import {
  STUDIO_MODES,
  type StudioMode,
  type StudioOutputCount,
  type StudioVideoDurationSeconds,
} from "./commands.js";

/** The immutable schema contract for registries committed with Air v2. */
export const AIR_MODEL_REGISTRY_SCHEMA_VERSION = "air-model-registry/v1" as const;

/**
 * This build's registry version. Environment configuration must equal this
 * value exactly; it never selects a different registry at runtime.
 */
export const AIR_MODEL_REGISTRY_VERSION = "2.1.0" as const;

export const MODEL_AVAILABILITY_STATUSES = Object.freeze([
  "enabled",
  "unavailable",
  "deprecated",
  "experimental",
] as const);
export type ModelAvailabilityStatus = (typeof MODEL_AVAILABILITY_STATUSES)[number];

export const MODEL_PROVIDERS = Object.freeze(["gmi", "fal"] as const);
export type StudioModelProvider = (typeof MODEL_PROVIDERS)[number];

export const MODEL_OUTPUT_TYPES = Object.freeze(["image", "video"] as const);
export type StudioModelOutputType = (typeof MODEL_OUTPUT_TYPES)[number];

export const MODEL_INPUT_HOSTING_MODES = Object.freeze(["data-uri", "hosted-public"] as const);
export type StudioModelInputHostingMode = (typeof MODEL_INPUT_HOSTING_MODES)[number];

export const MODEL_PROMPT_REQUIREMENTS = Object.freeze([
  "required",
  "optional",
  "forbidden",
] as const);
export type ModelPromptRequirement = (typeof MODEL_PROMPT_REQUIREMENTS)[number];

const CONTROL_CHARACTER_PATTERN = /\p{Cc}/u;
const STABLE_MODEL_SLUG_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const REGISTRY_VERSION_PATTERN = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;
const ISO_UTC_TIMESTAMP_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;
const HOSTNAME_PATTERN =
  /^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;
const SHA256_FINGERPRINT_PATTERN = /^sha256:[a-f0-9]{64}$/;
/**
 * Provider bindings are code-facing identifiers, not display labels. The
 * provider namespace and explicit version make an accidental adapter swap
 * invalid before a provider request can be constructed.
 */
const PROVIDER_ADAPTER_KEY_PATTERN = /^(?:gmi|fal)\.[a-z0-9]+(?:[.-][a-z0-9]+)*\.v[1-9][0-9]*$/;

function committedString(maximumLength: number) {
  return z
    .string()
    .min(1)
    .max(maximumLength)
    .refine(
      (value) => value.trim() === value && !CONTROL_CHARACTER_PATTERN.test(value),
      "must be trimmed and contain no control characters",
    );
}

const stableSlugSchema = z
  .string()
  .min(1)
  .max(80)
  .regex(STABLE_MODEL_SLUG_PATTERN, "must be a lowercase ASCII slug");

const studioOutputCountSchema = z.union([
  z.literal(1),
  z.literal(2),
  z.literal(3),
  z.literal(4),
]);

const studioVideoDurationSecondsSchema = z.union([
  z.literal(5),
  z.literal(10),
  z.literal(15),
]);

const utcTimestampSchema = committedString(40).refine(
  (value) => ISO_UTC_TIMESTAMP_PATTERN.test(value) && Number.isFinite(Date.parse(value)),
  "must be a UTC ISO-8601 timestamp",
);

/** A canonical digest, never a mutable label or an unreviewed URL. */
const sha256FingerprintSchema = committedString(71).refine(
  (value) => SHA256_FINGERPRINT_PATTERN.test(value),
  "must be a lowercase sha256 fingerprint",
);

const providerAdapterKeySchema = committedString(160).refine(
  (value) => PROVIDER_ADAPTER_KEY_PATTERN.test(value),
  "must be a provider-namespaced, explicitly versioned adapter key",
);

const providerPayloadFieldSchema = z
  .object({
    name: committedString(120),
    /** Preserve the provider schema's declared type verbatim, e.g. "string". */
    declaredType: committedString(160),
    required: z.boolean(),
  })
  .strict();

/**
 * Registry locations are consumed by provider adapters as SSRF allowlists.
 * Keep the committed form at least as narrow as the runtime fetch boundary so
 * a reviewed registry cannot name a location that would later be normalized
 * differently by an adapter.
 */
function isCanonicalDnsHost(value: string): boolean {
  if (value.length < 4 || value.length > 253 || value.endsWith(".") || !HOSTNAME_PATTERN.test(value)) {
    return false;
  }

  try {
    const hostname = new URL(`https://${value}`).hostname;
    // WHATWG URL normalizes legacy numeric IPv4 spellings such as `127.1`.
    // A registry hostname must remain an exact DNS name after parsing and can
    // never be an IP literal, including link-local/private IPv4 addresses.
    return hostname === value && isIP(hostname) === 0;
  } catch {
    return false;
  }
}

const canonicalDnsHostSchema = z
  .string()
  .max(253)
  .refine(isCanonicalDnsHost, "must be a canonical lowercase DNS hostname, not an IP address");

const canonicalObjectPathPrefixSchema = z
  .string()
  .min(3)
  .max(512)
  .refine(
    (value) => {
      if (
        !value.startsWith("/") ||
        !value.endsWith("/") ||
        value === "/" ||
        value.includes("//") ||
        value.includes("\\") ||
        value.includes("?") ||
        value.includes("#") ||
        value.includes("%") ||
        CONTROL_CHARACTER_PATTERN.test(value)
      ) {
        return false;
      }
      const segments = value.slice(1, -1).split("/");
      return segments.length > 0 && segments.every((segment) =>
        /^[A-Za-z0-9._~-]+$/.test(segment) && segment !== "." && segment !== ".."
      );
    },
    "must be a non-root slash-terminated canonical object path prefix",
  );

const dataUriInputHostingSchema = z
  .object({
    mode: z.literal("data-uri"),
  })
  .strict();

const hostedPublicInputHostingSchema = z
  .object({
    mode: z.literal("hosted-public"),
    host: canonicalDnsHostSchema,
    /**
     * A path prefix must be more specific than `/`, so a compromised URL
     * cannot broaden an allowlist to every object on a shared host.
     */
    pathPrefix: canonicalObjectPathPrefixSchema,
  })
  .strict();

const inputHostingSchema = z.union([dataUriInputHostingSchema, hostedPublicInputHostingSchema]);

/**
 * Provider result retrieval is a separate contract from input hosting. A
 * model can accept a data URI yet return its output from a provider-owned
 * object store, so the worker needs an exact allowlist before it is ever
 * permitted to fetch a result. The visibility assertion is deliberately a
 * literal `false`: Air cannot call itself private while an enabled model's
 * result contract says the generated object is public.
 */
const outputRetrievalSchema = z
  .object({
    host: canonicalDnsHostSchema,
    pathPrefix: canonicalObjectPathPrefixSchema,
    /** Only a reviewed private or signed retrieval contract may be enabled. */
    access: z.enum(["provider-private", "signed-url"]),
    publiclyAccessible: z.literal(false),
    verifiedAt: utcTimestampSchema,
    evidenceFingerprint: sha256FingerprintSchema,
  })
  .strict();

const modelInputSchema = z
  .object({
    prompt: z.enum(MODEL_PROMPT_REQUIREMENTS),
    image: z.boolean(),
    video: z.boolean(),
    /** Whether a media-only request is valid for this exact model. */
    mediaOnly: z.boolean(),
  })
  .strict();

const modelOutputSchema = z
  .object({
    type: z.enum(MODEL_OUTPUT_TYPES),
    maximumOutputCount: z.number().int().min(1).max(4),
    supportedOutputCounts: z.array(studioOutputCountSchema).min(1).max(4),
    supportedDurationSeconds: z.array(studioVideoDurationSecondsSchema).max(3),
    /** A provider literal or the literal provider default, never a user setting. */
    lockedAspectRatio: committedString(80),
  })
  .strict();

const verificationSchema = z
  .object({
    verifiedAt: utcTimestampSchema,
    catalogFingerprint: sha256FingerprintSchema,
    schemaFingerprint: sha256FingerprintSchema,
  })
  .strict();

/**
 * Pins the registry entry to one reviewed implementation contract. Runtime
 * adapter lookup must use `adapterKey` exactly; it must not infer an adapter
 * from a provider label or provider model ID.
 */
const providerBindingSchema = z
  .object({
    adapterKey: providerAdapterKeySchema,
    requestSchemaFingerprint: sha256FingerprintSchema,
    resultSchemaFingerprint: sha256FingerprintSchema,
  })
  .strict();

const priceQuoteSchema = z
  .object({
    count: studioOutputCountSchema,
    /** Null is required for image modes so lookup cannot infer a duration. */
    durationSeconds: z.union([studioVideoDurationSecondsSchema, z.null()]),
    /** Integer minor units avoid a rounding-down path in admission. */
    maximumQuotedCostCents: z.number().int().min(0).max(100_000_000),
  })
  .strict();

const pricingSchema = z
  .object({
    source: z.string().url().max(2_048),
    currency: z.string().regex(/^[A-Z]{3}$/, "must be an ISO-4217 currency"),
    verifiedAt: utcTimestampSchema,
    /** Digest of the reviewed pricing evidence, not merely its landing page. */
    evidenceFingerprint: sha256FingerprintSchema,
    quotes: z.array(priceQuoteSchema).min(1).max(12),
  })
  .strict();

const pollingSchema = z
  .object({
    requestTimeoutMs: z.number().int().min(1_000).max(900_000),
    pollIntervalMs: z.number().int().min(250).max(300_000),
    maxPollAttempts: z.number().int().min(1).max(10_000),
    deadlineMs: z.number().int().min(1_000).max(86_400_000),
  })
  .strict();

const modelSchema = z
  .object({
    displayName: committedString(120),
    slug: stableSlugSchema,
    mode: z.enum(STUDIO_MODES),
    provider: z.enum(MODEL_PROVIDERS),
    /** Exact provider identifier, never a display-name guess or candidate hint. */
    providerModelId: committedString(512),
    providerBinding: providerBindingSchema,
    verification: verificationSchema,
    input: modelInputSchema,
    inputHosting: inputHostingSchema,
    output: modelOutputSchema,
    /** Exact policy for fetching provider output before native delivery. */
    outputRetrieval: outputRetrievalSchema,
    requiredPayloadFields: z.array(providerPayloadFieldSchema).min(1).max(64),
    pricing: pricingSchema,
    polling: pollingSchema,
    status: z.enum(MODEL_AVAILABILITY_STATUSES),
    /** Explicit Fable acknowledgement required before an enabled public-input path. */
    hostedPublicInputAcknowledged: z.boolean(),
  })
  .strict();

const defaultsSchema = z
  .object({
    imagine: stableSlugSchema.optional(),
    animate: stableSlugSchema.optional(),
    "edit-image": stableSlugSchema.optional(),
    "edit-video": stableSlugSchema.optional(),
  })
  .strict();

const baseModelRegistrySchema = z
  .object({
    schemaVersion: z.literal(AIR_MODEL_REGISTRY_SCHEMA_VERSION),
    registryVersion: z
      .string()
      .regex(REGISTRY_VERSION_PATTERN, "must be a semantic registry version"),
    models: z.array(modelSchema).max(128),
    defaults: defaultsSchema,
  })
  .strict();

export type StudioModel = z.infer<typeof modelSchema>;
export type AirModelRegistry = z.infer<typeof baseModelRegistrySchema>;
export type ModelPriceQuote = z.infer<typeof priceQuoteSchema>;

export type ModelRegistryValidationIssue = {
  readonly path: readonly (string | number)[];
  readonly message: string;
};

/** Contains registry-only validation failures; it never includes user media or prompts. */
export class ModelRegistryValidationError extends Error {
  public constructor(readonly issues: readonly ModelRegistryValidationIssue[]) {
    super("air_model_registry_invalid");
    this.name = "ModelRegistryValidationError";
  }
}

const EXPECTED_OUTPUT_BY_MODE: Readonly<Record<StudioMode, StudioModelOutputType>> = Object.freeze({
  imagine: "image",
  animate: "video",
  "edit-image": "image",
  "edit-video": "video",
});

function quoteKey(count: number, durationSeconds: number | null): string {
  return `${count}:${durationSeconds === null ? "none" : durationSeconds}`;
}

function addIssue(
  issues: ModelRegistryValidationIssue[],
  path: readonly (string | number)[],
  message: string,
): void {
  issues.push({ path, message });
}

function collectSemanticIssues(registry: AirModelRegistry): readonly ModelRegistryValidationIssue[] {
  const issues: ModelRegistryValidationIssue[] = [];
  const slugs = new Set<string>();

  for (const [index, model] of registry.models.entries()) {
    const path = ["models", index] as const;
    if (slugs.has(model.slug)) {
      addIssue(issues, [...path, "slug"], "model slugs must be unique");
    }
    slugs.add(model.slug);

    if (model.output.type !== EXPECTED_OUTPUT_BY_MODE[model.mode]) {
      addIssue(issues, [...path, "output", "type"], "output type does not match mode");
    }

    if (!model.providerBinding.adapterKey.startsWith(`${model.provider}.`)) {
      addIssue(
        issues,
        [...path, "providerBinding", "adapterKey"],
        "adapter key must be namespaced to the exact model provider",
      );
    }

    const counts = new Set(model.output.supportedOutputCounts);
    if (counts.size !== model.output.supportedOutputCounts.length) {
      addIssue(issues, [...path, "output", "supportedOutputCounts"], "output counts must be unique");
    }
    if (Math.max(...model.output.supportedOutputCounts) !== model.output.maximumOutputCount) {
      addIssue(
        issues,
        [...path, "output", "maximumOutputCount"],
        "maximumOutputCount must equal the largest supported output count",
      );
    }

    const durations = new Set(model.output.supportedDurationSeconds);
    if (durations.size !== model.output.supportedDurationSeconds.length) {
      addIssue(
        issues,
        [...path, "output", "supportedDurationSeconds"],
        "durations must be unique",
      );
    }

    if (model.output.type === "image" && model.output.supportedDurationSeconds.length !== 0) {
      addIssue(
        issues,
        [...path, "output", "supportedDurationSeconds"],
        "image models cannot declare video durations",
      );
    }
    if (model.output.type === "video" && model.output.supportedDurationSeconds.length === 0) {
      addIssue(
        issues,
        [...path, "output", "supportedDurationSeconds"],
        "video models must declare at least one supported duration",
      );
    }

    if (model.input.mediaOnly && (!model.input.image && !model.input.video)) {
      addIssue(issues, [...path, "input", "mediaOnly"], "media-only requires an accepted media input");
    }
    if (model.input.mediaOnly && model.input.prompt === "required") {
      addIssue(
        issues,
        [...path, "input", "mediaOnly"],
        "media-only cannot be true when a prompt is required",
      );
    }
    if (model.mode === "edit-image" && !model.input.image) {
      addIssue(issues, [...path, "input", "image"], "edit-image requires image input support");
    }
    if (model.mode === "edit-video" && !model.input.video) {
      addIssue(issues, [...path, "input", "video"], "edit-video requires video input support");
    }
    if (model.inputHosting.mode === "hosted-public" && model.status === "enabled") {
      if (!model.hostedPublicInputAcknowledged) {
        addIssue(
          issues,
          [...path, "hostedPublicInputAcknowledged"],
          "enabled hosted-public models require an explicit acknowledgement",
        );
      }
    }

    // Every ledger in the direct runtime is denominated in integer US cents.
    // A non-USD enabled model would make the configured dollar caps ambiguous,
    // while a zero reservation would bypass every cap if price evidence were
    // stale or mis-entered. Keep discoveries representable, but require an
    // explicit future conversion/free-model design before either can go live.
    if (model.status === "enabled" && model.pricing.currency !== "USD") {
      addIssue(
        issues,
        [...path, "pricing", "currency"],
        "enabled models require USD pricing because Air spend caps are USD cents",
      );
    }

    if (model.polling.deadlineMs < model.polling.requestTimeoutMs) {
      addIssue(issues, [...path, "polling", "deadlineMs"], "deadline must cover one request timeout");
    }

    const fieldNames = new Set<string>();
    for (const [fieldIndex, field] of model.requiredPayloadFields.entries()) {
      if (fieldNames.has(field.name)) {
        addIssue(
          issues,
          [...path, "requiredPayloadFields", fieldIndex, "name"],
          "provider payload field names must be unique",
        );
      }
      fieldNames.add(field.name);
    }
    const quoteKeys = new Set<string>();
    for (const [quoteIndex, quote] of model.pricing.quotes.entries()) {
      const key = quoteKey(quote.count, quote.durationSeconds);
      if (quoteKeys.has(key)) {
        addIssue(issues, [...path, "pricing", "quotes", quoteIndex], "price quotes must be unique");
      }
      quoteKeys.add(key);

      if (!counts.has(quote.count)) {
        addIssue(
          issues,
          [...path, "pricing", "quotes", quoteIndex, "count"],
          "price quote count is not supported by this model",
        );
      }
      if (model.output.type === "image" && quote.durationSeconds !== null) {
        addIssue(
          issues,
          [...path, "pricing", "quotes", quoteIndex, "durationSeconds"],
          "image price quotes require a null duration",
        );
      }
      if (
        model.output.type === "video" &&
        (quote.durationSeconds === null || !durations.has(quote.durationSeconds))
      ) {
        addIssue(
          issues,
          [...path, "pricing", "quotes", quoteIndex, "durationSeconds"],
          "video quote duration is not supported by this model",
        );
      }
      if (model.status === "enabled" && quote.maximumQuotedCostCents < 1) {
        addIssue(
          issues,
          [...path, "pricing", "quotes", quoteIndex, "maximumQuotedCostCents"],
          "enabled models require a positive worst-case USD-cent reservation",
        );
      }
    }

    for (const count of model.output.supportedOutputCounts) {
      const requiredDurations = model.output.type === "image" ? [null] : model.output.supportedDurationSeconds;
      for (const durationSeconds of requiredDurations) {
        if (!quoteKeys.has(quoteKey(count, durationSeconds))) {
          addIssue(
            issues,
            [...path, "pricing", "quotes"],
            `missing a maximum cost quote for count ${count} and duration ${durationSeconds ?? "none"}`,
          );
        }
      }
    }
  }

  for (const mode of STUDIO_MODES) {
    const slug = registry.defaults[mode];
    if (slug === undefined) continue;
    const model = registry.models.find((candidate) => candidate.slug === slug);
    if (model === undefined) {
      addIssue(issues, ["defaults", mode], "default references no registry model");
      continue;
    }
    if (model.mode !== mode) {
      addIssue(issues, ["defaults", mode], "default model belongs to a different mode");
    }
    if (model.status !== "enabled") {
      addIssue(issues, ["defaults", mode], "default model must be enabled");
    }
    if (!model.output.supportedOutputCounts.includes(1)) {
      addIssue(issues, ["defaults", mode], "default model must support one output");
    }
    if (
      (mode === "animate" || mode === "edit-video") &&
      !model.output.supportedDurationSeconds.includes(5)
    ) {
      addIssue(issues, ["defaults", mode], "video defaults must support five seconds");
    }
  }

  return issues;
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) {
      deepFreeze(child);
    }
    Object.freeze(value);
  }
  return value;
}

/**
 * Parses a registry as untrusted JSON, rejects unknown keys and invalid
 * version/schema combinations, then freezes the accepted committed data.
 */
export function validateModelRegistry(input: unknown): AirModelRegistry {
  const parsed = baseModelRegistrySchema.safeParse(input);
  if (!parsed.success) {
    throw new ModelRegistryValidationError(
      parsed.error.issues.map((issue) => ({
        // Zod represents paths as PropertyKey[]; symbols are not meaningful
        // in a committed JSON registry, so normalize them to their stable
        // textual representation before exposing a validation issue.
        path: issue.path.map(
          (segment): string | number => typeof segment === "symbol" ? String(segment) : segment,
        ),
        message: issue.message,
      })),
    );
  }

  const semanticIssues = collectSemanticIssues(parsed.data);
  if (semanticIssues.length > 0) {
    throw new ModelRegistryValidationError(semanticIssues);
  }

  return deepFreeze(parsed.data);
}

export type RegistryVersionValidation =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly code: "missing" | "mismatch";
      readonly expectedVersion: string;
    };

/** Fail closed when environment/code registry versions do not exactly match. */
export function validateRegistryVersion(
  environmentVersion: string | undefined,
  registry: AirModelRegistry = AIR_MODEL_REGISTRY,
): RegistryVersionValidation {
  if (environmentVersion === undefined || environmentVersion.trim().length === 0) {
    return { ok: false, code: "missing", expectedVersion: registry.registryVersion };
  }
  return environmentVersion === registry.registryVersion
    ? { ok: true }
    : { ok: false, code: "mismatch", expectedVersion: registry.registryVersion };
}

export type ExactModelResolution =
  | { readonly ok: true; readonly model: StudioModel }
  | {
      readonly ok: false;
      readonly code: "unknown_model" | "model_mode_mismatch" | "model_not_enabled";
    };

/** Resolves only the exact requested slug. It intentionally never falls back. */
export function resolveEnabledModel(
  registry: AirModelRegistry,
  mode: StudioMode,
  slug: string,
): ExactModelResolution {
  const model = registry.models.find((candidate) => candidate.slug === slug);
  if (model === undefined) {
    return { ok: false, code: "unknown_model" };
  }
  if (model.mode !== mode) {
    return { ok: false, code: "model_mode_mismatch" };
  }
  if (model.status !== "enabled") {
    return { ok: false, code: "model_not_enabled" };
  }
  return { ok: true, model };
}

export type DefaultModelResolution =
  | { readonly ok: true; readonly model: StudioModel }
  | {
      readonly ok: false;
      readonly code: "no_default_configured" | "default_not_enabled" | "invalid_default";
    };

/** Obtains a deliberately locked default, never the first enabled model. */
export function resolveDefaultModel(
  registry: AirModelRegistry,
  mode: StudioMode,
): DefaultModelResolution {
  const slug = registry.defaults[mode];
  if (slug === undefined) return { ok: false, code: "no_default_configured" };

  const model = registry.models.find((candidate) => candidate.slug === slug);
  if (model === undefined || model.mode !== mode) {
    return { ok: false, code: "invalid_default" };
  }
  if (model.status !== "enabled") {
    return { ok: false, code: "default_not_enabled" };
  }
  return { ok: true, model };
}

/** `/models` and settings UI must only show live, explicitly enabled models. */
export function listEnabledModels(
  registry: AirModelRegistry,
  mode: StudioMode | null = null,
): readonly StudioModel[] {
  return registry.models.filter(
    (model) => model.status === "enabled" && (mode === null || model.mode === mode),
  );
}

export type ModelInputMedia = "none" | "image" | "video";

export type ModelCompatibilityRequest = {
  readonly mode: StudioMode;
  readonly count: StudioOutputCount;
  /** Null is mandatory for image outputs; video outputs require one exact value. */
  readonly durationSeconds: StudioVideoDurationSeconds | null;
  readonly hasPrompt: boolean;
  readonly media: ModelInputMedia;
};

export type ModelCompatibility =
  | { readonly ok: true; readonly maximumQuotedCost: ModelPriceQuote }
  | {
      readonly ok: false;
      readonly code:
        | "model_mode_mismatch"
        | "model_not_enabled"
        | "unsupported_count"
        | "duration_not_supported"
        | "duration_required"
        | "prompt_required"
        | "prompt_not_accepted"
        | "image_not_accepted"
        | "video_not_accepted"
        | "media_required"
        | "input_required"
        | "missing_price_quote";
      readonly acceptedCounts?: readonly StudioOutputCount[];
      readonly acceptedDurations?: readonly StudioVideoDurationSeconds[];
    };

/** Looks up the required price shape exactly. It does not choose a nearby duration or count. */
export function getMaximumQuotedCost(
  model: StudioModel,
  count: StudioOutputCount,
  durationSeconds: StudioVideoDurationSeconds | null,
): ModelPriceQuote | null {
  return (
    model.pricing.quotes.find(
      (quote) => quote.count === count && quote.durationSeconds === durationSeconds,
    ) ?? null
  );
}

/**
 * Checks a requested job/settings snapshot against one exact model. This is a
 * pure compatibility check and never mutates settings or substitutes a model.
 */
export function checkModelCompatibility(
  model: StudioModel,
  request: ModelCompatibilityRequest,
): ModelCompatibility {
  if (model.mode !== request.mode) {
    return { ok: false, code: "model_mode_mismatch" };
  }
  if (model.status !== "enabled") {
    return { ok: false, code: "model_not_enabled" };
  }
  if (!model.output.supportedOutputCounts.includes(request.count)) {
    return {
      ok: false,
      code: "unsupported_count",
      acceptedCounts: model.output.supportedOutputCounts,
    };
  }

  if (model.output.type === "image") {
    if (request.durationSeconds !== null) {
      return { ok: false, code: "duration_not_supported", acceptedDurations: [] };
    }
  } else if (request.durationSeconds === null) {
    return {
      ok: false,
      code: "duration_required",
      acceptedDurations: model.output.supportedDurationSeconds,
    };
  } else if (!model.output.supportedDurationSeconds.includes(request.durationSeconds)) {
    return {
      ok: false,
      code: "duration_not_supported",
      acceptedDurations: model.output.supportedDurationSeconds,
    };
  }

  if (model.input.prompt === "required" && !request.hasPrompt) {
    return { ok: false, code: "prompt_required" };
  }
  if (model.input.prompt === "forbidden" && request.hasPrompt) {
    return { ok: false, code: "prompt_not_accepted" };
  }
  if (request.media === "image" && !model.input.image) {
    return { ok: false, code: "image_not_accepted" };
  }
  if (request.media === "video" && !model.input.video) {
    return { ok: false, code: "video_not_accepted" };
  }
  if (request.media === "none" && model.input.prompt === "forbidden") {
    return { ok: false, code: "media_required" };
  }
  if (!request.hasPrompt && request.media === "none") {
    return { ok: false, code: "input_required" };
  }
  if (!request.hasPrompt && request.media !== "none" && !model.input.mediaOnly) {
    return { ok: false, code: "prompt_required" };
  }

  const quote = getMaximumQuotedCost(model, request.count, request.durationSeconds);
  return quote === null ? { ok: false, code: "missing_price_quote" } : { ok: true, maximumQuotedCost: quote };
}

/**
 * Filters only from explicitly enabled entries using a complete requested
 * shape. It is useful for settings screens and never changes the selection.
 */
export function listCompatibleEnabledModels(
  registry: AirModelRegistry,
  request: ModelCompatibilityRequest,
): readonly StudioModel[] {
  return registry.models.filter(
    (model) => model.mode === request.mode && checkModelCompatibility(model, request).ok,
  );
}

export type PaidRegistryReadiness = {
  readonly ready: boolean;
  readonly missingEnabledModes: readonly StudioMode[];
  readonly missingDefaultModes: readonly StudioMode[];
};

/**
 * A dark-deploy registry can be valid yet not ready for paid admission. This
 * reports that distinction without changing availability or choosing defaults.
 */
export function getPaidRegistryReadiness(registry: AirModelRegistry): PaidRegistryReadiness {
  const missingEnabledModes = STUDIO_MODES.filter(
    (mode) => !registry.models.some((model) => model.mode === mode && model.status === "enabled"),
  );
  const missingDefaultModes = STUDIO_MODES.filter(
    (mode) => !resolveDefaultModel(registry, mode).ok,
  );
  return {
    ready: missingEnabledModes.length === 0 && missingDefaultModes.length === 0,
    missingEnabledModes,
    missingDefaultModes,
  };
}

/**
 * Candidate display labels stay outside the registry until an authenticated
 * catalog and schema verification supplies every required field above.
 */
export type ModelDiscoveryHint = {
  readonly displayName: string;
  readonly mode: StudioMode;
  readonly provider: StudioModelProvider | "unknown";
};

export const UNVERIFIED_MODEL_DISCOVERY_HINTS: readonly ModelDiscoveryHint[] = Object.freeze([
  { displayName: "Grok Imagine", mode: "imagine", provider: "unknown" },
  { displayName: "GPT Image 2", mode: "imagine", provider: "unknown" },
  { displayName: "Reve", mode: "imagine", provider: "unknown" },
  { displayName: "Nano Banana Lite", mode: "imagine", provider: "fal" },
  { displayName: "LTX 2.3 Fast", mode: "animate", provider: "unknown" },
  { displayName: "Grok Imagine", mode: "animate", provider: "unknown" },
  { displayName: "Kling 3.0", mode: "animate", provider: "fal" },
  { displayName: "Seedance 2.0", mode: "animate", provider: "unknown" },
  { displayName: "Nano Banana 2 Edit", mode: "edit-image", provider: "fal" },
  { displayName: "GPT Image 2", mode: "edit-image", provider: "unknown" },
  { displayName: "Seedream Edit", mode: "edit-image", provider: "fal" },
  { displayName: "Bria", mode: "edit-image", provider: "unknown" },
  { displayName: "Grok Edit", mode: "edit-video", provider: "unknown" },
  { displayName: "Wan 2.7 Edit", mode: "edit-video", provider: "unknown" },
  { displayName: "Seedance 2", mode: "edit-video", provider: "unknown" },
  { displayName: "Gemini Omni", mode: "edit-video", provider: "unknown" },
  { displayName: "Happy Horse", mode: "edit-video", provider: "unknown" },
  { displayName: "Kling 03 Edit", mode: "edit-video", provider: "fal" },
]);

/**
 * Zero-spend build default. There are intentionally no provider model IDs,
 * defaults, or enabled entries until a live catalog/schema verification is
 * recorded and a paid-enablement review authorizes the relevant model.
 */
export const AIR_MODEL_REGISTRY: AirModelRegistry = validateModelRegistry({
  schemaVersion: AIR_MODEL_REGISTRY_SCHEMA_VERSION,
  registryVersion: AIR_MODEL_REGISTRY_VERSION,
  models: [],
  defaults: {},
});
