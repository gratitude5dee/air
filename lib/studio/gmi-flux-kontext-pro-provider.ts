import { isStableModelSlug, validateStudioPrompt } from "./commands.js";
import type {
  GmiJsonObject,
  GmiRequestQueueClient,
  GmiRequestStatus,
  GmiSubmitResult,
} from "./gmi-request-queue.js";
import {
  fetchStudioProviderArtifact,
  normalizeStudioProviderArtifactReference,
  StudioProviderArtifactFetchError,
  type StudioProviderArtifactFetchPolicy,
} from "./provider-artifact-fetch.js";
import {
  createStudioProviderResultReference,
  isStudioProviderResultReference,
  openStudioProviderResultReference,
  type StudioProviderResultReference,
} from "./provider-result-reference.js";
import type {
  StudioProviderObservationInput,
  StudioProviderObservationPort,
  StudioProviderObservationResult,
  StudioProviderResultMaterializationInput,
  StudioProviderResultMaterializationPort,
  StudioProviderResultMaterializationResult,
  StudioProviderResultPollResult,
  StudioProviderResultPollingPort,
  StudioProviderSubmitInput,
  StudioProviderSubmitResult,
} from "./provider-port.js";

/**
 * The exact documented GMI Request Queue identifier for the first image-only
 * candidate. This is a provider contract constant, not an enabled registry
 * entry or a statement that catalog/pricing verification has happened.
 */
export const GMI_FLUX_KONTEXT_PRO_MODEL_ID = "flux-kontext-pro" as const;

/**
 * A model registry must bind this adapter key exactly before this provider can
 * be injected into a runtime. It is intentionally more specific than `gmi`.
 */
export const GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY = "gmi.flux-kontext-pro.v1" as const;

/**
 * The narrow injected dependency prevents this adapter from constructing a
 * client, reading credentials, or doing provider discovery as an import side
 * effect. Tests pass a local fake; a future reviewed runtime must explicitly
 * inject an authenticated `GmiRequestQueueClient`.
 */
export type GmiFluxKontextProRequestQueueClient = Pick<GmiRequestQueueClient, "submitRequest">
  & Partial<Pick<GmiRequestQueueClient, "getRequestStatus">>;

/**
 * The result URL policy belongs to a committed model review, not a provider
 * response. Leaving it absent keeps observation fail-closed even though this
 * adapter's submit implementation is available for deterministic tests.
 */
export interface GmiFluxKontextProStudioProviderOptions {
  readonly outputPolicy?: StudioProviderArtifactFetchPolicy | undefined;
  /** Dependency injection keeps result polling and download tests offline. */
  readonly fetch?: typeof globalThis.fetch | undefined;
  /** A model review may choose a shorter bounded artifact-read deadline. */
  readonly artifactFetchTimeoutMs?: number | undefined;
}

/**
 * Prompt-only submission adapter for Flux Kontext Pro through GMI's Request
 * Queue API.
 *
 * This class is deliberately not registered in the model registry or runtime.
 * Its `verified` capability flag means only that its small submission contract
 * is implemented; it cannot enable generation by itself. A future activation
 * still needs live catalog/schema/pricing evidence, an enabled registry entry,
 * an authenticated injected client, and the existing release/cap gates.
 */
export class GmiFluxKontextProStudioProvider implements
  StudioProviderObservationPort,
  StudioProviderResultPollingPort,
  StudioProviderResultMaterializationPort
{
  public readonly verified = true;
  public readonly observationSupported = true;
  public readonly resultPollingSupported = true;
  public readonly resultMaterializationSupported = true;

  public constructor(
    private readonly client: GmiFluxKontextProRequestQueueClient,
    private readonly options: GmiFluxKontextProStudioProviderOptions = {},
  ) {}

  public async submit(input: StudioProviderSubmitInput): Promise<StudioProviderSubmitResult> {
    let payload: GmiJsonObject | null;
    try {
      payload = createPromptOnlyPayload(input);
    } catch {
      // This happens before the provider boundary. Treat malformed local work
      // as a definitive non-submission instead of attempting a best-effort
      // request with an uncertain binding.
      return definitiveRejection();
    }
    if (payload === null) return definitiveRejection();

    try {
      const result = await this.client.submitRequest(Object.freeze({
        model: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
        payload,
      }));
      return mapGmiSubmitResult(result);
    } catch {
      // A client exception can occur after a request reached GMI. Never turn
      // it into a failed paid job or retry it here; the durable executor will
      // route this to reconciliation.
      return ambiguous();
    }
  }

  /**
   * Compatibility bridge for the legacy unbounded observer. Production Cron
   * code can instead persist the sealed result reference returned by
   * `pollResult` and materialize it in a later bounded phase.
   */
  public async observe(input: StudioProviderObservationInput): Promise<StudioProviderObservationResult> {
    const polled = await this.pollResult(input);
    if (polled.kind !== "ready") return polled;
    return await this.materializeResult(Object.freeze({
      job: input.job,
      snapshot: input.snapshot,
      reference: polled.reference,
    }));
  }

  /**
   * Reads only the known provider request status. On success it seals the
   * reviewed retrieval capability without fetching media bytes or touching a
   * Photon transport boundary.
   */
  public async pollResult(input: StudioProviderObservationInput): Promise<StudioProviderResultPollResult> {
    if (!hasExactFluxKontextProObservationBinding(input)) return reconciliationRequired();
    if (!this.options.outputPolicy || typeof this.client.getRequestStatus !== "function") {
      return reconciliationRequired();
    }

    let observation: Awaited<ReturnType<NonNullable<GmiFluxKontextProRequestQueueClient["getRequestStatus"]>>>;
    try {
      observation = await this.client.getRequestStatus(input.providerRequestId);
    } catch {
      // A status GET is safe to retry under the store's bounded poll policy;
      // do not retain a provider error or URL in a durable record.
      return retryableFailure();
    }

    // The request ID binds the observation to one opaque provider job, while
    // the exact model identity binds its state semantics to this adapter. Do
    // not let a malformed or cross-model response terminalize, delay, or
    // deliver a Flux job merely because it reused the request ID.
    if (
      !observation ||
      observation.requestId !== input.providerRequestId ||
      !hasExactFluxKontextProResponseModel(observation.response)
    ) {
      return reconciliationRequired();
    }
    switch (observation.status) {
      case "dispatched":
      case "processing":
        return pending();
      case "failed":
      case "cancelled":
        return definitiveFailure();
      case "unknown":
        return reconciliationRequired();
      case "success":
        break;
    }

    const resultUrl = extractPrivateFluxOutputUrl(observation.response);
    if (resultUrl === null) return reconciliationRequired();

    try {
      // Do the same strict host/path/signature validation that materialization
      // will repeat later, but never issue a GET during this status phase.
      const url = normalizeStudioProviderArtifactReference({
        url: resultUrl,
        policy: this.options.outputPolicy,
      });
      const reference = createStudioProviderResultReference({
        jobId: input.job.jobId,
        provider: "gmi",
        adapterKey: GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY,
        providerModelId: GMI_FLUX_KONTEXT_PRO_MODEL_ID,
        resultSchemaFingerprint: input.snapshot.model.resultSchemaFingerprint,
        outputs: Object.freeze([Object.freeze({
          outputIndex: 1 as const,
          mediaType: "image" as const,
        })]),
        material: Object.freeze({ kind: "signed-url" as const, url }),
      });
      return Object.freeze({
        kind: "ready" as const,
        reference,
      });
    } catch {
      return reconciliationRequired();
    }
  }

  /**
   * Downloads one previously sealed private result. It deliberately has no
   * access to a provider request ID and cannot issue a status poll or submit
   * another billable request.
   */
  public async materializeResult(
    input: StudioProviderResultMaterializationInput,
  ): Promise<StudioProviderResultMaterializationResult> {
    if (!hasExactFluxKontextProExecutionBinding(input) || !this.options.outputPolicy) {
      return reconciliationRequired();
    }
    if (!hasExactFluxKontextProResultReference(input.reference, input)) {
      return reconciliationRequired();
    }
    const material = openStudioProviderResultReference(input.reference);
    if (material?.kind !== "signed-url") return reconciliationRequired();

    try {
      const artifact = await fetchStudioProviderArtifact({
        url: material.url,
        policy: this.options.outputPolicy,
        ...(this.options.fetch === undefined ? {} : { fetch: this.options.fetch }),
        ...(this.options.artifactFetchTimeoutMs === undefined
          ? {}
          : { timeoutMs: this.options.artifactFetchTimeoutMs }),
      });
      return Object.freeze({
        kind: "ready" as const,
        artifacts: Object.freeze([
          Object.freeze({
            outputIndex: 1 as const,
            mediaType: "image" as const,
            ...(artifact.declaredMimeType === undefined ? {} : { declaredMimeType: artifact.declaredMimeType }),
            stream: artifact.stream,
          }),
        ]),
      });
    } catch (error) {
      return isRetryableArtifactFetchError(error) ? retryableFailure() : reconciliationRequired();
    }
  }
}

/** A named factory keeps future composition explicit and test-friendly. */
export function createGmiFluxKontextProStudioProvider(
  client: GmiFluxKontextProRequestQueueClient,
  options?: GmiFluxKontextProStudioProviderOptions,
): GmiFluxKontextProStudioProvider {
  return new GmiFluxKontextProStudioProvider(client, options);
}

/**
 * The public model contract permits optional controls, but Air deliberately
 * sends none until each has separately passed live schema review. The payload
 * therefore contains exactly one user-controlled field: the validated prompt.
 */
function createPromptOnlyPayload(input: StudioProviderSubmitInput): GmiJsonObject | null {
  if (!hasExactFluxKontextProBinding(input) || input.request.input !== undefined) return null;

  const prompt = validateStudioPrompt(input.request.prompt);
  if (!prompt.ok) return null;
  return Object.freeze({ prompt: prompt.prompt });
}

/**
 * This duplicates the crucial immutable fields at the final provider
 * boundary. It does not invent catalog, schema, or pricing fingerprints: it
 * only requires the already-admitted snapshot to carry canonical evidence and
 * to name this exact provider/model/adapter combination.
 */
function hasExactFluxKontextProBinding(input: StudioProviderSubmitInput): boolean {
  return hasExactFluxKontextProExecutionBinding(input) && isOpaqueSubmissionIntentId(input.submissionIntentId);
}

/** Observation has no submit intent, but must retain every other exact binding. */
function hasExactFluxKontextProObservationBinding(input: StudioProviderObservationInput): boolean {
  return isGmiProviderRequestId(input.providerRequestId) && hasExactFluxKontextProExecutionBinding(input);
}

function hasExactFluxKontextProExecutionBinding(
  input: Pick<StudioProviderSubmitInput, "job" | "snapshot"> | StudioProviderObservationInput | StudioProviderResultMaterializationInput,
): boolean {
  const { job, snapshot } = input;
  const { model, output, workflow } = snapshot;

  return job.mode === "imagine"
    && job.provider === "gmi"
    && isStableModelSlug(job.modelSlug)
    && job.outputCount === 1
    && job.durationSeconds === undefined
    && isPositiveSafeInteger(job.reservedCents)
    && snapshot.schemaVersion === 2
    && workflow.workflowId === "air:imagine:v1"
    && workflow.workflowVersion === 1
    && workflow.stepId === "provider-generation"
    && model.slug === job.modelSlug
    && model.provider === "gmi"
    && model.providerModelId === GMI_FLUX_KONTEXT_PRO_MODEL_ID
    && model.adapterKey === GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY
    && isCanonicalFingerprint(model.catalogFingerprint)
    && isCanonicalFingerprint(model.requestSchemaFingerprint)
    && isCanonicalFingerprint(model.resultSchemaFingerprint)
    && isCanonicalFingerprint(model.pricingEvidenceFingerprint)
    && output.type === "image"
    && output.count === 1
    && output.durationSeconds === undefined
    && snapshot.maximumQuotedCostCents === job.reservedCents
    && isPositiveSafeInteger(snapshot.maximumQuotedCostCents);
}

function hasExactFluxKontextProResultReference(
  reference: StudioProviderResultReference,
  input: StudioProviderResultMaterializationInput,
): boolean {
  if (!isStudioProviderResultReference(reference)) return false;
  const output = reference.outputs[0];
  return reference.jobId === input.job.jobId
    && reference.provider === "gmi"
    && reference.adapterKey === GMI_FLUX_KONTEXT_PRO_ADAPTER_KEY
    && reference.providerModelId === GMI_FLUX_KONTEXT_PRO_MODEL_ID
    && reference.resultSchemaFingerprint === input.snapshot.model.resultSchemaFingerprint
    && reference.outputCount === 1
    && reference.outputs.length === 1
    && output?.outputIndex === 1
    && output.mediaType === "image";
}

/**
 * The documented Flux status response is intentionally parsed as a narrow
 * protocol. `is_public: false`, one zero-indexed image, and the exact model
 * ID are all required before Air is even allowed to inspect the result URL.
 */
function extractPrivateFluxOutputUrl(response: GmiJsonObject): string | null {
  if (!isPlainJsonObject(response)) return null;
  if (!hasExactFluxKontextProResponseModel(response) || response.is_public !== false) return null;
  const outcome = response.outcome;
  if (!isPlainJsonObject(outcome)) return null;
  const mediaUrls = outcome.media_urls;
  if (!Array.isArray(mediaUrls) || mediaUrls.length !== 1) return null;
  const first = mediaUrls[0];
  if (!isPlainJsonObject(first) || first.id !== "0" || typeof first.url !== "string") return null;
  return first.url;
}

function hasExactFluxKontextProResponseModel(response: GmiJsonObject): boolean {
  return isPlainJsonObject(response) && response.model === GMI_FLUX_KONTEXT_PRO_MODEL_ID;
}

function isPlainJsonObject(value: unknown): value is GmiJsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isRetryableArtifactFetchError(error: unknown): boolean {
  if (!(error instanceof StudioProviderArtifactFetchError)) return false;
  switch (error.code) {
    case "request_timeout":
    case "transport_failure":
    case "unexpected_status":
    case "response_body_unavailable":
    case "response_stream_failure":
    case "response_stream_too_large":
      return true;
    default:
      return false;
  }
}

function mapGmiSubmitResult(result: GmiSubmitResult): StudioProviderSubmitResult {
  if (!result || typeof result !== "object" || Array.isArray(result)) return ambiguous();

  switch (result.classification) {
    case "accepted":
      return result.model === GMI_FLUX_KONTEXT_PRO_MODEL_ID
        && isGmiProviderRequestId(result.requestId)
        && isGmiRequestStatus(result.status)
        ? Object.freeze({ kind: "accepted", providerRequestId: result.requestId })
        : ambiguous();
    case "definitive_rejection":
      // The queue client only exposes a definitive rejection for a non-429
      // 4xx. Preserve that distinction if a future injected implementation is
      // malformed rather than silently terminalizing a possibly accepted job.
      return isDefinitiveGmiRejectionStatus(result.httpStatus)
        ? definitiveRejection()
        : ambiguous();
    case "ambiguous":
      return ambiguous();
    default:
      return ambiguous();
  }
}

function definitiveRejection(): StudioProviderSubmitResult {
  return Object.freeze({ kind: "definitive_rejection" });
}

function ambiguous(): StudioProviderSubmitResult {
  return Object.freeze({ kind: "ambiguous" });
}

function pending(): Readonly<{ readonly kind: "pending" }> {
  return Object.freeze({ kind: "pending" });
}

function definitiveFailure(): Readonly<{ readonly kind: "definitive_failure" }> {
  return Object.freeze({ kind: "definitive_failure" });
}

function retryableFailure(): Readonly<{ readonly kind: "retryable_failure" }> {
  return Object.freeze({ kind: "retryable_failure" });
}

function reconciliationRequired(): Readonly<{ readonly kind: "reconciliation_required" }> {
  return Object.freeze({ kind: "reconciliation_required" });
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isOpaqueSubmissionIntentId(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/u.test(value);
}

function isCanonicalFingerprint(value: unknown): value is string {
  return typeof value === "string" && /^sha256:[a-f0-9]{64}$/u.test(value);
}

function isGmiProviderRequestId(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$/u.test(value);
}

function isGmiRequestStatus(value: unknown): value is GmiRequestStatus {
  return value === "dispatched"
    || value === "processing"
    || value === "success"
    || value === "failed"
    || value === "cancelled"
    || value === "unknown";
}

function isDefinitiveGmiRejectionStatus(value: unknown): value is number {
  return typeof value === "number"
    && Number.isInteger(value)
    && value >= 400
    && value < 500
    && value !== 429;
}
