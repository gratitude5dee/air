import { isStableModelSlug, validateStudioPrompt } from "./commands.js";
import type {
  GmiJsonObject,
  GmiRequestQueueClient,
  GmiSubmitResult,
} from "./gmi-request-queue.js";
import type {
  StudioProviderPort,
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
export type GmiFluxKontextProRequestQueueClient = Pick<GmiRequestQueueClient, "submitRequest">;

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
export class GmiFluxKontextProStudioProvider implements StudioProviderPort {
  public readonly verified = true;

  public constructor(private readonly client: GmiFluxKontextProRequestQueueClient) {}

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
}

/** A named factory keeps future composition explicit and test-friendly. */
export function createGmiFluxKontextProStudioProvider(
  client: GmiFluxKontextProRequestQueueClient,
): GmiFluxKontextProStudioProvider {
  return new GmiFluxKontextProStudioProvider(client);
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
  const { job, snapshot, submissionIntentId } = input;
  const { model, output, workflow } = snapshot;

  return job.mode === "imagine"
    && job.provider === "gmi"
    && isStableModelSlug(job.modelSlug)
    && job.outputCount === 1
    && job.durationSeconds === undefined
    && isPositiveSafeInteger(job.reservedCents)
    && isOpaqueSubmissionIntentId(submissionIntentId)
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

function mapGmiSubmitResult(result: GmiSubmitResult): StudioProviderSubmitResult {
  if (!result || typeof result !== "object" || Array.isArray(result)) return ambiguous();

  switch (result.classification) {
    case "accepted":
      return isGmiProviderRequestId(result.requestId)
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

function isDefinitiveGmiRejectionStatus(value: unknown): value is number {
  return typeof value === "number"
    && Number.isInteger(value)
    && value >= 400
    && value < 500
    && value !== 429;
}
