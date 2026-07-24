import type {
  StudioOutputIndex,
  StudioProviderArtifactMediaType,
} from "./artifact-validation.js";

/**
 * A status poll may learn a private provider retrieval capability before Air
 * is ready to download it. This public shape deliberately contains only the
 * immutable binding and output shape; the provider URL/token stays in a
 * module-private WeakMap until a reviewed materializer opens it.
 */
export interface StudioProviderResultReference {
  readonly source: "air-sealed-provider-result-reference";
  readonly schemaVersion: 1;
  /** Opaque Air job identity; prevents a sealed result from crossing jobs. */
  readonly jobId: string;
  readonly provider: "gmi" | "fal";
  readonly adapterKey: string;
  readonly providerModelId: string;
  readonly resultSchemaFingerprint: string;
  readonly outputCount: number;
  readonly outputs: readonly StudioProviderResultReferenceOutput[];
}

export interface StudioProviderResultReferenceOutput {
  readonly outputIndex: StudioOutputIndex;
  readonly mediaType: StudioProviderArtifactMediaType;
}

/**
 * This is intentionally available only through
 * `openStudioProviderResultReference`. Callers must keep it in memory and
 * must never serialize, log, or persist it without a dedicated encrypted
 * result-reference store boundary.
 */
export interface StudioProviderResultReferenceMaterial {
  readonly kind: "signed-url" | "provider-private";
  readonly url: string;
}

export interface CreateStudioProviderResultReferenceInput {
  readonly jobId: string;
  readonly provider: "gmi" | "fal";
  readonly adapterKey: string;
  readonly providerModelId: string;
  readonly resultSchemaFingerprint: string;
  readonly outputs: readonly StudioProviderResultReferenceOutput[];
  readonly material: StudioProviderResultReferenceMaterial;
}

const references = new WeakSet<object>();
const materialByReference = new WeakMap<StudioProviderResultReference, StudioProviderResultReferenceMaterial>();

/**
 * Builds a non-forgeable in-process capability. A future durable store can
 * encrypt the material through this module, but no ordinary provider-port
 * result, log serializer, or outbox payload can observe the retrieval URL.
 */
export function createStudioProviderResultReference(
  input: CreateStudioProviderResultReferenceInput,
): StudioProviderResultReference {
  if (!isCreateInput(input)) throw new Error("invalid_provider_result_reference");

  const outputs = Object.freeze(input.outputs.map((output) => Object.freeze({
    outputIndex: output.outputIndex,
    mediaType: output.mediaType,
  })));
  const reference: StudioProviderResultReference = Object.freeze({
    source: "air-sealed-provider-result-reference" as const,
    schemaVersion: 1 as const,
    jobId: input.jobId,
    provider: input.provider,
    adapterKey: input.adapterKey,
    providerModelId: input.providerModelId,
    resultSchemaFingerprint: input.resultSchemaFingerprint,
    outputCount: outputs.length,
    outputs,
  });
  const material = Object.freeze({
    kind: input.material.kind,
    url: input.material.url,
  });
  references.add(reference);
  materialByReference.set(reference, material);
  return reference;
}

/** A copied, deserialized, or manually shaped object is never a capability. */
export function isStudioProviderResultReference(value: unknown): value is StudioProviderResultReference {
  return typeof value === "object" && value !== null && references.has(value);
}

/**
 * Opens a sealed reference only at the dedicated provider materialization
 * boundary. It returns `null` for forged/copy-shaped values instead of
 * exposing an error that might include a provider retrieval capability.
 */
export function openStudioProviderResultReference(
  reference: unknown,
): StudioProviderResultReferenceMaterial | null {
  if (!isStudioProviderResultReference(reference)) return null;
  return materialByReference.get(reference) ?? null;
}

function isCreateInput(value: CreateStudioProviderResultReferenceInput): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (!isSafeBinding(value.jobId, 160)) return false;
  if (value.provider !== "gmi" && value.provider !== "fal") return false;
  if (!isSafeBinding(value.adapterKey, 160) || !isSafeBinding(value.providerModelId, 512)) return false;
  if (!/^sha256:[a-f0-9]{64}$/u.test(value.resultSchemaFingerprint)) return false;
  if (!Array.isArray(value.outputs) || value.outputs.length < 1 || value.outputs.length > 4) return false;
  if (!value.material || typeof value.material !== "object" || Array.isArray(value.material)) return false;
  if (
    (value.material.kind !== "signed-url" && value.material.kind !== "provider-private")
    || !isSafeUrlMaterial(value.material.url)
  ) return false;

  return value.outputs.every((output, offset) =>
    isReferenceOutput(output) && output.outputIndex === offset + 1,
  );
}

function isReferenceOutput(value: unknown): value is StudioProviderResultReferenceOutput {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const output = value as Partial<StudioProviderResultReferenceOutput>;
  return (output.outputIndex === 1 || output.outputIndex === 2 || output.outputIndex === 3 || output.outputIndex === 4)
    && (output.mediaType === "image" || output.mediaType === "video");
}

function isSafeBinding(value: unknown, maximumLength: number): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= maximumLength
    && value.trim() === value
    && !/[\u0000\r\n]/u.test(value);
}

function isSafeUrlMaterial(value: unknown): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= 8_192
    && !/[\u0000\r\n]/u.test(value);
}
