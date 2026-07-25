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
 * The immutable identity that binds a private result capability to exactly
 * one admitted provider operation. A durable store reconstructs this from
 * its job + execution snapshot before it restores an encrypted reference.
 */
export interface StudioProviderResultReferenceBinding {
  readonly jobId: string;
  readonly provider: "gmi" | "fal";
  readonly adapterKey: string;
  readonly providerModelId: string;
  readonly resultSchemaFingerprint: string;
  readonly outputCount: number;
  readonly outputs: readonly StudioProviderResultReferenceOutput[];
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

/**
 * Plaintext input to the store's existing encryption boundary. It is the
 * only shape that may cross an invocation boundary, and must be encrypted as
 * one opaque value; it is never job/outbox metadata or a log payload.
 */
export interface StudioProviderResultReferencePersistencePayload {
  readonly source: "air-provider-result-reference-persistence";
  readonly schemaVersion: 1;
  readonly binding: StudioProviderResultReferenceBinding;
  readonly material: StudioProviderResultReferenceMaterial;
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

/**
 * Extracts a sealed in-process capability into the explicit payload intended
 * for the store's encrypted reference record. It never serializes a WeakMap
 * object, and copied/deserialized public reference shapes cannot be encoded.
 */
export function encodeStudioProviderResultReferenceForPersistence(
  reference: unknown,
): StudioProviderResultReferencePersistencePayload | null {
  if (!isStudioProviderResultReference(reference)) return null;
  const material = materialByReference.get(reference);
  if (material === undefined) return null;

  return freezePersistencePayload({
    source: "air-provider-result-reference-persistence",
    schemaVersion: 1,
    binding: bindingFromReference(reference),
    material,
  });
}

/**
 * Rehydrates a fresh in-process capability after the store decrypts a
 * persistence payload. Restoration is fail-closed: malformed encrypted data
 * and a mismatch with the caller's independently reconstructed binding both
 * return null, leaving the job for reconciliation.
 */
export function restoreStudioProviderResultReferenceFromPersistence(
  value: unknown,
  expectedBinding: unknown,
): StudioProviderResultReference | null {
  if (!isPersistencePayload(value) || !isBinding(expectedBinding)) return null;
  if (!bindingsEqual(value.binding, expectedBinding)) return null;

  try {
    return createStudioProviderResultReference({
      jobId: value.binding.jobId,
      provider: value.binding.provider,
      adapterKey: value.binding.adapterKey,
      providerModelId: value.binding.providerModelId,
      resultSchemaFingerprint: value.binding.resultSchemaFingerprint,
      outputs: value.binding.outputs,
      material: value.material,
    });
  } catch {
    return null;
  }
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

function bindingFromReference(reference: StudioProviderResultReference): StudioProviderResultReferenceBinding {
  return freezeBinding({
    jobId: reference.jobId,
    provider: reference.provider,
    adapterKey: reference.adapterKey,
    providerModelId: reference.providerModelId,
    resultSchemaFingerprint: reference.resultSchemaFingerprint,
    outputCount: reference.outputCount,
    outputs: reference.outputs,
  });
}

function freezePersistencePayload(
  payload: StudioProviderResultReferencePersistencePayload,
): StudioProviderResultReferencePersistencePayload {
  return Object.freeze({
    source: "air-provider-result-reference-persistence" as const,
    schemaVersion: 1 as const,
    binding: freezeBinding(payload.binding),
    material: Object.freeze({
      kind: payload.material.kind,
      url: payload.material.url,
    }),
  });
}

function freezeBinding(binding: StudioProviderResultReferenceBinding): StudioProviderResultReferenceBinding {
  return Object.freeze({
    jobId: binding.jobId,
    provider: binding.provider,
    adapterKey: binding.adapterKey,
    providerModelId: binding.providerModelId,
    resultSchemaFingerprint: binding.resultSchemaFingerprint,
    outputCount: binding.outputCount,
    outputs: Object.freeze(binding.outputs.map((output) => Object.freeze({
      outputIndex: output.outputIndex,
      mediaType: output.mediaType,
    }))),
  });
}

function isPersistencePayload(value: unknown): value is StudioProviderResultReferencePersistencePayload {
  if (!isPlainObject(value)) return false;
  if (
    value.source !== "air-provider-result-reference-persistence"
    || value.schemaVersion !== 1
    || !isBinding(value.binding)
    || !isMaterial(value.material)
  ) return false;

  return hasExactKeys(value, ["source", "schemaVersion", "binding", "material"])
    && hasExactKeys(value.binding, [
      "jobId",
      "provider",
      "adapterKey",
      "providerModelId",
      "resultSchemaFingerprint",
      "outputCount",
      "outputs",
    ])
    && value.binding.outputs.every((output) => hasExactKeys(output, ["outputIndex", "mediaType"]))
    && hasExactKeys(value.material, ["kind", "url"]);
}

function isBinding(value: unknown): value is StudioProviderResultReferenceBinding {
  if (!isPlainObject(value)) return false;
  const fingerprint = value.resultSchemaFingerprint;
  if (
    !isSafeBinding(value.jobId, 160)
    || (value.provider !== "gmi" && value.provider !== "fal")
    || !isSafeBinding(value.adapterKey, 160)
    || !isSafeBinding(value.providerModelId, 512)
    || !isSafeBinding(fingerprint, 71)
    || !/^sha256:[a-f0-9]{64}$/u.test(fingerprint)
    || !Number.isSafeInteger(value.outputCount)
    || !Array.isArray(value.outputs)
    || value.outputCount !== value.outputs.length
    || value.outputs.length < 1
    || value.outputs.length > 4
  ) return false;

  return value.outputs.every((output, offset) =>
    isReferenceOutput(output) && output.outputIndex === offset + 1,
  );
}

function isMaterial(value: unknown): value is StudioProviderResultReferenceMaterial {
  if (!isPlainObject(value)) return false;
  return (value.kind === "signed-url" || value.kind === "provider-private")
    && isSafeUrlMaterial(value.url);
}

function bindingsEqual(
  left: StudioProviderResultReferenceBinding,
  right: StudioProviderResultReferenceBinding,
): boolean {
  return left.jobId === right.jobId
    && left.provider === right.provider
    && left.adapterKey === right.adapterKey
    && left.providerModelId === right.providerModelId
    && left.resultSchemaFingerprint === right.resultSchemaFingerprint
    && left.outputCount === right.outputCount
    && left.outputs.length === right.outputs.length
    && left.outputs.every((output, index) => {
      const expected = right.outputs[index];
      return expected !== undefined
        && output.outputIndex === expected.outputIndex
        && output.mediaType === expected.mediaType;
    });
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasExactKeys(value: object, keys: readonly string[]): boolean {
  const actual = Object.keys(value);
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key));
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
