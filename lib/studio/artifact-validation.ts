import {
  validateStudioImageMetadata,
  validateStudioMp4Metadata,
  type StudioImageMetadata,
  type StudioMp4Metadata,
} from "./media-validation.js";

/**
 * A provider result is never allowed to occupy more memory than Air can
 * validate and hand directly to the private iMessage transport. This matches
 * both launch output media contracts (images and MP4) without trusting a
 * provider's advertised content length.
 */
export const STUDIO_PROVIDER_ARTIFACT_MAX_BYTES = 25 * 1024 * 1024;
/** Prevent an infinite sequence of empty/tiny chunks from consuming a worker. */
export const STUDIO_PROVIDER_ARTIFACT_MAX_CHUNKS = 4_096;

/** The launch registry and delivery cursor both support at most four outputs. */
export const STUDIO_OUTPUT_INDEXES = Object.freeze([1, 2, 3, 4] as const);
export type StudioOutputIndex = (typeof STUDIO_OUTPUT_INDEXES)[number];

export type StudioProviderArtifactMediaType = "image" | "video";

/**
 * A byte source obtained through a provider adapter. It intentionally carries
 * no URL, provider file name, or other reference that could escape into logs
 * or an iMessage fallback.
 */
export type StudioProviderArtifactByteStream = ReadableStream<Uint8Array> | AsyncIterable<Uint8Array>;

export interface ValidateStudioProviderArtifactInput {
  /** The immutable model registry's expected result type, never provider input. */
  readonly expectedMediaType: StudioProviderArtifactMediaType;
  /** Provider-declared type is only a consistency check; bytes remain authoritative. */
  readonly declaredMimeType?: string | null | undefined;
  /** A provider result body; it may not be buffered or forwarded directly. */
  readonly stream: StudioProviderArtifactByteStream | null | undefined;
  /** One-based and durable so result delivery can resume at the next output. */
  readonly outputIndex: StudioOutputIndex;
}

/**
 * The only artifact shape that may cross from provider observation into the
 * outbound attachment path. `source` deliberately prevents an inbound
 * `StudioAttachmentReference` from being structurally substituted here.
 */
export interface StudioValidatedProviderArtifact {
  readonly source: "air-validated-provider-output";
  readonly outputIndex: StudioOutputIndex;
  readonly mediaType: StudioProviderArtifactMediaType;
  /** Air-created filename; no provider-owned name or URL is retained. */
  readonly filename: string;
  /** Canonical type inferred from validated bytes, not a provider claim. */
  readonly mimeType: "image/jpeg" | "image/png" | "video/mp4";
  readonly byteLength: number;
  /**
   * An inspectable defensive copy. It is intentionally never the source for
   * an outbound upload: the module retains an opaque canonical snapshot and
   * makes a fresh copy immediately before Photon receives bytes.
   */
  readonly bytes: Uint8Array;
  readonly metadata: StudioImageMetadata | StudioMp4Metadata;
}

/** The only byte payload that the Photon adapter may upload. */
export interface StudioPreparedProviderArtifactUpload {
  readonly filename: string;
  /** Freshly copied and revalidated canonical bytes. Do not retain or reuse. */
  readonly bytes: Uint8Array;
}

export type StudioArtifactValidationCode =
  | "artifact_stream_unavailable"
  | "artifact_stream_invalid"
  | "artifact_stream_too_fragmented"
  | "artifact_stream_too_large"
  | "artifact_not_validated"
  | "artifact_revalidation_failed"
  | "invalid_output_index"
  | "invalid_expected_media_type"
  | "declared_mime_type_mismatch";

/**
 * These registries are deliberately module-private. A public discriminator
 * can be copied with an object spread, while object identity in a WeakSet
 * cannot be forged by an inbound webhook payload or provider response.
 */
const validatedProviderArtifacts = new WeakSet<StudioValidatedProviderArtifact>();
const canonicalBytesByValidatedArtifact = new WeakMap<StudioValidatedProviderArtifact, Uint8Array>();

/** A stable, provider-body-free failure code for stream/boundary failures. */
export class StudioArtifactValidationError extends Error {
  public constructor(readonly code: StudioArtifactValidationCode) {
    super(code);
    this.name = "StudioArtifactValidationError";
  }
}

/**
 * Copies a provider stream in bounded chunks. The returned bytes share no
 * ArrayBuffer with the source, so a provider implementation cannot mutate a
 * validated attachment after yielding it. Stream errors are intentionally
 * collapsed to a non-sensitive code.
 */
export async function readBoundedStudioProviderArtifact(
  stream: StudioProviderArtifactByteStream | null | undefined,
): Promise<Uint8Array> {
  if (stream === null || stream === undefined) fail("artifact_stream_unavailable");

  const collector = new StudioArtifactCollector();
  if (isReadableStream(stream)) {
    return readReadableStream(stream, collector);
  }
  if (isAsyncIterable(stream)) {
    return readAsyncIterable(stream, collector);
  }
  fail("artifact_stream_invalid");
}

/**
 * Validates a complete private provider result and assigns deterministic,
 * Air-owned output metadata. This function does not fetch a URL, invoke a
 * provider, write storage, or contact Photon.
 */
export async function validateStudioProviderArtifact(
  input: ValidateStudioProviderArtifactInput,
): Promise<StudioValidatedProviderArtifact> {
  const outputIndex = validateOutputIndex(input.outputIndex);
  const expectedMediaType = validateExpectedMediaType(input.expectedMediaType);
  const declaredMimeType = normalizeDeclaredMimeType(input.declaredMimeType);
  const bytes = await readBoundedStudioProviderArtifact(input.stream);

  if (expectedMediaType === "image") {
    const metadata = validateStudioImageMetadata(bytes, {
      role: "output",
      ...(declaredMimeType === undefined
        ? {}
        : { mimeType: declaredMimeType }),
    });
    return freezeArtifact({
      outputIndex,
      mediaType: "image",
      filename: airOwnedFilename(outputIndex, metadata.mimeType),
      mimeType: metadata.mimeType,
      byteLength: metadata.byteLength,
      bytes,
      metadata,
    });
  }

  if (!isAbsentOrCanonicalMp4MimeType(declaredMimeType)) {
    fail("declared_mime_type_mismatch");
  }
  const metadata = validateStudioMp4Metadata(bytes);
  return freezeArtifact({
    outputIndex,
    mediaType: "video",
    filename: airOwnedFilename(outputIndex, metadata.mimeType),
    mimeType: metadata.mimeType,
    byteLength: metadata.byteLength,
    bytes,
    metadata,
  });
}

/** A runtime guard for boundaries that deserialize an output index. */
export function isStudioOutputIndex(value: unknown): value is StudioOutputIndex {
  return value === 1 || value === 2 || value === 3 || value === 4;
}

/**
 * Runtime boundary guard for the attachment transport. In addition to the
 * canonical shape, it requires module-private validation provenance. A public
 * source discriminator alone is never proof that a payload was validated.
 */
export function isStudioValidatedProviderArtifact(value: unknown): value is StudioValidatedProviderArtifact {
  if (typeof value !== "object" || value === null) return false;
  if (!validatedProviderArtifacts.has(value as StudioValidatedProviderArtifact)) return false;
  const artifact = value as Partial<StudioValidatedProviderArtifact>;
  if (
    artifact.source !== "air-validated-provider-output" ||
    !isStudioOutputIndex(artifact.outputIndex) ||
    (artifact.mediaType !== "image" && artifact.mediaType !== "video") ||
    !(artifact.bytes instanceof Uint8Array) ||
    !Number.isSafeInteger(artifact.byteLength) ||
    artifact.byteLength !== artifact.bytes.byteLength ||
    artifact.byteLength < 1 ||
    artifact.byteLength > STUDIO_PROVIDER_ARTIFACT_MAX_BYTES ||
    typeof artifact.filename !== "string" ||
    !isArtifactMetadata(artifact.metadata, artifact.mimeType, artifact.byteLength)
  ) {
    return false;
  }
  const mimeType = artifact.mimeType;
  if (artifact.mediaType === "image" && mimeType !== "image/jpeg" && mimeType !== "image/png") {
    return false;
  }
  if (artifact.mediaType === "video" && mimeType !== "video/mp4") return false;
  if (mimeType !== "image/jpeg" && mimeType !== "image/png" && mimeType !== "video/mp4") return false;
  return artifact.filename === airOwnedFilename(artifact.outputIndex, mimeType);
}

/**
 * Makes the final private attachment payload. This boundary accepts only an
 * artifact created by this module, copies its unexposed canonical bytes, and
 * proves the copy still matches the Air-owned metadata before any outbound
 * RPC. The inspectable `artifact.bytes` value is never forwarded to Photon.
 *
 * A caller can mutate a public Uint8Array after validation; that mutation
 * cannot alter what is sent. The returned bytes are a one-use payload owned by
 * the transport, so this function must remain adjacent to the upload call.
 */
export function copyAndRevalidateStudioProviderArtifactForUpload(
  value: unknown,
): StudioPreparedProviderArtifactUpload {
  if (!isStudioValidatedProviderArtifact(value)) fail("artifact_not_validated");

  const canonicalBytes = canonicalBytesByValidatedArtifact.get(value);
  if (canonicalBytes === undefined) fail("artifact_not_validated");

  const bytes = copyArtifactBytes(canonicalBytes);
  try {
    const metadata = validateCanonicalArtifactBytes(value.mediaType, bytes);
    if (!hasCanonicalArtifactMetadata(value, metadata)) fail("artifact_revalidation_failed");
  } catch (error) {
    if (error instanceof StudioArtifactValidationError) throw error;
    // No provider-originating validation detail crosses the Photon boundary.
    fail("artifact_revalidation_failed");
  }

  return Object.freeze({ filename: value.filename, bytes });
}

function validateOutputIndex(value: unknown): StudioOutputIndex {
  if (!isStudioOutputIndex(value)) fail("invalid_output_index");
  return value;
}

function validateExpectedMediaType(value: unknown): StudioProviderArtifactMediaType {
  if (value !== "image" && value !== "video") fail("invalid_expected_media_type");
  return value;
}

function freezeArtifact(input: Omit<StudioValidatedProviderArtifact, "source">): StudioValidatedProviderArtifact {
  // `input.bytes` was created by the bounded collector and has no external
  // alias. Keep it module-private, then expose a separate inspectable copy.
  // Never freeze a non-empty typed array: JavaScript rejects that operation.
  const canonicalBytes = input.bytes;
  const artifact: StudioValidatedProviderArtifact = Object.freeze({
    source: "air-validated-provider-output" as const,
    ...input,
    bytes: copyArtifactBytes(canonicalBytes),
  });
  canonicalBytesByValidatedArtifact.set(artifact, canonicalBytes);
  validatedProviderArtifacts.add(artifact);
  return artifact;
}

function copyArtifactBytes(source: Uint8Array): Uint8Array {
  if (source.byteLength < 1 || source.byteLength > STUDIO_PROVIDER_ARTIFACT_MAX_BYTES) {
    fail("artifact_revalidation_failed");
  }
  try {
    const copy = new Uint8Array(source.byteLength);
    copy.set(source);
    return copy;
  } catch {
    // A detached or otherwise invalid typed array cannot become an upload.
    fail("artifact_revalidation_failed");
  }
}

function validateCanonicalArtifactBytes(
  mediaType: StudioProviderArtifactMediaType,
  bytes: Uint8Array,
): StudioImageMetadata | StudioMp4Metadata {
  if (mediaType === "image") return validateStudioImageMetadata(bytes, { role: "output" });
  return validateStudioMp4Metadata(bytes);
}

function hasCanonicalArtifactMetadata(
  artifact: StudioValidatedProviderArtifact,
  metadata: StudioImageMetadata | StudioMp4Metadata,
): boolean {
  if (
    metadata.mimeType !== artifact.mimeType ||
    metadata.byteLength !== artifact.byteLength ||
    artifact.filename !== airOwnedFilename(artifact.outputIndex, metadata.mimeType)
  ) {
    return false;
  }

  if (artifact.mediaType === "image") {
    if (metadata.mimeType !== "image/jpeg" && metadata.mimeType !== "image/png") return false;
    if (!("kind" in artifact.metadata) || !("kind" in metadata)) return false;
    return artifact.metadata.kind === metadata.kind &&
      artifact.metadata.width === metadata.width &&
      artifact.metadata.height === metadata.height &&
      artifact.metadata.pixels === metadata.pixels &&
      artifact.metadata.decodedRgbaBytes === metadata.decodedRgbaBytes;
  }

  if (metadata.mimeType !== "video/mp4") return false;
  if (!("videoCodec" in artifact.metadata) || !("videoCodec" in metadata)) return false;
  return artifact.metadata.width === metadata.width &&
    artifact.metadata.height === metadata.height &&
    artifact.metadata.durationSeconds === metadata.durationSeconds &&
    artifact.metadata.frameRate === metadata.frameRate &&
    artifact.metadata.videoCodec === metadata.videoCodec &&
    artifact.metadata.audioCodec === metadata.audioCodec;
}

function airOwnedFilename(
  outputIndex: StudioOutputIndex,
  mimeType: StudioValidatedProviderArtifact["mimeType"],
): string {
  switch (mimeType) {
    case "image/jpeg":
      return `air-output-${outputIndex}.jpg`;
    case "image/png":
      return `air-output-${outputIndex}.png`;
    case "video/mp4":
      return `air-output-${outputIndex}.mp4`;
  }
}

function isAbsentOrCanonicalMp4MimeType(value: string | null | undefined): boolean {
  if (value === undefined || value === null || value.trim() === "") return true;
  return value.split(";", 1)[0]?.trim().toLowerCase() === "video/mp4";
}

function normalizeDeclaredMimeType(value: unknown): string | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value !== "string") fail("declared_mime_type_mismatch");
  return value;
}

function isArtifactMetadata(
  value: unknown,
  mimeType: unknown,
  byteLength: number,
): value is StudioImageMetadata | StudioMp4Metadata {
  if (typeof value !== "object" || value === null) return false;
  const metadata = value as { readonly mimeType?: unknown; readonly byteLength?: unknown };
  return metadata.mimeType === mimeType && metadata.byteLength === byteLength;
}

function isReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return typeof value === "object" && value !== null &&
    "getReader" in value && typeof (value as { getReader?: unknown }).getReader === "function";
}

function isAsyncIterable(value: unknown): value is AsyncIterable<Uint8Array> {
  return typeof value === "object" && value !== null &&
    Symbol.asyncIterator in value && typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function";
}

async function readReadableStream(
  stream: ReadableStream<Uint8Array>,
  collector: StudioArtifactCollector,
): Promise<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array>;
  try {
    reader = stream.getReader();
  } catch {
    fail("artifact_stream_unavailable");
  }

  try {
    for (;;) {
      const result = await reader.read();
      if (result.done) return collector.toBytes();
      collector.add(result.value);
    }
  } catch (error) {
    cancelReader(reader);
    rethrowBoundedReadError(error);
  } finally {
    reader.releaseLock();
  }
}

async function readAsyncIterable(
  stream: AsyncIterable<Uint8Array>,
  collector: StudioArtifactCollector,
): Promise<Uint8Array> {
  let iterator: AsyncIterator<Uint8Array>;
  try {
    iterator = stream[Symbol.asyncIterator]();
  } catch {
    fail("artifact_stream_unavailable");
  }

  try {
    for (;;) {
      const result = await iterator.next();
      if (result.done) return collector.toBytes();
      collector.add(result.value);
    }
  } catch (error) {
    closeIterator(iterator);
    rethrowBoundedReadError(error);
  }
}

function cancelReader(reader: ReadableStreamDefaultReader<Uint8Array>): void {
  // Do not await provider-owned cancellation: the bounded reader must still
  // return promptly if a source's cleanup promise never settles.
  void reader.cancel().catch(() => {
    // The original bounded/stream failure is the only safe caller-facing fact.
  });
}

function closeIterator(iterator: AsyncIterator<Uint8Array>): void {
  // As above, iterator cleanup must not turn a bounded read into an unbounded
  // wait. Providers never receive the underlying failure details.
  void Promise.resolve(iterator.return?.()).catch(() => {
    // The original bounded/stream failure is the only safe caller-facing fact.
  });
}

function rethrowBoundedReadError(error: unknown): never {
  if (error instanceof StudioArtifactValidationError) throw error;
  fail("artifact_stream_unavailable");
}

class StudioArtifactCollector {
  private readonly chunks: Uint8Array[] = [];
  private byteLength = 0;

  public add(value: unknown): void {
    if (!(value instanceof Uint8Array)) fail("artifact_stream_invalid");
    if (this.chunks.length >= STUDIO_PROVIDER_ARTIFACT_MAX_CHUNKS) {
      fail("artifact_stream_too_fragmented");
    }
    if (value.byteLength > STUDIO_PROVIDER_ARTIFACT_MAX_BYTES - this.byteLength) {
      fail("artifact_stream_too_large");
    }

    // `new Uint8Array` is deliberate: the source can be a Buffer or a view
    // over a mutable provider-owned ArrayBuffer, neither of which may escape
    // the validation boundary.
    let copy: Uint8Array;
    try {
      copy = new Uint8Array(value.byteLength);
      copy.set(value);
    } catch {
      fail("artifact_stream_invalid");
    }
    this.chunks.push(copy);
    this.byteLength += copy.byteLength;
  }

  public toBytes(): Uint8Array {
    const output = new Uint8Array(this.byteLength);
    let offset = 0;
    for (const chunk of this.chunks) {
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return output;
  }
}

function fail(code: StudioArtifactValidationCode): never {
  throw new StudioArtifactValidationError(code);
}
