import { inflateSync } from "node:zlib";

/**
 * Bounded, dependency-free media validation for Air's direct studio. These
 * helpers inspect bytes that have already passed a bounded download; they do
 * not render pixels or decode media samples. PNG scanlines are inflated only
 * inside a fixed memory ceiling so the byte-level contract can be proven.
 */

export const STUDIO_IMAGE_INPUT_MAX_BYTES = 10 * 1024 * 1024;
export const STUDIO_IMAGE_OUTPUT_MAX_BYTES = 25 * 1024 * 1024;
export const STUDIO_IMAGE_MAX_DIMENSION = 4_096;
export const STUDIO_IMAGE_MAX_PIXELS = 16_777_216;
export const STUDIO_IMAGE_MAX_DECODED_RGBA_BYTES = 64 * 1024 * 1024;

export const STUDIO_MP4_MAX_BYTES = 25 * 1024 * 1024;
export const STUDIO_MP4_MAX_DURATION_SECONDS = 15;
export const STUDIO_MP4_MAX_WIDTH = 1_920;
export const STUDIO_MP4_MAX_HEIGHT = 1_080;
export const STUDIO_MP4_MAX_FPS = 30;

const PNG_SIGNATURE = Object.freeze([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const HEIF_BRANDS = new Set([
  "heif",
  "heic",
  "heix",
  "hevc",
  "hevx",
  "heim",
  "heis",
  "hevm",
  "hevs",
  "mif1",
  "msf1",
]);
const MP4_BRANDS = new Set([
  "isom",
  "iso2",
  "iso3",
  "iso4",
  "iso5",
  "iso6",
  "mp41",
  "mp42",
  "avc1",
  "avc2",
  "avc3",
  "dash",
  "m4v ",
]);
const AAC_AUDIO_OBJECT_TYPES = new Set([
  1,
  2,
  3,
  4,
  5,
  6,
  17,
  19,
  20,
  21,
  22,
  23,
  29,
  39,
  42,
]);
const MAX_ISO_BOXES = 4_096;
const MAX_ISO_BOX_DEPTH = 16;
const MAX_STTS_ENTRIES = 2_048;
const MAX_SAMPLE_DESCRIPTIONS = 16;
const MAX_PNG_CHUNKS = 4_096;
/** Scanline filters add at most two rows' worth of bytes across Adam7 passes. */
const MAX_PNG_INFLATED_BYTES = STUDIO_IMAGE_MAX_DECODED_RGBA_BYTES + STUDIO_IMAGE_MAX_DIMENSION * 2;
const PNG_ADAM7_PASSES = Object.freeze([
  Object.freeze({ startX: 0, startY: 0, stepX: 8, stepY: 8 }),
  Object.freeze({ startX: 4, startY: 0, stepX: 8, stepY: 8 }),
  Object.freeze({ startX: 0, startY: 4, stepX: 4, stepY: 8 }),
  Object.freeze({ startX: 2, startY: 0, stepX: 4, stepY: 4 }),
  Object.freeze({ startX: 0, startY: 2, stepX: 2, stepY: 4 }),
  Object.freeze({ startX: 1, startY: 0, stepX: 2, stepY: 2 }),
  Object.freeze({ startX: 0, startY: 1, stepX: 1, stepY: 2 }),
] as const);
const PNG_CRC_TABLE = createPngCrcTable();

export type StudioImageRole = "input" | "output";
export type StudioImageKind = "jpeg" | "png";
export type StudioMediaValidationCode =
  | "invalid_image"
  | "unsupported_image"
  | "heic_not_supported"
  | "image_too_large"
  | "invalid_image_dimensions"
  | "invalid_mp4"
  | "video_too_large"
  | "unsupported_video_codec"
  | "unsupported_audio_codec"
  | "invalid_video_dimensions"
  | "video_too_long"
  | "video_frame_rate_exceeded";

/** A stable code suitable for a caller's user-safe error mapping. */
export class StudioMediaValidationError extends Error {
  public constructor(readonly code: StudioMediaValidationCode) {
    super(code);
    this.name = "StudioMediaValidationError";
  }
}

export interface StudioImageValidationOptions {
  /** Inputs are limited to 10 MiB; generated outputs may be up to 25 MiB. */
  readonly role?: StudioImageRole | undefined;
  /** Optional claimed media type. A claim must agree with the byte signature. */
  readonly mimeType?: string | null | undefined;
}

export interface StudioImageMetadata {
  readonly kind: StudioImageKind;
  readonly mimeType: "image/jpeg" | "image/png";
  readonly byteLength: number;
  readonly width: number;
  readonly height: number;
  readonly pixels: number;
  readonly decodedRgbaBytes: number;
}

export interface StudioMp4Metadata {
  readonly mimeType: "video/mp4";
  readonly byteLength: number;
  readonly width: number;
  readonly height: number;
  readonly durationSeconds: number;
  readonly frameRate: number;
  readonly videoCodec: "h264";
  readonly audioCodec?: "aac" | undefined;
}

interface Dimensions {
  readonly width: number;
  readonly height: number;
}

interface PngHeader extends Dimensions {
  readonly bitDepth: number;
  readonly colorType: number;
  readonly interlaceMethod: 0 | 1;
  readonly bitsPerPixel: number;
}

interface IsoBox {
  readonly type: string;
  readonly start: number;
  readonly dataStart: number;
  readonly end: number;
}

interface IsoParseState {
  boxes: number;
}

interface SampleEntry {
  readonly type: string;
  readonly start: number;
  readonly end: number;
}

interface MediaTime {
  readonly timeScale: number;
  readonly duration: bigint;
}

interface SampleTiming {
  readonly samples: bigint;
  readonly duration: bigint;
}

interface ParsedVideoTrack {
  readonly width: number;
  readonly height: number;
  readonly durationSeconds: number;
  readonly frameRate: number;
}

/**
 * Validates a JPEG or PNG and returns bounded metadata. HEIC/HEIF is rejected
 * even when its ftyp brand is only present in the compatible-brand list.
 */
export function validateStudioImageMetadata(
  bytes: Uint8Array,
  options: StudioImageValidationOptions = {},
): StudioImageMetadata {
  const declaredKind = declaredImageKind(options.mimeType);
  if (declaredKind === "heic") fail("heic_not_supported");
  if (declaredKind === "unsupported") fail("unsupported_image");
  if (isHeicOrHeif(bytes)) fail("heic_not_supported");

  const maximum = options.role === "output"
    ? STUDIO_IMAGE_OUTPUT_MAX_BYTES
    : STUDIO_IMAGE_INPUT_MAX_BYTES;
  if (bytes.byteLength < 1) fail("invalid_image");
  if (bytes.byteLength > maximum) fail("image_too_large");

  let kind: StudioImageKind;
  let dimensions: Dimensions;
  if (hasJpegMagic(bytes)) {
    kind = "jpeg";
    dimensions = parseJpegDimensions(bytes);
  } else if (hasPngMagic(bytes)) {
    kind = "png";
    dimensions = parsePngDimensions(bytes);
  } else {
    fail("unsupported_image");
  }
  if (declaredKind !== undefined && declaredKind !== kind) fail("invalid_image");

  const metadata = imageMetadata(kind, bytes.byteLength, dimensions);
  return Object.freeze(metadata);
}

/** Alias for the common call-site name. */
export const validateStudioImage = validateStudioImageMetadata;

/**
 * Validates an unfragmented ISO-BMFF MP4 containing exactly one H.264 video
 * track and, optionally, one AAC audio track. The parser never follows a box
 * with size zero and always proves box boundaries before reading child data.
 */
export function validateStudioMp4(bytes: Uint8Array): StudioMp4Metadata {
  if (bytes.byteLength < 1) fail("invalid_mp4");
  if (bytes.byteLength > STUDIO_MP4_MAX_BYTES) fail("video_too_large");

  const state: IsoParseState = { boxes: 0 };
  const root = parseIsoBoxes(bytes, 0, bytes.byteLength, state, 0);
  const fileType = exactlyOneBox(root, "ftyp");
  validateMp4FileType(bytes, fileType);

  if (root.some((box) => box.type === "moof")) fail("invalid_mp4");
  const movie = exactlyOneBox(root, "moov");
  if (!root.some((box) => box.type === "mdat")) fail("invalid_mp4");

  const movieBoxes = parseIsoBoxes(bytes, movie.dataStart, movie.end, state, 1);
  const tracks = movieBoxes.filter((box) => box.type === "trak");
  if (tracks.length < 1 || tracks.length > 2) fail("invalid_mp4");

  const videoTracks: ParsedVideoTrack[] = [];
  let audioTracks = 0;
  for (const track of tracks) {
    const parsed = parseTrack(bytes, track, state);
    if (parsed.kind === "video") videoTracks.push(parsed.track);
    else audioTracks += 1;
  }
  if (videoTracks.length !== 1 || audioTracks > 1) fail("invalid_mp4");

  const video = videoTracks[0];
  if (!video) fail("invalid_mp4");
  return Object.freeze({
    mimeType: "video/mp4",
    byteLength: bytes.byteLength,
    width: video.width,
    height: video.height,
    durationSeconds: video.durationSeconds,
    frameRate: video.frameRate,
    videoCodec: "h264",
    ...(audioTracks === 0 ? {} : { audioCodec: "aac" as const }),
  });
}

/** Alias that makes the metadata role explicit at call sites. */
export const validateStudioMp4Metadata = validateStudioMp4;

function imageMetadata(kind: StudioImageKind, byteLength: number, dimensions: Dimensions): StudioImageMetadata {
  const { width, height } = dimensions;
  if (!Number.isInteger(width) || !Number.isInteger(height) ||
    width < 1 || height < 1 ||
    width > STUDIO_IMAGE_MAX_DIMENSION || height > STUDIO_IMAGE_MAX_DIMENSION) {
    fail("invalid_image_dimensions");
  }
  const pixels = width * height;
  const decodedRgbaBytes = pixels * 4;
  if (!Number.isSafeInteger(pixels) || pixels > STUDIO_IMAGE_MAX_PIXELS ||
    !Number.isSafeInteger(decodedRgbaBytes) || decodedRgbaBytes > STUDIO_IMAGE_MAX_DECODED_RGBA_BYTES) {
    fail("invalid_image_dimensions");
  }
  return {
    kind,
    mimeType: kind === "jpeg" ? "image/jpeg" : "image/png",
    byteLength,
    width,
    height,
    pixels,
    decodedRgbaBytes,
  };
}

function declaredImageKind(value: string | null | undefined): StudioImageKind | "heic" | "unsupported" | undefined {
  const normalized = value?.split(";", 1)[0]?.trim().toLowerCase();
  if (!normalized) return undefined;
  if (normalized === "image/jpeg") return "jpeg";
  if (normalized === "image/png") return "png";
  if (normalized === "image/heic" || normalized === "image/heif") return "heic";
  return "unsupported";
}

function hasJpegMagic(bytes: Uint8Array): boolean {
  return bytes.byteLength >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff;
}

function hasPngMagic(bytes: Uint8Array): boolean {
  if (bytes.byteLength < PNG_SIGNATURE.length) return false;
  return PNG_SIGNATURE.every((value, index) => bytes[index] === value);
}

function isHeicOrHeif(bytes: Uint8Array): boolean {
  if (bytes.byteLength < 16 || fourcc(bytes, 4) !== "ftyp") return false;
  const size32 = uint32(bytes, 0);
  let headerSize = 8;
  let size = size32;
  if (size32 === 1) {
    if (bytes.byteLength < 24) return false;
    const extended = uint64(bytes, 8);
    if (extended > BigInt(Number.MAX_SAFE_INTEGER)) return false;
    size = Number(extended);
    headerSize = 16;
  }
  if (size === 0 || size > bytes.byteLength || size < headerSize + 8) return false;
  const end = size;
  const majorBrand = fourcc(bytes, headerSize).toLowerCase();
  if (HEIF_BRANDS.has(majorBrand)) return true;
  for (let cursor = headerSize + 8; cursor + 4 <= end; cursor += 4) {
    if (HEIF_BRANDS.has(fourcc(bytes, cursor).toLowerCase())) return true;
  }
  return false;
}

function parseJpegDimensions(bytes: Uint8Array): Dimensions {
  if (!hasJpegMagic(bytes) || bytes.byteLength < 10 ||
    bytes[bytes.byteLength - 2] !== 0xff || bytes[bytes.byteLength - 1] !== 0xd9) {
    fail("invalid_image");
  }

  let cursor = 2;
  let segments = 0;
  let dimensions: Dimensions | undefined;
  let sawScan = false;
  let sawEntropy = false;
  while (cursor < bytes.byteLength) {
    if (segments++ >= 256 || bytes[cursor] !== 0xff) fail("invalid_image");
    while (cursor < bytes.byteLength && bytes[cursor] === 0xff) cursor += 1;
    if (cursor >= bytes.byteLength) fail("invalid_image");
    const marker = bytes[cursor]!;
    cursor += 1;

    if (marker === 0xd9) {
      if (cursor !== bytes.byteLength || dimensions === undefined || !sawScan || !sawEntropy) {
        fail("invalid_image");
      }
      return dimensions;
    }
    if (marker === 0x00 || marker === 0xd8 || (marker >= 0xd0 && marker <= 0xd7) || marker === 0x01) {
      fail("invalid_image");
    }

    if (marker === 0xda) {
      if (dimensions === undefined) fail("invalid_image");
      cursor = parseJpegStartOfScan(bytes, cursor);
      sawScan = true;
      const scan = consumeJpegEntropy(bytes, cursor);
      if (!scan.sawEntropy) fail("invalid_image");
      sawEntropy = true;
      cursor = scan.nextMarkerStart;
      continue;
    }

    if (cursor + 2 > bytes.byteLength) fail("invalid_image");
    const segmentLength = uint16(bytes, cursor);
    if (segmentLength < 2 || segmentLength > bytes.byteLength - cursor) fail("invalid_image");
    const segmentEnd = cursor + segmentLength;
    if (isJpegStartOfFrame(marker)) {
      if (dimensions !== undefined) fail("invalid_image");
      dimensions = parseJpegFrameDimensions(bytes, cursor, segmentLength, segmentEnd);
    }
    cursor = segmentEnd;
  }
  fail("invalid_image");
}

/**
 * A JPEG cannot be accepted from a frame header alone. Real JPEGs contain at
 * least one Start-of-Scan segment followed by entropy-coded bytes, with a
 * terminal EOI marker. Parsing that bounded envelope fixes the former false
 * rejection of every normal JPEG while also rejecting header-only polyglots.
 */
function parseJpegStartOfScan(bytes: Uint8Array, segmentStart: number): number {
  if (segmentStart + 2 > bytes.byteLength) fail("invalid_image");
  const segmentLength = uint16(bytes, segmentStart);
  if (segmentLength < 8 || segmentLength > bytes.byteLength - segmentStart) fail("invalid_image");
  const components = bytes[segmentStart + 2]!;
  if (components < 1 || components > 4 || segmentLength !== 6 + components * 2) fail("invalid_image");

  const selectorStart = segmentStart + 3;
  const selectorEnd = selectorStart + components * 2;
  const selectors = new Set<number>();
  for (let cursor = selectorStart; cursor < selectorEnd; cursor += 2) {
    const selector = bytes[cursor]!;
    if (selector === 0 || selectors.has(selector)) fail("invalid_image");
    selectors.add(selector);
  }
  const spectralStart = bytes[selectorEnd]!;
  const spectralEnd = bytes[selectorEnd + 1]!;
  const successiveApproximation = bytes[selectorEnd + 2]!;
  if (spectralStart > spectralEnd || spectralEnd > 63 || (successiveApproximation >> 4) > 13 || (successiveApproximation & 0x0f) > 13) {
    fail("invalid_image");
  }
  return segmentStart + segmentLength;
}

function consumeJpegEntropy(
  bytes: Uint8Array,
  start: number,
): { readonly nextMarkerStart: number; readonly sawEntropy: boolean } {
  let cursor = start;
  let sawEntropy = false;
  while (cursor < bytes.byteLength) {
    const value = bytes[cursor]!;
    cursor += 1;
    if (value !== 0xff) {
      sawEntropy = true;
      continue;
    }

    const markerStart = cursor - 1;
    while (cursor < bytes.byteLength && bytes[cursor] === 0xff) cursor += 1;
    if (cursor >= bytes.byteLength) fail("invalid_image");
    const marker = bytes[cursor]!;
    if (marker === 0x00) {
      // Byte stuffing represents a literal 0xff inside entropy-coded data.
      sawEntropy = true;
      cursor += 1;
      continue;
    }
    if (marker >= 0xd0 && marker <= 0xd7) {
      // Restart markers delimit entropy intervals but carry no payload.
      cursor += 1;
      continue;
    }
    return { nextMarkerStart: markerStart, sawEntropy };
  }
  fail("invalid_image");
}

function parseJpegFrameDimensions(
  bytes: Uint8Array,
  segmentStart: number,
  segmentLength: number,
  segmentEnd: number,
): Dimensions {
  if (segmentLength < 11 || segmentStart + 7 >= segmentEnd) fail("invalid_image");
  const precision = bytes[segmentStart + 2]!;
  const height = uint16(bytes, segmentStart + 3);
  const width = uint16(bytes, segmentStart + 5);
  const components = bytes[segmentStart + 7]!;
  if (precision < 2 || precision > 16 || components < 1 || components > 4 || segmentLength !== 8 + components * 3) {
    fail("invalid_image");
  }

  const componentIds = new Set<number>();
  for (let cursor = segmentStart + 8; cursor < segmentEnd; cursor += 3) {
    const componentId = bytes[cursor]!;
    const sampling = bytes[cursor + 1]!;
    const quantizationTable = bytes[cursor + 2]!;
    const horizontalSampling = sampling >> 4;
    const verticalSampling = sampling & 0x0f;
    if (
      componentId === 0 ||
      componentIds.has(componentId) ||
      horizontalSampling < 1 ||
      horizontalSampling > 4 ||
      verticalSampling < 1 ||
      verticalSampling > 4 ||
      quantizationTable > 3
    ) {
      fail("invalid_image");
    }
    componentIds.add(componentId);
  }
  return { width, height };
}

function isJpegStartOfFrame(marker: number): boolean {
  return (marker >= 0xc0 && marker <= 0xc3) ||
    (marker >= 0xc5 && marker <= 0xc7) ||
    (marker >= 0xc9 && marker <= 0xcb) ||
    (marker >= 0xcd && marker <= 0xcf);
}

function parsePngDimensions(bytes: Uint8Array): Dimensions {
  if (!hasPngMagic(bytes) || bytes.byteLength < 45) fail("invalid_image");
  let cursor = 8;
  let chunks = 0;
  let header: PngHeader | undefined;
  let sawIdat = false;
  let idatFinished = false;
  let sawPalette = false;
  const idatChunks: Uint8Array[] = [];
  let idatByteLength = 0;
  while (cursor < bytes.byteLength) {
    if (chunks++ >= MAX_PNG_CHUNKS || cursor + 12 > bytes.byteLength) fail("invalid_image");
    const length = uint32(bytes, cursor);
    const type = fourcc(bytes, cursor + 4);
    if (!isPngChunkType(type) || length > bytes.byteLength - cursor - 12) fail("invalid_image");
    const dataStart = cursor + 8;
    const dataEnd = dataStart + length;
    const end = dataEnd + 4;
    if (pngCrc32(bytes, cursor + 4, dataEnd) !== uint32(bytes, dataEnd)) fail("invalid_image");

    if (cursor === 8) {
      if (type !== "IHDR" || length !== 13) fail("invalid_image");
      header = parsePngHeader(bytes, dataStart);
      // Bound dimensions before any zlib allocation. The returned metadata is
      // discarded here and reconstructed by the caller with the true byte size.
      imageMetadata("png", 1, header);
      cursor = end;
      continue;
    }
    if (header === undefined || type === "IHDR") fail("invalid_image");

    if (type === "PLTE") {
      if (sawPalette || sawIdat) fail("invalid_image");
      validatePngPalette(length, header);
      sawPalette = true;
    } else if (type === "IDAT") {
      if (idatFinished) fail("invalid_image");
      sawIdat = true;
      if (length > bytes.byteLength - idatByteLength) fail("invalid_image");
      idatChunks.push(bytes.subarray(dataStart, dataEnd));
      idatByteLength += length;
    } else if (type === "IEND") {
      if (length !== 0 || !sawIdat || end !== bytes.byteLength || (header.colorType === 3 && !sawPalette)) {
        fail("invalid_image");
      }
      validatePngRaster(idatChunks, idatByteLength, header);
      return { width: header.width, height: header.height };
    } else {
      if (isPngCriticalChunk(type)) fail("invalid_image");
      if (sawIdat) idatFinished = true;
    }
    cursor = end;
  }
  fail("invalid_image");
}

function parsePngHeader(bytes: Uint8Array, start: number): PngHeader {
  if (start < 0 || start + 13 > bytes.byteLength) fail("invalid_image");
  const width = uint32(bytes, start);
  const height = uint32(bytes, start + 4);
  const bitDepth = bytes[start + 8]!;
  const colorType = bytes[start + 9]!;
  const validDepth = (colorType === 0 && [1, 2, 4, 8, 16].includes(bitDepth)) ||
    (colorType === 2 && [8, 16].includes(bitDepth)) ||
    (colorType === 3 && [1, 2, 4, 8].includes(bitDepth)) ||
    (colorType === 4 && [8, 16].includes(bitDepth)) ||
    (colorType === 6 && [8, 16].includes(bitDepth));
  const interlaceMethod = bytes[start + 12]!;
  if (!validDepth || bytes[start + 10] !== 0 || bytes[start + 11] !== 0 || (interlaceMethod !== 0 && interlaceMethod !== 1)) {
    fail("invalid_image");
  }
  const channels = pngChannels(colorType);
  return { width, height, bitDepth, colorType, interlaceMethod, bitsPerPixel: channels * bitDepth };
}

function isPngChunkType(type: string): boolean {
  return type.length === 4 && [...type].every((character) =>
    (character >= "A" && character <= "Z") || (character >= "a" && character <= "z"),
  );
}

function validatePngPalette(length: number, header: PngHeader): void {
  if (header.colorType === 0 || header.colorType === 4 || length < 3 || length > 768 || length % 3 !== 0) {
    fail("invalid_image");
  }
  if (header.colorType === 3 && length / 3 > 2 ** header.bitDepth) fail("invalid_image");
}

function validatePngRaster(chunks: readonly Uint8Array[], compressedLength: number, header: PngHeader): void {
  const expectedLength = pngInflatedByteLength(header);
  const compressed = new Uint8Array(compressedLength);
  let compressedOffset = 0;
  for (const chunk of chunks) {
    compressed.set(chunk, compressedOffset);
    compressedOffset += chunk.byteLength;
  }
  if (compressedOffset !== compressed.byteLength) fail("invalid_image");

  let inflated: Uint8Array;
  try {
    inflated = inflateSync(compressed, { maxOutputLength: expectedLength });
  } catch {
    // zlib details can include provider bytes; only the stable image failure
    // crosses this boundary.
    fail("invalid_image");
  }
  if (inflated.byteLength !== expectedLength) fail("invalid_image");

  let offset = 0;
  for (const pass of pngPassDimensions(header)) {
    const rowBytes = pngRowByteLength(pass.width, header.bitsPerPixel);
    for (let row = 0; row < pass.height; row += 1) {
      if (offset >= inflated.byteLength || inflated[offset]! > 4) fail("invalid_image");
      offset += 1 + rowBytes;
    }
  }
  if (offset !== inflated.byteLength) fail("invalid_image");
}

function pngInflatedByteLength(header: PngHeader): number {
  let total = 0;
  for (const pass of pngPassDimensions(header)) {
    const rowBytes = pngRowByteLength(pass.width, header.bitsPerPixel);
    const passLength = pass.height * (rowBytes + 1);
    if (!Number.isSafeInteger(passLength) || passLength > MAX_PNG_INFLATED_BYTES - total) {
      // A valid PNG may use high bit depth, but Air's launch contract never
      // allocates more than its decoded-image safety ceiling to validate one.
      fail("invalid_image_dimensions");
    }
    total += passLength;
  }
  if (total < 1) fail("invalid_image");
  return total;
}

function pngPassDimensions(header: PngHeader): readonly Dimensions[] {
  if (header.interlaceMethod === 0) return [{ width: header.width, height: header.height }];
  const passes: Dimensions[] = [];
  for (const pass of PNG_ADAM7_PASSES) {
    const width = pngPassLength(header.width, pass.startX, pass.stepX);
    const height = pngPassLength(header.height, pass.startY, pass.stepY);
    if (width > 0 && height > 0) passes.push({ width, height });
  }
  return passes;
}

function pngPassLength(length: number, start: number, step: number): number {
  return length <= start ? 0 : Math.ceil((length - start) / step);
}

function pngRowByteLength(width: number, bitsPerPixel: number): number {
  const bits = width * bitsPerPixel;
  if (!Number.isSafeInteger(bits)) fail("invalid_image_dimensions");
  return Math.ceil(bits / 8);
}

function pngChannels(colorType: number): number {
  switch (colorType) {
    case 0:
    case 3:
      return 1;
    case 2:
      return 3;
    case 4:
      return 2;
    case 6:
      return 4;
    default:
      fail("invalid_image");
  }
}

function isPngCriticalChunk(type: string): boolean {
  const first = type.charCodeAt(0);
  return first >= 65 && first <= 90;
}

function createPngCrcTable(): Uint32Array {
  const table = new Uint32Array(256);
  for (let index = 0; index < table.length; index += 1) {
    let value = index;
    for (let bit = 0; bit < 8; bit += 1) value = (value >>> 1) ^ ((value & 1) === 1 ? 0xedb88320 : 0);
    table[index] = value >>> 0;
  }
  return table;
}

function pngCrc32(bytes: Uint8Array, start: number, end: number): number {
  let crc = 0xffffffff;
  for (let cursor = start; cursor < end; cursor += 1) {
    crc = PNG_CRC_TABLE[(crc ^ bytes[cursor]!) & 0xff]! ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function validateMp4FileType(bytes: Uint8Array, box: IsoBox): void {
  const length = box.end - box.dataStart;
  if (length < 8 || (length - 8) % 4 !== 0) fail("invalid_mp4");
  const brands = [fourcc(bytes, box.dataStart).toLowerCase()];
  for (let cursor = box.dataStart + 8; cursor < box.end; cursor += 4) {
    brands.push(fourcc(bytes, cursor).toLowerCase());
  }
  if (brands.some((brand) => HEIF_BRANDS.has(brand)) || !brands.some((brand) => MP4_BRANDS.has(brand))) {
    fail("invalid_mp4");
  }
}

function parseTrack(
  bytes: Uint8Array,
  track: IsoBox,
  state: IsoParseState,
): { readonly kind: "video"; readonly track: ParsedVideoTrack } | { readonly kind: "audio" } {
  const trackBoxes = parseIsoBoxes(bytes, track.dataStart, track.end, state, 2);
  const trackDimensions = parseTrackDimensions(bytes, exactlyOneBox(trackBoxes, "tkhd"));
  const media = exactlyOneBox(trackBoxes, "mdia");
  const mediaBoxes = parseIsoBoxes(bytes, media.dataStart, media.end, state, 3);
  const handler = parseHandlerType(bytes, exactlyOneBox(mediaBoxes, "hdlr"));
  const mediaTime = parseMediaTime(bytes, exactlyOneBox(mediaBoxes, "mdhd"));
  validateMediaDuration(mediaTime);
  const mediaInformation = exactlyOneBox(mediaBoxes, "minf");
  const informationBoxes = parseIsoBoxes(bytes, mediaInformation.dataStart, mediaInformation.end, state, 4);
  const sampleTable = exactlyOneBox(informationBoxes, "stbl");
  const sampleTableBoxes = parseIsoBoxes(bytes, sampleTable.dataStart, sampleTable.end, state, 5);
  const entries = parseSampleDescriptions(bytes, exactlyOneBox(sampleTableBoxes, "stsd"));

  if (handler === "vide") {
    if (entries.length !== 1) fail("unsupported_video_codec");
    const sampleDimensions = parseH264SampleEntry(bytes, entries[0]!, state);
    validateVideoDimensions(trackDimensions);
    validateVideoDimensions(sampleDimensions);
    const timing = parseSampleTiming(bytes, exactlyOneBox(sampleTableBoxes, "stts"));
    if (timing.duration > mediaTime.duration) fail("invalid_mp4");
    const denominator = timing.duration * BigInt(STUDIO_MP4_MAX_FPS);
    if (timing.samples * BigInt(mediaTime.timeScale) > denominator) fail("video_frame_rate_exceeded");
    return {
      kind: "video",
      track: {
        width: sampleDimensions.width,
        height: sampleDimensions.height,
        durationSeconds: Number(mediaTime.duration) / mediaTime.timeScale,
        frameRate: Number(timing.samples * BigInt(mediaTime.timeScale)) / Number(timing.duration),
      },
    };
  }

  if (handler === "soun") {
    if (entries.length !== 1) fail("unsupported_audio_codec");
    parseAacSampleEntry(bytes, entries[0]!, state);
    return { kind: "audio" };
  }
  fail("invalid_mp4");
}

function parseTrackDimensions(bytes: Uint8Array, box: IsoBox): Dimensions {
  const version = byteAt(bytes, box.dataStart, "invalid_mp4");
  if (version === 0) {
    if (box.end - box.dataStart < 84) fail("invalid_mp4");
    return {
      width: fixed1616(uint32(bytes, box.dataStart + 76)),
      height: fixed1616(uint32(bytes, box.dataStart + 80)),
    };
  }
  if (version === 1) {
    if (box.end - box.dataStart < 96) fail("invalid_mp4");
    return {
      width: fixed1616(uint32(bytes, box.dataStart + 88)),
      height: fixed1616(uint32(bytes, box.dataStart + 92)),
    };
  }
  fail("invalid_mp4");
}

function fixed1616(value: number): number {
  if ((value & 0xffff) !== 0) fail("invalid_video_dimensions");
  return Math.floor(value / 0x1_0000);
}

function parseHandlerType(bytes: Uint8Array, box: IsoBox): string {
  if (box.end - box.dataStart < 12) fail("invalid_mp4");
  return fourcc(bytes, box.dataStart + 8);
}

function parseMediaTime(bytes: Uint8Array, box: IsoBox): MediaTime {
  const version = byteAt(bytes, box.dataStart, "invalid_mp4");
  if (version === 0) {
    if (box.end - box.dataStart < 24) fail("invalid_mp4");
    const timeScale = uint32(bytes, box.dataStart + 12);
    const duration = BigInt(uint32(bytes, box.dataStart + 16));
    if (timeScale < 1 || duration < 1n) fail("invalid_mp4");
    return { timeScale, duration };
  }
  if (version === 1) {
    if (box.end - box.dataStart < 36) fail("invalid_mp4");
    const timeScale = uint32(bytes, box.dataStart + 20);
    const duration = uint64(bytes, box.dataStart + 24);
    if (timeScale < 1 || duration < 1n) fail("invalid_mp4");
    return { timeScale, duration };
  }
  fail("invalid_mp4");
}

function validateMediaDuration(mediaTime: MediaTime): void {
  if (mediaTime.duration > BigInt(mediaTime.timeScale) * BigInt(STUDIO_MP4_MAX_DURATION_SECONDS)) {
    fail("video_too_long");
  }
}

function parseSampleDescriptions(bytes: Uint8Array, box: IsoBox): readonly SampleEntry[] {
  if (box.end - box.dataStart < 8) fail("invalid_mp4");
  const entryCount = uint32(bytes, box.dataStart + 4);
  if (entryCount < 1 || entryCount > MAX_SAMPLE_DESCRIPTIONS) fail("invalid_mp4");
  const entries: SampleEntry[] = [];
  let cursor = box.dataStart + 8;
  for (let index = 0; index < entryCount; index += 1) {
    if (cursor + 8 > box.end) fail("invalid_mp4");
    const size = uint32(bytes, cursor);
    if (size < 8 || size > box.end - cursor) fail("invalid_mp4");
    entries.push({ type: fourcc(bytes, cursor + 4), start: cursor, end: cursor + size });
    cursor += size;
  }
  if (cursor !== box.end) fail("invalid_mp4");
  return Object.freeze(entries);
}

function parseH264SampleEntry(bytes: Uint8Array, entry: SampleEntry, state: IsoParseState): Dimensions {
  if (entry.type !== "avc1" && entry.type !== "avc3") fail("unsupported_video_codec");
  if (entry.end - entry.start < 86) fail("invalid_mp4");
  const dimensions = {
    width: uint16(bytes, entry.start + 32),
    height: uint16(bytes, entry.start + 34),
  };
  const children = parseIsoBoxes(bytes, entry.start + 86, entry.end, state, 6);
  validateAvcConfiguration(bytes, exactlyOneBox(children, "avcC"));
  return dimensions;
}

function validateAvcConfiguration(bytes: Uint8Array, box: IsoBox): void {
  const length = box.end - box.dataStart;
  if (length < 7 || bytes[box.dataStart] !== 1) fail("invalid_mp4");
  const end = box.end;
  let cursor = box.dataStart + 6;
  const spsCount = bytes[box.dataStart + 5]! & 0x1f;
  if (spsCount < 1) fail("invalid_mp4");
  for (let index = 0; index < spsCount; index += 1) {
    if (cursor + 2 > end) fail("invalid_mp4");
    const length = uint16(bytes, cursor);
    cursor += 2;
    if (length < 1 || length > end - cursor) fail("invalid_mp4");
    cursor += length;
  }
  if (cursor >= end) fail("invalid_mp4");
  const ppsCount = bytes[cursor]!;
  cursor += 1;
  if (ppsCount < 1) fail("invalid_mp4");
  for (let index = 0; index < ppsCount; index += 1) {
    if (cursor + 2 > end) fail("invalid_mp4");
    const length = uint16(bytes, cursor);
    cursor += 2;
    if (length < 1 || length > end - cursor) fail("invalid_mp4");
    cursor += length;
  }
  if (cursor > end) fail("invalid_mp4");
}

function parseAacSampleEntry(bytes: Uint8Array, entry: SampleEntry, state: IsoParseState): void {
  if (entry.type !== "mp4a") fail("unsupported_audio_codec");
  if (entry.end - entry.start < 36) fail("invalid_mp4");
  const children = parseIsoBoxes(bytes, entry.start + 36, entry.end, state, 6);
  validateAacEsds(bytes, exactlyOneBox(children, "esds"));
}

function validateAacEsds(bytes: Uint8Array, box: IsoBox): void {
  if (box.end - box.dataStart < 5) fail("invalid_mp4");
  const es = parseDescriptor(bytes, box.dataStart + 4, box.end);
  if (es.tag !== 0x03 || es.payloadEnd - es.payloadStart < 3) fail("unsupported_audio_codec");
  let cursor = es.payloadStart + 2;
  const flags = byteAt(bytes, cursor, "invalid_mp4");
  cursor += 1;
  if ((flags & 0x80) !== 0) cursor += 2;
  if ((flags & 0x40) !== 0) {
    const urlLength = byteAt(bytes, cursor, "invalid_mp4");
    cursor += 1 + urlLength;
  }
  if ((flags & 0x20) !== 0) cursor += 2;
  if (cursor > es.payloadEnd) fail("invalid_mp4");

  const decoder = parseDescriptor(bytes, cursor, es.payloadEnd);
  if (decoder.tag !== 0x04 || decoder.payloadEnd - decoder.payloadStart < 13 ||
    bytes[decoder.payloadStart] !== 0x40) {
    fail("unsupported_audio_codec");
  }
  cursor = decoder.payloadStart + 13;
  let audioSpecificConfig: { readonly start: number; readonly end: number } | undefined;
  let descriptors = 0;
  while (cursor < decoder.payloadEnd) {
    if (descriptors++ > 32) fail("invalid_mp4");
    const descriptor = parseDescriptor(bytes, cursor, decoder.payloadEnd);
    if (descriptor.tag === 0x05) {
      if (audioSpecificConfig !== undefined) fail("invalid_mp4");
      audioSpecificConfig = { start: descriptor.payloadStart, end: descriptor.payloadEnd };
    }
    cursor = descriptor.payloadEnd;
  }
  if (!audioSpecificConfig) fail("unsupported_audio_codec");
  validateAacAudioSpecificConfig(bytes, audioSpecificConfig.start, audioSpecificConfig.end);
}

function validateAacAudioSpecificConfig(bytes: Uint8Array, start: number, end: number): void {
  let bitOffset = 0;
  let objectType = readBits(bytes, start, end, bitOffset, 5);
  bitOffset += 5;
  if (objectType === 31) {
    objectType = 32 + readBits(bytes, start, end, bitOffset, 6);
    bitOffset += 6;
  }
  const frequencyIndex = readBits(bytes, start, end, bitOffset, 4);
  bitOffset += 4;
  if (frequencyIndex > 12 && frequencyIndex !== 15) fail("unsupported_audio_codec");
  if (frequencyIndex === 15) {
    readBits(bytes, start, end, bitOffset, 24);
  }
  if (!AAC_AUDIO_OBJECT_TYPES.has(objectType)) fail("unsupported_audio_codec");
}

function parseDescriptor(
  bytes: Uint8Array,
  start: number,
  end: number,
): { readonly tag: number; readonly payloadStart: number; readonly payloadEnd: number } {
  if (start >= end) fail("invalid_mp4");
  const tag = bytes[start]!;
  let cursor = start + 1;
  let length = 0;
  for (let index = 0; index < 4; index += 1) {
    if (cursor >= end) fail("invalid_mp4");
    const next = bytes[cursor]!;
    cursor += 1;
    length = length * 128 + (next & 0x7f);
    if ((next & 0x80) === 0) {
      if (length > end - cursor) fail("invalid_mp4");
      return { tag, payloadStart: cursor, payloadEnd: cursor + length };
    }
  }
  fail("invalid_mp4");
}

function readBits(bytes: Uint8Array, start: number, end: number, offset: number, count: number): number {
  if (count < 1 || offset + count > (end - start) * 8) fail("invalid_mp4");
  let value = 0;
  for (let index = 0; index < count; index += 1) {
    const absolute = offset + index;
    value = (value << 1) | ((bytes[start + Math.floor(absolute / 8)]! >> (7 - (absolute % 8))) & 1);
  }
  return value;
}

function parseSampleTiming(bytes: Uint8Array, box: IsoBox): SampleTiming {
  if (box.end - box.dataStart < 8) fail("invalid_mp4");
  const entryCount = uint32(bytes, box.dataStart + 4);
  if (entryCount < 1 || entryCount > MAX_STTS_ENTRIES ||
    box.end - box.dataStart !== 8 + entryCount * 8) {
    fail("invalid_mp4");
  }
  let samples = 0n;
  let duration = 0n;
  let cursor = box.dataStart + 8;
  for (let index = 0; index < entryCount; index += 1) {
    const count = uint32(bytes, cursor);
    const delta = uint32(bytes, cursor + 4);
    if (count < 1 || delta < 1) fail("invalid_mp4");
    samples += BigInt(count);
    duration += BigInt(count) * BigInt(delta);
    cursor += 8;
  }
  return { samples, duration };
}

function validateVideoDimensions(dimensions: Dimensions): void {
  if (!Number.isInteger(dimensions.width) || !Number.isInteger(dimensions.height) ||
    dimensions.width < 1 || dimensions.height < 1 ||
    dimensions.width > STUDIO_MP4_MAX_WIDTH || dimensions.height > STUDIO_MP4_MAX_HEIGHT) {
    fail("invalid_video_dimensions");
  }
}

function parseIsoBoxes(
  bytes: Uint8Array,
  start: number,
  end: number,
  state: IsoParseState,
  depth: number,
): readonly IsoBox[] {
  if (depth > MAX_ISO_BOX_DEPTH || start < 0 || end < start || end > bytes.byteLength) fail("invalid_mp4");
  const boxes: IsoBox[] = [];
  let cursor = start;
  while (cursor < end) {
    if (state.boxes++ >= MAX_ISO_BOXES || end - cursor < 8) fail("invalid_mp4");
    const size32 = uint32(bytes, cursor);
    const type = fourcc(bytes, cursor + 4);
    let headerSize = 8;
    let size: number;
    if (size32 === 0) fail("invalid_mp4");
    if (size32 === 1) {
      if (end - cursor < 16) fail("invalid_mp4");
      const extendedSize = uint64(bytes, cursor + 8);
      if (extendedSize > BigInt(Number.MAX_SAFE_INTEGER)) fail("invalid_mp4");
      size = Number(extendedSize);
      headerSize = 16;
    } else {
      size = size32;
    }
    if (type === "uuid") headerSize += 16;
    if (size < headerSize || size > end - cursor) fail("invalid_mp4");
    boxes.push({ type, start: cursor, dataStart: cursor + headerSize, end: cursor + size });
    cursor += size;
  }
  return Object.freeze(boxes);
}

function exactlyOneBox(boxes: readonly IsoBox[], type: string): IsoBox {
  const matches = boxes.filter((box) => box.type === type);
  if (matches.length !== 1) fail("invalid_mp4");
  const match = matches[0];
  if (!match) fail("invalid_mp4");
  return match;
}

function uint16(bytes: Uint8Array, offset: number): number {
  if (offset < 0 || offset + 2 > bytes.byteLength) fail("invalid_mp4");
  return bytes[offset]! * 0x100 + bytes[offset + 1]!;
}

function uint32(bytes: Uint8Array, offset: number): number {
  if (offset < 0 || offset + 4 > bytes.byteLength) fail("invalid_mp4");
  return bytes[offset]! * 0x1_000000 + bytes[offset + 1]! * 0x1_0000 + bytes[offset + 2]! * 0x100 + bytes[offset + 3]!;
}

function uint64(bytes: Uint8Array, offset: number): bigint {
  if (offset < 0 || offset + 8 > bytes.byteLength) fail("invalid_mp4");
  let value = 0n;
  for (let index = 0; index < 8; index += 1) value = (value << 8n) | BigInt(bytes[offset + index]!);
  return value;
}

function fourcc(bytes: Uint8Array, offset: number): string {
  if (offset < 0 || offset + 4 > bytes.byteLength) fail("invalid_mp4");
  return String.fromCharCode(bytes[offset]!, bytes[offset + 1]!, bytes[offset + 2]!, bytes[offset + 3]!);
}

function byteAt(bytes: Uint8Array, offset: number, code: StudioMediaValidationCode): number {
  if (offset < 0 || offset >= bytes.byteLength) fail(code);
  return bytes[offset]!;
}

function fail(code: StudioMediaValidationCode): never {
  throw new StudioMediaValidationError(code);
}
