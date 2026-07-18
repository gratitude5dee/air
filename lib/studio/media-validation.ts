/**
 * Bounded, dependency-free media validation for Air's direct studio. These
 * helpers inspect bytes that have already passed a bounded download; they do
 * not decode pixels or media samples, and they reject anything they cannot
 * prove is within the studio contract.
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
  while (cursor < bytes.byteLength - 2) {
    if (segments++ > 256 || bytes[cursor] !== 0xff) fail("invalid_image");
    while (cursor < bytes.byteLength && bytes[cursor] === 0xff) cursor += 1;
    if (cursor >= bytes.byteLength) fail("invalid_image");
    const marker = bytes[cursor]!;
    cursor += 1;

    if (marker === 0x00 || marker === 0xd8 || marker === 0xd9) fail("invalid_image");
    if ((marker >= 0xd0 && marker <= 0xd7) || marker === 0x01) continue;
    if (marker === 0xda) fail("invalid_image");
    if (cursor + 2 > bytes.byteLength) fail("invalid_image");
    const segmentLength = uint16(bytes, cursor);
    if (segmentLength < 2 || segmentLength > bytes.byteLength - cursor) fail("invalid_image");
    const segmentEnd = cursor + segmentLength;
    if (isJpegStartOfFrame(marker)) {
      if (segmentLength < 8 || cursor + 7 > segmentEnd) fail("invalid_image");
      const height = uint16(bytes, cursor + 3);
      const width = uint16(bytes, cursor + 5);
      return { width, height };
    }
    cursor = segmentEnd;
  }
  fail("invalid_image");
}

function isJpegStartOfFrame(marker: number): boolean {
  return (marker >= 0xc0 && marker <= 0xc3) ||
    (marker >= 0xc5 && marker <= 0xc7) ||
    (marker >= 0xc9 && marker <= 0xcb) ||
    (marker >= 0xcd && marker <= 0xcf);
}

function parsePngDimensions(bytes: Uint8Array): Dimensions {
  if (!hasPngMagic(bytes) || bytes.byteLength < 33 || uint32(bytes, 8) !== 13 || fourcc(bytes, 12) !== "IHDR") {
    fail("invalid_image");
  }
  const width = uint32(bytes, 16);
  const height = uint32(bytes, 20);
  validatePngIhdr(bytes);

  let cursor = 8;
  let chunks = 0;
  let sawIdat = false;
  while (cursor < bytes.byteLength) {
    if (chunks++ > 4_096 || cursor + 12 > bytes.byteLength) fail("invalid_image");
    const length = uint32(bytes, cursor);
    const type = fourcc(bytes, cursor + 4);
    if (!isPngChunkType(type) || length > bytes.byteLength - cursor - 12) fail("invalid_image");
    const end = cursor + 12 + length;
    if (cursor === 8 && type !== "IHDR") fail("invalid_image");
    if (type === "IDAT") sawIdat = true;
    if (type === "IEND") {
      if (length !== 0 || !sawIdat || end !== bytes.byteLength) fail("invalid_image");
      return { width, height };
    }
    cursor = end;
  }
  fail("invalid_image");
}

function validatePngIhdr(bytes: Uint8Array): void {
  const bitDepth = bytes[24]!;
  const colorType = bytes[25]!;
  const validDepth = (colorType === 0 && [1, 2, 4, 8, 16].includes(bitDepth)) ||
    (colorType === 2 && [8, 16].includes(bitDepth)) ||
    (colorType === 3 && [1, 2, 4, 8].includes(bitDepth)) ||
    (colorType === 4 && [8, 16].includes(bitDepth)) ||
    (colorType === 6 && [8, 16].includes(bitDepth));
  if (!validDepth || bytes[26] !== 0 || bytes[27] !== 0 || (bytes[28] !== 0 && bytes[28] !== 1)) {
    fail("invalid_image");
  }
}

function isPngChunkType(type: string): boolean {
  return type.length === 4 && [...type].every((character) =>
    (character >= "A" && character <= "Z") || (character >= "a" && character <= "z"),
  );
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
