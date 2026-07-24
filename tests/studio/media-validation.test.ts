import { deflateSync } from "node:zlib";

import { describe, expect, it } from "vitest";
import {
  STUDIO_IMAGE_INPUT_MAX_BYTES,
  STUDIO_IMAGE_OUTPUT_MAX_BYTES,
  STUDIO_MP4_MAX_BYTES,
  StudioMediaValidationError,
  validateStudioImageMetadata,
  validateStudioMp4,
  type StudioMediaValidationCode,
} from "../../lib/studio/media-validation.js";

function ascii(value: string): Buffer {
  if (value.length !== 4) throw new Error(`expected a fourcc, got ${value}`);
  return Buffer.from(value, "ascii");
}

function uint16(value: number): Buffer {
  const bytes = Buffer.alloc(2);
  bytes.writeUInt16BE(value);
  return bytes;
}

function uint32(value: number): Buffer {
  const bytes = Buffer.alloc(4);
  bytes.writeUInt32BE(value);
  return bytes;
}

function isoBox(type: string, payload: Uint8Array): Buffer {
  return Buffer.concat([uint32(payload.byteLength + 8), ascii(type), payload]);
}

function fullBox(payload: Uint8Array, version = 0): Buffer {
  return Buffer.concat([Buffer.from([version, 0, 0, 0]), payload]);
}

function descriptor(tag: number, payload: Uint8Array): Buffer {
  if (payload.byteLength > 0x7f) throw new Error("fixture descriptor is too large");
  return Buffer.concat([Buffer.from([tag, payload.byteLength]), payload]);
}

function jpeg(width: number, height: number): Buffer {
  return Buffer.from([
    0xff, 0xd8,
    0xff, 0xc0, 0x00, 0x0b, 0x08,
    (height >>> 8) & 0xff, height & 0xff,
    (width >>> 8) & 0xff, width & 0xff,
    0x01,
    0x01, 0x11, 0x00,
    0xff, 0xda, 0x00, 0x08,
    0x01, 0x01, 0x00,
    0x00, 0x3f, 0x00,
    0x00,
    0xff, 0xd9,
  ]);
}

function jpegHeaderOnly(width: number, height: number): Buffer {
  return Buffer.from([
    0xff, 0xd8,
    0xff, 0xc0, 0x00, 0x0b, 0x08,
    (height >>> 8) & 0xff, height & 0xff,
    (width >>> 8) & 0xff, width & 0xff,
    0x01,
    0x01, 0x11, 0x00,
    0xff, 0xd9,
  ]);
}

function paddedJpeg(byteLength: number): Buffer {
  const source = jpeg(1, 1);
  const output = Buffer.alloc(byteLength);
  source.copy(output, 0, 0, source.byteLength - 2);
  output[byteLength - 2] = 0xff;
  output[byteLength - 1] = 0xd9;
  return output;
}

function pngChunk(type: string, data: Uint8Array): Buffer {
  const typeBytes = ascii(type);
  return Buffer.concat([uint32(data.byteLength), typeBytes, data, uint32(pngCrc32(Buffer.concat([typeBytes, data])))]);
}

function png(width: number, height: number): Buffer {
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  const raster = Buffer.alloc(height * (1 + width * 4));
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk("IHDR", ihdr),
    pngChunk("IDAT", deflateSync(raster)),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

function invalidCompressedPng(): Buffer {
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(1, 0);
  ihdr.writeUInt32BE(1, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk("IHDR", ihdr),
    pngChunk("IDAT", Buffer.from([0])),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

function pngCrc32(bytes: Uint8Array): number {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ ((crc & 1) === 1 ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function heicFileType(): Buffer {
  return isoBox("ftyp", Buffer.concat([
    ascii("isom"),
    uint32(0),
    ascii("heic"),
  ]));
}

function trackHeader(width: number, height: number): Buffer {
  const payload = Buffer.alloc(84);
  payload.writeUInt32BE(width * 0x1_0000, 76);
  payload.writeUInt32BE(height * 0x1_0000, 80);
  return isoBox("tkhd", payload);
}

function mediaHeader(timeScale: number, duration: number): Buffer {
  const payload = Buffer.alloc(24);
  payload.writeUInt32BE(timeScale, 12);
  payload.writeUInt32BE(duration, 16);
  return isoBox("mdhd", payload);
}

function handler(type: "vide" | "soun"): Buffer {
  const payload = Buffer.alloc(12);
  ascii(type).copy(payload, 8);
  return isoBox("hdlr", payload);
}

function sampleTiming(samples: number, delta: number): Buffer {
  return isoBox("stts", fullBox(Buffer.concat([uint32(1), uint32(samples), uint32(delta)])));
}

function avcConfiguration(): Buffer {
  return isoBox("avcC", Buffer.from([
    0x01, 0x64, 0x00, 0x1f, 0xff,
    0xe1, 0x00, 0x01, 0x67,
    0x01, 0x00, 0x01, 0x68,
  ]));
}

function h264SampleEntry(width: number, height: number, codec = "avc1"): Buffer {
  const visualHeader = Buffer.alloc(86);
  ascii(codec).copy(visualHeader, 4);
  visualHeader.writeUInt16BE(width, 32);
  visualHeader.writeUInt16BE(height, 34);
  const entry = Buffer.concat([visualHeader, avcConfiguration()]);
  entry.writeUInt32BE(entry.byteLength, 0);
  return entry;
}

function aacSampleEntry(decoderObjectType = 0x40): Buffer {
  const audioSpecificConfig = descriptor(0x05, Buffer.from([0x12, 0x10]));
  const decoderConfig = descriptor(0x04, Buffer.concat([
    Buffer.from([decoderObjectType, 0x15, 0x00, 0x00, 0x00]),
    uint32(0),
    uint32(0),
    audioSpecificConfig,
  ]));
  const es = descriptor(0x03, Buffer.concat([
    uint16(1),
    Buffer.from([0]),
    decoderConfig,
    descriptor(0x06, Buffer.from([0x02])),
  ]));
  const audioHeader = Buffer.alloc(36);
  ascii("mp4a").copy(audioHeader, 4);
  const entry = Buffer.concat([audioHeader, isoBox("esds", fullBox(es))]);
  entry.writeUInt32BE(entry.byteLength, 0);
  return entry;
}

function sampleDescriptions(entry: Uint8Array): Buffer {
  return isoBox("stsd", fullBox(Buffer.concat([uint32(1), entry])));
}

function videoTrack(input: {
  readonly width: number;
  readonly height: number;
  readonly duration: number;
  readonly samples: number;
  readonly delta: number;
  readonly codec?: string | undefined;
}): Buffer {
  const table = isoBox("stbl", Buffer.concat([
    sampleDescriptions(h264SampleEntry(input.width, input.height, input.codec)),
    sampleTiming(input.samples, input.delta),
  ]));
  const media = isoBox("mdia", Buffer.concat([
    mediaHeader(90_000, input.duration),
    handler("vide"),
    isoBox("minf", table),
  ]));
  return isoBox("trak", Buffer.concat([trackHeader(input.width, input.height), media]));
}

function audioTrack(duration: number, decoderObjectType = 0x40): Buffer {
  const table = isoBox("stbl", Buffer.concat([
    sampleDescriptions(aacSampleEntry(decoderObjectType)),
    sampleTiming(1, duration),
  ]));
  const media = isoBox("mdia", Buffer.concat([
    mediaHeader(90_000, duration),
    handler("soun"),
    isoBox("minf", table),
  ]));
  return isoBox("trak", Buffer.concat([trackHeader(0, 0), media]));
}

function mp4(input: {
  readonly width?: number | undefined;
  readonly height?: number | undefined;
  readonly samples?: number | undefined;
  readonly delta?: number | undefined;
  readonly duration?: number | undefined;
  readonly codec?: string | undefined;
  readonly audio?: boolean | undefined;
  readonly audioObjectType?: number | undefined;
} = {}): Buffer {
  const samples = input.samples ?? 30;
  const delta = input.delta ?? 3_000;
  const duration = input.duration ?? samples * delta;
  const video = videoTrack({
    width: input.width ?? 1_280,
    height: input.height ?? 720,
    duration,
    samples,
    delta,
    codec: input.codec,
  });
  const ftyp = isoBox("ftyp", Buffer.concat([
    ascii("isom"),
    uint32(0),
    ascii("iso2"),
    ascii("avc1"),
    ascii("mp41"),
  ]));
  const movie = isoBox("moov", Buffer.concat([
    video,
    ...(input.audio ? [audioTrack(duration, input.audioObjectType)] : []),
  ]));
  return Buffer.concat([ftyp, movie, isoBox("mdat", Buffer.from([0]))]);
}

function failureCode(action: () => unknown): StudioMediaValidationCode {
  try {
    action();
  } catch (error) {
    expect(error).toBeInstanceOf(StudioMediaValidationError);
    return (error as StudioMediaValidationError).code;
  }
  throw new Error("expected media validation to fail");
}

describe("studio image metadata validation", () => {
  it("accepts JPEG and PNG magic bytes only after dimensions are bounded", () => {
    const ceilingJpeg = jpeg(4_096, 4_096);
    expect(validateStudioImageMetadata(jpeg(4_096, 4_096), { mimeType: "image/jpeg" })).toEqual({
      kind: "jpeg",
      mimeType: "image/jpeg",
      byteLength: ceilingJpeg.byteLength,
      width: 4_096,
      height: 4_096,
      pixels: 16_777_216,
      decodedRgbaBytes: 64 * 1024 * 1024,
    });
    expect(validateStudioImageMetadata(png(32, 18))).toMatchObject({
      kind: "png",
      width: 32,
      height: 18,
      decodedRgbaBytes: 32 * 18 * 4,
    });
  });

  it("rejects image dimensions and declared types that cannot be safely decoded", () => {
    expect(failureCode(() => validateStudioImageMetadata(jpeg(4_097, 1)))).toBe("invalid_image_dimensions");
    expect(failureCode(() => validateStudioImageMetadata(png(1, 4_097)))).toBe("invalid_image_dimensions");
    expect(failureCode(() => validateStudioImageMetadata(jpeg(1, 1), { mimeType: "image/png" }))).toBe("invalid_image");
    expect(failureCode(() => validateStudioImageMetadata(Buffer.from([0xff, 0xd8, 0xff])))).toBe("invalid_image");
  });

  it("requires a terminal JPEG scan instead of accepting a frame header as media", () => {
    expect(failureCode(() => validateStudioImageMetadata(jpegHeaderOnly(1, 1)))).toBe("invalid_image");

    // This ordinary JFIF sample exercises DQT/DHT segments, a real SOS
    // envelope, byte-stuffed entropy data, and terminal EOI.
    const jfif = Buffer.from(
      "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////2wBDAf//////////////////////////////////////////////////////////////////////////////////////wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIQAxAAAAF//8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABBQJ//8QAFBEBAAAAAAAAAAAAAAAAAAAAAP/aAAgBAwEBPwF//8QAFBEBAAAAAAAAAAAAAAAAAAAAAP/aAAgBAgEBPwF//8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQAGPwJ//8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABPyF//9k=",
      "base64",
    );
    expect(validateStudioImageMetadata(jfif)).toMatchObject({
      kind: "jpeg",
      width: 1,
      height: 1,
    });
  });

  it("requires checksum-valid, bounded PNG scanlines instead of trusting an IHDR", () => {
    const canonicalPng = Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
      "base64",
    );
    expect(validateStudioImageMetadata(canonicalPng)).toMatchObject({
      kind: "png",
      width: 1,
      height: 1,
    });

    const badCrc = png(1, 1);
    const lastByte = badCrc.byteLength - 1;
    badCrc[lastByte] = (badCrc[lastByte] ?? 0) ^ 0x01;
    expect(failureCode(() => validateStudioImageMetadata(badCrc))).toBe("invalid_image");
    expect(failureCode(() => validateStudioImageMetadata(invalidCompressedPng()))).toBe("invalid_image");
  });

  it("uses separate input and output ceilings and explicitly blocks HEIC/HEIF", () => {
    const aboveInputLimit = paddedJpeg(STUDIO_IMAGE_INPUT_MAX_BYTES + 1);
    expect(failureCode(() => validateStudioImageMetadata(aboveInputLimit))).toBe("image_too_large");
    expect(validateStudioImageMetadata(aboveInputLimit, { role: "output" })).toMatchObject({
      kind: "jpeg",
      byteLength: STUDIO_IMAGE_INPUT_MAX_BYTES + 1,
    });
    expect(failureCode(() => validateStudioImageMetadata(paddedJpeg(STUDIO_IMAGE_OUTPUT_MAX_BYTES + 1), { role: "output" })))
      .toBe("image_too_large");
    expect(failureCode(() => validateStudioImageMetadata(heicFileType()))).toBe("heic_not_supported");
    expect(failureCode(() => validateStudioImageMetadata(jpeg(1, 1), { mimeType: "image/heif" }))).toBe("heic_not_supported");
  });
});

describe("bounded MP4 metadata validation", () => {
  it("accepts H.264 MP4 video at the duration, dimension, and fps ceilings", () => {
    const metadata = validateStudioMp4(mp4({ samples: 450, delta: 3_000 }));
    expect(metadata).toEqual({
      mimeType: "video/mp4",
      byteLength: expect.any(Number),
      width: 1_280,
      height: 720,
      durationSeconds: 15,
      frameRate: 30,
      videoCodec: "h264",
    });
    expect(validateStudioMp4(mp4({ audio: true }))).toMatchObject({ audioCodec: "aac" });
  });

  it("rejects non-H.264 tracks, excess duration, dimensions, and frame rates", () => {
    expect(failureCode(() => validateStudioMp4(mp4({ codec: "hvc1" })))).toBe("unsupported_video_codec");
    expect(failureCode(() => validateStudioMp4(mp4({ width: 1_921 })))).toBe("invalid_video_dimensions");
    expect(failureCode(() => validateStudioMp4(mp4({ samples: 480, delta: 3_000 })))).toBe("video_too_long");
    expect(failureCode(() => validateStudioMp4(mp4({ samples: 31, delta: 2_900 })))).toBe("video_frame_rate_exceeded");
    expect(failureCode(() => validateStudioMp4(mp4({ audio: true, audioObjectType: 0x6b })))).toBe("unsupported_audio_codec");
  });

  it("fails closed for malformed, unbounded, and oversize ISO-BMFF input", () => {
    expect(failureCode(() => validateStudioMp4(Buffer.concat([
      uint32(0), ascii("ftyp"), ascii("isom"), uint32(0), ascii("avc1"),
    ])))).toBe("invalid_mp4");
    expect(failureCode(() => validateStudioMp4(Buffer.concat([
      uint32(64), ascii("ftyp"), ascii("isom"), uint32(0), ascii("avc1"),
    ])))).toBe("invalid_mp4");
    expect(failureCode(() => validateStudioMp4(new Uint8Array(STUDIO_MP4_MAX_BYTES + 1)))).toBe("video_too_large");
  });
});
