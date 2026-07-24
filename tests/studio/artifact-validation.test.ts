import { describe, expect, it } from "vitest";

import {
  STUDIO_PROVIDER_ARTIFACT_MAX_BYTES,
  STUDIO_PROVIDER_ARTIFACT_MAX_CHUNKS,
  StudioArtifactValidationError,
  copyAndRevalidateStudioProviderArtifactForUpload,
  isStudioValidatedProviderArtifact,
  readBoundedStudioProviderArtifact,
  validateStudioProviderArtifact,
  type StudioOutputIndex,
} from "../../lib/studio/artifact-validation.js";

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

function fullBox(payload: Uint8Array): Buffer {
  return Buffer.concat([Buffer.from([0, 0, 0, 0]), payload]);
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

/** A compact valid H.264 MP4 fixture matching the launch output contract. */
function mp4(): Buffer {
  const trackHeader = Buffer.alloc(84);
  trackHeader.writeUInt32BE(1_280 * 0x1_0000, 76);
  trackHeader.writeUInt32BE(720 * 0x1_0000, 80);

  const mediaHeader = Buffer.alloc(24);
  mediaHeader.writeUInt32BE(90_000, 12);
  mediaHeader.writeUInt32BE(90_000, 16);

  const handler = Buffer.alloc(12);
  ascii("vide").copy(handler, 8);

  const avcConfiguration = isoBox("avcC", Buffer.from([
    0x01, 0x64, 0x00, 0x1f, 0xff,
    0xe1, 0x00, 0x01, 0x67,
    0x01, 0x00, 0x01, 0x68,
  ]));
  const sampleEntryHeader = Buffer.alloc(86);
  ascii("avc1").copy(sampleEntryHeader, 4);
  sampleEntryHeader.writeUInt16BE(1_280, 32);
  sampleEntryHeader.writeUInt16BE(720, 34);
  const sampleEntry = Buffer.concat([sampleEntryHeader, avcConfiguration]);
  sampleEntry.writeUInt32BE(sampleEntry.byteLength, 0);

  const sampleDescriptions = isoBox("stsd", fullBox(Buffer.concat([uint32(1), sampleEntry])));
  const timing = isoBox("stts", fullBox(Buffer.concat([uint32(1), uint32(30), uint32(3_000)])));
  const sampleTable = isoBox("stbl", Buffer.concat([sampleDescriptions, timing]));
  const media = isoBox("mdia", Buffer.concat([
    isoBox("mdhd", mediaHeader),
    isoBox("hdlr", handler),
    isoBox("minf", sampleTable),
  ]));
  const videoTrack = isoBox("trak", Buffer.concat([isoBox("tkhd", trackHeader), media]));
  const fileType = isoBox("ftyp", Buffer.concat([
    ascii("isom"),
    uint32(0),
    ascii("iso2"),
    ascii("avc1"),
    ascii("mp41"),
  ]));
  return Buffer.concat([fileType, isoBox("moov", videoTrack), isoBox("mdat", Buffer.alloc(0))]);
}

async function* byteChunks(...chunks: readonly Uint8Array[]): AsyncGenerator<Uint8Array> {
  for (const chunk of chunks) yield chunk;
}

function artifactErrorCode(error: unknown): string | null {
  return error instanceof StudioArtifactValidationError ? error.code : null;
}

describe("artifact validation", () => {
  it("copies a bounded private image stream and derives only Air-owned metadata", async () => {
    const source = jpeg(2, 1);
    const original = Uint8Array.from(source);
    async function* mutableProviderResult(): AsyncGenerator<Uint8Array> {
      yield source;
      // A provider-owned buffer can mutate once the adapter asks for another
      // chunk. The validated artifact must retain the original bytes.
      source.fill(0);
    }

    const artifact = await validateStudioProviderArtifact({
      expectedMediaType: "image",
      declaredMimeType: "IMAGE/JPEG; charset=binary",
      stream: mutableProviderResult(),
      outputIndex: 2,
    });

    expect(artifact).toMatchObject({
      source: "air-validated-provider-output",
      outputIndex: 2,
      mediaType: "image",
      filename: "air-output-2.jpg",
      mimeType: "image/jpeg",
      byteLength: original.byteLength,
      metadata: { width: 2, height: 1 },
    });
    expect(artifact.bytes).toEqual(original);
    expect(artifact.bytes).not.toBe(source);
    expect(isStudioValidatedProviderArtifact(artifact)).toBe(true);
  });

  it("uses the existing ISO-BMFF validator before accepting a video result", async () => {
    const artifact = await validateStudioProviderArtifact({
      expectedMediaType: "video",
      declaredMimeType: "video/mp4; codecs=avc1",
      stream: byteChunks(mp4()),
      outputIndex: 3,
    });

    expect(artifact).toMatchObject({
      source: "air-validated-provider-output",
      outputIndex: 3,
      mediaType: "video",
      filename: "air-output-3.mp4",
      mimeType: "video/mp4",
      metadata: {
        width: 1_280,
        height: 720,
        durationSeconds: 1,
        frameRate: 30,
        videoCodec: "h264",
      },
    });
  });

  it("propagates byte-level media validation failures instead of trusting the provider claim", async () => {
    await expect(validateStudioProviderArtifact({
      expectedMediaType: "image",
      declaredMimeType: "image/jpeg",
      stream: byteChunks(Buffer.from("not an image")),
      outputIndex: 1,
    })).rejects.toMatchObject({
      name: "StudioMediaValidationError",
      code: "unsupported_image",
    });

    await expect(validateStudioProviderArtifact({
      expectedMediaType: "video",
      declaredMimeType: "image/png",
      stream: byteChunks(mp4()),
      outputIndex: 1,
    })).rejects.toSatisfy((error: unknown) => artifactErrorCode(error) === "declared_mime_type_mismatch");
  });

  it("enforces the 25 MiB cap while canceling a web stream before more bytes can be read", async () => {
    let canceled = false;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(STUDIO_PROVIDER_ARTIFACT_MAX_BYTES + 1));
      },
      cancel() {
        canceled = true;
      },
    });

    await expect(readBoundedStudioProviderArtifact(stream)).rejects.toSatisfy(
      (error: unknown) => artifactErrorCode(error) === "artifact_stream_too_large",
    );
    expect(canceled).toBe(true);
  });

  it("stops an infinite-fragmentation attempt even when its byte total is small", async () => {
    let canceled = false;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(new Uint8Array(0));
      },
      cancel() {
        canceled = true;
      },
    });

    await expect(readBoundedStudioProviderArtifact(stream)).rejects.toSatisfy(
      (error: unknown) => artifactErrorCode(error) === "artifact_stream_too_fragmented",
    );
    expect(canceled).toBe(true);
    expect(STUDIO_PROVIDER_ARTIFACT_MAX_CHUNKS).toBe(4_096);
  });

  it("rejects zero-based and out-of-range indexes before consuming a provider body", async () => {
    let reads = 0;
    const unreadable = {
      async *[Symbol.asyncIterator](): AsyncGenerator<Uint8Array> {
        reads += 1;
        yield jpeg(1, 1);
      },
    };

    for (const outputIndex of [0, 5] as const) {
      await expect(validateStudioProviderArtifact({
        expectedMediaType: "image",
        stream: unreadable,
        outputIndex: outputIndex as unknown as StudioOutputIndex,
      })).rejects.toSatisfy((error: unknown) => artifactErrorCode(error) === "invalid_output_index");
    }
    expect(reads).toBe(0);
  });

  it("requires opaque validation provenance, not a copied public discriminator", async () => {
    const artifact = await validateStudioProviderArtifact({
      expectedMediaType: "image",
      stream: byteChunks(jpeg(1, 1)),
      outputIndex: 4,
    });

    expect(isStudioValidatedProviderArtifact({
      id: "inbound-guid",
      mimeType: "image/jpeg",
      sizeBytes: artifact.byteLength,
    })).toBe(false);
    expect(isStudioValidatedProviderArtifact({ ...artifact, filename: "provider-name.jpg" })).toBe(false);
    expect(isStudioValidatedProviderArtifact({ ...artifact, outputIndex: 0 })).toBe(false);

    const forgedSameShape = {
      ...artifact,
      bytes: Uint8Array.from(artifact.bytes),
      metadata: { ...artifact.metadata },
    };
    expect(isStudioValidatedProviderArtifact(forgedSameShape)).toBe(false);
    expect(() => copyAndRevalidateStudioProviderArtifactForUpload(forgedSameShape))
      .toThrow("artifact_not_validated");
  });

  it("uses an unexposed canonical snapshot after the public byte view is mutated", async () => {
    const artifact = await validateStudioProviderArtifact({
      expectedMediaType: "image",
      stream: byteChunks(jpeg(1, 1)),
      outputIndex: 4,
    });
    const original = Uint8Array.from(artifact.bytes);

    artifact.bytes.fill(0);
    const upload = copyAndRevalidateStudioProviderArtifactForUpload(artifact);

    expect(upload.filename).toBe("air-output-4.jpg");
    expect(upload.bytes).toEqual(original);
    expect(upload.bytes).not.toBe(artifact.bytes);
    expect(upload.bytes).not.toEqual(artifact.bytes);
  });
});
