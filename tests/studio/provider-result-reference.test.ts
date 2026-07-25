import { describe, expect, it } from "vitest";

import {
  createStudioProviderResultReference,
  encodeStudioProviderResultReferenceForPersistence,
  isStudioProviderResultReference,
  openStudioProviderResultReference,
  restoreStudioProviderResultReferenceFromPersistence,
} from "../../lib/studio/provider-result-reference.js";

const FINGERPRINT = `sha256:${"a".repeat(64)}`;
const PRIVATE_URL = `https://storage.googleapis.com/air/private-output.jpg?X-Goog-Signature=${"b".repeat(64)}`;

function reference() {
  return createStudioProviderResultReference({
    jobId: "opaque-air-job-001",
    provider: "gmi",
    adapterKey: "gmi.flux-kontext-pro.v1",
    providerModelId: "flux-kontext-pro",
    resultSchemaFingerprint: FINGERPRINT,
    outputs: [{ outputIndex: 1, mediaType: "image" }],
    material: { kind: "signed-url", url: PRIVATE_URL },
  });
}

function expectedBinding() {
  return {
    jobId: "opaque-air-job-001",
    provider: "gmi" as const,
    adapterKey: "gmi.flux-kontext-pro.v1",
    providerModelId: "flux-kontext-pro",
    resultSchemaFingerprint: FINGERPRINT,
    outputCount: 1,
    outputs: [{ outputIndex: 1 as const, mediaType: "image" as const }],
  };
}

describe("sealed provider result references", () => {
  it("keeps a retrieval URL out of the public serializable shape", () => {
    const sealed = reference();

    expect(isStudioProviderResultReference(sealed)).toBe(true);
    expect(Object.isFrozen(sealed)).toBe(true);
    expect(Object.isFrozen(sealed.outputs)).toBe(true);
    expect(JSON.stringify(sealed)).not.toContain("X-Goog-Signature");
    expect(JSON.stringify(sealed)).not.toContain("private-output.jpg");
    expect(Object.keys(sealed)).not.toContain("url");
    expect(openStudioProviderResultReference(sealed)).toEqual({ kind: "signed-url", url: PRIVATE_URL });
  });

  it("rejects copied and manually shaped references at the materialization boundary", () => {
    const sealed = reference();
    const copied = { ...sealed };
    const forged = {
      ...sealed,
      source: "air-sealed-provider-result-reference",
      schemaVersion: 1,
    };

    expect(isStudioProviderResultReference(copied)).toBe(false);
    expect(openStudioProviderResultReference(copied)).toBeNull();
    expect(isStudioProviderResultReference(forged)).toBe(false);
    expect(openStudioProviderResultReference(forged)).toBeNull();
  });

  it("rejects malformed private material before creating a capability", () => {
    expect(() => createStudioProviderResultReference({
      jobId: "opaque-air-job-001",
      provider: "gmi",
      adapterKey: "gmi.flux-kontext-pro.v1",
      providerModelId: "flux-kontext-pro",
      resultSchemaFingerprint: FINGERPRINT,
      outputs: [{ outputIndex: 1, mediaType: "image" }],
      material: { kind: "signed-url", url: "https://example.test/ok\nunsafe" },
    })).toThrow("invalid_provider_result_reference");
  });

  it("round-trips a fresh sealed reference through an explicit encrypted-persistence payload", () => {
    const sealed = reference();
    const persisted = encodeStudioProviderResultReferenceForPersistence(sealed);

    expect(persisted).not.toBeNull();
    if (persisted === null) throw new Error("expected persistence payload");
    expect(Object.isFrozen(persisted)).toBe(true);
    expect(persisted.binding).toEqual(expectedBinding());
    expect(persisted.material).toEqual({ kind: "signed-url", url: PRIVATE_URL });

    // Simulate a cipher's JSON payload after a Vercel invocation boundary.
    const decrypted = JSON.parse(JSON.stringify(persisted)) as unknown;
    const restored = restoreStudioProviderResultReferenceFromPersistence(decrypted, expectedBinding());

    expect(restored).not.toBeNull();
    if (restored === null) throw new Error("expected restored sealed reference");
    expect(restored).not.toBe(sealed);
    expect(isStudioProviderResultReference(restored)).toBe(true);
    expect(JSON.stringify(restored)).not.toContain("X-Goog-Signature");
    expect(JSON.stringify(restored)).not.toContain("private-output.jpg");
    expect(openStudioProviderResultReference(restored)).toEqual({ kind: "signed-url", url: PRIVATE_URL });
  });

  it("fails closed for copied references, malformed payloads, and binding mismatches", () => {
    const sealed = reference();
    const persisted = encodeStudioProviderResultReferenceForPersistence(sealed);
    if (persisted === null) throw new Error("expected persistence payload");

    expect(encodeStudioProviderResultReferenceForPersistence({ ...sealed })).toBeNull();

    const malformed = {
      ...persisted,
      binding: { ...persisted.binding, outputCount: 2 },
    };
    expect(restoreStudioProviderResultReferenceFromPersistence(malformed, expectedBinding())).toBeNull();

    const extraMetadata = { ...persisted, metadata: { url: PRIVATE_URL } };
    expect(restoreStudioProviderResultReferenceFromPersistence(extraMetadata, expectedBinding())).toBeNull();

    expect(restoreStudioProviderResultReferenceFromPersistence(
      persisted,
      { ...expectedBinding(), jobId: "opaque-air-job-002" },
    )).toBeNull();
    expect(restoreStudioProviderResultReferenceFromPersistence(
      persisted,
      { ...expectedBinding(), outputs: [{ outputIndex: 1 as const, mediaType: "video" as const }] },
    )).toBeNull();
  });
});
