import { describe, expect, it } from "vitest";

import {
  createStudioProviderResultReference,
  isStudioProviderResultReference,
  openStudioProviderResultReference,
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
});
