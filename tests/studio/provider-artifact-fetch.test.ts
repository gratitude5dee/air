import { describe, expect, it, vi } from "vitest";

import {
  STUDIO_PROVIDER_ARTIFACT_FETCH_MAX_BYTES,
  StudioProviderArtifactFetchError,
  fetchStudioProviderArtifact,
  validateStudioProviderArtifactReferenceLifetime,
} from "../../lib/studio/provider-artifact-fetch.js";

const policy = {
  host: "storage.googleapis.com",
  pathPrefix: "/gmi-generated-assets/air/",
  access: "signed-url",
} as const;

const DEFAULT_SIGNED_AT_MS = Math.floor(Date.now() / 1_000) * 1_000 - 60_000;
const DEFAULT_EXPIRES_SECONDS = 604_800;

function gcsSigningDate(timeMs: number): string {
  const date = new Date(timeMs);
  const component = (value: number, width: number): string => String(value).padStart(width, "0");
  return `${component(date.getUTCFullYear(), 4)}${component(date.getUTCMonth() + 1, 2)}${component(date.getUTCDate(), 2)}`
    + `T${component(date.getUTCHours(), 2)}${component(date.getUTCMinutes(), 2)}${component(date.getUTCSeconds(), 2)}Z`;
}

function signedQueryFor(
  signedAtMs = DEFAULT_SIGNED_AT_MS,
  expiresSeconds: number | string = DEFAULT_EXPIRES_SECONDS,
): string {
  return [
    "X-Goog-Algorithm=GOOG4-RSA-SHA256",
    "X-Goog-Credential=air%2F20260723%2Fauto%2Fstorage%2Fgoog4_request",
    `X-Goog-Date=${gcsSigningDate(signedAtMs)}`,
    `X-Goog-Expires=${expiresSeconds}`,
    "X-Goog-SignedHeaders=host",
    `X-Goog-Signature=${"a".repeat(64)}`,
  ].join("&");
}

const signedQuery = signedQueryFor();

function signedUrl(
  path = "output-1.png",
  signedAtMs = DEFAULT_SIGNED_AT_MS,
  expiresSeconds: number | string = DEFAULT_EXPIRES_SECONDS,
): string {
  return `https://storage.googleapis.com/gmi-generated-assets/air/${path}?${signedQueryFor(signedAtMs, expiresSeconds)}`;
}

const allowedUrl = signedUrl();

function fetchErrorCode(error: unknown): string | null {
  return error instanceof StudioProviderArtifactFetchError ? error.code : null;
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let byteLength = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      chunks.push(chunk.value);
      byteLength += chunk.value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

describe("provider artifact fetch boundary", () => {
  it("fetches only a validated exact host and non-root path namespace without credentials or redirects", async () => {
    let request: Request | undefined;
    const result = await fetchStudioProviderArtifact({
      url: allowedUrl,
      policy,
      fetch: async (input, init) => {
        request = new Request(input, init);
        return new Response(new Uint8Array([1, 2, 3]), {
          status: 200,
          headers: {
            "content-length": "3",
            "content-type": "IMAGE/PNG; charset=binary",
          },
        });
      },
    });

    expect(request?.url).toBe(allowedUrl);
    expect(request?.method).toBe("GET");
    expect(request?.credentials).toBe("omit");
    expect(request?.redirect).toBe("error");
    expect(request?.referrerPolicy).toBe("no-referrer");
    expect(result.contentLength).toBe(3);
    expect(result.declaredMimeType).toBe("image/png");
    await expect(readAll(result.stream)).resolves.toEqual(new Uint8Array([1, 2, 3]));
  });

  it("rejects a non-HTTPS, wrong-host, root-path, traversal, or fragment URL before fetch", async () => {
    let calls = 0;
    const fetch = async (): Promise<Response> => {
      calls += 1;
      return new Response("should not be fetched");
    };
    const rejectedUrls = [
      `http://storage.googleapis.com/gmi-generated-assets/air/output.png?${signedQuery}`,
      `https://evil.example/gmi-generated-assets/air/output.png?${signedQuery}`,
      "https://storage.googleapis.com/",
      "https://storage.googleapis.com/gmi-generated-assets/air/",
      `https://storage.googleapis.com/gmi-generated-assets/air/%2e%2e/private.png?${signedQuery}`,
      `https://storage.googleapis.com/gmi-generated-assets/air/output.png?${signedQuery}#fragment`,
      `https://storage.googleapis.com:443/gmi-generated-assets/air/output.png?${signedQuery}`,
      `https://user:pass@storage.googleapis.com/gmi-generated-assets/air/output.png?${signedQuery}`,
    ];

    for (const url of rejectedUrls) {
      await expect(fetchStudioProviderArtifact({ url, policy, fetch })).rejects.toSatisfy(
        (error: unknown) => fetchErrorCode(error) === "invalid_url",
      );
    }
    expect(calls).toBe(0);
  });

  it("requires a canonical exact DNS host and a non-root static path prefix", async () => {
    const invalidPolicies = [
      { host: "storage.googleapis.com:443", pathPrefix: "/gmi-generated-assets/air/", access: "signed-url" },
      { host: "STORAGE.googleapis.com", pathPrefix: "/gmi-generated-assets/air/", access: "signed-url" },
      { host: "169.254.169.254", pathPrefix: "/gmi-generated-assets/air/", access: "signed-url" },
      { host: "127.1", pathPrefix: "/gmi-generated-assets/air/", access: "signed-url" },
      { host: "storage.googleapis.com", pathPrefix: "/", access: "signed-url" },
      { host: "storage.googleapis.com", pathPrefix: "/gmi-generated-assets/%2f/", access: "signed-url" },
      { host: "storage.googleapis.com", pathPrefix: "/gmi-generated-assets/../air/", access: "signed-url" },
      { host: "storage.googleapis.com", pathPrefix: "/gmi-generated-assets//air/", access: "signed-url" },
      { host: "storage.googleapis.com", pathPrefix: "/gmi-generated-assets/air/", access: "unauthenticated" },
    ] as const;

    let calls = 0;

    for (const invalidPolicy of invalidPolicies) {
      await expect(fetchStudioProviderArtifact({
        url: allowedUrl,
        // The runtime boundary receives registry-derived data, but this test
        // intentionally supplies an untrusted invalid literal.
        policy: invalidPolicy as unknown as typeof policy,
        fetch: async () => {
          calls += 1;
          return new Response("must not be reached");
        },
      })).rejects.toSatisfy((error: unknown) => fetchErrorCode(error) === "invalid_policy");
    }
    expect(calls).toBe(0);
  });

  it("requires a complete GCS V4 signed URL before an allowed result is fetched", async () => {
    let calls = 0;
    const fetch = async (): Promise<Response> => {
      calls += 1;
      return new Response("must not be fetched");
    };
    const unsignedOrMalformedUrls = [
      "https://storage.googleapis.com/gmi-generated-assets/air/bare-output.png",
      "https://storage.googleapis.com/gmi-generated-assets/air/partial-output.png?X-Goog-Signature=private",
      signedUrl("bad-algorithm.png").replace("GOOG4-RSA-SHA256", "not-a-signature"),
      signedUrl("bad-expiry.png").replace(
        `X-Goog-Expires=${DEFAULT_EXPIRES_SECONDS}`,
        "X-Goog-Expires=0",
      ),
      `${signedUrl("duplicate-signature.png")}&X-Goog-Signature=${"b".repeat(64)}`,
    ];

    for (const url of unsignedOrMalformedUrls) {
      await expect(fetchStudioProviderArtifact({ url, policy, fetch })).rejects.toSatisfy(
        (error: unknown) => fetchErrorCode(error) === "invalid_signed_url",
      );
    }
    expect(calls).toBe(0);
  });

  it("calculates an exact GCS V4 expiry and exposes only safe timing metadata for a conservative persistence gate", () => {
    const signedAtMs = Date.UTC(2026, 6, 25, 12, 0, 0);
    const url = signedUrl("bounded-lifetime.png", signedAtMs, 120);

    const lifetime = validateStudioProviderArtifactReferenceLifetime({
      url,
      policy,
      nowMs: signedAtMs + 45_000,
      minimumRemainingMs: 75_000,
    });

    expect(lifetime).toEqual({
      expiresAtMs: signedAtMs + 120_000,
      remainingMs: 75_000,
    });
    expect(JSON.stringify(lifetime)).not.toContain(url);
    expect(JSON.stringify(lifetime)).not.toContain("X-Goog-Signature");

    let thrown: unknown;
    try {
      validateStudioProviderArtifactReferenceLifetime({
        url,
        policy,
        nowMs: signedAtMs + 45_000,
        minimumRemainingMs: 75_001,
      });
    } catch (error) {
      thrown = error;
    }
    expect(fetchErrorCode(thrown)).toBe("signed_url_insufficient_lifetime");
    expect(String(thrown)).not.toContain(url);
  });

  it("accepts bounded even-length GCS RSA signatures while retaining a strict HMAC digest shape", () => {
    const signedAtMs = Date.UTC(2026, 6, 25, 12, 0, 0);
    const originalSignature = `X-Goog-Signature=${"a".repeat(64)}`;
    const rsaSignature = `X-Goog-Signature=${"b".repeat(512)}`;
    const rsaUrl = signedUrl("rsa-signature.png", signedAtMs, 120).replace(originalSignature, rsaSignature);

    expect(validateStudioProviderArtifactReferenceLifetime({
      url: rsaUrl,
      policy,
      nowMs: signedAtMs,
      minimumRemainingMs: 1,
    })).toEqual({ expiresAtMs: signedAtMs + 120_000, remainingMs: 120_000 });

    const rejectedUrls = [
      rsaUrl.replace("GOOG4-RSA-SHA256", "GOOG4-HMAC-SHA256"),
      rsaUrl.replace(rsaSignature, `X-Goog-Signature=${"b".repeat(511)}`),
      rsaUrl.replace(rsaSignature, `X-Goog-Signature=${"b".repeat(1_026)}`),
    ];
    for (const url of rejectedUrls) {
      let thrown: unknown;
      try {
        validateStudioProviderArtifactReferenceLifetime({
          url,
          policy,
          nowMs: signedAtMs,
          minimumRemainingMs: 1,
        });
      } catch (error) {
        thrown = error;
      }
      expect(fetchErrorCode(thrown)).toBe("invalid_signed_url");
      expect(String(thrown)).not.toContain(url);
    }
  });

  it("permits GCS's documented fifteen-minute pre-active window but rejects a farther-future capability", () => {
    const nowMs = Date.UTC(2026, 6, 25, 12, 0, 0);
    const earlyUsableUrl = signedUrl("early-usable.png", nowMs + 15 * 60_000, 120);

    expect(validateStudioProviderArtifactReferenceLifetime({
      url: earlyUsableUrl,
      policy,
      nowMs,
      minimumRemainingMs: 1,
    })).toEqual({
      expiresAtMs: nowMs + 17 * 60_000,
      remainingMs: 17 * 60_000,
    });

    const notYetUsableUrl = signedUrl("not-yet-usable.png", nowMs + 15 * 60_000 + 1_000, 120);
    let thrown: unknown;
    try {
      validateStudioProviderArtifactReferenceLifetime({
        url: notYetUsableUrl,
        policy,
        nowMs,
        minimumRemainingMs: 1,
      });
    } catch (error) {
      thrown = error;
    }
    expect(fetchErrorCode(thrown)).toBe("signed_url_not_yet_valid");
    expect(String(thrown)).not.toContain(notYetUsableUrl);
  });

  it("rejects malformed calendar timestamps and expired signed URLs before fetch", async () => {
    const signedAtMs = Date.UTC(2026, 6, 25, 12, 0, 0);
    const expiredUrl = signedUrl("expired.png", signedAtMs, 60);
    const malformedCalendarUrl = signedUrl("invalid-calendar.png", signedAtMs, 60)
      .replace("X-Goog-Date=20260725T120000Z", "X-Goog-Date=20260229T120000Z");

    expect(() => validateStudioProviderArtifactReferenceLifetime({
      url: malformedCalendarUrl,
      policy,
      nowMs: signedAtMs,
      minimumRemainingMs: 1,
    })).toThrowError(StudioProviderArtifactFetchError);
    try {
      validateStudioProviderArtifactReferenceLifetime({
        url: malformedCalendarUrl,
        policy,
        nowMs: signedAtMs,
        minimumRemainingMs: 1,
      });
    } catch (error) {
      expect(fetchErrorCode(error)).toBe("invalid_signed_url");
      expect(String(error)).not.toContain(malformedCalendarUrl);
    }

    let calls = 0;
    vi.spyOn(Date, "now").mockReturnValue(signedAtMs + 60_000);
    await expect(fetchStudioProviderArtifact({
      url: expiredUrl,
      policy,
      fetch: async () => {
        calls += 1;
        return new Response("must not be fetched");
      },
    })).rejects.toSatisfy((error: unknown) => {
      expect(String(error)).not.toContain(expiredUrl);
      return fetchErrorCode(error) === "signed_url_expired";
    });
    expect(calls).toBe(0);
  });

  it("requires a provider-specific authenticated retrieval implementation for provider-private outputs", async () => {
    let calls = 0;
    const providerPrivatePolicy = {
      host: "storage.googleapis.com",
      pathPrefix: "/gmi-generated-assets/air/",
      access: "provider-private",
    } as const;

    await expect(fetchStudioProviderArtifact({
      url: signedUrl("provider-private.png"),
      policy: providerPrivatePolicy,
      fetch: async () => {
        calls += 1;
        return new Response("must not be fetched");
      },
    })).rejects.toSatisfy(
      (error: unknown) => fetchErrorCode(error) === "provider_private_transport_required",
    );
    expect(calls).toBe(0);
  });

  it("rejects non-200 responses and oversized or malformed Content-Length without exposing the URL", async () => {
    const secretUrl = signedUrl("private-output.png");
    const cases = [
      { response: new Response("ignored", { status: 403 }), code: "unexpected_status" },
      {
        response: new Response(new Uint8Array([1]), {
          status: 200,
          headers: { "content-length": String(STUDIO_PROVIDER_ARTIFACT_FETCH_MAX_BYTES + 1) },
        }),
        code: "content_length_too_large",
      },
      {
        response: new Response(new Uint8Array([1]), {
          status: 200,
          headers: { "content-length": "4; private-provider-detail" },
        }),
        code: "content_length_invalid",
      },
    ] as const;

    for (const { response, code } of cases) {
      let thrown: unknown;
      try {
        await fetchStudioProviderArtifact({ url: secretUrl, policy, fetch: async () => response });
      } catch (error) {
        thrown = error;
      }
      expect(fetchErrorCode(thrown)).toBe(code);
      expect(String(thrown)).not.toContain(secretUrl);
      expect(String(thrown)).not.toContain("top-secret");
    }
  });

  it("enforces the same cap on streamed bytes when a provider lies about or omits Content-Length", async () => {
    let canceled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]));
        controller.enqueue(new Uint8Array([4, 5, 6]));
      },
      cancel() {
        canceled = true;
      },
    });
    const artifact = await fetchStudioProviderArtifact({
      url: allowedUrl,
      policy,
      maxBytes: 5,
      fetch: async () => new Response(body, { status: 200 }),
    });
    const reader = artifact.stream.getReader();

    await expect(reader.read()).resolves.toMatchObject({ done: false, value: new Uint8Array([1, 2, 3]) });
    await expect(reader.read()).rejects.toSatisfy(
      (error: unknown) => fetchErrorCode(error) === "response_stream_too_large",
    );
    expect(canceled).toBe(true);
  });

  it("converts a provider stream failure and an elapsed deadline into stable redacted errors", async () => {
    const streamFailure = new ReadableStream<Uint8Array>({
      pull() {
        throw new Error("private provider URL: https://storage.googleapis.com/secret");
      },
    });
    const failedArtifact = await fetchStudioProviderArtifact({
      url: allowedUrl,
      policy,
      fetch: async () => new Response(streamFailure, { status: 200 }),
    });
    await expect(readAll(failedArtifact.stream)).rejects.toSatisfy(
      (error: unknown) => fetchErrorCode(error) === "response_stream_failure",
    );

    const timeoutUrl = signedUrl("timeout.png");
    await expect(fetchStudioProviderArtifact({
      url: timeoutUrl,
      policy,
      timeoutMs: 250,
      fetch: async (_input, init) => new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("private transport detail", "AbortError"));
        }, { once: true });
      }),
    })).rejects.toSatisfy((error: unknown) => {
      expect(String(error)).not.toContain(timeoutUrl);
      return fetchErrorCode(error) === "request_timeout";
    });
  });
});
