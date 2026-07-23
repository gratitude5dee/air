import { describe, expect, it } from "vitest";

import {
  STUDIO_PROVIDER_ARTIFACT_FETCH_MAX_BYTES,
  StudioProviderArtifactFetchError,
  fetchStudioProviderArtifact,
} from "../../lib/studio/provider-artifact-fetch.js";

const policy = {
  host: "storage.googleapis.com",
  pathPrefix: "/gmi-generated-assets/air/",
} as const;

const allowedUrl = "https://storage.googleapis.com/gmi-generated-assets/air/output-1.png?signature=private";

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
      "http://storage.googleapis.com/gmi-generated-assets/air/output.png",
      "https://evil.example/gmi-generated-assets/air/output.png",
      "https://storage.googleapis.com/",
      "https://storage.googleapis.com/gmi-generated-assets/air/",
      "https://storage.googleapis.com/gmi-generated-assets/air/%2e%2e/private.png",
      "https://storage.googleapis.com/gmi-generated-assets/air/output.png#fragment",
      "https://storage.googleapis.com:443/gmi-generated-assets/air/output.png",
      "https://user:pass@storage.googleapis.com/gmi-generated-assets/air/output.png",
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
      { host: "storage.googleapis.com:443", pathPrefix: "/gmi-generated-assets/air/" },
      { host: "STORAGE.googleapis.com", pathPrefix: "/gmi-generated-assets/air/" },
      { host: "storage.googleapis.com", pathPrefix: "/" },
      { host: "storage.googleapis.com", pathPrefix: "/gmi-generated-assets/%2f/" },
      { host: "storage.googleapis.com", pathPrefix: "/gmi-generated-assets/../air/" },
      { host: "storage.googleapis.com", pathPrefix: "/gmi-generated-assets//air/" },
    ] as const;

    for (const invalidPolicy of invalidPolicies) {
      await expect(fetchStudioProviderArtifact({
        url: allowedUrl,
        policy: invalidPolicy,
        fetch: async () => new Response("must not be reached"),
      })).rejects.toSatisfy((error: unknown) => fetchErrorCode(error) === "invalid_policy");
    }
  });

  it("rejects non-200 responses and oversized or malformed Content-Length without exposing the URL", async () => {
    const secretUrl = "https://storage.googleapis.com/gmi-generated-assets/air/private-output.png?signature=top-secret";
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

    const timeoutUrl = "https://storage.googleapis.com/gmi-generated-assets/air/timeout.png?signature=private";
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
