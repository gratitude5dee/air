import { describe, expect, it } from "vitest";
import {
  createZapVideoServiceClient,
  ZapVideoServiceError,
} from "../../lib/zap-video-service.js";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function client(fetch: typeof globalThis.fetch) {
  return createZapVideoServiceClient({
    serviceUrl: "https://zap.example.test",
    serviceToken: "zap-service-token",
    fetch,
  });
}

describe("Zap video service client", () => {
  it("uses private bearer auth and strictly parses a presigned upload response", async () => {
    const requests: Request[] = [];
    const service = client(async (input, init) => {
      requests.push(new Request(input, init));
      return json({
        uploadUrl: "https://storage.googleapis.com/air-upload/put?signature=opaque",
        publicUrl: "https://storage.googleapis.com/air/frame.jpg",
        headers: { "content-type": "image/jpeg", "x-amz-acl": "private" },
        method: "PUT",
        expiresAt: "2099-01-01T00:00:00.000Z",
      }, 201);
    });

    const upload = await service.createUpload({
      mimeType: "image/jpeg",
      sizeBytes: 1_024,
      idempotencyKey: "b".repeat(64),
    });

    expect(upload).toEqual({
      uploadUrl: "https://storage.googleapis.com/air-upload/put?signature=opaque",
      publicUrl: "https://storage.googleapis.com/air/frame.jpg",
      headers: { "content-type": "image/jpeg", "x-amz-acl": "private" },
      method: "PUT",
      expiresAt: "2099-01-01T00:00:00.000Z",
    });
    expect(requests).toHaveLength(1);
    const request = requests[0];
    expect(request?.url).toBe("https://zap.example.test/api/services/air/v1/uploads");
    expect(request?.headers.get("authorization")).toBe("Bearer zap-service-token");
    expect(request?.headers.get("idempotency-key")).toBe("b".repeat(64));
    expect(await request?.json()).toEqual({ mimeType: "image/jpeg", sizeBytes: 1_024 });
  });

  it("submits a run with an opaque idempotency key and parses first queued acceptance", async () => {
    let request: Request | undefined;
    const service = client(async (input, init) => {
      request = new Request(input, init);
      return json({ runId: "run_123", status: "queued" }, 202);
    });

    await expect(service.createRun({
      prompt: "  A cinematic paper airplane over a neon city  ",
      firstFrameUrl: "https://storage.googleapis.com/air/frame.jpg",
      idempotencyKey: "a".repeat(64),
    })).resolves.toEqual({ runId: "run_123", status: "queued" });

    expect(request?.url).toBe("https://zap.example.test/api/services/air/v1/runs");
    expect(request?.headers.get("idempotency-key")).toBe("a".repeat(64));
    expect(await request?.json()).toEqual({
      prompt: "A cinematic paper airplane over a neon city",
      firstFrameUrl: "https://storage.googleapis.com/air/frame.jpg",
    });
  });

  it("rejects malformed completion data and mismatched run IDs without exposing its body", async () => {
    const service = client(async () => json({
      runId: "different-run",
      status: "video_ready",
      video: { url: "https://blob.vercel-storage.com/air/output.mp4", mimeType: "video/mp4" },
    }));

    await expect(service.getRun("run_123")).rejects.toMatchObject({
      name: "ZapVideoServiceError",
      code: "invalid_response",
      retryable: false,
    });
  });

  it("rejects ambiguous case-insensitive upload headers", async () => {
    const service = client(async () => json({
      uploadUrl: "https://storage.googleapis.com/air-upload/put",
      publicUrl: "https://storage.googleapis.com/air/frame.jpg",
      headers: { "content-type": "image/jpeg", "X-Amz-Acl": "private", "x-amz-acl": "public-read" },
      method: "PUT",
      expiresAt: "2099-01-01T00:00:00.000Z",
    }));

    await expect(service.createUpload({
      mimeType: "image/jpeg",
      sizeBytes: 1,
      idempotencyKey: "b".repeat(64),
    })).rejects.toMatchObject({
      code: "invalid_response",
      retryable: false,
    });
  });

  it("maps service availability faults to a safe retryable error", async () => {
    const service = client(async () => new Response("internal detail that must not surface", {
      status: 503,
      headers: { "content-type": "text/plain" },
    }));

    await expect(service.getRun("run_123")).rejects.toEqual(
      new ZapVideoServiceError("unexpected_status", true, 503),
    );
  });

  it("accepts an idempotent 200 replay in a later durable state", async () => {
    const service = client(async () => json({
      runId: "run_123",
      status: "video_ready",
      progress: 100,
      video: { url: "https://blob.vercel-storage.com/air/output.mp4", mimeType: "video/mp4" },
    }, 200));

    await expect(service.createRun({
      prompt: "A fox made of rain",
      idempotencyKey: "a".repeat(64),
    })).resolves.toMatchObject({
      runId: "run_123",
      status: "video_ready",
      video: { url: "https://blob.vercel-storage.com/air/output.mp4", mimeType: "video/mp4" },
    });
  });

  it("rejects upload tickets and completed videos outside the approved storage hosts", async () => {
    const uploadService = client(async () => json({
      uploadUrl: "https://attacker.example/put",
      publicUrl: "https://storage.googleapis.com/air/frame.jpg",
      headers: { "content-type": "image/jpeg" },
      method: "PUT",
      expiresAt: "2099-01-01T00:00:00.000Z",
    }, 201));
    await expect(uploadService.createUpload({
      mimeType: "image/jpeg",
      sizeBytes: 1,
      idempotencyKey: "b".repeat(64),
    })).rejects.toMatchObject({ code: "invalid_response" });

    const videoService = client(async () => json({
      runId: "run_123",
      status: "video_ready",
      video: { url: "https://attacker.example/output.mp4", mimeType: "video/mp4" },
    }));
    await expect(videoService.getRun("run_123")).rejects.toMatchObject({ code: "invalid_response" });
  });

  it("fails closed for insecure service origins and unsupported request media", async () => {
    expect(() => createZapVideoServiceClient({
      serviceUrl: "http://zap.example.test",
      serviceToken: "token",
    })).toThrow(/invalid_configuration/);

    const service = client(async () => json({}));
    await expect(service.createUpload({
      mimeType: "image/png",
      sizeBytes: 0,
      idempotencyKey: "b".repeat(64),
    })).rejects.toMatchObject({
      code: "invalid_request",
      retryable: false,
    });
    await expect(service.createRun({
      prompt: "A valid prompt",
      idempotencyKey: "a".repeat(257),
    })).rejects.toMatchObject({ code: "invalid_request" });
  });

  it("allows loopback IPv6 only for local fixtures and rejects newline-bearing service tokens", () => {
    expect(() => createZapVideoServiceClient({
      serviceUrl: "http://[::1]",
      serviceToken: "fixture-token",
    })).not.toThrow();
    expect(() => createZapVideoServiceClient({
      serviceUrl: "https://zap.example.test",
      serviceToken: "fixture-token\nattacker",
    })).toThrow(/invalid_configuration/);
  });
});
