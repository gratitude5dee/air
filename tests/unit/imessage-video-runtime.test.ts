import { createHmac } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";

const spectrumMock = vi.hoisted(() => ({
  getAttachment: vi.fn(),
}));

const heicMock = vi.hoisted(() => vi.fn());

vi.mock("@spectrum-ts/imessage", () => ({
  imessage: () => ({ getAttachment: spectrumMock.getAttachment }),
}));

vi.mock("heic-convert", () => ({ default: heicMock }));

import {
  __setAirRuntimeForTests,
  createAirRuntime,
} from "../../lib/imessage-channel-runtime.js";
import { loadAirEnvironment } from "../../lib/env.js";
import {
  authorizeIMessageVideoDrain,
  drainIMessageVideoJobs,
  handleIMessageVideoWebhook,
  MAX_INPUT_IMAGE_BYTES,
  type AirVideoIMessageAdapter,
} from "../../lib/imessage-video-runtime.js";
import {
  AIR_VIDEO_ACTIVE_NOTICE,
  AIR_VIDEO_GENERATING_NOTICE,
  AIR_VIDEO_IMAGE_NOTICE,
} from "../../lib/messages.js";
import { MemoryRedis } from "../../lib/redis.js";
import { deriveCorrelationId, hashSenderIdentifier } from "../../lib/security.js";
import { inboxRecordKey } from "../../lib/durable-inbox.js";
import { VideoJobStore, videoJobRecordKey } from "../../lib/video-job-store.js";

const webhookSecret = "video-runtime-webhook-secret";
const identitySecret = "video-runtime-identity-secret";
const queueKey = Buffer.alloc(32, 19).toString("base64");
const projectId = "video-runtime-project";
const sender = "+14155551234";

function createRuntime() {
  return createAirRuntime({
    redis: new MemoryRedis(),
    environment: loadAirEnvironment({
      NODE_ENV: "development",
      IMESSAGE_PROJECT_ID: projectId,
      IMESSAGE_PROJECT_SECRET: "project-secret",
      IMESSAGE_WEBHOOK_SECRET: webhookSecret,
      REDIS_URL: "redis://fixture.invalid:6379",
      ZAP_AIR_SERVICE_URL: "https://zap.example.test",
      ZAP_AIR_SERVICE_TOKEN: "zap-service-token",
      AIR_ID_HASH_SECRET: identitySecret,
      AIR_QUEUE_ENCRYPTION_KEY: queueKey,
      AIR_ALLOWED_SENDER_HASHES: hashSenderIdentifier(identitySecret, sender),
      AIR_PUBLIC_BASE_URL: "https://air.example.test",
      CRON_SECRET: "cron-secret",
    }),
  });
}

function signedRequest(payload: Record<string, unknown>, options: { readonly webhookId?: string } = {}): Request {
  const timestamp = String(Math.floor(Date.now() / 1_000));
  const raw = JSON.stringify(payload);
  const signature = `v0=${createHmac("sha256", webhookSecret)
    .update(`v0:${timestamp}:${raw}`)
    .digest("hex")}`;
  return new Request("https://air.example.test/webhooks/imessage", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-spectrum-signature": signature,
      "x-spectrum-timestamp": timestamp,
      "x-spectrum-webhook-id": options.webhookId ?? "video-runtime-delivery",
    },
    body: raw,
  });
}

function attachmentPayload(options: { readonly chatGuid?: string; readonly spaceType?: "dm" | "group" | null } = {}) {
  const space = {
    id: options.chatGuid ?? "iMessage;-;+14155550000",
    phone: "+14155550001",
    ...(options.spaceType === null
      ? {}
      : { type: (options.spaceType ?? "dm") as "dm" | "group" }),
  };
  return {
    event: "messages",
    space,
    message: {
      id: "video-runtime-message",
      direction: "inbound",
      sender: { id: sender },
      space,
      content: {
        type: "group",
        items: [
          { content: { type: "text", text: "A paper kite over a moonlit ocean" } },
          {
            content: {
              type: "attachment",
              id: "attachment-guid-runtime",
              name: "frame.jpg",
              mimeType: "image/jpeg",
              size: null,
            },
          },
        ],
      },
    },
  };
}

function textPayload(options: { readonly messageId: string; readonly chatGuid?: string }) {
  const space = {
    id: options.chatGuid ?? "iMessage;-;+14155550000",
    phone: "+14155550001",
    type: "dm" as const,
  };
  return {
    event: "messages",
    space,
    message: {
      id: options.messageId,
      direction: "inbound",
      sender: { id: sender },
      space,
      content: { type: "text", text: "A paper kite over a moonlit ocean" },
    },
  };
}

function reactionPayload() {
  const space = {
    id: "iMessage;-;+14155550000",
    phone: "+14155550001",
    type: "dm" as const,
  };
  return {
    event: "messages",
    space,
    message: {
      id: "video-runtime-reaction",
      direction: "inbound",
      sender: { id: sender },
      space,
      content: { type: "reaction", emoji: "❤️" },
    },
  };
}

class FixtureAdapter implements AirVideoIMessageAdapter {
  public app: unknown | null = {};
  public readonly posted: unknown[] = [];

  encodeThreadId({ chatGuid, phone }: { readonly chatGuid: string; readonly phone?: string }): string {
    return phone ? `imessage:${chatGuid}~${phone}` : `imessage:${chatGuid}`;
  }

  async fetchMessage(): Promise<unknown> {
    return undefined;
  }

  async postMessage(
    _threadId: string,
    message: Parameters<AirVideoIMessageAdapter["postMessage"]>[1],
  ): Promise<unknown> {
    this.posted.push(message);
    return { id: "outbound-fixture" };
  }
}

function webhookOptions(
  adapter: AirVideoIMessageAdapter,
  background: Promise<unknown>[],
  assertRuntimeConfiguration: () => void = () => undefined,
) {
  return {
    adapter,
    waitUntil: (task: Promise<unknown>) => {
      background.push(task);
    },
    assertRuntimeConfiguration,
  };
}

afterEach(() => {
  __setAirRuntimeForTests(undefined);
  spectrumMock.getAttachment.mockReset();
  heicMock.mockReset();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe("iMessage video runtime safeguards", () => {
  it("verifies an unsigned request before evaluating deployment configuration", async () => {
    const assertRuntimeConfiguration = vi.fn(() => {
      throw new Error("missing deployment configuration");
    });
    const background: Promise<unknown>[] = [];
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const response = await handleIMessageVideoWebhook(
      new Request("https://air.example.test/webhooks/imessage", { method: "POST", body: "{}" }),
      webhookOptions(new FixtureAdapter(), background, assertRuntimeConfiguration),
    );

    expect(response.status).toBe(400);
    expect(assertRuntimeConfiguration).not.toHaveBeenCalled();
    expect(background).toEqual([]);
    expect(warning).toHaveBeenCalledTimes(1);
    expect(warning).toHaveBeenCalledWith("air_imessage_ingress_signature_headers_missing");
  });

  it("returns a retryable 503 rather than throwing when signed ingress configuration is incomplete", async () => {
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    const assertRuntimeConfiguration = vi.fn(() => {
      throw new Error("missing deployment configuration");
    });
    const background: Promise<unknown>[] = [];

    const response = await handleIMessageVideoWebhook(
      signedRequest(textPayload({ messageId: "configuration-guard" })),
      webhookOptions(new FixtureAdapter(), background, assertRuntimeConfiguration),
    );

    expect(response.status).toBe(503);
    expect(assertRuntimeConfiguration).toHaveBeenCalledTimes(1);
    expect(background).toEqual([]);
  });

  it("rejects an unauthenticated manual drain before runtime configuration is evaluated", async () => {
    __setAirRuntimeForTests(undefined);
    vi.stubEnv("VERCEL_ENV", "production");
    // If authorization tried to load the runtime first, this incomplete
    // production configuration would produce a 503 instead of a 401.
    vi.stubEnv("IMESSAGE_PROJECT_ID", "");
    const response = await authorizeIMessageVideoDrain(
      new Request("https://air.example.test/internal/drain-inbox", { method: "POST" }),
    );

    expect(response).toBeInstanceOf(Response);
    expect((response as Response).status).toBe(401);
  });

  it("cancels an oversized attachment stream before it can become an in-memory Buffer", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);

    const cancelled = vi.fn();
    let emitted = false;
    spectrumMock.getAttachment.mockResolvedValue({
      size: undefined,
      stream: async () => new ReadableStream<Uint8Array>({
        pull(controller) {
          if (!emitted) {
            emitted = true;
            controller.enqueue(new Uint8Array(MAX_INPUT_IMAGE_BYTES + 1));
          }
        },
        cancel: cancelled,
      }),
    });
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];
    const reserve = vi.spyOn(runtime.limiter, "reserveAtReceipt");

    const response = await handleIMessageVideoWebhook(
      signedRequest(attachmentPayload()),
      webhookOptions(adapter, background),
    );
    await Promise.all(background);

    expect(response.status).toBe(200);
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(reserve).not.toHaveBeenCalled();
    expect(adapter.posted).toEqual([AIR_VIDEO_IMAGE_NOTICE]);
    expect(await runtime.inbox.listDue()).toEqual([]);
  });

  it("drops reaction events before queueing or replying", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];

    const response = await handleIMessageVideoWebhook(
      signedRequest(reactionPayload()),
      webhookOptions(adapter, background),
    );

    expect(response.status).toBe(204);
    expect(background).toEqual([]);
    expect(adapter.posted).toEqual([]);
    expect(await runtime.inbox.listDue()).toEqual([]);
  });

  it("does not persist an unclassified chat space that could bypass the DM gate", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];

    const response = await handleIMessageVideoWebhook(
      signedRequest(attachmentPayload({ chatGuid: "unclassified-chat-guid", spaceType: null })),
      webhookOptions(adapter, background),
    );

    expect(response.status).toBe(204);
    expect(background).toEqual([]);
    expect(await runtime.inbox.listDue()).toEqual([]);
  });

  it("moves a valid image prompt through direct upload and one durable Zap submission", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);

    spectrumMock.getAttachment.mockResolvedValue({
      size: 3,
      stream: async () => new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([0xff, 0xd8, 0xff]));
          controller.close();
        },
      }),
    });
    const requests: string[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = input instanceof Request ? input.url : String(input);
      requests.push(url);
      if (url.endsWith("/api/services/air/v1/uploads")) {
        return new Response(JSON.stringify({
          uploadUrl: "https://storage.googleapis.com/gmi/first-frame.jpg?signature=fixture",
          publicUrl: "https://storage.googleapis.com/gmi/first-frame.jpg",
          headers: { "content-type": "image/jpeg" },
          method: "PUT",
          expiresAt: "2099-01-01T00:00:00.000Z",
        }), { status: 201, headers: { "content-type": "application/json" } });
      }
      if (url.startsWith("https://storage.googleapis.com/")) return new Response(null, { status: 200 });
      if (url.endsWith("/api/services/air/v1/runs")) {
        return new Response(JSON.stringify({ runId: "run_fixture", status: "queued" }), {
          status: 202,
          headers: { "content-type": "application/json" },
        });
      }
      throw new Error("unexpected network destination");
    });
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];

    const response = await handleIMessageVideoWebhook(
      signedRequest(attachmentPayload()),
      webhookOptions(adapter, background),
    );
    await Promise.all(background);

    const correlation = deriveCorrelationId(identitySecret, projectId, "video-runtime-message");
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const record = await jobs.get(videoJobRecordKey(correlation));
    const inbox = await runtime.inbox.getRecord(inboxRecordKey(correlation));

    expect(response.status).toBe(200);
    expect(record?.state).toBe("queued");
    expect(inbox).toMatchObject({ state: "handed_off", encryptedEnvelope: undefined });
    const inboxMetadata = await runtime.redis.get(`${inboxRecordKey(correlation)}:metadata`);
    expect(inboxMetadata).not.toContain(projectId);
    expect(inboxMetadata).not.toContain("video-runtime-message");
    expect(inboxMetadata).not.toContain("paper kite");
    expect(requests).toEqual([
      "https://zap.example.test/api/services/air/v1/uploads",
      "https://storage.googleapis.com/gmi/first-frame.jpg?signature=fixture",
      "https://zap.example.test/api/services/air/v1/runs",
    ]);
    expect(adapter.posted).toEqual(["Generating your 5-second video now."]);
  });

  it("converts a validated HEIC first frame to JPEG before the direct upload", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    const heic = Buffer.from([0, 0, 0, 0, 0x66, 0x74, 0x79, 0x70, 0x68, 0x65, 0x69, 0x63]);
    const jpeg = Buffer.from([0xff, 0xd8, 0xff, 0]);
    heicMock.mockResolvedValue(jpeg);
    spectrumMock.getAttachment.mockResolvedValue({
      size: heic.byteLength,
      stream: async () => new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(heic);
          controller.close();
        },
      }),
    });
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = input instanceof Request ? input.url : String(input);
      if (url.endsWith("/api/services/air/v1/uploads")) {
        return new Response(JSON.stringify({
          uploadUrl: "https://storage.googleapis.com/gmi/converted-frame.jpg?signature=fixture",
          publicUrl: "https://storage.googleapis.com/gmi/converted-frame.jpg",
          headers: { "content-type": "image/jpeg" },
          method: "PUT",
          expiresAt: "2099-01-01T00:00:00.000Z",
        }), { status: 201, headers: { "content-type": "application/json" } });
      }
      if (url.startsWith("https://storage.googleapis.com/")) return new Response(null, { status: 200 });
      if (url.endsWith("/api/services/air/v1/runs")) {
        return new Response(JSON.stringify({ runId: "run_heic_fixture", status: "queued" }), {
          status: 202,
          headers: { "content-type": "application/json" },
        });
      }
      throw new Error("unexpected network destination");
    });
    const payload = attachmentPayload();
    const content = (payload.message as { content: { items: Array<{ content: Record<string, unknown> }> } }).content;
    const attachment = content.items[1]?.content;
    if (!attachment) throw new Error("expected attachment fixture");
    attachment.name = "frame.heic";
    attachment.mimeType = "image/heic";
    attachment.size = heic.byteLength;
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];

    const response = await handleIMessageVideoWebhook(
      signedRequest(payload),
      webhookOptions(adapter, background),
    );
    await Promise.all(background);

    expect(response.status).toBe(200);
    expect(heicMock).toHaveBeenCalledWith(expect.objectContaining({ buffer: heic, format: "JPEG" }));
    expect(adapter.posted).toEqual([AIR_VIDEO_GENERATING_NOTICE]);
  });

  it("coalesces concurrent duplicate signed deliveries into one Zap submission", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    let runPosts = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = input instanceof Request ? input.url : String(input);
      if (!url.endsWith("/api/services/air/v1/runs")) {
        throw new Error("unexpected network destination");
      }
      runPosts += 1;
      return new Response(JSON.stringify({ runId: "run_concurrent_duplicate", status: "queued" }), {
        status: 202,
        headers: { "content-type": "application/json" },
      });
    });
    const adapter = new FixtureAdapter();
    const firstBackground: Promise<unknown>[] = [];
    const secondBackground: Promise<unknown>[] = [];
    const payload = textPayload({ messageId: "concurrent-duplicate" });

    const [first, second] = await Promise.all([
      handleIMessageVideoWebhook(
        signedRequest(payload, { webhookId: "delivery-concurrent-a" }),
        webhookOptions(adapter, firstBackground),
      ),
      handleIMessageVideoWebhook(
        signedRequest(payload, { webhookId: "delivery-concurrent-b" }),
        webhookOptions(adapter, secondBackground),
      ),
    ]);
    await Promise.all([...firstBackground, ...secondBackground]);

    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    expect(runPosts).toBe(1);
    expect(adapter.posted).toEqual([AIR_VIDEO_GENERATING_NOTICE]);
  });

  it("waits for durable Zap acceptance before sending the generating acknowledgement", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);

    let resolveRun: ((response: Response) => void) | undefined;
    let markRunStarted: (() => void) | undefined;
    const runStarted = new Promise<void>((resolve) => {
      markRunStarted = resolve;
    });
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = input instanceof Request ? input.url : String(input);
      if (!url.endsWith("/api/services/air/v1/runs")) {
        throw new Error("unexpected network destination");
      }
      markRunStarted?.();
      return new Promise<Response>((resolve) => {
        resolveRun = resolve;
      });
    });
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];

    const response = await handleIMessageVideoWebhook(
      signedRequest(textPayload({ messageId: "ack-after-acceptance" })),
      webhookOptions(adapter, background),
    );
    await runStarted;

    const correlation = deriveCorrelationId(identitySecret, projectId, "ack-after-acceptance");
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    expect(response.status).toBe(200);
    expect(adapter.posted).toEqual([]);
    const awaitingAcceptance = await jobs.get(videoJobRecordKey(correlation));
    expect(awaitingAcceptance?.state).toBe("submission_unknown");
    expect(awaitingAcceptance?.status.acknowledgementAttemptedAtMs).toBeUndefined();

    resolveRun?.(new Response(JSON.stringify({ runId: "run_ack_fixture", status: "queued" }), {
      status: 202,
      headers: { "content-type": "application/json" },
    }));
    await Promise.all(background);

    expect(adapter.posted).toEqual([AIR_VIDEO_GENERATING_NOTICE]);
    expect(await jobs.get(videoJobRecordKey(correlation))).toMatchObject({
      state: "queued",
      status: { acknowledgementAttemptedAtMs: expect.any(Number) },
    });
  });

  it("acknowledges an immediately completed accepted Zap run without regressing it", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      runId: "run_completed_fixture",
      status: "video_ready",
      video: {
        url: "https://blob.vercel-storage.com/air/fixture.mp4",
        mimeType: "video/mp4",
      },
    }), {
      // A completed status is an idempotent replay, which Zap returns as 200;
      // a first-time 202 acceptance is necessarily queued.
      status: 200,
      headers: { "content-type": "application/json" },
    }));
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];

    await handleIMessageVideoWebhook(
      signedRequest(textPayload({ messageId: "ack-immediate-completion" })),
      webhookOptions(adapter, background),
    );
    await Promise.all(background);

    const correlation = deriveCorrelationId(identitySecret, projectId, "ack-immediate-completion");
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const completed = await jobs.get(videoJobRecordKey(correlation));
    expect(completed).toMatchObject({
      state: "video_ready",
      status: { acknowledgementAttemptedAtMs: expect.any(Number) },
    });
    expect(adapter.posted).toEqual([AIR_VIDEO_GENERATING_NOTICE]);
  });

  it("does not reserve quota for a receipt that loses the active-job fence", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    const reserve = vi.spyOn(runtime.limiter, "reserveAtReceipt");
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = input instanceof Request ? input.url : String(input);
      if (url.endsWith("/api/services/air/v1/runs")) {
        return new Response(JSON.stringify({ runId: "run_fixture", status: "queued" }), {
          status: 202,
          headers: { "content-type": "application/json" },
        });
      }
      throw new Error("unexpected network destination");
    });
    const adapter = new FixtureAdapter();
    const firstBackground: Promise<unknown>[] = [];
    const secondBackground: Promise<unknown>[] = [];

    const first = await handleIMessageVideoWebhook(signedRequest(textPayload({
      messageId: "video-runtime-first",
    })), webhookOptions(adapter, firstBackground));
    await Promise.all(firstBackground);

    const second = await handleIMessageVideoWebhook(signedRequest(textPayload({
      messageId: "video-runtime-second",
    })), webhookOptions(adapter, secondBackground));
    await Promise.all(secondBackground);

    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const firstCorrelation = deriveCorrelationId(identitySecret, projectId, "video-runtime-first");
    const secondCorrelation = deriveCorrelationId(identitySecret, projectId, "video-runtime-second");

    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    expect(reserve).toHaveBeenCalledTimes(1);
    expect(await jobs.get(videoJobRecordKey(firstCorrelation))).not.toBeNull();
    expect(await jobs.get(videoJobRecordKey(secondCorrelation))).toBeNull();
    expect(adapter.posted).toEqual([AIR_VIDEO_GENERATING_NOTICE, AIR_VIDEO_ACTIVE_NOTICE]);
  });

  it("terminalizes a phone-less receipt before any paid job or user-visible send", async () => {
    const runtime = createRuntime();
    __setAirRuntimeForTests(runtime);
    vi.stubEnv("IMESSAGE_PROJECT_ID", projectId);
    vi.stubEnv("IMESSAGE_WEBHOOK_SECRET", webhookSecret);
    const payload = textPayload({ messageId: "video-runtime-no-line" });
    delete (payload.space as { phone?: string }).phone;
    const adapter = new FixtureAdapter();
    const background: Promise<unknown>[] = [];
    const fetch = vi.spyOn(globalThis, "fetch");

    const response = await handleIMessageVideoWebhook(
      signedRequest(payload),
      webhookOptions(adapter, background),
    );
    await Promise.all(background);

    const correlation = deriveCorrelationId(identitySecret, projectId, "video-runtime-no-line");
    expect(response.status).toBe(200);
    expect(fetch).not.toHaveBeenCalled();
    expect(adapter.posted).toEqual([]);
    expect(await runtime.inbox.getRecord(inboxRecordKey(correlation))).toMatchObject({
      state: "dead_letter",
      status: { failureCategory: "message_line_missing" },
    });
  });

  it("keeps an accepted-but-malformed Zap submission in submission_unknown", async () => {
    const runtime = createRuntime();
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const correlation = deriveCorrelationId(identitySecret, projectId, "malformed-submit");
    const created = await jobs.createOrGet({
      correlationId: correlation,
      senderHash: hashSenderIdentifier(identitySecret, sender),
      prompt: "A heron flying above a misty lake",
      delivery: { chatGuid: "iMessage;-;+14155550000", phone: "+14155550001" },
    });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ runId: "run_fixture" }), {
      status: 202,
      headers: { "content-type": "application/json" },
    }));

    await drainIMessageVideoJobs({ runtime, adapter: new FixtureAdapter() });

    expect(await jobs.get(created.record.immutable.recordKey)).toMatchObject({
      state: "submission_unknown",
      status: { failureCategory: "zap_submission_unknown" },
    });
  });

  it("keeps webhook drains best-effort while recovery can observe queue-read faults", async () => {
    const runtime = createRuntime();
    vi.spyOn(runtime.inbox, "listDue").mockRejectedValue(new Error("fixture queue fault"));
    const adapter = new FixtureAdapter();
    let storageFaults = 0;

    await expect(drainIMessageVideoJobs({ runtime, adapter })).resolves.toBeUndefined();
    await expect(drainIMessageVideoJobs({
      runtime,
      adapter,
      onStorageUnavailable: () => { storageFaults += 1; },
    })).resolves.toBeUndefined();
    expect(storageFaults).toBe(1);

    const healthyRuntime = createRuntime();
    let healthyStorageFaults = 0;
    await drainIMessageVideoJobs({
      runtime: healthyRuntime,
      adapter,
      onStorageUnavailable: () => { healthyStorageFaults += 1; },
    });
    expect(healthyStorageFaults).toBe(0);
  });

  it("retries a malformed Zap poll response for an existing durable run", async () => {
    const runtime = createRuntime();
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const created = await jobs.createOrGet({
      correlationId: deriveCorrelationId(identitySecret, projectId, "malformed-poll"),
      senderHash: hashSenderIdentifier(identitySecret, sender),
      prompt: "A lighthouse in a storm",
      delivery: { chatGuid: "iMessage;-;+14155550000", phone: "+14155550001" },
    });
    const submission = await jobs.beginWork(created.record.immutable.recordKey);
    if (!submission) throw new Error("expected submission intent");
    await submission.transition({
      expectedStates: ["submission_unknown"],
      state: "queued",
      zapRunId: "run_fixture",
    });
    await submission.release();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ runId: "run_fixture" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    }));

    await drainIMessageVideoJobs({ runtime, adapter: new FixtureAdapter() });

    expect(await jobs.get(created.record.immutable.recordKey)).toMatchObject({
      state: "retryable",
      status: { failureCategory: "zap_poll_retryable" },
    });
  });

  it("delivers a validated MP4 as an attachment-only iMessage", async () => {
    const runtime = createRuntime();
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const created = await jobs.createOrGet({
      correlationId: deriveCorrelationId(identitySecret, projectId, "attachment-delivery"),
      senderHash: hashSenderIdentifier(identitySecret, sender),
      prompt: "A whale made of clouds",
      delivery: { chatGuid: "iMessage;-;+14155550000", phone: "+14155550001" },
    });
    const submission = await jobs.beginWork(created.record.immutable.recordKey);
    if (!submission) throw new Error("expected submission intent");
    await submission.transition({
      expectedStates: ["submission_unknown"],
      state: "video_ready",
      zapRunId: "run_attachment_delivery",
      video: { url: "https://blob.vercel-storage.com/air/attachment.mp4", mimeType: "video/mp4" },
    });
    await submission.release();

    const mp4 = Buffer.from([0, 0, 0, 8, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6f, 0x6d]);
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(mp4, {
      status: 200,
      headers: { "content-type": "video/mp4", "content-length": String(mp4.byteLength) },
    }));
    const adapter = new FixtureAdapter();

    await drainIMessageVideoJobs({ runtime, adapter });

    expect(adapter.posted).toHaveLength(1);
    expect(adapter.posted[0]).toMatchObject({
      raw: "",
      files: [{ filename: "air-video.mp4", mimeType: "video/mp4" }],
    });
    const sent = adapter.posted[0] as { files: Array<{ data: Buffer }> };
    expect(sent.files[0]?.data.equals(mp4)).toBe(true);
    expect(await jobs.get(created.record.immutable.recordKey)).toMatchObject({ state: "delivered" });
    expect(await jobs.getPayload(created.record.immutable.recordKey)).toBeNull();
  });

  it("uses the approved Blob URL only after deterministic output validation fails", async () => {
    const runtime = createRuntime();
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const videoUrl = "https://blob.vercel-storage.com/air/oversized.mp4";
    const created = await jobs.createOrGet({
      correlationId: deriveCorrelationId(identitySecret, projectId, "approved-fallback"),
      senderHash: hashSenderIdentifier(identitySecret, sender),
      prompt: "A sunset over an alien sea",
      delivery: { chatGuid: "iMessage;-;+14155550000", phone: "+14155550001" },
    });
    const submission = await jobs.beginWork(created.record.immutable.recordKey);
    if (!submission) throw new Error("expected submission intent");
    await submission.transition({
      expectedStates: ["submission_unknown"],
      state: "video_ready",
      zapRunId: "run_approved_fallback",
      video: { url: videoUrl, mimeType: "video/mp4" },
    });
    await submission.release();

    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, {
      status: 200,
      headers: { "content-type": "video/mp4", "content-length": String(25 * 1024 * 1024 + 1) },
    }));
    const adapter = new FixtureAdapter();

    await drainIMessageVideoJobs({ runtime, adapter });

    expect(adapter.posted).toEqual([
      `Your video is ready, but it could not be attached. Temporary link: ${videoUrl}`,
    ]);
    expect(await jobs.get(created.record.immutable.recordKey)).toMatchObject({ state: "delivered" });
  });

  it("does not send a fallback link for an unapproved output URL", async () => {
    const runtime = createRuntime();
    const jobs = new VideoJobStore({
      redis: runtime.redis,
      cipher: runtime.cipher,
      identityHashSecret: identitySecret,
    });
    const created = await jobs.createOrGet({
      correlationId: deriveCorrelationId(identitySecret, projectId, "unapproved-output"),
      senderHash: hashSenderIdentifier(identitySecret, sender),
      prompt: "A constellation becoming a whale",
      delivery: { chatGuid: "iMessage;-;+14155550000", phone: "+14155550001" },
    });
    const submission = await jobs.beginWork(created.record.immutable.recordKey);
    if (!submission) throw new Error("expected submission intent");
    await submission.transition({
      expectedStates: ["submission_unknown"],
      state: "video_ready",
      zapRunId: "run_fixture",
      video: { url: "https://attacker.example/video.mp4", mimeType: "video/mp4" },
    });
    await submission.release();
    const adapter = new FixtureAdapter();
    const fetch = vi.spyOn(globalThis, "fetch");

    await drainIMessageVideoJobs({ runtime, adapter });

    expect(fetch).not.toHaveBeenCalled();
    expect(adapter.posted).toEqual([]);
    expect(await jobs.get(created.record.immutable.recordKey)).toMatchObject({
      state: "dead_letter",
      status: { failureCategory: "output_rejected" },
    });
  });
});
