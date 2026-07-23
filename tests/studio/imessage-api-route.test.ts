import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StudioRuntime } from "../../lib/studio/runtime.js";
import type { WebhookHandlerOptions } from "../../lib/studio/webhook-handler.js";

const mocks = vi.hoisted(() => ({
  drainStudio: vi.fn<(runtime: StudioRuntime) => Promise<void>>(),
  handleStudioIMessageWebhook: vi.fn<(
    request: Request,
    options: WebhookHandlerOptions,
  ) => Promise<Response>>(),
  waitUntil: vi.fn<(work: Promise<unknown>) => void>(),
}));

vi.mock("@vercel/functions", () => ({ waitUntil: mocks.waitUntil }));
vi.mock("../../lib/studio/engine.js", () => ({ drainStudio: mocks.drainStudio }));
vi.mock("../../lib/studio/webhook-handler.js", () => ({
  handleStudioIMessageWebhook: mocks.handleStudioIMessageWebhook,
}));

import handler from "../../api/webhooks/imessage.js";

function nodeRequest(): IncomingMessage {
  const stream = Readable.from([]);
  Object.assign(stream, {
    method: "POST",
    url: "/webhooks/imessage",
    headers: { host: "air.wzrd.tech" },
  });
  return stream as unknown as IncomingMessage;
}

function nodeResponse(): ServerResponse {
  return {
    statusCode: 200,
    setHeader: vi.fn(),
    end: vi.fn(),
  } as unknown as ServerResponse;
}

describe("Vercel iMessage webhook bridge", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("registers the post-receipt drain with Vercel's supported waitUntil hook", async () => {
    const response = nodeResponse();
    const drainWork = Promise.resolve();
    mocks.handleStudioIMessageWebhook.mockImplementation(async (_request, options) => {
      options.waitUntil?.(drainWork);
      return new Response(null, { status: 200 });
    });

    await handler(nodeRequest(), response);

    expect(mocks.handleStudioIMessageWebhook).toHaveBeenCalledTimes(1);
    expect(mocks.handleStudioIMessageWebhook).toHaveBeenCalledWith(
      expect.any(Request),
      expect.objectContaining({
        drain: mocks.drainStudio,
        waitUntil: mocks.waitUntil,
      }),
    );
    expect(mocks.waitUntil).toHaveBeenCalledWith(drainWork);
    expect(response.statusCode).toBe(200);
  });
});
