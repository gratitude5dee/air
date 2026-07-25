import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  drainStudio: vi.fn(),
  handleStudioIMessageWebhook: vi.fn<(request: Request) => Promise<Response>>(),
  waitUntil: vi.fn(),
}));

vi.mock("@vercel/functions", () => ({ waitUntil: mocks.waitUntil }));
vi.mock("../../lib/studio/engine.js", () => ({ drainStudio: mocks.drainStudio }));
vi.mock("../../lib/studio/webhook-handler.js", () => ({
  handleStudioIMessageWebhook: mocks.handleStudioIMessageWebhook,
}));

import handler, { config } from "../../api/webhooks/imessage.js";

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

  it("calls the receipt-only handler without scheduling drain work", async () => {
    const response = nodeResponse();
    mocks.handleStudioIMessageWebhook.mockResolvedValue(new Response(null, { status: 200 }));

    await handler(nodeRequest(), response);

    expect(mocks.handleStudioIMessageWebhook).toHaveBeenCalledTimes(1);
    expect(mocks.handleStudioIMessageWebhook).toHaveBeenCalledWith(expect.any(Request));
    expect(mocks.drainStudio).not.toHaveBeenCalled();
    expect(mocks.waitUntil).not.toHaveBeenCalled();
    expect(response.statusCode).toBe(200);
  });

  it("disables Vercel's body parser so raw signed bytes reach the verifier", () => {
    expect(config).toEqual({ api: { bodyParser: false } });
  });
});
