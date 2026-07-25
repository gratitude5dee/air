import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import {
  asNodeHandler,
  asRawBodyNodeHandler,
  incomingRequestToWebRequest,
} from "../../lib/studio/node-handler.js";

function nodeRequest(input: {
  readonly method: string;
  readonly url: string;
  readonly headers?: Record<string, string | string[] | undefined>;
  readonly body?: string;
}): IncomingMessage {
  const stream = Readable.from(input.body === undefined ? [] : [Buffer.from(input.body, "utf8")]);
  Object.assign(stream, {
    method: input.method,
    url: input.url,
    headers: input.headers ?? {},
  });
  return stream as unknown as IncomingMessage;
}

describe("Vercel Node request adapter", () => {
  it("preserves the proxied URL, headers, and raw POST bytes for the signed route", async () => {
    const request = incomingRequestToWebRequest(nodeRequest({
      method: "POST",
      url: "/webhooks/imessage?source=photon",
      headers: {
        host: "preview.air.wzrd.tech",
        "x-forwarded-proto": "https, http",
        "x-spectrum-signature": "v0=abc",
      },
      body: "{\"raw\":true}",
    }));

    expect(request.url).toBe("https://preview.air.wzrd.tech/webhooks/imessage?source=photon");
    expect(request.headers.get("x-spectrum-signature")).toBe("v0=abc");
    expect(await request.text()).toBe("{\"raw\":true}");
  });

  it("writes route response status, headers, and bytes back to Vercel's Node response", async () => {
    const setHeader = vi.fn();
    const end = vi.fn();
    const response = {
      statusCode: 200,
      setHeader,
      end,
    } as unknown as ServerResponse;
    const route = vi.fn(async (request: Request) => {
      expect(await request.text()).toBe("signed body");
      return new Response("acknowledged", {
        status: 202,
        headers: { "cache-control": "no-store", "x-air-route": "imessage" },
      });
    });

    await asNodeHandler(route)(nodeRequest({
      method: "POST",
      url: "/webhooks/imessage",
      headers: { host: "air.wzrd.tech" },
      body: "signed body",
    }), response);

    expect(response.statusCode).toBe(202);
    expect(setHeader).toHaveBeenCalledWith("cache-control", "no-store");
    expect(setHeader).toHaveBeenCalledWith("x-air-route", "imessage");
    expect(end).toHaveBeenCalledWith(Buffer.from("acknowledged"));
  });

  it("buffers a bounded native body before passing a signed route fresh raw bytes", async () => {
    const setHeader = vi.fn();
    const end = vi.fn();
    const response = {
      statusCode: 200,
      setHeader,
      end,
    } as unknown as ServerResponse;
    const route = vi.fn(async (request: Request) => {
      expect(await request.text()).toBe("signed raw bytes");
      return new Response(null, { status: 204 });
    });

    await asRawBodyNodeHandler(route, { maxBodyBytes: 64 })(nodeRequest({
      method: "POST",
      url: "/webhooks/imessage",
      headers: { host: "air.wzrd.tech" },
      body: "signed raw bytes",
    }), response);

    expect(route).toHaveBeenCalledTimes(1);
    expect(response.statusCode).toBe(204);
  });

  it("rejects an oversized raw body before invoking its route", async () => {
    const setHeader = vi.fn();
    const end = vi.fn();
    const response = {
      statusCode: 200,
      setHeader,
      end,
    } as unknown as ServerResponse;
    const route = vi.fn(async () => new Response(null, { status: 200 }));

    await asRawBodyNodeHandler(route, { maxBodyBytes: 4 })(nodeRequest({
      method: "POST",
      url: "/webhooks/imessage",
      headers: { host: "air.wzrd.tech" },
      body: "oversized",
    }), response);

    expect(route).not.toHaveBeenCalled();
    expect(response.statusCode).toBe(413);
    expect(setHeader).toHaveBeenCalledWith("cache-control", "no-store");
  });
});
