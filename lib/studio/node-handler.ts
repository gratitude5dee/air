import type { IncomingHttpHeaders, IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";

/**
 * Thin Node-to-Web adapter for Vercel's Node.js functions. Business handlers
 * use Request/Response so their raw-body and authorization behavior is easy to
 * test without a framework, while Vercel retains its normal Node runtime.
 */
export type WebRoute = (request: Request) => Promise<Response> | Response;

export function asNodeHandler(route: WebRoute): (request: IncomingMessage, response: ServerResponse) => Promise<void> {
  return async (request, response) => {
    try {
      const webRequest = incomingRequestToWebRequest(request);
      const webResponse = await route(webRequest);
      await writeWebResponse(response, webResponse);
    } catch {
      response.statusCode = 500;
      response.setHeader("cache-control", "no-store");
      response.end();
    }
  };
}

export function incomingRequestToWebRequest(request: IncomingMessage): Request {
  const protocol = (request.headers["x-forwarded-proto"] ?? "https").toString().split(",")[0]?.trim() || "https";
  const host = request.headers.host ?? "air.wzrd.tech";
  const url = new URL(request.url ?? "/", `${protocol}://${host}`);
  const method = request.method ?? "GET";
  const headers = new Headers();
  copyHeaders(request.headers, headers);
  const hasBody = method !== "GET" && method !== "HEAD";
  const init: RequestInit & { duplex?: "half" } = {
    method,
    headers,
    ...(hasBody ? { body: Readable.toWeb(request) as unknown as ReadableStream<Uint8Array>, duplex: "half" } : {}),
  };
  return new Request(url, init);
}

async function writeWebResponse(response: ServerResponse, webResponse: Response): Promise<void> {
  response.statusCode = webResponse.status;
  for (const [name, value] of webResponse.headers) response.setHeader(name, value);
  const bytes = Buffer.from(await webResponse.arrayBuffer());
  response.end(bytes);
}

function copyHeaders(source: IncomingHttpHeaders, target: Headers): void {
  for (const [name, value] of Object.entries(source)) {
    if (value === undefined) continue;
    target.set(name, Array.isArray(value) ? value.join(",") : value);
  }
}
