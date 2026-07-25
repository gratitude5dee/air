import type { IncomingHttpHeaders, IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import getRawBody from "raw-body";

/**
 * Thin Node-to-Web adapter for Vercel's Node.js functions. Business handlers
 * use Request/Response so their raw-body and authorization behavior is easy to
 * test without a framework, while Vercel retains its normal Node runtime.
 */
export type WebRoute = (request: Request) => Promise<Response> | Response;

export interface RawBodyNodeHandlerOptions {
  /** Exact byte ceiling enforced before unverified payload reaches a route. */
  readonly maxBodyBytes: number;
}

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

/**
 * Vercel's Node runtime expects signed webhook handlers to consume the native
 * request with a raw-body reader. Converting its live request stream through
 * `Readable.toWeb()` can leave a POST pending until the function deadline,
 * even when the route would reject the signature. Buffer only the bounded
 * byte sequence, then give the Fetch-style route a fresh in-memory stream.
 */
export function asRawBodyNodeHandler(
  route: WebRoute,
  options: RawBodyNodeHandlerOptions,
): (request: IncomingMessage, response: ServerResponse) => Promise<void> {
  return async (request, response) => {
    try {
      const rawBody = hasRequestBody(request.method)
        ? await getRawBody(request, { limit: options.maxBodyBytes })
        : undefined;
      const webRequest = incomingRequestToWebRequest(request, rawBody);
      const webResponse = await route(webRequest);
      await writeWebResponse(response, webResponse);
    } catch (error) {
      response.statusCode = rawBodyStatus(error);
      response.setHeader("cache-control", "no-store");
      response.end();
    }
  };
}

export function incomingRequestToWebRequest(request: IncomingMessage, rawBody?: Uint8Array): Request {
  const protocol = (request.headers["x-forwarded-proto"] ?? "https").toString().split(",")[0]?.trim() || "https";
  const host = request.headers.host ?? "air.wzrd.tech";
  const url = new URL(request.url ?? "/", `${protocol}://${host}`);
  const method = request.method ?? "GET";
  const headers = new Headers();
  copyHeaders(request.headers, headers);
  const hasBody = hasRequestBody(method);
  const init: RequestInit & { duplex?: "half" } = hasBody
    ? {
      method,
      headers,
      body: rawBody === undefined
        ? Readable.toWeb(request) as unknown as BodyInit
        : rawBody as unknown as BodyInit,
      duplex: "half",
    }
    : { method, headers };
  return new Request(url, init);
}

function hasRequestBody(method: string | undefined): boolean {
  return method !== "GET" && method !== "HEAD";
}

function rawBodyStatus(error: unknown): 400 | 413 | 500 {
  if (
    typeof error === "object"
    && error !== null
    && "statusCode" in error
    && typeof error.statusCode === "number"
  ) {
    if (error.statusCode === 413) return 413;
    return 400;
  }
  return 500;
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
