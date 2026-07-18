import { asNodeHandler } from "../lib/studio/node-handler.js";
import { studioPublicStatusResponse } from "../lib/studio/runtime.js";

export default asNodeHandler((request) => {
  if (request.method !== "GET" && request.method !== "HEAD") return methodNotAllowed();
  return studioPublicStatusResponse();
});

function methodNotAllowed(): Response {
  return new Response(null, {
    status: 405,
    headers: { allow: "GET, HEAD", "cache-control": "no-store" },
  });
}
