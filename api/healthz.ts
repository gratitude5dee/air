import { asNodeHandler } from "../lib/studio/node-handler.js";
import { studioHealthResponse } from "../lib/studio/runtime.js";

export default asNodeHandler((request) => {
  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response(null, {
      status: 405,
      headers: { allow: "GET, HEAD", "cache-control": "no-store" },
    });
  }
  return studioHealthResponse();
});
