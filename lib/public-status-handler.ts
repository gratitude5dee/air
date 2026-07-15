import { publicStatusResponse } from "./imessage-channel-runtime.js";

/**
 * Nitro registers application handlers before Eve adds its framework home
 * page. Keep the minimal public status page here so GET / remains Air-owned
 * without patching Eve or adding a second conversational surface.
 */
export default function publicStatusHandler(): Response {
  return publicStatusResponse();
}
