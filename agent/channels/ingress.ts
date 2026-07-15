import { GET, POST, defineChannel } from "eve/channels";
import {
  assertIMessageRuntimeConfiguration,
  imessageAdapter,
} from "../../lib/imessage-adapter.js";
import {
  authorizeIMessageVideoDrain,
  drainIMessageVideoJobs,
  handleIMessageVideoWebhook,
} from "../../lib/imessage-video-runtime.js";
import { healthResponse } from "../../lib/imessage-channel-runtime.js";

/**
 * Air's sole public inbound dispatcher. No Chat SDK adapter webhook route is
 * mounted; Photon must point only at `/webhooks/imessage` so a signed delivery
 * is durable before acknowledgement.
 */
export default defineChannel({
  routes: [
    GET("/healthz", async () => healthResponse()),
    POST("/webhooks/imessage", async (request, args) => {
      assertIMessageRuntimeConfiguration();
      return handleIMessageVideoWebhook(request, {
        adapter: imessageAdapter,
        waitUntil: args.waitUntil,
      });
    }),
    POST("/internal/drain-inbox", async (request, args) => {
      assertIMessageRuntimeConfiguration();
      const authorized = await authorizeIMessageVideoDrain(request);
      if (authorized instanceof Response) return authorized;
      args.waitUntil(drainIMessageVideoJobs({
        adapter: imessageAdapter,
        runtime: authorized.runtime,
      }));
      return new Response(null, { status: 202 });
    }),
  ],
});
