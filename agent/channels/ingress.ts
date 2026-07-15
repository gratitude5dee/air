import { GET, POST, defineChannel } from "eve/channels";
import imessageChannel, {
  assertIMessageRuntimeConfiguration,
  imessageAdapter,
} from "./imessage.js";
import {
  drainFromOperatorRoute,
  handleIMessageWebhook,
  healthResponse,
} from "../../lib/imessage-channel-runtime.js";

/**
 * Air's sole public inbound dispatcher. The Chat SDK adapter route is mounted
 * elsewhere but intentionally has no bot handlers; Photon must point only at
 * `/webhooks/imessage` so a signed delivery is durable before acknowledgement.
 */
export default defineChannel({
  routes: [
    GET("/healthz", async () => healthResponse()),
    POST("/webhooks/imessage", async (request, args) => {
      assertIMessageRuntimeConfiguration();
      return handleIMessageWebhook(request, {
        adapter: imessageAdapter,
        receive: (input) => args.receive(imessageChannel, input),
        waitUntil: args.waitUntil,
      });
    }),
    POST("/internal/drain-inbox", async (request, args) => {
      assertIMessageRuntimeConfiguration();
      return drainFromOperatorRoute(request, {
        adapter: imessageAdapter,
        receive: (input) => args.receive(imessageChannel, input),
      });
    }),
  ],
});
