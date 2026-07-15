import { defineSchedule } from "eve/schedules";
import { drainDurableInbox } from "../../lib/imessage-channel-runtime.js";
import imessageChannel, {
  assertIMessageRuntimeConfiguration,
  imessageAdapter,
} from "../channels/imessage.js";

/**
 * Recovery only: Photon ingress remains webhook-driven. This drain gives
 * encrypted inbox records another chance to enter the same ordered Eve/Chat
 * SDK hand-off after an invocation interruption.
 */
export default defineSchedule({
  cron: "* * * * *",
  async run({ receive }) {
    assertIMessageRuntimeConfiguration();

    await drainDurableInbox({
      adapter: imessageAdapter,
      receive: (input) => receive(imessageChannel, input),
    });
  },
});
