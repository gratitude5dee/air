import { defineSchedule } from "eve/schedules";
import { drainIMessageVideoJobs } from "../../lib/imessage-video-runtime.js";
import {
  assertIMessageRuntimeConfiguration,
  imessageAdapter,
} from "../../lib/imessage-adapter.js";

/**
 * Recovery only: Photon ingress remains webhook-driven. This drain gives
 * encrypted inbox records another chance to enter the same ordered Eve/Chat
 * SDK hand-off after an invocation interruption.
 */
export default defineSchedule({
  cron: "* * * * *",
  async run() {
    assertIMessageRuntimeConfiguration();
    await drainIMessageVideoJobs({ adapter: imessageAdapter });
  },
});
