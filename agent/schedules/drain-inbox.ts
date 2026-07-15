import { defineSchedule } from "eve/schedules";
import { drainIMessageVideoJobs } from "../../lib/imessage-video-runtime.js";
import {
  assertIMessageRuntimeConfiguration,
  imessageAdapter,
} from "../../lib/imessage-adapter.js";
import { assertQueueEnvironment } from "../../lib/imessage-channel-runtime.js";
import { loadAirEnvironment } from "../../lib/env.js";

/**
 * Recovery only: Photon ingress remains webhook-driven. This drain gives
 * encrypted inbox records another chance to enter the same ordered Eve/Chat
 * SDK hand-off after an invocation interruption.
 */
export default defineSchedule({
  cron: "* * * * *",
  async run() {
    try {
      assertIMessageRuntimeConfiguration();
    } catch {
      console.error("air_imessage_recovery_configuration_unavailable");
      return;
    }

    try {
      // Validate the full environment before opening Redis. This keeps the
      // operator signal precise while still never emitting values or errors.
      assertQueueEnvironment(loadAirEnvironment());
    } catch {
      console.error("air_imessage_recovery_environment_invalid");
      return;
    }

    try {
      await drainIMessageVideoJobs({
        adapter: imessageAdapter,
        // The scheduled recovery path needs a redacted failure signal; its
        // normal empty-queue read is already the runtime probe, so do not add
        // another request or a success log every minute.
        onStorageUnavailable: () => console.error("air_imessage_recovery_redis_unavailable"),
      });
    } catch {
      // Do not let Redis or provider details leak through a scheduled-function
      // log. The fixed category is enough for operators.
      console.error("air_imessage_recovery_redis_unavailable");
    }
  },
});
