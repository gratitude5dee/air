/**
 * Inert Workflow "world" for the Cloudflare Workers deployment of air-backend.
 *
 * Air registers no durable workflow runs — Eve uses channels + schedules only —
 * but the framework still boots a World at worker startup. The default
 * world-local backend requires a writable filesystem, which Workers do not
 * have, so this stub satisfies the World surface and fails loudly only if a
 * workflow delivery is ever attempted against this deployment.
 */
const DEPLOYMENT_ID = "air-workers";

function unavailable(name) {
  return () => {
    throw new Error(
      `Workflow storage "${name}" is unavailable in this deployment: air registers no workflow runs and the Workers build installs an inert world.`,
    );
  };
}

function unavailableGroup(group) {
  return new Proxy(
    {},
    {
      get(target, prop) {
        if (typeof prop === "string") return unavailable(`${group}.${prop}`);
        return undefined;
      },
    },
  );
}

/** @returns {import("./types").World} */
export function createWorld() {
  return {
    specVersion: 5,
    processExitTriggersQueueRedelivery: false,
    async getDeploymentId() {
      return DEPLOYMENT_ID;
    },
    async resolveLatestDeploymentId() {
      return DEPLOYMENT_ID;
    },
    queue: unavailable("queue"),
    createQueueHandler() {
      return async () =>
        Response.json(
          { error: "Workflow queueing is unavailable in this deployment." },
          { status: 503 },
        );
    },
    runs: unavailableGroup("runs"),
    steps: unavailableGroup("steps"),
    events: unavailableGroup("events"),
    hooks: unavailableGroup("hooks"),
    streams: unavailableGroup("streams"),
    async start() {},
    async close() {},
  };
}

export default createWorld;
