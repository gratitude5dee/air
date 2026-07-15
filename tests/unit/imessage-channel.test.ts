import { describe, expect, it } from "vitest";
import {
  channel,
  createAttemptScopedIMessageReceive,
  IMESSAGE_ADAPTER_NAME,
  IMESSAGE_CHANNEL_REGISTRATION_ROUTE,
} from "../../agent/channels/imessage.js";

describe("attempt-scoped iMessage Eve receiver", () => {
  it("keeps the physical thread state separate from a task-scoped continuation", async () => {
    const calls: Array<{ message: unknown; options: unknown }> = [];
    const receive = createAttemptScopedIMessageReceive({
      thread: (threadId) => ({
        toJSON: () => ({
          _type: "chat:Thread" as const,
          adapterName: IMESSAGE_ADAPTER_NAME,
          channelId: "photon-channel",
          id: threadId,
          isDM: true,
        }),
      }),
    });
    const continuationKey = "a".repeat(64);

    await receive(
      {
        auth: {
          authenticator: "air-imessage",
          principalId: "sender-hash",
          principalType: "user",
          attributes: {},
        },
        message: "How should this release be sequenced?",
        target: {
          adapterName: IMESSAGE_ADAPTER_NAME,
          threadId: "imessage:thread-a",
          continuationKey,
        },
      },
      {
        send: async (message, options) => {
          calls.push({ message, options });
          return { id: "eve-session-a" } as never;
        },
      },
    );

    expect(calls).toEqual([
      {
        message: "How should this release be sequenced?",
        options: {
          auth: {
            authenticator: "air-imessage",
            principalId: "sender-hash",
            principalType: "user",
            attributes: {},
          },
          continuationToken: continuationKey,
          mode: "task",
          state: {
            thread: {
              _type: "chat:Thread",
              adapterName: IMESSAGE_ADAPTER_NAME,
              channelId: "photon-channel",
              id: "imessage:thread-a",
              isDM: true,
            },
          },
        },
      },
    ]);
  });

  it("rejects a non-HMAC continuation before it can start or resume a session", async () => {
    const receive = createAttemptScopedIMessageReceive({
      thread: () => {
        throw new Error("thread resolution must not run for an invalid key");
      },
    });

    await expect(receive(
      {
        auth: {
          authenticator: "air-imessage",
          principalId: "sender-hash",
          principalType: "user",
          attributes: {},
        },
        message: "hello",
        target: {
          adapterName: IMESSAGE_ADAPTER_NAME,
          threadId: "imessage:thread-a",
          continuationKey: "not-an-hmac",
        },
      },
      { send: async () => ({ id: "unexpected" } as never) },
    )).rejects.toThrow(/continuation key is invalid/i);
  });

  it("keeps the compiled-channel registration route inert", async () => {
    const route = channel.routes.find((candidate) =>
      candidate.method === "GET" && candidate.path === IMESSAGE_CHANNEL_REGISTRATION_ROUTE,
    );
    expect(route).toBeDefined();
    if (!route || route.transport === "websocket") throw new Error("missing iMessage registration route");

    const response = await route.handler(
      new Request(`https://air.example${IMESSAGE_CHANNEL_REGISTRATION_ROUTE}`),
      {
        send: async () => {
          throw new Error("the inert route must not start an Eve session");
        },
        getSession: () => {
          throw new Error("the inert route must not read an Eve session");
        },
        receive: async () => {
          throw new Error("the inert route must not hand off a message");
        },
        params: {},
        requestIp: null,
        waitUntil: () => {
          throw new Error("the inert route must not schedule work");
        },
      },
    );

    expect(response.status).toBe(404);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });
});
