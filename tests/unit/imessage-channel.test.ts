import { createHmac } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Lock, QueueEntry, StateAdapter } from "chat";
import {
  AR_AGENT_IMESSAGE_WORKFLOW_TITLE,
  AIR_CHAT_LOGGER,
  bot,
  channel,
  createAttemptScopedIMessageReceive,
  createOpaqueIMessageThread,
  createPrivacySafeChatState,
  IMESSAGE_ADAPTER_NAME,
  IMESSAGE_ADAPTER_INTERNAL_ROUTE,
  imessageAdapter,
  privacySafeChatInstrumentationMetadata,
} from "../../agent/channels/imessage.js";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("attempt-scoped iMessage Eve receiver", () => {
  it("keeps the physical thread outside task-scoped Eve state and workflow attributes", async () => {
    const calls: Array<{ message: unknown; options: unknown }> = [];
    const receive = createAttemptScopedIMessageReceive();
    const continuationKey = "a".repeat(64);
    const deliveryTargetId = "b".repeat(64);
    const physicalThreadId = "imessage:chat-guid~+14155551234";

    await receive(
      {
        auth: {
          authenticator: "air-imessage",
          principalId: "sender-hash",
          principalType: "user",
          attributes: {},
        },
        message: "How should this release be sequenced?",
        // `threadId` represents a hostile/legacy caller attempting to smuggle
        // provider addressing into the hand-off. The receiver accepts only the
        // typed opaque target and never copies unknown fields into Eve state.
        target: {
          adapterName: IMESSAGE_ADAPTER_NAME,
          deliveryTargetId,
          continuationKey,
          threadId: physicalThreadId,
        } as never,
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
          title: AR_AGENT_IMESSAGE_WORKFLOW_TITLE,
          state: {
            deliveryTargetId,
          },
        },
      },
    ]);
    expect(JSON.stringify(calls)).not.toContain(physicalThreadId);
    expect(JSON.stringify(calls)).not.toContain("+14155551234");
  });

  it("rejects a non-HMAC continuation before it can start or resume a session", async () => {
    const receive = createAttemptScopedIMessageReceive();

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
          deliveryTargetId: "b".repeat(64),
          continuationKey: "not-an-hmac",
        },
      },
      { send: async () => ({ id: "unexpected" } as never) },
    )).rejects.toThrow(/continuation key is invalid/i);
  });

  it("uses an opaque channel context and resolves a physical thread only inside post", async () => {
    const deliveryTargetId = "c".repeat(64);
    const sensitivePhone = "+14155551234";
    const sensitiveThread = `imessage:chat-guid~${sensitivePhone}`;
    const posted: Array<{ threadId: string; message: string }> = [];
    const thread = createOpaqueIMessageThread(deliveryTargetId, {
      resolveThreadId: async (targetId) => {
        expect(targetId).toBe(deliveryTargetId);
        return sensitiveThread;
      },
      threadFromId: (threadId) => ({
        id: threadId,
        post: async (message) => { posted.push({ threadId, message }); },
      }),
    });
    const adapter = (channel as unknown as {
      adapter: {
        createAdapterContext(input: {
          state: { deliveryTargetId: string | null };
          session: unknown;
          ctx: unknown;
        }): { state: { deliveryTargetId: string | null }; thread: { id: string } | null };
        instrumentation: {
          metadata(state: { deliveryTargetId: string | null }): Record<string, unknown>;
        };
      };
    }).adapter;
    const context = adapter.createAdapterContext({
      state: { deliveryTargetId },
      session: {},
      ctx: {},
    });
    const metadata = adapter.instrumentation.metadata({ deliveryTargetId });
    const directMetadata = privacySafeChatInstrumentationMetadata(
      { deliveryTargetId },
      "metadata-secret",
    );

    expect(thread.id).toBe(deliveryTargetId);
    expect(context.thread?.id).toBe(deliveryTargetId);
    expect(directMetadata.threadId).toMatch(/^[a-f0-9]{64}$/);
    expect(directMetadata.threadId).not.toBe(deliveryTargetId);
    expect(JSON.stringify({ context, metadata, directMetadata })).not.toContain(sensitiveThread);
    expect(JSON.stringify({ context, metadata, directMetadata })).not.toContain(sensitivePhone);

    await thread.post("A validated answer. [S1]");
    expect(posted).toEqual([{ threadId: sensitiveThread, message: "A validated answer. [S1]" }]);
  });

  it("retains only the pinned, unregistered adapter webhook route", () => {
    expect(channel.routes).toHaveLength(2);
    expect(channel.routes.map((route) => ({ method: route.method, path: route.path })))
      .toEqual([
        { method: "GET", path: IMESSAGE_ADAPTER_INTERNAL_ROUTE },
        { method: "POST", path: IMESSAGE_ADAPTER_INTERNAL_ROUTE },
      ]);
  });

  it("has no Chat SDK message-dispatch handlers on the internal adapter route", () => {
    const handlers = bot as unknown as {
      directMessageHandlers: readonly unknown[];
      mentionHandlers: readonly unknown[];
      messagePatterns: readonly unknown[];
      subscribedMessageHandlers: readonly unknown[];
    };

    expect(handlers.directMessageHandlers).toEqual([]);
    expect(handlers.mentionHandlers).toEqual([]);
    expect(handlers.messagePatterns).toEqual([]);
    expect(handlers.subscribedMessageHandlers).toEqual([]);
  });

  it("accepts a correctly signed internal adapter webhook without starting an Eve session or posting", async () => {
    const route = channel.routes.find(
      (candidate) => candidate.method === "POST" && candidate.path === IMESSAGE_ADAPTER_INTERNAL_ROUTE,
    );
    if (!route || route.transport === "websocket") {
      throw new Error("expected the internal iMessage adapter POST route");
    }

    const webhookSecret = "internal-adapter-route-test-secret";
    const timestamp = String(Math.floor(Date.now() / 1_000));
    const rawBody = JSON.stringify({
      event: "messages",
      space: {
        id: "iMessage;-;+15555550123",
        phone: "+15555550124",
        type: "dm",
      },
      message: {
        id: "internal-route-message-1",
        direction: "inbound",
        sender: { id: "+14155551234" },
        content: { type: "text", text: "This must remain inert." },
      },
    });
    const signature = `v0=${createHmac("sha256", webhookSecret)
      .update(`v0:${timestamp}:${rawBody}`)
      .digest("hex")}`;

    const state = bot.getState() as StateAdapter;
    const originalSetIfNotExists = state.setIfNotExists;
    const originalAcquireLock = state.acquireLock;
    const originalReleaseLock = state.releaseLock;
    const originalIsSubscribed = state.isSubscribed;
    const mutableBot = bot as unknown as { initialized: boolean };
    const originalInitialized = mutableBot.initialized;
    const mutableAdapter = imessageAdapter as unknown as {
      chat: unknown;
      webhookSecret: string | undefined;
    };
    const originalChat = mutableAdapter.chat;
    const originalWebhookSecret = mutableAdapter.webhookSecret;
    const outboundPost = vi.spyOn(imessageAdapter, "postMessage");
    const scheduled: Promise<unknown>[] = [];
    const sessionStarts: unknown[][] = [];

    // Keep this as a black-box route test: the official adapter still hands
    // the delivery to Chat SDK, but a tiny local state facade avoids a real
    // Redis/Spectrum initialization while the empty handler sets are exercised.
    state.setIfNotExists = async () => true;
    state.acquireLock = async (threadId): Promise<Lock> => ({
      threadId,
      token: "internal-route-lock",
      expiresAt: Date.now() + 30_000,
    });
    state.releaseLock = async () => undefined;
    state.isSubscribed = async () => false;
    mutableBot.initialized = true;
    mutableAdapter.chat = bot;
    mutableAdapter.webhookSecret = webhookSecret;

    try {
      const response = await route.handler(
        new Request(`https://air.example${IMESSAGE_ADAPTER_INTERNAL_ROUTE}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-spectrum-signature": signature,
            "x-spectrum-timestamp": timestamp,
          },
          body: rawBody,
        }),
        {
          send: async (...args: unknown[]) => {
            sessionStarts.push(args);
            return { id: "unexpected-eve-session" } as never;
          },
          getSession: () => ({ id: "unused" }) as never,
          receive: async () => ({ id: "unused" }) as never,
          params: {},
          waitUntil: (task: Promise<unknown>) => scheduled.push(task),
          requestIp: null,
        } as never,
      );

      expect(response.status).toBe(200);
      expect(scheduled).toHaveLength(1);
      await Promise.all(scheduled);
      expect(sessionStarts).toEqual([]);
      expect(outboundPost).not.toHaveBeenCalled();
    } finally {
      state.setIfNotExists = originalSetIfNotExists;
      state.acquireLock = originalAcquireLock;
      state.releaseLock = originalReleaseLock;
      state.isSubscribed = originalIsSubscribed;
      mutableBot.initialized = originalInitialized;
      mutableAdapter.chat = originalChat;
      mutableAdapter.webhookSecret = originalWebhookSecret;
    }
  });

  it("keeps framework transport errors out of console output", async () => {
    const sensitivePhone = "+14155551234";
    const sensitiveSecret = "photon-project-secret-must-not-log";
    const sensitiveText = "this inbound iMessage text must not log";
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const consoleInfo = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const consoleDebug = vi.spyOn(console, "debug").mockImplementation(() => undefined);
    const chat = bot as unknown as {
      logger: unknown;
      getState(): StateAdapter;
      processMessage(
        adapter: unknown,
        threadId: string,
        message: unknown,
      ): Promise<unknown>;
    };
    const state = chat.getState() as StateAdapter & {
      setIfNotExists: StateAdapter["setIfNotExists"];
      acquireLock: StateAdapter["acquireLock"];
    };
    const originalSetIfNotExists = state.setIfNotExists;
    const originalAcquireLock = state.acquireLock;
    state.setIfNotExists = async () => true;
    state.acquireLock = async () => {
      throw new Error(`${sensitiveSecret} ${sensitiveText}`);
    };

    try {
      await expect(chat.processMessage(
        {
          name: "imessage",
          channelIdFromThreadId: () => "photon-channel",
          isDM: () => true,
        },
        `imessage:chat-guid~${sensitivePhone}`,
        {
          id: "sensitive-message-id",
          text: sensitiveText,
          author: { userId: sensitivePhone, userName: sensitivePhone, isMe: false, isBot: false },
        },
      )).rejects.toThrow(/photon-project-secret-must-not-log/);
      await Promise.resolve();
    } finally {
      state.setIfNotExists = originalSetIfNotExists;
      state.acquireLock = originalAcquireLock;
    }

    const emitted = [
      ...consoleError.mock.calls,
      ...consoleWarn.mock.calls,
      ...consoleInfo.mock.calls,
      ...consoleDebug.mock.calls,
    ].flat().map((value) => String(value)).join("\n");
    expect(emitted).toBe("");
    expect(emitted).not.toContain(sensitivePhone);
    expect(emitted).not.toContain(sensitiveSecret);
    expect(emitted).not.toContain(sensitiveText);
    expect(chat.logger).toBe(AIR_CHAT_LOGGER);
  });

  it("pseudonymizes every Chat SDK Redis key and thread identifier", async () => {
    const observed: string[] = [];
    const base: StateAdapter = {
      connect: async () => undefined,
      disconnect: async () => undefined,
      subscribe: async (threadId) => { observed.push(threadId); },
      unsubscribe: async (threadId) => { observed.push(threadId); },
      isSubscribed: async (threadId) => {
        observed.push(threadId);
        return false;
      },
      acquireLock: async (threadId): Promise<Lock> => {
        observed.push(threadId);
        return { threadId, token: "lock-token", expiresAt: Date.now() + 1_000 };
      },
      forceReleaseLock: async (threadId) => { observed.push(threadId); },
      releaseLock: async (lock) => { observed.push(lock.threadId); },
      extendLock: async (lock) => {
        observed.push(lock.threadId);
        return true;
      },
      get: async <T>(_key: string): Promise<T | null> => {
        observed.push(_key);
        return null;
      },
      set: async <T>(key: string, _value: T) => { observed.push(key); },
      setIfNotExists: async (key) => {
        observed.push(key);
        return true;
      },
      delete: async (key) => { observed.push(key); },
      appendToList: async (key) => { observed.push(key); },
      enqueue: async (threadId, _entry: QueueEntry) => {
        observed.push(threadId);
        return 1;
      },
      dequeue: async (threadId) => {
        observed.push(threadId);
        return null;
      },
      queueDepth: async (threadId) => {
        observed.push(threadId);
        return 0;
      },
      getList: async <T>(key: string): Promise<T[]> => {
        observed.push(key);
        return [];
      },
    };
    const state = createPrivacySafeChatState(base, () => "identity-secret");
    const threadId = "imessage:chat-guid~+14155551234";
    const cacheKey = `thread-state:${threadId}`;
    const lock = await state.acquireLock(threadId, 1_000);

    await Promise.all([
      state.subscribe(threadId),
      state.unsubscribe(threadId),
      state.isSubscribed(threadId),
      state.forceReleaseLock(threadId),
      state.releaseLock(lock as Lock),
      state.extendLock(lock as Lock, 1_000),
      state.get(cacheKey),
      state.set(cacheKey, { ok: true }),
      state.setIfNotExists(cacheKey, true),
      state.delete(cacheKey),
      state.appendToList(cacheKey, { ok: true }),
      state.enqueue(threadId, { enqueuedAt: Date.now(), expiresAt: Date.now() + 1_000, message: {} as never }, 1),
      state.dequeue(threadId),
      state.queueDepth(threadId),
      state.getList(cacheKey),
    ]);

    expect(observed).toHaveLength(16);
    expect(observed.every((key) => /^[a-f0-9]{64}$/.test(key))).toBe(true);
    expect(observed.join("\n")).not.toContain(threadId);
    expect(observed.join("\n")).not.toContain("+14155551234");
  });
});
