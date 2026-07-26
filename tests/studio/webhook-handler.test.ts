import { createHmac } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createQueueCipher } from "../../lib/crypto.js";
import { MemoryRedis } from "../../lib/redis.js";
import { createSenderIdentity, hashThreadIdentifier } from "../../lib/security.js";
import { loadStudioEnvironment } from "../../lib/studio/config.js";
import { drainStudio } from "../../lib/studio/engine.js";
import {
  PhotonIMessageTransport,
  type PhotonIMessageClient,
  type PhotonIMessagePort,
} from "../../lib/studio/photon-transport.js";
import type { StudioProviderResultReferencePort } from "../../lib/studio/provider-port.js";
import { StudioStore } from "../../lib/studio/store.js";
import { authorizeStudioDrain, handleStudioIMessageWebhook } from "../../lib/studio/webhook-handler.js";
import type { StudioRuntime } from "../../lib/studio/runtime.js";

const webhookSecret = "webhook-secret";
const identitySecret = "identity-secret";
const senderId = "+14155550123";
const queueKey = Buffer.alloc(32, 19).toString("base64");
const welcomeOne = "Hey — I’m Air by WZRD ⚡ Your image + video studio in iMessage.";
const welcomeTwo = "Create with /imagine, /animate, /edit-image, or /edit-video. Try: /imagine a chrome hummingbird at sunrise. Settings: /model-settings • Help: /help. Generation is currently unavailable while Air completes provider review; any later approved path will disclose its media-retention terms.";
const helpText = [
  "Air commands:",
  "/imagine <prompt>",
  "/animate <prompt>",
  "/edit-image <prompt>",
  "/edit-video <prompt>",
  "/models [mode] • /model-settings • /status • /cancel • /privacy",
].join("\n");

type PersistInput = {
  readonly receiptId: string;
  readonly senderHash: string;
  readonly threadHash: string;
  readonly envelope: {
    readonly chatGuid: string;
    readonly sendingLine: string;
    readonly replyTargetId?: string;
    readonly text: string | null;
    readonly attachments: readonly unknown[];
  };
};

afterEach(() => vi.unstubAllEnvs());

function configureEnvironment(overrides: Record<string, string | undefined> = {}): void {
  const values: Record<string, string | undefined> = {
    VERCEL_ENV: "preview",
    IMESSAGE_PROJECT_ID: "photon-project",
    IMESSAGE_PROJECT_SECRET: "photon-project-secret",
    IMESSAGE_WEBHOOK_SECRET: webhookSecret,
    REDIS_URL: "rediss://redis.example.test:6380",
    UPSTASH_REDIS_REST_URL: undefined,
    UPSTASH_REDIS_REST_TOKEN: undefined,
    AIR_ID_HASH_SECRET: identitySecret,
    AIR_QUEUE_ENCRYPTION_KEY: queueKey,
    AIR_ALLOWED_SENDER_HASHES: createSenderIdentity(identitySecret, senderId).senderHash,
    AIR_ACCEPT_NEW_JOBS: "false",
    AIR_ALLOW_PROVIDER_SUBMIT: "false",
    AIR_ALLOW_PROVIDER_RECOVERY: "false",
    AIR_DELIVERY_ENABLED: "false",
    ...overrides,
  };
  for (const [name, value] of Object.entries(values)) vi.stubEnv(name, value);
}

function messagePayload(overrides: {
  readonly chatGuid?: string;
  readonly phone?: string;
  readonly spaceType?: "dm" | "group";
  readonly direction?: "inbound" | "outbound";
  readonly senderId?: string;
  readonly messageId?: string;
  readonly text?: string;
  readonly content?: unknown;
} = {}): Record<string, unknown> {
  const space = {
    id: overrides.chatGuid ?? "any;-;+14155550123",
    phone: overrides.phone ?? "+14155550999",
    type: overrides.spaceType ?? "dm",
  };
  return {
    event: "messages",
    space,
    message: {
      id: overrides.messageId ?? "message-001",
      direction: overrides.direction ?? "inbound",
      sender: { id: overrides.senderId ?? senderId },
      space,
      content: overrides.content ?? { type: "text", text: overrides.text ?? "Make a rain-soaked cyberpunk alley." },
    },
  };
}

function signedRequest(payload: unknown, options: { readonly signature?: string; readonly webhookId?: string } = {}): Request {
  const raw = typeof payload === "string" ? payload : JSON.stringify(payload);
  const timestamp = String(Math.floor(Date.now() / 1_000));
  const signature = options.signature ?? `v0=${createHmac("sha256", webhookSecret)
    .update(`v0:${timestamp}:${raw}`)
    .digest("hex")}`;
  return new Request("https://air.example.test/webhooks/imessage", {
    method: "POST",
    headers: {
      "x-spectrum-signature": signature,
      "x-spectrum-timestamp": timestamp,
      "x-spectrum-webhook-id": options.webhookId ?? "delivery-001",
    },
    body: raw,
  });
}

function runtimeWith(persistInbox: (input: PersistInput) => Promise<unknown>): StudioRuntime {
  return {
    store: {
      receiptId: (projectId: string, messageId: string) => `receipt:${projectId}:${messageId}`,
      persistInbox,
    },
  } as unknown as StudioRuntime;
}

/**
 * Production-shaped delivery-only wiring for the ingress/drain regression.
 * The provider exposes every paid recovery boundary as a tripwire so this
 * non-paid path proves it does not submit, poll, or materialize an artifact.
 */
async function createActiveDeliveryOnlyFixture() {
  configureEnvironment({
    VERCEL_ENV: "production",
    AIR_RELEASE_PHASE: "production",
    AIR_RELEASE_GENERATION: "1",
    AIR_RELEASE_FENCE_SECRET: "test-release-fence-secret",
    AIR_DELIVERY_ENABLED: "true",
    AIR_JOB_SPEND_CAP_USD: "0",
    AIR_DAILY_SPEND_CAP_USD: "0",
  });
  const environment = loadStudioEnvironment();
  const now = Date.now();
  const cipher = createQueueCipher(queueKey);
  const redis = new MemoryRedis(() => now);
  const store = new StudioStore({
    redis,
    cipher,
    identityHashSecret: identitySecret,
    clock: () => now,
    releaseStamp: { generation: environment.releaseGeneration, phase: environment.releasePhase },
  });
  const darkStore = new StudioStore({
    redis,
    cipher,
    identityHashSecret: identitySecret,
    clock: () => now,
    releaseStamp: { generation: 0, phase: "dark" },
  });
  expect(await darkStore.advanceReleaseFence(-1)).toEqual({ kind: "advanced" });
  expect(await store.advanceReleaseFence(0)).toEqual({ kind: "advanced" });

  const sendText = vi.fn<PhotonIMessageClient["messages"]["sendText"]>(async () => ({ guid: "mock-sent-message" }));
  const sendAttachment = vi.fn<PhotonIMessageClient["messages"]["sendAttachment"]>(async () => ({ guid: "mock-attachment-message" }));
  const setReaction = vi.fn<PhotonIMessageClient["messages"]["setReaction"]>(async () => ({ guid: "mock-reaction-message" }));
  const upload = vi.fn<PhotonIMessageClient["attachments"]["upload"]>(async () => ({
    attachment: { guid: "mock-uploaded-attachment" },
  }));
  const client: PhotonIMessageClient = {
    chats: { setTyping: vi.fn<PhotonIMessageClient["chats"]["setTyping"]>(async () => undefined) },
    attachments: { upload },
    messages: { sendText, sendAttachment, setReaction },
  };
  const getClient = vi.fn<PhotonIMessagePort["getClient"]>(async () => client);
  const transport = new PhotonIMessageTransport({
    projectId: "photon-project",
    projectSecret: "photon-project-secret",
    webhookSecret,
    port: { getClient },
  });
  const prepareValidatedProviderArtifact = vi.spyOn(transport, "prepareValidatedProviderArtifact");
  const sendPreparedOutput = vi.spyOn(transport, "sendPreparedOutput");
  const fetchAttachment = vi.spyOn(transport, "fetchAttachment");
  const provider = {
    verified: true,
    resultPollingSupported: true as const,
    resultMaterializationSupported: true as const,
    submit: vi.fn<StudioProviderResultReferencePort["submit"]>(async () => ({
      kind: "accepted" as const,
      providerRequestId: "must-not-submit",
    })),
    pollResult: vi.fn<StudioProviderResultReferencePort["pollResult"]>(async () => ({ kind: "pending" as const })),
    materializeResult: vi.fn<StudioProviderResultReferencePort["materializeResult"]>(async () => ({ kind: "pending" as const })),
  } satisfies StudioProviderResultReferencePort;
  const runtime: StudioRuntime = {
    environment,
    cipher,
    store,
    transport,
    provider,
    storageTransport: "tcp",
    readiness: () => [],
    close: async () => undefined,
  };

  return {
    runtime,
    store,
    provider,
    transport,
    sendText,
    sendAttachment,
    setReaction,
    upload,
    getClient,
    prepareValidatedProviderArtifact,
    sendPreparedOutput,
    fetchAttachment,
  };
}

describe("Air iMessage webhook", () => {
  it("persists an allowlisted inbound DM before acknowledging its signed raw delivery", async () => {
    configureEnvironment();
    let releasePersistence!: () => void;
    const persistenceGate = new Promise<void>((resolve) => {
      releasePersistence = resolve;
    });
    const persistInbox = vi.fn(async (input: PersistInput) => {
      await persistenceGate;
      return { created: true, record: { receiptId: input.receiptId } };
    });
    const runtime = runtimeWith(persistInbox);
    const responsePromise = handleStudioIMessageWebhook(signedRequest(messagePayload()), {
      getRuntime: async () => runtime,
    });

    await vi.waitFor(() => expect(persistInbox).toHaveBeenCalledTimes(1));
    let responseSettled = false;
    void responsePromise.then(() => {
      responseSettled = true;
    });
    await Promise.resolve();
    expect(responseSettled).toBe(false);

    releasePersistence();
    const response = await responsePromise;
    expect(response.status).toBe(200);
    expect(persistInbox).toHaveBeenCalledWith(expect.objectContaining({
      receiptId: "receipt:photon-project:message-001",
      senderHash: createSenderIdentity(identitySecret, senderId).senderHash,
      threadHash: hashThreadIdentifier(
        identitySecret,
        "photon-project\u0000any;-;+14155550123\u0000+14155550999",
      ),
      envelope: expect.objectContaining({
        chatGuid: "any;-;+14155550123",
        sendingLine: "+14155550999",
        replyTargetId: "message-001",
        text: "Make a rain-soaked cyberpunk alley.",
        attachments: [],
      }),
    }));
    const persisted = persistInbox.mock.calls[0]?.[0];
    if (!persisted) throw new Error("expected inbound persistence");
    expect(persisted.envelope).not.toHaveProperty("senderIdentifier");
    expect(persisted.envelope).not.toHaveProperty("messageId");
  });

  it("persists duplicate deliveries without scheduling any ingress drain", async () => {
    configureEnvironment();
    const persistInbox = vi.fn(async (input: PersistInput) => ({
      created: false,
      record: { receiptId: input.receiptId },
    }));
    const response = await handleStudioIMessageWebhook(signedRequest(messagePayload()), {
      getRuntime: async () => runtimeWith(persistInbox),
    });

    expect(response.status).toBe(200);
    expect(persistInbox).toHaveBeenCalledTimes(1);
  });

  it("deduplicates a signed /help retry into exactly one delivery-only Photon response sequence", async () => {
    const fixture = await createActiveDeliveryOnlyFixture();
    const persistInbox = vi.spyOn(fixture.store, "persistInbox");
    const payload = messagePayload({ messageId: "help-delivery-001", text: "/help" });
    const receiptId = fixture.store.receiptId("photon-project", "help-delivery-001");

    const first = await handleStudioIMessageWebhook(signedRequest(payload, { webhookId: "help-first" }), {
      getRuntime: async () => fixture.runtime,
    });
    const retry = await handleStudioIMessageWebhook(signedRequest(payload, { webhookId: "help-retry" }), {
      getRuntime: async () => fixture.runtime,
    });

    expect(first.status).toBe(200);
    expect(retry.status).toBe(200);
    expect(persistInbox).toHaveBeenCalledTimes(2);
    expect(persistInbox.mock.calls.map(([input]) => input.receiptId)).toEqual([
      receiptId,
      receiptId,
    ]);
    expect(fixture.sendText).not.toHaveBeenCalled();

    await drainStudio(fixture.runtime);

    expect(fixture.sendText.mock.calls.map(([, text]) => text)).toEqual([welcomeOne, welcomeTwo, helpText]);
    expect(fixture.sendText).toHaveBeenCalledTimes(3);
    for (const [chatGuid, _text, options] of fixture.sendText.mock.calls) {
      expect(chatGuid).toBe("any;-;+14155550123");
      expect(options).toEqual(expect.objectContaining({
        clientMessageId: expect.stringMatching(/^air-v2-outbox-[a-f0-9]{64}$/),
        enableDataDetection: false,
        enableLinkPreview: false,
        replyTo: "help-delivery-001",
      }));
    }
    expect(fixture.getClient).toHaveBeenCalledWith(
      "+14155550999",
      expect.objectContaining({
        deadlineAtMs: expect.any(Number),
        minimumPhysicalActionRemainingMs: expect.any(Number),
        minimumDeliveryPreparationRemainingMs: expect.any(Number),
      }),
    );
    // The delivery-only release has no paid-provider or media path, even when
    // a fully shaped provider/Photon artifact surface is present as a trap.
    expect(fixture.provider.submit).not.toHaveBeenCalled();
    expect(fixture.provider.pollResult).not.toHaveBeenCalled();
    expect(fixture.provider.materializeResult).not.toHaveBeenCalled();
    expect(fixture.fetchAttachment).not.toHaveBeenCalled();
    expect(fixture.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    expect(fixture.sendPreparedOutput).not.toHaveBeenCalled();
    expect(fixture.sendAttachment).not.toHaveBeenCalled();
    expect(fixture.upload).not.toHaveBeenCalled();
    expect(fixture.setReaction).not.toHaveBeenCalled();
    expect(await fixture.store.getActiveJob(createSenderIdentity(identitySecret, senderId).senderHash)).toBeNull();
  });

  it("keeps an active delivery-only release from admitting or submitting signed generation", async () => {
    const fixture = await createActiveDeliveryOnlyFixture();
    const admitJob = vi.spyOn(fixture.store, "admitJobWithInitialNotifications");
    const receiptId = fixture.store.receiptId("photon-project", "delivery-only-create-001");
    const response = await handleStudioIMessageWebhook(
      signedRequest(messagePayload({ messageId: "delivery-only-create-001", text: "/imagine a glass lantern" })),
      { getRuntime: async () => fixture.runtime },
    );

    expect(response.status).toBe(200);
    await drainStudio(fixture.runtime);
    // Run the next job-first Cron pass too: no deferred accepted job may find
    // a provider boundary after the delivery-only inbox response is sent.
    await drainStudio(fixture.runtime);

    await expect(fixture.store.readInbox(receiptId))
      .resolves.toMatchObject({ state: "maintenance_rejected" });
    expect(admitJob).not.toHaveBeenCalled();
    expect(await fixture.store.getActiveJob(createSenderIdentity(identitySecret, senderId).senderHash)).toBeNull();
    expect(fixture.provider.submit).not.toHaveBeenCalled();
    expect(fixture.provider.pollResult).not.toHaveBeenCalled();
    expect(fixture.provider.materializeResult).not.toHaveBeenCalled();
    expect(fixture.fetchAttachment).not.toHaveBeenCalled();
    expect(fixture.prepareValidatedProviderArtifact).not.toHaveBeenCalled();
    expect(fixture.sendPreparedOutput).not.toHaveBeenCalled();
    expect(fixture.upload).not.toHaveBeenCalled();
    expect(fixture.sendText.mock.calls.map(([, text]) => text)).toContain(
      "Air is in maintenance mode: new generation requests are not being accepted. No generation was submitted.",
    );
  });

  it("ignores signed groups, outbound echoes, and non-allowlisted DMs without opening storage", async () => {
    configureEnvironment();
    const getRuntime = vi.fn(async () => runtimeWith(async () => ({ created: true, record: {} })));
    const requests = [
      signedRequest(messagePayload({
        chatGuid: "any;+;+14155550123,+14155550456",
        spaceType: "group",
      }), { webhookId: "delivery-group" }),
      signedRequest(messagePayload({ direction: "outbound" }), { webhookId: "delivery-outbound" }),
      signedRequest(messagePayload({ senderId: "+14155550124" }), { webhookId: "delivery-unlisted" }),
    ];

    for (const request of requests) {
      expect((await handleStudioIMessageWebhook(request, { getRuntime })).status).toBe(200);
    }
    expect(getRuntime).not.toHaveBeenCalled();
  });

  it("ignores signed reaction, typing, membership, and empty message-shaped events before storage", async () => {
    configureEnvironment();
    const getRuntime = vi.fn(async () => runtimeWith(async () => ({ created: true, record: {} })));
    const payloads = [
      messagePayload({ content: { type: "reaction", emoji: "👍" }, messageId: "reaction-001" }),
      messagePayload({ content: { type: "typing", state: "started" }, messageId: "typing-001" }),
      messagePayload({ content: { type: "membership", action: "joined" }, messageId: "membership-001" }),
      messagePayload({ content: { type: "group", items: [] }, messageId: "empty-group-001" }),
    ];

    for (const [index, payload] of payloads.entries()) {
      expect((await handleStudioIMessageWebhook(
        signedRequest(payload, { webhookId: `non-actionable-${index}` }),
        { getRuntime },
      )).status).toBe(200);
    }
    expect(getRuntime).not.toHaveBeenCalled();
  });

  it("fails closed with a 503 before acknowledgement when durability configuration or runtime setup is unavailable", async () => {
    configureEnvironment({ REDIS_URL: undefined });
    const getRuntime = vi.fn(async () => runtimeWith(async () => ({ created: true, record: {} })));
    expect((await handleStudioIMessageWebhook(signedRequest(messagePayload()), { getRuntime })).status).toBe(503);
    expect(getRuntime).not.toHaveBeenCalled();

    configureEnvironment();
    expect((await handleStudioIMessageWebhook(
      signedRequest(messagePayload({ messageId: "message-runtime-down" })),
      { getRuntime: async () => Promise.reject(new Error("unavailable")) },
    )).status).toBe(503);
  });

  it("does not parse an invalid body before rejecting an invalid raw signature", async () => {
    configureEnvironment();
    const getRuntime = vi.fn(async () => runtimeWith(async () => ({ created: true, record: {} })));
    const response = await handleStudioIMessageWebhook(
      signedRequest("{this is deliberately not JSON", { signature: "v0=not-a-valid-signature" }),
      { getRuntime },
    );
    expect(response.status).toBe(401);
    expect(getRuntime).not.toHaveBeenCalled();
  });

  it("contains signed identifier validation failures instead of leaking an unhandled 500", async () => {
    configureEnvironment();
    const getRuntime = vi.fn(async () => runtimeWith(async () => ({ created: true, record: {} })));
    const response = await handleStudioIMessageWebhook(signedRequest(messagePayload({
      chatGuid: `any;-;${"x".repeat(1_100)}`,
    })), { getRuntime });

    expect(response.status).toBe(400);
    expect(getRuntime).not.toHaveBeenCalled();
  });
});

describe("Air studio internal drain authorization", () => {
  it("accepts only GET or POST with an exact bearer secret", () => {
    expect(authorizeStudioDrain(new Request("https://air.example.test/internal/drain", {
      method: "GET",
      headers: { authorization: "Bearer cron-secret" },
    }), "cron-secret")).toBe(true);
    expect(authorizeStudioDrain(new Request("https://air.example.test/internal/drain", {
      method: "POST",
      headers: { authorization: "Bearer cron-secret" },
    }), "cron-secret")).toBe(true);
    expect(authorizeStudioDrain(new Request("https://air.example.test/internal/drain", {
      method: "DELETE",
      headers: { authorization: "Bearer cron-secret" },
    }), "cron-secret")).toBe(false);
    expect(authorizeStudioDrain(new Request("https://air.example.test/internal/drain", {
      method: "GET",
      headers: { authorization: "Bearer almost-cron-secret" },
    }), "cron-secret")).toBe(false);
    expect(authorizeStudioDrain(new Request("https://air.example.test/internal/drain", {
      method: "GET",
      headers: { authorization: "Bearer cron-secret" },
    }), undefined)).toBe(false);
  });
});
