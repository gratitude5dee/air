import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  createClient: vi.fn(),
}));

vi.mock("@photon-ai/advanced-imessage", () => ({ createClient: mocks.createClient }));

import {
  validateStudioProviderArtifact,
  type StudioOutputIndex,
} from "../../lib/studio/artifact-validation.js";
import {
  isStudioPreparedOutputTransport,
  UnverifiedPhotonTransport,
  type IMessageThreadReference,
} from "../../lib/studio/imessage-transport.js";
import type { StudioPreparedOutput } from "../../lib/studio/store.js";
import { assertNoPhotonTelemetryExporters } from "../../lib/studio/photon-telemetry.js";
import {
  createPhotonIMessageTransport,
  PhotonIMessageTransport,
  resolvePhotonIMessageClientRoute,
  type PhotonTokenIssuer,
  type PhotonIMessagePort,
} from "../../lib/studio/photon-transport.js";

const THREAD: IMessageThreadReference = Object.freeze({
  chatGuid: "any;-;+15555550101",
  sendingLine: "shared",
});
const REPLY_THREAD: IMessageThreadReference = Object.freeze({
  ...THREAD,
  replyTargetId: "incoming-message-guid",
});
const ONE_PIXEL_PNG = Uint8Array.from(Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
  "base64",
));

async function* singleChunk(bytes: Uint8Array): AsyncGenerator<Uint8Array> {
  yield bytes;
}

async function validatedOutput(outputIndex: StudioOutputIndex = 1) {
  return validateStudioProviderArtifact({
    expectedMediaType: "image",
    declaredMimeType: "image/png",
    stream: singleChunk(ONE_PIXEL_PNG),
    outputIndex,
  });
}

function fixture(): {
  readonly transport: PhotonIMessageTransport;
  readonly port: { readonly getClient: ReturnType<typeof vi.fn> };
  readonly client: {
    readonly chats: { readonly setTyping: ReturnType<typeof vi.fn> };
    readonly attachments: { readonly upload: ReturnType<typeof vi.fn> };
    readonly messages: {
      readonly sendText: ReturnType<typeof vi.fn>;
      readonly sendAttachment: ReturnType<typeof vi.fn>;
      readonly setReaction: ReturnType<typeof vi.fn>;
    };
  };
} {
  const client = {
    chats: { setTyping: vi.fn(async () => undefined) },
    attachments: { upload: vi.fn(async () => ({ attachment: { guid: "uploaded-attachment-guid" } })) },
    messages: {
      sendText: vi.fn(async () => ({ guid: "sent-message" })),
      sendAttachment: vi.fn(async () => ({ guid: "sent-attachment-message" })),
      setReaction: vi.fn(async () => ({ guid: "reaction-message" })),
    },
  };
  const port = { getClient: vi.fn(async () => client) };
  const transport = new PhotonIMessageTransport({
    projectId: "project",
    projectSecret: "project-secret",
    webhookSecret: "webhook-secret",
    port: port as unknown as PhotonIMessagePort,
  });
  return { transport, port, client };
}

describe("Photon iMessage text transport", () => {
  beforeEach(() => {
    mocks.createClient.mockReset();
  });

  it("uses the pooled client for a shared Cloud token while preserving dedicated-line routing", () => {
    expect(resolvePhotonIMessageClientRoute("shared", "+15555550101")).toBe("shared");
    expect(resolvePhotonIMessageClientRoute("dedicated", "+15555550101")).toBe("+15555550101");
    expect(() => resolvePhotonIMessageClientRoute("shared", "bad\nline")).toThrow("invalid_photon_sending_line");
  });

  it("requires the signed inbound webhook credential before marking delivery verified", () => {
    const port = { getClient: vi.fn() } as unknown as PhotonIMessagePort;
    expect(() => new PhotonIMessageTransport({
      projectId: "project",
      projectSecret: "project-secret",
      webhookSecret: "",
      port,
    })).toThrow("IMESSAGE_WEBHOOK_SECRET is invalid");
  });

  it("bounds Cloud token issuance before any iMessage client can be created", async () => {
    vi.useFakeTimers();
    try {
      const issueImessageTokens = vi.fn<PhotonTokenIssuer>(() => new Promise(() => undefined));
      const creation = createPhotonIMessageTransport({
        projectId: "project",
        projectSecret: "project-secret",
        webhookSecret: "webhook-secret",
        issueImessageTokens,
        tokenIssueTimeoutMs: 5,
      });
      const result = creation.then(
        () => null,
        (error: unknown) => error,
      );

      await vi.advanceTimersByTimeAsync(5);

      expect(await result).toBeInstanceOf(Error);
      expect((await result as Error).message).toBe("photon_operation_deadline_exceeded");
      expect(issueImessageTokens).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("refreshes a short-lived Cloud token before SDK dispatch and never mints from the token callback", async () => {
    vi.useFakeTimers();
    try {
      let sdkToken: (() => Promise<string>) | undefined;
      const client = {
        chats: { setTyping: vi.fn(async () => undefined) },
        attachments: { upload: vi.fn(async () => ({ attachment: { guid: "uploaded-attachment-guid" } })) },
        messages: {
          sendText: vi.fn(async () => {
            const token = sdkToken;
            if (!token) throw new Error("sdk_token_callback_unavailable");
            await token();
            return { guid: "sent-message" };
          }),
          sendAttachment: vi.fn(async () => ({ guid: "sent-attachment-message" })),
          setReaction: vi.fn(async () => ({ guid: "reaction-message" })),
        },
      };
      mocks.createClient.mockImplementation((options: { readonly token: () => Promise<string> }) => {
        sdkToken = options.token;
        return client;
      });
      const issueImessageTokens = vi.fn<PhotonTokenIssuer>()
        .mockResolvedValueOnce({ type: "shared", token: "initial-short-token", expiresIn: 1 })
        .mockResolvedValueOnce({ type: "shared", token: "refreshed-token", expiresIn: 60 });
      const transport = await createPhotonIMessageTransport({
        projectId: "project",
        projectSecret: "project-secret",
        webhookSecret: "webhook-secret",
        issueImessageTokens,
      });

      await expect(transport.sendText(THREAD, "Air is ready.", "short-token-outbox-key", {
        deadlineAtMs: Date.now() + 50_000,
      })).resolves.toEqual({ kind: "sent" });
      expect(sdkToken).toBeTypeOf("function");
      expect(issueImessageTokens).toHaveBeenCalledTimes(2);

      // If the SDK asks again after the token crosses the normal refresh
      // window, it receives the existing token. It must not trigger a new
      // network mint without the caller-owned deadline from getClient.
      await vi.advanceTimersByTimeAsync(31_000);
      await expect(sdkToken?.()).resolves.toBe("refreshed-token");
      expect(issueImessageTokens).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects a repeated short-TTL refresh before the SDK can admit a physical RPC", async () => {
    const client = {
      chats: { setTyping: vi.fn(async () => undefined) },
      attachments: { upload: vi.fn(async () => ({ attachment: { guid: "uploaded-attachment-guid" } })) },
      messages: {
        sendText: vi.fn(async () => ({ guid: "sent-message" })),
        sendAttachment: vi.fn(async () => ({ guid: "sent-attachment-message" })),
        setReaction: vi.fn(async () => ({ guid: "reaction-message" })),
      },
    };
    mocks.createClient.mockReturnValue(client);
    const issueImessageTokens = vi.fn<PhotonTokenIssuer>()
      .mockResolvedValueOnce({ type: "shared", token: "initial-short-token", expiresIn: 1 })
      .mockResolvedValueOnce({ type: "shared", token: "still-short-token", expiresIn: 1 })
      .mockResolvedValueOnce({ type: "shared", token: "must-not-be-minted", expiresIn: 60 });
    const transport = await createPhotonIMessageTransport({
      projectId: "project",
      projectSecret: "project-secret",
      webhookSecret: "webhook-secret",
      issueImessageTokens,
    });

    await expect(transport.sendText(THREAD, "Air is ready.", "repeated-short-token-outbox-key", {
      deadlineAtMs: Date.now() + 50_000,
    })).resolves.toEqual({ kind: "retryable_failure", code: "photon_client_unavailable" });

    expect(issueImessageTokens).toHaveBeenCalledTimes(2);
    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("does not begin a replacement token mint after the caller deadline has elapsed", async () => {
    const client = {
      chats: { setTyping: vi.fn(async () => undefined) },
      attachments: { upload: vi.fn(async () => ({ attachment: { guid: "uploaded-attachment-guid" } })) },
      messages: {
        sendText: vi.fn(async () => ({ guid: "sent-message" })),
        sendAttachment: vi.fn(async () => ({ guid: "sent-attachment-message" })),
        setReaction: vi.fn(async () => ({ guid: "reaction-message" })),
      },
    };
    mocks.createClient.mockReturnValue(client);
    const issueImessageTokens = vi.fn<PhotonTokenIssuer>()
      .mockResolvedValueOnce({ type: "shared", token: "initial-short-token", expiresIn: 1 });
    const transport = await createPhotonIMessageTransport({
      projectId: "project",
      projectSecret: "project-secret",
      webhookSecret: "webhook-secret",
      issueImessageTokens,
    });

    await expect(transport.sendText(THREAD, "Air is ready.", "expired-token-deadline-key", {
      deadlineAtMs: Date.now(),
    })).resolves.toEqual({ kind: "retryable_failure", code: "photon_client_unavailable" });

    expect(issueImessageTokens).toHaveBeenCalledTimes(1);
    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("rebuilds the stored sending line and carries Air's stable outbox key to Photon", async () => {
    const { transport, port, client } = fixture();

    await expect(transport.sendText(THREAD, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "sent" });

    expect(port.getClient).toHaveBeenCalledWith(THREAD.sendingLine);
    expect(client.messages.sendText).toHaveBeenCalledWith(THREAD.chatGuid, "Air is ready.", {
      clientMessageId: "stable-outbox-key",
      enableDataDetection: false,
      enableLinkPreview: false,
    });
  });

  it("uses the signed inbound message GUID as a typed native reply target", async () => {
    const { transport, client } = fixture();

    await expect(transport.sendText(REPLY_THREAD, "Air is ready.", "stable-reply-key"))
      .resolves.toEqual({ kind: "sent" });

    expect(client.messages.sendText).toHaveBeenCalledWith(REPLY_THREAD.chatGuid, "Air is ready.", {
      clientMessageId: "stable-reply-key",
      enableDataDetection: false,
      enableLinkPreview: false,
      replyTo: REPLY_THREAD.replyTargetId,
    });
  });

  it("maps a rejected physical send to manual reconciliation instead of replay", async () => {
    const { transport, client } = fixture();
    client.messages.sendText.mockRejectedValueOnce(new Error("network"));

    await expect(transport.sendText(THREAD, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "send_unknown", code: "photon_text_send_unknown" });
  });

  it("records a started send that outlives its deadline as unknown", async () => {
    vi.useFakeTimers();
    try {
      const { transport, client } = fixture();
      client.messages.sendText.mockImplementationOnce(async () => await new Promise<never>(() => undefined));
      const result = transport.sendText(THREAD, "Air is ready.", "deadline-outbox-key", {
        deadlineAtMs: Date.now() + 5,
      });

      await vi.advanceTimersByTimeAsync(5);

      await expect(result).resolves.toEqual({ kind: "send_unknown", code: "photon_text_send_unknown" });
      expect(client.messages.sendText).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("uses a retryable result only when a client cannot be resolved before a send RPC", async () => {
    const { transport, port, client } = fixture();
    port.getClient.mockRejectedValueOnce(new Error("token mint unavailable"));

    await expect(transport.sendText(THREAD, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "retryable_failure", code: "photon_client_unavailable" });
    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("does not pass invalid text, routes, or idempotency keys to Photon", async () => {
    const { transport, port, client } = fixture();

    await expect(transport.sendText(THREAD, "unsafe\rtext", "stable-outbox-key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_text_payload" });
    await expect(transport.sendText({ ...THREAD, chatGuid: "bad\nroute" }, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_thread_reference" });
    await expect(transport.sendText(THREAD, "Air is ready.", "bad key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_idempotency_key" });
    await expect(transport.sendText({ ...THREAD, replyTargetId: "bad\nreply" }, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_thread_reference" });

    expect(port.getClient).not.toHaveBeenCalled();
    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("fails a legacy chat GUID locally before resolving a Photon client or attempting an RPC", async () => {
    const { transport, port, client } = fixture();

    await expect(transport.sendText({ ...THREAD, chatGuid: "iMessage;-;+15555550101" }, "Air is ready.", "stable-outbox-key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_thread_reference" });

    expect(port.getClient).not.toHaveBeenCalled();
    expect(client.messages.sendText).not.toHaveBeenCalled();
  });

  it("uses Photon typing only as a best-effort display signal on the same stored route", async () => {
    const { transport, port, client } = fixture();

    await expect(transport.showTyping(THREAD)).resolves.toBeUndefined();
    await expect(transport.stopTyping(THREAD)).resolves.toBeUndefined();

    expect(port.getClient).toHaveBeenNthCalledWith(1, THREAD.sendingLine);
    expect(port.getClient).toHaveBeenNthCalledWith(2, THREAD.sendingLine);
    expect(client.chats.setTyping).toHaveBeenNthCalledWith(1, THREAD.chatGuid, true);
    expect(client.chats.setTyping).toHaveBeenNthCalledWith(2, THREAD.chatGuid, false);
  });

  it("advertises prepared output delivery only for the direct Photon transport", () => {
    const { transport } = fixture();

    expect(isStudioPreparedOutputTransport(transport)).toBe(true);
    expect(isStudioPreparedOutputTransport(new UnverifiedPhotonTransport())).toBe(false);
  });

  it("splits a canonical provider upload from the one physical attachment send", async () => {
    const { transport, client } = fixture();
    const artifact = await validatedOutput(1);

    const prepared = await transport.prepareValidatedProviderArtifact(REPLY_THREAD, artifact);
    expect(prepared).toEqual({
      kind: "prepared",
      output: { transport: "photon", attachmentGuid: "uploaded-attachment-guid" },
    });
    if (prepared.kind !== "prepared") throw new Error("expected an uploaded Photon attachment");

    const uploaded = client.attachments.upload.mock.calls[0]?.[0];
    expect(uploaded).toMatchObject({ fileName: "air-output-1.png" });
    expect(uploaded?.data).toEqual(artifact.bytes);
    expect(uploaded?.data).not.toBe(artifact.bytes);
    expect(client.messages.sendAttachment).not.toHaveBeenCalled();

    await expect(transport.sendPreparedOutput(REPLY_THREAD, prepared.output, "stable-artifact-key"))
      .resolves.toEqual({ kind: "sent" });

    expect(client.attachments.upload).toHaveBeenCalledTimes(1);
    expect(client.messages.sendAttachment).toHaveBeenCalledWith(
      REPLY_THREAD.chatGuid,
      "uploaded-attachment-guid",
      { clientMessageId: "stable-artifact-key", replyTo: REPLY_THREAD.replyTargetId },
    );
  });

  it("keeps legacy sendAttachment structurally compatible while routing a validated artifact through the split path", async () => {
    const { transport, client } = fixture();
    const artifact = await validatedOutput(2);

    await expect(transport.sendAttachment(REPLY_THREAD, artifact, "legacy-artifact-key"))
      .resolves.toEqual({ kind: "sent" });

    expect(client.attachments.upload).toHaveBeenCalledTimes(1);
    expect(client.messages.sendAttachment).toHaveBeenCalledWith(
      REPLY_THREAD.chatGuid,
      "uploaded-attachment-guid",
      { clientMessageId: "legacy-artifact-key", replyTo: REPLY_THREAD.replyTargetId },
    );
  });

  it("does not mark the legacy bridge retryable after an upload when its later send cannot resolve a client", async () => {
    const { transport, port, client } = fixture();
    const artifact = await validatedOutput(2);
    port.getClient.mockResolvedValueOnce(client).mockRejectedValueOnce(new Error("token refresh unavailable"));

    await expect(transport.sendAttachment(THREAD, artifact, "legacy-no-reupload-key"))
      .resolves.toEqual({ kind: "send_unknown", code: "photon_attachment_send_unknown" });

    expect(client.attachments.upload).toHaveBeenCalledTimes(1);
    expect(client.messages.sendAttachment).not.toHaveBeenCalled();
  });

  it("sends only a validated prepared Photon GUID and rejects a payload-bearing surrogate", async () => {
    const { transport, port, client } = fixture();
    const forged = {
      transport: "photon",
      attachmentGuid: "uploaded-attachment-guid",
      bytes: Uint8Array.of(1),
    } as unknown as StudioPreparedOutput;

    await expect(transport.sendPreparedOutput(THREAD, forged, "prepared-key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_prepared_photon_output" });
    expect(port.getClient).not.toHaveBeenCalled();
    expect(client.messages.sendAttachment).not.toHaveBeenCalled();

    await expect(transport.sendPreparedOutput(
      THREAD,
      { transport: "photon", attachmentGuid: "uploaded-attachment-guid" },
      "prepared-key",
    )).resolves.toEqual({ kind: "sent" });
    expect(client.attachments.upload).not.toHaveBeenCalled();
    expect(client.messages.sendAttachment).toHaveBeenCalledWith(
      THREAD.chatGuid,
      "uploaded-attachment-guid",
      { clientMessageId: "prepared-key" },
    );
  });

  it("never forwards an inbound attachment, zero-based output, or forged same-shape artifact", async () => {
    const { transport, port, client } = fixture();
    const artifact = await validatedOutput(1);
    const zeroBased = { ...artifact, outputIndex: 0 } as unknown as typeof artifact;
    const forgedSameShape = {
      ...artifact,
      bytes: Uint8Array.from(artifact.bytes),
      metadata: { ...artifact.metadata },
    } as unknown as typeof artifact;

    await expect(transport.sendAttachment(THREAD, { id: "inbound", mimeType: "image/png", sizeBytes: 1 }, "key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_outbound_artifact" });
    await expect(transport.sendAttachment(THREAD, zeroBased, "key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_outbound_artifact" });
    await expect(transport.sendAttachment(THREAD, forgedSameShape, "key"))
      .resolves.toEqual({ kind: "failed", code: "invalid_outbound_artifact" });

    expect(port.getClient).not.toHaveBeenCalled();
    expect(client.attachments.upload).not.toHaveBeenCalled();
    expect(client.messages.sendAttachment).not.toHaveBeenCalled();
  });

  it("uploads a fresh canonical copy after a caller mutates the public byte view", async () => {
    const { transport, client } = fixture();
    const artifact = await validatedOutput(3);
    const original = Uint8Array.from(artifact.bytes);

    artifact.bytes.fill(0);

    await expect(transport.sendAttachment(THREAD, artifact, "stable-mutated-artifact-key"))
      .resolves.toEqual({ kind: "sent" });

    const uploaded = client.attachments.upload.mock.calls[0]?.[0];
    expect(uploaded?.data).toEqual(original);
    expect(uploaded?.data).not.toBe(artifact.bytes);
    expect(uploaded?.data).not.toEqual(artifact.bytes);
  });

  it("records upload and attachment-send ambiguity for manual reconciliation", async () => {
    const { transport, client } = fixture();
    const artifact = await validatedOutput(2);
    client.attachments.upload.mockRejectedValueOnce(new Error("network"));

    await expect(transport.sendAttachment(THREAD, artifact, "stable-upload-key"))
      .resolves.toEqual({ kind: "send_unknown", code: "photon_attachment_upload_unknown" });
    expect(client.messages.sendAttachment).not.toHaveBeenCalled();

    client.messages.sendAttachment.mockRejectedValueOnce(new Error("network"));
    await expect(transport.sendAttachment(THREAD, artifact, "stable-send-key"))
      .resolves.toEqual({ kind: "send_unknown", code: "photon_attachment_send_unknown" });
  });

  it("uses the semantic like tapback and a durable client message ID", async () => {
    const { transport, client } = fixture();

    await expect(transport.addLikeTapback(THREAD, "incoming-message-guid", "stable-like-key"))
      .resolves.toEqual({ kind: "sent" });

    expect(client.messages.setReaction).toHaveBeenCalledWith(
      THREAD.chatGuid,
      "incoming-message-guid",
      { kind: "like" },
      true,
      { clientMessageId: "stable-like-key" },
    );

    client.messages.setReaction.mockRejectedValueOnce(new Error("network"));
    await expect(transport.addLikeTapback(THREAD, "incoming-message-guid", "stable-like-unknown"))
      .resolves.toEqual({ kind: "send_unknown", code: "photon_like_tapback_unknown" });
  });

  it("keeps inbound attachment fetching disabled", async () => {
    const { transport, client } = fixture();

    await expect(transport.fetchAttachment(THREAD, "attachment"))
      .rejects.toThrow("attachment_transport_not_enabled");

    expect(client.messages.sendText).not.toHaveBeenCalled();
    expect(client.attachments.upload).not.toHaveBeenCalled();
  });

  it("fails closed before Photon initialization when telemetry or logging is unsafe", () => {
    expect(() => assertNoPhotonTelemetryExporters({ OTEL_EXPORTER_OTLP_ENDPOINT: "https://telemetry.example.test" }))
      .toThrow("Photon telemetry exporters are not permitted for Air");
    expect(() => assertNoPhotonTelemetryExporters({ LOG_LEVEL: "info" }))
      .toThrow("Photon telemetry exporters are not permitted for Air");
    expect(() => assertNoPhotonTelemetryExporters({ LOG_LEVEL: "silent", OTEL_TRACES_EXPORTER: "none" })).not.toThrow();
  });
});
