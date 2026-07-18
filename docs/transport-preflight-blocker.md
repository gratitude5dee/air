# iMessage transport preflight — delivery-only evidence

This record constrains Air's first real-line test. It does **not** authorize
image/video generation, a provider call, attachment delivery, or any paid
operation.

## Reviewed implementation

- `lib/studio/photon-transport.ts` uses the direct Photon iMessage client,
  rather than the retired Chat adapter or Spectrum's high-level `space.send`.
- The encrypted inbound `chatGuid` and sending line select the exact cloud
  client. Each outbound text receives Air's durable outbox ID as
  `clientMessageId`.
- Client-side automatic retry and generated idempotency are disabled. A
  rejected physical send is retained as `send_unknown` and is never replayed
  automatically. Token/client lookup failure before an RPC is the only
  retryable transport result.
- The narrow `@photon-ai/advanced-imessage` build exception is limited to this
  file. It is a direct iMessage transport primitive, not an LLM or Chat
  runtime. The generic no-Chat/no-LLM guard remains in force everywhere else.
- Process-wide OTLP exporters and non-silent `LOG_LEVEL` fail startup before a
  Photon module is dynamically imported. No inbound SDK subscription or
  handler is initialized; Air's signed webhook remains authoritative.
- Attachment upload/fetch, tapbacks, native reply delivery, and all media
  paths fail closed.

## Required real-line evidence before claiming success

1. Deploy the delivery-only configuration with provider/job switches false,
   zero spend caps, and no GMI credential.
2. Confirm protected readiness is healthy only after the shared release fence
   has been explicitly advanced.
3. Point the existing Photon project webhook at
   `https://air.wzrd.tech/webhooks/imessage` with the matching signing secret.
4. The owner sends `/help` from the existing allowlisted number. Automation
   does not send the test message.
5. Record only coarse outcome evidence: signed receipt accepted, Cron/current
   drain processed it, and one text reply arrived. Do not retain message
   bodies, phone numbers, webhook payloads, or secrets in this repository.

If any send is ambiguous, stop automatic delivery and reconcile the opaque
outbox state before another attempt.
