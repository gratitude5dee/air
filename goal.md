# Air Direct Studio — v2.1 delivery goal

Air is being rebuilt as a private, allowlisted iMessage studio for image and
video generation on the existing Vercel `air` project and existing Redis
resource. The runtime path is deliberately direct: signed iMessage receipt →
encrypted durable inbox/outbox → approved provider boundary → iMessage result.
It contains no Eve, Zap, chat-adapter, or LLM runtime dependency.

## Current authorized scope

This revision may be built, tested, pushed, and deployed as a Vercel **dark
preview** only. It must remain non-billable and non-delivering:

- `AIR_RELEASE_PHASE=dark`.
- `AIR_ACCEPT_NEW_JOBS=false`.
- `AIR_ALLOW_PROVIDER_SUBMIT=false`.
- `AIR_ALLOW_PROVIDER_RECOVERY=false`.
- `AIR_DELIVERY_ENABLED=false`.
- The committed registry has zero enabled models and no default model.
- The Photon transport remains unverified and fails closed.

No upstream iMessage webhook, paid GMI request, provider catalog call, canary,
production promotion, payment action, or new paid infrastructure is authorized
by this document.

## Required safety properties

1. The single public webhook verifies the raw Photon/Spectrum HMAC before JSON
   parsing and persists an encrypted idempotent receipt before `200`.
2. Only direct messages from hashed allowlisted senders can enter the durable
   inbox. Non-allowlisted, group, malformed, duplicate, and outbound events
   cannot reach a provider path.
3. Prompts, identifiers, attachment references, model settings, provider IDs,
   and outbox payloads are encrypted at rest. Logs and plaintext metadata must
   not contain them.
4. A durable outbox intent is recorded before every user-visible reply.
   Ambiguous provider submission or iMessage delivery is reconciled manually,
   never blindly retried.
5. The minute Vercel recovery route is `CRON_SECRET` bearer-protected and each
   serverless function has a 60-second execution limit. Protected readiness
   verifies Redis with a non-tenant operational key, reports a
   environment-scoped Cron heartbeat, and reports both configured and effective
   (release-phase-gated) kill-switch state without returning values.

## Activation gates beyond dark deployment

A separate Fable approval must cover all of the following before a canary:

- authenticated GMI catalog and exact model-schema verification;
- immutable registry entry, exact input-host policy, verified current price,
  explicit budget ID, and per-job/daily/canary caps;
- fixture tests covering the selected provider schema and ambiguity handling;
- a real-line Photon send/fetch transport spike and allowlisted iPhone test;
- explicit enablement of only the necessary kill switches.

Production remains blocked until the canary has completed, the limits are
enforced by evidence, and the owner explicitly approves promotion.
