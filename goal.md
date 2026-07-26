# Air Direct Studio — v2.1 delivery-only recovery goal

Air is being rebuilt as a private, allowlisted iMessage studio for image and
video generation on the existing Vercel `air` project and existing Redis
resource. The runtime path is deliberately direct: signed iMessage receipt →
encrypted durable inbox/outbox → approved provider boundary → iMessage result.
It contains no Eve, Zap, chat-adapter, or LLM runtime dependency.

## Current authorized scope

The original dark-preview posture has been superseded by a narrowly scoped
**delivery-only production recovery**. It may receive an allowlisted signed
iMessage receipt and send the non-paid text responses needed to prove the
direct path. It remains non-billable and may not admit or submit generation:

- `AIR_RELEASE_PHASE=production` with a reviewed positive release generation
  and matching durable release fence.
- `AIR_ACCEPT_NEW_JOBS=false`.
- `AIR_ALLOW_PROVIDER_SUBMIT=false`.
- `AIR_ALLOW_PROVIDER_RECOVERY=false`.
- `AIR_DELIVERY_ENABLED=true` only for direct text delivery.
- The committed registry has zero enabled models and no default model.
- The Photon transport may be lazily verified only by authenticated readiness
  or a durable outbox drain; it must fail closed before any physical send.

No new Photon project, user, line, or webhook is authorized. Existing inbound
webhook configuration may be inspected but not changed without a separate
trust-boundary approval. No paid GMI/fal request, provider catalog call,
canary, payment action, or new paid infrastructure is authorized by this
document.

## Required safety properties

1. The single public webhook verifies the raw Photon/Spectrum HMAC before JSON
   parsing and persists an encrypted idempotent receipt before `200`.
2. Only direct messages from hashed allowlisted senders can enter the durable
   inbox. Non-allowlisted, group, malformed, duplicate, and outbound events
   cannot reach a provider path.
3. Prompts, identifiers, attachment references, model settings, provider IDs,
   and outbox payloads are encrypted at rest. Logs and plaintext metadata must
   not contain them. Terminal inbox receipts atomically scrub their encrypted
   envelope and retain only pseudonymous receipt metadata. Nonterminal job
   metadata is content-free and durable; its encrypted input snapshot has an
   immutable 24-hour lifetime.
4. A durable outbox intent is recorded before every user-visible reply.
   Ambiguous provider submission or iMessage delivery is reconciled manually,
   never blindly retried.
5. The minute Vercel recovery route is `CRON_SECRET` bearer-protected and each
   serverless function has a 60-second execution limit. Protected readiness
   verifies Redis with a non-tenant operational key, reports a
   environment-scoped Cron heartbeat, and reports both configured and effective
   (release-phase-gated) kill-switch state without returning values.
6. Pending assembly is scoped by Photon project, sender, chat, and sending
   line. Durable receipt order fences every write, cancel, and maintenance
   transition; retries cannot refresh the absolute 10-minute encrypted media
   retention or resurrect an older pending item. Media-first correction is a
   durable 500 ms `dueAt` entry, never an in-function debounce.

## Activation gates beyond dark deployment

A separate Fable approval must cover all of the following before a canary:

- authenticated GMI catalog and exact model-schema verification;
- immutable registry entry, exact input-host policy, verified current price,
  explicit budget ID, and per-job/daily/canary caps;
- fixture tests covering the selected provider schema and ambiguity handling;
- a real-line Photon send/fetch transport spike and allowlisted iPhone test;
- explicit enablement of only the necessary kill switches.
- an authenticated shared Redis release-generation fence operated with an
  approved rollback procedure. The implemented outbox fence covers generic
  responses outside pending assembly; the future provider executor must bind
  its own intents before any provider enablement.

Paid generation remains blocked until the canary has completed, the limits are
enforced by evidence, and the owner explicitly approves a separate paid
promotion. The delivery-only handset recovery is not a canary or provider
approval.
