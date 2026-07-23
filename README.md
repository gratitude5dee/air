# Air

Air is a private, allowlisted image-and-video studio operated entirely through
iMessage. Its direct Vercel runtime is the source of truth; it does not route
new work through Zap, Eve, a chat adapter, or an LLM runtime. Zap remains
available only to drain or roll back jobs that were accepted by an older
deployment.

## Live status

The current direct release is Vercel deployment
`dpl_5ihyMswovHd4Aymcd6tJ5XtPTeTq` from `codex/air-direct-studio`, aliased to
[`https://air.wzrd.tech`](https://air.wzrd.tech). Its public health endpoint
returned `{"ok":true,"version":"2"}` after deployment.

This is **not** a paid-generation activation:

- The committed `2.1.0` model registry has zero enabled models and no mode
  defaults. `/imagine`, `/animate`, `/edit-image`, and `/edit-video` cannot
  reserve spend or call a provider.
- `GMI_API_KEY` is present as an encrypted, server-only Vercel environment
  variable for Preview and Production. Its value was not retrieved. A key by
  itself cannot provision the runtime: provider composition requires one
  exactly reviewed, enabled registry entry.
- The already-approved shared `$100` ceiling remains a background hard safety
  control. It is not a user-confirmation step and it is not a model approval.
- The direct Photon webhook configuration and an owner-originated allowlisted
  `/help` reply have not yet been observed on this deployment. Text delivery
  is therefore deployed but not yet accepted as a handset smoke result.

## Privacy and ingress guarantees

- Only direct messages from HMAC-hashed allowlisted senders are eligible.
- `POST /webhooks/imessage` is the sole unauthenticated ingress. It verifies
  the raw Photon/Spectrum signature before JSON parsing, then encrypts and
  idempotently persists the receipt before returning `200`.
- Prompts, sender/thread identifiers, attachment references, settings, and
  outbox payloads are encrypted in Redis. Media bytes, API keys, raw provider
  replies, provider URLs, and raw iMessage content are not logged.
- Terminal inbox records erase their encrypted prompt, attachment, and thread
  envelope; only bounded pseudonymous idempotency/audit metadata remains.
- Pending media is encrypted for at most ten minutes. Admission snapshots and
  outbox payloads have independent, bounded retention and cannot be extended
  by a retry.
- Every outbound intent is stamped with the shared release generation. A stale
  or missing fence cancels or pauses delivery before transport dispatch.

```mermaid
flowchart LR
  I["Allowlisted iMessage user"] --> P["Photon / Spectrum webhook"]
  P --> W["Raw-signature verification"]
  W --> R["Encrypted Redis receipt and outbox"]
  R --> D["Vercel minute drain"]
  D --> T["Direct Photon transport"]
  T --> I
  D -. "disabled until reviewed registry + evidence" .-> G["Direct GMI Request Queue"]
```

## Route surface

| Route | Method | Purpose |
| --- | --- | --- |
| `/` | `GET` | Minimal public Air status. |
| `/healthz` | `GET` | Static health response with no dependency details. |
| `/webhooks/imessage` | `POST` | Signed iMessage receipt ingress. |
| `/internal/drain-inbox` | `GET`, `POST` | `CRON_SECRET` bearer-protected minute recovery route. |
| `/internal/provider-audit` | `GET` | Manually invoked, `CRON_SECRET` bearer-protected, read-only GMI contract review. |
| `/internal/readiness` | `GET` | `CRON_SECRET` bearer-protected, secret-safe readiness report. |
| `/internal/release-fence` | `GET`, `POST` | `AIR_RELEASE_FENCE_SECRET` bearer-protected epoch read and release-generation CAS. |

`vercel.json` declares exactly one Cron: `/internal/drain-inbox` every minute,
with a 60-second function limit. The provider-audit route is intentionally not
a Cron and is never reachable without its bearer check.

## Safe server-only GMI audit

`GET /internal/provider-audit` is the only live use of the GMI credential in
the current release. It is deliberately narrow:

- It accepts only `GET` with the existing `CRON_SECRET` bearer credential.
- It makes only the fixed, read-only catalog and exact-model-detail requests
  for `flux-kontext-pro`.
- It cannot submit, poll, cancel, upload, fetch an artifact, enable a model,
  or modify the registry or release fence.
- Its response contains a fixed model ID, availability, minimized parameter
  names/types/requiredness, and deterministic evidence fingerprints. It does
  not return raw GMI payloads, API keys, prompts, request IDs, result URLs, or
  prices. `pricing: "manual_console_required"` is expected.

An operator may invoke it from a terminal where they already hold the secret;
never paste `CRON_SECRET` into chat, source, a URL, or logs:

```sh
curl --fail-with-body -sS \
  -H "Authorization: Bearer ${CRON_SECRET}" \
  https://air.wzrd.tech/internal/provider-audit
```

A `401` or `503` is a stop condition, not a reason to expose a secret or
broaden the endpoint. A successful audit is evidence for review only; it does
not activate `/imagine`.

## Dormant direct provider-result delivery path

The direct result lifecycle is implemented and tested, but it remains dormant
until the registry and release gates are satisfied. When a model is reviewed
and enabled, it protects the post-submit path as follows:

1. It observes only a durably known provider request; an ambiguous submission
   is terminalized for reconciliation rather than resubmitted.
2. It permits result retrieval only through the registry's exact reviewed
   host/path prefix and private or signed access contract. The registry must
   assert `publiclyAccessible: false`; a signed result currently requires a
   complete GCS V4 signed-query envelope, while a `provider-private` result
   needs a dedicated authenticated retriever rather than a generic HTTP GET.
3. A validated result is prepared as one native Photon attachment. An expired
   or ambiguous upload becomes `upload_unknown` and
   `reconciliation_required`; Air does not refetch the provider result or
   repeat the Photon upload.
4. One atomic finalizer persists the text preamble, the attachment intent,
   the delivery cursor, the successful observation, and `executing → ready`.
   Dispatch enforces preamble-before-attachment ordering.
5. A transport ambiguity becomes `send_unknown` for reconciliation, never an
   automatic physical resend. A release-fence mismatch blocks stale outbox
   delivery before dispatch.

These safeguards do not establish that a provider model is approved, private,
priced, or live. They are failure boundaries for a later reviewed release.

## Conditions to activate `/imagine`

`/imagine` is the first proposed paid mode. It remains disabled until all of
the following are recorded and deployed together:

1. The protected audit confirms exact `flux-kontext-pro` availability and its
   request schema, producing reviewable catalog and schema fingerprints.
2. An operator reviews the account-specific GMI Console/Model Hub price and
   records an exact bounded quote; public documentation is not price
   authority.
3. The output contract is proven private/signed for this exact model, including
   its exact result host and non-root path prefix. Generic provider examples
   are not sufficient. In particular, a bare storage URL is not evidence of
   private access even when a provider labels the response `is_public: false`.
4. A reviewed committed registry entry pins the adapter, request/result schema
   fingerprints, input policy, output policy, price quote, polling bounds, and
   one-image contract. The release must then pass typecheck, tests, and the
   Vercel build verifier.
5. The direct Photon webhook and allowlisted handset `/help` smoke have passed.
6. A separately reviewed release advances the shared release fence and keeps
   the existing `$100` hard ceiling in force. Flags alone can never enable
   spend.

`/animate`, `/edit-image`, and `/edit-video` remain disabled after an
`/imagine` activation until each has its own exact model, media, price, and
private-output evidence. Air must not use a GMI upload flow that returns a
documented publicly accessible input URL for allowlisted user media; those
modes require a separately verified private input-object contract.

## Configuration and release fence

Use [`.env.example`](.env.example) as a names-only dark-development template;
it intentionally leaves `GMI_API_KEY` absent. Production secrets belong in
Vercel and must be checked by name/status only, never pulled into a local file
or printed. In particular, `GMI_API_KEY`, Photon credentials, Redis tokens,
allowlist hashes, `CRON_SECRET`, and `AIR_RELEASE_FENCE_SECRET` stay
server-only.

Paid provider work requires all of the following at runtime: an active release
phase, explicit admission/submit policy, explicit positive per-job and daily
caps, a matching release fence, and a verified provider composed from the
committed registry. Provider recovery has its own explicit policy so a
rollback can stop new work without guessing about an already known request.

The fence is advanced only through authenticated compare-and-swap. Deploying
code or allowing the minute Cron to run does not arm a release. At publication,
claim, and dispatch, a nonmatching generation is blocked; the fence cannot
undo a transport call already authorized by an older worker, so that edge
remains a reconciliation event.

## Checks

```sh
npm ci
npm run typecheck
npm test
npm run verify:build
npm run build
```

See [RUNBOOK.md](RUNBOOK.md) for the current deployment handoff, protected
audit procedure, handset smoke test, activation gates, and rollback behavior.
