# Air direct production runbook

This runbook covers the direct Vercel deployment at `air.wzrd.tech`. It keeps
the current release private, fail-closed, and non-billable while the GMI model
contract and real iMessage path are proved. It does not authorize secret
retrieval, a provider submission, or a generation command.

## Current handoff

- Source of truth: `codex/air-direct-studio`. Zap is retained only to drain or
  roll back legacy jobs that it already accepted; it is not a new-work path.
- Live public alias: [`https://air.wzrd.tech`](https://air.wzrd.tech). Record
  the exact deployment ID at promotion; do not reuse a stale ID from an older
  handoff.
- Verified public probe: `GET /healthz` returned `{"ok":true,"version":"2"}`.
- Production webhook posture: `POST /webhooks/imessage` validates and persists
  a signed receipt, then returns. It does not drain queues or perform provider
  or Photon delivery work. The minute `/internal/drain-inbox` Cron is the only
  scheduled worker that can progress stored work.
- Vercel has encrypted `GMI_API_KEY` configuration for Preview and Production.
  A protected production process check received an empty value. No secret was
  printed or retained; a variable name alone does not prove a usable key. The
  owner must correct it directly in Vercel before a protected GMI audit can be
  accepted as readiness evidence.
- The committed registry still has zero enabled models. The direct provider is
  therefore unprovisioned at runtime, even with the server-only key present.
- The public Flux result example's bare storage URL—even with
  `is_public: false`—does not prove a private/signed output contract. It is
  insufficient for Air, so no GMI model is armed.
- No real Photon webhook delivery or allowlisted handset `/help` response has
  been observed for this deployment. The test is also blocked on the
  explicitly authorized replacement of the exposed outbound Photon credential
  described below. Do not mark the iMessage path accepted until that test is
  completed.

The existing shared `$100` cap is a hard background safety control. It does
not add a per-request approval step, and it does not substitute for the model
and privacy gates below.

## Non-negotiable operator rules

1. Do not retrieve, print, commit, paste, or send any secret. Inspect Vercel
   environment variables by name/status only; never use an environment pull
   as a shortcut. The only rotation exception is the explicitly authorized
   Photon incident procedure below.
2. Do not create a second Vercel Cron. The only scheduled job is the minute
   drain at `/internal/drain-inbox`; `/internal/provider-audit` is manually
   invoked and authenticated.
3. Do not turn on a model by flipping a policy flag. The model registry, exact
   adapter binding, private-output contract, price evidence, release fence,
   and positive bounded spend policy must agree.
4. Do not log raw webhook bodies, phone numbers, prompts, provider responses,
   request IDs, artifact URLs, media bytes, or transport credentials.
5. Do not use a failed result upload or a `submission_unknown`,
   `upload_unknown`, or `send_unknown` state as permission to retry a physical
   provider or Photon action. Those states require reconciliation.
6. Do not treat `is_public: false` as proof that a result URL is private. A
   signed result must use the reviewed signed-URL contract; a provider-private
   result needs a dedicated authenticated retrieval implementation. Never use
   a documented publicly accessible provider upload URL for user media.
7. Keep Photon credentials separate. `IMESSAGE_WEBHOOK_SECRET` verifies the
   inbound raw-HMAC webhook; `IMESSAGE_PROJECT_SECRET` is an outbound project
   API credential used only at lazy delivery. Never substitute one for the
   other or paste either into a command transcript.

## Release verification

Before a future direct deployment, run the checks from the exact source
revision:

```sh
npm ci
npm run typecheck
npm test
npm run verify:build
npm run build
```

Deploy only to the existing Vercel project, then record the new deployment ID,
commit, and alias in this section. Confirm the public health route without
printing configuration:

```sh
curl --fail-with-body -sS https://air.wzrd.tech/healthz
```

The expected response is the static version response. It does not prove
Redis, Photon, GMI, a release fence, or paid generation.

For a names-only environment check, use the Vercel project/team already in
scope. The output must be treated as deployment metadata, not secret material:

```sh
vercel env ls production --scope 5dee-studios
```

Do not run `vercel env pull`, do not call a command that echoes a value, and do
not include an environment value in a support transcript.

## Protected inbox FIFO cutover

This is a required ordering-safety migration before pending prompt handling can
be considered. It is not a provider rollout: it cannot enable `/imagine` (or
any other generation mode), alter a release fence, enable a registry entry,
raise a cap, submit a provider request, or spend money. All current
zero-spend and dark-release restrictions remain in force before, during, and
after the cutover.

The migration route is intentionally separate from both the inbound webhook
HMAC and the minute Cron credential. Use it only from an operator-controlled
terminal where `AIR_RELEASE_FENCE_SECRET` is already present; do not paste,
print, retrieve, export to a transcript, or put the value in a URL. In
particular, do not substitute `IMESSAGE_WEBHOOK_SECRET` or `CRON_SECRET`.

Set only a non-secret route variable in that terminal:

```sh
AIR_FIFO_URL="https://air.wzrd.tech/internal/inbox-fifo-migration"
```

The route exposes a secret-free status view and exactly three literal,
one-way actions. There is no force, skip, reset, rollback, or arbitrary phase
setter.

| Phase | Meaning | Operator rule |
| --- | --- | --- |
| `legacy` | Existing compatibility global-ready behavior. | Do not begin until its ready index is empty. Old receipts are never backfilled or guessed into FIFO order. |
| `holding` | New signed receipts are persisted durably but are not claimable. | Prepare every holding scope; a held receipt cannot progress during this phase. |
| `fifo` | Only the exact durable receipt-order head for a sender/thread scope can be claimed. | A delayed, failed, or missing head blocks its own later receipts; it cannot be bypassed. |

Perform the cutover in order, preserving only the returned enum/counter
evidence in a handoff (never headers, prompts, phone numbers, receipt IDs, or
secrets).

1. Read status with the release-fence bearer credential already held by the
   operator:

   ```sh
   curl --fail-with-body -sS \
     -H "Authorization: Bearer ${AIR_RELEASE_FENCE_SECRET}" \
     "${AIR_FIFO_URL}"
   ```

   Expect a secret-free response such as
   `{"kind":"current","phase":"legacy","holdingScopes":0}`. A `401`
   or `503` is a stop condition. Do not diagnose it by exposing a credential.

2. Only when status is `legacy`, request the first one-way transition:

   ```sh
   curl --fail-with-body -sS -X POST \
     -H "Authorization: Bearer ${AIR_RELEASE_FENCE_SECRET}" \
     -H "X-Air-Fifo-Migration-Action: begin" \
     "${AIR_FIFO_URL}"
   ```

   A `202` response of `{"kind":"holding"}` starts the cutover.
   `{"kind":"already_holding"}` is a safe resumed invocation. A `409`
   `legacy_not_drained` means stop: preserve Redis state, allow the established
   recovery/reconciliation path to drain the old ready work, then reread
   status. Never delete, reorder, backfill, or force old receipts into FIFO.

3. While status is `holding`, invoke the bounded preparation action, then
   reread status. Repeat this exact action until it reports no remaining
   holding scopes:

   ```sh
   curl --fail-with-body -sS -X POST \
     -H "Authorization: Bearer ${AIR_RELEASE_FENCE_SECRET}" \
     -H "X-Air-Fifo-Migration-Action: prepare" \
     "${AIR_FIFO_URL}"
   ```

   A `202` `prepared` response includes only `preparedScopes` and
   `remainingScopes`; it does not disclose a scope or message. A `409`
   `blocked`, `not_holding`, or other non-`202` result is a stop condition.
   Do not activate while `holdingScopes` is nonzero. If a new receipt arrives
   between checks, prepare again; holding deliberately accepts it without
   claiming it.

4. Once a status read reports `{"kind":"current","phase":"holding",
   "holdingScopes":0}`, request activation:

   ```sh
   curl --fail-with-body -sS -X POST \
     -H "Authorization: Bearer ${AIR_RELEASE_FENCE_SECRET}" \
     -H "X-Air-Fifo-Migration-Action: activate" \
     "${AIR_FIFO_URL}"
   ```

   A `202` `activated` response completes the one-way transition. A `409`
   `holding_not_prepared` means a scope appeared or remains: return to
   `prepare`; do not bypass it. `already_fifo` is the safe resumed outcome.

5. Read status once more and record only that it reports `phase: "fifo"`.
   At this point each sender/thread scope advances only from its durable head
   to its exact next sequence after a terminal result. The minute drain
   remains the sole scheduler; this route creates no extra Cron or worker.

If any result is `invalid`, `blocked`, `legacy_not_drained`, or otherwise
unexpected, leave the migration in its current phase, preserve encrypted
state, and escalate through the normal reconciliation path. A one-way safety
gate is not an incident-recovery shortcut, and no endpoint can be used to
skip a predecessor.

## Photon credential incident and coordinated rotation

`IMESSAGE_WEBHOOK_SECRET` is the existing inbound signing secret for the
webhook target. `IMESSAGE_PROJECT_SECRET` is a different outbound Photon API
credential. A Photon CLI response unexpectedly emitted the outbound credential
in local tool output, so it must be treated as exposed.

Do not rotate either credential merely to perform a smoke test. Rotation of the
outbound credential requires explicit owner authorization because it will
invalidate the current outbound API access. Once that authorization exists,
perform one coordinated, secret-safe operation:

1. Rotate only the outbound Photon project credential. Capture it only long
   enough to update the encrypted Vercel environment; never print, save, or
   paste it.
2. Update every Vercel environment target intentionally using that credential,
   then deploy the direct branch. Do not change `IMESSAGE_WEBHOOK_SECRET` as
   part of this incident unless the inbound secret has a separate confirmed
   exposure.
3. Verify deployment and environment names/status without retrieving a value.
   Only then run the signed allowlisted handset `/help` smoke below.

Until explicit authorization and this coordinated update are complete, the
outbound credential must not be used for a live test. This is a security stop
condition, not a reason to ask anyone to paste a signing or API secret.

## Protected GMI catalog/schema audit

The GMI key may be used only through the deployed read-only audit until a
separate activation release is approved. The current protected production
process check received `GMI_API_KEY` as empty, so do not treat the listed Vercel
variable as a working credential or run this audit as acceptance evidence yet.
The owner must set a nonempty value directly in Vercel and deploy it without
sharing the value. After that, from an operator-controlled terminal where
`CRON_SECRET` is already set, run:

```sh
curl --fail-with-body -sS \
  -H "Authorization: Bearer ${CRON_SECRET}" \
  https://air.wzrd.tech/internal/provider-audit
```

Never reveal the header value. The response is intentionally safe to retain as
review evidence and has this shape:

```json
{
  "modelId": "flux-kontext-pro",
  "available": true,
  "catalogFingerprint": "sha256:<digest>",
  "schemaFingerprint": "sha256:<digest>",
  "parameters": [{ "name": "…", "declaredType": "…", "required": true }],
  "pricing": "manual_console_required"
}
```

The route makes only the fixed GMI model-list and exact-model-detail calls. It
cannot submit, poll, cancel, upload, fetch a result, update a registry, or
advance a release fence. It returns no raw provider response, key, prompt,
request ID, result URL, or price.

Interpret the result conservatively:

- `401` means stop and correct the operator authentication path without
  exposing the credential.
- `503` means stop and inspect deployment configuration (including the current
  empty GMI credential) or provider availability through the normal private
  operator path; do not broaden the route or add logging.
- `available: false`, a mismatched model ID, or an unexpected parameter schema
  means `/imagine` stays disabled.
- `pricing: "manual_console_required"` means obtain the exact account-specific
  price from GMI Console/Model Hub. Public docs and third-party price pages are
  not enough.

This audit does not prove result privacy. Before activation, record the exact
private/signed result host and non-root path prefix for this model and proof
that the result is not publicly accessible.

## Photon webhook and handset `/help` smoke

This remains the missing live acceptance test and is blocked until the
explicitly authorized outbound-credential rotation above has completed.

1. After that coordinated rotation and deployment, confirm the signed webhook target is
   `https://air.wzrd.tech/webhooks/imessage` and it uses the already-matching
   inbound signing secret. Do not retrieve or expose the signing secret, and
   do not alter it as part of the outbound-credential incident.
2. From the already allowlisted owner number, send exactly `/help` to Air. Do
   not send an attachment or a generation command during this smoke.
3. Wait through one minute-drain interval. A first successful contact can
   deliver two ordered welcome bubbles plus the help response; a later `/help`
   should deliver only the help response.
4. Record only the timestamp, deployment ID, signed-webhook acceptance, and
   whether the expected text arrived. Keep the phone number, message body,
   headers, and identifiers out of logs and handoff notes.

If the response does not arrive, stop. Inspect Photon delivery status and
Vercel function logs with redaction, verify the signature/target pairing, and
do not resend attachments or test generation as a diagnostic shortcut. Until
this result is observed, “iMessage live” remains unproven.

## `/imagine` activation gate

Do not enable paid work in this release. A later `/imagine` activation must
complete every gate below, in order:

1. Confirm through a secret-safe production check that the owner has supplied a
   nonempty `GMI_API_KEY` directly in Vercel. A variable name and the current
   empty-value check do not pass this gate.
2. Preserve the protected audit response showing exact
   `flux-kontext-pro` availability and its reviewed parameter schema.
3. Record the exact account-specific GMI price and an upper bound for the
   single-image request. Keep the already-approved shared `$100` ceiling in
   force; it is a ceiling, not a license to substitute a model.
4. Prove the exact input and result contracts. Result retrieval must be private
   or signed, non-public, and pinned to a specific host plus non-root path
   prefix. A bare storage URL is insufficient even if a provider returns
   `is_public: false`; Air currently accepts only a complete GCS V4 signed URL
   at its generic retrieval boundary. Do not infer the contract from a generic
   documentation example. The public [Flux quickstart](https://docs.gmicloud.ai/model-quickstarts/image/flux-kontext-pro)
   is specifically insufficient: its success example combines `is_public:false`
   with a bare storage URL and does not document access semantics.
5. Commit and review a registry entry that pins the exact GMI adapter, model
   ID, request/result schema fingerprints, required payload fields, input
   policy, output contract, price quote, polling bounds, and private retrieval
   policy. Run the full release verification suite and deploy it.
6. Deploy a durable encrypted provider-result reference split, or a reviewed
   proof that the complete observe/retrieve/validate/upload/finalize chain
   fits the bounded Vercel worker deadline. The current bounded Cron
   deliberately defers `executing` provider-result recovery before any
   provider or Photon action.
7. Complete the handset `/help` smoke above after the explicitly authorized
   outbound-credential rotation.
8. In a separately reviewed release, advance the shared release fence with its
   authenticated compare-and-swap and explicitly configure admission,
   provider-submit, provider-recovery, and delivery policies alongside
   positive bounded spend caps. A flag or key alone must still fail closed.
9. Submit only the reviewed one-image `/imagine` canary and observe its direct
   lifecycle without copying a provider URL or media into logs.

`/animate`, `/edit-image`, and `/edit-video` remain disabled after the first
`/imagine` release. Each needs its own exact model, media-input, pricing, and
private-output review. In particular, a GMI upload response documented as a
public URL is not an acceptable input-media transport for this private studio.

## Result delivery and incident response

The direct lifecycle is intentionally conservative once a reviewed model is
enabled:

- In the bounded minute Cron, an `executing` provider result is deferred before
  token minting, provider observation, artifact fetch, or Photon upload. This
  preserves the worker deadline and avoids an ambiguous physical action. Only
  already prepared `ready` and `delivering` state may be reconciled there until
  the durable result-reference design is released.
- It can observe only a durably known submitted request; it never resubmits an
  ambiguous request.
- It checks the registry's private/signed result allowlist before fetching an
  artifact. Bare storage URLs and malformed signatures are rejected before
  fetch; provider-private artifacts need a dedicated authenticated retriever.
- It prepares one Photon attachment, then atomically persists the text
  preamble, attachment outbox intent, cursor, observation success, and
  `executing → ready` transition. The preamble is dispatched before the
  attachment, and an attachment cannot leave after a durable preamble failure
  survives a worker restart.
- A crashed or ambiguous attachment upload becomes `upload_unknown` and
  `reconciliation_required`, without a GMI fetch or Photon re-upload.
- A transport ambiguity becomes `send_unknown`; it is reconciled, not replayed
  automatically.

For a rollback or provider safety incident:

1. Stop new admission and provider work through an explicitly reviewed higher
   release generation/fence transition before moving a Vercel alias.
2. Preserve encrypted Redis state; do not delete records or rotate secrets
   blindly. The only exception is the exposed outbound Photon credential after
   its explicitly authorized coordinated rotation.
3. Reconcile only established evidence through the controlled operational path.
   Do not make a new provider or Photon request to “see whether it worked.”
4. Use Zap only for legacy accepted-job drain/rollback support, never to accept
   a new Air request or replace the direct runtime.

## Handoff checklist

Record the following without secrets or personal data:

- direct branch commit and Vercel deployment ID;
- public health response/version;
- GMI audit result fingerprints and reviewed parameter summary (if run);
- account price evidence reference and the private-output contract review
  status;
- release-fence generation/phase (not its secret);
- Photon webhook target check, outbound-credential incident/rotation status,
  and handset `/help` outcome;
- any reconciliation-required job count/state, with opaque IDs only.
