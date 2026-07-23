# Air direct production runbook

This runbook covers the direct Vercel deployment at `air.wzrd.tech`. It keeps
the current release private, fail-closed, and non-billable while the GMI model
contract and real iMessage path are proved. It does not authorize secret
retrieval, a provider submission, or a generation command.

## Current handoff

- Source of truth: `codex/air-direct-studio`. Zap is retained only to drain or
  roll back legacy jobs that it already accepted; it is not a new-work path.
- Live deployment: `dpl_5ihyMswovHd4Aymcd6tJ5XtPTeTq`, aliased to
  [`https://air.wzrd.tech`](https://air.wzrd.tech).
- Verified public probe: `GET /healthz` returned `{"ok":true,"version":"2"}`.
- Vercel has encrypted `GMI_API_KEY` configuration for Preview and Production.
  Only the environment-variable name/status was inspected; no value was read.
- The committed registry still has zero enabled models. The direct provider is
  therefore unprovisioned at runtime, even with the server-only key present.
- No real Photon webhook delivery or allowlisted handset `/help` response has
  been observed for this deployment. Do not mark the iMessage path accepted
  until that test is completed.

The existing shared `$100` cap is a hard background safety control. It does
not add a per-request approval step, and it does not substitute for the model
and privacy gates below.

## Non-negotiable operator rules

1. Do not retrieve, print, commit, paste, or send any secret. Inspect Vercel
   environment variables by name/status only; never use an environment pull
   as a shortcut.
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

## Protected GMI catalog/schema audit

The GMI key may be used only through the deployed read-only audit until a
separate activation release is approved. From an operator-controlled terminal
where `CRON_SECRET` is already set, run:

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
- `503` means stop and inspect deployment configuration or provider
  availability through the normal private operator path; do not broaden the
  route or add logging.
- `available: false`, a mismatched model ID, or an unexpected parameter schema
  means `/imagine` stays disabled.
- `pricing: "manual_console_required"` means obtain the exact account-specific
  price from GMI Console/Model Hub. Public docs and third-party price pages are
  not enough.

This audit does not prove result privacy. Before activation, record the exact
private/signed result host and non-root path prefix for this model and proof
that the result is not publicly accessible.

## Photon webhook and handset `/help` smoke

This remains the missing live acceptance test.

1. In the existing Photon project, confirm the signed webhook target is
   `https://air.wzrd.tech/webhooks/imessage` and it uses the already-matching
   signing secret. Do not create a replacement project, rotate secrets, or
   expose a line number while checking.
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

1. Preserve the protected audit response showing exact
   `flux-kontext-pro` availability and its reviewed parameter schema.
2. Record the exact account-specific GMI price and an upper bound for the
   single-image request. Keep the already-approved shared `$100` ceiling in
   force; it is a ceiling, not a license to substitute a model.
3. Prove the exact input and result contracts. Result retrieval must be private
   or signed, non-public, and pinned to a specific host plus non-root path
   prefix. A bare storage URL is insufficient even if a provider returns
   `is_public: false`; Air currently accepts only a complete GCS V4 signed URL
   at its generic retrieval boundary. Do not infer the contract from a generic
   documentation example.
4. Commit and review a registry entry that pins the exact GMI adapter, model
   ID, request/result schema fingerprints, required payload fields, input
   policy, output contract, price quote, polling bounds, and private retrieval
   policy. Run the full release verification suite and deploy it.
5. Complete the handset `/help` smoke above.
6. In a separately reviewed release, advance the shared release fence with its
   authenticated compare-and-swap and explicitly configure admission,
   provider-submit, provider-recovery, and delivery policies alongside
   positive bounded spend caps. A flag or key alone must still fail closed.
7. Submit only the reviewed one-image `/imagine` canary and observe its direct
   lifecycle without copying a provider URL or media into logs.

`/animate`, `/edit-image`, and `/edit-video` remain disabled after the first
`/imagine` release. Each needs its own exact model, media-input, pricing, and
private-output review. In particular, a GMI upload response documented as a
public URL is not an acceptable input-media transport for this private studio.

## Result delivery and incident response

The direct lifecycle is intentionally conservative once a reviewed model is
enabled:

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
   blindly.
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
- Photon webhook target check and handset `/help` outcome;
- any reconciliation-required job count/state, with opaque IDs only.
