# Air delivery-only iMessage release runbook

This procedure makes `air.wzrd.tech` available for one allowlisted, text-only
iMessage smoke test. It does not authorize image/video generation, provider
submission/recovery, uploads, polling, artifact delivery, or any paid action.
The owner sends the test text; release automation never sends an iMessage.

## Prior deployment baseline

Before a subsequent verified release, Vercel production deployment
`dpl_49byWqfH1UH9nwwsnVb6uuz6848F` from `dc4a8d6` is aliased to
`air.wzrd.tech`. Its public health endpoint returns Air Studio version 2, the
authenticated release fence is production generation 1, and the Vercel Cron
has recorded successful minute drains. This is not a Photon-webhook or
real-line acceptance record: the existing dashboard target and one owner-sent
`/help` reply still need to be verified before claiming the smoke test passed.
Record the new deployment identity in the release handoff after every
production deployment; do not treat this baseline as proof that later code is
live.

## Preconditions

1. Work from the existing Vercel project named `air`; do not create a new
   project or replace the existing Redis/Photon resources.
2. Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run build` from the
   exact revision being deployed.
3. Configure the existing signed-ingress and privacy fields in Vercel:
   `IMESSAGE_PROJECT_ID`, `IMESSAGE_PROJECT_SECRET`,
   `IMESSAGE_WEBHOOK_SECRET`, Redis credentials, `AIR_ID_HASH_SECRET`,
   `AIR_QUEUE_ENCRYPTION_KEY`, `AIR_ALLOWED_SENDER_HASHES`, `CRON_SECRET`, and
   `AIR_MODEL_REGISTRY_VERSION=2.1.0`.
4. Use this exact effective side-effect policy:

   ```text
   AIR_RELEASE_PHASE=production
   AIR_RELEASE_GENERATION=1
   AIR_RELEASE_FENCE_SECRET=<independent operator secret>
   AIR_ACCEPT_NEW_JOBS=false
   AIR_ALLOW_PROVIDER_SUBMIT=false
   AIR_ALLOW_PROVIDER_RECOVERY=false
   AIR_DELIVERY_ENABLED=true
   AIR_JOB_SPEND_CAP_USD=0
   AIR_DAILY_SPEND_CAP_USD=0
   AIR_CANARY_TOTAL_CAP_USD=0
   AIR_CANARY_FAL_CAP_USD=0
   ```

   Keep `GMI_API_KEY` absent. Do not enable a model or alter the zero-model
   registry. The runtime also treats either zero spend cap as a hard provider
   kill switch, so a provider call cannot be enabled by a policy flip alone.
5. Confirm `LOG_LEVEL` is unset or `silent`; do not configure OTLP exporters.
   The runtime fails closed if either could emit sensitive Photon metadata.

## Deploy and arm

1. Deploy the verified revision to the existing production Vercel project and
   verify `/healthz` at `https://air.wzrd.tech` reports Air Studio version 2.
2. Check protected `/internal/readiness`. Before the shared fence is advanced,
   a send path must not be treated as live.
3. Read the current secret-free fence epoch with authenticated
   `GET /internal/release-fence`, then explicitly create/advance it through
   `POST /internal/release-fence`, authenticated with
   `AIR_RELEASE_FENCE_SECRET` and the returned literal previous generation.
   A deployment or Cron invocation never arms the fence by itself.
4. Wait for Vercel's production minute Cron, then verify protected readiness
   reports a fresh Cron heartbeat, verified transport, zero enabled models,
   and no provider problem.
5. In the Photon dashboard, set the existing project's signed webhook target
   to `https://air.wzrd.tech/webhooks/imessage` with the existing matching
   signing secret. The Photon CLI has no webhook-management command.

## Owner smoke test

From the existing allowlisted iMessage number, send `/help` to Air. Confirm
one text response arrives. This may take one Cron interval after the signed
webhook receipt is accepted. Do not send an image, video, attachment, or a
generation command during this release.

## Stop and rollback

- If readiness is unhealthy, the webhook signature is invalid, a send becomes
  `send_unknown`, or any unexpected provider activity is observed, stop the
  smoke test. Do not resend automatically.
- Disable delivery with a new higher release generation and explicit shared
  fence transition before restoring the prior Vercel deployment/alias. A
  stale worker must never deliver an older generation's outbox item.
- Do not delete Redis records, rotate secrets blindly, or create a replacement
  Photon/Vercel project as a rollback shortcut.

## Future provider work remains prohibited

Any later image/video capability requires a separate explicit approval,
authenticated catalog/schema/pricing evidence, a bounded budget, provider
release fencing, and a real-media delivery review. None of those gates are
part of this text-only release.
