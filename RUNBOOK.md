# Air dark-deployment runbook

This runbook is deliberately narrow. It prepares a non-billable preview in the
existing Vercel project; it does not authorize an iMessage webhook change,
provider call, canary, production promotion, or creation of infrastructure.

## Before deployment

1. Work from the dedicated Git branch and confirm the project remains the
   existing Vercel project named `air`.
2. Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run build`.
3. Configure only the existing Redis and Photon secrets needed for an
   encrypted, signed dark shell. Keep `GMI_API_KEY` absent.
4. Set all release switches explicitly to `false` and set
   `AIR_RELEASE_PHASE=dark`. Set `AIR_MODEL_REGISTRY_VERSION=2.1.0`.
5. Do not set an upstream Photon/Spectrum webhook URL. The webhook endpoint
   exists for a later signed-fixture test but must not receive real traffic in
   this phase.

## Preview verification

Deploy a preview from the existing Vercel project. Check only non-billable
surfaces:

- `GET /` returns the minimal Air status with `cache-control: no-store`.
- `GET /healthz` returns `{ "ok": true, "version": "2" }`.
- An unauthenticated `GET` request to either internal path returns `401`
  without a body containing configuration details. Readiness is GET-only;
  drain also accepts POST for manual recovery.
- An authorized request to `/internal/readiness` shows `transport_unverified`
  and `no_enabled_models`; that is the expected dark state.

Do not use an actual GMI key, make a model catalog call, or send an iMessage
while doing these checks. A signed webhook fixture is a later gate and must use
non-production credentials and a deliberate owner approval.

## Rollback

The dark shell has no provider or delivery side effects. If the preview is
unhealthy, leave it unpromoted and return traffic to the currently promoted
deployment. Do not delete Redis records or alter the existing iMessage project
as a rollback shortcut.

## Canary and production prerequisites

Do not change the release phase or kill switches until separate Fable approval
records a bounded budget and all of the following evidence:

1. An authenticated GMI catalog/detail snapshot validates the exact selected
   model, request schema, input hosting rule, output contract, and current
   price.
2. The registry contains only that verified enabled model and an exact default;
   the version is committed and matches `AIR_MODEL_REGISTRY_VERSION`.
3. Provider fixture tests prove that `429`, `5xx`, timeout, malformed success,
   and ambiguous delivery never resubmit automatically.
4. A real-line Photon transport spike proves sending, attachment fetch, reply
   threading, typing, and explicit `send_unknown` handling on an allowlisted
   iPhone.
5. A limited canary has explicit per-job, daily, and total caps with an
   approved budget identifier. Production requires another explicit approval.
