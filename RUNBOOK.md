# Air dark-deployment runbook

This runbook is deliberately narrow. It prepares a dark preview in the
existing Vercel project; it does not authorize an iMessage webhook change,
provider call, iMessage delivery, canary, production promotion, or creation
of infrastructure.

## Before deployment

1. Work from the dedicated Git branch and confirm the project remains the
   existing Vercel project named `air`.
2. Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run build`.
3. Configure only the existing Redis and Photon secrets needed for an
   encrypted, signed dark shell. Keep `GMI_API_KEY` absent.
4. Set all release switches explicitly to `false`, set
   `AIR_RELEASE_PHASE=dark`, and set `AIR_RELEASE_GENERATION=0`. Set
   `AIR_MODEL_REGISTRY_VERSION=2.1.0`; leave `AIR_RELEASE_FENCE_SECRET` absent
   unless a separately approved operator fence exercise is in scope.
5. Do not set an upstream Photon/Spectrum webhook URL. The webhook endpoint
   exists for a later signed-fixture test but must not receive real traffic in
   this phase.

## Preview verification

Deploy a preview from the existing Vercel project. Check only non-billable
surfaces:

- `GET /` returns the minimal Air status with `cache-control: no-store`.
- `GET /healthz` returns `{ "ok": true, "version": "2" }`.
- An unauthenticated request to `/internal/drain-inbox` or
  `/internal/readiness` returns `401` without a body containing configuration
  details. Readiness is GET-only; drain also accepts POST for manual recovery.
- `POST /internal/release-fence` is not part of preview verification. It
  requires its own bearer secret and a strict `If-Match` compare-and-swap
  generation; an unauthenticated POST returns `401`, while a GET is
  deliberately `405 Allow: POST`. Do not arm or advance it during this dark
  preview.
- An authorized request to `/internal/readiness` shows `transport_unverified`
  and `no_enabled_models`; that is the expected dark state. With Redis
  configured, preview readiness reports `redis: "reachable"` and
  `cron: "not_scheduled"`: Vercel only runs configured Cron jobs on a
  production deployment.
- A successful authenticated `GET /internal/drain-inbox` records a
  environment-scoped Redis heartbeat after the drain completes. Manual `POST`
  recovery never updates it. On production, readiness distinguishes a missing
  or invalid marker from `cron: "stale"` after three missed minute beats; a
  bearer-authenticated GET is still not a substitute for verifying Vercel's
  scheduler behavior in deployment logs.

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
6. An authenticated shared Redis release-generation fence is deployed and
   operated with a written rollback procedure. The current implementation
   stamps every Studio outbox intent and rejects an older generation at
   creation, claim, and final dispatch; no deployment or Cron run advances the
   fence automatically. The provider executor must implement the equivalent
   per-intent fence before it can be enabled, and the complete procedure must
   pass a bounded real-line rollback drill. A per-conversation pending fence is
   not a substitute for this global protection of onboarding, help, status,
   and maintenance responses.
