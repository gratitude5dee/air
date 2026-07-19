# Dark preview evidence — 2026-07-18

This record describes the non-billable Air dark preview only. It is not a
canary, production approval, provider test, or real-line iMessage acceptance
record. The later delivery-only production deployment is recorded separately
below so this historical preview evidence is not mistaken for current state.

## Deployed revision

- Existing Vercel project: `air` (`prj_oVVN8AruxpRajmJDO92xdVj7cFWY`) in the
  existing `5-Dee Studios` team.
- Implementation revision: `f8153b7e5e6150d4d728e4a6e7613296d9c07a00`
  (`feat: add fenced reconciliation primitives`).
- Preview deployment: `dpl_Ec8cEciamCFYBmZhczHPmEk7xRt4`, Ready, Git-sourced,
  branch `codex/air-direct-studio`, preview target (not production).
- Preview URL: `https://air-n2vomk3hl-5dee-studios.vercel.app`.
- Stable branch alias:
  `https://air-git-codex-air-direct-studio-5dee-studios.vercel.app`.
- Vercel metadata verified the exact Git SHA, private-project visibility, and
  a Ready build with the six expected serverless entrypoints. Local checks at
  this revision passed `npm run check` (137 tests) and `npm run build`.

## Isolation and verification boundary

- The deployed code remains `AIR_RELEASE_PHASE=dark` by default and has zero
  enabled models. It cannot submit a provider job or send an iMessage without
  a later explicit phase and switch change.
- A names-only Vercel environment inventory found protected configuration
  entries targeted to `production` only, not `preview`. No values were read.
  Therefore this preview is a build-only dark shell: it cannot authenticate
  the internal operator routes or prove live Redis/Cron reachability.
- Vercel SSO protected the preview URL during automated fetches, returning
  `302` before the application routes; no protected endpoint, webhook, or
  provider endpoint was invoked.
- At the time of this preview, production remained
  `dpl_ozYZFaMMGsbGYPCSbMjbmUwqLMLY` on the pre-studio baseline. That historical
  state was superseded by the delivery-only production release below.

## Readiness contract added in this revision

`GET /internal/readiness` now probes only the fixed non-tenant key
`air:v2:operations:cron-heartbeat:<environment>` in the existing Redis
namespace. It returns coarse Redis/Cron states and effective side-effect
states without configuration values or user data. A successful authenticated
`GET /internal/drain-inbox` writes the heartbeat only after the drain finishes;
a manual `POST` never writes it.

The marker demonstrates an authorized GET drain, not cryptographic proof that
Vercel scheduled it. Production Cron logs, entitlement, header behavior, and
a controlled protected-readiness test remain required evidence before any
enablement decision.

## Subsequent delivery-only production release — 2026-07-18

- Production deployment:
  `dpl_49byWqfH1UH9nwwsnVb6uuz6848F`, Ready, from source revision `dc4a8d6`
  (`feat: expose authenticated release fence status`).
- The deployment aliases include `https://air.wzrd.tech`; the public health
  check returns `200 {"ok":true,"version":"2"}` and the public status is the
  private allowlisted Air Studio response.
- The authenticated shared release-fence read confirmed production generation
  1. No secret or user data was recorded.
- Vercel's production Cron invoked `GET /internal/drain-inbox` successfully at
  one-minute cadence, returning `202` in the recorded production logs.
- A names-only production environment audit found the delivery, policy, and
  spend controls and no `GMI_API_KEY`, `FAL_KEY`, or `FAL_*` name. Encrypted
  values were not read.

This is still a zero-spend, zero-model, text-only release. It does not prove
the Photon webhook target, a real allowlisted inbound receipt, outbound
iMessage delivery, any attachment behavior, or any provider capability.
