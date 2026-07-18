# Dark preview evidence — 2026-07-18

This record describes the non-billable Air dark preview only. It is not a
canary, production approval, provider test, or real-line iMessage acceptance
record.

## Deployed revision

- Existing Vercel project: `air` (`prj_oVVN8AruxpRajmJDO92xdVj7cFWY`) in the
  existing `5-Dee Studios` team.
- Git revision: `76c53992fddd4c606dc53a6c038415f0ce027ce8`
  (`feat: verify Redis and Cron readiness`).
- Preview deployment: `dpl_32Ky5yeXu4TNMtZV5MDiPwsy3WaZ`, Ready, Git-sourced,
  branch `codex/air-direct-studio`, target `null` (not production).
- Preview URL: `https://air-hvvq5jd5j-5dee-studios.vercel.app`.
- Vercel errors-only build log ended with `Build Completed`; local checks at
  this revision passed `npm run check` (85 tests) and `npm run build`.

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
- The currently promoted production deployment remains
  `dpl_ozYZFaMMGsbGYPCSbMjbmUwqLMLY` on the pre-studio baseline. It was not
  modified by this preview deployment.

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
