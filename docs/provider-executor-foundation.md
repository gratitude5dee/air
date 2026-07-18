# Provider submission foundation — dark-only evidence

This is a zero-spend implementation record. It does not authorize a model,
provider credential, catalog lookup, upload, generation request, artifact
download, or iMessage delivery.

## What is implemented

The direct-studio branch has a narrow injected `StudioProviderPort` and a
single-subrun executor that can operate only in tests with a fake provider.
The deployed runtime always constructs `UnprovisionedStudioProvider` with
`verified: false`; no webhook, Cron, route, or inbox drain imports the
executor.

For an already admitted job whose encrypted payload contains a reviewed
workflow/model snapshot, the executor:

1. Requires an active release phase, explicit provider-submit policy, and a
   verified injected port before it reads or claims work.
2. Transitions the job to `executing`, creates one HMAC-derived subrun, and
   takes a token-checked Redis lease.
3. Atomically persists `submitting` plus its stable opaque submission intent
   while checking the shared release generation and current `executing` job
   version at the final durable pre-call boundary. A cancellation that wins
   before this point prevents the call; a change after it must be reconciled,
   because Redis cannot atomically revoke a third-party request.
4. Stores an accepted provider request ID only in a separately encrypted
   reference record whose TTL is copied from the source job payload's Redis
   `PTTL` inside the same completion transaction. Work too near that deadline
   does not start; a late accepted response becomes an unknown submission
   without retaining its request ID.
5. Converts a definitive rejection to a known terminal state. Any thrown,
   timed-out, lost-lease, or recovered-in-flight outcome becomes
   `submission_unknown` / job `reconciliation_required`; it is never blindly
   resubmitted. A cancellation that races a provider acceptance can escalate
   from `canceled` to this audit-only state, but never restores its scrubbed
   payload, active-job marker, or delivery eligibility.

The in-memory Redis test double mirrors both new Lua transitions. Current test
coverage includes dark no-op behavior, intent-before-submit ordering, accepted
idempotency, definitive rejection, ambiguous outcomes, a crash after intent,
release rollback before submit, cancellation that wins before the pre-call
boundary, cancellation and rollback after durable intent, source-payload TTL
loss at completion, cancellation observed after provider completion, capped
provider-reference retention, and expired or expiring encrypted job snapshots.

## What is intentionally absent

- The committed registry still has zero enabled models, and creation commands
  do not admit a job into this executor.
- No runtime constructs `GmiRequestQueueClient`, calls `fetch`, or contains a
  fal/GMI provider adapter.
- There is no catalog/schema/pricing approval, provider submission/recovery
  ledger, cancellation adapter, polling, artifact validation, output cursor,
  or delivery path.
- Photon/Spectrum transport remains `UnverifiedPhotonTransport`; no actual
  iMessage attachment fetch, reply, tapback, typing, or send is possible.

## Required before activation

An owner-approved model registry, positive budget and release approval,
provider-specific adapter conformance test, reservation/reconciliation ledger,
media/artifact pipeline, transport spike on a real test line, and rollback
drill are all still required. This foundation narrows the future integration
surface; it does not relax any of those gates.
