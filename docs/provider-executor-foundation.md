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

## Reconciliation foundation

`StudioStore` now has zero-spend, version-fenced reconciliation primitives for
the ambiguous boundaries. They do not call a provider, look up a provider
request, send iMessage, or expose an HTTP operator route.

- A `submission_unknown` subrun can receive a verified provider request ID or
  be terminalized with a bounded reason. Both operations compare the current
  job and subrun versions, bind the stored subrun ID to the requested key, and
  append an opaque HMAC-derived actor audit record. Attaching evidence resumes
  only observation-ready `executing`; it cannot submit again.
- A `send_unknown` outbox can be resolved as sent or confirmed absent under an
  exact outbox version. Confirmed absence remains durable and keeps its lane
  blocked until an explicit outcome is known.
- Requeueing a confirmed absence creates one new encrypted outbox record only
  while the source payload remains inside its original Redis TTL. The source
  becomes terminal `requeued`, its payload is scrubbed, and the replacement
  atomically inherits the original lane position—even for isolated response,
  tapback, and job lanes. A media-first replacement also retains the original
  pending guard, so a later cancellation, supersession, or expiry still blocks
  it before dispatch.
- If the private payload has expired before (or during) a confirmed-absent
  requeue, the source terminalizes as `failed` with
  `terminalReason: artifact_expired`. Air does not refetch, regenerate, or
  silently reopen it.

Reconciliation metadata and audit records are content-free and non-expiring
while reconciliation is outstanding. Encrypted job/provider/outbox references
still retain their independent 24-hour privacy deadline. Audit keys are
write-once; a collision or stale version fails closed before a state mutation.

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
- There is no authenticated reconciliation command surface yet. Binding these
  primitives to an operator endpoint requires a separately authorized
  responder/authentication design and a recovery drill; it must not be added
  speculatively to the dark preview.

## Required before activation

An owner-approved model registry, positive budget and release approval,
provider-specific adapter conformance test, reservation/reconciliation ledger,
media/artifact pipeline, transport spike on a real test line, and rollback
drill are all still required. This foundation narrows the future integration
surface; it does not relax any of those gates.
