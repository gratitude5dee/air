# Global release fence — implemented, not an activation approval

This record applies to future canary or production enablement. It does not
change the current dark preview, where provider submission and iMessage
delivery remain disabled.

## What the current scoped guard covers

Each `{project, sender, chat, sending line}` scope now has an opaque,
non-expiring receipt-order counter plus a versioned pending control record.
Every pending write, `/cancel`, and dark maintenance transition compares that
durable source sequence. The encrypted payload has an immutable absolute
expiry; retries return the existing guard and cannot refresh its ten-minute
retention. A guarded media acknowledgement validates the same control record
at publication, queue claim, and immediately before physical delivery.

That prevents A → B → retry-A overwrites, stale retries after `/cancel`, and a
known dark/rollback receipt later sending a false “attachment saved” claim.
The control record stores only opaque receipt IDs, guard IDs, counters, and
expiry metadata; attachment references remain encrypted and short-lived.

## What the global outbox fence now covers

`POST /internal/release-fence` is an independent-secret, literal
compare-and-swap operation over one non-expiring opaque Redis record. It is
never called by runtime construction, webhook handling, or Cron. The initial
write can create only dark generation `0`; later writes can advance by exactly
one generation. Repeating the same successful transition is idempotent, while
stale or malformed preconditions fail closed.

Every Studio outbox record carries the configured `{ generation, phase }`.
The store refuses publication when it does not match the current fence, then
checks persisted records at queue claim and immediately before the physical
transport call. A known stale persisted record terminalizes as `canceled` and
removes its encrypted payload; a missing or corrupt active fence releases the
lease and pauses before a physical side effect. This covers generic onboarding,
help, status, maintenance, and pending-assembly messages, not only media
acknowledgements.

## What it deliberately does not claim to solve

- A Vercel deployment still does not write the shared Redis release epoch. An
  operator must explicitly invoke the protected route on the intended artifact
  under a separately approved rollout/rollback plan.
- Redis cannot atomically revoke bytes already committed to a third-party
  iMessage transport. The final guarded authorization is the documented
  commitment point; a stricter live policy needs a per-thread send barrier and
  a transport-aware rollback procedure.
- The repository now has an unprovisioned, fake-tested provider-submission
  foundation. It persists one encrypted admission snapshot and durable
  `reserved → submitting` intent at a final durable pre-call boundary, then
  fences that transition against the same shared release generation and current
  `executing` job version. A rollback or cancellation that happens after this
  Redis authorization cannot atomically revoke an external provider call and
  must reconcile conservatively. It deliberately has no live adapter, catalog,
  upload, polling, output, or recovery path, and no ingress/Cron path invokes
  it. It would still be inaccurate to claim a production-ready provider
  integration from this dark-only foundation.

## Required before canary

Use the global fence only after an explicit owner approval, a deployment
sequence that accounts for in-flight invocations, and a bounded real-line
rollback drill. Before provider enablement, add and test a matching fence for
each provider submission/recovery intent. Until transport evidence, provider
evidence, and that drill exist, do not enable `AIR_DELIVERY_ENABLED`, accept
paid work, or treat this branch as canary-ready.
