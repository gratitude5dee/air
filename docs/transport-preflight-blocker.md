# iMessage transport preflight — blocked for review

This is a deliberate stop record required by the Fable v2.1 Section 8.2
transport preflight. It does not authorize a real line, SDK install, webhook
change, or iMessage send.

## Observed at `76c53992fddd4c606dc53a6c038415f0ce027ce8`

- The current direct-studio manifest contains only Redis and validation
  dependencies; it does not include a Spectrum/Photon iMessage SDK or the
  withdrawn legacy `@photon-ai/chat-adapter-imessage` package.
- `StudioRuntime` intentionally constructs `UnverifiedPhotonTransport`. Its
  physical send, tapback, attachment-fetch, and reply operations fail closed.
- The branch retains encrypted `chatGuid`, sending line, and attachment GUID
  references, but has no SDK-proven way to rehydrate a space/message, fetch
  bytes, start/stop typing, resolve a native reply, send a local Buffer, or
  prove shared-pool pacing/idempotency behavior.
- No Photon credential, iMessage webhook, or real device was contacted during
  this inspection.

## Why implementation stops here

The legacy production baseline previously declared Spectrum 10 packages and a
withdrawn adapter, while the direct-studio branch has neither. That is a
material mismatch with the Fable transport assumption. Inventing a replacement
API from memory would risk duplicate sends, incorrect attachment semantics, or
an unsafe webhook path.

## Required next evidence

1. The owner must identify or approve the exact current Photon/Spectrum SDK
   package and version to inspect locally.
2. An offline adapter conformance spike must freeze the actual API surface for
   space/message rehydration, typing deadlines, reply fallback, GUID fetch,
   Buffer/stream delivery, tapback semantics, idempotency, and size/pacing
   limits.
3. A separately approved, bounded real-line/iPhone test must prove those facts
   before `transport.verified` or `AIR_DELIVERY_ENABLED` can ever be true.

Until then, the dark transport is intentional and all physical delivery
remains disabled.
