# Air → Zap Seedance iMessage Video Goal

## Mission

Repurpose `gratitude5dee/air` from MusicRAG into a private, allowlisted
iMessage video-generation service at `https://air.wzrd.tech`.

Air owns only signed Photon/Spectrum ingress, an encrypted Redis inbox and
outbox, ordering, deduplication, recovery, and conservative delivery. Zap at
`https://zap.wzrd.tech` owns GMI credentials, Seedance requests and polling,
cost controls, run records, and temporary video assets.

Air must not use MongoDB, Atlas, Voyage, Vercel AI Gateway, RAG, citations, or
an Eve LLM lifecycle. Eve/Nitro remain only as the route and schedule host.

## Production Resources

- Use the existing `zap-production` Upstash Redis database, with isolated
  `air:video:v1:*` and `zap:service:air:v1:*` key namespaces.
- Never put media bytes, raw prompts, raw sender IDs, GUIDs, run IDs, or media
  URLs into unencrypted Redis records.
- Use the existing Vercel projects and production domains:
  `air.wzrd.tech` and `zap.wzrd.tech`.
- Persist generated MP4s in Zap's Vercel Blob store for 24 hours, then delete
  them via the cleanup queue.

## Zap Service

Create and maintain a private, single-step `air-imessage-video` service using
the fixed launch preset:

| Setting | Value |
| --- | --- |
| Model | `seedance-2-0-fast-260128` |
| Duration | 5 seconds |
| Resolution | 720p |
| Ratio | `adaptive` |
| Audio | enabled |
| Watermark | disabled |
| Web search | disabled |
| Seed | random |
| Optional image | `first_frame` |

GMI integration must:

1. Submit `{ model, payload }` to
   `POST /api/v1/ie/requestqueue/apikey/requests`.
2. Poll `GET /api/v1/ie/requestqueue/apikey/requests/{request_id}`.
3. Map `queued`, `processing`, `success`, `failed`, and `cancelled` safely.
4. Use the provider output only after HTTPS-host, MIME, timeout, and 25 MB
   validation. `storage.googleapis.com` is an explicit provider-output host.
5. Use GMI's presigned upload flow for first-frame images.
6. Never require `GMI_ORG_ID`.

Expose only authenticated service endpoints using
`Authorization: Bearer ZAP_AIR_SERVICE_TOKEN`:

- `POST /api/services/air/v1/uploads` — validates image MIME/size and returns
  a GMI upload URL and public URL.
- `POST /api/services/air/v1/runs` — accepts `{ prompt, firstFrameUrl? }` and
  `Idempotency-Key`, then returns `202 { runId, status }`.
- `GET /api/services/air/v1/runs/{runId}` — returns sanitized status, progress,
  failures, and completed video metadata only.

Derive each run ID from an HMAC of Air's idempotency key. Make Convex records
and Upstash reservations idempotent. If Zap crashes after GMI accepts a request
but before it records the request ID, enter `submission_unknown`; never submit
again automatically. Use Zap's durable provider poll queue and two-minute
Convex cron, with a 60-minute maximum poll window.

Store only redacted prompt/image metadata in the ledger. Fail closed unless the
current authenticated GMI account provides a verified 720p fast-model price.
Require explicit per-run and daily USD caps before enabling production spend.

## Air iMessage Flow

Normalize signed Spectrum webhook content recursively, including text, groups,
and replies. Keep attachment GUID, sending line phone, filename, MIME type, and
size only inside the encrypted envelope.

Accept direct-message requests only from hashed allowlisted senders:

- Text is required and must be 1–2,000 characters.
- Allow a plain-text prompt or one captioned image in the same iMessage.
- Accept JPEG/PNG directly and convert HEIC/HEIF to JPEG before upload.
- Reject image-only messages, multiple attachments, non-image files, and files
  over 10 MB before paid work.
- Fetch the attachment by Spectrum GUID plus the receiving line and bound its
  actual stream bytes. PUT it directly to Zap's GMI upload URL. Do not store
  input image bytes in Redis or Vercel Blob.

Use the durable state machine:

`received → validated → image_uploading? → zap_submitting → queued/running → video_ready → sending → sent`

Terminal/recovery states include `failed_retryable`, `submission_unknown`,
`send_unknown`, and `dead_letter`.

After Zap has durably accepted a run, send exactly one tracked “Generating your
video…” acknowledgement. The minute recovery drain resumes pending work.

For final delivery:

1. Download and validate a completed MP4 over HTTPS.
2. Send an attachment-only `video/mp4` message.
3. If it is deterministically too large or rejected before an ambiguous send,
   send its 24-hour Blob URL instead.
4. Preserve `send_unknown` for ambiguous Photon delivery and never blindly
   retry an uncertain send.

Enforce DM-only access, hashed sender IDs, one active generation per sender,
three accepted/completed requests per rolling day, and a global Zap concurrency
limit of two. Delete encrypted prompts, GUIDs, run IDs, and URLs after delivery
or terminal dead-lettering; retain redacted status metadata for seven days.

## Required Configuration

Zap Production requires:

- `GMI_API_KEY`
- `GMI_SEEDANCE_FAST_USD_PER_SECOND`
- `ZAP_AIR_DAILY_CAP_USD`
- `ZAP_AIR_MAX_RUN_USD`
- `ZAP_AIR_SERVICE_TOKEN`
- `ZAP_AIR_IDEMPOTENCY_HMAC_SECRET`
- `ZAP_AIR_UPLOAD_TICKET_ENCRYPTION_KEY`
- `REDIS_URL`, `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`
- `BLOB_READ_WRITE_TOKEN`
- existing Convex and Zap poll configuration

Air Production requires:

- `IMESSAGE_PROJECT_ID`, `IMESSAGE_PROJECT_SECRET`,
  `IMESSAGE_WEBHOOK_SECRET`
- `AIR_ALLOWED_SENDER_HASHES`
- `AIR_ID_HASH_SECRET`, `AIR_QUEUE_ENCRYPTION_KEY`, `CRON_SECRET`
- `REDIS_URL` plus `UPSTASH_REDIS_REST_URL` and
  `UPSTASH_REDIS_REST_TOKEN` for the serverless HTTPS Redis transport
- `ZAP_AIR_SERVICE_URL=https://zap.wzrd.tech`
- matching `ZAP_AIR_SERVICE_TOKEN`
- `AIR_PUBLIC_BASE_URL=https://air.wzrd.tech`

## Deployment and Rollout

1. Keep the Air and Zap changes on their dedicated GitHub branches and pass
   package, unit, integration, normal-build, and Vercel-build checks.
2. Deploy and verify Zap preview and production at `zap.wzrd.tech`.
3. Configure GMI, Blob, Upstash, Convex, service token, verified price, and
   spending caps. Run one authenticated paid Zap production smoke test at the
   fixed preset; verify Blob persistence and recovery.
4. Deploy and verify Air at `air.wzrd.tech`, including `/healthz` and signed
   fixture ingress. Configure encryption, allowlist, Photon project credentials,
   Redis, Zap URL, and cron secret.
5. **Last**, set the Photon webhook to
   `https://air.wzrd.tech/webhooks/imessage`; retain its signing secret only in
   Photon and Vercel Production.
6. Roll back by disabling the Photon webhook or promoting the previous Air
   deployment. The additive Zap service can remain deployed without affecting
   other Zap users.

## Acceptance

- Exact GMI submit/poll/upload wire formats, output extraction, timeout,
  malformed responses, and pricing refusal are tested.
- Signed text and captioned-image ingress, nested reply/group parsing, HEIC
  conversion, size limits, duplicate delivery, and rejection-without-spend are
  tested.
- Concurrent retries result in exactly one Zap run and one GMI submission.
- Redis ciphertext contains no raw prompt, sender, attachment GUID, Zap run ID,
  or media URL.
- Queued/running/completed/failed/cancelled/timeout/recovery/
  `submission_unknown`/oversize fallback/ambiguous delivery paths are tested.
- Unauthorized, group, duplicate, and rate-limited messages never reach
  Zap/GMI.
- Production health, runtime logs, preview fixtures, builds, and typechecks
  pass with secret-safe logging.
- Real-device acceptance from an allowlisted iPhone proves:
  - text prompt → acknowledgement + playable MP4 with audio;
  - captioned photo → image-conditioned video;
  - duplicate webhook → one charge + one final video;
  - concurrent request → refusal/defer without extra spend;
  - recovery works after cold start/interrupted worker; and
  - a non-allowlisted sender triggers no generation or spend.

Production activation remains blocked until valid GMI credentials, verified
fast-model pricing, explicit spend caps, Photon webhook configuration, an
allowlisted sender, and real-device acceptance are available.
