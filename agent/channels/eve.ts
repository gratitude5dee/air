/**
 * Air is iMessage-only. Eve normally mounts its browser/session API at
 * `/eve/v1/*`; disabling that framework channel prevents an alternate web
 * conversation surface from bypassing the durable iMessage ingress.
 *
 * Eve 0.24.3 implements `disableRoute()` with this documented sentinel but
 * does not re-export that helper from a public `eve/channels/*` entrypoint.
 * Keep the shape intentionally minimal so the compiler recognizes it before
 * it attempts to normalize a regular channel definition.
 */
export default {
  kind: "eve:disabled-channel",
} as const;
