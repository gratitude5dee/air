/**
 * Spectrum Cloud's logger may be configured process-wide. Air handles
 * encrypted-but-sensitive iMessage content and intentionally has no redacting
 * telemetry boundary, so a non-silent logger or OTLP exporter is a startup
 * error rather than a configuration preference.
 *
 * This module deliberately has no Photon/Spectrum imports. Runtime code can
 * run this check before dynamically importing a provider package whose module
 * initialization might otherwise observe the environment.
 */
export function assertNoPhotonTelemetryExporters(
  environment: Readonly<Record<string, string | undefined>> = process.env,
): void {
  const unsafe = Object.entries(environment).some(([name, configured]) => {
    const value = configured?.trim().toLowerCase();
    if (!value) return false;
    if (name === "LOG_LEVEL") return value !== "silent";
    if (name === "OTEL_TRACES_EXPORTER" || name === "OTEL_LOGS_EXPORTER" || name === "OTEL_METRICS_EXPORTER") {
      return value !== "none";
    }
    return name.startsWith("OTEL_EXPORTER_OTLP_");
  });
  if (unsafe) throw new Error("Photon telemetry exporters are not permitted for Air");
}
