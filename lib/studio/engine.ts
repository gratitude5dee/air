import { createHash } from "node:crypto";

import {
  parseStudioCommand,
  type StudioCommand,
  type StudioMode,
  type StudioOutputCount,
  type StudioVideoDurationSeconds,
} from "./commands.js";
import {
  AIR_MODEL_REGISTRY,
  checkModelCompatibility,
  listEnabledModels,
  resolveDefaultModel,
  resolveEnabledModel,
  type StudioModel,
} from "./model-registry.js";
import {
  isSafeTransportText,
  type IMessageThreadReference,
  type TransportOutcome,
} from "./imessage-transport.js";
import type { StudioRuntime } from "./runtime.js";
import type {
  StudioAttachmentReference,
  StudioInboxRecord,
  StudioInboxRecord as InboxRecord,
  StudioOutboxPayload,
  StudioOutboxRecord,
} from "./store.js";
import { getStudioWorkflow, isSinglePaidStepWorkflow } from "./workflows.js";

const ONBOARDING_VERSION = 1;
const DEFAULT_OUTPUT_COUNT: StudioOutputCount = 1;
const DEFAULT_VIDEO_DURATION_SECONDS: StudioVideoDurationSeconds = 5;
const MAX_INBOX_PER_DRAIN = 12;
const MAX_OUTBOX_PER_DRAIN = 16;
const TYPING_DWELL_MS = 300;

const HELP_TEXT = [
  "Air commands:",
  "/imagine <prompt>",
  "/animate <prompt>",
  "/edit-image <prompt>",
  "/edit-video <prompt>",
  "/models [mode] • /model-settings • /status • /cancel • /privacy",
].join("\n");

const ONBOARDING_BUBBLES = Object.freeze([
  "Hey — I’m Air by WZRD ⚡ Your image + video studio in iMessage.",
  "Create with /imagine, /animate, /edit-image, or /edit-video. Try: /imagine a chrome hummingbird at sunrise. Settings: /model-settings • Help: /help. Air keeps its own temporary media references for up to 24 hours; providers process media under their own retention terms.",
] as const);

type StoredModeSettings = {
  readonly modelSlug?: string | undefined;
  readonly outputCount: StudioOutputCount;
  readonly durationSeconds?: StudioVideoDurationSeconds | undefined;
};

export interface StudioUserSettings {
  readonly schemaVersion: 1;
  readonly onboardingVersion: number;
  readonly modes: Readonly<Record<StudioMode, StoredModeSettings>>;
}

type PendingIntent =
  | {
      readonly schemaVersion: 1;
      readonly kind: "media";
      readonly attachment: StudioAttachmentReference;
      readonly createdAtMs: number;
    }
  | {
      readonly schemaVersion: 1;
      readonly kind: "awaiting_prompt";
      readonly mode: StudioMode;
      readonly attachment?: StudioAttachmentReference | undefined;
      readonly createdAtMs: number;
    }
  | {
      readonly schemaVersion: 1;
      readonly kind: "awaiting_media";
      readonly mode: "edit-image" | "edit-video";
      readonly prompt: string;
      readonly expectedMedia: "image" | "video";
      readonly createdAtMs: number;
    };

type InboxDisposition = "processed" | "maintenance_rejected";

export interface StudioDrainStats {
  readonly inbox: number;
  readonly outbox: number;
}

/**
 * Authoritative bounded worker entrypoint. The minute Vercel Cron invokes it;
 * webhook-triggered work is only a latency hint. It is deliberately safe to
 * run more than once because receipts and outbox IDs are stable.
 */
export async function drainStudio(runtime: StudioRuntime): Promise<void> {
  await drainStudioWithStats(runtime);
}

export async function drainStudioWithStats(runtime: StudioRuntime): Promise<StudioDrainStats> {
  let inbox = 0;
  for (let index = 0; index < MAX_INBOX_PER_DRAIN; index += 1) {
    const claim = await runtime.store.claimNextInbox();
    if (!claim) break;

    try {
      const disposition = await processInbox(runtime, claim.record);
      // A stale worker may have lost its lease while it was processing. In
      // that case the CAS-backed store refuses this finish and the new owner
      // remains the only worker permitted to advance the receipt.
      if (await runtime.store.finishInbox(claim.record.receiptId, claim.leaseToken, disposition)) {
        inbox += 1;
      }
    } catch {
      // Do not mark a partially processed receipt terminal. Its durable
      // outbox IDs make a later fenced retry safe; store recovery reclaims the
      // expired lease. No error detail is logged because envelopes may carry
      // prompts, GUIDs, and identifiers.
      break;
    }
  }

  return Object.freeze({ inbox, outbox: await drainStudioOutbox(runtime) });
}

/**
 * Dispatches at most one physical action per outbox entry. In a dark release
 * it exits before claiming any intent, which preserves every durable intent
 * for the later verified transport/release gate.
 */
export async function drainStudioOutbox(runtime: StudioRuntime): Promise<number> {
  if (!runtime.environment.policy.deliveryEnabled || !runtime.transport.verified) return 0;

  let delivered = 0;
  for (let index = 0; index < MAX_OUTBOX_PER_DRAIN; index += 1) {
    const claim = await runtime.store.claimNextOutbox();
    if (!claim) break;

    const payload = runtime.store.decryptOutboxPayload(claim.record);
    const outcome = await dispatchOutboxEntry(runtime, claim.record, payload);
    const finished = await runtime.store.finishOutbox(claim.record.outboxId, claim.leaseToken, outcome.kind);
    if (finished) delivered += 1;

    // An uncertain send pauses all further dispatch in this invocation. The
    // store additionally fences the affected lane; this conservative global
    // pause is safer than accidentally passing a dependent attachment.
    if (outcome.kind === "send_unknown") break;
  }
  return delivered;
}

async function processInbox(runtime: StudioRuntime, record: StudioInboxRecord): Promise<InboxDisposition> {
  const envelope = runtime.store.decryptInboxEnvelope(record);
  const initialized = await runtime.store.initializeSettingsIfAbsent(
    record.senderHash,
    defaultSettings(ONBOARDING_VERSION),
  );
  let settings = normalizeSettings(initialized.value);

  if (initialized.created || settings.onboardingVersion < ONBOARDING_VERSION) {
    settings = Object.freeze({ ...settings, onboardingVersion: ONBOARDING_VERSION });
    await runtime.store.putSettings(record.senderHash, settings);
    await enqueueText(runtime, record, envelope, "onboarding-1", "onboarding", 1, ONBOARDING_BUBBLES[0]);
    await enqueueText(runtime, record, envelope, "onboarding-2", "onboarding", 2, ONBOARDING_BUBBLES[1]);
  }

  if (envelope.attachments.length > 1) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      "multiple-inputs",
      "Send exactly one image or video with a creation command. Air does not choose between multiple inputs.",
    );
    return "processed";
  }

  const attachment = envelope.attachments[0];
  if (envelope.text === null) {
    if (attachment) {
      await runtime.store.putPending<PendingIntent>(record.senderHash, record.threadHash, Object.freeze({
        schemaVersion: 1,
        kind: "media",
        attachment,
        createdAtMs: record.receivedAtMs,
      }));
      await enqueueResponse(
        runtime,
        record,
        envelope,
        "media-first",
        "I saved that one attachment for 10 minutes. Send an explicit command such as /animate a slow orbit, /edit-image brighten the sky, or /edit-video trim the opening.",
      );
      return "processed";
    }
    await enqueueResponse(runtime, record, envelope, "empty-message", HELP_TEXT);
    return "processed";
  }

  const parsed = parseStudioCommand(envelope.text);
  if (parsed.kind === "help") {
    await enqueueResponse(runtime, record, envelope, `help-${parsed.reason}`, HELP_TEXT);
    return "processed";
  }
  if (parsed.kind === "invalid") {
    await enqueueResponse(runtime, record, envelope, `invalid-${parsed.error.code}`, `${parsed.error.message}\n${HELP_TEXT}`);
    return "processed";
  }
  if (parsed.kind === "bare-text") {
    return handleBareText(runtime, record, envelope, settings, attachment, parsed.text);
  }

  return handleCommand(runtime, record, envelope, settings, attachment, parsed.command);
}

async function handleBareText(
  runtime: StudioRuntime,
  record: InboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  _settings: StudioUserSettings,
  attachment: StudioAttachmentReference | undefined,
  text: string,
): Promise<InboxDisposition> {
  if (attachment) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      "bare-text-with-media",
      "Use an explicit command with one attachment, for example /imagine a watercolor version or /animate a gentle camera move.",
    );
    return "processed";
  }
  const pending = await runtime.store.getPending<PendingIntent>(record.senderHash, record.threadHash);
  if (pending?.kind !== "awaiting_prompt") {
    await enqueueResponse(runtime, record, envelope, "bare-text-help", HELP_TEXT);
    return "processed";
  }

  // A pending prompt is only actionable after a separately approved release
  // enables admissions. In dark mode do not turn a follow-up bubble into a
  // paid job or silently preserve an intent that looks accepted.
  if (!runtime.environment.policy.acceptNewJobs) {
    await runtime.store.clearPending(record.senderHash, record.threadHash);
    await enqueueMaintenanceNotice(runtime, record, envelope, "pending-prompt-maintenance");
    return "maintenance_rejected";
  }

  await enqueueResponse(
    runtime,
    record,
    envelope,
    "pending-prompt-unavailable",
    "Air cannot submit generation work until a verified model and paid-release approval are in place.",
  );
  return "processed";
}

async function handleCommand(
  runtime: StudioRuntime,
  record: InboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  settings: StudioUserSettings,
  attachment: StudioAttachmentReference | undefined,
  command: StudioCommand,
): Promise<InboxDisposition> {
  switch (command.kind) {
    case "help":
      await enqueueResponse(runtime, record, envelope, "help", HELP_TEXT);
      return "processed";
    case "privacy":
      await enqueueResponse(runtime, record, envelope, "privacy", privacyText());
      return "processed";
    case "status":
      await enqueueResponse(
        runtime,
        record,
        envelope,
        "status",
        "No active or recent jobs are available in this dark deployment. Use /help for the command reference.",
      );
      return "processed";
    case "cancel": {
      const pending = await runtime.store.getPending<PendingIntent>(record.senderHash, record.threadHash);
      if (pending) {
        await runtime.store.clearPending(record.senderHash, record.threadHash);
        await enqueueResponse(runtime, record, envelope, "cancel-pending", "Canceled the pending request. No generation was submitted.");
      } else {
        await enqueueResponse(runtime, record, envelope, "cancel-none", "There is no pending request to cancel.");
      }
      return "processed";
    }
    case "models":
      await enqueueResponse(runtime, record, envelope, `models-${command.mode ?? "all"}`, modelsText(command.mode));
      return "processed";
    case "model-settings":
      return handleModelSettings(runtime, record, envelope, settings, command);
    case "create":
      return handleCreation(runtime, record, envelope, settings, attachment, command.mode, command.prompt);
  }
}

async function handleCreation(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  settings: StudioUserSettings,
  attachment: StudioAttachmentReference | undefined,
  mode: StudioMode,
  prompt: string | null,
): Promise<InboxDisposition> {
  // This is the explicit Section 16 rollback/dark gate. It is deliberately
  // checked before pending assembly, model selection, reservation, or any
  // provider code so a dark release cannot create a later paid surprise.
  if (!runtime.environment.policy.acceptNewJobs) {
    await enqueueMaintenanceNotice(runtime, record, envelope, `maintenance-${mode}`);
    return "maintenance_rejected";
  }

  const selection = selectedModel(settings, mode);
  if (!selection.model) {
    await enqueueResponse(runtime, record, envelope, `missing-model-${mode}`, noModelText(mode));
    return "processed";
  }
  const media = attachment ? mediaKindFromAttachment(attachment) : "none";
  const durationSeconds = selection.model.output.type === "video"
    ? settings.modes[mode].durationSeconds ?? DEFAULT_VIDEO_DURATION_SECONDS
    : null;
  const compatibility = checkModelCompatibility(selection.model, {
    mode,
    count: settings.modes[mode].outputCount,
    durationSeconds,
    hasPrompt: prompt !== null,
    media,
  });
  if (!compatibility.ok) {
    await enqueueResponse(runtime, record, envelope, `incompatible-${mode}-${compatibility.code}`, compatibilityText(mode, compatibility.code));
    return "processed";
  }

  // The committed workflow declaration protects future registry changes from
  // accidentally introducing a multi-step paid plan. This build intentionally
  // does not contain an enabled execution adapter: GMI catalog evidence,
  // numeric spend authority, and real transport evidence are still absent.
  if (!isSinglePaidStepWorkflow(getStudioWorkflow(mode)) || !runtime.environment.policy.allowProviderSubmit) {
    await enqueueResponse(
      runtime,
      record,
      envelope,
      `provider-disabled-${mode}`,
      "Air has no verified paid execution path enabled in this deployment. No generation was submitted.",
    );
    return "processed";
  }

  // Defence in depth: a future configuration cannot admit paid work merely by
  // toggling an environment variable. The direct execution adapter is enabled
  // only by a reviewed registry/runtime change after the separate Fable gate.
  await enqueueResponse(
    runtime,
    record,
    envelope,
    `execution-not-provisioned-${mode}`,
    "Air’s verified execution adapter is not provisioned yet. No generation was submitted.",
  );
  return "processed";
}

async function handleModelSettings(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  settings: StudioUserSettings,
  command: Extract<StudioCommand, { readonly kind: "model-settings" }>,
): Promise<InboxDisposition> {
  if (command.action === "show-all") {
    await enqueueResponse(runtime, record, envelope, "settings-all", allSettingsText(settings));
    return "processed";
  }
  if (command.action === "show-mode") {
    await enqueueResponse(runtime, record, envelope, `settings-${command.mode}`, modeSettingsText(settings, command.mode));
    return "processed";
  }
  if (command.action === "reset") {
    const next = resetSettings(settings, command.target);
    await runtime.store.putSettings(record.senderHash, next);
    const message = command.target === "all"
      ? "Reset all model settings to the committed defaults. " + allSettingsText(next)
      : `Reset ${command.target} to its committed default. ${modeSettingsText(next, command.target)}`;
    await enqueueResponse(runtime, record, envelope, `settings-reset-${command.target}`, message);
    return "processed";
  }

  const current = settings.modes[command.mode];
  if (command.action === "select-model") {
    const resolved = resolveEnabledModel(AIR_MODEL_REGISTRY, command.mode, command.modelSlug);
    if (!resolved.ok) {
      await enqueueResponse(runtime, record, envelope, `settings-model-${command.mode}`, noModelText(command.mode));
      return "processed";
    }
    if (!modelSupportsStoredValues(resolved.model, current)) {
      await enqueueResponse(
        runtime,
        record,
        envelope,
        `settings-model-incompatible-${command.mode}`,
        supportedValuesText(resolved.model),
      );
      return "processed";
    }
    const next = replaceModeSettings(settings, command.mode, Object.freeze({ ...current, modelSlug: resolved.model.slug }));
    await runtime.store.putSettings(record.senderHash, next);
    await enqueueResponse(runtime, record, envelope, `settings-model-saved-${command.mode}`, `Selected ${resolved.model.displayName}. ${modeSettingsText(next, command.mode)}`);
    return "processed";
  }

  const selected = selectedModel(settings, command.mode).model;
  if (!selected) {
    await enqueueResponse(runtime, record, envelope, `settings-no-model-${command.mode}`, noModelText(command.mode));
    return "processed";
  }
  if (command.action === "set-count") {
    if (!selected.output.supportedOutputCounts.includes(command.count)) {
      await enqueueResponse(runtime, record, envelope, `settings-count-invalid-${command.mode}`, supportedValuesText(selected));
      return "processed";
    }
    const next = replaceModeSettings(settings, command.mode, Object.freeze({ ...current, outputCount: command.count }));
    await runtime.store.putSettings(record.senderHash, next);
    await enqueueResponse(runtime, record, envelope, `settings-count-saved-${command.mode}`, `Set ${command.mode} output count to ${command.count}.`);
    return "processed";
  }

  if (selected.output.type !== "video" || !selected.output.supportedDurationSeconds.includes(command.durationSeconds)) {
    await enqueueResponse(runtime, record, envelope, `settings-duration-invalid-${command.mode}`, supportedValuesText(selected));
    return "processed";
  }
  const next = replaceModeSettings(settings, command.mode, Object.freeze({ ...current, durationSeconds: command.durationSeconds }));
  await runtime.store.putSettings(record.senderHash, next);
  await enqueueResponse(runtime, record, envelope, `settings-duration-saved-${command.mode}`, `Set ${command.mode} duration to ${command.durationSeconds} seconds.`);
  return "processed";
}

async function loadSettings(runtime: StudioRuntime, senderHash: StudioInboxRecord["senderHash"]): Promise<StudioUserSettings> {
  const stored = await runtime.store.getSettings<unknown>(senderHash);
  return normalizeSettings(stored);
}

function normalizeSettings(value: unknown): StudioUserSettings {
  const fallback = defaultSettings();
  if (!value || typeof value !== "object" || Array.isArray(value)) return fallback;
  const source = value as { schemaVersion?: unknown; onboardingVersion?: unknown; modes?: unknown };
  if (source.schemaVersion !== 1 || !source.modes || typeof source.modes !== "object" || Array.isArray(source.modes)) {
    return fallback;
  }
  const modeSource = source.modes as Record<string, unknown>;
  return Object.freeze({
    schemaVersion: 1,
    onboardingVersion: safeOnboardingVersion(source.onboardingVersion),
    modes: Object.freeze({
      imagine: normalizeModeSettings(modeSource.imagine, false),
      animate: normalizeModeSettings(modeSource.animate, true),
      "edit-image": normalizeModeSettings(modeSource["edit-image"], false),
      "edit-video": normalizeModeSettings(modeSource["edit-video"], true),
    }),
  });
}

function defaultSettings(onboardingVersion = 0): StudioUserSettings {
  return Object.freeze({
    schemaVersion: 1,
    onboardingVersion,
    modes: Object.freeze({
      imagine: Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT }),
      animate: Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT, durationSeconds: DEFAULT_VIDEO_DURATION_SECONDS }),
      "edit-image": Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT }),
      "edit-video": Object.freeze({ outputCount: DEFAULT_OUTPUT_COUNT, durationSeconds: DEFAULT_VIDEO_DURATION_SECONDS }),
    }),
  });
}

function normalizeModeSettings(value: unknown, video: boolean): StoredModeSettings {
  const fallback = video
    ? { outputCount: DEFAULT_OUTPUT_COUNT, durationSeconds: DEFAULT_VIDEO_DURATION_SECONDS }
    : { outputCount: DEFAULT_OUTPUT_COUNT };
  if (!value || typeof value !== "object" || Array.isArray(value)) return Object.freeze(fallback);
  const source = value as { modelSlug?: unknown; outputCount?: unknown; durationSeconds?: unknown };
  const modelSlug = typeof source.modelSlug === "string" && /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(source.modelSlug)
    ? source.modelSlug
    : undefined;
  const outputCount = isOutputCount(source.outputCount) ? source.outputCount : DEFAULT_OUTPUT_COUNT;
  const durationSeconds = video && isVideoDuration(source.durationSeconds)
    ? source.durationSeconds
    : DEFAULT_VIDEO_DURATION_SECONDS;
  return Object.freeze({
    ...(modelSlug === undefined ? {} : { modelSlug }),
    outputCount,
    ...(video ? { durationSeconds } : {}),
  });
}

function safeOnboardingVersion(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= 100
    ? value
    : 0;
}

function isOutputCount(value: unknown): value is StudioOutputCount {
  return value === 1 || value === 2 || value === 3 || value === 4;
}

function isVideoDuration(value: unknown): value is StudioVideoDurationSeconds {
  return value === 5 || value === 10 || value === 15;
}

function resetSettings(settings: StudioUserSettings, target: StudioMode | "all"): StudioUserSettings {
  const defaults = defaultSettings();
  return Object.freeze({
    ...defaults,
    onboardingVersion: settings.onboardingVersion,
    modes: target === "all"
      ? defaults.modes
      : Object.freeze({ ...settings.modes, [target]: defaults.modes[target] }),
  });
}

function replaceModeSettings(settings: StudioUserSettings, mode: StudioMode, next: StoredModeSettings): StudioUserSettings {
  return Object.freeze({ ...settings, modes: Object.freeze({ ...settings.modes, [mode]: next }) });
}

function selectedModel(settings: StudioUserSettings, mode: StudioMode): { readonly model?: StudioModel | undefined } {
  const explicit = settings.modes[mode].modelSlug;
  if (explicit !== undefined) {
    const resolved = resolveEnabledModel(AIR_MODEL_REGISTRY, mode, explicit);
    return resolved.ok ? { model: resolved.model } : {};
  }
  const fallback = resolveDefaultModel(AIR_MODEL_REGISTRY, mode);
  return fallback.ok ? { model: fallback.model } : {};
}

function modelSupportsStoredValues(model: StudioModel, settings: StoredModeSettings): boolean {
  if (!model.output.supportedOutputCounts.includes(settings.outputCount)) return false;
  if (model.output.type !== "video") return settings.durationSeconds === undefined;
  return settings.durationSeconds !== undefined && model.output.supportedDurationSeconds.includes(settings.durationSeconds);
}

function modelsText(mode: StudioMode | null): string {
  const models = listEnabledModels(AIR_MODEL_REGISTRY, mode);
  if (models.length === 0) {
    return mode === null
      ? "No verified models are enabled in this dark deployment. Candidate names are not selectable until catalog, schema, price, and privacy review complete."
      : noModelText(mode);
  }
  return models.map((model) => `${model.slug} — ${model.displayName}`).join("\n");
}

function noModelText(mode: StudioMode): string {
  return `No verified ${mode} model is enabled yet. /models lists only reviewed models; Air will not substitute a different model.`;
}

function allSettingsText(settings: StudioUserSettings): string {
  return (["imagine", "animate", "edit-image", "edit-video"] as const)
    .map((mode) => modeSettingsText(settings, mode))
    .join("\n");
}

function modeSettingsText(settings: StudioUserSettings, mode: StudioMode): string {
  const values = settings.modes[mode];
  const selected = selectedModel(settings, mode).model;
  const model = selected ? selected.displayName : "No verified model selected";
  const duration = mode === "animate" || mode === "edit-video"
    ? `, duration ${values.durationSeconds ?? DEFAULT_VIDEO_DURATION_SECONDS}s`
    : "";
  const aspectRatio = selected ? `, aspect ${selected.output.lockedAspectRatio}` : "";
  const choices = listEnabledModels(AIR_MODEL_REGISTRY, mode);
  const choiceText = choices.length > 0
    ? ` Models: ${choices.map((candidate) => candidate.slug).join(", ")}.`
    : " No verified models enabled.";
  return `${mode}: ${model}; count ${values.outputCount}${duration}${aspectRatio}.${choiceText}`;
}

function supportedValuesText(model: StudioModel): string {
  const counts = model.output.supportedOutputCounts.join(", ");
  const durations = model.output.type === "video"
    ? ` Durations: ${model.output.supportedDurationSeconds.join(", ")} seconds.`
    : " Duration is not available for this image model.";
  return `${model.displayName} supports output counts: ${counts}.${durations}`;
}

function compatibilityText(mode: StudioMode, code: string): string {
  switch (code) {
    case "prompt_required":
      return `/${mode} needs a prompt for the selected model.`;
    case "media_required":
      return `/${mode} needs one compatible attachment for the selected model.`;
    case "image_not_accepted":
    case "video_not_accepted":
      return `That attachment type is not supported by the selected ${mode} model.`;
    case "unsupported_count":
    case "duration_not_supported":
    case "duration_required":
      return `Your saved ${mode} settings are not compatible with the selected model. Use /model-settings ${mode}.`;
    default:
      return `That ${mode} request is not compatible with the selected model. No generation was submitted.`;
  }
}

function mediaKindFromAttachment(attachment: StudioAttachmentReference): "none" | "image" | "video" {
  const mime = attachment.mimeType?.toLowerCase() ?? "";
  if (mime.startsWith("image/")) return "image";
  if (mime.startsWith("video/")) return "video";
  // Unknown declared types are never treated as a compatible media input.
  return "none";
}

function privacyText(): string {
  return "Air is private and allowlist-only. Air keeps encrypted pending media references for up to 10 minutes and Air-owned accepted-work references for up to 24 hours. No provider model is enabled in this dark deployment, so Air is not sending media to a generation provider. Avoid sending sensitive media; provider retention terms will be disclosed before any model is enabled.";
}

async function enqueueMaintenanceNotice(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  label: string,
): Promise<void> {
  await enqueueResponse(
    runtime,
    record,
    envelope,
    label,
    "Air is in maintenance mode: new generation requests are not being accepted. No generation was submitted.",
  );
}

async function enqueueResponse(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  label: string,
  text: string,
): Promise<void> {
  await enqueueText(runtime, record, envelope, `response-${label}`, "request-response", 1, text);
}

async function enqueueText(
  runtime: StudioRuntime,
  record: StudioInboxRecord,
  envelope: ReturnType<StudioRuntime["store"]["decryptInboxEnvelope"]>,
  label: string,
  lane: StudioOutboxRecord["lane"],
  sequence: number,
  text: string,
): Promise<void> {
  if (!isSafeTransportText(text)) throw new Error("unsafe studio response text");
  const payload: StudioOutboxPayload = Object.freeze({
    kind: "text",
    thread: Object.freeze({
      chatGuid: envelope.chatGuid,
      sendingLine: envelope.sendingLine,
      ...(envelope.replyTargetId === undefined ? {} : { replyTargetId: envelope.replyTargetId }),
    }),
    text,
  });
  await runtime.store.createOutbox({
    outboxId: stableRecordId(record.receiptId, label),
    senderHash: record.senderHash,
    lane,
    sequence,
    state: "intent",
    attempts: 0,
    payload,
  });
}

function stableRecordId(receiptId: string, purpose: string): string {
  return createHash("sha256").update(`air:v2:outbox:${receiptId}:${purpose}`, "utf8").digest("hex");
}

async function dispatchOutboxEntry(
  runtime: StudioRuntime,
  record: StudioOutboxRecord,
  payload: StudioOutboxPayload,
): Promise<TransportOutcome> {
  const thread: IMessageThreadReference = payload.thread;
  const idempotencyKey = `air-v2-outbox-${record.outboxId}`;

  if (payload.kind === "tapback") {
    if (!payload.targetMessageId) return Object.freeze({ kind: "failed", code: "missing_tapback_target" });
    try {
      return await runtime.transport.addLikeTapback(thread, payload.targetMessageId, idempotencyKey);
    } catch {
      return Object.freeze({ kind: "send_unknown", code: "tapback_dispatch_unknown" });
    }
  }

  let typingStarted = false;
  try {
    try {
      await runtime.transport.showTyping(thread);
      typingStarted = true;
    } catch {
      // Typing is strictly best-effort and never blocks a durable message.
    }
    await dwell(TYPING_DWELL_MS);
    if (payload.kind === "text") {
      if (!payload.text || !isSafeTransportText(payload.text)) {
        return Object.freeze({ kind: "failed", code: "invalid_text_payload" });
      }
      return await runtime.transport.sendText(thread, payload.text, idempotencyKey);
    }
    if (!payload.attachment) return Object.freeze({ kind: "failed", code: "missing_attachment_payload" });
    return await runtime.transport.sendAttachment(thread, payload.attachment, idempotencyKey);
  } catch {
    // A rejected send promise can occur after the transport accepted bytes, so
    // it is intentionally unknown rather than a retryable failure.
    return Object.freeze({ kind: "send_unknown", code: "transport_dispatch_unknown" });
  } finally {
    if (typingStarted) {
      try {
        await runtime.transport.stopTyping(thread);
      } catch {
        // No action: a failed stop must not alter the message outcome.
      }
    }
  }
}

function dwell(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
