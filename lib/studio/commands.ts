/**
 * Deterministic grammar for Air's public iMessage commands.
 *
 * This module intentionally does not assemble pending media, pick models, or
 * submit work. Callers must make those decisions from durable state and the
 * model registry. Keeping parsing separate prevents a typo from becoming a
 * paid action.
 */

export const STUDIO_MODES = Object.freeze([
  "imagine",
  "animate",
  "edit-image",
  "edit-video",
] as const);

export type StudioMode = (typeof STUDIO_MODES)[number];

export const STUDIO_COMMAND_NAMES = Object.freeze([
  "/imagine",
  "/animate",
  "/edit-image",
  "/edit-video",
  "/model-settings",
  "/models",
  "/status",
  "/cancel",
  "/help",
  "/privacy",
] as const);

export const STUDIO_OUTPUT_COUNTS = Object.freeze([1, 2, 3, 4] as const);
export type StudioOutputCount = (typeof STUDIO_OUTPUT_COUNTS)[number];

export const STUDIO_VIDEO_DURATIONS_SECONDS = Object.freeze([5, 10, 15] as const);
export type StudioVideoDurationSeconds = (typeof STUDIO_VIDEO_DURATIONS_SECONDS)[number];

export const MAX_STUDIO_PROMPT_CHARACTERS = 2_000;

export type StudioCreationCommand = {
  readonly kind: "create";
  readonly mode: StudioMode;
  /** Null means the command was explicit but supplied no text. */
  readonly prompt: string | null;
};

export type StudioModelSettingsCommand =
  | {
      readonly kind: "model-settings";
      readonly action: "show-all";
    }
  | {
      readonly kind: "model-settings";
      readonly action: "show-mode";
      readonly mode: StudioMode;
    }
  | {
      readonly kind: "model-settings";
      readonly action: "select-model";
      readonly mode: StudioMode;
      readonly modelSlug: string;
    }
  | {
      readonly kind: "model-settings";
      readonly action: "set-count";
      readonly mode: StudioMode;
      readonly count: StudioOutputCount;
    }
  | {
      readonly kind: "model-settings";
      readonly action: "set-duration";
      readonly mode: StudioMode;
      readonly durationSeconds: StudioVideoDurationSeconds;
    }
  | {
      readonly kind: "model-settings";
      readonly action: "reset";
      readonly target: StudioMode | "all";
    };

export type StudioCommand =
  | StudioCreationCommand
  | StudioModelSettingsCommand
  | {
      readonly kind: "models";
      readonly mode: StudioMode | null;
    }
  | {
      readonly kind: "status" | "cancel" | "help" | "privacy";
    };

export type StudioPromptValidationErrorCode =
  | "invalid_input"
  | "control_character"
  | "empty_prompt"
  | "prompt_too_long";

export type StudioPromptValidationResult =
  | {
      readonly ok: true;
      /** The user text after the required boundary trim, otherwise verbatim. */
      readonly prompt: string;
    }
  | {
      readonly ok: false;
      readonly code: StudioPromptValidationErrorCode;
      readonly message: string;
    };

export type StudioHelpReason = "unknown_command" | "unsupported_syntax";

export type StudioCommandParseError =
  | Exclude<StudioPromptValidationResult, { readonly ok: true }>
  | {
      readonly ok: false;
      readonly code: "duration_not_supported_for_mode";
      readonly message: string;
    };

export type StudioCommandParseResult =
  | {
      readonly kind: "command";
      readonly command: StudioCommand;
    }
  | {
      /** Bare text can only be consumed by an already-persisted pending intent. */
      readonly kind: "bare-text";
      readonly text: string;
    }
  | {
      /** Unknown commands and malformed command syntax must be answered with help. */
      readonly kind: "help";
      readonly reason: StudioHelpReason;
    }
  | {
      readonly kind: "invalid";
      readonly error: StudioCommandParseError;
    };

const CONTROL_CHARACTER_PATTERN = /\p{Cc}/u;
const STABLE_MODEL_SLUG_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

const MODE_BY_NAME: Readonly<Record<string, StudioMode>> = Object.freeze({
  imagine: "imagine",
  animate: "animate",
  "edit-image": "edit-image",
  "edit-video": "edit-video",
});

const OUTPUT_COUNT_BY_TEXT: Readonly<Record<string, StudioOutputCount>> = Object.freeze({
  "1": 1,
  "2": 2,
  "3": 3,
  "4": 4,
});

const DURATION_BY_TEXT: Readonly<Record<string, StudioVideoDurationSeconds>> = Object.freeze({
  "5": 5,
  "10": 10,
  "15": 15,
});

function invalidPrompt(
  code: StudioPromptValidationErrorCode,
  message: string,
): Exclude<StudioPromptValidationResult, { readonly ok: true }> {
  return { ok: false, code, message };
}

/**
 * Validates text that may be sent to a provider. It deliberately preserves
 * letter case and internal whitespace. Only surrounding whitespace is removed.
 */
export function validateStudioPrompt(input: unknown): StudioPromptValidationResult {
  if (typeof input !== "string") {
    return invalidPrompt("invalid_input", "Air needs text, not a non-text value.");
  }

  if (CONTROL_CHARACTER_PATTERN.test(input)) {
    return invalidPrompt(
      "control_character",
      "That prompt contains unsupported control characters.",
    );
  }

  const prompt = input.trim();
  if (prompt.length === 0) {
    return invalidPrompt("empty_prompt", "Add a prompt between 1 and 2,000 characters.");
  }

  if (Array.from(prompt).length > MAX_STUDIO_PROMPT_CHARACTERS) {
    return invalidPrompt(
      "prompt_too_long",
      "Prompts can be at most 2,000 characters.",
    );
  }

  return { ok: true, prompt };
}

export function isStudioMode(value: string): value is StudioMode {
  return Object.hasOwn(MODE_BY_NAME, value);
}

export function isStableModelSlug(value: string): boolean {
  return STABLE_MODEL_SLUG_PATTERN.test(value);
}

function modeFromText(value: string): StudioMode | null {
  const normalized = value.toLowerCase();
  return MODE_BY_NAME[normalized] ?? null;
}

function commandResult(command: StudioCommand): StudioCommandParseResult {
  return { kind: "command", command };
}

function help(reason: StudioHelpReason): StudioCommandParseResult {
  return { kind: "help", reason };
}

function parseCreation(mode: StudioMode, remainder: string): StudioCommandParseResult {
  if (remainder.trim().length === 0) {
    return commandResult({ kind: "create", mode, prompt: null });
  }

  const prompt = validateStudioPrompt(remainder);
  if (!prompt.ok) {
    return { kind: "invalid", error: prompt };
  }

  return commandResult({ kind: "create", mode, prompt: prompt.prompt });
}

function parseSettings(remainder: string): StudioCommandParseResult {
  const text = remainder.trim();
  if (text.length === 0) {
    return commandResult({ kind: "model-settings", action: "show-all" });
  }

  const parts = text.split(/\s+/u);
  const first = parts[0];
  if (first === undefined) {
    return help("unsupported_syntax");
  }

  if (first.toLowerCase() === "reset") {
    const target = parts[1]?.toLowerCase();
    if (parts.length !== 2 || target === undefined) {
      return help("unsupported_syntax");
    }
    if (target === "all") {
      return commandResult({ kind: "model-settings", action: "reset", target: "all" });
    }
    const mode = modeFromText(target);
    return mode === null
      ? help("unsupported_syntax")
      : commandResult({ kind: "model-settings", action: "reset", target: mode });
  }

  const mode = modeFromText(first);
  if (mode === null) {
    return help("unsupported_syntax");
  }

  if (parts.length === 1) {
    return commandResult({ kind: "model-settings", action: "show-mode", mode });
  }

  const operation = parts[1];
  if (operation === undefined) {
    return help("unsupported_syntax");
  }

  if (operation.toLowerCase() === "count") {
    const countText = parts[2];
    if (parts.length !== 3 || countText === undefined) {
      return help("unsupported_syntax");
    }
    const count = OUTPUT_COUNT_BY_TEXT[countText];
    return count === undefined
      ? help("unsupported_syntax")
      : commandResult({ kind: "model-settings", action: "set-count", mode, count });
  }

  if (operation.toLowerCase() === "duration") {
    const durationText = parts[2];
    if (parts.length !== 3 || durationText === undefined) {
      return help("unsupported_syntax");
    }
    if (mode === "imagine" || mode === "edit-image") {
      return {
        kind: "invalid",
        error: {
          ok: false,
          code: "duration_not_supported_for_mode",
          message: "Duration is available only for /animate and /edit-video.",
        },
      };
    }
    const durationSeconds = DURATION_BY_TEXT[durationText];
    return durationSeconds === undefined
      ? help("unsupported_syntax")
      : commandResult({
          kind: "model-settings",
          action: "set-duration",
          mode,
          durationSeconds,
        });
  }

  if (parts.length !== 2 || !isStableModelSlug(operation)) {
    return help("unsupported_syntax");
  }

  return commandResult({
    kind: "model-settings",
    action: "select-model",
    mode,
    modelSlug: operation,
  });
}

function parseModels(remainder: string): StudioCommandParseResult {
  const text = remainder.trim();
  if (text.length === 0) {
    return commandResult({ kind: "models", mode: null });
  }
  if (/\s/u.test(text)) {
    return help("unsupported_syntax");
  }
  const mode = modeFromText(text);
  return mode === null ? help("unsupported_syntax") : commandResult({ kind: "models", mode });
}

function parseNoArgumentCommand(
  kind: "status" | "cancel" | "help" | "privacy",
  remainder: string,
): StudioCommandParseResult {
  return remainder.trim().length === 0 ? commandResult({ kind }) : help("unsupported_syntax");
}

/**
 * Parses one inbound text bubble. Command keywords are case-insensitive;
 * prompt text is never lowercased or otherwise rewritten.
 */
export function parseStudioCommand(input: unknown): StudioCommandParseResult {
  if (typeof input !== "string") {
    return { kind: "invalid", error: invalidPrompt("invalid_input", "Air needs text, not a non-text value.") };
  }

  if (CONTROL_CHARACTER_PATTERN.test(input)) {
    return {
      kind: "invalid",
      error: invalidPrompt(
        "control_character",
        "That message contains unsupported control characters.",
      ),
    };
  }

  const text = input.trim();
  if (text.length === 0) {
    return help("unsupported_syntax");
  }

  if (!text.startsWith("/")) {
    const bareText = validateStudioPrompt(text);
    return bareText.ok
      ? { kind: "bare-text", text: bareText.prompt }
      : { kind: "invalid", error: bareText };
  }

  const match = /^(\S+)(?:\s+([\s\S]*))?$/u.exec(text);
  if (match === null) {
    return help("unsupported_syntax");
  }

  const token = match[1];
  if (token === undefined) {
    return help("unsupported_syntax");
  }
  const commandName = token.toLowerCase();
  const remainder = match[2] ?? "";

  switch (commandName) {
    case "/imagine":
      return parseCreation("imagine", remainder);
    case "/animate":
      return parseCreation("animate", remainder);
    case "/edit-image":
      return parseCreation("edit-image", remainder);
    case "/edit-video":
      return parseCreation("edit-video", remainder);
    case "/model-settings":
      return parseSettings(remainder);
    case "/models":
      return parseModels(remainder);
    case "/status":
      return parseNoArgumentCommand("status", remainder);
    case "/cancel":
      return parseNoArgumentCommand("cancel", remainder);
    case "/help":
      return parseNoArgumentCommand("help", remainder);
    case "/privacy":
      return parseNoArgumentCommand("privacy", remainder);
    default:
      return help("unknown_command");
  }
}
