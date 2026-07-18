import { describe, expect, it } from "vitest";

import {
  MAX_STUDIO_PROMPT_CHARACTERS,
  parseStudioCommand,
  validateStudioPrompt,
} from "../../lib/studio/commands.js";

describe("Air studio command grammar", () => {
  it("recognizes every public command without inferring an action", () => {
    expect(parseStudioCommand("/ImAgInE  Keep MY Capitalization")).toEqual({
      kind: "command",
      command: { kind: "create", mode: "imagine", prompt: "Keep MY Capitalization" },
    });
    expect(parseStudioCommand("/ANIMATE")).toEqual({
      kind: "command",
      command: { kind: "create", mode: "animate", prompt: null },
    });
    expect(parseStudioCommand("/edit-image brighten the subject")).toEqual({
      kind: "command",
      command: { kind: "create", mode: "edit-image", prompt: "brighten the subject" },
    });
    expect(parseStudioCommand("/EDIT-VIDEO remove the logo")).toEqual({
      kind: "command",
      command: { kind: "create", mode: "edit-video", prompt: "remove the logo" },
    });

    expect(parseStudioCommand("/model-settings")).toEqual({
      kind: "command",
      command: { kind: "model-settings", action: "show-all" },
    });
    expect(parseStudioCommand("/model-settings ANIMATE")).toEqual({
      kind: "command",
      command: { kind: "model-settings", action: "show-mode", mode: "animate" },
    });
    expect(parseStudioCommand("/model-settings animate count 4")).toEqual({
      kind: "command",
      command: { kind: "model-settings", action: "set-count", mode: "animate", count: 4 },
    });
    expect(parseStudioCommand("/model-settings edit-video duration 15")).toEqual({
      kind: "command",
      command: {
        kind: "model-settings",
        action: "set-duration",
        mode: "edit-video",
        durationSeconds: 15,
      },
    });
    expect(parseStudioCommand("/model-settings reset ALL")).toEqual({
      kind: "command",
      command: { kind: "model-settings", action: "reset", target: "all" },
    });
    expect(parseStudioCommand("/model-settings imagine verified-model")).toEqual({
      kind: "command",
      command: {
        kind: "model-settings",
        action: "select-model",
        mode: "imagine",
        modelSlug: "verified-model",
      },
    });
    expect(parseStudioCommand("/models EDIT-IMAGE")).toEqual({
      kind: "command",
      command: { kind: "models", mode: "edit-image" },
    });
    expect(parseStudioCommand("/status")).toEqual({ kind: "command", command: { kind: "status" } });
    expect(parseStudioCommand("/cancel")).toEqual({ kind: "command", command: { kind: "cancel" } });
    expect(parseStudioCommand("/help")).toEqual({ kind: "command", command: { kind: "help" } });
    expect(parseStudioCommand("/privacy")).toEqual({ kind: "command", command: { kind: "privacy" } });
  });

  it("uses only strict settings grammar", () => {
    for (const input of [
      "/model-settings animate count 0",
      "/model-settings animate count 01",
      "/model-settings animate count 5",
      "/model-settings animate duration 7",
      "/model-settings animate duration 5 extra",
      "/model-settings reset",
      "/model-settings reset image",
      "/model-settings not-a-mode",
      "/models animate extra",
      "/model-settings imagine Model-With-Caps",
    ]) {
      expect(parseStudioCommand(input)).toEqual({ kind: "help", reason: "unsupported_syntax" });
    }

    expect(parseStudioCommand("/model-settings imagine duration 5")).toMatchObject({
      kind: "invalid",
      error: { code: "duration_not_supported_for_mode" },
    });
    expect(parseStudioCommand("/model-settings edit-image duration 15")).toMatchObject({
      kind: "invalid",
      error: { code: "duration_not_supported_for_mode" },
    });
  });

  it("keeps bare text separate from commands so only a pending intent may use it", () => {
    expect(parseStudioCommand("  A caption with CAPS  ")).toEqual({
      kind: "bare-text",
      text: "A caption with CAPS",
    });
    expect(parseStudioCommand("/made-up something")).toEqual({
      kind: "help",
      reason: "unknown_command",
    });
    expect(parseStudioCommand("/status now")).toEqual({
      kind: "help",
      reason: "unsupported_syntax",
    });
  });

  it("trims prompt boundaries, preserves the prompt body, and rejects controls", () => {
    expect(validateStudioPrompt("  A  MiXeD Case prompt  ")).toEqual({
      ok: true,
      prompt: "A  MiXeD Case prompt",
    });
    expect(validateStudioPrompt("\u0000unsafe")).toMatchObject({
      ok: false,
      code: "control_character",
    });
    expect(parseStudioCommand("/imagine one\ntwo")).toMatchObject({
      kind: "invalid",
      error: { code: "control_character" },
    });
    expect(validateStudioPrompt(" ")).toMatchObject({ ok: false, code: "empty_prompt" });
    expect(validateStudioPrompt("x".repeat(MAX_STUDIO_PROMPT_CHARACTERS + 1))).toMatchObject({
      ok: false,
      code: "prompt_too_long",
    });
  });
});
