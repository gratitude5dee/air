import { expect, test } from "vitest";
import {
  answerMusicQuestion,
  collectBoundedGenerationText,
  GroundingValidationError,
  MAX_GENERATION_OUTPUT_CHARS,
  NOT_FOUND_ANSWER,
} from "../../lib/grounded-answer.js";
import type { Source, SourceId } from "../../lib/types.js";

function source(id: SourceId): Source {
  return {
    id,
    chunk_uid: id,
    title: `Title ${id}`,
    channel: "Test Channel",
    guests: [],
    video_id: id,
    start_sec: 61,
    end_sec: 90,
    deep_link: "https://youtu.be/abc123",
    snippet: "Evidence from a transcript.",
    score: 1,
    chunk_index: 0,
  };
}

test("grounded generation repairs an invalid first answer and renders only cited sources", async () => {
  let calls = 0;
  const result = await answerMusicQuestion("What should an artist prioritize?", {
    retrieve: async () => ({ intent: "thematic", sources: [source("S1"), source("S2")] }),
    generate: async () => {
      calls += 1;
      return calls === 1
        ? "Artists should prioritize a clear strategy."
        : "Artists should prioritize a clear strategy [S2].";
    },
  });

  expect(calls).toBe(2);
  expect(result.citationRetries).toBe(1);
  expect(result.citedSourceIds).toEqual(["S2"]);
  expect(result.sources.map((item) => item.id)).toEqual(["S2"]);
  expect(result.answer).toMatch(/Sources:\n\[S2\] Title S2/);
  expect(result.answer).not.toMatch(/\[S1\] Title S1/);
});

test("empty retrieval produces the controlled evidence-missing answer without generation", async () => {
  const result = await answerMusicQuestion("Unknown question", {
    retrieve: async () => ({ intent: "thematic", sources: [] }),
    generate: async () => {
      throw new Error("must not generate");
    },
  });
  expect(result.answer).toBe(NOT_FOUND_ANSWER);
  expect(result.sources).toEqual([]);
  expect(result.citedSourceIds).toEqual([]);
});

test("repair exhaustion never returns rejected model prose", async () => {
  let calls = 0;
  await expect(
    answerMusicQuestion("Question", {
      retrieve: async () => ({ intent: "thematic", sources: [source("S1")] }),
      generate: async () => {
        calls += 1;
        return "Unsupported factual prose without a marker.";
      },
    }),
  ).rejects.toBeInstanceOf(GroundingValidationError);
  expect(calls).toBe(4);
});

test("generation collection aborts an overlong stream before an unsafe body is retained", async () => {
  async function* stream(): AsyncGenerator<{ type: "text-delta"; text: string }> {
    yield { type: "text-delta", text: "a".repeat(MAX_GENERATION_OUTPUT_CHARS) };
    yield { type: "text-delta", text: "b" };
  }

  let aborted = false;
  await expect(collectBoundedGenerationText(stream(), MAX_GENERATION_OUTPUT_CHARS, () => {
    aborted = true;
  })).rejects.toMatchObject({
    name: "GroundingValidationError",
    reason: "generation_output_limit",
  });
  expect(aborted).toBe(true);
});
