# MusicRAG golden retrieval evaluation

`musicrag-golden.jsonl` is Air's pinned, 40-question retrieval fixture. It is
a verbatim snapshot of
`/Users/gratitud3/Downloads/musicindustry/musicRAG/eval/golden.jsonl`, copied
on 2026-07-15. The source fixture and its Python metric definitions are the
provenance for this Air-owned evaluation; the fixture is deliberately stored
here so Air's regression gate does not depend on a sibling checkout.
Its source-snapshot SHA-256 is
`b14df08a798394d46d1bfb3d602ec9eb0856581d36c02255ddeefe72b04c787c`.

Each JSON Lines row contains a public research question, its expected episode
video ID(s), a channel-coverage label, and a short human note. The `channel`
field is a coverage label, not a retrieval filter: the live Air harness scores
the same unmodified question path used by the agent.

The recorded MusicRAG agent baseline in the execution goal is Recall@10 0.963,
MRR@10 0.955, and nDCG@10 0.947. Those are comparison thresholds only; this
repository intentionally does not claim that Air has achieved them until the
credential-gated live command is run against the approved read-only corpus.

Run the live retrieval gate only from a trusted shell with the isolated
read-only `MONGODB_URI`, `MONGODB_DB=music_rag`, and `VOYAGE_API_KEY` in the
environment:

```sh
npm run eval:golden
```

It does not generate answers, send iMessages, write to Atlas, or persist a
report. It queries the existing corpus and Voyage, prints a redacted aggregate
summary, and fails if the fixed recorded baseline is not met. Normal `npm test`
keeps this credential-dependent test skipped.
