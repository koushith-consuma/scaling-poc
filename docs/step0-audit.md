# Step 0 — Pre-flight code audit

**Context:** The target directory contained only `agent.md` and `.claude/` at audit
time. There is **no pre-existing codebase** to audit — this is a greenfield build.
The three audit questions are therefore answered by decision rather than discovery,
and those decisions shape the build below.

## 1. Mongo indexes

No collections, no indexes exist yet. **Decision:** build the required indexes into
the schema-init path from the start, so the load test measures a *correctly indexed*
system first and any regression is deliberate.

Indexes created by `src/lib/mongo.ts` on first connect:

- `events`: compound `{ threadId: 1, seq: 1 }` — per-thread ordered reads / backfill.
- `events`: compound `{ runId: 1, seq: 1 }` — per-run ordered reads (SSE backfill by
  `lastEventSeq`).
- `runs`: `{ threadId: 1, status: 1 }` — the per-thread guard's active-run lookup.
- `runs`: `{ status: 1 }` — reaper scan for stuck/crashed runs.

For the "missing index is the bottleneck" experiment in Step 8, indexes can be
dropped via `MONGO_SKIP_INDEXES=1` to reproduce the un-indexed regression on demand.

## 2. Consumer coupling

No consumer exists yet. **Decision:** the agent loop (`src/lib/runLoop.ts`) is written
from day one as a pure module with **zero web-framework imports** — it takes plain
data (`{ runId, threadId }`) and dependency-injected collaborators (model, emit,
sandbox). The worker (`src/worker.ts`) is the only entrypoint that consumes it.
Extraction (Step 3) is consequently a no-op confirmation, not real work: the worker
already has its own entrypoint and `Dockerfile.worker` with no `express` import.

## 3. Streaming

No model call exists yet, and the real LLM is explicitly out of scope for the entire
POC. The mock model returns a **whole decision per turn** (not a token stream); each
turn emits exactly one event to Mongo and one to Redis. So the current emit path is
**non-streaming / whole-response**. This does not block the POC. A later perf decision
could split a turn into token-level events on the same Redis channel; the event
`seq` model already supports arbitrarily fine-grained events, so that change is
additive and does not alter the architecture.

## Scope impact summary

| Question        | Finding (greenfield decision)                | Effect on later steps            |
|-----------------|----------------------------------------------|----------------------------------|
| Mongo indexes   | Built in up front; togglable off for Step 8  | Step 8 can A/B indexed vs not    |
| Consumer coupling | Loop is pure, no web imports from day one  | Step 3 becomes a confirmation    |
| Streaming       | Whole-response per turn (non-streaming)      | Informs a later, additive change |
