# Architecture — what each piece does and why

This is the deep-dive. For *how to run/test* see [`TESTING.md`](TESTING.md); for
*results* see [`REPORT.md`](REPORT.md).

## The one-sentence model

A message becomes a **job** on a queue; a pool of **workers** competes for jobs,
runs the agent loop, and writes every step to a **database** (the source of
truth) while also pushing it to a **pub/sub bus** (a speed layer); the **browser**
watches its run over a live stream that reads from the bus and falls back to the
database. Nothing in the request path waits for the agent to finish.

## Full data flow (one message)

```
 ┌─────────┐   POST /runs      ┌──────────┐   publish job    ┌────────────┐
 │ Browser │ ────────────────▶ │ Web tier │ ───────────────▶ │  RabbitMQ  │  jobs IN
 │ (Next)  │ ◀──────202────────│  (:3000) │                  │ run-execute│
 └────┬────┘  {runId}          └────┬─────┘                  └─────┬──────┘
      │                             │                              │ consume (prefetch=1)
      │ GET /runs/:id/stream (SSE)  │                              ▼
      │                             │                        ┌───────────┐
      │                             │                        │  Worker   │  claim (atomic pending→running)
      │                             │                        │  (×N)     │  per-thread guard (Mongo lock)
      │                             │                        │           │  loop: mockModel → mockTool
      │                             │                        └────┬──────┘
      │                             │        each step:           │ emitEvent
      │                             │                             ├──────────────▶ ┌────────┐  source of truth
      │                             │                             │  insert event  │ MongoDB│  (runs, events)
      │                             │                             │                └────────┘
      │                             │                             └──────────────▶ ┌────────┐  events OUT (fire&forget)
      │                             │       subscribe run:{id}                      │ Redis  │  pub/sub
      │       live events           │ ◀───────────────────────────────────────────└────────┘
      │ ◀───────────────────────────┤ + poll Mongo every 1s as reliable fallback
```

**Two buses, never conflated:**
- **RabbitMQ carries jobs IN** (work to be done → workers).
- **Redis carries events OUT** (results → browsers).

---

## Component by component

### 1. RabbitMQ — the job queue (work intake)

**Role.** Holds `{ runId, threadId, seed }` jobs on a durable queue named
`run-execute`. Workers are *competing consumers*: each job is delivered to
exactly one worker.

**Why it's here.** Decouples "accept the request" from "do the work." The web
tier publishes and returns `202` immediately; workers pull at their own pace.
This is what lets you scale workers independently and absorb bursts.

**Key settings (why they matter):**
- `durable: true` queue + `persistent: true` messages → jobs survive a broker
  restart.
- `channel.prefetch(1)` on each worker → a worker holds **one** unacked job at a
  time. This is the core scaling knob: **max concurrent runs = workers ×
  prefetch**. It also means a slow run can't starve others — RabbitMQ hands the
  next job to whichever worker is free.
- **ack/nack:** a job is `ack`'d only after the run reaches `done`. If a worker
  dies mid-run, the channel closes, the job is **unacked**, and RabbitMQ
  redelivers it to another worker → automatic crash recovery at the queue layer.

**Code:** `src/lib/queue.ts` (`connectQueue`, `publishJob`, `parseJob`).
Consumed in `src/worker.ts`.

**Inspect it:** RabbitMQ management UI at http://localhost:15672 (guest/guest) →
Queues → `run-execute` shows `ready` (waiting), `unacked` (in-flight), and
`consumers` (attached workers).

---

### 2. MongoDB — the source of truth

**Role.** Two collections plus a lock collection:
- **`runs`** — one doc per run: `status` (`pending→running→done/failed`),
  `lastEventSeq` (the per-run event counter), `threadId`, `claimedBy`, timestamps.
- **`events`** — the append-only event log: one doc per step, with a per-run
  monotonic `seq`. This is what the SSE backfill and the whole audit trail read.
- **`thread_locks`** — the per-thread guard (see §6).

**Why it's here.** Redis is fire-and-forget and lossy by design; Mongo is the
durable record. If Redis drops a message or is down, the browser still gets every
event because it can always re-read Mongo. "Mongo is truth, Redis is speed."

**The `seq` trick (gap-free ordering).** `emitEvent` does an atomic
`findOneAndUpdate` on the run doc that `$inc`s `lastEventSeq` and returns the new
value, then inserts the event with that `seq`. Because the increment is atomic,
every event gets a unique, gap-free, strictly increasing sequence number per run
— even with concurrent writers. That number is what powers SSE dedup and
reconnect backfill (`seq > lastSeen`).

**Indexes (this is the headline bottleneck — see REPORT):** created up front in
`src/lib/mongo.ts`:
- `events {threadId, seq}` — per-thread ordered reads.
- `events {runId, seq}` (unique) — SSE backfill by run. **The important one.**
- `runs {threadId, status}` — the guard's active-run lookup.
- `runs {status, claimedAt}` — the reaper's stuck-run scan.

Without the `{runId, seq}` index, every SSE (re)connect does a full collection
scan → ~40× slower at 205k events, and it gets worse as the log grows. Toggle
off with `MONGO_SKIP_INDEXES=1` to reproduce the regression.

**Code:** `src/lib/mongo.ts`, `src/lib/emitEvent.ts`, `src/lib/claimRun.ts`,
`src/lib/createRun.ts`.

---

### 3. Redis — the live event bus (speed layer, OUT)

**Role.** Pub/sub. When a worker emits an event, after the Mongo write it also
`PUBLISH`es the event JSON to a **per-run channel** `run:{runId}`. The web tier,
while streaming a run to a browser, `SUBSCRIBE`s to just that run's channel and
forwards messages down the SSE connection.

**Why it's here.** Polling Mongo alone would mean 1s+ latency per event. Redis
gives sub-millisecond live push. But it must **never** be load-bearing:

**Fire-and-forget, three guarantees (all in code):**
1. The publish happens *after* the Mongo write and its error is swallowed —
   a Redis failure can't fail the write or the loop (`emitEvent.ts` wraps the
   publisher in try/catch; `redisPub.ts` `.catch()`es the publish).
2. The publisher **skips** when the socket isn't ready (`client.isReady`) so it
   never queues unbounded offline commands, and it **self-heals** with capped
   reconnect backoff.
3. Failure logging is rate-limited to 1/sec so a Redis outage can't spam logs.

**Per-run channels, not one global channel.** The web tier only subscribes to
channels for runs it is *actively streaming*. This keeps fan-out cheap and avoids
every web node seeing every event.

**Why the browser still works with Redis down:** the SSE handler runs a **Mongo
poll every 1 second** in parallel with the Redis subscription. If Redis is dead
or drops a message, the next poll picks up everything with `seq > lastSent` and
forwards it. Slower, but lossless. (Verified live in the chaos test: stop Redis,
all events still arrive.)

**Code:** `src/lib/redisPub.ts` (publisher, on the worker),
`src/web/server.ts` `GET /runs/:id/stream` (subscriber + poll fallback).

---

### 4. Worker — the thing being scaled

**Role.** A standalone Node process (no web framework imported) that:
1. connects to RabbitMQ, `prefetch(1)`, consumes `run-execute`;
2. runs the **per-thread guard** — if the thread is busy, requeue and move on
   (never block idle);
3. **claims** the run (atomic `pending→running`; loses gracefully on duplicate
   delivery);
4. drives the **agent loop** (`runLoop`): ask `mockModel` → on `tool_call`, exec
   `mockTool` (optionally in a real container) → `emitEvent` each step → repeat
   until `done`;
5. `ack`s on success, `nack(requeue)` on failure;
6. releases the thread lock in `finally`.

**Why standalone.** This is the unit you replicate. It has its own
`Dockerfile.worker`, its own entrypoint, and zero coupling to the web tier — so
`docker compose up --scale worker=N` is the entire scaling story. Proven in
Step 3 (the audit deliberately kept the loop a pure module).

**Optional add-ons, all env-gated:**
- `SANDBOX_ENABLED=1` → real Docker containers (see §5).
- `REDIS_ENABLED=1` → live event push (see §3).
- `REAPER_ENABLED=1` → crash-recovery scanner (see §7).

**Code:** `src/worker.ts` (entrypoint + consume loop), `src/lib/runLoop.ts`
(the pure loop), `src/lib/claimRun.ts`.

---

### 5. Sandbox orchestrator — real container lifecycle

**Role.** Behind a tiny `claim / exec / release` interface, maintains a **warm
pool** of pre-booted Docker containers:
- `claim(runId)` → hand over a warm container **instantly**; kick off a
  background boot to refill the pool. On pool exhaustion, cold-boot one on demand
  and measure the cost (~190ms).
- `exec(handle, cmd)` → `docker exec` a command in the claimed container.
- `release(handle)` → `docker rm -f` at run end (per-run workspace discarded).

One container is claimed **once per run** and reused across all the run's tool
calls, so a file written in call 1 is visible in call 2 (workspace persists
within a run). Verified in Step 4.

**Why functions-in-worker, not a service.** POC scope: it's a module behind an
interface, not a separate microservice. Swapping in a remote orchestrator later
means implementing the same three methods.

**Code:** `src/sandbox/orchestrator.ts` (`DockerOrchestrator` + `StubSandbox`).
When `SANDBOX_ENABLED` is off, `StubSandbox` returns a fake handle so Steps 2/3
run with no Docker.

---

### 6. Per-thread serialization guard — the correctness piece

**Problem.** Multiple workers pulling from one queue could run two messages of
the **same conversation thread** at once → interleaved, incoherent event order.

**Rule.** At most one active run per thread. At claim time, atomically ask "does
this thread already have an active run?" If yes, **requeue the job** (ack the
original + republish with a 250ms delay) and move on — the worker does **not**
block idle waiting; it picks up other threads' work. A new same-thread message
queues *behind* the active one; the in-flight run is never cancelled.

**Implementation.** A Mongo `thread_locks` collection with `_id = threadId`. The
acquire is an upsert that only succeeds if the lock is free or already held by
this run (idempotent on redelivery); a duplicate-key error means "busy → not
acquired." Released in the worker's `finally`.

Because the lock lives in Mongo (not worker memory), it holds **across all N
worker processes**. Verified in Step 5: 3 same-thread jobs across 3 workers ran
strictly sequentially, zero interleaving, while distinct threads ran concurrently.

**Code:** `src/lib/threadGuard.ts` (`tryAcquireThread`, `releaseThread`),
called in `src/worker.ts`.

---

### 7. Reaper — crash recovery for DB state

**Problem.** RabbitMQ redelivery recovers the *job* when a worker dies, but the
run doc is stuck in `running` and its thread lock is orphaned.

**Role.** A periodic scan (opt-in via `REAPER_ENABLED=1`): find runs `running`
longer than a lease TTL with no progress, atomically reset them to `pending`,
release the stale thread lock, and republish the job. Belt-and-suspenders
alongside RabbitMQ redelivery. Verified in Step 8: `kill -9` a worker mid-run →
the run recovers and finishes.

**Code:** `src/lib/reaper.ts`, wired in `src/worker.ts`.

---

### 8. Web tier — the thin edge

**Role.** Stateless HTTP + SSE. Three jobs:
- `POST /runs` → create the pending run doc, publish the job, return `202`
  immediately (never waits for the agent). Returns `503` if Mongo/Rabbit are down.
- `GET /runs/:id/stream` → Server-Sent Events. On connect, **backfill** from
  `?lastSeq=` (everything the client missed), then live-tail via Redis **and**
  poll Mongo every 1s as the reliable fallback. Dedups on `seq`.
- Observability + chaos APIs (`/api/health`, `/api/stats`, `/api/ops/stream`,
  `/api/chaos`) — see §10.

**Why "thin."** It holds no run state and does no agent work, so you can run many
copies behind a load balancer. All durable state is in Mongo; all live state is
in Redis.

**Reconnect backfill in detail.** SSE sends each event with `id: <seq>`. The
browser tracks the max seq it saw. On reconnect it calls
`?lastSeq=<maxSeq>`; the server returns only `seq > lastSeq` from Mongo, then
resumes live. Result: disconnect mid-run, reconnect, and you get exactly the
missed events with no gap and no duplicate. Verified in Step 6.

**Code:** `src/web/server.ts`.

---

### 9. Frontend — Next.js chat (separate service)

**Role.** The chat UI at :3001. A client component that:
- `POST`s a message → creates a run, opens an `EventSource` to the SSE endpoint,
  and renders the streamed steps live inside an assistant bubble;
- pins the `threadId` after the first send so follow-ups reuse the thread (and
  thus **serialize** — you can watch the guard work);
- subscribes to `/api/ops/stream` for the live health/stats panel;
- has chaos buttons that `POST /api/chaos`.

**Why a separate service + proxy.** It's its own deployable with its own
`Dockerfile`. `next.config.mjs` rewrites `/runs` and `/api/*` to the backend so
the browser uses same-origin relative URLs — no CORS, and SSE works cleanly.

**Code:** `frontend/app/page.tsx` (UI + streaming logic), `frontend/app/lib.ts`
(types + fetch helpers), `frontend/next.config.mjs` (proxy).

---

### 10. Observability + Chaos (what makes it testable)

**Observability** (`src/web/ops.ts`):
- `health()` — pings Mongo, Redis, RabbitMQ (mgmt API), and counts running
  worker containers via Docker labels.
- `queueStat()` — reads `run-execute` depth/consumers from the RabbitMQ mgmt API.
- `runCounts()` — aggregates run docs by status.
- `/api/ops/stream` pushes all of the above every 1s to the UI tiles.

**Chaos** (`src/web/chaos.ts`) — a fixed, allow-listed set of actions that shell
out to the Docker CLI (scoped to the compose project, no user input
interpolated): `kill-worker`, `stop/start-redis|mongo|rabbit`, `scale-workers`.
This is why the web tier runs on the **host** — it needs Docker CLI access.
Disabled in-container (`CHAOS_ENABLED=0`).

---

## The two mocks (and only these)

Per POC rules, exactly two things are faked; everything above is real.
- **`mockModel`** (`src/mock/mockModel.ts`) — stands in for the LLM. Sleeps a
  random delay, then returns `tool_call` or `done`. Driven by `avgTurns` and
  `toolCallProbability`, and **seedable** so load runs are reproducible.
- **`mockTool`** (`src/mock/mockTool.ts`) — stands in for tool work. Sleeps, then
  touches a file in the container workspace, returns a canned string.

The real LLM is never wired in.

---

## Configuration knobs (env)

All read in `src/config.ts`.

| Env | Default | Effect |
|---|---|---|
| `WORKER_PREFETCH` | 1 | jobs a worker holds at once → concurrency per worker |
| `POOL_SIZE` | 3 | warm sandbox containers |
| `SANDBOX_ENABLED` | 0 | use real Docker containers vs stub |
| `REDIS_ENABLED` | 0 | live event push (else Mongo-poll only) |
| `REAPER_ENABLED` | 0 | crash-recovery scanner |
| `MONGO_SKIP_INDEXES` | 0 | drop indexes → reproduce the bottleneck |
| `MODEL_AVG_TURNS` | 4 | expected turns before `done` |
| `MODEL_MIN/MAX_DELAY_MS` | 500/3000 | mock model latency |
| `TOOL_MIN/MAX_DELAY_MS` | 200/1500 | mock tool latency |
| `CHAOS_ENABLED` | 1 | allow the chaos endpoints (host only) |
| `WEB_PORT` | 3000 | web tier port |

## Where the guarantees live (quick map)

| Guarantee | Enforced by | File |
|---|---|---|
| Request never blocks on the agent | `202` + queue | `web/server.ts`, `lib/queue.ts` |
| Exactly-once-ish claim | atomic `pending→running` | `lib/claimRun.ts` |
| Gap-free event order | atomic `$inc` seq | `lib/emitEvent.ts` |
| One run per thread | Mongo lock, requeue-not-block | `lib/threadGuard.ts` |
| Fast reads at scale | compound indexes | `lib/mongo.ts` |
| Live updates | Redis per-run channel | `lib/redisPub.ts` |
| No data loss if Redis dies | Mongo poll fallback | `web/server.ts` |
| Recover missed events | seq-based backfill | `web/server.ts` |
| Crash recovery | RabbitMQ redelivery + reaper | `worker.ts`, `lib/reaper.ts` |
| Scaling | competing consumers, `--scale worker=N` | `docker-compose.yml` |
