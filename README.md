# Viper Scaling POC

Proves a decoupled agent-run architecture scales: agent runs execute in
**standalone worker processes behind a queue**, independent of any HTTP request,
with **per-thread ordering, live SSE updates, reconnect backfill, and crash
recovery**. The deliverable is **measurements** (see [`docs/REPORT.md`](docs/REPORT.md)
and open [`docs/dashboard.html`](docs/dashboard.html)), not a demo.

> **Headline result:** the queue/worker layer scales linearly (`throughput =
> workers × prefetch`); the **first layer to break is un-indexed Mongo reads**
> (~40× regression on SSE backfill at 205k events). Indexed, Mongo stays flat at
> 4–5 ms under all load.

## What's mocked

Only two things: the **model call** (`src/mock/mockModel.ts` — seedable random
delay → `tool_call`/`done`) and the **tool's inner work** (`src/mock/mockTool.ts`
— sleep + touch a file). Everything else is real: RabbitMQ, standalone workers,
per-thread guard, MongoDB, Redis pub/sub, real Docker containers, SSE. The real
LLM is never wired in.

## Architecture

- **RabbitMQ** — jobs IN (`run-execute` queue, prefetch(1) competing consumers).
- **MongoDB** — source of truth: runs, event log (indexed), thread locks.
- **Redis** — events OUT, per-run channel `run:{runId}`, fire-and-forget.
- **worker** (`src/worker.ts`) — standalone, no web imports; the thing scaled.
- **web** (`src/web/server.ts`) — thin tier: `POST /runs`, SSE stream.

## Run the interactive app (chat + ops + chaos)

A **Next.js chat UI** (separate service) lets you send messages and watch each
one flow live through the whole chain — **browser → web → RabbitMQ → worker →
Mongo → Redis → SSE → browser** — with a live ops panel (service health, queue
depth, run counts, worker count) and **chaos buttons** to break things on
purpose and watch it degrade + recover.

```bash
# 1. infra + workers (workers are their own scaled service)
npm install
docker compose up -d rabbitmq mongo redis
docker compose up -d --build --scale worker=3 worker

# 2. backend web tier ON THE HOST (needs docker CLI for chaos/ops)
CHAOS_ENABLED=1 npm run web            # → http://localhost:3000 (API + SSE)

# 3. Next.js chat frontend
cd frontend && npm install && npm run dev   # → http://localhost:3001
```

Open **http://localhost:3001**, type a message, hit Send. You'll see the
assistant bubble stream its worker steps live (`tool_call`, `tool_result`, …)
as they actually happen on a worker.

### How to test it like a real app

- **Basic chat** — send a message; watch it go `pending → running → done` with
  live steps. That's the full pipeline round-trip.
- **Per-thread ordering** — hit **“×3 same thread”**. Three runs enter the same
  thread; the guard makes them run **one after another**, never concurrently.
- **Scaling** — set worker count in the chaos panel (or
  `docker compose up -d --scale worker=N worker`) and watch queue-depth /
  pickup change.

### How to break it (chaos panel)

| Button | What it does | What you should see |
|---|---|---|
| 💥 Kill a worker | `docker kill` one worker mid-run | worker count drops; in-flight run is recovered (RabbitMQ redelivery + reaper) and still finishes |
| Stop Redis | stops the events-OUT layer | live push stops, but events **keep arriving** via the Mongo poll fallback — nothing lost |
| Stop Mongo | stops the source of truth | new sends get a `503`; existing streams pause; **Start Mongo** and it resumes |
| Stop RabbitMQ | stops job intake | `POST /runs` fails fast; **Start RabbitMQ** and sends work again |
| Scale workers | `--scale worker=N` | queue drains faster/slower; pickup latency moves |

> The web tier runs on the host because the chaos/ops endpoints shell out to the
> Docker CLI. You can also run everything in compose
> (`docker compose up -d --build`), but then chaos is disabled in the container
> (`CHAOS_ENABLED=0`) — use the host web tier for the full experience.

## Quick start (CLI, no UI)

```bash
docker compose up -d rabbitmq mongo redis
docker compose up -d --build --scale worker=3 worker

curl -XPOST localhost:3000/runs -H 'content-type: application/json' -d '{"threadId":"t1"}'
npm run sse -- <runId>
```

## Steps (build order) & how to verify each

| Step | Proves | Verify |
|---|---|---|
| 0 | Audit / scope | [`docs/step0-audit.md`](docs/step0-audit.md) |
| 1 | Controllable fake workload | `npm run unit:mocks` |
| 2 | One run flows e2e (queue + Mongo) | `npm run worker` + `npm run publish:one` |
| 3 | Worker is independent | `docker build -f Dockerfile.worker .` |
| 4 | Real container lifecycle | `SANDBOX_ENABLED=1 npm run sandbox:check` |
| 5 | Per-thread ordering | `npm run publish:burst -- <thread> 3` then `npm run verify:ordering -- <thread>` |
| 6 | Scaling + live + reconnect | `docker compose up --scale worker=3`; `npm run sse` |
| 7 | Pressure + measurement | `npm run loadtest -- --users 100 --label w3-u100` |
| 8 | Where it breaks first | `bash scripts/run-matrix.sh`; `npm run charts`; `bash scripts/durability-test.sh`; `npm run index:probe` |

## Key scripts

- `npm run loadtest -- --users N --label L` — N concurrent users (some fire a
  2nd same-thread message, some disconnect/reconnect). Writes
  `loadtest-results/L/{runs,queue,mongo}.csv` + `summary.json`.
- `bash scripts/run-matrix.sh` — `{workers}×{users}` matrix.
- `npm run charts` — regenerate SVG charts in `docs/charts/` from the summaries.
- `bash scripts/durability-test.sh` — kill a worker mid-run, prove recovery.
- `npm run index:probe` / `MONGO_SKIP_INDEXES=1 npm run index:probe` — the
  indexed-vs-not regression.

## Results

Open [`docs/dashboard.html`](docs/dashboard.html) in a browser, or read
[`docs/REPORT.md`](docs/REPORT.md). Raw per-cell data under `loadtest-results/`.
# scaling-poc
