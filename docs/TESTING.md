# How to test the Viper POC

A complete, copy-paste guide. Two ways to test:

- **A. Interactive app** — a Next.js chat UI + ops panel + chaos buttons. Best
  for *seeing* the system work and breaking it by hand.
- **B. CLI / automated** — scripts that prove each guarantee and produce the
  load-test numbers. Best for *evidence*.

Everything is currently **stopped**. Start from a clean slate below.

---

## 0. Prerequisites

- Docker Desktop running (`docker version` works)
- Node 20+ (`node -v`)
- Free ports: **3000** (backend web tier), **3001** (Next.js UI),
  **5672 / 15672** (RabbitMQ + its UI), **27017** (Mongo), **6379** (Redis)

```bash
cd "/Users/consuma/Desktop/consuma/pos's"
npm install                 # backend deps (first time only)
```

---

## A. Interactive app (chat + ops + chaos)

### A1. Start it — 3 terminals

**Terminal 1 — infra + workers** (workers are their own scaled service):
```bash
cd "/Users/consuma/Desktop/consuma/pos's"
docker compose up -d rabbitmq mongo redis
docker compose up -d --build --scale worker=3 worker
docker compose ps            # all should be "running"/"healthy"
```

**Terminal 2 — backend web tier (runs on the HOST on purpose):**
```bash
cd "/Users/consuma/Desktop/consuma/pos's"
CHAOS_ENABLED=1 npm run web
# → [web] interactive app on http://localhost:3000
# → [web] chaos ENABLED (project=poss)
```
> It runs on the host (not in a container) because the chaos/ops buttons shell
> out to the Docker CLI to kill/scale containers. That's simpler and safer than
> mounting the Docker socket.

**Terminal 3 — Next.js chat frontend:**
```bash
cd "/Users/consuma/Desktop/consuma/pos's/frontend"
npm install                  # first time only
npm run dev
# → http://localhost:3001
```

**Open http://localhost:3001.**

### A2. Test scenarios (do these in order)

Each row: what to do → what proves the architecture works.

| # | Do this | Expect to see | Proves |
|---|---------|---------------|--------|
| 1 | Type “hello agent”, hit **Send** | assistant bubble goes `pending → running → done`; steps (`tool_call`, `tool_result`) stream in live | full pipeline round-trip: browser → web → RabbitMQ → worker → Mongo → Redis → SSE → browser |
| 2 | Click **“×3 same thread”** | three runs on the same thread finish **one at a time**, never overlapping | per-thread serialization guard |
| 3 | Set **Scale workers = 1**, click Apply, then “×3 same thread” a few times; watch **queue depth** tile | queue depth climbs then drains; runs still all finish | queue backpressure + workers as the scaling dial |
| 4 | Set **Scale workers = 3** again | queue drains faster; pickup speeds up | linear scaling |
| 5 | Send a message, and **while it’s running** click **💥 Kill a worker** | workers tile drops (e.g. 3→2); the in-flight run still reaches **done** | crash recovery (RabbitMQ redelivery + reaper) |
| 6 | Click **Stop Redis**, then Send a message | live push stops, but the steps still appear (a beat slower); run completes | Redis is a speed layer; Mongo poll fallback loses nothing |
| 7 | Click **Start Redis** | health dot goes green; live push resumes | graceful recovery |
| 8 | Click **Stop Mongo**, then Send | send fails (`503` in event log); health dot red | Mongo is the source of truth — nothing proceeds without it |
| 9 | Click **Start Mongo**, Send again | works again | recovery |
| 10 | Click **Stop RabbitMQ**, then Send | send fails fast (can’t publish job) | RabbitMQ is job intake |
| 11 | Click **Start RabbitMQ** | sends work again | recovery |

While testing, watch the **right-hand panel**: service-health dots, worker count,
queue depth, and running/done/pending/failed counts all update live (1s).

### A3. Cross-check the raw infrastructure (optional)

- **RabbitMQ UI:** http://localhost:15672 (guest/guest) → Queues → `run-execute`
  shows depth + consumers.
- **Mongo:**
  ```bash
  docker exec poss-mongo-1 mongosh viper --quiet --eval \
    'printjson(db.runs.find().sort({createdAt:-1}).limit(3).toArray())'
  ```

### A4. Stop the interactive app
```bash
# Ctrl-C in terminals 2 and 3, then:
docker compose down          # keeps Mongo data
docker compose down -v       # also wipes Mongo data (clean slate)
```

---

## B. CLI / automated tests (evidence per guarantee)

Only infra is needed for most of these:
```bash
docker compose up -d rabbitmq mongo redis
```

### B1. Mocks behave (Step 1)
```bash
npm run unit:mocks
```
Confirms: deterministic with a seed, `avgTurns` changes run length,
`toolCallProbability` changes tool mix, delays are real.

### B2. One run end-to-end (Step 2)
```bash
# terminal 1
npm run worker
# terminal 2
npm run publish:one -- demo-thread 123
# then inspect ordering in Mongo:
docker exec poss-mongo-1 mongosh viper --quiet --eval \
  'db.events.find({},{seq:1,type:1,_id:0}).sort({seq:1}).limit(20).forEach(e=>print(e.seq+"  "+e.type))'
```
Expect events in exact seq order, run marked `done`.

### B3. Worker is a standalone deployable (Step 3)
```bash
docker build -f Dockerfile.worker -t viper-worker:poc .
docker run --rm --network poss_default \
  -e RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672 \
  -e MONGO_URL=mongodb://mongo:27017 viper-worker:poc &
npm run publish:one -- standalone-thread
# the container worker (no web tier) processes the job
```

### B4. Real Docker sandbox lifecycle (Step 4)
```bash
SANDBOX_ENABLED=1 POOL_SIZE=2 npm run sandbox:check
```
Proves: warm claim ~0ms, cold claim (~190ms) on pool exhaustion, file written in
call 1 is readable in call 2 (workspace persists), container released at run end.

### B5. Per-thread ordering under multiple workers (Step 5)
```bash
# start 3 workers
for i in 1 2 3; do npm run worker & done
# fire 3 jobs at the SAME thread at once
npm run publish:burst -- race-thread 3
sleep 15
# verify no interleaving / no overlap
npm run verify:ordering -- race-thread
```
Expect: `window overlap: none — serialized OK`,
`event interleaving: none — coherently ordered OK`.

### B6. Live + reconnect backfill + Redis-down fallback (Step 6)
```bash
docker compose up -d redis
REDIS_ENABLED=1 npm run web            # terminal
REDIS_ENABLED=1 npm run worker         # terminal(s)

# live tail:
RUN=$(curl -s -XPOST localhost:3000/runs -H 'content-type: application/json' -d '{}' \
      | sed 's/.*"runId":"//;s/".*//'); npm run sse -- "$RUN"

# reconnect backfill: disconnect at ~3s then resume from a seq:
npm run sse -- "$RUN" 0 3000      # drops at 3s, prints maxSeq
npm run sse -- "$RUN" <maxSeq>    # resumes with no gap, tails to done
```

### B7. Load test — the numbers (Step 7)
```bash
docker compose up -d --scale worker=3 worker
npm run loadtest -- --users 100 --label w3-u100 --ramp-ms 3000
# writes loadtest-results/w3-u100/{runs,queue,mongo}.csv + summary.json
```

### B8. Full matrix + charts + report (Step 8)
```bash
WORKERS="1 3" USERS="10 50 100" bash scripts/run-matrix.sh
npm run charts                          # → docs/charts/*.svg
open docs/dashboard.html                # visual summary
```

### B9. Crash recovery (durability)
```bash
bash scripts/durability-test.sh
# publishes a long run, kill -9's the worker mid-run, starts a fresh one,
# and asserts the run is recovered and finishes. Prints DURABILITY: PASS.
```

### B10. The "which layer breaks first" experiment (indexes)
```bash
npm run index:probe                      # indexed baseline (sub-ms)
MONGO_SKIP_INDEXES=1 npm run index:probe # drops indexes → ~40x slower backfill
```

---

## Quick reference

| Thing | Where |
|---|---|
| Chat UI | http://localhost:3001 |
| Backend API + SSE | http://localhost:3000 |
| RabbitMQ UI | http://localhost:15672 (guest/guest) |
| Health JSON | `curl localhost:3000/api/health` |
| Live stats JSON | `curl localhost:3000/api/stats` |
| Results / charts | `docs/dashboard.html`, `docs/REPORT.md`, `loadtest-results/` |

## Troubleshooting

- **UI health dots all red / workers 0** — the backend web tier (terminal 2)
  isn’t running, or you started it without `CHAOS_ENABLED=1`. Ops needs the host
  web tier.
- **Chaos buttons say “chaos disabled”** — start the web tier with
  `CHAOS_ENABLED=1 npm run web` on the host.
- **`POST /runs` returns 503** — Mongo or RabbitMQ is down (maybe from a chaos
  test). Bring them back: `docker compose start mongo rabbitmq`.
- **Port already in use** — something didn’t shut down:
  `lsof -ti :3000 :3001 | xargs kill`, then `docker compose down`.
- **Full reset** — `docker compose down -v` (wipes Mongo data too).
