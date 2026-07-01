Viper scaling POC — build plan

Goal

Prove the decoupled architecture scales: agent runs execute in standalone worker
processes behind a queue, independent of any HTTP request, with per-thread
ordering, live updates, and crash recovery. Produce load-test measurements, not
just a running system.

What is mocked vs real

Mocked (only these two):


The model call — replace with a function that sleeps a randomized delay and
returns a canned decision (done or tool_call).
The tool's inner work — replace with a function that sleeps and touches a file
inside the container.


Real (everything else): RabbitMQ, standalone workers, per-thread guard,
MongoDB writes + indexed reads, Redis pub/sub, container lifecycle (real docker
spin-up / claim / release), SSE relay + reconnect backfill.

Rationale: the two mocked things are slow, costly, and nondeterministic, and are
NOT what the POC tests. Mocking them lets the whole load test run on one laptop.
Do NOT wire in the real LLM at any point in this POC.

Services (all run locally via docker-compose)


rabbitmq — job queue (jobs going IN to workers). Use the management image so
the UI is available for inspecting queue depth.
mongo — source of truth: runs, event log, checkpoints.
redis — live event pub/sub (events going OUT to clients). Fire-and-forget.
worker — standalone Node process running the agent loop. Replicated to N
copies. This is the thing being scaled.
web — thin Next.js/Express tier: publishes jobs, exposes SSE, relays events.
(S3 — SKIP for v1. Add artifact writes later; not needed to prove scaling.)



Step 0 — Pre-flight code audit (no new code)

Investigate the existing codebase and report findings. These change scope.


Mongo indexes. Check for a compound index on the event/message collection
keyed by threadId + sort key (e.g. createdAt or seq), and an index on
runId. Report whether they exist. If missing, note it — the load test will
confirm it's a bottleneck.
Consumer coupling. Locate the current consumer/agent-loop code. Report
whether it imports Next-specific runtime (request/response objects, Next
server internals). If clean, extraction is a file move. If tangled into route
handlers, extraction is real work in Step 3.
Streaming. Trace whether the current model call streams tokens out through
the event-emit path, or waits for the full response before emitting. Report
which. (Doesn't block the POC; informs a later perf decision.)


Deliverable: a short findings note answering the three above.


Step 1 — Mock model + mock tool modules

Build the two fakes in isolation first; everything downstream consumes them.

Files:


mock/mockModel.ts — export mockModel(context, config) that:

sleeps a randomized delay (default 500–3000ms) to mimic model latency
returns { type: 'tool_call', tool: 'noop' } or { type: 'done' }
is driven by config: avgTurns (how many turns before done) and
toolCallProbability (chance each turn is a tool call vs plain response)
is seedable/deterministic when given a seed, so load runs are reproducible



mock/mockTool.ts — export mockTool(container, config) that:

sleeps a randomized delay
writes/touches a file in the container's workspace
returns a canned result string





Acceptance: unit-call both functions in a script; confirm delays, canned
outputs, and that avgTurns/toolCallProbability visibly change behavior.


Step 2 — Thinnest vertical slice: 1 worker + real queue + real Mongo

Prove one run flows end to end through real infra. No sandbox, no web tier yet.

Services up: rabbitmq, mongo (docker-compose, just these two).

Files:


worker.ts — standalone entrypoint:

connect to RabbitMQ, channel.prefetch(1), channel.consume('run-execute', ...)
on message { runId, threadId }: claim the run (atomic status transition
pending → running in Mongo), run the loop calling mockModel, write each
step via the real event-emit to Mongo, ack on success
on failure: nack with requeue



lib/runLoop.ts — the loop: claim → ask (mockModel) → if tool_call, call a
stubbed tool for now → emit event to Mongo → repeat until done → mark run done
lib/emitEvent.ts — real Mongo append with an incrementing per-run sequence
(lastEventSeq)
lib/claimRun.ts — atomic pending → running status transition
scripts/publishOne.ts — drops a single { runId, threadId } job on the queue


Acceptance: run worker.ts, run publishOne.ts, observe the worker claim
the job, drive the mock loop, and see events land in Mongo in correct sequence
order. One run, start to finish, through real queue + real DB.


Step 3 — Confirm/extract standalone worker

Make the worker a truly independent deployable with zero web-app coupling.


If Step 0 found coupling: pull the loop logic out of the web app into the
worker package so it has no Next imports.
If Step 0 found it clean: confirm worker.ts runs as its own process with its
own Dockerfile, fully separate from any web tier.


Files:


Dockerfile.worker — builds and runs node worker.js
worker package has its own entrypoint, its own deps, no web framework import


Acceptance: docker build -f Dockerfile.worker produces an image that runs
the worker standalone and processes a published job with no web tier present.


Step 4 — Real sandbox orchestration

Add real container lifecycle. Keep it as functions inside the worker for now
(NOT a separate service yet) behind a clean interface.

Files:


sandbox/orchestrator.ts — exports claim(runId), exec(handle, cmd),
release(handle):

maintains a warm pool of POOL_SIZE pre-booted containers (start 2–3)
claim hands over a warm container instantly and triggers a background boot
of a replacement to refill the pool
if the pool is empty on claim: for v1, boot one on demand (measure the
cold-start cost); note this as the overflow policy
exec runs a command in the claimed container via docker exec
release returns the container to a teardown/replace state at run end
container is long-lived for the whole run (per-run workspace persists across
tool calls within the run)



wire runLoop so a tool_call does: claim (once per run, reused across
calls) → exec running mockTool → result back into the loop → release at
run end


Acceptance: a run's tool calls execute in a real container; a file written by
mockTool in call 1 is visible in call 2 (same container, workspace persists);
container is released at run end; pool refills in the background. Log claim
latency (warm vs cold).


Step 5 — Per-thread serialization guard

Add the correctness guard before going multi-worker (multi-worker is what
exposes the race).

Files:


lib/threadGuard.ts — at claim time, atomically check "does this thread have
an active run?" If yes, the worker requeues the job (nack+requeue, or republish
with a short delay) and moves on — it does NOT block waiting.
default policy: a new same-thread message queues behind the active run (do NOT
cancel the in-flight run). Cancel-on-explicit-stop is out of scope for the POC.


Acceptance: publish two jobs for the SAME thread near-simultaneously; confirm
they process one-then-the-other (not concurrently), the second sees the first's
completed output, and the event log is coherently ordered with no interleaving.
Confirm a worker never blocks idle waiting — it picks up other threads' work
while a thread is busy.


Step 6 — Multi-worker + web tier + Redis + SSE

Scale to N workers and add the live path back to the client.

Services up: all of rabbitmq, mongo, redis, worker (replicas), web.

Files:


docker-compose.yml — brings up rabbitmq, mongo, redis, N worker replicas,
and web. Worker count controllable via --scale worker=N or a replicas
field.
extend emitEvent.ts — after the Mongo write, ALSO publish the event to Redis
on a per-run channel (run:{runId}). Fire-and-forget; a Redis failure must
never fail the Mongo write or the loop.
web/ thin tier:

POST /runs — write initial run (pending) to Mongo, publish job to
RabbitMQ, respond immediately (do NOT wait for the run)
GET /runs/:id/stream — SSE endpoint: subscribe to the run's Redis channel
for live push AND poll Mongo on an interval as the reliable fallback; on
connect, backfill from lastEventSeq (query Mongo for everything since the
client's last-seen seq), then live-tail



web subscribes to Redis per-run channels only for runs it is actively
streaming (not a global channel).


Acceptance:


run docker compose up --scale worker=3; publish jobs; confirm RabbitMQ
distributes across all 3 workers and the per-thread guard still holds
a client hits the SSE endpoint, sees live updates as the worker emits them
disconnect mid-run and reconnect: confirm backfill from lastEventSeq returns
the missed events, then live updates resume
kill Redis mid-run: confirm the client still receives events via the Mongo poll
(slower, but nothing lost)



Step 7 — Load harness

Generate the "100 users" pressure and instrument everything.

Files:


loadtest/harness.ts — spawns N concurrent simulated users. Each user:

starts a run (POST /runs)
with some probability, fires a SECOND message into the SAME thread (exercises
the guard)
with some probability, disconnects and reconnects mid-run (exercises backfill)
ramps: run the harness at 10, 50, 100, 200 concurrent users



loadtest/metrics.ts — record and export:

timestamp job-published, job-claimed, run-completed (→ time-to-pickup, run
duration)
queue depth sampled on an interval (from RabbitMQ management API)
Mongo query latency + connection-pool saturation
sandbox claim latency (warm vs exhausted) + pool occupancy





Acceptance: harness runs against the docker-compose stack at each user level
and emits a metrics file/CSV per run.


Step 8 — Experiments + report

Run the matrix and produce the numbers. This is the actual deliverable.

Run the harness at each {worker replicas} × {concurrent users} combination and
capture:


time-to-pickup (p50/p95) vs worker replica count, per user level — the
headline curve.
queue depth over time — does it drain or grow unbounded, per worker count.
sandbox: peak concurrent container demand vs pool size; claim latency warm
vs exhausted.
Mongo latency under peak load — this is where a missing index shows up.
serialization correctness — same-thread bursts stay coherently ordered.
durability — kill a worker mid-run during load; confirm the run recovers
and finishes (this exercises whether the crash-recovery / reaper piece is
needed and works).


Deliverable: a short report with the curves and, most importantly, a
statement of WHICH LAYER BREAKS FIRST (predicted: Mongo indexes or sandbox pool
sizing, not the queue/worker layer). Finding the first bottleneck is the point,
not proving it scales.


Build order summary

StepAddsServices upProves0audit—scope of the work1mock model + tool—controllable fake workload2worker + queue + Mongorabbitmq, mongoone run flows e2e3standalone extractionrabbitmq, mongoworker is independent4real sandbox+ dockercontainer lifecycle under a run5per-thread guardrabbitmq, mongoordering correctness6multi-worker + web + Redis + SSEallscaling + live + reconnect7load harnessallgenerate pressure + measure8experimentsallwhere it breaks first

Rules for the agent


Add ONE real component per step; keep the previous step runnable.
NEVER wire in the real LLM. The model call stays mocked for the entire POC.
Do NOT build the sandbox orchestrator as a separate service — keep it as
functions in the worker behind a claim/exec/release interface. Extraction to
a service is out of scope.
Redis is fire-and-forget: a Redis failure must never fail a Mongo write or the
loop. Mongo is the source of truth; Redis is a speed layer.
RabbitMQ carries jobs IN; Redis carries events OUT. Do not conflate them.
The per-thread guard requeues, it does NOT block a worker idle.
The deliverable is measurements, not a demo.