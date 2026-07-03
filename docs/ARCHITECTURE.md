# Architecture

## System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CLIENT (Next.js :3001)                             │
│  POST /runs → SSE /runs/:id/stream → POST /runs/:id/cancel                 │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         API (Express :3000)                                   │
│                                                                             │
│  ┌────────────┐  ┌───────────┐  ┌──────────────┐  ┌─────────────────────┐ │
│  │Rate Limiter│─▶│POST /runs │─▶│Create Run    │─▶│Publish to RabbitMQ  │ │
│  │60 req/min  │  │           │  │(MongoDB)     │  │                     │ │
│  └────────────┘  └───────────┘  └──────────────┘  └─────────────────────┘ │
│                                                                             │
│  POST /runs/:id/cancel │ GET /runs/:id/stream (SSE backfill + Redis live)  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      RABBITMQ (durable queue)                                │
│  agent-run-queue │ prefetch: 20/worker │ persistent │ unacked → redeliver   │
└─────────┬────────────────────┬────────────────────┬─────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌────────────────┐  ┌────────────────┐  ┌────────────────┐
│   WORKER 1     │  │   WORKER 2     │  │   WORKER N     │
│                │  │                │  │                │
│ Thread Lock    │  │ Thread Lock    │  │ Thread Lock    │
│ Idempotency   │  │ Idempotency   │  │ Idempotency   │
│ Run Loop       │  │ Run Loop       │  │ Run Loop       │
│  + Timeout     │  │  + Timeout     │  │  + Timeout     │
│  + Cancel Poll │  │  + Cancel Poll │  │  + Cancel Poll │
│ Graceful Shtdn │  │ Graceful Shtdn │  │ Graceful Shtdn │
└────────┬───────┘  └────────┬───────┘  └────────┬───────┘
         └───────────────────┼───────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  MongoDB (state)          Redis (live events)       Reaper (background)     │
│  • runs, events           • pub/sub per run         • scans stale >15s      │
│  • thread_locks           • worker heartbeats       • resets + republishes  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Mechanisms

### Thread Lock (per-thread ordering)

MongoDB `thread_locks` collection. `_id` = threadId, holds current `runId`.

- Worker acquires lock atomically before processing
- If locked by another run → requeue with 250ms delay (worker stays free)
- Released after run completes or on crash (reaper cleans it)
- Guarantees: messages in same thread always process sequentially

### Idempotency (no duplicate side effects)

On re-execution after crash recovery, the run loop queries `events` collection for already-completed turns. Turns with existing `tool_result` events are skipped entirely.

### Run Timeout

AbortController fires after `RUN_TIMEOUT_MS` (default 2 min). Checked cooperatively between turns. Run marked "failed" with reason "timed out".

### Run Cancellation

`POST /runs/:id/cancel` sets MongoDB status to "cancelled". Worker polls every 2s, aborts signal on detection. Run marked "cancelled" at next turn boundary.

### Rate Limiting

Sliding window per IP on `POST /runs`. Default 60 requests/minute. Returns 429 with `Retry-After` header.

### Graceful Shutdown

SIGTERM → stop accepting new messages → wait for in-flight (up to 28s) → ACK/NACK → clean exit. No orphaned runs on planned scale-down.

### Crash Recovery (Reaper)

Background loop (every 5s) finds runs with `status=running` and `updatedAt` older than 15s. Resets to pending, releases thread lock, republishes to queue.

### RabbitMQ Redelivery

Unacked messages automatically redeliver when worker disconnects. Belt-and-suspenders with the reaper.

---

## Safety Matrix

| Threat | Protection | Recovery |
|--------|-----------|----------|
| Client flood | Rate limiter (60/min) | 429 + Retry-After |
| Run hangs | Timeout (2 min) | Abort → failed |
| User wants to stop | Cancel endpoint | Abort → cancelled |
| Worker crash (graceful) | Graceful shutdown | Finish in-flight + clean exit |
| Worker crash (hard kill) | Reaper + RabbitMQ redeliver | Reset + republish (15-20s) |
| Double execution | Idempotency guard | Skip completed turns |
| Thread ordering | Thread lock | Requeue + retry |
| Queue overload | Auto-scale (KEDA/HPA) | Spin up workers |

---

## Auto-Scaling

Workers scale on queue depth. KEDA or K8s HPA watches `rabbitmq_queue_messages_ready`.

```yaml
# KEDA ScaledObject (the whole thing)
triggers:
- type: rabbitmq
  metadata:
    queueName: agent-run-queue
    value: "20"  # scale when > 20 msgs per pod
minReplicaCount: 10
maxReplicaCount: 1000
```

**Formula:** `workers needed = peak concurrent users / prefetch`

| Traffic | Workers | Cost/mo |
|---------|---------|---------|
| < 200 concurrent | 10 | ~$400 |
| 200-500 | 20-30 | ~$1K |
| 500-2K | 50-100 | ~$2-4K |
| 2K-10K | 200-500 | ~$7-15K |

Cost-saving: queue position display, priority tiers, rate limiting, off-peak batching.

---

## File Map

```
backend/src/
├── api/server.ts          API: rate limit, cancel, SSE
├── worker.ts              Worker process (all wired together)
├── config.ts              Env-driven config
├── lib/
│   ├── rateLimiter.ts     Sliding window rate limiter
│   ├── runTimeout.ts      Per-run timeout (AbortController)
│   ├── idempotency.ts     Skip already-executed turns
│   ├── threadGuard.ts     Thread lock (distributed mutex)
│   ├── gracefulShutdown.ts SIGTERM handler
│   ├── reaper.ts          Crash recovery scanner
│   ├── workerHeartbeat.ts Redis worker status
│   ├── claimRun.ts        Atomic pending→running + finish
│   ├── queue.ts           RabbitMQ connection + publish
│   ├── runLoop.ts         Agent loop + signal + idempotency
│   ├── createRun.ts       Insert pending run doc
│   ├── emitEvent.ts       Write events to Mongo + Redis pub
│   ├── mongo.ts           MongoDB connection + indexes
│   └── redisPub.ts        Redis publisher
├── mock/
│   ├── mockModel.ts       Fake LLM (seedable delays)
│   └── mockTool.ts        Fake tool execution
├── sandbox/               Real Docker container orchestration
├── scripts/               CLI helpers + test scripts
└── loadtest/              Load harness + chart generation
```
