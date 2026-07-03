# Scaling & Worker Internals

How auto-scaling works, how RabbitMQ dispatches messages, and how Node.js workers handle concurrent runs.

---

## Auto-Scaling

### Trigger: Queue Depth

Workers auto-scale based on `rabbitmq_queue_messages_ready` — the number of messages sitting in `agent-run-queue` that no worker has picked up yet.

### The Flow

1. User sends a message → API creates a run doc (MongoDB) + publishes a job to RabbitMQ
2. Workers consume jobs with `prefetch=20` — each worker can hold up to 20 unacked messages at once
3. When all workers are saturated (all prefetch slots full), messages pile up as "ready" in the queue
4. KEDA (or HPA) polls that metric every few seconds
5. If `ready > threshold × current_pods`, it scales up new worker pods
6. New workers connect → RabbitMQ immediately dispatches queued messages to them
7. When queue drains below threshold, KEDA scales down (sends SIGTERM → graceful shutdown)

### The Formula

```
workers needed = messages_ready / target_per_pod
```

In the KEDA config, `value: "20"` means: "scale so each pod has ~20 ready messages." So if there are 200 messages queued and you have 5 workers, KEDA scales to 10.

### Scale-Down Safety

- KEDA sends SIGTERM to excess pods
- Worker catches it → stops accepting new messages (`channel.cancel`)
- Finishes in-flight runs (up to 28s grace period)
- ACKs completed work → clean exit
- Any unACKed messages automatically redeliver to remaining workers

### KEDA Configuration

```yaml
triggers:
- type: rabbitmq
  metadata:
    queueName: agent-run-queue
    value: "20"           # scale when > 20 msgs per pod
minReplicaCount: 10
maxReplicaCount: 1000
```

### Cost Projections

| Traffic | Workers | Cost/mo |
|---------|---------|---------|
| < 200 concurrent | 10 | ~$400 |
| 200-500 | 20-30 | ~$1K |
| 500-2K | 50-100 | ~$2-4K |
| 2K-10K | 200-500 | ~$7-15K |

---

## RabbitMQ Dispatch Algorithms

### What This System Uses: Fair Dispatch

Not pure round-robin. With `prefetch=20`, RabbitMQ sends messages to whichever consumer has free prefetch slots. A slow worker fills its 20 slots and stops getting messages until it ACKs. A fast worker keeps getting fed.

### All Strategies

| Strategy | How | When to use |
|----------|-----|-------------|
| **Round-robin** | No prefetch set — alternates 1-2-3-1-2-3 regardless of speed | All consumers equally fast, uniform work |
| **Fair dispatch** (ours) | `prefetch(N)` — only sends if consumer has capacity | Variable work duration (our case — runs take 1-10s) |
| **Priority queues** | `x-max-priority` on queue — higher priority messages dispatched first | Paid tier before free tier |
| **Consistent hash exchange** | Route by hash of routing key — same key always goes to same worker | Stateful consumers needing affinity |
| **Single active consumer** | Only one consumer receives at a time, failover on disconnect | Strict global ordering |
| **Streams** (RabbitMQ 3.9+) | Offset-based like Kafka — consumers read from position, messages persist | Replay, audit logs, multi-consumer fan-out |

### Why Fair Dispatch Is Right Here

Runs have variable duration (1-10s depending on turns). Pure round-robin would overload slow workers while fast ones sit idle. With `prefetch=20`, a worker processing a 10-second run stops getting new work once its 20 slots fill, while a worker finishing quick 1-second runs keeps pulling.

If we wanted affinity (same thread → same worker for cache locality), we'd use consistent-hash exchange on `threadId`. But workers are stateless — thread ordering is handled by the MongoDB thread lock, not by routing.

---

## Worker Concurrency: How 20 Runs Execute In Parallel On One Node.js Process

### The Misconception

> "Node.js is single-threaded. If a worker gets 20 messages, don't the other 19 wait until the first one finishes?"

No. All 20 run **concurrently**. Here's why.

### What `channel.consume` Does

The consume callback is `async`. Every `await` inside it yields back to the event loop instantly:

```
Message 1 arrives → async callback fires → hits first await → SUSPENDS (~0.01ms)
Message 2 arrives → async callback fires → hits first await → SUSPENDS (~0.01ms)
Message 3 arrives → async callback fires → hits first await → SUSPENDS (~0.01ms)
...
All 20 fire in ~0.2ms total
```

Each message only occupies the thread for **microseconds** (until its first `await`). Then it yields. The continuation (code after the await) re-enters the microtask queue when the I/O response arrives.

### What `await` Actually Means

`await` does NOT mean "block here until response comes."

It means: **"Suspend this function, free the event loop, wake me up when the result is ready."**

```
t=0ms       msg1: await fetch(openai) → sends HTTP request → SUSPENDS
t=0.1ms     msg2: await fetch(openai) → sends HTTP request → SUSPENDS
t=0.2ms     msg3: await fetch(openai) → sends HTTP request → SUSPENDS
...
t=2ms       all 20 HTTP requests are IN FLIGHT simultaneously on the network

            EVENT LOOP IS IDLE. All 20 are waiting on remote servers.

t=10,000ms  OpenAI responds to msg7 → continuation enters microtask queue → runs
t=10,001ms  msg7 finishes processing, ACKs
t=12,000ms  OpenAI responds to msg1 → runs
t=13,000ms  OpenAI responds to msg15 → runs
```

All 20 HTTP requests are in-flight **at the same time**. The OS network stack handles multiple simultaneous TCP connections. Node's libuv layer tracks all open sockets and notifies which one has data ready.

### The Uber Eats Analogy

Ordering food from 20 restaurants simultaneously: you don't wait for restaurant 1 to deliver before placing order 2. You place all 20 orders immediately, then sit idle until deliveries arrive one by one. Each delivery is handled as it arrives, independently.

### Where The Time Actually Goes

An LLM call takes 10-20 seconds **wall clock time**, but that time is spent on OpenAI/Anthropic's GPU servers — not on your CPU:

| Step | Time | Event loop busy? |
|------|------|-----------------|
| Serialize request, hand to OS network stack | ~0.1ms | Yes |
| Packet on the wire, remote GPU crunching | 10-20s | **No — idle and free** |
| Response arrives, libuv signals, callback enters queue | ~1ms | Yes |

Total event loop occupation per LLM call: **~1ms**, regardless of how long the remote server takes to respond.

### The Same Applies To Mock Delays

```javascript
await new Promise(resolve => setTimeout(resolve, 800));
```

`setTimeout` registers a timer with libuv and **immediately returns**. The thread is free for 800ms. Same mechanism.

### Timeline of 20 Concurrent Runs (Real)

```
t=0ms       msg1:  claimRun() → Mongo network I/O → suspends
t=0ms       msg2:  claimRun() → Mongo network I/O → suspends
t=0ms       msg3:  claimRun() → Mongo network I/O → suspends
            ...all 20 in flight

t=5ms       msg1:  claim response back → enters runLoop → mockModel(800ms) → suspends
t=5ms       msg2:  claim response back → enters runLoop → mockModel(500ms) → suspends

t=505ms     msg2:  model done → emitEvent() → Mongo write → suspends
t=800ms     msg1:  model done → emitEvent() → Mongo write → suspends
            ...all interleaved naturally
```

### When This Breaks

If the LLM ran **locally on the same process** — actual matrix multiplication on your CPU:

```javascript
// THIS would block for 10-20s — synchronous CPU work
const result = localLlama.inference(prompt); // no await helps here
```

That would starve the event loop. But:
- HTTP calls to external APIs (OpenAI, Anthropic) → network I/O → non-blocking
- MongoDB queries → network I/O → non-blocking
- Redis publishes → network I/O → non-blocking
- setTimeout delays → timer → non-blocking

Our worker is 100% I/O-bound. One process with prefetch=20 handles 20 concurrent runs just as well as 20 separate processes with prefetch=1 — actually better due to less memory overhead.

---

## Summary

| Layer | Mechanism | Handles |
|-------|-----------|---------|
| **Cluster** | KEDA watches queue depth | Adding/removing worker pods |
| **RabbitMQ** | Fair dispatch (prefetch) | Distributing messages to workers with capacity |
| **Worker (Node.js)** | async/await + event loop | 20 concurrent runs on one thread via non-blocking I/O |
| **OS kernel** | epoll/kqueue + TCP stack | Tracking thousands of simultaneous network connections |

The whole stack is designed so that "waiting for a response" costs zero CPU. Waiting happens at the OS/network level, not in your code.
