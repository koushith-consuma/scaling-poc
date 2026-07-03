import { randomUUID } from 'node:crypto';
import { connectQueue, parseJob, publishJob } from './lib/queue.js';
import { claimRun } from './lib/claimRun.js';
import { runLoop, type Sandbox } from './lib/runLoop.js';
import { getMongo, closeMongo } from './lib/mongo.js';
import { config } from './config.js';
import { tryAcquireThread, releaseThread } from './lib/threadGuard.js';
import { createSandbox } from './sandbox/orchestrator.js';
import { setLivePublisher } from './lib/emitEvent.js';
import { createRedisPublisher } from './lib/redisPub.js';
import { startReaper } from './lib/reaper.js';
import { WorkerHeartbeatTracker } from './lib/workerHeartbeat.js';
import { createShutdownHandler, type ShutdownHandler } from './lib/gracefulShutdown.js';
import { createRunTimeout } from './lib/runTimeout.js';

/**
 * Standalone worker. No web-framework import. Runs the agent loop behind the
 * queue. Replicate this process to scale.
 *
 *   connect → prefetch(1) → consume(agent-run-queue):
 *     - per-thread guard: if the thread has an active run, requeue (don't block)
 *     - claim run (atomic pending → running)
 *     - drive the loop (mockModel + mockTool), emitting each step to Mongo
 *     - ack on success; nack+requeue on failure
 */
const workerId = `worker-${process.pid}-${randomUUID().slice(0, 8)}`;

async function main() {
  await getMongo(); // connect + ensure indexes

  // Step 6: live event layer (Redis). Opt-in; fire-and-forget; safe if down.
  const redisPub = config.redisEnabled
    ? await createRedisPublisher().catch((e) => {
        console.warn(`[${workerId}] redis publisher unavailable:`, (e as Error).message);
        return null;
      })
    : null;
  if (redisPub) setLivePublisher(redisPub.publish);

  // Worker heartbeat tracker (shows which worker is processing what in real-time)
  const heartbeat = new WorkerHeartbeatTracker(workerId);
  await heartbeat.start();

  // Step 4: real sandbox if enabled, else a no-op stub sandbox.
  const sandbox: Sandbox | undefined = config.sandboxEnabled ? await createSandbox() : undefined;

  // Step 8 durability: opt-in crash-recovery reaper. Enable on at least one
  // worker via REAPER_ENABLED=1 (safe to run on several — resets are atomic).
  const reaper = process.env.REAPER_ENABLED === '1'
    ? startReaper({ leaseTtlMs: Number(process.env.REAPER_LEASE_MS ?? 15000) })
    : null;
  if (reaper) console.log(`[${workerId}] reaper enabled`);

  // Resilient connect + consume. On a RabbitMQ blip (heartbeat timeout / broker
  // restart, e.g. the chaos "Stop RabbitMQ" test) the connection closes; instead
  // of crashing, we reconnect and re-consume after a short backoff.
  let channel!: Awaited<ReturnType<typeof connectQueue>>['channel'];
  let shutdownHandler!: ShutdownHandler;

  // Keep retrying a connect until it succeeds (RabbitMQ may still be booting
  // after a restart → ECONNREFUSED for a few seconds). Backs off, never gives up.
  const connectWithRetry = async (): Promise<Awaited<ReturnType<typeof connectQueue>>> => {
    let attempt = 0;
    for (;;) {
      if (shutdownHandler?.isShuttingDown()) throw new Error('shutting down');
      try {
        return await connectQueue();
      } catch (e) {
        attempt++;
        const wait = Math.min(1000 * attempt, 5000);
        console.warn(`[${workerId}] connect attempt ${attempt} failed (${(e as Error).message}); retrying in ${wait}ms`);
        await new Promise((r) => setTimeout(r, wait));
      }
    }
  };

  const connectAndConsume = async (): Promise<void> => {
    const c = await connectWithRetry();
    channel = c.channel;
    await channel.prefetch(config.workerPrefetch);

    const consumerResult = await channel.consume(config.runQueue, async (msg) => {
      if (!msg) return;

      // Don't accept new work during shutdown
      if (shutdownHandler?.isShuttingDown()) {
        console.log(`[${workerId}] Shutting down, nacking message`);
        channel.nack(msg, false, true); // Requeue for another worker
        return;
      }

      let job;
      try {
        job = parseJob(msg.content);
      } catch {
        // Poison message — drop it.
        channel.ack(msg);
        return;
      }
      const { runId, threadId} = job;

      // Track in-flight for graceful shutdown
      shutdownHandler?.markRunning(runId);

    // --- Per-thread serialization guard (Step 5) ---
    const acquired = await tryAcquireThread(threadId, runId);
    if (!acquired) {
      // Thread busy: requeue with a short delay. Do NOT block this worker.
      channel.ack(msg); // remove original...
      await publishJob(channel, job, 250); // ...and re-drop shortly.
      return;
    }

    try {
      const claimed = await claimRun(runId, workerId);
      if (!claimed) {
        // Already claimed / not pending (dup delivery). Drop.
        channel.ack(msg);
        return;
      }

      // Timeout + cancellation: abort signal shared between both mechanisms
      const timeout = createRunTimeout(config.runTimeoutMs);
      const { runs } = await getMongo();

      // Poll MongoDB for external cancellation (user hit POST /runs/:id/cancel)
      const cancelPoll = setInterval(async () => {
        try {
          const doc = await runs.findOne({ _id: runId }, { projection: { status: 1 } });
          if (doc?.status === 'cancelled') timeout.controller.abort('cancelled');
        } catch { /* ignore transient errors */ }
      }, 2000);

      await heartbeat.markProcessing(runId, threadId);
      const result = await runLoop(job, {
        modelConfig: config.model,
        toolConfig: config.tool,
        sandbox,
        workerId,
        signal: timeout.controller.signal,
        timeoutMs: config.runTimeoutMs,
      });
      timeout.clear();
      clearInterval(cancelPoll);
      await heartbeat.markIdle();

      if (result.status === 'done') {
        channel.ack(msg);
        console.log(`[${workerId}] run ${runId} done (${result.turns} turns, ${result.toolCalls} tools)`);
      } else {
        channel.nack(msg, false, true); // requeue
        console.warn(`[${workerId}] run ${runId} failed → requeued`);
      }
    } catch (err) {
      console.error(`[${workerId}] error on ${runId}:`, (err as Error).message);
      try { channel.nack(msg, false, true); } catch { /* channel gone */ }
    } finally {
      await releaseThread(threadId, runId).catch(() => {});
      shutdownHandler?.markComplete(runId);
    }
    });

    // Create shutdown handler AFTER consumer is set up
    const consumerTag = consumerResult.consumerTag;
    shutdownHandler = createShutdownHandler(workerId, channel, c.conn, consumerTag);

    let reconnected = false;
    const onGone = () => {
      if (shutdownHandler?.isShuttingDown() || reconnected) return;
      reconnected = true;
      console.warn(`[${workerId}] RabbitMQ connection lost — reconnecting…`);
      setTimeout(() => connectAndConsume().catch((e) => console.error(`[${workerId}] reconnect loop error:`, e.message)), 1000);
    };
    c.conn.on('close', onGone);
    c.conn.on('error', onGone);
    console.log(`[${workerId}] up. queue=${config.runQueue} prefetch=${config.workerPrefetch} sandbox=${config.sandboxEnabled}`);
  };

  await connectAndConsume();

  const shutdown = async () => {
    // Graceful shutdown: finish in-flight work, then exit
    await shutdownHandler?.initiate();

    reaper?.stop();
    await heartbeat.stop();
    await redisPub?.close();
    await sandbox?.shutdown?.();
    await closeMongo();
    process.exit(0);
  };

  // SIGTERM = K8s/Docker sending shutdown signal
  // SIGINT = Ctrl+C in terminal
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((e) => {
  console.error(`[${workerId}] fatal:`, e);
  process.exit(1);
});
