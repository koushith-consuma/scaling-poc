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

/**
 * Standalone worker. No web-framework import. Runs the agent loop behind the
 * queue. Replicate this process to scale.
 *
 *   connect → prefetch(1) → consume(run-execute):
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

  // Step 4: real sandbox if enabled, else a no-op stub sandbox.
  const sandbox: Sandbox | undefined = config.sandboxEnabled ? await createSandbox() : undefined;

  const { conn, channel } = await connectQueue();
  await channel.prefetch(config.workerPrefetch);

  // Step 8 durability: opt-in crash-recovery reaper. Enable on at least one
  // worker via REAPER_ENABLED=1 (safe to run on several — resets are atomic).
  const reaper = process.env.REAPER_ENABLED === '1'
    ? startReaper({ leaseTtlMs: Number(process.env.REAPER_LEASE_MS ?? 15000) })
    : null;
  if (reaper) console.log(`[${workerId}] reaper enabled`);

  console.log(`[${workerId}] up. queue=${config.runQueue} prefetch=${config.workerPrefetch} sandbox=${config.sandboxEnabled}`);

  await channel.consume(config.runQueue, async (msg) => {
    if (!msg) return;
    let job;
    try {
      job = parseJob(msg.content);
    } catch {
      // Poison message — drop it.
      channel.ack(msg);
      return;
    }
    const { runId, threadId } = job;

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

      const result = await runLoop(job, {
        modelConfig: config.model,
        toolConfig: config.tool,
        sandbox,
      });

      if (result.status === 'done') {
        channel.ack(msg);
        console.log(`[${workerId}] run ${runId} done (${result.turns} turns, ${result.toolCalls} tools)`);
      } else {
        channel.nack(msg, false, true); // requeue
        console.warn(`[${workerId}] run ${runId} failed → requeued`);
      }
    } catch (err) {
      console.error(`[${workerId}] error on ${runId}:`, (err as Error).message);
      channel.nack(msg, false, true);
    } finally {
      await releaseThread(threadId, runId);
    }
  });

  const shutdown = async () => {
    console.log(`[${workerId}] shutting down…`);
    try {
      await channel.close();
      await conn.close();
    } catch {
      /* ignore */
    }
    reaper?.stop();
    await redisPub?.close();
    await sandbox?.shutdown?.();
    await closeMongo();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((e) => {
  console.error(`[${workerId}] fatal:`, e);
  process.exit(1);
});
