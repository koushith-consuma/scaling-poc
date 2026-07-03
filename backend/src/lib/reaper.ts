import { getMongo } from './mongo.js';
import { connectQueue, publishJob } from './queue.js';

/**
 * Crash-recovery reaper (Step 8 durability).
 *
 * A worker that dies mid-run leaves its run stuck in `running` (claimedBy set,
 * never acked → RabbitMQ will actually redeliver the unacked message when the
 * channel closes, but the run doc + thread lock also need reconciling). The
 * reaper periodically finds runs that have been `running` longer than a lease
 * TTL with no progress, resets them to `pending`, releases the stale thread
 * lock, and republishes the job so another worker picks it up.
 *
 * Note: RabbitMQ redelivery handles the queue side on channel close; the reaper
 * is the belt-and-suspenders path for the DB state and for the case where the
 * message was already acked (e.g. our guard-requeue path) but the run crashed.
 */
export interface ReaperOptions {
  leaseTtlMs?: number; // consider a run dead if running & stale beyond this
  intervalMs?: number;
}

export async function reapOnce(leaseTtlMs: number): Promise<number> {
  const { runs, threadLocks } = await getMongo();
  const cutoff = new Date(Date.now() - leaseTtlMs);

  // Stale = running, updated before cutoff (no event progress since).
  const stale = await runs
    .find({ status: 'running', updatedAt: { $lt: cutoff } })
    .toArray();

  if (stale.length === 0) return 0;

  const { conn, channel } = await connectQueue();
  let reaped = 0;
  for (const run of stale) {
    // Reset to pending so it can be re-claimed.
    const res = await runs.updateOne(
      { _id: run._id, status: 'running', updatedAt: { $lt: cutoff } },
      { $set: { status: 'pending', updatedAt: new Date() }, $unset: { claimedBy: '', claimedAt: '' } },
    );
    if (res.modifiedCount === 1) {
      // Release any stale thread lock this run held.
      await threadLocks.deleteOne({ _id: run.threadId, runId: run._id });
      // Republish the job.
      await publishJob(channel, { runId: run._id, threadId: run.threadId, seed: run.seed });
      reaped++;
      console.log(`[reaper] recovered run ${run._id} (thread ${run.threadId}) → requeued`);
    }
  }
  await channel.close();
  await conn.close();
  return reaped;
}

/** Long-running reaper loop (can run inside a worker or standalone). */
export function startReaper(opts: ReaperOptions = {}): { stop: () => void } {
  const leaseTtlMs = opts.leaseTtlMs ?? 15000;
  const intervalMs = opts.intervalMs ?? 5000;
  let stopped = false;
  const tick = async () => {
    if (stopped) return;
    try {
      await reapOnce(leaseTtlMs);
    } catch (e) {
      console.warn('[reaper] error:', (e as Error).message);
    }
    if (!stopped) setTimeout(tick, intervalMs);
  };
  setTimeout(tick, intervalMs);
  return { stop: () => { stopped = true; } };
}
