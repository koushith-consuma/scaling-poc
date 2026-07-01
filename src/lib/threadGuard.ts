import { getMongo } from './mongo.js';

/**
 * Per-thread serialization guard (Step 5).
 *
 * Correctness rule: at most one active run per thread. At claim time we
 * atomically check "does this thread already have an active run?" If yes, the
 * caller requeues the job (nack+requeue / delayed republish) and moves on — it
 * does NOT block the worker idle. A new same-thread message queues BEHIND the
 * active run; we never cancel the in-flight run.
 *
 * Implementation: a dedicated `thread_locks` collection with _id = threadId.
 * Acquire = atomic upsert that only succeeds if unlocked or already held by
 * this run. This is a Mongo-backed lock (works across N worker processes).
 */
/** Try to acquire the thread for this run. Returns true if acquired. */
export async function tryAcquireThread(threadId: string, runId: string): Promise<boolean> {
  const { threadLocks: col } = await getMongo();
  const now = new Date();
  try {
    // Succeeds only if no doc exists (or it's already ours — idempotent redelivery).
    const res = await col.findOneAndUpdate(
      { _id: threadId, $or: [{ runId }, { runId: { $exists: false } }] },
      { $setOnInsert: { runId, acquiredAt: now } },
      { upsert: true, returnDocument: 'after' },
    );
    return res?.runId === runId;
  } catch (err: any) {
    // Duplicate key = another run holds the thread. Not acquired.
    if (err?.code === 11000) return false;
    throw err;
  }
}

/** Release the thread iff this run holds it. */
export async function releaseThread(threadId: string, runId: string): Promise<void> {
  const { threadLocks: col } = await getMongo();
  await col.deleteOne({ _id: threadId, runId });
}
