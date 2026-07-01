import { getMongo } from './mongo.js';
import type { RunDoc } from '../types.js';

/**
 * Atomic pending → running transition. Only one worker can win the claim.
 * Returns the claimed run doc, or null if it was already claimed / not pending.
 */
export async function claimRun(runId: string, workerId: string): Promise<RunDoc | null> {
  const { runs } = await getMongo();
  const now = new Date();
  const res = await runs.findOneAndUpdate(
    { _id: runId, status: 'pending' },
    {
      $set: {
        status: 'running',
        claimedBy: workerId,
        claimedAt: now,
        updatedAt: now,
      },
    },
    { returnDocument: 'after' },
  );
  return res ?? null;
}

/** Mark a claimed run terminal. */
export async function finishRun(
  runId: string,
  status: 'done' | 'failed',
  error?: string,
): Promise<void> {
  const { runs } = await getMongo();
  const now = new Date();
  await runs.updateOne(
    { _id: runId },
    { $set: { status, finishedAt: now, updatedAt: now, ...(error ? { error } : {}) } },
  );
}
