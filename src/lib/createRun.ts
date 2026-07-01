import { randomUUID } from 'node:crypto';
import { getMongo } from './mongo.js';
import type { RunDoc } from '../types.js';

/** Insert an initial pending run doc. Web tier & scripts both use this before
 *  publishing the job. Returns the created runId. */
export async function createRun(params: {
  runId?: string;
  threadId: string;
  seed?: number;
}): Promise<RunDoc> {
  const { runs } = await getMongo();
  const now = new Date();
  const doc: RunDoc = {
    _id: params.runId ?? randomUUID(),
    threadId: params.threadId,
    status: 'pending',
    lastEventSeq: 0,
    createdAt: now,
    updatedAt: now,
    ...(params.seed !== undefined ? { seed: params.seed } : {}),
  };
  await runs.insertOne(doc);
  return doc;
}
