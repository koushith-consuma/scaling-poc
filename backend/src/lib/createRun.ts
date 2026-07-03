import { randomUUID } from 'node:crypto';
import { getMongo } from './mongo.js';
import type { RunDoc } from '../types.js';

/** Insert an initial pending run doc. Web tier & scripts both use this before
 *  publishing the job. Returns the created runId. */
export async function createRun(params: {
  runId?: string;
  threadId: string;
  seed?: number;
  prompt?: string; // the user's message text, so history can be rebuilt
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
    ...(params.prompt !== undefined ? { prompt: params.prompt } : {}),
  };
  await runs.insertOne(doc);
  return doc;
}
