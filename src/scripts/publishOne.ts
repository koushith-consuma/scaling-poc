/**
 * Drops a single { runId, threadId } job on the queue (Step 2).
 *
 *   npm run publish:one -- [threadId] [seed]
 *
 * Creates the pending run doc, then publishes the job. The worker claims it.
 */
import { randomUUID } from 'node:crypto';
import { connectQueue, publishJob } from '../lib/queue.js';
import { createRun } from '../lib/createRun.js';
import { closeMongo } from '../lib/mongo.js';

async function main() {
  const threadId = process.argv[2] ?? `thread-${randomUUID().slice(0, 8)}`;
  const seed = process.argv[3] ? Number(process.argv[3]) : undefined;

  const run = await createRun({ threadId, seed });
  const { conn, channel } = await connectQueue();
  await publishJob(channel, { runId: run._id, threadId, seed });

  console.log(`published job runId=${run._id} threadId=${threadId} seed=${seed ?? '(random)'}`);

  // Give the buffer a moment to flush, then close.
  await new Promise((r) => setTimeout(r, 200));
  await channel.close();
  await conn.close();
  await closeMongo();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
