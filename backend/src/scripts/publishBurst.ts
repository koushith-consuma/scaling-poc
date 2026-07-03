/**
 * Step 5 helper — publish a burst of jobs.
 *
 *   npm run publish:burst -- <threadId> <count> [seed]
 *
 * Publishes <count> runs to the SAME thread near-simultaneously to exercise the
 * per-thread serialization guard. Pass threadId="__spread" to instead publish
 * each run to a DISTINCT thread (to confirm the worker stays busy across threads).
 */
import { randomUUID } from 'node:crypto';
import { connectQueue, publishJob } from '../lib/queue.js';
import { createRun } from '../lib/createRun.js';
import { closeMongo } from '../lib/mongo.js';

async function main() {
  const threadArg = process.argv[2] ?? `thread-${randomUUID().slice(0, 8)}`;
  const count = Number(process.argv[3] ?? 2);
  const seed = process.argv[4] ? Number(process.argv[4]) : undefined;
  const spread = threadArg === '__spread';

  const { conn, channel } = await connectQueue();
  const published: { runId: string; threadId: string }[] = [];

  // Create all run docs first, then fire all jobs together (near-simultaneous).
  const jobs = [];
  for (let i = 0; i < count; i++) {
    const threadId = spread ? `spread-${randomUUID().slice(0, 6)}` : threadArg;
    const run = await createRun({ threadId, seed });
    jobs.push({ runId: run._id, threadId, seed });
  }
  await Promise.all(jobs.map((j) => publishJob(channel, j)));
  published.push(...jobs);

  for (const p of published) console.log(`published runId=${p.runId} threadId=${p.threadId}`);
  console.log(`\n${count} jobs published (${spread ? 'distinct threads' : `thread=${threadArg}`})`);

  await new Promise((r) => setTimeout(r, 200));
  await channel.close();
  await conn.close();
  await closeMongo();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
