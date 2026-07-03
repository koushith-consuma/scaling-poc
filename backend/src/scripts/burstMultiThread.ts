#!/usr/bin/env tsx
/**
 * Burst messages across MULTIPLE threads to demonstrate TRUE parallelism.
 *
 * Usage:
 *   npm run burst:parallel -- --threads 10 --messages 5
 *
 * This sends 5 messages to each of 10 different threads (50 total messages),
 * all processing in parallel across your worker pool.
 */

import { randomUUID } from 'node:crypto';
import { connectQueue, publishJob } from '../lib/queue.js';
import { createRun } from '../lib/createRun.js';
import { getMongo } from '../lib/mongo.js';

interface BurstOptions {
  threads: number;      // how many different threads
  messagesPerThread: number; // messages per thread
  delayBetweenMs?: number;   // stagger the burst
}

async function burstMultiThread(opts: BurstOptions) {
  const { threads, messagesPerThread, delayBetweenMs = 0 } = opts;
  const total = threads * messagesPerThread;

  console.log(`\n🚀 Bursting ${total} messages across ${threads} threads (${messagesPerThread} each)...\n`);

  await getMongo();
  const { conn, channel } = await connectQueue();

  const threadIds = Array.from({ length: threads }, (_, i) => `burst-thread-${i + 1}`);
  let sent = 0;
  const startTime = Date.now();

  for (const threadId of threadIds) {
    for (let i = 0; i < messagesPerThread; i++) {
      const prompt = `Message ${i + 1} for ${threadId}`;
      const run = await createRun({ threadId, prompt });
      await publishJob(channel, { runId: run._id, threadId, seed: Date.now() + sent });
      sent++;
      console.log(`[${sent}/${total}] ${threadId} → run ${run._id.slice(0, 8)}`);
      if (delayBetweenMs > 0) await new Promise(r => setTimeout(r, delayBetweenMs));
    }
  }

  const elapsed = Date.now() - startTime;
  console.log(`\n✓ Published ${total} jobs in ${elapsed}ms (${Math.round(total / (elapsed / 1000))}/sec)`);
  console.log(`\n📊 Watch the workers process these in parallel:`);
  console.log(`   - Open http://localhost:3001`);
  console.log(`   - Check Worker Activity panel (right side)`);
  console.log(`   - All ${threads} threads can process at the same time!\n`);

  await channel.close();
  await conn.close();
  process.exit(0);
}

// Parse CLI args
const args = process.argv.slice(2);
const opts: BurstOptions = {
  threads: 10,
  messagesPerThread: 3,
  delayBetweenMs: 0,
};

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--threads' || args[i] === '-t') opts.threads = Number(args[++i]);
  if (args[i] === '--messages' || args[i] === '-m') opts.messagesPerThread = Number(args[++i]);
  if (args[i] === '--delay' || args[i] === '-d') opts.delayBetweenMs = Number(args[++i]);
}

burstMultiThread(opts).catch(console.error);
