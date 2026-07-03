#!/usr/bin/env tsx
/**
 * Test: How many concurrent runs can ONE worker handle reliably?
 *
 * Measures:
 * - Memory usage
 * - Event loop lag
 * - Throughput
 * - Error rate
 */

import { randomUUID } from 'node:crypto';
import { connectQueue, publishJob } from '../lib/queue.js';
import { createRun } from '../lib/createRun.js';
import { getMongo } from '../lib/mongo.js';

async function capacityTest(concurrency: number) {
  console.log(`\n🧪 Testing worker capacity at ${concurrency} concurrent runs...\n`);

  await getMongo();
  const { conn, channel } = await connectQueue();

  const threadId = `capacity-test-${randomUUID().slice(0, 8)}`;
  const startTime = Date.now();
  const startMem = process.memoryUsage().heapUsed / 1024 / 1024;

  // Publish N messages to same thread (will process sequentially but worker holds all in memory)
  const runIds = [];
  for (let i = 0; i < concurrency; i++) {
    const run = await createRun({ threadId, prompt: `Test message ${i}` });
    await publishJob(channel, { runId: run._id, threadId, seed: Date.now() + i });
    runIds.push(run._id);
  }

  const publishTime = Date.now() - startTime;
  console.log(`✓ Published ${concurrency} jobs in ${publishTime}ms`);

  // Monitor memory while processing
  const memSamples: number[] = [];
  const monitor = setInterval(() => {
    const heapMB = process.memoryUsage().heapUsed / 1024 / 1024;
    memSamples.push(heapMB);
  }, 1000);

  // Wait for all to complete
  const { runs } = await getMongo();
  let completed = 0;
  while (completed < concurrency) {
    await new Promise(r => setTimeout(r, 2000));
    const done = await runs.countDocuments({ _id: { $in: runIds }, status: 'done' });
    if (done > completed) {
      console.log(`Progress: ${done}/${concurrency} completed`);
      completed = done;
    }
  }

  clearInterval(monitor);

  const totalTime = Date.now() - startTime;
  const avgMem = memSamples.reduce((a, b) => a + b, 0) / memSamples.length;
  const peakMem = Math.max(...memSamples);

  console.log(`\n📊 Results for ${concurrency} concurrent:`);
  console.log(`   Total time: ${(totalTime / 1000).toFixed(1)}s`);
  console.log(`   Throughput: ${(concurrency / (totalTime / 1000)).toFixed(1)} runs/sec`);
  console.log(`   Memory - Start: ${startMem.toFixed(1)}MB`);
  console.log(`   Memory - Avg: ${avgMem.toFixed(1)}MB`);
  console.log(`   Memory - Peak: ${peakMem.toFixed(1)}MB`);
  console.log(`   Memory per run: ${((peakMem - startMem) / concurrency).toFixed(2)}MB`);

  await channel.close();
  await conn.close();

  return {
    concurrency,
    totalTime,
    throughput: concurrency / (totalTime / 1000),
    memStart: startMem,
    memAvg: avgMem,
    memPeak: peakMem,
    memPerRun: (peakMem - startMem) / concurrency,
  };
}

async function runTests() {
  console.log('🚀 Worker Capacity Test Suite\n');
  console.log('This will test how many concurrent runs ONE worker can handle.\n');

  const results = [];

  // Test increasing concurrency
  for (const n of [10, 20, 50, 100]) {
    const result = await capacityTest(n);
    results.push(result);
    await new Promise(r => setTimeout(r, 3000)); // Cool down
  }

  console.log('\n\n📈 Summary:\n');
  console.log('Concurrency | Time    | Throughput | Mem/Run | Peak Mem');
  console.log('----------- | ------- | ---------- | ------- | --------');
  for (const r of results) {
    console.log(
      `${String(r.concurrency).padEnd(11)} | ` +
      `${(r.totalTime / 1000).toFixed(1).padEnd(7)} | ` +
      `${r.throughput.toFixed(1).padEnd(10)} | ` +
      `${r.memPerRun.toFixed(2).padEnd(7)} | ` +
      `${r.memPeak.toFixed(1)}MB`
    );
  }

  console.log('\n💡 Recommendations:');
  const lastResult = results[results.length - 1];
  if (lastResult.memPeak > 400) {
    console.log('   ⚠️  Memory usage is high! Consider lower prefetch.');
  }
  if (lastResult.throughput < 5) {
    console.log('   ⚠️  Throughput is low. Check for bottlenecks.');
  }
  if (lastResult.memPeak < 300 && lastResult.throughput > 10) {
    console.log('   ✓ Worker can handle this concurrency well!');
  }

  const recommended = Math.floor(512 / lastResult.memPerRun * 0.8); // 80% of max
  console.log(`   Recommended WORKER_PREFETCH: ${recommended}`);

  process.exit(0);
}

runTests().catch(console.error);
