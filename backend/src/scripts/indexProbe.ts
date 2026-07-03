/**
 * Step 8 — missing-index regression probe.
 *
 *   npm run index:probe                      # measures with current indexes
 *   MONGO_SKIP_INDEXES=1 npm run index:probe # drops indexes first, then measures
 *
 * Measures the two hot-path queries the system runs constantly:
 *   A) SSE backfill:  events.find({runId, seq > x}).sort({seq})
 *   B) thread guard :  runs.find({threadId, status})
 * over the events already in the DB from the load matrix, so the collection is
 * realistically sized. Prints p50/p95 latency for each.
 */
import { getMongo, closeMongo } from '../lib/mongo.js';
import { config } from '../config.js';

function pct(xs: number[], p: number): number {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] ?? NaN;
}

async function main() {
  const { runs, events } = await getMongo();

  const total = await events.estimatedDocumentCount();
  console.log(`events in collection: ${total}`);
  console.log(`indexes: ${config.mongoSkipIndexes ? 'DROPPED (regression mode)' : 'present'}`);

  if (config.mongoSkipIndexes) {
    // Actively drop any indexes left from a prior indexed run.
    await events.dropIndexes().catch(() => {});
    await runs.dropIndexes().catch(() => {});
    console.log('dropped existing secondary indexes');
  }

  // Sample real runIds/threadIds to query.
  const sampleRuns = await runs.find({}, { projection: { _id: 1, threadId: 1 } }).limit(200).toArray();
  if (sampleRuns.length === 0) {
    console.error('no runs in DB — run the load matrix first');
    process.exit(1);
  }

  const backfillLat: number[] = [];
  const guardLat: number[] = [];
  const N = 300;
  for (let i = 0; i < N; i++) {
    const r = sampleRuns[i % sampleRuns.length]!;

    let t0 = Date.now();
    await events.find({ runId: r._id, seq: { $gt: 0 } }).sort({ seq: 1 }).toArray();
    backfillLat.push(Date.now() - t0);

    t0 = Date.now();
    await runs.find({ threadId: r.threadId, status: 'running' }).toArray();
    guardLat.push(Date.now() - t0);
  }

  console.log('\n=== hot-path query latency (ms) ===');
  console.table({
    'SSE backfill (events by runId+seq)': { p50: pct(backfillLat, 50), p95: pct(backfillLat, 95), max: Math.max(...backfillLat) },
    'thread guard (runs by threadId+status)': { p50: pct(guardLat, 50), p95: pct(guardLat, 95), max: Math.max(...guardLat) },
  });

  await closeMongo();
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
