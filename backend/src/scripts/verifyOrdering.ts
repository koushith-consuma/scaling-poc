/**
 * Step 5 verifier — confirms per-thread serialization from the event log.
 *
 *   npm run verify:ordering -- <threadId>
 *
 * For the given thread, groups events by run and checks that runs did NOT
 * interleave in time: each run's [firstEvent, lastEvent] window must not
 * overlap another run's window on the same thread. Also prints the coherent
 * ordering and confirms the second run started after the first completed.
 */
import { getMongo, closeMongo } from '../lib/mongo.js';

interface RunWindow {
  runId: string;
  start: Date;
  end: Date;
  events: number;
  done: boolean;
}

async function main() {
  const threadId = process.argv[2];
  if (!threadId) {
    console.error('usage: npm run verify:ordering -- <threadId>');
    process.exit(1);
  }
  const { events } = await getMongo();

  // Pull all events for the thread in global (createdAt, seq) order.
  const all = await events.find({ threadId }).sort({ createdAt: 1, seq: 1 }).toArray();
  if (all.length === 0) {
    console.error(`no events for thread ${threadId}`);
    process.exit(1);
  }

  // Build per-run time windows.
  const byRun = new Map<string, RunWindow>();
  for (const e of all) {
    let w = byRun.get(e.runId);
    if (!w) {
      w = { runId: e.runId, start: e.createdAt, end: e.createdAt, events: 0, done: false };
      byRun.set(e.runId, w);
    }
    w.end = e.createdAt;
    w.events++;
    if (e.type === 'run_done' || e.type === 'run_failed') w.done = true;
  }

  const windows = [...byRun.values()].sort((a, b) => a.start.getTime() - b.start.getTime());

  console.log(`thread ${threadId}: ${windows.length} runs, ${all.length} events\n`);
  for (const w of windows) {
    console.log(
      `run ${w.runId.slice(0, 8)}  start=${w.start.toISOString()}  end=${w.end.toISOString()}  events=${w.events}  done=${w.done}`,
    );
  }

  // Check no two run windows overlap (strict serialization).
  let overlap = false;
  for (let i = 1; i < windows.length; i++) {
    const prev = windows[i - 1]!;
    const cur = windows[i]!;
    if (cur.start.getTime() < prev.end.getTime()) {
      overlap = true;
      console.log(
        `\n!! OVERLAP: run ${cur.runId.slice(0, 8)} started at ${cur.start.toISOString()} ` +
          `before run ${prev.runId.slice(0, 8)} ended at ${prev.end.toISOString()}`,
      );
    }
  }

  // Global interleaving check: events must be run-contiguous (no A,B,A pattern).
  let interleaved = false;
  let lastRun = '';
  const seenRuns = new Set<string>();
  for (const e of all) {
    if (e.runId !== lastRun) {
      if (seenRuns.has(e.runId)) {
        interleaved = true;
        break;
      }
      if (lastRun) seenRuns.add(lastRun);
      lastRun = e.runId;
    }
  }

  console.log('\n=== verdict ===');
  console.log('window overlap   :', overlap ? 'FAIL (runs ran concurrently)' : 'none — serialized OK');
  console.log('event interleaving:', interleaved ? 'FAIL (A,B,A pattern)' : 'none — coherently ordered OK');
  console.log('all runs done     :', windows.every((w) => w.done) ? 'OK' : 'some incomplete');

  await closeMongo();
  process.exit(overlap || interleaved ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
