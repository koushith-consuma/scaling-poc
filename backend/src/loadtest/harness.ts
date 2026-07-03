/**
 * Load harness (Step 7) — generates the "100 users" pressure and instruments
 * everything.
 *
 *   npm run loadtest -- --users 100 --label u100-w3 [options]
 *
 * Options (all have defaults):
 *   --users N              concurrent simulated users (default 10)
 *   --label STR            results subfolder name (default users-<N>)
 *   --web URL              web base url (default http://localhost:3000)
 *   --second-prob P        chance a user fires a 2nd same-thread msg (default 0.3)
 *   --reconnect-prob P     chance a user disconnects+reconnects mid-run (0.3)
 *   --ramp-ms MS           spread user starts over this window (default 2000)
 *   --timeout-ms MS        max wait for all runs to finish (default 120000)
 *   --avg-turns N          override model avgTurns for this run (informational)
 *
 * Each user: POST /runs → (maybe) POST a 2nd run to the SAME thread → open SSE,
 * (maybe) disconnect and reconnect from last seq → wait for terminal event.
 */
import { config } from '../config.js';
import { getMongo, closeMongo } from '../lib/mongo.js';
import { Metrics } from './metrics.js';
import { makeRng, sleep } from '../lib/rng.js';

interface Args {
  users: number;
  label: string;
  web: string;
  secondProb: number;
  reconnectProb: number;
  rampMs: number;
  timeoutMs: number;
}

function parseArgs(): Args {
  const a = process.argv.slice(2);
  const get = (flag: string, def: string) => {
    const i = a.indexOf(flag);
    return i >= 0 && a[i + 1] !== undefined ? a[i + 1]! : def;
  };
  const users = Number(get('--users', '10'));
  return {
    users,
    label: get('--label', `users-${users}`),
    web: get('--web', `http://localhost:${config.webPort}`),
    secondProb: Number(get('--second-prob', '0.3')),
    reconnectProb: Number(get('--reconnect-prob', '0.3')),
    rampMs: Number(get('--ramp-ms', '2000')),
    timeoutMs: Number(get('--timeout-ms', '120000')),
  };
}

async function postRun(web: string, threadId: string, seed?: number): Promise<string> {
  const res = await fetch(`${web}/runs`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ threadId, seed }),
  });
  const body: any = await res.json();
  return body.runId as string;
}

/** Stream a run to terminal. If disconnectAtSeq is set, drop once reached and
 *  reconnect from that seq (exercises backfill). Resolves at terminal event. */
async function streamRun(web: string, runId: string, disconnectAtSeq: number): Promise<void> {
  let lastSeq = 0;
  let didReconnect = false;

  for (;;) {
    const ctrl = new AbortController();
    let terminal = false;
    try {
      const res = await fetch(`${web}/runs/${runId}/stream?lastSeq=${lastSeq}`, {
        signal: ctrl.signal,
        headers: { Accept: 'text/event-stream' },
      });
      const reader = res.body!.getReader();
      const dec = new TextDecoder();
      let buf = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        const parts = buf.split('\n\n');
        buf = parts.pop() ?? '';
        for (const chunk of parts) {
          const idLine = chunk.split('\n').find((l) => l.startsWith('id: '));
          const evLine = chunk.split('\n').find((l) => l.startsWith('event: '));
          if (!idLine || !evLine) continue;
          lastSeq = Math.max(lastSeq, Number(idLine.slice(4)));
          if (evLine.includes('run_done') || evLine.includes('run_failed')) {
            terminal = true;
            break;
          }
          if (!didReconnect && disconnectAtSeq > 0 && lastSeq >= disconnectAtSeq) {
            ctrl.abort(); // drop mid-run
            break;
          }
        }
        if (terminal) {
          ctrl.abort();
          break;
        }
      }
    } catch {
      /* aborted or transient network — fall through to reconnect logic */
    }

    if (terminal) return;
    if (!didReconnect && disconnectAtSeq > 0) {
      didReconnect = true;
      await sleep(300); // brief gap before reconnect
      continue; // reconnect from lastSeq
    }
    // Stream ended without terminal and not our planned reconnect: retry once.
    // Guard against infinite loop by requiring progress via Mongo poll instead.
    return;
  }
}

async function main() {
  const args = parseArgs();
  const rng = makeRng(1234); // deterministic user behavior selection
  const { runs } = await getMongo();
  const metrics = new Metrics();

  console.log(`[load] users=${args.users} label=${args.label} web=${args.web}`);
  console.log(`[load] secondProb=${args.secondProb} reconnectProb=${args.reconnectProb} ramp=${args.rampMs}ms`);

  // Background samplers: queue depth + Mongo query latency.
  const stop = { done: false };
  const queueSampler = (async () => {
    while (!stop.done) {
      await metrics.sampleQueue();
      await sleep(500);
    }
  })();
  const mongoSampler = (async () => {
    while (!stop.done) {
      const t0 = Date.now();
      // Representative indexed read: latest events for a random known run.
      const any = [...metrics.runs.keys()];
      const runId = any.length ? any[Math.floor(rng() * any.length)]! : '__none__';
      await runs.findOne({ _id: runId }).catch(() => null);
      metrics.addMongoSample({ t: Date.now(), latencyMs: Date.now() - t0 });
      await sleep(500);
    }
  })();

  // Spawn users, ramped over rampMs.
  const userTasks: Promise<void>[] = [];
  const allRunIds: string[] = [];

  for (let u = 0; u < args.users; u++) {
    const startDelay = (args.rampMs * u) / Math.max(1, args.users);
    const doSecond = rng() < args.secondProb;
    const doReconnect = rng() < args.reconnectProb;
    const seed = 100000 + u;

    userTasks.push(
      (async () => {
        await sleep(startDelay);
        const threadId = `load-${args.label}-u${u}`;

        const runId = await postRun(args.web, threadId, seed);
        metrics.markPublished(runId, threadId, { secondMessage: false });
        allRunIds.push(runId);

        let secondRunId: string | undefined;
        if (doSecond) {
          // Fire a SECOND message into the SAME thread (exercises the guard).
          secondRunId = await postRun(args.web, threadId, seed + 1);
          metrics.markPublished(secondRunId, threadId, { secondMessage: true });
          allRunIds.push(secondRunId);
        }

        // Stream the first run; maybe disconnect+reconnect mid-way.
        const disconnectAt = doReconnect ? 3 : 0;
        if (doReconnect) metrics.markReconnected(runId);
        await streamRun(args.web, runId, disconnectAt);
      })().catch((e) => console.warn(`[load] user ${u} error:`, (e as Error).message)),
    );
  }

  await Promise.all(userTasks);
  console.log(`[load] all ${allRunIds.length} runs published & first-run streams closed; draining…`);

  // Wait for ALL runs (including 2nd messages) to reach terminal in Mongo.
  const deadline = Date.now() + args.timeoutMs;
  for (;;) {
    const docs = await runs
      .find({ _id: { $in: allRunIds } }, { projection: { _id: 1, status: 1, claimedAt: 1 } })
      .toArray();
    const terminal = docs.filter((d) => d.status === 'done' || d.status === 'failed');
    for (const d of terminal) {
      const rec = metrics.runs.get(d._id);
      if (rec && !rec.completedAt) {
        metrics.markCompleted(d._id, d.claimedAt ? d.claimedAt.getTime() : undefined, d.status);
      }
    }
    if (terminal.length >= allRunIds.length || Date.now() > deadline) {
      if (Date.now() > deadline) console.warn(`[load] TIMEOUT: ${terminal.length}/${allRunIds.length} terminal`);
      break;
    }
    await sleep(500);
  }

  stop.done = true;
  await Promise.allSettled([queueSampler, mongoSampler]);

  const { dir } = metrics.export(args.label);
  const s = metrics.summary();
  console.log(`\n[load] === summary (${args.label}) ===`);
  console.table(s);
  console.log(`[load] results written to ${dir}/`);

  await closeMongo();
  process.exit(0);
}

main().catch((e) => {
  console.error('[load] fatal:', e);
  process.exit(1);
});
