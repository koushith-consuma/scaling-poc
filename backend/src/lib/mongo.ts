import { MongoClient, type Collection, type Db } from 'mongodb';
import { config } from '../config.js';
import type { EventDoc, RunDoc } from '../types.js';

let client: MongoClient | null = null;
let db: Db | null = null;

export interface ThreadLock {
  _id: string; // threadId
  runId: string; // the active run holding the thread
  acquiredAt: Date;
}

export interface Collections {
  runs: Collection<RunDoc>;
  events: Collection<EventDoc>;
  threadLocks: Collection<ThreadLock>;
}

let cached: Collections | null = null;

/** Connect once (idempotent) and ensure indexes. Source of truth = Mongo. */
export async function getMongo(): Promise<Collections> {
  if (cached) return cached;

  client = new MongoClient(config.mongoUrl, {
    maxPoolSize: 50,
    minPoolSize: 5,
  });
  await client.connect();
  db = client.db(config.mongoDb);

  const runs = db.collection<RunDoc>('agent_runs');
  const events = db.collection<EventDoc>('run_events');
  const threadLocks = db.collection<ThreadLock>('thread_locks');

  if (!config.mongoSkipIndexes) {
    await ensureIndexes(runs, events);
  } else {
    console.warn('[mongo] MONGO_SKIP_INDEXES=1 — running WITHOUT indexes (Step 8 regression mode)');
  }

  cached = { runs, events, threadLocks };
  return cached;
}

async function ensureIndexes(runs: Collection<RunDoc>, events: Collection<EventDoc>) {
  await Promise.all([
    // Per-thread ordered reads / backfill.
    events.createIndex({ threadId: 1, seq: 1 }, { name: 'events_thread_seq' }),
    // Per-run ordered reads (SSE backfill by lastEventSeq).
    events.createIndex({ runId: 1, seq: 1 }, { name: 'events_run_seq', unique: true }),
    // Per-thread guard active-run lookup.
    runs.createIndex({ threadId: 1, status: 1 }, { name: 'runs_thread_status' }),
    // Reaper scan for stuck/crashed runs.
    runs.createIndex({ status: 1, claimedAt: 1 }, { name: 'runs_status_claimedAt' }),
  ]);
}

export async function closeMongo(): Promise<void> {
  await client?.close();
  client = null;
  db = null;
  cached = null;
}
