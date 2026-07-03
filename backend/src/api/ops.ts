import { exec as execCb } from 'node:child_process';
import { promisify } from 'node:util';
import { createClient } from 'redis';
import { getMongo } from '../lib/mongo.js';
import { config } from '../config.js';

const exec = promisify(execCb);

/**
 * Observability helpers for the interactive web app: service health, queue
 * depth, run-state counts, and live worker container count. Read-only.
 */

export interface QueueStat {
  ok: boolean;
  ready: number;
  unacked: number;
  depth: number;
  consumers: number;
}

export async function queueStat(): Promise<QueueStat> {
  try {
    const raw = config.rabbitMgmtUrl.replace(/\/$/, '');
    const parsed = new URL(raw);
    const user = decodeURIComponent(parsed.username || 'guest');
    const pass = decodeURIComponent(parsed.password || 'guest');
    parsed.username = '';
    parsed.password = '';
    const base = parsed.toString().replace(/\/$/, '');
    const res = await fetch(`${base}/api/queues/%2F/${config.runQueue}`, {
      headers: { Authorization: `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}` },
    });
    if (!res.ok) return { ok: false, ready: 0, unacked: 0, depth: 0, consumers: 0 };
    const q: any = await res.json();
    const ready = q.messages_ready ?? 0;
    const unacked = q.messages_unacknowledged ?? 0;
    return { ok: true, ready, unacked, depth: ready + unacked, consumers: q.consumers ?? 0 };
  } catch {
    return { ok: false, ready: 0, unacked: 0, depth: 0, consumers: 0 };
  }
}

export interface RunCounts {
  pending: number;
  running: number;
  done: number;
  failed: number;
  total: number;
}

export async function runCounts(): Promise<RunCounts> {
  try {
    const { runs } = await getMongo();
    const agg = await runs
      .aggregate<{ _id: string; n: number }>([{ $group: { _id: '$status', n: { $sum: 1 } } }])
      .toArray();
    const c: RunCounts = { pending: 0, running: 0, done: 0, failed: 0, total: 0 };
    for (const g of agg) {
      if (g._id in c) (c as any)[g._id] = g.n;
      c.total += g.n;
    }
    return c;
  } catch {
    return { pending: 0, running: 0, done: 0, failed: 0, total: 0 };
  }
}

/** Count running worker containers in the compose project. */
export async function workerCount(): Promise<number> {
  try {
    const { stdout } = await exec(
      `docker ps --filter "label=com.docker.compose.project=${config.composeProject}" ` +
        `--filter "label=com.docker.compose.service=worker" --filter "status=running" -q`,
    );
    return stdout.trim() ? stdout.trim().split('\n').length : 0;
  } catch {
    return 0;
  }
}

export interface Health {
  mongo: boolean;
  redis: boolean;
  rabbit: boolean;
  workers: number;
}

export async function health(): Promise<Health> {
  const [mongoOk, redisOk, q, workers] = await Promise.all([
    (async () => {
      try {
        const { runs } = await getMongo();
        await runs.estimatedDocumentCount();
        return true;
      } catch {
        return false;
      }
    })(),
    (async () => {
      try {
        const c = createClient({ url: config.redisUrl, socket: { reconnectStrategy: () => false, connectTimeout: 1000 } });
        c.on('error', () => {});
        await c.connect();
        await c.ping();
        await c.quit();
        return true;
      } catch {
        return false;
      }
    })(),
    queueStat(),
    workerCount(),
  ]);
  return { mongo: mongoOk, redis: redisOk, rabbit: q.ok, workers };
}

/** Recent runs for the activity table. */
export async function recentRuns(limit = 20) {
  const { runs } = await getMongo();
  return runs
    .find({}, { projection: { _id: 1, threadId: 1, status: 1, lastEventSeq: 1, createdAt: 1, finishedAt: 1, claimedBy: 1 } })
    .sort({ createdAt: -1 })
    .limit(limit)
    .toArray();
}
