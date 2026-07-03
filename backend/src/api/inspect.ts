import { createClient } from 'redis';
import { getMongo } from '../lib/mongo.js';
import { config } from '../config.js';
import { RECENT_EVENTS_KEY, runChannel } from '../lib/redisPub.js';

/**
 * Data inspectors for the GUI "Data" page: show what actually lives in MongoDB
 * (agent_runs, run_events, thread_locks), in Redis (recent-events list, per-run
 * channel subscriber counts, keyspace info), and in RabbitMQ (queue depth +
 * a peek at the jobs currently waiting). Read-only.
 */

export interface QueueInspection {
  ok: boolean;
  error?: string;
  name: string;
  ready: number;
  unacked: number;
  consumers: number;
  messages: { runId: string; threadId: string }[]; // peeked, non-destructive
}

/** Peek at the jobs currently sitting in the queue via the RabbitMQ mgmt API.
 *  Uses ackmode "reject_requeue_true" so peeking does NOT consume them. */
export async function inspectQueue(limit = 20): Promise<QueueInspection> {
  const base = (() => {
    const p = new URL(config.rabbitMgmtUrl.replace(/\/$/, ''));
    const user = decodeURIComponent(p.username || 'guest');
    const pass = decodeURIComponent(p.password || 'guest');
    p.username = ''; p.password = '';
    return { url: p.toString().replace(/\/$/, ''), auth: 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64') };
  })();
  try {
    // Queue stats
    const sres = await fetch(`${base.url}/api/queues/%2F/${config.runQueue}`, { headers: { Authorization: base.auth } });
    if (!sres.ok) return { ok: false, error: `mgmt ${sres.status}`, name: config.runQueue, ready: 0, unacked: 0, consumers: 0, messages: [] };
    const q: any = await sres.json();

    // Peek messages (non-destructive requeue). Only returns "ready" messages.
    let messages: { runId: string; threadId: string }[] = [];
    try {
      const pres = await fetch(`${base.url}/api/queues/%2F/${config.runQueue}/get`, {
        method: 'POST',
        headers: { Authorization: base.auth, 'content-type': 'application/json' },
        body: JSON.stringify({ count: limit, ackmode: 'reject_requeue_true', encoding: 'auto', truncate: 50000 }),
      });
      if (pres.ok) {
        const arr: any[] = await pres.json();
        messages = arr.map((m) => {
          try { const j = JSON.parse(m.payload); return { runId: j.runId, threadId: j.threadId }; }
          catch { return { runId: '?', threadId: '?' }; }
        });
      }
    } catch { /* peek best-effort */ }

    return {
      ok: true,
      name: config.runQueue,
      ready: q.messages_ready ?? 0,
      unacked: q.messages_unacknowledged ?? 0,
      consumers: q.consumers ?? 0,
      messages,
    };
  } catch (e) {
    return { ok: false, error: (e as Error).message, name: config.runQueue, ready: 0, unacked: 0, consumers: 0, messages: [] };
  }
}

export interface MongoInspection {
  ok: boolean;
  error?: string;
  counts: { runs: number; events: number; threadLocks: number };
  runs: any[];
  events: any[];
  threadLocks: any[];
}

export async function inspectMongo(limit = 25): Promise<MongoInspection> {
  try {
    const { runs, events, threadLocks } = await getMongo();
    const [runCount, evCount, lockCount, runDocs, evDocs, lockDocs] = await Promise.all([
      runs.estimatedDocumentCount(),
      events.estimatedDocumentCount(),
      threadLocks.estimatedDocumentCount(),
      runs.find({}).sort({ createdAt: -1 }).limit(limit).toArray(),
      events.find({}).sort({ createdAt: -1 }).limit(limit).toArray(),
      threadLocks.find({}).limit(limit).toArray(),
    ]);
    return {
      ok: true,
      counts: { runs: runCount, events: evCount, threadLocks: lockCount },
      runs: runDocs,
      events: evDocs,
      threadLocks: lockDocs,
    };
  } catch (e) {
    return {
      ok: false,
      error: (e as Error).message,
      counts: { runs: 0, events: 0, threadLocks: 0 },
      runs: [],
      events: [],
      threadLocks: [],
    };
  }
}

export interface RedisInspection {
  ok: boolean;
  error?: string;
  dbSize: number;
  recentCount: number;
  recentEvents: any[];
  activeChannels: { channel: string; subscribers: number }[];
  keys: { key: string; type: string; size?: number }[];
}

export async function inspectRedis(limit = 25): Promise<RedisInspection> {
  const client = createClient({
    url: config.redisUrl,
    socket: { reconnectStrategy: () => false, connectTimeout: 1500 },
  });
  client.on('error', () => {});
  try {
    await client.connect();

    const dbSize = await client.dbSize();
    const recentRaw = await client.lRange(RECENT_EVENTS_KEY, 0, limit - 1);
    const recentEvents = recentRaw.map((s) => {
      try {
        return JSON.parse(s);
      } catch {
        return { raw: s };
      }
    });
    const recentCount = await client.lLen(RECENT_EVENTS_KEY);

    // List keys (POC scale is small; SCAN keeps it safe).
    const keys: { key: string; type: string; size?: number }[] = [];
    for await (const key of client.scanIterator({ COUNT: 100 })) {
      const list = Array.isArray(key) ? key : [key];
      for (const k of list) {
        const type = await client.type(k);
        let size: number | undefined;
        if (type === 'list') size = await client.lLen(k);
        else if (type === 'string') size = (await client.get(k))?.length ?? 0;
        keys.push({ key: k, type, size });
        if (keys.length >= 100) break;
      }
      if (keys.length >= 100) break;
    }

    // Active per-run pub/sub channels + subscriber counts.
    const channels = (await client.pubSubChannels('run:*')) as string[];
    const activeChannels: { channel: string; subscribers: number }[] = [];
    if (channels.length) {
      const counts = await client.pubSubNumSub(channels);
      for (const ch of channels) activeChannels.push({ channel: ch, subscribers: Number((counts as any)[ch] ?? 0) });
    }

    await client.quit();
    return { ok: true, dbSize, recentCount, recentEvents, activeChannels, keys };
  } catch (e) {
    await client.quit().catch(() => {});
    return { ok: false, error: (e as Error).message, dbSize: 0, recentCount: 0, recentEvents: [], activeChannels: [], keys: [] };
  }
}

/** Everything a single run touches, across both stores — handy for the GUI. */
export async function inspectRun(runId: string) {
  const { runs, events } = await getMongo();
  const [run, evs] = await Promise.all([
    runs.findOne({ _id: runId }),
    events.find({ runId }).sort({ seq: 1 }).toArray(),
  ]);
  return { runId, run, events: evs, channel: runChannel(runId) };
}
