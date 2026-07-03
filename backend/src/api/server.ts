import express, { type Request, type Response } from 'express';
import { randomUUID } from 'node:crypto';
import { createClient, type RedisClientType } from 'redis';
import { config } from '../config.js';
import { getMongo, closeMongo } from '../lib/mongo.js';
import { ResilientPublisher } from '../lib/queue.js';
import { createRun } from '../lib/createRun.js';
import { runChannel } from '../lib/redisPub.js';
import { rateLimitMiddleware } from '../lib/rateLimiter.js';
import type { EventDoc } from '../types.js';
import { health, queueStat, runCounts, recentRuns } from './ops.js';
import { runChaos, type ChaosAction } from './chaos.js';
import { inspectMongo, inspectRedis, inspectRun, inspectQueue } from './inspect.js';
import { getWorkerStatuses } from '../lib/workerHeartbeat.js';

/**
 * Backend API tier — thin, stateless, horizontally scalable. Serves the
 * Next.js frontend (which proxies to it). Does NO agent work itself: it
 * publishes jobs to the queue and streams results back.
 *
 * Run lifecycle:
 *   POST /runs                 → create pending run, publish job, respond now (202)
 *   GET  /runs/:id/stream      → SSE: backfill from lastSeq → live (Redis) + Mongo poll
 *
 * Observability:
 *   GET  /api/health           → {mongo,redis,rabbit,workers}
 *   GET  /api/stats            → {queue, runs, workers}
 *   GET  /api/runs             → recent runs
 *   GET  /api/ops/stream       → SSE of stats every 1s (dashboard live tiles)
 *   GET  /api/inspect/{mongo,redis,run/:id} → data inspector
 *
 * Failure simulator (break it on purpose):
 *   POST /api/chaos            → {action, arg?}  (kill-worker, stop/start-redis, …)
 */
async function main() {
  await getMongo();
  const publisher = new ResilientPublisher();
  await publisher.start().catch((e) => console.warn('[api] initial RabbitMQ connect failed (will retry):', e.message));

  const app = express();
  app.use(express.json());

  app.get('/health', (_req, res) => res.json({ ok: true }));

  // ---------- runs ----------
  const limiter = rateLimitMiddleware({
    windowMs: config.rateLimit.windowMs,
    maxRequests: config.rateLimit.maxRequests,
  });

  app.post('/runs', limiter, async (req: Request, res: Response) => {
    try {
      const threadId: string = req.body?.threadId ?? `thread-${randomUUID().slice(0, 8)}`;
      const seed: number | undefined = req.body?.seed;
      const prompt: string | undefined = req.body?.prompt;
      const run = await createRun({ threadId, seed, prompt });
      await publisher.publish({ runId: run._id, threadId, seed });
      res.status(202).json({ runId: run._id, threadId, status: 'pending' });
    } catch (e) {
      // e.g. Mongo/Rabbit down — surface it so the UI can show the failure.
      res.status(503).json({ error: (e as Error).message });
    }
  });

  // ---------- cancel a run ----------
  app.post('/runs/:id/cancel', async (req: Request, res: Response) => {
    try {
      const runId = req.params.id ?? '';
      const { runs } = await getMongo();
      const run = await runs.findOne({ _id: runId });
      if (!run) return res.status(404).json({ error: 'run not found' });

      // Already terminal — conflict
      const terminalStatuses = ['done', 'failed', 'cancelled'];
      if (terminalStatuses.includes(run.status)) {
        return res.status(409).json({ error: `run already ${run.status}` });
      }

      const now = new Date();
      await runs.updateOne(
        { _id: runId },
        { $set: { status: 'cancelled', cancelledAt: now, updatedAt: now } },
      );
      res.status(200).json({ runId, status: 'cancelled', cancelledAt: now });
    } catch (e) {
      res.status(503).json({ error: (e as Error).message });
    }
  });

  app.get('/runs/:id/stream', async (req: Request, res: Response) => {
    const runId = req.params.id ?? '';
    const lastSeq = Number(req.query.lastSeq ?? 0);

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      // Prevent the Next.js dev proxy (and any reverse proxy) from gzip-buffering
      // the stream — gzip accumulates bytes, so EventSource sees an open socket
      // but no decodable events. no-transform + X-Accel-Buffering:no disable it.
      'Content-Encoding': 'identity',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders?.();
    res.write(`retry: 2000\n\n`);

    let sentSeq = lastSeq;
    let closed = false;

    const send = (event: EventDoc) => {
      if (closed || event.seq <= sentSeq) return;
      sentSeq = Math.max(sentSeq, event.seq);
      res.write(`id: ${event.seq}\n`);
      res.write(`event: ${event.type}\n`);
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    };

    // 1) Backfill since last-seen seq (tolerate Mongo being down).
    try {
      const { events } = await getMongo();
      const backfill = await events.find({ runId, seq: { $gt: lastSeq } }).sort({ seq: 1 }).toArray();
      for (const e of backfill) send(e);
    } catch {
      res.write(`event: transport\ndata: ${JSON.stringify({ note: 'mongo unavailable for backfill' })}\n\n`);
    }

    // 2) Live-tail via Redis (per-run channel only).
    let sub: RedisClientType | null = null;
    try {
      sub = createClient({ url: config.redisUrl, socket: { reconnectStrategy: (r) => Math.min(200 * (r + 1), 3000) } });
      sub.on('error', () => {});
      await sub.connect();
      await sub.subscribe(runChannel(runId), (msg) => {
        try {
          send(JSON.parse(msg) as EventDoc);
        } catch {
          /* ignore */
        }
      });
    } catch {
      sub = null;
    }

    // 3) Reliable Mongo poll fallback (covers Redis down / dropped publish).
    const poll = setInterval(async () => {
      if (closed) return;
      try {
        const { events } = await getMongo();
        const missed = await events.find({ runId, seq: { $gt: sentSeq } }).sort({ seq: 1 }).toArray();
        for (const e of missed) send(e);
        if (missed.some((e) => e.type === 'run_done' || e.type === 'run_failed' || e.type === 'run_cancelled')) cleanup();
      } catch {
        /* transient (e.g. mongo restarting) */
      }
    }, 1000);

    const cleanup = () => {
      if (closed) return;
      closed = true;
      clearInterval(poll);
      sub?.quit().catch(() => {});
      res.end();
    };
    req.on('close', cleanup);
  });

  // ---------- observability ----------
  app.get('/api/health', async (_req, res) => res.json(await health()));

  app.get('/api/stats', async (_req, res) => {
    const [queue, runs] = await Promise.all([queueStat(), runCounts()]);
    res.json({ queue, runs, workers: queue.consumers });
  });

  app.get('/api/workers', async (_req, res) => {
    try {
      res.json(await getWorkerStatuses());
    } catch (e) {
      res.status(503).json({ error: (e as Error).message });
    }
  });

  app.get('/api/runs', async (_req, res) => {
    try {
      res.json(await recentRuns(25));
    } catch (e) {
      res.status(503).json({ error: (e as Error).message });
    }
  });

  app.get('/api/ops/stream', async (_req, res) => {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'Content-Encoding': 'identity',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders?.();
    let closed = false;
    const tick = async () => {
      if (closed) return;
      try {
        const [h, q, rc] = await Promise.all([health(), queueStat(), runCounts()]);
        res.write(`data: ${JSON.stringify({ health: h, queue: q, runs: rc, t: Date.now() })}\n\n`);
      } catch {
        /* skip a tick */
      }
    };
    const iv = setInterval(tick, 1000);
    tick();
    res.on('close', () => {
      closed = true;
      clearInterval(iv);
    });
  });

  // ---------- data inspector (GUI "Data" page) ----------
  app.get('/api/inspect/mongo', async (req, res) => {
    const limit = Math.min(100, Number(req.query.limit ?? 25));
    res.json(await inspectMongo(limit));
  });
  app.get('/api/inspect/redis', async (req, res) => {
    const limit = Math.min(100, Number(req.query.limit ?? 25));
    res.json(await inspectRedis(limit));
  });
  app.get('/api/inspect/run/:id', async (req, res) => {
    try {
      res.json(await inspectRun(req.params.id ?? ''));
    } catch (e) {
      res.status(503).json({ error: (e as Error).message });
    }
  });
  app.get('/api/inspect/queue', async (req, res) => {
    const limit = Math.min(50, Number(req.query.limit ?? 20));
    res.json(await inspectQueue(limit));
  });

  // ---------- conversation history (rebuild chats from Mongo) ----------
  // Groups runs by threadId → a "conversation". Lets the UI restore history
  // after a refresh, since Mongo is the source of truth.
  app.get('/api/conversations', async (_req, res) => {
    try {
      const { runs } = await getMongo();
      const docs = await runs.find({}).sort({ createdAt: 1 }).limit(500).toArray();
      const byThread = new Map<string, any>();
      for (const r of docs) {
        let c = byThread.get(r.threadId);
        if (!c) { c = { threadId: r.threadId, runs: [], lastAt: r.createdAt }; byThread.set(r.threadId, c); }
        c.runs.push({ runId: r._id, status: r.status, claimedBy: r.claimedBy ?? null, createdAt: r.createdAt });
        c.lastAt = r.createdAt;
      }
      const convos = [...byThread.values()].sort((a, b) => +new Date(b.lastAt) - +new Date(a.lastAt));
      res.json(convos);
    } catch (e) {
      res.status(503).json({ error: (e as Error).message });
    }
  });

  // Full transcript of one thread: every run + its user prompt + final reply +
  // which worker handled it. Used to hydrate a conversation on open/refresh.
  app.get('/api/thread/:threadId', async (req, res) => {
    try {
      const { runs, events } = await getMongo();
      const threadId = req.params.threadId ?? '';
      const runDocs = await runs.find({ threadId }).sort({ createdAt: 1 }).toArray();
      const out = [];
      for (const r of runDocs) {
        const evs = await events.find({ runId: r._id }).sort({ seq: 1 }).toArray();
        const done = evs.find((e) => e.type === 'run_done');
        out.push({
          runId: r._id,
          status: r.status,
          claimedBy: r.claimedBy ?? null,
          createdAt: r.createdAt,
          prompt: r.prompt ?? '',
          reply: done ? String((done.payload as any).summary ?? '') : '',
          steps: evs.filter((e) => e.type === 'model_turn' || e.type === 'tool_call' || e.type === 'tool_result')
                    .map((e) => ({ seq: e.seq, type: e.type, payload: e.payload })),
        });
      }
      res.json({ threadId, runs: out });
    } catch (e) {
      res.status(503).json({ error: (e as Error).message });
    }
  });

  // ---------- chaos ----------
  app.post('/api/chaos', async (req: Request, res: Response) => {
    const action = req.body?.action as ChaosAction;
    const arg = req.body?.arg as number | undefined;
    const allowed: ChaosAction[] = [
      'kill-worker', 'stop-redis', 'start-redis', 'stop-mongo', 'start-mongo',
      'stop-rabbit', 'start-rabbit', 'scale-workers',
    ];
    if (!allowed.includes(action)) return res.status(400).json({ ok: false, detail: 'unknown action' });
    res.json(await runChaos(action, arg));
  });

  app.listen(config.webPort, () => {
    console.log('');
    console.log(`  Backend API + live stream ready → http://localhost:${config.webPort}`);
    console.log(`  (serves: POST /runs, SSE event stream, health/metrics, failure-simulator)`);
    console.log(`  Failure simulator: ${config.chaosEnabled ? 'ENABLED' : 'disabled (set CHAOS_ENABLED=1)'} · docker project "${config.composeProject}"`);
    console.log(`  → open the chat UI at http://localhost:3001`);
    console.log('');
  });

  const shutdown = async () => {
    await closeMongo();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((e) => {
  console.error('[web] fatal:', e);
  process.exit(1);
});
