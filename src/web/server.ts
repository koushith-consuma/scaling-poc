import express, { type Request, type Response } from 'express';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { createClient, type RedisClientType } from 'redis';
import { config } from '../config.js';
import { getMongo, closeMongo } from '../lib/mongo.js';
import { connectQueue, publishJob } from '../lib/queue.js';
import { createRun } from '../lib/createRun.js';
import { runChannel } from '../lib/redisPub.js';
import type { EventDoc } from '../types.js';
import { health, queueStat, runCounts, recentRuns } from './ops.js';
import { runChaos, type ChaosAction } from './chaos.js';

/**
 * Interactive web app + thin tier.
 *
 * User-facing:
 *   GET  /                     → the test UI (public/index.html)
 *   POST /runs                 → create pending run, publish job, respond now
 *   GET  /runs/:id/stream      → SSE: backfill from lastSeq → live (Redis) + Mongo poll
 *
 * Observability:
 *   GET  /api/health           → {mongo,redis,rabbit,workers}
 *   GET  /api/stats            → {queue, runs, workers}
 *   GET  /api/runs             → recent runs
 *   GET  /api/ops/stream       → SSE of stats every 1s (dashboard live tiles)
 *
 * Chaos (break it on purpose):
 *   POST /api/chaos            → {action, arg?}  (kill-worker, stop/start-redis, …)
 */
const __dirname = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = join(__dirname, 'public');

async function main() {
  await getMongo();
  const { channel } = await connectQueue();

  const app = express();
  app.use(express.json());
  app.use(express.static(PUBLIC_DIR));

  app.get('/health', (_req, res) => res.json({ ok: true }));

  // ---------- runs ----------
  app.post('/runs', async (req: Request, res: Response) => {
    try {
      const threadId: string = req.body?.threadId ?? `thread-${randomUUID().slice(0, 8)}`;
      const seed: number | undefined = req.body?.seed;
      const run = await createRun({ threadId, seed });
      await publishJob(channel, { runId: run._id, threadId, seed });
      res.status(202).json({ runId: run._id, threadId, status: 'pending' });
    } catch (e) {
      // e.g. Mongo/Rabbit down — surface it so the UI can show the failure.
      res.status(503).json({ error: (e as Error).message });
    }
  });

  app.get('/runs/:id/stream', async (req: Request, res: Response) => {
    const runId = req.params.id ?? '';
    const lastSeq = Number(req.query.lastSeq ?? 0);

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
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
        if (missed.some((e) => e.type === 'run_done' || e.type === 'run_failed')) cleanup();
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

  app.get('/api/runs', async (_req, res) => {
    try {
      res.json(await recentRuns(25));
    } catch (e) {
      res.status(503).json({ error: (e as Error).message });
    }
  });

  app.get('/api/ops/stream', async (_req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
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
    console.log(`[web] interactive app on http://localhost:${config.webPort}`);
    console.log(`[web] chaos ${config.chaosEnabled ? 'ENABLED' : 'disabled'} (project=${config.composeProject})`);
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
