import { writeFileSync, mkdirSync } from 'node:fs';
import { config } from '../config.js';

/**
 * Load-test instrumentation (Step 7).
 *
 * Records per-run lifecycle timestamps (published → claimed → completed),
 * samples RabbitMQ queue depth on an interval via the management API, measures
 * Mongo query latency, and exports everything to CSV.
 */

export interface RunRecord {
  runId: string;
  threadId: string;
  publishedAt: number;
  claimedAt?: number; // from run doc (status→running)
  completedAt?: number; // observed done
  timeToPickupMs?: number;
  durationMs?: number;
  secondMessage: boolean;
  reconnected: boolean;
  status?: string;
}

export interface QueueSample {
  t: number;
  depth: number; // messages ready + unacked
  ready: number;
  unacked: number;
}

export interface MongoSample {
  t: number;
  latencyMs: number; // a representative indexed query
}

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx]!;
}

export class Metrics {
  runs = new Map<string, RunRecord>();
  queueSamples: QueueSample[] = [];
  mongoSamples: MongoSample[] = [];
  readonly startedAt = Date.now();

  markPublished(runId: string, threadId: string, opts: { secondMessage?: boolean } = {}) {
    this.runs.set(runId, {
      runId,
      threadId,
      publishedAt: Date.now(),
      secondMessage: opts.secondMessage ?? false,
      reconnected: false,
    });
  }

  markReconnected(runId: string) {
    const r = this.runs.get(runId);
    if (r) r.reconnected = true;
  }

  markCompleted(runId: string, claimedAtMs: number | undefined, status: string) {
    const r = this.runs.get(runId);
    if (!r) return;
    r.completedAt = Date.now();
    r.status = status;
    if (claimedAtMs) {
      r.claimedAt = claimedAtMs;
      r.timeToPickupMs = claimedAtMs - r.publishedAt;
    }
    r.durationMs = r.completedAt - r.publishedAt;
  }

  addQueueSample(s: QueueSample) {
    this.queueSamples.push(s);
  }
  addMongoSample(s: MongoSample) {
    this.mongoSamples.push(s);
  }

  /** Sample RabbitMQ queue depth from the management API. */
  async sampleQueue(): Promise<QueueSample | null> {
    try {
      // Node's fetch (undici) ignores userinfo in the URL, so split it out and
      // send credentials via an Authorization header instead.
      const raw = config.rabbitMgmtUrl.replace(/\/$/, '');
      const parsed = new URL(raw);
      const user = decodeURIComponent(parsed.username || 'guest');
      const pass = decodeURIComponent(parsed.password || 'guest');
      parsed.username = '';
      parsed.password = '';
      const base = parsed.toString().replace(/\/$/, '');
      const url = `${base}/api/queues/%2F/${config.runQueue}`;
      const res = await fetch(url, {
        headers: { Authorization: `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}` },
      });
      if (!res.ok) return null;
      const q: any = await res.json();
      const sample: QueueSample = {
        t: Date.now(),
        ready: q.messages_ready ?? 0,
        unacked: q.messages_unacknowledged ?? 0,
        depth: (q.messages_ready ?? 0) + (q.messages_unacknowledged ?? 0),
      };
      this.addQueueSample(sample);
      return sample;
    } catch {
      return null;
    }
  }

  summary() {
    const done = [...this.runs.values()].filter((r) => r.status === 'done');
    const pickup = done.map((r) => r.timeToPickupMs ?? 0).filter((x) => x > 0).sort((a, b) => a - b);
    const dur = done.map((r) => r.durationMs ?? 0).sort((a, b) => a - b);
    const queueDepths = this.queueSamples.map((s) => s.depth);
    const mongoLat = this.mongoSamples.map((s) => s.latencyMs).sort((a, b) => a - b);
    return {
      totalRuns: this.runs.size,
      completed: done.length,
      timeToPickupP50: pct(pickup, 50),
      timeToPickupP95: pct(pickup, 95),
      durationP50: pct(dur, 50),
      durationP95: pct(dur, 95),
      peakQueueDepth: queueDepths.length ? Math.max(...queueDepths) : 0,
      finalQueueDepth: queueDepths.at(-1) ?? 0,
      mongoLatP50: pct(mongoLat, 50),
      mongoLatP95: pct(mongoLat, 95),
    };
  }

  /** Write per-run + samples CSVs and a summary JSON. Returns file paths. */
  export(label: string): { dir: string } {
    const dir = `loadtest-results/${label}`;
    mkdirSync(dir, { recursive: true });

    const runRows = ['runId,threadId,publishedAt,claimedAt,completedAt,timeToPickupMs,durationMs,secondMessage,reconnected,status'];
    for (const r of this.runs.values()) {
      runRows.push(
        [
          r.runId,
          r.threadId,
          r.publishedAt,
          r.claimedAt ?? '',
          r.completedAt ?? '',
          r.timeToPickupMs ?? '',
          r.durationMs ?? '',
          r.secondMessage,
          r.reconnected,
          r.status ?? '',
        ].join(','),
      );
    }
    writeFileSync(`${dir}/runs.csv`, runRows.join('\n'));

    const qRows = ['t,ready,unacked,depth'];
    for (const s of this.queueSamples) qRows.push([s.t, s.ready, s.unacked, s.depth].join(','));
    writeFileSync(`${dir}/queue.csv`, qRows.join('\n'));

    const mRows = ['t,latencyMs'];
    for (const s of this.mongoSamples) mRows.push([s.t, s.latencyMs].join(','));
    writeFileSync(`${dir}/mongo.csv`, mRows.join('\n'));

    writeFileSync(`${dir}/summary.json`, JSON.stringify(this.summary(), null, 2));
    return { dir };
  }
}
