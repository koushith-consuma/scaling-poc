import { createClient, type RedisClientType } from 'redis';
import { config } from '../config.js';

/**
 * Worker heartbeat system — tracks which workers are alive and what they're
 * processing in real-time. Uses Redis for ephemeral state (perfect for this).
 *
 * Schema (Redis keys):
 *   worker:{workerId}:heartbeat     → JSON { alive, lastSeen, currentRun?, startedAt? }
 *   worker:active                   → Set of active workerIds
 *   run:{runId}:worker              → workerId processing this run (with TTL)
 */

export interface WorkerStatus {
  workerId: string;
  alive: boolean;
  lastSeen: number; // timestamp
  currentRun?: string; // runId
  currentThread?: string; // threadId
  startedAt?: number; // when it started processing current run
}

export class WorkerHeartbeatTracker {
  private redis: RedisClientType | null = null;
  private workerId: string;
  private interval: NodeJS.Timeout | null = null;
  private currentRun: { runId: string; threadId: string } | null = null;

  constructor(workerId: string) {
    this.workerId = workerId;
  }

  async start(): Promise<void> {
    if (!config.redisEnabled) return;
    try {
      this.redis = createClient({
        url: config.redisUrl,
        socket: { reconnectStrategy: (r) => Math.min(200 * (r + 1), 3000) },
      });
      this.redis.on('error', () => {});
      await this.redis.connect();

      // Heartbeat every 2s (mark alive + current work)
      this.interval = setInterval(() => this.beat(), 2000);
      await this.beat();
    } catch (e) {
      console.warn(`[${this.workerId}] heartbeat tracker unavailable:`, (e as Error).message);
      this.redis = null;
    }
  }

  async markProcessing(runId: string, threadId: string): Promise<void> {
    this.currentRun = { runId, threadId };
    await this.beat();
  }

  async markIdle(): Promise<void> {
    this.currentRun = null;
    await this.beat();
  }

  private async beat(): Promise<void> {
    if (!this.redis) return;
    try {
      const status: WorkerStatus = {
        workerId: this.workerId,
        alive: true,
        lastSeen: Date.now(),
        ...(this.currentRun
          ? {
              currentRun: this.currentRun.runId,
              currentThread: this.currentRun.threadId,
              startedAt: Date.now(),
            }
          : {}),
      };
      const key = `worker:${this.workerId}:heartbeat`;
      await this.redis.setEx(key, 10, JSON.stringify(status)); // 10s TTL
      await this.redis.sAdd('worker:active', this.workerId);
      if (this.currentRun) {
        await this.redis.setEx(`run:${this.currentRun.runId}:worker`, 30, this.workerId);
      }
    } catch {
      // transient redis blip, skip this beat
    }
  }

  async stop(): Promise<void> {
    if (this.interval) clearInterval(this.interval);
    if (!this.redis) return;
    try {
      await this.redis.sRem('worker:active', this.workerId);
      await this.redis.del(`worker:${this.workerId}:heartbeat`);
      await this.redis.quit();
    } catch {
      /* ignore */
    }
  }
}

/**
 * Get all active worker statuses (for the ops panel / UI).
 */
export async function getWorkerStatuses(): Promise<WorkerStatus[]> {
  if (!config.redisEnabled) return [];
  let redis: RedisClientType | null = null;
  try {
    redis = createClient({
      url: config.redisUrl,
      socket: { reconnectStrategy: false },
    });
    redis.on('error', () => {});
    await redis.connect();

    const workerIds = await redis.sMembers('worker:active');
    const statuses: WorkerStatus[] = [];
    for (const wid of workerIds) {
      const key = `worker:${wid}:heartbeat`;
      const raw = await redis.get(key);
      if (raw) {
        try {
          statuses.push(JSON.parse(raw) as WorkerStatus);
        } catch (e) {
          console.warn(`Failed to parse worker status for ${wid}:`, e);
        }
      }
    }
    await redis.quit();
    return statuses.sort((a, b) => a.workerId.localeCompare(b.workerId));
  } catch (e) {
    console.error('Failed to fetch worker statuses:', (e as Error).message);
    if (redis) await redis.quit().catch(() => {});
    return [];
  }
}
