import { createClient, type RedisClientType } from 'redis';
import { config } from '../config.js';
import type { EventDoc } from '../types.js';

/**
 * Redis publisher (Step 6). Events go OUT to clients on a per-run channel
 * `run:{runId}`. Fire-and-forget — a Redis failure must NEVER fail the Mongo
 * write or the loop, so publish() swallows errors.
 */
export interface RedisPublisher {
  publish: (event: EventDoc) => void;
  close: () => Promise<void>;
}

export function runChannel(runId: string): string {
  return `run:${runId}`;
}

export async function createRedisPublisher(): Promise<RedisPublisher> {
  const client: RedisClientType = createClient({
    url: config.redisUrl,
    // Self-healing: keep reconnecting with capped backoff so the publisher
    // recovers when Redis comes back (e.g. after the Step 6 kill-redis test).
    // The initial connect() below still fails fast if Redis is absent at boot.
    socket: {
      reconnectStrategy: (retries) => Math.min(200 * (retries + 1), 3000),
      connectTimeout: 2000,
    },
  });
  // Swallow errors quietly — Redis is fire-and-forget; Mongo is source of truth.
  client.on('error', () => {});
  await client.connect();

  // Rate-limit publish-failure logging so a Redis outage doesn't spam the log
  // (which would drown the load-test output). One line per second at most.
  let lastWarn = 0;
  let suppressed = 0;
  const warn = (msg: string) => {
    const now = Number(process.hrtime.bigint() / 1_000_000n);
    if (now - lastWarn > 1000) {
      const extra = suppressed > 0 ? ` (+${suppressed} suppressed)` : '';
      console.warn(`[redis-pub] ${msg}${extra}`);
      lastWarn = now;
      suppressed = 0;
    } else {
      suppressed++;
    }
  };

  return {
    publish: (event: EventDoc) => {
      // Fire-and-forget. Do not await inside the loop's hot path. Skip entirely
      // when the socket is down so we don't queue unbounded offline commands.
      if (!client.isReady) {
        warn('skip publish: redis not ready');
        return;
      }
      client
        .publish(runChannel(event.runId), JSON.stringify(event))
        .catch((e) => warn(`publish failed (ignored): ${(e as Error).message}`));
    },
    close: async () => {
      await client.quit().catch(() => {});
    },
  };
}
