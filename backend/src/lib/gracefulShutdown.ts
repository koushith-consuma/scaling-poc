import type { Channel, Connection } from 'amqplib';

/**
 * Graceful shutdown handler for workers.
 *
 * WHY THIS IS CRITICAL:
 * - When K8s/Docker scales down → sends SIGTERM
 * - Worker gets 30 seconds to finish in-flight work
 * - Without this: runs orphaned, thread locks stuck, messages lost
 *
 * This handler:
 * 1. Stops accepting new messages (cancel consumer)
 * 2. Waits for in-flight runs to complete (up to 30s)
 * 3. Nacks any unfinished messages (RabbitMQ redelivers)
 * 4. Releases resources
 */

export interface ShutdownHandler {
  initiate: () => Promise<void>;
  markRunning: (runId: string) => void;
  markComplete: (runId: string) => void;
  isShuttingDown: () => boolean;
}

export function createShutdownHandler(
  workerId: string,
  channel: Channel,
  conn: Connection,
  consumerTag: string,
): ShutdownHandler {
  let shuttingDown = false;
  const inFlightRuns = new Set<string>();
  let shutdownResolve: (() => void) | null = null;

  const initiate = async () => {
    if (shuttingDown) return;
    shuttingDown = true;

    console.log(`[${workerId}] 🛑 Graceful shutdown initiated`);
    console.log(`[${workerId}] In-flight runs: ${inFlightRuns.size}`);

    // 1. Stop consuming new messages
    try {
      await channel.cancel(consumerTag);
      console.log(`[${workerId}] ✓ Stopped consuming new messages`);
    } catch (e) {
      console.warn(`[${workerId}] Failed to cancel consumer:`, (e as Error).message);
    }

    // 2. Wait for in-flight runs (max 30s)
    if (inFlightRuns.size > 0) {
      console.log(`[${workerId}] Waiting for ${inFlightRuns.size} runs to complete...`);

      await Promise.race([
        new Promise<void>((resolve) => { shutdownResolve = resolve; }),
        new Promise<void>((resolve) => setTimeout(() => {
          console.warn(`[${workerId}] ⚠️  Timeout: ${inFlightRuns.size} runs still in-flight, forcing shutdown`);
          resolve();
        }, 28000)), // 28s (leave 2s buffer before K8s kills)
      ]);
    }

    // 3. Close connections
    try {
      await channel.close();
      await conn.close();
      console.log(`[${workerId}] ✓ Connections closed`);
    } catch (e) {
      console.warn(`[${workerId}] Error closing connections:`, (e as Error).message);
    }

    console.log(`[${workerId}] ✓ Graceful shutdown complete`);
  };

  const markRunning = (runId: string) => {
    inFlightRuns.add(runId);
  };

  const markComplete = (runId: string) => {
    inFlightRuns.delete(runId);

    // If all in-flight runs complete during shutdown, resolve early
    if (shuttingDown && inFlightRuns.size === 0 && shutdownResolve) {
      console.log(`[${workerId}] ✓ All in-flight runs completed`);
      shutdownResolve();
    }
  };

  const isShuttingDown = () => shuttingDown;

  return { initiate, markRunning, markComplete, isShuttingDown };
}
