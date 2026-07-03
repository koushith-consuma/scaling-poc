import { exec as execCb } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { config } from '../config.js';
import type { Sandbox } from '../lib/runLoop.js';
import type { ContainerHandle } from '../mock/mockTool.js';

const exec = promisify(execCb);

/**
 * Real container lifecycle (Step 4). Kept as functions INSIDE the worker behind
 * a claim/exec/release interface — NOT a separate service (explicitly out of
 * scope). Maintains a warm pool of pre-booted containers.
 *
 *   claim  → hand over a warm container instantly; background-boot a replacement
 *   exec   → run a command in the claimed container via `docker exec`
 *   release→ tear the container down (workspace is per-run, discarded at end)
 *
 * Overflow policy (pool empty on claim): boot one ON DEMAND and measure the
 * cold-start cost. Logged as claim latency (warm vs cold).
 */

interface PooledContainer extends ContainerHandle {
  cold: boolean;
}

export interface SandboxMetrics {
  warmClaims: number;
  coldClaims: number;
  lastClaimMs: number;
  poolOccupancy: number;
}

class DockerOrchestrator implements Sandbox {
  private pool: PooledContainer[] = [];
  private booting = 0;
  readonly metrics: SandboxMetrics = { warmClaims: 0, coldClaims: 0, lastClaimMs: 0, poolOccupancy: 0 };

  async init(): Promise<void> {
    // Pre-boot the warm pool.
    await Promise.all(Array.from({ length: config.poolSize }, () => this.boot().then((c) => this.pool.push(c))));
    this.metrics.poolOccupancy = this.pool.length;
    console.log(`[sandbox] warm pool ready: ${this.pool.length}/${config.poolSize}`);
  }

  private async boot(): Promise<PooledContainer> {
    const id = `viper-sbx-${randomUUID().slice(0, 8)}`;
    const workspace = '/workspace';
    // Long-lived container: sleep forever, we exec into it per tool call.
    await exec(
      `docker run -d --name ${id} --label viper-poc=1 -w ${workspace} ${config.sandboxImage} sh -c 'mkdir -p ${workspace} && sleep infinity'`,
    );
    return {
      id,
      workspace,
      cold: false,
      exec: async (cmd: string) => {
        const { stdout } = await exec(`docker exec ${id} ${cmd}`);
        return { stdout, code: 0 };
      },
    };
  }

  async claim(_runId: string): Promise<ContainerHandle> {
    const t0 = Date.now();
    let container = this.pool.pop();
    if (container) {
      this.metrics.warmClaims++;
      // Refill the pool in the background.
      void this.refill();
    } else {
      // Overflow: cold-boot on demand and measure it.
      this.metrics.coldClaims++;
      container = await this.boot();
      container.cold = true;
    }
    this.metrics.lastClaimMs = Date.now() - t0;
    this.metrics.poolOccupancy = this.pool.length;
    console.log(`[sandbox] claim ${container.id} ${container.cold ? 'COLD' : 'warm'} ${this.metrics.lastClaimMs}ms pool=${this.pool.length}`);
    return container;
  }

  private async refill(): Promise<void> {
    if (this.pool.length + this.booting >= config.poolSize) return;
    this.booting++;
    try {
      const c = await this.boot();
      this.pool.push(c);
    } catch (err) {
      console.warn('[sandbox] refill boot failed:', (err as Error).message);
    } finally {
      this.booting--;
      this.metrics.poolOccupancy = this.pool.length;
    }
  }

  async release(handle: ContainerHandle): Promise<void> {
    // Per-run workspace is discarded: tear the container down entirely.
    await exec(`docker rm -f ${handle.id}`).catch(() => {});
  }

  async shutdown(): Promise<void> {
    await exec(`docker ps -aq --filter label=viper-poc=1 | xargs -r docker rm -f`).catch(() => {});
  }
}

/** No-op sandbox for when real docker is disabled (Step 2/3). */
class StubSandbox implements Sandbox {
  async claim(runId: string): Promise<ContainerHandle> {
    return { id: `stub-${runId}`, workspace: `/workspace/${runId}` };
  }
  async release(): Promise<void> {}
}

let singleton: DockerOrchestrator | null = null;

/** Factory used by the worker. Returns a real orchestrator if enabled. */
export async function createSandbox(): Promise<Sandbox> {
  if (!config.sandboxEnabled) return new StubSandbox();
  if (!singleton) {
    singleton = new DockerOrchestrator();
    await singleton.init();
  }
  return singleton;
}

export function getSandboxMetrics(): SandboxMetrics | null {
  return singleton?.metrics ?? null;
}
