import { exec as execCb } from 'node:child_process';
import { promisify } from 'node:util';
import { config } from '../config.js';

const exec = promisify(execCb);

/**
 * Chaos controls for the interactive web app — break the system on purpose and
 * watch it degrade + recover. All actions operate on the docker-compose stack.
 *
 * Safety: only the fixed set of actions below is allowed, and every docker
 * invocation is scoped to `--project-name <composeProject>`. No user input is
 * interpolated into a shell command.
 */

const PROJECT = config.composeProject;

async function dc(args: string): Promise<string> {
  const { stdout, stderr } = await exec(`docker compose -p ${PROJECT} ${args}`);
  return (stdout + stderr).trim();
}

export type ChaosAction =
  | 'kill-worker' // SIGKILL one worker container (crash mid-run)
  | 'stop-redis' // stop Redis (events-OUT layer down → poll fallback)
  | 'start-redis'
  | 'stop-mongo' // stop Mongo (source of truth down → writes fail/retry)
  | 'start-mongo'
  | 'stop-rabbit' // stop RabbitMQ (job intake down)
  | 'start-rabbit'
  | 'scale-workers'; // set worker replicas to N

export interface ChaosResult {
  action: string;
  ok: boolean;
  detail: string;
}

export async function runChaos(action: ChaosAction, arg?: number): Promise<ChaosResult> {
  if (!config.chaosEnabled) return { action, ok: false, detail: 'chaos disabled (CHAOS_ENABLED=0)' };
  try {
    switch (action) {
      case 'kill-worker': {
        // Pick one running worker container id and SIGKILL it (docker kill -s KILL).
        const ids = (await exec(
          `docker ps --filter "label=com.docker.compose.project=${PROJECT}" ` +
            `--filter "label=com.docker.compose.service=worker" --filter "status=running" -q`,
        )).stdout.trim().split('\n').filter(Boolean);
        if (ids.length === 0) return { action, ok: false, detail: 'no running workers to kill' };
        const victim = ids[0]!;
        await exec(`docker kill -s KILL ${victim}`);
        return { action, ok: true, detail: `killed worker ${victim.slice(0, 12)} (${ids.length}→${ids.length - 1})` };
      }
      case 'stop-redis':
        await dc('stop redis');
        return { action, ok: true, detail: 'redis stopped — SSE should fall back to Mongo poll' };
      case 'start-redis':
        await dc('start redis');
        return { action, ok: true, detail: 'redis started — live push resumes' };
      case 'stop-mongo':
        await dc('stop mongo');
        return { action, ok: true, detail: 'mongo stopped — source of truth down' };
      case 'start-mongo':
        await dc('start mongo');
        return { action, ok: true, detail: 'mongo started' };
      case 'stop-rabbit':
        await dc('stop rabbitmq');
        return { action, ok: true, detail: 'rabbitmq stopped — new jobs cannot be published' };
      case 'start-rabbit':
        await dc('start rabbitmq');
        return { action, ok: true, detail: 'rabbitmq started' };
      case 'scale-workers': {
        const n = Math.max(0, Math.min(20, Math.floor(arg ?? 3)));
        await dc(`up -d --no-recreate --scale worker=${n} worker`);
        return { action, ok: true, detail: `scaled workers → ${n}` };
      }
      default:
        return { action, ok: false, detail: 'unknown action' };
    }
  } catch (e) {
    return { action, ok: false, detail: (e as Error).message };
  }
}
