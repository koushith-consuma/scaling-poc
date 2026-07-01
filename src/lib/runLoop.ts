import { emitEvent } from './emitEvent.js';
import { finishRun } from './claimRun.js';
import { mockModel } from '../mock/mockModel.js';
import { mockTool, type ContainerHandle } from '../mock/mockTool.js';
import type { ModelConfig, RunJob } from '../types.js';

/**
 * The agent loop. PURE module — zero web-framework imports. Consumes injected
 * collaborators so it can run inside a worker, a test, or (later) anything else.
 *
 *   claim (done by caller) → ask model → if tool_call, claim sandbox once/run &
 *   exec tool → emit each step to Mongo → repeat until done → mark run done.
 */

/** Sandbox interface the loop depends on. Step 2/3 pass a stub; Step 4 passes
 *  the real docker-backed orchestrator. claim() is called at most once per run
 *  and the handle is reused across tool calls (workspace persists). */
export interface Sandbox {
  claim(runId: string): Promise<ContainerHandle>;
  release(handle: ContainerHandle): Promise<void>;
  /** Tear down the whole pool (worker shutdown). Optional. */
  shutdown?(): Promise<void>;
}

export interface RunLoopDeps {
  modelConfig: ModelConfig;
  toolConfig: { minDelayMs: number; maxDelayMs: number };
  sandbox?: Sandbox; // absent in Step 2 → tool runs against a stub handle
  maxTurns?: number; // safety cap
}

export interface RunLoopResult {
  turns: number;
  toolCalls: number;
  status: 'done' | 'failed';
}

/** Stub container used when no real sandbox is wired (Step 2/3). */
function stubHandle(runId: string): ContainerHandle {
  return { id: `stub-${runId}`, workspace: `/workspace/${runId}` };
}

export async function runLoop(job: RunJob, deps: RunLoopDeps): Promise<RunLoopResult> {
  const { runId, threadId, seed } = job;
  const maxTurns = deps.maxTurns ?? 100;
  const modelConfig: ModelConfig = { ...deps.modelConfig, seed: seed ?? deps.modelConfig.seed };

  await emitEvent({ runId, threadId, type: 'run_started', payload: { seed } });

  let container: ContainerHandle | null = null;
  let toolCalls = 0;

  try {
    let turn = 0;
    for (; turn < maxTurns; turn++) {
      const decision = await mockModel({ runId, threadId, turn }, modelConfig);

      if (decision.type === 'done') {
        await emitEvent({
          runId,
          threadId,
          type: 'run_done',
          payload: { summary: decision.summary, turns: turn + 1, toolCalls },
        });
        break;
      }

      // tool_call
      await emitEvent({
        runId,
        threadId,
        type: 'tool_call',
        payload: { tool: decision.tool, turn },
      });

      // Claim the sandbox once per run, reuse across calls (workspace persists).
      if (deps.sandbox && !container) {
        container = await deps.sandbox.claim(runId);
      }
      const handle = container ?? stubHandle(runId);

      const toolResult = await mockTool(handle, decision, {
        minDelayMs: deps.toolConfig.minDelayMs,
        maxDelayMs: deps.toolConfig.maxDelayMs,
        seed: seed !== undefined ? seed + turn : undefined,
      });
      toolCalls++;

      await emitEvent({
        runId,
        threadId,
        type: 'tool_result',
        payload: { tool: decision.tool, turn, result: toolResult.result, file: toolResult.file },
      });
    }

    if (turn >= maxTurns) {
      await emitEvent({ runId, threadId, type: 'run_done', payload: { note: 'maxTurns reached', turns: turn, toolCalls } });
    }

    await finishRun(runId, 'done');
    return { turns: turn + 1, toolCalls, status: 'done' };
  } catch (err) {
    const message = (err as Error).message;
    await emitEvent({ runId, threadId, type: 'run_failed', payload: { error: message } }).catch(() => {});
    await finishRun(runId, 'failed', message);
    return { turns: 0, toolCalls, status: 'failed' };
  } finally {
    if (container && deps.sandbox) {
      await deps.sandbox.release(container).catch(() => {});
    }
  }
}
