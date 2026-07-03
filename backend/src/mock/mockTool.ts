import type { ModelDecision } from '../types.js';
import { makeRng, randInt, sleep } from '../lib/rng.js';

export interface MockToolConfig {
  minDelayMs: number;
  maxDelayMs: number;
  seed?: number;
}

/** A container handle the tool runs against. In Step 2/3 this is a stub
 *  (workspace path only). In Step 4+ it's backed by a real docker container
 *  and `exec` runs a real `docker exec`. */
export interface ContainerHandle {
  id: string;
  workspace: string;
  /** Run a shell command inside the container. Stub impl just resolves. */
  exec?: (cmd: string) => Promise<{ stdout: string; code: number }>;
}

export interface MockToolResult {
  ok: boolean;
  result: string;
  file: string;
  delayMs: number;
}

/**
 * Mock tool — stands in for the tool's inner work.
 *  - sleeps a randomized delay
 *  - touches/writes a file in the container's workspace (proves persistence)
 *  - returns a canned result string
 */
export async function mockTool(
  container: ContainerHandle,
  toolCall: Extract<ModelDecision, { type: 'tool_call' }>,
  config: MockToolConfig,
): Promise<MockToolResult> {
  const rng = makeRng(config.seed);
  const delayMs = randInt(rng, config.minDelayMs, config.maxDelayMs);
  await sleep(delayMs);

  const turn = Number(toolCall.args?.turn ?? 0);
  const file = `${container.workspace}/step-${turn}.txt`;
  const line = `tool=${toolCall.tool} turn=${turn} container=${container.id}`;

  if (container.exec) {
    // Real container path (Step 4+): append a line to a file in the workspace.
    await container.exec(`sh -c 'echo "${line}" >> ${file}'`);
  }
  // Stub path (Step 2/3): no real fs write; the result string is enough to
  // drive the loop. Real persistence is proven once the sandbox is wired.

  return {
    ok: true,
    result: `${toolCall.tool} wrote ${file}`,
    file,
    delayMs,
  };
}
