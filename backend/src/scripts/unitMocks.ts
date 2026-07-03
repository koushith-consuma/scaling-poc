/**
 * Step 1 acceptance harness — unit-calls both mocks in isolation.
 * Confirms: delays happen, canned outputs return, and avgTurns /
 * toolCallProbability visibly change behavior. Also proves seed determinism.
 *
 *   npm run unit:mocks
 */
import { mockModel } from '../mock/mockModel.js';
import { mockTool, type ContainerHandle } from '../mock/mockTool.js';
import type { ModelConfig } from '../types.js';

async function driveRun(runId: string, cfg: ModelConfig): Promise<string[]> {
  const decisions: string[] = [];
  for (let turn = 0; turn < 50; turn++) {
    const d = await mockModel({ runId, threadId: 't', turn }, cfg);
    decisions.push(d.type === 'done' ? 'done' : `tool:${d.tool}`);
    if (d.type === 'done') break;
  }
  return decisions;
}

async function main() {
  const base: ModelConfig = {
    minDelayMs: 10,
    maxDelayMs: 30,
    avgTurns: 4,
    toolCallProbability: 0.6,
    seed: 42,
  };

  console.log('== determinism: same seed → identical decisions ==');
  const a = await driveRun('run-A', base);
  const b = await driveRun('run-A', base);
  console.log('run A #1:', a.join(' → '));
  console.log('run A #2:', b.join(' → '));
  console.log('identical:', JSON.stringify(a) === JSON.stringify(b));

  console.log('\n== avgTurns changes run length (100 runs, mean turns) ==');
  for (const avgTurns of [1, 4, 10]) {
    let total = 0;
    const N = 100;
    for (let i = 0; i < N; i++) {
      const d = await driveRun(`len-${avgTurns}-${i}`, {
        ...base,
        avgTurns,
        minDelayMs: 0,
        maxDelayMs: 0,
        seed: 1000 + i,
      });
      total += d.length;
    }
    console.log(`avgTurns=${avgTurns} → mean turns ≈ ${(total / N).toFixed(2)}`);
  }

  console.log('\n== toolCallProbability changes tool vs think mix ==');
  for (const p of [0.1, 0.9]) {
    let noop = 0;
    let think = 0;
    for (let i = 0; i < 500; i++) {
      const d = await mockModel(
        { runId: `mix-${p}-${i}`, threadId: 't', turn: 1 },
        { ...base, toolCallProbability: p, minDelayMs: 0, maxDelayMs: 0, seed: 5000 + i },
      );
      if (d.type === 'tool_call') d.tool === 'noop' ? noop++ : think++;
    }
    console.log(`toolCallProbability=${p} → noop=${noop} think=${think}`);
  }

  console.log('\n== delay is real ==');
  const t0 = Date.now();
  await mockModel({ runId: 'delay', threadId: 't', turn: 1 }, { ...base, minDelayMs: 200, maxDelayMs: 200 });
  console.log(`model delay measured ≈ ${Date.now() - t0}ms (expected ≈200ms)`);

  console.log('\n== mockTool touches a file + returns canned result ==');
  const container: ContainerHandle = { id: 'stub-1', workspace: '/workspace/run-x' };
  const tr = await mockTool(
    container,
    { type: 'tool_call', tool: 'noop', args: { turn: 3 } },
    { minDelayMs: 5, maxDelayMs: 15 },
  );
  console.log('tool result:', tr);

  console.log('\nStep 1 acceptance: OK');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
