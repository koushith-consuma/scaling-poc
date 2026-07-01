/**
 * Step 4 acceptance — real container lifecycle.
 * Requires docker + SANDBOX_ENABLED=1.
 *
 *   SANDBOX_ENABLED=1 POOL_SIZE=3 npm run sandbox:check
 *
 * Proves:
 *  - a file written by a tool in call 1 is visible in call 2 (same container,
 *    workspace persists across tool calls within a run)
 *  - container is released at run end
 *  - pool refills in the background
 *  - claim latency logged (warm vs cold)
 */
import { config } from '../config.js';
import { createSandbox, getSandboxMetrics } from '../sandbox/orchestrator.js';
import { sleep } from '../lib/rng.js';

async function main() {
  if (!config.sandboxEnabled) {
    console.error('Set SANDBOX_ENABLED=1 to run this check.');
    process.exit(1);
  }

  const sandbox = await createSandbox();

  console.log('\n== warm claim (from pool) ==');
  const h1 = await sandbox.claim('run-1');
  console.log('claimed', h1.id, 'workspace', h1.workspace);

  console.log('\n== persistence across tool calls (same container) ==');
  // Call 1: write a file.
  await h1.exec!(`sh -c 'echo "written-in-call-1" > ${h1.workspace}/persist.txt'`);
  // Call 2: read it back.
  const read = await h1.exec!(`cat ${h1.workspace}/persist.txt`);
  console.log('call 2 reads:', read.stdout.trim());
  console.log('persistence:', read.stdout.trim() === 'written-in-call-1' ? 'OK' : 'FAIL');

  // Let the background refill run.
  await sleep(1500);
  console.log('pool occupancy after refill:', getSandboxMetrics()?.poolOccupancy);

  console.log('\n== release at run end ==');
  await sandbox.release(h1);
  const gone = await import('node:child_process').then(
    (cp) =>
      new Promise<string>((resolve) =>
        cp.exec(`docker ps -aq --filter name=${h1.id}`, (_e, out) => resolve(out.trim())),
      ),
  );
  console.log('container after release:', gone === '' ? 'removed OK' : `still present (${gone})`);

  console.log('\n== drain pool to force a COLD claim (overflow policy) ==');
  const held = [];
  for (let i = 0; i < config.poolSize + 1; i++) {
    held.push(await sandbox.claim(`drain-${i}`));
  }
  const m = getSandboxMetrics();
  console.log('metrics:', m);
  console.log('cold claims observed:', (m?.coldClaims ?? 0) > 0 ? 'OK' : 'none (pool was large enough)');

  console.log('\n== cleanup ==');
  await Promise.all(held.map((h) => sandbox.release(h)));
  // Shutdown any remaining pooled containers.
  await (sandbox as any).shutdown?.();
  console.log('done');
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
