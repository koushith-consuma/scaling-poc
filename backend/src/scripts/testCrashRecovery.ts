#!/usr/bin/env tsx
/**
 * Test crash recovery:
 * 1. Send messages
 * 2. Kill a worker mid-processing
 * 3. Watch recovery happen
 */

import { execSync } from 'node:child_process';
import { getMongo } from '../lib/mongo.js';

async function sleep(ms: number) {
  return new Promise(r => setTimeout(r, ms));
}

async function testCrashRecovery() {
  console.log('\n🧪 Crash Recovery Test\n');

  // Step 1: Send burst of messages
  console.log('1️⃣  Sending 20 messages across 10 threads...');
  execSync('npm run burst:parallel -- --threads 10 --messages 2', {
    cwd: '/Users/consuma/Desktop/consuma/pos\'s/backend',
    stdio: 'inherit'
  });

  await sleep(2000);

  // Step 2: Check initial state
  const { runs } = await getMongo();
  const initial = await runs.countDocuments({ status: 'running' });
  console.log(`\n2️⃣  Currently running: ${initial} runs`);

  if (initial === 0) {
    console.log('   ⚠️  Nothing running yet, waiting...');
    await sleep(2000);
  }

  // Step 3: Kill a worker
  console.log('\n3️⃣  💥 CRASHING A WORKER...\n');
  try {
    execSync('docker kill --signal=SIGKILL poss-worker-1', { stdio: 'inherit' });
  } catch {
    console.log('   (Worker may already be dead)');
  }

  await sleep(1000);

  // Step 4: Check orphaned runs
  const orphaned = await runs.find({
    status: 'running',
    claimedBy: /worker-.*/
  }).toArray();

  console.log(`\n4️⃣  Orphaned runs: ${orphaned.length}`);
  if (orphaned.length > 0) {
    console.log('   Orphaned run IDs:');
    orphaned.forEach(r => {
      const age = (Date.now() - new Date(r.updatedAt).getTime()) / 1000;
      console.log(`   - ${r._id.slice(0, 8)} (stale for ${age.toFixed(1)}s, claimed by ${r.claimedBy})`);
    });
  }

  // Step 5: Watch reaper recover (if enabled)
  console.log('\n5️⃣  Waiting for reaper to recover (15-20s)...');
  console.log('   (If no reaper enabled, runs stay orphaned)\n');

  for (let i = 0; i < 25; i++) {
    await sleep(1000);
    const stillOrphaned = await runs.countDocuments({
      status: 'running',
      updatedAt: { $lt: new Date(Date.now() - 15000) }
    });

    if (stillOrphaned === 0 && orphaned.length > 0) {
      console.log(`\n   ✅ RECOVERED! All orphaned runs cleaned up after ${i + 1}s`);
      break;
    }

    if (i % 5 === 0) {
      const running = await runs.countDocuments({ status: 'running' });
      const done = await runs.countDocuments({ status: 'done' });
      console.log(`   t=${i}s: ${running} running, ${done} done, ${stillOrphaned} stale`);
    }
  }

  // Step 6: Final state
  await sleep(5000);
  const final = {
    running: await runs.countDocuments({ status: 'running' }),
    done: await runs.countDocuments({ status: 'done' }),
    stale: await runs.countDocuments({
      status: 'running',
      updatedAt: { $lt: new Date(Date.now() - 15000) }
    }),
  };

  console.log('\n6️⃣  Final state:');
  console.log(`   Running: ${final.running}`);
  console.log(`   Done: ${final.done}`);
  console.log(`   Stale (orphaned): ${final.stale}`);

  if (final.stale > 0) {
    console.log('\n   ⚠️  WARNING: Orphaned runs remain!');
    console.log('   Enable reaper to auto-recover:');
    console.log('   docker compose run -e REAPER_ENABLED=1 worker &');
  } else {
    console.log('\n   ✅ SUCCESS: All runs recovered!');
  }

  process.exit(final.stale > 0 ? 1 : 0);
}

testCrashRecovery().catch(console.error);
