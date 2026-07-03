#!/usr/bin/env tsx
/**
 * Demonstrate RabbitMQ prefetch behavior:
 * - How many messages does worker actually hold?
 * - Does it exceed prefetch limit?
 */

import { connectQueue } from '../lib/queue.js';

async function demo() {
  console.log('\n📊 RabbitMQ Prefetch Demo\n');

  const { conn, channel } = await connectQueue();

  // Publish 100 test messages
  await channel.assertQueue('prefetch-demo', { durable: false });
  await channel.purgeQueue('prefetch-demo');

  console.log('Publishing 100 messages...');
  for (let i = 1; i <= 100; i++) {
    await channel.sendToQueue('prefetch-demo', Buffer.from(JSON.stringify({ id: i })));
  }
  console.log('✓ 100 messages in queue\n');

  // Consumer with prefetch=5
  const PREFETCH = 5;
  await channel.prefetch(PREFETCH);

  let messagesInFlight = 0;
  let messagesReceived = 0;
  let maxInFlight = 0;

  console.log(`Starting consumer with prefetch=${PREFETCH}\n`);
  console.log('Time | Received | In Flight | Max Ever | Status');
  console.log('---- | -------- | --------- | -------- | ------');

  const startTime = Date.now();
  const log = () => {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(
      `${elapsed.padStart(4)}s | ` +
      `${String(messagesReceived).padStart(8)} | ` +
      `${String(messagesInFlight).padStart(9)} | ` +
      `${String(maxInFlight).padStart(8)} | ` +
      `${messagesInFlight > PREFETCH ? '❌ EXCEEDED!' : '✓ Within limit'}`
    );
  };

  await channel.consume('prefetch-demo', async (msg) => {
    if (!msg) return;

    messagesReceived++;
    messagesInFlight++;
    maxInFlight = Math.max(maxInFlight, messagesInFlight);

    log();

    // Simulate long-running agent loop (2 seconds)
    await new Promise(r => setTimeout(r, 2000));

    // ACK after processing
    channel.ack(msg);
    messagesInFlight--;

    // Stop after 20 messages to keep demo short
    if (messagesReceived >= 20) {
      console.log(`\n📈 Results:`);
      console.log(`   Max in-flight: ${maxInFlight}`);
      console.log(`   Prefetch limit: ${PREFETCH}`);
      console.log(`   ${maxInFlight <= PREFETCH ? '✓' : '❌'} Prefetch limit respected!\n`);

      await channel.close();
      await conn.close();
      process.exit(0);
    }
  });
}

demo().catch(console.error);
