import amqp, { type Channel, type ChannelModel } from 'amqplib';
import { config } from '../config.js';
import type { RunJob } from '../types.js';

/** Thin RabbitMQ helper. Jobs go IN to workers on a durable queue. */
export async function connectQueue(): Promise<{ conn: ChannelModel; channel: Channel }> {
  const conn = await amqp.connect(config.rabbitUrl);
  const channel = await conn.createChannel();
  await channel.assertQueue(config.runQueue, { durable: true });
  return { conn, channel };
}

export async function publishJob(channel: Channel, job: RunJob, delayMs = 0): Promise<void> {
  const body = Buffer.from(JSON.stringify(job));
  const send = () =>
    channel.sendToQueue(config.runQueue, body, {
      persistent: true,
      contentType: 'application/json',
    });
  if (delayMs > 0) {
    // POC-simple delayed requeue (no plugin): hold then publish.
    setTimeout(send, delayMs);
  } else {
    send();
  }
}

export function parseJob(content: Buffer): RunJob {
  return JSON.parse(content.toString()) as RunJob;
}
