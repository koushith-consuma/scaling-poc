import amqp, { type Channel, type ChannelModel } from 'amqplib';
import { config } from '../config.js';
import type { RunJob } from '../types.js';

/**
 * Thin RabbitMQ helper. Jobs go IN to workers on a durable queue.
 *
 * Resilience: amqplib emits an 'error' event on heartbeat timeout / broker
 * restart. If nothing listens, Node treats it as an unhandled 'error' and
 * CRASHES the process. We attach handlers and auto-reconnect so the web tier
 * and workers survive RabbitMQ blips (including the chaos "Stop RabbitMQ" test).
 */

export interface QueueConnection {
  conn: ChannelModel;
  channel: Channel;
}

/** One-shot connect (used by workers, which re-consume on reconnect). */
export async function connectQueue(): Promise<QueueConnection> {
  const conn = await amqp.connect(config.rabbitUrl, { heartbeat: 30 });
  // Prevent unhandled 'error' from crashing the process.
  conn.on('error', (e) => console.warn('[queue] connection error:', e.message));
  conn.on('close', () => console.warn('[queue] connection closed'));
  const channel = await conn.createChannel();
  channel.on('error', (e) => console.warn('[queue] channel error:', e.message));
  channel.on('close', () => {});
  await channel.assertQueue(config.runQueue, { durable: true });
  return { conn, channel };
}

/**
 * A self-healing publisher connection for the web tier. Always exposes a live
 * channel via `getChannel()`, reconnecting in the background on failure. This is
 * what keeps `POST /runs` working after a RabbitMQ restart instead of 500ing
 * forever (or crashing).
 */
export class ResilientPublisher {
  private conn: ChannelModel | null = null;
  private channel: Channel | null = null;
  private connecting: Promise<void> | null = null;

  async start(): Promise<void> {
    await this.ensure();
  }

  private async ensure(): Promise<void> {
    if (this.channel) return;
    if (this.connecting) return this.connecting;
    this.connecting = (async () => {
      try {
        const conn = await amqp.connect(config.rabbitUrl, { heartbeat: 30 });
        conn.on('error', (e) => console.warn('[publisher] conn error:', e.message));
        conn.on('close', () => {
          console.warn('[publisher] conn closed — will reconnect on next publish');
          this.conn = null;
          this.channel = null;
        });
        const channel = await conn.createChannel();
        channel.on('error', (e) => console.warn('[publisher] channel error:', e.message));
        channel.on('close', () => {
          this.channel = null;
        });
        await channel.assertQueue(config.runQueue, { durable: true });
        this.conn = conn;
        this.channel = channel;
        console.log('[publisher] connected to RabbitMQ');
      } finally {
        this.connecting = null;
      }
    })();
    return this.connecting;
  }

  /** Publish a job, (re)connecting if needed. Throws if RabbitMQ is unreachable
   *  so the caller (POST /runs) can return a clean 503. */
  async publish(job: RunJob, delayMs = 0): Promise<void> {
    await this.ensure();
    if (!this.channel) throw new Error('RabbitMQ unavailable');
    await publishJob(this.channel, job, delayMs);
  }

  async close(): Promise<void> {
    try {
      await this.channel?.close();
      await this.conn?.close();
    } catch {
      /* ignore */
    }
    this.channel = null;
    this.conn = null;
  }
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
