import { randomUUID } from 'node:crypto';
import { getMongo } from './mongo.js';
import type { EventDoc, EventType } from '../types.js';

/**
 * Optional live-event publisher (Redis, Step 6+). Injected so the loop has no
 * hard Redis dependency. Fire-and-forget: a failure here must NEVER fail the
 * Mongo write or the loop. Mongo is source of truth; Redis is the speed layer.
 */
export type LivePublisher = (event: EventDoc) => void;

let livePublisher: LivePublisher | null = null;
export function setLivePublisher(pub: LivePublisher | null): void {
  livePublisher = pub;
}

export interface EmitInput {
  runId: string;
  threadId: string;
  type: EventType;
  payload?: Record<string, unknown>;
}

/**
 * Append an event to Mongo with an atomically-incremented per-run seq, then
 * (optionally) publish it to the live layer. Returns the assigned seq.
 *
 * The seq comes from incrementing runs.lastEventSeq in the same atomic op that
 * updates the run doc, guaranteeing gap-free ordering per run.
 */
export async function emitEvent(input: EmitInput): Promise<number> {
  const { runs, events } = await getMongo();
  const now = new Date();

  // Atomically bump the per-run counter and read the new value.
  const updated = await runs.findOneAndUpdate(
    { _id: input.runId },
    { $inc: { lastEventSeq: 1 }, $set: { updatedAt: now } },
    { returnDocument: 'after', projection: { lastEventSeq: 1 } },
  );
  if (!updated) {
    throw new Error(`emitEvent: run ${input.runId} not found`);
  }
  const seq = updated.lastEventSeq;

  const event: EventDoc = {
    _id: randomUUID(),
    runId: input.runId,
    threadId: input.threadId,
    seq,
    type: input.type,
    payload: input.payload ?? {},
    createdAt: now,
  };

  await events.insertOne(event);

  // Fire-and-forget live push. Swallow any error.
  if (livePublisher) {
    try {
      livePublisher(event);
    } catch (err) {
      console.warn('[emitEvent] live publish failed (ignored):', (err as Error).message);
    }
  }

  return seq;
}
