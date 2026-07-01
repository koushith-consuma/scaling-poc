/** Shared domain types across worker, web, mocks, and load harness. */

export type RunStatus = 'pending' | 'running' | 'done' | 'failed';

export interface RunDoc {
  _id: string; // runId
  threadId: string;
  status: RunStatus;
  lastEventSeq: number;
  createdAt: Date;
  updatedAt: Date;
  claimedBy?: string; // worker id, for crash-recovery / reaper
  claimedAt?: Date;
  finishedAt?: Date;
  error?: string;
  seed?: number; // for reproducible mock runs
}

export type EventType =
  | 'run_started'
  | 'model_turn'
  | 'tool_call'
  | 'tool_result'
  | 'run_done'
  | 'run_failed';

export interface EventDoc {
  _id: string;
  runId: string;
  threadId: string;
  seq: number; // per-run incrementing sequence
  type: EventType;
  payload: Record<string, unknown>;
  createdAt: Date;
}

/** Job placed on RabbitMQ. Jobs go IN to workers. */
export interface RunJob {
  runId: string;
  threadId: string;
  seed?: number;
}

/** Mock model decision returned each turn. */
export type ModelDecision =
  | { type: 'tool_call'; tool: string; args?: Record<string, unknown> }
  | { type: 'done'; summary?: string };

export interface ModelContext {
  runId: string;
  threadId: string;
  turn: number; // 0-based turn index within the run
}

export interface ModelConfig {
  minDelayMs: number;
  maxDelayMs: number;
  avgTurns: number; // expected turns before 'done'
  toolCallProbability: number; // chance a non-final turn is a tool_call
  seed?: number; // deterministic when provided
}
