/**
 * Idempotency guard for run re-execution.
 *
 * When a run is re-processed after a crash, the loop checks which turns
 * already executed by reading the events collection. If turn N already has
 * a tool_result event, we skip re-executing that turn and use the stored result.
 *
 * This prevents duplicate side effects (sending emails twice, double-charging, etc.)
 */

import { getMongo } from './mongo.js';

export interface CompletedTurn {
  turn: number;
  type: 'tool_call' | 'done';
  tool?: string;
  result?: any;
}

/**
 * Load the execution history for a run that may have partially executed before.
 * Returns the list of already-completed turns so the run loop can skip them.
 */
export async function getCompletedTurns(runId: string): Promise<CompletedTurn[]> {
  const { events } = await getMongo();

  // Find all tool_result events for this run (these represent completed side effects)
  const toolResults = await events.find({
    runId,
    type: { $in: ['tool_result', 'run_done'] }
  }).sort({ seq: 1 }).toArray();

  return toolResults.map(e => ({
    turn: (e.payload as any).turn ?? 0,
    type: e.type === 'run_done' ? 'done' : 'tool_call',
    tool: (e.payload as any).tool,
    result: (e.payload as any).result,
  }));
}

/**
 * Check if a specific turn has already been executed.
 * Used by the run loop to skip re-execution of side effects.
 */
export function isAlreadyExecuted(completedTurns: CompletedTurn[], turn: number): CompletedTurn | null {
  return completedTurns.find(t => t.turn === turn) ?? null;
}
