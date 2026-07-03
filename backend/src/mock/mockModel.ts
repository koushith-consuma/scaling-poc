import type { ModelConfig, ModelContext, ModelDecision } from '../types.js';
import { hashSeed, makeRng, mulberry32, randInt, sleep } from '../lib/rng.js';

/**
 * Mock model — stands in for the real LLM call for the ENTIRE POC.
 *
 * Behavior:
 *  - sleeps a randomized delay (minDelayMs..maxDelayMs) to mimic model latency
 *  - each turn either returns { type: 'tool_call' } or { type: 'done' }
 *  - avgTurns controls how many turns before 'done' (geometric-ish stop)
 *  - toolCallProbability controls tool_call vs a plain (still non-final) response
 *  - deterministic when config.seed is provided: the decision for a given
 *    (seed, runId, turn) is stable, so a replayed load run behaves identically.
 *
 * NOTE: never wire in the real LLM. This is the only model in the POC.
 */
export async function mockModel(
  context: ModelContext,
  config: ModelConfig,
): Promise<ModelDecision> {
  // Per-turn deterministic rng: derive a sub-seed from (seed, runId, turn).
  const rng =
    config.seed === undefined
      ? makeRng()
      : mulberry32(hashSeed(config.seed, context.runId, context.turn));

  const delay = randInt(rng, config.minDelayMs, config.maxDelayMs);
  await sleep(delay);

  // Stop probability so the expected number of turns ≈ avgTurns.
  // P(done) per turn = 1/avgTurns (clamped). turn 0 is never forced done.
  const stopProb = config.avgTurns > 0 ? 1 / config.avgTurns : 1;
  const roll = rng();

  const forceContinue = context.turn === 0; // at least one turn of work
  if (!forceContinue && roll < stopProb) {
    // A natural-language final reply so the chat UI shows a real answer (not
    // just "completed in N turns"). Canned + varied by the rng for realism.
    const replies = [
      "Done — I looked into that and here's what I found. Everything checks out.",
      'All set. I ran the steps and the result is ready for you.',
      'Finished — I handled the request end to end. Let me know if you want more.',
      "Here's your answer: the task completed successfully across all the steps.",
      "Wrapped up. I went through it turn by turn and it's good to go.",
    ];
    const reply = replies[Math.floor(rng() * replies.length)] ?? replies[0]!;
    return { type: 'done', summary: reply };
  }

  // Non-final turn: tool_call vs plain response.
  if (rng() < config.toolCallProbability) {
    return { type: 'tool_call', tool: 'noop', args: { turn: context.turn } };
  }
  // A plain (non-tool) response that still isn't done — model "thinking".
  // Represented as a tool_call to a benign 'think' tool so the loop advances.
  return { type: 'tool_call', tool: 'think', args: { turn: context.turn } };
}
