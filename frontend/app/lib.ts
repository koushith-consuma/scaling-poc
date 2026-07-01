// Shared client types + helpers for the chat UI.

export type EventType = 'run_started' | 'model_turn' | 'tool_call' | 'tool_result' | 'run_done' | 'run_failed';

export interface RunEvent {
  runId: string;
  threadId: string;
  seq: number;
  type: EventType;
  payload: Record<string, unknown>;
}

export type RunStatus = 'pending' | 'running' | 'done' | 'failed';

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  text: string;
  runId?: string;
  threadId?: string;
  status?: RunStatus;
  steps: Step[]; // live agent activity for an assistant message
  seq: number; // last seen seq (for reconnect backfill)
}

export interface Step {
  seq: number;
  type: EventType;
  label: string;
}

export interface OpsSnapshot {
  health: { mongo: boolean; redis: boolean; rabbit: boolean; workers: number };
  queue: { ok: boolean; ready: number; unacked: number; depth: number; consumers: number };
  runs: { pending: number; running: number; done: number; failed: number; total: number };
  t: number;
}

export async function postRun(threadId: string | undefined): Promise<{ runId: string; threadId: string }> {
  const res = await fetch('/runs', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ threadId }),
  });
  if (!res.ok) {
    const e = await res.json().catch(() => ({}));
    throw new Error(e.error || `POST /runs ${res.status}`);
  }
  return res.json();
}

export async function chaos(action: string, arg?: number) {
  const res = await fetch('/api/chaos', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ action, arg }),
  });
  return res.json();
}

/** Turn a raw run event into a human chat "step" line. */
export function stepLabel(ev: RunEvent): string {
  const p = ev.payload || {};
  switch (ev.type) {
    case 'run_started': return 'Picked up by a worker…';
    case 'tool_call': return `🔧 calling tool "${(p as any).tool}" (turn ${(p as any).turn})`;
    case 'tool_result': return `↳ ${(p as any).result ?? 'ok'}`;
    case 'run_done': return `✅ done — ${(p as any).turns} turns, ${(p as any).toolCalls} tool calls`;
    case 'run_failed': return `❌ failed: ${(p as any).error ?? 'unknown'}`;
    default: return ev.type;
  }
}
