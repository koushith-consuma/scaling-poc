/**
 * Step 6 helper — a minimal SSE client that can simulate disconnect/reconnect.
 *
 *   npm run sse -- <runId> [lastSeq] [disconnectAfterMs]
 *
 * Connects to GET /runs/:id/stream?lastSeq=..., prints each event's seq+type.
 * If disconnectAfterMs is given, it drops the connection at that point (the
 * caller can reconnect with the last seq it saw to exercise backfill).
 */
import { config } from '../config.js';

async function main() {
  const runId = process.argv[2];
  const lastSeq = Number(process.argv[3] ?? 0);
  const disconnectAfterMs = process.argv[4] ? Number(process.argv[4]) : 0;
  if (!runId) {
    console.error('usage: npm run sse -- <runId> [lastSeq] [disconnectAfterMs]');
    process.exit(1);
  }

  const url = `http://localhost:${config.webPort}/runs/${runId}/stream?lastSeq=${lastSeq}`;
  const ctrl = new AbortController();
  if (disconnectAfterMs > 0) {
    setTimeout(() => {
      console.log(`\n[client] disconnecting after ${disconnectAfterMs}ms (maxSeq=${maxSeq})`);
      ctrl.abort();
    }, disconnectAfterMs);
  }

  let maxSeq = lastSeq;
  const res = await fetch(url, { signal: ctrl.signal, headers: { Accept: 'text/event-stream' } });
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buf = '';

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const chunks = buf.split('\n\n');
      buf = chunks.pop() ?? '';
      for (const chunk of chunks) {
        const idLine = chunk.split('\n').find((l) => l.startsWith('id: '));
        const evLine = chunk.split('\n').find((l) => l.startsWith('event: '));
        if (idLine && evLine) {
          const seq = Number(idLine.slice(4));
          maxSeq = Math.max(maxSeq, seq);
          console.log(`[client] seq=${seq} ${evLine.slice(7)}`);
          if (evLine.includes('run_done') || evLine.includes('run_failed')) {
            console.log(`[client] terminal event; maxSeq=${maxSeq}`);
            ctrl.abort();
            return;
          }
        }
      }
    }
  } catch (e: any) {
    if (e.name !== 'AbortError') throw e;
  }
  console.log(`[client] stream ended; maxSeq=${maxSeq}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
