'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { chaos, postRun, stepLabel, type ChatMessage, type OpsSnapshot, type RunEvent } from './lib';
import styles from './page.module.css';

export default function Page() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('hello agent');
  const [threadId, setThreadId] = useState<string>('');
  const [ops, setOps] = useState<OpsSnapshot | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [sending, setSending] = useState(false);
  const streams = useRef<Map<string, EventSource>>(new Map());
  const scrollRef = useRef<HTMLDivElement>(null);

  const log = useCallback((m: string) => {
    setLogs((l) => [`${new Date().toLocaleTimeString()}  ${m}`, ...l].slice(0, 40));
  }, []);

  // --- ops live stream (health + queue + counts) ---
  useEffect(() => {
    const es = new EventSource('/api/ops/stream');
    es.onmessage = (e) => setOps(JSON.parse(e.data));
    return () => es.close();
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [messages]);

  const patchMsg = useCallback((assistantId: string, fn: (m: ChatMessage) => ChatMessage) => {
    setMessages((ms) => ms.map((m) => (m.id === assistantId ? fn(m) : m)));
  }, []);

  const openStream = useCallback(
    (runId: string, assistantId: string, lastSeq = 0) => {
      streams.current.get(runId)?.close();
      const es = new EventSource(`/runs/${runId}/stream?lastSeq=${lastSeq}`);
      streams.current.set(runId, es);

      const onEvent = (e: MessageEvent) => {
        let ev: RunEvent;
        try { ev = JSON.parse(e.data); } catch { return; }
        patchMsg(assistantId, (m) => {
          if (ev.seq <= m.seq) return m; // dedup across reconnect backfill
          const steps = [...m.steps, { seq: ev.seq, type: ev.type, label: stepLabel(ev) }];
          let status = m.status;
          let text = m.text;
          if (ev.type === 'run_started') status = 'running';
          if (ev.type === 'run_done') { status = 'done'; text = `Completed in ${(ev.payload as any).turns} turns.`; }
          if (ev.type === 'run_failed') { status = 'failed'; text = 'The run failed.'; }
          return { ...m, seq: ev.seq, steps, status, text };
        });
        if (ev.type === 'run_done' || ev.type === 'run_failed') {
          es.close();
          streams.current.delete(runId);
        }
      };
      ['run_started', 'tool_call', 'tool_result', 'run_done', 'run_failed'].forEach((t) =>
        es.addEventListener(t, onEvent as EventListener),
      );
      es.onerror = () => log(`stream ${runId.slice(0, 8)}: transport blip (browser will reconnect + backfill)`);
    },
    [patchMsg, log],
  );

  const doSend = useCallback(
    async (text: string, forceThread?: string) => {
      const tId = forceThread ?? (threadId || undefined);
      const userMsg: ChatMessage = { id: crypto.randomUUID(), role: 'user', text, steps: [], seq: 0 };
      const assistantId = crypto.randomUUID();
      const assistant: ChatMessage = { id: assistantId, role: 'assistant', text: '', status: 'pending', steps: [], seq: 0 };
      setMessages((m) => [...m, userMsg, assistant]);
      try {
        const run = await postRun(tId);
        if (!threadId) setThreadId(run.threadId); // pin the thread so follow-ups serialize
        patchMsg(assistantId, (m) => ({ ...m, runId: run.runId, threadId: run.threadId }));
        log(`sent → run ${run.runId.slice(0, 8)} on thread ${run.threadId}`);
        openStream(run.runId, assistantId);
      } catch (e) {
        patchMsg(assistantId, (m) => ({ ...m, status: 'failed', text: `send failed: ${(e as Error).message}` }));
        log(`send failed: ${(e as Error).message}`);
      }
    },
    [threadId, patchMsg, openStream, log],
  );

  const send = useCallback(() => {
    const t = input.trim();
    if (!t || sending) return;
    setSending(true);
    setInput('');
    doSend(t).finally(() => setSending(false));
  }, [input, sending, doSend]);

  const doChaos = useCallback(
    async (action: string, arg?: number) => {
      log(`chaos: ${action}${arg != null ? ' ' + arg : ''}…`);
      const r = await chaos(action, arg);
      log(`↳ ${r.ok ? '✓' : '✗'} ${r.detail}`);
    },
    [log],
  );

  const h = ops?.health;
  return (
    <div className={styles.app}>
      {/* ---------- chat column ---------- */}
      <section className={styles.chatCol}>
        <header className={styles.chatHeader}>
          <div className={styles.title}>⚡ Viper Chat</div>
          <div className={styles.sub}>
            thread: <code>{threadId || '(new on first send)'}</code>
            {threadId && (
              <button className={styles.link} onClick={() => setThreadId('')}>new thread</button>
            )}
          </div>
        </header>

        <div className={styles.messages} ref={scrollRef}>
          {messages.length === 0 && (
            <div className={styles.empty}>
              Send a message. It travels browser → web → RabbitMQ → worker → Mongo → Redis → back to you over SSE.
              <br />Follow-ups reuse the same thread, so they <b>serialize</b> behind each other (per-thread guard).
            </div>
          )}
          {messages.map((m) =>
            m.role === 'user' ? (
              <div key={m.id} className={`${styles.bubble} ${styles.user}`}>{m.text}</div>
            ) : (
              <div key={m.id} className={`${styles.bubble} ${styles.assistant}`}>
                <div className={styles.assistantHead}>
                  <span className={`${styles.badge} ${styles[m.status ?? 'pending']}`}>{m.status ?? 'pending'}</span>
                  {m.runId && <code className={styles.runid}>{m.runId.slice(0, 8)}</code>}
                </div>
                <div className={styles.steps}>
                  {m.steps.map((s) => (
                    <div key={s.seq} className={`${styles.step} ${styles['ev_' + s.type]}`}>
                      <span className={styles.stepSeq}>#{s.seq}</span> {s.label}
                    </div>
                  ))}
                  {(m.status === 'pending' || m.status === 'running') && (
                    <div className={styles.typing}><span/><span/><span/></div>
                  )}
                </div>
                {m.text && <div className={styles.finalText}>{m.text}</div>}
              </div>
            ),
          )}
        </div>

        <div className={styles.composer}>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && send()}
            placeholder="Message the agent…"
          />
          <button onClick={send} disabled={sending}>Send</button>
          <button
            className={styles.ghost}
            title="Fire 3 messages into this same thread at once — watch them serialize"
            onClick={() => {
              const t = threadId || `burst-${Math.random().toString(36).slice(2, 7)}`;
              setThreadId(t);
              [1, 2, 3].forEach((i) => doSend(`burst message ${i}`, t));
            }}
          >×3 same thread</button>
        </div>
      </section>

      {/* ---------- ops + chaos column ---------- */}
      <aside className={styles.opsCol}>
        <h3>Service health</h3>
        <div className={styles.health}>
          {[['Mongo', h?.mongo], ['Redis', h?.redis], ['RabbitMQ', h?.rabbit], ['Workers', (h?.workers ?? 0) > 0, h?.workers]].map(
            ([n, up, extra]: any) => (
              <span key={n} className={`${styles.svc} ${up ? styles.up : styles.down}`}>
                <span className={styles.dot} />{n}{extra != null && typeof extra === 'number' ? ` ·${extra}` : ''}
              </span>
            ),
          )}
        </div>

        <h3>Live stats</h3>
        <div className={styles.tiles}>
          <Tile n={ops?.health.workers} k="workers" cls="accent" />
          <Tile n={ops?.queue.depth} k="queue depth" cls="warn" />
          <Tile n={ops?.runs.running} k="running" />
          <Tile n={ops?.runs.done} k="done" cls="ok" />
          <Tile n={ops?.runs.pending} k="pending" />
          <Tile n={ops?.runs.failed} k="failed" cls="bad" />
        </div>

        <h3>Chaos — break it live</h3>
        <div className={styles.chaos}>
          <button className={styles.warn} onClick={() => doChaos('kill-worker')}>💥 Kill a worker</button>
          <div className={styles.row}>
            <button className={styles.bad} onClick={() => doChaos('stop-redis')}>Stop Redis</button>
            <button className={styles.ghost} onClick={() => doChaos('start-redis')}>Start</button>
          </div>
          <div className={styles.row}>
            <button className={styles.bad} onClick={() => doChaos('stop-mongo')}>Stop Mongo</button>
            <button className={styles.ghost} onClick={() => doChaos('start-mongo')}>Start</button>
          </div>
          <div className={styles.row}>
            <button className={styles.bad} onClick={() => doChaos('stop-rabbit')}>Stop RabbitMQ</button>
            <button className={styles.ghost} onClick={() => doChaos('start-rabbit')}>Start</button>
          </div>
          <div className={styles.row}>
            <span className={styles.scaleLbl}>Scale workers</span>
            <ScaleControl onApply={(n) => doChaos('scale-workers', n)} />
          </div>
        </div>

        <h3>Event log</h3>
        <div className={styles.log}>
          {logs.map((l, i) => <div key={i}>{l}</div>)}
        </div>
      </aside>
    </div>
  );
}

function Tile({ n, k, cls }: { n: number | undefined; k: string; cls?: string }) {
  return (
    <div className={`${styles.tile} ${cls ? styles[cls] : ''}`}>
      <div className={styles.tileN}>{n ?? '–'}</div>
      <div className={styles.tileK}>{k}</div>
    </div>
  );
}

function ScaleControl({ onApply }: { onApply: (n: number) => void }) {
  const [n, setN] = useState(3);
  return (
    <span className={styles.scaler}>
      <input type="number" min={0} max={20} value={n} onChange={(e) => setN(Number(e.target.value))} />
      <button className={styles.ghost} onClick={() => onApply(n)}>Apply</button>
    </span>
  );
}
