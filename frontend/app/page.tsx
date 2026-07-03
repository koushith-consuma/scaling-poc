'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { chaos, fetchConversations, fetchThread, fetchWorkers, postRun, stepLabel, type ChatMessage, type Conversation, type OpsSnapshot, type RunEvent, type WorkerStatus } from './lib';
import styles from './page.module.css';

const uid = () => (crypto?.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`);

function newConversation(): Conversation {
  return { id: uid(), threadId: `thread-${uid().slice(0, 8)}`, title: 'New chat', messages: [] };
}

export default function Page() {
  const [convos, setConvos] = useState<Conversation[]>([newConversation()]);
  const [activeId, setActiveId] = useState<string>(() => '');
  const [input, setInput] = useState('');
  const [ops, setOps] = useState<OpsSnapshot | null>(null);
  const [workers, setWorkers] = useState<WorkerStatus[]>([]);
  const [logs, setLogs] = useState<string[]>([]);
  const [panelOpen, setPanelOpen] = useState(true);
  const streams = useRef<Map<string, EventSource>>(new Map());
  const scrollRef = useRef<HTMLDivElement>(null);

  // pick the first convo as active on mount
  useEffect(() => { setActiveId((id) => id || convos[0]?.id || ''); }, [convos]);

  const active = convos.find((c) => c.id === activeId) ?? convos[0];

  const log = useCallback((m: string) => {
    setLogs((l) => [`${new Date().toLocaleTimeString()}  ${m}`, ...l].slice(0, 40));
  }, []);

  // --- live ops (health + metrics) ---
  useEffect(() => {
    const es = new EventSource('/api/ops/stream');
    es.onmessage = (e) => setOps(JSON.parse(e.data));
    return () => es.close();
  }, []);

  // --- poll worker activity every 2s ---
  useEffect(() => {
    const poll = async () => setWorkers(await fetchWorkers());
    poll();
    const iv = setInterval(poll, 2000);
    return () => clearInterval(iv);
  }, []);

  // --- load past conversations from Mongo on mount (survives refresh) ---
  useEffect(() => {
    (async () => {
      const list = await fetchConversations();
      if (list.length === 0) return;
      const restored: Conversation[] = await Promise.all(
        list.slice(0, 20).map(async (c) => {
          const t = await fetchThread(c.threadId);
          const messages: ChatMessage[] = [];
          for (const r of t.runs) {
            if (r.prompt) messages.push({ id: uid(), role: 'user', text: r.prompt, steps: [], seq: 0 });
            messages.push({
              id: uid(), role: 'assistant', text: r.reply || (r.status === 'failed' ? '⚠️ failed' : ''),
              status: r.status, worker: r.claimedBy ?? undefined, seq: 0,
              steps: (r.steps ?? []).map((s: any) => ({ seq: s.seq, type: s.type, label: stepLabel({ ...s } as RunEvent) })),
            });
          }
          const title = messages.find((m) => m.role === 'user')?.text?.slice(0, 40) ?? 'Chat';
          return { id: uid(), threadId: c.threadId, title, messages };
        }),
      );
      // Keep the current empty "New chat" at the top, then history.
      setConvos((cur) => [...cur, ...restored]);
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [convos, activeId]);

  // patch a specific message inside a specific conversation
  const patchMsg = useCallback((convoId: string, msgId: string, fn: (m: ChatMessage) => ChatMessage) => {
    setConvos((cs) => cs.map((c) => (c.id !== convoId ? c : { ...c, messages: c.messages.map((m) => (m.id === msgId ? fn(m) : m)) })));
  }, []);

  const openStream = useCallback(
    (convoId: string, runId: string, assistantId: string, lastSeq = 0) => {
      streams.current.get(runId)?.close();
      const es = new EventSource(`/runs/${runId}/stream?lastSeq=${lastSeq}`);
      streams.current.set(runId, es);

      const onEvent = (e: MessageEvent) => {
        let ev: RunEvent;
        try { ev = JSON.parse(e.data); } catch { return; }
        patchMsg(convoId, assistantId, (m) => {
          if (ev.seq <= m.seq) return m; // dedup on reconnect backfill
          const steps = ev.type === 'model_turn' || ev.type === 'tool_call' || ev.type === 'tool_result'
            ? [...m.steps, { seq: ev.seq, type: ev.type, label: stepLabel(ev) }]
            : m.steps;
          let status = m.status;
          let text = m.text;
          let worker = m.worker;
          if (ev.type === 'run_started') { status = 'running'; worker = (ev.payload as any).worker ?? worker; }
          if (ev.type === 'run_done') { status = 'done'; text = String((ev.payload as any).summary ?? 'Done.'); }
          if (ev.type === 'run_failed') { status = 'failed'; text = `⚠️ ${(ev.payload as any).error ?? 'The run failed.'}`; }
          return { ...m, seq: ev.seq, steps, status, text, worker };
        });
        if (ev.type === 'run_done' || ev.type === 'run_failed') {
          es.close();
          streams.current.delete(runId);
        }
      };
      ['run_started', 'model_turn', 'tool_call', 'tool_result', 'run_done', 'run_failed'].forEach((t) =>
        es.addEventListener(t, onEvent as EventListener),
      );
      es.onerror = () => log(`stream ${runId.slice(0, 8)}: reconnecting…`);
    },
    [patchMsg, log],
  );

  const send = useCallback(() => {
    const text = input.trim();
    if (!text || !active) return;
    setInput('');

    // "/btw" command: spawn a new parallel thread (like a subagent/interrupt)
    const isBtw = text.toLowerCase().startsWith('/btw ');
    const actualText = isBtw ? text.slice(5).trim() : text;
    if (!actualText) return;

    let convoId = active.id;
    let threadId = active.threadId;

    // If /btw, create a new conversation (new thread) for the interrupt
    if (isBtw) {
      const newConvo = newConversation();
      newConvo.title = `🔀 ${actualText.slice(0, 30)}`;
      setConvos((cs) => [newConvo, ...cs]);
      setActiveId(newConvo.id);
      convoId = newConvo.id;
      threadId = newConvo.threadId;
      log(`spawned parallel thread: ${actualText.slice(0, 40)}`);
    }

    const userMsg: ChatMessage = { id: uid(), role: 'user', text: actualText, steps: [], seq: 0 };
    const assistantId = uid();
    const assistant: ChatMessage = { id: assistantId, role: 'assistant', text: '', status: 'pending', steps: [], seq: 0 };

    setConvos((cs) => cs.map((c) => (c.id !== convoId ? c : {
      ...c,
      title: c.messages.length === 0 ? actualText.slice(0, 40) : c.title,
      messages: [...c.messages, userMsg, assistant],
    })));

    (async () => {
      try {
        const run = await postRun(threadId, actualText);
        patchMsg(convoId, assistantId, (m) => ({ ...m, runId: run.runId }));
        log(`sent → run ${run.runId.slice(0, 8)} ${isBtw ? '(parallel)' : ''}`);
        openStream(convoId, run.runId, assistantId);
      } catch (e) {
        patchMsg(convoId, assistantId, (m) => ({ ...m, status: 'failed', text: `Couldn't send: ${(e as Error).message}` }));
      }
    })();
  }, [input, active, patchMsg, openStream, log]);

  const doChaos = useCallback(async (action: string, arg?: number) => {
    log(`simulate: ${action}${arg != null ? ' ' + arg : ''}…`);
    const r = await chaos(action, arg);
    log(`↳ ${r.ok ? '✓' : '✗'} ${r.detail}`);
  }, [log]);

  const startNewChat = () => {
    const c = newConversation();
    setConvos((cs) => [c, ...cs]);
    setActiveId(c.id);
  };

  const h = ops?.health;

  return (
    <div className={styles.app}>
      {/* ---------- LEFT: conversation list ---------- */}
      <nav className={styles.sidebar}>
        <div className={styles.brand}>⚡ Viper Chat</div>
        <button className={styles.newChat} onClick={startNewChat}>+ New chat</button>
        <div className={styles.convos}>
          {convos.map((c) => (
            <button
              key={c.id}
              className={`${styles.convo} ${c.id === activeId ? styles.convoActive : ''}`}
              onClick={() => setActiveId(c.id)}
            >
              <span className={styles.convoTitle}>{c.title}</span>
              <span className={styles.convoMeta}>{c.messages.filter((m) => m.role === 'user').length} msg</span>
            </button>
          ))}
        </div>
        <a href="/data" className={styles.dataLink}>🗄️ Data inspector →</a>
      </nav>

      {/* ---------- CENTER: active conversation ---------- */}
      <main className={styles.chat}>
        <div className={styles.messages} ref={scrollRef}>
          {active && active.messages.length === 0 && (
            <div className={styles.empty}>
              <div className={styles.emptyBig}>Start chatting</div>
              <p>Send a message and watch the agent think, call tools, and reply — streamed live
                through RabbitMQ → worker → Mongo → Redis → SSE.</p>
              <p className={styles.emptyHint}>Messages in this chat share a thread, so they’re answered <b>in order</b>.</p>
              <p className={styles.emptyHint}>💡 Use <code>/btw your message</code> to spawn a parallel thread that processes concurrently!</p>
            </div>
          )}
          {active?.messages.map((m) =>
            m.role === 'user' ? (
              <div key={m.id} className={`${styles.row} ${styles.rowUser}`}>
                <div className={`${styles.bubble} ${styles.user}`}>{m.text}</div>
              </div>
            ) : (
              <div key={m.id} className={`${styles.row} ${styles.rowAssistant}`}>
                <div className={styles.avatar}>🤖</div>
                <div className={styles.assistantWrap}>
                  {m.worker && (
                    <div className={styles.workerTag} title="Which worker process handled this run">
                      handled by <code>{m.worker}</code>
                    </div>
                  )}
                  <AgentSteps message={m} />
                  {m.text ? (
                    <div className={`${styles.bubble} ${styles.assistant}`}>{m.text}</div>
                  ) : (
                    <div className={`${styles.bubble} ${styles.assistant} ${styles.thinkingBubble}`}>
                      <span className={styles.typing}><span/><span/><span/></span>
                    </div>
                  )}
                </div>
              </div>
            ),
          )}
        </div>

        <div className={styles.composer}>
          <button
            className={styles.parallelBtn}
            onClick={() => setInput('/btw ')}
            title="Spawn a parallel thread (processes concurrently)"
          >
            🔀
          </button>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && send()}
            placeholder="Message the agent… (try /btw for parallel)"
            autoFocus
          />
          <button onClick={send} disabled={!input.trim()}>Send</button>
        </div>
      </main>

      {/* ---------- RIGHT: system panel (collapsible) ---------- */}
      <aside className={`${styles.panel} ${panelOpen ? '' : styles.panelClosed}`}>
        <button className={styles.panelToggle} onClick={() => setPanelOpen((v) => !v)}>
          {panelOpen ? '›' : '‹'}
        </button>
        {panelOpen && (
          <div className={styles.panelInner}>
            <h3>System health</h3>
            <div className={styles.health}>
              {[
                ['Database', h?.mongo], ['Live bus', h?.redis], ['Queue', h?.rabbit],
                ['Workers', (h?.workers ?? 0) > 0, h?.workers],
              ].map(([n, up, extra]: any) => (
                <span key={n} className={`${styles.svc} ${up ? styles.up : styles.down}`}>
                  <span className={styles.dot} />{n}{typeof extra === 'number' ? ` ·${extra}` : ''}
                </span>
              ))}
            </div>

            <h3>Live metrics</h3>
            <div className={styles.tiles}>
              <Tile n={ops?.health.workers} k="workers" cls="accent" />
              <Tile n={ops?.queue.depth} k="queued jobs" cls="warn" />
              <Tile n={ops?.runs.running} k="in progress" />
              <Tile n={ops?.runs.done} k="completed" cls="ok" />
            </div>

            <h3>Worker activity</h3>
            <div className={styles.workerActivity}>
              {workers.length === 0 && <div className={styles.hint}>No workers connected</div>}
              {workers.map((w) => (
                <div key={w.workerId} className={`${styles.workerRow} ${w.currentRun ? styles.busy : styles.idle}`}>
                  <div className={styles.workerIdShort} title={w.workerId}>
                    {w.workerId.split('-').pop()?.slice(0, 6) || w.workerId}
                  </div>
                  {w.currentRun ? (
                    <div className={styles.workerWork}>
                      <span className={styles.workerStatus}>⚙️ processing</span>
                      <code className={styles.workerRun} title={`Run: ${w.currentRun}\nThread: ${w.currentThread}`}>
                        {w.currentRun.slice(0, 8)}
                      </code>
                    </div>
                  ) : (
                    <span className={styles.workerStatus}>💤 idle</span>
                  )}
                </div>
              ))}
            </div>

            <h3>Failure simulator</h3>
            <p className={styles.hint}>Break a piece on purpose — the chat should survive.</p>
            <div className={styles.sim}>
              <button className={styles.warn} onClick={() => doChaos('kill-worker')}>💥 Crash a worker</button>
              <SimRow label="Live bus (Redis)" onStop={() => doChaos('stop-redis')} onStart={() => doChaos('start-redis')} />
              <SimRow label="Database (Mongo)" onStop={() => doChaos('stop-mongo')} onStart={() => doChaos('start-mongo')} />
              <SimRow label="Queue (RabbitMQ)" onStop={() => doChaos('stop-rabbit')} onStart={() => doChaos('start-rabbit')} />
              <ScaleRow onApply={(n) => doChaos('scale-workers', n)} />
            </div>

            <h3>Activity</h3>
            <div className={styles.log}>{logs.map((l, i) => <div key={i}>{l}</div>)}</div>
          </div>
        )}
      </aside>
    </div>
  );
}

/** Collapsible "thinking / tool" trace above the assistant's final reply. */
function AgentSteps({ message }: { message: ChatMessage }) {
  const [open, setOpen] = useState(true);
  if (message.steps.length === 0 && message.status !== 'pending') return null;
  const running = message.status === 'pending' || message.status === 'running';
  return (
    <div className={styles.stepsBox}>
      <button className={styles.stepsHead} onClick={() => setOpen((v) => !v)}>
        {running ? '⚙️ working' : '✓ steps'} · {message.steps.length}
        <span className={styles.caret}>{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <div className={styles.steps}>
          {message.steps.map((s) => (
            <div key={s.seq} className={`${styles.step} ${styles['ev_' + s.type]}`}>{s.label}</div>
          ))}
        </div>
      )}
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

function SimRow({ label, onStop, onStart }: { label: string; onStop: () => void; onStart: () => void }) {
  return (
    <div className={styles.simRow}>
      <span className={styles.simLbl}>{label}</span>
      <button className={styles.bad} onClick={onStop}>Stop</button>
      <button className={styles.ghost} onClick={onStart}>Start</button>
    </div>
  );
}

function ScaleRow({ onApply }: { onApply: (n: number) => void }) {
  const [n, setN] = useState(3);
  return (
    <div className={styles.simRow}>
      <span className={styles.simLbl}>Workers</span>
      <input className={styles.scaleInput} type="number" min={0} max={20} value={n} onChange={(e) => setN(Number(e.target.value))} />
      <button className={styles.ghost} onClick={() => onApply(n)}>Set</button>
    </div>
  );
}
