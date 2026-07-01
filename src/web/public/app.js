// Viper POC — interactive tester frontend. Talks to the web tier's REST + SSE.
const $ = (s) => document.querySelector(s);
const runsEl = $('#runs');
const logEl = $('#log');
const openStreams = new Map(); // runId -> EventSource

function log(msg, cls) {
  const d = document.createElement('div');
  d.textContent = `${new Date().toLocaleTimeString()}  ${msg}`;
  if (cls) d.style.color = `var(--${cls})`;
  logEl.prepend(d);
}

// ---- pipeline stage flashing ----
function flash(stage) {
  const el = document.querySelector(`.stage[data-s="${stage}"]`);
  if (!el) return;
  el.classList.add('live');
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove('live'), 700);
}
function setDead(stage, dead) {
  const el = document.querySelector(`.stage[data-s="${stage}"]`);
  if (el) el.classList.toggle('dead', dead);
}

// ---- send a run ----
async function send(threadId) {
  const msg = $('#msg').value || 'hello';
  flash('browser');
  try {
    const body = { threadId: threadId || $('#thread').value || undefined };
    const res = await fetch('/runs', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    flash('web');
    if (!res.ok) {
      const e = await res.json().catch(() => ({}));
      log(`POST /runs FAILED ${res.status}: ${e.error || ''}`, 'bad');
      return null;
    }
    flash('rabbit');
    const run = await res.json();
    log(`sent "${msg}" → run ${run.runId.slice(0, 8)} (thread ${run.threadId})`, 'accent');
    addRunCard(run);
    stream(run.runId);
    return run;
  } catch (e) {
    log(`send error: ${e.message}`, 'bad');
    return null;
  }
}

// ---- run cards + live events ----
function addRunCard(run) {
  if (document.getElementById(`run-${run.runId}`)) return;
  const el = document.createElement('div');
  el.className = 'run';
  el.id = `run-${run.runId}`;
  el.innerHTML = `
    <div class="head">
      <span class="badge pending" id="badge-${run.runId}">pending</span>
      <span class="id">${run.runId.slice(0, 8)} · ${run.threadId}</span>
      <span class="meta" id="meta-${run.runId}">waiting for worker…</span>
    </div>
    <div class="events" id="ev-${run.runId}"></div>`;
  runsEl.prepend(el);
}

function addEvent(runId, ev) {
  const box = document.getElementById(`ev-${runId}`);
  if (!box) return;
  const row = document.createElement('div');
  row.className = `ev ${ev.type}`;
  const pl = JSON.stringify(ev.payload || {});
  row.innerHTML = `<span class="seq">#${ev.seq}</span><span class="ty">${ev.type}</span><span class="pl">${pl}</span>`;
  box.appendChild(row);
  box.scrollTop = box.scrollHeight;

  const badge = document.getElementById(`badge-${runId}`);
  const meta = document.getElementById(`meta-${runId}`);
  if (ev.type === 'run_started') { badge.className = 'badge running'; badge.textContent = 'running'; meta.textContent = 'worker claimed it'; flash('worker'); }
  if (ev.type === 'tool_call') flash('worker');
  if (ev.type === 'run_done') { badge.className = 'badge done'; badge.textContent = 'done'; meta.textContent = `${ev.seq} events`; }
  if (ev.type === 'run_failed') { badge.className = 'badge failed'; badge.textContent = 'failed'; meta.textContent = 'failed'; }
  flash('mongo'); flash('redis'); flash('sse');
}

function stream(runId, lastSeq = 0) {
  if (openStreams.has(runId)) openStreams.get(runId).close();
  const es = new EventSource(`/runs/${runId}/stream?lastSeq=${lastSeq}`);
  openStreams.set(runId, es);
  let maxSeq = lastSeq;
  const handler = (e) => {
    try {
      const ev = JSON.parse(e.data);
      maxSeq = Math.max(maxSeq, ev.seq || 0);
      addEvent(runId, ev);
      if (ev.type === 'run_done' || ev.type === 'run_failed') { es.close(); openStreams.delete(runId); }
    } catch {}
  };
  ['run_started', 'tool_call', 'tool_result', 'run_done', 'run_failed'].forEach((t) => es.addEventListener(t, handler));
  es.onerror = () => { /* browser auto-reconnects; backfill via lastSeq handled server-side on reconnect */ };
}

// ---- ops stream (live tiles + health + pipeline liveness) ----
function opsStream() {
  const es = new EventSource('/api/ops/stream');
  es.onopen = () => $('#conn').textContent = 'live';
  es.onerror = () => $('#conn').textContent = 'reconnecting…';
  es.onmessage = (e) => {
    const s = JSON.parse(e.data);
    $('#t-workers').textContent = s.health.workers;
    $('#t-queue').textContent = s.queue.depth;
    $('#t-running').textContent = s.runs.running;
    $('#t-done').textContent = s.runs.done;
    $('#t-pending').textContent = s.runs.pending;
    $('#t-failed').textContent = s.runs.failed;
    renderHealth(s.health);
    // pipeline liveness reflects real service state
    setDead('rabbit', !s.health.rabbit);
    setDead('mongo', !s.health.mongo);
    setDead('redis', !s.health.redis);
    setDead('worker', s.health.workers === 0);
  };
}

function renderHealth(h) {
  const el = $('#health');
  const svcs = [
    ['Mongo', h.mongo], ['Redis', h.redis], ['RabbitMQ', h.rabbit],
    ['Workers', h.workers > 0, `${h.workers}`],
  ];
  el.innerHTML = svcs.map(([name, up, extra]) =>
    `<span class="svc ${up ? 'up' : 'down'}"><span class="d"></span>${name}${extra ? ' ·' + extra : ''}</span>`).join('');
}

// ---- chaos ----
async function chaos(action) {
  const arg = action === 'scale-workers' ? Number($('#scaleN').value) : undefined;
  log(`chaos: ${action}${arg != null ? ' ' + arg : ''}…`, 'warn');
  try {
    const res = await fetch('/api/chaos', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ action, arg }),
    });
    const r = await res.json();
    log(`↳ ${r.ok ? '✓' : '✗'} ${r.detail}`, r.ok ? 'ok' : 'bad');
  } catch (e) { log(`chaos error: ${e.message}`, 'bad'); }
}

// ---- wire up ----
$('#send').onclick = () => send();
$('#msg').addEventListener('keydown', (e) => { if (e.key === 'Enter') send(); });
$('#burst').onclick = async () => {
  const t = $('#thread').value || `burst-${Math.random().toString(36).slice(2, 7)}`;
  $('#thread').value = t;
  for (let i = 0; i < 3; i++) await send(t);
  log(`fired 3 into thread ${t} — watch them serialize (guard)`, 'accent');
};
$('#flood').onclick = async () => {
  log('flooding 25 runs across distinct threads…', 'warn');
  for (let i = 0; i < 25; i++) send(`flood-${Date.now()}-${i}`);
};
document.querySelectorAll('[data-chaos]').forEach((b) => b.onclick = () => chaos(b.dataset.chaos));

opsStream();
log('ready — type a message and hit Send', 'ok');
