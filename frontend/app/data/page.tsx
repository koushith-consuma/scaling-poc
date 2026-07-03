'use client';

import { useCallback, useEffect, useState } from 'react';
import styles from './data.module.css';

interface MongoData {
  ok: boolean;
  error?: string;
  counts: { runs: number; events: number; threadLocks: number };
  runs: any[];
  events: any[];
  threadLocks: any[];
}
interface RedisData {
  ok: boolean;
  error?: string;
  dbSize: number;
  recentCount: number;
  recentEvents: any[];
  activeChannels: { channel: string; subscribers: number }[];
  keys: { key: string; type: string; size?: number }[];
}
interface QueueData {
  ok: boolean;
  error?: string;
  name: string;
  ready: number;
  unacked: number;
  consumers: number;
  messages: { runId: string; threadId: string }[];
}

const badgeClass = (s: string) =>
  ({ pending: styles.pending, running: styles.running, done: styles.done, failed: styles.failed } as any)[s] || styles.pending;

function shortId(id: string, n = 8) {
  return typeof id === 'string' && id.length > n ? id.slice(0, n) : id;
}
function ago(d?: string) {
  if (!d) return '';
  const s = (Date.now() - new Date(d).getTime()) / 1000;
  if (s < 60) return `${Math.round(s)}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  return `${Math.round(s / 3600)}h ago`;
}

export default function DataPage() {
  const [mongo, setMongo] = useState<MongoData | null>(null);
  const [redis, setRedis] = useState<RedisData | null>(null);
  const [queue, setQueue] = useState<QueueData | null>(null);
  const [auto, setAuto] = useState(true);
  const [tab, setTab] = useState<'runs' | 'events' | 'locks' | 'queue'>('runs');
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [m, r, q] = await Promise.all([
        fetch('/api/inspect/mongo?limit=30').then((x) => x.json()),
        fetch('/api/inspect/redis?limit=30').then((x) => x.json()),
        fetch('/api/inspect/queue?limit=50').then((x) => x.json()),
      ]);
      setMongo(m);
      setRedis(r);
      setQueue(q);
    } catch {
      /* leave stale */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);
  useEffect(() => {
    if (!auto) return;
    const iv = setInterval(refresh, 2000);
    return () => clearInterval(iv);
  }, [auto, refresh]);

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div className={styles.title}>
          🗄️ Data Inspector
          <a href="/" className={styles.navLink}>← back to chat</a>
        </div>
        <div className={styles.controls}>
          <label className={styles.auto}>
            <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} /> auto-refresh (2s)
          </label>
          <button onClick={refresh} disabled={loading}>{loading ? '…' : '⟳ Refresh'}</button>
        </div>
      </header>

      <div className={styles.grid}>
        {/* ---------------- MONGO ---------------- */}
        <section className={styles.col}>
          <div className={styles.storeHead}>
            <h2>🍃 MongoDB <span className={styles.sub}>source of truth</span></h2>
            <span className={`${styles.dot} ${mongo?.ok ? styles.up : styles.down}`} />
          </div>
          {mongo && !mongo.ok && <div className={styles.err}>Mongo unavailable: {mongo.error}</div>}

          <div className={styles.counts}>
            <div className={styles.count}><b>{mongo?.counts.runs ?? '–'}</b><span>runs</span></div>
            <div className={styles.count}><b>{mongo?.counts.events ?? '–'}</b><span>events</span></div>
            <div className={styles.count}><b>{mongo?.counts.threadLocks ?? '–'}</b><span>thread locks</span></div>
          </div>

          <div className={styles.tabs}>
            <button className={tab === 'runs' ? styles.activeTab : ''} onClick={() => setTab('runs')}>runs</button>
            <button className={tab === 'events' ? styles.activeTab : ''} onClick={() => setTab('events')}>events</button>
            <button className={tab === 'locks' ? styles.activeTab : ''} onClick={() => setTab('locks')}>thread_locks</button>
            <button className={tab === 'queue' ? styles.activeTab : ''} onClick={() => setTab('queue')}>queue (RabbitMQ)</button>
          </div>

          {tab === 'runs' && (
            <table className={styles.table}>
              <thead><tr><th>runId</th><th>thread</th><th>status</th><th>seq</th><th>worker</th><th>created</th></tr></thead>
              <tbody>
                {mongo?.runs.map((r) => (
                  <tr key={r._id}>
                    <td className={styles.mono}>{shortId(r._id)}</td>
                    <td className={styles.mono}>{shortId(r.threadId, 12)}</td>
                    <td><span className={`${styles.badge} ${badgeClass(r.status)}`}>{r.status}</span></td>
                    <td>{r.lastEventSeq}</td>
                    <td className={styles.mono}>{r.claimedBy ? shortId(r.claimedBy, 14) : '—'}</td>
                    <td className={styles.muted}>{ago(r.createdAt)}</td>
                  </tr>
                ))}
                {mongo?.runs.length === 0 && <tr><td colSpan={6} className={styles.empty}>no runs yet — send a message</td></tr>}
              </tbody>
            </table>
          )}

          {tab === 'events' && (
            <table className={styles.table}>
              <thead><tr><th>seq</th><th>type</th><th>runId</th><th>payload</th></tr></thead>
              <tbody>
                {mongo?.events.map((e) => (
                  <tr key={e._id}>
                    <td>{e.seq}</td>
                    <td><span className={`${styles.evt} ${styles['t_' + e.type]}`}>{e.type}</span></td>
                    <td className={styles.mono}>{shortId(e.runId)}</td>
                    <td className={styles.payload}>{JSON.stringify(e.payload)}</td>
                  </tr>
                ))}
                {mongo?.events.length === 0 && <tr><td colSpan={4} className={styles.empty}>no events yet</td></tr>}
              </tbody>
            </table>
          )}

          {tab === 'locks' && (
            <table className={styles.table}>
              <thead><tr><th>threadId (_id)</th><th>held by runId</th><th>acquired</th></tr></thead>
              <tbody>
                {mongo?.threadLocks.map((l) => (
                  <tr key={l._id}>
                    <td className={styles.mono}>{shortId(l._id, 14)}</td>
                    <td className={styles.mono}>{shortId(l.runId)}</td>
                    <td className={styles.muted}>{ago(l.acquiredAt)}</td>
                  </tr>
                ))}
                {mongo?.threadLocks.length === 0 && <tr><td colSpan={3} className={styles.empty}>no active thread locks (nothing running)</td></tr>}
              </tbody>
            </table>
          )}

          {tab === 'queue' && (
            <>
              {queue && !queue.ok && <div className={styles.err}>RabbitMQ unavailable: {queue.error}</div>}
              <div className={styles.queueStats}>
                <div className={styles.queueStat}>
                  <b>{queue?.ready ?? '–'}</b>
                  <span>ready (waiting)</span>
                </div>
                <div className={styles.queueStat}>
                  <b>{queue?.unacked ?? '–'}</b>
                  <span>unacked (processing)</span>
                </div>
                <div className={styles.queueStat}>
                  <b>{queue?.consumers ?? '–'}</b>
                  <span>consumers</span>
                </div>
              </div>
              <table className={styles.table}>
                <thead><tr><th>runId</th><th>threadId</th></tr></thead>
                <tbody>
                  {queue?.messages.map((m, i) => (
                    <tr key={i}>
                      <td className={styles.mono}>{shortId(m.runId)}</td>
                      <td className={styles.mono}>{shortId(m.threadId, 14)}</td>
                    </tr>
                  ))}
                  {queue?.messages.length === 0 && <tr><td colSpan={2} className={styles.empty}>queue is empty</td></tr>}
                </tbody>
              </table>
            </>
          )}
        </section>

        {/* ---------------- REDIS ---------------- */}
        <section className={styles.col}>
          <div className={styles.storeHead}>
            <h2>🔴 Redis <span className={styles.sub}>speed layer · events OUT</span></h2>
            <span className={`${styles.dot} ${redis?.ok ? styles.up : styles.down}`} />
          </div>
          {redis && !redis.ok && <div className={styles.err}>Redis unavailable: {redis.error} (chat still works via Mongo poll)</div>}

          <div className={styles.counts}>
            <div className={styles.count}><b>{redis?.dbSize ?? '–'}</b><span>keys</span></div>
            <div className={styles.count}><b>{redis?.recentCount ?? '–'}</b><span>recent events</span></div>
            <div className={styles.count}><b>{redis?.activeChannels.length ?? '–'}</b><span>live channels</span></div>
          </div>

          <h3 className={styles.h3}>Keys</h3>
          <table className={styles.table}>
            <thead><tr><th>key</th><th>type</th><th>size</th></tr></thead>
            <tbody>
              {redis?.keys.map((k) => (
                <tr key={k.key}><td className={styles.mono}>{k.key}</td><td>{k.type}</td><td>{k.size ?? '—'}</td></tr>
              ))}
              {redis?.keys.length === 0 && <tr><td colSpan={3} className={styles.empty}>no keys — send a message with Redis up</td></tr>}
            </tbody>
          </table>

          <h3 className={styles.h3}>Active pub/sub channels <span className={styles.muted}>(runs being watched now)</span></h3>
          <table className={styles.table}>
            <thead><tr><th>channel</th><th>subscribers</th></tr></thead>
            <tbody>
              {redis?.activeChannels.map((c) => (
                <tr key={c.channel}><td className={styles.mono}>{c.channel}</td><td>{c.subscribers}</td></tr>
              ))}
              {redis?.activeChannels.length === 0 && <tr><td colSpan={2} className={styles.empty}>no active subscribers right now</td></tr>}
            </tbody>
          </table>

          <h3 className={styles.h3}>Recent events <span className={styles.muted}>(list: {'recent:events'})</span></h3>
          <div className={styles.stream}>
            {redis?.recentEvents.map((e, i) => (
              <div key={i} className={styles.streamRow}>
                <span className={styles.mono}>#{e.seq}</span>
                <span className={`${styles.evt} ${styles['t_' + e.type]}`}>{e.type}</span>
                <span className={styles.mono}>{shortId(e.runId)}</span>
                <span className={styles.payload}>{JSON.stringify(e.payload)}</span>
              </div>
            ))}
            {redis?.recentEvents.length === 0 && <div className={styles.empty}>empty — events land here as runs execute</div>}
          </div>
        </section>
      </div>
    </div>
  );
}
