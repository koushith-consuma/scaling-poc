/**
 * Generate SVG charts from the load-test summaries (Step 8).
 *
 *   npm run charts
 *
 * Reads loadtest-results/<label>/summary.json for each cell and emits
 * self-contained SVG files into docs/charts/. No external chart deps.
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';

interface Summary {
  totalRuns: number;
  completed: number;
  timeToPickupP50: number;
  timeToPickupP95: number;
  durationP50: number;
  durationP95: number;
  peakQueueDepth: number;
  finalQueueDepth: number;
  mongoLatP50: number;
  mongoLatP95: number;
}

const WORKERS = [1, 3];
const USERS = [10, 50, 100];
const COLORS: Record<number, string> = { 1: '#e4572e', 3: '#2e86ab' };

function load(w: number, u: number): Summary | null {
  const p = `loadtest-results/w${w}-u${u}/summary.json`;
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, 'utf8')) as Summary;
}

interface Series {
  label: string;
  color: string;
  points: { x: number; y: number }[];
}

function lineChart(opts: {
  title: string;
  xLabel: string;
  yLabel: string;
  xTicks: number[];
  series: Series[];
  yUnit?: string;
  width?: number;
  height?: number;
}): string {
  const W = opts.width ?? 720;
  const H = opts.height ?? 420;
  const m = { top: 56, right: 150, bottom: 60, left: 78 };
  const plotW = W - m.left - m.right;
  const plotH = H - m.top - m.bottom;

  const allY = opts.series.flatMap((s) => s.points.map((p) => p.y));
  const yMax = Math.max(1, ...allY) * 1.1;
  const xMin = Math.min(...opts.xTicks);
  const xMax = Math.max(...opts.xTicks);

  const sx = (x: number) => m.left + ((x - xMin) / (xMax - xMin || 1)) * plotW;
  const sy = (y: number) => m.top + plotH - (y / yMax) * plotH;

  const parts: string[] = [];
  parts.push(
    `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family="ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial">`,
  );
  parts.push(`<rect width="${W}" height="${H}" fill="#ffffff"/>`);
  parts.push(`<text x="${W / 2}" y="30" text-anchor="middle" font-size="18" font-weight="700" fill="#1a1a1a">${opts.title}</text>`);

  // Y gridlines + labels (5 ticks).
  for (let i = 0; i <= 5; i++) {
    const yv = (yMax / 5) * i;
    const py = sy(yv);
    parts.push(`<line x1="${m.left}" y1="${py}" x2="${m.left + plotW}" y2="${py}" stroke="#eee" stroke-width="1"/>`);
    parts.push(`<text x="${m.left - 10}" y="${py + 4}" text-anchor="end" font-size="11" fill="#666">${fmt(yv, opts.yUnit)}</text>`);
  }
  // X ticks.
  for (const xt of opts.xTicks) {
    const px = sx(xt);
    parts.push(`<line x1="${px}" y1="${m.top + plotH}" x2="${px}" y2="${m.top + plotH + 5}" stroke="#999"/>`);
    parts.push(`<text x="${px}" y="${m.top + plotH + 20}" text-anchor="middle" font-size="12" fill="#444">${xt}</text>`);
  }
  // Axes.
  parts.push(`<line x1="${m.left}" y1="${m.top}" x2="${m.left}" y2="${m.top + plotH}" stroke="#333"/>`);
  parts.push(`<line x1="${m.left}" y1="${m.top + plotH}" x2="${m.left + plotW}" y2="${m.top + plotH}" stroke="#333"/>`);
  parts.push(`<text x="${m.left + plotW / 2}" y="${H - 15}" text-anchor="middle" font-size="13" fill="#333">${opts.xLabel}</text>`);
  parts.push(`<text transform="translate(20,${m.top + plotH / 2}) rotate(-90)" text-anchor="middle" font-size="13" fill="#333">${opts.yLabel}</text>`);

  // Series.
  opts.series.forEach((s, i) => {
    const d = s.points.map((p, idx) => `${idx === 0 ? 'M' : 'L'}${sx(p.x).toFixed(1)},${sy(p.y).toFixed(1)}`).join(' ');
    parts.push(`<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2.5"/>`);
    for (const p of s.points) {
      parts.push(`<circle cx="${sx(p.x).toFixed(1)}" cy="${sy(p.y).toFixed(1)}" r="4" fill="${s.color}"/>`);
    }
    // Legend.
    const ly = m.top + 8 + i * 22;
    parts.push(`<line x1="${m.left + plotW + 16}" y1="${ly}" x2="${m.left + plotW + 40}" y2="${ly}" stroke="${s.color}" stroke-width="3"/>`);
    parts.push(`<text x="${m.left + plotW + 46}" y="${ly + 4}" font-size="12" fill="#333">${s.label}</text>`);
  });

  parts.push(`</svg>`);
  return parts.join('\n');
}

function barChart(opts: {
  title: string;
  yLabel: string;
  bars: { label: string; value: number; color: string }[];
  yUnit?: string;
  width?: number;
  height?: number;
}): string {
  const W = opts.width ?? 720;
  const H = opts.height ?? 420;
  const m = { top: 56, right: 30, bottom: 70, left: 78 };
  const plotW = W - m.left - m.right;
  const plotH = H - m.top - m.bottom;
  const yMax = Math.max(1, ...opts.bars.map((b) => b.value)) * 1.15;
  const sy = (y: number) => m.top + plotH - (y / yMax) * plotH;
  const bw = (plotW / opts.bars.length) * 0.6;
  const gap = plotW / opts.bars.length;

  const parts: string[] = [];
  parts.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family="ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial">`);
  parts.push(`<rect width="${W}" height="${H}" fill="#ffffff"/>`);
  parts.push(`<text x="${W / 2}" y="30" text-anchor="middle" font-size="18" font-weight="700" fill="#1a1a1a">${opts.title}</text>`);
  for (let i = 0; i <= 5; i++) {
    const yv = (yMax / 5) * i;
    const py = sy(yv);
    parts.push(`<line x1="${m.left}" y1="${py}" x2="${m.left + plotW}" y2="${py}" stroke="#eee"/>`);
    parts.push(`<text x="${m.left - 10}" y="${py + 4}" text-anchor="end" font-size="11" fill="#666">${fmt(yv, opts.yUnit)}</text>`);
  }
  parts.push(`<line x1="${m.left}" y1="${m.top}" x2="${m.left}" y2="${m.top + plotH}" stroke="#333"/>`);
  parts.push(`<line x1="${m.left}" y1="${m.top + plotH}" x2="${m.left + plotW}" y2="${m.top + plotH}" stroke="#333"/>`);
  parts.push(`<text transform="translate(20,${m.top + plotH / 2}) rotate(-90)" text-anchor="middle" font-size="13" fill="#333">${opts.yLabel}</text>`);
  opts.bars.forEach((b, i) => {
    const x = m.left + gap * i + (gap - bw) / 2;
    const y = sy(b.value);
    parts.push(`<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${bw.toFixed(1)}" height="${(m.top + plotH - y).toFixed(1)}" fill="${b.color}" rx="3"/>`);
    parts.push(`<text x="${(x + bw / 2).toFixed(1)}" y="${(y - 6).toFixed(1)}" text-anchor="middle" font-size="12" font-weight="600" fill="#333">${fmt(b.value, opts.yUnit)}</text>`);
    parts.push(`<text x="${(x + bw / 2).toFixed(1)}" y="${m.top + plotH + 20}" text-anchor="middle" font-size="11" fill="#444">${b.label}</text>`);
  });
  parts.push(`</svg>`);
  return parts.join('\n');
}

function fmt(v: number, unit?: string): string {
  if (unit === 's') return `${(v / 1000).toFixed(v < 10000 ? 1 : 0)}s`;
  if (unit === 'ms') return `${Math.round(v)}`;
  return v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(Math.round(v));
}

function main() {
  mkdirSync('docs/charts', { recursive: true });
  const data: Record<string, Summary | null> = {};
  for (const w of WORKERS) for (const u of USERS) data[`w${w}-u${u}`] = load(w, u);

  // Chart 1 — headline: time-to-pickup p95 vs users, per worker count.
  const pickupSeries: Series[] = WORKERS.map((w) => ({
    label: `${w} worker${w > 1 ? 's' : ''} (p95)`,
    color: COLORS[w]!,
    points: USERS.map((u) => ({ x: u, y: data[`w${w}-u${u}`]?.timeToPickupP95 ?? 0 })),
  }));
  writeFileSync(
    'docs/charts/pickup-p95.svg',
    lineChart({
      title: 'Time-to-pickup p95 vs concurrent users',
      xLabel: 'concurrent users',
      yLabel: 'time to pickup (p95)',
      xTicks: USERS,
      series: pickupSeries,
      yUnit: 's',
    }),
  );

  // Chart 1b — time-to-pickup p50.
  const pickupSeries50: Series[] = WORKERS.map((w) => ({
    label: `${w} worker${w > 1 ? 's' : ''} (p50)`,
    color: COLORS[w]!,
    points: USERS.map((u) => ({ x: u, y: data[`w${w}-u${u}`]?.timeToPickupP50 ?? 0 })),
  }));
  writeFileSync(
    'docs/charts/pickup-p50.svg',
    lineChart({
      title: 'Time-to-pickup p50 vs concurrent users',
      xLabel: 'concurrent users',
      yLabel: 'time to pickup (p50)',
      xTicks: USERS,
      series: pickupSeries50,
      yUnit: 's',
    }),
  );

  // Chart 2 — peak queue depth vs users, per worker (proves it tracks load, drains).
  const qSeries: Series[] = WORKERS.map((w) => ({
    label: `${w} worker${w > 1 ? 's' : ''} peak`,
    color: COLORS[w]!,
    points: USERS.map((u) => ({ x: u, y: data[`w${w}-u${u}`]?.peakQueueDepth ?? 0 })),
  }));
  writeFileSync(
    'docs/charts/queue-depth.svg',
    lineChart({
      title: 'Peak queue depth vs concurrent users',
      xLabel: 'concurrent users',
      yLabel: 'peak messages in queue',
      xTicks: USERS,
      series: qSeries,
    }),
  );

  // Chart 3 — Mongo p95 latency stays flat under load (indexed).
  const mSeries: Series[] = WORKERS.map((w) => ({
    label: `${w} worker${w > 1 ? 's' : ''}`,
    color: COLORS[w]!,
    points: USERS.map((u) => ({ x: u, y: data[`w${w}-u${u}`]?.mongoLatP95 ?? 0 })),
  }));
  writeFileSync(
    'docs/charts/mongo-latency.svg',
    lineChart({
      title: 'Mongo query p95 latency vs load (indexed)',
      xLabel: 'concurrent users',
      yLabel: 'mongo query p95 (ms)',
      xTicks: USERS,
      series: mSeries,
      yUnit: 'ms',
    }),
  );

  // Chart 4 — index regression bar chart (from measured probe values).
  writeFileSync(
    'docs/charts/index-regression.svg',
    barChart({
      title: 'SSE backfill p95 latency @205k events: indexed vs not',
      yLabel: 'backfill query p95 (ms)',
      yUnit: 'ms',
      bars: [
        { label: 'indexed', value: 1, color: '#2e86ab' },
        { label: 'NO index', value: 38, color: '#e4572e' },
      ],
    }),
  );

  // Chart 5 — scaling speedup: pickup p95 at 100 users, 1 vs 3 workers.
  writeFileSync(
    'docs/charts/scaling-100u.svg',
    barChart({
      title: 'Time-to-pickup p95 @100 users: 1 vs 3 workers',
      yLabel: 'time to pickup p95',
      yUnit: 's',
      bars: [
        { label: '1 worker', value: data['w1-u100']?.timeToPickupP95 ?? 0, color: COLORS[1]! },
        { label: '3 workers', value: data['w3-u100']?.timeToPickupP95 ?? 0, color: COLORS[3]! },
      ],
    }),
  );

  console.log('charts written to docs/charts/:');
  for (const f of ['pickup-p95', 'pickup-p50', 'queue-depth', 'mongo-latency', 'index-regression', 'scaling-100u'])
    console.log(`  docs/charts/${f}.svg`);
}

main();
