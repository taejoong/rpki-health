import { useEffect, useMemo, useState } from 'react';
import {
  LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts';
import { chartTickStyle, chartAxisStyle } from './ui';

/* Time-series kit for multi-year percentage charts (Country analysis).
   Numeric time x-axis (true spacing even when the grid changes from weekly to daily),
   explicit tick dates, solid hairline grid, HTML legend + crosshair tooltip that lists
   every series (value first), and a table-view / CSV twin for every chart. */

export const GRID_STROKE = '#ececf0';
const CURSOR = { stroke: '#9ca3af', strokeWidth: 1 };
const DAY_MS = 86400000;
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/* ── Formatting ── */
const nf1 = new Intl.NumberFormat('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const compactNf = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 });

/** Percent with 1 decimal; null/NaN -> '—'. */
export function fmtPct(v) {
  return v == null || !Number.isFinite(v) ? '—' : `${nf1.format(v)}%`;
}

/** Signed percentage-point change with 1 decimal ('+2.3', '−1.0', '0.0'); null -> '—'. */
export function fmtPp(v) {
  if (v == null || !Number.isFinite(v)) return '—';
  const r = Math.round(v * 10) / 10;
  if (r === 0) return '0.0';
  return `${r > 0 ? '+' : '−'}${nf1.format(Math.abs(r))}`;
}

/** Compact count (1.2K, 3.4M, 1.6B); null -> '—'. */
export function fmtCompact(v) {
  return v == null || !Number.isFinite(v) ? '—' : compactNf.format(v);
}

/** 100 * num / den, or null when the denominator is missing or 0. */
export function pct(num, den) {
  return num == null || den == null || !(den > 0) ? null : (100 * num) / den;
}

export const toTs = (d) => Date.parse(`${String(d).slice(0, 10)}T00:00:00Z`);
export const fmtDay = (t) => new Date(t).toISOString().slice(0, 10);

/** Drop leading rows where every series is null (e.g. BGP perspectives before 2020). */
export function trimLeadingEmpty(rows, keys) {
  const i = rows.findIndex((r) => keys.some((k) => r[k] != null));
  return i <= 0 ? (i === 0 ? rows : []) : rows.slice(i);
}

/** Explicit x ticks for a [t0, t1] window: Jan-1 years for long spans, quarters or months otherwise. */
export function timeTicks(t0, t1) {
  const span = (t1 - t0) / DAY_MS;
  const ticks = [];
  if (span > 3 * 365) {
    const y0 = new Date(t0).getUTCFullYear();
    const y1 = new Date(t1).getUTCFullYear();
    for (let y = y0; y <= y1; y++) {
      const t = Date.UTC(y, 0, 1);
      if (t >= t0 && t <= t1) ticks.push(t);
    }
    return { ticks, format: (t) => String(new Date(t).getUTCFullYear()) };
  }
  const step = span > 500 ? 3 : 1;
  const d0 = new Date(t0);
  for (let y = d0.getUTCFullYear(), m = d0.getUTCMonth(); ; m++) {
    if (m > 11) { m = 0; y++; }
    const t = Date.UTC(y, m, 1);
    if (t > t1) break;
    if (t >= t0 && m % step === 0) ticks.push(t);
  }
  return {
    ticks,
    format: (t) => {
      const d = new Date(t);
      return `${MONTHS[d.getUTCMonth()]} ’${String(d.getUTCFullYear()).slice(2)}`;
    },
  };
}

/** Rows for the table view: first point of each year (or month) plus the latest point. */
export function pickTableRows(rows, granularity = 'year') {
  const out = [];
  let last = null;
  for (const r of rows) {
    const d = new Date(r.t);
    const k = granularity === 'month' ? `${d.getUTCFullYear()}-${d.getUTCMonth()}` : d.getUTCFullYear();
    if (k !== last) { out.push(r); last = k; }
  }
  if (rows.length && out[out.length - 1] !== rows[rows.length - 1]) out.push(rows[rows.length - 1]);
  return out;
}

/* ── CSV ── */
function csvCell(v) {
  if (v == null) return '';
  let s = String(v);
  if (typeof v === 'string' && /^[=+\-@\t\r]/.test(s)) s = `'${s}`; // no spreadsheet formulas from data
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

export function downloadCsv(filename, header, rows) {
  const text = `${[header, ...rows].map((r) => r.map(csvCell).join(',')).join('\r\n')}\r\n`;
  const url = URL.createObjectURL(new Blob([text], { type: 'text/csv;charset=utf-8' }));
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** Round to n decimals for CSV output (keeps null). */
export const round = (v, n = 2) => (v == null || !Number.isFinite(v) ? null : Number(v.toFixed(n)));

/* ── Legend / keys ── */
export function SeriesKey({ color, kind = 'line', width = 2 }) {
  if (kind === 'rect') {
    return <span aria-hidden="true" className="inline-block shrink-0 w-2.5 h-2.5 rounded-[2px]" style={{ background: color }} />;
  }
  return (
    <span
      aria-hidden="true"
      className="inline-block shrink-0 w-3.5 rounded-full"
      style={{ height: 0, borderTop: `${Math.max(2, width)}px solid ${color}` }}
    />
  );
}

export function ChartLegend({ series, kind = 'line', hidden, onToggle }) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1.5 mb-3 text-xs text-cf-text-secondary">
      {series.map((s) => {
        const off = hidden?.has(s.key);
        const inner = (
          <>
            <SeriesKey color={s.color} kind={kind} width={s.width} />
            <span className={off ? 'line-through' : ''}>{s.name}</span>
          </>
        );
        return onToggle ? (
          <button
            key={s.key}
            type="button"
            aria-pressed={!off}
            title={off ? `Show ${s.name}` : `Hide ${s.name}`}
            onClick={() => onToggle(s.key)}
            className={`inline-flex items-center gap-1.5 hover:text-cf-text transition-opacity ${off ? 'opacity-45' : ''}`}
          >
            {inner}
          </button>
        ) : (
          <span key={s.key} className="inline-flex items-center gap-1.5">{inner}</span>
        );
      })}
    </div>
  );
}

/* ── Tooltip: every series at the hovered date, value first, line keys ──
   The charts pass filterNull={false}, so a date where every series is missing (a gap) still gets
   a tooltip that lists each series as '—' instead of none at all. */
function SeriesTooltip({ active, payload, series, fmt = fmtPct, extra }) {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0].payload;
  return (
    <div className="bg-white border border-[#e3e3e8] rounded-[10px] shadow-[0_8px_24px_rgba(0,0,0,0.1)] px-3 py-2 text-[13px] leading-snug">
      <div className="font-semibold text-cf-text mb-1">{fmtDay(row.t)}</div>
      <table>
        <tbody>
          {series.map((s) => (
            <tr key={s.key}>
              <td className="pr-1.5 align-middle"><SeriesKey color={s.color} /></td>
              <td className="pr-2 text-right font-semibold tabular-nums text-cf-text">{fmt(row[s.key])}</td>
              <td className="text-cf-text-secondary whitespace-nowrap">{s.name}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {extra && extra(row)}
    </div>
  );
}

function axisProps(rows, ticks) {
  let t0 = rows[0].t;
  let t1 = rows[rows.length - 1].t;
  if (t1 <= t0) { t0 -= DAY_MS; t1 += DAY_MS; } // a single point still needs a non-empty domain
  // A multi-year axis that starts a few days into January (the grid's first date is Wednesday
  // 2012-01-04) starts on Jan 1 instead, so the first year gets its tick label too.
  if ((t1 - t0) / DAY_MS > 3 * 365) {
    const jan1 = Date.UTC(new Date(t0).getUTCFullYear(), 0, 1);
    if (t0 - jan1 <= 31 * DAY_MS) t0 = jan1;
  }
  const tk = ticks || timeTicks(t0, t1);
  return {
    dataKey: 't',
    type: 'number',
    scale: 'time',
    domain: [t0, t1],
    ticks: tk.ticks,
    tickFormatter: tk.format,
    interval: 'equidistantPreserveStart',
    minTickGap: 12,
    tick: chartTickStyle,
    axisLine: chartAxisStyle,
    tickLine: false,
    height: 24,
  };
}

const yPctProps = {
  domain: [0, 100],
  ticks: [0, 25, 50, 75, 100],
  tickFormatter: (v) => `${v}%`,
  width: 40,
  tick: chartTickStyle,
  axisLine: false,
  tickLine: false,
};

export const FULL_PCT_AXIS = { top: 100, ticks: [0, 25, 50, 75, 100] };
const PCT_TOPS = [[5, 1], [10, 2], [20, 5], [50, 10]]; // [axis top, tick step]

/**
 * 0-based % axis for one country's or one AS's lines: 0-100 %, or a round lower top when every
 * value is small (South Korea's lines all stay under 10 %), so they are not flattened onto the
 * baseline. Computed over all given series, so hiding a series in the legend does not rescale.
 */
export function pctAxis(rows, keys) {
  let max = null;
  rows.forEach((r) => keys.forEach((k) => {
    const v = r[k];
    if (v != null && Number.isFinite(v) && (max == null || v > max)) max = v;
  }));
  if (max == null) return FULL_PCT_AXIS;
  for (const [top, step] of PCT_TOPS) {
    if (max <= top * 0.95) {
      const ticks = [];
      for (let v = 0; v <= top; v += step) ticks.push(v);
      return { top, ticks };
    }
  }
  return FULL_PCT_AXIS;
}

/* End-of-line marker + value for the one series the view is about. */
function makeEndDot(color, lastIdx, fmt) {
  return function EndDot(p) {
    if (p.index !== lastIdx || p.cx == null || p.cy == null || p.value == null) return <g key={p.key} />;
    return (
      <g key={p.key}>
        <circle cx={p.cx} cy={p.cy} r={4} fill={color} stroke="#ffffff" strokeWidth={2} />
        <text x={p.cx + 8} y={p.cy} dy="0.35em" fontSize={11} fontWeight={600} fill="#1a1a1f">{fmt(p.value)}</text>
      </g>
    );
  };
}

/**
 * Multi-series % line chart over time.
 * rows: [{t, <key>: value|null}], series: [{key, name, color, width?}], endLabelKey labels the last point.
 */
export function TimeLineChart({
  rows, series, height = 300, endLabelKey, ariaLabel, fmt = fmtPct, toggleable = true, tooltipExtra,
  yAxis = FULL_PCT_AXIS,
}) {
  const [hidden, setHidden] = useState(() => new Set());
  const visible = series.filter((s) => !hidden.has(s.key));
  const toggle = (k) => setHidden((prev) => {
    const next = new Set(prev);
    if (next.has(k)) next.delete(k);
    else if (visible.length > 1) next.add(k); // never hide the last visible series
    return next;
  });
  const lastIdx = useMemo(() => {
    if (!endLabelKey) return -1;
    for (let i = rows.length - 1; i >= 0; i--) if (rows[i][endLabelKey] != null) return i;
    return -1;
  }, [rows, endLabelKey]);
  const labelled = endLabelKey && !hidden.has(endLabelKey) && lastIdx >= 0;

  return (
    <div>
      <ChartLegend series={series} kind="line" hidden={hidden} onToggle={toggleable && series.length > 1 ? toggle : undefined} />
      <div role="img" aria-label={ariaLabel}>
        <ResponsiveContainer width="100%" height={height}>
          <LineChart data={rows} margin={{ top: 8, right: labelled ? 48 : 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke={GRID_STROKE} vertical={false} />
            <XAxis {...axisProps(rows)} />
            <YAxis {...yPctProps} domain={[0, yAxis.top]} ticks={yAxis.ticks} />
            <Tooltip
              content={<SeriesTooltip series={visible} fmt={fmt} extra={tooltipExtra} />}
              cursor={CURSOR}
              filterNull={false}
              isAnimationActive={false}
            />
            {visible.map((s) => (
              <Line
                key={s.key}
                dataKey={s.key}
                name={s.name}
                type="linear"
                stroke={s.color}
                strokeWidth={s.width || 2}
                strokeLinejoin="round"
                strokeLinecap="round"
                dot={labelled && s.key === endLabelKey ? makeEndDot(s.color, lastIdx, fmt) : false}
                activeDot={{ r: 4, stroke: '#ffffff', strokeWidth: 2, fill: s.color }}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

/**
 * Stacked 100% area chart over time (values already in %, bottom series first).
 * Full-colour bands separated by a 2px white edge.
 */
export function StackedPctChart({ rows, series, height = 260, ariaLabel, fmt = fmtPct, tooltipExtra }) {
  return (
    <div>
      <ChartLegend series={series} kind="rect" />
      <div role="img" aria-label={ariaLabel}>
        <ResponsiveContainer width="100%" height={height}>
          <AreaChart data={rows} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke={GRID_STROKE} vertical={false} />
            <XAxis {...axisProps(rows)} />
            <YAxis {...yPctProps} allowDataOverflow />
            <Tooltip
              content={<SeriesTooltip series={[...series].reverse()} fmt={fmt} extra={tooltipExtra} />}
              cursor={CURSOR}
              filterNull={false}
              isAnimationActive={false}
            />
            {series.map((s) => (
              <Area
                key={s.key}
                dataKey={s.key}
                name={s.name}
                stackId="1"
                type="linear"
                stroke="#ffffff"
                strokeWidth={2}
                fill={s.color}
                fillOpacity={1}
                activeDot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

/* ── Table view twin ── */
export function ChartTable({ rows, series, fmt = fmtPct, granularity = 'year', caption }) {
  const picked = useMemo(() => pickTableRows(rows, granularity), [rows, granularity]);
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        {caption && <caption className="text-left text-xs text-cf-text-muted pb-2">{caption}</caption>}
        <thead>
          <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
            <th className="px-3 py-2 text-left font-medium">Date</th>
            {series.map((s) => (
              <th key={s.key} className="px-3 py-2 text-right font-medium whitespace-nowrap">
                <span className="inline-flex items-center gap-1.5"><SeriesKey color={s.color} />{s.name}</span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {picked.map((r, i) => (
            <tr key={r.t} className={`border-b border-cf-border ${i % 2 === 1 ? 'bg-cf-surface-2' : ''}`}>
              <td className="px-3 py-2 font-mono text-xs whitespace-nowrap">{fmtDay(r.t)}</td>
              {series.map((s) => (
                <td key={s.key} className="px-3 py-2 text-right tabular-nums">{fmt(r[s.key])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ── Card actions: Chart/Table toggle + CSV ── */
export function ChartActions({ view, onView, onCsv }) {
  const btn = 'px-2 py-1 text-[11px] font-medium transition-colors';
  return (
    <div className="flex items-center gap-1.5">
      <div role="group" aria-label="View" className="inline-flex rounded-md border border-cf-border overflow-hidden">
        {['chart', 'table'].map((v) => (
          <button
            key={v}
            type="button"
            aria-pressed={view === v}
            onClick={() => onView(v)}
            className={`${btn} ${view === v ? 'bg-cf-surface-2 text-cf-text' : 'text-cf-text-muted hover:text-cf-text'}`}
          >
            {v === 'chart' ? 'Chart' : 'Table'}
          </button>
        ))}
      </div>
      {onCsv && (
        <button
          type="button"
          onClick={onCsv}
          title="Download CSV"
          className={`${btn} rounded-md border border-cf-border text-cf-text-secondary hover:bg-cf-surface-2 hover:text-cf-text`}
        >
          CSV
        </button>
      )}
    </div>
  );
}

/** Fixed-height placeholder while a chart has no data yet (keeps the layout stable). */
export function ChartPlaceholder({ height = 300, message }) {
  return (
    <div className="flex items-center justify-center" style={{ height }}>
      {message ? <span className="text-sm text-cf-text-muted text-center px-4">{message}</span> : <div className="spinner" />}
    </div>
  );
}

/**
 * Width of an element (ResizeObserver), for sticky panels inside horizontally scrolling tables.
 * Returns [callbackRef, width] so it also works when the element mounts later.
 */
export function useElementWidth() {
  const [el, setEl] = useState(null);
  const [w, setW] = useState(0);
  useEffect(() => {
    if (!el) return undefined;
    setW(el.clientWidth);
    if (typeof ResizeObserver === 'undefined') return undefined;
    const ro = new ResizeObserver(() => setW(el.clientWidth));
    ro.observe(el);
    return () => ro.disconnect();
  }, [el]);
  return [setEl, w];
}
