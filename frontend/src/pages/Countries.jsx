import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';
import {
  Page, Card, Select, Input, Button, ErrorBox, Empty, RirBadge, fmtNum, RIRS,
} from '../components/ui';
import {
  TimeLineChart, StackedPctChart, ChartTable, ChartActions, ChartPlaceholder, SeriesKey,
  downloadCsv, fmtPct, fmtPp, fmtCompact, pct, toTs, fmtDay, round, trimLeadingEmpty, pctAxis,
} from '../components/TimeCharts';
import {
  LENSES, LENS_BY_ID, BREAKDOWN, RIR_LINE_COLORS, ALL_RIRS_COLOR, RIR_NAMES, BGP_FROM,
  countryName, flagEmoji, useCountryApi, errorText, errorStatus,
} from '../components/countryShared';
import CountryAsTable from '../components/CountryAsTable';

/* Analysis -> Countries: ROA deployment per RIR and per registered country over time.
   URL state: ?cc=&af=4|6&lens=routes|space|registered|ases&range=all|2020|1y */

const DAY_MS = 86400000;
const DEFAULTS = { af: '4', lens: 'routes', range: 'all' };
const RANGES = [
  { value: 'all', label: 'All' },
  { value: '2020', label: 'Since 2020' },
  { value: '1y', label: '1 year' },
];
const AF_OPTIONS = [{ value: '4', label: 'IPv4' }, { value: '6', label: 'IPv6' }];
const LENS_OPTIONS = LENSES.map((l) => ({ value: l.id, label: l.label }));
const RIR_SERIES = [
  { key: 'all', name: RIR_NAMES.all, color: ALL_RIRS_COLOR },
  ...RIRS.map((r) => ({ key: r, name: RIR_NAMES[r], color: RIR_LINE_COLORS[r] })),
];
const RIR_KEYS = RIR_SERIES.map((s) => s.key);
const LENS_KEYS = LENSES.map((l) => l.id);

function applyRange(rows, range, asOf) {
  if (!rows.length || range === 'all') return rows;
  if (range === '2020') return rows.filter((r) => r.t >= Date.UTC(2020, 0, 1));
  // 1 year: from prev(L), the latest grid date at least 365 days before as_of. Every Δ 1y on
  // the page compares with that date, so the chart starts at the point the Δ is measured from.
  const limit = (asOf ? toTs(asOf) : rows[rows.length - 1].t) - 365 * DAY_MS;
  const i = rows.findIndex((r) => r.t > limit);
  if (i < 0) return [];
  return rows.slice(Math.max(0, i - 1));
}

/** Latest index with a defined lens value, and the 1-year-earlier grid index (prev(L)). */
function lensLatest(metrics, dates, lens, asOf) {
  if (!metrics || !dates) return null;
  const num = metrics[lens.num] || [];
  const den = metrics[lens.den] || [];
  let i = dates.length - 1;
  while (i >= 0 && pct(num[i], den[i]) == null) i--;
  if (i < 0) return null;
  const limit = toTs(asOf || dates[dates.length - 1]) - 365 * DAY_MS;
  let j = dates.length - 1;
  while (j >= 0 && toTs(dates[j]) > limit) j--;
  const value = pct(num[i], den[i]);
  const prev = j >= 0 ? pct(num[j], den[j]) : null;
  return {
    value, num: num[i], den: den[i], date: dates[i], end: dates[dates.length - 1],
    prev, prevDate: j >= 0 ? dates[j] : null, delta: prev == null ? null : value - prev,
  };
}

function deltaClass(v) {
  if (v == null || Math.abs(v) < 0.05) return 'text-cf-text-muted';
  return v > 0 ? 'text-emerald-700' : 'text-red-700';
}

/* ── Controls ── */
function Segmented({ label, value, options, onChange }) {
  return (
    <div className="flex flex-col gap-1 text-xs font-medium text-cf-text-secondary">
      <span>{label}</span>
      <div role="group" aria-label={label} className="inline-flex rounded-lg border border-cf-border bg-cf-surface p-0.5">
        {options.map((o) => (
          <button
            key={o.value}
            type="button"
            aria-pressed={value === o.value}
            onClick={() => onChange(o.value)}
            className={`px-3 py-1.5 rounded-md text-sm whitespace-nowrap transition-colors ${
              value === o.value ? 'bg-cf-orange-light text-cf-orange font-semibold' : 'text-cf-text-secondary hover:text-cf-text'
            }`}
          >
            {o.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function FilterRow({ range, lens, af, update }) {
  return (
    <div className="flex flex-wrap items-end gap-2 lg:gap-3 mb-6 pb-5 border-b border-cf-border">
      <Segmented label="Range" value={range} options={RANGES} onChange={(v) => update({ range: v })} />
      <Select label="Perspective" value={lens.id} options={LENS_OPTIONS} onChange={(e) => update({ lens: e.target.value })} />
      <Segmented label="Address family" value={String(af)} options={AF_OPTIONS} onChange={(v) => update({ af: v })} />
    </div>
  );
}

function Refetching({ stale, children }) {
  return (
    <div className={`transition-opacity duration-150 ${stale ? 'opacity-50' : ''}`} aria-busy={stale}>
      {children}
    </div>
  );
}

function CardNote({ children }) {
  return <p className="text-xs text-cf-text-muted mb-3 -mt-1">{children}</p>;
}

const BGP_FROM_TS = toTs(BGP_FROM);

/* Explain where BGP-based series start, only when the chart actually shows that boundary:
   'mixed' charts (all perspectives) when they begin before 2020; single-perspective charts when
   the BGP start is what trimmed them (not a country that appears later). */
function bgpNote(kind, lens, range, rows) {
  if (range !== 'all' || !rows.length) return null;
  const t0 = rows[0].t;
  if (kind === 'mixed') {
    return t0 < BGP_FROM_TS ? ` BGP-based perspectives start ${BGP_FROM}; registered address space goes back to 2012.` : null;
  }
  return lens.bgp && t0 >= BGP_FROM_TS && t0 < BGP_FROM_TS + 45 * DAY_MS
    ? ` BGP-based data starts ${BGP_FROM}; the registered address space perspective goes back to 2012.`
    : null;
}

const nowrap = (s) => <span className="whitespace-nowrap">{s}</span>;
const nbsp = (s) => String(s).replace(/ /g, '\u00a0'); // 'RIPE NCC', 'All RIRs' stay on one line

/* ── Country picker (searchable combobox) ── */
function CountryPicker({ countries, value, onPick }) {
  const [q, setQ] = useState('');
  const [open, setOpen] = useState(false);
  const [hi, setHi] = useState(0);
  const listRef = useRef(null);
  const options = useMemo(() => {
    const all = (countries || [])
      .filter((c) => c.cc && c.cc !== 'ZZ')
      .map((c) => ({ cc: c.cc, name: countryName(c.cc), routes: c.routes || 0 }))
      .sort((a, b) => a.name.localeCompare(b.name));
    const s = q.trim().toLowerCase();
    if (!s) return all;
    // Best match first: the code or the exact name, then names starting with the text, then a
    // word starting with it, then any match; ties go to the country with more routes ("korea" ->
    // South Korea, then North Korea; "united" -> United States first), so Enter picks the likely one.
    const rank = (o) => {
      const n = o.name.toLowerCase();
      if (o.cc.toLowerCase() === s || n === s) return 0;
      if (n.startsWith(s)) return 1;
      if (n.split(/[^\p{L}]+/u).some((w) => w.startsWith(s))) return 2;
      return 3;
    };
    return all
      .filter((o) => o.cc.toLowerCase() === s || o.name.toLowerCase().includes(s))
      .map((o) => ({ ...o, r: rank(o) }))
      .sort((a, b) => a.r - b.r || b.routes - a.routes || a.name.localeCompare(b.name));
  }, [countries, q]);

  useEffect(() => {
    const el = listRef.current?.querySelector('[data-active="true"]');
    if (el) el.scrollIntoView({ block: 'nearest' });
  }, [hi, open]);

  function pick(cc) {
    setOpen(false);
    setQ('');
    if (cc && cc !== value) onPick(cc);
  }
  function onKeyDown(e) {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (!open) setOpen(true);
      else setHi((h) => Math.min(h + 1, options.length - 1));
    } else if (e.key === 'ArrowUp') { e.preventDefault(); setHi((h) => Math.max(h - 1, 0)); }
    else if (e.key === 'Enter') {
      if (open && options[hi]) { e.preventDefault(); pick(options[hi].cc); }
    } else if (e.key === 'Escape') { setOpen(false); }
  }
  const activeId = open && options[hi] ? `country-opt-${options[hi].cc}` : undefined;

  return (
    <div className="relative w-full sm:w-72">
      <label className="flex flex-col gap-1 text-xs font-medium text-cf-text-secondary">
        {value ? 'Switch country' : 'Country'}
        <input
          role="combobox"
          aria-expanded={open}
          aria-controls="country-picker-list"
          aria-autocomplete="list"
          aria-activedescendant={activeId}
          autoComplete="off"
          maxLength={60}
          className="bg-cf-surface border border-cf-border rounded-lg px-3 py-2 text-sm text-cf-text
                     focus:outline-none focus:ring-2 focus:ring-cf-orange/20 focus:border-cf-orange"
          placeholder="Name or code, e.g. Korea or KR"
          value={q}
          onChange={(e) => { setQ(e.target.value); setOpen(true); setHi(0); }}
          onFocus={() => setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 120)}
          onKeyDown={onKeyDown}
        />
      </label>
      {open && (
        <ul
          id="country-picker-list"
          ref={listRef}
          role="listbox"
          className="absolute z-30 left-0 right-0 mt-1 max-h-72 overflow-y-auto bg-cf-surface border border-cf-border rounded-lg shadow-[0_8px_24px_rgba(0,0,0,0.1)] py-1"
        >
          {options.length === 0 && <li className="px-3 py-2 text-sm text-cf-text-muted">No match</li>}
          {options.map((o, i) => (
            <li
              key={o.cc}
              id={`country-opt-${o.cc}`}
              role="option"
              aria-selected={o.cc === value}
              data-active={i === hi}
              onMouseDown={(e) => { e.preventDefault(); pick(o.cc); }}
              onMouseEnter={() => setHi(i)}
              className={`px-3 py-1.5 text-sm cursor-pointer flex items-center gap-2 ${i === hi ? 'bg-cf-surface-2' : ''} ${o.cc === value ? 'font-semibold' : ''}`}
            >
              <span aria-hidden="true" className="w-5 shrink-0">{flagEmoji(o.cc)}</span>
              <span className="truncate">{o.name}</span>
              <span className="ml-auto font-mono text-xs text-cf-text-muted">{o.cc}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/* ── Methodology (lazy /meta; the text comes from the build manifest written by crc.assemble) ── */
const ACRONYMS = {
  as: 'AS', ases: 'ASes', asn: 'ASN', asns: 'ASNs', bgp: 'BGP', rir: 'RIR', rirs: 'RIRs',
  roa: 'ROA', roas: 'ROAs', vrp: 'VRP', vrps: 'VRPs',
};
// Whole keys whose words are not acronyms ("as of" is a date, not an AS).
const KEY_LABELS = { as_of: 'As of' };
const humanize = (k) => KEY_LABELS[k] || String(k).split('_').filter(Boolean)
  .map((w, i) => ACRONYMS[w.toLowerCase()] || (i === 0 ? w.charAt(0).toUpperCase() + w.slice(1) : w))
  .join(' ');
// Reading order for the manifest's method entries (the file may arrive with sorted keys): the
// current manifest's keys, then the keys of earlier builds, so an older manifest still reads in order.
const METHOD_ORDER = [
  'country', 'routes', 'invalid_routes', 'routed_space', 'registered_space', 'origin_ases',
  'validation_and_roas', 'rir_lines', 'time_grid', 'as_table',
  'routes_and_visibility', 'visibility', 'collectors', 'validation', 'vrps', 'country_rule', 'space', 'units',
];
const methodEntries = (method) => Object.entries(method).sort(([a], [b]) => {
  const ia = METHOD_ORDER.indexOf(a);
  const ib = METHOD_ORDER.indexOf(b);
  return (ia < 0 ? METHOD_ORDER.length : ia) - (ib < 0 ? METHOD_ORDER.length : ib);
});
const noPeriod = (s) => String(s).replace(/\.\s*$/, '');

function plain(v) {
  if (v == null) return '—';
  if (typeof v !== 'object') return String(v);
  if (Array.isArray(v)) return v.map(plain).join(', ');
  return Object.entries(v).map(([k, x]) => `${humanize(k)}: ${plain(x)}`).join('; ');
}

function SafeLink({ href, children }) {
  if (typeof href !== 'string' || !/^https?:\/\//i.test(href)) return children;
  return <a href={href} target="_blank" rel="noopener noreferrer" className="text-cf-orange hover:underline break-all">{children}</a>;
}

function noteText(n) {
  if (n == null || typeof n !== 'object') return { when: null, text: plain(n) };
  const from = n.from || n.start || n.date_from;
  const to = n.to || n.end || n.date_to;
  let when = n.date || null;
  if (!when && from && to) when = from === to ? from : `${from} – ${to}`;
  else if (!when && from) when = `since ${from}`; // open-ended (to: null)
  else if (!when && to) when = `until ${to}`;
  const text = n.text || n.note || n.description || n.label || plain(
    Object.fromEntries(Object.entries(n).filter(([k]) => !['date', 'from', 'to', 'start', 'end', 'date_from', 'date_to'].includes(k))),
  );
  return { when, text };
}

/* Notes in time order (the manifest lists them by topic), undated ones last. */
const noteStart = (n) => (n && typeof n === 'object' ? String(n.from || n.start || n.date_from || n.date || '') : '');
const sortNotes = (notes) => [...notes].sort((a, b) => {
  const x = noteStart(a);
  const y = noteStart(b);
  if (!x || !y) return (x ? 0 : 1) - (y ? 0 : 1);
  return x.localeCompare(y);
});

function Methodology() {
  const [open, setOpen] = useState(false);
  const meta = useCountryApi('meta', {}, open);
  const m = meta.data;
  const lensMeta = m && m.lenses && typeof m.lenses === 'object' ? m.lenses : {};
  return (
    <details
      className="group mt-6 bg-cf-surface rounded-xl shadow-[0_2px_8px_rgba(0,0,0,0.06)]"
      onToggle={(e) => setOpen(e.currentTarget.open)}
    >
      <summary className="px-5 py-3.5 text-[13px] font-semibold tracking-tight text-cf-text cursor-pointer select-none flex items-center gap-2 list-none [&::-webkit-details-marker]:hidden">
        <span aria-hidden="true" className="text-[9px] text-cf-text-muted transition-transform group-open:rotate-90">&#9654;</span>
        Methodology
      </summary>
      <div className="px-5 pb-5 pt-4 border-t border-cf-border/60 text-sm text-cf-text-secondary leading-relaxed space-y-4">
        <p>
          A country is the <strong className="text-cf-text">registered country</strong> of the address space, not
          where it is used: the RIR delegation record that contains it, in the delegation snapshot of the first of
          the month. EU and AP are RIR-wide codes without a single country. Routes are BGP announcements (prefix,
          origin AS) seen by the RouteViews and RIPE RIS collectors, for IPv4 prefixes from {nowrap('/8 to /24')} and
          IPv6 prefixes from {nowrap('/16 to /48')}, validated against the ROAs (VRPs) of the same UTC day.
        </p>
        <div>
          <div className="text-xs font-semibold uppercase tracking-wide text-cf-text-muted mb-1.5">Perspectives</div>
          <ul className="space-y-1">
            {LENSES.map((l) => {
              const lm = lensMeta[l.id] || {};
              return (
                <li key={l.id} className="flex items-start gap-2">
                  <span className="h-[1.625em] flex items-center shrink-0"><SeriesKey color={l.color} /></span>
                  <span className="min-w-0">
                    <strong className="text-cf-text">{lm.label || l.label}</strong>: {noPeriod(lm.description || l.what)}{' '}
                    <span className="font-mono text-xs">({lm.numerator || l.num} / {lm.denominator || l.den})</span>
                    {(lm.from || l.bgp) && <span className="text-cf-text-muted">, from {nowrap(lm.from || BGP_FROM)}</span>}.
                  </span>
                </li>
              );
            })}
          </ul>
        </div>
        {meta.loading && <div className="py-2"><div className="spinner" /></div>}
        {meta.error && <ErrorBox message={errorText(meta.error)} />}
        {m && (
          <>
            <p>
              Data as of <strong className="text-cf-text">{m.as_of}</strong>
              {m.generated_at ? <> (built {String(m.generated_at).replace('T', ' ').replace('Z', ' UTC')})</> : null}.
              {m.grid ? <> Time grid: every Wednesday from {m.grid.weekly_from} to 2025-12-31, then daily from {m.grid.daily_from}.</> : null}
              {m.bgp_from ? <> BGP-based perspectives start {m.bgp_from}.</> : null}
              {m.prev_date ? <> One-year changes (&Delta; 1y) compare with {m.prev_date}.</> : null}
            </p>
            {m.method && typeof m.method === 'object' && (
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-cf-text-muted mb-1.5">Method</div>
                <dl className="grid grid-cols-1 sm:grid-cols-[minmax(0,10rem)_minmax(0,1fr)] gap-x-4 gap-y-1.5">
                  {methodEntries(m.method).map(([k, v]) => (
                    <div key={k} className="contents">
                      <dt className="font-medium text-cf-text">{humanize(k)}</dt>
                      <dd className="break-words">{plain(v)}</dd>
                    </div>
                  ))}
                </dl>
              </div>
            )}
            {Array.isArray(m.notes) && m.notes.length > 0 && (
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-cf-text-muted mb-1.5">Data quality notes</div>
                <ul className="list-disc pl-5 space-y-1">
                  {sortNotes(m.notes).map((n, i) => {
                    const { when, text } = noteText(n);
                    return (
                      <li key={i}>
                        {when && <span className="font-mono text-xs text-cf-text">{when}: </span>}
                        {text}
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}
            {Array.isArray(m.sources) && m.sources.length > 0 && (
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-cf-text-muted mb-1.5">Sources</div>
                <ul className="list-disc pl-5 space-y-1">
                  {m.sources.map((s, i) => (
                    <li key={i}>
                      {s && typeof s === 'object' ? (
                        <>
                          <SafeLink href={s.url}>{s.name || s.label || s.url || plain(s)}</SafeLink>
                          {(s.use || s.description) && <span className="text-cf-text-secondary">: {s.use || s.description}</span>}
                          {(s.from || s.to) && <span className="text-cf-text-muted"> ({[s.from, s.to].filter(Boolean).join(' – ')})</span>}
                        </>
                      ) : plain(s)}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </>
        )}
      </div>
    </details>
  );
}

/* ── Default view: RIR chart + country ranking ── */
const RANK_COLUMNS = [
  { key: 'name', label: 'Country', align: 'left', dir: 'asc' },
  { key: 'registry', label: 'RIR', align: 'left', dir: 'asc', hideOnPhone: true },
  { key: 'routes', label: 'Routes', align: 'right' },
  { key: 'valid_pct', label: 'Valid %', align: 'right', lens: 'routes' },
  { key: 'invalid_pct', label: 'Invalid %', align: 'right' },
  { key: 'space_pct', label: 'Routed space valid %', align: 'right', lens: 'space' },
  { key: 'reg_pct', label: 'Registered space covered %', align: 'right', lens: 'registered' },
  { key: 'ases_pct', label: 'Origin ASes with ROA %', align: 'right', lens: 'ases' },
  { key: 'delta', label: 'Δ 1y (pp)', align: 'right' },
];
const RANK_INITIAL = 50;

/* Minimum size for a sort by a share or by Δ 1y. Below it a percentage rests on a handful of
   routes, ASes or address blocks (CK: 6 routes, +100 pp; DJ: 69 routes, +97 pp), so those rows
   are listed after the others. Every perspective needs MIN_ROUTES; origin ASes and the two space
   perspectives also need a minimum of their own denominator. Δ 1y needs it on both dates. */
const MIN_ROUTES = 100;
const MIN_ASES = 10;
const MIN_SPACE = { 4: 65536, 6: 1048576 }; // IPv4: a /16 (addresses); IPv6: a /28 (1,048,576 /48s)
const MIN_SPACE_LABEL = { 4: '/16', 6: '/28' };
const SORT_LENS = { valid_pct: 'routes', invalid_pct: 'routes', space_pct: 'space', reg_pct: 'registered', ases_pct: 'ases' };

/** True when a country's values ({routes, ases, space, deleg}) are large enough to rank by `lensId`. */
function bigEnough(lensId, v, af) {
  if (!v || !(v.routes >= MIN_ROUTES)) return false;
  if (lensId === 'ases') return v.ases >= MIN_ASES;
  if (lensId === 'space') return v.space >= MIN_SPACE[af];
  if (lensId === 'registered') return v.deleg >= MIN_SPACE[af];
  return true;
}

function minRuleText(lensId, af) {
  const block = `${af === 6 ? 'an IPv6' : 'an IPv4'} ${MIN_SPACE_LABEL[af]}`;
  if (lensId === 'ases') return `fewer than ${MIN_ROUTES} routes or ${MIN_ASES} origin ASes`;
  if (lensId === 'space') return `fewer than ${MIN_ROUTES} routes, or less routed space than ${block}`;
  if (lensId === 'registered') return `fewer than ${MIN_ROUTES} routes, or less registered space than ${block}`;
  return `fewer than ${MIN_ROUTES} routes`;
}

function RankHeader({ col, sort, onSort, lensId }) {
  const active = sort.key === col.key;
  const right = col.align === 'right';
  const highlight = col.lens && col.lens === lensId;
  return (
    <th
      scope="col"
      aria-sort={active ? (sort.dir === 'asc' ? 'ascending' : 'descending') : 'none'}
      className={`px-2.5 py-2.5 font-medium select-none align-bottom ${right ? 'text-right' : 'text-left'} ${
        col.key === 'name' ? 'sticky left-0 z-[2] bg-cf-surface-2 min-w-[8rem]' : 'min-w-[4.75rem]'
      } ${highlight ? 'text-cf-text' : ''} ${col.hideOnPhone ? 'hidden sm:table-cell' : ''}`}
    >
      <button
        type="button"
        onClick={() => onSort(col)}
        className={`inline-flex items-end gap-1 uppercase tracking-wider leading-tight hover:text-cf-text ${right ? 'flex-row-reverse text-right' : 'text-left'}`}
      >
        <span>{col.label}</span>
        <span className="inline-flex flex-col leading-none mb-0.5 shrink-0" aria-hidden="true">
          <span className={`text-[8px] ${active && sort.dir === 'asc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'▲'}</span>
          <span className={`text-[8px] ${active && sort.dir === 'desc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'▼'}</span>
        </span>
      </button>
    </th>
  );
}

function RankingCard({ list, lens, af, linkFor }) {
  const navigate = useNavigate();
  const [q, setQ] = useState('');
  const [rir, setRir] = useState('');
  const [sort, setSort] = useState({ key: 'routes', dir: 'desc' });
  const [showAll, setShowAll] = useState(false);
  const [smallLast, setSmallLast] = useState(true);
  const data = list.data;
  const dataAf = Number(data?.af ?? af); // minimum space follows the rows on screen (stale rows during a refetch)
  // The minimum that applies to the current sort: a share column, or Δ 1y of the selected perspective.
  const minLens = sort.key === 'delta' ? lens.id : SORT_LENS[sort.key] || null;

  const base = useMemo(() => (data?.rows || [])
    .filter((r) => r.cc && r.cc !== 'ZZ')
    .map((r) => {
      const p = r.prev || {};
      const now = Object.fromEntries(LENSES.map((l) => [l.id, pct(r[l.num], r[l.den])]));
      const then = Object.fromEntries(LENSES.map((l) => [l.id, pct(p[l.num], p[l.den])]));
      const inv = r.routes_inv_as == null && r.routes_inv_len == null ? null : (r.routes_inv_as || 0) + (r.routes_inv_len || 0);
      return {
        ...r,
        name: countryName(r.cc),
        valid_pct: now.routes,
        invalid_pct: pct(inv, r.routes),
        space_pct: now.space,
        reg_pct: now.registered,
        ases_pct: now.ases,
        now,
        then,
      };
    }), [data]);

  const { rows, nSmall, partitioned } = useMemo(() => {
    const s = q.trim().toLowerCase();
    const filtered = base
      .filter((r) => !rir || r.registry === rir)
      .filter((r) => !s || r.cc.toLowerCase() === s || r.name.toLowerCase().includes(s) || r.cc.toLowerCase().startsWith(s))
      .map((r) => ({
        ...r,
        delta: r.now[lens.id] == null || r.then[lens.id] == null ? null : r.now[lens.id] - r.then[lens.id],
        small: !!minLens && !(bigEnough(minLens, r, dataAf) && (sort.key !== 'delta' || bigEnough(minLens, r.prev, dataAf))),
      }));
    const small = filtered.filter((r) => r.small).length;
    // Small rows go after the others (in the same order) unless the reader mixes them in; the
    // CSV keeps every row either way.
    const split = !!minLens && smallLast && small > 0;
    const { key, dir } = sort;
    filtered.sort((a, b) => {
      if (split && a.small !== b.small) return a.small ? 1 : -1;
      const av = a[key];
      const bv = b[key];
      if (av == null && bv == null) return a.cc.localeCompare(b.cc);
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv));
      return (dir === 'asc' ? cmp : -cmp) || a.cc.localeCompare(b.cc);
    });
    return { rows: filtered, nSmall: small, partitioned: split };
  }, [base, q, rir, sort, lens.id, minLens, smallLast, dataAf]);
  const ruleText = minLens
    ? `${minRuleText(minLens, dataAf)}${sort.key === 'delta' ? `, now or on ${data?.prev_date || 'the comparison date'}` : ''}`
    : '';
  const firstSmall = partitioned ? rows.findIndex((r) => r.small) : -1;

  function onSort(col) {
    setSort((s) => (s.key === col.key ? { key: s.key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key: col.key, dir: col.dir || 'desc' }));
  }

  function csv() {
    const header = [
      'cc', 'country', 'registry', 'routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len', 'routes_nf',
      'valid_pct', 'invalid_pct', 'space', 'space_valid', 'space_valid_pct', 'deleg', 'deleg_cov',
      'registered_covered_pct', 'ases', 'ases_any_valid', 'ases_with_roa_pct', 'vrps', `delta_1y_pp_${lens.id}`,
    ];
    downloadCsv(
      `countries-roa-ipv${af}-${data.as_of}.csv`,
      header,
      rows.map((r) => [
        r.cc, r.name, r.registry, r.routes, r.routes_valid, r.routes_inv_as, r.routes_inv_len, r.routes_nf,
        round(r.valid_pct), round(r.invalid_pct), r.space, r.space_valid, round(r.space_pct), r.deleg, r.deleg_cov,
        round(r.reg_pct), r.ases, r.ases_any_valid, round(r.ases_pct), r.vrps, round(r.delta),
      ]),
    );
  }

  const shown = showAll ? rows : rows.slice(0, RANK_INITIAL);
  const rirOptions = [{ value: '', label: 'All RIRs' }, ...RIRS.map((r) => ({ value: r, label: RIR_NAMES[r] }))];

  return (
    <Card
      title="Countries"
      className="mb-6"
      action={data && rows.length > 0 ? (
        <button
          type="button"
          onClick={csv}
          className="px-2 py-1 text-[11px] font-medium rounded-md border border-cf-border text-cf-text-secondary hover:bg-cf-surface-2 hover:text-cf-text"
        >
          Download CSV
        </button>
      ) : null}
    >
      <CardNote>
        Latest values{data ? <> ({nowrap(data.as_of)})</> : ''} for address space registered to each country, IPv{af}.
        {' '}&Delta; 1y is the change of the selected perspective ({lens.label.toLowerCase()}) since {data?.prev_date || 'one year earlier'}.
        Click a country for its history and origin ASes.
      </CardNote>
      <div className="flex flex-wrap items-end gap-3 mb-4">
        <div className="w-full sm:w-64">
          <Input label="Search" type="search" placeholder="Country or code" value={q} onChange={(e) => setQ(e.target.value)} />
        </div>
        <Select label="RIR" value={rir} options={rirOptions} onChange={(e) => setRir(e.target.value)} />
        {data && <span className="text-xs text-cf-text-muted pb-2">{fmtNum(rows.length)} of {fmtNum(base.length)} countries</span>}
      </div>
      {data && minLens && nSmall > 0 && (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 -mt-1 mb-3 text-xs text-cf-text-muted" aria-live="polite">
          <span>
            <strong className="font-semibold text-cf-text-secondary">
              {fmtNum(nSmall)} small {nSmall === 1 ? 'country' : 'countries'} {smallLast ? 'listed last' : 'sorted with the rest'}
            </strong>{' '}
            ({ruleText}).
          </span>
          <button
            type="button"
            aria-pressed={!smallLast}
            onClick={() => setSmallLast((v) => !v)}
            className="px-2 py-1 text-[11px] font-medium rounded-md border border-cf-border text-cf-text-secondary hover:bg-cf-surface-2 hover:text-cf-text"
          >
            {smallLast ? 'Sort them with the rest' : 'List them last'}
          </button>
        </div>
      )}
      {list.error ? (
        <ErrorBox message={errorText(list.error)} />
      ) : !data ? (
        <ChartPlaceholder height={320} />
      ) : rows.length === 0 ? (
        <Empty message="No country matches." />
      ) : (
        <Refetching stale={list.stale}>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
                  {RANK_COLUMNS.map((c) => <RankHeader key={c.key} col={c} sort={sort} onSort={onSort} lensId={lens.id} />)}
                </tr>
              </thead>
              <tbody>
                {shown.map((r, i) => {
                  const to = linkFor(r.cc);
                  const cell = (key) => `px-2.5 py-2 text-right tabular-nums ${RANK_COLUMNS.find((c) => c.key === key)?.lens === lens.id ? 'font-semibold text-cf-text' : ''}`;
                  return (
                    <React.Fragment key={r.cc}>
                      {i === firstSmall && i > 0 && (
                        <tr className="bg-cf-surface" data-small-divider="">
                          <td colSpan={RANK_COLUMNS.length} className="px-2.5 pt-4 pb-1.5 border-b border-cf-border text-xs text-cf-text-muted">
                            {/* inline-block + a viewport cap: stays readable at the left edge while the
                                table scrolls sideways on phones */}
                            <span className="sticky left-2.5 inline-block max-w-[calc(100vw-5rem)]">
                              Small countries ({ruleText}), sorted the same way
                            </span>
                          </td>
                        </tr>
                      )}
                      <tr
                        onClick={() => navigate(to)}
                        className={`border-b border-cf-border cursor-pointer transition-colors ${i % 2 === 1 ? 'bg-cf-surface-2' : 'bg-cf-surface'} hover:bg-cf-orange-light`}
                      >
                        <td className="px-2.5 py-2 sticky left-0 z-[1] bg-inherit">
                          <Link
                            to={to}
                            onClick={(e) => e.stopPropagation()}
                            className="flex items-center gap-2 max-w-[8.5rem] sm:max-w-[13rem] text-cf-text hover:text-cf-orange"
                          >
                            <span aria-hidden="true" className="w-5 shrink-0">{flagEmoji(r.cc)}</span>
                            <span className="min-w-0 line-clamp-2 break-words leading-snug" title={r.name}>{r.name}</span>
                            {r.name !== r.cc && <span className="font-mono text-xs text-cf-text-muted shrink-0">{r.cc}</span>}
                          </Link>
                        </td>
                        <td className="px-2.5 py-2 hidden sm:table-cell"><RirBadge rir={r.registry} /></td>
                        <td className="px-2.5 py-2 text-right tabular-nums">{fmtNum(r.routes)}</td>
                        <td className={cell('valid_pct')}>{fmtPct(r.valid_pct)}</td>
                        <td className="px-2.5 py-2 text-right tabular-nums">{fmtPct(r.invalid_pct)}</td>
                        <td className={cell('space_pct')}>{fmtPct(r.space_pct)}</td>
                        <td className={cell('reg_pct')}>{fmtPct(r.reg_pct)}</td>
                        <td className={cell('ases_pct')}>{fmtPct(r.ases_pct)}</td>
                        <td className={`px-2.5 py-2 text-right tabular-nums ${deltaClass(r.delta)}`}>{fmtPp(r.delta)}</td>
                      </tr>
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
          {rows.length > RANK_INITIAL && (
            <div className="mt-4 pt-4 border-t border-cf-border flex flex-wrap items-center justify-between gap-3 text-sm">
              <span className="text-cf-text-muted">
                Showing {fmtNum(shown.length)} of {fmtNum(rows.length)}
              </span>
              <Button variant="secondary" onClick={() => setShowAll((v) => !v)}>
                {showAll ? `Show top ${RANK_INITIAL}` : `Show all ${fmtNum(rows.length)}`}
              </Button>
            </div>
          )}
        </Refetching>
      )}
    </Card>
  );
}

function RirChartCard({ rir, lens, range, af }) {
  const [view, setView] = useState('chart');
  const data = rir.data;
  const allRows = useMemo(() => {
    if (!data || !data.dates) return [];
    return data.dates.map((d, i) => {
      const row = { t: toTs(d), i };
      RIR_KEYS.forEach((k) => {
        const s = data.series?.[k];
        row[k] = s ? pct(s[lens.num]?.[i], s[lens.den]?.[i]) : null;
      });
      return row;
    });
  }, [data, lens]);
  const rows = useMemo(() => trimLeadingEmpty(applyRange(allRows, range, data?.as_of), RIR_KEYS), [allRows, range, data]);
  const series = RIR_SERIES.filter((s) => data?.series?.[s.key]);

  function csv() {
    const header = ['date'];
    series.forEach((s) => header.push(`${s.key}_pct`, `${s.key}_${lens.num}`, `${s.key}_${lens.den}`));
    downloadCsv(
      `rir-roa-${lens.id}-ipv${af}-${data.as_of}.csv`,
      header,
      rows.map((r) => {
        const out = [fmtDay(r.t)];
        series.forEach((s) => out.push(round(r[s.key]), data.series[s.key][lens.num]?.[r.i], data.series[s.key][lens.den]?.[r.i]));
        return out;
      }),
    );
  }

  return (
    <Card
      title="ROA deployment by RIR"
      className="mb-6"
      action={rows.length > 0 ? <ChartActions view={view} onView={setView} onCsv={csv} /> : null}
    >
      <CardNote>
        {lens.label}: {lens.what} &middot; IPv{af} &middot; by the RIR that registered the address space
        {data ? <>, as of {nowrap(data.as_of)}</> : ''}.{bgpNote('single', lens, range, rows)}
      </CardNote>
      {rir.error ? (
        <ErrorBox message={errorText(rir.error)} />
      ) : !data ? (
        <ChartPlaceholder height={330} />
      ) : rows.length === 0 ? (
        <ChartPlaceholder height={200} message="No data for this perspective and range." />
      ) : (
        <Refetching stale={rir.stale}>
          {view === 'chart' ? (
            <TimeLineChart
              rows={rows}
              series={series}
              height={300}
              ariaLabel={`${lens.label} deployment by RIR over time, IPv${af}`}
            />
          ) : (
            <ChartTable rows={rows} series={series} granularity={range === '1y' ? 'month' : 'year'} />
          )}
        </Refetching>
      )}
    </Card>
  );
}

function Overview({ af, lens, range, update, linkFor }) {
  const rir = useCountryApi('rir-trend', { af });
  const list = useCountryApi('countries', { af });
  const asOf = list.data?.as_of || rir.data?.as_of;
  return (
    <Page
      title="ROA Deployment by Country"
      subtitle={<>RPKI ROA deployment per RIR and per registered country{asOf ? <> &middot; data as of {nowrap(asOf)}</> : null}</>}
    >
      <FilterRow range={range} lens={lens} af={af} update={update} />
      <RirChartCard rir={rir} lens={lens} range={range} af={af} />
      <RankingCard list={list} lens={lens} af={af} linkFor={linkFor} />
      <Methodology />
    </Page>
  );
}

/* ── Country view ── */
function KpiTile({ lens, stats, selected, onSelect, af, ctx, asOf }) {
  const loading = stats === undefined;
  const v = stats?.value;
  // The latest point of a perspective can be older than the build: the country's whole series
  // ends early (EU IPv6 ends 2022-11-30: no later data at all), or the series goes on to as_of but
  // this perspective has nothing to measure there (Tuvalu IPv6: 0 visible routes since 2026-09-22).
  // Show it with its own date and the reason, without a 1-year change, never as a current value.
  const stale = !!stats && !!asOf && stats.date < asOf;
  const noneNow = stale && stats.end === asOf;
  const why = noneNow ? (lens.bgp ? 'no visible routes now' : 'no registered space now') : 'no later data';
  let detail = null;
  if (stats) {
    if (lens.unit === 'routes') detail = `${fmtNum(stats.num)} of ${fmtNum(stats.den)} ${stats.den === 1 ? 'route' : 'routes'}`;
    else if (lens.unit === 'ases') detail = `${fmtNum(stats.num)} of ${fmtNum(stats.den)} ${stats.den === 1 ? 'AS' : 'ASes'}`;
    else detail = `${fmtCompact(stats.num)} of ${fmtCompact(stats.den)} ${af === 6 ? '/48s' : 'addresses'}`;
  }
  const d = stale ? null : stats?.delta;
  return (
    <button
      type="button"
      aria-pressed={selected}
      onClick={onSelect}
      title={!stale ? undefined : noneNow
        ? `Latest value: ${stats.date}. On ${asOf} there ${lens.bgp ? 'is no visible route' : 'is no registered address space'}, so this is not a current value and has no 1-year change.`
        : `Latest data: ${stats.date}. There is none on ${asOf}, so this is not a current value and has no 1-year change.`}
      className={`min-w-0 text-left bg-cf-surface rounded-xl p-4 lg:p-5 shadow-[0_2px_8px_rgba(0,0,0,0.06)] border-2 transition-colors ${
        selected ? 'border-cf-orange/60' : 'border-transparent hover:border-cf-border'
      }`}
    >
      <div className="flex items-start gap-1.5 min-h-[2.5em] text-[10px] lg:text-xs leading-tight text-cf-text-muted uppercase tracking-wide font-medium">
        <span className="h-[1.25em] flex items-center shrink-0"><SeriesKey color={lens.color} /></span>
        <span className="min-w-0 break-words">{lens.label}</span>
      </div>
      <div className={`text-xl lg:text-3xl font-bold mt-1 ${stale ? 'text-cf-text-muted' : 'text-cf-text'}`}>{fmtPct(v)}</div>
      <div className={`text-xs mt-1 font-medium ${stale ? 'text-amber-800' : deltaClass(d)}`}>
        {loading || stats === null ? '\u00a0' : stale ? (
          <>as of {nowrap(stats.date)}<span className="text-cf-text-muted font-normal"> &middot; {nowrap(why)}</span></>
        ) : d == null ? <span className="text-cf-text-muted font-normal">No 1-year comparison</span> : (
          <>
            {Math.abs(d) >= 0.05 && <span aria-hidden="true">{d > 0 ? '▲ ' : '▼ '}</span>}
            {fmtPp(d)} pp
            <span className="text-cf-text-muted font-normal"> vs {nowrap(stats.prevDate)}</span>
          </>
        )}
      </div>
      <div className="text-xs text-cf-text-secondary mt-1 break-words">{detail || (stats === null ? (lens.bgp ? 'No visible routes' : 'No registered address space') : '\u00a0')}</div>
      <div className="text-[11px] text-cf-text-muted mt-1 break-words">{ctx || '\u00a0'}</div>
    </button>
  );
}

function DeploymentCard({ cc, t, lens, range, af, stale }) {
  const [view, setView] = useState('chart');
  const allRows = useMemo(() => {
    if (!t || !t.dates) return [];
    const m = t.metrics || {};
    return t.dates.map((d, i) => {
      const row = { t: toTs(d), i };
      LENSES.forEach((l) => { row[l.id] = pct(m[l.num]?.[i], m[l.den]?.[i]); });
      return row;
    });
  }, [t]);
  const rows = useMemo(() => trimLeadingEmpty(applyRange(allRows, range, t?.as_of), LENS_KEYS), [allRows, range, t]);
  const yAxis = useMemo(() => pctAxis(rows, LENS_KEYS), [rows]);
  const series = LENSES.map((l) => ({ key: l.id, name: l.label, color: l.color, width: l.id === lens.id ? 2.5 : 1.5 }));

  function csv() {
    const m = t.metrics || {};
    const header = ['date'];
    LENSES.forEach((l) => header.push(`${l.id}_pct`, l.num, l.den));
    downloadCsv(
      `${cc}-roa-deployment-ipv${af}-${t.as_of}.csv`,
      header,
      rows.map((r) => {
        const out = [fmtDay(r.t)];
        LENSES.forEach((l) => out.push(round(r[l.id]), m[l.num]?.[r.i], m[l.den]?.[r.i]));
        return out;
      }),
    );
  }

  return (
    <Card
      title="ROA deployment over time"
      className="mb-6"
      action={rows.length > 0 ? <ChartActions view={view} onView={setView} onCsv={csv} /> : null}
    >
      <CardNote>
        Deployment share under each perspective, IPv{af}; the selected perspective is drawn thicker.
        {bgpNote('mixed', lens, range, rows)}
        {view === 'chart' && yAxis.top < 100 && rows.length > 0
          ? <> The y axis runs from 0 to {yAxis.top}% because every value is low.</>
          : null}
      </CardNote>
      {!t ? (
        <ChartPlaceholder height={330} />
      ) : rows.length === 0 ? (
        <ChartPlaceholder height={200} message="No data for this range." />
      ) : (
        <Refetching stale={stale}>
          {view === 'chart' ? (
            <TimeLineChart
              rows={rows}
              series={series}
              height={300}
              endLabelKey={lens.id}
              yAxis={yAxis}
              ariaLabel={`ROA deployment in ${cc} over time under four perspectives, IPv${af}`}
            />
          ) : (
            <ChartTable rows={rows} series={series} granularity={range === '1y' ? 'month' : 'year'} />
          )}
        </Refetching>
      )}
    </Card>
  );
}

function BreakdownCard({ cc, t, lens, range, af, stale }) {
  const [view, setView] = useState('chart');
  const bd = BREAKDOWN[lens.id];
  const keys = useMemo(() => bd.bands.map((b) => b.key), [bd]);
  const allRows = useMemo(() => {
    if (!t || !t.dates) return [];
    const m = t.metrics || {};
    return t.dates.map((d, i) => {
      const den = m[bd.den]?.[i];
      const row = { t: toTs(d), i, den };
      bd.bands.forEach((b) => { row[b.key] = den > 0 ? pct(b.value(m, i), den) : null; });
      return row;
    });
  }, [t, bd]);
  const rows = useMemo(() => trimLeadingEmpty(applyRange(allRows, range, t?.as_of), keys), [allRows, range, t, keys]);
  const series = bd.bands.map((b) => ({ key: b.key, name: b.name, color: b.color }));
  const denLabel = lens.unit === 'routes' ? 'routes' : lens.unit === 'ases' ? 'origin ASes'
    : lens.id === 'registered' ? `registered ${af === 6 ? '/48s' : 'addresses'}` : `routed ${af === 6 ? '/48s' : 'addresses'}`;

  function csv() {
    const m = t.metrics || {};
    downloadCsv(
      `${cc}-${lens.id}-breakdown-ipv${af}-${t.as_of}.csv`,
      ['date', ...bd.bands.map((b) => `${b.key}_pct`), ...bd.bands.map((b) => b.key), bd.den],
      rows.map((r) => [fmtDay(r.t), ...bd.bands.map((b) => round(r[b.key])), ...bd.bands.map((b) => b.value(m, r.i)), r.den]),
    );
  }

  return (
    <Card
      title={`Validity breakdown — ${lens.label}`}
      className="mb-6"
      action={rows.length > 0 ? <ChartActions view={view} onView={setView} onCsv={csv} /> : null}
    >
      <CardNote>
        Share of all {denLabel} in {cc}, IPv{af} (100% = all {denLabel}).{bgpNote('single', lens, range, rows)}
      </CardNote>
      {!t ? (
        <ChartPlaceholder height={290} />
      ) : rows.length === 0 ? (
        <ChartPlaceholder height={200} message="No data for this perspective and range." />
      ) : (
        <Refetching stale={stale}>
          {view === 'chart' ? (
            <StackedPctChart
              rows={rows}
              series={series}
              height={260}
              ariaLabel={`${lens.label} validity breakdown in ${cc} over time, IPv${af}`}
              tooltipExtra={(r) => (r.den != null ? (
                <div className="mt-1 text-xs text-cf-text-muted">
                  of {lens.unit === 'space' ? fmtCompact(r.den) : fmtNum(r.den)} {denLabel}
                </div>
              ) : null)}
            />
          ) : (
            <ChartTable rows={rows} series={series} granularity={range === '1y' ? 'month' : 'year'} />
          )}
        </Refetching>
      )}
    </Card>
  );
}

function CountryView({ cc, af, lens, range, update, linkFor }) {
  const valid = /^[A-Z]{2}$/.test(cc) && cc !== 'ZZ';
  const trend = useCountryApi('trend', { cc, af }, valid);
  const rir = useCountryApi('rir-trend', { af });
  const list = useCountryApi('countries', { af });
  const t = trend.data;
  const listRow = list.data?.rows?.find((r) => r.cc === cc);
  const registry = (t && t.cc === cc ? t.registry : null) || listRow?.registry || null;
  // An unknown or malformed code (400/404) gets only the header, the picker and the message. Any
  // other failure (503, 502, network) keeps the filter row so the reader can switch back.
  const failed = !!trend.error;
  const notFound = !valid || [400, 404].includes(errorStatus(trend.error));
  const name = countryName(cc);
  const flag = notFound ? '' : flagEmoji(cc);
  const asOf = t?.as_of || list.data?.as_of;

  const stats = useMemo(() => Object.fromEntries(LENSES.map((l) => [
    l.id, t ? lensLatest(t.metrics, t.dates, l, t.as_of) : undefined,
  ])), [t]);
  // Context: the country's RIR and All RIRs (the five RIRs) on the tile's own date, so a tile
  // whose data ends early is compared with the same day, not with today's RIR values.
  const context = useMemo(() => {
    const out = {};
    const r = rir.data;
    if (!r || !r.series || !r.dates) return out;
    const index = new Map(r.dates.map((d, i) => [d, i]));
    LENSES.forEach((l) => {
      const i = index.get(stats[l.id]?.date || r.as_of);
      const at = (s) => (s && i != null ? pct(s[l.num]?.[i], s[l.den]?.[i]) : null);
      const parts = [];
      if (registry && r.series[registry]) parts.push(`${nbsp(RIR_NAMES[registry] || registry)}\u00a0${fmtPct(at(r.series[registry]))}`);
      if (r.series.all) parts.push(`${nbsp(RIR_NAMES.all)}\u00a0${fmtPct(at(r.series.all))}`);
      out[l.id] = parts.join(' · ');
    });
    return out;
  }, [rir.data, registry, stats]);
  const lastDate = t?.dates?.length ? t.dates[t.dates.length - 1] : null;
  const endsEarly = !!(t && t.cc === cc && lastDate && t.as_of && lastDate < t.as_of);

  const header = (
    <div className="page-enter">
      <Link to={linkFor(null)} className="text-xs font-medium text-cf-orange hover:underline">&larr; All countries</Link>
      <div className="mt-2 mb-6 flex flex-wrap items-end justify-between gap-4">
        <div className="min-w-0">
          {/* Inline flow (not flex) so the flag stays on the first line of a long, wrapping name. */}
          <h1 className="text-[22px] leading-snug font-semibold tracking-tight text-cf-orange break-words">
            {flag && <span aria-hidden="true" className="mr-2">{flag}</span>}
            {name}
            {name !== cc && <span className="ml-2 font-mono text-base font-medium text-cf-text-muted whitespace-nowrap">{cc}</span>}
            {registry && <span className="ml-2 inline-block align-[3px]"><RirBadge rir={registry} /></span>}
          </h1>
          {!notFound && (
            <p className="text-sm text-cf-text-secondary mt-1">
              ROA deployment for address space registered to {name}{asOf ? <> &middot; data as of {nowrap(asOf)}</> : ''}
            </p>
          )}
        </div>
        <CountryPicker countries={list.data?.rows} value={notFound ? '' : cc} onPick={(next) => update({ cc: next })} />
      </div>
    </div>
  );

  if (notFound || failed) {
    return (
      <div>
        {header}
        {!notFound && <FilterRow range={range} lens={lens} af={af} update={update} />}
        <ErrorBox message={!valid ? `“${cc}” is not a country code. Pick a country from the list.` : errorText(trend.error)} />
      </div>
    );
  }

  return (
    <div>
      {header}
      <div className="page-enter">
        <FilterRow range={range} lens={lens} af={af} update={update} />
        <Refetching stale={trend.stale}>
          {endsEarly && (
            <p className="mb-4 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
              No IPv{t.af ?? af} data for {name} after {nowrap(lastDate)} (no registered address space, routes or VRPs),
              so the values below are from that date, not from {nowrap(t.as_of)}.
            </p>
          )}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 lg:gap-4 mb-6">
            {LENSES.map((l) => (
              <KpiTile
                key={l.id}
                lens={l}
                af={Number(t?.af ?? af)}
                stats={t ? stats[l.id] : undefined}
                asOf={t?.as_of}
                selected={l.id === lens.id}
                onSelect={() => update({ lens: l.id })}
                ctx={context[l.id]}
              />
            ))}
          </div>
        </Refetching>
        <DeploymentCard cc={cc} t={t} lens={lens} range={range} af={af} stale={trend.stale} />
        <BreakdownCard cc={cc} t={t} lens={lens} range={range} af={af} stale={trend.stale} />
        <Card title={`Origin ASes announcing ${cc} address space`}>
          <CardNote>
            Origin ASes of visible routes whose prefixes are registered to {name}, IPv{af}
            {asOf ? <>, as of {nowrap(asOf)}</> : ''}. &Delta; 1y is the change of the AS&rsquo;s valid share of
            {lens.id === 'space' ? ' routed address space' : ' routes'} since {list.data?.prev_date || 'one year earlier'}, in
            percentage points. Click a row for its history and links to its ROAs.
          </CardNote>
          <CardNote>
            Each row counts only the AS&rsquo;s visible routes in address space registered to {name} on{' '}
            {asOf ? nowrap(asOf) : 'the as-of day'}, so the shares differ from the ASN Coverage page, which counts all of
            the AS&rsquo;s prefixes seen by three collectors over seven days.
          </CardNote>
          <CountryAsTable cc={cc} af={af} lens={lens.id} />
        </Card>
        <Methodology />
      </div>
    </div>
  );
}

export default function Countries() {
  const [params, setParams] = useSearchParams();
  const cc = (params.get('cc') || '').trim().toUpperCase();
  const af = params.get('af') === '6' ? 6 : 4;
  const lens = LENS_BY_ID[params.get('lens')] || LENS_BY_ID[DEFAULTS.lens];
  const range = RANGES.some((r) => r.value === params.get('range')) ? params.get('range') : DEFAULTS.range;

  // Country changes push a history entry (Back works); filter changes replace it.
  const update = useCallback((patch) => {
    const push = Object.prototype.hasOwnProperty.call(patch, 'cc');
    setParams((prev) => {
      const next = new URLSearchParams(prev);
      Object.entries(patch).forEach(([k, v]) => {
        if (v == null || v === '' || String(v) === DEFAULTS[k]) next.delete(k);
        else next.set(k, String(v));
      });
      return next;
    }, { replace: !push });
  }, [setParams]);

  const linkFor = useCallback((code) => {
    const q = new URLSearchParams();
    if (code) q.set('cc', code);
    if (af !== 4) q.set('af', String(af));
    if (lens.id !== DEFAULTS.lens) q.set('lens', lens.id);
    if (range !== DEFAULTS.range) q.set('range', range);
    const s = q.toString();
    return `/analysis/countries${s ? `?${s}` : ''}`;
  }, [af, lens.id, range]);

  // A new country (or back to the list) starts at the top of the page.
  useEffect(() => { window.scrollTo(0, 0); }, [cc]);

  return cc
    ? <CountryView cc={cc} af={af} lens={lens} range={range} update={update} linkFor={linkFor} />
    : <Overview af={af} lens={lens} range={range} update={update} linkFor={linkFor} />;
}
