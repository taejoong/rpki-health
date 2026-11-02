import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { Input, Button, ErrorBox, Empty, fmtNum } from './ui';
import {
  TimeLineChart, ChartTable, ChartActions, ChartPlaceholder, downloadCsv,
  fmtPct, fmtPp, fmtCompact, pct, toTs, fmtDay, round, useElementWidth, pctAxis,
} from './TimeCharts';
import { useCountryApi, errorText, countryName, BGP_FROM, LENS_BY_ID } from './countryShared';

/* Origin ASes announcing a country's address space: server-side sort / paging / search via
   /analytics/country/ases; a row click expands the AS's monthly history from /as-trend. */

const PAGE_SIZE = 100;

const COLUMNS = [
  { key: 'asn', label: 'AS', sort: 'asn', dir: 'asc', align: 'left' },
  { key: 'as_cc', label: 'Reg.', sort: 'as_cc', dir: 'asc', align: 'left', title: 'Registration country of the AS (highlighted when it differs from the address space)' },
  { key: 'routes', label: 'Routes', sort: 'routes', align: 'right', title: 'Visible routes (prefix, origin) in this country’s address space' },
  { key: 'routes_valid', label: 'Valid', sort: 'routes_valid', align: 'right' },
  { key: 'invalid', label: 'Invalid', align: 'right', title: 'Invalid routes: wrong origin AS plus prefix longer than maxLength' },
  { key: 'routes_nf', label: 'Not\u200bFound', sort: 'routes_nf', align: 'right' },
  { key: 'pct_valid', label: 'Valid %', sort: 'pct_valid', align: 'right' },
  { key: 'space', label: 'Address space', sort: 'space', align: 'right' },
  { key: 'pct_space_valid', label: 'Space valid %', sort: 'pct_space_valid', align: 'right' },
  { key: 'delta', label: 'Δ 1y (pp)', align: 'right' },
  { key: 'first_valid', label: 'ROA since', sort: 'first_valid', dir: 'asc', align: 'right', title: 'First grid date with a valid route (BGP data starts 2020-01-01)' },
];

const MINI_SERIES = [
  { key: 'routes', name: 'Routes valid', color: LENS_BY_ID.routes.color },
  { key: 'space', name: 'Address space valid', color: LENS_BY_ID.space.color },
];

const diffPct = (a, b, c, d) => {
  const now = pct(a, b);
  const then = pct(c, d);
  return now == null || then == null ? null : now - then;
};

/* Header text: the label (upper case, like every table here) plus an optional plain-case unit line. */
function HeaderLabel({ col }) {
  return (
    <span>
      {col.label}
      {col.sub && <span className="block normal-case tracking-normal font-normal text-[10px] text-cf-text-muted">{col.sub}</span>}
    </span>
  );
}

function SortHeader({ col, sort, onSort }) {
  const right = col.align === 'right';
  const cls = `px-2 py-2.5 font-medium select-none align-bottom leading-tight ${right ? 'text-right' : 'text-left'} ${col.key === 'asn' ? 'sticky left-0 z-[2] bg-cf-surface-2' : ''}`;
  if (!col.sort) return <th className={cls} title={col.title} scope="col"><HeaderLabel col={col} /></th>;
  const active = sort.key === col.sort;
  return (
    <th className={cls} title={col.title} scope="col" aria-sort={active ? (sort.dir === 'asc' ? 'ascending' : 'descending') : 'none'}>
      <button
        type="button"
        onClick={() => onSort(col)}
        className={`inline-flex items-end gap-1 uppercase tracking-wider leading-tight hover:text-cf-text ${right ? 'flex-row-reverse text-right' : 'text-left'}`}
      >
        <HeaderLabel col={col} />
        <span className="inline-flex flex-col leading-none mb-0.5 shrink-0" aria-hidden="true">
          <span className={`text-[8px] ${active && sort.dir === 'asc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'▲'}</span>
          <span className={`text-[8px] ${active && sort.dir === 'desc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'▼'}</span>
        </span>
      </button>
    </th>
  );
}

function AsTrendPanel({ cc, af, row }) {
  const res = useCountryApi('as-trend', { cc, af, asn: row.asn });
  const [view, setView] = useState('chart');
  const d = res.data && String(res.data.asn) === String(row.asn) ? res.data : null;
  const rows = useMemo(() => {
    if (!d) return [];
    const m = d.metrics || {};
    return (d.dates || []).map((day, i) => ({
      t: toTs(day),
      i,
      routes: pct(m.routes_valid?.[i], m.routes?.[i]),
      space: pct(m.space_valid?.[i], m.space?.[i]),
      n: m.routes?.[i],
    }));
  }, [d]);
  const yAxis = useMemo(() => pctAxis(rows, MINI_SERIES.map((s) => s.key)), [rows]);

  function csv() {
    const m = d.metrics || {};
    const keys = ['routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len', 'routes_nf', 'space', 'space_valid'];
    downloadCsv(
      `AS${row.asn}-${cc}-ipv${af}-${d.as_of}.csv`,
      ['date', ...keys, 'routes_valid_pct', 'space_valid_pct'],
      rows.map((r) => [fmtDay(r.t), ...keys.map((k) => m[k]?.[r.i]), round(r.routes), round(r.space)]),
    );
  }

  return (
    <div className="px-4 py-4">
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-2 mb-3">
        <div className="min-w-0 text-xs text-cf-text-secondary">
          <div className="text-[13px] font-semibold text-cf-text break-words">
            AS{row.asn}{row.name ? <span className="font-normal text-cf-text-secondary"> &middot; {row.name}</span> : null}
          </div>
          <div className="mt-0.5">
            Valid share of this AS&rsquo;s routes and address space in {cc}, monthly since 2020.
            {view === 'chart' && d && rows.length > 0 && yAxis.top < 100 ? <> The y axis runs to {yAxis.top}%.</> : null}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <Link to={`/roa/by-asn?asn=${row.asn}`} className="text-xs font-medium text-cf-orange hover:underline">
            ROAs (By ASN) &rarr;
          </Link>
          <Link
            to={`/roa/coverage?asn=${row.asn}`}
            title="ASN Coverage counts all of this AS's prefixes seen by three collectors over seven days, so its shares differ from this table"
            className="text-xs font-medium text-cf-orange hover:underline"
          >
            ASN Coverage &rarr;
          </Link>
          {d && rows.length > 0 && <ChartActions view={view} onView={setView} onCsv={csv} />}
        </div>
      </div>
      {res.error ? (
        <ErrorBox message={errorText(res.error)} />
      ) : !d ? (
        <ChartPlaceholder height={210} />
      ) : rows.length === 0 ? (
        <ChartPlaceholder height={120} message="No history for this AS." />
      ) : view === 'chart' ? (
        <TimeLineChart
          rows={rows}
          series={MINI_SERIES}
          height={180}
          toggleable={false}
          yAxis={yAxis}
          ariaLabel={`Valid share of AS${row.asn} routes and address space in ${cc} since 2020`}
          tooltipExtra={(r) => (r.n != null ? <div className="mt-1 text-xs text-cf-text-muted">{fmtNum(r.n)} routes</div> : null)}
        />
      ) : (
        <ChartTable rows={rows} series={MINI_SERIES} />
      )}
    </div>
  );
}

export default function CountryAsTable({ cc, af, lens }) {
  const [sort, setSort] = useState({ key: 'routes', dir: 'desc' });
  const [qInput, setQInput] = useState('');
  const [q, setQ] = useState('');
  // A search belongs to one country: picking another country clears it (sort order is kept).
  const [qCc, setQCc] = useState(cc);
  if (qCc !== cc) {
    setQCc(cc);
    setQInput('');
    setQ('');
  }
  useEffect(() => {
    const id = setTimeout(() => setQ(qInput.trim()), 300);
    return () => clearTimeout(id);
  }, [qInput]);

  // Paging and the expanded row belong to one query; a new query starts at offset 0.
  const base = `${cc}|${af}|${q}|${sort.key}|${sort.dir}`;
  const [page, setPage] = useState({ base, offset: 0 });
  const offset = page.base === base ? page.offset : 0;
  const pageKey = `${base}|${offset}`;
  const [expanded, setExpanded] = useState({ pageKey: null, asn: null });
  const openAsn = expanded.pageKey === pageKey ? expanded.asn : null;

  const res = useCountryApi('ases', { cc, af, sort: sort.key, order: sort.dir, limit: PAGE_SIZE, offset, q });
  const data = res.data;
  const [scrollRef, boxWidth] = useElementWidth();

  const deltaSpace = lens === 'space';
  const dataAf = Number(data?.af ?? af); // units follow the rows on screen (stale rows during a refetch)
  const unit = dataAf === 6 ? '/48s' : 'addresses';

  function onSort(col) {
    setSort((s) => (s.key === col.sort
      ? { key: s.key, dir: s.dir === 'asc' ? 'desc' : 'asc' }
      : { key: col.sort, dir: col.dir || 'desc' }));
  }
  function toggle(asn) {
    setExpanded((e) => (e.pageKey === pageKey && e.asn === asn ? { pageKey, asn: null } : { pageKey, asn }));
  }

  const rows = data?.rows || [];
  const total = data?.total ?? 0;
  const sortLabel = COLUMNS.find((c) => c.sort === sort.key)?.label || sort.key;
  const headerCol = (c) => {
    if (c.key === 'space') {
      return { ...c, sub: unit, title: `Routed address space in ${unit}` };
    }
    if (c.key === 'delta') {
      return {
        ...c,
        sub: deltaSpace ? 'space' : 'routes',
        title: `Change in ${deltaSpace ? 'space valid %' : 'valid % of routes'} since ${data?.prev_date || 'one year earlier'} (percentage points)`,
      };
    }
    return c;
  };

  return (
    <div>
      <div className="flex flex-wrap items-end gap-3 mb-4">
        <div className="w-full sm:w-64">
          <Input
            label="Search"
            type="search"
            placeholder="ASN (e.g. AS4766) or name"
            maxLength={100}
            value={qInput}
            onChange={(e) => setQInput(e.target.value)}
          />
        </div>
        <div className="text-xs text-cf-text-muted pb-2">
          {data ? (
            <>
              <strong className="text-cf-text">{fmtNum(total)}</strong> origin AS{total === 1 ? '' : 'es'}
              {q ? <> matching &ldquo;{q}&rdquo;</> : null} &middot; sorted by {sortLabel} {sort.dir === 'asc' ? '↑' : '↓'}
            </>
          ) : null}
        </div>
      </div>

      {res.error ? (
        <ErrorBox message={errorText(res.error)} />
      ) : !data ? (
        <ChartPlaceholder height={240} />
      ) : rows.length === 0 ? (
        <Empty message={q ? `No origin AS matches “${q}”.` : 'No origin AS announces address space registered to this country.'} />
      ) : (
        <div className={`transition-opacity duration-150 ${res.stale ? 'opacity-50' : ''}`} aria-busy={res.loading}>
          <div ref={scrollRef} className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
                  {COLUMNS.map((c) => (
                    <SortHeader key={c.key} col={headerCol(c)} sort={sort} onSort={onSort} />
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => {
                  const isOpen = openAsn === r.asn;
                  const invalid = r.routes_inv_as == null && r.routes_inv_len == null ? null : (r.routes_inv_as || 0) + (r.routes_inv_len || 0);
                  const delta = deltaSpace
                    ? diffPct(r.space_valid, r.space, r.prev_space_valid, r.prev_space)
                    : diffPct(r.routes_valid, r.routes, r.prev_routes_valid, r.prev_routes);
                  const foreign = r.as_cc && r.as_cc !== cc;
                  return (
                    <React.Fragment key={r.asn}>
                      <tr
                        className={`border-b border-cf-border cursor-pointer transition-colors ${isOpen ? 'bg-cf-orange-light' : i % 2 === 1 ? 'bg-cf-surface-2' : 'bg-cf-surface'} hover:bg-cf-orange-light`}
                        onClick={() => toggle(r.asn)}
                        onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(r.asn); } }}
                        tabIndex={0}
                        aria-expanded={isOpen}
                      >
                        <td className="px-2 py-2 sticky left-0 z-[1] bg-inherit">
                          <div className="flex items-start gap-2 w-[130px] sm:w-[180px]">
                            <span aria-hidden="true" className={`text-[9px] mt-1 text-cf-text-muted transition-transform ${isOpen ? 'rotate-90' : ''}`}>&#9654;</span>
                            <div className="min-w-0">
                              <div className="font-mono text-[13px] text-cf-text">AS{r.asn}</div>
                              {r.name && <div className="text-xs text-cf-text-secondary truncate" title={r.name}>{r.name}</div>}
                            </div>
                          </div>
                        </td>
                        <td className="px-2 py-2">
                          {r.as_cc == null ? (
                            <span className="text-cf-text-muted">&mdash;</span>
                          ) : foreign ? (
                            <span title={`AS registered in ${countryName(r.as_cc)}`} className="inline-block px-1.5 py-0.5 rounded bg-amber-50 text-amber-800 font-mono text-xs font-semibold">{r.as_cc}</span>
                          ) : (
                            <span className="font-mono text-xs text-cf-text-secondary">{r.as_cc}</span>
                          )}
                        </td>
                        <td className="px-2 py-2 text-right tabular-nums">{fmtNum(r.routes)}</td>
                        <td className="px-2 py-2 text-right tabular-nums">{fmtNum(r.routes_valid)}</td>
                        <td
                          className="px-2 py-2 text-right tabular-nums"
                          title={invalid ? `${fmtNum(r.routes_inv_as)} wrong origin AS, ${fmtNum(r.routes_inv_len)} too specific` : undefined}
                        >
                          {invalid ? <span className="text-red-700">{fmtNum(invalid)}</span> : fmtNum(invalid)}
                        </td>
                        <td className="px-2 py-2 text-right tabular-nums">{fmtNum(r.routes_nf)}</td>
                        <td className="px-2 py-2 text-right tabular-nums font-medium">{fmtPct(pct(r.routes_valid, r.routes))}</td>
                        <td className="px-2 py-2 text-right tabular-nums" title={r.space != null ? `${fmtNum(r.space)} ${unit}` : undefined}>{fmtCompact(r.space)}</td>
                        <td className="px-2 py-2 text-right tabular-nums">{fmtPct(pct(r.space_valid, r.space))}</td>
                        <td className={`px-2 py-2 text-right tabular-nums ${delta > 0.05 ? 'text-emerald-700' : delta < -0.05 ? 'text-red-700' : 'text-cf-text-muted'}`}>
                          {fmtPp(delta)}
                        </td>
                        <td className="px-2 py-2 text-right tabular-nums text-cf-text-secondary whitespace-nowrap">
                          {r.first_valid == null ? '—' : r.first_valid === BGP_FROM ? '≤ 2020-01' : r.first_valid}
                          {r.first_valid != null && r.routes_valid === 0 && (
                            <span
                              className="block text-[10px] leading-tight text-cf-text-muted"
                              title={`First valid route on ${r.first_valid}; no valid route on ${data?.as_of || 'the latest date'}`}
                            >
                              none valid now
                            </span>
                          )}
                        </td>
                      </tr>
                      {isOpen && (
                        <tr className="bg-cf-surface-2">
                          <td colSpan={COLUMNS.length} className="p-0 border-b border-cf-border">
                            <div className="sticky left-0" style={boxWidth ? { width: boxWidth } : undefined}>
                              <AsTrendPanel cc={cc} af={af} row={r} />
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
          {total > PAGE_SIZE && (
            <div className="flex flex-wrap items-center justify-between gap-3 mt-4 pt-4 border-t border-cf-border text-sm">
              <span className="text-cf-text-muted">
                Showing {fmtNum(offset + 1)}&ndash;{fmtNum(Math.min(offset + PAGE_SIZE, total))} of {fmtNum(total)}
              </span>
              <div className="flex gap-2">
                <Button variant="secondary" disabled={offset === 0 || res.loading} onClick={() => setPage({ base, offset: Math.max(0, offset - PAGE_SIZE) })}>
                  Previous
                </Button>
                <Button variant="secondary" disabled={offset + PAGE_SIZE >= total || res.loading} onClick={() => setPage({ base, offset: offset + PAGE_SIZE })}>
                  Next
                </Button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
