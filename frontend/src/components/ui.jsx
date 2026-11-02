import { useEffect, useState, useCallback } from 'react';

/* ── Constants ── */
export const RIRS = ['afrinic', 'apnic', 'arin', 'lacnic', 'ripencc'];

export const RIR_COLORS = {
  afrinic: '#E5751F',
  apnic:   '#2563eb',
  arin:    '#861F41',
  lacnic:  '#7c3aed',
  ripencc: '#0891b2',
};

export const CHART_COLORS = ['#861F41', '#E5751F', '#2563eb', '#16a34a', '#7c3aed', '#0891b2', '#d97706', '#db2777'];

/* ── Formatting helpers ── */
export function fmtNum(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString();
}

export function fmtDate(d) {
  if (!d) return '—';
  return d.replace('T', ' ').slice(0, 19);
}

export function fmtDuration(hours) {
  if (hours == null) return '—';
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  if (hours < 24) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

export function timeAgo(dateStr) {
  if (!dateStr) return '';
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

/* ── useFetch hook ── */
export function useFetch(fn, deps = []) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  const refetch = useCallback(() => {
    setLoading(true);
    setError(null);
    fn()
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, deps);

  useEffect(() => { refetch(); }, [refetch]);

  return { data, error, loading, refetch };
}

/* ── Loading ── */
export function Loading() {
  return (
    <div className="flex items-center justify-center h-64">
      <div className="spinner" />
    </div>
  );
}

/* ── Error ── */
export function ErrorBox({ message }) {
  return (
    <div className="bg-red-50 border border-red-200 rounded-xl p-6 text-red-700 text-sm">
      <span className="font-semibold text-red-800">Error:</span> {message}
    </div>
  );
}

/* ── Empty state ── */
export function Empty({ message = 'No data available.' }) {
  return (
    <div className="bg-cf-surface-2 border border-cf-border rounded-xl p-10 text-center text-cf-text-muted text-sm">
      {message}
    </div>
  );
}

/* ── Page wrapper ── */
export function Page({ title, subtitle, action, children }) {
  return (
    <div className="page-enter">
      <div className="mb-6 flex items-start justify-between">
        <div>
          <h1 className="text-[22px] font-semibold tracking-tight text-cf-orange">{title}</h1>
          {subtitle && <p className="text-sm text-cf-text-secondary mt-1">{subtitle}</p>}
        </div>
        {action && <div>{action}</div>}
      </div>
      {children}
    </div>
  );
}

/* ── Stat card ── */
export function Stat({ label, value, sub, color = 'text-cf-text' }) {
  return (
    <div className="bg-cf-surface rounded-xl p-4 lg:p-5 shadow-[0_2px_8px_rgba(0,0,0,0.06)]">
      <div className={`text-xl lg:text-3xl font-bold ${color} break-all`}>{value}</div>
      <div className="text-[10px] lg:text-xs text-cf-text-muted mt-1 uppercase tracking-wide font-medium">{label}</div>
      {sub && <div className="text-xs text-cf-text-secondary mt-2">{sub}</div>}
    </div>
  );
}

/* ── Card ── */
export function Card({ title, action, className = '', children }) {
  return (
    <div className={`bg-cf-surface rounded-xl shadow-[0_2px_8px_rgba(0,0,0,0.06)] card-enter ${className}`}>
      {title && (
        <div className="px-5 py-3.5 border-b border-cf-border/60 text-[13px] font-semibold tracking-tight text-cf-text flex items-center justify-between">
          <span>{title}</span>
          {action && <div>{action}</div>}
        </div>
      )}
      <div className="p-5">{children}</div>
    </div>
  );
}

/* ── Badge ── */
const badgeStyles = {
  green:  'bg-emerald-50 text-emerald-700',
  red:    'bg-red-50 text-red-700',
  amber:  'bg-amber-50 text-amber-700',
  blue:   'bg-blue-50 text-blue-700',
  orange: 'bg-orange-50 text-orange-700',
  maroon: 'bg-rose-50 text-rose-800',
  slate:  'bg-gray-100 text-gray-600',
};
export function Badge({ color = 'slate', children }) {
  return (
    <span className={`inline-block px-2.5 py-0.5 rounded-full text-xs font-semibold ${badgeStyles[color] || badgeStyles.slate}`}>
      {children}
    </span>
  );
}

/* ── RIR Badge ── */
const rirBadgeColor = {
  afrinic: 'orange', apnic: 'blue', arin: 'maroon', lacnic: 'amber', ripencc: 'green',
};
export function RirBadge({ rir }) {
  return <Badge color={rirBadgeColor[rir] || 'slate'}>{rir}</Badge>;
}

/* ── Sort helpers (for raw tables) ── */
export function useSort() {
  const [sortKey, setSortKey] = useState(null);
  const [sortDir, setSortDir] = useState('asc');
  function toggle(key) {
    if (sortKey === key) setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDir('asc'); }
  }
  function sorted(rows) {
    if (!sortKey || !rows) return rows;
    return [...rows].sort((a, b) => {
      const av = a[sortKey], bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv));
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }
  return { sortKey, sortDir, toggle, sorted };
}

export function SortTh({ k, sortKey, sortDir, onClick, className = '', children }) {
  return (
    <th
      className={`px-4 py-2.5 text-left font-medium select-none cursor-pointer hover:text-cf-text ${className}`}
      onClick={() => onClick(k)}
    >
      <span className="inline-flex items-center gap-1">
        {children}
        <span className="inline-flex flex-col leading-none -my-1">
          <span className={`text-[8px] ${sortKey === k && sortDir === 'asc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'\u25B2'}</span>
          <span className={`text-[8px] ${sortKey === k && sortDir === 'desc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'\u25BC'}</span>
        </span>
      </span>
    </th>
  );
}

/* ── DataTable with column sorting ── */
export function DataTable({ columns, rows, onRowClick, emptyMsg }) {
  const [sortKey, setSortKey] = useState(null);
  const [sortDir, setSortDir] = useState('asc'); // 'asc' or 'desc'

  if (!rows || rows.length === 0) return <Empty message={emptyMsg || 'No results.'} />;

  function handleSort(key) {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  }

  const sortedRows = sortKey
    ? [...rows].sort((a, b) => {
        const av = a[sortKey], bv = b[sortKey];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv));
        return sortDir === 'asc' ? cmp : -cmp;
      })
    : rows;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
            {columns.map((col) => (
              <th
                key={col.key}
                className={`px-4 py-2.5 text-left font-medium select-none cursor-pointer hover:text-cf-text ${col.className || ''}`}
                onClick={() => handleSort(col.key)}
              >
                <span className="inline-flex items-center gap-1">
                  {col.label}
                  <span className="inline-flex flex-col leading-none -my-1">
                    <span className={`text-[8px] ${sortKey === col.key && sortDir === 'asc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'\u25B2'}</span>
                    <span className={`text-[8px] ${sortKey === col.key && sortDir === 'desc' ? 'text-cf-orange' : 'text-cf-border-strong'}`}>{'\u25BC'}</span>
                  </span>
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, i) => (
            <tr
              key={row._key || i}
              className={`border-b border-cf-border hover:bg-cf-surface-2 transition-colors ${i % 2 === 1 ? 'bg-cf-surface-2' : ''} ${onRowClick ? 'cursor-pointer' : ''}`}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
            >
              {columns.map((col) => (
                <td key={col.key} className={`px-4 py-3 ${col.className || ''}`}>
                  {col.render ? col.render(row[col.key], row) : row[col.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ── FilterBar ── */
export function FilterBar({ children, onSubmit }) {
  return (
    <form
      className="flex flex-wrap items-end gap-2 lg:gap-3 mb-6 pb-5 border-b border-cf-border"
      onSubmit={(e) => { e.preventDefault(); onSubmit?.(); }}
    >
      {children}
    </form>
  );
}

/* ── Form inputs ── */
export function Input({ label, ...props }) {
  return (
    <label className="flex flex-col gap-1 text-xs font-medium text-cf-text-secondary">
      {label}
      <input
        className="bg-cf-surface border border-cf-border rounded-lg px-3 py-2 text-sm text-cf-text
                   focus:outline-none focus:ring-2 focus:ring-cf-orange/20 focus:border-cf-orange"
        {...props}
      />
    </label>
  );
}

export function Select({ label, options, ...props }) {
  return (
    <label className="flex flex-col gap-1 text-xs font-medium text-cf-text-secondary">
      {label}
      <select
        className="bg-cf-surface border border-cf-border rounded-lg px-3 py-2 text-sm text-cf-text
                   focus:outline-none focus:ring-2 focus:ring-cf-orange/20 focus:border-cf-orange"
        {...props}
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
    </label>
  );
}

export function Button({ children, variant = 'primary', ...props }) {
  const base = 'rounded-lg px-4 py-2 text-sm font-semibold transition-colors';
  const styles = variant === 'primary'
    ? `${base} bg-cf-orange hover:bg-cf-orange-hover text-white shadow-sm transition-all duration-150`
    : `${base} bg-cf-surface border border-cf-border hover:bg-cf-surface-2 text-cf-text`;
  return <button className={styles} {...props}>{children}</button>;
}

/* ── Recharts shared config ── */
export const chartTickStyle = { fill: '#9ca3af', fontSize: 11 };
export const chartAxisStyle = { stroke: '#ebebef' };
export const chartTooltipStyle = {
  contentStyle: {
    background: '#ffffff',
    border: '1px solid #e3e3e8',
    borderRadius: 10,
    fontSize: 13,
    boxShadow: '0 8px 24px rgba(0,0,0,0.1)',
  },
  labelStyle: { color: '#1d1d1f', fontWeight: 600 },
  itemStyle: { color: '#6b7280' },
};
