import { useState, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts';
import { api } from '../api';
import {
  Card, Stat, Badge, Empty, fmtNum, chartTickStyle, chartAxisStyle, chartTooltipStyle,
} from '../components/ui';

const TOKEN_KEY = 'rpki_admin_token';

function fmtTs(ts) {
  if (!ts) return '—';
  return ts.replace('T', ' ').slice(0, 19);
}

function LoginGate({ onAuth }) {
  const [pw, setPw] = useState('');
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);

  async function submit(e) {
    e.preventDefault();
    if (!pw) return;
    setBusy(true);
    setErr(null);
    try {
      await api.adminStats(pw, { days: 1 }); // probe credentials
      onAuth(pw);
    } catch (e) {
      setErr(e.message.startsWith('401') ? 'Invalid admin token' : e.message);
    }
    setBusy(false);
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-cf-bg p-4">
      <form onSubmit={submit} className="w-full max-w-sm bg-cf-surface border border-cf-border rounded-xl p-8">
        <div className="flex items-center gap-2.5 mb-6">
          <img src="/favicon.svg" className="w-8 h-8" alt="RPKI" />
          <div>
            <div className="text-base font-bold text-cf-text">Admin</div>
            <div className="text-xs text-cf-text-muted">Visitor analytics</div>
          </div>
        </div>
        <label className="flex flex-col gap-1 text-xs font-medium text-cf-text-secondary mb-4">
          Admin token
          <input
            type="password"
            autoFocus
            value={pw}
            onChange={(e) => setPw(e.target.value)}
            className="bg-cf-surface border border-cf-border rounded-lg px-3 py-2 text-sm text-cf-text
                       focus:outline-none focus:ring-2 focus:ring-cf-orange/30 focus:border-cf-orange"
          />
        </label>
        {err && <div className="text-xs text-red-600 mb-3">{err}</div>}
        <button
          type="submit"
          disabled={!pw || busy}
          className="w-full rounded-lg px-4 py-2 text-sm font-semibold bg-cf-orange hover:bg-cf-orange-hover text-white disabled:opacity-50"
        >
          {busy ? 'Checking...' : 'Sign in'}
        </button>
      </form>
    </div>
  );
}

function SimpleTable({ title, columns, rows, action }) {
  return (
    <Card title={title} action={action} className="mb-6">
      {(rows || []).length > 0 ? (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2">
                {columns.map((c) => (
                  <th key={c.key} className="px-4 py-2.5 text-left font-medium">{c.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i} className={`border-b border-cf-border hover:bg-cf-surface-2 ${i % 2 === 1 ? 'bg-cf-surface-2' : ''}`}>
                  {columns.map((c) => (
                    <td key={c.key} className={`px-4 py-2.5 ${c.className || ''}`}>
                      {c.render ? c.render(r[c.key], r) : (r[c.key] ?? '—')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty message="No data yet." />
      )}
    </Card>
  );
}

export default function Admin() {
  const [token, setToken] = useState(() => sessionStorage.getItem(TOKEN_KEY) || '');
  const [days, setDays] = useState(14);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async (tok, d) => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.adminStats(tok, { days: d });
      setData(res);
    } catch (e) {
      if (e.message.startsWith('401')) {
        sessionStorage.removeItem(TOKEN_KEY);
        setToken('');
      }
      setError(e.message);
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    if (token) load(token, days);
  }, [token, days, load]);

  function onAuth(pw) {
    sessionStorage.setItem(TOKEN_KEY, pw);
    setToken(pw);
  }

  function logout() {
    sessionStorage.removeItem(TOKEN_KEY);
    setToken('');
    setData(null);
  }

  if (!token) return <LoginGate onAuth={onAuth} />;

  return (
    <div className="min-h-screen bg-cf-bg p-4 lg:p-8">
      <div className="max-w-[1100px] mx-auto">
        <div className="flex items-center justify-end mb-6">
          <div className="flex items-center gap-2">
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="bg-cf-surface border border-cf-border rounded-lg px-3 py-2 text-sm text-cf-text"
            >
              <option value={1}>Last 1 day</option>
              <option value={7}>Last 7 days</option>
              <option value={14}>Last 14 days</option>
              <option value={30}>Last 30 days</option>
              <option value={90}>Last 90 days</option>
            </select>
            <button onClick={() => load(token, days)} className="rounded-lg px-3 py-2 text-sm font-semibold bg-cf-surface border border-cf-border hover:bg-cf-surface-2 text-cf-text">
              Refresh
            </button>
            <button onClick={logout} className="rounded-lg px-3 py-2 text-sm font-semibold text-cf-text-muted hover:text-cf-text">
              Logout
            </button>
          </div>
        </div>

        {loading && !data && (
          <div className="text-center py-12"><div className="spinner mx-auto" /></div>
        )}
        {error && !data && (
          <div className="bg-red-50 border border-red-200 rounded-xl p-6 text-red-700 text-sm">{error}</div>
        )}

        {data && (
          <>
            <div className="grid grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
              <Stat label={`Unique Visitors (${data.days}d)`} value={fmtNum(data.totals?.unique_ips)} color="text-cf-orange" />
              <Stat label={`Total Requests (${data.days}d)`} value={fmtNum(data.totals?.events)} />
              <Stat label="Distinct ASNs Searched" value={fmtNum((data.top_asns || []).length)} />
            </div>

            <Card title="Daily Unique Visitors" className="mb-6">
              {(data.daily || []).length > 0 ? (
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart data={data.daily} margin={{ top: 10, right: 20, bottom: 5, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f3" vertical={false} />
                    <XAxis dataKey="date" tick={chartTickStyle} axisLine={chartAxisStyle} tickLine={false} />
                    <YAxis tick={chartTickStyle} axisLine={false} tickLine={false} allowDecimals={false} />
                    <Tooltip {...chartTooltipStyle} />
                    <Bar dataKey="visitors" name="Visitors" fill="#f6821f" radius={[3, 3, 0, 0]} />
                    <Bar dataKey="events" name="Requests" fill="#cbd5e1" radius={[3, 3, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              ) : <Empty message="No visits yet." />}
            </Card>

            <div className="grid lg:grid-cols-2 gap-0 lg:gap-6">
              <SimpleTable
                title="Top Searched ASNs"
                columns={[
                  { key: 'asn', label: 'ASN', className: 'font-mono', render: (v) => `AS${v}` },
                  { key: 'count', label: 'Searches', render: (v) => fmtNum(v) },
                ]}
                rows={data.top_asns}
              />
              <SimpleTable
                title="Top Visitors"
                columns={[
                  { key: 'ip', label: 'IP', className: 'font-mono' },
                  { key: 'country', label: 'Country', render: (v) => v ? <Badge color="blue">{v}</Badge> : '—' },
                  { key: 'events', label: 'Requests', render: (v) => fmtNum(v) },
                  { key: 'last_seen', label: 'Last Seen', render: fmtTs },
                ]}
                rows={data.top_visitors}
              />
            </div>

            <div className="grid lg:grid-cols-2 gap-0 lg:gap-6">
              <SimpleTable
                title="Top Searched Prefixes"
                columns={[
                  { key: 'prefix', label: 'Prefix', className: 'font-mono' },
                  { key: 'count', label: 'Searches', render: (v) => fmtNum(v) },
                ]}
                rows={data.top_prefixes}
              />
              <SimpleTable
                title="Top Searched Orgs"
                columns={[
                  { key: 'q', label: 'Org Query' },
                  { key: 'count', label: 'Searches', render: (v) => fmtNum(v) },
                ]}
                rows={data.top_orgs}
              />
            </div>

            <SimpleTable
              title="Recent Activity"
              columns={[
                { key: 'ts', label: 'Time', render: fmtTs, className: 'text-cf-text-secondary whitespace-nowrap' },
                { key: 'ip', label: 'IP', className: 'font-mono' },
                { key: 'country', label: 'Country', render: (v) => v ? <Badge color="blue">{v}</Badge> : '—' },
                { key: 'path', label: 'Path', className: 'font-mono text-xs' },
                { key: 'query', label: 'Query', className: 'font-mono text-xs text-cf-text-muted', render: (v) => v || '—' },
              ]}
              rows={data.recent}
            />
          </>
        )}
      </div>
    </div>
  );
}
