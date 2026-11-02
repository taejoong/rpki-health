import { useState, useEffect } from 'react';
import {
  ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import { api } from '../api';
import {
  Page, Card, FilterBar, Select, Input, Button, DataTable, Loading, ErrorBox, Empty, Stat, Badge, RirBadge,
  fmtNum, fmtDate, RIRS, RIR_COLORS, chartTickStyle, chartAxisStyle, chartTooltipStyle,
} from '../components/ui';

const rirOptions = RIRS.map((r) => ({ value: r, label: r }));

export default function Aspa() {
  const [stats, setStats] = useState(null);
  const [statsLoading, setStatsLoading] = useState(true);
  const [statsError, setStatsError] = useState(null);

  // Trend
  const [trendRir, setTrendRir] = useState('ripencc');
  const [trend, setTrend] = useState(null);
  const [trendLoading, setTrendLoading] = useState(false);

  // List
  const [listRir, setListRir] = useState('');
  const [customerAsn, setCustomerAsn] = useState('');
  const [list, setList] = useState(null);
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState(null);

  // Load stats on mount
  useEffect(() => {
    api.aspaStats()
      .then(setStats)
      .catch((e) => setStatsError(e.message))
      .finally(() => setStatsLoading(false));
  }, []);

  async function loadTrend() {
    setTrendLoading(true);
    try {
      const res = await api.aspaTrend({ rir: trendRir, granularity: 'month' });
      setTrend(Array.isArray(res) ? res : res.data || []);
    } catch {}
    setTrendLoading(false);
  }

  async function loadList() {
    setListLoading(true);
    setListError(null);
    try {
      const res = await api.aspaList({
        rir: listRir || undefined, customer_asn: customerAsn || undefined, limit: 500,
      });
      setList(res);
    } catch (e) {
      setListError(e.message);
    }
    setListLoading(false);
  }

  return (
    <Page title="ASPA Observatory" subtitle="ASPA deployment status and adoption trends">
      {/* Stats */}
      {statsLoading && <Loading />}
      {statsError && <ErrorBox message={statsError} />}
      {stats && (
        <div className="mb-6">
          <div className="grid grid-cols-2 gap-4 mb-4">
            <Stat label="Total Live ASPA Objects" value={fmtNum(stats.total_live_objects)} color="text-cf-orange" />
            <Stat label="Total Customer ASNs" value={fmtNum(stats.total_customer_asns)} />
          </div>
          <Card title="Per-RIR Breakdown">
            <DataTable
              columns={[
                { key: 'rir', label: 'RIR', render: (v) => <RirBadge rir={v} /> },
                { key: 'live_objects', label: 'Live Objects', render: fmtNum },
                { key: 'customer_asns', label: 'Customer ASNs', render: fmtNum },
                { key: 'provider_pairs', label: 'Provider Pairs', render: fmtNum },
              ]}
              rows={stats.per_rir ? Object.entries(stats.per_rir).map(([rir, d]) => ({ rir, ...d })) : []}
            />
          </Card>
        </div>
      )}

      {/* Trend */}
      <Card title="ASPA Adoption Trend" className="mb-6" action={
        <div className="flex items-center gap-2">
          <select
            value={trendRir}
            onChange={(e) => setTrendRir(e.target.value)}
            className="text-xs border border-cf-border rounded px-2 py-1"
          >
            {RIRS.map((r) => <option key={r} value={r}>{r}</option>)}
          </select>
          <button onClick={loadTrend} className="text-xs text-cf-orange hover:underline font-semibold">
            {trendLoading ? 'Loading...' : 'Load'}
          </button>
        </div>
      }>
        {trend && trend.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={trend} margin={{ top: 10, right: 20, bottom: 5, left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f3" vertical={false} />
              <XAxis dataKey="period" tick={chartTickStyle} axisLine={chartAxisStyle} tickLine={false} />
              <YAxis tick={chartTickStyle} axisLine={false} tickLine={false} />
              <Tooltip {...chartTooltipStyle} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Area dataKey="new_objects" fill={RIR_COLORS[trendRir] + '20'} stroke={RIR_COLORS[trendRir]} strokeWidth={2} />
              <Line dataKey="new_customer_asns" stroke="#f6821f" strokeWidth={2} dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        ) : (
          <Empty message="Click Load to view ASPA adoption trend." />
        )}
      </Card>

      {/* ASPA List */}
      <Card title="ASPA Objects">
        <div className="flex items-end gap-3 mb-4 pb-4 border-b border-cf-border">
          <Select label="RIR" value={listRir} onChange={(e) => setListRir(e.target.value)}
            options={[{ value: '', label: 'All' }, ...rirOptions]} />
          <Input label="Customer ASN" placeholder="e.g. 53808" type="number"
            value={customerAsn} onChange={(e) => setCustomerAsn(e.target.value)} />
          <Button onClick={loadList} disabled={listLoading}>Search</Button>
        </div>

        {listLoading && <Loading />}
        {listError && <ErrorBox message={listError} />}
        {list && !listLoading && (
          <>
            <div className="mb-3 text-sm text-cf-text-secondary">
              <strong className="text-cf-text">{fmtNum(list.count)}</strong> ASPA entries
            </div>
            <DataTable
              columns={[
                { key: 'rir', label: 'RIR', render: (v) => <RirBadge rir={v} /> },
                { key: 'customer_asn', label: 'Customer ASN', className: 'font-mono', render: (v) => `AS${v}` },
                { key: 'provider_asn', label: 'Provider ASN', className: 'font-mono', render: (v) => `AS${v}` },
                { key: 'afi_limit', label: 'AFI Limit', render: (v) => v || <span className="text-cf-text-muted">any</span> },
                { key: 'first_seen', label: 'First Seen', render: fmtDate },
                { key: 'last_seen', label: 'Last Seen', render: fmtDate },
              ]}
              rows={list.entries || []}
            />
          </>
        )}
      </Card>
    </Page>
  );
}
