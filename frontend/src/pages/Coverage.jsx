import { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  PieChart, Pie, Cell, ResponsiveContainer, Tooltip,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Legend,
} from 'recharts';
import { api } from '../api';
import {
  Page, Card, FilterBar, Input, Select, Button, DataTable, ErrorBox, Empty, Stat, Badge,
  fmtNum, chartTickStyle, chartAxisStyle, chartTooltipStyle,
} from '../components/ui';

const familyOptions = [
  { value: 'both', label: 'IPv4 + IPv6' },
  { value: 'ipv4', label: 'IPv4 only' },
  { value: 'ipv6', label: 'IPv6 only' },
];

function CoverageRing({ signed, unsigned, label }) {
  const total = signed + unsigned;
  if (total === 0) return null;
  const pct = ((signed / total) * 100).toFixed(1);
  const data = [
    { name: 'Signed', value: signed },
    { name: 'Unsigned', value: unsigned },
  ];
  const colors = ['#16a34a', '#e5e7eb'];

  return (
    <div className="flex flex-col items-center">
      <div className="relative w-36 h-36">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={data} cx="50%" cy="50%" innerRadius={45} outerRadius={60}
              dataKey="value" startAngle={90} endAngle={-270} strokeWidth={0}>
              {data.map((_, i) => <Cell key={i} fill={colors[i]} />)}
            </Pie>
            <Tooltip {...chartTooltipStyle} />
          </PieChart>
        </ResponsiveContainer>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-2xl font-bold text-cf-text">{pct}%</span>
        </div>
      </div>
      <div className="mt-2 text-xs font-medium text-cf-text-secondary">{label}</div>
      <div className="text-xs text-cf-text-muted">{fmtNum(signed)} / {fmtNum(total)}</div>
    </div>
  );
}

// ?asn=13335 or ?asn=AS13335 -> '13335'; anything else -> ''
function asnFromParam(v) {
  const m = /^(?:AS)?(\d{1,10})$/i.exec((v || '').trim());
  return m ? m[1] : '';
}

function coverageColor(pct) {
  if (pct >= 90) return 'green';
  if (pct >= 50) return 'amber';
  return 'red';
}

export default function Coverage() {
  const [params] = useSearchParams();
  const urlAsn = asnFromParam(params.get('asn'));
  const [asn, setAsn] = useState(urlAsn || '13335');
  const [family, setFamily] = useState('both');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Trend
  const [trend, setTrend] = useState(null);



  async function search(asnArg) {
    const q = asnArg || asn;
    if (!q) return;
    setLoading(true);
    setError(null);
    setTrend(null);
    try {
      const [cov, tr] = await Promise.all([
        api.roaCoverage({ asn: q, family }),
        api.roaCoverageTrend({ asn: q }),
      ]);
      setData(cov);
      setTrend(tr);
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }

  // Deep link (/roa/coverage?asn=N, e.g. from Analysis -> Countries): load that ASN on arrival.
  useEffect(() => {
    if (!urlAsn) return;
    setAsn(urlAsn);
    search(urlAsn);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlAsn]);



  const s = data?.summary;

  return (
    <Page title="RPKI Coverage" subtitle="Signing ratio — ROA-covered vs BGP-announced prefixes">
      <FilterBar onSubmit={search}>
        <Input label="ASN" placeholder="e.g. 13335" type="number" value={asn} onChange={(e) => setAsn(e.target.value)} required />
        <Select label="Address Family" value={family} onChange={(e) => setFamily(e.target.value)} options={familyOptions} />
        <Button type="submit" disabled={!asn || loading}>
          {loading ? 'Loading...' : 'Analyze'}
        </Button>
      </FilterBar>

      {loading && (
        <div className="text-center py-12">
          <div className="spinner mx-auto mb-3" />
          <div className="text-sm text-cf-text-muted">Fetching BGP announcements... this may take up to 60s</div>
        </div>
      )}
      {error && <ErrorBox message={error} />}

      {data && !loading && (
        <>
          {/* Summary stats */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <Stat label="Announced Prefixes" value={fmtNum(s.announced)} />
            <Stat label="ROA Covered" value={fmtNum(s.roa_covered)} color="text-emerald-600" />
            <Stat label="Unsigned" value={fmtNum(s.unsigned)} color={s.unsigned > 0 ? 'text-red-600' : 'text-emerald-600'} />
            <Stat label="Coverage" value={<Badge color={coverageColor(s.coverage_pct)}>{s.coverage_pct}%</Badge>} />
          </div>

          {/* Coverage rings */}
          <Card title={`AS${data.asn} — RPKI Signing Coverage`} className="mb-6">
            <div className="flex flex-wrap justify-center gap-8 lg:gap-16 py-4">
              <CoverageRing signed={s.roa_covered} unsigned={s.unsigned} label="Overall" />
              {data.ipv4.announced > 0 && <CoverageRing signed={data.ipv4.signed} unsigned={data.ipv4.unsigned} label="IPv4" />}
              {data.ipv6.announced > 0 && <CoverageRing signed={data.ipv6.signed} unsigned={data.ipv6.unsigned} label="IPv6" />}
            </div>
            <div className="grid grid-cols-2 gap-4 mt-4 pt-4 border-t border-cf-border text-sm">
              <div>
                <div className="text-cf-text-muted text-xs uppercase mb-2">IPv4</div>
                <div>Announced: <strong>{fmtNum(data.ipv4.announced)}</strong></div>
                <div>Signed: <strong className="text-emerald-600">{fmtNum(data.ipv4.signed)}</strong></div>
                <div>Unsigned: <strong className="text-red-600">{fmtNum(data.ipv4.unsigned)}</strong></div>
                <div>Coverage: <strong>{data.ipv4.coverage_pct}%</strong></div>
              </div>
              <div>
                <div className="text-cf-text-muted text-xs uppercase mb-2">IPv6</div>
                <div>Announced: <strong>{fmtNum(data.ipv6.announced)}</strong></div>
                <div>Signed: <strong className="text-emerald-600">{fmtNum(data.ipv6.signed)}</strong></div>
                <div>Unsigned: <strong className="text-red-600">{fmtNum(data.ipv6.unsigned)}</strong></div>
                <div>Coverage: <strong>{data.ipv6.coverage_pct}%</strong></div>
              </div>
            </div>
          </Card>

          {/* Coverage trend chart */}
          {trend?.data?.length > 0 && (
            <Card title={`AS${data.asn} — ROA Coverage Growth (Yearly)`} className="mb-6">
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={trend.data.map((d) => ({ ...d, year: String(d.year) }))} margin={{ top: 10, right: 20, bottom: 5, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f3" vertical={false} />
                  <XAxis dataKey="year" tick={chartTickStyle} axisLine={chartAxisStyle} tickLine={false} />
                  <YAxis tick={chartTickStyle} axisLine={false} tickLine={false} />
                  <Tooltip {...chartTooltipStyle} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Bar dataKey="roa_ipv4" name="IPv4 ROAs" fill="#2563eb" radius={[3, 3, 0, 0]} />
                  <Bar dataKey="roa_ipv6" name="IPv6 ROAs" fill="#f6821f" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          )}

          {/* ROA-only prefixes */}
          {data.roa_not_announced?.length > 0 && (
            <Card title={`ROA Exists but Not Announced (${data.roa_not_announced.length})`} className="mb-6">
              <div className="text-xs text-cf-text-muted mb-3">These prefixes have ROAs but are not seen in BGP.</div>
              <div className="flex flex-wrap gap-2">
                {data.roa_not_announced.map((p) => (
                  <span key={p} className="font-mono text-xs bg-amber-50 text-amber-700 px-2 py-1 rounded">{p}</span>
                ))}
              </div>
            </Card>
          )}

          {/* Unsigned prefixes */}
          {data.unsigned_prefixes?.length > 0 && (
            <Card title={`Unsigned Prefixes (${data.unsigned_prefixes.length}${data.unsigned_prefixes.length >= 100 ? '+' : ''})`}>
              <div className="text-xs text-cf-text-muted mb-3">No ROA — vulnerable to route hijacking.</div>
              <DataTable
                columns={[
                  { key: 'prefix', label: 'Prefix', className: 'font-mono' },
                  { key: '_status', label: 'Status', render: () => <Badge color="red">No ROA</Badge> },
                ]}
                rows={data.unsigned_prefixes.map((p) => ({ prefix: p }))}
              />
            </Card>
          )}
        </>
      )}

      {!data && !loading && !error && (
        <div className="mt-6">
          <Empty message="Enter an ASN to analyze RPKI signing coverage." />
        </div>
      )}
    </Page>
  );
}
