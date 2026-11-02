import { useState } from 'react';
import { api } from '../api';
import {
  Page, Card, FilterBar, Select, Input, Button, DataTable, Loading, ErrorBox, Empty, Stat, Badge, RirBadge,
  fmtNum, fmtDate, RIRS,
} from '../components/ui';

const rirOptions = RIRS.map((r) => ({ value: r, label: r }));
const dayOptions = [
  { value: '1', label: '1 day' },
  { value: '3', label: '3 days' },
  { value: '7', label: '7 days' },
  { value: '14', label: '14 days' },
  { value: '30', label: '30 days' },
];

export default function Alerts() {
  const [rir, setRir] = useState('arin');
  const [asn, setAsn] = useState('');
  const [days, setDays] = useState('7');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function search() {
    setLoading(true);
    setError(null);
    try {
      const res = await api.recentChanges({
        rir, asn: asn || undefined, days: Number(days), limit: 500,
      });
      setData(res);
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }

  const changes = data?.changes || [];
  const newCount = changes.filter((c) => c.change_type === 'new').length;
  const withdrawnCount = changes.filter((c) => c.change_type === 'withdrawn').length;

  return (
    <Page title="Recent Changes" subtitle="Detect new and withdrawn ROAs within a time window">
      <FilterBar onSubmit={search}>
        <Select label="RIR" value={rir} onChange={(e) => setRir(e.target.value)} options={rirOptions} />
        <Input label="ASN (optional)" placeholder="e.g. 13335" type="number" value={asn} onChange={(e) => setAsn(e.target.value)} />
        <Select label="Look Back" value={days} onChange={(e) => setDays(e.target.value)} options={dayOptions} />
        <Button type="submit" disabled={loading}>Load</Button>
      </FilterBar>

      {loading && <Loading />}
      {error && <ErrorBox message={error} />}
      {data && !loading && (
        <>
          <div className="grid grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
            <Stat label="Total Changes" value={fmtNum(data.count)} />
            <Stat label="New ROAs" value={fmtNum(newCount)} color="text-emerald-600" />
            <Stat label="Withdrawn" value={fmtNum(withdrawnCount)} color="text-red-600" />
          </div>

          <Card title={`ROA Changes — ${rir.toUpperCase()} (last ${days} days)`}>
            <DataTable
              columns={[
                {
                  key: 'change_type', label: 'Type',
                  render: (v) => (
                    <Badge color={v === 'new' ? 'green' : 'red'}>
                      {v === 'new' ? 'NEW' : 'WITHDRAWN'}
                    </Badge>
                  ),
                },
                { key: 'asn', label: 'ASN', className: 'font-mono', render: (v) => `AS${v}` },
                { key: 'prefix', label: 'Prefix', className: 'font-mono' },
                { key: 'max_length', label: 'Max Len', render: (v) => `/${v}` },
                { key: 'first_seen', label: 'First Seen', render: fmtDate },
                { key: 'last_seen', label: 'Last Seen', render: fmtDate },
                {
                  key: 'object_hash', label: 'Hash',
                  render: (v) => <span className="font-mono text-xs text-cf-text-muted">{v?.slice(0, 12)}...</span>,
                },
              ]}
              rows={changes}
              emptyMsg="No changes detected in this period."
            />
          </Card>
        </>
      )}
      {!data && !loading && !error && <Empty message="Select an RIR and click Load to view recent changes." />}
    </Page>
  );
}
