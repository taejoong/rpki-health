import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { api } from '../api';
import {
  Page, Card, FilterBar, Input, Select, Button, Loading, ErrorBox, Empty,
  RirBadge, Badge, fmtNum, fmtDate, RIRS, useSort, SortTh,
} from '../components/ui';
import WaterfallChart from '../components/WaterfallChart';

const rirOptions = [{ value: '', label: 'All RIRs' }, ...RIRS.map((r) => ({ value: r, label: r }))];

export default function RoaLookup() {
  const [searchParams] = useSearchParams();
  const [prefix, setPrefix] = useState(searchParams.get('prefix') || '1.0.0.0/24');
  const [asn, setAsn] = useState('');
  const [rir, setRir] = useState('');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [autoSearched, setAutoSearched] = useState(false);

  const sort = useSort();

  // History
  const [history, setHistory] = useState(null);
  const [histLoading, setHistLoading] = useState(false);
  const [showHistory, setShowHistory] = useState(false);

  // Auto-search if prefix is in URL query
  useEffect(() => {
    const p = searchParams.get('prefix');
    if (p && !autoSearched) {
      setPrefix(p);
      setAutoSearched(true);
      doSearch(p);
    }
  }, [searchParams]);

  async function doSearch(pfx) {
    const p = pfx || prefix;
    if (!p) return;
    setLoading(true);
    setError(null);
    setHistory(null);
    // Show full history by default — fetch it in parallel with the live lookup.
    setShowHistory(true);
    setHistLoading(true);
    const params = { prefix: p, asn: asn || undefined, rir: rir || undefined };
    api.roaHistory(params)
      .then(setHistory)
      .catch(() => {})
      .finally(() => setHistLoading(false));
    try {
      const res = await api.roaLookup(params);
      setData(res);
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }

  async function loadHistory() {
    if (history) {
      setShowHistory(!showHistory);
      return;
    }
    setHistLoading(true);
    setShowHistory(true);
    try {
      const res = await api.roaHistory({ prefix, asn: asn || undefined, rir: rir || undefined });
      setHistory(res);
    } catch {}
    setHistLoading(false);
  }

  function lifespanDays(first, last) {
    if (!first || !last) return '—';
    const d = (new Date(last) - new Date(first)) / 86400000;
    return d < 1 ? '<1' : Math.round(d).toString();
  }

  return (
    <Page title="Prefix Lookup" subtitle="Look up live ROAs for a prefix, with full lifecycle history">
      <FilterBar onSubmit={() => doSearch()}>
        <Input label="Prefix" placeholder="e.g. 1.0.0.0/24" value={prefix} onChange={(e) => setPrefix(e.target.value)} required />
        <Input label="ASN (optional)" placeholder="e.g. 13335" type="number" value={asn} onChange={(e) => setAsn(e.target.value)} />
        <Select label="RIR" value={rir} onChange={(e) => setRir(e.target.value)} options={rirOptions} />
        <Button type="submit" disabled={!prefix || loading}>Search</Button>
      </FilterBar>

      {loading && <Loading />}
      {error && <ErrorBox message={error} />}
      {data && !loading && (
        <>
          <div className="mb-4 text-sm text-cf-text-secondary">
            <strong className="text-cf-text">{fmtNum(data.count)}</strong> live ROAs for <code className="font-mono bg-cf-surface-2 px-1.5 py-0.5 rounded">{data.query_prefix}</code>
          </div>

          {/* Live ROAs table */}
          <Card title="Live ROAs" className="mb-6" action={
            <Badge color="green">Current</Badge>
          }>
            {(data.results || []).length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
                      <SortTh k="rir" {...sort} onClick={sort.toggle}>RIR</SortTh>
                      <SortTh k="asn" {...sort} onClick={sort.toggle}>ASN</SortTh>
                      <SortTh k="prefix" {...sort} onClick={sort.toggle}>Prefix</SortTh>
                      <SortTh k="max_length" {...sort} onClick={sort.toggle}>Max Len</SortTh>
                      <SortTh k="afi" {...sort} onClick={sort.toggle}>AFI</SortTh>
                      <SortTh k="first_seen" {...sort} onClick={sort.toggle}>First Seen</SortTh>
                      <SortTh k="last_seen" {...sort} onClick={sort.toggle}>Last Seen</SortTh>
                    </tr>
                  </thead>
                  <tbody>
                    {sort.sorted(data.results).map((r, i) => (
                      <tr key={i} className={`border-b border-cf-border hover:bg-cf-surface-2 ${i % 2 === 1 ? 'bg-cf-surface-2' : ''}`}>
                        <td className="px-4 py-3"><RirBadge rir={r.rir} /></td>
                        <td className="px-4 py-3 font-mono">AS{r.asn}</td>
                        <td className="px-4 py-3 font-mono">{r.prefix}</td>
                        <td className="px-4 py-3">/{r.max_length}</td>
                        <td className="px-4 py-3"><Badge color={r.afi === 4 ? 'blue' : 'amber'}>{r.afi === 4 ? 'IPv4' : 'IPv6'}</Badge></td>
                        <td className="px-4 py-3 text-cf-text-secondary">{fmtDate(r.first_seen)}</td>
                        <td className="px-4 py-3 text-cf-text-secondary">{fmtDate(r.last_seen)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <Empty message="No live ROAs found for this prefix." />
            )}
          </Card>

          {/* History toggle */}
          <button
            onClick={loadHistory}
            className="flex items-center gap-2 text-sm text-cf-orange hover:text-cf-orange-hover font-semibold mb-4 transition-colors"
          >
            <span className={`transition-transform ${showHistory ? 'rotate-90' : ''}`}>&#9654;</span>
            {showHistory ? 'Hide' : 'Show'} Full History
            {history && <span className="text-cf-text-muted font-normal">({fmtNum(history.count)} entries)</span>}
          </button>

          {/* History section */}
          {showHistory && (
            <>
              {histLoading && <Loading />}
              {history && !histLoading && (
                <>
                  {history.truncated && (
                    <div className="mb-4 bg-amber-50 border border-amber-200 rounded-lg px-4 py-2 text-sm text-amber-700">
                      Results truncated at {fmtNum(history.count)} entries.
                    </div>
                  )}

                  {/* Waterfall chart */}
                  {(history.entries || []).length > 0 && (
                    <Card title="ROA Timeline" className="mb-6" action={
                      <span className="text-xs text-cf-text-muted">x = time, y = origin ASN</span>
                    }>
                      <WaterfallChart entries={history.entries} />
                    </Card>
                  )}

                  {/* History table */}
                  <Card title="All Historical ROA Entries">
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
                            <th className="px-4 py-2.5 text-left font-medium">RIR</th>
                            <th className="px-4 py-2.5 text-left font-medium">ASN</th>
                            <th className="px-4 py-2.5 text-left font-medium">Prefix</th>
                            <th className="px-4 py-2.5 text-left font-medium">Max Len</th>
                            <th className="px-4 py-2.5 text-left font-medium">First Seen</th>
                            <th className="px-4 py-2.5 text-left font-medium">Last Seen</th>
                            <th className="px-4 py-2.5 text-left font-medium">Lifespan</th>
                            <th className="px-4 py-2.5 text-left font-medium">Semantic Hash</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(history.entries || []).map((e, i) => (
                            <tr key={i} className={`border-b border-cf-border hover:bg-cf-surface-2 ${i % 2 === 1 ? 'bg-cf-surface-2' : ''}`}>
                              <td className="px-4 py-3"><RirBadge rir={e.rir} /></td>
                              <td className="px-4 py-3 font-mono">AS{e.asn}</td>
                              <td className="px-4 py-3 font-mono">{e.prefix}</td>
                              <td className="px-4 py-3">/{e.max_length}</td>
                              <td className="px-4 py-3 text-cf-text-secondary">{fmtDate(e.first_seen)}</td>
                              <td className="px-4 py-3 text-cf-text-secondary">{fmtDate(e.last_seen)}</td>
                              <td className="px-4 py-3 font-mono">{lifespanDays(e.first_seen, e.last_seen)}d</td>
                              <td className="px-4 py-3">
                                <span className="font-mono text-xs text-cf-text-muted">{e.semantic_hash?.slice(0, 12)}...</span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </Card>
                </>
              )}
            </>
          )}
        </>
      )}
      {!data && !loading && !error && <Empty message="Enter a prefix to search." />}
    </Page>
  );
}
