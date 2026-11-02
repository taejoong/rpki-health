import React, { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { api } from '../api';
import {
  Page, Card, FilterBar, Input, Select, Button, Loading, ErrorBox, Empty,
  RirBadge, Badge, fmtNum, fmtDate, RIRS, useSort, SortTh,
} from '../components/ui';
import WaterfallChart from '../components/WaterfallChart';

const rirOptions = [{ value: '', label: 'All RIRs' }, ...RIRS.map((r) => ({ value: r, label: r }))];
const afiOptions = [
  { value: '', label: 'All' },
  { value: '4', label: 'IPv4' },
  { value: '6', label: 'IPv6' },
];

// ?asn=13335 or ?asn=AS13335 -> '13335'; anything else -> ''
function asnFromParam(v) {
  const m = /^(?:AS)?(\d{1,10})$/i.exec((v || '').trim());
  return m ? m[1] : '';
}

function lifespanDays(first, last) {
  if (!first || !last) return '—';
  const d = (new Date(last) - new Date(first)) / 86400000;
  return d < 1 ? '<1' : Math.round(d).toString();
}

export default function RoaByAsn() {
  const [params] = useSearchParams();
  const urlAsn = asnFromParam(params.get('asn'));
  const [asn, setAsn] = useState(urlAsn || '13335');
  const [rir, setRir] = useState('');
  const [afi, setAfi] = useState('');
  const [offset, setOffset] = useState(0);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const limit = 1000;

  const sort = useSort();

  // History (per-clicked prefix)
  const [selectedPrefix, setSelectedPrefix] = useState(null);
  const [history, setHistory] = useState(null);
  const [histLoading, setHistLoading] = useState(false);

  async function search(newOffset = 0, asnArg) {
    const q = asnArg || asn;
    if (!q) return;
    setLoading(true);
    setError(null);
    setOffset(newOffset);
    setSelectedPrefix(null);
    setHistory(null);
    try {
      const res = await api.roaByAsn({
        asn: q, rir: rir || undefined, afi: afi || undefined, limit, offset: newOffset,
      });
      setData(res);
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }

  // Deep link (/roa/by-asn?asn=N, e.g. from Analysis -> Countries): load that ASN on arrival.
  useEffect(() => {
    if (!urlAsn) return;
    setAsn(urlAsn);
    search(0, urlAsn);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlAsn]);

  async function loadHistory(prefix, rowRir) {
    // Toggle off if clicking the already-open prefix
    if (selectedPrefix === prefix) {
      setSelectedPrefix(null);
      setHistory(null);
      return;
    }
    setSelectedPrefix(prefix);
    setHistory(null);
    setHistLoading(true);
    try {
      const res = await api.roaHistory({ prefix, asn, rir: rowRir || undefined });
      setHistory(res);
    } catch {}
    setHistLoading(false);
  }

  return (
    <Page title="ROAs by ASN" subtitle="List all current ROA prefixes for an ASN — click a prefix to see its full history">
      <FilterBar onSubmit={() => search(0)}>
        <Input label="ASN" placeholder="e.g. 13335" type="number" value={asn} onChange={(e) => setAsn(e.target.value)} required />
        <Select label="RIR" value={rir} onChange={(e) => setRir(e.target.value)} options={rirOptions} />
        <Select label="Address Family" value={afi} onChange={(e) => setAfi(e.target.value)} options={afiOptions} />
        <Button type="submit" disabled={!asn || loading}>Search</Button>
      </FilterBar>

      {loading && <Loading />}
      {error && <ErrorBox message={error} />}
      {data && !loading && (
        <>
          <div className="mb-4 text-sm text-cf-text-secondary">
            <strong className="text-cf-text">{fmtNum(data.count)}</strong> prefixes for <code className="font-mono bg-cf-surface-2 px-1.5 py-0.5 rounded">AS{data.query_asn}</code>
          </div>
          <Card>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
                    <th className="px-4 py-2.5 text-left font-medium w-8"></th>
                    <SortTh k="rir" {...sort} onClick={sort.toggle}>RIR</SortTh>
                    <SortTh k="prefix" {...sort} onClick={sort.toggle}>Prefix</SortTh>
                    <SortTh k="max_length" {...sort} onClick={sort.toggle}>Max Len</SortTh>
                    <SortTh k="afi" {...sort} onClick={sort.toggle}>AFI</SortTh>
                    <SortTh k="signing_time" {...sort} onClick={sort.toggle}>Signing Time</SortTh>
                    <SortTh k="first_seen" {...sort} onClick={sort.toggle}>First Seen</SortTh>
                    <SortTh k="last_seen" {...sort} onClick={sort.toggle}>Last Seen</SortTh>
                  </tr>
                </thead>
                <tbody>
                  {sort.sorted(data.results || []).map((r, i) => (
                    <React.Fragment key={i}>
                      <tr
                        className={`border-b border-cf-border cursor-pointer hover:bg-cf-surface-2 ${selectedPrefix === r.prefix ? 'bg-cf-orange-light' : i % 2 === 1 ? 'bg-cf-surface-2' : ''}`}
                        onClick={() => loadHistory(r.prefix, r.rir)}
                      >
                        <td className="px-4 py-3">
                          <span className={`text-xs transition-transform inline-block ${selectedPrefix === r.prefix ? 'rotate-90' : ''}`}>&#9654;</span>
                        </td>
                        <td className="px-4 py-3"><RirBadge rir={r.rir} /></td>
                        <td className="px-4 py-3 font-mono">
                          <span className="text-cf-orange hover:text-cf-orange-hover font-semibold">{r.prefix}</span>
                        </td>
                        <td className="px-4 py-3">/{r.max_length}</td>
                        <td className="px-4 py-3"><Badge color={r.afi === 4 ? 'blue' : 'amber'}>{r.afi === 4 ? 'IPv4' : 'IPv6'}</Badge></td>
                        <td className="px-4 py-3 text-cf-text-secondary">{fmtDate(r.signing_time)}</td>
                        <td className="px-4 py-3 text-cf-text-secondary">{fmtDate(r.first_seen)}</td>
                        <td className="px-4 py-3 text-cf-text-secondary">{fmtDate(r.last_seen)}</td>
                      </tr>
                      {selectedPrefix === r.prefix && (
                        <tr>
                          <td colSpan={8} className="p-0">
                            <div className="bg-cf-surface-2/60 border-b border-cf-border px-6 py-4">
                              {histLoading && <div className="py-4 flex justify-center"><div className="spinner" /></div>}
                              {history && !histLoading && (
                                <>
                                  <div className="text-xs font-semibold text-cf-text mb-2">
                                    History for {selectedPrefix} <span className="text-cf-text-muted font-normal">({fmtNum(history.count)} entries)</span>
                                  </div>
                                  {history.truncated && (
                                    <div className="mb-3 bg-amber-50 border border-amber-200 rounded px-3 py-1.5 text-xs text-amber-700">
                                      Truncated at {fmtNum(history.count)} entries.
                                    </div>
                                  )}
                                  {(history.entries || []).length > 0 && (
                                    <div className="mb-3 rounded border border-cf-border bg-cf-surface p-3">
                                      <div className="text-[10px] font-semibold text-cf-text-muted uppercase mb-1">ROA Timeline</div>
                                      <WaterfallChart entries={history.entries} />
                                    </div>
                                  )}
                                  {(history.entries || []).length > 0 ? (
                                    <table className="w-full text-xs">
                                      <thead>
                                        <tr className="text-cf-text-muted text-xs uppercase tracking-wider bg-cf-surface-2">
                                          <th className="px-3 py-1.5 text-left font-medium">ASN</th>
                                          <th className="px-3 py-1.5 text-left font-medium">Max Len</th>
                                          <th className="px-3 py-1.5 text-left font-medium">First Seen</th>
                                          <th className="px-3 py-1.5 text-left font-medium">Last Seen</th>
                                          <th className="px-3 py-1.5 text-left font-medium">Lifespan</th>
                                          <th className="px-3 py-1.5 text-left font-medium">Semantic Hash</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {history.entries.map((e, j) => (
                                          <tr key={j} className={`border-t border-cf-border/50 ${j % 2 === 1 ? 'bg-cf-surface-2' : ''}`}>
                                            <td className="px-3 py-1.5 font-mono">AS{e.asn}</td>
                                            <td className="px-3 py-1.5">/{e.max_length}</td>
                                            <td className="px-3 py-1.5 text-cf-text-secondary">{fmtDate(e.first_seen)}</td>
                                            <td className="px-3 py-1.5 text-cf-text-secondary">{fmtDate(e.last_seen)}</td>
                                            <td className="px-3 py-1.5 font-mono">{lifespanDays(e.first_seen, e.last_seen)}d</td>
                                            <td className="px-3 py-1.5 font-mono text-cf-text-muted">{e.semantic_hash?.slice(0, 12)}...</td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  ) : (
                                    <div className="text-xs text-cf-text-muted">No history found.</div>
                                  )}
                                </>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  ))}
                </tbody>
              </table>
            </div>
            {/* Pagination */}
            {data.count > limit && (
              <div className="flex items-center justify-between mt-4 pt-4 border-t border-cf-border text-sm">
                <span className="text-cf-text-muted">
                  Showing {offset + 1}–{Math.min(offset + limit, data.count)} of {fmtNum(data.count)}
                </span>
                <div className="flex gap-2">
                  <Button variant="secondary" disabled={offset === 0} onClick={() => search(offset - limit)}>
                    Previous
                  </Button>
                  <Button variant="secondary" disabled={offset + limit >= data.count} onClick={() => search(offset + limit)}>
                    Next
                  </Button>
                </div>
              </div>
            )}
          </Card>

        </>
      )}
      {!data && !loading && !error && <Empty message="Enter an ASN to search." />}
    </Page>
  );
}
