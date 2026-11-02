import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../api';
import {
  Page, Card, FilterBar, Input, Button, Loading, ErrorBox, Empty,
  RirBadge, Badge, fmtNum, fmtDate, RIRS, useSort, SortTh,
} from '../components/ui';
import WaterfallChart from '../components/WaterfallChart';

/* ── Expandable ASN Row ── */
function AsnRow({ entry }) {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [roaData, setRoaData] = useState(null);
  const [history, setHistory] = useState(null);
  const [loading, setLoading] = useState(false);
  const sort = useSort();

  async function toggle() {
    if (open) { setOpen(false); return; }
    setOpen(true);
    if (roaData) return; // already loaded
    setLoading(true);
    try {
      const [roa, hist] = await Promise.all([
        api.roaByAsn({ asn: entry.asn, limit: 200 }),
        api.roaHistory({ asn: entry.asn, limit: 500 }),
      ]);
      setRoaData(roa);
      setHistory(hist);
    } catch {}
    setLoading(false);
  }

  return (
    <div className="border-b border-cf-border">
      {/* Header row */}
      <button onClick={toggle} className="w-full flex items-center gap-3 px-4 py-3 hover:bg-cf-surface-2 transition-colors text-left">
        <span className={`text-cf-text-muted transition-transform text-xs ${open ? 'rotate-90' : ''}`}>&#9654;</span>
        <span className="font-mono text-sm font-semibold w-20">AS{entry.asn}</span>
        <span className="text-sm text-cf-text-secondary flex-1">{entry.as_name}</span>
        <span className="text-xs text-cf-text-muted">{entry.org_name}</span>
        <RirBadge rir={entry.rir} />
      </button>

      {/* Expanded content */}
      {open && (
        <div className="px-6 pb-5 bg-cf-surface-2/50">
          {loading && <div className="py-6 flex justify-center"><div className="spinner" /></div>}
          {roaData && !loading && (
            <>
              {/* Live ROA count */}
              <div className="flex items-center gap-4 mb-3 mt-2">
                <Badge color="green">{fmtNum(roaData.count)} live ROA prefixes</Badge>
                {history && <span className="text-xs text-cf-text-muted">{fmtNum(history.count)} historical entries</span>}
              </div>

              {/* Live ROA table */}
              {(roaData.results || []).length > 0 && (
                <div className="overflow-x-auto mb-4 rounded-lg border border-cf-border bg-cf-surface">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-cf-text-muted text-xs uppercase bg-cf-surface-2 tracking-wider">
                        <SortTh k="prefix" {...sort} onClick={sort.toggle}>Prefix</SortTh>
                        <SortTh k="max_length" {...sort} onClick={sort.toggle}>Max Len</SortTh>
                        <SortTh k="afi" {...sort} onClick={sort.toggle}>AFI</SortTh>
                        <SortTh k="first_seen" {...sort} onClick={sort.toggle}>First Seen</SortTh>
                        <SortTh k="last_seen" {...sort} onClick={sort.toggle}>Last Seen</SortTh>
                      </tr>
                    </thead>
                    <tbody>
                      {sort.sorted(roaData.results.slice(0, 50)).map((r, i) => (
                        <tr key={i} className={`border-b border-cf-border last:border-0 ${i % 2 === 1 ? 'bg-cf-surface-2' : ''}`}>
                          <td className="px-3 py-2 font-mono text-xs">
                            <button
                              onClick={(e) => { e.stopPropagation(); navigate(`/roa/lookup?prefix=${encodeURIComponent(r.prefix)}`); }}
                              className="text-cf-orange hover:underline"
                            >{r.prefix}</button>
                          </td>
                          <td className="px-3 py-2">/{r.max_length}</td>
                          <td className="px-3 py-2"><Badge color={r.afi === 4 ? 'blue' : 'amber'}>{r.afi === 4 ? 'v4' : 'v6'}</Badge></td>
                          <td className="px-3 py-2 text-xs text-cf-text-secondary">{fmtDate(r.first_seen)}</td>
                          <td className="px-3 py-2 text-xs text-cf-text-secondary">{fmtDate(r.last_seen)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {roaData.results.length > 50 && (
                    <div className="px-3 py-2 text-xs text-cf-text-muted bg-cf-surface-2">
                      Showing 50 of {fmtNum(roaData.count)} prefixes
                    </div>
                  )}
                </div>
              )}

              {/* Waterfall timeline */}
              {history?.entries?.length > 0 && (
                <div className="rounded-lg border border-cf-border bg-cf-surface p-4">
                  <div className="text-xs font-semibold text-cf-text mb-2">ROA Timeline</div>
                  <WaterfallChart entries={history.entries} />
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

/* ── Main Page ── */
export default function OrgLookup() {
  const [query, setQuery] = useState('Comcast');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Selected org family
  const [selectedOrg, setSelectedOrg] = useState(null);
  const [orgAsns, setOrgAsns] = useState(null);
  const [orgLoading, setOrgLoading] = useState(false);

  async function search() {
    if (!query) return;
    setLoading(true);
    setError(null);
    setSelectedOrg(null);
    setOrgAsns(null);
    try {
      // If it's a number, treat as ASN → get org family directly
      if (/^\d+$/.test(query.trim())) {
        const seedAsn = Number(query.trim());
        const data = await api.orgAsns({ asn: seedAsn });
        setSelectedOrg({ org_name: data.org_name, seed_asn: seedAsn });
        setOrgAsns(data.asns);
        setResults(null);
      } else {
        const data = await api.orgSearch({ q: query, limit: 20 });
        setResults(data || []);
      }
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }

  async function selectOrg(family) {
    const seedAsn = family.asn_entries?.[0]?.asn;
    if (!seedAsn) return;
    setOrgLoading(true);
    setSelectedOrg({ org_name: family.asn_entries[0]?.org_name, seed_asn: seedAsn });
    try {
      const data = await api.orgAsns({ asn: seedAsn });
      setOrgAsns(data.asns);
    } catch {}
    setOrgLoading(false);
  }

  return (
    <Page title="Organization Lookup" subtitle="Search by organization name or ASN — explore all ASNs in the org family">
      <FilterBar onSubmit={search}>
        <Input label="Organization or ASN" placeholder='e.g. "Comcast" or 7922' value={query} onChange={(e) => setQuery(e.target.value)} required />
        <Button type="submit" disabled={!query || loading}>Search</Button>
      </FilterBar>

      {loading && <Loading />}
      {error && <ErrorBox message={error} />}

      {/* Search results — org list to pick from */}
      {results && !selectedOrg && !loading && (
        <Card title={`${results.length} organizations found`}>
          {results.length === 0 ? (
            <Empty message="No organizations found." />
          ) : (
            <div className="divide-y divide-cf-border">
              {results.map((family, i) => {
                const entries = family.asn_entries || [];
                const mainOrg = entries[0]?.org_name || 'Unknown';
                return (
                  <button
                    key={i}
                    onClick={() => selectOrg(family)}
                    className="w-full flex items-center justify-between px-4 py-3 hover:bg-cf-surface-2 transition-colors text-left"
                  >
                    <div>
                      <div className="text-sm font-semibold text-cf-text">{mainOrg}</div>
                      <div className="text-xs text-cf-text-muted mt-0.5">
                        {entries.length} ASN{entries.length !== 1 ? 's' : ''}: {entries.slice(0, 5).map((e) => `AS${e.asn}`).join(', ')}
                        {entries.length > 5 && ` + ${entries.length - 5} more`}
                      </div>
                    </div>
                    <span className="text-cf-orange text-sm">&#9654;</span>
                  </button>
                );
              })}
            </div>
          )}
        </Card>
      )}

      {/* Selected org — ASN list with expandable rows */}
      {orgLoading && <Loading />}
      {selectedOrg && orgAsns && !orgLoading && (
        <Card title={
          <div className="flex items-center gap-3">
            <button onClick={() => { setSelectedOrg(null); setOrgAsns(null); }} className="text-cf-orange hover:text-cf-orange-hover text-sm">
              &#9664; Back
            </button>
            <span>{selectedOrg.org_name}</span>
            <Badge color="slate">{orgAsns.length} ASNs</Badge>
          </div>
        }>
          <div className="divide-y-0">
            {orgAsns.map((entry) => (
              <AsnRow key={entry.asn} entry={entry} />
            ))}
          </div>
        </Card>
      )}

      {!results && !selectedOrg && !loading && !error && (
        <Empty message='Search for an organization (e.g. "Comcast") or enter an ASN (e.g. 7922) to find its org family.' />
      )}
    </Page>
  );
}
