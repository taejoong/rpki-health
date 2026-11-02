import { useEffect, useState } from 'react';
import { api } from '../api';
import {
  Page, Card, FilterBar, Select, Input, Button, DataTable, Loading, ErrorBox, Empty, Stat, Badge,
  RirBadge, fmtNum, fmtDate, RIRS,
} from '../components/ui';

const rirOptions = RIRS.map((r) => ({ value: r, label: r }));

// Expiry signal for operators: the whole validation path fails if any cert
// expires, so surface CA certs that are expired or expiring within 30 days.
function expiryBadge(notAfter) {
  if (!notAfter) return <span className="text-cf-text-muted">—</span>;
  const d = new Date(String(notAfter).replace(' ', 'T'));
  const days = Math.floor((d.getTime() - Date.now()) / 86400000);
  if (Number.isNaN(days)) return <span className="font-mono text-xs">{String(notAfter).slice(0, 10)}</span>;
  if (days < 0) return <Badge color="red">expired</Badge>;
  if (days <= 30) return <Badge color="amber">D-{days}</Badge>;
  return <span className="font-mono text-xs">{String(notAfter).slice(0, 10)}</span>;
}

export default function CaExplorer() {
  const [rir, setRir] = useState('arin');
  const [ski, setSki] = useState('');
  const [certId, setCertId] = useState('');
  const [ca, setCa] = useState(null);
  const [children, setChildren] = useState(null);
  const [caStats, setCaStats] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [breadcrumbs, setBreadcrumbs] = useState([]);
  const [roots, setRoots] = useState(null);
  const [rootsLoading, setRootsLoading] = useState(false);
  const [asn, setAsn] = useState('');
  const [byResource, setByResource] = useState(null);
  const [brLoading, setBrLoading] = useState(false);
  const [brError, setBrError] = useState(null);
  const [tab, setTab] = useState('asn'); // 'asn' | 'browse'

  // Load the RIR's trust-anchor root(s) as a no-input entry point.
  useEffect(() => {
    let cancelled = false;
    setRootsLoading(true);
    api.caRoots({ rir })
      .then((r) => { if (!cancelled) setRoots(r); })
      .catch(() => { if (!cancelled) setRoots([]); })
      .finally(() => { if (!cancelled) setRootsLoading(false); });
    return () => { cancelled = true; };
  }, [rir]);

  function openRoot(root) {
    setSki('');
    setCertId(String(root.cert_id));
    setBreadcrumbs([]);
    lookup(rir, '', String(root.cert_id), true);
  }

  async function lookup(lookupRir, lookupSki, lookupCertId, addBreadcrumb = true) {
    setLoading(true);
    setError(null);
    setChildren(null);
    setCaStats(null);
    try {
      const params = {
        rir: lookupRir,
        ski: lookupSki || undefined,
        cert_id: lookupCertId || undefined,
      };
      const info = await api.caInfo(params);
      setCa(info);

      if (addBreadcrumb) {
        setBreadcrumbs((prev) => [
          ...prev,
          { rir: lookupRir, cert_id: info.cert_id, subject_dn: info.subject_dn },
        ]);
      }

      // Load children and stats in parallel
      if (info.cert_id) {
        const [ch, st] = await Promise.all([
          api.caChildren({ rir: lookupRir, cert_id: info.cert_id }).catch(() => null),
          api.caStats({ rir: lookupRir, cert_id: info.cert_id }).catch(() => null),
        ]);
        setChildren(ch);
        setCaStats(st);
      }
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }

  function handleSearch() {
    setBreadcrumbs([]);
    lookup(rir, ski, certId, true);
  }

  async function searchByResource() {
    const n = parseInt(asn, 10);
    if (!Number.isFinite(n)) return;
    setBrLoading(true);
    setBrError(null);
    setByResource(null);
    try {
      const r = await api.caByResource({ asn: n });
      setByResource(r);
    } catch (e) {
      setBrError(e.message);
    }
    setBrLoading(false);
  }

  function jumpToCa(entry) {
    setTab('browse');
    setRir(entry.rir);
    setSki('');
    setCertId(String(entry.cert_id));
    setBreadcrumbs([]);
    lookup(entry.rir, '', String(entry.cert_id), true);
  }

  function drillDown(child) {
    setCertId(String(child.cert_id));
    setSki('');
    lookup(rir, '', String(child.cert_id), true);
  }

  function goToBreadcrumb(idx) {
    const bc = breadcrumbs[idx];
    setBreadcrumbs(breadcrumbs.slice(0, idx));
    lookup(bc.rir, '', String(bc.cert_id), true);
  }

  return (
    <Page title="CA Explorer" subtitle="Browse the RPKI certificate authority hierarchy">
      {/* Tab bar: separate the operator entry point (ASN lookup) from raw hierarchy browsing */}
      <div className="flex gap-1 border-b border-cf-border mb-6">
        {[
          { id: 'asn', label: 'Find by ASN' },
          { id: 'browse', label: 'Browse Hierarchy' },
        ].map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
              tab === t.id
                ? 'border-cf-orange text-cf-orange'
                : 'border-transparent text-cf-text-muted hover:text-cf-text'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab: Find by ASN — reverse lookup, the operator-facing entry point. */}
      {tab === 'asn' && (
        <Card title="Find CA by ASN">
          <div className="flex items-end gap-3 flex-wrap">
            <Input
              label="ASN"
              placeholder="e.g. 13335"
              type="number"
              value={asn}
              onChange={(e) => setAsn(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') searchByResource(); }}
            />
            <Button onClick={searchByResource} disabled={brLoading || !asn}>Find covering CAs</Button>
          </div>
          <p className="mt-2 text-xs text-cf-text-muted">
            Find the CA(s) that issued the live ROAs covering an ASN. Click a row to open that CA in the hierarchy.
          </p>

          {brLoading && <Loading />}
          {brError && <ErrorBox message={brError} />}
          {byResource && !brLoading && (
            byResource.results.length > 0 ? (
              <div className="mt-4">
                <DataTable
                  columns={[
                    { key: 'rir', label: 'RIR', render: (v) => <RirBadge rir={v} /> },
                    { key: 'subject_dn', label: 'Issuing CA', className: 'font-mono text-xs' },
                    { key: 'prefix_count', label: 'Prefixes Covered', render: fmtNum },
                    { key: 'not_after', label: 'CA Expiry', render: expiryBadge },
                    {
                      key: 'all_valid', label: 'ROAs',
                      render: (v) => (v ? <Badge color="green">all valid</Badge> : <Badge color="red">has invalid</Badge>),
                    },
                  ]}
                  rows={byResource.results}
                  onRowClick={jumpToCa}
                  emptyMsg="No covering CA."
                />
              </div>
            ) : (
              <Empty message={`No live RPKI ROA covers AS${byResource.asn} — this ASN has no RPKI-issuing CA. Consider creating ROAs.`} />
            )
          )}
        </Card>
      )}

      {/* Tab: Browse Hierarchy — raw cert lookup + trust-anchor drill-down. */}
      {tab === 'browse' && (
      <>
      <FilterBar onSubmit={handleSearch}>
        <Select label="RIR" value={rir} onChange={(e) => setRir(e.target.value)} options={rirOptions} />
        <Input label="SKI" placeholder="Subject Key Identifier" value={ski} onChange={(e) => setSki(e.target.value)} />
        <Input label="Cert ID" placeholder="Certificate ID" type="number" value={certId} onChange={(e) => setCertId(e.target.value)} />
        <Button type="submit" disabled={loading || (!ski && !certId)}>Lookup</Button>
      </FilterBar>

      {/* Breadcrumbs */}
      {breadcrumbs.length > 1 && (
        <div className="flex items-center gap-1 mb-4 text-sm flex-wrap">
          {breadcrumbs.map((bc, i) => (
            <span key={i} className="flex items-center gap-1">
              {i > 0 && <span className="text-cf-text-muted mx-1">/</span>}
              {i < breadcrumbs.length - 1 ? (
                <button onClick={() => goToBreadcrumb(i)} className="text-cf-orange hover:underline">
                  {bc.subject_dn || `cert:${bc.cert_id}`}
                </button>
              ) : (
                <span className="font-semibold text-cf-text">{bc.subject_dn || `cert:${bc.cert_id}`}</span>
              )}
            </span>
          ))}
        </div>
      )}

      {loading && <Loading />}
      {error && <ErrorBox message={error} />}

      {ca && !loading && (
        <div className="space-y-6">
          {/* CA Info */}
          <Card title="CA Certificate Info">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 text-sm">
              <div><span className="text-cf-text-muted">Subject DN:</span> <span className="font-mono">{ca.subject_dn}</span></div>
              <div><span className="text-cf-text-muted">Cert ID:</span> <span className="font-mono">{ca.cert_id}</span></div>
              <div><span className="text-cf-text-muted">SKI:</span> <span className="font-mono text-xs">{ca.ski}</span></div>
              <div><span className="text-cf-text-muted">AKI:</span> <span className="font-mono text-xs">{ca.aki || 'none (root)'}</span></div>
              <div><span className="text-cf-text-muted">Not Before:</span> {fmtDate(ca.not_before)}</div>
              <div><span className="text-cf-text-muted">Not After:</span> {fmtDate(ca.not_after)}</div>
              <div><span className="text-cf-text-muted">First Seen:</span> {fmtDate(ca.first_seen)}</div>
              <div><span className="text-cf-text-muted">Last Seen:</span> {fmtDate(ca.last_seen)}</div>
              {ca.resources_ip && <div className="col-span-2"><span className="text-cf-text-muted">IP Resources:</span> <span className="font-mono text-xs">{ca.resources_ip}</span></div>}
              {ca.resources_asn && <div className="col-span-2"><span className="text-cf-text-muted">ASN Resources:</span> <span className="font-mono text-xs">{ca.resources_asn}</span></div>}
            </div>
          </Card>

          {/* CA Stats */}
          {caStats?.by_type && Object.keys(caStats.by_type).length > 0 && (
            <Card title="Signed Object Counts">
              <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
                {Object.entries(caStats.by_type).map(([type, count]) => (
                  <Stat key={type} label={type} value={fmtNum(count)} />
                ))}
              </div>
            </Card>
          )}

          {/* Children */}
          {children && (
            <Card title={`Child Certificates (${children.length || 0})`}>
              <DataTable
                columns={[
                  { key: 'cert_id', label: 'Cert ID', className: 'font-mono' },
                  { key: 'subject_dn', label: 'Subject DN', className: 'font-mono text-xs' },
                  {
                    key: 'ski', label: 'SKI',
                    render: (v) => <span className="font-mono text-xs">{v?.slice(0, 16)}...</span>,
                  },
                  { key: 'first_seen', label: 'First Seen', render: fmtDate },
                  { key: 'last_seen', label: 'Last Seen', render: fmtDate },
                ]}
                rows={children || []}
                onRowClick={drillDown}
                emptyMsg="No child certificates."
              />
            </Card>
          )}
        </div>
      )}

      {/* Entry point: trust-anchor roots for the selected RIR */}
      {!ca && !loading && (
        <Card title={`${rir.toUpperCase()} Trust Anchor — start here`}>
          {rootsLoading && <Loading />}
          {!rootsLoading && roots && roots.length > 0 && (
            <DataTable
              columns={[
                { key: 'subject_dn', label: 'Root CA', className: 'font-mono text-xs' },
                {
                  key: 'ski', label: 'SKI',
                  render: (v) => <span className="font-mono text-xs">{v?.slice(0, 16)}...</span>,
                },
                { key: 'first_seen', label: 'First Seen', render: fmtDate },
                { key: 'last_seen', label: 'Last Seen', render: fmtDate },
              ]}
              rows={roots}
              onRowClick={openRoot}
              emptyMsg="No root CA found."
            />
          )}
          {!rootsLoading && roots && roots.length === 0 && (
            <Empty message="No trust-anchor root found for this RIR. Use the advanced lookup above." />
          )}
          <p className="mt-3 text-xs text-cf-text-muted">
            Click a root to drill into its children. Know the exact SKI or Cert ID? Use the advanced lookup above.
          </p>
        </Card>
      )}
      </>
      )}
    </Page>
  );
}
