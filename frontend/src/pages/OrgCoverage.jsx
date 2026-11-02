import { useState } from 'react';
import { api } from '../api';
import {
  Page, Card, FilterBar, Input, Button, DataTable, ErrorBox, Empty, Stat, Badge,
  fmtNum,
} from '../components/ui';

function coverageColor(pct) {
  if (pct >= 90) return 'green';
  if (pct >= 50) return 'amber';
  return 'red';
}

export default function OrgCoverage() {
  const [orgQuery, setOrgQuery] = useState('Comcast');
  const [orgData, setOrgData] = useState(null);
  const [orgLoading, setOrgLoading] = useState(false);
  const [orgError, setOrgError] = useState(null);

  async function searchOrg() {
    if (!orgQuery) return;
    setOrgLoading(true);
    setOrgError(null);
    setOrgData(null);
    try {
      const seedAsn = /^\d+$/.test(orgQuery.trim()) ? Number(orgQuery.trim()) : null;
      if (seedAsn) {
        const res = await api.orgCoverage({ asn: seedAsn });
        setOrgData(res);
      } else {
        const results = await api.orgSearch({ q: orgQuery, limit: 20 });
        if (!results || results.length === 0) {
          setOrgError('No organizations found');
          setOrgLoading(false);
          return;
        }
        const best = results.reduce((a, b) =>
          (b.asn_entries?.length || 0) > (a.asn_entries?.length || 0) ? b : a
        , results[0]);
        const seedAsn2 = best?.asn_entries?.[0]?.asn;
        if (!seedAsn2) {
          setOrgError('No ASNs found for this organization');
          setOrgLoading(false);
          return;
        }
        const res = await api.orgCoverage({ asn: seedAsn2 });
        setOrgData(res);
      }
    } catch (e) {
      setOrgError(e.message);
    }
    setOrgLoading(false);
  }

  return (
    <Page title="Organization Coverage" subtitle="RPKI signing coverage across all ASNs in an organization family">
      <FilterBar onSubmit={searchOrg}>
        <Input label="Organization" placeholder="Org name or seed ASN (e.g. Cloudflare, 4766)" value={orgQuery} onChange={(e) => setOrgQuery(e.target.value)} required />
        <Button type="submit" disabled={!orgQuery || orgLoading}>
          {orgLoading ? 'Loading...' : 'Lookup'}
        </Button>
      </FilterBar>

      <p className="-mt-2 mb-6 text-xs text-cf-text-muted">
        Organization-to-ASN mapping is provided by{' '}
        <a
          href="https://asint.netsecurelab.org"
          target="_blank"
          rel="noopener noreferrer"
          className="text-cf-orange hover:underline"
        >
          ASINT
        </a>
        . Enter an organization name (e.g. Comcast) or a seed ASN — we resolve the full
        org family and compute RPKI coverage across every ASN in it.
      </p>

      {orgLoading && (
        <div className="text-center py-12">
          <div className="spinner mx-auto mb-3" />
          <div className="text-sm text-cf-text-muted">Finding org family and computing coverage...</div>
        </div>
      )}
      {orgError && <ErrorBox message={orgError} />}

      {orgData && !orgLoading && (
        <Card className="mb-6">
          <>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
              {/* Full width on phones. Stat breaks values anywhere (break-all), which split long
                  names mid-word ("Comcast Ca|ble Commun|ications LLC"); wrap names at spaces. */}
              <div className="col-span-2 lg:col-span-1 [&>div]:h-full">
                <Stat
                  label="Organization"
                  value={(
                    <span className="block text-lg lg:text-xl leading-snug [word-break:normal] [overflow-wrap:anywhere]">
                      {orgData.org_name || '—'}
                    </span>
                  )}
                />
              </div>
              <Stat label="ASNs in Family" value={orgData.asn_count} />
              <Stat label="Announced Prefixes" value={orgData.total_announced > 0 ? fmtNum(orgData.total_announced) : 'BGP unavailable'} />
              <Stat
                label="Org Coverage"
                value={<Badge color={coverageColor(orgData.org_coverage_pct)}>{orgData.org_coverage_pct}%</Badge>}
              />
            </div>
            <DataTable
              columns={[
                { key: 'asn', label: 'ASN', className: 'font-mono', render: (v) => `AS${v}` },
                { key: 'as_name', label: 'AS Name' },
                { key: 'rir', label: 'RIR', render: (v) => <Badge color={v === 'apnic' ? 'blue' : v === 'arin' ? 'green' : v === 'ripe' ? 'amber' : 'slate'}>{v}</Badge> },
                { key: 'announced', label: 'Announced', render: (v) => v > 0 ? fmtNum(v) : <span className="text-cf-text-muted text-xs">N/A</span> },
                { key: 'signed', label: 'Signed', render: (v) => v > 0 ? <span className="text-emerald-600 font-semibold">{fmtNum(v)}</span> : <span className="text-cf-text-muted">0</span> },
                { key: 'roa_total', label: 'ROA Total', render: (v) => v > 0 ? <span className="text-cf-orange font-semibold">{fmtNum(v)}</span> : <span className="text-cf-text-muted">0</span> },
                {
                  key: 'coverage_pct', label: 'Coverage',
                  render: (v, row) => row.announced > 0
                    ? <Badge color={coverageColor(v)}>{v}%</Badge>
                    : <span className="text-cf-text-muted">—</span>,
                },
              ]}
              rows={orgData.per_asn || []}
            />
          </>
        </Card>
      )}

      {!orgData && !orgLoading && !orgError && (
        <Empty message="Enter an organization name or seed ASN to see RPKI coverage across all ASNs in the org family." />
      )}
    </Page>
  );
}
