import { useState, useEffect, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { api } from '../api';
import {
  Page, Card, FilterBar, Select, Button, Loading, ErrorBox, Empty,
  DataTable, RirBadge, Badge, Stat, fmtNum, fmtDate, RIRS,
} from '../components/ui';

const rirOptions = [{ value: '', label: 'All RIRs' }, ...RIRS.map((r) => ({ value: r, label: r }))];

const typeBadgeColor = {
  ROA: 'orange', Certificate: 'blue', Manifest: 'amber', ASPA: 'maroon', CRL: 'green',
};

export default function ValidityObjects() {
  const [params, setParams] = useSearchParams();

  const [rir, setRir] = useState('');
  const [reason, setReason] = useState(params.get('reason') || '');
  const [offset, setOffset] = useState(0);

  const [summary, setSummary] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const limit = 500;

  // Summary drives the reason-filter dropdown options + headline counts.
  useEffect(() => {
    api.validitySummary({ rir: rir || undefined })
      .then(setSummary)
      .catch(() => setSummary(null));
  }, [rir]);

  const reasonOptions = [
    { value: '', label: 'All reasons' },
    ...((summary?.by_reason || []).map((r) => ({
      value: r.reason_code,
      label: `${r.reason_code} (${fmtNum(r.count)})`,
    }))),
  ];

  const search = useCallback(async (newOffset = 0, opts = {}) => {
    const useRir = opts.rir !== undefined ? opts.rir : rir;
    const useReason = opts.reason !== undefined ? opts.reason : reason;
    setLoading(true);
    setError(null);
    setOffset(newOffset);
    try {
      const res = await api.validityObjects({
        rir: useRir || undefined,
        reason: useReason || undefined,
        limit,
        offset: newOffset,
      });
      setData(res);
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }, [rir, reason]);

  // Initial load (and whenever the URL ?reason= changes from outside).
  useEffect(() => {
    search(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function onSubmit() {
    // Keep the URL in sync so the view is shareable / matches Dashboard links.
    const next = {};
    if (reason) next.reason = reason;
    setParams(next, { replace: true });
    search(0);
  }

  const columns = [
    { key: 'rir', label: 'RIR', render: (v) => <RirBadge rir={v} /> },
    {
      key: 'object_type', label: 'Type',
      render: (v) => <Badge color={typeBadgeColor[v] || 'slate'}>{v}</Badge>,
    },
    {
      key: 'reason_code', label: 'Reason',
      render: (v) => <span className="font-mono text-xs text-red-700">{v}</span>,
    },
    { key: 'detail', label: 'Identifier', render: (v) => <span className="font-mono text-xs">{v || '—'}</span> },
    { key: 'first_seen', label: 'First Seen', render: (v) => <span className="text-cf-text-secondary">{fmtDate(v)}</span> },
    { key: 'last_seen', label: 'Last Seen', render: (v) => <span className="text-cf-text-secondary">{fmtDate(v)}</span> },
    {
      key: 'object_hash', label: 'Hash',
      render: (v) => v ? <span className="font-mono text-xs text-cf-text-muted">{v.slice(0, 12)}…</span> : '—',
    },
  ];

  return (
    <Page
      title="RP-Invalid Objects"
      subtitle="Objects rpki-client rejected in the latest validation cycle, with reason"
    >
      {summary && (
        <div className="grid grid-cols-3 gap-4 mb-6">
          <Stat label="Repo-present" value={fmtNum(summary.repo_present)} sub="All cached objects" />
          <Stat label="RP-valid" value={fmtNum(summary.rp_valid)} color="text-emerald-600" sub="Passed validation" />
          <Stat label="Invalid" value={fmtNum(summary.invalid)} color="text-red-600" sub="Rejected this cycle" />
        </div>
      )}

      <FilterBar onSubmit={onSubmit}>
        <Select label="RIR" value={rir} onChange={(e) => setRir(e.target.value)} options={rirOptions} />
        <Select label="Reason" value={reason} onChange={(e) => setReason(e.target.value)} options={reasonOptions} />
        <Button type="submit" disabled={loading}>Filter</Button>
      </FilterBar>

      {loading && <Loading />}
      {error && <ErrorBox message={error} />}
      {data && !loading && (
        <>
          <div className="mb-4 text-sm text-cf-text-secondary">
            <strong className="text-cf-text">{fmtNum(data.count)}</strong> invalid object
            {data.count === 1 ? '' : 's'}
            {data.reason ? <> with reason <code className="font-mono bg-cf-surface-2 px-1.5 py-0.5 rounded">{data.reason}</code></> : ''}
            {data.rir !== 'all' ? <> in <code className="font-mono bg-cf-surface-2 px-1.5 py-0.5 rounded">{data.rir}</code></> : ''}
          </div>
          {data.entries.length > 0 ? (
            <Card>
              <DataTable columns={columns} rows={data.entries} />
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
          ) : (
            <Empty message="No invalid objects — either everything passed validation, or no validation cycle has run yet." />
          )}
        </>
      )}
    </Page>
  );
}
