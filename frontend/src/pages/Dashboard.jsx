import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { api } from '../api';
import {
  Page, Stat, Card, Loading, ErrorBox, DataTable, RirBadge, Badge,
  fmtNum, timeAgo, RIRS, RIR_COLORS, chartTickStyle, chartAxisStyle, chartTooltipStyle,
} from '../components/ui';

/** Parse snapshot timestamps like "2026-05-29 00-00-01" into valid Date strings */
function parseSnapshotTs(ts) {
  if (!ts) return '';
  return ts.replace(/ /, 'T').replace(/-(\d{2})-(\d{2})$/, ':$1:$2');
}

export default function Dashboard() {
  const [health, setHealth] = useState(null);
  const [healthLoading, setHealthLoading] = useState(true);
  const [healthError, setHealthError] = useState(null);

  const [inventory, setInventory] = useState(null);
  const [invLoading, setInvLoading] = useState(false);

  const [validity, setValidity] = useState(null);

  // Load health first (fast ~30ms)
  useEffect(() => {
    api.health()
      .then(setHealth)
      .catch((e) => setHealthError(e.message))
      .finally(() => setHealthLoading(false));
  }, []);

  // RP-validity summary (all RIRs) — lightweight, non-blocking
  useEffect(() => {
    api.validitySummary().then(setValidity).catch(() => setValidity(null));
  }, []);

  // Load inventory sequentially to avoid overloading DuckDB
  useEffect(() => {
    if (!health) return;
    setInvLoading(true);
    (async () => {
      const results = [];
      for (const rir of RIRS) {
        try {
          const d = await api.objectSummary({ rir });
          results.push({ rir, ...d });
        } catch {}
      }
      setInventory(results);
      setInvLoading(false);
    })();
  }, [health]);

  if (healthLoading) return <Loading />;
  if (healthError) return <ErrorBox message={healthError} />;

  const warehouses = health.warehouses || [];

  // Build chart data from inventory
  const objectTypes = ['ROA', 'Certificate', 'CRL', 'Manifest', 'ASPA'];
  const chartData = inventory ? objectTypes.map((type) => {
    const row = { type };
    inventory.forEach((inv) => {
      const obj = inv.objects?.find((o) => o.object_type === type);
      row[inv.rir] = obj?.count || 0;
    });
    return row;
  }) : [];

  return (
    <Page title="Dashboard" subtitle="RPKI Warehouse overview and health status">
      {/* Intro */}
      <div className="mb-6 bg-cf-orange-light border border-rose-200 rounded-xl px-5 py-4">
        <div className="text-sm font-semibold text-cf-text mb-1.5">RPKI Warehouse</div>
        <p className="text-sm text-cf-text-secondary leading-relaxed">
          A longitudinal observatory for RPKI objects across all five RIRs, from 2012 to today. Look up
          ROAs, follow object lifecycles, measure RPKI signing coverage by AS and organization, and
          compare ROA deployment across countries and RIRs. Our rpki-client collection runs every
          20 minutes; history before April 2026 comes from the RIPE NCC RPKI archive.
        </p>
      </div>

      {/* Stat cards — render immediately from /health */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-2">
        <Stat label="Live ROAs" value={fmtNum(health.live_roa_total)} color="text-cf-orange" sub="Distinct ROA objects" />
        <Stat label="Live VRPs" value={fmtNum(health.live_vrp_total)} sub="Distinct (ASN, prefix, max-length)" />
        <Stat label="Warehouses" value={warehouses.length} />
        <Stat
          label="Latest Snapshot"
          value={warehouses.length > 0 ? timeAgo(parseSnapshotTs(warehouses.reduce((latest, w) =>
            w.latest_snapshot > latest ? w.latest_snapshot : latest, ''
          ))) : '—'}
        />
        <Stat
          label="Total Snapshots"
          value={fmtNum(warehouses.reduce((sum, w) => sum + (w.snapshot_count || 0), 0))}
        />
      </div>
      <p className="text-xs text-cf-text-muted mb-6">
        A <strong>ROA</strong> is a single signed object; a <strong>VRP</strong> is one
        (ASN, prefix, max-length) payload. One ROA can carry several VRPs.
      </p>

      {/* Object inventory chart — loads lazily */}
      <Card title="Object Inventory by RIR" className="mb-6">
        {invLoading ? (
          <div className="flex items-center justify-center h-[320px]">
            <div className="text-center">
              <div className="spinner mx-auto mb-3" />
              <div className="text-sm text-cf-text-muted">Loading inventory across 5 RIRs...</div>
            </div>
          </div>
        ) : chartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={chartData} margin={{ top: 10, right: 20, bottom: 5, left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f3" vertical={false} />
              <XAxis dataKey="type" tick={chartTickStyle} axisLine={chartAxisStyle} tickLine={false} />
              <YAxis tick={chartTickStyle} axisLine={false} tickLine={false} tickFormatter={fmtNum} />
              <Tooltip {...chartTooltipStyle} formatter={(v) => fmtNum(v)} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              {RIRS.map((rir) => (
                <Bar key={rir} dataKey={rir} fill={RIR_COLORS[rir]} radius={[3, 3, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        ) : null}
      </Card>

      {/* RP-validity — repo-present vs RP-valid split */}
      {validity && (
        <Card
          title="RP-Validity"
          className="mb-6"
          action={
            <Link to="/validity" className="text-xs font-medium text-cf-orange hover:underline">
              View invalid objects &rarr;
            </Link>
          }
        >
          <p className="text-sm text-cf-text-secondary mb-4">
            The warehouse ingests every object rpki-client caches (<strong>repo-present</strong>).
            <strong> RP-valid</strong> excludes the objects rpki-client rejected in the latest
            validation cycle. ROA lookups default to RP-valid only.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-4">
            <Stat label="Repo-present" value={fmtNum(validity.repo_present)} sub="All cached objects" />
            <Stat label="RP-valid" value={fmtNum(validity.rp_valid)} color="text-emerald-600" sub="Passed validation" />
            <Stat label="Invalid" value={fmtNum(validity.invalid)} color="text-red-600" sub="Rejected this cycle" />
          </div>
          {validity.by_reason && validity.by_reason.length > 0 ? (
            <div className="flex flex-wrap gap-2">
              {validity.by_reason.map((r) => (
                <Link
                  key={r.reason_code}
                  to={`/validity?reason=${encodeURIComponent(r.reason_code)}`}
                  className="inline-flex items-center gap-1.5 rounded-full bg-red-50 hover:bg-red-100 px-3 py-1 text-xs font-medium text-red-700"
                >
                  <span className="font-mono">{r.reason_code}</span>
                  <span className="text-red-500">{fmtNum(r.count)}</span>
                </Link>
              ))}
            </div>
          ) : (
            <div className="text-xs text-cf-text-muted">
              No invalid objects in the latest cycle (or no validation cycle has run yet).
            </div>
          )}
          {validity.latest_validation_ts && (
            <div className="text-[11px] text-cf-text-muted mt-3">
              Latest validation: <span className="font-mono">{validity.latest_validation_ts}</span>
            </div>
          )}
        </Card>
      )}

      {/* Warehouse status table — from /health, renders immediately */}
      <Card title="Warehouse Status">
        <DataTable
          columns={[
            { key: 'rir', label: 'RIR', render: (v) => <RirBadge rir={v} /> },
            { key: 'latest_snapshot', label: 'Latest Snapshot', render: (v) => <span className="font-mono text-xs">{v}</span> },
            { key: 'snapshot_count', label: 'Snapshots', render: (v) => fmtNum(v) },
            { key: 'live_roa_count', label: 'Live ROAs', render: (v) => fmtNum(v) },
            { key: 'live_vrp_count', label: 'Live VRPs', render: (v) => fmtNum(v) },
            { key: 'unique_object_count', label: 'Unique Objects', render: (v) => fmtNum(v) },
            { key: 'object_count', label: 'Total Objects', render: (v) => fmtNum(v) },
            {
              key: '_status', label: 'Status',
              render: (_, row) => {
                const age = (Date.now() - new Date(parseSnapshotTs(row.latest_snapshot)).getTime()) / 3600000;
                return age < 24 ? <Badge color="green">Fresh</Badge>
                     : age < 48 ? <Badge color="amber">Stale</Badge>
                     : <Badge color="red">Outdated</Badge>;
              },
            },
          ]}
          rows={warehouses}
        />
      </Card>

      {/* Raw data download */}
      <Card title="Download Raw Data" className="mt-6">
        <div className="p-5">
          <p className="text-sm text-cf-text-secondary mb-4">
            Export the current set of live <strong>VRPs</strong> (distinct ASN, prefix, max-length).
            Available as CSV or JSON, all RIRs or per-RIR.
          </p>
          <div className="flex flex-wrap items-center gap-2">
            <a
              href="/api/roa/export?format=csv"
              className="rounded-lg px-3 py-2 text-sm font-semibold bg-cf-orange hover:bg-cf-orange-hover text-white"
            >
              All RIRs (CSV)
            </a>
            <a
              href="/api/roa/export?format=json"
              className="rounded-lg px-3 py-2 text-sm font-semibold bg-cf-surface border border-cf-border hover:bg-cf-surface-2 text-cf-text"
            >
              All RIRs (JSON)
            </a>
            {RIRS.map((rir) => (
              <a
                key={rir}
                href={`/api/roa/export?format=csv&rir=${rir}`}
                className="rounded-lg px-3 py-2 text-xs font-medium bg-cf-surface border border-cf-border hover:bg-cf-surface-2 text-cf-text-secondary font-mono"
              >
                {rir} CSV
              </a>
            ))}
          </div>
        </div>
      </Card>
    </Page>
  );
}
