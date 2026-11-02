const BASE = import.meta.env.VITE_API_URL || '';

function buildUrl(path, params = {}) {
  const url = new URL(`${BASE}${path}`, window.location.origin);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined && v !== '') url.searchParams.set(k, v);
  });
  return url.pathname + url.search;
}

async function get(path, params) {
  const res = await fetch(buildUrl(path, params));
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} — ${text.slice(0, 200)}`);
  }
  return res.json();
}

export const api = {
  // ROA
  roaLookup:      (p) => get('/api/roa/lookup', p),
  roaByAsn:       (p) => get('/api/roa/by-asn', p),
  roaHistory:     (p) => get('/api/roa/history', p),
  roaCoverage:    (p) => get('/api/roa/coverage', p),
  roaCoverageTrend: (p) => get('/api/roa/coverage-trend', p),
  orgSearch:      (p) => get('/api/roa/org-search', p),
  orgAsns:        (p) => get('/api/roa/org-asns', p),
  orgCoverage:    (p) => get('/api/roa/org-coverage', p),

  // Analytics
  churn:          (p) => get('/api/analytics/churn', p),
  lifespan:       (p) => get('/api/analytics/lifespan', p),
  signingCadence: (p) => get('/api/analytics/signing-cadence', p),
  crlStaleness:   (p) => get('/api/analytics/crl-staleness', p),
  maxLength:      (p) => get('/api/analytics/maxlength-issues', p),
  rirComparison:  (p) => get('/api/analytics/rir-comparison', p),

  // Analysis: country ROA deployment
  countryMeta:     (p) => get('/api/analytics/country/meta', p),
  countryList:     (p) => get('/api/analytics/country/countries', p),
  countryRirTrend: (p) => get('/api/analytics/country/rir-trend', p),
  countryTrend:    (p) => get('/api/analytics/country/trend', p),
  countryAses:     (p) => get('/api/analytics/country/ases', p),
  countryAsTrend:  (p) => get('/api/analytics/country/as-trend', p),

  // CA
  caRoots:        (p) => get('/api/ca/roots', p),
  caInfo:         (p) => get('/api/ca/info', p),
  caChildren:     (p) => get('/api/ca/children', p),
  caStats:        (p) => get('/api/ca/stats', p),
  caByResource:   (p) => get('/api/ca/by-resource', p),

  // ASPA
  aspaList:       (p) => get('/api/aspa/list', p),
  aspaStats:      ()  => get('/api/aspa/stats'),
  aspaTrend:      (p) => get('/api/aspa/trend', p),

  // Alerts
  recentChanges:  (p) => get('/api/alerts/recent', p),

  // Objects & Meta
  objectSummary:  (p) => get('/api/objects/summary', p),
  health:         ()  => get('/api/health'),

  // RP-validity
  validitySummary: (p) => get('/api/validity/summary', p),
  validityObjects: (p) => get('/api/validity/objects', p),

  // Admin (visitor analytics) — token sent via X-Admin-Token header
  adminStats: async (token, params = {}) => {
    const res = await fetch(buildUrl('/api/admin/stats', params), {
      headers: { 'X-Admin-Token': token },
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`${res.status} — ${text.slice(0, 200)}`);
    }
    return res.json();
  },
};
