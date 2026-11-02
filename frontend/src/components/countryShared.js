import { useEffect, useState } from 'react';
import { api } from '../api';
import { RIR_COLORS } from './ui';

/* Shared definitions for the Country analysis page (/analysis/countries). */

export const BGP_FROM = '2020-01-01';

/* Perspectives ("lens"): deployment % = num / den. Colors are a validated adjacent order. */
export const LENSES = [
  {
    id: 'routes', label: 'Routes', num: 'routes_valid', den: 'routes', color: '#2a78d6', bgp: true,
    short: 'Routes valid', what: 'share of visible routes that are RPKI-valid', unit: 'routes',
  },
  {
    id: 'space', label: 'Routed address space', num: 'space_valid', den: 'space', color: '#eb6834', bgp: true,
    short: 'Routed space valid', what: 'share of routed address space that is RPKI-valid', unit: 'space',
  },
  {
    id: 'registered', label: 'Registered address space', num: 'deleg_cov', den: 'deleg', color: '#6250d6', bgp: false,
    short: 'Registered space covered', what: 'share of registered address space covered by a ROA, incl. AS0', unit: 'space',
  },
  {
    id: 'ases', label: 'Origin ASes', num: 'ases_any_valid', den: 'ases', color: '#1baf7a', bgp: true,
    short: 'ASes with a valid route', what: 'share of origin ASes with at least one valid route', unit: 'ases',
  },
];
export const LENS_BY_ID = Object.fromEntries(LENSES.map((l) => [l.id, l]));

/* RIR line colors: RIR_COLORS, except ARIN stepped to #8f2248 (the brand maroon fails the
   lightness band); "All RIRs" in the de-emphasis gray. */
export const RIR_LINE_COLORS = { ...RIR_COLORS, arin: '#8f2248' };
export const ALL_RIRS_COLOR = '#6b7280';
export const RIR_NAMES = {
  afrinic: 'AFRINIC', apnic: 'APNIC', arin: 'ARIN', lacnic: 'LACNIC', ripencc: 'RIPE NCC',
  iana: 'IANA', unknown: 'Unknown', all: 'All RIRs',
};

export const STATUS_COLORS = {
  valid: '#0ca30c', invalid: '#d03b3b', notfound: '#c3c2b7', partial: '#7cc47c',
};

const sum = (...xs) => (xs.some((x) => x == null) ? null : xs.reduce((a, b) => a + b, 0));
const diff = (a, b) => (a == null || b == null ? null : Math.max(0, a - b));

/* Validity breakdown bands per perspective (bottom band first). value(m, i) reads metric arrays. */
export const BREAKDOWN = {
  routes: {
    den: 'routes',
    bands: [
      { key: 'valid', name: 'Valid', color: STATUS_COLORS.valid, value: (m, i) => m.routes_valid?.[i] },
      { key: 'invalid', name: 'Invalid', color: STATUS_COLORS.invalid, value: (m, i) => sum(m.routes_inv_as?.[i], m.routes_inv_len?.[i]) },
      { key: 'notfound', name: 'NotFound', color: STATUS_COLORS.notfound, value: (m, i) => m.routes_nf?.[i] },
    ],
  },
  space: {
    den: 'space',
    bands: [
      { key: 'valid', name: 'Valid', color: STATUS_COLORS.valid, value: (m, i) => m.space_valid?.[i] },
      { key: 'invalid', name: 'Invalid', color: STATUS_COLORS.invalid, value: (m, i) => m.space_invalid?.[i] },
      { key: 'notfound', name: 'NotFound', color: STATUS_COLORS.notfound, value: (m, i) => m.space_nf?.[i] },
    ],
  },
  registered: {
    den: 'deleg',
    bands: [
      { key: 'covered', name: 'Covered', color: STATUS_COLORS.valid, value: (m, i) => m.deleg_cov?.[i] },
      { key: 'uncovered', name: 'Not covered', color: STATUS_COLORS.notfound, value: (m, i) => diff(m.deleg?.[i], m.deleg_cov?.[i]) },
    ],
  },
  ases: {
    den: 'ases',
    bands: [
      { key: 'all', name: 'All routes valid', color: STATUS_COLORS.valid, value: (m, i) => m.ases_all_valid?.[i] },
      { key: 'some', name: 'Some valid', color: STATUS_COLORS.partial, value: (m, i) => diff(m.ases_any_valid?.[i], m.ases_all_valid?.[i]) },
      { key: 'none', name: 'No valid route', color: STATUS_COLORS.notfound, value: (m, i) => diff(m.ases?.[i], m.ases_any_valid?.[i]) },
    ],
  },
};

/* ── Country names and flags ── */
let regionNames = null;
try {
  regionNames = new Intl.DisplayNames(['en'], { type: 'region' });
} catch {
  regionNames = null;
}
const NAME_OVERRIDES = {
  EU: 'EU (RIPE NCC, no single country)',
  AP: 'AP (APNIC, no single country)',
};

export function countryName(cc) {
  if (!cc) return '';
  if (NAME_OVERRIDES[cc]) return NAME_OVERRIDES[cc];
  try {
    const n = regionNames ? regionNames.of(cc) : null;
    if (n && n !== cc) return n;
  } catch {
    /* malformed code: fall back to the code */
  }
  return cc;
}

/** Regional-indicator flag emoji; '' for codes without a flag (AP, ZZ, malformed, no known region). */
export function flagEmoji(cc) {
  if (!/^[A-Z]{2}$/.test(cc || '') || cc === 'AP' || cc === 'ZZ' || countryName(cc) === cc) return '';
  return String.fromCodePoint(...[...cc].map((c) => 0x1f1e6 + c.charCodeAt(0) - 65));
}

/* ── Data access: module-level cache keyed by request URL ── */
const ENDPOINTS = {
  meta: (p) => api.countryMeta(p),
  countries: (p) => api.countryList(p),
  'rir-trend': (p) => api.countryRirTrend(p),
  trend: (p) => api.countryTrend(p),
  ases: (p) => api.countryAses(p),
  'as-trend': (p) => api.countryAsTrend(p),
};
const promises = new Map(); // url -> Promise
const results = new Map(); // url -> resolved JSON
const MAX_ENTRIES = 150;

export function countryUrl(endpoint, params = {}) {
  const qs = new URLSearchParams();
  Object.keys(params).sort().forEach((k) => {
    const v = params[k];
    if (v !== null && v !== undefined && v !== '') qs.set(k, v);
  });
  const s = qs.toString();
  return `/api/analytics/country/${endpoint}${s ? `?${s}` : ''}`;
}

export function loadCountry(endpoint, params = {}) {
  const url = countryUrl(endpoint, params);
  if (!promises.has(url)) {
    const clean = Object.fromEntries(Object.entries(params).filter(([, v]) => v !== null && v !== undefined && v !== ''));
    const p = ENDPOINTS[endpoint](clean).then(
      (data) => { results.set(url, data); return data; },
      (e) => { promises.delete(url); throw e; },
    );
    promises.set(url, p);
    if (promises.size > MAX_ENTRIES) {
      const oldest = promises.keys().next().value;
      promises.delete(oldest);
      results.delete(oldest);
    }
  }
  return promises.get(url);
}

/**
 * Fetch one country endpoint with the shared cache. While a new URL loads, `data` keeps the
 * previous response (stale = true) so views can hold their last render at reduced opacity.
 * A URL that failed returns its error and no data: the previous response belongs to another
 * country or address family and must not stay on screen as if it were the answer.
 */
export function useCountryApi(endpoint, params, enabled = true) {
  const url = enabled ? countryUrl(endpoint, params) : null;
  const [state, setState] = useState({ url: null, data: null, error: null, errorUrl: null });
  useEffect(() => {
    if (!url) return undefined;
    let cancelled = false;
    // Coming back to a URL that failed before retries it: show it loading, not the old error.
    setState((s) => (s.errorUrl === url ? { ...s, error: null, errorUrl: null } : s));
    loadCountry(endpoint, params).then(
      (data) => { if (!cancelled) setState({ url, data, error: null, errorUrl: null }); },
      (e) => { if (!cancelled) setState((s) => ({ ...s, error: e.message, errorUrl: url })); },
    );
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url]);
  const cached = url ? results.get(url) : undefined;
  const fresh = cached !== undefined ? cached : (state.url === url ? state.data : undefined);
  const error = url && state.errorUrl === url ? state.error : null;
  const data = error ? undefined : (fresh !== undefined ? fresh : state.data);
  return {
    data,
    error,
    loading: !!url && fresh === undefined && !error,
    stale: !!url && fresh === undefined && !error && data != null,
  };
}

/** HTTP status of an api.js error message ('<status> — <body>'), or null (network error). */
export function errorStatus(msg) {
  const m = /^(\d{3}) — /.exec(msg || '');
  return m ? Number(m[1]) : null;
}

/** Readable message from api.js errors ('<status> — <first 200 chars of the body>'). */
export function errorText(msg) {
  const m = /^(\d{3}) — ([\s\S]*)$/.exec(msg || '');
  if (!m) {
    return /failed to fetch|networkerror|load failed/i.test(msg || '')
      ? 'Could not reach the server. Please check your connection and try again.'
      : msg || 'Request failed';
  }
  let detail = m[2].trim();
  try {
    const j = JSON.parse(detail);
    if (j && j.detail) {
      detail = typeof j.detail === 'string' ? j.detail
        : (Array.isArray(j.detail) && j.detail[0] && j.detail[0].msg) || JSON.stringify(j.detail);
    }
  } catch {
    /* body was not JSON (or truncated at 200 characters) */
  }
  if (m[1] === '503') {
    return /not built/i.test(detail)
      ? 'Country analytics are not built yet. Please check back later.'
      : 'Country analytics are temporarily unavailable. Please check back later.';
  }
  if (!detail || detail.startsWith('<')) detail = 'The server returned an error'; // HTML error page
  return `${detail} (HTTP ${m[1]})`;
}
