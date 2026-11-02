"""Read-only aggregation over the visitor-analytics DB for GET /stats: token-gated
usage statistics (X-Stats-Token) for the lab's access-controlled dashboard.

Opens its OWN read-only connection to ANALYTICS_DB; it never touches the tracking
writer, so it cannot affect request logging or the request path. Volume is small
(a few thousand rows per window) so the roll-up is done in Python rather than SQL.

Returns a plain dict (JSON-serialisable). Country comes for free from the stored
`X-Visitor-Country`; organisation/sector needs IP->ASN enrichment (app.enrich),
which is cached and bounded per call so a cold cache warms over a few loads.
"""

import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

from app import enrich
from app.config import ANALYTICS_DB

# Real API surface: a request path is "real" iff it starts with one of these
# router prefixes. Everything else logged (/.env, /v1/.env, /session/properties,
# wp-login probes, …) is internet background-noise scanning, counted separately.
_API_PREFIXES = (
    "/roa/", "/validity/", "/analytics/", "/aspa/", "/ca/",
    "/objects/", "/meta/", "/alerts/",
)
try:  # routers of the optional app/extensions.py (see app/main.py)
    from app.extensions import API_PREFIXES as _EXTENSION_PREFIXES
except ModuleNotFoundError as e:
    if e.name != "app.extensions":
        raise
    _EXTENSION_PREFIXES = ()
_API_PREFIXES += tuple(_EXTENSION_PREFIXES)

# query-string params that represent a "search" of each kind -> dashboard bucket
_SEARCH_PARAMS = {"asn": "asn", "prefix": "prefix", "q": "org", "org": "org"}


def _is_api(path: str) -> bool:
    p = path or ""
    return any(p.startswith(x) or p == x.rstrip("/") for x in _API_PREFIXES)


def _ms(iso: str) -> int:
    """ISO8601 -> epoch ms (the dashboard's formatters expect ms)."""
    try:
        return int(datetime.fromisoformat(iso).timestamp() * 1000)
    except Exception:
        return 0


def _top(d: dict, key_name: str, limit: int):
    return [{key_name: k, "n": v} for k, v in
            sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:limit]]


def usage_stats(days: int = 30, max_enrich: int = 150) -> dict:
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    con = sqlite3.connect(f"file:{ANALYTICS_DB}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT ts, ip, country, ua, path, query FROM events "
            "WHERE ts >= ? ORDER BY ts DESC",
            (since,),
        ).fetchall()
        earliest = con.execute("SELECT MIN(ts) FROM events").fetchone()[0]
    finally:
        con.close()

    # ── first pass: split real vs scanner, accumulate the enrichment-free facts
    daily_req = defaultdict(int)
    daily_vis = defaultdict(set)
    cc_req = defaultdict(int)
    cc_vis = defaultdict(set)
    ep_req = defaultdict(int)
    ep_vis = defaultdict(set)
    scan_path = defaultdict(int)
    look = {"asn": defaultdict(int), "prefix": defaultdict(int), "org": defaultdict(int)}

    ip_req = defaultdict(int)          # real requests per ip
    ip_cc = defaultdict(lambda: defaultdict(int))
    ip_last = defaultdict(int)
    real_rows = []                      # (ms, ip, cc, ua, path) for org roll-up
    recent = []                         # last 60 across everything
    scanner_hits = 0
    last_ts = 0

    for ts_iso, ip, cc, ua, path, query in rows:
        ms = _ms(ts_iso)
        if ms > last_ts:
            last_ts = ms
        ip = ip or ""
        cc = (cc or "").upper()
        path = path or ""
        if len(recent) < 60:
            recent.append({
                "ts": ms, "ip": ip, "cc": cc, "path": path,
                "query": (query or "")[:200], "api": _is_api(path),
            })
        if not _is_api(path):
            scanner_hits += 1
            scan_path[path[:80]] += 1
            continue
        day = ts_iso[:10]
        daily_req[day] += 1
        ep_req[path] += 1
        if ip:
            daily_vis[day].add(ip)
            ep_vis[path].add(ip)
            ip_req[ip] += 1
            ip_last[ip] = max(ip_last[ip], ms)
            real_rows.append((ms, ip, cc, ua or "", path))
        if cc:
            cc_req[cc] += 1
            if ip:
                cc_vis[cc].add(ip)
                ip_cc[ip][cc] += 1
        if query:
            parsed = parse_qs(query)
            for param, bucket in _SEARCH_PARAMS.items():
                for val in parsed.get(param, []):
                    val = val.strip()[:64]
                    if val:
                        look[bucket][val] += 1

    # ── enrich: resolve the real visitor IPs to ASN/org (heaviest first, bounded)
    ordered_ips = [ip for ip, _ in sorted(ip_req.items(), key=lambda kv: kv[1], reverse=True)]
    meta = enrich.enrich_ips(ordered_ips, max_new=max_enrich)  # {ip:(asn,org)}

    # ── org roll-up (who uses it): group real traffic by resolved organisation
    org_req = defaultdict(int)
    org_vis = defaultdict(set)
    org_eps = defaultdict(set)
    org_last = defaultdict(int)
    org_asn = {}
    org_ua = {}
    for ms, ip, cc, ua, path in real_rows:
        asn, as_org = meta.get(ip, ("", ""))
        key = as_org or ("AS" + asn if asn else "")  # unresolved -> "" -> Unidentified
        org_req[key] += 1
        org_vis[key].add(ip)
        org_eps[key].add(path)
        org_last[key] = max(org_last[key], ms)
        if key and key not in org_asn and asn:
            org_asn[key] = asn
        if key and ua and (key not in org_ua):
            org_ua[key] = ua[:300]

    by_org = sorted(
        ({
            "as_org": k, "asn": org_asn.get(k, ""),
            "requests": org_req[k], "visitors": len(org_vis[k]),
            "endpoints": len(org_eps[k]), "last_ts": org_last[k],
            "ua": org_ua.get(k, ""),
        } for k in org_req),
        key=lambda o: o["requests"], reverse=True,
    )[:25]

    # ── top visitors (gated dashboard -> raw IP is fine)
    top_visitors = []
    for ip, n in sorted(ip_req.items(), key=lambda kv: kv[1], reverse=True)[:25]:
        asn, as_org = meta.get(ip, ("", ""))
        cc = max(ip_cc[ip].items(), key=lambda kv: kv[1])[0] if ip_cc.get(ip) else ""
        top_visitors.append({
            "ip": ip, "cc": cc, "asn": asn, "as_org": as_org,
            "requests": n, "last_ts": ip_last[ip],
        })

    # attach org to the recent sample
    for r in recent:
        asn, as_org = meta.get(r["ip"], ("", ""))
        r["as_org"] = as_org
        r["asn"] = asn

    total_requests = sum(daily_req.values())
    distinct_orgs = len({o["as_org"] for o in by_org if o["as_org"]})

    return {
        "days": days,
        "generated": int(time.time() * 1000),
        "data_since": earliest or "",
        "totals": {
            "requests": total_requests,
            "visitors": len(ip_req),
            "countries": len(cc_req),
            "orgs": distinct_orgs,
            "scanner_hits": scanner_hits,
            "last_ts": last_ts,
        },
        "daily": [
            {"date": d, "requests": daily_req[d], "visitors": len(daily_vis[d])}
            for d in sorted(daily_req)
        ],
        "by_country": [
            {"cc": k, "requests": v, "visitors": len(cc_vis[k])}
            for k, v in sorted(cc_req.items(), key=lambda kv: kv[1], reverse=True)[:15]
        ],
        "by_org": by_org,
        "top_endpoints": [
            {"path": k, "requests": v, "visitors": len(ep_vis[k])}
            for k, v in sorted(ep_req.items(), key=lambda kv: kv[1], reverse=True)[:20]
        ],
        "lookups_asn": _top(look["asn"], "k", 20),
        "lookups_prefix": _top(look["prefix"], "k", 20),
        "lookups_org": _top(look["org"], "k", 20),
        "top_visitors": top_visitors,
        "recent": recent,
        "scanners": _top(scan_path, "path", 12),
    }
