"""ROA lookup, history, and query endpoints."""

import csv
import io
import json
from datetime import datetime

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app import coverage
from app.config import ASINT_API_URL, BGP_COVERAGE_WINDOW_DAYS, RIRS
from app.db import cache_get, cache_set, get_con
from app.models import (
    ASNLookupResponse,
    PrefixLookupResponse,
    ROAEntry,
    ROAHistoryEntry,
    ROAHistoryResponse,
)

router = APIRouter(prefix="/roa", tags=["ROA"])

# Live ROA queries default to RP-valid only (objects rpki-client did not reject
# in the latest validation cycle). ``validity=all`` returns the full repo-present
# set. Note: only *live* views can be filtered — history has no per-timestamp
# verdicts, so it stays repo-present.
VALIDITY_DESC = "valid (RP-valid only, default) or all (repo-present)"


def _validity_filter(validity: str) -> str:
    """SQL fragment appended after an existing WHERE clause."""
    v = (validity or "valid").lower()
    if v == "valid":
        return " AND is_valid"
    if v == "all":
        return ""
    raise HTTPException(400, "validity must be 'valid' or 'all'")


@router.get("/lookup", response_model=PrefixLookupResponse)
def roa_lookup(
    prefix: str = Query(..., description="IP prefix (e.g. 1.0.0.0/24)"),
    asn: int | None = Query(None, description="Filter by ASN"),
    rir: str | None = Query(None, description="Filter by RIR"),
    validity: str = Query("valid", description=VALIDITY_DESC),
):
    """Live VRPs for exactly this prefix (no covering-prefix match).

    An exact string match against the stored canonical form; the query is not
    normalized, so 1.0.0.1/24 or upper-case IPv6 match nothing.
    """
    con = get_con()
    conditions = ["prefix = $prefix"]
    params = {"prefix": prefix}

    if asn is not None:
        conditions.append("asn = $asn")
        params["asn"] = asn
    if rir is not None:
        conditions.append("rir = $rir")
        params["rir"] = rir.lower()

    where = " AND ".join(conditions) + _validity_filter(validity)
    rows = con.execute(
        f"""
        SELECT rir, asn, prefix, max_length, afi, object_hash,
               signing_time::VARCHAR, first_seen_ts, last_seen_ts
        FROM live_roas
        WHERE {where}
        ORDER BY rir, asn
        """,
        params,
    ).fetchall()

    results = [
        ROAEntry(
            rir=r[0], asn=r[1], prefix=r[2], max_length=r[3], afi=r[4],
            object_hash=r[5], signing_time=r[6], first_seen=r[7], last_seen=r[8],
        )
        for r in rows
    ]
    return PrefixLookupResponse(
        query_prefix=prefix, query_asn=asn, count=len(results), results=results,
    )


@router.get("/by-asn", response_model=ASNLookupResponse)
def roa_by_asn(
    asn: int = Query(..., description="Autonomous System Number"),
    rir: str | None = Query(None, description="Filter by RIR"),
    afi: int | None = Query(None, description="Address family: 4 or 6"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    validity: str = Query("valid", description=VALIDITY_DESC),
):
    """List all current ROA prefixes for an ASN."""
    con = get_con()
    conditions = ["asn = $asn"]
    params: dict = {"asn": asn}

    if rir is not None:
        conditions.append("rir = $rir")
        params["rir"] = rir.lower()
    if afi is not None:
        conditions.append("afi = $afi")
        params["afi"] = afi

    where = " AND ".join(conditions) + _validity_filter(validity)
    rows = con.execute(
        f"""
        SELECT rir, asn, prefix, max_length, afi, object_hash,
               signing_time::VARCHAR, first_seen_ts, last_seen_ts
        FROM live_roas
        WHERE {where}
        ORDER BY afi, prefix
        LIMIT {limit} OFFSET {offset}
        """,
        params,
    ).fetchall()

    count_row = con.execute(
        f"SELECT COUNT(*) FROM live_roas WHERE {where}", params,
    ).fetchone()

    results = [
        ROAEntry(
            rir=r[0], asn=r[1], prefix=r[2], max_length=r[3], afi=r[4],
            object_hash=r[5], signing_time=r[6], first_seen=r[7], last_seen=r[8],
        )
        for r in rows
    ]
    return ASNLookupResponse(
        query_asn=asn, count=count_row[0], results=results,
    )


def _history_query(rir: str, where_clause: str, params: dict) -> list[ROAHistoryEntry]:
    """Run a ROA history query against the in-memory roa_history table."""
    con = get_con()

    # Rewrite column references: rp.prefix -> prefix, ro.asn -> asn, etc.
    wc = where_clause.replace("rp.", "").replace("ro.", "")

    rows = con.execute(
        f"""
        SELECT rir, asn, prefix, max_length, afi, object_hash,
               semantic_hash, signing_time::VARCHAR, first_seen_ts, last_seen_ts
        FROM roa_history
        WHERE rir = '{rir}' AND {wc}
        ORDER BY first_seen_ts
        """,
        params,
    ).fetchall()

    return [
        ROAHistoryEntry(
            rir=r[0], asn=r[1], prefix=r[2], max_length=r[3], afi=r[4],
            object_hash=r[5], semantic_hash=r[6], signing_time=r[7],
            first_seen=r[8], last_seen=r[9],
        )
        for r in rows
    ]


@router.get("/history", response_model=ROAHistoryResponse)
def roa_history(
    prefix: str | None = Query(None, description="IP prefix"),
    asn: int | None = Query(None, description="ASN"),
    rir: str | None = Query(None, description="Filter by RIR"),
    limit: int = Query(5000, ge=1, le=50000),
):
    """Full lifecycle history of ROAs for a prefix (exact match) or ASN.

    Returns every ROA object that ever existed for the query,
    with first_seen/last_seen timestamps showing creation and withdrawal.
    """
    if prefix is None and asn is None:
        raise HTTPException(400, "Provide at least one of: prefix, asn")

    conditions = []
    params: dict = {}
    if prefix is not None:
        conditions.append("rp.prefix = $prefix")
        params["prefix"] = prefix
    if asn is not None:
        conditions.append("ro.asn = $asn")
        params["asn"] = asn

    where = " AND ".join(conditions)

    target_rirs = [rir.lower()] if rir else RIRS
    for r in target_rirs:
        if r not in RIRS:
            raise HTTPException(400, f"Unknown RIR: {r}. Valid: {RIRS}")

    all_entries: list[ROAHistoryEntry] = []
    for r in target_rirs:
        all_entries.extend(_history_query(r, where, params))

    all_entries.sort(key=lambda e: e.first_seen or "")
    truncated = len(all_entries) > limit
    all_entries = all_entries[:limit]

    return ROAHistoryResponse(
        query_prefix=prefix,
        query_asn=asn,
        count=len(all_entries),
        truncated=truncated,
        entries=all_entries,
    )


@router.get("/coverage-trend")
def roa_coverage_trend(
    asn: int = Query(..., description="Autonomous System Number"),
):
    """Per year, the distinct prefixes in an ASN's ROAs (IPv4, IPv6).

    A prefix counts for a year when one of its ROA objects was present at
    any time during that year (first_seen <= Dec 31 and last_seen >= Jan 1).
    No BGP data is used.
    """
    con = get_con()

    # Get all historical ROA prefix entries from in-memory roa_history table
    all_entries = con.execute(
        "SELECT DISTINCT prefix, afi, first_seen_ts, last_seen_ts FROM roa_history WHERE asn = $asn",
        {"asn": asn},
    ).fetchall()

    if not all_entries:
        return {"asn": asn, "data": [], "message": "No ROA history found"}

    # Find year range
    min_year = min(int(str(e[2])[:4]) for e in all_entries if e[2])
    max_year = datetime.now().year

    # For each year-end, count active prefixes
    data = []
    for year in range(min_year, max_year + 1):
        year_end = f"{year}-12-31"
        year_start = f"{year}-01-01"

        # A prefix is "active" in this year if first_seen <= year_end AND last_seen >= year_start
        active_v4 = set()
        active_v6 = set()
        for prefix, afi, first_seen, last_seen in all_entries:
            fs = str(first_seen)[:10] if first_seen else "9999-12-31"
            ls = str(last_seen)[:10] if last_seen else "2000-01-01"
            if fs <= year_end and ls >= year_start:
                if afi == 4:
                    active_v4.add(prefix)
                else:
                    active_v6.add(prefix)

        data.append({
            "year": year,
            "roa_prefixes": len(active_v4) + len(active_v6),
            "roa_ipv4": len(active_v4),
            "roa_ipv6": len(active_v6),
        })

    return {"asn": asn, "data": data}


WINDOW_DESC = "BGP window: prefixes originated in the last N days of RIBs (default 7)"
# Largest org family whose ASNs get a BGP lookup (each takes ~1-4 s, 8 at a time)
ORG_BGP_MAX_ASNS = 200


def _family_block(t: dict) -> dict:
    """Per-family counts. ``signed`` = RPKI-valid; ``unsigned`` = everything else
    (invalid + not_found) so that announced = signed + unsigned, as before."""
    return {
        "announced": t["announced"],
        "signed": t["valid"],
        "unsigned": t["announced"] - t["valid"],
        "invalid": t["invalid"],
        "not_found": t["not_found"],
        "coverage_pct": t["coverage_pct"],
    }


@router.get("/coverage")
def roa_coverage(
    asn: int = Query(..., description="Autonomous System Number"),
    family: str = Query("both", pattern="^(ipv4|ipv6|both)$", description="ipv4, ipv6, or both"),
    validity: str = Query("valid", description=VALIDITY_DESC),
    window_days: int = Query(BGP_COVERAGE_WINDOW_DAYS, ge=1, le=90, description=WINDOW_DESC),
):
    """RPKI coverage of the prefixes an ASN currently originates in BGP.

    Each announced prefix is origin-validated (RFC 6811) against the live VRPs:
    ``coverage_pct`` is the share that is RPKI-valid. Invalid announcements
    (covered by a VRP for another AS, or longer than its maxLength) are counted
    separately and listed in ``invalid_prefixes``. ``unsigned_prefixes`` lists
    only prefixes no VRP covers (the live UI labels them "No ROA").
    See app/coverage.py.
    """
    validity_sql = _validity_filter(validity)
    cache_key = f"coverage:{asn}:{family}:{validity}:{window_days}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        window = coverage.coverage_window(window_days)
        announced = coverage.announced_prefixes(asn, family, window)
    except coverage.BGPError as e:
        raise HTTPException(502, str(e))
    announced, excluded = coverage.routable(announced)

    con = get_con()
    v = coverage.Validator(con, announced, validity_sql)
    states, reasons, used_vrps = {}, {}, set()
    for p in v.nets:
        state, reason, matched = v.validate(p, asn)
        states[p] = state
        used_vrps |= matched
        if reason:
            reasons[p] = reason
    t = coverage.tally(states, v.nets)

    # This AS's own ROA prefixes that validate none of its announcements
    afis = {"ipv4": (4,), "ipv6": (6,)}.get(family, (4, 6))
    roa_prefixes = {
        r[0] for r in con.execute(
            f"SELECT DISTINCT prefix, afi FROM live_roas WHERE asn = $asn{validity_sql}",
            {"asn": asn},
        ).fetchall()
        if r[1] in afis
    }
    roa_only = roa_prefixes - used_vrps
    no_roa = sorted(p for p, s in states.items() if s == coverage.NOT_FOUND)

    result = {
        "asn": asn,
        "family": family,
        "method": coverage.method(window),
        "summary": {
            "announced": t["all"]["announced"],
            "roa_covered": t["all"]["valid"],
            "unsigned": t["all"]["announced"] - t["all"]["valid"],
            "invalid": t["all"]["invalid"],
            "not_found": t["all"]["not_found"],
            "roa_only": len(roa_only),
            "coverage_pct": t["all"]["coverage_pct"],
            "excluded_more_specifics": excluded,
        },
        "ipv4": _family_block(t["ipv4"]),
        "ipv6": _family_block(t["ipv6"]),
        "unsigned_prefixes": no_roa[:100],
        "invalid_prefixes": [{"prefix": p, "reason": reasons[p]} for p in sorted(reasons)][:100],
        "roa_not_announced": sorted(roa_only)[:100],
    }
    cache_set(cache_key, result)
    return result


@router.get("/org-search")
def org_search(
    q: str = Query(..., min_length=2, description="Organization name to search"),
    limit: int = Query(10, ge=1, le=50),
):
    """Search organizations via ASINT API and return their ASNs."""
    try:
        resp = httpx.get(
            f"{ASINT_API_URL}/api/v1/org-entries/",
            params={"q": q, "limit": limit},
            timeout=15.0,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        raise HTTPException(502, f"ASINT API error: {e}")


@router.get("/org-asns")
def org_asns(
    asn: int = Query(..., description="Seed ASN to find the org family"),
):
    """Get all ASNs belonging to the same organization family via ASINT API."""
    try:
        resp = httpx.get(
            f"{ASINT_API_URL}/api/v1/asn-entries/{asn}",
            timeout=15.0,
        )
        resp.raise_for_status()
        families = resp.json()
    except httpx.HTTPError as e:
        raise HTTPException(502, f"ASINT API error: {e}")

    # Find the family that contains the seed ASN and collect all ASNs
    all_asns = []
    org_name = None
    for family in families:
        for entry in family.get("asn_entries", []):
            if entry["asn"] == asn:
                org_name = entry.get("org_name")
            all_asns.append({
                "asn": entry["asn"],
                "as_name": entry.get("as_name", ""),
                "org_name": entry.get("org_name", ""),
                "rir": entry.get("rir", ""),
            })

    # Deduplicate by ASN
    seen = set()
    unique = []
    for a in all_asns:
        if a["asn"] not in seen:
            seen.add(a["asn"])
            unique.append(a)

    return {
        "seed_asn": asn,
        "org_name": org_name,
        "asn_count": len(unique),
        "asns": sorted(unique, key=lambda x: x["asn"]),
    }


@router.get("/org-coverage")
def org_coverage(
    asn: int = Query(..., description="Seed ASN to find the org family"),
    validity: str = Query("valid", description=VALIDITY_DESC),
    window_days: int = Query(BGP_COVERAGE_WINDOW_DAYS, ge=1, le=90, description=WINDOW_DESC),
):
    """RPKI coverage for an entire organization.

    Uses ASINT API to find all ASNs in the org family, then validates each
    ASN's announced prefixes like /roa/coverage. An ASN whose BGP lookup fails
    gets ``bgp_error`` and is left out of the totals instead of counting as
    zero announcements.
    """
    validity_sql = _validity_filter(validity)
    cache_key = f"org-coverage:{asn}:{validity}:{window_days}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    # 1) Get org family ASNs from ASINT
    try:
        resp = httpx.get(
            f"{ASINT_API_URL}/api/v1/asn-entries/{asn}",
            timeout=15.0,
        )
        resp.raise_for_status()
        families = resp.json()
    except httpx.HTTPError as e:
        raise HTTPException(502, f"ASINT API error: {e}")

    # Collect all ASNs from the family
    all_entries = []
    org_name = None
    for family in families:
        for entry in family.get("asn_entries", []):
            if entry["asn"] == asn and not org_name:
                org_name = entry.get("org_name")
            all_entries.append(entry)

    # Deduplicate
    seen = set()
    unique_asns = []
    for e in all_entries:
        if e["asn"] not in seen:
            seen.add(e["asn"])
            unique_asns.append(e)

    # 2) BGP-announced prefixes per ASN (BGP warehouse, through the shared pool).
    #    Any BGP failure leaves the ROA-only table with bgp_error set (the UI then
    #    shows "BGP unavailable"), instead of failing the whole page.
    asns = sorted(e["asn"] for e in unique_asns)
    window, window_err = None, None
    if len(asns) > ORG_BGP_MAX_ASNS:
        # ~1-4 s per ASN; larger families cannot finish within the tunnel's 100 s
        window_err = f"org family has {len(asns)} ASNs; BGP lookup is limited to {ORG_BGP_MAX_ASNS}"
    else:
        try:
            window = coverage.coverage_window(window_days)
        except coverage.BGPError as e:
            window_err = str(e)

    def fetch(a):
        if window_err:
            return a, [], window_err
        try:
            return a, coverage.announced_prefixes(a, "both", window, timeout=60.0), None
        except coverage.BGPError as e:
            return a, [], str(e)

    bgp, excluded = {}, 0
    for a, pfx, err in coverage.bgp_pool.map(fetch, asns):
        pfx, n = coverage.routable(pfx)
        bgp[a] = (pfx, err)
        excluded += n

    # 3) Validate every ASN's announcements against the live VRPs in one pass
    con = get_con()
    v = coverage.Validator(con, [p for pfx, _ in bgp.values() for p in pfx], validity_sql)
    per_asn = []
    total_roa = total_announced = total_signed = total_invalid = bgp_errors = 0

    for entry in sorted(unique_asns, key=lambda x: x["asn"]):
        a = entry["asn"]

        # ROA prefixes from local DuckDB
        rows = con.execute(
            f"SELECT DISTINCT prefix, afi FROM live_roas WHERE asn = $asn{validity_sql}",
            {"asn": a},
        ).fetchall()
        roa_v4 = len({r[0] for r in rows if r[1] == 4})
        roa_v6 = len({r[0] for r in rows if r[1] == 6})

        prefixes, err = bgp[a]
        states = {p: v.validate(p, a)[0] for p in prefixes if p in v.nets}
        t = coverage.tally(states, v.nets)["all"]

        total_roa += roa_v4 + roa_v6
        if err:
            bgp_errors += 1
        else:
            total_announced += t["announced"]
            total_signed += t["valid"]
            total_invalid += t["invalid"]

        per_asn.append({
            "asn": a,
            "as_name": entry.get("as_name", ""),
            "org_name": entry.get("org_name", ""),
            "rir": entry.get("rir", ""),
            "roa_v4": roa_v4,
            "roa_v6": roa_v6,
            "roa_total": roa_v4 + roa_v6,
            "announced": t["announced"],
            "signed": t["valid"],
            "invalid": t["invalid"],
            "not_found": t["not_found"],
            "coverage_pct": round(t["coverage_pct"], 1),
            "bgp_error": err,
        })

    org_coverage_pct = round(total_signed / total_announced * 100, 1) if total_announced > 0 else 0.0

    result = {
        "seed_asn": asn,
        "org_name": org_name,
        "asn_count": len(unique_asns),
        "method": coverage.method(window) if window else None,
        "total_roa_prefixes": total_roa,
        "total_announced": total_announced,
        "total_signed": total_signed,
        "total_invalid": total_invalid,
        "total_excluded_more_specifics": excluded,
        "org_coverage_pct": org_coverage_pct,
        "bgp_errors": bgp_errors,
        "per_asn": per_asn,
    }
    if not bgp_errors:
        cache_set(cache_key, result)
    return result


@router.get("/export")
def export_live_vrps(
    rir: str | None = Query(None, description="Filter by RIR (afrinic/apnic/arin/lacnic/ripencc)"),
    afi: int | None = Query(None, description="Address family: 4 (IPv4) or 6 (IPv6)"),
    format: str = Query("csv", pattern="^(csv|json)$", description="Output format"),
    validity: str = Query("valid", description=VALIDITY_DESC),
):
    """Download the current set of live VRPs (distinct asn, prefix, max_length).

    A VRP is a (ASN, prefix, max-length) payload. This deduplicates the
    object×prefix rows in live_roas down to distinct VRPs. Defaults to RP-valid
    only (the openbgpd-equivalent set); ``validity=all`` exports repo-present.
    """
    conditions = []
    params = {}
    if rir is not None:
        if rir.lower() not in RIRS:
            raise HTTPException(status_code=400, detail=f"Unknown RIR: {rir}")
        conditions.append("rir = $rir")
        params["rir"] = rir.lower()
    if afi is not None:
        if afi not in (4, 6):
            raise HTTPException(status_code=400, detail="afi must be 4 or 6")
        conditions.append("afi = $afi")
        params["afi"] = afi
    if (validity or "valid").lower() == "valid":
        conditions.append("is_valid")
    elif validity.lower() != "all":
        raise HTTPException(status_code=400, detail="validity must be 'valid' or 'all'")

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = (
        "SELECT DISTINCT rir, asn, prefix, max_length, afi "
        f"FROM live_roas{where} ORDER BY rir, asn, prefix, max_length"
    )
    rows = get_con().execute(sql, params).fetchall()

    suffix = rir.lower() if rir else "all"
    cols = ["rir", "asn", "prefix", "max_length", "afi"]

    if format == "json":
        data = [dict(zip(cols, r)) for r in rows]
        headers = {"Content-Disposition": f'attachment; filename="rpki_vrps_{suffix}.json"'}
        return StreamingResponse(
            iter([json.dumps(data)]),
            media_type="application/json",
            headers=headers,
        )

    def gen():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(cols)
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)
        for r in rows:
            writer.writerow(r)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    headers = {"Content-Disposition": f'attachment; filename="rpki_vrps_{suffix}.csv"'}
    return StreamingResponse(gen(), media_type="text/csv", headers=headers)
