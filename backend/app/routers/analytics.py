"""Analytics endpoints: churn, lifespan, signing cadence, CRL staleness, maxLength."""

from fastapi import APIRouter, HTTPException, Query

from app.config import RIRS
from app.db import _presence_src, cache_get, cache_set, get_con, latest_snapshot_ts, live_cutoff, parquet_glob
from app.models import (
    CadenceBucket,
    ChurnDataPoint,
    ChurnResponse,
    CRLStalenessResponse,
    LifespanResponse,
    LifespanStats,
    MaxLengthIssue,
    MaxLengthResponse,
    SigningCadenceResponse,
    StaleCRLEntry,
)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/churn", response_model=ChurnResponse)
def roa_churn(
    rir: str = Query(..., description="RIR name (e.g. arin)"),
    granularity: str = Query("month", description="'week' or 'month'"),
    start: str | None = Query(None, description="Start period (e.g. 2024-01)"),
    end: str | None = Query(None, description="End period (e.g. 2024-12)"),
):
    """ROA churn: new and withdrawn ROA entries per time period.

    One entry per (ROA object, prefix) row of roa_history; first and last seen
    come from object_presence_stats (new = first seen in the period,
    withdrawn = last seen in the period and before the newest last_seen).
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}. Valid: {RIRS}")

    # Check cache
    cache_key = f"churn:{rir}:{granularity}:{start}:{end}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    if granularity == "month":
        fmt = "'%Y-%m'"
    elif granularity == "week":
        fmt = "'%G-W%V'"  # ISO week
    else:
        raise HTTPException(400, "granularity must be 'week' or 'month'")

    con = get_con()

    # Sanitize start/end — only allow YYYY-MM or YYYY-Wnn patterns
    import re
    for val, name in [(start, "start"), (end, "end")]:
        if val and not re.match(r'^\d{4}(-\d{2}|-W\d{2})$', val):
            raise HTTPException(400, f"Invalid {name} format. Use YYYY-MM or YYYY-Wnn")

    period_conditions = []
    params: dict = {"rir": rir}
    if start:
        period_conditions.append("period >= $start")
        params["start"] = start
    if end:
        period_conditions.append("period <= $end")
        params["end"] = end
    period_where = (" AND " + " AND ".join(period_conditions)) if period_conditions else ""

    # Single materialized scan of this RIR's history, then aggregate new/withdrawn.
    #
    # "withdrawn" = objects whose last_seen is strictly before the global max
    # last_seen (live objects share that max and are excluded). The most recent
    # withdrawn bucket is always distorted: snapshots are sparse near the live
    # edge, so every object that died during the gap before the final snapshot
    # gets dumped into a single trailing period. We detect that boundary bucket
    # and drop it (period < boundary) so the chart doesn't show a phantom cliff.
    sql = f"""
    WITH h AS MATERIALIZED (
        SELECT CAST(first_seen_ts AS TIMESTAMP) AS fs,
               CAST(last_seen_ts AS TIMESTAMP)  AS ls
        FROM roa_history WHERE rir = $rir
    ),
    b AS (SELECT MAX(ls) AS gmax FROM h),
    nw AS (SELECT strftime(fs, {fmt}) AS p, COUNT(*) AS cnt FROM h GROUP BY p),
    wd AS (
        SELECT strftime(ls, {fmt}) AS p, COUNT(*) AS cnt
        FROM h, b WHERE ls < gmax GROUP BY p
    ),
    boundary AS (SELECT MAX(p) AS bp FROM wd)
    SELECT period, new_cnt, withdrawn_cnt FROM (
        SELECT
            COALESCE(nw.p, wd.p) AS period,
            COALESCE(nw.cnt, 0)  AS new_cnt,
            COALESCE(wd.cnt, 0)  AS withdrawn_cnt
        FROM nw
        FULL OUTER JOIN wd ON nw.p = wd.p
        WHERE COALESCE(nw.p, wd.p) IS NOT NULL
          AND COALESCE(COALESCE(nw.p, wd.p) < (SELECT bp FROM boundary), TRUE)
    ) sub
    WHERE TRUE {period_where}
    ORDER BY period
    """
    rows = con.execute(sql, params).fetchall()

    data = [
        ChurnDataPoint(period=r[0], new=r[1], withdrawn=r[2], net=r[1] - r[2])
        for r in rows
    ]
    result = ChurnResponse(
        rir=rir, granularity=granularity, start=start, end=end, data=data,
    )
    cache_set(cache_key, result)
    return result


@router.get("/lifespan", response_model=LifespanResponse)
def object_lifespan(
    rir: str = Query(..., description="RIR name"),
    object_type: str = Query(
        "roa", description="Object type: roa, cert, crl, manifest",
    ),
):
    """Object lifespan statistics: avg, median, p10, p90 in days.

    Only considers objects that have been withdrawn (last_seen < latest snapshot).
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}. Valid: {RIRS}")

    type_to_table = {
        "roa": "roa_objects",
        "cert": "certs",
        "crl": "crls",
        "manifest": "manifests",
    }
    obj_table = type_to_table.get(object_type)
    if not obj_table:
        raise HTTPException(400, f"Unknown type: {object_type}. Valid: {list(type_to_table)}")

    con = get_con()
    obj_pq = parquet_glob(rir, obj_table)
    presence = _presence_src(parquet_glob(rir, "object_presence_stats"))

    sql = f"""
    WITH lifespans AS (
        SELECT
            datediff('day',
                CAST(ofl.first_seen_ts AS TIMESTAMP),
                CAST(ofl.last_seen_ts AS TIMESTAMP)
            ) AS days_alive
        FROM read_parquet('{obj_pq}') o
        JOIN {presence} ofl ON o.object_id = ofl.object_id
        WHERE ofl.last_seen_ts < (
            SELECT MAX(last_seen_ts) FROM {presence}
        )
        AND ofl.first_seen_ts IS NOT NULL
    )
    SELECT
        COUNT(*) AS cnt,
        ROUND(AVG(days_alive), 1) AS avg_d,
        ROUND(MEDIAN(days_alive), 1) AS med_d,
        ROUND(PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY days_alive), 1) AS p10,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_alive), 1) AS p90
    FROM lifespans
    WHERE days_alive >= 0
    """
    row = con.execute(sql).fetchone()

    stats = LifespanStats(
        rir=rir,
        object_type=object_type,
        count=row[0],
        avg_days=row[1] or 0,
        median_days=row[2] or 0,
        p10_days=row[3] or 0,
        p90_days=row[4] or 0,
    )
    return LifespanResponse(results=[stats])


@router.get("/signing-cadence", response_model=SigningCadenceResponse)
def signing_cadence(
    rir: str = Query(..., description="RIR name"),
    ca_aki: str | None = Query(None, description="Filter by CA's AKI (authority key identifier)"),
):
    """CRL-based re-signing cadence per CA.

    Measures time between consecutive CRL this_update timestamps for each CA
    (identified by AKI). This reveals how frequently each CA re-signs its
    repository contents. Only the CRLs in the live window count (last seen on
    or after the day before the newest presence date), so it covers about the
    last one to two days, not the CA's history.
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}")

    con = get_con()
    crls_pq = parquet_glob(rir, "crls")
    obj_fl = parquet_glob(rir, "object_presence_stats")
    cutoff = live_cutoff(rir)

    aki_filter = ""
    params = []
    if ca_aki is not None:
        aki_filter = "AND c.aki = $1"
        params = [ca_aki]

    sql = f"""
    WITH ca_crls AS (
        SELECT c.aki, c.this_update,
               LAG(c.this_update) OVER (
                   PARTITION BY c.aki ORDER BY c.this_update
               ) AS prev_update
        FROM read_parquet('{crls_pq}') c
        JOIN {_presence_src(obj_fl)} ofl ON c.object_id = ofl.object_id
        WHERE ofl.last_seen_ts >= '{cutoff}'
          {aki_filter}
    ),
    intervals AS (
        SELECT datediff('hour', prev_update, this_update) AS interval_h
        FROM ca_crls
        WHERE prev_update IS NOT NULL
    )
    SELECT
        COUNT(*) AS total,
        ROUND(AVG(interval_h), 1) AS avg_h,
        ROUND(MEDIAN(interval_h), 1) AS med_h,
        SUM(CASE WHEN interval_h < 6 THEN 1 ELSE 0 END) AS h0_6,
        SUM(CASE WHEN interval_h >= 6 AND interval_h < 12 THEN 1 ELSE 0 END) AS h6_12,
        SUM(CASE WHEN interval_h >= 12 AND interval_h < 24 THEN 1 ELSE 0 END) AS h12_24,
        SUM(CASE WHEN interval_h >= 24 AND interval_h < 48 THEN 1 ELSE 0 END) AS h24_48,
        SUM(CASE WHEN interval_h >= 48 AND interval_h < 168 THEN 1 ELSE 0 END) AS h48_168,
        SUM(CASE WHEN interval_h >= 168 THEN 1 ELSE 0 END) AS h168_plus
    FROM intervals
    """
    row = con.execute(sql, params).fetchone()

    buckets = [
        CadenceBucket(interval_hours="0-6", count=row[3] or 0),
        CadenceBucket(interval_hours="6-12", count=row[4] or 0),
        CadenceBucket(interval_hours="12-24", count=row[5] or 0),
        CadenceBucket(interval_hours="24-48", count=row[6] or 0),
        CadenceBucket(interval_hours="48-168", count=row[7] or 0),
        CadenceBucket(interval_hours="168+", count=row[8] or 0),
    ]

    return SigningCadenceResponse(
        rir=rir, signer_cert_id=None,
        total_re_signs=row[0] or 0, avg_interval_hours=row[1] or 0,
        median_interval_hours=row[2] or 0, buckets=buckets,
    )


@router.get("/crl-staleness", response_model=CRLStalenessResponse)
def crl_staleness(
    rir: str = Query(..., description="RIR name"),
    min_staleness_hours: float = Query(0, description="Minimum staleness in hours"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Find CRLs past their next_update time (stale).

    Only considers currently live CRLs. Staleness = now - next_update.
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}")

    con = get_con()
    crls_pq = parquet_glob(rir, "crls")
    obj_fl = parquet_glob(rir, "object_presence_stats")
    cutoff = live_cutoff(rir)

    sql = f"""
    WITH live_crls AS (
        SELECT c.issuer_dn, c.aki, c.this_update, c.next_update, c.revoked_count
        FROM read_parquet('{crls_pq}') c
        JOIN {_presence_src(obj_fl)} ofl ON c.object_id = ofl.object_id
        WHERE ofl.last_seen_ts >= '{cutoff}'
    )
    SELECT
        issuer_dn, aki, this_update::VARCHAR, next_update::VARCHAR, revoked_count,
        ROUND(datediff('minute', CAST(next_update AS TIMESTAMP), now()) / 60.0, 1) AS stale_hours
    FROM live_crls
    WHERE CAST(next_update AS TIMESTAMP) < now()
      AND datediff('minute', CAST(next_update AS TIMESTAMP), now()) / 60.0 >= {min_staleness_hours}
    ORDER BY stale_hours DESC
    LIMIT {limit}
    """
    rows = con.execute(sql).fetchall()

    total_live = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{crls_pq}') c
        JOIN {_presence_src(obj_fl)} ofl ON c.object_id = ofl.object_id
        WHERE ofl.last_seen_ts >= '{cutoff}'
        """,
    ).fetchone()[0]

    entries = [
        StaleCRLEntry(
            rir=rir, issuer_dn=r[0], aki=r[1], this_update=r[2],
            next_update=r[3], staleness_hours=r[5], revoked_count=r[4],
        )
        for r in rows
    ]

    return CRLStalenessResponse(
        rir=rir, total_live_crls=total_live,
        stale_count=len(entries), entries=entries,
    )


@router.get("/maxlength-issues", response_model=MaxLengthResponse)
def maxlength_issues(
    rir: str = Query(..., description="RIR name"),
    min_excess: int = Query(8, description="Minimum excess bits (maxLen - prefixLen)"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Find ROAs with excessively loose maxLength.

    A large gap between prefix length and maxLength is a security risk,
    as it allows sub-prefix hijacking.
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}")

    con = get_con()

    sql = f"""
    SELECT
        '{rir}' AS rir,
        prefix,
        CAST(split_part(prefix, '/', 2) AS INT) AS prefix_len,
        max_length,
        max_length - CAST(split_part(prefix, '/', 2) AS INT) AS excess,
        asn
    FROM live_roas
    WHERE rir = '{rir}'
      AND max_length > CAST(split_part(prefix, '/', 2) AS INT) + {min_excess - 1}
    ORDER BY excess DESC
    LIMIT {limit}
    """
    rows = con.execute(sql).fetchall()

    total = con.execute(
        f"SELECT COUNT(*) FROM live_roas WHERE rir = '{rir}'",
    ).fetchone()[0]

    entries = [
        MaxLengthIssue(
            rir=r[0], prefix=r[1], prefix_length=r[2],
            max_length=r[3], excess=r[4], asn=r[5],
        )
        for r in rows
    ]

    return MaxLengthResponse(
        rir=rir, total_roas=total, issues_count=len(entries), entries=entries,
    )
