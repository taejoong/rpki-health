"""ASPA Observatory endpoints."""

from fastapi import APIRouter, HTTPException, Query

from app.config import RIRS, WAREHOUSE_DIRS
from app.db import _presence_src, get_con, live_cutoff, parquet_glob
from app.models import (
    ASPAEntry,
    ASPAListResponse,
    ASPAStatsResponse,
    ASPATrendPoint,
)

router = APIRouter(prefix="/aspa", tags=["ASPA"])

ASPA_RIRS = []  # populated at startup


def _has_aspa(rir: str) -> bool:
    return (WAREHOUSE_DIRS[rir] / "tables" / "table=aspa_objects").exists()


@router.get("/list", response_model=ASPAListResponse)
def aspa_list(
    rir: str | None = Query(None, description="Filter by RIR"),
    customer_asn: int | None = Query(None, description="Filter by customer ASN"),
    current: bool = Query(True, description="Only live objects"),
    limit: int = Query(500, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """List ASPA objects with their provider AS pairs."""
    con = get_con()
    target_rirs = [rir.lower()] if rir else RIRS
    target_rirs = [r for r in target_rirs if _has_aspa(r)]

    all_rows = []
    for r in target_rirs:
        aspa_pq = parquet_glob(r, "aspa_objects")
        prov_pq = parquet_glob(r, "aspa_providers")
        obj_fl = parquet_glob(r, "object_presence_stats")
        cutoff = live_cutoff(r)

        live_clause = f"AND ofl.last_seen_ts >= '{cutoff}'" if current else ""
        asn_clause = f"AND a.customer_asn = {customer_asn}" if customer_asn else ""

        rows = con.execute(
            f"""
            SELECT
                '{r}' AS rir,
                a.customer_asn,
                p.provider_asn,
                p.afi_limit,
                a.signing_time::VARCHAR,
                ofl.first_seen_ts,
                ofl.last_seen_ts
            FROM read_parquet('{aspa_pq}') a
            JOIN read_parquet('{prov_pq}') p ON a.object_id = p.object_id
            JOIN {_presence_src(obj_fl)} ofl ON a.object_id = ofl.object_id
            WHERE 1=1 {live_clause} {asn_clause}
            ORDER BY a.customer_asn, p.provider_asn
            """,
        ).fetchall()
        all_rows.extend(rows)

    total = len(all_rows)
    page = all_rows[offset : offset + limit]

    entries = [
        ASPAEntry(
            rir=r[0], customer_asn=r[1], provider_asn=r[2],
            afi_limit=r[3], signing_time=r[4],
            first_seen=r[5], last_seen=r[6],
        )
        for r in page
    ]

    return ASPAListResponse(count=total, entries=entries)


@router.get("/stats", response_model=ASPAStatsResponse)
def aspa_stats():
    """ASPA deployment summary across all RIRs."""
    con = get_con()
    per_rir = {}
    total_objects = 0
    total_customer_asns = set()

    for r in RIRS:
        if not _has_aspa(r):
            per_rir[r] = {"live_objects": 0, "customer_asns": 0, "provider_pairs": 0}
            continue

        aspa_pq = parquet_glob(r, "aspa_objects")
        prov_pq = parquet_glob(r, "aspa_providers")
        obj_fl = parquet_glob(r, "object_presence_stats")
        cutoff = live_cutoff(r)

        row = con.execute(
            f"""
            SELECT
                COUNT(DISTINCT a.object_id) AS obj_cnt,
                COUNT(DISTINCT a.customer_asn) AS cust_cnt,
                COUNT(*) AS pair_cnt
            FROM read_parquet('{aspa_pq}') a
            JOIN read_parquet('{prov_pq}') p ON a.object_id = p.object_id
            JOIN {_presence_src(obj_fl)} ofl ON a.object_id = ofl.object_id
            WHERE ofl.last_seen_ts >= '{cutoff}'
            """,
        ).fetchone()

        per_rir[r] = {
            "live_objects": row[0],
            "customer_asns": row[1],
            "provider_pairs": row[2],
        }
        total_objects += row[0]

        asns = con.execute(
            f"""
            SELECT DISTINCT a.customer_asn
            FROM read_parquet('{aspa_pq}') a
            JOIN {_presence_src(obj_fl)} ofl ON a.object_id = ofl.object_id
            WHERE ofl.last_seen_ts >= '{cutoff}'
            """,
        ).fetchall()
        total_customer_asns.update(r[0] for r in asns)

    return ASPAStatsResponse(
        total_live_objects=total_objects,
        total_customer_asns=len(total_customer_asns),
        per_rir=per_rir,
    )


@router.get("/trend", response_model=list[ASPATrendPoint])
def aspa_trend(
    rir: str = Query(..., description="RIR name"),
    granularity: str = Query("month", description="'week' or 'month'"),
):
    """ASPA adoption trend over time (new objects per period)."""
    rir = rir.lower()
    if rir not in RIRS or not _has_aspa(rir):
        raise HTTPException(400, f"No ASPA data for {rir}")

    fmt = "'%Y-%m'" if granularity == "month" else "'%G-W%V'"

    con = get_con()
    aspa_pq = parquet_glob(rir, "aspa_objects")
    obj_fl = parquet_glob(rir, "object_presence_stats")

    rows = con.execute(
        f"""
        SELECT
            strftime(CAST(ofl.first_seen_ts AS TIMESTAMP), {fmt}) AS period,
            COUNT(DISTINCT a.object_id) AS new_objects,
            COUNT(DISTINCT a.customer_asn) AS new_customer_asns
        FROM read_parquet('{aspa_pq}') a
        JOIN {_presence_src(obj_fl)} ofl ON a.object_id = ofl.object_id
        GROUP BY period
        ORDER BY period
        """,
    ).fetchall()

    return [
        ASPATrendPoint(period=r[0], new_objects=r[1], new_customer_asns=r[2])
        for r in rows
    ]
