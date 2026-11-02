"""RIR comparison endpoint."""

import re

from fastapi import APIRouter, HTTPException, Query

from app.config import RIRS, WAREHOUSE_DIRS
from app.db import _presence_src, cache_get, cache_set, get_con, parquet_glob
from app.models import RIRComparisonResponse, RIRDataPoint

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/rir-comparison", response_model=RIRComparisonResponse)
def rir_comparison(
    metric: str = Query("roa_count", description="Metric: roa_count, aspa_count, cert_count"),
    granularity: str = Query("month", description="'week' or 'month'"),
    start: str | None = Query(None, description="Start period (e.g. 2024-01)"),
    end: str | None = Query(None, description="End period (e.g. 2024-12)"),
):
    """Compare a metric across all 5 RIRs over time."""

    # Validate inputs
    metric_to_table = {
        "roa_count": "roa_objects",
        "aspa_count": "aspa_objects",
        "cert_count": "certs",
    }
    obj_table = metric_to_table.get(metric)
    if not obj_table:
        raise HTTPException(400, f"Unknown metric: {metric}. Valid: {list(metric_to_table)}")

    for val, name in [(start, "start"), (end, "end")]:
        if val and not re.match(r'^\d{4}(-\d{2}|-W\d{2})$', val):
            raise HTTPException(400, f"Invalid {name} format. Use YYYY-MM or YYYY-Wnn")

    # Check cache
    cache_key = f"rir-comparison:{metric}:{granularity}:{start}:{end}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    fmt = "'%Y-%m'" if granularity == "month" else "'%G-W%V'"

    con = get_con()

    # Build period filter with params
    period_conditions = []
    params: dict = {}
    if start:
        period_conditions.append("period >= $start")
        params["start"] = start
    if end:
        period_conditions.append("period <= $end")
        params["end"] = end
    period_where = (" WHERE " + " AND ".join(period_conditions)) if period_conditions else ""

    # Gather data per RIR
    rir_data: dict[str, dict[str, int]] = {}
    for rir in RIRS:
        table_dir = WAREHOUSE_DIRS[rir] / "tables" / f"table={obj_table}"
        if not table_dir.exists():
            continue

        obj_pq = parquet_glob(rir, obj_table)
        obj_fl = parquet_glob(rir, "object_presence_stats")

        rows = con.execute(
            f"""
            SELECT period, cnt FROM (
                SELECT
                    strftime(CAST(ofl.first_seen_ts AS TIMESTAMP), {fmt}) AS period,
                    COUNT(DISTINCT o.object_id) AS cnt
                FROM read_parquet('{obj_pq}') o
                JOIN {_presence_src(obj_fl)} ofl ON o.object_id = ofl.object_id
                GROUP BY period
            ) sub
            {period_where}
            ORDER BY period
            """,
            params,
        ).fetchall()

        for period, cnt in rows:
            if period not in rir_data:
                rir_data[period] = {}
            rir_data[period][rir] = cnt

    data = []
    for period in sorted(rir_data.keys()):
        vals = rir_data[period]
        data.append(RIRDataPoint(
            period=period,
            afrinic=vals.get("afrinic", 0),
            apnic=vals.get("apnic", 0),
            arin=vals.get("arin", 0),
            lacnic=vals.get("lacnic", 0),
            ripencc=vals.get("ripencc", 0),
        ))

    result = RIRComparisonResponse(metric=metric, granularity=granularity, data=data)
    cache_set(cache_key, result)
    return result
