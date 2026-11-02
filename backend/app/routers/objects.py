"""Object inventory endpoints."""

from fastapi import APIRouter, HTTPException, Query

from app.config import RIRS, WAREHOUSE_DIRS
from app.db import _presence_src, get_con, latest_snapshot_ts, live_cutoff, object_summary_cache, parquet_glob
from app.models import ObjectCount, ObjectSummaryResponse

router = APIRouter(prefix="/objects", tags=["Objects"])

OBJECT_TABLES = [
    ("roa_objects", "ROA"),
    ("certs", "Certificate"),
    ("crls", "CRL"),
    ("manifests", "Manifest"),
    ("aspa_objects", "ASPA"),
]


@router.get("/summary", response_model=ObjectSummaryResponse)
def object_summary(
    rir: str = Query(..., description="RIR name (e.g. arin)"),
    current: bool = Query(True, description="Only count currently live objects"),
):
    """Count of objects by type for an RIR."""
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}. Valid: {RIRS}")

    snap_ts = latest_snapshot_ts.get(rir)

    # Use startup cache for current=true (default, most common)
    if current and rir in object_summary_cache:
        counts = [ObjectCount(**c) for c in object_summary_cache[rir]]
        return ObjectSummaryResponse(rir=rir, snapshot=snap_ts, objects=counts)

    # Fallback: live query for current=false
    con = get_con()
    date_cutoff = live_cutoff(rir)
    counts = []
    for table_name, label in OBJECT_TABLES:
        table_dir = WAREHOUSE_DIRS[rir] / "tables" / f"table={table_name}"
        if not table_dir.exists():
            continue
        obj_pq = parquet_glob(rir, table_name)
        if current:
            obj_fl = parquet_glob(rir, "object_presence_stats")
            sql = f"""
                SELECT COUNT(DISTINCT o.object_id)
                FROM read_parquet('{obj_pq}') o
                JOIN {_presence_src(obj_fl)} ofl ON o.object_id = ofl.object_id
                WHERE ofl.last_seen_ts >= '{date_cutoff}'
            """
        else:
            sql = f"SELECT COUNT(DISTINCT object_id) FROM read_parquet('{obj_pq}')"
        row = con.execute(sql).fetchone()
        counts.append(ObjectCount(object_type=label, count=row[0]))

    return ObjectSummaryResponse(rir=rir, snapshot=snap_ts, objects=counts)
