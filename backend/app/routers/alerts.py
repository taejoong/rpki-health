"""ROA change detection endpoints."""

from fastapi import APIRouter, HTTPException, Query

from app.config import RIRS
from app.db import cache_get, cache_set, get_con
from app.models import RecentChangesResponse, ROAChange

router = APIRouter(prefix="/alerts", tags=["Alerts"])


@router.get("/recent", response_model=RecentChangesResponse)
def recent_changes(
    asn: int | None = Query(None, description="Filter by ASN"),
    rir: str = Query(..., description="RIR name"),
    days: int = Query(7, ge=1, le=90, description="Look back N days"),
    limit: int = Query(500, ge=1, le=10000),
):
    """Detect recent ROA changes: new and withdrawn."""
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}. Valid: {RIRS}")

    cache_key = f"alerts:{rir}:{asn}:{days}:{limit}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    con = get_con()

    # Get data edge (max last_seen) for this RIR
    max_ts = con.execute(
        "SELECT MAX(CAST(last_seen_ts AS TIMESTAMP)) FROM roa_history WHERE rir = $rir",
        {"rir": rir},
    ).fetchone()[0]

    if not max_ts:
        return RecentChangesResponse(since=f"{days} days ago", count=0, changes=[])

    asn_filter = "AND asn = $asn" if asn else ""
    params = {"rir": rir}
    if asn:
        params["asn"] = asn

    all_changes: list[ROAChange] = []

    # New ROAs
    new_rows = con.execute(
        f"""
        WITH edge AS (SELECT MAX(CAST(last_seen_ts AS TIMESTAMP)) AS mx FROM roa_history WHERE rir = $rir)
        SELECT asn, prefix, max_length, object_hash, first_seen_ts, last_seen_ts
        FROM roa_history, edge
        WHERE rir = $rir
          AND CAST(last_seen_ts AS TIMESTAMP) >= edge.mx - INTERVAL '{days} days'
          AND CAST(first_seen_ts AS TIMESTAMP) >= edge.mx - INTERVAL '{days} days'
          {asn_filter}
        ORDER BY first_seen_ts DESC
        LIMIT {limit}
        """,
        params,
    ).fetchall()

    for row in new_rows:
        all_changes.append(ROAChange(
            rir=rir, change_type="new", asn=row[0], prefix=row[1],
            max_length=row[2], object_hash=row[3],
            first_seen=row[4], last_seen=row[5],
        ))

    # Withdrawn ROAs
    withdrawn_rows = con.execute(
        f"""
        WITH edge AS (SELECT MAX(CAST(last_seen_ts AS TIMESTAMP)) AS mx FROM roa_history WHERE rir = $rir)
        SELECT asn, prefix, max_length, object_hash, first_seen_ts, last_seen_ts
        FROM roa_history, edge
        WHERE rir = $rir
          AND CAST(last_seen_ts AS TIMESTAMP) < edge.mx
          AND CAST(last_seen_ts AS TIMESTAMP) >= edge.mx - INTERVAL '{days} days'
          {asn_filter}
        ORDER BY last_seen_ts DESC
        LIMIT {limit}
        """,
        params,
    ).fetchall()

    for row in withdrawn_rows:
        all_changes.append(ROAChange(
            rir=rir, change_type="withdrawn", asn=row[0], prefix=row[1],
            max_length=row[2], object_hash=row[3],
            first_seen=row[4], last_seen=row[5],
        ))

    all_changes.sort(key=lambda c: c.first_seen or c.last_seen or "", reverse=True)
    all_changes = all_changes[:limit]

    result = RecentChangesResponse(
        since=f"{days} days ago (data edge: {str(max_ts)[:19]})",
        count=len(all_changes),
        changes=all_changes,
    )
    cache_set(cache_key, result)
    return result
