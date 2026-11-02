"""RP-validity summary endpoints.

The warehouse ingests every object rpki-client caches (repo-present). RP-validity
is tracked separately in the time-varying object_validity table; the backend
materializes the latest cycle's reject set into ``current_invalid`` at startup.
These endpoints expose the repo-present vs RP-valid split (the reconciliation:
repo-present total vs the ~RP-valid figure a public validator reports).
"""

from fastapi import APIRouter, HTTPException, Query

from app.config import RIRS
from app.db import get_con, latest_validation_ts, object_summary_cache
from app.models import (
    InvalidObjectEntry,
    InvalidObjectsResponse,
    ValidityReasonCount,
    ValiditySummaryResponse,
)

router = APIRouter(prefix="/validity", tags=["Validity"])


def _repo_present(rir: str) -> int:
    """Currently-live object count for an RIR (sum of cached per-type counts)."""
    return sum(c["count"] for c in object_summary_cache.get(rir, []))


@router.get("/summary", response_model=ValiditySummaryResponse)
def validity_summary(
    rir: str | None = Query(
        None, description="RIR name (e.g. arin). Omit to aggregate all RIRs."
    ),
):
    """Repo-present vs RP-valid vs invalid, with a per-reason breakdown.

    ``rp_valid = repo_present - invalid``. Until the first validation cycle writes
    object_validity, ``invalid`` is 0 and everything reads as valid.
    """
    con = get_con()

    if rir is not None:
        rir = rir.lower()
        if rir not in RIRS:
            raise HTTPException(400, f"Unknown RIR: {rir}. Valid: {RIRS}")
        rirs = [rir]
        label = rir
    else:
        rirs = RIRS
        label = "all"

    repo_present = sum(_repo_present(r) for r in rirs)

    placeholders = ",".join(f"'{r}'" for r in rirs)
    invalid = con.execute(
        f"SELECT COUNT(*) FROM current_invalid WHERE rir IN ({placeholders})"
    ).fetchone()[0]
    reason_rows = con.execute(
        f"""
        SELECT reason_code, COUNT(*) AS n
        FROM current_invalid
        WHERE rir IN ({placeholders})
        GROUP BY reason_code
        ORDER BY n DESC
        """
    ).fetchall()

    ts_values = [latest_validation_ts[r] for r in rirs if r in latest_validation_ts]
    latest_ts = max(ts_values) if ts_values else None

    return ValiditySummaryResponse(
        rir=label,
        latest_validation_ts=latest_ts,
        repo_present=repo_present,
        rp_valid=max(repo_present - invalid, 0),
        invalid=invalid,
        by_reason=[
            ValidityReasonCount(reason_code=rc or "unknown", count=n)
            for rc, n in reason_rows
        ],
    )


@router.get("/objects", response_model=InvalidObjectsResponse)
def invalid_objects(
    rir: str | None = Query(None, description="RIR name (e.g. arin). Omit for all."),
    reason: str | None = Query(None, description="Filter by reason_code (e.g. expired)."),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    """List the objects rpki-client rejected in the latest validation cycle.

    Reads the in-memory ``current_invalid`` table, which is already enriched with
    object type / label / lifecycle at load time (see db._build_current_invalid_table),
    so this is a trivial filter. Empty until the first cron cycle writes object_validity.
    """
    if rir is not None:
        rir = rir.lower()
        if rir not in RIRS:
            raise HTTPException(400, f"Unknown RIR: {rir}. Valid: {RIRS}")
        rirs = [rir]
        label = rir
    else:
        rirs = RIRS
        label = "all"

    con = get_con()
    placeholders = ",".join(f"'{r}'" for r in rirs)
    where = f"rir IN ({placeholders})"
    params: list = []
    if reason is not None:
        where += " AND reason_code = ?"
        params.append(reason)

    total = con.execute(
        f"SELECT COUNT(*) FROM current_invalid WHERE {where}", params
    ).fetchone()[0]

    rows = con.execute(
        f"""
        SELECT rir, object_id, reason_code, object_type, detail, object_hash,
               first_seen, last_seen
        FROM current_invalid
        WHERE {where}
        ORDER BY rir, object_type, object_id
        LIMIT {limit} OFFSET {offset}
        """,
        params,
    ).fetchall()
    entries = [
        InvalidObjectEntry(
            rir=row[0],
            object_id=row[1],
            reason_code=row[2] or "unknown",
            object_type=row[3],
            detail=row[4],
            object_hash=row[5],
            first_seen=row[6],
            last_seen=row[7],
        )
        for row in rows
    ]

    return InvalidObjectsResponse(
        rir=label,
        reason=reason,
        count=total,
        truncated=offset + len(entries) < total,
        entries=entries,
    )
