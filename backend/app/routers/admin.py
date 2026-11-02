"""Admin endpoints: visitor analytics dashboard data (protected by the admin token)."""

from fastapi import APIRouter, Header, HTTPException, Query

from app import tracking

router = APIRouter(prefix="/admin", tags=["Admin"])


def _require_admin(token: str | None) -> None:
    if tracking.admin_token() is None:
        raise HTTPException(503, "Admin not configured (token file missing on server)")
    if not tracking.verify_token(token):
        raise HTTPException(401, "Invalid admin token")


@router.get("/stats")
def admin_stats(
    days: int = Query(14, ge=1, le=90),
    x_admin_token: str | None = Header(None),
):
    """Visitor analytics for the last N days. Requires X-Admin-Token header."""
    _require_admin(x_admin_token)
    return tracking.stats(days=days)
