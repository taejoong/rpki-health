"""Usage statistics endpoint for the lab's access-controlled dashboard.

GET /stats?days=N  ->  aggregated visitor/API analytics (app.statsview).

Gated by a dedicated view token (X-Stats-Token header) kept separate from the
admin token, so the dashboard holds only read-stats rights. Read-only: it never
mutates anything and runs off its own connection to the analytics DB.
"""

import hmac

from fastapi import APIRouter, Header, HTTPException, Query

from app import statsview
from app.config import STATS_TOKEN_FILE

router = APIRouter(tags=["Stats"])


def _stats_token() -> str | None:
    try:
        tok = STATS_TOKEN_FILE.read_text().strip()
        return tok or None
    except FileNotFoundError:
        return None
    except Exception:
        return None


@router.get("/stats")
def usage_stats(
    days: int = Query(30, ge=1, le=365),
    x_stats_token: str | None = Header(default=None),
):
    """Token-gated usage statistics (X-Stats-Token) for the lab's access-controlled dashboard."""
    expected = _stats_token()
    if expected is None:
        raise HTTPException(503, "stats view not configured (token file missing on server)")
    if not x_stats_token or not hmac.compare_digest(x_stats_token, expected):
        raise HTTPException(401, "invalid stats token")
    try:
        return statsview.usage_stats(days=days)
    except Exception as e:  # never leak a stack trace to the edge
        raise HTTPException(500, f"stats error: {type(e).__name__}")
