"""Health and metadata endpoints."""

import asyncio
import hmac
import threading
from concurrent.futures import Future

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import JSONResponse

from app.config import RELOAD_TOKEN_FILE, RIRS
from app.db import (
    cache_get,
    cache_set,
    get_con,
    hot_reload,
    latest_snapshot_ts,
    object_summary_cache,
    reload_running,
    warehouse_meta,
)
from app.models import HealthResponse, WarehouseInfo, WarehouseListResponse

router = APIRouter(tags=["Meta"])


def _live_counts():
    """Distinct live ROA objects and VRPs, globally and per-RIR.

    A ROA is one cryptographic object (object_hash); a VRP is a distinct
    (asn, prefix, max_length) payload. One ROA can carry several VRPs, so
    these two numbers differ and must not be conflated.
    Cached to keep /health cheap.
    """
    cached = cache_get("live_counts")
    if cached is not None:
        return cached

    con = get_con()
    # Per-RIR distinct ROA objects and distinct VRPs.
    per_rir = {}
    rows = con.execute(
        """
        SELECT rir,
               COUNT(DISTINCT object_hash) AS roa_cnt,
               COUNT(DISTINCT (asn || '|' || prefix || '|' || max_length)) AS vrp_cnt
        FROM live_roas
        GROUP BY rir
        """
    ).fetchall()
    for rir, roa_cnt, vrp_cnt in rows:
        per_rir[rir] = (roa_cnt, vrp_cnt)

    # Global totals. object_hash is unique per-RIR, so ROA total = sum of per-RIR.
    # VRPs can be shared across RIRs, so the global VRP count is computed fresh.
    roa_total = sum(r for r, _ in per_rir.values())
    vrp_total = con.execute(
        "SELECT COUNT(DISTINCT (asn || '|' || prefix || '|' || max_length)) FROM live_roas"
    ).fetchone()[0]

    result = {"roa_total": roa_total, "vrp_total": vrp_total, "per_rir": per_rir}
    cache_set("live_counts", result)
    return result


@router.get("/health", response_model=HealthResponse)
def health():
    """API health check with warehouse freshness info."""
    counts = _live_counts()

    wh_infos = []
    for rir in RIRS:
        meta = warehouse_meta.get(rir, {})
        roa_count, vrp_count = counts["per_rir"].get(rir, (0, 0))
        # Fallback: if metadata.json is missing, compute counts from object_summary_cache
        obj_count = meta.get("object_count")
        unique_count = meta.get("unique_object_count")
        if obj_count is None and rir in object_summary_cache:
            obj_count = sum(c["count"] for c in object_summary_cache[rir])
            unique_count = obj_count  # best approximation without metadata

        wh_infos.append(WarehouseInfo(
            rir=rir,
            latest_snapshot=latest_snapshot_ts.get(rir),
            snapshot_count=meta.get("snapshot_count", 0),
            object_count=obj_count,
            unique_object_count=unique_count,
            live_roa_count=roa_count,
            live_vrp_count=vrp_count,
        ))

    return HealthResponse(
        status="ok",
        live_roa_total=counts["roa_total"],
        live_vrp_total=counts["vrp_total"],
        warehouses=wh_infos,
    )


@router.get("/meta/warehouses", response_model=WarehouseListResponse)
def list_warehouses():
    """Detailed info per warehouse."""
    counts = _live_counts()
    wh_infos = []
    for rir in RIRS:
        meta = warehouse_meta.get(rir, {})
        roa_count, vrp_count = counts["per_rir"].get(rir, (0, 0))
        wh_infos.append(WarehouseInfo(
            rir=rir,
            latest_snapshot=latest_snapshot_ts.get(rir),
            snapshot_count=meta.get("snapshot_count", 0),
            object_count=meta.get("object_count"),
            unique_object_count=meta.get("unique_object_count"),
            live_roa_count=roa_count,
            live_vrp_count=vrp_count,
        ))
    return WarehouseListResponse(warehouses=wh_infos)


def _reload_token() -> str | None:
    try:
        return RELOAD_TOKEN_FILE.read_text().strip() or None
    except FileNotFoundError:
        return None


@router.post("/admin/reload")
async def trigger_reload(
    wait: bool = Query(False, description="Wait for the reload and return its result (cron uses this)"),
    x_reload_token: str | None = Header(None),
):
    """Hot-reload all materialized tables without downtime.
    Builds new tables in background, then swaps atomically.

    Requires X-Reload-Token (~/.config/rpki-reload.token): the API is public
    through the Cloudflare tunnel and each reload is a ~12 min, 30+ GB rebuild.
    With ``wait=true`` the status code reports the outcome (200 ok, 500 failed)
    so the cron heartbeat only fires on a successful reload. The rebuild always
    runs on a daemon thread that the request only awaits, so stopping the service
    mid-reload is not held up by it (uvicorn --timeout-graceful-shutdown).
    """
    expected = _reload_token()
    if expected is None:
        raise HTTPException(503, "Reload not configured (token file missing on server)")
    if not x_reload_token or not hmac.compare_digest(x_reload_token.encode(), expected.encode()):
        raise HTTPException(401, "Invalid reload token")
    if reload_running():
        return JSONResponse({"status": "already_running"}, status_code=409)

    done: Future = Future()

    def run():
        try:
            done.set_result(hot_reload())
        except BaseException as e:  # hot_reload reports its own errors; backstop only
            done.set_exception(e)

    threading.Thread(target=run, daemon=True).start()
    if not wait:
        return JSONResponse(
            {"status": "reload_started", "message": "Rebuilding tables in background. Check /health for updated snapshot timestamps."},
            status_code=202,
        )
    result = await asyncio.wrap_future(done)
    code = {"ok": 200, "already_running": 409}.get(result["status"], 500)
    return JSONResponse(result, status_code=code)
