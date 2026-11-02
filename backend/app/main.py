"""RPKI Warehouse API — ROA Visualizer Backend."""

import logging
from collections import OrderedDict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app import tracking
from app.client_ip import client_ip, over_limit, rate_key
from app.db import init_db
from app.routers import (
    admin, alerts, analytics, aspa, ca, comparison, meta, objects, roa, validity,
)
from app.routers import stats
from app.routers import country

# Deployments may add site-specific routers in an optional module
# app/extensions.py: routers(), init() (run at startup) and API_PREFIXES (see
# app/statsview.py). Its routes stay out of the OpenAPI document.
try:
    import app.extensions as extensions
except ModuleNotFoundError as e:
    if e.name != "app.extensions":
        raise
    extensions = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

_extension_routers = extensions.routers() if extensions else []
if extensions:
    logging.getLogger("app.main").info(
        "app.extensions: %d router(s), %d route(s)",
        len(_extension_routers), sum(len(r.routes) for r in _extension_routers))


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    tracking.init_db()
    if extensions:
        extensions.init()
    yield


app = FastAPI(
    title="RPKI Warehouse API",
    description="REST API for querying RPKI longitudinal data warehouse",
    version="0.2.0",
    lifespan=lifespan,
)

# CORS — allow frontend origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://rpki.netsecurelab.org", "https://rpki-warehouse.pages.dev", "http://localhost:5174"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Rate limiter (per-IP, in-memory) ──
# Keyed by client_ip(); over_limit() caps it at MAX_KEYS distinct IPs.
_rate_limits: OrderedDict[str, list[float]] = OrderedDict()
RATE_LIMIT = 60  # requests per window
RATE_WINDOW = 60  # seconds


# Paths excluded from visitor logging (infra/admin noise)
_TRACK_SKIP_PREFIXES = ("/admin", "/docs", "/redoc", "/openapi")
_TRACK_SKIP_EXACT = {"/health", "/favicon.ico", "/stats"}


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Token-gated, and the cron's loopback POST carries no Cloudflare headers, so it
    # would share the "127.0.0.1" bucket with any other local caller; keep it out
    # of the limiter.
    if request.url.path == "/admin/reload":
        return await call_next(request)

    ip = client_ip(request)
    if over_limit(_rate_limits, rate_key(ip), RATE_LIMIT, RATE_WINDOW):
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Try again later."},
        )

    # Record the visit (GET only, skip infra/admin paths). Never breaks requests.
    path = request.url.path
    if (
        request.method == "GET"
        and path not in _TRACK_SKIP_EXACT
        and not path.startswith(_TRACK_SKIP_PREFIXES)
    ):
        tracking.log_event(
            ip=ip,
            country=request.headers.get("X-Visitor-Country"),
            ua=request.headers.get("User-Agent"),
            path=path,
            query=request.url.query,
        )

    # Global error handler — never crash the server
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logging.getLogger("app").error("Unhandled error: %s", e, exc_info=True)
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})


app.include_router(roa.router)
app.include_router(admin.router)
app.include_router(analytics.router)
app.include_router(comparison.router)
app.include_router(ca.router)
app.include_router(aspa.router)
app.include_router(objects.router)
app.include_router(alerts.router)
app.include_router(meta.router)
app.include_router(validity.router)
for router in _extension_routers:
    app.include_router(router, include_in_schema=False)
app.include_router(stats.router)
app.include_router(country.router)
