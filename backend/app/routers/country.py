"""Country analytics (Analysis -> Countries): ROA deployment per RIR and per
country over time, and the origin ASes that announce a country's address space.

Everything is precomputed once a day by pipelines/country (crc.assemble) into a
build directory under COUNTRY_RPKI_DIR (app/config.py):

  current -> builds/<YYYYMMDDTHHMMSSZ>/   swapped by an atomic rename
    manifest.json      version, as_of, prev_date, generated_at, grid, bgp_from,
                       method, lenses, sources, notes
    series.parquet     date, level ('all'|'registry'|'cc'), key, af, METRICS
    countries.parquet  cc, af, registry, METRICS, prev_<PREV_METRICS>   at as_of
    ases.parquet       cc, af, AS_COLUMNS                               at as_of
    as_series.parquet  cc, af, asn, date, AS_TREND_METRICS  monthly since 2020 + as_of

This router only filters those files with read_parquet() on the shared DuckDB
connection. It never touches init_db() or hot_reload(), so startup and
/admin/reload are unchanged, and a new build is served from the first request
after the swap. Every endpoint returns 503 until the first build exists.
Units: IPv4 addresses, IPv6 /48s. Dates are UTC days, 'YYYY-MM-DD'.
"""

import json
import logging
import math
import os
import re
import threading
from datetime import date

import duckdb
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from app.config import COUNTRY_RPKI_DIR, RIRS
from app.db import cache_get, cache_set, get_con

log = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics/country", tags=["Country"])

# Metric columns of series.parquet and countries.parquet.
METRICS = (
    "routes", "routes_valid", "routes_inv_as", "routes_inv_len", "routes_nf",
    "space", "space_valid", "space_invalid", "space_nf",
    "ases", "ases_any_valid", "ases_all_valid", "ases_any_invalid",
    "deleg", "deleg_cov", "deleg_cov_noas0", "vrps",
)
# countries.parquet also has prev_<m>, the value at the manifest's prev_date.
PREV_METRICS = (
    "routes", "routes_valid", "routes_inv_as", "routes_inv_len",
    "space", "space_valid", "deleg", "deleg_cov", "ases", "ases_any_valid",
)
# /ases row fields in response order. Also the sort allow-list.
AS_COLUMNS = (
    "asn", "name", "as_cc",
    "routes", "routes_valid", "routes_inv_as", "routes_inv_len", "routes_nf",
    "space", "space_valid", "space_invalid", "space_nf",
    "pct_valid", "pct_space_valid",
    "prev_routes", "prev_routes_valid", "prev_space", "prev_space_valid",
    "first_valid",
)
_AS_FLOAT_COLUMNS = {
    "space", "space_valid", "space_invalid", "space_nf", "pct_valid",
    "pct_space_valid", "prev_space", "prev_space_valid",
}
AS_TREND_METRICS = (
    "routes", "routes_valid", "routes_inv_as", "routes_inv_len", "routes_nf",
    "space", "space_valid",
)
META_FIELDS = (
    "version", "as_of", "prev_date", "generated_at", "grid", "bgp_from",
    "method", "lenses", "sources", "notes",
)
SERIES_KEYS = ("all", *RIRS)  # /rir-trend: level 'all' + the five RIRs at level 'registry'

NOT_BUILT = "Country analytics not built yet"
UNREADABLE = "Country analytics build unreadable"
# /ases paging. A country has at most tens of thousands of origin ASes; without a
# bound, an offset >= 2**63 overflowed DuckDB's BIGINT OFFSET (a 500).
MAX_OFFSET = 1_000_000

_CC_RE = re.compile(r"[A-Za-z]{2}")
_ASN_RE = re.compile(r"(?:AS)?\s*(\d{1,10})", re.IGNORECASE)

# Parameters cast to the column types (af UTINYINT, asn UBIGINT). With a bare
# int parameter DuckDB casts the column instead, and the Parquet reader can no
# longer skip row groups by their min/max statistics.
_AF = "af = CAST($af AS UTINYINT)"
_ASN = "asn = CAST($asn AS UBIGINT)"

_build_lock = threading.Lock()
_build_state: dict = {"dir": None, "manifest": None}


def _build() -> tuple[str, dict]:
    """(resolved build directory, its manifest). 503 until a build exists.

    ``current`` is resolved on every request; manifest.json is read again only
    when it points somewhere new.
    """
    try:
        target = os.path.realpath(os.path.join(COUNTRY_RPKI_DIR, "current"), strict=True)
    except OSError:
        raise HTTPException(503, NOT_BUILT)
    with _build_lock:
        if _build_state["dir"] != target:
            try:
                with open(os.path.join(target, "manifest.json")) as f:
                    # NaN/Infinity tokens, and numbers too large for a float (1e999), -> null
                    manifest = json.load(f, parse_constant=lambda _: None, parse_float=_finite_float)
                if not isinstance(manifest, dict):
                    raise ValueError("manifest is not a JSON object")
            except (OSError, ValueError) as e:
                log.warning("Country build %s unreadable: %s", target, e)
                raise HTTPException(503, UNREADABLE)
            _build_state.update(dir=target, manifest=manifest)
        return target, _build_state["manifest"]


def _finite_float(s: str) -> float | None:
    v = float(s)
    return v if math.isfinite(v) else None


def _src(build_dir: str, name: str) -> str:
    """read_parquet() of one build file. The path comes from the resolved build
    directory, never from the request. hive_partitioning = false: a key=value
    directory name must not turn into a column."""
    path = os.path.join(build_dir, f"{name}.parquet").replace("'", "''")
    return f"read_parquet('{path}', hive_partitioning = false)"


def _val(v):
    """JSON-safe value: NaN/Inf -> None, DATE -> 'YYYY-MM-DD'."""
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, date):
        return v.isoformat()[:10]
    return v


def _rows(sql: str, params: dict) -> list[tuple]:
    """Run one query on the shared connection and return JSON-safe rows. A build
    file that is missing (IOException), truncated or not Parquet (an
    InvalidInputException naming the file) is a 503 like a missing build;
    anything else is a bug."""
    try:
        rows = get_con().execute(sql, params).fetchall()
    except (duckdb.IOException, duckdb.InvalidInputException) as e:
        if isinstance(e, duckdb.InvalidInputException) and ".parquet" not in str(e):
            raise
        log.warning("Country build file unreadable: %s", e)
        raise HTTPException(503, UNREADABLE)
    return [tuple(_val(v) for v in r) for r in rows]


def _envelope(manifest: dict, **fields) -> dict:
    """Every response starts with the build's version and as_of."""
    return {"version": manifest.get("version"), "as_of": manifest.get("as_of"), **fields}


def _cached(key: str, make) -> Response:
    """Response-cached view (key includes the resolved build dir). The cache keeps
    the rendered JSON body: re-encoding the ~0.7 MB rir-trend dict on every hit
    cost ~100 ms, and hot_reload() clears the response cache every 30 min, so
    misses are common too; json.dumps keeps FastAPI's jsonable_encoder (~75 ms
    for rir-trend) off that path. Same bytes as JSONResponse; NaN/Inf raise."""
    body = cache_get(key)
    if body is None:
        body = json.dumps(make(), ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
        cache_set(key, body)
    return Response(body, media_type="application/json")


def _af(af: int) -> int:
    if af not in (4, 6):
        raise HTTPException(400, "af must be 4 or 6")
    return af


def _cc(cc: str) -> str:
    if not _CC_RE.fullmatch(cc):
        raise HTTPException(400, "cc must be a two-letter country code, e.g. KR")
    return cc.upper()


def _sort_key(col: str) -> str:
    """ORDER BY expression for an allow-listed /ases column."""
    if col in _AS_FLOAT_COLUMNS:  # NaN/Inf are served as null, so they sort with the nulls
        return f'CASE WHEN isfinite("{col}") THEN "{col}" END'
    if col == "name":
        return '"name" COLLATE NOCASE'
    return f'"{col}"'


@router.get("/meta")
def country_meta():
    """Build manifest: as_of, prev_date (the 1-year comparison date), time grid,
    method, lens definitions, sources and data-quality notes."""
    d, m = _build()
    return _cached(f"country:meta:{d}", lambda: {k: m.get(k) for k in META_FIELDS})


@router.get("/countries")
def country_list(af: int = Query(4, description="Address family: 4 or 6")):
    """Every country at as_of (the ranking table): dominant registry, all
    metrics, and under ``prev`` the same counts at prev_date."""
    af = _af(af)
    d, m = _build()

    def make():
        head = ("cc", "registry", *METRICS)
        prev_cols = ", ".join(f"prev_{p}" for p in PREV_METRICS)
        rows = []
        for r in _rows(
            f"SELECT {', '.join(head)}, {prev_cols} FROM {_src(d, 'countries')} "
            f"WHERE {_AF} ORDER BY cc",
            {"af": af},
        ):
            row = dict(zip(head, r))
            row["prev"] = dict(zip(PREV_METRICS, r[len(head):]))
            rows.append(row)
        return _envelope(m, prev_date=m.get("prev_date"), af=af, rows=rows)

    return _cached(f"country:countries:{d}:{af}", make)


@router.get("/rir-trend")
def rir_trend(af: int = Query(4, description="Address family: 4 or 6")):
    """All RIRs and each RIR over the whole grid, columnar: ``dates``, and per
    series one array per metric aligned to ``dates`` (null where missing). A
    route counts for the RIR whose delegation holds its address space."""
    af = _af(af)
    d, m = _build()

    def make():
        rirs = ", ".join(f"'{r}'" for r in RIRS)
        rows = _rows(
            f"SELECT key, date, {', '.join(METRICS)} FROM {_src(d, 'series')} "
            f"WHERE {_AF} AND ((level = 'all' AND key = 'all') "
            f"OR (level = 'registry' AND key IN ({rirs}))) ORDER BY date, key",
            {"af": af},
        )
        dates = sorted({r[1] for r in rows})
        pos = {day: i for i, day in enumerate(dates)}
        series = {k: {mt: [None] * len(dates) for mt in METRICS} for k in SERIES_KEYS}
        for key, day, *vals in rows:
            s, i = series[key], pos[day]
            for mt, v in zip(METRICS, vals):
                s[mt][i] = v
        return _envelope(m, af=af, dates=dates, series=series)

    return _cached(f"country:rir-trend:{d}:{af}", make)


@router.get("/trend")
def country_trend(
    cc: str = Query(..., description="Two-letter country code, e.g. KR"),
    af: int = Query(4, description="Address family: 4 or 6"),
):
    """One country over the whole grid, columnar like /rir-trend. BGP-based
    metrics are null before bgp_from. 404 if the country never appears; empty
    arrays if it does but not in this address family.

    Not response-cached: a filtered read takes milliseconds, and ~250
    countries x 2 families would crowd the shared 256-entry cache."""
    cc, af = _cc(cc), _af(af)
    d, m = _build()
    src = _src(d, "series")
    rows = _rows(
        f"SELECT date, {', '.join(METRICS)} FROM {src} "
        f"WHERE level = 'cc' AND key = $cc AND {_AF} ORDER BY date",
        {"cc": cc, "af": af},
    )
    if not rows and not _rows(f"SELECT 1 FROM {src} WHERE level = 'cc' AND key = $cc LIMIT 1", {"cc": cc}):
        raise HTTPException(404, f"Unknown country: {cc}")
    registry = _rows(  # the same for both families; prefer the requested one's row
        f"SELECT registry FROM {_src(d, 'countries')} WHERE cc = $cc ORDER BY {_AF} DESC, af LIMIT 1",
        {"cc": cc, "af": af},
    )
    return _envelope(
        m, cc=cc, af=af, registry=registry[0][0] if registry else None,
        dates=[r[0] for r in rows],
        metrics={mt: [r[i] for r in rows] for i, mt in enumerate(METRICS, 1)},
    )


@router.get("/ases")
def country_ases(
    cc: str = Query(..., description="Two-letter country code, e.g. KR"),
    af: int = Query(4, description="Address family: 4 or 6"),
    sort: str = Query("routes", description="Any field of a row"),
    order: str = Query("desc", description="'asc' or 'desc'"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0, le=MAX_OFFSET),
    q: str | None = Query(None, max_length=100, description="ASN (4766 or AS4766) or part of the AS name"),
):
    """Origin ASes announcing the country's address space at as_of, with their
    values at prev_date and first_valid (first grid date with a valid route).
    Sorted server-side with nulls last and ties by asn, paged, filtered by q."""
    cc, af = _cc(cc), _af(af)
    if sort not in AS_COLUMNS:
        raise HTTPException(400, f"Unknown sort column: {sort}. Valid: {list(AS_COLUMNS)}")
    order = order.lower()
    if order not in ("asc", "desc"):
        raise HTTPException(400, "order must be 'asc' or 'desc'")
    d, m = _build()
    where, params = f"cc = $cc AND {_AF}", {"cc": cc, "af": af}
    q = (q or "").strip()
    if q:
        # q is matched literally: escape ILIKE's wildcards
        params["pat"] = "%" + re.sub(r"([\\%_])", r"\\\1", q) + "%"
        match = "name ILIKE $pat ESCAPE '\\'"
        asn = _ASN_RE.fullmatch(q)
        if asn:
            params["asn"] = int(asn.group(1))
            match = f"({_ASN} OR {match})"
        where += f" AND {match}"
    src = _src(d, "ases")
    total = _rows(f"SELECT COUNT(*) FROM {src} WHERE {where}", params)[0][0]
    rows = _rows(
        f"SELECT {', '.join(AS_COLUMNS)} FROM {src} WHERE {where} "
        f"ORDER BY {_sort_key(sort)} {order.upper()} NULLS LAST, asn ASC "
        "LIMIT $limit OFFSET $offset",
        {**params, "limit": limit, "offset": offset},
    )
    return _envelope(
        m, prev_date=m.get("prev_date"), cc=cc, af=af, total=total, offset=offset, limit=limit,
        rows=[dict(zip(AS_COLUMNS, r)) for r in rows],
    )


@router.get("/as-trend")
def country_as_trend(
    cc: str = Query(..., description="Two-letter country code, e.g. KR"),
    af: int = Query(4, description="Address family: 4 or 6"),
    asn: int = Query(..., ge=0, le=4294967295, description="Origin AS number"),
):
    """One origin AS within one country: the first grid date of every month
    since bgp_from, plus as_of. Columnar like /trend."""
    cc, af = _cc(cc), _af(af)
    d, m = _build()
    rows = _rows(
        f"SELECT date, {', '.join(AS_TREND_METRICS)} FROM {_src(d, 'as_series')} "
        f"WHERE cc = $cc AND {_AF} AND {_ASN} ORDER BY date",
        {"cc": cc, "af": af, "asn": asn},
    )
    if not rows:
        raise HTTPException(404, f"No data for AS{asn} in {cc} (IPv{af})")
    return _envelope(
        m, cc=cc, af=af, asn=asn, dates=[r[0] for r in rows],
        metrics={mt: [r[i] for r in rows] for i, mt in enumerate(AS_TREND_METRICS, 1)},
    )
