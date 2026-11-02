"""DuckDB connection manager and warehouse data loader."""

import fcntl
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from app.config import PRESENCE_LOCK_FILE, RIRS, WAREHOUSE_DIRS

log = logging.getLogger(__name__)

_con: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()

# Warehouse metadata populated at startup
warehouse_meta: dict[str, dict] = {}
latest_snapshot_ts: dict[str, str] = {}
_live_cutoffs: dict[str, str] = {}
object_summary_cache: dict[str, list] = {}  # rir -> [{object_type, count}, ...]
latest_validation_ts: dict[str, str] = {}  # rir -> MAX(object_validity.dataset_ts)


def live_cutoff(rir: str) -> str:
    """Return the date cutoff for 'live' object queries."""
    return _live_cutoffs.get(rir, "2020-01-01")


def get_con() -> duckdb.DuckDBPyConnection:
    """Return a thread-safe cursor. Each caller gets its own cursor to avoid
    concurrent access segfaults on the shared connection."""
    assert _con is not None, "DB not initialized — call init_db() first"
    with _lock:
        return _con.cursor()


PRESENCE_LOCK_WAIT_S = 30 * 60


@contextmanager
def _presence_lock_shared(required: bool):
    """Hold PRESENCE_LOCK_FILE shared while reading the warehouses.

    build_presence_runs.py (daily 01:00) rewrites object_presence_stats parts in
    place under an exclusive lock. Reading them mid-rewrite fails with "No magic
    bytes found" or, worse, mixes old and new parts. Waits at most
    PRESENCE_LOCK_WAIT_S; after that ``required`` decides between TimeoutError
    (a reload just skips this run) and carrying on unlocked (startup: an API
    with the old race beats no API).
    """
    try:
        try:
            fd = os.open(PRESENCE_LOCK_FILE, os.O_RDONLY)
        except FileNotFoundError:
            fd = os.open(PRESENCE_LOCK_FILE, os.O_RDONLY | os.O_CREAT, 0o664)
    except OSError as e:
        log.warning("Cannot open %s (%s); reading without the presence lock", PRESENCE_LOCK_FILE, e)
        yield
        return
    try:
        t0 = time.time()
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() - t0 > PRESENCE_LOCK_WAIT_S:
                    if required:
                        raise TimeoutError(f"{PRESENCE_LOCK_FILE} still held after {PRESENCE_LOCK_WAIT_S}s")
                    log.warning("%s still held after %ds; reading without it", PRESENCE_LOCK_FILE, PRESENCE_LOCK_WAIT_S)
                    break
                time.sleep(5)
        if time.time() - t0 > 1:
            log.info("Waited %.0fs for %s", time.time() - t0, PRESENCE_LOCK_FILE)
        yield
    finally:
        os.close(fd)  # releases the lock


def _build_tables(con: duckdb.DuckDBPyConnection, suffix: str = "") -> dict[str, str]:
    """Materialize every derived table as ``<name><suffix>``, in dependency order.
    Keep in step with _RELOAD_TABLES. Returns the per-RIR validation cycle
    timestamps behind the new current_invalid (for latest_validation_ts)."""
    invalid, ca = f"current_invalid{suffix}", f"ca_certs{suffix}"
    validation_ts = _build_current_invalid_table(con, invalid)
    _materialize_live_roas(con, f"live_roas{suffix}", invalid)
    _materialize_roa_history(con, f"roa_history{suffix}")
    _materialize_ca_certs(con, ca)
    _materialize_resource_ca(con, f"ca_resource_map{suffix}", ca, invalid)
    return validation_ts


def _set_latest_validation_ts(new: dict[str, str]) -> None:
    # In place: routers import the dict itself
    latest_validation_ts.clear()
    latest_validation_ts.update(new)


def init_db() -> None:
    global _con
    _con = duckdb.connect(":memory:")
    _con.execute("SET threads = 16")
    _con.execute("SET memory_limit = '64GB'")

    with _presence_lock_shared(required=False):
        _discover_warehouses()
        _set_latest_validation_ts(_build_tables(get_con()))
        _cache_object_summaries()


def _discover_warehouses() -> None:
    """Scan each warehouse for latest snapshot metadata."""
    for rir in RIRS:
        wdir = WAREHOUSE_DIRS[rir]
        snap_dir = wdir / "snapshots"
        if not snap_dir.is_dir():
            log.warning("No snapshots dir for %s", rir)
            continue

        snaps = sorted(d.name for d in snap_dir.iterdir() if d.name.startswith("ts="))
        if not snaps:
            continue

        latest_snap = snaps[-1]
        latest_snapshot_ts[rir] = latest_snap.replace("ts=", "")

        # Compute cutoff from actual max(last_ts) in object_presence_stats,
        # since it may lag behind the latest snapshot timestamp
        obj_fl = parquet_glob(rir, "object_presence_stats")
        try:
            max_ts = _con.execute(
                f"SELECT MAX(last_ts) FROM read_parquet('{obj_fl}')"
            ).fetchone()[0]
            if max_ts:
                cutoff_date = datetime.strptime(str(max_ts)[:10], "%Y-%m-%d")
                _live_cutoffs[rir] = (cutoff_date - timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                _live_cutoffs[rir] = "2020-01-01"
        except Exception:
            snap_date = datetime.strptime(latest_snapshot_ts[rir][:10], "%Y-%m-%d")
            _live_cutoffs[rir] = (snap_date - timedelta(days=1)).strftime("%Y-%m-%d")
        meta_path = snap_dir / latest_snap / "metadata.json"
        if meta_path.exists():
            with open(meta_path) as f:
                warehouse_meta[rir] = json.load(f)
                warehouse_meta[rir]["snapshot_count"] = len(snaps)
        else:
            warehouse_meta[rir] = {"snapshot_count": len(snaps)}

        log.info("%s: latest snapshot %s (%d total)", rir, latest_snap, len(snaps))


def parquet_glob(rir: str, table: str) -> str:
    # Part files only: the ETL writes <table>-part-*.parquet (the presence tables
    # part-NNNNN.parquet) and points <table>.parquet at the newest part after every
    # flush, so a plain *.parquet read that part twice.
    return str(WAREHOUSE_DIRS[rir] / "tables" / f"table={table}" / "**" / "*part-*.parquet")


def _presence_src(glob: str) -> str:
    """Aliased read of object_presence_stats so callers can keep using the
    legacy column names first_seen_ts / last_seen_ts.

    object_presence_stats (built daily by build_presence_runs.py) is the live,
    up-to-date source of per-object lifecycle timestamps. It replaces the
    object_first_last rollup, which the 20-minute ETL cycle skips
    (--skip-first-last) and is therefore stale.
    """
    return (
        "(SELECT object_id, first_ts AS first_seen_ts, last_ts AS last_seen_ts "
        f"FROM read_parquet('{glob}'))"
    )


def _has_table(rir: str, table: str) -> bool:
    return (WAREHOUSE_DIRS[rir] / "tables" / f"table={table}").is_dir()


_CURRENT_INVALID_COLS = (
    "rir VARCHAR, object_id INTEGER, reason_code VARCHAR, tal_id VARCHAR, "
    "object_type VARCHAR, detail VARCHAR, object_hash VARCHAR, "
    "first_seen VARCHAR, last_seen VARCHAR"
)


def _current_invalid_enrich_sql(rir: str, max_ts: str) -> str:
    """Per-RIR SELECT resolving each rejected object_id to its type and a
    human-readable label, computed ONCE at load time so the /validity/objects
    endpoint is a trivial in-memory filter rather than a 5×5 head-table scan.

    object_validity is deduped (SELECT DISTINCT) because the source partition can
    carry duplicate (dataset_ts, object_id) rows. Each head table is read through
    an ``object_id IN (<reject set>)`` filter so DuckDB prunes to the handful of
    invalid rows instead of materializing a full LEFT JOIN over millions of rows.
    object_id is unique per RIR across all object types, so exactly one head-table
    match exists per row.
    """
    ov = parquet_glob(rir, "object_validity")
    reject = f"(SELECT object_id FROM read_parquet('{ov}') WHERE dataset_ts = '{max_ts}')"
    roa = parquet_glob(rir, "roa_objects")
    cert = parquet_glob(rir, "certs")
    mft = parquet_glob(rir, "manifests")
    crl = parquet_glob(rir, "crls")
    presence = _presence_src(parquet_glob(rir, "object_presence_stats"))

    aspa_join = aspa_type = aspa_detail = aspa_hash = ""
    if _has_table(rir, "aspa_objects"):
        aspa = parquet_glob(rir, "aspa_objects")
        aspa_join = (
            f"LEFT JOIN (SELECT object_id, customer_asn, object_hash FROM read_parquet('{aspa}') "
            f"WHERE object_id IN {reject}) asp ON asp.object_id = inv.object_id"
        )
        aspa_type = "WHEN asp.object_id IS NOT NULL THEN 'ASPA'"
        aspa_detail = "'AS' || asp.customer_asn::VARCHAR,"
        aspa_hash = "asp.object_hash,"

    return f"""
        SELECT
            '{rir}' AS rir,
            inv.object_id,
            inv.reason_code,
            inv.tal_id,
            CASE
                WHEN ro.object_id IS NOT NULL THEN 'ROA'
                WHEN ce.object_id IS NOT NULL THEN 'Certificate'
                WHEN mf.object_id IS NOT NULL THEN 'Manifest'
                {aspa_type}
                WHEN cr.object_id IS NOT NULL THEN 'CRL'
                ELSE 'Unknown'
            END AS object_type,
            COALESCE(
                'AS' || ro.asn::VARCHAR,
                ce.subject_dn,
                'mft#' || mf.manifest_number::VARCHAR,
                {aspa_detail}
                cr.issuer_dn
            ) AS detail,
            COALESCE(ro.object_hash, {aspa_hash} ce.cert_hash) AS object_hash,
            ofl.first_seen_ts::VARCHAR AS first_seen,
            ofl.last_seen_ts::VARCHAR AS last_seen
        FROM (
            SELECT DISTINCT object_id, reason_code, tal_id
            FROM read_parquet('{ov}') WHERE dataset_ts = '{max_ts}'
        ) inv
        LEFT JOIN (SELECT object_id, asn, object_hash FROM read_parquet('{roa}') WHERE object_id IN {reject}) ro ON ro.object_id = inv.object_id
        LEFT JOIN (SELECT object_id, subject_dn, cert_hash FROM read_parquet('{cert}') WHERE object_id IN {reject}) ce ON ce.object_id = inv.object_id
        LEFT JOIN (SELECT object_id, manifest_number FROM read_parquet('{mft}') WHERE object_id IN {reject}) mf ON mf.object_id = inv.object_id
        {aspa_join}
        LEFT JOIN (SELECT object_id, issuer_dn FROM read_parquet('{crl}') WHERE object_id IN {reject}) cr ON cr.object_id = inv.object_id
        LEFT JOIN {presence} ofl ON ofl.object_id = inv.object_id
    """


def _build_current_invalid_table(con: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, str]:
    """(Re)build the small in-memory set of objects rpki-client rejected in the
    LATEST validation cycle, enriched with object type / label / lifecycle.

    object_validity is time-varying (one row per present-but-rejected object per
    cycle); the live/current view only needs the most recent cycle's verdicts, so
    we take MAX(dataset_ts) per RIR. The result is tiny (hundreds of rows) and
    fully resolved, so both the live_roas is_valid join and the /validity/objects
    explorer read it directly. Graceful when the partition does not exist yet (the
    table is written from the first cron cycle onward): the result is an empty
    table, so every object reads as valid (the prior behavior). Returns
    {rir: MAX(dataset_ts)}; the caller publishes it once the table is live.
    """
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    validation_ts: dict[str, str] = {}
    parts = []
    for rir in RIRS:
        if not _has_table(rir, "object_validity"):
            continue
        glob = parquet_glob(rir, "object_validity")
        max_ts = con.execute(
            f"SELECT MAX(dataset_ts) FROM read_parquet('{glob}')"
        ).fetchone()[0]
        if max_ts is None:
            continue
        validation_ts[rir] = str(max_ts)
        parts.append(_current_invalid_enrich_sql(rir, str(max_ts)))

    if not parts:
        con.execute(f"CREATE TABLE {table_name} ({_CURRENT_INVALID_COLS})")
        log.info("%s: no object_validity partitions yet (all objects read as valid)", table_name)
        return validation_ts

    con.execute(f"CREATE TABLE {table_name} AS {' UNION ALL '.join(parts)}")
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    log.info("Built %s: %s invalid object(s) in latest cycle", table_name, f"{count:,}")
    return validation_ts


def _collapse_vrp(union_sql: str) -> str:
    """Collapse object×prefix rows to one row per live VRP.

    A single VRP (rir, asn, prefix, max_length, afi) re-issued as several ROA
    objects — e.g. rapid cert rollover — otherwise appears once per object while
    all of them are still inside the live window. Keep the most recently signed
    object's hash, the VRP's full first/last span, and mark it valid if any live
    object is RP-valid.
    """
    return f"""
        SELECT rir, asn, prefix, max_length, afi,
               arg_max(object_hash, signing_time) AS object_hash,
               max(signing_time)  AS signing_time,
               min(first_seen_ts) AS first_seen_ts,
               max(last_seen_ts)  AS last_seen_ts,
               bool_or(is_valid)  AS is_valid
        FROM ({union_sql})
        GROUP BY rir, asn, prefix, max_length, afi
    """


def _materialize_live_roas(
    con: duckdb.DuckDBPyConnection,
    table: str = "live_roas",
    invalid_table: str = "current_invalid",
) -> None:
    """Build an in-memory table of currently live ROA prefixes across all RIRs.

    Each row carries ``is_valid`` (rpki-client did not reject it in the latest
    cycle). The default user-facing views filter on this; ``validity=all`` keeps
    the full repo-present set queryable from the same table.
    """
    con.execute(f"DROP TABLE IF EXISTS {table}")

    parts = []
    for rir in RIRS:
        ts = latest_snapshot_ts.get(rir)
        if not ts:
            continue
        date_cutoff = live_cutoff(rir)
        roa_obj = parquet_glob(rir, "roa_objects")
        roa_pfx = parquet_glob(rir, "roa_prefixes")
        obj_fl = parquet_glob(rir, "object_presence_stats")

        parts.append(f"""
            SELECT
                '{rir}' AS rir,
                ro.object_id,
                ro.asn,
                rp.prefix,
                rp.max_length,
                rp.afi,
                ro.object_hash,
                ro.signing_time,
                ofl.first_seen_ts,
                ofl.last_seen_ts,
                (civ.object_id IS NULL) AS is_valid
            FROM read_parquet('{roa_obj}') ro
            JOIN read_parquet('{roa_pfx}') rp ON ro.object_id = rp.object_id
            JOIN {_presence_src(obj_fl)} ofl ON ro.object_id = ofl.object_id
            LEFT JOIN {invalid_table} civ
                ON civ.rir = '{rir}' AND civ.object_id = ro.object_id
            WHERE ofl.last_seen_ts >= '{date_cutoff}'
        """)

    if not parts:
        con.execute(f"CREATE TABLE {table} AS SELECT 1 WHERE false")
        return

    union_sql = " UNION ALL ".join(parts)
    log.info("Materializing %s from %d RIRs...", table, len(parts))
    t0 = time.time()
    con.execute(f"CREATE TABLE {table} AS {_collapse_vrp(union_sql)}")
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    invalid = con.execute(f"SELECT COUNT(*) FROM {table} WHERE NOT is_valid").fetchone()[0]
    elapsed = time.time() - t0
    log.info(
        "Materialized %s live ROA prefix rows (%s invalid) in %.1fs",
        f"{count:,}", f"{invalid:,}", elapsed,
    )


def _materialize_roa_history(con: duckdb.DuckDBPyConnection, table: str = "roa_history") -> None:
    """Build an in-memory table of ALL ROA history (roa_objects + roa_prefixes + object_presence_stats).
    This makes /roa/history, /analytics/churn, /roa/coverage-trend instant instead of Parquet scans."""
    con.execute(f"DROP TABLE IF EXISTS {table}")

    parts = []
    for rir in RIRS:
        ts = latest_snapshot_ts.get(rir)
        if not ts:
            continue
        roa_obj = parquet_glob(rir, "roa_objects")
        roa_pfx = parquet_glob(rir, "roa_prefixes")
        obj_fl = parquet_glob(rir, "object_presence_stats")

        parts.append(f"""
            SELECT
                '{rir}' AS rir,
                ro.object_id,
                ro.asn,
                rp.prefix,
                rp.max_length,
                rp.afi,
                ro.object_hash,
                ro.semantic_hash,
                ro.signing_time,
                ofl.first_seen_ts,
                ofl.last_seen_ts
            FROM read_parquet('{roa_obj}') ro
            JOIN read_parquet('{roa_pfx}') rp ON ro.object_id = rp.object_id
            JOIN {_presence_src(obj_fl)} ofl ON ro.object_id = ofl.object_id
        """)

    if not parts:
        con.execute(f"CREATE TABLE {table} AS SELECT 1 WHERE false")
        return

    union_sql = " UNION ALL ".join(parts)
    log.info("Materializing %s from %d RIRs...", table, len(parts))
    t0 = time.time()
    con.execute(f"CREATE TABLE {table} AS {union_sql}")
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    elapsed = time.time() - t0
    log.info("Materialized %s ROA history rows in %.1fs", f"{count:,}", elapsed)


def _materialize_ca_certs(con: duckdb.DuckDBPyConnection, table: str = "ca_certs") -> None:
    """In-memory table of CA certificates (is_ca_flag = true) across all RIRs,
    joined to live presence timestamps.

    The CA-explorer endpoints derive the hierarchy live from aki->ski links.
    Scanning the full certs parquet (millions of EE certs) on every request is
    slow, so we materialize the CA certs (~700k historical instances) once per
    load. One row per (rir, cert_id), keeping the most recently observed
    instance. No ART indexes: DuckDB cannot rename a table that has one, and
    hot_reload swaps tables by rename; filtered scans over this size take
    milliseconds.
    """
    con.execute(f"DROP TABLE IF EXISTS {table}")

    parts = []
    for rir in RIRS:
        if not latest_snapshot_ts.get(rir):
            continue
        certs_pq = parquet_glob(rir, "certs")
        obj_fl = parquet_glob(rir, "object_presence_stats")
        parts.append(f"""
            SELECT
                '{rir}' AS rir,
                c.cert_id,
                c.object_id,
                c.subject_dn,
                c.issuer_dn,
                c.ski,
                c.aki,
                c.not_before::VARCHAR AS not_before,
                c.not_after::VARCHAR AS not_after,
                c.resources_ip,
                c.resources_asn,
                ofl.first_seen_ts,
                ofl.last_seen_ts
            FROM read_parquet('{certs_pq}') c
            JOIN {_presence_src(obj_fl)} ofl ON c.object_id = ofl.object_id
            WHERE c.is_ca_flag = true
        """)

    if not parts:
        con.execute(f"CREATE TABLE {table} AS SELECT 1 WHERE false")
        return

    union_sql = " UNION ALL ".join(parts)
    log.info("Materializing %s from %d RIRs...", table, len(parts))
    t0 = time.time()
    con.execute(f"""
        CREATE TABLE {table} AS
        SELECT * FROM ({union_sql})
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY rir, cert_id ORDER BY last_seen_ts DESC
        ) = 1
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    elapsed = time.time() - t0
    log.info("Materialized %s CA cert rows in %.1fs", f"{count:,}", elapsed)


def _materialize_resource_ca(
    con: duckdb.DuckDBPyConnection,
    table: str = "ca_resource_map",
    ca_table: str = "ca_certs",
    invalid_table: str = "current_invalid",
) -> None:
    """Map each live ROA prefix to the CA that issued it, for the resource->CA
    reverse lookup ("which CA covers my ASN?").

    A ROA is signed by a one-time EE cert whose issuer (aki) is the member CA's
    ski, so we resolve ROA -> EE signer -> (ee.aki = ca.ski) -> issuing CA.
    Re-issued CAs share a ski, so we collapse to the most recently observed CA
    instance per ski. One row per (rir, asn, prefix, issuing CA). Depends on
    ca_table and invalid_table, so it must run after both are built.
    """
    con.execute(f"DROP TABLE IF EXISTS {table}")

    parts = []
    for rir in RIRS:
        if not latest_snapshot_ts.get(rir):
            continue
        roa_obj = parquet_glob(rir, "roa_objects")
        roa_pfx = parquet_glob(rir, "roa_prefixes")
        sig = parquet_glob(rir, "object_signatures")
        certs_pq = parquet_glob(rir, "certs")
        obj_fl = parquet_glob(rir, "object_presence_stats")
        cutoff = live_cutoff(rir)
        parts.append(f"""
            SELECT
                '{rir}' AS rir,
                ro.asn,
                rp.prefix,
                rp.max_length,
                rp.afi,
                ofl.last_seen_ts,
                (civ.object_id IS NULL) AS is_valid,
                ca.cert_id AS ca_cert_id,
                ca.subject_dn AS ca_subject_dn,
                ca.ski AS ca_ski,
                ca.not_after AS ca_not_after
            FROM read_parquet('{roa_obj}') ro
            JOIN read_parquet('{roa_pfx}') rp ON ro.object_id = rp.object_id
            JOIN {_presence_src(obj_fl)} ofl ON ro.object_id = ofl.object_id
            JOIN read_parquet('{sig}') s
                ON ro.object_id = s.object_id AND s.object_type = 'ROA'
            JOIN read_parquet('{certs_pq}') ee ON s.signer_cert_id = ee.cert_id
            JOIN ca_latest ca ON ca.rir = '{rir}' AND ca.ski = ee.aki
            LEFT JOIN {invalid_table} civ
                ON civ.rir = '{rir}' AND civ.object_id = ro.object_id
            WHERE ofl.last_seen_ts >= '{cutoff}'
        """)

    if not parts:
        con.execute(f"CREATE TABLE {table} AS SELECT 1 WHERE false")
        return

    union_sql = " UNION ALL ".join(parts)
    log.info("Materializing %s from %d RIRs...", table, len(parts))
    t0 = time.time()
    con.execute(f"""
        CREATE TABLE {table} AS
        WITH ca_latest AS (
            SELECT * FROM {ca_table}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY rir, ski ORDER BY last_seen_ts DESC
            ) = 1
        )
        {union_sql}
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    elapsed = time.time() - t0
    log.info("Materialized %s resource->CA rows in %.1fs", f"{count:,}", elapsed)


OBJECT_TABLES = [
    ("roa_objects", "ROA"),
    ("certs", "Certificate"),
    ("crls", "CRL"),
    ("manifests", "Manifest"),
    ("aspa_objects", "ASPA"),
]


def _cache_object_summaries() -> None:
    """Pre-compute object counts per RIR at startup so /objects/summary is instant."""
    con = get_con()
    log.info("Caching object summaries for %d RIRs...", len(RIRS))
    t0 = time.time()
    for rir in RIRS:
        date_cutoff = live_cutoff(rir)
        counts = []
        for table_name, label in OBJECT_TABLES:
            table_dir = WAREHOUSE_DIRS[rir] / "tables" / f"table={table_name}"
            if not table_dir.exists():
                continue
            obj_pq = parquet_glob(rir, table_name)
            obj_fl = parquet_glob(rir, "object_presence_stats")
            sql = f"""
                SELECT COUNT(DISTINCT o.object_id)
                FROM read_parquet('{obj_pq}') o
                JOIN {_presence_src(obj_fl)} ofl ON o.object_id = ofl.object_id
                WHERE ofl.last_seen_ts >= '{date_cutoff}'
            """
            row = con.execute(sql).fetchone()
            counts.append({"object_type": label, "count": row[0]})
        object_summary_cache[rir] = counts
    elapsed = time.time() - t0
    log.info("Cached object summaries in %.1fs", elapsed)


# ── Response cache ──
# TTL-based cache for expensive API responses. Data changes daily,
# so 1-hour TTL is safe and avoids redundant computation.

_response_cache: dict[str, tuple[float, object]] = {}
_cache_lock = threading.Lock()
CACHE_TTL = 3600  # 1 hour
CACHE_MAX_SIZE = 256


def cache_get(key: str):
    """Return cached value if still valid, else None."""
    with _cache_lock:
        entry = _response_cache.get(key)
        if entry and time.time() - entry[0] < CACHE_TTL:
            return entry[1]
        elif entry:
            del _response_cache[key]  # expired
    return None


def cache_set(key: str, value: object):
    """Store a value in the response cache with size eviction."""
    with _cache_lock:
        if len(_response_cache) >= CACHE_MAX_SIZE:
            oldest = min(_response_cache, key=lambda k: _response_cache[k][0])
            del _response_cache[oldest]
        _response_cache[key] = (time.time(), value)


# ── Hot reload ──
# Every table _build_tables() materializes. hot_reload rebuilds all of them;
# before 2026-10 it skipped the CA tables, which then stayed frozen at the
# last process start (CA Explorer and the other ca_certs readers).
_RELOAD_TABLES = ("current_invalid", "live_roas", "roa_history", "ca_certs", "ca_resource_map")

_reload_lock = threading.Lock()


def reload_running() -> bool:
    return _reload_lock.locked()


def hot_reload() -> dict:
    """Rebuild all materialized tables without downtime.

    Every table is built under a ``_new`` name while the old ones keep serving,
    then all of them are swapped in one transaction, so a request never sees a
    fresh live_roas next to a stale CA map.
    """
    if not _reload_lock.acquire(blocking=False):
        return {"status": "already_running"}

    con = None
    try:
        con = get_con()
        t0 = time.time()
        log.info("Hot reload started — rebuilding tables...")

        with _presence_lock_shared(required=True):
            # 1) Re-discover warehouses (picks up new snapshots), build *_new
            _discover_warehouses()
            validation_ts = _build_tables(con, suffix="_new")
            built = {r[0] for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE '%\\_new' ESCAPE '\\'"
            ).fetchall()}
            if built != {f"{t}_new" for t in _RELOAD_TABLES}:
                raise RuntimeError(f"_build_tables and _RELOAD_TABLES disagree: built {sorted(built)}")
            counts = {
                t: con.execute(f"SELECT COUNT(*) FROM {t}_new").fetchone()[0]
                for t in _RELOAD_TABLES
            }

            # 2) Atomic swap: concurrent readers see either all old or all new tables
            con.execute("BEGIN TRANSACTION")
            try:
                for t in _RELOAD_TABLES:
                    con.execute(f"DROP TABLE IF EXISTS {t}")
                    con.execute(f"ALTER TABLE {t}_new RENAME TO {t}")
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            _set_latest_validation_ts(validation_ts)
            with _cache_lock:
                _response_cache.clear()

            # 3) Refresh object summary cache. The tables are already swapped, so a
            #    failure here (e.g. a part file the 20-min cycle is still writing)
            #    only keeps the previous counts.
            try:
                _cache_object_summaries()
            except Exception as e:
                log.warning("Hot reload: object summaries not refreshed: %s", e)

        elapsed = time.time() - t0
        log.info(
            "Hot reload complete in %.1fs — %s", elapsed,
            ", ".join(f"{t}={n:,}" for t, n in counts.items()),
        )
        return {"status": "ok", "elapsed_seconds": round(elapsed, 1), **counts}
    except Exception as e:
        log.error("Hot reload failed: %s", e, exc_info=True)
        # Free the half-built tables now instead of holding them until the next run
        for t in _RELOAD_TABLES if con is not None else ():
            try:
                con.execute(f"DROP TABLE IF EXISTS {t}_new")
            except Exception:
                pass
        return {"status": "error", "detail": str(e)}
    finally:
        _reload_lock.release()
