"""Visitor analytics: lightweight SQLite event log + admin aggregations.

Every proxied API request is recorded (IP, country, path, query) so the /admin
dashboard can show who visited and what they searched. Kept fully separate from
the read-only DuckDB warehouse layer.
"""

import hmac
import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, unquote_plus

from app.config import ADMIN_TOKEN_FILE, ANALYTICS_DB

log = logging.getLogger("app.tracking")

_lock = threading.Lock()
_con: sqlite3.Connection | None = None


def init_db() -> None:
    """Create the analytics DB + schema if missing."""
    global _con
    ANALYTICS_DB.parent.mkdir(parents=True, exist_ok=True)
    _con = sqlite3.connect(str(ANALYTICS_DB), check_same_thread=False)
    _con.execute("PRAGMA journal_mode=WAL")
    _con.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            ts      TEXT NOT NULL,
            ip      TEXT,
            country TEXT,
            ua      TEXT,
            path    TEXT,
            query   TEXT
        )
        """
    )
    _con.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)")
    _con.execute("CREATE INDEX IF NOT EXISTS idx_events_ip ON events(ip)")
    _con.commit()
    log.info("Analytics DB ready at %s", ANALYTICS_DB)


# Query parameters that carry credentials: their values are never stored.
_SECRET_PARAMS = frozenset({"token", "key", "password", "secret"})


def _redact(query: str) -> str:
    """``query`` with the values of _SECRET_PARAMS replaced by "redacted"."""
    if not query or not any(p in query.lower() for p in _SECRET_PARAMS):
        return query
    out = []
    for item in query.split("&"):
        name, sep, _ = item.partition("=")
        secret = sep and unquote_plus(name).strip().lower() in _SECRET_PARAMS
        out.append(f"{name}=redacted" if secret else item)
    return "&".join(out)


def log_event(ip: str, country: str | None, ua: str | None, path: str, query: str) -> None:
    """Record a single visit. Never raises into the request path."""
    if _con is None:
        return
    try:
        ts = datetime.now(timezone.utc).isoformat()
        with _lock:
            _con.execute(
                "INSERT INTO events (ts, ip, country, ua, path, query) VALUES (?, ?, ?, ?, ?, ?)",
                (ts, ip, country, ua, path, _redact(query)),
            )
            _con.commit()
    except Exception as e:  # pragma: no cover - logging must not break requests
        log.warning("Failed to log visit: %s", e)


# ── Admin auth ──
def admin_token() -> str | None:
    """Read the configured admin token from its file."""
    try:
        tok = ADMIN_TOKEN_FILE.read_text().strip()
        return tok or None
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("Failed to read admin token file: %s", e)
        return None


def verify_token(provided: str | None) -> bool:
    expected = admin_token()
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


# ── Aggregations for the admin dashboard ──
# Which query-string params count as a "search" of each kind.
_SEARCH_PARAMS = {
    "asn": "top_asns",
    "prefix": "top_prefixes",
    "q": "top_orgs",
}


def stats(days: int = 14) -> dict:
    """Return aggregated visitor stats for the last `days` days."""
    if _con is None:
        return {}
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    with _lock:
        # Daily unique visitors + event count
        daily = [
            {"date": r[0], "visitors": r[1], "events": r[2]}
            for r in _con.execute(
                "SELECT substr(ts,1,10) d, COUNT(DISTINCT ip), COUNT(*) "
                "FROM events WHERE ts >= ? GROUP BY d ORDER BY d",
                (since,),
            ).fetchall()
        ]

        totals_row = _con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT ip) FROM events WHERE ts >= ?",
            (since,),
        ).fetchone()

        top_visitors = [
            {"ip": r[0], "country": r[1], "events": r[2], "last_seen": r[3]}
            for r in _con.execute(
                "SELECT ip, MAX(country), COUNT(*) c, MAX(ts) "
                "FROM events WHERE ts >= ? GROUP BY ip ORDER BY c DESC LIMIT 25",
                (since,),
            ).fetchall()
        ]

        recent = [
            {"ts": r[0], "ip": r[1], "country": r[2], "path": r[3], "query": r[4]}
            for r in _con.execute(
                "SELECT ts, ip, country, path, query FROM events "
                "ORDER BY id DESC LIMIT 60"
            ).fetchall()
        ]

        # Parse search params out of stored query strings (low volume → fine in Python)
        rows = _con.execute(
            "SELECT query FROM events WHERE ts >= ?", (since,)
        ).fetchall()

    buckets: dict[str, dict[str, int]] = {v: {} for v in _SEARCH_PARAMS.values()}
    for (q,) in rows:
        if not q:
            continue
        parsed = parse_qs(q)
        for param, bucket in _SEARCH_PARAMS.items():
            for val in parsed.get(param, []):
                val = val.strip()
                if val:
                    buckets[bucket][val] = buckets[bucket].get(val, 0) + 1

    def top(bucket: str, key_name: str) -> list[dict]:
        items = sorted(buckets[bucket].items(), key=lambda kv: kv[1], reverse=True)[:20]
        return [{key_name: k, "count": c} for k, c in items]

    return {
        "days": days,
        "totals": {
            "events": totals_row[0] or 0,
            "unique_ips": totals_row[1] or 0,
        },
        "daily": daily,
        "top_asns": top("top_asns", "asn"),
        "top_prefixes": top("top_prefixes", "prefix"),
        "top_orgs": top("top_orgs", "q"),
        "top_visitors": top_visitors,
        "recent": recent,
    }
