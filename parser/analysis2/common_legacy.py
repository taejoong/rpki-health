# ASCII-only punctuation.
"""
Common helpers for repository-only longitudinal RPKI analyses.

What this module provides
- DuckDB connection with sane settings
- Membership snapshot discovery and bitmap loader
- Daily iterator that picks the last snapshot per day
- Utilities to parse prefix len and compute date ranges
- Fallback from bitmaps to object_presence_runs when pyroaring is missing
"""

from __future__ import annotations
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import duckdb
import pandas as pd

try:
    from pyroaring import BitMap  # optional
except Exception:  # pragma: no cover - optional dependency
    BitMap = None  # type: ignore


def connect_duckdb(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with a bounded thread count."""

    if threads is None:
        cpu = os.cpu_count() or 8
        threads = max(1, cpu - 1)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    return con


def list_latest_membership(
    warehouse: Path, start: str | None = None, end: str | None = None
) -> List[Tuple[str, Path]]:
    """Return the most recent membership bitmap for each day."""

    mem_root = warehouse / "indexes" / "membership"
    if not mem_root.exists():
        return []

    per_day: Dict[str, Tuple[str, Path]] = {}
    for entry in mem_root.iterdir():
        if not entry.is_dir():
            continue
        name = entry.name  # ts=YYYY-MM-DD HH-MM-SS
        if not name.startswith("ts="):
            continue
        ts_label = name[3:]
        day = ts_label[:10]
        roar = entry / "objects.roar"
        if not roar.exists():
            continue
        current = per_day.get(day)
        if current is None or name > current[0]:
            per_day[day] = (name, roar)

    days = sorted(per_day.keys())
    if start:
        days = [d for d in days if d >= start]
    if end:
        days = [d for d in days if d <= end]
    return [(d, per_day[d][1]) for d in days]


def load_bitmap(path: Path) -> Optional[Iterable[int]]:
    """Load a Roaring bitmap file if pyroaring is available."""

    if BitMap is None:
        return None
    data = path.read_bytes()
    # Try a few constructor variants that pyroaring exposes
    try:
        return BitMap.deserialize(data)
    except Exception:
        pass
    try:
        bm = BitMap()
        bm.deserialize(data)
        return bm
    except Exception:
        pass
    try:
        return BitMap.frombytes(data)
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"cannot load bitmap {path}: {exc}") from exc


def day_range_from_presence(
    con: duckdb.DuckDBPyConnection, warehouse: Path
) -> Tuple[str, str]:
    """Return the min and max day from object_presence_runs."""

    pr_glob = str(
        warehouse / "tables" / "table=object_presence_runs" / "**" / "*.parquet"
    )
    df = con.execute(
        f"""
        SELECT MIN(CAST(start_ts AS TIMESTAMP)) AS s,
               MAX(CAST(end_ts   AS TIMESTAMP)) AS e
        FROM read_parquet('{pr_glob}')
        """
    ).fetch_df()
    if df.empty or pd.isna(df.loc[0, "s"]) or pd.isna(df.loc[0, "e"]):
        raise SystemExit("object_presence_runs is empty or missing")
    start = str(df.loc[0, "s"])[:10]
    end = str(df.loc[0, "e"])[:10]
    return start, end


def iter_days(warehouse: Path, start: str | None, end: str | None) -> List[str]:
    """Return the ISO dates covered by membership or presence data."""

    latest = list_latest_membership(warehouse, start, end)
    if latest:
        return [day for day, _ in latest]

    con = connect_duckdb()
    s, e = day_range_from_presence(con, warehouse)
    if start and start > s:
        s = start
    if end and end < e:
        e = end

    cursor = datetime.fromisoformat(s).date()
    stop = datetime.fromisoformat(e).date()
    days: List[str] = []
    while cursor <= stop:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def parse_prefix_len(prefix: str) -> int:
    """Return the prefix length component or -1 if malformed."""

    if not prefix or "/" not in prefix:
        return -1
    try:
        return int(prefix.split("/", 1)[1])
    except ValueError:
        return -1


def ensure_out_dir(path: Path) -> None:
    """Ensure the output directory exists."""

    path.mkdir(parents=True, exist_ok=True)
