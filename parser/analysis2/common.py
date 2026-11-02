# ASCII-only punctuation.
from __future__ import annotations
import os, re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timedelta

import duckdb
import pandas as pd

try:
    from pyroaring import BitMap  # optional
except Exception:
    BitMap = None  # type: ignore


def connect_duckdb(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    if threads is None:
        threads = max(1, (os.cpu_count() or 8) - 1)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    return con


def list_latest_membership(warehouse: Path,
                           start: Optional[str] = None,
                           end: Optional[str] = None) -> List[Tuple[str, Path]]:
    """
    Return list of (YYYY-MM-DD, path/to/objects.roar) selecting the last snapshot per day.
    """
    mem_root = warehouse / "indexes" / "membership"
    if not mem_root.exists():
        return []
    per_day: Dict[str, Tuple[str, Path]] = {}
    for d in mem_root.iterdir():
        if not d.is_dir():
            continue
        name = d.name  # ts=YYYY-MM-DD HH-MM-SS
        if not name.startswith("ts="):
            continue
        ts_label = name[3:]
        day = ts_label[:10]
        roar = d / "objects.roar"
        if not roar.exists():
            continue
        cur = per_day.get(day)
        if cur is None or name > cur[0]:
            per_day[day] = (name, roar)
    days = sorted(per_day.keys())
    if start:
        days = [x for x in days if x >= start]
    if end:
        days = [x for x in days if x <= end]
    return [(d, per_day[d][1]) for d in days]


def load_bitmap(path: Path) -> Optional[Iterable[int]]:
    """
    Load a pyroaring bitmap file and return an iterable of ints.
    Returns None if pyroaring is unavailable.
    """
    if BitMap is None:
        return None
    data = path.read_bytes()
    # Try several constructors to be robust
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
    return BitMap.frombytes(data)


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def extract_hosts_from_sia(sia: Optional[str]) -> List[str]:
    """
    Extract hostnames from SIA string. Supports rsync://host/... and http(s)://host/...
    Returns unique hosts in order.
    """
    if not sia:
        return []
    hosts: List[str] = []
    for m in re.finditer(r"(?:rsync|https?)://([^/]+)/", sia):
        hosts.append(m.group(1).lower())
    # dedup preserve order
    seen = set()
    out = []
    for h in hosts:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def registrable_domain(host: str) -> str:
    """
    Cheap registrable domain heuristic: last two labels. Good enough for RIR domains.
    """
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:]).lower()
    return host.lower()
