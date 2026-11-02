#!/usr/bin/env python3
# Shared helpers for presence-run based analysis.

from __future__ import annotations
import os
import re
from pathlib import Path
from typing import List, Tuple, Optional

import duckdb
import pandas as pd

_TS_DIR_RE = re.compile(r"^ts=(\d{4}-\d{2}-\d{2})(?:[ _](\d{2})[-:]?(\d{2})[-:]?(\d{2}))?$")

def duck(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    n = threads if threads and threads > 0 else (os.cpu_count() or 8)
    con.execute(f"PRAGMA threads={n}")
    con.execute("PRAGMA enable_progress_bar=false")
    return con

def _label_from_dir(dirname: str) -> Optional[str]:
    s = dirname.replace("_", " ")
    m = _TS_DIR_RE.match(s)
    if not m:
        return None
    date = m.group(1)
    hh = m.group(2) or "00"
    mm = m.group(3) or "00"
    ss = m.group(4) or "00"
    return f"{date} {hh}:{mm}:{ss}"

def snapshot_index(membership_root: Path) -> pd.DataFrame:
    """
    Walk indexes/membership and return a DataFrame with columns:
      sid:int, ts:str, dir:str
    Ordered by time.
    """
    rows: List[Tuple[int, str, str]] = []
    pairs: List[Tuple[str, str]] = []
    for child in membership_root.iterdir():
        if not child.is_dir():
            continue
        ts = _label_from_dir(child.name)
        if not ts:
            continue
        pairs.append((ts, child.as_posix()))
    pairs.sort(key=lambda x: x[0])
    for sid, (ts, d) in enumerate(pairs):
        rows.append((sid, ts, d))
    return pd.DataFrame(rows, columns=["sid", "ts", "dir"])

def sample_sids(df_idx: pd.DataFrame, freq: str = "M",
                start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """
    Given snapshot index df with columns sid, ts, pick sample rows at a given frequency.
    freq in {"D", "W", "M"} -> daily, weekly, month-start.
    Returns DataFrame with columns sid, ts.
    """
    s = pd.to_datetime(df_idx["ts"])
    base = pd.DataFrame({"sid": df_idx["sid"].values, "ts": s})
    if start:
        base = base[base["ts"] >= pd.to_datetime(start)]
    if end:
        base = base[base["ts"] <= pd.to_datetime(end)]
    if base.empty:
        return base.assign(ts=base["ts"].dt.strftime("%Y-%m-%d %H:%M:%S"))
    # Resample by choosing the first snapshot in each period bucket so that
    # earlier months/weeks with data are preserved instead of being shifted
    # forward to the next boundary.
    base = base.set_index("ts").sort_index()
    rule = {"D": "D", "W": "W-MON", "M": "MS"}[freq]
    sampled = base.resample(rule).first().dropna(subset=["sid"])
    sampled = sampled.reset_index()
    return sampled.assign(ts=sampled["ts"].dt.strftime("%Y-%m-%d %H:%M:%S"))
