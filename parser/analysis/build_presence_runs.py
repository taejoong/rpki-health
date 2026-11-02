#!/usr/bin/env python3
"""
Build flicker-aware presence runs from snapshot membership bitmaps.

Inputs:
  warehouse/indexes/membership/ts=YYYY-MM-DD[ HH-MM-SS]/objects.roar   (roaring or JSON fallback)

Outputs under warehouse/tables/:
  table=object_presence_runs/part-*.parquet
    - object_id : int32
    - start_sid : int32    (inclusive)
    - end_sid   : int32    (inclusive)
    - start_ts  : string   ("YYYY-MM-DD HH:MM:SS")
    - end_ts    : string

  table=object_presence_stats/part-*.parquet
    - object_id   : int32
    - first_sid   : int32
    - last_sid    : int32
    - first_ts    : string
    - last_ts     : string
    - runs_count  : int32
    - total_days  : int32   (sum over all runs: end_sid - start_sid + 1)

Algorithm:
  Process snapshots in time order.
  Let prev = membership[sid-1], curr = membership[sid].
  entering = curr - prev  -> start a new run at sid for each object in 'entering'.
  leaving  = prev - curr  -> close the run for each object in 'leaving' at sid-1.

At the end, close any still-open runs at the final sid.

Requirements:
  - Python 3.11
  - pyarrow, duckdb (for nothing here), tqdm
  - pyroaring (optional; recommended). If missing, JSON membership files are still supported.

Usage:
  python analysis/build_presence_runs.py --warehouse warehouse --batch-rows 500000
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Iterable, Optional

from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq

# Optional fast path
try:
    from pyroaring import BitMap  # pip install pyroaring
    HAVE_ROARING = True
except Exception:
    BitMap = None
    HAVE_ROARING = False

_TS_DIR_RE = re.compile(r"^ts=(\d{4}-\d{2}-\d{2})(?:[ _](\d{2})[-:]?(\d{2})[-:]?(\d{2}))?$")

def normalize_ts(dirname: str) -> Optional[str]:
    """
    Accepts:
      ts=YYYY-MM-DD
      ts=YYYY-MM-DD_HH-MM-SS
      ts=YYYY-MM-DD HH-MM-SS
      ts=YYYY-MM-DD HH:MM:SS
    Returns 'YYYY-MM-DD HH:MM:SS' or None if not a snapshot dir.
    """
    s = dirname.replace("_", " ")
    m = _TS_DIR_RE.match(s)
    if not m:
        return None
    date = m.group(1)
    hh = m.group(2) or "00"
    mm = m.group(3) or "00"
    ss = m.group(4) or "00"
    return f"{date} {hh}:{mm}:{ss}"

def list_snapshots(membership_root: Path) -> List[Tuple[int, str, Path]]:
    """
    Return a sorted list of (sid, ts_str, dir_path) for membership snapshots.
    """
    entries = []
    for child in membership_root.iterdir():
        if not child.is_dir():
            continue
        ts = normalize_ts(child.name)
        if ts is None:
            continue
        entries.append((ts, child))
    # sort by timestamp string lexicographically (safe for YYYY-MM-DD HH:MM:SS)
    entries.sort(key=lambda x: x[0])
    out = []
    for i, (ts, p) in enumerate(entries):
        out.append((i, ts, p))
    return out

def load_membership_bitmap(dir_path: Path) -> Iterable[int]:
    """
    Load object_ids present in this snapshot.
    Supports binary roaring via pyroaring and JSON fallback.
    Yields ints (object_id).
    """
    f = dir_path / "objects.roar"
    if not f.exists():
        return []
    head = f.read_bytes()[:1]
    # JSON fallback
    if head in (b"{", b"["):
        payload = json.loads(f.read_text())
        data = payload.get("data") or []
        return data  # already a Python list of ints
    # Binary roaring
    if not HAVE_ROARING:
        raise RuntimeError(f"binary roaring file but pyroaring not installed: {f}")
    bm = BitMap.deserialize(f.read_bytes())
    return bm  # iterable of ints

class RunsWriter:
    def __init__(self, out_dir: Path, batch_rows: int = 500_000) -> None:
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.schema = pa.schema([
            pa.field("object_id", pa.int32()),
            pa.field("start_sid", pa.int32()),
            pa.field("end_sid", pa.int32()),
            pa.field("start_ts", pa.string()),
            pa.field("end_ts", pa.string()),
        ])
        self.buf: List[Tuple[int,int,int,str,str]] = []
        self.batch_rows = batch_rows
        self.part = 0

    def append(self, object_id: int, start_sid: int, end_sid: int, start_ts: str, end_ts: str) -> None:
        self.buf.append((object_id, start_sid, end_sid, start_ts, end_ts))
        if len(self.buf) >= self.batch_rows:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        tbl = pa.table({
            "object_id":  pa.array([r[0] for r in self.buf], type=pa.int32()),
            "start_sid":  pa.array([r[1] for r in self.buf], type=pa.int32()),
            "end_sid":    pa.array([r[2] for r in self.buf], type=pa.int32()),
            "start_ts":   pa.array([r[3] for r in self.buf], type=pa.string()),
            "end_ts":     pa.array([r[4] for r in self.buf], type=pa.string()),
        }, schema=self.schema)
        out_path = self.out_dir / f"part-{self.part:05d}.parquet"
        pq.write_table(tbl, out_path, compression="zstd")
        self.part += 1
        self.buf.clear()

    def close(self) -> None:
        self.flush()

class StatsWriter:
    def __init__(self, out_dir: Path, batch_rows: int = 500_000) -> None:
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.schema = pa.schema([
            pa.field("object_id",  pa.int32()),
            pa.field("first_sid",  pa.int32()),
            pa.field("last_sid",   pa.int32()),
            pa.field("first_ts",   pa.string()),
            pa.field("last_ts",    pa.string()),
            pa.field("runs_count", pa.int32()),
            pa.field("total_days", pa.int32()),
        ])
        self.buf: List[Tuple[int,int,int,str,str,int,int]] = []
        self.batch_rows = batch_rows
        self.part = 0

    def append(self, object_id: int, first_sid: int, last_sid: int, first_ts: str, last_ts: str, runs_count: int, total_days: int) -> None:
        self.buf.append((object_id, first_sid, last_sid, first_ts, last_ts, runs_count, total_days))
        if len(self.buf) >= self.batch_rows:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        tbl = pa.table({
            "object_id":  pa.array([r[0] for r in self.buf], type=pa.int32()),
            "first_sid":  pa.array([r[1] for r in self.buf], type=pa.int32()),
            "last_sid":   pa.array([r[2] for r in self.buf], type=pa.int32()),
            "first_ts":   pa.array([r[3] for r in self.buf], type=pa.string()),
            "last_ts":    pa.array([r[4] for r in self.buf], type=pa.string()),
            "runs_count": pa.array([r[5] for r in self.buf], type=pa.int32()),
            "total_days": pa.array([r[6] for r in self.buf], type=pa.int32()),
        }, schema=self.schema)
        out_path = self.out_dir / f"part-{self.part:05d}.parquet"
        pq.write_table(tbl, out_path, compression="zstd")
        self.part += 1
        self.buf.clear()

    def close(self) -> None:
        self.flush()

def build_runs(membership_root: Path, tables_root: Path, batch_rows: int = 500_000) -> None:
    snaps = list_snapshots(membership_root)
    if not snaps:
        print(f"no snapshots under {membership_root}", file=sys.stderr)
        return

    # Snapshot index for label lookups
    sid_to_ts = {sid: ts for sid, ts, _ in snaps}

    runs_writer  = RunsWriter(tables_root / "table=object_presence_runs", batch_rows=batch_rows)
    stats_writer = StatsWriter(tables_root / "table=object_presence_stats", batch_rows=batch_rows)

    # State: start_sid per object currently present
    # And stats: first_sid, last_sid, runs_count, total_days
    start_sid: Dict[int, int] = {}
    first_sid: Dict[int, int] = {}
    last_sid:  Dict[int, int] = {}
    runs_cnt:  Dict[int, int] = {}
    total_days: Dict[int, int] = {}

    prev: Optional[Iterable[int]] = None
    prev_set = BitMap() if HAVE_ROARING else set()

    for sid, ts, sdir in tqdm(snaps, desc="Presence runs"):
        present_iter = load_membership_bitmap(sdir)
        if HAVE_ROARING:
            # JSON-fallback snapshots (written while pyroaring was missing) can sit
            # next to binary ones; normalize so set ops never mix set and BitMap.
            curr = present_iter if isinstance(present_iter, BitMap) else BitMap(present_iter)
        else:
            # present_iter is list[int]
            curr = set(int(x) for x in present_iter)

        if prev is None:
            # first snapshot: everyone is entering
            entering = curr
            leaving = BitMap() if HAVE_ROARING else set()
        else:
            if HAVE_ROARING:
                entering = curr - prev_set
                leaving  = prev_set - curr
            else:
                entering = curr.difference(prev_set)
                leaving  = prev_set.difference(curr)

        # handle entering: start run at sid
        if HAVE_ROARING and isinstance(entering, BitMap):
            it = entering
        else:
            it = entering
        for oid in it:
            start_sid[oid] = sid
            if oid not in first_sid:
                first_sid[oid] = sid
                runs_cnt[oid]  = 0
                total_days[oid]= 0

        # handle leaving: close run at sid-1
        end_sid = sid - 1
        end_ts = sid_to_ts[end_sid] if end_sid in sid_to_ts else ts
        if HAVE_ROARING and isinstance(leaving, BitMap):
            it2 = leaving
        else:
            it2 = leaving
        for oid in it2:
            s = start_sid.pop(oid, None)
            if s is None:
                continue  # should not happen; guard
            runs_writer.append(oid, s, end_sid, sid_to_ts[s], end_ts)
            # stats
            runs_cnt[oid] = runs_cnt.get(oid, 0) + 1
            days = end_sid - s + 1
            total_days[oid] = total_days.get(oid, 0) + (days if days > 0 else 0)
            last_sid[oid] = end_sid

        # advance
        prev_set = curr
        prev = curr

    # close any still-open runs at the final sid
    final_sid = snaps[-1][0]
    final_ts  = snaps[-1][1]
    for oid, s in start_sid.items():
        runs_writer.append(oid, s, final_sid, sid_to_ts[s], final_ts)
        runs_cnt[oid] = runs_cnt.get(oid, 0) + 1
        days = final_sid - s + 1
        total_days[oid] = total_days.get(oid, 0) + (days if days > 0 else 0)
        last_sid[oid] = final_sid

    runs_writer.close()

    # write stats
    # Note: some objects may have only last_sid without explicit last update if they never left; handled above
    for oid, fsid in first_sid.items():
        lsid = last_sid.get(oid, final_sid)
        stats_writer.append(
            oid,
            fsid,
            lsid,
            sid_to_ts[fsid],
            sid_to_ts.get(lsid, final_ts),
            runs_cnt.get(oid, 0),
            total_days.get(oid, 0),
        )
    stats_writer.close()

def main():
    ap = argparse.ArgumentParser(description="Build presence runs from roaring membership bitmaps")
    ap.add_argument("--warehouse", type=Path, required=True, help="warehouse root")
    ap.add_argument("--batch-rows", type=int, default=500_000, help="rows per Parquet part flush")
    args = ap.parse_args()

    membership_root = args.warehouse / "indexes" / "membership"
    tables_root     = args.warehouse / "tables"
    build_runs(membership_root, tables_root, batch_rows=args.batch_rows)

if __name__ == "__main__":
    main()

