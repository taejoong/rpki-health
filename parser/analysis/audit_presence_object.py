#!/usr/bin/env python3
"""
Audit a single object's presence runs by comparing:
  - Derived runs from raw roaring membership snapshots, vs
  - Rows in tables/table=object_presence_runs and table=object_presence_stats

It supports:
  - --random to pick a random object from a random snapshot
  - --object-id <int>
  - --object-hash <hex> resolved via kv.sqlite

Outputs a clear diff of run segments and stats.

Requirements:
  - Python 3.11
  - duckdb, pyarrow
  - pyroaring (optional; binary .roar path); JSON fallback is supported

Examples:
  python analysis/audit_presence_object.py --warehouse warehouse --random
  python analysis/audit_presence_object.py --warehouse warehouse --object-id 123456
  python analysis/audit_presence_object.py --warehouse warehouse --object-hash deadbeef... --kv warehouse/kv.sqlite
  # Limit to first 200 snapshots for speed:
  python analysis/audit_presence_object.py --warehouse warehouse --random --max-snapshots 200
"""

from __future__ import annotations
import argparse
import os
import random
import sqlite3
import sys
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterable

import duckdb

# Optional fast path for binary roaring bitmaps
try:
    from pyroaring import BitMap  # pip install pyroaring
    HAVE_ROARING = True
except Exception:
    BitMap = None
    HAVE_ROARING = False

TS_DIR_RE = re.compile(r"^ts=(\d{4}-\d{2}-\d{2})(?:[ _](\d{2})[-:]?(\d{2})[-:]?(\d{2}))?$")


def log(msg: str) -> None:
    print(msg, flush=True)


def snapshot_label(dirname: str) -> Optional[str]:
    """
    Accept names like:
      ts=YYYY-MM-DD
      ts=YYYY-MM-DD_HH-MM-SS
      ts=YYYY-MM-DD HH-MM-SS
      ts=YYYY-MM-DD HH:MM:SS
    Return 'YYYY-MM-DD HH:MM:SS' or None if not a snapshot dir.
    """
    s = dirname.replace("_", " ")
    m = TS_DIR_RE.match(s)
    if not m:
        return None
    date = m.group(1)
    hh = m.group(2) or "00"
    mm = m.group(3) or "00"
    ss = m.group(4) or "00"
    return f"{date} {hh}:{mm}:{ss}"


def list_snapshots(membership_root: Path) -> List[Tuple[int, str, Path]]:
    """
    Return sorted list of (sid, ts, dir) for membership snapshots that have an 'objects.roar' file.
    """
    items: List[Tuple[str, Path]] = []
    if not membership_root.exists():
        return []
    for child in membership_root.iterdir():
        if not child.is_dir():
            continue
        ts = snapshot_label(child.name)
        if not ts:
            continue
        if not (child / "objects.roar").exists():
            continue
        items.append((ts, child))
    items.sort(key=lambda x: x[0])
    out: List[Tuple[int, str, Path]] = [(i, ts, d) for i, (ts, d) in enumerate(items)]
    return out


def load_membership_ids(dir_path: Path) -> Iterable[int]:
    """
    Load the set of object_ids for the snapshot.
    Supports binary roaring and JSON fallback.

    JSON fallback structure:
      { "data": [1,2,3,...] } or a raw list [1,2,3]
    """
    f = dir_path / "objects.roar"
    b = f.read_bytes()
    if not b:
        return []
    if b[:1] in (b"{", b"["):
        payload = json.loads(b.decode("utf-8"))
        if isinstance(payload, dict):
            data = payload.get("data", [])
        else:
            data = payload
        return [int(x) for x in data]
    if not HAVE_ROARING:
        raise RuntimeError(f"binary roaring membership found but pyroaring is not installed: {f}")
    return BitMap.deserialize(b)


def derive_runs_from_membership(object_id: int,
                                snaps: List[Tuple[int, str, Path]]) -> List[Tuple[int, int]]:
    """
    From raw membership bitmaps, compute inclusive runs (start_sid, end_sid) for object_id.
    """
    in_run = False
    run_start: Optional[int] = None
    runs: List[Tuple[int, int]] = []

    prev_present = False
    prev_sid = None

    for sid, _ts, sdir in snaps:
        present_iter = load_membership_ids(sdir)
        if HAVE_ROARING and isinstance(present_iter, BitMap):
            present = object_id in present_iter
        else:
            # For list fallback, convert to set for O(1) membership
            present = object_id in set(present_iter)

        if present and not in_run:
            in_run = True
            run_start = sid
        elif not present and in_run:
            # close run at previous sid
            end_sid = sid - 1
            runs.append((run_start, end_sid))  # type: ignore[arg-type]
            in_run = False
            run_start = None

        prev_present = present
        prev_sid = sid

    # close at final snapshot if still present
    if in_run and prev_sid is not None:
        runs.append((run_start, prev_sid))  # type: ignore[arg-type]
    return runs


def duck(threads: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute("PRAGMA enable_progress_bar=false")
    return con


def choose_random_object_id(snaps: List[Tuple[int, str, Path]]) -> int:
    """
    Pick a random snapshot, then pick a random object_id from its membership.
    This avoids scanning huge Parquet to RANDOM() LIMIT 1.
    """
    if not snaps:
        raise SystemExit("no snapshots found")
    sid, ts, d = random.choice(snaps)
    ids = load_membership_ids(d)
    # Efficient selection when roaring is available
    if HAVE_ROARING and isinstance(ids, BitMap):
        if len(ids) == 0:
            return choose_random_object_id(snaps)  # try another
        idx = random.randrange(0, len(ids))
        return int(ids.select(idx))
    # Fallback: list or set
    ids = list(ids)
    if not ids:
        return choose_random_object_id(snaps)
    return int(random.choice(ids))


def lookup_object_id_by_hash(kv_path: Path, hex_hash: str) -> Optional[int]:
    h = hex_hash.lower().strip()
    con = sqlite3.connect(str(kv_path))
    try:
        row = con.execute(
            "SELECT object_id FROM objects WHERE LOWER(object_hash) = ? LIMIT 1", (h,)
        ).fetchone()
        return int(row[0]) if row else None
    finally:
        con.close()


def read_runs_from_table(tables_root: Path, object_id: int) -> List[Tuple[int, int, str, str]]:
    """
    Return list of (start_sid, end_sid, start_ts, end_ts) for object_id from Parquet runs.
    """
    glob = str(tables_root / "table=object_presence_runs" / "**" / "*.parquet")
    con = duck(max(1, os.cpu_count() or 1))
    q = f"""
      SELECT start_sid::INT, end_sid::INT, start_ts::VARCHAR, end_ts::VARCHAR
      FROM read_parquet('{glob}')
      WHERE object_id = {object_id}
      ORDER BY start_sid, end_sid
    """
    df = con.execute(q).fetch_df()
    return [(int(a), int(b), str(c), str(d)) for a, b, c, d in df.values.tolist()]


def read_stats_from_table(tables_root: Path, object_id: int) -> Optional[Tuple[int, int, int]]:
    """
    Return (runs_count, first_sid, last_sid) from stats table if present.
    """
    glob = str(tables_root / "table=object_presence_stats" / "**" / "*.parquet")
    con = duck(max(1, os.cpu_count() or 1))
    try:
        q = f"""
          SELECT runs_count::INT, first_sid::INT, last_sid::INT
          FROM read_parquet('{glob}')
          WHERE object_id = {object_id}
          LIMIT 1
        """
        row = con.execute(q).fetchone()
        if not row:
            return None
        return int(row[0]), int(row[1]), int(row[2])
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description="Audit an object's presence runs against roaring membership")
    ap.add_argument("--warehouse", type=Path, required=True, help="warehouse root")
    ap.add_argument("--kv", type=Path, help="kv.sqlite; required if --object-hash is used")
    ap.add_argument("--object-id", type=int, default=None, help="object_id to audit")
    ap.add_argument("--object-hash", type=str, default=None, help="object_hash hex to resolve via kv.sqlite")
    ap.add_argument("--random", action="store_true", help="choose a random object from a random snapshot")
    ap.add_argument("--max-snapshots", type=int, default=None, help="limit number of earliest snapshots scanned")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 8)
    args = ap.parse_args()

    membership_root = args.warehouse / "indexes" / "membership"
    tables_root = args.warehouse / "tables"

    snaps_all = list_snapshots(membership_root)
    if not snaps_all:
        raise SystemExit(f"no snapshots under {membership_root}")
    snaps = snaps_all
    if args.max_snapshots is not None and args.max_snapshots > 0:
        snaps = snaps_all[: args.max_snapshots]

    # Choose object_id
    object_id: Optional[int] = args.object_id
    if args.object_hash:
        if not args.kv:
            raise SystemExit("--kv is required when using --object-hash")
        object_id = lookup_object_id_by_hash(args.kv, args.object_hash)
        if object_id is None:
            raise SystemExit(f"object_hash not found in kv: {args.object_hash}")
    if args.random and object_id is None:
        object_id = choose_random_object_id(snaps)
    if object_id is None:
        raise SystemExit("provide --object-id, --object-hash with --kv, or --random")

    log(f"Object to audit: object_id={object_id}")

    # Derive runs from membership
    mem_runs = derive_runs_from_membership(object_id, snaps)
    # Read runs from Parquet
    tbl_runs = read_runs_from_table(tables_root, object_id)
    tbl_runs_pairs = [(s, e) for (s, e, _st, _et) in tbl_runs]

    # Read stats
    stats = read_stats_from_table(tables_root, object_id)

    # Pretty print runs
    sid_to_ts = {sid: ts for sid, ts, _ in snaps}

    def fmt_run(pair: Tuple[int, int]) -> str:
        s, e = pair
        return f"[{s}..{e}] {sid_to_ts.get(s, '?')} -> {sid_to_ts.get(e, '?')} (len={e - s + 1})"

    log("\nDerived runs from roaring membership:")
    if not mem_runs:
        log("  (none)")
    for r in mem_runs:
        log("  " + fmt_run(r))

    log("\nRuns from table=object_presence_runs:")
    if not tbl_runs:
        log("  (none)")
    for s, e, st, et in tbl_runs:
        log(f"  [{s}..{e}] {st} -> {et} (len={e - s + 1})")

    # Compare
    mem_set = set(mem_runs)
    tbl_set = set(tbl_runs_pairs)
    only_mem = sorted(mem_set - tbl_set)
    only_tbl = sorted(tbl_set - mem_set)

    log("\nComparison:")
    log(f"  membership runs: {len(mem_runs)}")
    log(f"  table runs     : {len(tbl_runs_pairs)}")
    if not only_mem and not only_tbl:
        log("  OK: runs match exactly")
    else:
        if only_mem:
            log("  Missing in table (present in membership only):")
            for r in only_mem:
                log("    " + fmt_run(r))
        if only_tbl:
            log("  Extra in table (not present in membership):")
            for r in only_tbl:
                log("    " + fmt_run(r))

        # Try to detect off-by-one boundary drifts
        if only_mem and only_tbl:
            # If every only_mem run matches some only_tbl run with +/-1 at start or end, hint at boundary bug
            def near(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
                return abs(a[0] - b[0]) <= 1 and abs(a[1] - b[1]) <= 1
            suspicious = []
            for r in only_mem:
                if any(near(r, t) for t in only_tbl):
                    suspicious.append(r)
            if suspicious:
                log("  Note: some mismatches look like off-by-one boundary differences.")

    # Stats check
    if stats:
        runs_count, first_sid, last_sid = stats
        log("\nStats row (table=object_presence_stats):")
        log(f"  runs_count={runs_count} first_sid={first_sid} last_sid={last_sid}")
        # Compute from membership
        mem_runs_count = len(mem_runs)
        if mem_runs:
            mem_first_sid = min(r[0] for r in mem_runs)
            mem_last_sid = max(r[1] for r in mem_runs)
        else:
            mem_first_sid = None
            mem_last_sid = None
        log("Computed from membership:")
        log(f"  runs_count={mem_runs_count} first_sid={mem_first_sid} last_sid={mem_last_sid}")
        ok = (runs_count == mem_runs_count) and (first_sid == (mem_first_sid or first_sid)) and (last_sid == (mem_last_sid or last_sid))
        if ok:
            log("  OK: stats match")
        else:
            log("  MISMATCH in stats")

    # Exit code for CI use
    if mem_set != tbl_set:
        sys.exit(1)


if __name__ == "__main__":
    main()
