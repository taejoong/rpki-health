#!/usr/bin/env python3
# Multicore daily VRP coverage using membership bitmaps and interval unions.
# Families: IPv4, IPv6, or both. Outputs exact daily counts.

import argparse, os, sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb
from ipaddress import IPv4Network, IPv6Network

try:
    from pyroaring import BitMap
except Exception as e:
    print("pyroaring is required. pip install pyroaring", file=sys.stderr)
    raise

# ---------- interval helpers ----------

def parse_prefix_to_interval(s: str) -> Tuple[int, int, int]:
    s = s.strip()
    if ":" in s:
        n = IPv6Network(s, strict=False)
        return (6, int(n.network_address), int(n.broadcast_address))
    else:
        n = IPv4Network(s, strict=False)
        return (4, int(n.network_address), int(n.broadcast_address))

def merge_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not intervals:
        return []
    intervals.sort(key=lambda t: (t[0], t[1]))
    out: List[Tuple[int, int]] = []
    cs, ce = intervals[0]
    for s, e in intervals[1:]:
        if s <= ce + 1:
            if e > ce:
                ce = e
        else:
            out.append((cs, ce))
            cs, ce = s, e
    out.append((cs, ce))
    return out

def count_union(intervals: List[Tuple[int, int]]) -> int:
    if not intervals:
        return 0
    total = 0
    cs, ce = intervals[0]
    # assume intervals sorted; caller merges or sorts first
    for s, e in intervals[1:]:
        if s <= ce + 1:
            if e > ce:
                ce = e
        else:
            total += (ce - cs + 1)
            cs, ce = s, e
    total += (ce - cs + 1)
    return total

# ---------- global maps shared by workers ----------

G_V4: Dict[int, List[Tuple[int, int]]] = {}
G_V6: Dict[int, List[Tuple[int, int]]] = {}

def build_object_interval_maps(warehouse: Path) -> Tuple[Dict[int, List[Tuple[int, int]]],
                                                         Dict[int, List[Tuple[int, int]]]]:
    con = duckdb.connect()
    rp_glob = str(warehouse / "tables" / "table=roa_prefixes" / "**" / "*.parquet")
    df = con.execute(f"""
        SELECT object_id::INT AS object_id, afi::INT AS afi, prefix::VARCHAR AS prefix
        FROM read_parquet('{rp_glob}')
    """).fetch_df()
    if df.empty:
        raise SystemExit("roa_prefixes is empty")

    tmp4: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    tmp6: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    for oid, afi, pref in zip(df["object_id"], df["afi"], df["prefix"]):
        fam, a, b = parse_prefix_to_interval(pref)
        if fam == 4:
            tmp4[int(oid)].append((a, b))
        else:
            tmp6[int(oid)].append((a, b))

    # pre-merge per object to shrink daily work
    v4 = {oid: merge_intervals(lst) for oid, lst in tmp4.items()}
    v6 = {oid: merge_intervals(lst) for oid, lst in tmp6.items()}
    return v4, v6

# ---------- membership scanning ----------

def list_latest_membership_snapshots(warehouse: Path,
                                     start_date: str | None,
                                     end_date: str | None) -> List[Tuple[str, Path]]:
    """
    Returns (YYYY-MM-DD, objects.roar) for the last snapshot per day.
    """
    mem_root = warehouse / "indexes" / "membership"
    if not mem_root.exists():
        raise SystemExit(f"membership dir not found: {mem_root}")
    per_day: Dict[str, Tuple[str, Path]] = {}
    for entry in mem_root.iterdir():
        if not entry.is_dir():
            continue
        name = entry.name  # ts=YYYY-MM-DD HH-MM-SS
        if not name.startswith("ts="):
            continue
        ts_label = name[3:]  # 'YYYY-MM-DD HH-MM-SS'
        # day is the first 10 chars 'YYYY-MM-DD'
        day = ts_label[:10]
        roar = entry / "objects.roar"
        if not roar.exists():
            continue
        # keep the lexicographically last label per day (latest snapshot that day)
        cur = per_day.get(day)
        if cur is None or name > cur[0]:
            per_day[day] = (name, roar)

    days_sorted = sorted(per_day.keys())
    if start_date:
        days_sorted = [d for d in days_sorted if d >= start_date]
    if end_date:
        days_sorted = [d for d in days_sorted if d <= end_date]
    return [(d, per_day[d][1]) for d in days_sorted]

# ---------- bitmap loader ----------

def _load_bitmap(path: Path) -> BitMap:
    data = path.read_bytes()
    # prefer static constructor
    try:
        bm = BitMap.deserialize(data)
        if isinstance(bm, BitMap):
            return bm
    except Exception:
        pass
    # try in-place API
    try:
        bm2 = BitMap()
        bm2.deserialize(data)  # some builds mutate in place
        return bm2
    except Exception:
        pass
    # fallback frombytes
    try:
        return BitMap.frombytes(data)
    except Exception as e:
        raise RuntimeError(f"failed to load bitmap {path}: {e}")

# ---------- worker ----------

def _init_worker(v4_map: Dict[int, List[Tuple[int, int]]],
                 v6_map: Dict[int, List[Tuple[int, int]]]) -> None:
    global G_V4, G_V6
    G_V4 = v4_map
    G_V6 = v6_map

def _one_day_task(args: Tuple[str, Path, bool, bool]) -> Tuple[str, int, int]:
    day, roar_path, do_v4, do_v6 = args
    bm = _load_bitmap(roar_path)
    oids = list(bm)  # iterate once

    v4_count = 0
    v6_count = 0

    if do_v4:
        v4_intervals: List[Tuple[int, int]] = []
        extend = v4_intervals.extend
        g4 = G_V4
        for oid in oids:
            lst = g4.get(int(oid))
            if lst:
                extend(lst)
        if v4_intervals:
            v4_intervals.sort(key=lambda t: (t[0], t[1]))
            v4_count = count_union(v4_intervals)

    if do_v6:
        v6_intervals: List[Tuple[int, int]] = []
        extend6 = v6_intervals.extend
        g6 = G_V6
        for oid in oids:
            lst = g6.get(int(oid))
            if lst:
                extend6(lst)
        if v6_intervals:
            v6_intervals.sort(key=lambda t: (t[0], t[1]))
            v6_count = count_union(v6_intervals)

    return (day, v4_count, v6_count)

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Daily VRP covered addresses per TAL using membership bitmaps")
    ap.add_argument("--warehouse", type=Path, required=True, help="per-TAL warehouse path, e.g., warehouse-apnic")
    ap.add_argument("--v4-out", type=Path, default=Path("daily_ipv4.csv"))
    ap.add_argument("--v6-out", type=Path, default=Path("daily_ipv6.csv"))
    ap.add_argument("--family", choices=["v4","v6","both"], default="both")
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end", type=str, default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 8) - 2))
    args = ap.parse_args()

    do_v4 = args.family in ("v4","both")
    do_v6 = args.family in ("v6","both")

    print("Building object interval maps ...")
    v4_map, v6_map = build_object_interval_maps(args.warehouse)
    print(f"Objects: v4={len(v4_map)} v6={len(v6_map)}")

    day_roars = list_latest_membership_snapshots(args.warehouse, args.start, args.end)
    if not day_roars:
        raise SystemExit("No membership snapshots found in indexes/membership")
    print(f"Days to compute: {len(day_roars)}")

    # Prefer fork on Linux so v4_map/v6_map share memory COW
    try:
        ctx = mp.get_context("fork")
    except ValueError:
        ctx = mp.get_context("spawn")

    results: List[Tuple[str,int,int]] = []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=ctx,
                             initializer=_init_worker,
                             initargs=(v4_map, v6_map)) as pool:
        futs = [pool.submit(_one_day_task, (day, path, do_v4, do_v6))
                for (day, path) in day_roars]
        tick = max(1, len(futs)//40)
        for i, fut in enumerate(as_completed(futs), 1):
            day, v4c, v6c = fut.result()
            results.append((day, v4c, v6c))
            if i % tick == 0:
                print(f"progress {i}/{len(futs)} day={day} v4={v4c} v6={v6c}", flush=True)

    results.sort(key=lambda t: t[0])

    if do_v4:
        args.v4_out.parent.mkdir(parents=True, exist_ok=True)
        with open(args.v4_out, "w") as f:
            f.write("date,ipv4_addresses\n")
            for day, v4c, _ in results:
                f.write(f"{day},{v4c}\n")
        print(f"Wrote {args.v4_out}")

    if do_v6:
        args.v6_out.parent.mkdir(parents=True, exist_ok=True)
        with open(args.v6_out, "w") as f:
            f.write("date,ipv6_addresses\n")
            for day, _, v6c in results:
                f.write(f"{day},{v6c}\n")
        print(f"Wrote {args.v6_out}")

if __name__ == "__main__":
    main()

