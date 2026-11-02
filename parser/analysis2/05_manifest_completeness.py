# ASCII-only punctuation.
"""
Manifest completeness and ghost objects (repository-only approximation)

What to test
- Per day, how many active signed objects have file hashes present on the current manifest entries.

Why it matters
- The manifest should enumerate all published signed objects.

Repository-only approximation
- We do not have a directory listing here; we approximate by checking that the SHA-256 file hashes
  listed in manifest entries appear among known object hashes in KV and among active objects that day.
- This is weaker than a true filesystem compare but flags some hygiene issues.

Outputs
- daily_manifest_coverage.csv: date, active_objects_est, manifest_listed_hashes, intersection_est

References
- RFC 9286 sections 2, 6.4, 6.5:
  https://www.rfc-editor.org/rfc/rfc9286

Example
- python analysis2/05_manifest_completeness.py --warehouse /path/to/warehouse --kv /path/to/kv.sqlite --start 2024-01-01 --end 2024-01-31
"""

from __future__ import annotations
import argparse
import os
import sqlite3
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from .common_legacy import (
    connect_duckdb,
    ensure_out_dir,
    iter_days,
    list_latest_membership,
    load_bitmap,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument(
        "--kv",
        type=Path,
        required=True,
        help="kv.sqlite with objects(object_hash TEXT, object_id INTEGER)",
    )
    ap.add_argument("--out-dir", type=Path, default=Path("analysis2/out"))
    ap.add_argument("--start", type=str, default=None)
    ap.add_argument("--end", type=str, default=None)
    ap.add_argument(
        "--threads", type=int, default=max(1, (os.cpu_count() or 8) - 1)
    )
    args = ap.parse_args()

    ensure_out_dir(args.out_dir)
    con = connect_duckdb(args.threads)

    me_glob = str(
        args.warehouse / "tables" / "table=manifests_entries" / "**" / "*.parquet"
    )
    entries = con.execute(
        f"SELECT file_hash FROM read_parquet('{me_glob}') WHERE file_hash IS NOT NULL"
    ).fetch_df()
    if entries.empty:
        raise SystemExit("No manifest entries found")
    entries["file_hash"] = (
        entries["file_hash"].astype(str)
        .str.replace("^0x", "", regex=True)
        .str.replace("[^0-9A-Fa-f]", "", regex=True)
        .str.lower()
    )
    entries = entries[entries["file_hash"] != ""]

    kv = sqlite3.connect(args.kv)
    kv.row_factory = sqlite3.Row
    kv_df = pd.read_sql_query("SELECT object_hash, object_id FROM objects", kv)
    if kv_df.empty:
        raise SystemExit("KV database has no objects table rows")
    kv_df["object_hash"] = kv_df["object_hash"].str.lower()

    joined = entries.merge(
        kv_df,
        left_on="file_hash",
        right_on="object_hash",
        how="inner",
    )[["object_id"]]
    listed_ids = set(int(x) for x in joined["object_id"].tolist())

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps and load_bitmap(membership[0][1]) is None:
        use_bitmaps = False

    days = iter_days(args.warehouse, args.start, args.end)
    rows: list[tuple[str, int, int, int]] = []

    if use_bitmaps:
        for day, roar in tqdm(membership, desc="manifest completeness"):
            bm = load_bitmap(roar)
            assert bm is not None
            oids = set(int(x) for x in bm)
            active = len(oids)
            listed = len(listed_ids)
            inter = len(oids.intersection(listed_ids))
            rows.append((day, active, listed, inter))
    else:
        pr_glob = str(
            args.warehouse
            / "tables"
            / "table=object_presence_runs"
            / "**"
            / "*.parquet"
        )
        prs = con.execute(
            f"""
            SELECT object_id::INT AS object_id,
                   CAST(start_ts AS TIMESTAMP) AS s,
                   CAST(end_ts   AS TIMESTAMP) AS e
            FROM read_parquet('{pr_glob}')
            """
        ).fetch_df()
        if prs.empty:
            raise SystemExit("object_presence_runs required when bitmaps are unavailable")
        prs["sday"] = pd.to_datetime(prs["s"]).dt.date
        prs["eday"] = pd.to_datetime(prs["e"]).dt.date
        prs = prs.drop(columns=["s", "e"])
        for day in tqdm(days, desc="manifest completeness (presence)"):
            d = pd.Timestamp(day).date()
            active_rows = prs[(prs["sday"] <= d) & (prs["eday"] >= d)]
            active = int(active_rows["object_id"].nunique())
            listed = len(listed_ids)
            listed_active = int(
                active_rows[active_rows["object_id"].isin(listed_ids)]["object_id"].nunique()
            )
            rows.append((day, active, listed, listed_active))

    pd.DataFrame(
        rows,
        columns=[
            "date",
            "active_objects_est",
            "manifest_listed_hashes",
            "intersection_est",
        ],
    ).to_csv(args.out_dir / "daily_manifest_coverage.csv", index=False)


if __name__ == "__main__":
    main()
