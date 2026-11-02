# ASCII-only punctuation.
"""
Manifest timeliness and staleness windows

What to test
- For each day, fraction of active manifests that are stale (day > nextUpdate) or premature (day < thisUpdate).

Why it matters
- RFC 9286 requires RPs to treat stale or premature manifests as failure conditions, hurting availability.

How we compute
- For the last snapshot per day, list active manifests by object_id.
- Join to manifests table to read thisUpdate and nextUpdate.
- For day t (UTC), stale if t > nextUpdate; premature if t < thisUpdate.

Outputs
- daily_manifest_staleness.csv: date, total_active, stale_count, premature_count, stale_pct, premature_pct

Reference
- RFC 9286, sections 6.3 and 6.4:
  https://www.rfc-editor.org/rfc/rfc9286

Example
- python analysis2/04_manifest_timeliness.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-01-31
"""

from __future__ import annotations
import argparse
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from .common_legacy import (
    connect_duckdb,
    ensure_out_dir,
    list_latest_membership,
    load_bitmap,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("analysis2/out"))
    ap.add_argument("--start", type=str, default=None)
    ap.add_argument("--end", type=str, default=None)
    ap.add_argument(
        "--threads", type=int, default=max(1, (os.cpu_count() or 8) - 1)
    )
    args = ap.parse_args()

    ensure_out_dir(args.out_dir)
    con = connect_duckdb(args.threads)

    mft_glob = str(
        args.warehouse / "tables" / "table=manifests" / "**" / "*.parquet"
    )
    mft = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               CAST(this_update AS TIMESTAMP) AS this_update,
               CAST(next_update AS TIMESTAMP) AS next_update
        FROM read_parquet('{mft_glob}')
        """
    ).fetch_df()
    if mft.empty:
        raise SystemExit("No manifests found")

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    if not membership:
        raise SystemExit("Membership snapshots required for daily timeliness")
    if load_bitmap(membership[0][1]) is None:
        raise SystemExit("pyroaring is required for manifest timeliness; install pyroaring")

    rows: list[tuple[str, int, int, int, float, float]] = []
    for day, roar in tqdm(membership, desc="manifest staleness"):
        bm = load_bitmap(roar)
        assert bm is not None
        oids = pd.Series((int(x) for x in bm), name="object_id")
        sub = mft.merge(oids, on="object_id", how="inner")
        if sub.empty:
            rows.append((day, 0, 0, 0, 0.0, 0.0))
            continue
        t = pd.Timestamp(f"{day} 00:00:00", tz="UTC")
        stale = int((sub["next_update"] < t).sum())
        prem = int((sub["this_update"] > t).sum())
        total = len(sub)
        rows.append((day, total, stale, prem, 100.0 * stale / total, 100.0 * prem / total))

    pd.DataFrame(
        rows,
        columns=[
            "date",
            "total_active",
            "stale_count",
            "premature_count",
            "stale_pct",
            "premature_pct",
        ],
    ).to_csv(args.out_dir / "daily_manifest_staleness.csv", index=False)


if __name__ == "__main__":
    main()
