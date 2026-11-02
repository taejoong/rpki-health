# ASCII-only punctuation.
"""
Ghostbusters record presence and freshness

What to test
- Presence of GBR objects and how recently they are updated around key repository events.

Why it matters
- GBRs facilitate contact when a repository has issues; many operators do not publish or refresh them.

How we compute
- Identify GBR objects via table=object_signatures joined to object type derived from tables.
- For each day, count active GBRs and the share with signing_time within the last 365 days.

Outputs
- daily_gbr_presence.csv: date, active_gbrs, fresh_gbrs, fresh_pct

References
- RFC 6493 Ghostbusters record:
  https://www.rfc-editor.org/rfc/rfc6493

Example
- python analysis2/12_gbr_presence.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-12-31
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
    iter_days,
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

    sig_glob = str(
        args.warehouse / "tables" / "table=object_signatures" / "**" / "*.parquet"
    )
    sig = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               content_type_oid::VARCHAR AS oid,
               CAST(signing_time AS TIMESTAMP) AS signing_time
        FROM read_parquet('{sig_glob}')
        """
    ).fetch_df()
    if sig.empty:
        raise SystemExit("No object_signatures found")

    gbr_oid = "1.2.840.113549.1.9.16.1.35"
    gbr = sig[sig["oid"] == gbr_oid][["object_id", "signing_time"]].drop_duplicates()
    if gbr.empty:
        pd.DataFrame(
            columns=["date", "active_gbrs", "fresh_gbrs", "fresh_pct"]
        ).to_csv(args.out_dir / "daily_gbr_presence.csv", index=False)
        return

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps and load_bitmap(membership[0][1]) is None:
        use_bitmaps = False

    days = iter_days(args.warehouse, args.start, args.end)
    rows: list[tuple[str, int, int, float]] = []

    if use_bitmaps:
        for day, roar in tqdm(membership, desc="GBR presence"):
            bm = load_bitmap(roar)
            assert bm is not None
            oids = set(int(x) for x in bm)
            sub = gbr[gbr["object_id"].isin(oids)].copy()
            total = len(sub)
            if total == 0:
                rows.append((day, 0, 0, 0.0))
                continue
            t = pd.Timestamp(f"{day} 00:00:00", tz="UTC")
            mask = pd.to_datetime(sub["signing_time"]) >= (t - pd.Timedelta(days=365))
            fresh = int(mask.sum())
            rows.append((day, total, fresh, 100.0 * fresh / total))
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
        merged = prs.merge(gbr, on="object_id", how="inner")
        for day in tqdm(days, desc="GBR presence (presence)"):
            d = pd.Timestamp(day).date()
            sub = merged[(merged["sday"] <= d) & (merged["eday"] >= d)]
            if sub.empty:
                rows.append((day, 0, 0, 0.0))
                continue
            t = pd.Timestamp(f"{day} 00:00:00", tz="UTC")
            mask = pd.to_datetime(sub["signing_time"]) >= (t - pd.Timedelta(days=365))
            fresh = int(mask.sum())
            rows.append((day, len(sub), fresh, 100.0 * fresh / len(sub)))

    pd.DataFrame(
        rows,
        columns=["date", "active_gbrs", "fresh_gbrs", "fresh_pct"],
    ).to_csv(args.out_dir / "daily_gbr_presence.csv", index=False)


if __name__ == "__main__":
    main()
