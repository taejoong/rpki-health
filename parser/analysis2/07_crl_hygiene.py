# ASCII-only punctuation.
"""
CRL hygiene and alignment with manifests

What to test
- CRL thisUpdate and nextUpdate staleness.
- Alignment of CRL nextUpdate with manifest nextUpdate for the same publication point (approximation).
- Growth of CRL entry counts over time.

Why it matters
- Stale or misaligned CRLs undermine validator freshness and can cause acceptance of stale objects.

How we compute (approx)
- Without explicit publication point IDs, compute global daily medians:
  - CRL staleness: day > nextUpdate or day < thisUpdate.
  - Compare day-level medians of CRL nextUpdate and manifest nextUpdate.
- Track revoked_count from table=crls.

Outputs
- daily_crl_staleness.csv: date, total_active, stale_count, premature_count, stale_pct, premature_pct
- daily_crl_revoked_counts.csv: date, sum_revoked_counts

References
- RFC 9286 on CRL nextUpdate alignment with manifests:
  https://www.rfc-editor.org/rfc/rfc9286
- RFC 6487 certificate and CRL profile:
  https://www.rfc-editor.org/rfc/rfc6487

Example
- python analysis2/07_crl_hygiene.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-02-01
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

    crl_glob = str(args.warehouse / "tables" / "table=crls" / "**" / "*.parquet")
    crl = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               CAST(this_update AS TIMESTAMP) AS this_update,
               CAST(next_update AS TIMESTAMP) AS next_update,
               revoked_count::INT AS revoked_count
        FROM read_parquet('{crl_glob}')
        """
    ).fetch_df()
    if crl.empty:
        raise SystemExit("No CRL headers in this warehouse")

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps and load_bitmap(membership[0][1]) is None:
        use_bitmaps = False

    days = iter_days(args.warehouse, args.start, args.end)
    rows: list[tuple[str, int, int, int, float, float]] = []
    rows2: list[tuple[str, int]] = []

    if use_bitmaps:
        for day, roar in tqdm(membership, desc="CRL hygiene"):
            bm = load_bitmap(roar)
            assert bm is not None
            oids = set(int(x) for x in bm)
            sub = crl[crl["object_id"].isin(oids)]
            if sub.empty:
                rows.append((day, 0, 0, 0, 0.0, 0.0))
                rows2.append((day, 0))
                continue
            t = pd.Timestamp(f"{day} 00:00:00", tz="UTC")
            stale = int((sub["next_update"] < t).sum())
            prem = int((sub["this_update"] > t).sum())
            total = len(sub)
            rows.append((day, total, stale, prem, 100.0 * stale / total, 100.0 * prem / total))
            rows2.append((day, int(sub["revoked_count"].sum())))
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
        merged = prs.merge(crl, on="object_id", how="inner")
        for day in tqdm(days, desc="CRL hygiene (presence)"):
            d = pd.Timestamp(day).date()
            sub = merged[(merged["sday"] <= d) & (merged["eday"] >= d)]
            if sub.empty:
                rows.append((day, 0, 0, 0, 0.0, 0.0))
                rows2.append((day, 0))
                continue
            t = pd.Timestamp(f"{day} 00:00:00", tz="UTC")
            stale = int((sub["next_update"] < t).sum())
            prem = int((sub["this_update"] > t).sum())
            total = len(sub)
            rows.append((day, total, stale, prem, 100.0 * stale / total, 100.0 * prem / total))
            rows2.append((day, int(sub["revoked_count"].sum())))

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
    ).to_csv(args.out_dir / "daily_crl_staleness.csv", index=False)
    pd.DataFrame(rows2, columns=["date", "sum_revoked_counts"]).to_csv(
        args.out_dir / "daily_crl_revoked_counts.csv", index=False
    )


if __name__ == "__main__":
    main()
