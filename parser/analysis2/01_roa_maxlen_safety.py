# ASCII-only punctuation.
"""
Minimal-ROA and maxLength safety

What to test
- For each ASN, how often ROAs are minimal (maxLength == prefix length).
- Distribution of delta = maxLength - prefixLen across time and ASNs.

Why it matters
- Broad maxLength increases the forged-origin sub-prefix attack surface.
- Best current practice is to avoid maxLength except where necessary.

How we compute (repository-only)
- Read roa_prefixes joined with roa_objects for object_id and ASN.
- delta = maxLength - prefixLen extracted from prefix string.
- Time dimension via object_presence_runs; for each day, restrict to ROAs active that day.
- Output:
  - daily_share_minimal.csv: date, minimal_share (percent of prefixes with delta=0)
  - asn_delta_hist.csv: asn, delta, count across entire timespan

References
- RFC 9319: The Use of maxLength in RPKI ROAs (BCP guidance)
  https://datatracker.ietf.org/doc/html/rfc9319
- RFC 7115: Origin Validation Operation Based on RPKI
  https://datatracker.ietf.org/doc/html/rfc7115

Example
- python analysis2/01_roa_maxlen_safety.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-01-31
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
    parse_prefix_len,
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

    rp_glob = str(
        args.warehouse / "tables" / "table=roa_prefixes" / "**" / "*.parquet"
    )
    ro_glob = str(
        args.warehouse / "tables" / "table=roa_objects" / "**" / "*.parquet"
    )
    pr_glob = str(
        args.warehouse
        / "tables"
        / "table=object_presence_runs"
        / "**"
        / "*.parquet"
    )

    df = con.execute(
        f"""
        SELECT rp.object_id::INT AS object_id,
               rp.afi::INT AS afi,
               rp.prefix::VARCHAR AS prefix,
               rp.max_length::INT AS maxlen,
               ro.asn::BIGINT AS asn
        FROM read_parquet('{rp_glob}') rp
        JOIN read_parquet('{ro_glob}') ro
          ON ro.object_id = rp.object_id
        """
    ).fetch_df()

    if df.empty:
        raise SystemExit("No ROA prefixes found")

    df["plen"] = df["prefix"].apply(parse_prefix_len)
    df["delta"] = df["maxlen"] - df["plen"]

    hist = df.groupby(["asn", "delta"]).size().reset_index(name="count")
    hist.to_csv(args.out_dir / "asn_delta_hist.csv", index=False)

    days = iter_days(args.warehouse, args.start, args.end)
    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps:
        sample = load_bitmap(membership[0][1])
        if sample is None:
            use_bitmaps = False

    daily_rows: list[tuple[str, float, int, int]] = []
    if use_bitmaps:
        for day, roar in tqdm(membership, desc="daily minimal share"):
            bm = load_bitmap(roar)
            assert bm is not None  # guarded by use_bitmaps
            oids = set(int(x) for x in bm)
            sub = df[df["object_id"].isin(oids)]
            if sub.empty:
                daily_rows.append((day, 0.0, 0, 0))
                continue
            total = len(sub)
            minimal = int((sub["delta"] == 0).sum())
            daily_rows.append((day, 100.0 * minimal / total, minimal, total))
    else:
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
        prs = prs.merge(df[["object_id", "delta"]], on="object_id", how="inner")
        day_list = [pd.Timestamp(day).date() for day in days]
        for day in tqdm(day_list, desc="daily minimal share (presence)"):
            active = prs[(prs["sday"] <= day) & (prs["eday"] >= day)]
            if active.empty:
                daily_rows.append((day.isoformat(), 0.0, 0, 0))
            else:
                total = len(active)
                minimal = int((active["delta"] == 0).sum())
                daily_rows.append((day.isoformat(), 100.0 * minimal / total, minimal, total))

    pd.DataFrame(
        daily_rows,
        columns=["date", "minimal_share_pct", "minimal_count", "total_prefixes"],
    ).to_csv(args.out_dir / "daily_share_minimal.csv", index=False)


if __name__ == "__main__":
    main()
