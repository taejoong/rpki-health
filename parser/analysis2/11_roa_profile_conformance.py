# ASCII-only punctuation.
"""
Conformance to the 2024 ROA profile

What to test
- Structural sanity checks on ROA eContent fields per RFC 9582 (lightweight repository-only checks).

Why it matters
- Non-conformant ROAs risk rejection by modern RPs.

How we compute
- From roa_prefixes and roa_objects:
  - maxLength >= prefix length
  - IPv4 maxLength <= 32, IPv6 maxLength <= 128
  - no duplicate (afi, prefix, maxLength) tuples within one ROA
- Report violations per day and overall.

Outputs
- roa_profile_violations_overall.csv: violation, count
- daily_roa_profile_violations.csv: date, violation, count

Reference
- RFC 9582 ROA profile:
  https://www.rfc-editor.org/rfc/rfc9582

Example
- python analysis2/11_roa_profile_conformance.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-03-01
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

    rp = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               afi::INT AS afi,
               prefix::VARCHAR AS prefix,
               max_length::INT AS maxlen
        FROM read_parquet('{args.warehouse}/tables/table=roa_prefixes/**/*.parquet')
        """
    ).fetch_df()

    if rp.empty:
        raise SystemExit("No ROA prefixes")

    rp["plen"] = rp["prefix"].apply(parse_prefix_len)
    violations = []
    v1 = rp[rp["maxlen"] < rp["plen"]]
    violations.append(("maxlen_lt_plen", len(v1)))
    v2 = rp[(rp["afi"] == 4) & (rp["maxlen"] > 32)]
    violations.append(("v4_maxlen_gt_32", len(v2)))
    v3 = rp[(rp["afi"] == 6) & (rp["maxlen"] > 128)]
    violations.append(("v6_maxlen_gt_128", len(v3)))
    dup = rp.groupby(["object_id", "afi", "prefix", "maxlen"]).size().reset_index(name="n")
    v4 = dup[dup["n"] > 1]
    duplicate_rows = int(v4["n"].sum() - len(v4)) if not v4.empty else 0
    violations.append(("duplicate_prefix_rows_in_roa", duplicate_rows))

    pd.DataFrame(violations, columns=["violation", "count"]).to_csv(
        args.out_dir / "roa_profile_violations_overall.csv", index=False
    )

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps and load_bitmap(membership[0][1]) is None:
        use_bitmaps = False

    days = iter_days(args.warehouse, args.start, args.end)
    rows: list[tuple[str, str, int]] = []

    if use_bitmaps:
        for day, roar in tqdm(membership, desc="daily profile violations"):
            bm = load_bitmap(roar)
            assert bm is not None
            oids = set(int(x) for x in bm)
            sub = rp[rp["object_id"].isin(oids)]
            rows.extend(_daily_counts(day, sub))
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
        merged = prs.merge(rp, on="object_id", how="inner")
        for day in tqdm(days, desc="daily profile violations (presence)"):
            d = pd.Timestamp(day).date()
            sub = merged[(merged["sday"] <= d) & (merged["eday"] >= d)]
            rows.extend(_daily_counts(day, sub))

    pd.DataFrame(rows, columns=["date", "violation", "count"]).to_csv(
        args.out_dir / "daily_roa_profile_violations.csv", index=False
    )


def _daily_counts(day: str, df: pd.DataFrame) -> list[tuple[str, str, int]]:
    if df.empty:
        return [
            (day, "maxlen_lt_plen", 0),
            (day, "v4_maxlen_gt_32", 0),
            (day, "v6_maxlen_gt_128", 0),
            (day, "duplicate_prefix_rows_in_roa", 0),
        ]
    rows: list[tuple[str, str, int]] = []
    rows.append((day, "maxlen_lt_plen", int((df["maxlen"] < df["plen"]).sum())))
    rows.append((day, "v4_maxlen_gt_32", int(((df["afi"] == 4) & (df["maxlen"] > 32)).sum())))
    rows.append((day, "v6_maxlen_gt_128", int(((df["afi"] == 6) & (df["maxlen"] > 128)).sum())))
    dups = df.groupby(["object_id", "afi", "prefix", "maxlen"]).size().reset_index(name="n")
    duplicate_rows = int(dups[dups["n"] > 1]["n"].sum() - len(dups[dups["n"] > 1]))
    rows.append((day, "duplicate_prefix_rows_in_roa", max(duplicate_rows, 0)))
    return rows


if __name__ == "__main__":
    main()
