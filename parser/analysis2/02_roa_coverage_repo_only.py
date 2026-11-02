# ASCII-only punctuation.
"""
ROA coverage versus routing reality (repository-only variant)

What to test
- For each registrant or publication point, how much of their issued IP space appears to have a ROA at a given time.
- Repository-only approximation: for each ASN observed in ROAs, fraction of that ASN's ROAs that are minimal and evolution of count of distinct semantic ROA payloads.

Why it matters
- Operators aim to keep ROAs aligned with their routing plans.
- Even without BGP, we can quantify deployment scale and deduplication efficiency.

How we compute
- Count daily distinct semantic_hash across all ROAs per ASN and in total.
- Report per-day and per-ASN series. This indicates effective ROA set size independent of EE churn.

Outputs
- daily_distinct_semantic.csv: date, distinct_semantic_roas
- asn_daily_distinct_semantic.csv: date, asn, distinct_semantic_roas

References
- RFC 6811 origin validation behavior:
  https://datatracker.ietf.org/doc/html/rfc6811
- RFC 9255 identity caveat:
  https://www.rfc-editor.org/rfc/rfc9255

Example
- python analysis2/02_roa_coverage_repo_only.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-03-31
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

    ro_glob = str(
        args.warehouse / "tables" / "table=roa_objects" / "**" / "*.parquet"
    )
    df = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               asn::BIGINT AS asn,
               semantic_hash::VARCHAR AS sh
        FROM read_parquet('{ro_glob}')
        """
    ).fetch_df()
    if df.empty:
        raise SystemExit("No ROA objects")

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps and load_bitmap(membership[0][1]) is None:
        use_bitmaps = False

    rows_all: list[tuple[str, int]] = []
    rows_asn: list[tuple[str, int, int]] = []

    if use_bitmaps:
        for day, roar in tqdm(membership, desc="daily distinct semantic"):
            bm = load_bitmap(roar)
            assert bm is not None
            oids = set(int(x) for x in bm)
            sub = df[df["object_id"].isin(oids)]
            rows_all.append((day, sub["sh"].nunique()))
            if not sub.empty:
                for asn, grp in sub.groupby("asn"):
                    rows_asn.append((day, int(asn), grp["sh"].nunique()))
    else:
        raise SystemExit(
            "Membership bitmaps unavailable; install pyroaring or generate indexes/membership"
        )

    pd.DataFrame(rows_all, columns=["date", "distinct_semantic_roas"] ).to_csv(
        args.out_dir / "daily_distinct_semantic.csv", index=False
    )
    pd.DataFrame(rows_asn, columns=["date", "asn", "distinct_semantic_roas"] ).to_csv(
        args.out_dir / "asn_daily_distinct_semantic.csv", index=False
    )


if __name__ == "__main__":
    main()
