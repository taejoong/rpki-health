# ASCII-only punctuation.
"""
AS0 ROA hygiene

What to test
- Presence and evolution of AS0 ROAs and their overlap with non-AS0 ROAs.

Why it matters
- AS0 is a negative attestation for do not announce. Misuse can create avoidable Invalids.

How we compute (repository-only)
- Identify ROAs with asn == 0.
- Count active AS0 ROAs per day.
- Count overlaps: a given semantic hash that appears both with AS0 and non-zero ASN is a red flag.
  Repository-only overlap proxy: same prefix and maxLength present in AS0 and non-AS0 ROAs.

Outputs
- daily_as0_roas.csv: date, as0_roa_objects, as0_prefix_rows
- as0_overlap_prefixes.csv: prefix, max_length, first_seen_date, last_seen_date (proxy)

Reference
- RFC 6483: Validation of Route Origination and AS0 ROAs
  https://www.rfc-editor.org/rfc/rfc6483

Example
- python analysis2/03_as0_hygiene.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-06-30
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

    ro_glob = str(
        args.warehouse / "tables" / "table=roa_objects" / "**" / "*.parquet"
    )
    rp_glob = str(
        args.warehouse / "tables" / "table=roa_prefixes" / "**" / "*.parquet"
    )

    ro = con.execute(
        f"SELECT object_id::INT AS object_id, asn::BIGINT AS asn FROM read_parquet('{ro_glob}')"
    ).fetch_df()
    rp = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               afi::INT AS afi,
               prefix::VARCHAR AS prefix,
               max_length::INT AS maxlen
        FROM read_parquet('{rp_glob}')
        """
    ).fetch_df()

    df = rp.merge(ro, on="object_id", how="inner")

    as0 = df[df["asn"] == 0][["afi", "prefix", "maxlen"]].drop_duplicates()
    nz = df[df["asn"] != 0][["afi", "prefix", "maxlen"]].drop_duplicates()
    overlap = as0.merge(nz, on=["afi", "prefix", "maxlen"], how="inner")
    if not overlap.empty:
        overlap.to_csv(args.out_dir / "as0_overlap_prefixes.csv", index=False)

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps and load_bitmap(membership[0][1]) is None:
        use_bitmaps = False

    rows: list[tuple[str, int, int]] = []
    if use_bitmaps:
        ro_index = ro.set_index("object_id")
        df_index = df.set_index("object_id")
        for day, roar in tqdm(membership, desc="daily AS0"):
            bm = load_bitmap(roar)
            assert bm is not None
            oids = [int(x) for x in bm]
            roj = ro_index.loc[ro_index.index.intersection(oids)]
            as0_objects = int((roj["asn"] == 0).sum()) if not roj.empty else 0
            dpr = df_index.loc[df_index.index.intersection(oids)]
            as0_prefix_rows = int((dpr["asn"] == 0).sum()) if not dpr.empty else 0
            rows.append((day, as0_objects, as0_prefix_rows))
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
        prs = prs.merge(ro, on="object_id", how="left").merge(
            df[["object_id", "asn"]], on="object_id", how="left", suffixes=("_obj", "")
        )
        days = iter_days(args.warehouse, args.start, args.end)
        for day in tqdm(days, desc="daily AS0 (presence)"):
            d = pd.Timestamp(day).date()
            active = prs[(prs["sday"] <= d) & (prs["eday"] >= d)]
            if active.empty:
                rows.append((day, 0, 0))
                continue
            as0_objects = int((active["asn_obj"] == 0).sum())
            as0_prefix_rows = int((active["asn"] == 0).sum())
            rows.append((day, as0_objects, as0_prefix_rows))

    pd.DataFrame(rows, columns=["date", "as0_roa_objects", "as0_prefix_rows"]).to_csv(
        args.out_dir / "daily_as0_roas.csv", index=False
    )


if __name__ == "__main__":
    main()
