# ASCII-only punctuation.
"""
CMS signing-time adoption

What to test
- After May 2024, signed objects include signing-time and exclude binary-signing-time.

Why it matters
- The updated signed-object profile reduces unnecessary transfers; validators depend on consistent metadata.

How we compute
- From table=object_signatures: fraction of active objects per day that have signing_time populated.
- Split by object type using joins to ROA, MFT, CRL, CER.

Outputs
- daily_signing_time_adoption.csv: date, type, total_active, with_signing_time, pct

References
- RFC 9589 On the Use of the Cryptographic Message Syntax (CMS) Signed Attributes in RPKI:
  https://datatracker.ietf.org/doc/rfc9589/

Example
- python analysis2/10_signing_time_adoption.py --warehouse /path/to/warehouse --start 2024-01-01 --end 2024-04-30
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
               CAST(signing_time AS TIMESTAMP) AS signing_time
        FROM read_parquet('{sig_glob}')
        """
    ).fetch_df()

    type_frames = []
    for tname, label in [
        ("roa_objects", "ROA"),
        ("manifests", "MFT"),
        ("crls", "CRL"),
        ("certs", "CER"),
    ]:
        tglob = str(
            args.warehouse / "tables" / f"table={tname}" / "**" / "*.parquet"
        )
        df = con.execute(
            f"SELECT object_id::INT AS object_id, '{label}' AS type FROM read_parquet('{tglob}')"
        ).fetch_df()
        type_frames.append(df)
    types = pd.concat(type_frames, ignore_index=True).drop_duplicates()

    membership = list_latest_membership(args.warehouse, args.start, args.end)
    use_bitmaps = bool(membership)
    if use_bitmaps and load_bitmap(membership[0][1]) is None:
        use_bitmaps = False

    days = iter_days(args.warehouse, args.start, args.end)
    rows: list[tuple[str, str, int, int, float]] = []

    if use_bitmaps:
        for day, roar in tqdm(membership, desc="signing-time"):
            bm = load_bitmap(roar)
            assert bm is not None
            oids = pd.DataFrame({"object_id": [int(x) for x in bm]})
            mem = types.merge(oids, on="object_id", how="inner").merge(
                sig, on="object_id", how="left"
            )
            for typ, grp in mem.groupby("type"):
                total = len(grp)
                have = int(grp["signing_time"].notna().sum())
                pct = 100.0 * have / total if total else 0.0
                rows.append((day, typ, total, have, pct))
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
        merged = prs.merge(types, on="object_id", how="inner").merge(
            sig, on="object_id", how="left"
        )
        for day in tqdm(days, desc="signing-time (presence)"):
            d = pd.Timestamp(day).date()
            sub = merged[(merged["sday"] <= d) & (merged["eday"] >= d)]
            if sub.empty:
                continue
            for typ, grp in sub.groupby("type"):
                total = len(grp)
                have = int(grp["signing_time"].notna().sum())
                pct = 100.0 * have / total if total else 0.0
                rows.append((day, typ, total, have, pct))

    pd.DataFrame(
        rows,
        columns=["date", "type", "total_active", "with_signing_time", "pct"],
    ).to_csv(args.out_dir / "daily_signing_time_adoption.csv", index=False)


if __name__ == "__main__":
    main()
