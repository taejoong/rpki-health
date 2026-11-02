# ASCII-only punctuation.
"""
Replace-and-revoke compliance

What to test
- When an object disappears or is replaced, did the EE certificate get revoked on a subsequent CRL, and how quickly.
- For manifests, did each new manifest coincide with revocation of the prior manifest EE.

Why it matters
- RFC 9286 requires revocation of replaced objects and prior manifest EEs; prevents replay and stale objects.

How we compute (repository-only)
- Build object presence runs per object_id. Detect end of a run as a disappearance.
- Link object_id to its EE serial from object_signatures or embedded EE serial in ROAs.
- Scan crl_serials for the serial; compute time-to-revoke in days.
- For manifests, sort by this_update; check if each previous manifest EE serial appears on a CRL after the new one is published.

Outputs
- revoke_latency.csv: object_id, type, ee_serial, last_seen_date, revoked_date, days_to_revoke
- manifest_revoke_ratio.csv: year, checked, revoked_ok, ratio_pct

References
- RFC 9286, section 5.1 step 2 and section 5.2:
  https://www.rfc-editor.org/rfc/rfc9286

Example
- python analysis2/06_replace_and_revoke.py --warehouse /path/to/warehouse --start 2023-01-01 --end 2023-12-31
"""

from __future__ import annotations
import argparse
import os
from pathlib import Path

import pandas as pd

from .common_legacy import connect_duckdb, ensure_out_dir


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

    pr = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               CAST(start_ts AS TIMESTAMP) AS start_ts,
               CAST(end_ts   AS TIMESTAMP) AS end_ts
        FROM read_parquet('{args.warehouse}/tables/table=object_presence_runs/**/*.parquet')
        """
    ).fetch_df()
    if pr.empty:
        raise SystemExit("object_presence_runs is empty")
    if args.start:
        pr = pr[pd.to_datetime(pr["end_ts"]).dt.date >= pd.to_datetime(args.start).date()]
    if args.end:
        pr = pr[pd.to_datetime(pr["start_ts"]).dt.date <= pd.to_datetime(args.end).date()]

    sig = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               serial_number::VARCHAR AS serial_dec
        FROM read_parquet('{args.warehouse}/tables/table=object_signatures/**/*.parquet')
        """
    ).fetch_df()
    sig = sig.dropna(subset=["serial_dec"]).drop_duplicates()

    types = con.execute(
        f"""
        SELECT object_id::INT AS object_id, 'ROA' AS type FROM read_parquet('{args.warehouse}/tables/table=roa_objects/**/*.parquet')
        UNION ALL
        SELECT object_id::INT, 'MFT' FROM read_parquet('{args.warehouse}/tables/table=manifests/**/*.parquet')
        UNION ALL
        SELECT object_id::INT, 'CRL' FROM read_parquet('{args.warehouse}/tables/table=crls/**/*.parquet')
        UNION ALL
        SELECT object_id::INT, 'CER' FROM read_parquet('{args.warehouse}/tables/table=certs/**/*.parquet')
        """
    ).fetch_df()

    crls = con.execute(
        f"""
        SELECT revocation_date::TIMESTAMP AS revocation_date,
               serial_dec::VARCHAR AS serial_dec
        FROM read_parquet('{args.warehouse}/tables/table=crl_serials/**/*.parquet')
        """
    ).fetch_df()
    crls = crls.dropna(subset=["serial_dec"])

    ended = pr[["object_id", "end_ts"]].merge(types, on="object_id", how="left").merge(
        sig, on="object_id", how="left"
    )
    ended = ended.dropna(subset=["serial_dec"])
    if ended.empty:
        pd.DataFrame(
            columns=[
                "object_id",
                "type",
                "ee_serial",
                "last_seen_date",
                "revoked_date",
                "days_to_revoke",
            ]
        ).to_csv(args.out_dir / "revoke_latency.csv", index=False)
        return

    crl_min = (
        crls.groupby("serial_dec")["revocation_date"].min().reset_index()
    ).rename(columns={"revocation_date": "revoked_date"})

    joined = ended.merge(crl_min, on="serial_dec", how="left")
    joined["last_seen_date"] = pd.to_datetime(joined["end_ts"]).dt.date
    joined["revoked_day"] = pd.to_datetime(joined["revoked_date"]).dt.date
    joined["days_to_revoke"] = (
        pd.to_datetime(joined["revoked_day"]) - pd.to_datetime(joined["last_seen_date"])
    ).dt.days

    out = joined[
        ["object_id", "type", "serial_dec", "last_seen_date", "revoked_day", "days_to_revoke"]
    ].rename(columns={"serial_dec": "ee_serial", "revoked_day": "revoked_date"})
    out.to_csv(args.out_dir / "revoke_latency.csv", index=False)

    mft_only = out[out["type"] == "MFT"].copy()
    if not mft_only.empty:
        mft_only["year"] = pd.to_datetime(mft_only["last_seen_date"]).dt.year
        year_grp = mft_only.groupby("year").agg(
            checked=("ee_serial", "count"),
            revoked_ok=("revoked_date", lambda s: int(s.notna().sum())),
        )
        year_grp["ratio_pct"] = 100.0 * year_grp["revoked_ok"] / year_grp["checked"]
        year_grp.reset_index().to_csv(
            args.out_dir / "manifest_revoke_ratio.csv", index=False
        )


if __name__ == "__main__":
    main()
