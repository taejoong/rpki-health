# ASCII-only punctuation.
"""
Planned CA key rollovers per RFC 6489 (repository-only approximation)

What to test
- Evidence of planned CA key rollovers: SKI changes at the issuer with overlap windows and continuous child re-issuance.

Why it matters
- Incorrect rollovers cause widespread NotFound or Invalid states.

How we compute (approx)
- Detect same subject_dn with multiple SKIs over time.
- For each rollover event:
  - measure overlap days where both old and new CA certs are present
  - count children certificates and objects linked by AKI that migrate to the new SKI
- Outputs:
  - ca_rollovers.csv: subject_dn, old_ski, new_ski, first_seen_new, last_seen_old, overlap_days, child_reissued_pct

Reference
- RFC 6489 CA Key Rollover:
  https://www.rfc-editor.org/rfc/rfc6489

Example
- python analysis2/08_ca_key_rollovers.py --warehouse /path/to/warehouse --start 2022-01-01 --end 2024-01-01
"""

from __future__ import annotations
import argparse
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from .common_legacy import connect_duckdb, ensure_out_dir


def _to_timestamp(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value)
    if isinstance(ts, pd.Series):
        raise TypeError('unexpected Series in _to_timestamp')
    if pd.isna(ts):
        return None
    if getattr(ts, 'tzinfo', None) is not None:
        return ts.tz_convert(None)
    return ts


def _overlap_days(
    a_start: pd.Timestamp | None,
    a_end: pd.Timestamp | None,
    b_start: pd.Timestamp | None,
    b_end: pd.Timestamp | None,
) -> int:
    if a_start is None or b_start is None:
        return 0
    a_end = a_end or a_start
    b_end = b_end or b_start
    start = max(a_start, b_start)
    end = min(a_end, b_end)
    if end < start:
        return 0
    return int((end.date() - start.date()).days)


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

    cert_glob = str(
        args.warehouse / "tables" / "table=certs" / "**" / "*.parquet"
    )
    prs_glob = str(
        args.warehouse
        / "tables"
        / "table=object_presence_runs"
        / "**"
        / "*.parquet"
    )

    certs = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               subject_dn::VARCHAR AS subject_dn,
               issuer_dn::VARCHAR AS issuer_dn,
               ski::VARCHAR AS ski,
               aki::VARCHAR AS aki
        FROM read_parquet('{cert_glob}')
        """
    ).fetch_df()
    if certs.empty:
        raise SystemExit("No certificates found")

    prs = con.execute(
        f"""
        SELECT object_id::INT AS object_id,
               CAST(start_ts AS TIMESTAMP) AS s,
               CAST(end_ts   AS TIMESTAMP) AS e
        FROM read_parquet('{prs_glob}')
        """
    ).fetch_df()
    if args.start:
        prs = prs[pd.to_datetime(prs["e"]).dt.date >= pd.to_datetime(args.start).date()]
    if args.end:
        prs = prs[pd.to_datetime(prs["s"]).dt.date <= pd.to_datetime(args.end).date()]

    multi = certs.groupby("subject_dn")["ski"].nunique()
    subjects = multi[multi > 1].index.tolist()

    rows: list[tuple[str, str, str, str, str, int, float]] = []
    for subj in tqdm(subjects, desc="rollovers"):
        sub_certs = (
            certs[certs["subject_dn"] == subj]
            .dropna(subset=["ski"])
            .drop_duplicates(subset=["ski"])
        )
        if len(sub_certs) < 2:
            continue
        sc = sub_certs.merge(prs, on="object_id", how="left")
        sc["first_seen"] = sc["s"].apply(_to_timestamp)
        sc["last_seen"] = sc["e"].apply(_to_timestamp)
        sc = sc.sort_values("first_seen")
        for i in range(len(sc) - 1):
            old = sc.iloc[i]
            new = sc.iloc[i + 1]
            overlap = _overlap_days(
                old["first_seen"],
                old["last_seen"],
                new["first_seen"],
                new["last_seen"],
            )
            children_old = certs[certs["aki"] == old["ski"]]
            children_new = certs[certs["aki"] == new["ski"]]
            denom = len(children_old["subject_dn"].unique()) or 1
            pct = 100.0 * len(children_new["subject_dn"].unique()) / denom
            first_new = new["first_seen"].date() if pd.notna(new["first_seen"]) else None
            last_old = old["last_seen"].date() if pd.notna(old["last_seen"]) else None
            rows.append(
                (
                    subj,
                    str(old["ski"]),
                    str(new["ski"]),
                    str(first_new),
                    str(last_old),
                    overlap,
                    pct,
                )
            )

    pd.DataFrame(
        rows,
        columns=[
            "subject_dn",
            "old_ski",
            "new_ski",
            "first_seen_new",
            "last_seen_old",
            "overlap_days",
            "child_reissued_pct",
        ],
    ).to_csv(args.out_dir / "ca_rollovers.csv", index=False)


if __name__ == "__main__":
    main()
