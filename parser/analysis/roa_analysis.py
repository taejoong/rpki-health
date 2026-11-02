#!/usr/bin/env python3
"""
ROA analysis:
1) ROA object lifetime: for each ROA object_id, lifetime_days = datediff(day, first_seen_date, last_seen_date) + 1.
However, since last_seen_date could be the most recent date, you have to have an argument that specify today's date. Thus the objects that have last_seen_Date as today's date can be excluded 
from the anlaysis.

   Group by TAL and publication year = year(first_seen_ts). Output: analysis/roa_lifetime/<tal>-<year>.csv with DAYS,COUNT. 


2) EE validity span for ROAs: for each ROA object, take its signer EE cert (not_before, not_after),
   validity_days = datediff(day, not_before_date, not_after_date) + 1.
   Attribute to the ROA's TAL and publication year = year(first_seen_ts). Output: analysis/ee_validity_by_roa/<tal>-<year>.csv.

3) Days remaining at revocation: for ROAs whose signer EE appears on a CRL,
   days_remaining = datediff(day, revocation_date, cert.not_after_date).
   Keep only days_remaining > 0 (still valid when revoked). Group by TAL and revocation year. Output: analysis/roa_days_remaining_at_revocation/<tal>-<year>.csv.

Notes
- Reads Parquet directly with DuckDB. No need to access CAS bytes or kv.sqlite.
- If some tables are partitioned (e.g., certs/is_ca=true), the ** glob covers them.
"""

import argparse
import csv
import datetime
import os
from pathlib import Path
from typing import Iterable, Tuple

import duckdb


RowType = Tuple[str, int, int, int]


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_grouped_csv(
    rows: Iterable[RowType],
    out_dir: Path,
    tal_col: int,
    year_col: int,
    days_col: int,
    count_col: int,
    prefix: str,
) -> None:
    """
    rows: iterable of tuples (tal, year, days, count)
    Writes one CSV per (tal, year) with columns DAYS,COUNT.
    """
    buckets: dict[Tuple[str, int], list[Tuple[int, int]]] = {}
    for r in rows:
        tal = r[tal_col]
        year = int(r[year_col])
        days = int(r[days_col])
        cnt = int(r[count_col])
        buckets.setdefault((tal, year), []).append((days, cnt))

    for (tal, year), pairs in buckets.items():
        subdir = out_dir / prefix
        ensure_dir(subdir)
        out_path = subdir / f"{tal}-{year}.csv"
        pairs.sort(key=lambda x: x[0])  # sort by DAYS
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["DAYS", "COUNT"])
            for d, c in pairs:
                w.writerow([d, c])


def main() -> None:
    ap = argparse.ArgumentParser(
        description="ROA analyses: lifetimes, EE validity spans, and days-remaining-at-revocation."
    )
    ap.add_argument(
        "--warehouse",
        type=Path,
        default=Path("warehouse"),
        help="warehouse root containing tables/",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("warehouse/analysis"),
        help="output folder for CSVs",
    )
    ap.add_argument(
        "--threads",
        type=int,
        default=os.cpu_count() or 8,
        help="DuckDB threads",
    )
    ap.add_argument(
        "--limit-tal",
        dest="limit_tal",
        type=str,
        nargs="*",
        default=None,
        help="optional subset of TALs to analyze",
    )
    ap.add_argument(
        "--today",
        type=str,
        default=None,
        help="ISO date (YYYY-MM-DD) representing today; rows with last_seen equal to this date are excluded",
    )
    args = ap.parse_args()

    tables_root = args.warehouse / "tables"
    if not tables_root.exists():
        raise SystemExit(f"Not found: {tables_root}")

    roas_glob = str(tables_root / "table=roa_objects" / "**" / "*.parquet")
    ofl_glob = str(tables_root / "table=object_first_last" / "**" / "*.parquet")
    sig_glob = str(tables_root / "table=object_signatures" / "**" / "*.parquet")
    certs_glob = str(tables_root / "table=certs" / "**" / "*.parquet")
    crls_glob = str(tables_root / "table=crl_serials" / "**" / "*.parquet")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads}")
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute("PRAGMA enable_object_cache=true")

    # Optional TAL filter clause
    tal_filter = ""
    if args.limit_tal:
        in_list = ", ".join([f"'{t}'" for t in args.limit_tal])
        tal_filter = f" AND r.tal_id IN ({in_list}) "

    today_exclusion = ""
    if args.today:
        try:
            today_date = datetime.date.fromisoformat(args.today)
        except ValueError as exc:
            raise SystemExit(f"Invalid --today date: {args.today}") from exc
        today_literal = today_date.isoformat()
        today_exclusion = (
            " AND CAST(CAST(fl.last_seen_ts AS TIMESTAMP) AS DATE) <> DATE '"
            + today_literal
            + "' "
        )

    # 1) ROA lifetime distribution by TAL and publication year
    q1 = f"""
    WITH base AS (
      SELECT
        r.object_id,
        r.tal_id AS tal,
        CAST(strftime('%Y', CAST(fl.first_seen_ts AS TIMESTAMP)) AS INTEGER) AS year,
        CAST(CAST(fl.first_seen_ts AS TIMESTAMP) AS DATE) AS first_date,
        CAST(CAST(fl.last_seen_ts AS TIMESTAMP) AS DATE)  AS last_date
      FROM read_parquet('{roas_glob}') r
      JOIN read_parquet('{ofl_glob}')  fl
        ON fl.object_id = r.object_id
      WHERE 1=1 {tal_filter}
        {today_exclusion}
    ),
    lifetimes AS (
      SELECT
        tal,
        year,
        GREATEST(1, 1 + datediff('day', first_date, last_date)) AS days
      FROM base
      WHERE last_date >= first_date
    )
    SELECT tal, year, days, COUNT(*) AS cnt
    FROM lifetimes
    GROUP BY tal, year, days
    ORDER BY tal, year, days
    """
    rows1 = con.execute(q1).fetchall()
    write_grouped_csv(
        rows1,
        args.out,
        tal_col=0,
        year_col=1,
        days_col=2,
        count_col=3,
        prefix="roa_lifetime",
    )

    # 2) EE validity span by TAL and publication year, counted per ROA object
    non_ca_clause = "COALESCE(try_cast(c.is_ca AS BOOLEAN), FALSE) = FALSE"

    q2 = f"""
    WITH pub AS (
      SELECT
        r.object_id,
        r.tal_id AS tal,
        CAST(strftime('%Y', CAST(fl.first_seen_ts AS TIMESTAMP)) AS INTEGER) AS year
      FROM read_parquet('{roas_glob}') r
      JOIN read_parquet('{ofl_glob}')  fl
        ON fl.object_id = r.object_id
      WHERE 1=1 {tal_filter}
    ),
    ee AS (
      SELECT object_id, signer_cert_id
      FROM read_parquet('{sig_glob}')
      WHERE object_type = 'ROA' AND signer_cert_id IS NOT NULL
    ),
    valid AS (
      SELECT
        p.tal,
        p.year,
        GREATEST(1, 1 + datediff('day', CAST(c.not_before AS DATE), CAST(c.not_after AS DATE))) AS days
      FROM pub p
      JOIN ee  ON ee.object_id = p.object_id
      JOIN read_parquet('{certs_glob}') c
        ON c.cert_id = ee.signer_cert_id
      WHERE {non_ca_clause}
        AND c.not_before IS NOT NULL
        AND c.not_after  IS NOT NULL
    )
    SELECT tal, year, days, COUNT(*) AS cnt
    FROM valid
    GROUP BY tal, year, days
    ORDER BY tal, year, days
    """
    rows2 = con.execute(q2).fetchall()
    write_grouped_csv(
        rows2,
        args.out,
        tal_col=0,
        year_col=1,
        days_col=2,
        count_col=3,
        prefix="ee_validity_by_roa",
    )

    # 3) Days remaining at revocation for ROAs, grouped by TAL and revocation year
    tal_filter_q3 = tal_filter.replace('r.tal_id', 'tal_id') if tal_filter else ""
    q3 = f"""
    WITH roas AS (
      SELECT object_id, tal_id AS tal
      FROM read_parquet('{roas_glob}')
      WHERE 1=1 {tal_filter_q3}
    ),
    ee AS (
      SELECT object_id, signer_cert_id
      FROM read_parquet('{sig_glob}')
      WHERE object_type = 'ROA' AND signer_cert_id IS NOT NULL
    ),
    revs AS (
      SELECT revoked_cert_id AS cert_id, MIN(CAST(revocation_date AS DATE)) AS rev_date
      FROM read_parquet('{crls_glob}')
      WHERE revocation_date IS NOT NULL
      GROUP BY revoked_cert_id
    ),
    joined AS (
      SELECT
        r.tal,
        CAST(strftime('%Y', revs.rev_date) AS INTEGER) AS year,
        CAST(c.not_after AS DATE) AS not_after_date,
        revs.rev_date               AS rev_date
      FROM roas r
      JOIN ee    ON ee.object_id = r.object_id
      JOIN revs  ON revs.cert_id = ee.signer_cert_id
      JOIN read_parquet('{certs_glob}') c
        ON c.cert_id = ee.signer_cert_id
      WHERE {non_ca_clause}
        AND c.not_after IS NOT NULL
    ),
    days_rem AS (
      SELECT
        tal,
        year,
        datediff('day', rev_date, not_after_date) AS days
      FROM joined
    )
    SELECT tal, year, days, COUNT(*) AS cnt
    FROM days_rem
    WHERE days > 0
    GROUP BY tal, year, days
    ORDER BY tal, year, days
    """
    rows3 = con.execute(q3).fetchall()
    write_grouped_csv(
        rows3,
        args.out,
        tal_col=0,
        year_col=1,
        days_col=2,
        count_col=3,
        prefix="roa_days_remaining_at_revocation",
    )

    print(f"Done. Outputs under: {args.out}")
    print("- roa_lifetime/<tal>-<year>.csv")
    print("- ee_validity_by_roa/<tal>-<year>.csv")
    print("- roa_days_remaining_at_revocation/<tal>-<year>.csv")


if __name__ == "__main__":
    main()
