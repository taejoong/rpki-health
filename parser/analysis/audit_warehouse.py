#!/usr/bin/env python3
"""
Audit warehouse using presence runs and stats. No use of object_first_last.

Checks:
- Coverage: typed object_ids appear in presence runs.
- Stats sanity: no run with end_sid < start_sid; total_days equals sum of run lengths.
- Parity vs kv: typed distinct ids vs kv.objects.
- Integrity: ROA has prefixes; MFT file_count vs entries; CRL revoked_count vs serial rows; cert time bounds.

Usage:
  python analysis/audit_warehouse.py --warehouse warehouse --kv warehouse/kv.sqlite --threads 32
"""

import argparse
import os
import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

def duck(threads: int):
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute("PRAGMA enable_progress_bar=true")
    return con

def kv_total(kv_path: Path) -> int:
    con = sqlite3.connect(str(kv_path))
    try:
        return int(con.execute("SELECT COUNT(*) FROM objects").fetchone()[0])
    finally:
        con.close()

def main():
    ap = argparse.ArgumentParser(description="Audit with presence runs and stats")
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--kv", type=Path, required=True)
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 8)
    args = ap.parse_args()

    troot = args.warehouse / "tables"
    runs_glob = str(troot / "table=object_presence_runs" / "**" / "*.parquet")
    stats_glob= str(troot / "table=object_presence_stats" / "**" / "*.parquet")
    roas_glob = str(troot / "table=roa_objects" / "**" / "*.parquet")
    rpf_glob  = str(troot / "table=roa_prefixes" / "**" / "*.parquet")
    mft_glob  = str(troot / "table=manifests" / "**" / "*.parquet")
    mfe_glob  = str(troot / "table=manifests_entries" / "**" / "*.parquet")
    crl_glob  = str(troot / "table=crls" / "**" / "*.parquet")
    crs_glob  = str(troot / "table=crl_serials" / "**" / "*.parquet")
    cer_glob  = str(troot / "table=certs" / "**" / "*.parquet")

    con = duck(args.threads)

    # Coverage: typed subset of runs
    q_cov = f"""
    WITH typed AS (
      SELECT DISTINCT object_id FROM read_parquet('{roas_glob}')
      UNION
      SELECT DISTINCT object_id FROM read_parquet('{mft_glob}')
      UNION
      SELECT DISTINCT object_id FROM read_parquet('{crl_glob}')
      UNION
      SELECT DISTINCT object_id FROM read_parquet('{cer_glob}')
    ),
    pr AS (SELECT DISTINCT object_id FROM read_parquet('{runs_glob}'))
    SELECT
      (SELECT COUNT(*) FROM typed) AS typed_ids,
      (SELECT COUNT(*) FROM pr)    AS runs_ids,
      (SELECT COUNT(*) FROM (SELECT object_id FROM typed EXCEPT SELECT object_id FROM pr)) AS typed_missing_in_runs
    """
    typed_ids, runs_ids, missing = con.execute(q_cov).fetchone()

    # Stats sanity
    q_bad_runs = f"""
    SELECT
      SUM(CASE WHEN end_sid < start_sid THEN 1 ELSE 0 END) AS bad_runs
    FROM read_parquet('{runs_glob}')
    """
    bad_runs = int(con.execute(q_bad_runs).fetchone()[0])

    q_stats_check = f"""
    WITH lengths AS (
      SELECT object_id, SUM(end_sid - start_sid + 1) AS sum_len
      FROM read_parquet('{runs_glob}')
      GROUP BY object_id
    ),
    s AS (SELECT object_id, total_days FROM read_parquet('{stats_glob}'))
    SELECT COUNT(*) FROM (
      SELECT l.object_id
      FROM lengths l
      JOIN s ON s.object_id = l.object_id
      WHERE COALESCE(l.sum_len, 0) != COALESCE(s.total_days, 0)
    )
    """
    stats_mismatch = int(con.execute(q_stats_check).fetchone()[0])

    # Parity vs kv
    kv_cnt = kv_total(args.kv)
    q_typed_distinct = f"""
    WITH typed AS (
      SELECT object_id FROM read_parquet('{roas_glob}')
      UNION
      SELECT object_id FROM read_parquet('{mft_glob}')
      UNION
      SELECT object_id FROM read_parquet('{crl_glob}')
      UNION
      SELECT object_id FROM read_parquet('{cer_glob}')
    )
    SELECT COUNT(DISTINCT object_id) FROM typed
    """
    typed_distinct = int(con.execute(q_typed_distinct).fetchone()[0])
    unknown_in_kv = max(0, kv_cnt - typed_distinct)

    # Integrity checks
    roa_missing_prefix = int(con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{roas_glob}') r
        LEFT JOIN read_parquet('{rpf_glob}') p ON p.object_id = r.object_id
        WHERE p.object_id IS NULL
        """
    ).fetchone()[0])

    mft_mismatch = int(con.execute(
        f"""
        WITH e AS (
          SELECT object_id, COUNT(*) AS n
          FROM read_parquet('{mfe_glob}')
          GROUP BY object_id
        )
        SELECT SUM(CASE WHEN COALESCE(e.n, 0) != m.file_count THEN 1 ELSE 0 END)
        FROM read_parquet('{mft_glob}') m
        LEFT JOIN e ON e.object_id = m.object_id
        """
    ).fetchone()[0])

    # CRL counts
    try:
        crl_mismatch = int(con.execute(
            f"""
            WITH s AS (
              SELECT crl_id, COUNT(*) AS n
              FROM read_parquet('{crs_glob}')
              GROUP BY crl_id
            )
            SELECT SUM(CASE WHEN COALESCE(s.n, 0) != c.revoked_count THEN 1 ELSE 0 END)
            FROM read_parquet('{crl_glob}') c
            LEFT JOIN s ON s.crl_id = c.crl_id
            """
        ).fetchone()[0])
    except Exception:
        crl_mismatch = int(con.execute(
            f"""
            WITH s AS (
              SELECT object_id, COUNT(*) AS n
              FROM read_parquet('{crs_glob}')
              GROUP BY object_id
            )
            SELECT SUM(CASE WHEN COALESCE(s.n, 0) != c.revoked_count THEN 1 ELSE 0 END)
            FROM read_parquet('{crl_glob}') c
            LEFT JOIN s ON s.object_id = c.object_id
            """
        ).fetchone()[0])

    # Cert bounds
    null_bounds, reversed_bounds = con.execute(
        f"""
        SELECT
          SUM(CASE WHEN not_before IS NULL OR not_after IS NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN not_before IS NOT NULL AND not_after IS NOT NULL AND CAST(not_before AS TIMESTAMP) > CAST(not_after AS TIMESTAMP) THEN 1 ELSE 0 END)
        FROM read_parquet('{cer_glob}')
        """
    ).fetchone()

    # Report
    print("=== AUDIT SUMMARY (presence runs) ===")
    print(f"typed_ids={typed_ids} runs_ids={runs_ids} typed_missing_in_runs={missing}")
    print(f"bad_runs={bad_runs} stats_mismatch={stats_mismatch}")
    print(f"kv.objects={kv_cnt} typed_distinct_ids={typed_distinct} unknown_in_kv={unknown_in_kv}")
    print(f"roa_missing_prefix_rows={roa_missing_prefix}")
    print(f"manifest_count_mismatches={mft_mismatch}")
    print(f"crl_count_mismatches={crl_mismatch}")
    print(f"cert_null_bounds={int(null_bounds)} cert_reversed_bounds={int(reversed_bounds)}")

if __name__ == "__main__":
    main()
