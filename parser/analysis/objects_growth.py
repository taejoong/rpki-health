#!/usr/bin/env python3
"""
Active object counts by type over time using presence runs.

This script replaces the old ``object_first_last`` helpers and uses the
presence-run tables that account for flicker.  Object activity is sampled at a
user-provided cadence and bucketed by object type (ROA, manifest, CRL, cert).

Usage examples
--------------

Monthly counts for every TAL::

    python analysis/objects_growth.py --warehouse warehouse --out outputs --freq M

Weekly counts for specific TALs::

    python analysis/objects_growth.py --warehouse warehouse --out outputs --freq W --tal ARIN APNIC

Monthly ROA counts for ARIN only::

    python analysis/objects_growth.py --warehouse warehouse --freq M --tal ARIN --object-types ROA

Arguments
~~~~~~~~~

``--warehouse``
    Path to the warehouse root that contains ``tables/`` and ``indexes/``.

``--out``
    Output directory for generated plots.  Defaults to ``warehouse/analysis``.

``--threads``
    Number of DuckDB threads.  Defaults to the CPU count.

``--freq``
    Sampling frequency of snapshots: daily (``D``), weekly (``W``), or monthly
    (``M``).

``--tal``
    Optional list of TAL identifiers to include.  Case-insensitive.  When
    omitted, all TALs are counted.

``--object-types``
    Optional list of object types (``ROA``, ``MFT``, ``CRL``, ``CER``) to
    include.  Defaults to every object type.

``--start-date`` / ``--end-date``
    Optional inclusive bounds on timestamps (``YYYY-MM-DD`` or
    ``YYYY-MM-DD HH:MM:SS``) used when sampling snapshots.
"""

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from typing import List, Sequence

VALID_TYPES = {"ROA", "MFT", "CRL", "CER"}

from common_presence import duck, snapshot_index, sample_sids

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def main():
    ap = argparse.ArgumentParser(description="Plot active object counts by type using presence runs")
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("warehouse/analysis"))
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 8)
    ap.add_argument("--freq", type=str, default="M", choices=["D", "W", "M"])
    ap.add_argument(
        "--tal",
        type=str,
        nargs="+",
        default=None,
        help="optional TAL filter(s) like ARIN RIPE APNIC LACNIC AFRINIC",
    )
    ap.add_argument(
        "--object-types",
        type=str,
        nargs="+",
        default=None,
        help="optional object type filter(s): ROA MFT CRL CER",
    )
    ap.add_argument("--start-date", type=str, default=None)
    ap.add_argument("--end-date", type=str, default=None)
    args = ap.parse_args()

    out_dir = args.out; ensure_dir(out_dir)

    tables_root = args.warehouse / "tables"
    runs_glob  = str(tables_root / "table=object_presence_runs" / "**" / "*.parquet")
    roas_glob  = str(tables_root / "table=roa_objects" / "**" / "*.parquet")
    mft_glob   = str(tables_root / "table=manifests" / "**" / "*.parquet")
    crl_glob   = str(tables_root / "table=crls" / "**" / "*.parquet")
    cer_glob   = str(tables_root / "table=certs" / "**" / "*.parquet")
    sig_glob   = str(tables_root / "table=object_signatures" / "**" / "*.parquet")

    # Build sample dates -> sids
    idx = snapshot_index(args.warehouse / "indexes" / "membership")
    picks = sample_sids(idx, freq=args.freq, start=args.start_date, end=args.end_date)
    if picks.empty:
        print("No snapshots in range"); return

    con = duck(args.threads)
    con.register("dates", picks)  # dates(sid:int, ts:str)

    tal_join = ""
    tal_label = "all"
    tal_values: List[str] = []
    if args.tal:
        tal_values = sorted({t.strip().upper() for t in args.tal if t.strip()})
        if tal_values:
            tal_df = pd.DataFrame({"tal_lower": [t.lower() for t in tal_values]})
            con.register("filter_tals", tal_df)
            tal_join = "JOIN filter_tals f ON LOWER(t.tal_id) = f.tal_lower"
            tal_label = "_".join(v.lower() for v in tal_values)

    type_join = ""
    type_label = "all"
    type_values: Sequence[str] = []
    if args.object_types:
        type_values = sorted({t.strip().upper() for t in args.object_types if t.strip()})
        if type_values:
            unknown = [t for t in type_values if t not in VALID_TYPES]
            if unknown:
                ap.error(
                    "Unknown object type(s): "
                    + ", ".join(unknown)
                    + ". Choices: "
                    + ", ".join(sorted(VALID_TYPES))
                )
            type_df = pd.DataFrame({"otype": list(type_values)})
            con.register("filter_types", type_df)
            type_join = "JOIN filter_types ft ON t.type = ft.otype"
            type_label = "_".join(v.lower() for v in type_values)

    q = f"""
    WITH roa AS (
      SELECT object_id, 'ROA' AS type, tal_id
      FROM read_parquet('{roas_glob}')
      WHERE tal_id IS NOT NULL
    ),
    mft AS (
      SELECT m.object_id, 'MFT' AS type, c.tal_id
      FROM read_parquet('{mft_glob}') m
      JOIN read_parquet('{sig_glob}') s ON s.object_id = m.object_id
      JOIN read_parquet('{cer_glob}') c ON c.cert_id = s.signer_cert_id
      WHERE c.tal_id IS NOT NULL
    ),
    crl AS (
      SELECT object_id, 'CRL' AS type, tal_id
      FROM read_parquet('{crl_glob}')
      WHERE tal_id IS NOT NULL
    ),
    cer AS (
      SELECT object_id, 'CER' AS type, tal_id
      FROM read_parquet('{cer_glob}')
      WHERE tal_id IS NOT NULL
    ),
    typed_with_tal AS (
      SELECT * FROM roa
      UNION ALL
      SELECT * FROM mft
      UNION ALL
      SELECT * FROM crl
      UNION ALL
      SELECT * FROM cer
    )
    SELECT d.ts AS date, t.tal_id AS tal, t.type, COUNT(DISTINCT pr.object_id) AS cnt
    FROM dates d
    JOIN read_parquet('{runs_glob}') pr
      ON pr.start_sid <= d.sid AND pr.end_sid >= d.sid
    JOIN typed_with_tal t
      ON t.object_id = pr.object_id
    {tal_join}
    {type_join}
    GROUP BY d.ts, t.tal_id, t.type
    ORDER BY d.ts, t.tal_id, t.type
    """
    df = con.execute(q).fetch_df()
    if df.empty:
        print("No data for filters"); return

    df["tal"] = df["tal"].fillna("UNKNOWN").str.upper()
    df["label"] = df.apply(lambda r: f"{r['type']} ({r['tal']})", axis=1)
    wide = df.pivot(index="date", columns="label", values="cnt").fillna(0.0)
    plt.figure()
    for col in sorted(wide.columns):
        plt.plot(pd.to_datetime(wide.index), wide[col], label=col)
    title = "Active RPKI objects by type"
    if tal_values:
        title += f" [TAL={' '.join(tal_values)}]"
    if type_values:
        title += f" [TYPE={' '.join(type_values)}]"
    plt.title(title)
    plt.xlabel("Date"); plt.ylabel("Active objects"); plt.legend()
    out_png = out_dir / f"objects_by_type_{tal_label}_{type_label}_{args.freq}.png"
    plt.savefig(out_png, bbox_inches="tight")
    print(f"Wrote {out_png}")

if __name__ == "__main__":
    main()
