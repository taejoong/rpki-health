# ASCII-only punctuation.
"""
VRP stability versus cryptographic churn.

Goal
- Show that the VRP payload (semantic_hash) often remains identical while the signing EE or issuing CA changes.
- Optional split by hosted vs delegated using CA publication domains.

Inputs
- table=roa_objects: object_id, semantic_hash, asn, ee_serial
- table=certs: EE rows with serial_dec and aki; CA rows with ski and sia
- indexes/membership: daily bitmaps

Outputs
- analysis2/out/daily_vrp_churn.csv: date, semantics_active, semantics_persist, ee_churn, ca_churn
- analysis2/out/daily_vrp_churn_by_mode.csv: date, mode, semantics_active, semantics_persist, ee_churn, ca_churn (when --by-mode)

Method
- For each day, build semantic_hash -> sets of EE serials and issuer CA SKIs.
- Compare to the previous day; if sets differ for a semantic present on both days, that is churn.
"""

from __future__ import annotations
import argparse, os
from pathlib import Path
from collections import defaultdict

import duckdb, pandas as pd
from tqdm import tqdm

from common import (
    connect_duckdb, list_latest_membership, load_bitmap,
    ensure_out_dir, extract_hosts_from_sia, registrable_domain
)

RIR = {"afrinic.net","apnic.net","arin.net","lacnic.net","ripe.net"}


def load_roa_and_certs(con: duckdb.DuckDBPyConnection, warehouse: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ro_glob = str(warehouse / "tables" / "table=roa_objects" / "**" / "*.parquet")
    ce_glob = str(warehouse / "tables" / "table=certs" / "**" / "*.parquet")
    ro = con.execute(f"""
        SELECT object_id::INT AS object_id,
               semantic_hash::VARCHAR AS sh,
               asn::BIGINT AS asn,
               ee_serial::VARCHAR AS ee_serial
        FROM read_parquet('{ro_glob}')
    """).fetch_df()

    certs = con.execute(f"""
        SELECT object_id::INT AS object_id,
               is_ca::BOOLEAN AS is_ca,
               serial_dec::VARCHAR AS serial_dec,
               aki::VARCHAR AS aki,
               ski::VARCHAR AS ski,
               sia::VARCHAR AS sia
        FROM read_parquet('{ce_glob}')
    """).fetch_df()

    ee = certs[(certs["is_ca"] == False) & certs["serial_dec"].notna()][["serial_dec","aki"]].drop_duplicates()
    ca = certs[(certs["is_ca"] == True)][["object_id","ski","sia"]].drop_duplicates()
    return ro, ee, ca


def mode_from_sia(sia: str) -> str:
    hosts = extract_hosts_from_sia(sia)
    domains = {registrable_domain(h) for h in hosts}
    if domains and domains.issubset(RIR):
        return "hosted"
    return "delegated"


EXAMPLE = """Example:
  python analysis2/vrp_stability_vs_crypto_churn.py --warehouse warehouse-apnic --by-mode
"""


def main():
    ap = argparse.ArgumentParser(
        description="Compare semantic VRP stability against EE and CA changes across days.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLE,
    )
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("analysis2/out"))
    ap.add_argument("--threads", type=int, default=max(1, (os.cpu_count() or 8) - 1))
    ap.add_argument("--by-mode", action="store_true", help="split churn by hosted vs delegated")
    args = ap.parse_args()

    ensure_out_dir(args.out_dir)
    con = connect_duckdb(args.threads)

    ro, ee, ca = load_roa_and_certs(con, args.warehouse)
    if ro.empty:
        raise SystemExit("No ROAs found")

    ee_map = ee.set_index("serial_dec")["aki"].to_dict()  # ee serial -> issuer CA SKI
    ski_to_mode = {}
    if args.by_mode and not ca.empty:
        for _, r in ca.iterrows():
            ski_to_mode[str(r["ski"])] = mode_from_sia(r.get("sia") or "")

    latest = list_latest_membership(args.warehouse, None, None)
    if not latest:
        raise SystemExit("No membership bitmaps found")

    prev_sem_to_ees: dict[str, frozenset[str]] = {}
    prev_sem_to_cas: dict[str, frozenset[str]] = {}

    rows = []
    rows_mode = []

    for day, roar in tqdm(latest, desc="vrp churn"):
        bm = load_bitmap(roar)
        if bm is None:
            print("pyroaring not available; cannot load membership bitmap")
            break
        oids = set(int(x) for x in bm)
        sub = ro[ro["object_id"].isin(oids)].copy()
        if sub.empty:
            rows.append((day, 0, 0, 0, 0))
            continue

        sub["issuer_ski"] = sub["ee_serial"].map(lambda s: ee_map.get(s))
        cur_sem_to_ees: dict[str, frozenset[str]] = {}
        cur_sem_to_cas: dict[str, frozenset[str]] = {}
        for sh, grp in sub.groupby("sh"):
            cur_sem_to_ees[sh] = frozenset(grp["ee_serial"].dropna().astype(str).unique().tolist())
            cur_sem_to_cas[sh] = frozenset(grp["issuer_ski"].dropna().astype(str).unique().tolist())

        semantics_active = len(cur_sem_to_ees)
        commons = set(cur_sem_to_ees.keys()).intersection(prev_sem_to_ees.keys())
        ee_churn = sum(1 for sh in commons if cur_sem_to_ees[sh] != prev_sem_to_ees[sh])
        ca_churn = sum(1 for sh in commons if cur_sem_to_cas[sh] != prev_sem_to_cas[sh])
        rows.append((day, semantics_active, len(commons), ee_churn, ca_churn))

        if args.by_mode:
            def sem_mode(sh: str) -> str:
                cas = cur_sem_to_cas[sh]
                ms = {ski_to_mode.get(k, "delegated") for k in cas if k}
                if not ms:
                    return "delegated"
                if len(ms) == 1:
                    return next(iter(ms))
                return "delegated"
            agg = defaultdict(lambda: {"active":0,"persist":0,"ee":0,"ca":0})
            for sh in cur_sem_to_ees.keys():
                m = sem_mode(sh)
                agg[m]["active"] += 1
            for sh in commons:
                m = sem_mode(sh)
                agg[m]["persist"] += 1
                if cur_sem_to_ees[sh] != prev_sem_to_ees[sh]:
                    agg[m]["ee"] += 1
                if cur_sem_to_cas[sh] != prev_sem_to_cas[sh]:
                    agg[m]["ca"] += 1
            for m, v in agg.items():
                rows_mode.append((day, m, v["active"], v["persist"], v["ee"], v["ca"]))

        prev_sem_to_ees = cur_sem_to_ees
        prev_sem_to_cas = cur_sem_to_cas

    pd.DataFrame(rows, columns=["date","semantics_active","semantics_persist","ee_churn","ca_churn"])\
      .to_csv(args.out_dir / "daily_vrp_churn.csv", index=False)

    if args.by_mode and rows_mode:
        pd.DataFrame(rows_mode, columns=["date","mode","semantics_active","semantics_persist","ee_churn","ca_churn"])\
          .to_csv(args.out_dir / "daily_vrp_churn_by_mode.csv", index=False)


if __name__ == "__main__":
    main()
