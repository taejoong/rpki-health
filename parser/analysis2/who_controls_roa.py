# ASCII-only punctuation.
"""
Infer who controls ROAs based on issuer CA publication domain.

What
- Map each ROA to the issuer CA SKI via the EE serial, then infer a controller_domain from the issuer CA SIA host.
- Aggregate daily counts and overall totals. Also tag mode as hosted vs delegated by domain.

Why
- ROAs do not assert legal identity. Publication domain is a useful operational control signal.

Inputs
- table=roa_objects: object_id, asn, ee_serial
- table=certs: EE rows for serial_dec->aki; CA rows for ski->sia
- indexes/membership: for daily active sets

Outputs
- analysis2/out/controllers_daily.csv: date, controller_domain, mode, roa_objects, distinct_asns
- analysis2/out/top_controllers_overall.csv: controller_domain, mode, roa_objects, distinct_asns
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


def mode_from_domain(dom: str) -> str:
    return "hosted" if dom in RIR else "delegated"


EXAMPLE = """Example:
  python analysis2/who_controls_roa.py --warehouse warehouse-arin
"""


def main():
    ap = argparse.ArgumentParser(
        description="Attribute ROAs to controller domains derived from issuer CA SIAs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EXAMPLE,
    )
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("analysis2/out"))
    ap.add_argument("--threads", type=int, default=max(1, (os.cpu_count() or 8) - 1))
    args = ap.parse_args()

    ensure_out_dir(args.out_dir)
    con = connect_duckdb(args.threads)

    ro_glob = str(args.warehouse / "tables" / "table=roa_objects" / "**" / "*.parquet")
    ce_glob = str(args.warehouse / "tables" / "table=certs" / "**" / "*.parquet")

    ro = con.execute(f"""
        SELECT object_id::INT AS object_id,
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

    if ro.empty or certs.empty:
        raise SystemExit("Required tables missing")

    ees = certs[(certs["is_ca"] == False) & certs["serial_dec"].notna()][["serial_dec","aki"]].drop_duplicates()
    ee_to_issuer = ees.set_index("serial_dec")["aki"].to_dict()

    cas = certs[(certs["is_ca"] == True)][["ski","sia"]].drop_duplicates()
    ski_to_domain = {}
    for _, r in cas.iterrows():
        hosts = extract_hosts_from_sia(r.get("sia") or "")
        dom = registrable_domain(hosts[0]) if hosts else "unknown"
        ski_to_domain[str(r["ski"])] = dom

    latest = list_latest_membership(args.warehouse, None, None)
    if not latest:
        raise SystemExit("No membership bitmaps found")

    rows = []
    acc = defaultdict(lambda: {"roas": 0, "asns": set()})

    for day, roar in tqdm(latest, desc="controllers daily"):
        bm = load_bitmap(roar)
        if bm is None:
            print("pyroaring not available; cannot load membership bitmap")
            break
        oids = set(int(x) for x in bm)
        sub = ro[ro["object_id"].isin(oids)].copy()
        if sub.empty:
            continue
        sub["issuer_ski"] = sub["ee_serial"].map(lambda s: ee_to_issuer.get(s))
        sub["controller_domain"] = sub["issuer_ski"].map(lambda k: ski_to_domain.get(k, "unknown"))
        sub["mode"] = sub["controller_domain"].map(mode_from_domain)
        grp = sub.groupby(["controller_domain","mode"])
        for (dom, mode), g in grp:
            rows.append((day, dom, mode, len(g), g["asn"].nunique()))
            acc[(dom, mode)]["roas"] += len(g)
            acc[(dom, mode)]["asns"].update(set(g["asn"].tolist()))

    pd.DataFrame(rows, columns=["date","controller_domain","mode","roa_objects","distinct_asns"])\
      .to_csv(args.out_dir / "controllers_daily.csv", index=False)

    over = []
    for (dom, mode), v in acc.items():
        over.append((dom, mode, v["roas"], len(v["asns"])) )
    pd.DataFrame(over, columns=["controller_domain","mode","roa_objects","distinct_asns"])\
      .to_csv(args.out_dir / "top_controllers_overall.csv", index=False)


if __name__ == "__main__":
    main()
