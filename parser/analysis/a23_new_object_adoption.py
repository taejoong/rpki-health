from __future__ import annotations

"""A23 – Emerging object type adoption tracking.

Measures adoption of new CMS object types (e.g., ASPA) across snapshots and by
trust anchor, producing adoption curves.

Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``object_signatures`` for object types and signer mapping.
  - ``certs`` for TAL attribution.
* Snapshot memberships via ``list_snapshots``/``load_membership`` when counts
  need to be restricted to objects present per snapshot.
* CLI arguments ``--out-root``, ``--out-dir``, optional ``--kv`` (for future
  extensions; unused if not required).

Outputs
-------
* ``new_object_adoption.csv`` – counts by object type, snapshot, and TAL.
* ``aspa_adoption.png`` – adoption curve for ASPA objects.
  The analysis requires only warehouse metadata and optional membership snapshots.

Examples
--------
Generate adoption reports for APNIC and RIPE NCC TALs::

    python analysis/a23_new_object_adoption.py \
        --out-root warehouse-apnic warehouse-ripencc \
        --out-dir analysis-output/a23

Investigate a single TAL and store charts for lectures::

    python analysis/a23_new_object_adoption.py \
        --out-root warehouse-afrinic \
        --out-dir classroom/a23
"""


import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd

from analysis.analysis_common import list_snapshots, load_membership, load_table, ts_label_to_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emerging object types and adoption")
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (per TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--kv", type=Path, default=None)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_counts(out_root: Sequence[Path]) -> pd.DataFrame:
    signatures = load_table(out_root, "object_signatures")
    certs = load_table(out_root, "certs")

    if signatures.empty:
        return pd.DataFrame(columns=["dataset_ts", "object_type", "count", "tal_id"])

    cert_map = certs.set_index("cert_id")["tal_id"].to_dict() if not certs.empty else {}
    type_map = signatures.set_index("object_id")
    records = []
    for ts_label in list_snapshots(out_root):
        # Restrict counts to the objects visible in the snapshot so adoption
        # curves reflect publication behaviour, not the full warehouse history.
        members = set(load_membership(out_root, ts_label))
        if not members:
            continue
        subset = type_map.loc[type_map.index.intersection(members)]
        if subset.empty:
            continue
        # Normalise the timestamp label once so plotting and CSV outputs are
        # consistent across scripts.
        ts = pd.to_datetime(ts_label_to_iso(ts_label), utc=True, errors="coerce")
        grouped = subset.groupby(["object_type", "signer_cert_id"]).size().reset_index(name="count")
        for row in grouped.itertuples(index=False):
            records.append(
                {
                    "dataset_ts": ts,
                    "object_type": row.object_type,
                    "count": row.count,
                    "tal_id": cert_map.get(row.signer_cert_id),
                }
            )
    return pd.DataFrame(records)


def write_outputs(out_dir: Path, counts: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    counts.to_csv(out_dir / "new_object_adoption.csv", index=False)

    if counts.empty:
        # Emit an empty figure so downstream lesson material still has a file to
        # reference when no ASPA objects are present yet.
        plt.figure()
        plt.savefig(out_dir / "aspa_adoption.png", dpi=150, bbox_inches="tight")
        plt.close()
        return

    aspa = counts[counts["object_type"].str.upper() == "ASPA"].copy()
    if aspa.empty:
        aspa = counts.assign(object_type=counts["object_type"].str.upper())
        aspa = aspa[aspa["object_type"] == "ASPA"]

    adoption = (
        aspa.groupby("dataset_ts")["count"].sum().reset_index()
        if not aspa.empty
        else pd.DataFrame({"dataset_ts": [], "count": []})
    )
    adoption.sort_values("dataset_ts", inplace=True)
    plt.figure(figsize=(8, 4))
    plt.plot(adoption["dataset_ts"], adoption["count"], marker="o")
    plt.xlabel("Dataset timestamp")
    plt.ylabel("ASPA objects present")
    plt.tight_layout()
    plt.savefig(out_dir / "aspa_adoption.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    counts = build_counts(args.out_root)
    write_outputs(args.out_dir, counts)


if __name__ == "__main__":
    main()

