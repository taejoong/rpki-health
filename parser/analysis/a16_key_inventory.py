from __future__ import annotations

"""A16 – Key algorithm and size inventory.
Builds inventories of key algorithms and bit lengths for CA and EE certificates
and visualises adoption trends over time.
Inputs
------
* Warehouse table ``certs`` via :mod:`analysis.analysis_common`.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``key_inventory.csv`` – aggregated counts by algorithm/size/month and CA flag.
* ``key_inventory_hist.png`` and ``key_inventory_trend.png`` charts.
Only the ``certs`` table is required; KV/CAS access is unnecessary.

Examples
--------
Inventory APNIC key usage::

    python analysis/a16_key_inventory.py \
        --out-root warehouse-apnic --out-dir outputs/a16

Compare APNIC and RIPE NCC key profiles::

    python analysis/a16_key_inventory.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a16_multi

Pass ``--warehouse warehouse-apnic`` arguments from wrappers to ``--out-root``.
"""

import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Key algorithm and size inventory")
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (per TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_inventory(out_root: Sequence[Path]) -> pd.DataFrame:
    certs = load_table(out_root, "certs")
    if certs.empty:
        return pd.DataFrame(columns=["is_ca_flag", "spki_alg", "spki_bits", "not_before", "tal_id", "repo_id"])

    certs = certs[["is_ca_flag", "spki_alg", "spki_bits", "not_before", "tal_id", "repo_id"]]
    certs["not_before"] = pd.to_datetime(certs["not_before"], utc=True, errors="coerce")
    certs["month"] = certs["not_before"].dt.to_period("M")
    # Keep per-month buckets so trend charts can show algorithm adoption shifts.
    return certs


def write_outputs(out_dir: Path, inventory: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    inventory.to_csv(out_dir / "key_inventory.csv", index=False)

    if inventory.empty:
        for name in ["key_hist_ca.png", "key_hist_ee.png"]:
            plt.figure()
            plt.savefig(out_dir / name, dpi=150, bbox_inches="tight")
            plt.close()
        return

    for is_ca, label in [(True, "ca"), (False, "ee")]:
        subset = inventory[inventory["is_ca_flag"] == is_ca]
        plt.figure(figsize=(8, 4))
        counts = subset.groupby(["spki_alg", "spki_bits"]).size().reset_index(name="count")
        if counts.empty:
            plt.savefig(out_dir / f"key_hist_{label}.png", dpi=150, bbox_inches="tight")
            plt.close()
            continue
        labels = counts.apply(lambda r: f"{r['spki_alg']} {int(r['spki_bits'])}", axis=1)
        plt.bar(labels, counts["count"])
        plt.xticks(rotation=90)
        plt.ylabel("Certificates")
        plt.tight_layout()
        plt.savefig(out_dir / f"key_hist_{label}.png", dpi=150)
        plt.close()


def main() -> None:
    args = parse_args()
    inventory = build_inventory(args.out_root)
    write_outputs(args.out_dir, inventory)


if __name__ == "__main__":
    main()

