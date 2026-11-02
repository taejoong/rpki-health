from __future__ import annotations

"""A15 – Storage and bandwidth footprint estimation over time.
Calculates total snapshot storage footprint and change deltas using CAS object
sizes combined with membership snapshots.
Inputs
------
* Snapshot memberships via ``list_snapshots``/``load_membership``.
* Optional KV database (``--kv``) to resolve object hashes.
* CAS SQLite shards (``--cas`` or default) via ``load_cas_sizes`` for sizes.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``storage_footprint.csv`` – time series of snapshot bytes, delta bytes, and
  cumulative totals.
* ``storage_trend.png`` and ``delta_trend.png`` – visualisations of size trends.
The analysis degrades gracefully when CAS/KV data are missing (emitting zeros).

Examples
--------
Estimate storage needs for APNIC snapshots::

    python analysis/a15_storage_bandwidth.py \
        --out-root warehouse-apnic --out-dir outputs/a15

Compare APNIC and RIPE NCC storage growth::

    python analysis/a15_storage_bandwidth.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a15_multi

Map ``--warehouse warehouse-apnic`` arguments from wrappers onto ``--out-root``.
"""

import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd

from analysis.analysis_common import (
    list_snapshots,
    load_cas_sizes,
    load_kv_maps,
    load_membership,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Storage and bandwidth footprint over time")
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (per TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--kv", type=Path, default=None)
    parser.add_argument("--cas", type=Path, default=None)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def total_size(ids, id_to_hash, cas_sizes):
    total = 0
    for object_id in ids:
        hex_hash = id_to_hash.get(int(object_id))
        if not hex_hash:
            continue
        total += cas_sizes.get(hex_hash.lower(), 0)
    return total


def compute_footprint(
    out_root: Sequence[Path], id_to_hash: dict[int, str], cas_sizes: dict[str, int]
) -> pd.DataFrame:
    snapshots = list_snapshots(out_root)
    prev_total = 0
    records = []
    prev_ids = set()
    for ts in snapshots:
        members = set(load_membership(out_root, ts))
        total_bytes = total_size(members, id_to_hash, cas_sizes)
        # Delta approximates transfer volume between consecutive snapshots.
        delta = total_bytes - prev_total
        records.append(
            {
                "dataset_ts": ts,
                "snapshot_bytes": total_bytes,
                "delta_bytes": delta,
                "object_count": len(members),
                "delta_count": len(members - prev_ids) - len(prev_ids - members),
            }
        )
        prev_total = total_bytes
        prev_ids = members

    return pd.DataFrame.from_records(records)


def write_outputs(out_dir: Path, df: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    df.to_csv(out_dir / "storage_footprint.csv", index=False)

    if df.empty:
        for name in ["storage_total.png", "storage_delta.png"]:
            plt.figure()
            plt.savefig(out_dir / name, dpi=150, bbox_inches="tight")
            plt.close()
        return

    df["dataset_ts"] = pd.to_datetime(df["dataset_ts"], errors="coerce")
    df.sort_values("dataset_ts", inplace=True)

    plt.figure(figsize=(8, 4))
    plt.plot(df["dataset_ts"], df["snapshot_bytes"], label="Snapshot bytes")
    plt.xlabel("Dataset timestamp")
    plt.ylabel("Bytes")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "storage_total.png", dpi=150)
    plt.close()

    plt.figure(figsize=(8, 4))
    plt.bar(df["dataset_ts"], df["delta_bytes"], width=0.8)
    plt.xlabel("Dataset timestamp")
    plt.ylabel("Delta bytes")
    plt.tight_layout()
    plt.savefig(out_dir / "storage_delta.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    out_roots: Sequence[Path] = args.out_root
    _, id_to_hash = load_kv_maps(args.kv) if args.kv else ({}, {})
    cas_source = args.cas if args.cas else [root / "cas-sqlite" for root in out_roots]
    cas_sizes = load_cas_sizes(cas_source)
    df = compute_footprint(out_roots, id_to_hash, cas_sizes)
    write_outputs(args.out_dir, df)


if __name__ == "__main__":
    main()

