from __future__ import annotations

"""A07 – Transfer cost proxy comparing RRDP-style deltas with full rsync.
Estimates snapshot change volume by combining membership differences with CAS
object sizes to approximate delta vs. full snapshot bandwidth.
Inputs
------
* Snapshot membership ids via ``list_snapshots`` and ``load_membership``.
* Optional KV database path (``--kv``) for mapping object ids to hashes.
* CAS SQLite shards via ``load_cas_sizes`` (``--cas`` argument or default).
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir`` for data/output locations.
Outputs
-------
* ``transfer_proxy.csv`` listing per-snapshot total bytes, delta bytes, and the
  delta ratio.
If CAS sizes or the KV map are unavailable, the script emits zero-byte deltas
while still computing membership cardinalities.

Examples
--------
Estimate delta ratios for APNIC snapshots::

    python analysis/a07_transfer_cost_proxy.py \
        --out-root warehouse-apnic --out-dir outputs/a07

Compare APNIC and RIPE NCC bandwidth profiles::

    python analysis/a07_transfer_cost_proxy.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a07_multi

Forward any ``--warehouse warehouse-apnic`` arguments to ``--out-root`` when
running through a helper script.
"""


import argparse
from pathlib import Path
from typing import Dict, Iterable, Sequence, Set

import pandas as pd

from analysis.analysis_common import (
    list_snapshots,
    load_cas_sizes,
    load_kv_maps,
    load_membership,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RRDP vs rsync transfer cost proxy")
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


def calculate_sizes(id_to_hash: Dict[int, str], cas_sizes: Dict[str, int], ids: Iterable[int]) -> int:
    """Sum byte sizes for a collection of object ids using CAS metadata."""
    total = 0
    for object_id in ids:
        hex_hash = id_to_hash.get(int(object_id))
        if hex_hash is None:
            continue
        total += cas_sizes.get(hex_hash.lower(), 0)
    return total


def build_transfer_proxy(
    out_root: Sequence[Path],
    id_to_hash: Dict[int, str],
    cas_sizes: Dict[str, int],
) -> pd.DataFrame:
    snapshots = list_snapshots(out_root)
    prev_members: Set[int] = set()
    records = []

    for ts_label in snapshots:
        # Membership diffs approximate the RRDP delta set for this snapshot.
        members = set(load_membership(out_root, ts_label))
        added = members - prev_members
        removed = prev_members - members

        snapshot_bytes = calculate_sizes(id_to_hash, cas_sizes, members)
        delta_bytes = calculate_sizes(id_to_hash, cas_sizes, added) + calculate_sizes(
            id_to_hash, cas_sizes, removed
        )
        ratio = delta_bytes / snapshot_bytes if snapshot_bytes else 0.0

        records.append(
            {
                "dataset_ts": ts_label,
                "delta_bytes": delta_bytes,
                "snapshot_bytes": snapshot_bytes,
                "delta_ratio": ratio,
                "added_count": len(added),
                "removed_count": len(removed),
            }
        )

        prev_members = members

    return pd.DataFrame.from_records(records)


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir
    ensure_out_dir(out_dir)

    _, id_to_hash = load_kv_maps(args.kv) if args.kv else ({}, {})
    cas_source = args.cas if args.cas else [root / "cas-sqlite" for root in out_root]
    cas_sizes = load_cas_sizes(cas_source)

    df = build_transfer_proxy(out_root, id_to_hash, cas_sizes)
    df.to_csv(out_dir / "transfer_proxy.csv", index=False)


if __name__ == "__main__":
    main()

