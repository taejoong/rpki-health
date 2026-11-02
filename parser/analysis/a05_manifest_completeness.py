from __future__ import annotations

"""A05 – Manifest completeness and accuracy assessment.
Estimates per-snapshot manifest coverage by comparing listed file hashes with
objects present in membership files and highlights hash encoding anomalies.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``manifests`` and ``manifests_entries`` for manifest payloads.
  - ``object_signatures`` and ``certs`` (optional) for CA aggregation.
* Snapshot membership files via ``list_snapshots``/``load_membership``.
* Optional KV database path (``--kv``) for resolving object ids to hashes.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir`` for data and output paths.
Outputs
-------
* ``manifest_coverage_by_ts.csv`` – per-snapshot coverage metrics per manifest.
* ``manifest_coverage_by_ca.csv`` – aggregated completeness metrics per CA.
Coverage calculations gracefully degrade when the KV database is unavailable,
but hash-length checks are still emitted. CAS shards are not needed.

Examples
--------
Review manifest coverage for APNIC snapshots::

    python analysis/a05_manifest_completeness.py \
        --out-root warehouse-apnic --out-dir outputs/a05

Inspect both APNIC and RIPE NCC warehouses and write combined metrics::

    python analysis/a05_manifest_completeness.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a05_multi

Any wrapper that exposes ``--warehouse warehouse-apnic`` should pass the same
arguments to ``--out-root`` for this script.
"""

import argparse
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set

import numpy as np
import pandas as pd

from analysis.analysis_common import (
    list_snapshots,
    load_kv_maps,
    load_membership,
    load_table,
)


HASH_LENGTHS = {
    "sha256": 64,
    "sha-256": 64,
    "sha512": 128,
    "sha-512": 128,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manifest completeness analysis")
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


def build_manifest_entries(entries: pd.DataFrame) -> Dict[int, List[dict]]:
    """Group manifest rows under their parent object for fast reuse per snapshot."""
    if entries.empty:
        return {}

    records: Dict[int, List[dict]] = {}
    for row in entries.itertuples(index=False):
        object_id = int(getattr(row, "object_id"))
        # Keep filenames and hashes handy so we can evaluate completeness later.
        records.setdefault(object_id, []).append(
            {
                "filename": getattr(row, "filename", None),
                "hash_alg": str(getattr(row, "hash_alg", "")),
                "file_hash": str(getattr(row, "file_hash", "")),
            }
        )
    return records


def hash_length_valid(hash_alg: str, file_hash: str) -> bool:
    expected = HASH_LENGTHS.get(hash_alg.lower())
    if expected is None:
        return True
    return len(str(file_hash)) == expected


def manifest_coverage(
    out_root: Sequence[Path],
    manifests: pd.DataFrame,
    entries_map: Dict[int, List[dict]],
    id_to_hash: Dict[int, str],
) -> pd.DataFrame:
    """Calculate manifest completeness per snapshot using membership and hashes."""
    records = []
    manifest_signers = load_table(out_root, "object_signatures")[
        ["object_id", "signer_cert_id"]
    ]
    certs = load_table(out_root, "certs")[["cert_id", "subject_dn"]]
    signer_map = manifest_signers.merge(certs, left_on="signer_cert_id", right_on="cert_id", how="left")
    signer_map = signer_map.set_index("object_id")

    snapshots = list_snapshots(out_root)
    manifest_ids = set(entries_map.keys()) | set(manifests.get("object_id", []))
    manifests_idx = manifests.set_index("object_id") if not manifests.empty else pd.DataFrame()

    for ts_label in snapshots:
        members = load_membership(out_root, ts_label)
        if not members:
            continue
        present_ids = set(members)
        hashes: Set[str] = set()
        if id_to_hash:
            # Resolve object ids through the KV map when available for set maths.
            hashes = {id_to_hash[obj_id] for obj_id in present_ids if obj_id in id_to_hash}

        for manifest_id in manifest_ids & present_ids:
            entries = entries_map.get(manifest_id, [])
            if not entries:
                coverage = float("nan")
                missing = None
            elif not hashes:
                coverage = float("nan")
                missing = None
            else:
                listed_hashes = {entry["file_hash"] for entry in entries}
                if not listed_hashes:
                    coverage = float("nan")
                    missing = 0
                else:
                    present_count = len(listed_hashes & hashes)
                    coverage = present_count / len(listed_hashes)
                    missing = len(listed_hashes) - present_count

            bad_length = any(
                not hash_length_valid(entry["hash_alg"], entry["file_hash"]) for entry in entries
            )

            metadata = {
                "ts": ts_label,
                "object_id": manifest_id,
                "coverage": coverage,
                "missing_count": missing,
                "bad_length_flag": bad_length,
            }

            if manifest_id in signer_map.index:
                signer_row = signer_map.loc[manifest_id]
                metadata["signer_cert_id"] = signer_row.get("signer_cert_id")
                metadata["ca_subject_dn"] = signer_row.get("subject_dn")

            if manifest_id in manifests_idx.index:
                metadata["this_update"] = manifests_idx.loc[manifest_id].get("this_update")
                metadata["next_update"] = manifests_idx.loc[manifest_id].get("next_update")

            records.append(metadata)

    return pd.DataFrame.from_records(records)


def aggregate_by_ca(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["ca_subject_dn", "mean_coverage", "manifest_count", "bad_length_share"])

    agg = (
        df.groupby("ca_subject_dn")
        .agg(
            mean_coverage=("coverage", "mean"),
            manifest_count=("object_id", "nunique"),
            bad_length_share=("bad_length_flag", "mean"),
        )
        .reset_index()
    )
    return agg


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir
    kv_path: Optional[Path] = args.kv

    ensure_out_dir(out_dir)

    manifests = load_table(out_root, "manifests")
    entries = load_table(out_root, "manifests_entries")

    _, id_to_hash = load_kv_maps(kv_path) if kv_path else ({}, {})

    entries_map = build_manifest_entries(entries)
    coverage_df = manifest_coverage(out_root, manifests, entries_map, id_to_hash)
    coverage_df.to_csv(out_dir / "manifest_coverage_by_ts.csv", index=False)

    ca_agg = aggregate_by_ca(coverage_df)
    ca_agg.to_csv(out_dir / "manifest_coverage_by_ca.csv", index=False)


if __name__ == "__main__":
    main()

