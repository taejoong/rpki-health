from __future__ import annotations


"""A11 – ROA coverage stability across the snapshot timeline.
Tracks how consistently each ``(prefix, ASN)`` pair remains authorised over the
observation window and produces churn heatmaps.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``roas_signers_snapshot`` preferred; otherwise ``roa_objects`` + memberships.
  - ``roa_prefixes`` for prefix data when reconstructing coverage.
  - ``object_signatures`` and ``certs`` for CA attribution.
* Snapshot membership files via ``list_snapshots``/``load_membership`` when
  reconstruction is required.
* CLI arguments ``--out-root`` (one or more warehouse roots), ``--out-dir``, optional ``--kv``.
Outputs
-------
* ``coverage_stability.csv`` – fraction-of-days coverage statistics per
  ``(prefix, ASN)`` pair.
* ``coverage_heatmap.png`` – churn visualisation by ASN (if data available).
No CAS access is needed; KV is optional for membership reconstruction.

Examples
--------
Track APNIC ROA coverage over time::

    python analysis/a11_roa_coverage_stability.py \
        --out-root warehouse-apnic --out-dir outputs/a11

Blend APNIC and RIPE NCC coverage records::

    python analysis/a11_roa_coverage_stability.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a11_multi

Map any ``--warehouse warehouse-apnic`` flags from wrappers onto ``--out-root``.
"""

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.analysis_common import list_snapshots, load_membership, load_table, ts_label_to_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ROA coverage stability over time")
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


def load_roa_state(out_root: Sequence[Path]) -> pd.DataFrame:
    snapshots = load_table(out_root, "roas_signers_snapshot")
    if not snapshots.empty:
        snapshots["dataset_ts"] = pd.to_datetime(
            snapshots["dataset_ts"].map(ts_label_to_iso), utc=True, errors="coerce"
        )
    else:
        roa_objects = load_table(out_root, "roa_objects")
        roa_prefixes = load_table(out_root, "roa_prefixes")
        if roa_objects.empty or roa_prefixes.empty:
            return pd.DataFrame(columns=["dataset_ts", "object_id", "asn", "prefix"])
        object_info = roa_objects.merge(roa_prefixes, on="object_id", how="inner")
        records = []
        for ts_label in list_snapshots(out_root):
            members = set(load_membership(out_root, ts_label))
            if not members:
                continue
            subset = object_info[object_info["object_id"].isin(members)]
            if subset.empty:
                continue
            ts = pd.to_datetime(ts_label_to_iso(ts_label), utc=True, errors="coerce")
            subset = subset.copy()
            subset["dataset_ts"] = ts
            records.append(subset[["dataset_ts", "object_id", "asn", "prefix"]])
        snapshots = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
    return snapshots[["dataset_ts", "object_id", "asn", "prefix"]]


def compute_stability(records: pd.DataFrame, snapshot_labels: List[pd.Timestamp]) -> pd.DataFrame:
    """Derive coverage ratios and churn rates for each prefix/ASN pair."""
    if records.empty or not snapshot_labels:
        return pd.DataFrame(columns=["prefix", "asn", "coverage_ratio", "churn_rate"])

    presence: Dict[Tuple[str, int], Set[pd.Timestamp]] = defaultdict(set)
    for row in records.itertuples(index=False):
        presence[(str(row.prefix), int(row.asn))].add(row.dataset_ts)

    ordered_ts = sorted(snapshot_labels)
    results = []
    for (prefix, asn), dates in presence.items():
        # Coverage is the fraction of snapshots in which the tuple appears.
        coverage = len(dates) / len(ordered_ts)
        timeline = [1 if ts in dates else 0 for ts in ordered_ts]
        transitions = sum(abs(timeline[i] - timeline[i - 1]) for i in range(1, len(timeline)))
        churn_rate = transitions / max(1, len(timeline) - 1)
        results.append(
            {
                "prefix": prefix,
                "asn": asn,
                "coverage_ratio": coverage,
                "churn_rate": churn_rate,
            }
        )
    return pd.DataFrame(results)


def write_outputs(out_dir: Path, stability: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    stability.to_csv(out_dir / "coverage_stability.csv", index=False)

    if stability.empty:
        plt.figure()
        plt.savefig(out_dir / "coverage_heatmap.png", dpi=150, bbox_inches="tight")
        plt.close()
        return

    asn_summary = stability.groupby("asn")["coverage_ratio"].mean().reset_index()
    asn_summary = asn_summary.sort_values("coverage_ratio", ascending=False).head(50)
    plt.figure(figsize=(8, 4))
    plt.bar(asn_summary["asn"].astype(str), asn_summary["coverage_ratio"])
    plt.xticks(rotation=90)
    plt.ylabel("Average coverage ratio")
    plt.tight_layout()
    plt.savefig(out_dir / "coverage_heatmap.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir

    records = load_roa_state(out_root)
    snapshot_labels = sorted(records["dataset_ts"].dropna().unique()) if not records.empty else []
    stability = compute_stability(records, list(snapshot_labels))
    write_outputs(out_dir, stability)


if __name__ == "__main__":
    main()

