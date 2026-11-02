from __future__ import annotations

"""A21 – Churn spike detection using membership deltas.

Detects days where repository churn exceeds a rolling mean by configurable
standard deviations, using both object counts and byte deltas when available.

Inputs
------
* Snapshot memberships via ``list_snapshots``/``load_membership``.
* Optional KV (``--kv``) and CAS (``--cas``) for byte-level delta estimates.
* CLI arguments ``--out-root``, ``--out-dir``, and ``--z`` for thresholding.

Outputs
-------
* ``churn_spikes.csv`` – flagged days with z-scores and context metrics.
  The script gracefully handles missing KV/CAS by focusing on object counts.

Examples
--------
Run against two TAL-specific warehouses using the default churn threshold::

    python analysis/a21_churn_spike_detector.py \
        --out-root warehouse-apnic warehouse-ripencc \
        --out-dir analysis-output/a21 \
        --kv kv.sqlite \
        --cas cas-shards

Tighten the threshold when only membership data is available::

    python analysis/a21_churn_spike_detector.py \
        --out-root warehouse-afrinic warehouse-lacnic \
        --out-dir analysis-output/a21 \
        --z 2.0
"""


import argparse
from pathlib import Path
from typing import Dict, Sequence, Set

import pandas as pd

from analysis.analysis_common import (
    list_snapshots,
    load_cas_sizes,
    load_kv_maps,
    load_membership,
    ts_label_to_iso,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Churn spike detector")
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
    parser.add_argument("--z", type=float, default=3.0)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def compute_series(
    out_root: Sequence[Path],
    id_to_hash: Dict[int, str],
    cas_sizes: Dict[str, int],
) -> pd.DataFrame:
    snapshots = list_snapshots(out_root)
    prev_members: Set[int] = set()
    records = []

    for ts_label in snapshots:
        members = set(load_membership(out_root, ts_label))
        added = members - prev_members
        removed = prev_members - members
        delta_count = len(added) + len(removed)

        delta_bytes = 0
        if id_to_hash and cas_sizes:
            # When CAS size information is available, approximate the transfer
            # cost by summing the byte size of all churned objects.
            for object_id in added | removed:
                hex_hash = id_to_hash.get(int(object_id))
                if hex_hash:
                    delta_bytes += cas_sizes.get(hex_hash.lower(), 0)

        ts = pd.to_datetime(ts_label_to_iso(ts_label), utc=True, errors="coerce")

        records.append(
            {
                "dataset_ts": ts,
                "delta_count": delta_count,
                "delta_bytes": delta_bytes,
            }
        )
        prev_members = members

    return pd.DataFrame.from_records(records)


def detect_spikes(df: pd.DataFrame, z_threshold: float) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["dataset_ts", "metric", "value", "zscore"])

    df = df.copy()
    # Ensure the time series is sorted and indexed by timestamp so rolling
    # statistics operate correctly for each metric.
    df["dataset_ts"] = pd.to_datetime(df["dataset_ts"], errors="coerce")
    df.sort_values("dataset_ts", inplace=True)
    df.set_index("dataset_ts", inplace=True)

    spikes = []
    for metric in ["delta_count", "delta_bytes"]:
        series = df[metric].astype(float)
        rolling_mean = series.rolling(window=14, min_periods=5).mean()
        rolling_std = series.rolling(window=14, min_periods=5).std().replace(0, pd.NA)
        zscore = (series - rolling_mean) / rolling_std
        mask = zscore.abs() >= z_threshold
        for ts, value in series[mask].items():
            spikes.append(
                {
                    "dataset_ts": ts,
                    "metric": metric,
                    "value": value,
                    "zscore": zscore.loc[ts],
                }
            )

    return pd.DataFrame(spikes)


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    ensure_out_dir(args.out_dir)
    _, id_to_hash = load_kv_maps(args.kv) if args.kv else ({}, {})
    cas_source = args.cas if args.cas else [root / "cas-sqlite" for root in out_root]
    cas_sizes = load_cas_sizes(cas_source)
    series = compute_series(out_root, id_to_hash, cas_sizes)
    spikes = detect_spikes(series, args.z)
    spikes.to_csv(args.out_dir / "churn_spikes.csv", index=False)


if __name__ == "__main__":
    main()

