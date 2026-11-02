from __future__ import annotations

"""A06 – Freshness and staleness windows for CRLs and manifests.
Evaluates the age of CRLs and manifests at each snapshot and flags when
``next_update`` deadlines are exceeded.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``crls`` for CRL publication windows.
  - ``manifests`` for manifest validity intervals.
* Snapshot timestamps from ``list_snapshots`` / ``ts_label_to_iso``.
* CLI parameters ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``staleness_by_day.csv`` – per-snapshot averages and overdue flags.
* ``crl_staleness.png`` and ``manifest_staleness.png`` – trend charts.
Only warehouse metadata and snapshot directory names are required (no KV/CAS).

Examples
--------
Measure freshness for APNIC-only data::

    python analysis/a06_freshness_windows.py \
        --out-root warehouse-apnic --out-dir outputs/a06

Combine APNIC and RIPE NCC timelines to spot cross-TAL staleness::

    python analysis/a06_freshness_windows.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a06_multi

Use the same ``warehouse-<tal>`` paths if forwarding ``--warehouse`` arguments
from a wrapper script.
"""


import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.analysis_common import list_snapshots, load_table, ts_label_to_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Freshness windows for CRLs and manifests")
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


def compute_staleness(out_root: Sequence[Path]) -> pd.DataFrame:
    crls = load_table(out_root, "crls")
    manifests = load_table(out_root, "manifests")

    if crls.empty and manifests.empty:
        return pd.DataFrame(columns=[
            "dataset_ts",
            "crl_mean_age_days",
            "crl_past_due",
            "manifest_mean_age_days",
            "manifest_past_due",
        ])

    snapshots = list_snapshots(out_root)
    records = []

    crls_df = crls.copy()
    if not crls_df.empty:
        # Normalise CRL time bounds so we can subtract snapshot timestamps.
        crls_df["this_update"] = pd.to_datetime(crls_df["this_update"], utc=True, errors="coerce")
        crls_df["next_update"] = pd.to_datetime(crls_df["next_update"], utc=True, errors="coerce")

    mft_df = manifests.copy()
    if not mft_df.empty:
        # Manifests follow the same pattern: align their update windows first.
        mft_df["this_update"] = pd.to_datetime(mft_df["this_update"], utc=True, errors="coerce")
        mft_df["next_update"] = pd.to_datetime(mft_df["next_update"], utc=True, errors="coerce")

    for ts_label in snapshots:
        # Each snapshot is evaluated independently to compute age distributions.
        ts = pd.to_datetime(ts_label_to_iso(ts_label), utc=True, errors="coerce")
        record = {"dataset_ts": ts}

        if not crls_df.empty:
            ages = (ts - crls_df["this_update"]).dt.total_seconds().div(86400.0)
            record["crl_mean_age_days"] = ages.mean(skipna=True)
            past_due = (crls_df["next_update"].notna()) & (ts > crls_df["next_update"])
            record["crl_past_due"] = int(past_due.sum())
        else:
            record["crl_mean_age_days"] = np.nan
            record["crl_past_due"] = 0

        if not mft_df.empty:
            ages = (ts - mft_df["this_update"]).dt.total_seconds().div(86400.0)
            record["manifest_mean_age_days"] = ages.mean(skipna=True)
            past_due = (mft_df["next_update"].notna()) & (ts > mft_df["next_update"])
            record["manifest_past_due"] = int(past_due.sum())
        else:
            record["manifest_mean_age_days"] = np.nan
            record["manifest_past_due"] = 0

        records.append(record)

    return pd.DataFrame.from_records(records)


def write_outputs(out_dir: Path, df: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    df.to_csv(out_dir / "staleness_by_day.csv", index=False)

    if df.empty:
        for name in ["crl_staleness.png", "manifest_staleness.png"]:
            plt.figure()
            plt.savefig(out_dir / name, dpi=150, bbox_inches="tight")
            plt.close()
        return

    df_sorted = df.sort_values("dataset_ts")

    plt.figure(figsize=(8, 4))
    plt.plot(df_sorted["dataset_ts"], df_sorted["crl_mean_age_days"], label="CRL mean age")
    plt.xlabel("Dataset timestamp")
    plt.ylabel("Mean age (days)")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "crl_staleness.png", dpi=150)
    plt.close()

    plt.figure(figsize=(8, 4))
    plt.plot(
        df_sorted["dataset_ts"],
        df_sorted["manifest_mean_age_days"],
        label="Manifest mean age",
        color="orange",
    )
    plt.xlabel("Dataset timestamp")
    plt.ylabel("Mean age (days)")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "manifest_staleness.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    df = compute_staleness(args.out_root)
    write_outputs(args.out_dir, df)


if __name__ == "__main__":
    main()

