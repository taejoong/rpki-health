from __future__ import annotations

"""A04 – Object lifetime distribution vs. EE validity window analysis.
Relates the observed repository lifetime of signed objects to the validity span
of their signer EE certificates and visualises lifetime distributions.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``object_signatures`` for signer and type metadata.
  - ``object_presence_stats`` for per-object first/last seen timestamps.
  - ``certs`` for EE validity intervals.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``lifetime_vs_ee.csv`` – per-object metrics including lifetime, validity, and
  lifetime-to-validity ratios.
* ``object_lifetime_scatter_<type>.png`` and ``object_lifetime_cdf_<type>.png``
  – scatter/CDF visuals per object type.
The script works entirely from warehouse metadata tables without KV or CAS.

Examples
--------
Generate lifetime plots for APNIC data only::

    python analysis/a04_object_lifetime_distribution.py \
        --out-root warehouse-apnic --out-dir outputs/a04

Blend APNIC and RIPE NCC observations to compare distributions::

    python analysis/a04_object_lifetime_distribution.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a04_multi

Workflows that accept ``--warehouse warehouse-apnic`` should supply the same
paths via ``--out-root``.
"""


import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.analysis_common import load_presence_stats, load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Object lifetime distribution vs EE validity")
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


def compute_lifetimes(out_roots: Sequence[Path]) -> pd.DataFrame:
    signatures = load_table(out_roots, "object_signatures")
    certs = load_table(out_roots, "certs")
    presence_stats = load_presence_stats(out_roots)

    if signatures.empty or certs.empty or presence_stats.empty:
        return pd.DataFrame(
            columns=[
                "object_id",
                "object_type",
                "signer_cert_id",
                "lifetime_days",
                "ee_window_days",
                "ratio",
            ]
        )

    # Focus on each EE's validity span so we can measure waste later on.
    ee_certs = certs[["cert_id", "not_before", "not_after"]].copy()
    ee_certs.rename(columns={"cert_id": "signer_cert_id"}, inplace=True)
    ee_certs["not_before"] = pd.to_datetime(ee_certs["not_before"], utc=True, errors="coerce")
    ee_certs["not_after"] = pd.to_datetime(ee_certs["not_after"], utc=True, errors="coerce")

    sig = signatures[["object_id", "signer_cert_id", "object_type"]].copy()
    presence = presence_stats[["object_id", "first_seen_ts", "last_seen_ts"]].copy()
    presence["first_seen_ts"] = pd.to_datetime(
        presence["first_seen_ts"], utc=True, errors="coerce"
    )
    presence["last_seen_ts"] = pd.to_datetime(
        presence["last_seen_ts"], utc=True, errors="coerce"
    )

    merged = sig.merge(presence, on="object_id", how="left").merge(ee_certs, on="signer_cert_id", how="left")
    merged["lifetime_days"] = (
        (merged["last_seen_ts"] - merged["first_seen_ts"])
        .dt.total_seconds()
        .div(86400.0)
    )
    merged["ee_window_days"] = (
        (merged["not_after"] - merged["not_before"])
        .dt.total_seconds()
        .div(86400.0)
    )
    merged["ratio"] = np.where(
        merged["ee_window_days"] > 0,
        merged["lifetime_days"] / merged["ee_window_days"],
        np.nan,
    )
    return merged


def write_outputs(out_dir: Path, lifetimes: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    lifetimes[[
        "object_id",
        "object_type",
        "signer_cert_id",
        "lifetime_days",
        "ee_window_days",
        "ratio",
    ]].to_csv(out_dir / "lifetime_vs_ee.csv", index=False)

    if lifetimes.empty:
        return

    for object_type, group in lifetimes.groupby("object_type"):
        safe_type = str(object_type).lower().replace("/", "_")

        plt.figure(figsize=(6, 4))
        sample = group.dropna(subset=["lifetime_days", "ee_window_days"])
        if len(sample) > 200_000:
            sample = sample.sample(200_000, random_state=42)
        plt.scatter(sample["ee_window_days"], sample["lifetime_days"], s=2, alpha=0.3)
        plt.xlabel("EE validity window (days)")
        plt.ylabel("Object lifetime (days)")
        plt.title(f"Lifetime vs validity for {object_type}")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out_dir / f"lifetime_vs_ee_{safe_type}_scatter.png", dpi=150)
        plt.close()

        plt.figure(figsize=(6, 4))
        values = group["ratio"].dropna().sort_values()
        if not values.empty:
            y = np.linspace(0, 1, len(values), endpoint=False)
            plt.plot(values.values, y)
        plt.xlabel("Lifetime / EE validity ratio")
        plt.ylabel("CDF")
        plt.title(f"CDF for {object_type}")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out_dir / f"lifetime_vs_ee_{safe_type}_cdf.png", dpi=150)
        plt.close()


def main() -> None:
    args = parse_args()
    lifetimes = compute_lifetimes(args.out_root)
    write_outputs(args.out_dir, lifetimes)


if __name__ == "__main__":
    main()

