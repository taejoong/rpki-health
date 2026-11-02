from __future__ import annotations

"""A01 – EE validity windows vs. on-repository lifetime and CRL pressure.
This script quantifies how efficiently publication points revoke end-entity
certificates by comparing certificate validity windows with revocation timing
and with the observed lifetime of signed objects in the repository.
Inputs
------
* Warehouse tables read via :mod:`analysis.analysis_common`:
  - ``certs`` filtered to EE rows (``is_ca_flag == False``)
  - ``ee_revocations`` for ``first_revocation`` timestamps
  - ``object_signatures`` to associate repository objects with signer EEs
  - ``object_presence_stats`` for observed first/last sighting timestamps
* Command line arguments ``--out-root`` (one or more warehouse roots) and
  ``--out-dir`` (directory for generated artifacts).
Outputs
-------
* ``ee_waste_days_by_ca.csv`` – aggregated waste statistics per CA.
* ``ee_waste_days_cdf.png`` – CDF of waste days across EEs.
* ``object_lifetime_vs_ee_validity.png`` – scatter of object lifetime vs.
  signer EE validity window (sampled).
* ``top_ee_waste_leaderboard.csv`` – top EEs ranked by total waste days.
The analysis runs entirely on warehouse tables and does not require access to
CAS shards or the KV store. All timestamps are interpreted as UTC.

Examples
--------
Run against the APNIC slice of the warehouse and store outputs locally::

    python analysis/a01_ee_validity_and_crl_pressure.py \
        --out-root warehouse-apnic --out-dir outputs/a01

Compare APNIC and RIPE NCC TALs by pointing at both roots::

    python analysis/a01_ee_validity_and_crl_pressure.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a01_multi

When using a higher-level runner that forwards ``--warehouse warehouse-apnic``,
map those arguments directly to ``--out-root`` here.
"""

import argparse
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from analysis.analysis_common import load_presence_stats, load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyse EE validity, CRL pressure, and object lifetime overlap",
    )
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (separated by TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def compute_waste_days(ee_df: pd.DataFrame, rev_df: pd.DataFrame) -> pd.DataFrame:
    """Derive per-EE waste days by combining validity windows and revocations."""
    if ee_df.empty:
        return pd.DataFrame(columns=list(ee_df.columns) + ["first_revocation", "waste_days"])

    ee = ee_df.copy()
    # Normalize validity timestamps so downstream math stays in pandas-land.
    if "not_before" in ee.columns:
        ee["not_before"] = pd.to_datetime(ee["not_before"], utc=True, errors="coerce")
    if "not_after" in ee.columns:
        ee["not_after"] = pd.to_datetime(ee["not_after"], utc=True, errors="coerce")

    rev = rev_df.copy()
    if not rev.empty and "first_revocation" in rev.columns:
        rev["first_revocation"] = pd.to_datetime(
            rev["first_revocation"], utc=True, errors="coerce"
        )

    merged = ee.merge(rev, how="left", left_on="cert_id", right_on="cert_id")
    # Treat never-revoked EEs as expiring at ``Timestamp.max`` for the subtraction.
    merged["first_revocation"].fillna(pd.Timestamp.max.tz_localize("UTC"), inplace=True)
    merged["waste_days"] = (
        (merged["not_after"] - merged["first_revocation"])
        .dt.total_seconds()
        .div(86400.0)
        .clip(lower=0)
    )
    merged.loc[~np.isfinite(merged["waste_days"]), "waste_days"] = 0.0
    return merged


def compute_object_lifetimes(
    ee_df: pd.DataFrame,
    sig_df: pd.DataFrame,
    presence_stats_df: pd.DataFrame,
) -> pd.DataFrame:
    if sig_df.empty or ee_df.empty or presence_stats_df.empty:
        return pd.DataFrame(
            columns=[
                "object_id",
                "signer_cert_id",
                "object_type",
                "first_seen_ts",
                "last_seen_ts",
                "not_before",
                "not_after",
                "lifetime_days",
                "ee_window_days",
                "ratio",
            ]
        )

    sig = sig_df.copy()
    presence = presence_stats_df[["object_id", "first_seen_ts", "last_seen_ts"]].copy()
    presence["first_seen_ts"] = pd.to_datetime(
        presence["first_seen_ts"], utc=True, errors="coerce"
    )
    presence["last_seen_ts"] = pd.to_datetime(
        presence["last_seen_ts"], utc=True, errors="coerce"
    )

    ee = ee_df[["cert_id", "not_before", "not_after"]].copy()
    ee.rename(columns={"cert_id": "signer_cert_id"}, inplace=True)
    ee["not_before"] = pd.to_datetime(ee["not_before"], utc=True, errors="coerce")
    ee["not_after"] = pd.to_datetime(ee["not_after"], utc=True, errors="coerce")

    merged = sig.merge(presence, on="object_id", how="left")
    merged = merged.merge(ee, on="signer_cert_id", how="left")
    # Translate raw durations into human-friendly day counts before charting.
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


def write_aggregates(out_dir: Path, ee_waste: pd.DataFrame, lifetimes: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)

    if ee_waste.empty:
        ee_summary = pd.DataFrame(
            columns=["issuer_dn", "mean", "p50", "p95", "max", "count"]
        )
    else:
        ee_summary = (
            ee_waste.groupby("issuer_dn")["waste_days"]
            .agg(
                mean="mean",
                p50=lambda s: s.quantile(0.5),
                p95=lambda s: s.quantile(0.95),
                max="max",
                count="count",
            )
            .reset_index()
        )
    ee_summary.to_csv(out_dir / "ee_waste_days_by_ca.csv", index=False)

    if not ee_waste.empty:
        leaderboard = (
            ee_waste.groupby(["cert_id", "subject_dn", "issuer_dn"])["waste_days"]
            .sum()
            .reset_index()
            .sort_values("waste_days", ascending=False)
            .head(200)
        )
    else:
        leaderboard = pd.DataFrame(
            columns=["cert_id", "subject_dn", "issuer_dn", "waste_days"]
        )
    leaderboard.to_csv(out_dir / "top_ee_waste_leaderboard.csv", index=False)

    if not ee_waste.empty:
        tal_month = ee_waste.copy()
        tal_month["month"] = pd.to_datetime(
            tal_month["not_after"], utc=True, errors="coerce"
        ).dt.to_period("M")
        tal_month = (
            tal_month.groupby(["tal_id", "month"])["waste_days"].mean().reset_index()
        )
    else:
        tal_month = pd.DataFrame(columns=["tal_id", "month", "waste_days"])
    tal_month.to_csv(out_dir / "ee_waste_days_by_tal_month.csv", index=False)

    if lifetimes.empty:
        lifetimes.to_csv(out_dir / "lifetime_vs_ee.csv", index=False)
        for path in [
            out_dir / "ee_waste_days_cdf.png",
            out_dir / "object_lifetime_vs_ee_validity.png",
        ]:
            plt.figure()
            plt.savefig(path, dpi=150, bbox_inches="tight")
            plt.close()
        return

    lifetimes[[
        "object_id",
        "signer_cert_id",
        "object_type",
        "lifetime_days",
        "ee_window_days",
        "ratio",
    ]].to_csv(out_dir / "lifetime_vs_ee.csv", index=False)

    plt.figure(figsize=(6, 4))
    waste_values = ee_waste["waste_days"].dropna().sort_values()
    if not waste_values.empty:
        y = np.linspace(0, 1, len(waste_values), endpoint=False)
        plt.plot(waste_values.values, y)
    plt.xlabel("Waste days")
    plt.ylabel("CDF")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "ee_waste_days_cdf.png", dpi=150)
    plt.close()

    plt.figure(figsize=(6, 4))
    sample = lifetimes.dropna(subset=["lifetime_days", "ee_window_days"])
    if len(sample) > 200_000:
        sample = sample.sample(200_000, random_state=42)
    plt.scatter(sample["ee_window_days"], sample["lifetime_days"], s=2, alpha=0.3)
    plt.xlabel("EE validity window (days)")
    plt.ylabel("Object lifetime on repo (days)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "object_lifetime_vs_ee_validity.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    out_roots: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir

    certs = load_table(out_roots, "certs")
    if not certs.empty:
        mask = certs.get("is_ca_flag") == False  # noqa: E712
        if "is_ca" in certs.columns:
            mask &= certs["is_ca"].astype(str).str.lower().isin(["false", "0"])
        ee_certs = certs[mask].copy()
    else:
        ee_certs = pd.DataFrame()

    ee_revocations = load_table(out_roots, "ee_revocations")
    object_signatures = load_table(out_roots, "object_signatures")
    presence_stats = load_presence_stats(out_roots)

    ee_waste = compute_waste_days(ee_certs, ee_revocations)
    lifetimes = compute_object_lifetimes(ee_certs, object_signatures, presence_stats)
    write_aggregates(out_dir, ee_waste, lifetimes)


if __name__ == "__main__":
    main()

