from __future__ import annotations

"""A14 – What-if simulations for EE validity caps and CRL pressure.
Models alternative maximum validity windows for EE certificates to estimate how
CRL pressure would change under different policy caps.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``certs`` for EE validity periods and issuer metadata.
  - ``ee_revocations`` for revocation timing.
* CLI arguments ``--out-root`` (one or more warehouse roots), ``--out-dir``, and
  ``--caps`` (days).
Outputs
-------
* ``what_if_crl_pressure.csv`` – per-issuer pressure indices under each cap.
No KV or CAS access is required; computations rely solely on warehouse tables.

Examples
--------
Simulate shorter EE caps for APNIC::

    python analysis/a14_ee_validity_what_if.py \
        --out-root warehouse-apnic --caps 7 14 30 --out-dir outputs/a14

Contrast APNIC and RIPE NCC outcomes::

    python analysis/a14_ee_validity_what_if.py \
        --out-root warehouse-apnic warehouse-ripencc --caps 14 30 --out-dir outputs/a14_multi

Pass any ``--warehouse warehouse-apnic`` arguments to this script as
``--out-root``.
"""

import argparse
from pathlib import Path
from typing import Iterable, List, Sequence

import numpy as np
import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="What-if simulations for EE validity caps")
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (per TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--caps", type=int, nargs="*", default=[7, 14, 30])
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def compute_pressure(out_root: Sequence[Path], caps: Iterable[int]) -> pd.DataFrame:
    certs = load_table(out_root, "certs")
    revocations = load_table(out_root, "ee_revocations")

    if certs.empty:
        return pd.DataFrame(columns=["cap_days", "issuer_dn", "pressure_index"])

    ee = certs[certs.get("is_ca_flag") == False].copy()  # noqa: E712
    if ee.empty:
        return pd.DataFrame(columns=["cap_days", "issuer_dn", "pressure_index"])

    ee = ee.merge(revocations, on="cert_id", how="left")
    ee["not_before"] = pd.to_datetime(ee["not_before"], utc=True, errors="coerce")
    ee["not_after"] = pd.to_datetime(ee["not_after"], utc=True, errors="coerce")
    ee["first_revocation"] = pd.to_datetime(ee["first_revocation"], utc=True, errors="coerce")

    rows = []
    for cap in caps:
        cap = int(cap)
        window_days = (ee["not_after"] - ee["not_before"]).dt.total_seconds().div(86400.0)
        window_days = window_days.clip(lower=0)
        capped_window = np.minimum(window_days, cap)
        time_until_revocation = (
            (ee["first_revocation"] - ee["not_before"]).dt.total_seconds().div(86400.0)
        )
        time_until_revocation = time_until_revocation.fillna(window_days)
        # Waste beyond the cap becomes pressure; clamp negatives to zero days.
        remaining = np.clip(capped_window - np.maximum(time_until_revocation, 0), a_min=0, a_max=None)
        ee[f"pressure_{cap}"] = remaining

        summary = (
            ee.groupby("issuer_dn")[f"pressure_{cap}"]
            .sum()
            .reset_index()
            .rename(columns={f"pressure_{cap}": "pressure_index"})
        )
        summary["cap_days"] = cap
        rows.append(summary)

    if not rows:
        return pd.DataFrame(columns=["cap_days", "issuer_dn", "pressure_index"])

    return pd.concat(rows, ignore_index=True)


def main() -> None:
    args = parse_args()
    out_dir: Path = args.out_dir
    ensure_out_dir(out_dir)
    df = compute_pressure(args.out_root, args.caps)
    df.to_csv(out_dir / "what_if_crl_pressure.csv", index=False)


if __name__ == "__main__":
    main()

