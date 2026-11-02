from __future__ import annotations


"""A13 – CRL size and revocation entry dynamics.
Builds time series of CRL sizes and revocation insertions per CA to highlight
revocation churn and publication trends.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``crls`` for aggregate counts and timestamps.
  - ``crl_serials`` for individual revocation entries.
  - ``certs`` for CA identity metadata.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``crl_size_trends.csv`` – time series of CRL ``revoked_count`` per CA.
* ``crl_revocation_inserts.csv`` – per-day revocation entry counts.
* ``crl_dynamics.png`` – stacked area chart visualising CRL sizes over time.
Only warehouse tables are required; no KV or CAS data is used.

Examples
--------
Measure APNIC CRL growth and churn::

    python analysis/a13_crl_dynamics.py \
        --out-root warehouse-apnic --out-dir outputs/a13

Compare APNIC with RIPE NCC issuers::

    python analysis/a13_crl_dynamics.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a13_multi

When wrapping the script, map ``--warehouse warehouse-apnic`` to ``--out-root``.
"""

import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CRL size and entry dynamics")
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


def compute_crl_dynamics(out_root: Sequence[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    crls = load_table(out_root, "crls")
    crl_serials = load_table(out_root, "crl_serials")
    certs = load_table(out_root, "certs")

    if crls.empty:
        return pd.DataFrame(), pd.DataFrame()

    ca_map = certs.set_index("cert_id")["subject_dn"].to_dict() if not certs.empty else {}
    crls = crls.copy()
    # Align CRL metadata to calendar dates before grouping by issuer.
    crls["this_update"] = pd.to_datetime(crls["this_update"], utc=True, errors="coerce")
    crls["date"] = crls["this_update"].dt.date
    crls["ca_subject_dn"] = crls["issuer_cert_id"].map(ca_map)

    size_trend = (
        crls.groupby(["date", "ca_subject_dn"])["revoked_count"].max().reset_index()
    )

    rev_trend = pd.DataFrame()
    if not crl_serials.empty:
        crl_serials = crl_serials.copy()
        crl_serials["revocation_date"] = pd.to_datetime(
            crl_serials["revocation_date"], utc=True, errors="coerce"
        )
        crl_serials["date"] = crl_serials["revocation_date"].dt.date
        crl_serials = crl_serials.merge(
            crls[["crl_id", "issuer_cert_id"]], on="crl_id", how="left"
        )
        crl_serials["ca_subject_dn"] = crl_serials["issuer_cert_id"].map(ca_map)
        rev_trend = (
            crl_serials.groupby(["date", "ca_subject_dn"])["revoked_cert_id"].count().reset_index()
            .rename(columns={"revoked_cert_id": "new_revocations"})
        )

    return size_trend, rev_trend


def write_outputs(out_dir: Path, size_trend: pd.DataFrame, rev_trend: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    size_trend.to_csv(out_dir / "crl_size_trends.csv", index=False)
    rev_trend.to_csv(out_dir / "crl_revocation_churn.csv", index=False)

    if size_trend.empty:
        plt.figure()
        plt.savefig(out_dir / "crl_size_area.png", dpi=150, bbox_inches="tight")
        plt.close()
        return

    pivot = size_trend.pivot_table(
        index="date", columns="ca_subject_dn", values="revoked_count", aggfunc="max"
    ).fillna(0)
    pivot.sort_index(inplace=True)

    plt.figure(figsize=(10, 5))
    plt.stackplot(pivot.index, pivot.T, labels=pivot.columns)
    plt.legend(loc="upper left", fontsize="small", ncol=2)
    plt.xlabel("Date")
    plt.ylabel("Revoked certificates")
    plt.tight_layout()
    plt.savefig(out_dir / "crl_size_area.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    size_trend, rev_trend = compute_crl_dynamics(args.out_root)
    write_outputs(args.out_dir, size_trend, rev_trend)


if __name__ == "__main__":
    main()

