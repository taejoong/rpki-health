from __future__ import annotations


"""A09 – ROA maxLength risk quantification.
Evaluates ROAs for overly permissive ``maxLength`` settings and aggregates risk
metrics per ASN and issuing CA.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``roa_prefixes`` and ``roa_objects`` for prefix/maxLength/ASN data.
  - ``object_signatures`` and ``certs`` for signer attribution.
* CLI arguments ``--out-root`` (one or more warehouse roots), ``--out-dir``, optional ``--threshold``.
Outputs
-------
* ``roa_maxlength_by_asn.csv`` – ASN-level counts above the threshold.
* ``roa_maxlength_by_ca.csv`` – CA aggregates of permissive ROAs.
* ``roa_maxlength_cdf.png`` – distribution of ``maxLength - prefix_length``.
The script uses only warehouse metadata and requires no KV or CAS access.

Examples
--------
Inspect APNIC VRPs for excessive maxLength values::

    python analysis/a09_roa_maxlength_risk.py \
        --out-root warehouse-apnic --out-dir outputs/a09

Combine APNIC and RIPE NCC feeds for a broader perspective::

    python analysis/a09_roa_maxlength_risk.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a09_multi

Reuse the same ``warehouse-<tal>`` directories if a wrapper forwards
``--warehouse`` arguments.
"""


import argparse
import ipaddress
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ROA maxLength risk analysis")
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (per TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--threshold", type=int, default=8)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def prefix_length(prefix: str) -> int:
    try:
        network = ipaddress.ip_network(prefix, strict=False)
        return int(network.prefixlen)
    except Exception:
        return -1


def compute_risk(out_root: Sequence[Path]) -> pd.DataFrame:
    prefixes = load_table(out_root, "roa_prefixes")
    roas = load_table(out_root, "roa_objects")
    signatures = load_table(out_root, "object_signatures")
    certs = load_table(out_root, "certs")

    if prefixes.empty:
        return pd.DataFrame(columns=["object_id", "asn", "afi", "prefix", "max_length", "delta", "signer_cert_id", "ca_subject_dn"])

    roas = roas[["object_id", "asn"]]
    sig = signatures[["object_id", "signer_cert_id"]]
    ca_map = certs.set_index("cert_id")["subject_dn"].to_dict() if not certs.empty else {}

    merged = prefixes.merge(roas, on="object_id", how="left").merge(sig, on="object_id", how="left")
    # Convert prefixes to integer lengths so we can compute the margin easily.
    merged["prefix_len"] = merged["prefix"].astype(str).map(prefix_length)
    merged["delta"] = merged["max_length"] - merged["prefix_len"]
    merged = merged[(merged["prefix_len"] >= 0) & (merged["delta"] >= 0)]
    merged["ca_subject_dn"] = merged["signer_cert_id"].map(ca_map)
    return merged


def write_outputs(out_dir: Path, df: pd.DataFrame, threshold: int) -> None:
    ensure_out_dir(out_dir)
    df.to_csv(out_dir / "roa_maxlength_detail.csv", index=False)

    if df.empty:
        plt.figure()
        plt.savefig(out_dir / "roa_maxlength_cdf.png", dpi=150, bbox_inches="tight")
        plt.close()
        pd.DataFrame(columns=["asn", "risky_count", "total", "share"]).to_csv(
            out_dir / "roa_maxlength_by_asn.csv", index=False
        )
        pd.DataFrame(columns=["ca_subject_dn", "risky_count", "total", "share"]).to_csv(
            out_dir / "roa_maxlength_by_ca.csv", index=False
        )
        return

    values = df["delta"].dropna().sort_values()
    plt.figure(figsize=(6, 4))
    if not values.empty:
        y = np.linspace(0, 1, len(values), endpoint=False)
        plt.plot(values.values, y)
    plt.xlabel("maxLength - prefix length")
    plt.ylabel("CDF")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "roa_maxlength_cdf.png", dpi=150)
    plt.close()

    def summarise(group: pd.DataFrame, key_col: str) -> pd.DataFrame:
        risky = group[group["delta"] >= threshold]
        summary = (
            group.groupby(key_col)
            .agg(total=("object_id", "count"))
            .join(
                risky.groupby(key_col).agg(risky_count=("object_id", "count")),
                how="left",
            )
            .fillna(0)
            .reset_index()
        )
        summary["share"] = summary["risky_count"] / summary["total"].replace(0, np.nan)
        summary["share"].fillna(0, inplace=True)
        return summary

    summarise(df, "asn").to_csv(out_dir / "roa_maxlength_by_asn.csv", index=False)
    summarise(df, "ca_subject_dn").to_csv(out_dir / "roa_maxlength_by_ca.csv", index=False)


def main() -> None:
    args = parse_args()
    df = compute_risk(args.out_root)
    write_outputs(args.out_dir, df, args.threshold)


if __name__ == "__main__":
    main()

