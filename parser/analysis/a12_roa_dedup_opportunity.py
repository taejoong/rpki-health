from __future__ import annotations

"""A12 – ROA object deduplication opportunity analysis.
Identifies ROA payloads that are published multiple times and estimates the
potential gains from deduplicating identical semantic content.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``roa_objects`` for semantic hashes and ASNs.
  - ``object_signatures`` and ``certs`` for signer attribution.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``roa_dedup.csv`` – groups of duplicate semantic hashes with signer lists.
* ``roa_dedup_by_ca.csv`` – CA-level ranking of deduplication opportunity.
The analysis relies exclusively on warehouse tables; KV/CAS are not required.

Examples
--------
Spot duplicates inside the APNIC warehouse::

    python analysis/a12_roa_dedup_opportunity.py \
        --out-root warehouse-apnic --out-dir outputs/a12

Aggregate opportunity across APNIC and RIPE NCC::

    python analysis/a12_roa_dedup_opportunity.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a12_multi

Forward any ``--warehouse warehouse-apnic`` parameters to ``--out-root`` here.
"""

import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ROA deduplication opportunity analysis")
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


def compute_dedup(out_root: Sequence[Path]) -> tuple[pd.DataFrame, float]:
    roas = load_table(out_root, "roa_objects")
    signatures = load_table(out_root, "object_signatures")
    certs = load_table(out_root, "certs")

    if roas.empty:
        return pd.DataFrame(columns=["semantic_hash", "object_ids", "signers", "asn", "count"]), 0.0

    merged = roas.merge(signatures[["object_id", "signer_cert_id"]], on="object_id", how="left")
    ca_map = certs.set_index("cert_id")["subject_dn"].to_dict() if not certs.empty else {}

    groups = []
    for semantic_hash, group in merged.groupby("semantic_hash"):
        # Collect every publication that shares the same semantic payload.
        object_ids = sorted(group["object_id"].unique())
        signers = sorted(group["signer_cert_id"].dropna().unique())
        groups.append(
            {
                "semantic_hash": semantic_hash,
                "object_ids": ";".join(map(str, object_ids)),
                "signers": ";".join(map(str, signers)),
                "asn": ";".join(map(str, sorted(group["asn"].dropna().unique()))),
                "count": len(group),
            }
        )

    df = pd.DataFrame(groups)
    unique_hashes = df["semantic_hash"].nunique()
    total_roas = len(roas)
    redundancy_ratio = 1 - (unique_hashes / total_roas if total_roas else 0.0)

    if not df.empty:
        df["signer_subjects"] = df["signers"].apply(
            lambda ids: ";".join(
                sorted(
                    {
                        ca_map.get(int(cert_id), str(cert_id))
                        for cert_id in ids.split(";")
                        if cert_id
                    }
                )
            )
        )

    return df, redundancy_ratio


def write_outputs(out_dir: Path, df: pd.DataFrame, redundancy_ratio: float) -> None:
    ensure_out_dir(out_dir)
    df.to_csv(out_dir / "roa_dedup.csv", index=False)

    summary = pd.DataFrame([{"redundancy_ratio": redundancy_ratio}])
    summary.to_csv(out_dir / "roa_dedup_summary.csv", index=False)

    if df.empty:
        pd.DataFrame(columns=["ca_subject_dn", "duplicate_objects"]).to_csv(
            out_dir / "roa_dedup_by_ca.csv", index=False
        )
        return

    exploded = df.assign(signer_subjects=df["signer_subjects"].str.split(";"))
    exploded = exploded.explode("signer_subjects")
    ca_summary = (
        exploded.groupby("signer_subjects")["count"].sum().reset_index().rename(
            columns={"signer_subjects": "ca_subject_dn", "count": "duplicate_objects"}
        )
    )
    ca_summary.to_csv(out_dir / "roa_dedup_by_ca.csv", index=False)


def main() -> None:
    args = parse_args()
    df, ratio = compute_dedup(args.out_root)
    write_outputs(args.out_dir, df, ratio)


if __name__ == "__main__":
    main()

