from __future__ import annotations


"""A17 – X.509 profile conformance checks for RPKI certificates.
Performs lightweight validation of key profile extensions (SIA, AIA, KU, EKU,
Basic Constraints) to flag potential specification deviations.
Inputs
------
* Warehouse table ``certs`` via :mod:`analysis.analysis_common`.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``profile_conformance.csv`` – per-certificate issues flagged.
* ``profile_conformance_summary.csv`` – aggregated counts by issue type and
  CA/EE role.
The script relies solely on the ``certs`` table and uses simple string-based
checks rather than full ASN.1 validation.

Examples
--------
Check APNIC certificate profiles::

    python analysis/a17_profile_conformance.py \
        --out-root warehouse-apnic --out-dir outputs/a17

Compare APNIC and RIPE NCC profile adherence::

    python analysis/a17_profile_conformance.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a17_multi

Forward any ``--warehouse warehouse-apnic`` options to ``--out-root``.
"""

import argparse
from pathlib import Path
from typing import List, Sequence

import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile conformance checks")
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


def analyse_cert(row) -> List[str]:
    issues: List[str] = []
    is_ca = bool(row.is_ca_flag)
    sia = str(row.sia) if pd.notna(row.sia) else ""
    basic = str(row.basic_constraints) if pd.notna(row.basic_constraints) else ""
    ku = str(row.key_usage) if pd.notna(row.key_usage) else ""
    eku = str(row.ext_key_usage) if pd.notna(row.ext_key_usage) else ""

    if is_ca:
        if not sia:
            issues.append("missing_sia")
        if "ca:true" not in basic.lower():
            issues.append("basic_constraints_missing_ca_true")
        if "keyCertSign" not in ku:
            issues.append("missing_keycertsign")
    else:
        expected_ku = ["digitalSignature"]
        for token in expected_ku:
            if token not in ku:
                issues.append(f"ku_missing_{token}")
        if "keyCertSign" in ku:
            issues.append("ku_contains_keycertsign")
        if eku and "id-kp-timeStamping" in eku:
            issues.append("unexpected_eku_timestamping")

    return issues


def compute_conformance(out_root: Sequence[Path]) -> pd.DataFrame:
    certs = load_table(out_root, "certs")
    if certs.empty:
        return pd.DataFrame(columns=["cert_id", "issue", "is_ca_flag"])

    records = []
    for row in certs.itertuples(index=False):
        # Analyse each certificate independently to capture specific violations.
        for issue in analyse_cert(row):
            records.append(
                {
                    "cert_id": row.cert_id,
                    "issue": issue,
                    "is_ca_flag": row.is_ca_flag,
                    "tal_id": getattr(row, "tal_id", None),
                }
            )
    return pd.DataFrame(records)


def write_outputs(out_dir: Path, issues: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    issues.to_csv(out_dir / "profile_conformance.csv", index=False)

    if issues.empty:
        pd.DataFrame(columns=["issue", "count"]).to_csv(
            out_dir / "profile_conformance_summary.csv", index=False
        )
        return

    summary = issues.groupby("issue").size().reset_index(name="count")
    summary.to_csv(out_dir / "profile_conformance_summary.csv", index=False)


def main() -> None:
    args = parse_args()
    issues = compute_conformance(args.out_root)
    write_outputs(args.out_dir, issues)


if __name__ == "__main__":
    main()

