from __future__ import annotations


"""A18 – Serial number and SKI hygiene analysis.
Detects duplicate certificate serials issued by the same CA and re-use of SKIs
across distinct subjects.
Inputs
------
* Warehouse table ``certs`` via :mod:`analysis.analysis_common`.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``serial_ski_issues.csv`` – rows describing detected serial collisions or SKI
  re-use, including issuer/subject context.
Only the ``certs`` table is needed; no KV or CAS dependencies exist.

Examples
--------
Review APNIC serial/SKI hygiene::

    python analysis/a18_serial_ski_hygiene.py \
        --out-root warehouse-apnic --out-dir outputs/a18

Compare APNIC and RIPE NCC irregularities::

    python analysis/a18_serial_ski_hygiene.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a18_multi

Translate any ``--warehouse warehouse-apnic`` options to ``--out-root`` here.
"""


import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serial and SKI hygiene analysis")
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


def detect_issues(out_root: Sequence[Path]) -> pd.DataFrame:
    certs = load_table(out_root, "certs")
    if certs.empty:
        return pd.DataFrame(columns=["issue_type", "issuer_dn", "subject_dn", "serial_dec", "ski"])

    records = []

    serial_group = certs.groupby("issuer_dn")
    for issuer, group in serial_group:
        # Serial collisions matter only within the same issuing CA.
        duplicated_serials = group[group["serial_dec"].duplicated(keep=False)]
        for row in duplicated_serials.itertuples(index=False):
            records.append(
                {
                    "issue_type": "serial_collision",
                    "issuer_dn": issuer,
                    "subject_dn": row.subject_dn,
                    "serial_dec": row.serial_dec,
                    "ski": row.ski,
                }
            )

    ski_group = certs.groupby("ski")
    for ski, group in ski_group:
        if pd.isna(ski) or len(group) < 2:
            continue
        # Multiple subjects sharing the same SKI suggests key reuse worth flagging.
        subjects = group["subject_dn"].unique()
        if len(subjects) < 2:
            continue
        for row in group.itertuples(index=False):
            records.append(
                {
                    "issue_type": "ski_reuse",
                    "issuer_dn": row.issuer_dn,
                    "subject_dn": row.subject_dn,
                    "serial_dec": row.serial_dec,
                    "ski": ski,
                }
            )

    return pd.DataFrame(records)


def main() -> None:
    args = parse_args()
    out_dir: Path = args.out_dir
    ensure_out_dir(out_dir)
    df = detect_issues(args.out_root)
    df.to_csv(out_dir / "serial_ski_issues.csv", index=False)


if __name__ == "__main__":
    main()

