from __future__ import annotations


"""A20 – Trust anchor and repository comparisons.
Builds a scorecard of key metrics (certificate counts, ROAs, CRLs) grouped by
trust anchor (TAL) and repository identifier.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``certs`` for CA/EE counts and TAL/repo attribution.
  - ``roa_objects`` and ``crls`` for additional activity metrics.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``ta_repo_scorecard.csv`` – aggregated metrics per ``(tal_id, repo_id)``.
The analysis only depends on warehouse tables and needs no KV/CAS data.

Examples
--------
Summarise APNIC repository stats::

    python analysis/a20_ta_rir_comparisons.py \
        --out-root warehouse-apnic --out-dir outputs/a20

Compare APNIC and RIPE NCC scorecards::

    python analysis/a20_ta_rir_comparisons.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a20_multi

Reuse any ``--warehouse warehouse-apnic`` arguments here as ``--out-root``.
"""
import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TA and RIR comparisons")
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


def build_scorecard(out_root: Sequence[Path]) -> pd.DataFrame:
    certs = load_table(out_root, "certs")
    roas = load_table(out_root, "roa_objects")
    crls = load_table(out_root, "crls")

    if certs.empty:
        return pd.DataFrame(columns=["tal_id", "repo_id", "ca_certs", "ee_certs", "roa_objects", "crl_entries"])

    certs = certs[["tal_id", "repo_id", "is_ca_flag", "cert_id"]]
    ca_counts = (
        certs[certs["is_ca_flag"] == True]  # noqa: E712
        .groupby(["tal_id", "repo_id"])
        .size()
        .reset_index(name="ca_certs")
    )
    ee_counts = (
        certs[certs["is_ca_flag"] == False]  # noqa: E712
        .groupby(["tal_id", "repo_id"])
        .size()
        .reset_index(name="ee_certs")
    )

    scorecard = ca_counts.merge(ee_counts, on=["tal_id", "repo_id"], how="outer")

    if not roas.empty:
        # ROA totals provide a sense of routing object activity per repository.
        roa_counts = roas.groupby(["tal_id", "repo_id"]).size().reset_index(name="roa_objects")
        scorecard = scorecard.merge(roa_counts, on=["tal_id", "repo_id"], how="outer")

    if not crls.empty:
        crl_counts = crls.groupby(["tal_id", "repo_id"]).size().reset_index(name="crl_entries")
        scorecard = scorecard.merge(crl_counts, on=["tal_id", "repo_id"], how="outer")

    scorecard.fillna(0, inplace=True)
    return scorecard


def main() -> None:
    args = parse_args()
    out_dir: Path = args.out_dir
    ensure_out_dir(out_dir)
    scorecard = build_scorecard(args.out_root)
    scorecard.to_csv(out_dir / "ta_repo_scorecard.csv", index=False)


if __name__ == "__main__":
    main()

