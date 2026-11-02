from __future__ import annotations


"""A03 – Certificate authority rollover hygiene checks.
Evaluate CA key rollovers to ensure supporting artefacts (CRLs and manifests)
are published promptly when a new CA certificate becomes active.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``certs`` for CA certificate metadata and key identifiers.
  - ``crls`` for issuance timestamps tied to ``issuer_cert_id``.
  - ``object_signatures`` (manifests) for manifest publication timings.
* CLI parameters ``--out-root`` (one or more warehouse roots) and ``--out-dir`` for results.
Outputs
-------
* ``ca_rollovers.csv`` summarising predecessor/successor certificate ids,
  rollover intervals, first CRL/manifest delays, and a boolean hygiene flag.
No CAS or KV data is required; all timestamps are treated as UTC.

Examples
--------
Audit APNIC CA rollovers and write a CSV beside the script::

    python analysis/a03_ca_rollover_hygiene.py \
        --out-root warehouse-apnic --out-dir outputs/a03

Compare APNIC and RIPE NCC rollovers in one pass::

    python analysis/a03_ca_rollover_hygiene.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a03_multi

Any orchestration layer using ``--warehouse warehouse-apnic`` should forward the
same values here via ``--out-root``.
"""


import argparse
from datetime import timedelta
from pathlib import Path
from typing import Sequence

import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CA rollover hygiene checks")
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


def compute_first_event_delays(
    certs: pd.DataFrame, crls: pd.DataFrame, signatures: pd.DataFrame
) -> pd.DataFrame:
    """Measure how quickly supporting artefacts arrive after a new CA goes live."""
    if certs.empty:
        return pd.DataFrame(
            columns=[
                "subject_dn",
                "before_cert_id",
                "after_cert_id",
                "rollover_time",
                "first_crl_delay_days",
                "first_mft_delay_days",
                "flagged",
            ]
        )

    ca_certs = certs[certs.get("is_ca_flag") == True].copy()  # noqa: E712
    ca_certs["not_before"] = pd.to_datetime(ca_certs["not_before"], utc=True, errors="coerce")

    # Pre-compute the first CRL issuance per CA for quick lookups below.
    crl_map = (
        crls.groupby("issuer_cert_id")["this_update"].min().to_dict()
        if not crls.empty
        else {}
    )
    sig_mft = signatures[signatures.get("object_type") == "MFT"] if not signatures.empty else pd.DataFrame()
    # Identify the first manifest signed by each CA to detect publication lag.
    mft_map = (
        sig_mft.groupby("signer_cert_id")["signing_time"].min().to_dict()
        if not sig_mft.empty
        else {}
    )

    records = []
    for subject, group in ca_certs.groupby("subject_dn"):
        group = group.sort_values("not_before")
        prev_row = None
        for row in group.itertuples(index=False):
            if prev_row is None:
                prev_row = row
                continue
            if getattr(row, "spki_sha256", None) == getattr(prev_row, "spki_sha256", None):
                prev_row = row
                continue

            rollover_time = getattr(row, "not_before")
            prev_crl_time = crl_map.get(getattr(row, "cert_id"))
            prev_mft_time = mft_map.get(getattr(row, "cert_id"))

            if prev_crl_time is not None:
                prev_crl_time = pd.to_datetime(prev_crl_time, utc=True, errors="coerce")
            if prev_mft_time is not None:
                prev_mft_time = pd.to_datetime(prev_mft_time, utc=True, errors="coerce")

            crl_delay = None
            if rollover_time is not None and pd.notna(rollover_time) and prev_crl_time is not None:
                crl_delay = (prev_crl_time - rollover_time).total_seconds() / 86400.0

            mft_delay = None
            if rollover_time is not None and pd.notna(rollover_time) and prev_mft_time is not None:
                mft_delay = (prev_mft_time - rollover_time).total_seconds() / 86400.0

            flagged = False
            for value in [crl_delay, mft_delay]:
                if value is not None and value > 1:
                    flagged = True
                    break

            records.append(
                {
                    "subject_dn": subject,
                    "before_cert_id": getattr(prev_row, "cert_id"),
                    "after_cert_id": getattr(row, "cert_id"),
                    "rollover_time": rollover_time,
                    "first_crl_delay_days": crl_delay,
                    "first_mft_delay_days": mft_delay,
                    "flagged": flagged,
                }
            )
            prev_row = row

    return pd.DataFrame.from_records(records)


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir
    ensure_out_dir(out_dir)

    certs = load_table(out_root, "certs")
    crls = load_table(out_root, "crls")
    signatures = load_table(out_root, "object_signatures")

    result = compute_first_event_delays(certs, crls, signatures)
    result.to_csv(out_dir / "ca_rollovers.csv", index=False)


if __name__ == "__main__":
    main()

