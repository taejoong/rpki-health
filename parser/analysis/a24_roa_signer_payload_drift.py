from __future__ import annotations

"""A24 – ROA signer and payload drift audit.

Correlates ROA payload changes with signer EE certificate renewals to determine
whether updates align with signer lifecycle events.

Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``roas_signers_snapshot`` preferred; otherwise ``roa_objects`` plus
    reconstructed membership state.
  - ``object_signatures`` and ``certs`` for signer metadata and validity windows.
* Snapshot memberships via ``list_snapshots``/``load_membership`` when
  reconstruction is needed.
* CLI arguments ``--out-root``, ``--out-dir``, optional ``--kv``.

Outputs
-------
* ``roa_drift_audit.csv`` – ROA histories annotated with payload drift vs.
  signer validity correlations.
  The audit uses only warehouse tables and optional membership data; CAS is not
  required.

Examples
--------
Audit ROA drift across APNIC and RIPE NCC TALs with precomputed snapshots::

    python analysis/a24_roa_signer_payload_drift.py \
        --out-root warehouse-apnic warehouse-ripencc \
        --out-dir analysis-output/a24

Investigate a single TAL using reconstructed membership history::

    python analysis/a24_roa_signer_payload_drift.py \
        --out-root warehouse-arin \
        --out-dir classroom/a24
"""


import argparse
from pathlib import Path
from typing import List, Sequence

import pandas as pd

from analysis.analysis_common import list_snapshots, load_membership, load_table, ts_label_to_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ROA signer and payload drift audit")
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (per TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--kv", type=Path, default=None)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_roa_history(out_root: Sequence[Path]) -> pd.DataFrame:
    snapshot = load_table(out_root, "roas_signers_snapshot")
    if not snapshot.empty:
        def _normalize_ts(value: object) -> str:
            if isinstance(value, str) and " " in value:
                try:
                    return ts_label_to_iso(value)
                except ValueError:
                    return value
            return str(value)

        snapshot["dataset_ts"] = pd.to_datetime(
            snapshot["dataset_ts"].apply(_normalize_ts), utc=True, errors="coerce"
        )
        return snapshot[["dataset_ts", "object_id", "semantic_hash", "signer_cert_id"]]

    roas = load_table(out_root, "roa_objects")
    signatures = load_table(out_root, "object_signatures")
    if roas.empty:
        return pd.DataFrame(columns=["dataset_ts", "object_id", "semantic_hash", "signer_cert_id"])

    merged = roas[["object_id", "semantic_hash"]].merge(
        signatures[["object_id", "signer_cert_id"]], on="object_id", how="left"
    )
    records: List[pd.DataFrame] = []
    for ts_label in list_snapshots(out_root):
        # Rehydrate a snapshot-specific ROA view whenever the consolidated table
        # is unavailable so the resulting history respects publication timing.
        members = load_membership(out_root, ts_label)
        if not members:
            continue
        subset = merged[merged["object_id"].isin(members)].copy()
        if subset.empty:
            continue
        # Align timestamp representation with the rest of the analysis suite and
        # reuse pandas' datetime handling for later comparisons.
        subset["dataset_ts"] = pd.to_datetime(ts_label_to_iso(ts_label), utc=True, errors="coerce")
        records.append(subset)
    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


def compute_drift(out_root: Sequence[Path]) -> pd.DataFrame:
    history = load_roa_history(out_root)
    certs = load_table(out_root, "certs")

    if history.empty:
        return pd.DataFrame(columns=["object_id", "event_ts", "signer_cert_id", "change_type", "ca_subject_dn"])

    certs = certs[["cert_id", "subject_dn", "not_before", "not_after"]]
    certs["not_before"] = pd.to_datetime(certs["not_before"], utc=True, errors="coerce")
    certs["not_after"] = pd.to_datetime(certs["not_after"], utc=True, errors="coerce")
    cert_map = certs.set_index("cert_id").to_dict(orient="index")

    history = history.sort_values(["object_id", "dataset_ts"])
    records = []
    for object_id, group in history.groupby("object_id"):
        prev_hash = None
        prev_signer = None
        for row in group.itertuples(index=False):
            if prev_hash is None:
                prev_hash = row.semantic_hash
                prev_signer = row.signer_cert_id
                continue
            if row.semantic_hash == prev_hash:
                continue
            # Pull the signer certificate metadata from the cache so we can
            # classify the change relative to the old certificate's validity.
            cert_info = cert_map.get(prev_signer, {})
            change_type = "aligned_with_renewal"
            if row.signer_cert_id == prev_signer:
                not_after = cert_info.get("not_after")
                if not_after is None or pd.isna(not_after) or row.dataset_ts <= not_after:
                    change_type = "mid_period_change"
            ca_subject = cert_info.get("subject_dn")
            records.append(
                {
                    "object_id": object_id,
                    "event_ts": row.dataset_ts,
                    "signer_cert_id": prev_signer,
                    "change_type": change_type,
                    "ca_subject_dn": ca_subject,
                }
            )
            prev_hash = row.semantic_hash
            prev_signer = row.signer_cert_id

    return pd.DataFrame(records)


def write_outputs(out_dir: Path, df: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    df.to_csv(out_dir / "roa_drift_audit.csv", index=False)

    if df.empty:
        return

    summary = df.groupby(["ca_subject_dn", "change_type"]).size().reset_index(name="count")
    summary.to_csv(out_dir / "roa_drift_summary.csv", index=False)


def main() -> None:
    args = parse_args()
    df = compute_drift(args.out_root)
    write_outputs(args.out_dir, df)


if __name__ == "__main__":
    main()

