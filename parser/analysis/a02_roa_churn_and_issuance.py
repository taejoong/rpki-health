from __future__ import annotations

"""A02 – ROA churn and issuance rate trends across publication points.
This analysis tracks ROA lifecycle events (adds, updates, removals) across the
snapshot timeline and estimates per-CA issuance cadence.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``roas_signers_snapshot`` when available; otherwise ``roa_objects`` and
    ``object_signatures`` combined with membership files.
  - ``certs`` for mapping signer certificates to CA identities.
* Snapshot memberships loaded through ``list_snapshots``/``load_membership``
  when ``roas_signers_snapshot`` is absent.
* Command line arguments ``--out-root`` (one or more warehouse roots) and
  ``--out-dir`` specifying warehouse location and output directory. ``--kv`` is
  accepted for compatibility but the current implementation does not require it.
Outputs
-------
* ``churn_daily.csv`` – aggregate adds/updates/removals per calendar day.
* ``churn_daily_by_ca.csv`` – daily churn series per CA subject DN.
* ``churn_heatmap_weekly.png`` – weekly churn intensity heatmap.
* ``mean_inter_reissue_by_ca.csv`` – mean inter-update intervals per signer CA.
The script tolerates missing ``roas_signers_snapshot`` by reconstructing the
snapshot state from membership files. CAS access is not required.

Examples
--------
Calculate churn for APNIC snapshots only::

    python analysis/a02_roa_churn_and_issuance.py \
        --out-root warehouse-apnic --out-dir outputs/a02

Compare two TALs and store combined plots::

    python analysis/a02_roa_churn_and_issuance.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a02_multi

If invoking from a wrapper that accepts ``--warehouse warehouse-apnic``, pass
the same values to ``--out-root``.
"""

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.analysis_common import (
    list_snapshots,
    load_membership,
    load_table,
    ts_label_to_iso,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ROA churn and issuance analysis")
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


def load_roa_snapshot_records(out_root: Sequence[Path]) -> pd.DataFrame:
    snap = load_table(out_root, "roas_signers_snapshot")
    if not snap.empty:
        # Snapshot tables already encode the membership; normalise their labels.
        snap["dataset_ts"] = pd.to_datetime(
            snap["dataset_ts"].map(ts_label_to_iso), utc=True, errors="coerce"
        )
        return snap[["dataset_ts", "object_id", "asn", "semantic_hash", "signer_cert_id"]]

    roa_objects = load_table(out_root, "roa_objects")
    if roa_objects.empty:
        return pd.DataFrame(columns=["dataset_ts", "object_id", "asn", "semantic_hash", "signer_cert_id"])

    signatures = load_table(out_root, "object_signatures")[["object_id", "signer_cert_id"]]
    roa_objects = roa_objects.merge(signatures, on="object_id", how="left")

    records: List[pd.DataFrame] = []
    for ts_label in list_snapshots(out_root):
        # Fall back to membership files when the consolidated snapshot is absent.
        members = load_membership(out_root, ts_label)
        if not members:
            continue
        member_set = set(members)
        present = roa_objects[roa_objects["object_id"].isin(member_set)].copy()
        if present.empty:
            continue
        # Tag each reconstructed snapshot with its dataset timestamp for grouping.
        present["dataset_ts"] = pd.to_datetime(
            ts_label_to_iso(ts_label), utc=True, errors="coerce"
        )
        records.append(present[["dataset_ts", "object_id", "asn", "semantic_hash", "signer_cert_id"]])

    if not records:
        return pd.DataFrame(columns=["dataset_ts", "object_id", "asn", "semantic_hash", "signer_cert_id"])
    return pd.concat(records, ignore_index=True)


def derive_churn(records: pd.DataFrame, certs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ensure_cols = ["dataset_ts", "object_id", "asn", "semantic_hash", "signer_cert_id"]
    if records.empty:
        empty_events = pd.DataFrame(columns=["dataset_ts", "event", "signer_cert_id"])
        return empty_events, pd.DataFrame(), pd.DataFrame()

    records = records[ensure_cols].drop_duplicates()
    records = records.sort_values("dataset_ts")

    events: List[dict] = []
    prev_state: Dict[int, dict] = {}

    cert_subject = {
        row["cert_id"]: row.get("subject_dn")
        for _, row in certs.iterrows()
    }

    for ts, group in records.groupby("dataset_ts"):
        current_state = {
            int(row.object_id): {
                "asn": row.asn,
                "semantic_hash": row.semantic_hash,
                "signer_cert_id": row.signer_cert_id,
            }
            for row in group.itertuples(index=False)
        }

        added_ids = sorted(set(current_state.keys()) - set(prev_state.keys()))
        removed_ids = sorted(set(prev_state.keys()) - set(current_state.keys()))

        for obj_id in added_ids:
            signer = current_state[obj_id]["signer_cert_id"]
            events.append({
                "dataset_ts": ts,
                "event": "add",
                "signer_cert_id": signer,
                "ca_subject_dn": cert_subject.get(signer),
            })

        for obj_id in removed_ids:
            signer = prev_state[obj_id]["signer_cert_id"]
            events.append({
                "dataset_ts": ts,
                "event": "removal",
                "signer_cert_id": signer,
                "ca_subject_dn": cert_subject.get(signer),
            })

        overlap_ids = set(current_state.keys()) & set(prev_state.keys())
        for obj_id in overlap_ids:
            curr = current_state[obj_id]
            prev = prev_state[obj_id]
            if curr["asn"] != prev["asn"] or curr["semantic_hash"] != prev["semantic_hash"]:
                signer = curr["signer_cert_id"] or prev["signer_cert_id"]
                events.append({
                    "dataset_ts": ts,
                    "event": "update",
                    "signer_cert_id": signer,
                    "ca_subject_dn": cert_subject.get(signer),
                })

        prev_state = current_state

    if not events:
        return pd.DataFrame(columns=["dataset_ts", "event", "signer_cert_id", "ca_subject_dn"]), pd.DataFrame(), pd.DataFrame()

    events_df = pd.DataFrame(events)
    events_df["date"] = events_df["dataset_ts"].dt.date

    daily = (
        events_df.groupby(["date", "event"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for col in ["add", "update", "removal"]:
        if col not in daily.columns:
            daily[col] = 0
    daily = daily.rename(columns={"add": "adds", "update": "updates", "removal": "removals"})

    daily_by_ca = (
        events_df.groupby(["date", "ca_subject_dn", "event"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    for col in ["add", "update", "removal"]:
        if col not in daily_by_ca.columns:
            daily_by_ca[col] = 0
    daily_by_ca = daily_by_ca.rename(
        columns={"add": "adds", "update": "updates", "removal": "removals"}
    )

    return events_df, daily, daily_by_ca


def compute_mean_inter_reissue(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(columns=["signer_cert_id", "mean_reissue_days"])

    update_events = events_df[events_df["event"] == "update"].copy()
    if update_events.empty:
        return pd.DataFrame(columns=["signer_cert_id", "mean_reissue_days"])

    rows = []
    for signer, group in update_events.groupby("signer_cert_id"):
        times = group["dataset_ts"].sort_values()
        if len(times) < 2:
            continue
        diffs = times.diff().dropna().dt.total_seconds().div(86400.0)
        if diffs.empty:
            continue
        rows.append({"signer_cert_id": signer, "mean_reissue_days": diffs.mean()})

    return pd.DataFrame(rows)


def write_outputs(out_dir: Path, daily: pd.DataFrame, daily_by_ca: pd.DataFrame, mean_reissue: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    daily.to_csv(out_dir / "churn_daily.csv", index=False)
    daily_by_ca.to_csv(out_dir / "churn_daily_by_ca.csv", index=False)
    mean_reissue.to_csv(out_dir / "mean_inter_reissue_by_ca.csv", index=False)

    if daily.empty:
        plt.figure()
        plt.savefig(out_dir / "churn_heatmap_weekly.png", dpi=150, bbox_inches="tight")
        plt.close()
        return

    daily_series = daily.copy()
    daily_series["date"] = pd.to_datetime(daily_series["date"], errors="coerce")
    daily_series["week"] = daily_series["date"].dt.isocalendar().week
    weekly = daily_series.groupby("week")["adds"].sum()

    plt.figure(figsize=(8, 3))
    plt.imshow(weekly.to_frame().T, aspect="auto", cmap="viridis")
    plt.yticks([])
    plt.xticks(range(len(weekly)), weekly.index, rotation=45)
    plt.colorbar(label="Adds per week")
    plt.tight_layout()
    plt.savefig(out_dir / "churn_heatmap_weekly.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir

    certs = load_table(out_root, "certs")
    records = load_roa_snapshot_records(out_root)
    events_df, daily, daily_by_ca = derive_churn(records, certs)
    mean_reissue = compute_mean_inter_reissue(events_df)
    write_outputs(out_dir, daily, daily_by_ca, mean_reissue)


if __name__ == "__main__":
    main()

