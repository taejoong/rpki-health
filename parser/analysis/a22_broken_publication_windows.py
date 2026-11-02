from __future__ import annotations

"""A22 – Broken publication window detector.

Flags snapshots where manifests or CRLs are stale relative to the snapshot time
or where objects appear to have future-dated signatures.

Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``manifests`` and ``crls`` for ``this_update``/``next_update`` windows.
  - ``object_signatures`` for optional future-dated checks.
* Snapshot timestamps via ``list_snapshots`` and ``ts_label_to_iso``.
* CLI arguments ``--out-root`` and ``--out-dir``.

Outputs
-------
* ``publication_window_issues.csv`` – per-snapshot issues with context.
  No KV or CAS dependencies are required.

Examples
--------
Audit two TALs stored in ``warehouse-apnic`` and ``warehouse-ripencc``::

    python analysis/a22_broken_publication_windows.py \
        --out-root warehouse-apnic warehouse-ripencc \
        --out-dir analysis-output/a22

Focus on a single TAL while writing results to a teaching workspace::

    python analysis/a22_broken_publication_windows.py \
        --out-root warehouse-arin \
        --out-dir classroom/a22
"""


import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from analysis.analysis_common import list_snapshots, load_table, ts_label_to_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Broken publication window detector")
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
    manifests = load_table(out_root, "manifests")
    crls = load_table(out_root, "crls")
    signatures = load_table(out_root, "object_signatures")

    snapshots = list_snapshots(out_root)
    results = []

    signing_time = {}
    if not signatures.empty:
        # Keep only valid signing timestamps and convert them to pandas
        # datetimes for easy comparison against snapshot times.
        sig = signatures[signatures["signing_time"].notna()][["object_id", "signing_time"]]
        sig["signing_time"] = pd.to_datetime(sig["signing_time"], utc=True, errors="coerce")
        signing_time = sig.set_index("object_id")["signing_time"].to_dict()

    if not manifests.empty:
        manifests = manifests.copy()
        manifests["next_update_dt"] = pd.to_datetime(
            manifests["next_update"], utc=True, errors="coerce"
        )

    if not crls.empty:
        crls = crls.copy()
        crls["next_update_dt"] = pd.to_datetime(crls["next_update"], utc=True, errors="coerce")

    for ts_label in snapshots:
        # Convert the dataset label to a timezone-aware datetime so comparisons
        # against publication windows are reliable.
        ts = pd.to_datetime(ts_label_to_iso(ts_label), utc=True, errors="coerce")
        if ts is pd.NaT:
            continue

        if not manifests.empty:
            stale = manifests[manifests["next_update_dt"].notna() & (manifests["next_update_dt"] < ts)]
            for row in stale.itertuples(index=False):
                results.append(
                    {
                        "dataset_ts": ts,
                        "object_id": row.object_id,
                        "issue": "manifest_stale",
                    }
                )

        if not crls.empty:
            stale = crls[crls["next_update_dt"].notna() & (crls["next_update_dt"] < ts)]
            for row in stale.itertuples(index=False):
                results.append(
                    {
                        "dataset_ts": ts,
                        "object_id": row.crl_id,
                        "issue": "crl_stale",
                    }
                )

        if signing_time:
            # Highlight objects with signatures issued after the dataset was
            # captured, which would indicate a clocking or publication issue.
            for object_id, signed in signing_time.items():
                if pd.notna(signed) and signed > ts:
                    results.append(
                        {
                            "dataset_ts": ts,
                            "object_id": object_id,
                            "issue": "future_dated_signature",
                        }
                    )

    return pd.DataFrame(results)


def main() -> None:
    args = parse_args()
    ensure_out_dir(args.out_dir)
    df = detect_issues(args.out_root)
    df.to_csv(args.out_dir / "publication_window_issues.csv", index=False)


if __name__ == "__main__":
    main()

