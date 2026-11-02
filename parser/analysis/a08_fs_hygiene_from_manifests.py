from __future__ import annotations


"""A08 – File system hygiene insights derived from manifest history.
Analyses manifest listings to identify filenames that rapidly change hashes and
potential rename oscillations that may indicate publication hygiene issues.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``manifests`` for ordering by ``this_update``.
  - ``manifests_entries`` for filename/hash pairs.
* CLI arguments ``--out-root`` (one or more warehouse roots), ``--out-dir``, and
  ``--volatility-threshold``.
Outputs
-------
* ``fs_hygiene_flags.csv`` enumerating filenames or rename clusters with
  supporting metrics and observation windows.
This analysis uses only manifest metadata; no KV or CAS data is needed.

Examples
--------
Flag volatile filenames within APNIC data::

    python analysis/a08_fs_hygiene_from_manifests.py \
        --out-root warehouse-apnic --out-dir outputs/a08

Contrast APNIC and RIPE NCC manifest behaviour::

    python analysis/a08_fs_hygiene_from_manifests.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a08_multi

If using a wrapper that passes ``--warehouse warehouse-apnic``, reuse those
values for ``--out-root``.
"""


import argparse
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="File system hygiene derived from manifests")
    parser.add_argument(
        "--out-root",
        type=Path,
        nargs="+",
        required=True,
        help="One or more warehouse roots (per TAL)",
    )
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--volatility-threshold", type=int, default=3)
    return parser.parse_args()


def ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def detect_volatility(entries: pd.DataFrame, threshold: int) -> List[dict]:
    """Highlight filenames whose hashes churn faster than the threshold allows."""
    flags: List[dict] = []
    if entries.empty:
        return flags

    entries = entries.copy()
    # Order updates chronologically so rolling windows reflect true history.
    entries["this_update"] = pd.to_datetime(entries["this_update"], utc=True, errors="coerce")
    entries = entries.dropna(subset=["this_update"])
    if entries.empty:
        return flags

    for filename, group in entries.groupby("filename"):
        if group["file_hash"].nunique() <= 1:
            continue
        group = group.sort_values("this_update").set_index("this_update")
        window_unique = (
            group.groupby(pd.Grouper(freq="7D"))["file_hash"].nunique().max()
        )
        if window_unique is not None and window_unique > threshold:
            flags.append(
                {
                    "issue_type": "hash_volatility",
                    "filename": filename,
                    "details": f"{window_unique} unique hashes within 7 days",
                }
            )
    return flags


def detect_renames(entries: pd.DataFrame) -> List[dict]:
    """Search for clusters of files that appear to swap filenames over time."""
    flags: List[dict] = []
    if entries.empty:
        return flags

    hash_groups: Dict[str, set] = {}
    for row in entries.itertuples(index=False):
        hash_groups.setdefault(str(row.file_hash), set()).add(str(row.filename))

    for file_hash, filenames in hash_groups.items():
        if len(filenames) < 2:
            continue
        flags.append(
            {
                "issue_type": "possible_rename",
                "filename": ", ".join(sorted(filenames)),
                "details": f"Shared hash {file_hash} across {len(filenames)} filenames",
            }
        )
    return flags


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir
    ensure_out_dir(out_dir)

    manifests = load_table(out_root, "manifests")[["object_id", "this_update"]]
    entries = load_table(out_root, "manifests_entries")

    if manifests.empty or entries.empty:
        pd.DataFrame(columns=["issue_type", "filename", "details"]).to_csv(
            out_dir / "fs_hygiene_flags.csv", index=False
        )
        return

    merged = entries.merge(manifests, on="object_id", how="left")
    flags = detect_volatility(merged, args.volatility_threshold)
    flags.extend(detect_renames(merged))

    pd.DataFrame(flags).to_csv(out_dir / "fs_hygiene_flags.csv", index=False)


if __name__ == "__main__":
    main()

