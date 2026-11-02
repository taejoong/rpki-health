from __future__ import annotations


"""A10 – ROA overlap and conflict detection.
Identifies conflicting ROA authorisations where overlapping prefixes are routed
to different ASNs, assigning severity scores based on overlap depth.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``roa_objects`` for ASN relationships.
  - ``roa_prefixes`` for prefix/maxLength ranges.
  - ``object_signatures`` and ``certs`` for CA attribution.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``conflicts.csv`` – conflict records containing both ROA objects, affected
  prefixes, ASNs, CA identities, and severity scores.
The implementation uses in-memory interval processing and does not depend on
KV or CAS data.

Examples
--------
List conflicts for APNIC-issued ROAs::

    python analysis/a10_roa_conflicts.py \
        --out-root warehouse-apnic --out-dir outputs/a10

Compare APNIC and RIPE NCC conflict surfaces::

    python analysis/a10_roa_conflicts.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a10_multi

Feed any ``--warehouse warehouse-apnic`` arguments into this script as
``--out-root``.
"""


import argparse
import ipaddress
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

import pandas as pd

from analysis.analysis_common import load_table


@dataclass
class Interval:
    afi: str
    object_id: int
    asn: int
    prefix: str
    prefix_len: int
    max_length: int
    start: int
    end: int
    ca_subject_dn: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect conflicting ROA prefixes")
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


def build_intervals(out_root: Sequence[Path]) -> List[Interval]:
    prefixes = load_table(out_root, "roa_prefixes")
    roas = load_table(out_root, "roa_objects")[["object_id", "asn"]]
    signatures = load_table(out_root, "object_signatures")[["object_id", "signer_cert_id"]]
    certs = load_table(out_root, "certs")[["cert_id", "subject_dn"]]

    if prefixes.empty:
        return []

    merged = prefixes.merge(roas, on="object_id", how="left").merge(signatures, on="object_id", how="left")
    ca_map = certs.set_index("cert_id")["subject_dn"].to_dict() if not certs.empty else {}

    intervals: List[Interval] = []
    for row in merged.itertuples(index=False):
        # Ignore malformed prefixes so they do not poison the interval set.
        try:
            network = ipaddress.ip_network(row.prefix, strict=False)
        except Exception:
            continue
        prefix_len = network.prefixlen
        start = int(network.network_address)
        end = int(network.broadcast_address)
        afi = str(row.afi)
        intervals.append(
            Interval(
                afi=afi,
                object_id=int(row.object_id),
                asn=int(row.asn) if pd.notna(row.asn) else -1,
                prefix=str(row.prefix),
                prefix_len=prefix_len,
                max_length=int(row.max_length) if pd.notna(row.max_length) else prefix_len,
                start=start,
                end=end,
                ca_subject_dn=ca_map.get(row.signer_cert_id),
            )
        )

    return intervals


def detect_conflicts(intervals: List[Interval]) -> pd.DataFrame:
    records: List[dict] = []
    by_afi: dict[str, List[Interval]] = {}
    for interval in intervals:
        by_afi.setdefault(interval.afi, []).append(interval)

    for afi, group in by_afi.items():
        sorted_group = sorted(group, key=lambda x: (x.start, -x.prefix_len))
        active: List[Interval] = []
        for interval in sorted_group:
            active = [item for item in active if item.end >= interval.start]
            for other in active:
                if other.asn == interval.asn:
                    continue
                overlap_start = max(other.start, interval.start)
                overlap_end = min(other.end, interval.end)
                if overlap_start > overlap_end:
                    continue
                severity = max(interval.max_length - interval.prefix_len, other.max_length - other.prefix_len)
                records.append(
                    {
                        "afi": afi,
                        "prefix": interval.prefix,
                        "maxLength": interval.max_length,
                        "asn_a": other.asn,
                        "asn_b": interval.asn,
                        "object_id_a": other.object_id,
                        "object_id_b": interval.object_id,
                        "ca_a": other.ca_subject_dn,
                        "ca_b": interval.ca_subject_dn,
                        "severity": severity,
                    }
                )
            active.append(interval)

    if not records:
        return pd.DataFrame(columns=[
            "afi",
            "prefix",
            "maxLength",
            "asn_a",
            "asn_b",
            "object_id_a",
            "object_id_b",
            "ca_a",
            "ca_b",
            "severity",
        ])
    return pd.DataFrame.from_records(records).drop_duplicates()


def main() -> None:
    args = parse_args()
    out_root: Sequence[Path] = args.out_root
    out_dir: Path = args.out_dir
    ensure_out_dir(out_dir)

    intervals = build_intervals(out_root)
    conflicts = detect_conflicts(intervals)
    conflicts.to_csv(out_dir / "conflicts.csv", index=False)


if __name__ == "__main__":
    main()

