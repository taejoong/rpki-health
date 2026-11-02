from __future__ import annotations


"""A19 – CA topology depth and fanout analysis.
Builds a graph of CA certificate relationships to compute depths from trust
anchors and distribution of subordinate counts.
Inputs
------
* Warehouse tables via :mod:`analysis.analysis_common`:
  - ``cert_chain_edges`` for issuer/subject relationships.
  - ``certs`` for metadata including TAL association.
* CLI arguments ``--out-root`` (one or more warehouse roots) and ``--out-dir``.
Outputs
-------
* ``ca_topology.csv`` – per-CA depth, fanout, and TAL association.
* ``ca_topology_depth.png`` – visual summary of depth distribution.
Only warehouse metadata is required; KV/CAS dependencies are absent.

Examples
--------
Render the APNIC trust graph::

    python analysis/a19_ca_topology.py \
        --out-root warehouse-apnic --out-dir outputs/a19

Combine APNIC and RIPE NCC hierarchies::

    python analysis/a19_ca_topology.py \
        --out-root warehouse-apnic warehouse-ripencc --out-dir outputs/a19_multi

Forward ``--warehouse warehouse-apnic`` arguments through ``--out-root`` here.
"""


import argparse
from collections import deque, defaultdict
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd

from analysis.analysis_common import load_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CA topology depth and fanout")
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


def compute_topology(out_root: Sequence[Path]) -> pd.DataFrame:
    edges = load_table(out_root, "cert_chain_edges")
    certs = load_table(out_root, "certs")

    if edges.empty:
        return pd.DataFrame(columns=["cert_id", "subject_dn", "depth", "child_count", "tal_id"])

    adjacency = defaultdict(list)
    incoming = defaultdict(set)
    nodes = set()
    for row in edges.itertuples(index=False):
        issuer = getattr(row, "issuer_cert_id")
        subject = getattr(row, "subject_cert_id")
        adjacency[issuer].append(subject)
        incoming[subject].add(issuer)
        nodes.add(issuer)
        nodes.add(subject)

    cert_info = certs.set_index("cert_id").to_dict(orient="index") if not certs.empty else {}

    roots = [node for node in nodes if not incoming[node]]
    if not roots:
        roots = list(nodes)

    depth = {root: 0 for root in roots}
    queue = deque(roots)
    # Breadth-first traversal calculates depth relative to each trust anchor.
    while queue:
        current = queue.popleft()
        for child in adjacency.get(current, []):
            if child not in depth or depth[child] > depth[current] + 1:
                depth[child] = depth[current] + 1
                queue.append(child)

    rows = []
    for cert_id in nodes:
        info = cert_info.get(cert_id, {})
        rows.append(
            {
                "cert_id": cert_id,
                "subject_dn": info.get("subject_dn"),
                "depth": depth.get(cert_id),
                "child_count": len(adjacency.get(cert_id, [])),
                "tal_id": info.get("tal_id"),
            }
        )

    return pd.DataFrame(rows)


def write_outputs(out_dir: Path, topology: pd.DataFrame) -> None:
    ensure_out_dir(out_dir)
    topology.to_csv(out_dir / "ca_topology.csv", index=False)

    if topology.empty:
        plt.figure()
        plt.savefig(out_dir / "ca_depth_distribution.png", dpi=150, bbox_inches="tight")
        plt.close()
        return

    plt.figure(figsize=(6, 4))
    topology["depth"].dropna().astype(int).value_counts().sort_index().plot(kind="bar")
    plt.xlabel("Depth")
    plt.ylabel("Number of CAs")
    plt.tight_layout()
    plt.savefig(out_dir / "ca_depth_distribution.png", dpi=150)
    plt.close()


def main() -> None:
    args = parse_args()
    topology = compute_topology(args.out_root)
    write_outputs(args.out_dir, topology)


if __name__ == "__main__":
    main()

