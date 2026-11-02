"""Shared helper library for the analysis suite.
Provides reusable utilities for loading warehouse tables, iterating over
snapshot memberships, reading snapshot metadata, and resolving auxiliary
datasets such as KV mappings and CAS object sizes.
Inputs
------
* Parquet warehouse organised under ``<out_root>/tables/table=<name>/...``.
* Snapshot membership artefacts in ``<out_root>/indexes/membership``.
* Optional KV SQLite database mapping object hashes to ids.
* Optional CAS SQLite shards providing object sizes.
Outputs
-------
* Functions returning :class:`pandas.DataFrame` instances or dictionaries that
  feed the various analysis scripts.
The module keeps imports lightweight and gracefully handles missing optional
dependencies (e.g., ``pyroaring``). Callers pass :class:`pathlib.Path`
instances for the relevant roots.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3
from datetime import datetime
from os import PathLike
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc


try:  # pragma: no cover - optional dependency
    from pyroaring import BitMap  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    BitMap = None  # type: ignore


OutRootItem = Union[Path, PathLike, str]
OutRoots = Union[OutRootItem, Sequence[OutRootItem]]


def _coerce_roots(out_root: OutRoots) -> List[Path]:
    """Return a list of warehouse roots from *out_root* input variants."""

    if isinstance(out_root, Path):
        return [out_root]
    if isinstance(out_root, PathLike) or isinstance(out_root, str):
        return [Path(out_root)]

    roots: List[Path] = []
    for item in out_root:  # type: ignore[arg-type]
        if isinstance(item, Path):
            roots.append(item)
        elif isinstance(item, PathLike) or isinstance(item, str):
            roots.append(Path(item))
        else:
            raise TypeError(f"Unsupported warehouse root type: {type(item)!r}")
    return roots


def _tables_root(out_root: Path) -> Path:
    return out_root / "tables"


def load_table(out_root: OutRoots, table: str) -> pd.DataFrame:
    """Load a logical table from the warehouse.

    Parameters
    ----------
    out_root:
        Root directory of the ETL output.
    table:
        Logical table name, e.g. ``"certs"``.

    Returns
    -------
    pandas.DataFrame
        Data frame containing all rows for the requested table with
        partition columns included (e.g. ``table`` and ``is_ca`` for the
        ``certs`` table).
    """

    frames: List[pd.DataFrame] = []
    for root in _coerce_roots(out_root):
        dataset_root = _tables_root(root)
        if not dataset_root.exists():
            continue

        dataset = ds.dataset(dataset_root, format="parquet", partitioning="hive")
        table_filter = pc.field("table") == table
        try:
            table_obj = dataset.to_table(filter=table_filter)
        except (FileNotFoundError, OSError):
            continue

        if table_obj.num_rows == 0:
            continue

        frames.append(table_obj.to_pandas())

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def list_snapshots(out_root: OutRoots) -> List[str]:
    """Return ordered snapshot labels of the form ``YYYY-MM-DD HH-MM-SS``."""

    labels: set[str] = set()

    for root in _coerce_roots(out_root):
        memberships_root = root / "indexes" / "membership"
        if not memberships_root.exists():
            continue

        for child in memberships_root.iterdir():
            if not child.is_dir():
                continue
            if not child.name.startswith("ts="):
                continue
            labels.add(child.name[3:])

    return sorted(labels)


def load_membership(out_root: OutRoots, ts_label: str) -> List[int]:
    """Load the membership bitmap for a snapshot as sorted object ids."""

    combined: set[int] = set()

    for root in _coerce_roots(out_root):
        path = root / "indexes" / "membership" / f"ts={ts_label}" / "objects.roar"
        if not path.exists():
            continue

        data = path.read_bytes()
        if not data:
            continue

        if data[:1] == b"{" or data.strip().startswith(b"{"):
            payload = json.loads(data.decode("utf-8"))
            items = payload.get("data", [])
            combined.update(int(x) for x in items)
            continue

        if BitMap is None:
            raise RuntimeError(
                "Roaring membership file encountered but pyroaring is not installed"
            )

        bitmap = BitMap.deserialize(data)
        combined.update(int(v) for v in bitmap)

    return sorted(combined)


def load_kv_maps(
    kv_path: Path | PathLike | Sequence[Path | PathLike] | None,
) -> Tuple[Dict[str, int], Dict[int, str]]:
    """Load the object hash to id mapping from the KV database."""

    hash_to_id: Dict[str, int] = {}
    id_to_hash: Dict[int, str] = {}
    if kv_path is None:
        return hash_to_id, id_to_hash

    for path in _coerce_roots(kv_path):
        if not path.exists():
            continue

        conn = sqlite3.connect(path)
        try:
            cursor = conn.execute("SELECT object_hash, object_id FROM objects")
            for row in cursor:
                object_hash, object_id = row
                hash_to_id[str(object_hash)] = int(object_id)
                id_to_hash[int(object_id)] = str(object_hash)
        finally:
            conn.close()

    return hash_to_id, id_to_hash


def load_cas_sizes(cas_sqlite_root: Path | PathLike | Sequence[Path | PathLike] | None) -> Dict[str, int]:
    """Return a mapping from object hash (hex) to original length."""

    if cas_sqlite_root is None:
        return {}

    roots = _coerce_roots(cas_sqlite_root)

    sizes: Dict[str, int] = {}
    for root in roots:
        if not root.exists():
            continue

        for shard in sorted(root.glob("shard-*.db")):
            conn = sqlite3.connect(shard)
            try:
                cursor = conn.execute("SELECT hex(key), orig_len FROM objects")
                for key_hex, orig_len in cursor:
                    if key_hex is None:
                        continue
                    sizes[str(key_hex).lower()] = int(orig_len)
            finally:
                conn.close()

    return sizes


def load_presence_stats(out_root: OutRoots) -> pd.DataFrame:
    """Load aggregated object presence statistics per object id."""

    stats = load_table(out_root, "object_presence_stats")
    if stats.empty:
        return stats

    if "first_ts" in stats.columns and "first_seen_ts" not in stats.columns:
        stats["first_seen_ts"] = stats["first_ts"]
    if "last_ts" in stats.columns and "last_seen_ts" not in stats.columns:
        stats["last_seen_ts"] = stats["last_ts"]
    return stats


def read_snapshot_metadata(out_root: OutRoots) -> pd.DataFrame:
    """Read metadata.json files for every snapshot."""

    records = []

    for root in _coerce_roots(out_root):
        snapshots_root = root / "snapshots"
        if not snapshots_root.exists():
            continue

        for ts_dir in sorted(snapshots_root.glob("ts=*")):
            if not ts_dir.is_dir():
                continue
            ts_label = ts_dir.name[3:]
            metadata_path = ts_dir / "metadata.json"
            if not metadata_path.exists():
                continue
            with metadata_path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
            record = {
                "dataset_ts": ts_label,
                "repo_id": payload.get("repo_id"),
                "tal_id": payload.get("tal_id"),
                "file_count": payload.get("file_count"),
                "object_count": payload.get("object_count"),
                "unique_object_count": payload.get("unique_object_count"),
            }
            records.append(record)

    return pd.DataFrame.from_records(records)


def ts_label_to_iso(ts_label: str) -> str:
    """Convert ``YYYY-MM-DD HH-MM-SS`` to ``YYYY-MM-DD HH:MM:SS``."""

    date_part, time_part = ts_label.split(" ", 1)
    return f"{date_part} {time_part.replace('-', ':', 2)}"


def day_key(str_ts: str) -> str:
    """Return the ``YYYY-MM-DD`` component of a timestamp string."""

    if "T" in str_ts:
        return str_ts.split("T", 1)[0]
    return str_ts.split(" ", 1)[0]


__all__ = [
    "load_table",
    "list_snapshots",
    "load_membership",
    "load_kv_maps",
    "load_cas_sizes",
    "load_presence_stats",
    "read_snapshot_metadata",
    "ts_label_to_iso",
    "day_key",
]

