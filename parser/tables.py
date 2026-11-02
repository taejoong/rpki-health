"""Parquet table writers and table schemas."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq


def schema_crls() -> pa.schema:
    """Return the schema for the ``table=crls`` dataset."""

    timestamp_us = pa.timestamp("us", tz="UTC")
    return pa.schema(
        [
            ("crl_id", pa.int32()),
            ("object_id", pa.int32()),
            ("issuer_dn", pa.string()),
            ("crl_number", pa.string()),
            ("this_update", timestamp_us),
            ("next_update", timestamp_us),
            ("revoked_count", pa.int32()),
            ("aki", pa.string()),
            ("signature_alg", pa.string()),
            ("repo_id", pa.string()),
            ("tal_id", pa.string()),
        ]
    )


def schema_crl_serials() -> pa.schema:
    """Return the schema for the ``table=crl_serials`` dataset."""

    timestamp_us = pa.timestamp("us", tz="UTC")
    return pa.schema(
        [
            ("crl_id", pa.int32()),
            ("serial_dec", pa.string()),
            ("serial_bin", pa.string()),
            ("revocation_date", timestamp_us),
            ("reason_code", pa.int16()),
            ("invalidity_date", timestamp_us),
            ("cert_issuer_dn", pa.string()),
            ("revoked_cert_id", pa.int32()),
        ]
    )


def _unique_suffix() -> str:
    return f"{int(time.time() * 1e6)}-{os.getpid()}"


@dataclass
class ParquetBatchWriter:
    root: Path
    table_name: str
    schema: pa.schema
    partition_values: Dict[str, str]
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def append(self, row: Dict[str, Any]) -> None:
        self.rows.append(row)

    def flush(self) -> None:
        if not self.rows:
            return
        partition_path = self.root
        for key, value in self.partition_values.items():
            partition_path = partition_path / f"{key}={value}"
        partition_path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(self.rows, schema=self.schema)

        path = partition_path / f"{self.table_name}-part-{_unique_suffix()}.parquet"
        pq.write_table(
            table,
            path,
            compression="zstd",
            write_statistics=True,
            data_page_size=1 << 16,
            use_dictionary=True,
        )
        latest_path = partition_path / f"{self.table_name}.parquet"
        if latest_path.is_symlink() or latest_path.exists():
            try:
                latest_path.unlink()
            except FileNotFoundError:
                pass
        latest_path.symlink_to(path.name)

        self.rows.clear()
