"""Seeding the issuer index from accumulated CA certs lets the chain-edge
builder connect children to stable parents that were parsed in a prior run.

Regression guard for the cert_chain_edges incompleteness bug: incremental
parsing (CAS dedup) means a stable parent CA cert is not re-parsed in the run
that parses its child, so it is absent from the in-memory index and the child's
edge is dropped. seed_issuer_index_from_certs() reloads historical CA certs so
the edge can be built.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from rpki_etl import CertIndexEntry, PipelineState, SQLiteKV


def _write_parent_cert(certs_dir: Path, nb: datetime, na: datetime) -> None:
    certs_dir.mkdir(parents=True)
    tbl = pa.table(
        {
            "cert_id": pa.array([100], pa.int32()),
            "is_ca_flag": pa.array([True]),
            "subject_dn": pa.array(["CN=Parent"]),
            "issuer_dn": pa.array(["CN=Parent"]),  # self-signed root
            "serial_dec": pa.array(["1"]),
            "serial_bin": pa.array(["01"]),
            "ski": pa.array(["AAAA"]),
            "aki": pa.array(["AAAA"]),
            "not_before": pa.array([nb], pa.timestamp("us", tz="UTC")),
            "not_after": pa.array([na], pa.timestamp("us", tz="UTC")),
        }
    )
    pq.write_table(tbl, certs_dir / "part.parquet")


def test_seed_enables_chain_edge_to_stable_parent(tmp_path: Path) -> None:
    nb = datetime(2025, 1, 1, tzinfo=timezone.utc)
    na = datetime(2027, 1, 1, tzinfo=timezone.utc)

    table_root = tmp_path / "tables"
    # Parent CA cert lives ONLY in the accumulated parquet (a prior run); it is
    # NOT parsed in this run.
    _write_parent_cert(table_root / "table=certs", nb, na)

    kv = SQLiteKV(tmp_path / "kv.sqlite")
    state = PipelineState(repo_id="r", tal_id="t", kv=kv)

    # Child CA parsed in the current run; its parent (cert 100) is absent here.
    child = CertIndexEntry(
        cert_id=200, is_ca=True, subject_dn="CN=Child", issuer_dn="CN=Parent",
        serial_dec="2", serial_bin="02", ski="BBBB", aki="AAAA",
        not_before=nb, not_after=na,
    )
    state.cert_index[200] = child
    state.certs_by_ski["BBBB"].append(200)
    state.certs_by_subject_dn["CN=Child"].append(200)

    state.seed_issuer_index_from_certs(table_root)
    # Parent is now reachable as an issuer candidate.
    assert 100 in state.cert_index
    assert 100 in state.certs_by_ski["AAAA"]

    state.finalize()
    edges = {
        (row["issuer_cert_id"], row["subject_cert_id"])
        for row in state.cert_chain_rows
    }
    # The child→parent edge is built; the self-signed root has no parent edge.
    assert (100, 200) in edges
    assert not any(subject == 100 for _, subject in edges)
    kv.close()


def test_seed_is_noop_without_certs_table(tmp_path: Path) -> None:
    kv = SQLiteKV(tmp_path / "kv.sqlite")
    state = PipelineState(repo_id="r", tal_id="t", kv=kv)
    # No table=certs directory exists yet (first ever run): must not raise.
    state.seed_issuer_index_from_certs(tmp_path / "tables")
    assert state.cert_index == {}
    kv.close()
