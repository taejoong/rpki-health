"""parquet_glob() reads every part of a table once.

After each flush the ETL (rpki-parser tables.py) points <table>.parquet at the
newest part. The old **/*.parquet glob matched that symlink as well, so the
newest part's rows came back twice (2026-11-02, ARIN roa_objects: 3,030,492 rows
for 3,030,485 objects)."""

import duckdb

from app import db


def _write(con, path, ids):
    con.execute(f"COPY (SELECT unnest({list(ids)}) AS object_id) TO '{path}' (FORMAT parquet)")


def test_newest_part_symlink_is_not_read_twice(tmp_path, monkeypatch):
    wh = tmp_path / "warehouse-arin"
    roa = wh / "tables" / "table=roa_objects"
    roa.mkdir(parents=True)
    pres = wh / "tables" / "table=object_presence_stats"
    pres.mkdir(parents=True)
    con = duckdb.connect()
    _write(con, roa / "roa_objects-part-1-1.parquet", (1, 2))
    _write(con, roa / "roa_objects-part-2-1.parquet", (3,))
    (roa / "roa_objects.parquet").symlink_to("roa_objects-part-2-1.parquet")
    _write(con, pres / "part-00000.parquet", (1,))
    _write(con, pres / "part-00001.parquet", (2, 3))
    monkeypatch.setitem(db.WAREHOUSE_DIRS, "arin", wh)

    def ids(table):
        return [r[0] for r in con.execute(
            f"SELECT object_id FROM read_parquet('{db.parquet_glob('arin', table)}') ORDER BY 1").fetchall()]

    assert ids("roa_objects") == [1, 2, 3]
    assert ids("object_presence_stats") == [1, 2, 3]
