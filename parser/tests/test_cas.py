from pathlib import Path

from cas import SQLiteKV


def _make_hash(index: int) -> str:
    # Produce a deterministic 64-character hex string similar to a SHA-256 hash.
    return f"{index:064x}"


def test_get_or_create_many_batches(tmp_path: Path) -> None:
    kv = SQLiteKV(tmp_path / "kv.sqlite")

    hashes = [_make_hash(i) for i in range(1200)]

    first = kv.get_or_create_many(hashes)
    assert len(first) == len(hashes)
    assert all(first[h] == idx + 1 for idx, h in enumerate(hashes))

    second = kv.get_or_create_many(hashes)
    assert second == first
