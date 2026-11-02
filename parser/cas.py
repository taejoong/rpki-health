"""Content addressed storage utilities."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union

import os
import sqlite3
import tempfile
import threading

import zstandard

from util import compute_sha256


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _hex_to_bin(hash_hex: str) -> bytes:
    return bytes.fromhex(hash_hex)


class FileCAS:
    """Content-addressed store that writes one file per object."""

    def __init__(
        self, root: Path, compression_level: int = 3, zstd_threads: int = 0
    ) -> None:
        self.root = root
        _ensure_dir(self.root)
        self._compressor = zstandard.ZstdCompressor(
            level=compression_level, threads=zstd_threads
        )
        self._decompressor = zstandard.ZstdDecompressor()
        self._compress_lock = threading.Lock()
        self._decompress_lock = threading.Lock()

    def _object_path(self, object_hash: str) -> Path:
        directory = self.root / object_hash[0:2] / object_hash[2:4]
        _ensure_dir(directory)
        return directory / f"{object_hash}.der.zst"

    def exists(self, object_hash: str) -> bool:
        return self._object_path(object_hash).exists()

    def store(self, object_hash: str, raw_bytes: bytes) -> None:
        path = self._object_path(object_hash)
        if path.exists():
            return
        with self._compress_lock:
            compressed = self._compressor.compress(raw_bytes)
        path.write_bytes(compressed)

    def read(self, object_hash: str) -> bytes:
        path = self._object_path(object_hash)
        if not path.exists():
            raise FileNotFoundError(f"CAS object not found: {object_hash}")
        data = path.read_bytes()
        with self._decompress_lock:
            return self._decompressor.decompress(data)

    def materialize(self, object_hash: str, dest_path: Optional[Path] = None) -> Path:
        data = self.read(object_hash)
        if dest_path is None:
            tmp = tempfile.NamedTemporaryFile(prefix="cas-", suffix=".der", delete=False)
            tmp.write(data)
            tmp.flush()
            tmp.close()
            return Path(tmp.name)
        _ensure_dir(dest_path.parent)
        dest_path.write_bytes(data)
        return dest_path

    def close(self) -> None:  # pragma: no cover - compatibility hook
        return None


# Backwards compatibility alias
ObjectStore = FileCAS


_SQLITE_PRAGMAS = [
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("temp_store", "MEMORY"),
    ("page_size", "32768"),
    ("mmap_size", str(1 << 30)),
    ("cache_size", str(-400000)),
    ("locking_mode", "NORMAL"),
    ("busy_timeout", "30000"),
]

_SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    key      BLOB PRIMARY KEY,
    cbytes   BLOB,
    orig_len INTEGER NOT NULL,
    type     TEXT,
    path     TEXT
) WITHOUT ROWID;
"""


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, decl: str) -> None:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


class _SQLiteShard:
    def __init__(self, db_path: Path) -> None:
        _ensure_dir(db_path.parent)
        self.conn = sqlite3.connect(
            str(db_path),
            timeout=120.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.lock = threading.Lock()
        cursor = self.conn.cursor()
        for pragma, value in _SQLITE_PRAGMAS:
            cursor.execute(f"PRAGMA {pragma}={value}")
        cursor.execute(_SQL_SCHEMA)
        _ensure_column(self.conn, "objects", "path", "TEXT")
        cursor.close()

    def begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()


class SQLiteCAS:
    """Sharded SQLite-backed CAS using binary keys."""

    def __init__(
        self,
        root: Path,
        shards: int = 256,
        compression_level: int = 3,
        zstd_threads: int = 0,
        value_mode: str = "bytes",
        external_root: Optional[Path] = None,
        verify_on_read: bool = False,
    ) -> None:
        if shards not in (1, 16, 64, 128, 256, 512):
            raise ValueError("choose a reasonable shard count")
        if value_mode not in {"bytes", "path"}:
            raise ValueError("value_mode must be 'bytes' or 'path'")
        self.root = root
        _ensure_dir(self.root)
        self.shards = shards
        self._mode = value_mode
        self._external_root = external_root
        self._verify_on_read = verify_on_read
        self._compressor = zstandard.ZstdCompressor(
            level=compression_level, threads=zstd_threads
        )
        self._decompressor = zstandard.ZstdDecompressor()
        self._compress_lock = threading.Lock()
        self._decompress_lock = threading.Lock()
        self._dbs: List[_SQLiteShard] = []
        for index in range(self.shards):
            db_path = self.root / f"shard-{index:02x}.db"
            self._dbs.append(_SQLiteShard(db_path))

    def _shard_index(self, key: bytes) -> int:
        return key[0] % self.shards

    def _to_stored_path(self, src: Path) -> tuple[str, bool]:
        resolved = src.resolve()
        if self._external_root is not None:
            try:
                rel = resolved.relative_to(self._external_root.resolve())
            except Exception:
                pass
            else:
                return rel.as_posix(), True
        return resolved.as_posix(), False

    def _from_stored_path(self, stored: str) -> Path:
        path = Path(stored)
        if not path.is_absolute():
            if self._external_root is not None:
                return (self._external_root / path).resolve()
            return (Path.cwd() / path).resolve()
        return path

    def exists(self, object_hash: str) -> bool:
        key = _hex_to_bin(object_hash)
        shard = self._dbs[self._shard_index(key)]
        with shard.lock:
            cursor = shard.conn.execute(
                "SELECT 1 FROM objects WHERE key=? LIMIT 1", (key,)
            )
            row = cursor.fetchone()
        return row is not None

    def store_path(self, object_hash: str, src_path: Path) -> None:
        if self._mode != "path":
            raise RuntimeError("store_path is only available in path mode")
        key = _hex_to_bin(object_hash)
        shard = self._dbs[self._shard_index(key)]
        stored, _ = self._to_stored_path(src_path)
        size = os.path.getsize(src_path)
        with shard.lock:
            cursor = shard.conn.execute(
                "SELECT 1 FROM objects WHERE key=? LIMIT 1", (key,)
            )
            if cursor.fetchone() is not None:
                return
        with shard.lock:
            try:
                shard.begin()
                shard.conn.execute(
                    """
                    INSERT OR IGNORE INTO objects(key, cbytes, orig_len, type, path)
                    VALUES (?, NULL, ?, NULL, ?)
                    """,
                    (key, size, stored),
                )
                shard.commit()
            except Exception:
                shard.rollback()
                raise

    def store(self, object_hash: str, raw_bytes: bytes) -> None:
        if self._mode == "path":
            raise RuntimeError("SQLiteCAS.store called in path mode; use store_path")
        key = _hex_to_bin(object_hash)
        shard = self._dbs[self._shard_index(key)]
        with shard.lock:
            cursor = shard.conn.execute(
                "SELECT 1 FROM objects WHERE key=? LIMIT 1", (key,)
            )
            if cursor.fetchone() is not None:
                return
        with self._compress_lock:
            compressed = self._compressor.compress(raw_bytes)
        with shard.lock:
            try:
                shard.begin()
                shard.conn.execute(
                    """
                    INSERT OR IGNORE INTO objects(key, cbytes, orig_len, type, path)
                    VALUES (?, ?, ?, NULL, NULL)
                    """,
                    (key, sqlite3.Binary(compressed), len(raw_bytes)),
                )
                shard.commit()
            except Exception:
                shard.rollback()
                raise

    def read(self, object_hash: str) -> bytes:
        key = _hex_to_bin(object_hash)
        shard = self._dbs[self._shard_index(key)]
        with shard.lock:
            cursor = shard.conn.execute(
                "SELECT cbytes, path FROM objects WHERE key=? LIMIT 1",
                (key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise FileNotFoundError(
                f"CAS object not found in SQLite CAS: {object_hash}"
            )
        cbytes, stored_path = row
        if cbytes is not None:
            with self._decompress_lock:
                return self._decompressor.decompress(bytes(cbytes))
        if stored_path is None:
            raise FileNotFoundError(
                f"CAS row has no bytes and no path: {object_hash}"
            )
        resolved = self._from_stored_path(str(stored_path))
        data = Path(resolved).read_bytes()
        if self._verify_on_read:
            from util import compute_sha256 as _compute_sha256

            if _compute_sha256(data) != object_hash:
                raise IOError(f"verify_on_read failed for {object_hash}")
        return data

    def materialize(self, object_hash: str, dest_path: Optional[Path] = None) -> Path:
        key = _hex_to_bin(object_hash)
        shard = self._dbs[self._shard_index(key)]
        with shard.lock:
            cursor = shard.conn.execute(
                "SELECT cbytes, path FROM objects WHERE key=? LIMIT 1",
                (key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise FileNotFoundError(
                f"CAS object not found in SQLite CAS: {object_hash}"
            )
        cbytes, stored_path = row
        if stored_path is not None:
            src = self._from_stored_path(str(stored_path))
            if dest_path is None:
                tmp = tempfile.NamedTemporaryFile(
                    prefix="cas-", suffix=".der", delete=False
                )
                tmp.close()
                dest_path = Path(tmp.name)
            _ensure_dir(dest_path.parent)
            import shutil

            shutil.copyfile(src, dest_path)
            if self._verify_on_read:
                from util import compute_sha256 as _compute_sha256

                if _compute_sha256(dest_path.read_bytes()) != object_hash:
                    raise IOError(f"verify_on_read failed for {object_hash}")
            return dest_path
        with self._decompress_lock:
            data = self._decompressor.decompress(bytes(cbytes))
        if dest_path is None:
            tmp = tempfile.NamedTemporaryFile(prefix="cas-", suffix=".der", delete=False)
            tmp.write(data)
            tmp.flush()
            tmp.close()
            return Path(tmp.name)
        _ensure_dir(dest_path.parent)
        dest_path.write_bytes(data)
        return dest_path

    def close(self) -> None:
        for shard in self._dbs:
            try:
                shard.conn.close()
            except Exception:  # pragma: no cover - best effort
                pass


@dataclass
class ObjectRecord:
    object_id: int
    object_hash: str
    path: Path
    size: int


def ingest_bytes(
    data: bytes, store: Union[FileCAS, SQLiteCAS], kv: "SQLiteKV"
) -> ObjectRecord:
    object_hash = compute_sha256(data)
    object_id = kv.get_or_create(object_hash)
    if isinstance(store, SQLiteCAS) and store._mode == "path":
        raise RuntimeError("ingest_bytes is not supported in path mode")
    store.store(object_hash, data)
    return ObjectRecord(
        object_id=object_id,
        object_hash=object_hash,
        path=Path(object_hash),
        size=len(data),
    )


_SQLITE_MAX_VARIABLES = 900


def _chunked(items: Iterable[str], size: int) -> Iterable[list[str]]:
    chunk: list[str] = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class SQLiteKV:
    """Simple SQLite backed key/value store mapping hashes to integer ids."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA cache_size=-200000")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS objects (
                object_hash TEXT PRIMARY KEY,
                object_id INTEGER NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value INTEGER
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS emitted_rows (
                table_name TEXT NOT NULL,
                primary_key TEXT NOT NULL,
                PRIMARY KEY (table_name, primary_key)
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS parsed_objects (
                object_hash TEXT PRIMARY KEY
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS certificates (
                cert_hash TEXT PRIMARY KEY,
                cert_id INTEGER NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS objects_observed (
                object_id INTEGER PRIMARY KEY,
                first_seen_ts TEXT NOT NULL,
                last_seen_ts  TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _meta_get(self, key: str, default: int = 1) -> int:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return default
        return int(row[0])

    def _meta_set(self, key: str, value: int) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
            (key, value),
        )

    def _next_id(self) -> int:
        return self._meta_get("next_id")

    def _next_cert_id(self) -> int:
        return self._meta_get("next_cert_id")

    def get_or_create(self, object_hash: str) -> int:
        row = self._conn.execute(
            "SELECT object_id FROM objects WHERE object_hash = ?", (object_hash,)
        ).fetchone()
        if row is not None:
            return int(row[0])

        next_id = self._next_id()
        self._conn.execute(
            "INSERT INTO objects(object_hash, object_id) VALUES (?, ?)",
            (object_hash, next_id),
        )
        self._meta_set("next_id", next_id + 1)
        self._conn.commit()
        return next_id

    def get_or_create_many(self, hashes: list[str]) -> Dict[str, int]:
        if not hashes:
            return {}

        cursor = self._conn.cursor()
        existing: Dict[str, int] = {}
        for chunk in _chunked(hashes, _SQLITE_MAX_VARIABLES):
            placeholders = ",".join("?" for _ in chunk)
            for object_hash, object_id in cursor.execute(
                f"SELECT object_hash, object_id FROM objects WHERE object_hash IN ({placeholders})",
                chunk,
            ):
                existing[str(object_hash)] = int(object_id)

        to_insert = [h for h in hashes if h not in existing]
        if to_insert:
            next_id = self._next_id()
            assigned = 0
            for chunk in _chunked(to_insert, _SQLITE_MAX_VARIABLES):
                rows = [
                    (h, next_id + assigned + offset) for offset, h in enumerate(chunk)
                ]
                cursor.executemany(
                    "INSERT INTO objects(object_hash, object_id) VALUES (?, ?)", rows
                )
                existing.update(
                    {
                        h: next_id + assigned + offset
                        for offset, h in enumerate(chunk)
                    }
                )
                assigned += len(chunk)
            cursor.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES ('next_id', ?)",
                (next_id + assigned,),
            )
            self._conn.commit()
        return existing

    def get_many(self, object_hashes: Iterable[str]) -> Dict[str, int]:
        mapping: Dict[str, int] = {}
        for chunk in _chunked(object_hashes, _SQLITE_MAX_VARIABLES):
            placeholders = ",".join(["?"] * len(chunk))
            cursor = self._conn.execute(
                f"SELECT object_hash, object_id FROM objects WHERE object_hash IN ({placeholders})",
                chunk,
            )
            mapping.update({str(row[0]): int(row[1]) for row in cursor.fetchall()})
        return mapping

    def get(self, object_hash: str) -> Optional[int]:
        row = self._conn.execute(
            "SELECT object_id FROM objects WHERE object_hash = ?", (object_hash,)
        ).fetchone()
        return int(row[0]) if row is not None else None

    def get_or_create_cert_id(self, cert_hash: str) -> int:
        row = self._conn.execute(
            "SELECT cert_id FROM certificates WHERE cert_hash = ?",
            (cert_hash,),
        ).fetchone()
        if row is not None:
            return int(row[0])

        cert_id = self._next_cert_id()
        self._conn.execute(
            "INSERT INTO certificates(cert_hash, cert_id) VALUES (?, ?)",
            (cert_hash, cert_id),
        )
        self._meta_set("next_cert_id", cert_id + 1)
        self._conn.commit()
        return cert_id

    def mark_emitted(self, table_name: str, primary_key: str) -> bool:
        cursor = self._conn.execute(
            """
            INSERT OR IGNORE INTO emitted_rows(table_name, primary_key)
            VALUES (?, ?)
            """,
            (table_name, primary_key),
        )
        self._conn.commit()
        return cursor.rowcount == 1

    def is_parsed(self, object_hash: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM parsed_objects WHERE object_hash = ?", (object_hash,)
        ).fetchone()
        return row is not None

    def mark_parsed(self, object_hash: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO parsed_objects(object_hash) VALUES (?)",
            (object_hash,),
        )
        self._conn.commit()

    def update_observed_many(self, dataset_ts: str, object_ids: List[int]) -> None:
        if not object_ids:
            return
        cursor = self._conn.cursor()
        cursor.executemany(
            "UPDATE objects_observed SET last_seen_ts = ? WHERE object_id = ?",
            ((dataset_ts, object_id) for object_id in object_ids),
        )
        cursor.executemany(
            """
            INSERT OR IGNORE INTO objects_observed(object_id, first_seen_ts, last_seen_ts)
            VALUES (?, ?, ?)
            """,
            ((object_id, dataset_ts, dataset_ts) for object_id in object_ids),
        )
        self._conn.commit()

    def dump_observed(self) -> List[tuple[int, str, str]]:
        cursor = self._conn.cursor()
        return [
            (int(row[0]), str(row[1]), str(row[2]))
            for row in cursor.execute(
                "SELECT object_id, first_seen_ts, last_seen_ts FROM objects_observed"
            )
        ]


def make_cas_backend(
    kind: str,
    out_root: Path,
    compression_level: int = 3,
    zstd_threads: int = 0,
    sqlite_root: Optional[Path] = None,
    sqlite_shards: int = 256,
    cas_mode: str = "bytes",
    cas_external_root: Optional[Path] = None,
    cas_verify_on_read: bool = False,
):
    if kind == "file":
        return FileCAS(
            out_root / "cas",
            compression_level=compression_level,
            zstd_threads=zstd_threads,
        )
    if kind == "sqlite":
        root = sqlite_root if sqlite_root is not None else (out_root / "cas-sqlite")
        return SQLiteCAS(
            root=root,
            shards=sqlite_shards,
            compression_level=compression_level,
            zstd_threads=zstd_threads,
            value_mode=cas_mode,
            external_root=cas_external_root,
            verify_on_read=cas_verify_on_read,
        )
    raise ValueError(f"unknown CAS backend: {kind}")
