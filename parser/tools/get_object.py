#!/usr/bin/env python3
"""
Fetch the path or bytes for a specific RPKI object from the warehouse.

Examples
- By object_id:
  python tools/get_object.py --warehouse warehouse --kv warehouse/kv.sqlite \
    --cas-sqlite-root warehouse/cas-sqlite --object-id 123456 --print-path

- By object hash:
  python tools/get_object.py --warehouse warehouse --kv warehouse/kv.sqlite \
    --cas-sqlite-root warehouse/cas-sqlite --object-hash deadbeef... --print-path

- By ROA selector (ASN and optional prefix):
  python tools/get_object.py --warehouse warehouse --kv warehouse/kv.sqlite \
    --cas-sqlite-root warehouse/cas-sqlite --roa-asn 64496 --roa-prefix 203.0.113.0/24 \
    --materialize /tmp/roa.der

- By certificate serial (decimal):
  python tools/get_object.py --warehouse warehouse --kv warehouse/kv.sqlite \
    --cas-sqlite-root warehouse/cas-sqlite --cert-serial-dec 123456789 --print-path
"""

import argparse
import binascii
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Tuple, List

import duckdb
import zstandard

# ---------- helpers ----------

def hex_to_bin(hash_hex: str) -> bytes:
    hash_hex = hash_hex.strip().lower()
    if len(hash_hex) != 64:
        raise ValueError("object-hash must be 64 hex chars")
    return binascii.unhexlify(hash_hex.encode("ascii"))

def shard_index(hash_hex: str, shards: int = 256) -> int:
    first_byte = int(hash_hex[0:2], 16)
    return first_byte % shards

def kv_lookup_hash_by_object_id(kv_path: Path, object_id: int) -> Optional[str]:
    con = sqlite3.connect(str(kv_path))
    try:
        row = con.execute("SELECT object_hash FROM objects WHERE object_id = ?", (object_id,)).fetchone()
        return row[0] if row else None
    finally:
        con.close()

def cas_sqlite_lookup(cas_root: Path, hash_hex: str) -> Tuple[Optional[str], Optional[bytes], int]:
    """
    Returns (stored_path, cbytes, orig_len). One of stored_path or cbytes will be non‑None if found.
    """
    sidx = shard_index(hash_hex)
    shard_db = cas_root / f"shard-{sidx:02x}.db"
    if not shard_db.exists():
        return (None, None, 0)
    key = hex_to_bin(hash_hex)
    con = sqlite3.connect(str(shard_db))
    try:
        row = con.execute("SELECT path, cbytes, orig_len FROM objects WHERE key = ? LIMIT 1", (key,)).fetchone()
        if not row:
            return (None, None, 0)
        path, cbytes, orig_len = row
        return (path, cbytes, int(orig_len))
    finally:
        con.close()

def materialize_bytes(cbytes: bytes, out_path: Path) -> Path:
    d = zstandard.ZstdDecompressor().decompress(cbytes)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(d)
    return out_path

def ensure_duckdb(threads: int = 8):
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute("PRAGMA enable_progress_bar=false")
    return con

def find_roa_object_ids(warehouse: Path, asn: int, prefix: Optional[str]) -> List[int]:
    roas = str((warehouse / "tables" / "table=roa_objects" / "**" / "*.parquet").as_posix())
    rpf  = str((warehouse / "tables" / "table=roa_prefixes" / "**" / "*.parquet").as_posix())
    con = ensure_duckdb()
    try:
        if prefix:
            q = f"""
            SELECT DISTINCT r.object_id
            FROM read_parquet('{roas}') r
            JOIN read_parquet('{rpf}') p ON p.object_id = r.object_id
            WHERE r.asn = {asn} AND p.prefix = '{prefix}'
            """
        else:
            q = f"""
            SELECT DISTINCT object_id
            FROM read_parquet('{roas}')
            WHERE asn = {asn}
            """
        return [int(x[0]) for x in con.execute(q).fetchall()]
    finally:
        con.close()

def find_cert_object_ids_by_serial(warehouse: Path, serial_dec: Optional[str], serial_hex: Optional[str]) -> List[int]:
    certs = str((warehouse / "tables" / "table=certs" / "**" / "*.parquet").as_posix())
    con = ensure_duckdb()
    try:
        if serial_dec:
            q = f"SELECT DISTINCT object_id FROM read_parquet('{certs}') WHERE serial_dec = '{serial_dec}'"
        elif serial_hex:
            q = f"SELECT DISTINCT object_id FROM read_parquet('{certs}') WHERE serial_bin = '{serial_hex.lower()}'"
        else:
            return []
        return [int(x[0]) for x in con.execute(q).fetchall()]
    finally:
        con.close()

def find_object_hash_from_type_table(warehouse: Path, object_type: str, object_id: int) -> Optional[str]:
    """
    Try to get object_hash directly from the type table when present.
    Falls back to kv for types that do not store object_hash inline.
    """
    con = ensure_duckdb()
    try:
        if object_type.upper() == "ROA":
            roas = str((warehouse / "tables" / "table=roa_objects" / "**" / "*.parquet").as_posix())
            q = f"SELECT object_hash FROM read_parquet('{roas}') WHERE object_id = {object_id} LIMIT 1"
            row = con.execute(q).fetchone()
            return row[0] if row else None
        if object_type.upper() == "CER":
            # certs table uses cert_hash (DER hash) that matches the object hash
            certs = str((warehouse / "tables" / "table=certs" / "**" / "*.parquet").as_posix())
            q = f"SELECT cert_hash FROM read_parquet('{certs}') WHERE object_id = {object_id} LIMIT 1"
            row = con.execute(q).fetchone()
            return row[0] if row else None
        if object_type.upper() == "CRL":
            return None
        # MFT: rely on kv fallback
        return None
    finally:
        con.close()

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Return stored path or materialize DER for a specific RPKI object.")
    ap.add_argument("--warehouse", type=Path, required=True, help="warehouse root")
    ap.add_argument("--kv", type=Path, required=True, help="path to kv.sqlite")
    ap.add_argument("--cas-sqlite-root", type=Path, required=True, help="path to cas-sqlite directory")
    ap.add_argument("--print-path", action="store_true", help="print stored path if CAS path mode is used")
    ap.add_argument("--materialize", type=Path, default=None, help="write DER bytes to this path")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 8)

    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument("--object-id", type=int)
    sel.add_argument("--object-hash", type=str)
    sel.add_argument("--roa-asn", type=int)
    sel.add_argument("--cert-serial-dec", type=str)
    sel.add_argument("--cert-serial-hex", type=str)

    ap.add_argument("--roa-prefix", type=str, default=None, help="optional x/y when using --roa-asn")
    ap.add_argument("--type", type=str, default=None,
                    help="optional hint: ROA|CER|MFT|CRL; used only to try direct object_hash lookup")

    args = ap.parse_args()

    # Resolve object_ids based on selector
    object_ids: List[int] = []
    if args.object_id is not None:
        object_ids = [args.object_id]
    elif args.object_hash:
        # we only need to process this single hash
        object_ids = []
    elif args.roa_asn is not None:
        object_ids = find_roa_object_ids(args.warehouse, args.roa_asn, args.roa_prefix)
        if not object_ids:
            print("no matching ROA objects found", file=sys.stderr)
            sys.exit(2)
    elif args.cert_serial_dec or args.cert_serial_hex:
        object_ids = find_cert_object_ids_by_serial(args.warehouse, args.cert_serial_dec, args.cert_serial_hex)
        if not object_ids:
            print("no matching certificate objects found", file=sys.stderr)
            sys.exit(2)

    # Map object_id -> object_hash
    targets: List[Tuple[int, str]] = []
    if args.object_hash:
        # validate hex and proceed without kv
        _ = hex_to_bin(args.object_hash)  # will raise if malformed
        targets = [(-1, args.object_hash.lower())]
    else:
        for oid in object_ids:
            # try the type table first if user hinted a type
            hash_hex = None
            if args.type:
                hash_hex = find_object_hash_from_type_table(args.warehouse, args.type, oid)
            if not hash_hex:
                hash_hex = kv_lookup_hash_by_object_id(args.kv, oid)
            if not hash_hex:
                print(f"object_id {oid}: cannot resolve object_hash", file=sys.stderr)
                continue
            targets.append((oid, hash_hex.lower()))

    if not targets:
        print("nothing to do", file=sys.stderr)
        sys.exit(0)

    # For each hash, fetch stored path or bytes from CAS
    for oid, h in targets:
        stored_path, cbytes, orig_len = cas_sqlite_lookup(args.cas_sqlite_root, h)
        header = f"object_id={oid}" if oid >= 0 else "object_hash"
        if stored_path and args.print_path:
            # Resolve relative path against the mirror root to show a usable path
            p = Path(stored_path)
            if not p.is_absolute():
                # assume relative to the directory you used as --cas-external-root during ingest
                # user can prepend manually if needed
                print(f"{header} hash={h} path(rel)={stored_path}")
            else:
                print(f"{header} hash={h} path={stored_path}")
        if args.materialize:
            out_path = args.materialize
            # If multiple targets, suffix the filename
            if len(targets) > 1:
                stem = out_path.stem
                out_path = out_path.with_name(f"{stem}-{h}.der")
            if stored_path:
                src = Path(stored_path)
                if not src.is_absolute():
                    # user provided a relative CAS path; we cannot guess the external root here
                    print(f"warn: stored path is relative: {stored_path}; copying will likely fail unless cwd matches your mirror root", file=sys.stderr)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                import shutil
                shutil.copyfile(src, out_path)
                print(f"materialized from path -> {out_path} (size={out_path.stat().st_size} bytes)")
            elif cbytes is not None:
                materialize_bytes(cbytes, out_path)
                print(f"materialized from cbytes -> {out_path} (orig_len={orig_len})")
            else:
                print(f"{header} hash={h}: not found in CAS", file=sys.stderr)
        if not args.print_path and not args.materialize:
            # default: print a short summary
            where = f"path={stored_path}" if stored_path else (f"cbytes={len(cbytes)} bytes" if cbytes else "not-found")
            print(f"{header} hash={h} -> {where}")

if __name__ == "__main__":
    main()

