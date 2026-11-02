#!/usr/bin/env python3
"""
Fast CRL header backfill using manifests + multicore, robust KV join.

Usage:
  PYTHONPATH=. python tools/backfill_crl_headers_from_manifests_fast.py \
    --warehouse warehouse-afrinic \
    --kv warehouse-shared/kv.sqlite \
    --cas-sqlite-root warehouse-afrinic/cas-sqlite \
    --cas-external-root /ssd-data/rpki-objects/unpacked \
    --tal-id AFRINIC --repo-id ftp.ripe.net \
    --workers 40 --rows-per-file 300000 --shard-batch 30000
"""

from __future__ import annotations
import argparse, os, sqlite3, zstandard as zstd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# optional fast header parser
_FAST = True
try:
    from asn1crypto import x509
except Exception:
    _FAST = False

try:
    from parse_crl import parse_crl
except Exception as e:
    raise SystemExit("Import parse_crl failed; ensure PYTHONPATH=. and run from repo root") from e

try:
    from util import clean_dn
except Exception:
    def clean_dn(s):
        if s is None: return None
        t=str(s).strip()
        for p in ("Serial Number:", "Common Name :", "Common Name:", "CN=", "SN="):
            if t.startswith(p): return t[len(p):].strip()
        return t

def duck(threads:int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute("PRAGMA enable_progress_bar=false")
    return con

def load_crl_hex_hashes(tables_root: Path) -> List[str]:
    """
    Return 64-char lower hex sha256 file hashes for .crl entries.
    - Works whether file_hash is BLOB or VARCHAR.
    - Does not depend on hash_alg column existence.
    """
    glob = str(tables_root / "table=manifests_entries" / "**" / "*.parquet")
    con = duck(max(1, os.cpu_count() or 8))
    df = con.execute(f"""
        WITH raw AS (
          SELECT
            LOWER(
              CASE
                WHEN typeof(file_hash)='BLOB' THEN hex(file_hash)
                ELSE regexp_replace(regexp_replace(file_hash, '^0x',''), '[^0-9A-Fa-f]', '')
              END
            ) AS h,
            filename
          FROM read_parquet('{glob}')
          WHERE LOWER(filename) LIKE '%.crl'
        )
        SELECT DISTINCT h
        FROM raw
        WHERE length(h)=64
    """).fetch_df()
    if df.empty:
        return []
    out = []
    for s in df["h"].tolist():
        if isinstance(s, str) and len(s) == 64:
            try:
                bytes.fromhex(s)
                out.append(s)
            except Exception:
                pass
    return out

def kv_bulk_map_hash_to_id_hex(kv_path: Path, hashes_hex: List[str]) -> Dict[str, int]:
    """
    Robust KV join: compare against lower(objects.object_hash), which is TEXT hex in your KV.
    """
    if not hashes_hex:
        return {}
    con = sqlite3.connect(str(kv_path))
    try:
        con.execute("PRAGMA temp_store=MEMORY")
        con.execute("PRAGMA synchronous=OFF")
        con.execute("CREATE TEMP TABLE t_hash(h TEXT PRIMARY KEY)")
        con.executemany("INSERT INTO t_hash(h) VALUES (?)", [(h,) for h in hashes_hex])
        rows = con.execute("""
            SELECT t.h, o.object_id
            FROM t_hash AS t
            JOIN objects AS o
              ON lower(o.object_hash) = t.h
        """).fetchall()
        return {h: int(oid) for (h, oid) in rows}
    finally:
        con.close()

def schema_crls() -> pa.Schema:
    return pa.schema([
        ("crl_id",        pa.int32()),
        ("object_id",     pa.int32()),
        ("issuer_dn",     pa.string()),
        ("crl_number",    pa.string()),
        ("this_update",   pa.timestamp("us", tz="UTC")),
        ("next_update",   pa.timestamp("us", tz="UTC")),
        ("revoked_count", pa.int32()),
        ("aki",           pa.string()),
        ("signature_alg", pa.string()),
        ("repo_id",       pa.string()),
        ("tal_id",        pa.string()),
    ])

def fast_header(raw: bytes) -> Optional[dict]:
    if not _FAST:
        return None
    try:
        cl = x509.CertificateList.load(raw)
        tbs = cl["tbs_cert_list"]
        issuer = tbs["issuer"].human_friendly
        this_update = tbs["this_update"].native
        next_update = tbs["next_update"].native if tbs["next_update"].native else None
        rc = tbs["revoked_certificates"]
        revoked_count = 0 if rc.native in (None, []) else len(rc)
        sig_alg = cl["signature_algorithm"]["algorithm"].dotted
        aki = None
        crl_number = None
        exts = tbs["crl_extensions"]
        if exts.native not in (None, []):
            for ext in exts:
                oid = ext["extn_id"].dotted
                if oid == "2.5.29.35":  # AKI
                    v = ext["extn_value"].parsed
                    try:
                        kb = v.native.get("key_identifier")
                        aki = kb.hex() if isinstance(kb, bytes) else None
                    except Exception:
                        pass
                elif oid == "2.5.29.20":  # CRL Number
                    try:
                        crl_number = str(ext["extn_value"].parsed.native)
                    except Exception:
                        pass
        return {
            "issuer_dn": issuer,
            "this_update": this_update,
            "next_update": next_update,
            "revoked_count": revoked_count,
            "aki": aki,
            "crl_number": crl_number,
            "signature_alg": sig_alg,
        }
    except Exception:
        return None

# worker state
_W_OUT: Path | None = None
_W_SCHEMA: pa.Schema | None = None
_W_ROWS_PER_FILE: int = 200000
_W_REPO: str | None = None
_W_TAL: str | None = None
_W_CAS_ROOT: Path | None = None
_W_EXT_ROOT: Path | None = None
_W_CONN: Dict[str, sqlite3.Connection] = {}
_W_ZSTD: zstd.ZstdDecompressor | None = None

def worker_init(out_dir: Path, rows_per_file: int,
                repo_id: Optional[str], tal_id: Optional[str],
                cas_root: Path, ext_root: Optional[Path]) -> None:
    global _W_OUT, _W_SCHEMA, _W_ROWS_PER_FILE, _W_REPO, _W_TAL, _W_CAS_ROOT, _W_EXT_ROOT, _W_CONN, _W_ZSTD
    _W_OUT = out_dir
    _W_SCHEMA = schema_crls()
    _W_ROWS_PER_FILE = rows_per_file
    _W_REPO = repo_id
    _W_TAL = tal_id
    _W_CAS_ROOT = cas_root
    _W_EXT_ROOT = ext_root
    _W_CONN = {}
    _W_ZSTD = zstd.ZstdDecompressor()

def _conn_for_shard(sh: str) -> sqlite3.Connection:
    c = _W_CONN.get(sh)
    if c:
        return c
    db = _W_CAS_ROOT / f"shard-{sh}.db"  # type: ignore[operator]
    uri = f"file:{db.as_posix()}?immutable=1&mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None, check_same_thread=False)
    try:
        conn.execute("PRAGMA mmap_size=1073741824")
        conn.execute("PRAGMA page_size=32768")
        conn.execute("PRAGMA cache_size=-200000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA journal_mode=OFF")
    except Exception:
        pass
    _W_CONN[sh] = conn
    return conn

def _load_bytes(cbytes: bytes | None, spath: str | None) -> Optional[bytes]:
    if spath:
        p = Path(spath)
        if not p.is_absolute():
            base = _W_EXT_ROOT if _W_EXT_ROOT else _W_CAS_ROOT.parent  # type: ignore[union-attr]
            p = (base / p).resolve()
        try:
            return p.read_bytes()
        except Exception:
            return None
    if cbytes:
        try:
            return _W_ZSTD.decompress(cbytes)  # type: ignore[arg-type]
        except Exception:
            return None
    return None

def _write_part(rows: List[dict], widx: int, seq: int) -> None:
    tbl = pa.Table.from_pylist(rows, schema=_W_SCHEMA)
    path = _W_OUT / f"part-fast-w{widx:02d}-{seq:05d}.parquet"  # type: ignore[operator]
    pq.write_table(tbl, path, compression="zstd")

def worker_job(widx: int, shard_hex: str, items: List[Tuple[int, str]]) -> Tuple[int,int,int]:
    ok = parts = fail = 0
    buf: List[dict] = []
    conn = _conn_for_shard(shard_hex)
    for i in range(0, len(items), 900):
        sub = items[i:i+900]
        keys = [sqlite3.Binary(bytes.fromhex(h)) for _, h in sub]
        q = f"SELECT key, cbytes, path FROM objects WHERE key IN ({','.join(['?']*len(keys))})"
        rows = conn.execute(q, keys).fetchall()
        by_key = {k.hex(): (cbytes, path) for (k, cbytes, path) in rows}
        for oid, h in sub:
            pair = by_key.get(h)
            if not pair:
                fail += 1
                continue
            raw = _load_bytes(pair[0], pair[1])
            if not raw:
                fail += 1
                continue
            header = fast_header(raw) if _FAST else None
            if header is None:
                try:
                    crl = parse_crl(raw)
                    header = getattr(crl, "header", None) or (crl.get("header") if isinstance(crl, dict) else None)
                except Exception:
                    header = None
            if header is None:
                fail += 1
                continue
            buf.append({
                "crl_id": int(oid),
                "object_id": int(oid),
                "issuer_dn": clean_dn(header.get("issuer_dn")),
                "crl_number": header.get("crl_number"),
                "this_update": header.get("this_update"),
                "next_update": header.get("next_update"),
                "revoked_count": int(header.get("revoked_count") or 0),
                "aki": header.get("aki"),
                "signature_alg": header.get("signature_alg"),
                "repo_id": _W_REPO,
                "tal_id": _W_TAL,
            })
            ok += 1
            if len(buf) >= _W_ROWS_PER_FILE:
                _write_part(buf, widx, parts); parts += 1; buf.clear()
    if buf:
        _write_part(buf, widx, parts); parts += 1; buf.clear()
    return (ok, parts, fail)

def main():
    ap = argparse.ArgumentParser(description="Fast CRL header backfill via manifests (robust KV hex join)")
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--kv", type=Path, required=True)
    ap.add_argument("--cas-sqlite-root", type=Path, required=True)
    ap.add_argument("--cas-external-root", type=Path, default=None)
    ap.add_argument("--tal-id", type=str, default=None)
    ap.add_argument("--repo-id", type=str, default=None)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 8) - 2))
    ap.add_argument("--rows-per-file", type=int, default=200000)
    ap.add_argument("--shard-batch", type=int, default=20000)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    tables = args.warehouse / "tables"
    out_dir = tables / "table=crls"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) hashes from manifests
    mh = load_crl_hex_hashes(tables)
    if not mh:
        print("No .crl sha256 hashes found in manifests; nothing to do")
        return
    if args.limit:
        mh = mh[:args.limit]
    print(f"Manifests: {len(mh)} distinct CRL sha256 hashes")

    # 2) KV mapping by lower(objects.object_hash)
    h2id = kv_bulk_map_hash_to_id_hex(args.kv, mh)
    if not h2id:
        print("KV join produced 0 matches; quick KV probe:")
        print("  PRAGMA table_info(objects);")
        print("  SELECT lower(object_hash), length(object_hash) FROM objects LIMIT 5;")
        return
    crl_ids = sorted(set(h2id.values()))
    print(f"Resolved {len(crl_ids)} CRL object_ids via KV")

    # 3) exclude already present headers
    con = duck(max(1, os.cpu_count() or 8))
    try:
        existing = con.execute(
            f"SELECT DISTINCT COALESCE(crl_id, object_id)::INT AS crl_id "
            f"FROM read_parquet('{str(out_dir / '**' / '*.parquet')}')"
        ).fetch_df()
        have = set(int(x) for x in existing["crl_id"].tolist())
    except Exception:
        have = set()
    crl_ids = [oid for oid in crl_ids if oid not in have]
    if not crl_ids:
        print("All target CRL headers already present; nothing to do")
        return
    print(f"Remaining to backfill: {len(crl_ids)}")

    # 4) build id->hash; shard partition
    id2hash: Dict[int, str] = {}
    for h, oid in h2id.items():
        if oid in crl_ids:
            id2hash[oid] = h
    shard_map: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    for oid, h in id2hash.items():
        shard_map[h[:2]].append((oid, h))

    tasks: List[Tuple[int, str, List[Tuple[int,str]]]] = []
    widx = 0
    for sh, items in shard_map.items():
        for i in range(0, len(items), args.shard_batch):
            tasks.append((widx % max(1, args.workers), sh, items[i:i+args.shard_batch]))
            widx += 1
    print(f"Tasks: {len(tasks)} across {len(shard_map)} shards; workers={args.workers}")

    # 5) run workers
    ctx = mp.get_context("forkserver")
    start = datetime.now()
    total_ok = parts = fails = 0
    with ProcessPoolExecutor(
        max_workers=args.workers, mp_context=ctx,
        initializer=worker_init,
        initargs=(out_dir, args.rows_per_file, args.repo_id, args.tal_id,
                  args.cas_sqlite_root, args.cas_external_root)
    ) as pool:
        futs = [pool.submit(worker_job, wi, sh, batch) for (wi, sh, batch) in tasks]
        completed = 0
        tick = max(1, len(tasks)//40)
        for fut in as_completed(futs):
            ok, p, f = fut.result()
            total_ok += ok; parts += p; fails += f
            completed += 1
            if completed % tick == 0:
                elapsed = (datetime.now() - start).total_seconds()
                rate = total_ok / max(1.0, elapsed)
                print(f"progress {completed}/{len(tasks)} ok={total_ok} parts={parts} fail={fails} rate={rate:.1f}/s", flush=True)

    elapsed = (datetime.now() - start).total_seconds()
    rate = total_ok / max(1.0, elapsed)
    print(f"Done. wrote={total_ok} parts={parts} fail={fails} elapsed={elapsed:.1f}s rate={rate:.1f}/s")

if __name__ == "__main__":
    main()

