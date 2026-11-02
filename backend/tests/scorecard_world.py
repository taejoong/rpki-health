"""Synthetic RPKI warehouses for the scorecard tests (Parquet written with DuckDB COPY).

Layout of the production warehouses: <base>/warehouse-<rir>/tables/table=<name>/<name>-part-<n>-1.parquet
plus the <name>.parquet symlink to the newest part (which the pipeline must skip),
snapshots/ts=YYYY-MM-DD HH-MM-SS/metadata.json and indexes/membership/ts=.../objects.roar. Part files get an
mtime one day in the past, so the pipeline's 180-second age filter reads them.

  write_table(base, rir, 'certs/is_ca=true', [{'cert_id': 1, 'ski': 'k1', ...}, ...])
      columns not given are NULL; TIMESTAMPTZ columns take 'YYYY-MM-DD HH:MM:SS' (UTC) strings
  snapshots(base, rir, ['2026-10-19 00:00:01', ...], file_count=1000, repo='rpki-client')
  make_ctx(root, base, as_of, metrics, rirs=...)   a metrics.Ctx for these directories
"""
import datetime as dt
import json
import os
import sys
import time

import duckdb

PIPE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pipelines', 'scorecard')
if PIPE not in sys.path:
    sys.path.insert(0, PIPE)

from scs import common, metrics  # noqa: E402

OLD = time.time() - 86400

_CERT = [('cert_id', 'INTEGER'), ('object_id', 'INTEGER'), ('cert_hash', 'VARCHAR'), ('is_ca_flag', 'BOOLEAN'),
         ('subject_dn', 'VARCHAR'), ('issuer_dn', 'VARCHAR'), ('serial_dec', 'VARCHAR'), ('serial_bin', 'VARCHAR'),
         ('ski', 'VARCHAR'), ('aki', 'VARCHAR'), ('not_before', 'TIMESTAMPTZ'), ('not_after', 'TIMESTAMPTZ'),
         ('spki_alg', 'VARCHAR'), ('spki_bits', 'INTEGER'), ('spki_sha256', 'VARCHAR'), ('resources_ip', 'VARCHAR'),
         ('resources_asn', 'VARCHAR'), ('sia', 'VARCHAR'), ('aia', 'VARCHAR'), ('cdp', 'VARCHAR'),
         ('key_usage', 'VARCHAR'), ('ext_key_usage', 'VARCHAR'), ('basic_constraints', 'VARCHAR'),
         ('repo_id', 'VARCHAR'), ('tal_id', 'VARCHAR')]
SCHEMAS = {
    'crls': [('crl_id', 'INTEGER'), ('object_id', 'INTEGER'), ('issuer_dn', 'VARCHAR'), ('crl_number', 'VARCHAR'),
             ('this_update', 'TIMESTAMPTZ'), ('next_update', 'TIMESTAMPTZ'), ('revoked_count', 'INTEGER'),
             ('aki', 'VARCHAR'), ('signature_alg', 'VARCHAR'), ('repo_id', 'VARCHAR'), ('tal_id', 'VARCHAR')],
    'crl_serials': [('crl_id', 'INTEGER'), ('serial_dec', 'VARCHAR'), ('serial_bin', 'VARCHAR'),
                    ('revocation_date', 'TIMESTAMPTZ'), ('reason_code', 'INTEGER'), ('invalidity_date', 'TIMESTAMPTZ'),
                    ('cert_issuer_dn', 'VARCHAR'), ('revoked_cert_id', 'INTEGER')],
    'certs/is_ca=true': _CERT,
    'certs/is_ca=false': _CERT,
    'manifests': [('object_id', 'INTEGER'), ('manifest_number', 'VARCHAR'), ('this_update', 'VARCHAR'),
                  ('next_update', 'VARCHAR'), ('file_count', 'INTEGER')],
    'roa_objects': [('object_id', 'INTEGER'), ('object_hash', 'VARCHAR'), ('semantic_hash', 'VARCHAR'),
                    ('asn', 'BIGINT'), ('ee_serial', 'VARCHAR'), ('ee_not_before', 'TIMESTAMPTZ'),
                    ('ee_not_after', 'TIMESTAMPTZ'), ('signing_time', 'TIMESTAMPTZ'), ('ee_public_key', 'VARCHAR'),
                    ('repo_id', 'VARCHAR'), ('tal_id', 'VARCHAR')],
    'roa_prefixes': [('object_id', 'INTEGER'), ('afi', 'UTINYINT'), ('prefix', 'VARCHAR'), ('max_length', 'UTINYINT')],
    'object_signatures': [('object_id', 'INTEGER'), ('object_type', 'VARCHAR'), ('signer_cert_id', 'INTEGER'),
                          ('content_type_oid', 'VARCHAR'), ('digest_algorithm', 'VARCHAR'),
                          ('signature_algorithm', 'VARCHAR'), ('signing_time', 'TIMESTAMPTZ'),
                          ('message_digest', 'VARCHAR')],
    'object_presence_runs': [('object_id', 'INTEGER'), ('start_sid', 'INTEGER'), ('end_sid', 'INTEGER'),
                             ('start_ts', 'VARCHAR'), ('end_ts', 'VARCHAR')],
    'object_presence_stats': [('object_id', 'INTEGER'), ('first_sid', 'INTEGER'), ('last_sid', 'INTEGER'),
                              ('first_ts', 'VARCHAR'), ('last_ts', 'VARCHAR'), ('runs_count', 'INTEGER'),
                              ('total_days', 'INTEGER')],
    'object_validity': [('dataset_ts', 'VARCHAR'), ('object_id', 'INTEGER'), ('state', 'UTINYINT'),
                        ('reason_code', 'VARCHAR'), ('tal_id', 'VARCHAR')],
}


def write_table(base, rir, table, rows, part=1, schema=None, symlink=True, mtime=OLD):
    """One Parquet part of a warehouse table from a list of dicts; returns its path."""
    cols = schema or SCHEMAS[table]
    d = common.table_dir(rir, table, base)
    os.makedirs(d, exist_ok=True)
    leaf = table.split('/')[0]
    path = os.path.join(d, f'{leaf}-part-{part:06d}-1.parquet')
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute(f"CREATE TABLE t ({', '.join(f'{c} {t}' for c, t in cols)})")
        if rows:
            con.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in cols)})",
                            [[r.get(c) for c, _ in cols] for r in rows])
        con.execute(f"COPY t TO {common.lit(path)} (FORMAT parquet)")
    finally:
        con.close()
    os.utime(path, (mtime, mtime))
    if symlink:
        link = os.path.join(d, f'{leaf}.parquet')
        if os.path.lexists(link):
            os.unlink(link)
        os.symlink(os.path.basename(path), link)
    return path


def ts_dir(ts):
    """'2026-10-19 00:20:01' -> 'ts=2026-10-19 00-20-01'."""
    if isinstance(ts, dt.datetime):
        ts = ts.strftime('%Y-%m-%d %H:%M:%S')
    return 'ts=' + ts[:10] + ' ' + ts[11:19].replace(':', '-')


def snapshots(base, rir, stamps, file_count=1000, repo=None, metadata=True, membership=True):
    """Snapshot (and membership) directories; file_count: int, or {ts: n}; repo defaults by era."""
    wh = common.warehouse_dir(rir, base)
    for ts in stamps:
        t = ts.strftime('%Y-%m-%d %H:%M:%S') if isinstance(ts, dt.datetime) else ts
        name = ts_dir(t)
        sd = os.path.join(wh, 'snapshots', name)
        os.makedirs(sd, exist_ok=True)
        n = file_count.get(t, 1000) if isinstance(file_count, dict) else file_count
        r = repo or ('ftp.ripe.net' if t.endswith('00:00:00') and t < '2026-04-08' else 'rpki-client')
        if metadata:
            with open(os.path.join(sd, 'metadata.json'), 'w') as f:
                json.dump({'dataset_ts': t, 'repo_id': r, 'tal_id': rir.upper(), 'file_count': n,
                           'object_count': n, 'unique_object_count': n, 'error_count': 0}, f)
        if membership:
            md = os.path.join(wh, 'indexes', 'membership', name)
            os.makedirs(md, exist_ok=True)
            open(os.path.join(md, 'objects.roar'), 'wb').close()


def cycles(day_from, day_to, every_min=20, start='00:00:01'):
    """Era-2 cycle timestamps every `every_min` minutes from day_from to day_to (inclusive days)."""
    out = []
    d = dt.datetime.combine(common.to_date(day_from), dt.time.fromisoformat(start))
    end = dt.datetime.combine(common.to_date(day_to), dt.time(23, 59, 59))
    while d <= end:
        out.append(d.strftime('%Y-%m-%d %H:%M:%S'))
        d += dt.timedelta(minutes=every_min)
    return out


def make_ctx(root, base, as_of, metric_ids, rirs=common.REGISTRIES, **kw):
    kw.setdefault('min_part_age_s', 0)
    kw.setdefault('threads', 2)
    kw.setdefault('memory_limit', '1GB')
    kw.setdefault('code_commit', 'test')
    kw.setdefault('lock_path', os.path.join(root, 'presence.lock'))
    os.makedirs(root, exist_ok=True)
    return metrics.Ctx(root, as_of, metric_ids, base=base, rirs=rirs, **kw)
