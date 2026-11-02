#!/usr/bin/env python3
"""bgp_country_pairs: routable (prefix, origin) pairs per day with visibility counts, from the lab's
BGP warehouse (the pairs files of the country ROA deployment pipeline, pipelines/country/README.md).

Runs on the BGP warehouse host with the warehouse's own venv python. Needs only duckdb and the standard
library. Read-only on the warehouse (/data/bgp-warehouse); writes only under --out, --dim-cache,
--spill and the failures file.

Output for each date D, under --out (default ~/country-rpki/pairs), written atomically (tmp + rename,
parquet first, json last; a date whose parquet and json both exist is skipped unless --force):
  YYYY/pairs_D.parquet  af UTINYINT, prefix VARCHAR, origin_as UBIGINT, n_peers UINTEGER,
                        n_collectors USMALLINT, n_ff UINTEGER (NULL for legacy), src VARCHAR
                        sorted by af, prefix, origin_as; one row per (af, prefix, origin_as)
  YYYY/pairs_D.json     {"date", "src", "collectors": {"4", "6"}, "rows", "rows_by_af": {"4", "6"},
                         "generated_at", "extractor"} plus rows_np2, seconds, host, collector_names, detail
Rows: IPv4 /1-/24 and IPv6 /1-/48 (0.0.0.0/0 and ::/0 dropped), origin_as not NULL. The alias
collectors ix-br.gru, ix-br2.gru and locix.fra are dropped: they are byte-identical copies of
route-views.saopaulo, route-views2.saopaulo and route-views.amsix.
  n_peers      peer sessions carrying the pair at that day's dump (legacy: RIB rows, which have no
               peer identity; one row is one (peer, prefix) entry)
  n_collectors distinct collectors carrying the pair
  n_ff         peers flagged full feed by the warehouse's rule (>= 75% of the collector's max that day)
  collectors[af] (json) distinct non-alias collectors with >= 1 routable pair of that af on D

Sources (--source auto: snapshot if snapshots/days/D/manifest.json exists, else v2 from 2026-01-01,
else legacy):
  legacy   2020-01-01 .. 2025-12-31 (frozen tree). legacy_manifest.json names the file that owns
           (collector, af, D). The dump label T of D in that file is the latest run timestamp
           (segment_from or last_observed_ts) with date(T + 12h) = D. A row is alive iff
           segment_from <= T <= last_observed_ts. A file with no label for D has no dump that day
           and is skipped. Alive rows are counted with one query per file, with T as a literal:
           joining all files to the small label table sometimes made DuckDB build its hash table on
           the parquet side, which was about 8x slower (2020-10 and several 2021/2022 months).
           prefix_id -> prefix goes through the file's own dimension store, which is
           exported once to --dim-cache with sqlite3 '?immutable=1'. Never mode=ro or DuckDB's sqlite
           scanner: both leave -wal/-shm files next to the live store. (The manifest's own +-12h row
           rule is wrong: it keeps 77,330 instead of 4,560,665 rrc04 rows on 2020-06-01.)
  v2       2026-01-01 onward. Collectors whose ledger line for D (last line wins) has status ok or
           low_count. Runs alive at that ledger dump_ts: segment_from <= dump_ts <
           coalesce(segment_to, last_observed_ts + 1 day), read from the closed files of months
           >= month(D) plus the open file. Per (collector, af) the alive-run count must equal the
           ledger's entries_v4/entries_v6; otherwise the date fails (and is recorded).
  snapshot 2026-10-25 onward: snapshots/days/D route_peers x peers x routes, recounted without
           the aliases (the snapshot's own n_peers/n_collectors include them).

A failed date never stops the run. It is logged, appended to --failures as one JSON line, and
retried on the next run because it has no outputs. Exit status 1 if any date failed.
SIGTERM or SIGINT finishes the current date and then exits with status 128 + signal number; a
second signal aborts the current date. A stop is never recorded as a failure. (A handler that
raised would not work: DuckDB polls Python signal handlers during a query and turns the exception
into 'Query interrupted'.)
Before every date, if the UTC time is in [05:55, 06:50), the run sleeps until 06:50 UTC. This
keeps it clear of the warehouse's daily ingest, compaction and snapshot export, which replace v2 open
files.
One line per date goes to stderr: '<utc> ok|FAIL|skip {json}' (date, src, rows, rows_by_af,
collectors, seconds).

Examples (on the BGP warehouse host; run under nice):
  PY=/path/to/bgp-warehouse/.venv/bin/python
  nice -n 10 $PY -B bgp_country_pairs.py --dates-from-grid 2020-01-01 2025-12-31 --threads 28 --memory-limit 160GB
  nice -n 10 $PY -B bgp_country_pairs.py --date 2026-10-24 --source v2 --out ~/country-rpki/check/v2
"""
import argparse
import collections
import csv
import datetime as dt
import fcntl
import glob
import json
import os
import shutil
import signal
import socket
import sqlite3
import sys
import time
import urllib.parse

import duckdb

EXTRACTOR = 'bgp_country_pairs'
VERSION = '1.0.1'

WAREHOUSE = '/data/bgp-warehouse'
WORK = os.path.expanduser('~/country-rpki')
LIVE_CHAIN = '2026-01-01'               # v2 live chain id (state/<c>/<chain>, ledger/<c>/<chain>.jsonl)
OK_STATUS = ('ok', 'low_count')         # v2 ledger statuses whose dump is used
ALIASES = ('ix-br.gru', 'ix-br2.gru', 'locix.fra')  # = route-views.saopaulo, route-views2.saopaulo, route-views.amsix
SOURCES = ('legacy', 'v2', 'snapshot')

# Time grid (same rule as crc.common.grid): Wednesdays 2012-01-04 .. 2025-12-31, then daily.
WEEKLY_FROM = dt.date(2012, 1, 4)
DAILY_FROM = dt.date(2026, 1, 1)
BGP_FROM = dt.date(2020, 1, 1)
V2_FROM = dt.date(2026, 1, 1)

INGEST_WINDOW = ((5, 55), (6, 50))      # UTC [start, end): never start a date inside it

# Sanity floors per date (override with --min-rows-v4/--min-rows-v6/--min-collectors). The smallest
# legacy day (2020) has ~0.93M v4 / ~0.1M v6 routable pairs and ~28 collectors per af.
MIN_ROWS_V4 = 500_000
MIN_ROWS_V6 = 50_000
MIN_COLLECTORS = 10

EPOCH = dt.datetime(1970, 1, 1)
PLEN = "TRY_CAST(split_part(prefix, '/', 2) AS INTEGER)"
ROUTABLE = (f"(origin_as IS NOT NULL AND ((af = 4 AND {PLEN} BETWEEN 1 AND 24) OR "
            f"(af = 6 AND {PLEN} BETWEEN 1 AND 48)))")
TEMP_TABLES = ('pc', 'pairs', 'lpc0', 'dumps', 'ff')


class DateFailed(Exception):
    """A date cannot be extracted; the run records it and continues with the next date."""

    def __init__(self, msg, detail=None):
        super().__init__(msg)
        self.detail = detail or {}


# ── small helpers ───────────────────────────────────────────────────────────
def to_date(x):
    return x if isinstance(x, dt.date) else dt.date.fromisoformat(str(x)[:10])


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(t=None):
    return (t or utcnow()).strftime('%Y-%m-%dT%H:%M:%SZ')


def lit(s):
    return "'" + str(s).replace("'", "''") + "'"


def lit_list(xs):
    return '[' + ', '.join(lit(x) for x in xs) + ']'


def log(kind, obj=None):
    line = f'{iso_utc()} {kind}'
    if obj is not None:
        line += ' ' + json.dumps(obj, separators=(',', ':'), default=str)
    print(line, file=sys.stderr, flush=True)


def grid_dates(start, end):
    """Grid dates in [start, end]: every Wednesday from 2012-01-04 before 2026-01-01, then every day."""
    start, end = to_date(start), to_date(end)
    out, d = [], WEEKLY_FROM
    while d < DAILY_FROM and d <= end:
        if d >= start:
            out.append(d)
        d += dt.timedelta(days=7)
    d = max(DAILY_FROM, start)
    while d <= end:
        out.append(d)
        d += dt.timedelta(days=1)
    return out


def ingest_wait(now):
    """Seconds to sleep before starting a date at `now` (aware): until 06:50 UTC inside [05:55, 06:50)."""
    t = now.astimezone(dt.timezone.utc)
    (h0, m0), (h1, m1) = INGEST_WINDOW
    lo = t.replace(hour=h0, minute=m0, second=0, microsecond=0)
    hi = t.replace(hour=h1, minute=m1, second=0, microsecond=0)
    return (hi - t).total_seconds() if lo <= t < hi else 0.0


def ingest_guard(clock=utcnow, sleep=time.sleep, step=30.0):
    """Sleep until 06:50 UTC if called inside [05:55, 06:50) UTC; returns the planned wait in seconds.
    Sleeps in steps so that a stop request (SIGTERM/SIGINT) is honoured promptly."""
    wait = ingest_wait(clock())
    if wait > 0:
        log('wait', {'reason': 'BGP warehouse daily ingest window 05:55-06:50 UTC', 'seconds': round(wait)})
        left = wait
        while left > 0 and STOP['signal'] is None:
            s = min(step, left)
            sleep(s)
            left -= s
    return wait


def out_paths(out, d):
    base = os.path.join(out, f'{d.year:04d}', f'pairs_{d.isoformat()}')
    return base + '.parquet', base + '.json'


def connect(threads, memory_limit, spill):
    os.makedirs(spill, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={int(threads)}")
    con.execute(f"SET memory_limit={lit(memory_limit)}")
    con.execute(f"SET temp_directory={lit(spill)}")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    try:
        con.execute("SET TimeZone='UTC'")   # all warehouse timestamps are naive UTC; the host's clock is UTC
    except duckdb.Error:
        pass
    return con


def drop_temp(con):
    for t in TEMP_TABLES:
        con.execute(f"DROP TABLE IF EXISTS {t}")


# ── legacy (2020-01-01 .. 2025-12-31) ───────────────────────────────────────
def legacy_sources(man, d, wh):
    """[(collector, af, file, dim)] for the files that own (collector, af, d)."""
    bad = {b['file'] for b in man.get('bad_files', [])}
    s, out = d.isoformat(), []
    for c, e in sorted(man['collectors'].items()):
        if c in ALIASES:
            continue
        for af in ('ipv4', 'ipv6'):
            own = [f for f in e.get(af, []) if f['file'] not in bad and any(a <= s <= b for a, b in f['days'])]
            if len(own) > 1:
                raise DateFailed(f'legacy: {len(own)} files own {c} {af} on {s}', {'files': [f['file'] for f in own]})
            out += [(c, af, f['file'], f.get('dim') or os.path.join(wh, 'workers', c, 'dim.sqlite')) for f in own]
    return out


def dim_cache_path(dim_cache, dim, wh):
    pre = wh.rstrip('/') + '/'
    rel = dim[len(pre):] if dim.startswith(pre) else dim.lstrip('/')
    name = rel.replace('/', '__')
    if name.endswith('.sqlite'):
        name = name[:-len('.sqlite')]
    return os.path.join(dim_cache, name + '.parquet')


def build_dim_cache(con, dim, path, spill):
    """Export prefix_dim(prefix_id, prefix) of a SQLite dimension store to parquet.

    The store is opened ONLY as 'file:<path>?immutable=1': no locks, no journal, and no -wal/-shm
    files next to the live store. Rows go through a CSV in the spill dir, so pyarrow is not needed."""
    t0 = time.time()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    os.makedirs(spill, exist_ok=True)
    tcsv = os.path.join(spill, f'.dim-{os.getpid()}.csv')
    tpq = os.path.join(os.path.dirname(path), '.tmp-' + os.path.basename(path))
    n = 0
    try:
        s = sqlite3.connect('file:' + urllib.parse.quote(dim) + '?immutable=1', uri=True)
        try:
            cur = s.execute('SELECT prefix_id, prefix FROM prefix_dim')
            with open(tcsv, 'w', newline='') as f:
                w = csv.writer(f)
                while True:
                    rows = cur.fetchmany(200_000)
                    if not rows:
                        break
                    w.writerows(rows)
                    n += len(rows)
        finally:
            s.close()
        con.execute(f"""COPY (SELECT * FROM read_csv({lit(tcsv)}, header=false, auto_detect=false, delim=',',
                              quote='"', escape='"', columns={{'prefix_id': 'BIGINT', 'prefix': 'VARCHAR'}}))
                        TO {lit(tpq)} (FORMAT parquet, COMPRESSION zstd)""")
        m = con.execute(f"SELECT count(*) FROM read_parquet({lit(tpq)})").fetchone()[0]
        if m != n:
            raise RuntimeError(f'dim cache {path}: parquet has {m} rows, store gave {n}')
        os.replace(tpq, path)
    finally:
        for x in (tcsv, tpq):
            if os.path.exists(x):
                os.unlink(x)
    log('dimcache', {'dim': dim, 'cache': path, 'rows': n, 'seconds': round(time.time() - t0, 1)})
    return n


def ts_lit(t):
    return f"TIMESTAMP '{t.isoformat(sep=' ')}'"


def legacy_map(con, caches):
    """lpc0 (collector, dim_no, prefix_id, origin_as, n) -> pc via the dimension caches.

    Only (dim_no, prefix_id) is joined, a high-cardinality key: no join with a tiny table on a
    low-cardinality key (file name), which DuckDB sometimes plans with the big side as the build side."""
    dims_sql = ' UNION ALL '.join(f"SELECT {i} AS dim_no, prefix_id, prefix FROM read_parquet({lit(cp)})"
                                  for i, cp in enumerate(caches))
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pc AS
      SELECT l.collector, CASE WHEN contains(p.prefix, ':') THEN 6 ELSE 4 END::UTINYINT AS af, p.prefix,
             l.origin_as, sum(l.n)::UINTEGER AS n, NULL::UINTEGER AS n_ff
      FROM lpc0 l JOIN ({dims_sql}) p ON p.dim_no = l.dim_no AND p.prefix_id = l.prefix_id
      GROUP BY ALL""")
    want = con.execute("SELECT coalesce(sum(n), 0) FROM lpc0 WHERE prefix_id IS NOT NULL").fetchone()[0]
    got = con.execute("SELECT coalesce(sum(n), 0) FROM pc").fetchone()[0]
    return int(want), int(got)


def legacy_day(con, d, ctx):
    wh = ctx['warehouse']
    if ctx.get('manifest') is None:
        with open(os.path.join(wh, 'legacy_manifest.json')) as f:
            ctx['manifest'] = json.load(f)
    src = legacy_sources(ctx['manifest'], d, wh)
    if not src:
        raise DateFailed('legacy: no manifest file owns this date')
    files = sorted({s[2] for s in src})
    dims = sorted({s[3] for s in src})
    dim_no = {x: i for i, x in enumerate(dims)}
    owner = {f: (c, af, dim_no[dm]) for c, af, f, dm in src}
    lo = f"(TIMESTAMP '{d}' - INTERVAL 36 HOUR)"
    hi = f"(TIMESTAMP '{d}' + INTERVAL 12 HOUR)"
    scan = f"read_parquet({lit_list(files)}, filename=true, union_by_name=true)"
    # dump label of D per file: latest run timestamp with date(ts + 12h) = D
    labels = dict(con.execute(f"""
      SELECT filename, max(ts) AS T FROM (
        SELECT filename, segment_from AS ts FROM {scan} WHERE segment_from >= {lo} AND segment_from < {hi}
        UNION ALL
        SELECT filename, last_observed_ts AS ts FROM {scan} WHERE last_observed_ts >= {lo} AND last_observed_ts < {hi})
      WHERE CAST(ts + INTERVAL 12 HOUR AS DATE) = DATE '{d}'
      GROUP BY filename""").fetchall())
    # rows alive at the label, one query per file with its label as a literal. (A join of all files with
    # the label table was sometimes planned with the parquet side as the hash build side: ~10x slower.)
    con.execute("""CREATE OR REPLACE TEMP TABLE lpc0 (collector VARCHAR, dim_no INTEGER, prefix_id BIGINT,
                   origin_as UBIGINT, n UINTEGER)""")
    for f in files:
        if f not in labels:
            continue
        c, _, k = owner[f]
        t = ts_lit(labels[f])
        con.execute(f"""INSERT INTO lpc0
          SELECT {lit(c)}, {k}, prefix_id, origin_as, count(*)
          FROM read_parquet({lit(f)})
          WHERE segment_from <= {t} AND last_observed_ts >= {t} AND origin_as IS NOT NULL
          GROUP BY prefix_id, origin_as""")
    nolab = sorted((owner[f][0], owner[f][1], f) for f in files if f not in labels)
    # prefix_id -> prefix via each file's own dimension store (parquet cache, built once)
    caches = [dim_cache_path(ctx['dim_cache'], x, wh) for x in dims]
    for x, cp in zip(dims, caches):
        if not os.path.exists(cp):
            build_dim_cache(con, x, cp, ctx['spill'])
    want, got = legacy_map(con, caches)
    rebuilt = []
    if want != got:   # a cache older than the store: rebuild the stores of this date once, then re-map
        rebuilt = [x for x in dims if x not in ctx['rebuilt']]
        for x in rebuilt:
            build_dim_cache(con, x, dim_cache_path(ctx['dim_cache'], x, wh), ctx['spill'])
            ctx['rebuilt'].add(x)
        want, got = legacy_map(con, caches)
    if want != got:
        raise DateFailed(f'legacy: {want - got} alive rows have a prefix_id missing from their dimension store',
                         {'alive_rows': want, 'mapped_rows': got})
    null_ids = con.execute("SELECT coalesce(sum(n), 0) FROM lpc0 WHERE prefix_id IS NULL").fetchone()[0]
    return {'files': len(files), 'dims': len(dims), 'alive_rows': want, 'null_prefix_id_rows': int(null_ids),
            'files_without_label': [f'{c} {af} {f[len(wh) + 1:] if f.startswith(wh + "/") else f}' for c, af, f in nolab],
            'dim_caches_rebuilt': len(rebuilt)}


# ── v2 (2026-01-01 ..) ──────────────────────────────────────────────────────
def v2_collectors(wh):
    return sorted(p.split('/')[-3] for p in glob.glob(os.path.join(wh, 'v2', 'state', '*', LIVE_CHAIN, 'meta.json')))


def v2_ledger(wh, c, d):
    """The collector's ledger record for d (the last line for the date wins), or None."""
    p = os.path.join(wh, 'v2', 'ledger', c, f'{LIVE_CHAIN}.jsonl')
    if not os.path.exists(p):
        return None
    key, hit = d.isoformat(), None
    with open(p) as f:
        for line in f:
            if key not in line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if rec.get('date') == key:
                hit = rec
    return hit


def v2_files(wh, c, d):
    """Closed run files of months >= month(d) (month.parquet, else the daily files) plus the open file."""
    out, floor = [], d.strftime('%Y-%m')
    for af in ('ipv4', 'ipv6'):
        base = os.path.join(wh, 'v2', 'rib_runs', af)
        if not os.path.isdir(base):
            continue
        for month in sorted(os.listdir(base)):
            cdir = os.path.join(base, month, c)
            if month < floor or not os.path.isdir(cdir):
                continue
            mp = os.path.join(cdir, 'month.parquet')
            out += [mp] if os.path.exists(mp) else sorted(glob.glob(os.path.join(cdir, '????-??-??*.parquet')))
    sdir = os.path.join(wh, 'v2', 'state', c, LIVE_CHAIN)
    with open(os.path.join(sdir, 'meta.json')) as f:
        meta = json.load(f)
    if meta.get('open_file') and to_date(meta['last_date']) >= d:
        out.append(os.path.join(sdir, meta['open_file']))
    return out


def v2_day(con, d, ctx):
    wh = ctx['warehouse']
    if d < V2_FROM:
        raise DateFailed(f'v2: no v2 data before {V2_FROM}')
    inc, missing, status = [], [], collections.Counter()
    for c in v2_collectors(wh):
        if c in ALIASES:
            continue
        led = v2_ledger(wh, c, d)
        st = (led or {}).get('status', 'no_ledger_line')
        status[st] += 1
        if st in OK_STATUS:
            inc.append((c, led))
        else:
            missing.append(c)
    if not inc:
        raise DateFailed('v2: no collector has an ok/low_count ledger line for this date', {'status': dict(status)})
    files = [f for c, _ in inc for f in v2_files(wh, c, d)]
    con.execute("CREATE OR REPLACE TEMP TABLE dumps (collector VARCHAR, dump_ts TIMESTAMP)")
    con.executemany("INSERT INTO dumps VALUES (?, ?)",
                    [(c, EPOCH + dt.timedelta(microseconds=int(led['dump_ts']))) for c, led in inc])
    pd_files = [p for p in (os.path.join(wh, 'v2', 'peer_days', c, f'{LIVE_CHAIN}.parquet') for c, _ in inc)
                if os.path.exists(p)]
    if pd_files:
        con.execute(f"""CREATE OR REPLACE TEMP TABLE ff AS
          SELECT collector, peer_asn, peer_ip, bool_or(full_feed_v4) AS full_feed_v4, bool_or(full_feed_v6) AS full_feed_v6
          FROM read_parquet({lit_list(pd_files)}, union_by_name=true) WHERE date = DATE '{d}'
          GROUP BY ALL""")
    else:
        con.execute("""CREATE OR REPLACE TEMP TABLE ff (collector VARCHAR, peer_asn UINTEGER, peer_ip VARCHAR,
                       full_feed_v4 BOOLEAN, full_feed_v6 BOOLEAN)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pc AS
      SELECT r.collector, CASE WHEN contains(r.prefix, ':') THEN 6 ELSE 4 END::UTINYINT AS af, r.prefix, r.origin_as,
             count(*)::UINTEGER AS n,
             count(*) FILTER (WHERE CASE WHEN contains(r.prefix, ':') THEN f.full_feed_v6 ELSE f.full_feed_v4 END)::UINTEGER AS n_ff
      FROM read_parquet({lit_list(files)}, union_by_name=true) r
      JOIN dumps d USING (collector)
      LEFT JOIN ff f USING (collector, peer_asn, peer_ip)
      WHERE r.segment_from <= d.dump_ts
        AND coalesce(r.segment_to, r.last_observed_ts + INTERVAL 1 DAY) > d.dump_ts
        AND r.segment_from < TIMESTAMP '{d}' + INTERVAL 1 DAY
      GROUP BY ALL""")
    got = {(c, int(af)): int(n) for c, af, n in con.execute("SELECT collector, af, sum(n) FROM pc GROUP BY ALL").fetchall()}
    bad = []
    for c, led in inc:
        for af, key in ((4, 'entries_v4'), (6, 'entries_v6')):
            exp = int(led.get(key) or 0)
            if got.get((c, af), 0) != exp:
                bad.append([c, af, got.get((c, af), 0), exp])
    if bad:
        raise DateFailed(f'v2: alive-run count != ledger entries for {len(bad)} collector/af: {bad[:5]}',
                         {'mismatch': bad})
    return {'files': len(files), 'collectors_used': len(inc), 'collectors_missing': missing,
            'ledger_status': dict(status)}


# ── snapshot (2026-10-25 ..) ────────────────────────────────────────────────
def snapshot_dir(wh, d):
    return os.path.join(wh, 'snapshots', 'days', d.isoformat())


def snapshot_day(con, d, ctx):
    base = snapshot_dir(ctx['warehouse'], d)
    mf = os.path.join(base, 'manifest.json')
    if not os.path.exists(mf):
        raise DateFailed('snapshot: no snapshots/days/<D>/manifest.json')
    with open(mf) as f:
        m = json.load(f)
    if m.get('complete') is False:
        raise DateFailed('snapshot: manifest says complete=false')
    rp, pe, ro = (lit(os.path.join(base, x)) for x in ('route_peers.parquet', 'peers.parquet', 'routes.parquet'))
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pc AS
      SELECT p.collector, r.af::UTINYINT AS af, r.prefix, r.origin_as, count(*)::UINTEGER AS n,
             count(*) FILTER (WHERE (r.af = 4 AND p.genji_full_feed_v4) OR (r.af = 6 AND p.genji_full_feed_v6))::UINTEGER AS n_ff
      FROM read_parquet({rp}) x
      JOIN read_parquet({pe}) p USING (peer_id)
      JOIN (SELECT route_id, af, prefix, origin_as FROM read_parquet({ro}) WHERE {ROUTABLE}) r USING (route_id)
      WHERE p.collector NOT IN ({', '.join(lit(a) for a in ALIASES)})
      GROUP BY ALL""")
    return {'manifest_complete': m.get('complete'),
            'missing_collectors': [x.get('collector') for x in (m.get('missing_collectors') or [])]}


EXTRACT = {'legacy': legacy_day, 'v2': v2_day, 'snapshot': snapshot_day}


def pick_source(source, d, wh):
    if source != 'auto':
        return source
    if os.path.exists(os.path.join(snapshot_dir(wh, d), 'manifest.json')):
        return 'snapshot'
    return 'v2' if d >= V2_FROM else 'legacy'


# ── per date ────────────────────────────────────────────────────────────────
def finalize(con):
    """pc (one row per collector, af, prefix, origin) -> pairs + per-af counts."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pairs AS
      SELECT af, prefix, origin_as::UBIGINT AS origin_as, sum(n)::UINTEGER AS n_peers,
             count(DISTINCT collector)::USMALLINT AS n_collectors, sum(n_ff)::UINTEGER AS n_ff
      FROM pc WHERE {ROUTABLE}
      GROUP BY ALL""")
    rows, coll, names = {'4': 0, '6': 0}, {'4': 0, '6': 0}, {'4': [], '6': []}
    for af, n in con.execute("SELECT af, count(*) FROM pairs GROUP BY af").fetchall():
        rows[str(af)] = int(n)
    for af, n, lst in con.execute(f"SELECT af, count(DISTINCT collector), list(DISTINCT collector) FROM pc "
                                  f"WHERE {ROUTABLE} GROUP BY af").fetchall():
        coll[str(af)], names[str(af)] = int(n), sorted(lst)
    np2 = con.execute("SELECT count(*) FROM pairs WHERE n_peers >= 2").fetchone()[0]
    return {'rows': rows['4'] + rows['6'], 'rows_by_af': rows, 'collectors': coll,
            'collector_names': names, 'rows_np2': int(np2)}


def check_sanity(st, args):
    probs = []
    for af, floor in (('4', args.min_rows_v4), ('6', args.min_rows_v6)):
        if st['rows_by_af'][af] < floor:
            probs.append(f'v{af} rows {st["rows_by_af"][af]} < {floor}')
        if st['collectors'][af] < args.min_collectors:
            probs.append(f'v{af} collectors {st["collectors"][af]} < {args.min_collectors}')
    if probs:
        raise DateFailed('sanity: ' + '; '.join(probs), {'rows_by_af': st['rows_by_af'], 'collectors': st['collectors']})


def fsync_file(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_outputs(con, d, src, out, meta):
    pq, js = out_paths(out, d)
    os.makedirs(os.path.dirname(pq), exist_ok=True)
    tpq = os.path.join(os.path.dirname(pq), '.tmp-' + os.path.basename(pq))
    tjs = os.path.join(os.path.dirname(js), '.tmp-' + os.path.basename(js))
    try:
        con.execute(f"""COPY (SELECT af, prefix, origin_as, n_peers, n_collectors, n_ff, {lit(src)}::VARCHAR AS src
                              FROM pairs ORDER BY af, prefix, origin_as)
                        TO {lit(tpq)} (FORMAT parquet, COMPRESSION zstd)""")
        n = con.execute(f"SELECT count(*) FROM read_parquet({lit(tpq)})").fetchone()[0]
        if n != meta['rows']:
            raise DateFailed(f'written parquet has {n} rows, expected {meta["rows"]}')
        fsync_file(tpq)
        os.replace(tpq, pq)
        with open(tjs, 'w') as f:
            json.dump(meta, f, indent=1, sort_keys=True)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        os.replace(tjs, js)
    finally:
        for t in (tpq, tjs):
            if os.path.exists(t):
                os.unlink(t)
    return pq, js


def append_failure(path, rec):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, 'a') as f:
        f.write(json.dumps(rec, separators=(',', ':'), default=str) + '\n')


def process_date(con, d, args, ctx):
    t0 = time.time()
    src = pick_source(args.source, d, ctx['warehouse'])
    try:
        info = EXTRACT[src](con, d, ctx)
        st = finalize(con)
        check_sanity(st, args)
        meta = {'date': d.isoformat(), 'src': src, 'collectors': st['collectors'], 'rows': st['rows'],
                'rows_by_af': st['rows_by_af'], 'generated_at': iso_utc(), 'extractor': f'{EXTRACTOR} {VERSION}',
                'rows_np2': st['rows_np2'], 'collector_names': st['collector_names'], 'detail': info,
                'host': socket.gethostname(), 'seconds': round(time.time() - t0, 1)}
        write_outputs(con, d, src, args.out, meta)
    finally:
        try:
            drop_temp(con)
        except duckdb.Error:
            pass
    return src, meta, round(time.time() - t0, 1)


# ── main ────────────────────────────────────────────────────────────────────
def parse_args(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--date', action='append', default=[], metavar='D', help='one date (repeatable)')
    ap.add_argument('--dates-from-grid', nargs=2, metavar=('A', 'B'),
                    help='all grid dates in [A, B] (Wednesdays before 2026-01-01, daily after)')
    ap.add_argument('--source', choices=('auto',) + SOURCES, default='auto')
    ap.add_argument('--out', default=os.path.join(WORK, 'pairs'), help='pairs root (default %(default)s)')
    ap.add_argument('--threads', type=int, default=28)
    ap.add_argument('--memory-limit', default='160GB')
    ap.add_argument('--spill', default=os.path.join(WORK, 'tmp'),
                    help='DuckDB spill root; each run uses its own subdirectory (default %(default)s)')
    ap.add_argument('--dim-cache', default=os.path.join(WORK, 'dimcache'), help='default %(default)s')
    ap.add_argument('--failures', default=os.path.join(WORK, 'logs', 'failures.jsonl'),
                    help='failed dates are appended here as JSON lines (default %(default)s)')
    ap.add_argument('--warehouse', default=WAREHOUSE, help=argparse.SUPPRESS)
    ap.add_argument('--lock', help='exclusive lock file for this run (refuse to start if held)')
    ap.add_argument('--force', action='store_true', help='recompute dates whose outputs already exist')
    ap.add_argument('--dry-run', action='store_true', help='print the date list and exit')
    ap.add_argument('--min-rows-v4', type=int, default=MIN_ROWS_V4)
    ap.add_argument('--min-rows-v6', type=int, default=MIN_ROWS_V6)
    ap.add_argument('--min-collectors', type=int, default=MIN_COLLECTORS)
    a = ap.parse_args(argv)
    if not a.date and not a.dates_from_grid:
        ap.error('give --date and/or --dates-from-grid')
    return a


def date_list(args):
    days = {to_date(x) for x in args.date}
    if args.dates_from_grid:
        days |= set(grid_dates(*args.dates_from_grid))
    early = sorted(d for d in days if d < BGP_FROM)
    if early:
        log('warn', {'msg': f'dropping {len(early)} dates before {BGP_FROM} (no BGP data)', 'first': early[0]})
    return sorted(d for d in days if d >= BGP_FROM)


STOP = {'signal': None}


def _on_signal(signum, frame):
    """SIGTERM/SIGINT: finish the current date, then stop. A second signal also aborts the current date.

    The first signal only sets a flag. DuckDB polls Python signal handlers while a query runs and turns
    an exception raised there into 'Query interrupted'; the run must not record that as a failed date."""
    if STOP['signal'] is not None:
        raise KeyboardInterrupt
    STOP['signal'] = signum
    log('stop-requested', {'signal': signum, 'msg': 'finishing the current date; send again to abort it'})


def _is_interrupt(e):
    return isinstance(e, (KeyboardInterrupt, getattr(duckdb, 'InterruptException', KeyboardInterrupt))) \
        or 'interrupted' in str(e).lower()


def main(argv=None):
    args = parse_args(argv)
    args.out = os.path.abspath(os.path.expanduser(args.out))
    days = date_list(args)
    done = [d for d in days if all(os.path.exists(p) for p in out_paths(args.out, d))]
    if args.dry_run:
        print(json.dumps({'dates': len(days), 'first': days[0] if days else None, 'last': days[-1] if days else None,
                          'already_done': len(done), 'todo': len(days) - (0 if args.force else len(done))}, default=str))
        return 0
    lock_fd = None
    if args.lock:
        os.makedirs(os.path.dirname(os.path.abspath(args.lock)), exist_ok=True)
        lock_fd = open(args.lock, 'w')
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            log('error', {'msg': f'lock {args.lock} is held by another run'})
            return 3
        lock_fd.write(f'{os.getpid()}\n')
        lock_fd.flush()
    STOP['signal'] = None
    old_handlers = {s: signal.signal(s, _on_signal) for s in (signal.SIGTERM, signal.SIGINT)}
    spill = os.path.join(os.path.expanduser(args.spill), f'{EXTRACTOR}-{os.getpid()}')
    ctx = {'warehouse': args.warehouse.rstrip('/'), 'dim_cache': os.path.expanduser(args.dim_cache),
           'spill': spill, 'manifest': None, 'rebuilt': set()}
    log('start', {'extractor': f'{EXTRACTOR} {VERSION}', 'pid': os.getpid(), 'dates': len(days),
                  'first': days[0] if days else None, 'last': days[-1] if days else None,
                  'already_done': 0 if args.force else len(done), 'source': args.source, 'out': args.out,
                  'threads': args.threads, 'memory_limit': args.memory_limit})
    n_ok = n_skip = n_fail = 0
    t_run = time.time()
    con = None
    try:
        for d in days:
            if STOP['signal'] is not None:
                log('stop', {'signal': STOP['signal'], 'next_date': d})
                break
            if not args.force and all(os.path.exists(p) for p in out_paths(args.out, d)):
                n_skip += 1
                log('skip', {'date': d})
                continue
            ingest_guard()
            if STOP['signal'] is not None:
                log('stop', {'signal': STOP['signal'], 'next_date': d})
                break
            t0 = time.time()
            try:
                if con is None:
                    con = connect(args.threads, args.memory_limit, spill)
                src, meta, secs = process_date(con, d, args, ctx)
                n_ok += 1
                log('ok', {'date': d, 'src': src, 'rows': meta['rows'], 'rows_by_af': meta['rows_by_af'],
                           'collectors': meta['collectors'], 'rows_np2': meta['rows_np2'], 'seconds': secs})
            except (KeyboardInterrupt, SystemExit, Exception) as e:   # noqa: BLE001
                if STOP['signal'] is not None or _is_interrupt(e) or isinstance(e, SystemExit):
                    # a requested stop, not a bad date: no failure record; the date has no outputs
                    STOP['signal'] = STOP['signal'] or signal.SIGINT
                    log('stop', {'signal': STOP['signal'], 'aborted_date': d, 'error': f'{type(e).__name__}: {e}'[:300]})
                    break
                # one bad date must not stop the backfill
                n_fail += 1
                rec = {'date': d.isoformat(), 'src': pick_source(args.source, d, ctx['warehouse']),
                       'error': f'{type(e).__name__}: {e}'[:2000], 'detail': getattr(e, 'detail', {}),
                       'at': iso_utc(), 'extractor': f'{EXTRACTOR} {VERSION}', 'host': socket.gethostname(),
                       'seconds': round(time.time() - t0, 1)}
                try:
                    append_failure(args.failures, rec)
                except OSError as e2:
                    log('error', {'msg': f'cannot append to {args.failures}: {e2}'})
                log('FAIL', rec)
                if con is not None:   # start the next date on a fresh connection
                    try:
                        con.close()
                    except Exception:   # noqa: BLE001
                        pass
                    con = None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:   # noqa: BLE001
                pass
        shutil.rmtree(spill, ignore_errors=True)
        log('done', {'ok': n_ok, 'skipped': n_skip, 'failed': n_fail, 'stopped_by_signal': STOP['signal'],
                     'seconds': round(time.time() - t_run, 1)})
        for s, h in old_handlers.items():
            signal.signal(s, h)
        if lock_fd is not None:
            lock_fd.close()
    if STOP['signal'] is not None:
        return 128 + int(STOP['signal'])
    return 1 if n_fail else 0


if __name__ == '__main__':
    sys.exit(main())
