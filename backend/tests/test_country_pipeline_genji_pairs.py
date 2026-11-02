"""genji/bgp_country_pairs.py on a synthetic warehouse (legacy, v2 and snapshot layouts).

Checks the pairs files (schema, rows, json), the legacy label rule, the v2 ledger check,
alias/routable filters, resume, failure recording, the grid and the ingest-window guard.
"""
import datetime as dt
import importlib.util
import json
import os
import sqlite3
import sys

import duckdb
import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, '..', 'pipelines', 'country'))
from crc import common  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    'bgp_country_pairs', os.path.join(HERE, '..', 'pipelines', 'country', 'genji', 'bgp_country_pairs.py'))
bcp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bcp)
REAL_GUARD = bcp.ingest_guard   # the autouse fixture below replaces it for main() runs

LEG_D = dt.date(2020, 6, 3)      # a grid Wednesday
V2_D = dt.date(2026, 2, 4)
V2_BAD = dt.date(2026, 2, 5)     # ledger mismatch
SNAP_D = dt.date(2026, 10, 25)

LEG_COLS = [('prefix_id', 'BIGINT'), ('origin_as', 'UINTEGER'), ('segment_from', 'TIMESTAMP'),
            ('segment_to', 'TIMESTAMP'), ('last_observed_ts', 'TIMESTAMP')]
V2_COLS = [('collector', 'VARCHAR'), ('peer_asn', 'UINTEGER'), ('peer_ip', 'VARCHAR'), ('prefix', 'VARCHAR'),
           ('origin_as', 'UINTEGER'), ('segment_from', 'TIMESTAMP'), ('segment_to', 'TIMESTAMP'),
           ('last_observed_ts', 'TIMESTAMP')]


def _parquet(path, cols, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t ({', '.join(f'{n} {t}' for n, t in cols)})")
    if rows:
        con.executemany(f"INSERT INTO t VALUES ({', '.join('?' * len(cols))})", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
    con.close()


def _dim(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    s = sqlite3.connect(path)
    s.execute('PRAGMA journal_mode=WAL')
    s.execute('CREATE TABLE prefix_dim (prefix_id INTEGER PRIMARY KEY, prefix TEXT NOT NULL)')
    s.executemany('INSERT INTO prefix_dim VALUES (?, ?)', rows)
    s.commit()
    s.close()


def _ts(s):
    return dt.datetime.fromisoformat(s) if s else None


def _jsonl(path, recs):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        for r in recs:
            f.write(json.dumps(r, separators=(',', ':')) + '\n')


def _us(d):
    return int((dt.datetime(d.year, d.month, d.day) - dt.datetime(1970, 1, 1)).total_seconds()) * 1_000_000


def build_legacy(wh):
    L4 = f'{wh}/rib_runs/update_state_runs/ipv4/2020-06'
    rrc04, rrc05, ixbr = f'{L4}/rrc04.parquet', f'{L4}/rrc05.parquet', f'{L4}/ix-br.gru.parquet'
    rv2 = f'{wh}/rib_runs_merged/update_state_runs/ipv4/route-views2/2020.parquet'
    rv6 = f'{wh}/rib_runs/update_state_runs/ipv6/2020-06/route-views6.parquet'
    # rrc04: RIS label 2020-06-02 23:59:59 maps to 2020-06-03
    _dim(f'{wh}/workers/rrc04/dim.sqlite', [(1, '1.0.0.0/24'), (2, '1.0.0.0/25'), (3, '2.0.0.0/16'),
                                            (4, '3.0.0.0/16'), (5, '0.0.0.0/0'), (6, '4.0.0.0/16')])
    _parquet(rrc04, LEG_COLS, [
        (1, 13335, _ts('2020-05-01 23:59:59'), None, _ts('2020-06-10 23:59:59')),   # alive
        (1, 13335, _ts('2020-05-15 23:59:59'), None, _ts('2020-06-03 23:59:59')),   # alive (2nd peer)
        (2, 13335, _ts('2020-05-01 23:59:59'), None, _ts('2020-06-10 23:59:59')),   # /25: not routable
        (3, None, _ts('2020-05-01 23:59:59'), None, _ts('2020-06-10 23:59:59')),    # no origin
        (4, 3, _ts('2020-06-01 23:59:59'), _ts('2020-06-02 23:59:59'), _ts('2020-06-01 23:59:59')),  # ended before T
        (5, 1, _ts('2020-05-01 23:59:59'), None, _ts('2020-06-10 23:59:59')),       # 0.0.0.0/0
        (6, 4, _ts('2020-06-02 23:59:59'), None, _ts('2020-06-02 23:59:59')),       # alive exactly at T
        (1, 99, _ts('2020-06-03 23:59:59'), None, _ts('2020-06-05 23:59:59')),      # starts after T
    ])
    # route-views2 (merged year file, its own dim with different ids): label 2020-06-03 00:00:00
    _dim(f'{wh}/workers/route-views2/dim.sqlite', [(1, '5.0.0.0/16'), (2, '1.0.0.0/24')])
    _parquet(rv2, LEG_COLS, [
        (1, 5, _ts('2020-05-01 00:00:00'), None, _ts('2020-06-03 00:00:00')),
        (2, 13335, _ts('2020-06-03 00:00:00'), None, _ts('2020-06-20 00:00:00')),
        (2, 13335, _ts('2020-06-02 00:00:00'), _ts('2020-06-03 00:00:00'), _ts('2020-06-02 00:00:00')),  # gone at T
    ])
    # route-views6 uses the shared dimensions.sqlite
    _dim(f'{wh}/dimensions.sqlite', [(7, '2001:db8::/32'), (8, '2001:db8::/64')])
    _parquet(rv6, LEG_COLS, [
        (7, 64500, _ts('2020-06-03 00:00:01'), None, _ts('2020-06-04 00:00:01')),
        (8, 64500, _ts('2020-06-03 00:00:01'), None, _ts('2020-06-03 00:00:01')),   # /64: not routable
    ])
    # alias: identical copy of another collector, must be dropped
    _parquet(ixbr, LEG_COLS, [(1, 9, _ts('2020-05-01 00:00:00'), None, _ts('2020-06-10 00:00:00'))])
    # rrc05: owns the day per manifest but has no dump labelled 2020-06-03
    _dim(f'{wh}/workers/rrc05/dim.sqlite', [(1, '6.0.0.0/16')])
    _parquet(rrc05, LEG_COLS, [(1, 6, _ts('2020-05-01 23:59:59'), None, _ts('2020-05-20 23:59:59'))])
    jun = [['2020-06-01', '2020-06-30']]
    man = {'generated_at': 'x', 'bad_files': [{'file': f'{L4}/rrc00.parquet', 'error': 'No magic bytes'}],
           'collectors': {
               'rrc04': {'ipv4': [{'file': rrc04, 'kind': 'monthly', 'days': jun, 'dim': f'{wh}/workers/rrc04/dim.sqlite'}]},
               'route-views2': {'ipv4': [{'file': rv2, 'kind': 'merged_year', 'days': [['2020-01-01', '2020-12-31']],
                                          'dim': f'{wh}/workers/route-views2/dim.sqlite'}]},
               'route-views6': {'ipv6': [{'file': rv6, 'kind': 'monthly', 'days': jun, 'dim': f'{wh}/dimensions.sqlite'}]},
               'ix-br.gru': {'ipv4': [{'file': ixbr, 'kind': 'monthly', 'days': jun, 'dim': f'{wh}/workers/ix-br.gru/dim.sqlite'}]},
               'rrc05': {'ipv4': [{'file': rrc05, 'kind': 'monthly', 'days': jun, 'dim': f'{wh}/workers/rrc05/dim.sqlite'}]},
               'rrc00': {'ipv4': [{'file': f'{L4}/rrc00.parquet', 'kind': 'monthly', 'days': jun,
                                   'dim': f'{wh}/workers/rrc00/dim.sqlite'}]},
           }}
    with open(f'{wh}/legacy_manifest.json', 'w') as f:
        json.dump(man, f)


def build_v2(wh):
    V2 = f'{wh}/v2'
    for c in ('rrc00', 'route-views2', 'ix-br.gru', 'rrc01'):
        os.makedirs(f'{V2}/state/{c}/2026-01-01', exist_ok=True)
        with open(f'{V2}/state/{c}/2026-01-01/meta.json', 'w') as f:
            json.dump({'last_date': '2026-02-10', 'open_file': 'open-2026-02-10.parquet', 'status': 'running'}, f)
    # rrc00
    _parquet(f'{V2}/rib_runs/ipv4/2026-02/rrc00/month.parquet', V2_COLS, [
        ('rrc00', 100, '10.0.0.1', '1.0.0.0/24', 13335, _ts('2026-01-20'), _ts('2026-02-06'), _ts('2026-02-05')),
        ('rrc00', 101, '10.0.0.2', '1.0.0.0/24', 13335, _ts('2026-02-01'), _ts('2026-02-03'), _ts('2026-02-02')),
        ('rrc00', 101, '10.0.0.2', '7.0.0.0/25', 7, _ts('2026-02-01'), _ts('2026-02-08'), _ts('2026-02-07')),
    ])
    # an older month must not be read (it would break the ledger count)
    _parquet(f'{V2}/rib_runs/ipv4/2026-01/rrc00/month.parquet', V2_COLS, [
        ('rrc00', 100, '10.0.0.1', '8.0.0.0/16', 8, _ts('2026-01-01'), None, _ts('2026-02-20'))])
    _parquet(f'{V2}/state/rrc00/2026-01-01/open-2026-02-10.parquet', V2_COLS, [
        ('rrc00', 102, '2001:db8::2', '2001:db8::/32', 64500, _ts('2026-01-15'), None, _ts('2026-02-10')),
        ('rrc00', 100, '10.0.0.1', '9.0.0.0/16', None, _ts('2026-01-15'), None, _ts('2026-02-10')),
        ('rrc00', 103, '10.0.0.3', '1.0.0.0/24', 13335, _ts('2026-02-05'), None, _ts('2026-02-10')),
    ])
    _jsonl(f'{V2}/ledger/rrc00/2026-01-01.jsonl', [
        {'date': '2026-02-04', 'dump_ts': _us(V2_D), 'entries_v4': 3, 'entries_v6': 1, 'status': 'ok'},
        {'date': '2026-02-05', 'dump_ts': _us(V2_BAD), 'entries_v4': 999, 'entries_v6': 1, 'status': 'ok'}])
    _parquet(f'{V2}/peer_days/rrc00/2026-01-01.parquet',
             [('collector', 'VARCHAR'), ('date', 'DATE'), ('peer_asn', 'UINTEGER'), ('peer_ip', 'VARCHAR'),
              ('full_feed_v4', 'BOOLEAN'), ('full_feed_v6', 'BOOLEAN')],
             [('rrc00', V2_D, 100, '10.0.0.1', True, False), ('rrc00', V2_D, 102, '2001:db8::2', False, True),
              ('rrc00', V2_D - dt.timedelta(days=1), 101, '10.0.0.2', True, False)])
    # route-views2: daily files only; two ledger lines for the date, the last one wins
    _parquet(f'{V2}/rib_runs/ipv4/2026-02/route-views2/2026-02-05.parquet', V2_COLS, [
        ('route-views2', 200, '10.1.0.1', '1.0.0.0/24', 13335, _ts('2026-01-01'), _ts('2026-02-05'), _ts('2026-02-04')),
        ('route-views2', 201, '10.1.0.2', '1.0.0.0/24', 13335, _ts('2026-01-01'), _ts('2026-02-05'), _ts('2026-02-04')),
    ])
    _parquet(f'{V2}/state/route-views2/2026-01-01/open-2026-02-10.parquet', V2_COLS, [])
    _jsonl(f'{V2}/ledger/route-views2/2026-01-01.jsonl', [
        {'date': '2026-02-04', 'dump_ts': _us(V2_D), 'entries_v4': 0, 'entries_v6': 0, 'status': 'failed'},
        {'date': '2026-02-04', 'dump_ts': _us(V2_D), 'entries_v4': 2, 'entries_v6': 0, 'status': 'ok'}])
    _parquet(f'{V2}/peer_days/route-views2/2026-01-01.parquet',
             [('collector', 'VARCHAR'), ('date', 'DATE'), ('peer_asn', 'UINTEGER'), ('peer_ip', 'VARCHAR'),
              ('full_feed_v4', 'BOOLEAN'), ('full_feed_v6', 'BOOLEAN')],
             [('route-views2', V2_D, 200, '10.1.0.1', True, False), ('route-views2', V2_D, 201, '10.1.0.2', False, False)])
    # alias with an ok ledger line and rrc01 with no_data: both excluded
    for c, st in (('ix-br.gru', 'ok'), ('rrc01', 'no_data')):
        _parquet(f'{V2}/rib_runs/ipv4/2026-02/{c}/month.parquet', V2_COLS, [
            (c, 300, '10.2.0.1', '11.0.0.0/16', 11, _ts('2026-01-01'), None, _ts('2026-02-10'))])
        _parquet(f'{V2}/state/{c}/2026-01-01/open-2026-02-10.parquet', V2_COLS, [])
        _jsonl(f'{V2}/ledger/{c}/2026-01-01.jsonl', [
            {'date': '2026-02-04', 'dump_ts': _us(V2_D), 'entries_v4': 1, 'entries_v6': 0, 'status': st}])


def build_snapshot(wh):
    d = f'{wh}/snapshots/days/{SNAP_D}'
    _parquet(f'{d}/peers.parquet', [('peer_id', 'UINTEGER'), ('collector', 'VARCHAR'), ('peer_asn', 'UINTEGER'),
                                    ('peer_ip', 'VARCHAR'), ('genji_full_feed_v4', 'BOOLEAN'), ('genji_full_feed_v6', 'BOOLEAN')],
             [(1, 'rrc00', 100, '10.0.0.1', True, False), (2, 'route-views2', 200, '10.1.0.1', False, False),
              (3, 'ix-br.gru', 300, '10.2.0.1', True, True)])
    _parquet(f'{d}/routes.parquet', [('route_id', 'UINTEGER'), ('af', 'UTINYINT'), ('prefix', 'VARCHAR'),
                                     ('origin_as', 'UINTEGER'), ('n_peers', 'UINTEGER'), ('n_collectors', 'USMALLINT')],
             [(1, 4, '1.0.0.0/24', 13335, 3, 3), (2, 4, '1.0.0.0/25', 13335, 1, 1), (3, 6, '2001:db8::/32', 64500, 1, 1),
              (4, 4, '0.0.0.0/0', 1, 1, 1), (5, 4, '3.0.0.0/16', None, 1, 1)])
    _parquet(f'{d}/route_peers.parquet', [('route_id', 'UINTEGER'), ('peer_id', 'UINTEGER')],
             [(1, 1), (1, 2), (1, 3), (2, 1), (3, 1), (4, 1), (5, 2)])
    with open(f'{d}/manifest.json', 'w') as f:
        json.dump({'complete': True, 'missing_collectors': [{'collector': 'route-views.chile'}]}, f)


@pytest.fixture()
def wh(tmp_path):
    root = str(tmp_path / 'wh')
    os.makedirs(root)
    build_legacy(root)
    build_v2(root)
    build_snapshot(root)
    return root


@pytest.fixture(autouse=True)
def no_ingest_sleep(monkeypatch):
    monkeypatch.setattr(bcp, 'ingest_guard', lambda *a, **k: 0.0)
    bcp.STOP['signal'] = None
    yield
    bcp.STOP['signal'] = None


def _args(wh, tmp_path, *extra, sanity=False):
    a = ['--warehouse', wh, '--out', str(tmp_path / 'out'), '--spill', str(tmp_path / 'spill'),
         '--dim-cache', str(tmp_path / 'dimcache'), '--failures', str(tmp_path / 'logs' / 'failures.jsonl'),
         '--threads', '2', '--memory-limit', '1GB']
    if not sanity:
        a += ['--min-rows-v4', '0', '--min-rows-v6', '0', '--min-collectors', '0']
    return a + list(extra)


def _rows(path):
    con = duckdb.connect()
    return con.execute(f"SELECT af, prefix, origin_as, n_peers, n_collectors, n_ff, src FROM read_parquet('{path}')").fetchall()


def _sidecar_files(wh):
    return sorted(os.path.join(dp, f) for dp, _, fs in os.walk(wh) for f in fs if f.endswith(('-wal', '-shm', '-journal')))


def test_all_sources_end_to_end(wh, tmp_path):
    before = _sidecar_files(wh)
    rc = bcp.main(_args(wh, tmp_path, '--date', str(LEG_D), '--date', str(V2_D), '--date', str(V2_BAD),
                        '--date', str(SNAP_D)))
    assert rc == 1                                    # V2_BAD failed, the others were written
    out = str(tmp_path / 'out')

    pq, js = bcp.out_paths(out, LEG_D)
    assert _rows(pq) == [(4, '1.0.0.0/24', 13335, 3, 2, None, 'legacy'), (4, '4.0.0.0/16', 4, 1, 1, None, 'legacy'),
                         (4, '5.0.0.0/16', 5, 1, 1, None, 'legacy'), (6, '2001:db8::/32', 64500, 1, 1, None, 'legacy')]
    meta = json.load(open(js))
    assert meta['src'] == 'legacy' and meta['date'] == '2020-06-03'
    assert meta['collectors'] == {'4': 2, '6': 1} and meta['rows'] == 4 and meta['rows_by_af'] == {'4': 3, '6': 1}
    assert meta['collector_names'] == {'4': ['route-views2', 'rrc04'], '6': ['route-views6']}
    assert any('rrc05' in x for x in meta['detail']['files_without_label'])
    assert meta['extractor'] == f'bgp_country_pairs {bcp.VERSION}'

    pq, js = bcp.out_paths(out, V2_D)
    assert _rows(pq) == [(4, '1.0.0.0/24', 13335, 3, 2, 2, 'v2'), (6, '2001:db8::/32', 64500, 1, 1, 1, 'v2')]
    meta = json.load(open(js))
    assert meta['src'] == 'v2' and meta['collectors'] == {'4': 2, '6': 1} and meta['rows_by_af'] == {'4': 1, '6': 1}
    assert meta['detail']['collectors_missing'] == ['rrc01']

    pq, js = bcp.out_paths(out, SNAP_D)
    assert _rows(pq) == [(4, '1.0.0.0/24', 13335, 2, 2, 1, 'snapshot'), (6, '2001:db8::/32', 64500, 1, 1, 0, 'snapshot')]
    meta = json.load(open(js))
    assert meta['src'] == 'snapshot' and meta['collectors'] == {'4': 2, '6': 1}

    # the failed date: no outputs, one failure record with the ledger mismatch
    assert not any(os.path.exists(p) for p in bcp.out_paths(out, V2_BAD))
    fails = [json.loads(x) for x in open(tmp_path / 'logs' / 'failures.jsonl')]
    assert [f['date'] for f in fails] == ['2026-02-05']
    assert fails[0]['src'] == 'v2' and 'ledger' in fails[0]['error']
    # on 2026-02-05 rrc00 has 4 alive v4 runs (incl. peer 103, starting exactly at that dump), ledger says 999
    assert fails[0]['detail']['mismatch'] == [['rrc00', 4, 4, 999]]

    # atomic writes leave no temp files; the run's spill dir is removed; sqlite stores got no sidecars
    leftovers = [f for dp, _, fs in os.walk(out) for f in fs if f.startswith('.tmp-')]
    assert leftovers == []
    assert os.listdir(tmp_path / 'spill') == []
    assert _sidecar_files(wh) == before


def test_schema_contract(wh, tmp_path):
    assert bcp.main(_args(wh, tmp_path, '--date', str(V2_D))) == 0
    pq, js = bcp.out_paths(str(tmp_path / 'out'), V2_D)
    con = duckdb.connect()
    cols = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{pq}')").fetchall()]
    assert cols == [('af', 'UTINYINT'), ('prefix', 'VARCHAR'), ('origin_as', 'UBIGINT'), ('n_peers', 'UINTEGER'),
                    ('n_collectors', 'USMALLINT'), ('n_ff', 'UINTEGER'), ('src', 'VARCHAR')]
    meta = json.load(open(js))
    for k in ('date', 'src', 'collectors', 'rows', 'rows_by_af', 'generated_at', 'extractor'):
        assert k in meta
    assert set(meta['collectors']) == {'4', '6'} and set(meta['rows_by_af']) == {'4', '6'}
    assert dt.datetime.strptime(meta['generated_at'], '%Y-%m-%dT%H:%M:%SZ')
    # same layout as crc.common.pairs_path under <root>/pairs
    root = str(tmp_path)
    assert (pq, js) == (common.pairs_path(V2_D, root).replace(os.path.join(root, 'pairs'), str(tmp_path / 'out')),
                        common.pairs_meta_path(V2_D, root).replace(os.path.join(root, 'pairs'), str(tmp_path / 'out')))


def test_resume_skips_done_dates_and_retries_failures(wh, tmp_path):
    argv = _args(wh, tmp_path, '--date', str(V2_D), '--date', str(V2_BAD))
    assert bcp.main(argv) == 1
    pq, js = bcp.out_paths(str(tmp_path / 'out'), V2_D)
    m1 = (os.stat(pq).st_mtime_ns, os.stat(js).st_mtime_ns)
    assert bcp.main(argv) == 1                       # V2_D skipped, V2_BAD retried and failed again
    assert (os.stat(pq).st_mtime_ns, os.stat(js).st_mtime_ns) == m1
    assert len(open(tmp_path / 'logs' / 'failures.jsonl').readlines()) == 2
    # a parquet without its json is not "done": it is recomputed
    os.unlink(js)
    assert bcp.main(_args(wh, tmp_path, '--date', str(V2_D))) == 0
    assert os.path.exists(js)
    # --force recomputes
    assert bcp.main(_args(wh, tmp_path, '--date', str(V2_D), '--force')) == 0
    assert os.stat(js).st_mtime_ns != m1[1]


def test_forced_sources(wh, tmp_path):
    out = str(tmp_path / 'out')
    # v2 path on the snapshot day: no ledger line there -> fails; v2 before 2026 -> fails
    assert bcp.main(_args(wh, tmp_path, '--date', str(SNAP_D), '--source', 'v2')) == 1
    assert bcp.main(_args(wh, tmp_path, '--date', str(LEG_D), '--source', 'v2')) == 1
    assert bcp.main(_args(wh, tmp_path, '--date', str(V2_D), '--source', 'snapshot')) == 1
    assert not os.path.exists(bcp.out_paths(out, SNAP_D)[0])
    fails = [json.loads(x) for x in open(tmp_path / 'logs' / 'failures.jsonl')]
    assert [f['src'] for f in fails] == ['v2', 'v2', 'snapshot']
    assert bcp.pick_source('auto', SNAP_D, wh) == 'snapshot'
    assert bcp.pick_source('auto', V2_D, wh) == 'v2'
    assert bcp.pick_source('auto', LEG_D, wh) == 'legacy'


def test_sanity_floor_fails_the_date(wh, tmp_path):
    assert bcp.main(_args(wh, tmp_path, '--date', str(V2_D), sanity=True)) == 1
    fails = [json.loads(x) for x in open(tmp_path / 'logs' / 'failures.jsonl')]
    assert 'sanity' in fails[0]['error']
    assert not os.path.exists(bcp.out_paths(str(tmp_path / 'out'), V2_D)[0])


def test_dim_cache_reused_and_stale_cache_rebuilt(wh, tmp_path):
    cache = str(tmp_path / 'dimcache')
    stale = bcp.dim_cache_path(cache, f'{wh}/workers/rrc04/dim.sqlite', wh)
    assert stale == os.path.join(cache, 'workers__rrc04__dim.parquet')
    os.makedirs(cache)
    duckdb.connect().execute(f"COPY (SELECT 1::BIGINT prefix_id, '1.0.0.0/24' prefix) TO '{stale}' (FORMAT parquet)")
    assert bcp.main(_args(wh, tmp_path, '--date', str(LEG_D))) == 0
    pq, js = bcp.out_paths(str(tmp_path / 'out'), LEG_D)
    assert json.load(open(js))['detail']['dim_caches_rebuilt'] > 0
    good = _rows(pq)
    assert len(good) == 4
    # with the caches in place the stores are not needed any more
    for c in ('rrc04', 'route-views2', 'rrc05'):
        os.rename(f'{wh}/workers/{c}/dim.sqlite', f'{wh}/workers/{c}/dim.sqlite.moved')
    os.rename(f'{wh}/dimensions.sqlite', f'{wh}/dimensions.sqlite.moved')
    assert bcp.main(_args(wh, tmp_path, '--date', str(LEG_D), '--force')) == 0
    assert _rows(pq) == good


def test_output_sorted(wh, tmp_path):
    assert bcp.main(_args(wh, tmp_path, '--date', str(LEG_D))) == 0
    rows = _rows(bcp.out_paths(str(tmp_path / 'out'), LEG_D)[0])
    assert rows == sorted(rows, key=lambda r: (r[0], r[1], r[2]))


def test_grid_matches_crc_common():
    for a, b in (('2019-12-01', '2026-02-03'), ('2012-01-01', '2026-10-24'), ('2025-12-30', '2026-01-02'),
                 ('2020-01-02', '2020-01-07')):
        assert bcp.grid_dates(a, b) == common.grid(b, a)
    legacy = bcp.grid_dates('2020-01-01', '2025-12-31')
    assert len(legacy) == 314 and legacy[0] == dt.date(2020, 1, 1) and legacy[-1] == dt.date(2025, 12, 31)
    assert all(d.weekday() == 2 for d in legacy)
    v2 = bcp.grid_dates('2026-01-01', '2026-10-24')
    assert len(v2) == 297 and v2[-1] == dt.date(2026, 10, 24)


def test_date_list_drops_pre_bgp_dates(tmp_path):
    a = bcp.parse_args(['--dates-from-grid', '2019-12-01', '2020-01-15', '--date', '2026-03-01'])
    assert bcp.date_list(a) == [dt.date(2020, 1, 1), dt.date(2020, 1, 8), dt.date(2020, 1, 15), dt.date(2026, 3, 1)]


def test_ingest_window():
    U = dt.timezone.utc
    assert bcp.ingest_wait(dt.datetime(2026, 10, 28, 5, 54, 59, tzinfo=U)) == 0
    assert bcp.ingest_wait(dt.datetime(2026, 10, 28, 5, 55, 0, tzinfo=U)) == 55 * 60
    assert bcp.ingest_wait(dt.datetime(2026, 10, 28, 6, 49, 59, tzinfo=U)) == 1
    assert bcp.ingest_wait(dt.datetime(2026, 10, 28, 6, 50, 0, tzinfo=U)) == 0
    assert bcp.ingest_wait(dt.datetime(2026, 10, 28, 23, 0, 0, tzinfo=U)) == 0
    # a non-UTC clock is converted first: 01:30-04:00 is 05:30 UTC
    assert bcp.ingest_wait(dt.datetime(2026, 10, 28, 2, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=-4)))) == 50 * 60
    slept = []
    assert REAL_GUARD(clock=lambda: dt.datetime(2026, 10, 28, 6, 0, tzinfo=U), sleep=slept.append) == 50 * 60
    assert sum(slept) == 50 * 60 and max(slept) <= 30
    slept.clear()
    assert REAL_GUARD(clock=lambda: dt.datetime(2026, 10, 28, 7, 0, tzinfo=U), sleep=slept.append) == 0
    assert slept == []

    # a stop request ends the wait early
    def sleep_then_stop(s):
        slept.append(s)
        if len(slept) == 3:
            bcp.STOP['signal'] = 15
    REAL_GUARD(clock=lambda: dt.datetime(2026, 10, 28, 6, 0, tzinfo=U), sleep=sleep_then_stop)
    assert len(slept) == 3


def test_stop_request_finishes_current_date_without_failure(wh, tmp_path, monkeypatch):
    real = bcp.process_date

    def first_then_signal(con, d, args, ctx):
        r = real(con, d, args, ctx)
        bcp._on_signal(15, None)       # what SIGTERM does while a date is running
        return r
    monkeypatch.setattr(bcp, 'process_date', first_then_signal)
    rc = bcp.main(_args(wh, tmp_path, '--date', str(V2_D), '--date', str(SNAP_D)))
    assert rc == 128 + 15
    out = str(tmp_path / 'out')
    assert all(os.path.exists(p) for p in bcp.out_paths(out, V2_D))
    assert not any(os.path.exists(p) for p in bcp.out_paths(out, SNAP_D))
    assert not os.path.exists(tmp_path / 'logs' / 'failures.jsonl')


def test_interrupted_query_is_a_stop_not_a_failure(wh, tmp_path, monkeypatch):
    def interrupted(con, d, args, ctx):
        raise RuntimeError('Query interrupted')    # how DuckDB reports a signal during a query
    monkeypatch.setattr(bcp, 'process_date', interrupted)
    rc = bcp.main(_args(wh, tmp_path, '--date', str(V2_D), '--date', str(SNAP_D)))
    assert rc == 128 + 2
    assert not os.path.exists(tmp_path / 'logs' / 'failures.jsonl')
    assert os.listdir(tmp_path / 'spill') == []


def test_lock_refuses_second_run(wh, tmp_path):
    import fcntl
    lock = str(tmp_path / 'logs' / 'x.lock')
    os.makedirs(os.path.dirname(lock))
    with open(lock, 'w') as fd:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert bcp.main(_args(wh, tmp_path, '--date', str(V2_D), '--lock', lock)) == 3
    assert bcp.main(_args(wh, tmp_path, '--date', str(V2_D), '--lock', lock)) == 0


def test_dry_run(wh, tmp_path, capsys):
    assert bcp.main(_args(wh, tmp_path, '--dates-from-grid', '2020-01-01', '2025-12-31', '--dry-run')) == 0
    info = json.loads(capsys.readouterr().out)
    assert info['dates'] == 314 and info['already_done'] == 0
