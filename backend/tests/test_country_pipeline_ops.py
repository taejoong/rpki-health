"""Operations of the country pipeline: snapshot_pairs under network faults, assemble publishing
(idempotency, publish gate, cleanup, freshness warnings), per-connection DuckDB spill directories, and
run_daily.sh outcomes with a stub interpreter. Each test is a regression test for a problem that was
reproduced on 2026-10-28 (see the docstrings).
"""
import datetime as dt
import hashlib
import http.server
import json
import os
import subprocess
import sys
import textwrap
import threading
import time

import duckdb
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipelines', 'country'))
from crc import assemble, common, compute_day, intervals, snapshot_pairs  # noqa: E402

PIPE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pipelines', 'country')
RUN_DAILY = os.path.join(PIPE, 'run_daily.sh')
DAY = dt.date(2026, 10, 26)
KEY = 'dummy-test-key'


# ── fake rib snapshot API ─────────────────────────────────────────────────────
def _parquet(path, sql):
    con = duckdb.connect()
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
    con.close()


class FakeAPI:
    """127.0.0.1 snapshot API. `faults` counts how many of the next requests of a kind misbehave:
    trunc (Content-Length sent, half the body, close), chunk (chunked body cut off mid-stream),
    e500 (HTTP 500 for a file), stale (a file with the right size but other bytes: the snapshot was
    regenerated between manifest and file), list503 (HTTP 503 for the listing)."""

    def __init__(self, files, days):
        self.files, self.days, self.faults, self.hits = files, days, {}, []
        api = self

        class H(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *a):
                pass

            def _send(self, code, body):
                self.send_response(code)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _cut(self, head, body):
                self.send_response(200)
                for k, v in head:
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
                self.close_connection = True
                self.connection.shutdown(2)

            def do_GET(self):
                api.hits.append(self.path)
                if self.headers.get('X-API-Key') != KEY:
                    return self._send(401, b'{}')
                parts = self.path.split('/')
                if self.path == '/api/v1/snapshots':
                    if api.take('list503'):
                        return self._send(503, b'{}')
                    return self._send(200, json.dumps({'snapshots': [
                        {'date': d, 'complete': True, 'generated_at': f'{d}T06:26:11Z'} for d in api.days]}).encode())
                if len(parts) != 7 or parts[5] != 'files' or parts[4] not in api.days:
                    return self._send(404, b'{}')
                name = parts[6]
                if name == 'manifest.json':
                    return self._send(200, json.dumps({
                        'date': parts[4], 'complete': True, 'generated_at': f'{parts[4]}T06:26:11Z',
                        'files': {n: {'bytes': len(b), 'sha256': hashlib.sha256(b).hexdigest()}
                                  for n, b in api.files.items()}}).encode())
                body = api.files[name]
                if api.take('e500'):
                    return self._send(500, b'{}')
                if api.take('trunc'):
                    return self._cut([('Content-Length', str(len(body)))], body[:len(body) // 2])
                if api.take('chunk'):
                    half = body[:len(body) // 2]
                    return self._cut([('Transfer-Encoding', 'chunked')], b'%x\r\n' % len(half) + half + b'\r\n')
                if api.take('stale'):
                    return self._send(200, bytes(255 - c for c in body))
                return self._send(200, body)

        self.srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f'http://127.0.0.1:{self.srv.server_address[1]}/api/v1'
        self.lock = threading.Lock()

    def take(self, kind):
        with self.lock:
            if self.faults.get(kind, 0) > 0:
                self.faults[kind] -= 1
                return True
        return False


@pytest.fixture
def api(tmp_path, monkeypatch):
    d = tmp_path / 'snap'
    d.mkdir()
    _parquet(d / 'peers.parquet', "SELECT CAST(i AS UINTEGER) AS peer_id, 'rrc0' || (i % 4) AS collector, "
             "i % 2 = 0 AS genji_full_feed_v4, false AS genji_full_feed_v6 FROM range(8) t(i)")
    _parquet(d / 'routes.parquet', "SELECT CAST(i AS UINTEGER) AS route_id, CAST(4 AS UTINYINT) AS af, "
             "'10.0.' || i || '.0/24' AS prefix, CAST(64500 + i AS UINTEGER) AS origin_as FROM range(200) t(i)")
    _parquet(d / 'route_peers.parquet', "SELECT CAST(r.i AS UINTEGER) AS route_id, "
             "CAST((r.i + p.j) % 8 AS UINTEGER) AS peer_id FROM range(200) r(i), range(3) p(j)")
    fake = FakeAPI({n: (d / n).read_bytes() for n in ('peers.parquet', 'routes.parquet', 'route_peers.parquet')},
                   ['2026-10-25', DAY.isoformat()])
    key = tmp_path / 'key'
    key.write_text(KEY + '\n')
    monkeypatch.setattr(snapshot_pairs, 'API', fake.url)
    monkeypatch.setattr(snapshot_pairs, 'KEY_FILE', str(key))
    monkeypatch.setattr(snapshot_pairs.time, 'sleep', lambda s: None)    # no backoff waits
    monkeypatch.setattr(snapshot_pairs, 'MIN_ROWS', {'4': 100, '6': 0})   # the synthetic day is tiny
    monkeypatch.setattr(snapshot_pairs, 'MIN_COLLECTORS', 0)
    yield fake
    fake.srv.shutdown()


def _leftovers(root):
    out = []
    for dp, dn, fn in os.walk(root):
        out += [os.path.join(dp, f) for f in fn if f.endswith('.part') or f.startswith('.tmp-')]
    return out


def test_snapshot_retries_truncated_and_cut_off_downloads(api, tmp_path, capsys):
    """Reproduced: a body cut off after half its Content-Length was kept by urllib without an error and
    the day failed on the size check without a retry; a chunked body cut off mid-stream raised
    http.client.IncompleteRead, which was not retried and left a .part file."""
    root = str(tmp_path / 'root')
    api.faults = {'trunc': 1, 'chunk': 1, 'e500': 1}
    assert snapshot_pairs.main(['--root', root, '--date', DAY.isoformat()]) == 0
    err = capsys.readouterr().err
    assert 'ShortRead' in err and 'IncompleteRead' in err and 'HTTP 500' in err
    meta = common.read_json(common.pairs_meta_path(DAY, root))
    assert meta['rows'] == 200 and meta['collectors'] == {'4': 4, '6': 0}
    assert _leftovers(root) == [] and not os.path.exists(os.path.join(root, 'tmp', 'snap_raw', DAY.isoformat()))


def test_snapshot_downloads_again_after_a_manifest_mismatch(api, tmp_path, capsys):
    """A file that does not match the manifest (regenerated upstream between the manifest and the
    file) is deleted and the day is fetched again from a fresh manifest instead of failing the day."""
    root = str(tmp_path / 'root')
    api.faults = {'stale': 1}
    assert snapshot_pairs.main(['--root', root, '--date', DAY.isoformat()]) == 0
    assert f'downloading {DAY} again (2/3)' in capsys.readouterr().err
    assert sum(h.endswith('manifest.json') for h in api.hits) == 2
    api.faults = {'stale': 99}                                  # never matches: the day fails cleanly
    with pytest.raises(RuntimeError, match='sha256 mismatch.*after 3 attempts'):
        snapshot_pairs.download(DAY, str(tmp_path / 'raw'))
    assert os.listdir(tmp_path / 'raw') == ['manifest.json']   # no unverified parquet kept


def test_snapshot_below_the_floors_is_not_written(api, tmp_path, monkeypatch, capsys):
    """Only `rows == 0` was refused, so a partly ingested snapshot (a few collectors) became pairs and then
    the published as_of. The floors match genji/bgp_country_pairs.py (500k/50k pairs, 10 collectors)."""
    root = str(tmp_path / 'root')
    monkeypatch.setattr(snapshot_pairs, 'MIN_COLLECTORS', 10)
    assert snapshot_pairs.main(['--root', root, '--date', DAY.isoformat()]) == 1
    err = capsys.readouterr().err
    assert 'implausible snapshot, not written (IPv4: 200 pairs, 4 collectors; IPv6: 0 pairs, 0 collectors' in err
    assert common.pairs_days(root) == [] and _leftovers(root) == []


def test_snapshot_api_down_is_one_error_line(api, tmp_path, capsys):
    """Reproduced: with the API unreachable, --catch-up ended in a RuntimeError traceback."""
    api.faults = {'list503': 99}
    assert snapshot_pairs.main(['--root', str(tmp_path / 'root'), '--catch-up']) == 1
    err = capsys.readouterr().err
    assert 'ERROR cannot list snapshots' in err and 'giving up after 5 attempts' in err
    assert 'Traceback' not in err


def test_snapshot_catch_up_warns_when_yesterday_is_not_listed(tmp_path, capsys):
    """The BGP warehouse's snapshot of yesterday missing upstream was silent: the run reported OK with an old as_of."""
    listing = [{'date': '2026-10-25', 'generated_at': 'a'}, {'date': '2026-10-26', 'generated_at': 'b'}]
    root = str(tmp_path / 'root')
    assert snapshot_pairs.catch_up_days(root, listing, today=dt.date(2026, 10, 27)) == [dt.date(2026, 10, 25), DAY]
    assert 'WARNING' not in capsys.readouterr().err
    snapshot_pairs.catch_up_days(root, listing, today=dt.date(2026, 10, 28))
    assert 'the snapshot of 2026-10-27 is not listed yet (latest listed: 2026-10-26)' in capsys.readouterr().err


# ── assemble ──────────────────────────────────────────────────────────────────
DELEG_COLS = [('snap', 'DATE'), ('afi', 'UTINYINT'), ('registry', 'VARCHAR'), ('cc', 'VARCHAR'),
              ('status', 'VARCHAR'), ('start_', 'UBIGINT'), ('end_', 'UBIGINT'), ('src', 'VARCHAR')]
VRP_COLS = [('afi', 'UTINYINT'), ('prefix', 'VARCHAR'), ('max_length', 'UTINYINT'), ('asn', 'UBIGINT'),
            ('first_day', 'DATE'), ('last_day', 'DATE'), ('src', 'VARCHAR'), ('bridged_days', 'INTEGER')]
PAIR_COLS = [('af', 'UTINYINT'), ('prefix', 'VARCHAR'), ('origin_as', 'UBIGINT'), ('n_peers', 'UINTEGER'),
             ('n_collectors', 'USMALLINT'), ('n_ff', 'UINTEGER'), ('src', 'VARCHAR')]
DATES = [dt.date(2025, 10, 22), dt.date(2026, 10, 24), dt.date(2026, 10, 25), DAY]


def _table(path, cols, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t ({', '.join(f'{n} {t}' for n, t in cols)})")
    if rows:
        con.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in cols)})", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
    con.close()


def small_world(root, dates=DATES):
    """US (arin) and NL (ripencc) with one route each per date; computed with compute_day."""
    _table(common.vrp_intervals_path(root), VRP_COLS,
           [(4, '10.0.0.0/16', 24, 100, dt.date(2020, 1, 1), dt.date(2026, 12, 31), 'warehouse', 0),
            (6, '2001:db8::/32', 48, 200, dt.date(2020, 1, 1), dt.date(2026, 12, 31), 'warehouse', 0)])
    recs = [(4, 'arin', 'US', 'delegated', 0x0A000000, 0x0A00FFFF), (6, 'ripencc', 'NL', 'delegated',
            0x20010DB800000000, 0x20010DB8FFFFFFFF), (0, 'arin', 'US', 'delegated', 100, 100)]
    _table(common.deleg_path(root), DELEG_COLS,
           [(dt.date(2025, 10, 1),) + r + ('t',) for r in recs] + [(dt.date(2026, 10, 1),) + r + ('t',) for r in recs])
    _table(common.asnames_path(root), [('asn', 'UBIGINT'), ('name', 'VARCHAR'), ('cc', 'VARCHAR')],
           [(100, 'HUNDRED', 'US'), (200, 'TWOHUNDRED', 'NL')])
    for d in dates:
        _table(common.pairs_path(d, root), PAIR_COLS,
               [(4, '10.0.0.0/24', 100, 8, 4, None, 'v2'), (6, '2001:db8::/48', 200, 8, 4, None, 'v2')])
        common.atomic_write_json(common.pairs_meta_path(d, root), {'date': d.isoformat(), 'src': 'v2',
                                 'collectors': {'4': 4, '6': 4}, 'rows': 2, 'generated_at': 'x'})
        compute_day.compute_date(d, root, threads=2, memory_limit='1GB')
    return root


def _build(root, stamp, **kw):
    return assemble.build(root, stamp=stamp, threads=2, memory_limit='1GB', **kw)


def test_assemble_is_idempotent(tmp_path):
    """A rerun on the same daily data writes byte-identical Parquet files (only the stamp and the
    manifest's generated_at differ)."""
    root = small_world(str(tmp_path / 'root'))
    m1, b1 = _build(root, '20261027T074500Z')
    m2, b2 = _build(root, '20261027T074600Z')
    for name in ('series', 'countries', 'ases', 'as_series'):
        with open(os.path.join(b1, f'{name}.parquet'), 'rb') as f1, open(os.path.join(b2, f'{name}.parquet'), 'rb') as f2:
            assert f1.read() == f2.read(), name
    m1.pop('generated_at'), m2.pop('generated_at')
    assert m1 == m2


def test_assemble_gate_keeps_current_and_removes_the_failed_build(tmp_path):
    """A build without the world IPv6 row of as_of is not published: `current` stays on the previous
    build and the failed builds/<stamp>.tmp is removed at once (it used to wait 6 h for prune)."""
    root = small_world(str(tmp_path / 'root'))
    _build(root, '20261027T074500Z')
    geo = os.path.join(common.daily_dir(DAY, root), 'geo.parquet')
    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM read_parquet('{geo}') WHERE NOT (level = 'all' AND af = 6)) "
                f"TO '{geo}.x' (FORMAT parquet)")
    con.close()
    os.replace(geo + '.x', geo)
    with pytest.raises(RuntimeError, match=r"build not published: series has 'all' rows for af \[4\]"):
        _build(root, '20261028T074500Z')
    assert os.readlink(common.current_link(root)) == os.path.join('builds', '20261027T074500Z')
    assert sorted(os.listdir(common.builds_dir(root))) == ['20261027T074500Z']


def test_assemble_freshness_warnings(tmp_path):
    """Late or missing inputs used to publish silently: a stale as_of (the BGP snapshot missing), a VRP
    source that ends before as_of (the RIPE NCC archive day is late), BGP gaps and missing dates."""
    root = str(tmp_path / 'root')
    L = DAY
    g = common.grid(L)
    dones = {d: {'version': compute_day.VERSION} for d in g}
    bgp = [d for d in g if d >= common.BGP_FROM]
    snap = dt.date(2026, 10, 1)
    assert assemble.freshness_warnings(root, L, dones, bgp, snap, today=L + dt.timedelta(days=1)) == []
    # a manual assemble halfway through the recompute after a VERSION bump publishes mixed versions
    old = {**dones, dt.date(2020, 1, 1): {'version': 1}, dt.date(2026, 3, 14): {}}
    assert assemble.freshness_warnings(root, L, old, bgp, snap, today=L + dt.timedelta(days=1)) == [
        f'2 grid date(s) <= as_of were computed by another crc VERSION than {compute_day.VERSION}: '
        '2020-01-01, 2026-03-14 (compute_day --only-missing recomputes them)']
    common.atomic_write_json(os.path.join(common.inputs_dir(root), 'vrp_intervals.json'), {'sources': {
        'warehouse': {'last_day': '2026-10-26'}, 'ripe-archive': {'last_day': '2026-10-25'},
        'apnic-subtal': {'last_day': '2018-04-04'}}})
    del dones[dt.date(2016, 6, 1)]
    bgp.remove(dt.date(2026, 10, 24))
    w = assemble.freshness_warnings(root, L, dones, bgp, dt.date(2026, 8, 1), today=L + dt.timedelta(days=3))
    assert w == ['as_of 2026-10-26 is 3 days old (no BGP snapshot for a later day yet)',
                 '1 grid date(s) <= as_of have no daily data: 2016-06-01',
                 'no BGP pairs for 2026-10-24 (BGP columns are NULL there)',
                 'VRP source ripe-archive ends on 2026-10-25: VRPs of 2026-10-26..2026-10-26 lack it until it '
                 'catches up (the trailing recompute then fixes them)',
                 'delegation snapshot 2026-08-01 is 86 days older than as_of']


def test_assemble_reports_warnings_in_manifest_and_summary(tmp_path, capsys):
    root = small_world(str(tmp_path / 'root'))
    m, _ = _build(root, '20261027T074500Z', today=DAY + dt.timedelta(days=4))
    assert m['build_warnings'][0] == 'as_of 2026-10-26 is 4 days old (no BGP snapshot for a later day yet)'
    assert 'assemble: WARNING as_of 2026-10-26 is 4 days old' in capsys.readouterr().err


def test_publish_and_prune_edge_cases(tmp_path):
    """A stale current.new from a killed run is replaced; prune never deletes the build `current` points
    to, even when it is older than the newest 3; .tmp dirs go after 6 h."""
    root = str(tmp_path / 'root')
    bd = common.builds_dir(root)
    stamps = ['20261020T074500Z', '20261021T074500Z', '20261022T074500Z', '20261023T074500Z', '20261024T074500Z']
    for s in stamps:
        os.makedirs(os.path.join(bd, s))
    os.makedirs(os.path.join(bd, '20261019T074500Z.tmp'))
    os.makedirs(os.path.join(bd, '20261025T074500Z.tmp'))
    old = time.time() - 7 * 3600
    os.utime(os.path.join(bd, '20261019T074500Z.tmp'), (old, old))
    os.symlink('builds/nowhere', common.current_link(root) + '.new')       # left by a killed run
    assemble.publish(root, stamps[0])                                      # e.g. an older --as-of view
    assert os.readlink(common.current_link(root)) == os.path.join('builds', stamps[0])
    assert not os.path.lexists(common.current_link(root) + '.new')
    assert assemble.prune(root, keep=3) == [stamps[1]]
    assert sorted(os.listdir(bd)) == [stamps[0]] + stamps[2:] + ['20261025T074500Z.tmp']


# ── DuckDB spill directories ──────────────────────────────────────────────────
SPILL = textwrap.dedent("""
    import sys
    sys.path.insert(0, sys.argv[1])
    from crc import common
    con = common.connect(threads=2, memory_limit='100MB', tmp=sys.argv[2])
    print(con.execute("SELECT count(*) FROM (SELECT i FROM range(8000000) t(i) ORDER BY hash(i) LIMIT 7000000)").fetchone()[0])
""")


def test_connect_uses_one_spill_directory_per_connection(tmp_path):
    """Reproduced: two processes spilling into one temp_directory failed with 'IO Error: Could not read
    enough bytes from file .../duckdb_temp_storage_DEFAULT-0.tmp' (fixed spill file names; a closing
    instance also deletes every duckdb_temp_* file of the directory). Each connection now has its own
    directory, which DuckDB creates on the first spill and removes on close."""
    base = str(tmp_path / 'spill')
    c1, c2 = common.connect(threads=1, memory_limit='1GB', tmp=base), common.connect(threads=1, memory_limit='1GB', tmp=base)
    d1, d2 = (c.execute("SELECT current_setting('temp_directory')").fetchone()[0] for c in (c1, c2))
    assert d1 != d2 and os.path.dirname(d1) == base and os.path.basename(d1).startswith(f'duckdb-{os.getpid()}-')
    c1.close(), c2.close()
    script = tmp_path / 'spill.py'
    script.write_text(SPILL)
    procs = [subprocess.Popen([sys.executable, '-I', str(script), PIPE, base], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) for _ in range(2)]
    outs = [p.communicate(timeout=120) for p in procs]
    assert [p.returncode for p in procs] == [0, 0], [o[1][-300:] for o in outs]
    assert [o[0].strip() for o in outs] == ['7000000', '7000000']
    assert os.listdir(base) == []                       # each spill directory was removed on close


# ── run_daily.sh ──────────────────────────────────────────────────────────────
STUB = """#!/usr/bin/env bash
# stub interpreter: `stub -m crc.<module> ...` (STUB_FAIL: modules that exit 3; STUB_WARN: assemble warnings)
if [[ $1 == -c ]]; then exec "$REAL_PY" "$@"; fi
mod=${2#crc.}
echo "$mod exe=$0 cwd=$PWD ${*:3}" >> "$STUB_LOG"
case ",${STUB_FAIL:-}," in *",$mod,"*) echo "stub $mod failed" >&2; exit 3 ;; esac
if [[ $mod != assemble ]]; then echo "stdout of $mod"; fi       # like vrp_merge's per-source table
if [[ $mod == assemble ]]; then
  echo '{"as_of": "2026-10-27", "build": "20261028T074700Z", "counts": {}, "prev_date": "2025-10-27", "warnings": ['"${STUB_WARN:-}"']}'
fi
"""


@pytest.fixture
def stub(tmp_path):
    p = tmp_path / 'stubpy'
    p.write_text(STUB)
    p.chmod(0o755)
    return str(p)


def run_daily(stub, tmp_path, *args, cwd=None, **env):
    e = {'PATH': os.environ['PATH'], 'HOME': str(tmp_path), 'PY': stub, 'SUBTAL_PY': stub,
         'REAL_PY': sys.executable, 'STUB_LOG': str(tmp_path / 'stub.log'), **env}
    p = subprocess.run(['bash', RUN_DAILY, *args], env=e, cwd=cwd or str(tmp_path), capture_output=True,
                       text=True, timeout=60)
    return p.returncode, p.stdout.strip(), p.stderr


def test_run_daily_outcomes(stub, tmp_path):
    """Reproduced: a compute_day/assemble failure ended the run without a summary line, with the module's
    exit code (1, the same as PARTIAL). Now: OK 0, PARTIAL 1, FAILED 2 (+ summary line), lock busy 75."""
    root = str(tmp_path / 'root')
    rc, out, err = run_daily(stub, tmp_path, '--root', root)
    assert rc == 0 and ' country-rpki daily: OK in ' in out and '"as_of": "2026-10-27"' in out
    assert '\n' not in out and 'stdout of vrp_merge' in err      # stdout carries only the summary line
    for step in ('vrp_subtal', 'vrp_export', 'ripe_archive', 'vrp_merge', 'deleg', 'asnames', 'snapshot_pairs',
                 'compute_day', 'assemble'):
        assert f'run_daily: done {step}' in err
    rc, out, _ = run_daily(stub, tmp_path, '--root', root, STUB_FAIL='asnames')
    assert rc == 1 and ' PARTIAL in ' in out and 'failed: asnames;' in out
    rc, out, _ = run_daily(stub, tmp_path, '--root', root, STUB_WARN='"as_of 2026-10-26 is 2 days old"')
    assert rc == 1 and ' PARTIAL in ' in out and '1 warning(s)' in out
    rc, out, _ = run_daily(stub, tmp_path, '--root', root, STUB_FAIL='deleg,compute_day')
    assert rc == 2 and ' FAILED in ' in out and 'at compute_day (exit 3)' in out and 'soft failures before: deleg' in out
    rc, out, err = run_daily(stub, tmp_path, '--root', root, STUB_FAIL='assemble')
    assert rc == 2 and 'at assemble (exit 3); current build unchanged' in out
    with open(os.path.join(root, '.lock')) as f:          # a second run while one holds the lock
        import fcntl
        fcntl.flock(f, fcntl.LOCK_EX)
        rc, out, err = run_daily(stub, tmp_path, '--root', root)
    assert rc == 75 and out == '' and 'another run holds' in err
    rc, out, err = run_daily(stub, tmp_path, '--jobs', '0')
    assert rc == 64 and 'usage:' in err


def test_run_daily_trailing_window_covers_late_bridging(stub, tmp_path, monkeypatch):
    """TRAILING defaults to 32 (it was 14). VRP bridging closes a gap of up to 31 days only once the VRPs come
    back (crc.intervals.TIER2_DAYS), so the dates of such a gap change up to 31 days later; with 14 trailing
    dates, gaps longer than 13 days stayed unbridged in daily/. TRAILING and --trailing still
    override it (the option wins), and compute_day --auto has the same default."""
    root = str(tmp_path / 'root')
    log = tmp_path / 'stub.log'

    def compute_args(*args, **env):
        if log.exists():
            log.unlink()
        rc, out, _ = run_daily(stub, tmp_path, '--root', root, *args, **env)
        assert rc == 0 and ' OK in ' in out
        line, = [ln for ln in log.read_text().splitlines() if ln.startswith('compute_day ')]
        return line[line.index(' --root '):]
    assert compute_args() == f' --root {root} --auto --trailing 32 --jobs 2'
    assert compute_args(TRAILING='40').endswith(' --auto --trailing 40 --jobs 2')
    assert compute_args('--trailing', '7', TRAILING='40').endswith(' --auto --trailing 7 --jobs 2')
    assert compute_day.TRAILING == 32 >= intervals.TIER2_DAYS + 1
    seen = []
    monkeypatch.setattr(compute_day, 'auto_dates', lambda r, trailing: seen.append(trailing) or [])
    assert compute_day.main(['--root', root, '--auto']) == 0 and seen == [32]


def test_run_daily_relative_root_and_option_precedence(stub, tmp_path):
    """Reproduced: COUNTRY_RPKI_ROOT=relroot was resolved after `cd pipelines/country`, so the data tree
    and its .lock were created inside the code checkout."""
    work = tmp_path / 'work'
    work.mkdir()
    rc, out, _ = run_daily(stub, tmp_path, '--root=rel', cwd=str(work), COUNTRY_RPKI_ROOT='/nonexistent/unused')
    assert rc == 0
    assert os.path.isdir(work / 'rel' / 'daily') and os.path.exists(work / 'rel' / '.lock')
    assert not os.path.exists(os.path.join(PIPE, 'rel'))
    log = (tmp_path / 'stub.log').read_text()
    assert f'--root {work / "rel"}' in log and '/nonexistent' not in log


def test_run_daily_keeps_the_venv_python_symlink(stub, tmp_path):
    """The first fix resolved PY with `realpath`, which turned the venv's bin/python symlink into the base
    interpreter without the venv's packages (every step: ModuleNotFoundError: No module named 'duckdb').
    PY is made absolute without resolving symlinks."""
    work = tmp_path / 'work'
    (work / 'venv' / 'bin').mkdir(parents=True)
    os.symlink(stub, work / 'venv' / 'bin' / 'python')
    rc, out, _ = run_daily(stub, tmp_path, '--root', 'r', cwd=str(work), PY='venv/bin/python')
    assert rc == 0 and ' OK in ' in out
    lines = [ln for ln in (tmp_path / 'stub.log').read_text().splitlines() if not ln.startswith('vrp_subtal ')]
    assert len(lines) == 8 and all(f' exe={work}/venv/bin/python ' in ln for ln in lines), lines
    # a bare interpreter name is looked up in PATH (an `-x` test would reject it); a missing one is FAILED
    rc, out, _ = run_daily(stub, tmp_path, '--root', 'r', cwd=str(work), PY='python',
                           PATH=f"{work / 'venv' / 'bin'}:{os.environ['PATH']}")
    assert rc == 0 and ' OK in ' in out
    rc, out, err = run_daily(stub, tmp_path, '--root', 'r', cwd=str(work), PY='/nonexistent/python')
    assert rc == 2 and 'FAILED in ' in out and 'at setup' in out and 'is not an executable python' in err


def test_run_daily_removes_only_stale_leftovers(stub, tmp_path):
    """A killed run left daily/YYYY/.tmp-D-PID dirs, pairs/YYYY/.tmp-* files, tmp/snap_raw/D dirs and
    DuckDB spill dirs behind forever. Only this pipeline's own temp names are removed, and only when
    stale (6 h; 3 days for raw snapshots; spill dirs of processes that are gone)."""
    root = tmp_path / 'root'
    stale = [root / 'daily/2026/.tmp-2026-10-26-111', root / 'daily/2026/.old-2026-10-25-222',
             root / 'pairs/2026/.tmp-pairs_2026-10-24.parquet', root / 'tmp/snap_raw/2026-10-20',
             root / 'tmp/duckdb-999999999-0']
    keep = [root / 'daily/2026/.tmp-2026-10-27-333', root / 'daily/2026/2026-10-26',
            root / 'pairs/2026/pairs_2026-10-24.parquet', root / 'tmp/snap_raw/2026-10-27',
            root / f'tmp/duckdb-{os.getpid()}-0', root / 'tmp/devroot', root / 'tmp/other-tool.marker']
    for p in stale + keep:
        if p.suffix in ('.parquet', '.marker'):
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('x')
        else:
            p.mkdir(parents=True)
    old = time.time() - 4 * 86400
    for p in stale + [root / 'daily/2026/2026-10-26', root / 'pairs/2026/pairs_2026-10-24.parquet',
                      root / 'tmp/devroot', root / f'tmp/duckdb-{os.getpid()}-0']:
        os.utime(p, (old, old))
    rc, _, _ = run_daily(stub, tmp_path, '--root', str(root))
    assert rc == 0
    assert [str(p) for p in stale if os.path.exists(p)] == []
    assert [str(p) for p in keep if not os.path.exists(p)] == []
