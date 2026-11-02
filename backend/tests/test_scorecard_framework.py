"""Framework of the weekly scorecard (pipelines/scorecard/scs): host map and populations, the observation
ledger on a synthetic snapshot directory, status rules, the module interface (emit checks), assemble
(frozen state + trailing recompute, publish gate, idempotency, releases with CHANGES.csv) and the stub
metric end to end through scs.run on a synthetic five-RIR warehouse. Expected values are worked out by hand
in the comments.
"""
import datetime as dt
import hashlib
import importlib.machinery
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
import types

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scorecard_world as W  # noqa: E402
from scs import assemble, classify, common, defs, hostmap, ledger, metrics, run, status  # noqa: E402

D = dt.date
RIRS = common.REGISTRIES


def mondays(a, b):
    return common.weeks(a, b)


# ── registry, host map, populations ──────────────────────────────────────────
def test_registry_is_consistent():
    assert defs.check()
    assert defs.default_metrics() == ['crl_window', 'mft_window', 'crl_empty', 'roa_ee_revocation',
                                      'mft_ee_revocation', 'late_reissue', 'resign_consumed', 'ca_cert_lifetime',
                                      'rp_rejected', 'live_inventory']
    for m in defs.default_metrics():
        d = defs.METRICS[m]
        # rp_rejected 2 (2026-11-02): its 'verdicts' mask also needs 90% of the expected cycles
        assert d['def_version'] == {'rp_rejected': 2}.get(m, 1) and d['method'] and d['stats'], m
        assert d['rfc'] or m == 'stub_crl_count', m
    # status rules
    assert defs.rule_for('roa_ee_revocation', 'revoked_h35_samekey')['final_after'] == 6
    assert defs.rule_for('roa_ee_revocation', 'revoked_h7_ca')['final_after'] == 2
    assert defs.rule_for('roa_ee_revocation', 'early_pairs')['publish_after'] == 2
    assert (defs.rule_for('resign_consumed', 'consumed_p50')['publish_after'],
            defs.rule_for('resign_consumed', 'consumed_p50')['final_after']) == (1, 2)
    assert defs.rule_for('crl_window', 'p50')['mask'] == 'cycles'
    assert defs.rule_for('rp_rejected', 'objects')['mask'] == 'verdicts'
    assert defs.max_final_after('roa_ee_revocation') == 6 and defs.max_final_after('crl_window') == 1
    assert defs.stat_allowed('rp_rejected', 'objects_reason_not_yet_valid')
    assert not defs.stat_allowed('rp_rejected', 'objects_reason_Bad')
    assert {defs.METRICS[m]['module'] for m in defs.default_metrics()} == {'m_crl', 'm_pairs', 'm_mft', 'm_ca', 'm_health'}


def test_hostmap_classes_match_the_expected_hosts_and_live_evidence():
    hm = hostmap.load()
    raw = open(hostmap.HOSTMAP_PATH, 'rb').read()
    assert hm.version == json.loads(raw)['hostmap_version'] == 1
    assert hm.sha256 == hashlib.sha256(raw).hexdigest()
    expected = {'rir_hosted': {'rpki.afrinic.net': 'afrinic', 'rpki.apnic.net': 'apnic', 'rpki.arin.net': 'arin',
                             'repository.lacnic.net': 'lacnic', 'rpki.ripe.net': 'ripencc'},
              'rir_paas': {'rpki.sub.apnic.net': 'apnic', 'rpki-rps.arin.net': 'arin',
                           'rsync.paas.rpki.ripe.net': 'ripencc'},
              'nir_hosted': {'rpki-rsync.idnic.net': 'apnic', 'repo-rpki.idnic.net': 'apnic',
                             'rpki-repository.nic.ad.jp': 'apnic', 'rpki-rps.cnnic.cn': 'apnic',
                             'rpki.cnnic.cn': 'apnic', 'rpkica.twnic.tw': 'apnic', 'rpki-repo.registro.br': 'lacnic'}}
    for cls, hosts in expected.items():
        for h, rir in hosts.items():
            assert hm.hosts[h][:2] == (rir, cls), h
    # added with evidence: IDNIC's and TWNIC's earlier repositories; CERNET hosts are delegated, operator CERNET
    for h in ('rpki.idnic.net', 'rpki-ca.idnic.net'):
        assert hm.hosts[h] == ('apnic', 'nir_hosted', 'IDNIC')
    assert hm.hosts['rpkica.twnic.net.tw'] == ('apnic', 'nir_hosted', 'TWNIC')
    for h in ('rpki.cernet.edu.cn', 'rsync.cernet.net', 'rsync.fiti.net.cn', 'rpki.cernet.net'):
        assert hm.hosts[h] == ('apnic', 'delegated', 'CERNET')
    assert 'rpki.lacnic.net' not in hm.hosts          # the SIA host of no certificate
    assert hm.classify('rpki-rps.arin.net', 'arin') == ('rir_paas', 'ARIN')
    assert hm.classify('RPKI-RPS.ARIN.NET', 'arin') == ('rir_paas', 'ARIN')
    assert hm.classify('rpki-rps.arin.net', 'ripencc') == ('delegated', '')   # not RIPE NCC's service
    assert hm.classify('repo-rpki.idnic.net', 'apnic') == ('nir_hosted', 'IDNIC')
    assert hm.classify('rsync.fiti.net.cn', 'apnic') == ('delegated', '')     # operators only for NIR / PaaS
    assert hm.classify('krill.example.net', 'arin') == ('delegated', '')
    assert hm.classify(None, 'arin') == ('unknown', '') and hm.classify('', 'lacnic') == ('unknown', '')


@pytest.mark.parametrize('patch, msg', [
    ({'hosts': {'RPKI.X.NET': {'rir': 'arin', 'class': 'rir_hosted', 'operator': 'ARIN'}}}, 'lower-case'),
    ({'hosts': {'a.net': {'rir': 'arin', 'class': 'hosted', 'operator': 'ARIN'}}}, 'unknown class'),
    ({'hosts': {'a.net': {'rir': 'ripe', 'class': 'rir_hosted', 'operator': 'X'}}}, 'unknown rir'),
    ({'hostmap_version': '1'}, 'positive integer'),
])
def test_hostmap_rejects_bad_entries(tmp_path, patch, msg):
    doc = {'hostmap_version': 1, 'hosts': {}}
    doc.update(patch)
    p = tmp_path / 'hm.json'
    p.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match=msg):
        hostmap.load(str(p))


def test_classify_sql_matches_python_and_fans_out_populations(tmp_path):
    hm = hostmap.load()
    con = common.connect(threads=1, memory_limit='1GB', tmp=None, root=str(tmp_path / 'unused'))
    try:
        classify.load_hostmap(con, hm)
        hosts = sorted(hm.hosts) + ['krill.example.net', None]
        out = {}
        for rir in RIRS:
            common.values_table(con, 'h', [('id', 'INTEGER'), ('host', 'VARCHAR')], list(enumerate(hosts)))
            for i, cls, op, pop, oper in con.execute(
                    f"SELECT id, pop_class, pop_operator, population, operator FROM ({classify.populations_sql('h', 'host', rir)}) "
                    "ORDER BY ALL").fetchall():
                out.setdefault((rir, hosts[i]), set()).add((pop, oper))
                assert (cls, op) == hm.classify(hosts[i], rir), (rir, hosts[i])
        assert out[('apnic', 'rpki-repository.nic.ad.jp')] == {('nir_hosted', ''), ('all', ''), ('nir_hosted', 'JPNIC')}
        assert out[('arin', 'rpki.arin.net')] == {('rir_hosted', ''), ('all', '')}
        assert out[('arin', 'rpki-rps.arin.net')] == {('rir_paas', ''), ('all', ''), ('rir_paas', 'ARIN')}
        assert out[('ripencc', 'rpki-rps.arin.net')] == {('delegated', ''), ('all', '')}
        assert out[('lacnic', None)] == {('unknown', ''), ('all', '')}
    finally:
        con.close()


def test_ca_keys_take_the_newest_certificate_across_warehouses(tmp_path):
    """A key's host is the SIA host of its newest certificate (a CA that moved to another repository), and
    a CA certificate that sits in another warehouse (its parent's repository) still resolves."""
    base = str(tmp_path / 'wh')
    W.write_table(base, 'apnic', 'certs/is_ca=true', [
        {'cert_id': 1, 'ski': 'k1', 'aki': 'ta', 'is_ca_flag': True, 'not_before': '2020-01-10 00:00:00',
         'sia': 'rsync://rpki-ca.idnic.net/repo/,https://x/notify.xml'},
        {'cert_id': 2, 'ski': 'k1', 'aki': 'ta', 'is_ca_flag': True, 'not_before': '2020-10-22 00:00:00',
         'sia': 'rsync://repo-rpki.idnic.net/repo/'},
        {'cert_id': 3, 'ski': 'k2', 'aki': 'ta', 'is_ca_flag': True, 'not_before': '2024-01-01 00:00:00',
         'sia': 'https://rrdp.only/notify.xml'}])
    W.write_table(base, 'ripencc', 'certs/is_ca=true', [
        {'cert_id': 9, 'ski': 'k3', 'aki': 'ripe-ta', 'is_ca_flag': True, 'not_before': '2025-01-01 00:00:00',
         'sia': 'rsync://RPKI-RPS.ARIN.NET/repo/k3/'}])
    ctx = W.make_ctx(str(tmp_path / 'root'), base, '2026-10-26 00:40:01', ['stub_crl_count'])
    con = ctx.connect()
    try:
        ctx.load_ca_keys(con)
        rows = con.execute("SELECT ski, host, aki, certs, warehouses FROM ca_keys ORDER BY ski").fetchall()
    finally:
        con.close()
    assert rows == [('k1', 'repo-rpki.idnic.net', 'ta', 2, 'apnic'), ('k2', None, 'ta', 1, 'apnic'),
                    ('k3', 'rpki-rps.arin.net', 'ripe-ta', 1, 'ripencc')]


# ── warehouse reads ───────────────────────────────────────────────────────────
def test_part_files_skip_the_symlink_young_parts_and_other_files(tmp_path):
    base = str(tmp_path / 'wh')
    old = W.write_table(base, 'arin', 'crls', [{'crl_id': 1}], part=1)
    young = W.write_table(base, 'arin', 'crls', [{'crl_id': 2}], part=2, mtime=time.time())
    d = common.table_dir('arin', 'crls', base)
    open(os.path.join(d, 'notes.txt'), 'w').close()
    assert os.path.islink(os.path.join(d, 'crls.parquet'))
    assert common.part_files('arin', 'crls', base) == [old]                      # symlink and young part skipped
    assert common.part_files('arin', 'crls', base, min_age_s=0) == sorted([old, young])
    assert common.part_files('arin', 'manifests', base) == []
    con = duckdb.connect()
    assert con.execute(f"SELECT count(*) FROM {common.read_sql(common.part_files('arin', 'crls', base, min_age_s=0))}"
                       ).fetchone()[0] == 2                                      # each part read once


@pytest.mark.parametrize('hm, ok', [((0, 44), True), ((0, 45), False), ((1, 0), False), ((1, 19), False),
                                     ((1, 20), True), ((7, 50), True), ((23, 59), True)])
def test_no_warehouse_read_starts_in_the_presence_window(hm, ok):
    t = dt.datetime(2026, 11, 2, *hm, tzinfo=dt.timezone.utc)
    assert common.may_start(t) is ok


def test_presence_lock_is_shared_and_waits_for_the_exclusive_writer(tmp_path, monkeypatch):
    import fcntl
    import threading
    monkeypatch.setattr(common, 'may_start', lambda now=None: True)
    path = str(tmp_path / 'presence.lock')
    with open(path, 'w') as other:                 # another reader: shared + shared is fine
        fcntl.flock(other, fcntl.LOCK_SH)
        with common.presence_lock(path, wait_s=2, poll_s=0.05):
            pass
    writer = open(path, 'w')
    fcntl.flock(writer, fcntl.LOCK_EX)             # the presence build holds it exclusively
    with pytest.raises(SystemExit, match='no shared lock'):
        with common.presence_lock(path, wait_s=0.3, poll_s=0.05):
            pass
    threading.Timer(0.3, writer.close).start()     # ... and releases it
    t = time.monotonic()
    with common.presence_lock(path, wait_s=5, poll_s=0.05):
        assert time.monotonic() - t >= 0.25


def test_young_presence_parts_are_an_error_under_the_presence_lock(tmp_path, monkeypatch):
    """Only the presence build writes presence parts, under the exclusive lock: once the shared lock is
    held a part younger than 180 s is complete, and skipping it would drop the newest presence data
    silently, so listing it raises. Outside the lock, and for other tables, young parts are skipped."""
    monkeypatch.setattr(common, 'may_start', lambda now=None: True)
    base, lock = str(tmp_path / 'wh'), str(tmp_path / 'presence.lock')
    old = W.write_table(base, 'arin', 'object_presence_runs', [{'object_id': 1}], part=1)
    W.write_table(base, 'arin', 'object_presence_runs', [{'object_id': 2}], part=2, mtime=time.time())
    W.write_table(base, 'arin', 'crls', [{'crl_id': 1}], mtime=time.time())
    assert common.part_files('arin', 'object_presence_runs', base) == [old]
    with common.presence_lock(lock, wait_s=2, poll_s=0.05):
        with pytest.raises(RuntimeError, match='table=object_presence_runs: .* the shared presence lock is held'):
            common.part_files('arin', 'object_presence_runs', base)
        assert common.part_files('arin', 'crls', base) == []
        assert len(common.part_files('arin', 'object_presence_runs', base, min_age_s=0)) == 2
    assert common.part_files('arin', 'object_presence_runs', base) == [old]


def test_connect_caps_threads_memory_and_spill(tmp_path, monkeypatch):
    con = common.connect(threads=64, memory_limit='64GB', root=str(tmp_path))
    try:
        got = con.execute("SELECT current_setting('threads'), current_setting('memory_limit'), "
                          "current_setting('max_temp_directory_size'), current_setting('TimeZone')").fetchone()
    finally:
        con.close()
    assert got == (8, '7.4 GiB', '279.3 GiB', 'UTC')       # 8GB at most; spill 300GB (SCORECARD_MAX_SPILL)
    monkeypatch.setattr(common, 'MAX_SPILL', '2GB')
    con = common.connect(threads=1, memory_limit='1GB', root=str(tmp_path))
    try:
        assert con.execute("SELECT current_setting('max_temp_directory_size')").fetchone()[0] == '1.8 GiB'
    finally:
        con.close()


def test_code_commit_does_not_rewrite_the_git_index(tmp_path):
    """git runs with --no-optional-locks: `git status` of a checkout whose files changed stat data does not
    refresh (rewrite) its index."""
    repo = tmp_path / 'repo'
    (repo / 'scs').mkdir(parents=True)
    (repo / 'a.py').write_text('x = 1\n')
    git = ['git', '-C', str(repo), '-c', 'user.name=t', '-c', 'user.email=t@example.org', '-c', 'commit.gpgsign=false']
    for args in (['init', '-q'], ['add', 'a.py'], ['commit', '-q', '-m', 'one']):
        subprocess.run(git + args, check=True, capture_output=True)
    old = time.time() - 3600
    os.utime(repo / 'a.py', (old, old))
    index = repo / '.git' / 'index'
    before = index.stat().st_mtime_ns
    assert len(common.code_commit(str(repo / 'scs'))) == 12 and index.stat().st_mtime_ns == before
    (repo / 'a.py').write_text('x = 2\n')
    assert common.code_commit(str(repo / 'scs')).endswith('+dirty') and index.stat().st_mtime_ns == before


# ── observation ledger on a synthetic snapshot directory ─────────────────────
def test_ledger_on_a_synthetic_snapshot_directory(tmp_path):
    """ARIN, era 1: daily archive snapshots 2024-05-01..05-31 with partial ingests on 05-12 (400 files
    against 1,000: under half, no observation) and 05-13 (600: a drop, still observed) and no snapshot
    on 05-20. Era 2: cycles every 20 minutes 2026-10-01..10-14 with
      10-05 no cycle (outage);
      10-07 06:00..09:40 missing (12 cycles): longest gap 05:40:01 -> 10:00:01 = 4 h 20 min > 3 h;
      10-09 08:00..14:20 near-empty (20 cycles of 10 files): not observations, partial too;
      10-10 10 partial cycles of 500 files (>= 25% of the 7-day median): drops, still good;
      10-11 12:00:01 without metadata (being loaded), 10-12 12:00:01 without a membership index;
      verdict rows for 50 cycles of 10-13; the cut is 10-14 12:00, so 10-14 is incomplete."""
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    era1 = [f'2024-05-{d:02d} 00:00:00' for d in range(1, 32) if d != 20]
    W.snapshots(base, 'arin', era1, file_count={'2024-05-12 00:00:00': 400, '2024-05-13 00:00:00': 600})
    cyc = [c for c in W.cycles('2026-10-01', '2026-10-14') if c[:10] != '2026-10-05'
           and not ('2026-10-07 06:00' <= c < '2026-10-07 10:00')]
    near_empty = [c for c in cyc if '2026-10-09 08:00' <= c < '2026-10-09 14:40']
    partial = [c for c in cyc if c[:10] == '2026-10-10'][:10]
    counts = {c: 10 for c in near_empty} | {c: 500 for c in partial}
    special = {'2026-10-11 12:00:01', '2026-10-12 12:00:01'}
    W.snapshots(base, 'arin', [c for c in cyc if c not in special], file_count=counts)
    W.snapshots(base, 'arin', ['2026-10-11 12:00:01'], metadata=False)
    W.snapshots(base, 'arin', ['2026-10-12 12:00:01'], membership=False)
    W.write_table(base, 'arin', 'object_validity',
                  [{'dataset_ts': c, 'object_id': 7, 'state': 2, 'reason_code': 'expired'}
                   for c in [c for c in cyc if c[:10] == '2026-10-13'][:50]])
    meta = ledger.run_ledger(root, base, cut='2026-10-14 12:00:00', rirs=('arin',), min_age_s=0)
    assert len(near_empty) == 20 and meta['by_rir']['arin']['first_day'] == '2024-05-01'
    con = duckdb.connect()
    day = {r[0]: r[1:] for r in con.execute(
        f"""SELECT CAST(day AS VARCHAR), era, cycles, good_cycles, drops, empty, missing_index, expected,
                   round(longest_gap_h, 3), verdict_cycles, low, gap, complete
            FROM read_parquet('{root}/ledger/ledger.parquet')""").fetchall()}
    #                            era cyc good drops empty miss exp  gap_h vcyc  low    gap    complete
    assert day['2024-05-11'] == (1, 1, 1, 0, 0, 0, 1, 24.0, None, False, False, True)
    assert day['2024-05-12'] == (1, 1, 0, 1, 1, 0, 1, 24.0, None, True, True, True)    # under half
    assert day['2024-05-13'] == (1, 1, 1, 1, 0, 0, 1, 24.0, None, False, False, True)  # partial, observed
    assert day['2024-05-20'] == (1, 0, 0, 0, 0, 0, 1, 24.0, None, True, True, True)    # no snapshot
    assert day['2026-10-02'] == (2, 72, 72, 0, 0, 0, 72, 0.333, 0, False, False, True)
    assert day['2026-10-05'] == (2, 0, 0, 0, 0, 0, 72, 24.0, 0, True, True, True)      # outage
    assert day['2026-10-07'] == (2, 60, 60, 0, 0, 0, 72, 4.333, 0, True, True, True)
    assert day['2026-10-09'] == (2, 72, 52, 20, 20, 0, 72, 7.0, 0, True, True, True)   # 07:40:01 -> 14:40:01
    assert day['2026-10-10'] == (2, 72, 72, 10, 0, 0, 72, 0.333, 0, False, False, True)
    assert day['2026-10-11'] == (2, 72, 71, 0, 0, 1, 72, 0.667, 0, False, False, True)
    assert day['2026-10-12'] == (2, 72, 71, 0, 0, 1, 72, 0.667, 0, False, False, True)
    assert day['2026-10-13'] == (2, 72, 72, 0, 0, 0, 72, 0.333, 50, False, False, True)
    assert day['2026-10-14'][-1] is False and min(day) == '2011-12-26' and max(day) == '2026-10-14'
    assert day['2012-06-01'][1:4] == (0, 0, 0) and day['2012-06-01'][-2] is True        # before ARIN's history
    # sid = position in the sorted membership listing (the 10-12 cycle without an index has none)
    sids = dict(con.execute(f"SELECT CAST(ts AS VARCHAR), sid FROM read_parquet('{root}/ledger/cycles.parquet')").fetchall())
    assert sids['2024-05-01 00:00:00'] == 0 and sids['2026-10-01 00:00:01'] == len(era1)
    assert sids['2026-10-12 12:00:01'] is None and sids['2026-10-12 12:20:01'] == sids['2026-10-12 11:40:01'] + 1
    assert meta['by_rir']['arin']['gap_days'] >= 3 and os.path.exists(f'{root}/ledger/ledger.json')


# ── status rules ──────────────────────────────────────────────────────────────
def build_ledger(root, cut, first, last, holes=(), thin=None, verdicts=None, rirs=('arin', 'apnic')):
    """Era-2 ledger written to ROOT/ledger: cycles every 20 min on first..last for every rir, minus
    holes [(rir, from_ts, to_ts)] and thinned days thin {(rir, day): cycles kept, evenly spread};
    verdicts {rir: share of cycles with verdict rows} (default none)."""
    snaps, vrows = {}, {}
    for rir in rirs:
        cyc = W.cycles(first, last)
        for r, a, b in holes:
            if r == rir:
                cyc = [c for c in cyc if not (a <= c < b)]
        for (r, d), n in (thin or {}).items():
            if r == rir:
                day = [c for c in cyc if c[:10] == d]
                keep = {day[round(i * len(day) / n)] for i in range(n)}
                cyc = [c for c in cyc if c[:10] != d or c in keep]
        snaps[rir] = [(c, 'rpki-client', 1000, True, True, i) for i, c in enumerate(cyc)]
        share = (verdicts or {}).get(rir, 0)
        vrows[rir] = {c: 1 for i, c in enumerate(cyc) if share and (i % 100) < share * 100}
    con = common.connect(threads=2, memory_limit='1GB', root=root)
    try:
        ledger.build(con, snaps, vrows, common.parse_ts(cut), common.tmp_dir(root))
        ledger.write(con, root, common.parse_ts(cut), {}, rirs)
    finally:
        con.close()


ROW = metrics.ROW_COLS


def status_of(root, rows, as_of):
    con = common.connect(threads=2, memory_limit='1GB', root=root)
    try:
        common.values_table(con, 'r', ROW, rows)
        con.execute(f"CREATE TEMP TABLE ledger AS SELECT * FROM read_parquet('{root}/ledger/ledger.parquet')")
        status.annotations_table(con, assemble.ANNOTATIONS_PATH)
        status.apply(con, 'r', 'out', as_of, 1, 'test')
        cur = con.execute('SELECT * EXCLUDE (as_of), CAST(as_of AS VARCHAR) AS as_of FROM out')
        names = [d[0] for d in cur.description]
        return {(x['rir'], x['metric_id'], x['stat'], x['week'], x['window']): x
                for x in (dict(zip(names, r)) for r in cur.fetchall())}
    finally:
        con.close()


def row(rir, metric, stat, week, window='1w', n=100, n_ca=20, value=0.5, pop='all'):
    return (common.to_date(week), window, rir, pop, '', metric, stat, value, n, n_ca, 0.1)


@pytest.fixture
def status_world(tmp_path):
    """ARIN: a 6-hour hole on 2026-09-16 (gap day, 54 cycles); 60 cycles a day on 09-22..09-26 (low, no
    gap: the week holds 72 x 2 + 60 x 5 = 444 < 0.9 x 504 good cycles); no cycle on 10-06. APNIC: a
    4-hour hole on 10-25 (the last day of week 10-19), verdict rows in every cycle except in week 10-12
    (half). Ledger from 2026-08-24, cut 2026-11-02 05:00."""
    root = str(tmp_path / 'root')
    thin = {('arin', d): 60 for d in ('2026-09-22', '2026-09-23', '2026-09-24', '2026-09-25', '2026-09-26')}
    build_ledger(root, '2026-11-02 05:00:00', '2026-08-24', '2026-11-02',
                 holes=[('arin', '2026-09-16 06:00', '2026-09-16 12:00'), ('arin', '2026-10-06', '2026-10-07'),
                        ('apnic', '2026-10-25 08:00', '2026-10-25 12:20')],
                 thin=thin, verdicts={'apnic': 1.0})
    # APNIC week 10-12: drop the verdict rows of half the cycles
    con = duckdb.connect()
    p = f'{root}/ledger/ledger.parquet'
    con.execute(f"COPY (SELECT * REPLACE (CASE WHEN rir = 'apnic' AND day BETWEEN DATE '2026-10-12' AND DATE '2026-10-18' "
                f"THEN verdict_cycles // 2 ELSE verdict_cycles END AS verdict_cycles) FROM read_parquet('{p}')) "
                f"TO '{p}.x' (FORMAT parquet)")
    os.replace(p + '.x', p)
    return root


def test_status_publication_finality_and_masks(status_world):
    root = status_world
    weeks = mondays('2026-08-31', '2026-10-26')
    rows = []
    for w in weeks:
        for rir in ('arin', 'apnic'):
            rows += [row(rir, 'crl_window', 'share_le48', w), row(rir, 'resign_consumed', 'consumed_p50', w),
                     row(rir, 'roa_ee_revocation', 'revoked_h7', w), row(rir, 'roa_ee_revocation', 'revoked_h35', w),
                     row(rir, 'live_inventory', 'ca_keys', w, window='end', n=3, n_ca=3),
                     row(rir, 'rp_rejected', 'objects', w, n=4, n_ca=2)]
    s = status_of(root, rows, '2026-10-26 00:40:01')

    def st(rir, m, stat, w, win='1w'):
        x = s.get((rir, m, stat, D.fromisoformat(w), win))
        return x and x['status']
    # crl_window: published and final at W+1; masked by too few good cycles in W (not by a gap alone)
    assert st('arin', 'crl_window', 'share_le48', '2026-09-07') == 'final'
    assert st('arin', 'crl_window', 'share_le48', '2026-09-14') == 'final'      # 6-hour hole: 486 of 504
    assert s[('arin', 'crl_window', 'share_le48', D(2026, 9, 14), '1w')]['flags'] == 'ledger:gap;ledger:low'
    assert st('arin', 'crl_window', 'share_le48', '2026-09-21') == 'masked'     # 444 good cycles of 504
    assert st('arin', 'crl_window', 'share_le48', '2026-10-05') == 'masked'     # outage day
    assert st('arin', 'crl_window', 'share_le48', '2026-10-19') == 'final'
    assert st('arin', 'crl_window', 'share_le48', '2026-10-26') is None         # week not over: not published
    # resign_consumed: provisional at W+1, final at W+2, masked by a gap in W or in the first 7 days of W+1
    assert st('arin', 'resign_consumed', 'consumed_p50', '2026-09-07') == 'masked'   # gap 09-16 in W+1
    assert st('arin', 'resign_consumed', 'consumed_p50', '2026-09-14') == 'masked'
    assert st('arin', 'resign_consumed', 'consumed_p50', '2026-09-21') == 'final'    # low days are no gaps
    assert st('arin', 'resign_consumed', 'consumed_p50', '2026-09-28') == 'masked'   # 10-06 in W+1
    assert st('arin', 'resign_consumed', 'consumed_p50', '2026-10-12') == 'final'
    assert st('arin', 'resign_consumed', 'consumed_p50', '2026-10-19') == 'provisional'
    # ROA-EE revocation: first published at W+2; H=7 final then, H=35 final at W+6
    assert st('apnic', 'roa_ee_revocation', 'revoked_h7', '2026-10-19') is None
    assert st('apnic', 'roa_ee_revocation', 'revoked_h35', '2026-10-19') is None
    assert st('arin', 'roa_ee_revocation', 'revoked_h7', '2026-10-12') == 'final'
    assert st('arin', 'roa_ee_revocation', 'revoked_h35', '2026-10-12') == 'provisional'
    assert st('apnic', 'roa_ee_revocation', 'revoked_h7', '2026-10-12') == 'masked'  # 10-25 hole within 7 days
    assert st('apnic', 'roa_ee_revocation', 'revoked_h35', '2026-09-07') == 'final'  # W+6, [09-07, 10-19) clean
    assert st('apnic', 'roa_ee_revocation', 'revoked_h35', '2026-09-14') == 'masked'  # 10-25 within 35 days
    assert st('arin', 'roa_ee_revocation', 'revoked_h35', '2026-09-07') == 'masked'  # 09-16
    assert st('arin', 'roa_ee_revocation', 'revoked_h7', '2026-09-28') == 'masked'
    assert st('arin', 'roa_ee_revocation', 'revoked_h7', '2026-09-21') == 'final'
    # census at the week's end: never insufficient (n = 3), masked by a gap on the week's last day only
    assert st('apnic', 'live_inventory', 'ca_keys', '2026-10-12', 'end') == 'final'
    assert st('apnic', 'live_inventory', 'ca_keys', '2026-10-19', 'end') == 'masked'    # 10-25 hole
    assert st('arin', 'live_inventory', 'ca_keys', '2026-10-05', 'end') == 'final'      # 10-06 is not the end
    # verdict coverage: every cycle with verdicts -> final; half in week 10-12 -> masked; ARIN none -> masked
    assert st('apnic', 'rp_rejected', 'objects', '2026-10-05') == 'final'
    assert st('apnic', 'rp_rejected', 'objects', '2026-10-12') == 'masked'
    assert st('arin', 'rp_rejected', 'objects', '2026-10-05') == 'masked'
    assert {x['frozen'] for k, x in s.items() if x['status'] == 'final'} == {True}
    assert {x['frozen'] for k, x in s.items() if x['status'] == 'provisional'} == {False}
    assert {x['as_of'] for x in s.values()} == {'2026-10-26 00:40:01+00'}
    assert {(x['metric_id'] == 'rp_rejected', x['def_version'], x['hostmap_version'], x['code_commit'])
            for x in s.values()} == {(False, 1, 1, 'test'), (True, 2, 1, 'test')}


def test_status_is_provisional_before_the_look_ahead_is_observed(status_world):
    """The 10-06 gap lies in the first 7 days of W+1 for week 09-28: at W+1 (as_of 10-05) it has not been
    observed yet, so the row is provisional; at W+2 it is masked."""
    r = [row('arin', 'resign_consumed', 'consumed_p50', '2026-09-28')]
    k = ('arin', 'resign_consumed', 'consumed_p50', D(2026, 9, 28), '1w')
    assert status_of(status_world, r, '2026-10-05 00:40:01')[k]['status'] == 'provisional'
    assert status_of(status_world, r, '2026-10-12 00:40:01')[k]['status'] == 'masked'


def test_status_insufficient_census_and_precedence(status_world):
    rows = [row('apnic', 'crl_window', 'p50', '2026-10-05', n=29), row('apnic', 'crl_window', 'p90', '2026-10-05', n_ca=9),
            row('apnic', 'crl_window', 'max', '2026-10-05', n=30, n_ca=10),
            row('arin', 'crl_window', 'p50', '2026-09-21', n=5),                       # masked wins
            row('apnic', 'live_inventory', 'roas', '2026-10-05', window='end', n=1, n_ca=1)]
    s = status_of(status_world, rows, '2026-10-26 00:40:01')
    got = {(k[0], k[2]): v['status'] for k, v in s.items()}
    assert got == {('apnic', 'p50'): 'insufficient', ('apnic', 'p90'): 'insufficient', ('apnic', 'max'): 'final',
                   ('arin', 'p50'): 'masked', ('apnic', 'roas'): 'final'}


def test_verdict_mask_also_needs_the_expected_cycles(tmp_path):
    """rp_rejected (rule 'verdicts'): APNIC has no cycle from 2026-10-05 00:00 to 10-08 12:00 (252 of the
    504 expected cycles of week 10-05) and none in week 10-19, and every cycle that ran has verdict rows,
    so the verdict cycles equal the snapshot cycles. Both weeks are masked by the expected cycles (with the
    verdict ratio alone they were final, week 10-19 with 0 objects); the complete week 10-12 is final."""
    root = str(tmp_path / 'root')
    build_ledger(root, '2026-11-02 05:00:00', '2026-08-24', '2026-11-02', rirs=('apnic',), verdicts={'apnic': 1.0},
                 holes=[('apnic', '2026-10-05', '2026-10-08 12:00'), ('apnic', '2026-10-19', '2026-10-26')])
    rows = [row('apnic', 'rp_rejected', 'objects', w, n=n, n_ca=n)
            for w, n in (('2026-10-05', 2327), ('2026-10-12', 5), ('2026-10-19', 0))]
    s = status_of(root, rows, '2026-11-02 00:40:01')
    assert {k[3].isoformat(): x['status'] for k, x in s.items()} == \
        {'2026-10-05': 'masked', '2026-10-12': 'final', '2026-10-19': 'masked'}


def test_status_era_flags_and_rolling_windows(status_world):
    """A 52-week window ending 2026-10-19 starts 2025-10-27 (era 1): era 'mixed', the transition flag, the
    annotations of the period that apply to ARIN and the metric, and ledger:gap_in_window for 09-16 and 10-06;
    its status is that of its last week (provisional at W+1). A transition week has era 'transition'."""
    rows = [row('arin', 'resign_consumed', 'consumed_p50', '2026-10-19', window='52w'),
            row('apnic', 'resign_consumed', 'consumed_p50', '2026-04-13', window='1w', pop='delegated'),
            row('apnic', 'crl_window', 'p50', '2026-06-01', pop='delegated'),
            row('apnic', 'crl_window', 'p90', '2026-06-01', pop='rir_hosted')]
    s = status_of(status_world, rows, '2026-10-26 00:40:01')
    x = s[('arin', 'resign_consumed', 'consumed_p50', D(2026, 10, 19), '52w')]
    assert x['era'] == 'mixed' and x['status'] == 'provisional'
    flags = x['flags'].split(';')
    for f in ('era:transition', 'era-transition', 'outage-20261004', 'near-empty-20261010', 'arin-resign-20260729',
              'partial-cycles-2026q2', 'ledger:gap_in_window'):
        assert f in flags, f
    assert 'missing-20260407' not in flags and 'cernet-fiti-gap' not in flags and flags == sorted(flags)
    t = s[('apnic', 'resign_consumed', 'consumed_p50', D(2026, 4, 13), '1w')]
    assert t['era'] == 'transition' and t['status'] == 'masked'                 # no ledger before 08-24
    deleg = s[('apnic', 'crl_window', 'p50', D(2026, 6, 1), '1w')]['flags'].split(';')
    hosted = s[('apnic', 'crl_window', 'p90', D(2026, 6, 1), '1w')]['flags'].split(';')
    assert 'cernet-fiti-gap' in deleg and 'partial-cycles-2026q2' in deleg
    assert 'cernet-fiti-gap' not in hosted and 'partial-cycles-2026q2' in hosted


def test_annotations_are_consistent():
    con = duckdb.connect()
    status.annotations_table(con, assemble.ANNOTATIONS_PATH)
    assert status.check_annotations(con) == []
    ids = [r[0] for r in con.execute('SELECT id FROM annotations').fetchall()]
    for need in ('apnic-archive-2012-2018', 'arin-20241211', 'arin-20241215', 'arin-20241217', 'arin-202405-partial',
                 'missing-20260407', 'era-transition', 'outage-20261004', 'near-empty-20261010', 'cernet-fiti-gap',
                 'arin-resign-20260729', 'apnic-revocation-20260303', 'ripe-resign-202601', 'ripe-rollover-202607',
                 'rpki-client-9.7', 'jpnic-crl-720h'):
        assert need in ids, need


# ── the module interface ──────────────────────────────────────────────────────
FAKE = types.SimpleNamespace(rows=None, hist=None, calls=[])


def _fake_compute(ctx, rir, wf, wt):
    FAKE.calls.append((rir, wf, wt))
    con = ctx.connect()
    try:
        rows = FAKE.rows(rir, wf, wt)
        common.values_table(con, 'r', ROW, rows)
        hist = FAKE.hist(rir, wf, wt) if FAKE.hist else None
        if hist is not None:
            common.values_table(con, 'h', metrics.HIST_COLS, hist)
        ctx.emit(con, 'r', 'h' if hist is not None else None)
    finally:
        con.close()


TEST_METRICS = {
    't_share': {'def_version': 1, 'module': 'm_fake', 'title': 't', 'unit': 'share', 'kind': 'sample',
                'classified_by': 'ca_host', 'windows': ('1w', '13w'), 'available_from': None,
                'stats': {'a': '', 'b_h7': '', 'b_h35': '', '^c_[a-z]+$': ''},
                'histograms': {'x_h': {'desc': '', 'windows': ('1w',)}}, 'params': {},
                'rules': [defs.H35_RULE, defs.H7_RULE], 'rfc': [], 'method': 'test'},
    't_window': {'def_version': 1, 'module': 'm_fake', 'title': 't', 'unit': 'share', 'kind': 'sample',
                 'classified_by': 'ca_host', 'windows': ('1w',), 'available_from': None, 'stats': {'a': ''},
                 'histograms': {}, 'params': {}, 'rules': [defs.WINDOWS_RULE], 'rfc': [], 'method': 'test'},
}


@pytest.fixture
def fake(monkeypatch):
    """Two test metrics computed by an in-memory module scs.m_fake: t_share (ROA-revocation rules: a and b_h7
    final at W+2, b_h35 at W+6) and t_window (windows rule: final at W+1). FAKE.rows(rir, wf, wt) makes the
    rows; the default gives every week and RIR value = VALUE[0] for populations all and rir_hosted."""
    mod = types.ModuleType('scs.m_fake')
    mod.__spec__ = importlib.machinery.ModuleSpec('scs.m_fake', None)
    mod.compute = _fake_compute
    monkeypatch.setitem(sys.modules, 'scs.m_fake', mod)
    for k, v in TEST_METRICS.items():
        monkeypatch.setitem(defs.METRICS, k, v)
    FAKE.calls, FAKE.hist = [], None
    FAKE.value = [1.0]

    def rows(rir, wf, wt):
        out = []
        for w in common.weeks(wf, wt):
            for pop in ('all', 'rir_hosted'):
                for stat in ('a', 'b_h7', 'b_h35'):
                    out.append((w, '1w', rir, pop, '', 't_share', stat, FAKE.value[0], 100, 20, 0.25))
                out.append((w, '1w', rir, pop, '', 't_window', 'a', FAKE.value[0] + 1, 100, 20, 0.25))
        return out
    FAKE.rows = rows
    return FAKE


def emit_one(tmp_path, fake, rows, hist=None):
    fake.rows = lambda rir, wf, wt: rows
    fake.hist = (lambda rir, wf, wt: hist) if hist is not None else None
    ctx = W.make_ctx(str(tmp_path / 'root'), str(tmp_path / 'wh'), '2026-10-26 00:40:01', ['t_share', 't_window'])
    return ctx.call('m_fake', 'arin', '2026-10-12', '2026-10-19'), ctx


GOOD = (D(2026, 10, 12), '1w', 'arin', 'all', '', 't_share', 'a', 0.5, 100, 20, 0.2)


@pytest.mark.parametrize('bad, msg', [
    (GOOD[:2] + ('apnic',) + GOOD[3:], 'rir other than arin'),
    ((D(2026, 10, 13),) + GOOD[1:], 'not a Monday'),
    ((D(2026, 10, 5),) + GOOD[1:], 'not a Monday in 2026-10-12..2026-10-19'),
    (GOOD[:6] + ('zz',) + GOOD[7:], "stat 'zz' not declared"),
    (GOOD[:1] + ('52w',) + GOOD[2:], "window '52w' not in"),
    (GOOD[:5] + ('crl_window',) + GOOD[6:], "does not belong to m_fake"),
    (GOOD[:3] + ('hosted',) + GOOD[4:], "unknown population 'hosted'"),
    (GOOD[:4] + ('JPNIC',) + GOOD[5:], "operator 'JPNIC' on population 'all'"),
    (GOOD[:7] + (float('nan'),) + GOOD[8:], 'NaN or inf'),
    (GOOD[:7] + (float('inf'),) + GOOD[8:], 'NaN or inf'),
    (GOOD[:8] + (-1,) + GOOD[9:], 'n or n_ca NULL or negative'),
    (GOOD[:10] + (1.5,), 'top1_share outside'),
    (GOOD[:4] + (None,) + GOOD[5:], 'NULL in a key column'),
])
def test_emit_rejects_module_bugs(tmp_path, fake, bad, msg):
    with pytest.raises(ValueError, match=msg):
        emit_one(tmp_path, fake, [GOOD, bad])


def test_emit_rejects_duplicates_bad_columns_and_bad_histograms(tmp_path, fake):
    with pytest.raises(ValueError, match='duplicate keys'):
        emit_one(tmp_path, fake, [GOOD, GOOD[:7] + (0.7,) + GOOD[8:]])
    h = (D(2026, 10, 12), '1w', 'arin', 'all', '', 't_share', 'x_h', 0.0, 1.0, 5)
    with pytest.raises(ValueError, match='bin_hi <= bin_lo'):
        emit_one(tmp_path, fake, [GOOD], [h[:8] + (0.0,) + h[9:]])
    with pytest.raises(ValueError, match="histogram 'y_h' not declared"):
        emit_one(tmp_path, fake, [GOOD], [h[:6] + ('y_h',) + h[7:]])
    with pytest.raises(ValueError, match='count NULL or negative'):
        emit_one(tmp_path, fake, [GOOD], [h[:9] + (-2,)])
    ctx = W.make_ctx(str(tmp_path / 'root2'), str(tmp_path / 'wh'), '2026-10-26 00:40:01', ['t_share'])
    con = duckdb.connect()
    con.execute("CREATE TABLE x AS SELECT DATE '2026-10-12' AS week, 'arin' AS rir")
    ctx._call = ('m_fake', 'arin', D(2026, 10, 12), D(2026, 10, 12), {'files': [], 'rows': 0, 'hist': 0})
    with pytest.raises(ValueError, match='columns'):
        ctx.emit(con, 'x')
    ctx._call = None
    with pytest.raises(RuntimeError, match='outside Ctx.call'):
        ctx.emit(con, 'x')


def test_emit_stores_rounded_rows_and_fetch_returns_them(tmp_path, fake):
    rows = [GOOD[:7] + (1 / 3,) + GOOD[8:], GOOD[:6] + ('c_xy', 0.1 + 0.2) + GOOD[8:],
            GOOD[:6] + ('b_h7', None, 0, 0, None)]
    out, ctx = emit_one(tmp_path, fake, rows, [(D(2026, 10, 12), '1w', 'arin', 'all', '', 't_share', 'x_h', 0.07,
                                                None, 3)])
    assert out['rows'] == 3 and out['hist'] == 1
    got = {r['stat']: r['value'] for r in ctx.fetch('rows')}
    assert got == {'a': 0.3333333333, 'c_xy': 0.3, 'b_h7': None}
    assert ctx.fetch('hist')[0]['bin_lo'] == 0.07 and ctx.fetch('hist')[0]['bin_hi'] is None


# ── assemble: frozen state, trailing recompute, gate, idempotency, releases ──
def run_fake(root, as_of, metric_ids=('t_share', 't_window'), weeks=6, backfill=None, stamp=None, release=True,
             rirs=('arin', 'apnic'), allow_shrink=False):
    """One scs.run-like pass with the fake module: plan, module calls, assemble.build."""
    hm = hostmap.load()
    ctx = W.make_ctx(root, os.path.join(root, 'wh'), as_of, metric_ids, rirs=rirs)
    state, state_dir = assemble.load_state(root)
    prev, prev_dir = assemble.load_previous(root)
    plan = assemble.plan_weeks(list(metric_ids), state, prev, ctx.current_week, weeks, hm, backfill)
    for module, ranges in assemble.module_ranges(plan).items():
        for rir in ctx.rirs:
            for wf, wt in ranges:
                ctx.call(module, rir, wf, wt)
    manifest, bdir = assemble.build(ctx, plan, state, state_dir, prev, prev_dir, mode='backfill' if backfill else 'weekly',
                                    release=release, stamp=stamp, allow_shrink=allow_shrink)
    return manifest, bdir, plan


def read_rows(path, where='true'):
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    cur = con.execute(f"SELECT * EXCLUDE (as_of), CAST(as_of AS VARCHAR) AS as_of FROM read_parquet('{path}') "
                      f"WHERE {where}")
    names = [d[0] for d in cur.description]
    return {(r['rir'], r['population'], r['metric_id'], r['stat'], r['week']): r
            for r in (dict(zip(names, x)) for x in cur.fetchall())}


@pytest.fixture
def froot(tmp_path, fake):
    root = str(tmp_path / 'root')
    build_ledger(root, '2026-11-24 05:00:00', '2026-08-03', '2026-11-24')
    return root


def test_frozen_state_and_trailing_recompute(froot, fake):
    """Run 1 (as_of 2026-10-26, weeks 09-14..10-19): t_window rows are final at W+1 and frozen; t_share a/b_h7
    are published from W+2 (10-12 and older), b_h35 final only for 09-14 (W+6): the only closed week of
    t_share is 09-14, t_window's are all six. Run 2 one week later with every value changed: frozen rows keep
    their run-1 value, b_h35 of 09-21..10-12 and the new weeks get the new value, and 09-21 closes."""
    root = froot
    m1, b1, plan1 = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T050000Z')
    assert plan1['t_share'][0] == mondays('2026-09-14', '2026-10-19')
    r1 = read_rows(f'{b1}/scorecard.parquet')
    assert {k[4] for k in r1 if k[2] == 't_window'} == set(mondays('2026-09-14', '2026-10-19'))
    assert {k[4] for k in r1 if k[2] == 't_share'} == set(mondays('2026-09-14', '2026-10-12'))
    assert r1[('arin', 'all', 't_share', 'b_h35', D(2026, 9, 14))]['status'] == 'final'
    assert r1[('arin', 'all', 't_share', 'b_h35', D(2026, 9, 21))]['status'] == 'provisional'
    assert r1[('arin', 'all', 't_share', 'b_h7', D(2026, 10, 12))]['status'] == 'final'
    st1 = json.load(open(os.path.join(root, 'state', 'state.json')))
    assert st1['metrics']['t_share']['closed'] == [['2026-09-14', '2026-09-14']]
    assert st1['metrics']['t_window']['closed'] == [['2026-09-14', '2026-10-19']]

    fake.value[0] = 9.0
    m2, b2, plan2 = run_fake(root, '2026-11-02 00:40:01', stamp='20261102T050000Z')
    assert plan2['t_share'][0] == mondays('2026-09-21', '2026-10-26')
    r2 = read_rows(f'{b2}/scorecard.parquet')
    v = {(k[2], k[3], k[4]): x['value'] for k, x in r2.items() if k[0] == 'arin' and k[1] == 'all'}
    assert v[('t_window', 'a', D(2026, 10, 19))] == 2.0 and v[('t_window', 'a', D(2026, 10, 26))] == 10.0
    assert v[('t_share', 'b_h7', D(2026, 10, 12))] == 1.0 and v[('t_share', 'b_h7', D(2026, 10, 19))] == 9.0
    assert v[('t_share', 'b_h35', D(2026, 9, 14))] == 1.0                       # frozen in run 1
    assert v[('t_share', 'b_h35', D(2026, 9, 21))] == 9.0                       # final now, new value
    assert r2[('arin', 'all', 't_share', 'b_h35', D(2026, 9, 21))]['status'] == 'final'
    assert r2[('arin', 'all', 't_share', 'b_h35', D(2026, 9, 14))]['as_of'] == '2026-10-26 00:40:01+00'
    assert r2[('arin', 'all', 't_share', 'b_h35', D(2026, 10, 12))]['as_of'] == '2026-11-02 00:40:01+00'
    st2 = json.load(open(os.path.join(root, 'state', 'state.json')))
    assert st2['metrics']['t_share']['closed'] == [['2026-09-14', '2026-09-21']]
    assert os.readlink(os.path.join(root, 'current')) == os.path.join('builds', '20261102T050000Z')
    assert os.readlink(os.path.join(root, 'state')) == os.path.join('states', '20261102T050000Z')


def test_missed_runs_and_definition_change_recompute(froot, fake, monkeypatch):
    """After a run on 10-26, the next run comes three weeks later with --weeks 2: the weeks that never
    closed (09-21..11-02) are recomputed too. A def_version bump of t_share then recomputes its whole
    history from the first closed week (frozen values replaced), while t_window keeps its frozen rows."""
    root = froot
    run_fake(root, '2026-10-26 00:40:01', stamp='20261026T050000Z')
    fake.value[0] = 5.0
    _, b, plan = run_fake(root, '2026-11-16 00:40:01', weeks=2, stamp='20261116T050000Z')
    assert plan['t_share'][0] == mondays('2026-09-21', '2026-11-09')
    assert plan['t_window'][0] == mondays('2026-10-26', '2026-11-09')            # 10-26 was never computed
    assert any('open week' in w for w in plan['t_share'][1])
    r = read_rows(f'{b}/scorecard.parquet', "rir = 'arin' AND population = 'all'")
    assert r[('arin', 'all', 't_share', 'b_h35', D(2026, 9, 28))]['value'] == 5.0
    assert r[('arin', 'all', 't_window', 'a', D(2026, 10, 19))]['value'] == 2.0  # frozen
    assert {k[4] for k in r if k[2] == 't_window'} == set(mondays('2026-09-14', '2026-11-09'))
    monkeypatch.setitem(defs.METRICS, 't_share', dict(defs.METRICS['t_share'], def_version=2))
    fake.value[0] = 7.0
    _, b, plan = run_fake(root, '2026-11-16 00:40:01', weeks=2, stamp='20261116T060000Z')
    assert plan['t_share'][0] == mondays('2026-09-14', '2026-11-09')
    assert any('def_version' in w for w in plan['t_share'][1])
    r = read_rows(f'{b}/scorecard.parquet', "rir = 'arin' AND population = 'all'")
    assert {x['value'] for k, x in r.items() if k[2] == 't_share'} == {7.0}
    assert {x['def_version'] for k, x in r.items() if k[2] == 't_share'} == {2}
    assert r[('arin', 'all', 't_window', 'a', D(2026, 10, 19))]['value'] == 2.0
    assert json.load(open(os.path.join(root, 'state', 'state.json')))['metrics']['t_share']['def_version'] == 2


def test_hostmap_or_framework_change_recomputes_computed_metrics(froot, fake, monkeypatch):
    """Populations depend on the host map: a changed classification (or a framework VERSION bump)
    discards the frozen rows of every metric that is computed and recomputes its history; an edit of the
    evidence texts only (another file sha256, the same digest) does not."""
    root = froot
    run_fake(root, '2026-10-26 00:40:01', stamp='20261026T050000Z')
    real = hostmap.load()
    notes_only = hostmap.HostMap(real.version, 'e' * 64, real.hosts, real.path)
    monkeypatch.setattr(hostmap, 'load', lambda path=None: notes_only)
    _, _, plan = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T053000Z', weeks=1)
    assert plan['t_window'][0] == mondays('2026-10-19', '2026-10-19')
    changed = hostmap.HostMap(real.version, 'f' * 64, dict(real.hosts, **{'krill.example.net': ('arin', 'rir_paas', 'X')}),
                              real.path)
    assert changed.digest != real.digest == notes_only.digest
    monkeypatch.setattr(hostmap, 'load', lambda path=None: changed)
    fake.value[0] = 4.0
    _, b, plan = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T060000Z', weeks=1)
    assert plan['t_window'][0] == mondays('2026-09-14', '2026-10-19')
    r = read_rows(f'{b}/scorecard.parquet', "rir = 'arin' AND population = 'all'")
    assert {x['value'] for k, x in r.items() if k[2] == 't_window'} == {5.0}
    st = json.load(open(os.path.join(root, 'state', 'state.json')))
    assert st['metrics']['t_window']['hostmap_digest'] == changed.digest
    monkeypatch.setattr(assemble, 'VERSION', assemble.VERSION + 1)
    _, _, plan = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T070000Z', weeks=1)
    assert plan['t_share'][0] == mondays('2026-09-14', '2026-10-19') and 'VERSION' in plan['t_share'][1][-1]


def test_iso_week_helpers():
    assert common.monday('2026-11-01') == D(2026, 10, 26) and common.monday(dt.datetime(2026, 10, 26, 23)) == D(2026, 10, 26)
    assert common.week_label(D(2020, 12, 28)) == '2020-W53' and common.week_label(D(2021, 1, 4)) == '2021-W01'
    assert common.week_label(D(2011, 12, 26)) == '2011-W52' and common.HISTORY_FROM == D(2011, 12, 26)
    assert common.week_ranges([D(2026, 10, 19), D(2026, 10, 5), D(2026, 10, 12), D(2026, 9, 21)]) == \
        [(D(2026, 9, 21), D(2026, 9, 21)), (D(2026, 10, 5), D(2026, 10, 19))]
    assert [common.week_era(w) for w in mondays('2026-03-23', '2026-04-20')] == \
        ['1', 'transition', 'transition', 'transition', '2']
    assert common.parse_ts('2026-10-26T00:40:01Z') == dt.datetime(2026, 10, 26, 0, 40, 1) == \
        common.parse_ts('2026-10-26 00:40:01+00')


def test_metrics_not_requested_are_carried_over(froot, fake):
    root = froot
    run_fake(root, '2026-10-26 00:40:01', stamp='20261026T050000Z')
    fake.value[0] = 3.0
    m, b, plan = run_fake(root, '2026-11-02 00:40:01', metric_ids=('t_window',), stamp='20261102T050000Z')
    assert set(plan) == {'t_window'}
    r = read_rows(f'{b}/scorecard.parquet', "rir = 'arin' AND population = 'all'")
    assert {k[4] for k in r if k[2] == 't_share'} == set(mondays('2026-09-14', '2026-10-12'))   # unchanged
    assert r[('arin', 'all', 't_share', 'b_h35', D(2026, 10, 12))]['value'] == 1.0
    assert r[('arin', 'all', 't_window', 'a', D(2026, 10, 26))]['value'] == 4.0
    assert m['metrics']['t_share']['computed'] is False


def test_assemble_is_idempotent(froot, fake):
    """A rerun on the same inputs (and as_of) writes byte-identical scorecard and histogram files, also with
    rows frozen by the first run now coming from state/."""
    fake.hist = lambda rir, wf, wt: [(w, '1w', rir, 'all', '', 't_share', 'x_h', b, b + 1, 7)
                                     for w in common.weeks(wf, wt) for b in (0.0, 1.0)]
    root = froot
    _, b1, _ = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T050000Z')
    _, b2, _ = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T060000Z')
    for f in ('scorecard.parquet', 'scorecard.csv', 'histograms.parquet'):
        assert open(f'{b1}/{f}', 'rb').read() == open(f'{b2}/{f}', 'rb').read(), f
    m1, m2 = (json.load(open(f'{b}/manifest.json')) for b in (b1, b2))
    for m in (m1, m2):
        m.pop('generated_at'), m.pop('build'), m['inputs'].pop('module_calls', None)
    assert m1 == m2
    # x_h follows t_share's default rule (published from W+2): weeks 09-14..10-12 x 2 RIRs x 2 bins
    assert duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{b2}/histograms.parquet')").fetchone()[0] \
        == 5 * 2 * 2


def test_publish_gate_keeps_current(froot, fake):
    """A build whose newest week lacks a RIR is not published: current and state stay, the .tmp
    directory is removed."""
    root = froot
    run_fake(root, '2026-10-26 00:40:01', stamp='20261026T050000Z')
    rows0 = fake.rows
    fake.rows = lambda rir, wf, wt: [r for r in rows0(rir, wf, wt) if not (rir == 'apnic' and r[0] == wt)]
    with pytest.raises(RuntimeError, match=r't_window: newest week 2026-10-26 has no rows for apnic'):
        run_fake(root, '2026-11-02 00:40:01', stamp='20261102T050000Z')
    assert os.readlink(os.path.join(root, 'current')) == os.path.join('builds', '20261026T050000Z')
    assert os.readlink(os.path.join(root, 'state')) == os.path.join('states', '20261026T050000Z')
    assert sorted(os.listdir(os.path.join(root, 'builds'))) == ['20261026T050000Z']


def test_a_prune_failure_after_the_publish_is_a_warning(froot, fake):
    """An old build that cannot be removed (a directory without write permission in it) does not fail the
    run once `current` has moved: the state and the release are still written, and the failure is a build
    warning (the run is PARTIAL)."""
    if os.geteuid() == 0:
        pytest.skip('root ignores file permissions')
    root = froot
    for s in ('20261026T050000Z', '20261026T060000Z', '20261026T070000Z'):
        run_fake(root, '2026-10-26 00:40:01', stamp=s, release=False)
    stuck = os.path.join(root, 'builds', '20261026T050000Z', 'stuck')
    os.makedirs(stuck)
    open(os.path.join(stuck, 'f'), 'w').close()
    os.chmod(stuck, 0o555)
    try:
        m, _, _ = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T080000Z')
    finally:
        os.chmod(stuck, 0o755)
    assert [w.split(':')[:2] for w in m['build_warnings']] == [['old builds not pruned', ' PermissionError']]
    assert os.readlink(os.path.join(root, 'current')) == os.path.join('builds', '20261026T080000Z')
    assert os.readlink(os.path.join(root, 'state')) == os.path.join('states', '20261026T080000Z')
    assert m['release'] == '2026-W43'


def _gate_tables(con, values):
    cols = ', '.join(f'"{c}" {t}' for c, t in metrics.OUT_COLS)
    con.execute(f"CREATE OR REPLACE TABLE out ({cols})")
    con.execute(f"CREATE OR REPLACE TABLE out_hist ({', '.join(f'{chr(34)}{c}{chr(34)} {t}' for c, t in metrics.HIST_COLS)})")
    for week, rir, value, st in values:
        con.execute(f"""INSERT INTO out VALUES (DATE '{week}', '1w', '{rir}', 'all', '', 'crl_window', 'p50', {value},
            100, 20, 0.1, '{st}', '2', '', TIMESTAMPTZ '2026-10-26 00:40:01+00', 1, 1, 'x', true)""")


def test_gate_checks_nan_duplicates_status_rirs_and_shrinking():
    con = duckdb.connect()
    _gate_tables(con, [('2026-10-19', 'arin', "'nan'", 'final'), ('2026-10-19', 'arin', '1.0', 'final'),
                       ('2026-10-19', 'arin', '2.0', 'great')])
    with pytest.raises(RuntimeError) as e:
        assemble.gate(con, ('arin',), None)
    assert 'out: 1 rows with NaN/inf' in str(e.value) and 'out: 2 duplicate keys' in str(e.value)
    assert '1 rows with an unknown status' in str(e.value)
    _gate_tables(con, [('2026-10-12', 'arin', '1.0', 'final'), ('2026-10-12', 'apnic', '1.0', 'final'),
                       ('2026-10-19', 'arin', '1.0', 'final')])
    with pytest.raises(RuntimeError, match='crl_window: newest week 2026-10-19 has no rows for apnic'):
        assemble.gate(con, ('apnic', 'arin'), None)
    prev = {'metrics': {'crl_window': {'weeks': {'count': 3}}, 'retired_metric': {'weeks': {'count': 9}}}}
    with pytest.raises(RuntimeError, match='crl_window: 2 weeks, fewer than the 3 of the previous build'):
        assemble.gate(con, ('arin',), prev)
    assemble.gate(con, ('arin',), prev, allow_shrink=True)
    _gate_tables(con, [])
    with pytest.raises(RuntimeError, match='no rows'):
        assemble.gate(con, ('arin',), None)


def test_releases_and_changes(froot, fake):
    """The first weekly release has a CHANGES.csv with the header only; the next week's lists the rows added
    (new weeks) and changed (value or status) since; a second run in the same week makes <week>-2.
    Release files are the build's files (hard links; scorecard.csv gzip-compressed instead) and are never
    modified."""
    root = froot
    m1, b1, _ = run_fake(root, '2026-10-26 00:40:01', stamp='20261026T050000Z')
    assert m1['release'] == '2026-W43'
    rd = os.path.join(root, 'releases')
    ch1 = open(os.path.join(rd, '2026-W43', 'CHANGES.csv')).read().splitlines()
    assert ch1 == ['change,week,window,rir,population,operator,metric_id,stat,old_value,new_value,old_status,'
                   'new_status,old_n,new_n']
    assert open(os.path.join(rd, '2026-W43', 'scorecard.parquet'), 'rb').read() == \
        open(os.path.join(b1, 'scorecard.parquet'), 'rb').read()
    assert sorted(os.listdir(os.path.join(rd, '2026-W43'))) == [
        'CHANGES.csv', 'annotations.csv', 'histograms.parquet', 'ledger.parquet', 'manifest.json', 'release.json',
        'scorecard.csv.gz', 'scorecard.parquet']
    with gzip.open(os.path.join(rd, '2026-W43', 'scorecard.csv.gz')) as f:
        assert f.read() == open(os.path.join(b1, 'scorecard.csv'), 'rb').read()      # the build's CSV
    assert os.stat(os.path.join(rd, '2026-W43', 'scorecard.parquet')).st_ino == \
        os.stat(os.path.join(b1, 'scorecard.parquet')).st_ino
    fake.value[0] = 9.0
    m2, _, _ = run_fake(root, '2026-11-02 00:40:01', stamp='20261102T050000Z')
    assert m2['release'] == '2026-W44'
    rel = json.load(open(os.path.join(rd, '2026-W44', 'release.json')))
    assert rel['previous_release'] == '2026-W43'
    con = duckdb.connect()
    ch = con.execute(f"SELECT change, metric_id, stat, CAST(week AS VARCHAR), old_value, new_value, old_status, new_status "
                     f"FROM read_csv('{rd}/2026-W44/CHANGES.csv') WHERE rir = 'arin' AND population = 'all' "
                     f"ORDER BY ALL").fetchall()
    # added: t_window 10-26, t_share a/b_h7/b_h35 of 10-19; changed: b_h35 09-21..10-12 (value 1 -> 9;
    # 09-21 also provisional -> final)
    added = {(m, s, w) for c, m, s, w, *_ in ch if c == 'added'}
    assert added == {('t_window', 'a', '2026-10-26'), ('t_share', 'a', '2026-10-19'), ('t_share', 'b_h7', '2026-10-19'),
                     ('t_share', 'b_h35', '2026-10-19')}
    changed = {(s, w): x for c, m, s, w, *x in ch if c == 'changed'}
    assert set(changed) == {('b_h35', w.isoformat()) for w in mondays('2026-09-21', '2026-10-12')}
    assert changed[('b_h35', '2026-09-21')] == [1.0, 9.0, 'provisional', 'final']
    assert rel['changes'] == {'added': 4 * 2 * 2, 'changed': 4 * 2 * 2}
    m3, _, _ = run_fake(root, '2026-11-02 00:40:01', stamp='20261102T060000Z')
    assert m3['release'] == '2026-W44-2'
    assert open(os.path.join(rd, '2026-W44-2', 'CHANGES.csv')).read().count('\n') == 1     # nothing changed
    assert sorted(os.listdir(rd)) == ['2026-W43', '2026-W44', '2026-W44-2']
    _, _, _ = run_fake(root, '2026-11-02 00:40:01', stamp='20261102T070000Z', backfill=(D(2026, 9, 14), D(2026, 9, 14)))
    assert sorted(os.listdir(rd)) == ['2026-W43', '2026-W44', '2026-W44-2']                   # no release
    assert sorted(os.listdir(os.path.join(root, 'builds'))) == ['20261102T050000Z', '20261102T060000Z',
                                                               '20261102T070000Z']           # keep 3


# ── stub metric end to end through scs.run ───────────────────────────────────
HOSTS = {'afrinic': 'rpki.afrinic.net', 'apnic': 'rpki.apnic.net', 'arin': 'rpki.arin.net',
         'lacnic': 'repository.lacnic.net', 'ripencc': 'rpki.ripe.net'}


def stub_world(base, end=None, snapshots=True):
    """Per RIR: 12 hosted CA keys with one CRL each per day 2026-09-28..11-01 (window 48 h), one key on
    rpki-rps.arin.net whose CA certificate sits in warehouse-ripencc (an ARIN-issued CA cert published
    elsewhere: the cross-warehouse lookup), one key without any CA certificate; presence runs ending
    2026-11-02 00:40:01 (LACNIC 01:00:01), or all at `end`; snapshot directories unless snapshots=False."""
    for i, (rir, h) in enumerate(HOSTS.items()):
        certs = [{'cert_id': i * 100 + k, 'ski': f'{rir}-k{k}', 'aki': f'{rir}-ta', 'is_ca_flag': True,
                  'not_before': '2026-01-01 00:00:00', 'not_after': '2027-01-01 00:00:00',
                  'sia': f'rsync://{h}/repo/{k}/,https://rrdp.{h}/notification.xml,rsync://{h}/repo/{k}/m.mft'}
                 for k in range(12)]
        if rir == 'ripencc':
            certs += [{'cert_id': 999, 'ski': 'arin-paas', 'aki': 'x', 'is_ca_flag': True,
                       'not_before': '2026-01-01 00:00:00', 'sia': 'rsync://rpki-rps.arin.net/repo/p/'}]
        W.write_table(base, rir, 'certs/is_ca=true', certs)
        crls, cid = [], i * 100000
        for d in range(35):
            t = dt.datetime(2026, 9, 28, 3) + dt.timedelta(days=d)
            for k in [f'k{k}' for k in range(12)] + ['paas', 'orphan']:
                cid += 1
                crls.append({'crl_id': cid, 'object_id': cid, 'aki': f'{rir}-{k}', 'revoked_count': 0,
                             'this_update': str(t), 'next_update': str(t + dt.timedelta(hours=48))})
        W.write_table(base, rir, 'crls', crls)
        e = end or ('2026-11-02 01:00:01' if rir == 'lacnic' else '2026-11-02 00:40:01')
        W.write_table(base, rir, 'object_presence_runs',
                      [{'object_id': 1, 'start_sid': 0, 'end_sid': 9, 'start_ts': '2026-09-20 00:00:01', 'end_ts': e}])
        if snapshots:
            W.snapshots(base, rir, W.cycles('2026-09-14', '2026-11-02')[:-60])


def test_stub_metric_end_to_end(tmp_path, capsys, monkeypatch):
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    stub_world(base)
    monkeypatch.setattr(common, 'may_start', lambda now=None: True)
    ledger.run_ledger(root, base, cut='2026-11-02 05:00:00', min_age_s=0)
    monkeypatch.setattr(common, 'MIN_PART_AGE_S', 0)
    monkeypatch.setattr(common, 'utc_now', lambda: dt.datetime(2026, 11, 2, 5, 0, 13))   # the Monday run
    argv = ['--root', root, '--warehouse-base', base, '--metrics', 'stub_crl_count', '--lock', str(tmp_path / 'lock')]
    assert run.main(argv) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out['as_of'] == '2026-11-02T00:40:01Z' and out['newest_week'] == '2026-10-26' and out['weeks'] == 6
    assert out['release'] == '2026-W44' and out['warnings'] == [] and out['mode'] == 'weekly'
    b = os.path.realpath(os.path.join(root, 'current'))
    r = read_rows(f'{b}/scorecard.parquet', "week = DATE '2026-10-19' AND stat = 'crls'")
    for rir in RIRS:
        assert r[(rir, 'all', 'stub_crl_count', 'crls', D(2026, 10, 19))]['value'] == 14 * 7
        assert r[(rir, 'rir_hosted', 'stub_crl_count', 'crls', D(2026, 10, 19))]['n_ca'] == 12
        assert r[(rir, 'unknown', 'stub_crl_count', 'crls', D(2026, 10, 19))]['status'] == 'insufficient'
    assert r[('arin', 'rir_paas', 'stub_crl_count', 'crls', D(2026, 10, 19))]['value'] == 7       # cross-warehouse
    assert ('arin', 'rir_paas', 'stub_crl_count', 'crls', D(2026, 10, 19)) in r
    assert ('ripencc', 'unknown', 'stub_crl_count', 'crls', D(2026, 10, 19)) in r
    assert r[('arin', 'all', 'stub_crl_count', 'crls', D(2026, 10, 19))]['status'] == 'final'
    assert r[('arin', 'all', 'stub_crl_count', 'crls', D(2026, 10, 19))]['top1_share'] == round(1 / 14, 10)
    m = json.load(open(f'{b}/manifest.json'))
    assert m['inputs']['presence_end_by_rir']['lacnic'] == '2026-11-02T01:00:01Z'
    assert m['hostmap']['version'] == 1 and list(m['definitions']) == ['stub_crl_count']
    assert sorted(os.listdir(b)) == ['annotations.csv', 'histograms.parquet', 'ledger.parquet', 'manifest.json',
                                     'scorecard.csv', 'scorecard.parquet']
    assert not os.listdir(os.path.join(root, 'tmp')) or all(not n.startswith('run-') for n in os.listdir(os.path.join(root, 'tmp')))
    h = duckdb.connect().execute(f"SELECT bin_lo, bin_hi, count FROM read_parquet('{b}/histograms.parquet') "
                                 "WHERE rir = 'arin' AND population = 'all' AND week = DATE '2026-10-19'").fetchall()
    assert h == [(48.0, 72.0, 98)]


def test_run_checks_the_ledger_the_calendar_and_the_state(tmp_path, capsys, monkeypatch):
    """scs.run on Monday 2026-11-09 05:00 while the presence tables end on Sunday 11-08 23:40:01: a ledger
    that ends before as_of stops the run (its later days would count as gaps, and the weeks closed now
    would be frozen as masked) unless --allow-stale-ledger is given; a weekly run whose as_of is before
    this week's Monday warns (week 2026-W45 is not in the presence tables yet, the newest week stays
    2026-W44), one with --as-of does not; a `state` link whose target is gone is an error, and no state
    while a previous build exists is a warning."""
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    stub_world(base, end='2026-11-08 23:40:01', snapshots=False)
    monkeypatch.setattr(common, 'may_start', lambda now=None: True)
    monkeypatch.setattr(common, 'utc_now', lambda: dt.datetime(2026, 11, 9, 5, 0, 13))
    stamps, build = iter(f'20261109T05{i:02d}00Z' for i in range(10)), assemble.build   # runs within a second
    monkeypatch.setattr(assemble, 'build', lambda *a, **k: build(*a, stamp=next(stamps), **k))
    argv = ['--root', root, '--warehouse-base', base, '--metrics', 'stub_crl_count', '--lock', str(tmp_path / 'lock')]

    def main(*extra):
        assert run.main(argv + list(extra)) == 0
        return json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    calendar = ("as_of 2026-11-08T23:40:01Z is before this week's Monday 2026-11-09: week 2026-W45 is not complete "
                "in the presence tables, so the newest week is 2026-W44")
    build_ledger(root, '2026-11-08 05:00:00', '2026-09-14', '2026-11-08', rirs=RIRS)
    with pytest.raises(SystemExit, match='ledger ends 2026-11-08T05:00:00Z, before as_of 2026-11-08T23:40:01Z'):
        run.main(argv)
    assert not os.path.lexists(os.path.join(root, 'current'))
    out = main('--allow-stale-ledger', '--no-release')
    assert out['warnings'] == [calendar, 'ledger ends 2026-11-08T05:00:00Z, before as_of 2026-11-08T23:40:01Z: '
                                         'later days count as gaps']
    build_ledger(root, '2026-11-09 05:00:00', '2026-09-14', '2026-11-09', rirs=RIRS)
    out = main()
    assert out['warnings'] == [calendar] and out['newest_week'] == '2026-10-26' and out['release'] == '2026-W44'
    assert main('--as-of', '2026-11-08 23:40:01', '--no-release')['warnings'] == []
    shutil.rmtree(os.path.join(root, 'states'))
    with pytest.raises(RuntimeError, match=r'state -> .*/states/\d{8}T\d{6}Z is missing: restore it'):
        run.main(argv)
    os.unlink(os.path.join(root, 'state'))
    out = main('--no-release')
    assert out['warnings'][0] == calendar and out['warnings'][1].startswith('no state under ')
