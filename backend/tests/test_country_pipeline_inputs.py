"""Input steps of the country pipeline (VRP intervals, delegations, AS names) on synthetic data:
delegated-file parsing, overlap resolution, RIPE roas.csv parsing, interval bridging, the VRP union and
download integrity."""
import datetime as dt
import gzip
import json
import lzma
import os
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipelines', 'country'))
from crc import asnames, deleg, fetch, intervals, ripe_archive, vrp_export, vrp_merge, vrp_subtal  # noqa: E402

D = dt.date


def _con(tmp_path):
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{tmp_path}/spill'")
    return con


# ── delegated stats ──────────────────────────────────────────────────────────
NRO = """2|nro|20261024|9|19821213|20261024|+0000
nro|*|asn|*|3|summary
nro|*|ipv4|*|4|summary
nro|*|ipv6|*|2|summary
iana|ZZ|asn|0|1|20140311|reserved|ietf|iana
arin|US|asn|1|1|20010920|assigned|e5e3b9c1|e-stats
ripencc|EU|asn|64000|10|20050101|assigned|org-1|e-stats
ripencc|DE|ipv4|1.2.3.0|768|20100101|assigned|org-2|e-stats
apnic||ipv4|5.0.0.0|256|20100101|assigned|org-3|e-stats
lacnic|ZZ|ipv4|6.0.0.0|512|20100101|available|lacnic|e-stats
arin|ZZ|ipv4|7.0.0.0|1024|20100101|reserved|arin|e-stats
apnic||ipv6|2001:de3::|48|20100101|assigned|org-4|e-stats
ripencc|NL|ipv6|2a00:8::|29|20100101|assigned|org-5|e-stats
ripencc|NL|ipv6|2a01:1::|29|20100101|assigned|org-6|e-stats
"""


def test_nro_combined_parsing():
    recs, skipped = deleg.parse_lines(NRO.splitlines(), default_registry=None)
    assert skipped == {}
    assert len(recs) == 10                        # version + 3 summary lines skipped
    by = {(r[0], r[4]): r for r in recs}
    # ASNs: ranges, iana reserved -> free/ZZ
    assert by[(0, 0)] == (0, 'iana', 'ZZ', 'free', 0, 0)
    assert by[(0, 1)] == (0, 'arin', 'US', 'delegated', 1, 1)
    assert by[(0, 64000)] == (0, 'ripencc', 'EU', 'delegated', 64000, 64009)
    # IPv4: 768 addresses is not a CIDR block -> kept as a range
    s = (1 << 24) + (2 << 16) + (3 << 8)
    assert by[(4, s)] == (4, 'ripencc', 'DE', 'delegated', s, s + 767)
    # empty cc -> ZZ; available/reserved -> free
    assert by[(4, 5 << 24)][2:4] == ('ZZ', 'delegated')
    assert by[(4, 6 << 24)][2:] == ('ZZ', 'free', 6 << 24, (6 << 24) + 511)
    assert by[(4, 7 << 24)][3] == 'free'
    # IPv6: upper 64 bits, /64 units
    k = 0x20010de300000000
    assert by[(6, k)] == (6, 'apnic', 'ZZ', 'delegated', k, k + (1 << 16) - 1)
    k2 = 0x2a00000800000000
    assert by[(6, k2)][5] == k2 + (1 << 35) - 1
    assert (6, 0x2a01000000000000) in by          # a start that is not aligned is masked to its /29
    assert deleg.count_overlaps(recs) == 0


def test_old_nro_iana_statuses_and_long_ipv6():
    lines = ['iana|ZZ|asn|23456|1|20011001|ietf|ietf|iana',
             'iana|ZZ|ipv4|0.0.0.0|16777216|19810901|ianapool|iana|iana',
             'apnic|AU|ipv6|2001:db8:1:2::|96|20100101|assigned|x|e-stats',
             'arin|US|ipv4|1.0.0.0|abc|20100101|assigned', 'arin|US|weird|1|1|2010|assigned',
             'arin|US|ipv4|1.0.0.0|256|20100101|whatever']
    recs, skipped = deleg.parse_lines(lines)
    assert [r[3] for r in recs[:2]] == ['free', 'free']
    k = 0x20010db800010002                       # longer than /64 -> one /64 unit
    assert recs[2][4:] == (k, k)
    assert skipped == {'value': 1, 'type': 1, 'status:whatever': 1}


def test_legacy_per_rir_lines():
    legacy = """2|arin|20120101|5|00000000|20111231|-0500
arin|*|asn|*|2|summary
arin|*|ipv4|*|2|summary
arin|*|ipv6|*|1|summary
arin|US|asn|1|1|20010920|allocated
arin|CA|asn|5000|3|19910110|assigned
arin|US|ipv4|3.0.0.0|16777216|19880223|allocated
arin|us|ipv4|4.0.0.0|192|19921201|assigned
arin|US|ipv6|2001:400::|32|19990803|allocated
afrinic|MU|asn|5.1|1|20070504|allocated
2.1ap|apnic|20120101|39345||20111230|+1000
"""
    recs, skipped = deleg.parse_lines(legacy.splitlines(), default_registry='arin')
    assert skipped == {}
    assert (0, 'arin', 'CA', 'delegated', 5000, 5002) in recs
    assert (4, 'arin', 'US', 'delegated', 3 << 24, (4 << 24) - 1) in recs
    assert (4, 'arin', 'US', 'delegated', 4 << 24, (4 << 24) + 191) in recs   # lower-case cc fixed
    assert (0, 'afrinic', 'MU', 'delegated', 5 * 65536 + 1, 5 * 65536 + 1) in recs   # asdot
    assert len(recs) == 6


def test_overlap_resolution_most_specific_wins():
    big = (4, 'arin', 'US', 'delegated', 0, 1023, 'rir-legacy:20120101')
    small = (4, 'ripencc', 'DE', 'delegated', 256, 511, 'rir-extended:20120101')
    dup_free = (4, 'arin', 'ZZ', 'free', 256, 511, 'rir-legacy:20120101')
    other = (4, 'apnic', 'JP', 'delegated', 2000, 2047, 'rir-extended:20120101')
    asn_a = (0, 'ripencc', 'GB', 'delegated', 100, 100, 'x')
    asn_b = (0, 'arin', 'US', 'delegated', 100, 100, 'y')
    recs = [big, small, dup_free, other, asn_a, asn_b]
    assert deleg.count_overlaps(recs) == 3
    out, stats = deleg.resolve_overlaps(recs)
    v4 = sorted((r for r in out if r[0] == 4), key=lambda r: r[4])
    assert v4 == [
        (4, 'arin', 'US', 'delegated', 0, 255, 'rir-legacy:20120101'),        # big keeps its remainder
        (4, 'ripencc', 'DE', 'delegated', 256, 511, 'rir-extended:20120101'),  # smallest wins; delegated beats free
        (4, 'arin', 'US', 'delegated', 512, 1023, 'rir-legacy:20120101'),
        other]
    assert [r for r in out if r[0] == 0] == [asn_b]   # equal size: registry name decides (arin < ripencc)
    assert deleg.count_overlaps(out) == 0
    assert stats[4] == {'overlapping_records': 3, 'clusters': 1}
    assert sum(r[5] - r[4] + 1 for r in v4) == 1024 + 48   # no space lost or double counted


def test_overlap_resolution_nested_chain():
    recs = [(6, 'apnic', 'AU', 'delegated', 0, 99, ''), (6, 'apnic', 'NZ', 'delegated', 10, 59, ''),
            (6, 'apnic', 'FJ', 'delegated', 20, 29, ''), (6, 'apnic', 'TO', 'delegated', 50, 79, '')]
    out, _ = deleg.resolve_overlaps(recs)
    cc_at = {}
    for r in out:
        for k in range(r[4], r[5] + 1):
            assert k not in cc_at
            cc_at[k] = r[2]
    assert len(cc_at) == 100
    assert (cc_at[5], cc_at[15], cc_at[25], cc_at[55], cc_at[65], cc_at[85]) == ('AU', 'NZ', 'FJ', 'TO', 'TO', 'AU')


def test_build_month_from_raw_files(tmp_path):
    root = str(tmp_path)
    files = [
        {'registry': 'arin', 'name': 'delegated-arin-20120101.gz', 'kind': 'rir-legacy', 'date': '20120101', 'url': 'u1'},
        {'registry': 'ripencc', 'name': 'delegated-ripencc-extended-20120102.bz2', 'kind': 'rir-extended',
         'date': '20120102', 'url': 'u2'},
        {'registry': 'lacnic', 'name': 'delegated-lacnic-20120101', 'kind': 'rir-legacy', 'date': '20120101', 'url': 'u3'},
    ]
    import bz2
    texts = {
        0: "2|arin|20120101|2|0|0|-0500\narin|*|ipv4|*|2|summary\narin|US|ipv4|8.0.0.0|16777216|19921201|allocated\n"
           "arin|US|asn|3356|1|20000301|allocated\n",
        1: "2|ripencc|1|2|0|0|+0100\nripencc|*|ipv4|*|2|summary\nripencc|GB|ipv4|8.8.0.0|65536|20100101|allocated|o|e\n"
           "ripencc||ipv4|9.0.0.0|256|20100101|available|o|e\n",
        2: "2|lacnic|20120101|1|0|0|-0200\nlacnic|BR|ipv6|2801::|16|20100101|allocated\n",
    }
    os.makedirs(os.path.join(deleg.raw_dir(root), 'arin'))
    os.makedirs(os.path.join(deleg.raw_dir(root), 'ripencc'))
    os.makedirs(os.path.join(deleg.raw_dir(root), 'lacnic'))
    with gzip.open(deleg.local_path(root, files[0]), 'wt') as f:
        f.write(texts[0])
    with bz2.open(deleg.local_path(root, files[1]), 'wt') as f:
        f.write(texts[1])
    assert deleg.local_path(root, files[2]).endswith('.' + fetch.RAW_COMPRESS)   # uncompressed source kept compressed
    with open(deleg.local_path(root, files[2]), 'wb') as raw:
        c = fetch._compressor(fetch.RAW_COMPRESS, raw)
        c.write(texts[2].encode())
        c.close()
    con = _con(tmp_path)
    info = deleg.build_month(con, root, 2012, 1, files)
    assert info['overlapping_records_in'] == 1 and info['overlaps_out'] == 0
    assert deleg.month_is_current(root, 2012, 1, files) is not None
    pq = os.path.join(deleg.months_dir(root), '2012-01.parquet')
    types = dict((r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM '{pq}'").fetchall())
    assert types == {'snap': 'DATE', 'afi': 'UTINYINT', 'registry': 'VARCHAR', 'cc': 'VARCHAR', 'status': 'VARCHAR',
                     'start_': 'UBIGINT', 'end_': 'UBIGINT', 'src': 'VARCHAR'}
    rows = con.execute(f"SELECT snap, afi, registry, cc, status, start_, end_, src FROM '{pq}' ORDER BY afi, start_").fetchall()
    snap = D(2012, 1, 1)
    assert rows == [
        (snap, 0, 'arin', 'US', 'delegated', 3356, 3356, 'rir-legacy:20120101'),
        (snap, 4, 'arin', 'US', 'delegated', 8 << 24, (8 << 24) + (8 << 16) - 1, 'rir-legacy:20120101'),
        (snap, 4, 'ripencc', 'GB', 'delegated', (8 << 24) + (8 << 16), (8 << 24) + (9 << 16) - 1, 'rir-extended:20120102'),
        (snap, 4, 'arin', 'US', 'delegated', (8 << 24) + (9 << 16), (9 << 24) - 1, 'rir-legacy:20120101'),
        (snap, 4, 'ripencc', 'ZZ', 'free', 9 << 24, (9 << 24) + 255, 'rir-extended:20120102'),
        (snap, 6, 'lacnic', 'BR', 'delegated', 0x2801 << 48, (0x2802 << 48) - 1, 'rir-legacy:20120101'),
    ]


def _write_raw(root, f, text):
    p = deleg.local_path(root, f)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, 'wb') as raw:
        c = fetch._compressor(fetch.RAW_COMPRESS, raw)
        c.write(text.encode())
        c.close()
    return p


def test_file_problem_and_garbage_source_rejected(tmp_path):
    """Regression: an HTML error page (or any non-stats file) served with HTTP 200 for an
    uncompressed source parsed to 0 records and silently produced an empty month."""
    assert deleg.file_problem(5826 + 1, 5826) is None                     # one extra record is tolerated
    assert deleg.file_problem(9000, 10000) is not None                    # 10% short
    assert deleg.file_problem(10, None) is not None                       # no version line
    assert deleg.file_problem(0, 0) is not None
    assert deleg.file_problem(10, 10, {'arin', 'ripencc', 'apnic', 'lacnic', 'afrinic', 'iana'}) is None
    assert 'afrinic' in deleg.file_problem(10, 10, {'arin', 'ripencc', 'apnic', 'lacnic', 'iana'})
    root = str(tmp_path)
    f = {'registry': 'nro', 'name': '20261101.combined-stat', 'kind': 'nro-combined', 'date': '20261101', 'url': 'u'}
    _write_raw(root, f, '<html><body>Service temporarily unavailable</body></html>\n')
    with pytest.raises(deleg.BadSourceFile):
        deleg.build_month(_con(tmp_path), root, 2026, 11, [f])
    assert not os.path.exists(os.path.join(deleg.months_dir(root), '2026-11.parquet'))
    hdr = {}
    deleg.parse_lines(NRO.splitlines(), header=hdr)
    assert hdr == {'declared': 9}


def test_deleg_main_bad_current_month_fails_run(tmp_path, monkeypatch):
    """A bad file fails the run even for the current month (an unresolved current month alone is
    exit 0) and is moved aside as .bad; good months are still written, and a no-op rerun does not
    rewrite the manifest."""
    root = str(tmp_path)
    nro = NRO.replace('ripencc|NL|ipv6|2a01:1::|29', 'afrinic|ZA|ipv6|2c0f:f000::|29').replace('|9|19821213', '|10|19821213')
    good = {'registry': 'nro', 'name': '20261001.combined-stat', 'kind': 'nro-combined', 'date': '20261001', 'url': 'u1'}
    bad = {'registry': 'nro', 'name': '20261101.combined-stat', 'kind': 'nro-combined', 'date': '20261101', 'url': 'u2'}
    _write_raw(root, good, nro + 'afrinic|ZA|asn|36864|1|20050101|assigned|x|e-stats\n')
    _write_raw(root, bad, '<html>oops</html>\n')
    monkeypatch.setattr(deleg, 'resolve_month', lambda root_, listings, manifest, y, m: {(2026, 10): [good], (2026, 11): [bad]}[(y, m)])
    args = ['--root', root, '--from', '2026-10', '--to', '2026-11', '--threads', '2', '--memory-limit', '1GB']
    assert deleg.main(args) == 1
    meta = json.load(open(os.path.join(root, 'inputs', 'deleg.json')))
    assert meta['months_present'] == ['2026-10'] and meta['months_unresolved'] == ['2026-11'] and len(meta['bad_files']) == 1
    con = _con(tmp_path)
    assert con.execute(f"SELECT count(*) FROM '{root}/inputs/deleg.parquet' WHERE snap = DATE '2026-10-01'").fetchone()[0] == 11
    assert not os.path.exists(deleg.local_path(root, bad))                # quarantined: the next run fetches it again
    assert os.path.exists(deleg.local_path(root, bad) + '.bad')
    monkeypatch.setattr(deleg, 'resolve_month', lambda root_, listings, manifest, y, m: {(2026, 10): [good], (2026, 11): None}[(y, m)])
    assert deleg.main(args) == 0                                       # current month not published yet: fine
    man = os.path.join(deleg.raw_dir(root), 'manifest.json')
    t = os.stat(man).st_mtime_ns
    assert deleg.main(args) == 0 and os.stat(man).st_mtime_ns == t     # no-op rerun leaves the manifest alone


def test_find_rir_file_prefers_first_day_then_extended():
    class L:
        def get(self, url):
            return {'delegated-lacnic-20120601', 'delegated-lacnic-extended-20120628', 'delegated-lacnic-20120628',
                    'delegated-afrinic-extended-20150107', 'delegated-afrinic-20150107'}
    f = deleg.find_rir_file(L(), 'lacnic', 2012, 6)
    assert (f['name'], f['kind']) == ('delegated-lacnic-20120601', 'rir-legacy')
    f = deleg.find_rir_file(L(), 'afrinic', 2015, 1)
    assert (f['name'], f['kind'], f['date']) == ('delegated-afrinic-extended-20150107', 'rir-extended', '20150107')
    assert f['url'] == 'https://ftp.afrinic.net/pub/stats/afrinic/2015/delegated-afrinic-extended-20150107'
    assert deleg.find_rir_file(L(), 'arin', 2015, 1) is None

    class R:   # RIPE NCC extended files before 2012-02 have no cc on allocations -> legacy first
        def get(self, url):
            return {'delegated-ripencc-extended-20120101.bz2', 'delegated-ripencc-20120101.bz2',
                    'delegated-ripencc-extended-20120201.bz2', 'delegated-ripencc-20120201.bz2'}
    assert deleg.find_rir_file(R(), 'ripencc', 2012, 1)['name'] == 'delegated-ripencc-20120101.bz2'
    assert deleg.find_rir_file(R(), 'ripencc', 2012, 2)['name'] == 'delegated-ripencc-extended-20120201.bz2'
    assert deleg.month_range((2019, 11), (2020, 2)) == [(2019, 11), (2019, 12), (2020, 1), (2020, 2)]


# ── RIPE archive ─────────────────────────────────────────────────────────────
CSV_HEAD = 'URI,ASN,IP Prefix,Max Length,Not Before,Not After\n'


def _write_day(root, day, ta, rows):
    p = ripe_archive.raw_path(root, day, ta)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    body = CSV_HEAD + ''.join(f'"rsync://x/{i},y.roa",{a},{pfx},{ml},2026-01-01 00:00:00,2027-01-01 00:00:00\n'
                              for i, (a, pfx, ml) in enumerate(rows))
    with lzma.open(p, 'wt') as f:
        f.write(body)


def test_roas_csv_parsing(tmp_path):
    root = str(tmp_path)
    day = D(2026, 4, 8)
    _write_day(root, day, 'apnic', [('AS123', '10.0.0.0/8', '24'), ('AS123', '10.0.0.0/8', '24'),
                                    ('AS0', '192.0.2.0/24', ''), ('AS4200000000', '2001:DB8::/32', ''),
                                    ('as65000', '2001:db8:1::/48', '64')])
    _write_day(root, day, 'arin', [('AS7', '8.0.0.0/9', '16')])
    con = _con(tmp_path)
    counts = ripe_archive.convert_day(con, root, day, str(tmp_path))
    assert counts == {'apnic': 4, 'arin': 1}
    pq = ripe_archive.day_parquet(root, day)
    rows = con.execute(f"SELECT d, ta, afi, prefix, max_length, asn FROM '{pq}' ORDER BY ta, afi, prefix").fetchall()
    assert rows == [
        (day, 'apnic', 4, '10.0.0.0/8', 24, 123),
        (day, 'apnic', 4, '192.0.2.0/24', 24, 0),              # blank Max Length = prefix length
        (day, 'apnic', 6, '2001:db8:1::/48', 64, 65000),
        (day, 'apnic', 6, '2001:db8::/32', 32, 4200000000),    # IPv6 lower-cased, 32-bit ASN
        (day, 'arin', 4, '8.0.0.0/9', 16, 7),
    ]
    types = [r[1] for r in con.execute(f"DESCRIBE SELECT * FROM '{pq}'").fetchall()]
    assert types == ['DATE', 'VARCHAR', 'UTINYINT', 'VARCHAR', 'UTINYINT', 'UBIGINT']
    assert not ripe_archive.stale(root, day)
    assert ripe_archive.day_url(day, 'apnic') == 'https://ftp.ripe.net/rpki/apnic.tal/2026/04/08/roas.csv.xz'


def test_archive_intervals_bridge_up_to_3_days(tmp_path):
    root = str(tmp_path)
    con = _con(tmp_path)
    a = ('AS1', '10.0.0.0/8', '8')        # present days 1-3, gap 3 (4-6), present 7-8, gap 4 (9-12), present 13
    b = ('AS2', '11.0.0.0/8', '8')        # every day
    days = {k: D(2026, 5, k) for k in range(1, 14)}
    for k, d in days.items():
        rows = [b] + ([a] if k in (1, 2, 3, 7, 8, 13) else [])
        _write_day(root, d, 'ripencc', rows)
        ripe_archive.convert_day(con, root, d, str(tmp_path))
    out, stats = ripe_archive.build_intervals(con, root, days[1], days[13])
    got = con.execute(f"SELECT prefix, first_day, last_day, bridged_days, src, rir FROM '{out}' ORDER BY prefix, first_day").fetchall()
    assert got == [('10.0.0.0/8', days[1], days[8], 3, 'ripe-archive', 'ripencc'),
                   ('10.0.0.0/8', days[13], days[13], 0, 'ripe-archive', 'ripencc'),
                   ('11.0.0.0/8', days[1], days[13], 0, 'ripe-archive', 'ripencc')]
    assert stats['gaps'] == {'tier1': 1, 'tier2': 0, 'gaps': 2}
    assert stats['vrps_last_day'] == 2


# ── warehouse-style two-tier bridging ────────────────────────────────────────
def test_two_tier_bridging_with_bad_days(tmp_path):
    con = _con(tmp_path)
    con.execute("CREATE TABLE r (grp VARCHAR, prefix VARCHAR, max_length INT, asn BIGINT, afi INT, first_day DATE, last_day DATE)")
    rows = []
    for i in range(20):   # 20 stable VRPs keep the median high
        rows.append(('arin', f'10.{i}.0.0/16', 16, i, 4, D(2020, 1, 1), D(2020, 3, 31)))
    rows += [('arin', '11.0.0.0/8', 8, 1, 4, D(2020, 1, 1), D(2020, 1, 31)),
             ('arin', '11.0.0.0/8', 8, 1, 4, D(2020, 2, 11), D(2020, 3, 31)),     # 10-day gap, all days bad -> tier 2
             ('arin', '12.0.0.0/8', 8, 1, 4, D(2020, 1, 1), D(2020, 1, 10)),
             ('arin', '12.0.0.0/8', 8, 1, 4, D(2020, 1, 16), D(2020, 3, 31)),     # 5-day gap on good days -> kept
             ('arin', '13.0.0.0/8', 8, 1, 4, D(2020, 1, 1), D(2020, 1, 10)),
             ('arin', '13.0.0.0/8', 8, 1, 4, D(2020, 1, 14), D(2020, 3, 31))]     # 3-day gap -> tier 1
    con.executemany("INSERT INTO r VALUES (?,?,?,?,?,?,?)", rows)
    # snapshots every day except 2020-02-01..10 (an outage)
    con.execute("CREATE TABLE snapdays AS SELECT 'arin' AS grp, CAST(d AS DATE) AS d FROM range(DATE '2020-01-01', DATE '2020-04-01', INTERVAL 1 DAY) t(d) "
                "WHERE NOT d BETWEEN DATE '2020-02-01' AND DATE '2020-02-10'")
    con.execute("CREATE TABLE cal AS " + intervals.calendar_sql('r', 'snapdays', D(2020, 1, 1), D(2020, 3, 31)))
    assert con.execute("SELECT count(*) FROM cal WHERE bad").fetchone()[0] == 10
    got = con.execute(intervals.bridge_sql('r', 'cal') + " ORDER BY prefix, first_day").fetchall()
    # columns: grp, prefix, max_length, asn, afi, first_day, last_day, bridged_days
    br = {(g[1], g[5]): (g[6], g[7]) for g in got if not g[1].startswith('10.')}
    assert br == {('11.0.0.0/8', D(2020, 1, 1)): (D(2020, 3, 31), 10),
                  ('12.0.0.0/8', D(2020, 1, 1)): (D(2020, 1, 10), 0),
                  ('12.0.0.0/8', D(2020, 1, 16)): (D(2020, 3, 31), 0),
                  ('13.0.0.0/8', D(2020, 1, 1)): (D(2020, 3, 31), 3)}
    t1, t2, n = con.execute(intervals.gap_stats_sql('r', 'cal')).fetchone()
    assert (t1, t2, n) == (1, 1, 3)


def test_islands_merge_overlapping_and_touching(tmp_path):
    con = _con(tmp_path)
    con.execute("CREATE TABLE x (k INT, d_lo DATE, d_hi DATE)")
    con.executemany("INSERT INTO x VALUES (?,?,?)", [(1, D(2020, 1, 1), D(2020, 1, 3)), (1, D(2020, 1, 2), D(2020, 1, 2)),
                                                     (1, D(2020, 1, 4), D(2020, 1, 4)), (1, D(2020, 1, 6), D(2020, 1, 7)),
                                                     (2, D(2020, 1, 6), D(2020, 1, 7))])
    got = con.execute(intervals.islands_sql('x', ['k']) + " ORDER BY k, first_day").fetchall()
    assert got == [(1, D(2020, 1, 1), D(2020, 1, 4)), (1, D(2020, 1, 6), D(2020, 1, 7)), (2, D(2020, 1, 6), D(2020, 1, 7))]


def test_islands_are_deterministic_with_tied_rows(tmp_path):
    """Regression: the island flag and its running sum are two window sorts; rows tying on (lo, hi)
    could be ordered differently by the two, and a tied row that starts a new island was counted
    into the previous one (real data: 5.252.74.0/24 AS834 'present' across a 364-day gap, 24 rows
    with bridged_days = -1). At this size the old SQL broke on every run with 8 threads."""
    con = _con(tmp_path)
    con.execute("SET threads=8")
    n = 200_000
    con.execute(f"""CREATE TABLE t AS SELECT k, DATE '2020-01-01' + o1 AS d_lo, DATE '2020-01-01' + o2 AS d_hi
                    FROM range({n}) r(k), (VALUES (0, 1), (5, 5), (5, 5), (9, 9)) v(o1, o2) ORDER BY random()""")
    want = [(D(2020, 1, 1), D(2020, 1, 2), n), (D(2020, 1, 6), D(2020, 1, 6), n), (D(2020, 1, 10), D(2020, 1, 10), n)]
    for _ in range(5):
        got = con.execute(f"SELECT first_day, last_day, count(*) FROM ({intervals.islands_sql('t', ['k'])}) "
                          "GROUP BY ALL ORDER BY ALL").fetchall()
        assert got == want


def test_valid_islands_keep_the_validity_of_the_objects_at_both_ends(tmp_path):
    """valid_islands_sql: valid_from = earliest not_before among the objects present on the island's first
    day, valid_until = latest end of validity among those present on its last day (not over the whole
    island), and the result does not depend on the row order."""
    con = _con(tmp_path)
    ts = lambda m, d, h=0: dt.datetime(2026, m, d, h)  # noqa: E731
    con.execute("CREATE TABLE v (k INT, d_lo DATE, d_hi DATE, nb TIMESTAMP, vu TIMESTAMP)")
    con.executemany("INSERT INTO v VALUES (?,?,?,?,?)", [
        (1, D(2026, 1, 1), D(2026, 1, 10), ts(1, 1, 5), ts(1, 10, 7)),    # first object: starts the island
        (1, D(2026, 1, 1), D(2026, 1, 3), ts(1, 1, 2), ts(6, 1)),         # also on the first day, earlier nb
        (1, D(2026, 1, 5), D(2026, 1, 20), ts(1, 4), ts(1, 20, 9)),       # re-issue inside the island: ends it
        (1, D(2026, 1, 8), D(2026, 1, 20), ts(1, 8), ts(1, 20, 3)),       # also on the last day, earlier end
        (1, D(2026, 1, 25), D(2026, 1, 30), ts(1, 25), ts(12, 31)),       # next island
        (2, D(2026, 1, 1), D(2026, 1, 2), None, None)])                   # unknown validity
    for _ in range(3):
        got = con.execute(intervals.valid_islands_sql('(SELECT * FROM v ORDER BY random())', ['k'], vfrom='nb', vuntil='vu')
                          + " ORDER BY k, first_day").fetchall()
        assert got == [(1, D(2026, 1, 1), D(2026, 1, 20), ts(1, 1, 2), ts(1, 20, 9)),
                       (1, D(2026, 1, 25), D(2026, 1, 30), ts(1, 25), ts(12, 31)),
                       (2, D(2026, 1, 1), D(2026, 1, 2), None, None)]


def test_check_invariants_rejects_overlapping_rows(tmp_path):
    con = _con(tmp_path)
    con.execute("CREATE TABLE ok (grp VARCHAR, prefix VARCHAR, max_length INT, asn BIGINT, first_day DATE, last_day DATE, bridged_days INT)")
    con.executemany("INSERT INTO ok VALUES (?,?,?,?,?,?,?)", [
        ('arin', '10.0.0.0/8', 8, 1, D(2020, 1, 1), D(2020, 1, 5), 0),
        ('arin', '10.0.0.0/8', 8, 1, D(2020, 1, 7), D(2020, 1, 9), 2),
        ('ripencc', '10.0.0.0/8', 8, 1, D(2020, 1, 1), D(2020, 1, 9), 0)])   # other group may overlap
    assert intervals.check_invariants(con, 'ok') == {'overlapping': 0, 'inverted': 0, 'negative_bridged': 0}
    for bad in [('arin', '10.0.0.0/8', 8, 1, D(2020, 1, 6), D(2020, 1, 6), 0),     # touches both rows
                ('arin', '11.0.0.0/8', 8, 1, D(2020, 1, 6), D(2020, 1, 5), 0),     # inverted
                ('arin', '12.0.0.0/8', 8, 1, D(2020, 1, 6), D(2020, 1, 6), -1)]:   # negative bridging
        con.execute("CREATE OR REPLACE TABLE bad AS SELECT * FROM ok")
        con.execute("INSERT INTO bad VALUES (?,?,?,?,?,?,?)", bad)
        with pytest.raises(SystemExit):
            intervals.check_invariants(con, 'bad')


def _fake_warehouse(base, rir, objects, prefixes, runs, snaps, sia=None, revoked=None, foreign=()):
    """Minimal warehouse-<rir> with the tables vrp_export reads and snapshots/ts=* directories.

    sia: {object_id: SIA of the object's EE certificate} -> also table=object_signatures and
    table=certs/is_ca=false (one EE certificate per ROA, issuer 'CN=test-ca', serial = cert_id, plus one
    manifest that must be ignored); without it the warehouse cannot map objects to publication points.
    revoked: {object_id: revocation time} -> table=crl_serials, every entry listed in two CRLs and with
    revoked_cert_id NULL (as for most real entries: they are matched by issuer DN and serial); without it
    the warehouse has no CRLs and ROA validity comes from the EE certificates only. foreign: object ids
    whose serial another CA ('CN=other-ca') revoked on 2026-01-01 (must not count)."""
    t = os.path.join(base, f'warehouse-{rir}', 'tables')
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    tables = [
        ('roa_objects', 'object_id INTEGER, asn BIGINT, ee_not_before TIMESTAMPTZ, ee_not_after TIMESTAMPTZ', objects),
        ('roa_prefixes', 'object_id INTEGER, afi UTINYINT, prefix VARCHAR, max_length UTINYINT', prefixes),
        ('object_presence_runs', 'object_id INTEGER, start_sid INTEGER, end_sid INTEGER, start_ts VARCHAR, end_ts VARCHAR',
         [(o, 0, 0, s, e) for o, s, e in runs])]
    if sia:
        tables += [
            ('object_signatures', 'object_id INTEGER, object_type VARCHAR, signer_cert_id INTEGER',
             [(o, 'ROA', 500000 + o) for o in sia] + [(900000, 'MFT', 900000)]),
            ('certs/is_ca=false', 'cert_id INTEGER, object_id INTEGER, sia VARCHAR, is_ca_flag BOOLEAN, issuer_dn VARCHAR, '
                                  'serial_dec VARCHAR',
             [(500000 + o, None, s, False, 'CN=test-ca', str(500000 + o)) for o, s in sia.items()]
             + [(900000, None, 'rsync://manifest.example.net/repo/x.mft', False, 'CN=test-ca', '900000')])]
    if revoked:
        tables += [
            ('crl_serials', 'crl_id INTEGER, serial_dec VARCHAR, revocation_date TIMESTAMPTZ, cert_issuer_dn VARCHAR, '
                            'revoked_cert_id INTEGER',
             [(c, str(500000 + o), ts, 'CN=test-ca', None) for o, ts in revoked.items() for c in (1, 2)]
             + [(1, '900000', '2026-01-02 00:00:00+00', 'CN=test-ca', None)]                     # the manifest's EE
             + [(3, str(500000 + o), '2026-01-01 00:00:00+00', 'CN=other-ca', None) for o in foreign])]
    for i, (name, cols, rows) in enumerate(tables):
        os.makedirs(os.path.join(t, f'table={name}'))
        con.execute(f"CREATE TABLE t{i} ({cols})")
        con.executemany(f"INSERT INTO t{i} VALUES ({', '.join('?' * len(rows[0]))})", rows)
        con.execute(f"COPY t{i} TO '{t}/table={name}/part-0.parquet' (FORMAT parquet)")
    for s in snaps:
        os.makedirs(os.path.join(base, f'warehouse-{rir}', 'snapshots', 'ts=' + s.replace(':', '-')))


def test_vrp_export_end_to_end(tmp_path, monkeypatch):
    """Presence runs x EE window -> UTC days, merged per object and per VRP, <= 3-day gaps bridged,
    clipped to last_complete_day; a RIR whose presence lags is reported as stale."""
    monkeypatch.setattr(vrp_export, 'in_blackout', lambda now=None: False)   # independent of the wall clock
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    ee = ('2026-01-01 00:00:00+00', '2027-01-01 00:00:00+00')
    snaps = [f'2026-10-{d:02d} {h}:00:00' for d in range(1, 12) for h in ('00', '12')] + ['2026-10-12 00:00:00']
    objects = [(1, 1, *ee), (2, 2, ee[0], '2026-10-06 05:00:00+00'), (3, 3, *ee), (4, 3, *ee), (5, 5, *ee), (6, 0, *ee)]
    prefixes = [(1, 4, '10.0.0.0/8', 8), (2, 4, '11.0.0.0/8', 8), (3, 4, '12.0.0.0/8', 16), (4, 4, '12.0.0.0/8', 16),
                (5, 4, '13.0.0.0/8', 8), (6, 6, '2001:db8::/32', 48)]
    runs = [(1, '2026-10-01 00:00:00', '2026-10-03 12:00:00'), (1, '2026-10-05 00:00:00', '2026-10-12 00:00:00'),
            (2, '2026-10-01 00:00:00', '2026-10-12 00:00:00'),                     # EE ends 10-06 05:00
            (3, '2026-10-02 00:00:00', '2026-10-02 00:00:00'), (3, '2026-10-02 12:00:00', '2026-10-04 00:00:00'),
            (4, '2026-10-02 00:00:00', '2026-10-04 00:00:00'),                     # same VRP, tied ranges
            (5, '2026-10-12 00:00:00', '2026-10-12 00:00:00'),                     # only on the incomplete day
            (6, '2026-10-01 00:00:00', '2026-10-02 00:00:00'), (6, '2026-10-09 00:00:00', '2026-10-12 00:00:00')]
    for i in range(20):                                                            # stable VRPs
        objects.append((100 + i, 64500, *ee))
        prefixes.append((100 + i, 4, f'20.{i}.0.0/16', 16))
        runs.append((100 + i, '2026-10-01 00:00:00', '2026-10-12 00:00:00'))
    _fake_warehouse(base, 'arin', objects, prefixes, runs, snaps)
    _fake_warehouse(base, 'ripencc', [(1, 7, *ee)], [(1, 4, '193.0.0.0/21', 24)],
                    [(1, '2026-10-01 00:00:00', '2026-10-09 00:00:00')], snaps[:17])   # presence 3 days behind
    rirs = ('arin', 'ripencc')
    con, raw_rows, max_end, t_read, hosts = vrp_export.export(root, base, threads=2, memory_limit='2GB', rirs=rirs)
    meta = vrp_export.bridge_and_write(con, root, raw_rows, max_end, t_read, rirs=rirs, hosts=hosts)
    out = os.path.join(root, 'inputs', 'warehouse_intervals.parquet')
    got = con.execute(f"SELECT afi, prefix, max_length, asn, first_day, last_day, src, bridged_days, rir FROM '{out}' "
                      "WHERE NOT prefix LIKE '20.%' ORDER BY rir, afi, prefix, first_day").fetchall()
    o = lambda d: D(2026, 10, d)  # noqa: E731
    assert got == [(4, '10.0.0.0/8', 8, 1, o(1), o(11), 'warehouse', 1, 'arin'),      # 1-day gap bridged, clipped
                   (4, '11.0.0.0/8', 8, 2, o(1), o(6), 'warehouse', 0, 'arin'),       # EE window ends the VRP
                   (4, '12.0.0.0/8', 16, 3, o(2), o(4), 'warehouse', 0, 'arin'),      # 2 objects, 1 row
                   (6, '2001:db8::/32', 48, 0, o(1), o(2), 'warehouse', 0, 'arin'),   # 6-day gap on good days
                   (6, '2001:db8::/32', 48, 0, o(9), o(11), 'warehouse', 0, 'arin'),
                   (4, '193.0.0.0/21', 24, 7, o(1), o(9), 'warehouse', 0, 'ripencc')]
    assert meta['last_complete_day'] == '2026-10-11'                                  # 13.0.0.0/8 (10-12 only) dropped
    assert meta['still_present_rows'] == 22 and meta['rows'] == 26
    assert meta['stale_rirs'] == {'ripencc': '2026-10-09T00:00:00'}
    types = [r[1] for r in con.execute(f"DESCRIBE SELECT * FROM '{out}'").fetchall()]
    assert types == ['UTINYINT', 'VARCHAR', 'UTINYINT', 'UBIGINT', 'DATE', 'DATE', 'VARCHAR', 'INTEGER', 'VARCHAR']
    # no object_signatures / certs tables: every object gets host '?', i.e. the RIR-only rule above
    assert set(meta['host_fallback_rirs']) == {'arin', 'ripencc'}
    assert set(meta['validity_fallback_rirs']) == {'arin', 'ripencc'}
    assert meta['tier2_cross_host_by_domain'] == {}
    assert meta['superset_check']['rir_only_bridging']['uncovered'] == 0


# ── per-publication-point bridging (vrp_export hosts and domains) ────────────
S0 = D(2026, 1, 1)
OUTAGE = {D(2026, 3, 20) + dt.timedelta(days=i) for i in range(6)}       # no snapshot at all: RIR-wide outage
LAST = D(2026, 4, 30)                                                     # runs end 04-30 12:00 -> last complete day 04-29


def _span(a, b):
    return {a + dt.timedelta(days=i) for i in range((b - a).days + 1)}


def _scenario():
    """One synthetic 'apnic' warehouse, 2026-01-01..04-30, objects published by these hosts:
    rpki.big.example 3,000 VRPs (keeps the RIR calendar steady); rpkica.nir.example 200 VRPs missing
    02-01..02-10 (a repository dropout the RIR calendar cannot see); rpki.small.example 10 VRPs
    withdrawn 02-15..02-24; rpki.deleg.example 100 VRPs missing 02-15..02-24 (a small CA's outage: below
    the 150-VRP gate); rpki.ramp.example 1 more VRP every 2 days (one withdrawn 02-20..02-25);
    rpki.drop.example 50 VRPs, 45 withdrawn for good on 03-01 (one returns 03-10); one VRP moving
    old.example-a.net -> new.example-b.org with a 1-day gap (03-02); 160 VRPs moving
    rpki-ca.nic.example -> repo.nic.example (same domain) and 25 moving a.one.example -> b.two.example,
    both absent 04-05..04-14. Validity guard: rpki.roll.example 200 VRPs missing 01-15..01-24, re-issued
    during the dropout (old EE certificates revoked 01-18 10:00, new ones valid from 01-22; the host is
    not seen at all); rpki.lapse.example 300 VRPs of which 200 lapse (EE not_after 03-04 06:55, still
    published, re-issued with not_before 03-16 00:30) while 100 stay and 20 roll over (old ROAs last seen
    03-05, valid until 2027, one serial also revoked by another CA; new ones valid from 03-08, seen from
    03-11); rpki.revoke.example 200
    VRPs of which 150 are revoked 04-17 08:00 and withdrawn, re-issued with not_before 04-24 00:30.
    No snapshot on 03-20..03-25 (every object absent). Returns objects, prefixes, runs, snaps, sia, revoked,
    foreign."""
    ee = ('2026-01-01 00:00:00+00', '2027-01-01 00:00:00+00')
    everything = _span(S0, LAST) - OUTAGE
    objects, prefixes, runs, sia, revoked = [], [], [], {}, {}

    def add(oid, prefix, asn, uri, present, valid=ee):
        objects.append((oid, asn, *valid))
        prefixes.append((oid, 4, prefix, int(prefix.split('/')[1])))
        sia[oid] = uri + ',https://rrdp.example.net/notification.xml'    # several URIs, rsync first
        days = sorted(present - OUTAGE)
        start = prev = None
        for d in days + [None]:
            if start is not None and (d is None or d != prev + dt.timedelta(days=1)):
                runs.append((oid, f'{start} 00:00:00', f'{prev} 12:00:00'))
                start = None
            if d is not None and start is None:
                start = d
            prev = d

    for i in range(3000):
        add(10000 + i, f'10.{i // 250}.{i % 250}.0/24', 64500, f'rsync://rpki.big.example/repo/B/{i}.roa', everything)
    for i in range(200):
        add(2000 + i, f'11.0.{i}.0/24', 64501, f'rsync://rpkica.nir.example/rpki/A/{i}.roa',
            everything - _span(D(2026, 2, 1), D(2026, 2, 10)))
    for i in range(10):
        add(3000 + i, f'12.0.{i}.0/24', 64502, f'rsync://rpki.small.example/r/{i}.roa',
            everything - _span(D(2026, 2, 15), D(2026, 2, 24)))
    for i in range(100):
        add(9000 + i, f'18.0.{i}.0/24', 64508, f'rsync://rpki.deleg.example/r/{i}.roa',
            everything - _span(D(2026, 2, 15), D(2026, 2, 24)))
    for j in range(60):
        days = _span(S0 + dt.timedelta(days=2 * j), LAST)
        if j == 5:
            days -= _span(D(2026, 2, 20), D(2026, 2, 25))
        add(4000 + j, f'13.0.{j}.0/24', 64503, f'rsync://rpki.ramp.example/r/{j}.roa', days)
    for k in range(50):
        days = _span(S0, LAST) if k < 5 else _span(S0, D(2026, 2, 28))
        if k == 5:
            days |= _span(D(2026, 3, 10), LAST)
        add(5000 + k, f'14.0.{k}.0/24', 64504, f'rsync://rpki.drop.example/r/{k}.roa', days)
    add(6000, '15.0.0.0/24', 64505, 'rsync://old.example-a.net/r/m.roa', _span(S0, D(2026, 3, 1)))
    add(6001, '15.0.0.0/24', 64505, 'rsync://new.example-b.org/r/m.roa', _span(D(2026, 3, 3), LAST))
    for i in range(160):
        add(7000 + i, f'16.0.{i}.0/24', 64506, f'rsync://rpki-ca.nic.example/r/{i}.roa', _span(S0, D(2026, 4, 4)))
        add(7200 + i, f'16.0.{i}.0/24', 64506, f'rsync://repo.nic.example/r/{i}.roa', _span(D(2026, 4, 15), LAST))
    for i in range(25):
        add(8000 + i, f'17.0.{i}.0/24', 64507, f'rsync://a.one.example/r/{i}.roa', _span(S0, D(2026, 4, 4)))
        add(8100 + i, f'17.0.{i}.0/24', 64507, f'rsync://b.two.example/r/{i}.roa', _span(D(2026, 4, 15), LAST))
    for i in range(200):     # re-issued during a dropout of the host: the replacement is never seen
        add(22000 + i, f'22.0.{i}.0/24', 64511, f'rsync://rpki.roll.example/r/{i}.roa', _span(S0, D(2026, 1, 14)))
        revoked[22000 + i] = '2026-01-18 10:00:00+00'
        add(22500 + i, f'22.0.{i}.0/24', 64511, f'rsync://rpki.roll.example/r/{i}.roa', _span(D(2026, 1, 25), LAST),
            ('2026-01-22 00:00:00+00', '2027-01-22 00:00:00+00'))
    for i in range(300):     # 200 EE certificates expire 03-04 06:55 and are renewed only on 03-16
        p, uri = f'19.{i // 250}.{i % 250}.0/24', f'rsync://rpki.lapse.example/r/{i}.roa'
        if i < 200:
            add(20000 + i, p, 64509, uri, everything, (ee[0], '2026-03-04 06:55:00+00'))
            add(20500 + i, p, 64509, uri, _span(D(2026, 3, 16), LAST), ('2026-03-16 00:30:00+00', ee[1]))
        else:
            add(20000 + i, p, 64509, uri, everything)
    for i in range(300, 320):  # rollover the archive missed for 5 days: the old ROA is valid throughout
        p, uri = f'19.{i // 250}.{i % 250}.0/24', f'rsync://rpki.lapse.example/r/{i}.roa'
        add(20000 + i, p, 64509, uri, _span(S0, D(2026, 3, 5)))
        add(20100 + i, p, 64509, uri, _span(D(2026, 3, 11), LAST), ('2026-03-08 00:00:00+00', ee[1]))
    for i in range(200):     # 150 ROAs revoked 04-17 08:00 and withdrawn, re-issued on 04-24
        p, uri = f'21.0.{i}.0/24', f'rsync://rpki.revoke.example/r/{i}.roa'
        if i < 150:
            add(21000 + i, p, 64510, uri, _span(S0, D(2026, 4, 17)))
            revoked[21000 + i] = '2026-04-17 08:00:00+00'
            add(21500 + i, p, 64510, uri, _span(D(2026, 4, 24), LAST), ('2026-04-24 00:30:00+00', ee[1]))
        else:
            add(21000 + i, p, 64510, uri, everything)
    snaps = [f'{d} {h}:00:00' for d in sorted(_span(S0, LAST) - OUTAGE) for h in ('00', '12')]
    return objects, prefixes, runs, snaps, sia, revoked, [20300]


def _run_scenario(base, root, threads=2):
    if not os.path.isdir(os.path.join(base, 'warehouse-apnic')):
        _fake_warehouse(base, 'apnic', *_scenario())
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(vrp_export, 'in_blackout', lambda now=None: False)
        con, raw_rows, max_end, t_read, hosts = vrp_export.export(root, base, threads=threads, memory_limit='2GB',
                                                                  rirs=('apnic',))
        meta = vrp_export.bridge_and_write(con, root, raw_rows, max_end, t_read, rirs=('apnic',), hosts=hosts)
    return con, meta


@pytest.fixture(scope='module')
def scen(tmp_path_factory):
    tmp = tmp_path_factory.mktemp('pp')
    base, root = str(tmp / 'wh'), str(tmp / 'root')
    con, meta = _run_scenario(base, root)
    out = os.path.join(root, 'inputs', 'warehouse_intervals.parquet')

    def rows(prefix):
        return con.execute(f"SELECT first_day, last_day, bridged_days FROM '{out}' WHERE prefix = ? ORDER BY first_day",
                           [prefix]).fetchall()
    return {'con': con, 'meta': meta, 'root': root, 'base': base, 'out': out, 'rows': rows, 'tmp': tmp}


def _cal(scen, name, sub=None):
    f = os.path.join(scen['root'], 'inputs', f'warehouse_{name}_calendar.parquet' if sub else 'warehouse_calendar.parquet')
    w = f" AND {name} = '{sub}'" if sub else ''
    return scen['con'].execute(f"SELECT d FROM '{f}' WHERE bad AND has_snap{w} ORDER BY d").fetchall()


def test_host_dropout_is_bridged_although_the_rir_calendar_is_fine(scen):
    """A 10-day dropout of one repository (200 of ~4,000 VRPs) is invisible to the RIR calendar but bad
    for its host, so its VRPs are carried across it (plus the RIR-wide outage)."""
    assert _cal(scen, 'warehouse') == []                                    # RIR: no bad day with a snapshot
    assert [d for (d,) in _cal(scen, 'host', 'rpkica.nir.example')] == sorted(_span(D(2026, 2, 1), D(2026, 2, 10)))
    for i in (0, 117, 199):
        assert scen['rows'](f'11.0.{i}.0/24') == [(S0, D(2026, 4, 29), 10 + 6)]
    t2 = scen['meta']['tier2_by_host']['apnic']['rpkica.nir.example']
    assert t2['tier2_host_only'] == 200 and t2['tier2_gap_days'] == 200 * (10 + 6)
    assert t2['refused_validity_hole'] == 0


def test_rir_wide_outage_is_still_bridged(scen):
    """No snapshot on 03-20..03-25: every VRP is bridged by the RIR rule, as before."""
    assert scen['rows']('10.0.0.0/24') == [(S0, D(2026, 4, 29), 6)]
    cal = os.path.join(scen['root'], 'inputs', 'warehouse_calendar.parquet')
    got = scen['con'].execute(f"SELECT d FROM '{cal}' WHERE bad AND d >= DATE '{S0}' ORDER BY d").fetchall()
    assert got == [(d,) for d in sorted(OUTAGE)]                            # (the calendar starts in 2012)
    assert scen['meta']['gaps_by_rir']['apnic']['tier2'] >= 3000


def test_small_host_withdrawal_is_not_bridged(scen):
    """A host with fewer than 20 VRPs is never flagged: its 10-day withdrawal stays a gap."""
    assert _cal(scen, 'host', 'rpki.small.example') == []
    assert scen['rows']('12.0.3.0/24') == [(S0, D(2026, 2, 14), 0), (D(2026, 2, 25), D(2026, 4, 29), 6)]


def test_small_delegated_ca_outage_is_not_bridged(scen):
    """A host of 100 VRPs (20 <= ref < 150) that disappears for 10 days has no bad day: small delegated
    CAs that vanish from the archive were mostly really down (independent validators lacked the same
    ROAs), so their gaps stay. The 20-VRP rule of VERSION 2 bridged them."""
    assert _cal(scen, 'host', 'rpki.deleg.example') == []
    hc = os.path.join(scen['root'], 'inputs', 'warehouse_host_calendar.parquet')
    n, ref = scen['con'].execute(f"SELECT n, ref FROM '{hc}' WHERE host = 'rpki.deleg.example' AND d = DATE '2026-02-20'").fetchone()
    assert (n, ref) == (0, 100)
    assert scen['rows']('18.0.42.0/24') == [(S0, D(2026, 2, 14), 0), (D(2026, 2, 25), D(2026, 4, 29), 6)]
    t2 = scen['meta']['tier2_by_host']['apnic']['rpki.deleg.example']     # listed for the RIR-wide outage only
    assert (t2['tier2'], t2['tier2_host_only'], t2['refused_validity_hole']) == (100, 0, 0)


def test_ramp_up_and_permanent_drop_are_not_flagged(scen):
    """A growing host and a host that loses 90% of its VRPs for good have no bad day, so a VRP
    withdrawn during the ramp, or one that returns after the drop, keeps its gap."""
    assert _cal(scen, 'host', 'rpki.ramp.example') == []
    assert _cal(scen, 'host', 'rpki.drop.example') == []
    assert scen['rows']('13.0.5.0/24') == [(D(2026, 1, 11), D(2026, 2, 19), 0), (D(2026, 2, 26), D(2026, 4, 29), 6)]
    assert scen['rows']('14.0.5.0/24') == [(S0, D(2026, 2, 28), 0), (D(2026, 3, 10), D(2026, 4, 29), 6)]
    assert scen['rows']('14.0.9.0/24') == [(S0, D(2026, 2, 28), 0)]
    hc = os.path.join(scen['root'], 'inputs', 'warehouse_host_calendar.parquet')
    ref, n = scen['con'].execute(f"SELECT ref, n FROM '{hc}' WHERE host = 'rpki.drop.example' AND d = DATE '2026-03-05'").fetchone()
    assert (n, ref) == (5, 6)                       # least(50 before, 6 after): the drop is not a dip


def test_cross_host_migration_gaps(scen):
    """A VRP that moves to another host with a 1-day gap stays continuous (RIR rule, as before).
    A 10-day gap between two hosts of the same domain, on days the whole domain is missing, is
    bridged by the domain rule; the same gap between unrelated domains is not."""
    assert scen['rows']('15.0.0.0/24') == [(S0, D(2026, 4, 29), 1 + 6)]
    assert scen['rows']('16.0.7.0/24') == [(S0, D(2026, 4, 29), 6 + 10)]
    assert scen['rows']('17.0.7.0/24') == [(S0, D(2026, 4, 4), 6), (D(2026, 4, 15), D(2026, 4, 29), 0)]
    m = scen['meta']
    assert m['domains_with_several_hosts']['apnic']['nic.example'] == 2
    assert m['tier2_cross_host_by_domain'] == {'apnic': {'nic.example': {
        'gaps': 160, 'tier2': 160, 'tier2_domain_only': 160, 'tier2_gap_days': 1600, 'refused_validity_hole': 0,
        'refused_gap_days': 0}}}
    assert [d for (d,) in _cal(scen, 'domain', 'nic.example')] == sorted(_span(D(2026, 4, 5), D(2026, 4, 14)))
    # each host alone sees a permanent drop and a ramp: never bad
    assert _cal(scen, 'host', 'rpki-ca.nic.example') == [] and _cal(scen, 'host', 'repo.nic.example') == []


def test_roa_expiry_lapse_is_not_bridged(scen):
    """200 of 300 VRPs of a host lapse: their EE certificates expire on 03-04 06:55 and the renewed ROAs
    are valid only from 03-16 00:30, while the archive still sees the host (100 VRPs). The host's
    calendar is bad on 03-05..03-15, but no object was valid on those whole days, so the gap stays
    (CNNIC 2021-04-30..05-26 and 2022-05-28..06-06). The 100 others are unaffected."""
    assert [d for (d,) in _cal(scen, 'host', 'rpki.lapse.example')] == sorted(_span(D(2026, 3, 5), D(2026, 3, 15)))
    hc = os.path.join(scen['root'], 'inputs', 'warehouse_host_calendar.parquet')
    assert scen['con'].execute(f"SELECT n, ref FROM '{hc}' WHERE host = 'rpki.lapse.example' AND d = DATE '2026-03-08'"
                               ).fetchone() == (100, 320)
    assert scen['rows']('19.0.7.0/24') == [(S0, D(2026, 3, 4), 0), (D(2026, 3, 16), D(2026, 4, 29), 6)]
    assert scen['rows']('19.0.207.0/24') == [(S0, D(2026, 4, 29), 6)]
    # a rollover inside the bad days has no hole (the old ROA was valid), also for the serial that another
    # CA's CRL lists
    assert scen['rows']('19.1.50.0/24') == scen['rows']('19.1.69.0/24') == [(S0, D(2026, 4, 29), 5 + 6)]
    t2 = scen['meta']['tier2_by_host']['apnic']['rpki.lapse.example']
    # tier 2: the RIR-wide outage of all 320 and the 20 rollovers; the 200 lapse gaps are refused
    assert (t2['tier2'], t2['tier2_host_only'], t2['refused_validity_hole'], t2['refused_gap_days']) == (340, 20, 200, 200 * 11)


def test_crl_revocation_lapse_is_not_bridged(scen):
    """150 ROAs are revoked on 04-17 08:00 (EE still valid until 2027) and re-issued valid from 04-24
    00:30; the CRL entries are matched by issuer DN and serial (revoked_cert_id is NULL), and an entry
    with the same serial from another CA is ignored."""
    assert scen['rows']('21.0.3.0/24') == [(S0, D(2026, 4, 17), 6), (D(2026, 4, 24), D(2026, 4, 29), 0)]
    assert scen['rows']('21.0.170.0/24') == [(S0, D(2026, 4, 29), 6)]
    t2 = scen['meta']['tier2_by_host']['apnic']['rpki.revoke.example']
    assert (t2['refused_validity_hole'], t2['refused_gap_days']) == (150, 150 * 6)
    assert scen['meta']['hosts_by_rir']['apnic']['revoked_objects'] == 350
    assert scen['meta']['validity_fallback_rirs'] == {}


def test_reissue_during_a_dropout_is_still_bridged(scen):
    """The host is missing on 01-15..01-24 and its ROAs are re-issued meanwhile (old EE revoked 01-18,
    new ones valid from 01-22): there is a validity hole (01-19..01-21) only because the archive never
    saw the host on those days, so it does not count and the dropout is bridged (freerangecloud
    2023-05-27..06-02, which RPKIviews confirms)."""
    assert scen['rows']('22.0.9.0/24') == [(S0, D(2026, 4, 29), 10 + 6)]
    t2 = scen['meta']['tier2_by_host']['apnic']['rpki.roll.example']
    assert (t2['tier2_host_only'], t2['refused_validity_hole']) == (200, 0)


def test_bridging_invariants_and_superset(scen):
    """Output rows of one (rir, VRP) never overlap or touch, bridged_days counts the days on which no
    host had the VRP, and every (VRP, day) of the RIR-only bridging is in the output."""
    con, out, meta = scen['con'], scen['out'], scen['meta']
    con.execute(f"CREATE OR REPLACE VIEW o AS SELECT * FROM '{out}'")
    assert intervals.check_invariants(con, 'o', group_col='rir') == {'overlapping': 0, 'inverted': 0, 'negative_bridged': 0}
    assert meta['superset_check']['raw']['uncovered'] == 0
    assert meta['superset_check']['rir_only_bridging']['uncovered'] == 0
    # host rule (nir, roll, the lapse host's rollovers) + domain rule (nic) only; lapse, revoke and deleg stay open
    assert meta['vrp_days_by_rir']['apnic']['added'] == 200 * 10 + 200 * 10 + 20 * 5 + 160 * 10
    assert meta['rir_tier2_validity_holes'] == {'apnic': {'gaps': 0, 'gap_days': 0, 'hole_days': 0}}
    assert meta['bridging']['host_min_ref'] == 150 and meta['bridging']['host_part_min_ref'] == 20
    assert meta['host_fallback_rirs'] == {} and meta['hosts_by_rir']['apnic']['multi_host_objects'] == 0
    assert meta['hosts_by_rir']['apnic']['raw_rows_unknown_host'] == 0
    types = {r[0]: r[1] for r in con.execute(
        f"DESCRIBE SELECT * FROM '{scen['root']}/inputs/warehouse_host_calendar.parquet'").fetchall()}
    assert types == {'rir': 'VARCHAR', 'host': 'VARCHAR', 'd': 'DATE', 'n': 'BIGINT', 'ref': 'BIGINT',
                     'has_snap': 'BOOLEAN', 'bad': 'BOOLEAN'}


def test_per_host_bridging_is_deterministic(scen):
    """A second run (more threads) on the same warehouse writes byte-identical files."""
    import hashlib
    root2 = str(scen['tmp'] / 'root2')
    _run_scenario(scen['base'], root2, threads=4)
    for f in ('warehouse_intervals.parquet', 'warehouse_calendar.parquet', 'warehouse_host_calendar.parquet',
              'warehouse_domain_calendar.parquet'):
        h = [hashlib.sha256(open(os.path.join(r, 'inputs', f), 'rb').read()).hexdigest() for r in (scen['root'], root2)]
        assert h[0] == h[1], f


def test_host_calendar_gate_counts_the_whole_publication_point(tmp_path):
    """The 150-VRP gate looks at every RIR's part of a host: rpki.x.example has 200 VRPs under one RIR and
    60 under another and both vanish on days 11-20, so both parts are bad (one repository serves every
    trust anchor). rpki.y.example (100 + 40) never reaches the gate, and a part below 20 VRPs
    (rpki.z.example's 10 under arin) is never bad."""
    con = _con(tmp_path)
    d0 = D(2026, 1, 1)
    con.execute("CREATE TABLE r (grp VARCHAR, host VARCHAR, prefix VARCHAR, first_day DATE, last_day DATE)")
    rows = []
    for host, parts in (('rpki.x.example', {'ripencc': 200, 'arin': 60}), ('rpki.y.example', {'ripencc': 100, 'arin': 40}),
                        ('rpki.z.example', {'ripencc': 200, 'arin': 10})):
        for grp, n in parts.items():
            for i in range(n):
                p = f'{host}/{grp}/{i}'
                rows += [(grp, host, p, d0, d0 + dt.timedelta(days=9)), (grp, host, p, d0 + dt.timedelta(days=20), d0 + dt.timedelta(days=59))]
    con.executemany("INSERT INTO r VALUES (?,?,?,?,?)", rows)
    con.execute("CREATE TABLE snapdays AS SELECT g AS grp, CAST(d AS DATE) AS d FROM (VALUES ('arin'), ('ripencc')) v(g), "
                "range(DATE '2026-01-01', DATE '2026-03-02', INTERVAL 1 DAY) t(d)")
    con.execute("CREATE TABLE hc AS " + intervals.host_calendar_sql('r', 'snapdays'))
    got = con.execute("SELECT grp, sub, count(*), min(d), max(d) FROM hc WHERE bad GROUP BY ALL ORDER BY 1, 2").fetchall()
    gap = (10, d0 + dt.timedelta(days=10), d0 + dt.timedelta(days=19))
    assert got == [('arin', 'rpki.x.example', *gap), ('ripencc', 'rpki.x.example', *gap), ('ripencc', 'rpki.z.example', *gap)]
    # with the gate at the part floor (the VERSION 2 rule, ref >= 20) every part of 20 or more is bad
    con.execute("CREATE TABLE hc20 AS " + intervals.host_calendar_sql('r', 'snapdays', min_ref=20, part_min_ref=20))
    assert con.execute("SELECT count(DISTINCT grp || sub) FROM hc20 WHERE bad").fetchone()[0] == 5
    assert con.execute("SELECT max(cum_present) FROM hc WHERE sub = 'rpki.x.example' AND grp = 'arin'").fetchone()[0] == 50


def test_validity_hole_counts_only_whole_days_without_a_valid_object(tmp_path):
    """The validity hole of a gap is the whole UTC days strictly between date(prev_until) and
    date(valid_from), clipped to the gap: a day on which an object stops or starts being valid at mid-day
    is not a hole day, even when the object stays valid after its presence run ended."""
    con = _con(tmp_path)

    def hole(prev_until, valid_from, prev_last="DATE '2026-01-05'", first_day="DATE '2026-01-12'"):
        pu, vf = f"TIMESTAMP '{prev_until}'", f"TIMESTAMP '{valid_from}'"
        a, b = intervals.hole_range_sql(prev_last, first_day, pu, vf)
        return con.execute(f"SELECT {a}, {b}, {intervals.hole_days_sql(prev_last, first_day, pu, vf)}").fetchone()
    # last seen day 5, revoked at mid-day on day 7; the next object is valid from mid-day on day 10
    assert hole('2026-01-07 12:00:00', '2026-01-10 12:00:00') == (D(2026, 1, 8), D(2026, 1, 9), 2)
    # valid until mid-day 9, the next from mid-day 10: no whole day without a valid object
    assert hole('2026-01-09 12:00:00', '2026-01-10 12:00:00')[2] == 0
    # the hole never leaves the gap (days 6..11)
    assert hole('2026-01-01 00:00:00', '2026-01-20 00:00:00') == (D(2026, 1, 6), D(2026, 1, 11), 6)


def test_validity_guard_counts_only_days_the_host_was_seen(tmp_path):
    """bridge_sql(guard=True) on a hand-made calendar: a gap whose validity hole (no object valid on whole
    days) falls on days the host was seen stays open; the same hole on days the host was not seen, a gap
    without a hole, and a hole on days that are bad for the RIR (RIR rule, never guarded) are bridged.
    Also through merge_sql(valid=True) and cross_col (domain level)."""
    con = _con(tmp_path)
    d = lambda k: D(2026, 1, k)  # noqa: E731
    ts = lambda k, h=0: dt.datetime(2026, 1, k, h)  # noqa: E731
    con.execute("CREATE TABLE raw (rir VARCHAR, host VARCHAR, prefix VARCHAR, max_length INT, asn BIGINT, afi INT, "
                "first_day DATE, last_day DATE, valid_from TIMESTAMP, valid_until TIMESTAMP)")
    con.executemany("INSERT INTO raw VALUES (?,?,?,8,1,4,?,?,?,?)", [
        ('arin', 'h1', 'a', d(1), d(5), ts(1), ts(5, 7)), ('arin', 'h1', 'a', d(12), d(20), ts(11, 20), ts(25)),  # hole 6-10, seen
        ('arin', 'h2', 'b', d(1), d(5), ts(1), ts(5, 7)), ('arin', 'h2', 'b', d(12), d(20), ts(11, 20), ts(25)),  # hole 6-10, unseen
        ('arin', 'h1', 'c', d(1), d(5), ts(1), ts(9)), ('arin', 'h1', 'c', d(12), d(20), ts(8), ts(25)),          # rollover: no hole
        ('arin', 'h1', 'd', d(1), d(5), ts(1), None), ('arin', 'h1', 'd', d(12), d(20), ts(11, 20), ts(25)),        # unknown: no hole
        ('arin', 'h2', 'e', d(1), d(5), None, ts(30)), ('arin', 'h2', 'e', d(12), d(20), ts(8), ts(25)),            # NULL kept
        ('ripencc', 'h1', 'a', d(1), d(5), ts(1), ts(5, 7)), ('ripencc', 'h1', 'a', d(12), d(20), ts(11, 20), ts(25))])  # RIR bad
    # host calendars: every gap day bad; h1 seen (n > 0) on days 6-11, h2 never
    con.execute("""CREATE TABLE hcal AS SELECT grp, sub, d, sum(1) OVER w AS cum_good_all,
                          sum(CASE WHEN d BETWEEN DATE '2026-01-06' AND DATE '2026-01-11' THEN 0 ELSE 1 END) OVER w AS cum_good,
                          sum(CASE WHEN sub = 'h1' THEN 1 WHEN d BETWEEN DATE '2026-01-06' AND DATE '2026-01-11' THEN 0 ELSE 1 END) OVER w
                            AS cum_present
                   FROM (VALUES ('arin'), ('ripencc')) g(grp), (VALUES ('h1'), ('h2'), ('dom')) s(sub),
                        (SELECT CAST(unnest(range(DATE '2026-01-01', DATE '2026-01-21', INTERVAL 1 DAY)) AS DATE) AS d)
                   WINDOW w AS (PARTITION BY grp, sub ORDER BY d)""")
    con.execute("""CREATE TABLE cal AS SELECT grp, d, sum(CASE WHEN grp = 'ripencc' AND d BETWEEN DATE '2026-01-06'
                          AND DATE '2026-01-11' THEN 0 ELSE 1 END) OVER (PARTITION BY grp ORDER BY d) AS cum_good
                   FROM (VALUES ('arin'), ('ripencc')) g(grp), (SELECT CAST(unnest(range(DATE '2026-01-01', DATE '2026-01-21',
                        INTERVAL 1 DAY)) AS DATE) AS d)""")
    got = con.execute(intervals.bridge_sql('raw', 'cal', group_col='rir', sub_col='host', sub_cal='hcal', guard=True)
                      + " ORDER BY rir, prefix, first_day").fetchall()
    # columns: rir, host, prefix, max_length, asn, afi, first_day, last_day, bridged_days, valid_from, valid_until
    assert [(g[0], g[2], g[6], g[7], g[8]) for g in got] == [
        ('arin', 'a', d(1), d(5), 0), ('arin', 'a', d(12), d(20), 0),      # seen hole: refused
        ('arin', 'b', d(1), d(20), 6),                                     # unseen hole: bridged
        ('arin', 'c', d(1), d(20), 6),                                     # no hole: bridged
        ('arin', 'd', d(1), d(20), 6),                                     # unknown validity: bridged
        ('arin', 'e', d(1), d(20), 6),
        ('ripencc', 'a', d(1), d(20), 6)]                                  # RIR rule: bridged anyway
    assert [(g[9], g[10]) for g in got if g[2] == 'b'] == [(ts(1), ts(25))]
    assert [(g[9], g[10]) for g in got if g[2] in ('d', 'e')] == [(ts(1), ts(25)), (None, ts(25))]   # first/last row's
    st = con.execute(intervals.sub_gap_stats_sql('raw', 'cal', 'hcal', group_col='rir', sub_col='host', guard=True)).fetchall()
    assert st == [('arin', 'h1', 3, 0, 2, 2, 12, 1, 6), ('arin', 'h2', 2, 0, 2, 2, 12, 0, 0), ('ripencc', 'h1', 1, 0, 1, 0, 6, 0, 0)]
    assert con.execute("SELECT " + intervals.hole_days_sql("DATE '2026-01-05'", "DATE '2026-01-12'", 'NULL::TIMESTAMP',
                                                            "TIMESTAMP '2026-01-11'")).fetchone() == (0,)
    # domain level: the same rows of two hosts of one domain, merged with their validity; a seen hole is
    # refused, the cross-host gap without a hole is bridged
    con.execute("""CREATE TABLE dom AS SELECT rir, 'dom' AS domain, * EXCLUDE (rir) FROM raw WHERE rir = 'arin' AND prefix IN ('a', 'c')""")
    con.execute("UPDATE dom SET host = 'h9' WHERE first_day = DATE '2026-01-12'")
    con.execute("CREATE TABLE m AS SELECT 'arin' AS rir, * FROM (" + intervals.merge_sql(
        "(SELECT * EXCLUDE (rir) FROM dom)", ('domain',) + intervals.VRP_KEY, carry='host', valid=True) + ")")
    con.execute("UPDATE hcal SET cum_present = cum_good_all WHERE sub = 'dom'")    # the domain is seen every day
    got = con.execute(intervals.bridge_sql('m', 'cal', group_col='rir', sub_col='domain', sub_cal='hcal', cross_col='host',
                                           guard=True) + " ORDER BY prefix, first_day").fetchall()
    assert [(g[2], g[6], g[7], g[8]) for g in got] == [('a', d(1), d(5), 0), ('a', d(12), d(20), 0), ('c', d(1), d(20), 6)]


def test_registrable_domain():
    rd = vrp_export.registrable_domain
    assert rd('rpki-ca.idnic.net') == rd('repo-rpki.idnic.net') == 'idnic.net'
    assert rd('rpki-repository.nic.ad.jp') == 'nic.ad.jp'
    assert rd('rpkica.twnic.tw') == 'twnic.tw'
    assert rd('krill.sy5.nsw.au.infininet.com.au') == 'infininet.com.au'
    assert rd('rpki.cernet.edu.cn') == 'cernet.edu.cn' and rd('rpki.cernet.net') == 'cernet.net'
    assert rd('192.0.2.1') == '192.0.2.1' and rd('?') == '?' and rd('localhost') == 'localhost'


def test_merge_sql_records_hosts_and_is_deterministic_with_ties(tmp_path):
    """merge_sql merges touching rows of different hosts and names the host that starts and ends
    each island; rows tying on (first_day, last_day) on several hosts give the same result every time."""
    con = _con(tmp_path)
    con.execute("SET threads=8")
    con.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 'b', DATE '2020-01-01', DATE '2020-01-05'), (1, 'a', DATE '2020-01-06', DATE '2020-01-09'),
        (1, 'c', DATE '2020-01-03', DATE '2020-01-09'), (1, 'a', DATE '2020-01-20', DATE '2020-01-21'))
        t(k, host, first_day, last_day)""")
    got = con.execute(intervals.merge_sql('t', ['k'], 'host', extra=()) + " ORDER BY first_day").fetchall()
    assert got == [(1, D(2020, 1, 1), D(2020, 1, 9), 'b', 'a'), (1, D(2020, 1, 20), D(2020, 1, 21), 'a', 'a')]
    n = 100_000
    con.execute(f"""CREATE TABLE big AS SELECT k, h AS host, DATE '2020-01-01' + o1 AS first_day, DATE '2020-01-01' + o2 AS last_day
                    FROM range({n}) r(k), (VALUES ('x', 0, 1), ('y', 0, 1), ('x', 5, 5), ('y', 5, 6), ('z', 9, 9)) v(h, o1, o2)
                    ORDER BY random()""")
    want = [(D(2020, 1, 1), D(2020, 1, 2), 'x', 'x', n), (D(2020, 1, 6), D(2020, 1, 7), 'x', 'y', n),
            (D(2020, 1, 10), D(2020, 1, 10), 'z', 'z', n)]
    for _ in range(3):
        got = con.execute(f"SELECT first_day, last_day, first_host, last_host, count(*) FROM "
                          f"({intervals.merge_sql('big', ['k'], 'host', extra=())}) GROUP BY ALL ORDER BY ALL").fetchall()
        assert got == want


def test_exact_bridged_days_and_uncovered(tmp_path):
    """bridged_days of a merged row = its days without any raw interval (not the sum of per-host
    bridged days); uncovered_sql finds an inner row with a day missing from the outer table."""
    con = _con(tmp_path)
    con.execute("""CREATE TABLE raw AS SELECT * FROM (VALUES
        ('r', 'p', DATE '2020-01-01', DATE '2020-01-10'), ('r', 'p', DATE '2020-01-14', DATE '2020-01-20'))
        t(grp, prefix, first_day, last_day)""")
    con.execute("CREATE TABLE br AS SELECT 'r' AS grp, 'p' AS prefix, DATE '2020-01-01' AS first_day, DATE '2020-01-20' AS last_day")
    got = con.execute(intervals.exact_bridged_sql('br', 'raw', keys=('prefix',), extra=())).fetchall()
    assert got == [('r', 'p', D(2020, 1, 1), D(2020, 1, 20), 3)]
    assert con.execute(intervals.uncovered_sql('raw', 'br', keys=('prefix',))).fetchone() == (0, 2)
    con.execute("CREATE TABLE br2 AS SELECT 'r' AS grp, 'p' AS prefix, DATE '2020-01-01' AS first_day, DATE '2020-01-18' AS last_day")
    assert con.execute(intervals.uncovered_sql('raw', 'br2', keys=('prefix',))).fetchone() == (1, 2)


def test_subtal_runs_over_snapshot_days(tmp_path):
    """Sub-TAL runs follow consecutive snapshot days; a missing snapshot day does not split a run,
    an absence in an existing snapshot does (then <= 3-day bridging applies per VRP)."""
    con = _con(tmp_path)
    wh = tmp_path / 'warehouse-apnic-iana' / 'tables'
    (wh / 'table=roa_objects').mkdir(parents=True)
    (wh / 'table=roa_prefixes').mkdir(parents=True)
    con.execute(f"""COPY (SELECT * FROM (VALUES (1, 4608::BIGINT, TIMESTAMPTZ '2015-01-01', TIMESTAMPTZ '2016-01-01'),
                                               (2, 4608::BIGINT, TIMESTAMPTZ '2015-01-01', TIMESTAMPTZ '2015-01-05'))
                         t(object_id, asn, ee_not_before, ee_not_after)
                    UNION ALL SELECT i, 64500, TIMESTAMPTZ '2015-01-01', TIMESTAMPTZ '2016-01-01' FROM range(10, 30) r(i))
                    TO '{wh}/table=roa_objects/p0.parquet' (FORMAT parquet)""")
    con.execute(f"""COPY (SELECT * FROM (VALUES (1, 4::UTINYINT, '203.0.113.0/24', 24::UTINYINT),
                                               (2, 4::UTINYINT, '198.51.100.0/24', 24::UTINYINT))
                         t(object_id, afi, prefix, max_length)
                    UNION ALL SELECT i, 4, '10.' || i || '.0.0/16', 16 FROM range(10, 30) r(i))
                    TO '{wh}/table=roa_prefixes/p0.parquet' (FORMAT parquet)""")
    snaps = ['2015-01-01', '2015-01-02', '2015-01-04', '2015-01-05', '2015-01-06', '2015-01-07', '2015-01-08',
             '2015-01-09', '2015-01-10', '2015-01-11', '2015-01-12', '2015-01-13']   # no snapshot on 01-03
    present = {1: ['2015-01-01', '2015-01-02', '2015-01-04', '2015-01-05', '2015-01-06',   # absent 07-12 (6 days)
                   '2015-01-13'],
               2: snaps}                                                               # EE ends 01-05

    stable = list(range(10, 30))   # 20 VRPs present on every snapshot keep the daily count steady

    def loader(wh_dir, ids):
        oid, day = [], []
        for o, ds in list(present.items()) + [(o, snaps) for o in stable]:
            oid += [o] * len(ds)
            day += ds
        return oid, day, snaps

    vrp_subtal.build(con, base=str(tmp_path), subtals=('iana',), loader=loader)
    got = con.execute("SELECT prefix, first_day, last_day, bridged_days FROM st_br "
                      "WHERE NOT prefix LIKE '10.%' ORDER BY prefix, first_day").fetchall()
    assert got == [('198.51.100.0/24', D(2015, 1, 1), D(2015, 1, 5), 0),
                   ('203.0.113.0/24', D(2015, 1, 1), D(2015, 1, 6), 0),
                   ('203.0.113.0/24', D(2015, 1, 13), D(2015, 1, 13), 0)]


def test_vrp_export_blackout_window():
    utc = dt.timezone.utc
    assert not vrp_export.in_blackout(dt.datetime(2026, 10, 27, 0, 54, 59, tzinfo=utc))
    assert vrp_export.in_blackout(dt.datetime(2026, 10, 27, 0, 55, tzinfo=utc))
    assert vrp_export.in_blackout(dt.datetime(2026, 10, 27, 1, 19, 59, tzinfo=utc))
    assert not vrp_export.in_blackout(dt.datetime(2026, 10, 27, 1, 20, tzinfo=utc))


def test_vrp_export_start_cutoff_and_guard(tmp_path, monkeypatch):
    """Regression: a run that started at 00:54 still read the warehouse after 00:55. Starts are
    refused from 00:45, and the export stops before reading a warehouse inside the window."""
    utc = dt.timezone.utc
    assert vrp_export.may_start(dt.datetime(2026, 10, 27, 0, 44, 59, tzinfo=utc))
    assert not vrp_export.may_start(dt.datetime(2026, 10, 27, 0, 45, tzinfo=utc))
    assert not vrp_export.may_start(dt.datetime(2026, 10, 27, 0, 54, 59, tzinfo=utc))
    assert not vrp_export.may_start(dt.datetime(2026, 10, 27, 1, 19, 59, tzinfo=utc))
    assert vrp_export.may_start(dt.datetime(2026, 10, 27, 1, 20, tzinfo=utc))
    monkeypatch.setattr(vrp_export, 'in_blackout', lambda now=None: True)
    with pytest.raises(SystemExit, match='not reading'):
        vrp_export.export(str(tmp_path / 'root'), str(tmp_path / 'no-warehouse'), threads=1, memory_limit='1GB')


def test_vrp_export_shared_lock_coexists_and_times_out(tmp_path):
    import fcntl
    lock = str(tmp_path / 'rpki_presence.lock')
    with vrp_export.shared_lock(lock, wait_s=1):
        with vrp_export.shared_lock(lock, wait_s=1):    # two readers at once are fine
            pass
    fd = os.open(lock, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)                     # the presence build holds it exclusively
    try:
        with pytest.raises(SystemExit):
            with vrp_export.shared_lock(lock, wait_s=0.2, poll_s=0.05):
                pass
    finally:
        os.close(fd)


# ── vrp_merge ────────────────────────────────────────────────────────────────
def _write_src(con, path, rows):
    con.execute("CREATE OR REPLACE TABLE t (afi UTINYINT, prefix VARCHAR, max_length UTINYINT, asn UBIGINT, "
                "first_day DATE, last_day DATE, src VARCHAR, bridged_days INTEGER, rir VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?)", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")


def test_vrp_merge_union_and_types(tmp_path, capsys):
    root = str(tmp_path)
    inp = os.path.join(root, 'inputs')
    os.makedirs(inp)
    con = _con(tmp_path)
    _write_src(con, os.path.join(inp, 'warehouse_intervals.parquet'), [
        (4, '10.0.0.0/8', 8, 1, D(2026, 1, 1), D(2026, 10, 26), 'warehouse', 2, 'arin'),
        (4, '10.0.0.0/8', 8, 1, D(2026, 5, 1), D(2026, 10, 26), 'warehouse', 0, 'ripencc'),   # same VRP, 2nd RIR
        (6, '2001:DB8::/32', 48, 2, D(2026, 1, 1), D(2026, 10, 26), 'warehouse', 0, 'apnic'),  # not canonical
        (4, '192.0.2.0/24', 16, 3, D(2026, 1, 1), D(2026, 10, 26), 'warehouse', 0, 'arin'),    # max_length < len
    ])
    _write_src(con, os.path.join(inp, 'vrp_subtal.parquet'), [
        (4, '1.0.0.0/24', 24, 4, D(2013, 1, 1), D(2018, 4, 4), 'apnic-subtal', 0, 'apnic')])
    _write_src(con, os.path.join(inp, 'archive_intervals.parquet'), [
        (4, '10.0.0.0/8', 8, 1, D(2026, 4, 8), D(2026, 10, 27), 'ripe-archive', 0, 'arin'),
        (4, '20.0.0.0/8', 8, 0, D(2026, 4, 20), D(2026, 10, 27), 'ripe-archive', 1, 'apnic')])
    assert vrp_merge.main(['--root', root, '--threads', '2', '--memory-limit', '1GB']) == 0
    out = os.path.join(inp, 'vrp_intervals.parquet')
    desc = con.execute(f"DESCRIBE SELECT * FROM '{out}'").fetchall()
    assert [(r[0], r[1]) for r in desc] == [
        ('afi', 'UTINYINT'), ('prefix', 'VARCHAR'), ('max_length', 'UTINYINT'), ('asn', 'UBIGINT'),
        ('first_day', 'DATE'), ('last_day', 'DATE'), ('src', 'VARCHAR'), ('bridged_days', 'INTEGER')]
    assert con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0] == 6        # union keeps every row but the bad one
    assert con.execute(f"SELECT count(*) FROM '{out}' WHERE prefix = '2001:db8::/32' AND afi = 6").fetchone()[0] == 1

    def day_set(d):
        return con.execute(f"SELECT DISTINCT afi, prefix, max_length, asn FROM '{out}' "
                           f"WHERE DATE '{d}' BETWEEN first_day AND last_day ORDER BY prefix").fetchall()
    assert day_set('2026-10-26') == [(4, '10.0.0.0/8', 8, 1), (4, '20.0.0.0/8', 8, 0), (6, '2001:db8::/32', 48, 2)]
    assert day_set('2026-10-27') == [(4, '10.0.0.0/8', 8, 1), (4, '20.0.0.0/8', 8, 0)]
    assert day_set('2015-06-01') == [(4, '1.0.0.0/24', 24, 4)]
    meta = json.load(open(os.path.join(inp, 'vrp_intervals.json')))
    assert meta['dropped'] == 1 and meta['prefixes_rewritten'] == 1
    assert meta['latest_day'] == '2026-10-26' and meta['vrps_latest_day'] == 3
    assert meta['sources']['warehouse']['rows'] == 3
    printed = capsys.readouterr().out
    assert 'warehouse' in printed and 'ripe-archive' in printed and 'apnic-subtal' in printed
    os.remove(os.path.join(inp, 'vrp_subtal.parquet'))
    with pytest.raises(SystemExit):
        vrp_merge.main(['--root', root])


# ── downloads and AS names ───────────────────────────────────────────────────
def test_verify_file_detects_truncation(tmp_path):
    good = tmp_path / 'a.roas.csv.xz'
    good.write_bytes(lzma.compress(b'x' * 100000))
    assert fetch.verify_file(str(good)) == 100000
    bad = tmp_path / 'b.roas.csv.xz'
    bad.write_bytes(good.read_bytes()[:-20])
    with pytest.raises(fetch.IntegrityError):
        fetch.verify_file(str(bad))
    gz = tmp_path / 'c.gz'
    gz.write_bytes(gzip.compress(b'hello')[:-4])
    with pytest.raises(fetch.IntegrityError):
        fetch.verify_file(str(gz))
    plain = tmp_path / 'combined-stat'
    plain.write_bytes(b'abc')
    assert fetch.verify_file(str(plain)) == 3
    assert fetch.compression_of('x.bz2') == 'bz2' and fetch.compression_of('delegated-lacnic-20120101') is None
    assert fetch.MAX_PARALLEL == 4 and fetch.USER_AGENT == 'rpki-netsecurelab-country/1'


def test_download_skips_existing_file(tmp_path):
    dest = tmp_path / 'x' / 'present.csv.xz'
    dest.parent.mkdir()
    dest.write_bytes(b'already here')
    assert fetch.download('http://127.0.0.1:9/never-contacted', str(dest)) == ('present', 0)
    res = fetch.download_many([{'url': 'http://127.0.0.1:9/never-contacted', 'dest': str(dest)}])
    assert fetch.summarize(res)['present'] == 1


def test_list_dir_parses_listing(monkeypatch):
    page = ('<a href="../">../</a><a href="?C=N;O=D">x</a><a href="/pub/stats/">up</a>'
            '<a href="20191007/">20191007/</a><a href="delegated-afrinic-20120101">f</a>'
            '<a href="delegated-afrinic-20120101">f</a><a href="a%20b.gz">s</a>')
    monkeypatch.setattr(fetch, 'fetch_bytes', lambda url, **kw: page.encode())
    assert fetch.list_dir('https://example/') == ['20191007/', 'delegated-afrinic-20120101', 'a b.gz']


def test_asnames_parse():
    text = ('0 -Reserved AS-, ZZ\n1 LVLT-1 - Level 3 Parent, LLC, US\n23456 AS_TRANS; reserved by RFC6793\n'
            '3333 RIPE-NCC-AS Reseaux IP Europeens Network Coordination Centre (RIPE NCC), NL\n'
            '1 DUPLICATE, US\n\n')
    rows, skipped = asnames.parse(text)
    assert rows == [(0, '-Reserved AS-', 'ZZ'), (1, 'LVLT-1 - Level 3 Parent, LLC', 'US'),
                    (23456, 'AS_TRANS; reserved by RFC6793', None),
                    (3333, 'RIPE-NCC-AS Reseaux IP Europeens Network Coordination Centre (RIPE NCC)', 'NL')]
    assert skipped == 1
