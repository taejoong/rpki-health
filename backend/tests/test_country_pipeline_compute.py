"""Compute side of the country pipeline (snapshot_pairs recount, compute_day, assemble).

Tiny synthetic worlds; every expected number below is computed by hand (see the comments).

IPv4 world (delegation snapshot 2026-01-01):
  R1 10.0.0.0-10.0.255.255   arin    US delegated   65536
  R2 10.1.0.0-10.1.0.255     ripencc DE delegated     256
  R3 10.1.1.0-10.1.1.255     ripencc FR delegated     256
  R4 10.2.0.0-10.2.2.255     apnic   JP delegated     768  (not a CIDR block)
  R5 10.3.0.0-10.3.255.255   arin    ZZ free        65536
  -- 10.4.0.0/16 has no record ('unknown')
  R6 10.5.0.0-10.5.255.255   lacnic  BR delegated   65536
  R7 10.6.0.0-10.6.255.255   lacnic  AR delegated   65536
VRPs: V1 10.0.0.0/16-24 AS100, V2 10.0.0.0/17-17 AS200 (overlaps V1), V3 10.1.0.0/23-23 AS300,
  V4 10.2.0.0/24-24 AS400, V5 10.3.128.0/17-24 AS600 (free space), V6 10.4.5.0/24-24 AS0 (no record),
  V7 10.5.0.0/17-17 AS0 (AS0 only), V8 10.0.200.0/25-25 AS100 (longer than any routable route)
Pairs (v4 collectors 9 -> visible if n_collectors >= ceil(2.25) = 3):
  P1 10.0.0.0/16 AS100 valid          P2 10.0.0.0/18 AS200 invalid_len (V2 maxLength 17)
  P3 10.0.0.0/24 AS100 valid (nc=3)   P4 10.0.2.0/24 AS100 valid  P5 10.0.2.0/24 AS999 invalid_as (MOAS)
  P6 10.0.64.0/18 AS777 nc=2 -> not visible
  P7 10.0.65.0/24 AS100 valid         P8 10.0.128.0/17 AS200 invalid_as (only V1 covers)
  P9 10.1.0.0/23 AS300 valid, spans R2 (DE) and R3 (FR); counted in DE, space split 256/256
  P10 10.2.0.0/22 AS400 notfound, spans R4 (JP, 768) and no-record space (256)
  P11 10.2.0.0/24 AS400 valid         P12 10.3.0.0/24 AS600 notfound (free -> cc ZZ, registry arin)
  P13 10.4.0.0/16 AS700 notfound      P14 10.4.5.0/24 AS700 invalid_as (AS0 VRP only)
  P15 10.9.0.0/25 (not routable)      P16 0.0.0.0/0 (default)      P17 10.5.0.0/24 AS0 invalid_as
  P18 8.0.0.0/7 AS1 nc=9: visible but shorter than the /8 floor (a leak) -> not counted
IPv6 world: R8 2001:db8::/32 ripencc NL delegated (65536 /48s).
  V9 2001:db8:1::/48-48 AS800, V10 2001:db8:2:100::/56 AS800 (1/256 of a /48),
  V11 2001:db8:3::1/128 AS800 (clamped to one /64 = 1/65536 /48), V12 2001:db8:1::/48 AS0 (same
  prefix as V9: not double counted), V13 2001:db8:4::/48 AS0
  P20 2001:db8:1::/48 AS800 valid (v6 collectors 4 -> threshold 1)  P21 2001:db8::/32 AS801 notfound
  P22 2001:db8:4::/48 AS802 invalid_as  P23 2001:db8:5::/49 (not routable)
  P24 2000::/3 AS1 nc=4: visible but shorter than the /16 floor (a leak) -> not counted
The leaks P18 and P24 would add routed space without a record; the expected values below show that
they change nothing.
"""
import collections
import datetime as dt
import ipaddress
import json
import math
import os
import random
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipelines', 'country'))
from crc import assemble, common, compute_day, snapshot_pairs  # noqa: E402

D = dt.date(2026, 1, 2)


def v4(a):
    return int(ipaddress.IPv4Address(a))


def v6hi(a):
    return int(ipaddress.IPv6Address(a)) >> 64


def _write(path, cols, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t ({', '.join(f'{n} {t}' for n, t in cols)})")
    if rows:
        con.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in cols)})", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
    con.close()


DELEG_COLS = [('snap', 'DATE'), ('afi', 'UTINYINT'), ('registry', 'VARCHAR'), ('cc', 'VARCHAR'),
              ('status', 'VARCHAR'), ('start_', 'UBIGINT'), ('end_', 'UBIGINT'), ('src', 'VARCHAR')]


def base_records():
    r = [
        (4, 'arin', 'US', 'delegated', v4('10.0.0.0'), v4('10.0.255.255')),
        (4, 'ripencc', 'DE', 'delegated', v4('10.1.0.0'), v4('10.1.0.255')),
        (4, 'ripencc', 'FR', 'delegated', v4('10.1.1.0'), v4('10.1.1.255')),
        (4, 'apnic', 'JP', 'delegated', v4('10.2.0.0'), v4('10.2.2.255')),
        (4, 'arin', 'ZZ', 'free', v4('10.3.0.0'), v4('10.3.255.255')),
        (4, 'lacnic', 'BR', 'delegated', v4('10.5.0.0'), v4('10.5.255.255')),
        (4, 'lacnic', 'AR', 'delegated', v4('10.6.0.0'), v4('10.6.255.255')),
        (6, 'ripencc', 'NL', 'delegated', v6hi('2001:db8::'), v6hi('2001:db8::') + (1 << 32) - 1),
        (0, 'arin', 'US', 'delegated', 100, 199),
        (0, 'ripencc', 'DE', 'delegated', 300, 300),
        (0, 'arin', 'ZZ', 'free', 700, 799),
    ]
    return r


def later_records():
    """2026-02-01 snapshot: FR's block moved to BE, plus a small RIPE NCC block registered to US."""
    out = []
    for af, reg, cc, st, s, e in base_records():
        out.append((af, reg, 'BE' if cc == 'FR' else cc, st, s, e))
    out.append((4, 'ripencc', 'US', 'delegated', v4('10.7.0.0'), v4('10.7.0.255')))
    return out


def deleg_rows():
    rows = []
    for snap in ('2025-10-01', '2026-01-01'):
        rows += [(dt.date.fromisoformat(snap),) + r + ('test',) for r in base_records()]
    rows += [(dt.date(2026, 2, 1),) + r + ('test',) for r in later_records()]
    # 2016: only the US IPv4 block and the NL IPv6 block exist
    rows += [(dt.date(2016, 1, 1),) + r + ('test',) for r in base_records() if r[2] in ('US', 'NL') and r[0] != 0]
    return rows


OLD, NEW = (dt.date(2015, 1, 1), dt.date(2026, 12, 31)), (dt.date(2025, 1, 1), dt.date(2026, 12, 31))
VRPS = [  # prefix, max_length, asn, (first_day, last_day)
    ('10.0.0.0/16', 24, 100, OLD), ('10.0.0.0/17', 17, 200, NEW), ('10.1.0.0/23', 23, 300, NEW),
    ('10.2.0.0/24', 24, 400, NEW), ('10.3.128.0/17', 24, 600, NEW), ('10.4.5.0/24', 24, 0, NEW),
    ('10.5.0.0/17', 17, 0, NEW), ('10.0.200.0/25', 25, 100, NEW),
    ('2001:db8:1::/48', 48, 800, OLD), ('2001:db8:2:100::/56', 56, 800, NEW),
    ('2001:db8:3::1/128', 128, 800, NEW), ('2001:db8:1::/48', 48, 0, NEW), ('2001:db8:4::/48', 48, 0, NEW),
    ('10.8.0.0/16', 16, 1, (dt.date(2015, 1, 1), dt.date(2015, 12, 31))),   # expired: never used
]

PAIRS = [  # af, prefix, origin, n_collectors
    (4, '10.0.0.0/16', 100, 9), (4, '10.0.0.0/18', 200, 5), (4, '10.0.0.0/24', 100, 3),
    (4, '10.0.2.0/24', 100, 9), (4, '10.0.2.0/24', 999, 9), (4, '10.0.64.0/18', 777, 2),
    (4, '10.0.65.0/24', 100, 4), (4, '10.0.128.0/17', 200, 9), (4, '10.1.0.0/23', 300, 9),
    (4, '10.2.0.0/22', 400, 9), (4, '10.2.0.0/24', 400, 9), (4, '10.3.0.0/24', 600, 9),
    (4, '10.4.0.0/16', 700, 9), (4, '10.4.5.0/24', 700, 9), (4, '10.9.0.0/25', 1, 9),
    (4, '0.0.0.0/0', 1, 9), (4, '10.5.0.0/24', 0, 9), (4, '8.0.0.0/7', 1, 9),
    (6, '2001:db8:1::/48', 800, 1), (6, '2001:db8::/32', 801, 4), (6, '2001:db8:4::/48', 802, 2),
    (6, '2001:db8:5::/49', 1, 4), (6, '2000::/3', 1, 4),
]


def write_pairs(root, d, pairs=PAIRS, coll=(9, 4), src='v2'):
    rows = [(af, p, o, nc * 2, nc, None, src) for af, p, o, nc in pairs]
    _write(common.pairs_path(d, root),
           [('af', 'UTINYINT'), ('prefix', 'VARCHAR'), ('origin_as', 'UBIGINT'), ('n_peers', 'UINTEGER'),
            ('n_collectors', 'USMALLINT'), ('n_ff', 'UINTEGER'), ('src', 'VARCHAR')], rows)
    common.atomic_write_json(common.pairs_meta_path(d, root),
                             {'date': d.isoformat(), 'src': src, 'collectors': {'4': coll[0], '6': coll[1]},
                              'rows': len(rows), 'rows_by_af': {}, 'generated_at': 'x', 'extractor': 'test'})


def make_world(root):
    inp = common.inputs_dir(root)
    _write(os.path.join(inp, 'vrp_intervals.parquet'),
           [('afi', 'UTINYINT'), ('prefix', 'VARCHAR'), ('max_length', 'UTINYINT'), ('asn', 'UBIGINT'),
            ('first_day', 'DATE'), ('last_day', 'DATE'), ('src', 'VARCHAR'), ('bridged_days', 'INTEGER')],
           [(6 if ':' in p else 4, p, ml, asn, f, l, 'warehouse', 0) for p, ml, asn, (f, l) in VRPS])
    _write(common.deleg_path(root), DELEG_COLS, deleg_rows())
    _write(common.asnames_path(root), [('asn', 'UBIGINT'), ('name', 'VARCHAR'), ('cc', 'VARCHAR')],
           [(100, 'EXAMPLE-100 Example Net', 'US'), (200, 'TWO-HUNDRED Two Hundred Inc', 'CA'),
            (999, 'NINES', 'ZZ')])
    return root


def run(root, d):
    return compute_day.compute_date(d, root, threads=2, memory_limit='1GB', tmp=os.path.join(root, 'tmp'))


def rows(path, where='', order='', cols='*'):
    con = duckdb.connect()
    sql = f"SELECT {cols} FROM read_parquet('{path}')" + (f' WHERE {where}' if where else '') + \
        (f' ORDER BY {order}' if order else '')
    cur = con.execute(sql)
    names = [c[0] for c in cur.description]
    out = [dict(zip(names, r)) for r in cur.fetchall()]
    con.close()
    return out


def geo_map(root, d):
    out = {}
    for r in rows(os.path.join(common.daily_dir(d, root), 'geo.parquet')):
        out[(r['level'], r['key'], r['af'])] = r
    return out


M = ('routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len', 'routes_nf', 'space', 'space_valid',
     'space_invalid', 'space_nf', 'ases', 'ases_any_valid', 'ases_all_valid', 'ases_any_invalid',
     'deleg', 'deleg_cov', 'deleg_cov_noas0', 'vrps')


def exp(*vals):
    return dict(zip(M, vals))


# Expected geo rows on D, hand-computed (see the module docstring).
#   US space: 10.0.0.0/24 valid 256, 10.0.2.0/24 valid 256 (MOAS: one valid origin),
#   rest of the /18 invalid 16384-512 = 15872, 10.0.65.0/24 valid 256 (P6 invisible, so the /16 owns
#   around it), 10.0.128.0/17 invalid 32768, rest of the /16 valid 65536-16384-256-32768 = 16128.
#   valid = 256+256+256+16128 = 16896, invalid = 15872+32768 = 48640.
#   Registered US: union(V1, V2) = 65536 (overlap not double counted).
#   ZZ: 10.2.3.0/24 (tail of P10, no record) nf 256 + P12 nf 256 + 10.4/16: P14 invalid 256, nf 65280.
EXPECTED = {
    ('cc', 'US', 4): exp(7, 4, 2, 1, 0, 65536, 16896, 48640, 0, 3, 1, 1, 2, 65536, 65536, 65536, 3),
    ('cc', 'DE', 4): exp(1, 1, 0, 0, 0, 256, 256, 0, 0, 1, 1, 1, 0, 256, 256, 256, 1),
    ('cc', 'FR', 4): exp(0, 0, 0, 0, 0, 256, 256, 0, 0, 0, 0, 0, 0, 256, 256, 256, 0),
    ('cc', 'JP', 4): exp(2, 1, 0, 0, 1, 768, 256, 0, 512, 1, 1, 0, 0, 768, 256, 256, 1),
    ('cc', 'ZZ', 4): exp(3, 0, 1, 0, 2, 66048, 0, 256, 65792, 2, 0, 0, 1, 0, 0, 0, 2),
    ('cc', 'BR', 4): exp(1, 0, 1, 0, 0, 256, 0, 256, 0, 1, 0, 0, 1, 65536, 32768, 0, 1),
    ('cc', 'AR', 4): exp(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 65536, 0, 0, 0),
    ('registry', 'arin', 4): exp(8, 4, 2, 1, 1, 65792, 16896, 48640, 256, 4, 1, 1, 2, 65536, 65536, 65536, 4),
    ('registry', 'ripencc', 4): exp(1, 1, 0, 0, 0, 512, 512, 0, 0, 1, 1, 1, 0, 512, 512, 512, 1),
    ('registry', 'apnic', 4): exp(2, 1, 0, 0, 1, 768, 256, 0, 512, 1, 1, 0, 0, 768, 256, 256, 1),
    ('registry', 'lacnic', 4): exp(1, 0, 1, 0, 0, 256, 0, 256, 0, 1, 0, 0, 1, 131072, 32768, 0, 1),
    ('registry', 'unknown', 4): exp(2, 0, 1, 0, 1, 65792, 0, 256, 65536, 1, 0, 0, 1, 0, 0, 0, 1),
    # 'all' = the five RIRs: arin + ripencc + apnic + lacnic, without 'unknown' (P13, P14, V6, 10.4/16, 10.2.3/24)
    ('all', 'all', 4): exp(12, 6, 3, 1, 2, 67328, 17664, 48896, 768, 7, 3, 2, 3, 197888, 99072, 66304, 7),
    # IPv6 in /48s: registered = V9|V12 (1) + V10 (1/256) + V11 (1/65536) + V13 (1); no AS0: minus V13
    ('cc', 'NL', 6): exp(3, 1, 1, 0, 1, 65536, 1, 1, 65534, 3, 1, 1, 1, 65536,
                         2 + 1 / 256 + 1 / 65536, 1 + 1 / 256 + 1 / 65536, 5),
}
EXPECTED[('registry', 'ripencc', 6)] = EXPECTED[('cc', 'NL', 6)]
EXPECTED[('all', 'all', 6)] = EXPECTED[('cc', 'NL', 6)]

EXPECTED_AS = {  # (cc, af, asn): routes, valid, inv_as, inv_len, nf, space, s_valid, s_invalid, s_nf
    ('US', 4, 100): (4, 4, 0, 0, 0, 65536, 65536, 0, 0),       # own /24s nest in its /16
    ('US', 4, 200): (2, 0, 1, 1, 0, 49152, 0, 49152, 0),
    ('US', 4, 999): (1, 0, 1, 0, 0, 256, 0, 256, 0),
    ('DE', 4, 300): (1, 1, 0, 0, 0, 512, 512, 0, 0),           # whole /23 in its start cc
    ('JP', 4, 400): (2, 1, 0, 0, 1, 1024, 256, 0, 768),        # /24 valid inside its own /22 nf
    ('ZZ', 4, 600): (1, 0, 0, 0, 1, 256, 0, 0, 256),
    ('ZZ', 4, 700): (2, 0, 1, 0, 1, 65536, 0, 256, 65280),
    ('BR', 4, 0): (1, 0, 1, 0, 0, 256, 0, 256, 0),
    ('NL', 6, 800): (1, 1, 0, 0, 0, 1, 1, 0, 0),
    ('NL', 6, 801): (1, 0, 0, 0, 1, 65536, 0, 0, 65536),
    ('NL', 6, 802): (1, 0, 1, 0, 0, 1, 0, 1, 0),
}


@pytest.fixture
def world(tmp_path):
    root = str(tmp_path / 'root')
    make_world(root)
    write_pairs(root, D)
    return root


def test_compute_day_geo(world):
    done = run(world, D)
    assert done['inputs']['deleg_snap'] == '2026-01-01'          # latest snap <= D, not 2026-02-01
    assert done['inputs']['vrp_rows'] == 13                      # the 2015-only VRP is not in D's set
    assert done['inputs']['threshold'] == {'4': 3, '6': 1}       # ceil(0.25 * 9), ceil(0.25 * 4)
    assert done['inputs']['visible_pairs'] == 17                 # 23 rows - P6 (nc 2) - /0 - /25 - /49 - /7 - /3
    assert done['inputs']['short_pairs'] == 3                    # /0, P18 /7, P24 /3: seen, but below the floor
    assert done['inputs']['pairs_rows'] == len(PAIRS) == 23 and done['version'] == compute_day.VERSION
    got = geo_map(world, D)
    assert set(got) == set(EXPECTED), sorted(set(got) ^ set(EXPECTED))
    for k, e in EXPECTED.items():
        g = {m: got[k][m] for m in M}
        for m in M:
            assert g[m] == pytest.approx(e[m], abs=1e-12), (k, m, g[m], e[m])
    assert all(r['date'] == D for r in got.values())


def test_compute_day_types(world):
    run(world, D)
    con = duckdb.connect()
    p = os.path.join(common.daily_dir(D, world), 'geo.parquet')
    types = dict((c, t) for c, t, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall())
    assert list(types) == list(compute_day.GEO_COLUMNS)
    assert types['date'] == 'DATE' and types['af'] == 'UTINYINT' and types['key'] == 'VARCHAR'
    for m in M:
        assert types[m] == ('DOUBLE' if m in compute_day.DOUBLE_COLS else 'BIGINT'), m
    p = os.path.join(common.daily_dir(D, world), 'as.parquet')
    types = dict((c, t) for c, t, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall())
    assert list(types) == list(compute_day.AS_COLUMNS)
    assert types['asn'] == 'UBIGINT' and types['space'] == 'DOUBLE' and types['routes'] == 'BIGINT'


def test_compute_day_per_as(world):
    run(world, D)
    got = {(r['cc'], r['af'], r['asn']): (r['routes'], r['routes_valid'], r['routes_inv_as'], r['routes_inv_len'],
                                          r['routes_nf'], r['space'], r['space_valid'], r['space_invalid'],
                                          r['space_nf'])
           for r in rows(os.path.join(common.daily_dir(D, world), 'as.parquet'))}
    assert got == EXPECTED_AS


def test_pre_2020_registry_only(world):
    d = dt.date(2016, 6, 1)
    assert common.is_grid_date(d)
    done = run(world, d)
    assert done['inputs']['pairs_src'] is None and done['inputs']['deleg_snap'] == '2016-01-01'
    got = geo_map(world, d)
    assert set(got) == {('cc', 'US', 4), ('registry', 'arin', 4), ('all', 'all', 4),
                        ('cc', 'NL', 6), ('registry', 'ripencc', 6), ('all', 'all', 6)}
    for r in got.values():
        assert all(r[m] is None for m in compute_day.BGP_METRICS)
    assert (got[('cc', 'US', 4)]['deleg'], got[('cc', 'US', 4)]['deleg_cov'], got[('cc', 'US', 4)]['vrps']) == \
        (65536, 65536, 1)
    assert (got[('cc', 'NL', 6)]['deleg'], got[('cc', 'NL', 6)]['deleg_cov'],
            got[('cc', 'NL', 6)]['deleg_cov_noas0'], got[('cc', 'NL', 6)]['vrps']) == (65536, 1, 1, 1)
    assert not os.path.exists(os.path.join(common.daily_dir(d, world), 'as.parquet'))


def test_missing_pairs_then_available(world):
    d = dt.date(2026, 1, 3)
    done = run(world, d)
    assert done['inputs']['pairs_src'] is None
    assert all(r[m] is None for r in geo_map(world, d).values() for m in compute_day.BGP_METRICS)
    assert not compute_day.is_missing(d, world)
    write_pairs(world, d)                       # the backfill lands later
    assert compute_day.is_missing(d, world)
    run(world, d)
    assert geo_map(world, d)[('all', 'all', 4)]['routes'] == 12
    assert not compute_day.is_missing(d, world)


def test_recompute_replaces_directory(world):
    run(world, D)
    run(world, D)
    parent = os.path.dirname(common.daily_dir(D, world))
    assert sorted(os.listdir(parent)) == [D.isoformat()]
    assert sorted(os.listdir(common.daily_dir(D, world))) == ['as.parquet', 'done.json', 'geo.parquet']


def test_auto_dates(world):
    write_pairs(world, dt.date(2026, 1, 5))
    dates = compute_day.auto_dates(world, trailing=3)
    # trailing 3 grid dates up to the latest pairs day + every grid date without done.json
    assert dates[-3:] == [dt.date(2026, 1, 3), dt.date(2026, 1, 4), dt.date(2026, 1, 5)]
    assert dates[0] == dt.date(2012, 1, 4) and D in dates
    run(world, D)
    assert D not in compute_day.auto_dates(world, trailing=3)


# ── assemble ──────────────────────────────────────────────────────────────────
ASM_DATES = [dt.date(2016, 6, 1), dt.date(2025, 10, 22), dt.date(2026, 1, 1), dt.date(2026, 1, 2),
             dt.date(2026, 2, 1), dt.date(2026, 10, 26)]
L = dt.date(2026, 10, 26)
NO_P9 = [p for p in PAIRS if p[2] != 300]      # AS300's route appears from 2026-01-02


@pytest.fixture
def built(tmp_path):
    root = str(tmp_path / 'root')
    make_world(root)
    for d in ASM_DATES:
        if d >= common.BGP_FROM:
            write_pairs(root, d, NO_P9 if d < dt.date(2026, 1, 2) else PAIRS)
        run(root, d)
    manifest, bdir = assemble.build(root, stamp='20261027T074500Z', threads=2, memory_limit='1GB')
    return root, manifest, bdir


def test_assemble_manifest_and_series(built):
    root, m, bdir = built
    assert m['as_of'] == '2026-10-26' and m['prev_date'] == '2025-10-22'
    assert m['grid'] == {'weekly_from': '2012-01-04', 'daily_from': '2026-01-01'} and m['bgp_from'] == '2020-01-01'
    for k in ('method', 'lenses', 'sources', 'notes', 'counts', 'version', 'generated_at'):
        assert k in m
    assert set(m['lenses']) == {'routes', 'space', 'registered', 'ases'}
    assert m['lenses']['registered'] == {**m['lenses']['registered'], 'numerator': 'deleg_cov', 'denominator': 'deleg'}
    assert any(n['from'] == '2026-10-04' and n['to'] == '2026-10-12' for n in m['notes'])
    # the frontend reads the method keys in this order and renders method, notes and sources as they are
    assert list(m['method']) == ['country', 'routes', 'invalid_routes', 'routed_space', 'registered_space',
                                 'origin_ases', 'validation_and_roas', 'rir_lines', 'time_grid', 'as_table']
    assert all(isinstance(v, str) and len(v) > 80 for v in m['method'].values())
    assert 'IPv4 prefixes from /8 to /24 and IPv6 prefixes from /16 to /48' in m['method']['routes']
    for n in m['notes']:
        assert set(n) == {'from', 'to', 'text'} and n['text'].endswith('.'), n
        assert n['to'] is None or dt.date.fromisoformat(n['from']) <= dt.date.fromisoformat(n['to']), n
    assert [n['from'] for n in m['notes']] == sorted(n['from'] for n in m['notes'])
    assert 'genji' not in json.dumps(m).lower()                  # no internal host names on the page
    assert m['version'] == compute_day.VERSION == 3
    assert m['counts']['grid_dates'] == 6 and m['counts']['bgp_dates'] == 5
    with open(os.path.join(bdir, 'manifest.json')) as f:
        on_disk = json.load(f)
    assert on_disk['as_of'] == '2026-10-26'
    assert list(on_disk['method']) == list(assemble.METHOD) == list(m['method'])   # written unsorted
    s = rows(os.path.join(bdir, 'series.parquet'), cols='level, key, af, date')
    assert [(r['level'], r['key'], r['af'], r['date']) for r in s] == \
        sorted((r['level'], r['key'], r['af'], r['date']) for r in s)
    assert {r['date'] for r in s} == set(ASM_DATES)


def test_assemble_countries(built):
    root, m, bdir = built
    got = {(r['cc'], r['af']): r for r in rows(os.path.join(bdir, 'countries.parquet'))}
    # at L the 2026-02-01 snapshot applies: FR's block is BE now; ZZ is never listed
    assert set(got) == {('US', 4), ('DE', 4), ('BE', 4), ('JP', 4), ('BR', 4), ('AR', 4), ('NL', 6)}
    us = got[('US', 4)]
    assert us['registry'] == 'arin'                      # 65536 ARIN addresses beat 256 RIPE NCC ones
    assert us['deleg'] == 65536 + 256 and us['prev_deleg'] == 65536
    assert (us['routes'], us['prev_routes'], us['routes_valid'], us['prev_routes_valid']) == (7, 7, 4, 4)
    assert got[('NL', 6)]['registry'] == 'ripencc' and got[('BE', 4)]['registry'] == 'ripencc'
    assert got[('DE', 4)]['routes'] == 1 and got[('DE', 4)]['prev_routes'] == 0   # P9 absent at prev
    assert got[('BE', 4)]['prev_routes'] is None and got[('BE', 4)]['prev_deleg'] is None   # BE new
    assert got[('AR', 4)]['routes'] == 0 and got[('AR', 4)]['deleg'] == 65536     # deleg-only country
    cols = [c for c in rows(os.path.join(bdir, 'countries.parquet'))[0]]
    assert cols[:3] == ['cc', 'af', 'registry'] and cols[3:20] == list(M)
    assert cols[20:] == ['prev_' + c for c in assemble.PREV_COUNTRY]


def test_assemble_ases(built):
    root, m, bdir = built
    got = {(r['cc'], r['af'], r['asn']): r for r in rows(os.path.join(bdir, 'ases.parquet'))}
    assert set(got) == set(EXPECTED_AS)
    r = got[('US', 4, 100)]
    assert (r['name'], r['as_cc'], r['first_valid']) == ('EXAMPLE-100 Example Net', 'US', dt.date(2025, 10, 22))
    assert (r['prev_routes'], r['prev_routes_valid'], r['prev_space'], r['prev_space_valid']) == (4, 4, 65536, 65536)
    assert got[('US', 4, 200)]['as_cc'] == 'CA'                       # asnames country differs from cc
    assert got[('US', 4, 999)]['as_cc'] is None                       # asnames ZZ, no ASN record
    assert got[('US', 4, 999)]['first_valid'] is None                 # never valid
    assert got[('DE', 4, 300)]['as_cc'] == 'DE'                       # from the delegation ASN record
    assert got[('DE', 4, 300)]['first_valid'] == dt.date(2026, 1, 2)
    assert got[('DE', 4, 300)]['prev_routes'] is None                 # absent at prev(L)
    assert got[('ZZ', 4, 700)]['as_cc'] is None                       # free ASN record
    assert got[('JP', 4, 400)]['pct_valid'] == 0.5 and got[('JP', 4, 400)]['pct_space_valid'] == 0.25
    keys = [(r['cc'], r['af'], r['asn']) for r in rows(os.path.join(bdir, 'ases.parquet'))]
    assert keys == sorted(keys)


def test_assemble_as_series(built):
    root, m, bdir = built
    s = rows(os.path.join(bdir, 'as_series.parquet'))
    # first grid date of each month with BGP data (2025-10-22, 2026-01-01, 2026-02-01) plus L
    assert sorted({r['date'] for r in s}) == [dt.date(2025, 10, 22), dt.date(2026, 1, 1), dt.date(2026, 2, 1), L]
    assert list(s[0]) == list(assemble.AS_SERIES_COLS)
    keys = [(r['cc'], r['asn'], r['af'], r['date']) for r in s]
    assert keys == sorted(keys)
    de = [r['date'] for r in s if r['asn'] == 300]
    assert de == [dt.date(2026, 2, 1), L]


def test_assemble_no_nan_anywhere(built):
    root, m, bdir = built
    con = duckdb.connect()
    for name in ('series', 'countries', 'ases', 'as_series'):
        p = os.path.join(bdir, f'{name}.parquet')
        for c, t, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall():
            if t == 'DOUBLE':
                n = con.execute(f"SELECT count(*) FROM read_parquet('{p}') WHERE isnan({c}) OR isinf({c})").fetchone()[0]
                assert n == 0, (name, c)

    def walk(o):
        if isinstance(o, float):
            assert math.isfinite(o)
        elif isinstance(o, dict):
            [walk(v) for v in o.values()]
        elif isinstance(o, list):
            [walk(v) for v in o]
    walk(m)


def test_assemble_symlink_swap_and_retention(built):
    root, m, bdir = built
    cur = common.current_link(root)
    assert os.path.islink(cur) and os.readlink(cur) == os.path.join('builds', '20261027T074500Z')
    for i, stamp in enumerate(('20261028T074500Z', '20261029T074500Z', '20261030T074500Z')):
        assemble.build(root, stamp=stamp, threads=2, memory_limit='1GB')
        assert os.readlink(cur) == os.path.join('builds', stamp)
        assert os.path.isfile(os.path.join(cur, 'manifest.json'))
    assert sorted(os.listdir(common.builds_dir(root))) == ['20261028T074500Z', '20261029T074500Z',
                                                           '20261030T074500Z']
    assert not os.path.lexists(cur + '.new')
    # an explicit --as-of builds an older view
    m2, b2 = assemble.build(root, as_of='2026-01-02', stamp='20261031T074500Z', threads=2, memory_limit='1GB')
    assert m2['as_of'] == '2026-01-02' and m2['prev_date'] == '2025-01-01' and m2['counts']['grid_dates'] == 4


def test_dominant_registry_tiebreak(tmp_path):
    p = str(tmp_path / 'deleg.parquet')
    s = dt.date(2026, 1, 1)
    _write(p, DELEG_COLS, [r + ('test',) for r in [
        (s, 4, 'arin', 'AA', 'delegated', 0, 99), (s, 4, 'ripencc', 'AA', 'delegated', 100, 199),   # v4 tie
        (s, 6, 'ripencc', 'AA', 'delegated', 0, 9),                                                 # v6 decides
        (s, 0, 'lacnic', 'BB', 'delegated', 1, 5), (s, 0, 'apnic', 'BB', 'delegated', 6, 8),         # ASNs only
        (s, 4, 'afrinic', 'CC', 'delegated', 200, 209), (s, 4, 'apnic', 'CC', 'delegated', 210, 229),
        (s, 4, 'arin', 'CC', 'free', 230, 10000),                                                   # free: ignored
    ]])
    con = duckdb.connect()
    got = dict(con.execute(assemble.dominant_registry_sql(p, s)).fetchall())
    assert got == {'AA': 'ripencc', 'BB': 'lacnic', 'CC': 'apnic'}


# ── snapshot_pairs ────────────────────────────────────────────────────────────
def test_snapshot_recount(tmp_path):
    raw = str(tmp_path / 'raw')
    _write(os.path.join(raw, 'peers.parquet'),
           [('peer_id', 'UINTEGER'), ('collector', 'VARCHAR'), ('genji_full_feed_v4', 'BOOLEAN'),
            ('genji_full_feed_v6', 'BOOLEAN')],
           [(1, 'rrc00', True, True), (2, 'rrc00', False, False), (3, 'route-views2', True, False),
            (4, 'ix-br.gru', True, True), (5, 'route-views6', False, True)])
    _write(os.path.join(raw, 'routes.parquet'),
           [('route_id', 'UINTEGER'), ('af', 'UTINYINT'), ('prefix', 'VARCHAR'), ('origin_as', 'UINTEGER')],
           [(10, 4, '192.0.2.0/24', 64500), (11, 4, '192.0.2.0/25', 64500), (12, 4, '0.0.0.0/0', 64501),
            (13, 4, '198.51.100.0/24', None), (14, 6, '2001:db8::/32', 64502), (15, 4, '203.0.113.0/24', 64503)])
    _write(os.path.join(raw, 'route_peers.parquet'), [('route_id', 'UINTEGER'), ('peer_id', 'UINTEGER')],
           [(10, 1), (10, 2), (10, 3), (10, 4), (11, 1), (12, 1), (12, 3), (13, 1), (14, 1), (14, 5), (14, 4),
            (15, 4)])
    con = duckdb.connect()
    info = snapshot_pairs.recount(con, raw)
    got = con.execute("SELECT af, prefix, origin_as, n_peers, n_collectors, n_ff FROM sp_pairs ORDER BY af").fetchall()
    # 192.0.2.0/24: peers 1,2,3 (alias 4 dropped) -> 3 peers, 2 collectors, full feed v4: peers 1 and 3
    # 2001:db8::/32: peers 1,5 -> 2 peers, 2 collectors, full feed v6: peers 1 and 5
    # /25 (not routable), /0, NULL origin and the alias-only route are dropped
    assert got == [(4, '192.0.2.0/24', 64500, 3, 2, 2), (6, '2001:db8::/32', 64502, 2, 2, 2)]
    assert info == {'collectors': {'4': 2, '6': 2}, 'rows': 2, 'rows_by_af': {'4': 1, '6': 1}}
    types = dict((c, t) for c, t, *_ in con.execute("DESCRIBE sp_pairs").fetchall())
    assert (types['af'], types['origin_as'], types['n_peers'], types['n_collectors'], types['n_ff']) == \
        ('UTINYINT', 'UBIGINT', 'UINTEGER', 'USMALLINT', 'UINTEGER')


def test_snapshot_catch_up_days(tmp_path):
    root = str(tmp_path / 'root')
    write_pairs(root, dt.date(2026, 10, 25), src='snapshot')
    meta = common.read_json(common.pairs_meta_path(dt.date(2026, 10, 25), root))
    meta['snapshot_generated_at'] = '2026-10-26T06:26:00Z'
    common.atomic_write_json(common.pairs_meta_path(dt.date(2026, 10, 25), root), meta)
    listing = [{'date': '2026-10-25', 'generated_at': '2026-10-26T06:26:00Z'},
               {'date': '2026-10-27', 'generated_at': '2026-10-28T06:26:00Z'}]
    assert snapshot_pairs.catch_up_days(root, listing) == [dt.date(2026, 10, 27)]
    listing[0]['generated_at'] = '2026-10-26T13:00:00Z'          # regenerated upstream -> refetch
    assert snapshot_pairs.catch_up_days(root, listing) == [dt.date(2026, 10, 25), dt.date(2026, 10, 27)]


# ── semantics cross-check (2026-10-28) ────────────────────────────────────────
# An independent pure-Python implementation of the compute_day rules (pipelines/country/README.md,
# compute_day) matched compute_day exactly on 2,200 random worlds and on the full 2026-10-26 data (487 geo
# rows, 134,408 AS rows). The tests below pin the edge cases of that check and keep a small, fast version
# of the randomized cross-check.
VRP_COLS = [('afi', 'UTINYINT'), ('prefix', 'VARCHAR'), ('max_length', 'UTINYINT'), ('asn', 'UBIGINT'),
            ('first_day', 'DATE'), ('last_day', 'DATE'), ('src', 'VARCHAR'), ('bridged_days', 'INTEGER')]


def mini_world(root, recs, vrps, pairs, coll=(4, 4)):
    """recs [(af, registry, cc, status, start, end)] (snap 2026-01-01); vrps [(prefix, maxlen, asn)] valid on D;
    pairs [(af, prefix, origin, n_collectors)] for D with collectors (v4, v6)."""
    _write(common.vrp_intervals_path(root), VRP_COLS,
           [(6 if ':' in p else 4, p, ml, asn, dt.date(2025, 12, 1), dt.date(2026, 2, 1), 'warehouse', 0)
            for p, ml, asn in vrps])
    _write(common.deleg_path(root), DELEG_COLS, [(dt.date(2026, 1, 1),) + r + ('test',) for r in recs])
    write_pairs(root, D, pairs, coll)
    run(root, D)
    geo = geo_map(root, D)
    per_as = {(r['cc'], r['af'], r['asn']): r for r in rows(os.path.join(common.daily_dir(D, root), 'as.parquet'))}
    return geo, per_as


def pick(row, names):
    return tuple(row[n] for n in names)


ROUTE_M = ('routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len', 'routes_nf')
SPACE_M = ('space', 'space_valid', 'space_invalid', 'space_nf')
ASES_M = ('ases', 'ases_any_valid', 'ases_all_valid', 'ases_any_invalid')
REG_M = ('deleg', 'deleg_cov', 'deleg_cov_noas0', 'vrps')


def test_as0_origin_never_valid(tmp_path):
    """An AS0 origin is never valid or invalid_len, even under an AS0 VRP whose maxLength fits.
    app/coverage.py and the BGP warehouse's snapshot-export rpki_validate() have no AS0 rule: they would call
    10.0.1.0/24 AS0 'valid' and 10.0.4.0/24 AS0 'invalid (length)'. No AS0-origin pair exists on 2026-10-25/26."""
    geo, per_as = mini_world(str(tmp_path / 'r'), [(4, 'ripencc', 'DE', 'delegated', v4('10.0.0.0'), v4('10.0.255.255'))],
                             [('10.0.1.0/24', 24, 0), ('10.0.4.0/23', 23, 0), ('10.0.8.0/24', 24, 65000)],
                             [(4, '10.0.1.0/24', 0, 4), (4, '10.0.4.0/24', 0, 4), (4, '10.0.8.0/24', 0, 4),
                              (4, '10.0.9.0/24', 0, 4), (4, '10.0.1.0/24', 65000, 4), (4, '10.0.8.0/24', 65000, 4)])
    de = geo[('cc', 'DE', 4)]
    assert pick(de, ROUTE_M) == (6, 1, 4, 0, 1)
    assert pick(de, ASES_M) == (2, 1, 0, 2)                 # AS0: no valid route; AS65000: one valid, one invalid
    assert pick(de, SPACE_M) == (1024, 256, 512, 256)       # 10.0.1/24, 10.0.4/24 invalid; 10.0.8/24 valid (MOAS)
    assert pick(de, REG_M) == (65536, 1024, 256, 3)         # AS0 VRPs cover, but not for deleg_cov_noas0
    assert pick(per_as[('DE', 4, 0)], ROUTE_M + SPACE_M) == (4, 0, 3, 0, 1, 1024, 0, 768, 256)
    assert pick(per_as[('DE', 4, 65000)], ROUTE_M + SPACE_M) == (2, 1, 1, 0, 0, 512, 256, 256, 0)


def test_top_of_address_space(tmp_path):
    """Prefixes, VRPs and records that end at the last IPv4 address / last IPv6 /64 (end + 1 = 2^32, 2^64)."""
    top6 = (1 << 64) - (1 << 16)                            # ffff:ffff:ffff::/48 as /64 keys
    geo, per_as = mini_world(
        str(tmp_path / 'r'),
        [(4, 'arin', 'US', 'delegated', v4('255.255.0.0'), v4('255.255.255.255')),
         (6, 'apnic', 'JP', 'delegated', top6, (1 << 64) - 1)],
        [('255.255.0.0/16', 16, 2), ('255.255.255.0/24', 24, 1), ('255.255.255.255/32', 32, 0),
         ('ffff:ffff:ff00::/40', 48, 5),                    # starts below the JP record -> vrps of unknown/ZZ
         ('ffff:ffff:ffff:ffff::/64', 64, 5), ('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128', 128, 0)],
        [(4, '255.255.0.0/16', 2, 4), (4, '255.255.255.0/24', 1, 4), (4, '255.255.254.0/24', 3, 4),
         (6, 'ffff:ffff:ffff::/48', 5, 4), (6, 'ffff:ffff:fffe::/48', 6, 4)])
    assert set(geo) == {('cc', 'US', 4), ('registry', 'arin', 4), ('all', 'all', 4), ('cc', 'JP', 6),
                        ('registry', 'apnic', 6), ('cc', 'ZZ', 6), ('registry', 'unknown', 6), ('all', 'all', 6)}
    us = geo[('cc', 'US', 4)]
    assert pick(us, ROUTE_M + SPACE_M + ASES_M + REG_M) == \
        (3, 2, 1, 0, 0, 65536, 65280, 256, 0, 3, 2, 2, 1, 65536, 65536, 65536, 3)
    assert pick(geo[('cc', 'JP', 6)], ROUTE_M + SPACE_M + REG_M) == (1, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 2)
    assert pick(geo[('cc', 'ZZ', 6)], ROUTE_M + SPACE_M + REG_M) == (1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1)
    assert pick(geo[('all', 'all', 6)], ROUTE_M + SPACE_M + REG_M) == (1, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 2)  # JP
    assert pick(per_as[('US', 4, 2)], SPACE_M) == (65536, 65536, 0, 0)     # own group: just the /16
    assert pick(per_as[('ZZ', 6, 6)], ROUTE_M + SPACE_M) == (1, 0, 1, 0, 0, 1, 0, 1, 0)


def test_key_with_routed_space_only(tmp_path):
    """A /23 registered in DE (its start) whose upper half has no record: registry 'unknown' / cc 'ZZ' get
    256 addresses of routed space but no route, delegated space or VRP. compute_day keeps such rows so
    that the registry-level space adds up to everything routed; 'all' counts the five RIRs only."""
    geo, _ = mini_world(str(tmp_path / 'r'), [(4, 'ripencc', 'DE', 'delegated', v4('10.0.0.0'), v4('10.0.0.255'))],
                        [], [(4, '10.0.0.0/23', 7, 4)])
    assert pick(geo[('cc', 'DE', 4)], ROUTE_M + SPACE_M + REG_M) == (1, 0, 0, 0, 1, 256, 0, 0, 256, 256, 0, 0, 0)
    for k in (('cc', 'ZZ', 4), ('registry', 'unknown', 4)):
        assert pick(geo[k], ROUTE_M + SPACE_M + REG_M) == (0, 0, 0, 0, 0, 256, 0, 0, 256, 0, 0, 0, 0)
    assert geo[('all', 'all', 4)]['space'] == 256


def test_prefix_length_floor_drops_leaks(tmp_path):
    """Prefix-length floor (2026-10-28): a visible pair counts only if it is IPv4 /8../24 or IPv6 /16../48.
    Real leaks such as 2800::/6 (seen by 47 of 75 collectors on 2026-03-14) took the IPv6 routed-space share
    of All RIRs from 67.0% to 3.4%. Here the /7 and the /3 are seen by every collector and are left out; the /8 and
    the /16 at the floor are kept. Counted, the /7 would own 11.0.0.0/8 (no record: 'unknown'/ZZ) and the
    /3 most of 2000::/3."""
    root = str(tmp_path / 'r')
    recs = [(4, 'arin', 'US', 'delegated', v4('10.0.0.0'), v4('10.255.255.255')),
            (6, 'ripencc', 'DE', 'delegated', v6hi('2001::'), v6hi('2001::') + (1 << 48) - 1)]
    vrps = [('10.0.0.0/8', 8, 64501), ('2001::/16', 16, 64503)]
    pairs = [(4, '10.0.0.0/7', 64500, 4), (4, '10.0.0.0/8', 64501, 4),
             (6, '2000::/3', 64502, 4), (6, '2001::/16', 64503, 4)]
    geo, per_as = mini_world(root, recs, vrps, pairs)
    assert set(geo) == {('cc', 'US', 4), ('registry', 'arin', 4), ('all', 'all', 4),
                        ('cc', 'DE', 6), ('registry', 'ripencc', 6), ('all', 'all', 6)}
    v4_8, v6_16 = 1 << 24, 1 << 32                         # a /8 in addresses, a /16 in /48s
    assert pick(geo[('cc', 'US', 4)], ROUTE_M + SPACE_M + ASES_M + REG_M) == \
        (1, 1, 0, 0, 0, v4_8, v4_8, 0, 0, 1, 1, 1, 0, v4_8, v4_8, v4_8, 1)
    assert pick(geo[('cc', 'DE', 6)], ROUTE_M + SPACE_M + ASES_M + REG_M) == \
        (1, 1, 0, 0, 0, v6_16, v6_16, 0, 0, 1, 1, 1, 0, v6_16, v6_16, v6_16, 1)
    assert geo[('all', 'all', 4)]['space'] == v4_8 and geo[('all', 'all', 6)]['space'] == v6_16
    assert set(per_as) == {('US', 4, 64501), ('DE', 6, 64503)}
    inp = common.read_json(common.done_path(D, root))['inputs']
    assert (inp['visible_pairs'], inp['short_pairs']) == (2, 2)
    exp_geo, exp_as = brute_force_compute_day(recs, vrps, pairs, {4: 4, 6: 4})
    assert {k: {m: geo[k][m] for m in e} for k, e in exp_geo.items()} == exp_geo
    assert set(exp_as) == set(per_as)


def test_main_passes_absolute_paths_to_jobs(world, monkeypatch, tmp_path):
    """--jobs N children run with cwd=pipelines/country, so --root/--tmp must reach them absolute."""
    seen = {}

    def fake_parallel(dates, a):
        seen.update(root=a.root, tmp=a.tmp, n=len(dates))
        return []
    monkeypatch.setattr(compute_day, '_run_parallel', fake_parallel)
    monkeypatch.chdir(tmp_path)
    assert compute_day.main(['--root', 'root', '--tmp', 'spill', '--from', '2026-01-01', '--to', '2026-01-02',
                             '--jobs', '2']) == 0
    assert seen == {'root': world, 'tmp': str(tmp_path / 'spill'), 'n': 2}
    monkeypatch.setenv('HOME', str(tmp_path))
    assert compute_day.main(['--root=~/root', '--from', '2026-01-01', '--to', '2026-01-02', '--jobs', '2']) == 0
    assert seen['root'] == world


def test_rewritten_pairs_file_is_recomputed(world):
    """A pairs file rewritten after its date was computed (snapshot regenerated upstream and fetched again by
    snapshot_pairs --catch-up, or a backfill delivered again) makes the date missing, even outside --trailing."""
    run(world, D)
    meta = common.read_json(common.pairs_meta_path(D, world))
    assert common.read_json(common.done_path(D, world))['inputs']['pairs_generated_at'] == meta['generated_at']
    assert not compute_day.is_missing(D, world)
    meta['generated_at'] = '2026-10-28T06:30:00Z'
    common.atomic_write_json(common.pairs_meta_path(D, world), meta)
    assert compute_day.is_missing(D, world)
    run(world, D)
    assert not compute_day.is_missing(D, world)
    done = common.read_json(common.done_path(D, world))     # done.json written before the key existed
    del done['inputs']['pairs_generated_at']
    common.atomic_write_json(common.done_path(D, world), done)
    meta['generated_at'] = '2026-10-29T06:30:00Z'
    common.atomic_write_json(common.pairs_meta_path(D, world), meta)
    assert not compute_day.is_missing(D, world)

def _keys(net):
    """crc.ipkeys (start, end) keys; IPv6 = upper 64 bits, clamped to /64."""
    if net.version == 4:
        s = int(net.network_address)
        return s, s + 2 ** (32 - net.prefixlen) - 1
    clen = min(net.prefixlen, 64)
    s = (int(net.network_address) >> 64) >> (64 - clen) << (64 - clen)
    return s, s + 2 ** (64 - clen) - 1


def brute_force_compute_day(recs, vrps, pairs, coll):
    """The compute_day rules, literally and slowly (tiny worlds only). Returns (geo, per_as) with space and
    deleg in output units; keys = anything with routes, routed space, delegated space or VRPs."""
    unit = {4: 1, 6: 65536}
    V = [(ipaddress.ip_network(p), ml, asn) for p, ml, asn in set(vrps)]
    col = {'valid': 'routes_valid', 'invalid_as': 'routes_inv_as', 'invalid_len': 'routes_inv_len',
           'notfound': 'routes_nf'}

    def reg_cc(af, x):
        hit = [r for r in recs if r[0] == af and r[4] <= x <= r[5]]
        return ('unknown', 'ZZ') if not hit else (hit[0][1], 'ZZ' if hit[0][3] == 'free' else hit[0][2])

    def levels(af, x):
        reg, cc = reg_cc(af, x)
        return [('cc', cc, af), ('registry', reg, af)] + ([('all', 'all', af)] if reg in common.REGISTRIES else [])

    def covering(n):
        return [(asn, ml) for vn, ml, asn in V
                if vn.version == n.version and vn.prefixlen <= n.prefixlen and n.subnet_of(vn)]

    def owner_sizes(prefixes, extra_points=()):
        """[(a, b, state)] elementary intervals owned by the longest prefix containing a."""
        P = [(_keys(n), n.prefixlen, st) for n, st in prefixes.items()]
        pts = sorted({x for (s, e), _, _ in P for x in (s, e + 1)} | set(extra_points))
        out = []
        for a, b in zip(pts, pts[1:]):
            own = [(plen, st) for (s, e), plen, st in P if s <= a and b - 1 <= e]
            if own:
                out.append((a, b, max(own)[1]))
        return out

    geo = collections.defaultdict(collections.Counter)
    present = set()
    for r in recs:                                             # 8. registered lens
        if r[3] != 'delegated':
            continue
        iv = [(_keys(n), asn) for n, _, asn in V if n.version == r[0]]
        pts = sorted({r[4], r[5] + 1} | {x for (s, e), _ in iv for x in (s, e + 1) if r[4] < x <= r[5]})
        cov = sum(b - a for a, b in zip(pts, pts[1:]) if any(s <= a and b - 1 <= e for (s, e), _ in iv))
        cnz = sum(b - a for a, b in zip(pts, pts[1:]) if any(s <= a and b - 1 <= e for (s, e), asn in iv if asn))
        for k in levels(r[0], r[4]):
            geo[k].update(deleg=r[5] - r[4] + 1, deleg_cov=cov, deleg_cov_noas0=cnz)
            present.add(k)
    for n, _, _ in V:                                          # 9. vrps by the record of the VRP start
        for k in levels(n.version, _keys(n)[0]):
            geo[k]['vrps'] += 1
            present.add(k)
    thr = {af: -(-coll[af] // 4) for af in (4, 6)}            # 1. ceil(0.25 * collectors), IPv4 /8../24,
    vis = {(af, ipaddress.ip_network(p), o) for af, p, o, nc in pairs  # IPv6 /16../48
           if nc >= thr[af] and (8 if af == 4 else 16) <= ipaddress.ip_network(p).prefixlen <= (24 if af == 4 else 48)}
    state = {}
    for af, n, o in vis:                                       # 2. RFC 6811
        c = covering(n)
        state[(af, n, o)] = ('valid' if any(a == o != 0 and n.prefixlen <= ml for a, ml in c) else
                             'invalid_len' if any(a == o != 0 for a, ml in c) else
                             'invalid_as' if c else 'notfound')
    by_origin = collections.defaultdict(list)
    groups = collections.defaultdict(dict)
    for (af, n, o), st in state.items():                       # 3. registration, 5. routes, 6. ases
        for k in levels(af, _keys(n)[0]):
            geo[k].update(routes=1, **{col[st]: 1})
            by_origin[k + (o,)].append(st)
            present.add(k)
        groups[(reg_cc(af, _keys(n)[0])[1], af, o)][n] = st
    for (lv, key, af, o), sts in by_origin.items():
        geo[(lv, key, af)].update(ases=1, ases_any_valid=int('valid' in sts),
                                  ases_all_valid=int(set(sts) == {'valid'}),
                                  ases_any_invalid=int(any(s.startswith('invalid') for s in sts)))
    for af in (4, 6):                                          # 7. routed space, most specific
        pst = {}
        for (a, n, o), st in state.items():
            if a == af:
                pst[n] = 'valid' if st == 'valid' or pst.get(n) == 'valid' else \
                    ('invalid' if covering(n) else 'notfound')
        for a, b, st in owner_sizes(pst, {x for r in recs if r[0] == af for x in (r[4], r[5] + 1)}):
            for k in levels(af, a):
                geo[k].update(space=b - a, **{'space_' + ('nf' if st == 'notfound' else st): b - a})
                present.add(k)
    per_as = {}
    for (cc, af, o), G in groups.items():                      # 10. per-AS rows
        r = collections.Counter(routes=len(G))
        for st in G.values():
            r[col[st]] += 1
        for a, b, st in owner_sizes(G):
            r.update(space=b - a, **{'space_' + ('valid' if st == 'valid' else 'nf' if st == 'notfound'
                                                 else 'invalid'): b - a})
        per_as[(cc, af, o)] = {m: (r[m] / unit[af] if m.startswith('space') else r[m]) for m in ROUTE_M + SPACE_M}
    out = {k: {m: (geo[k][m] / unit[k[2]] if m.startswith(('space', 'deleg')) else geo[k][m])
               for m in ROUTE_M + SPACE_M + ASES_M + REG_M} for k in present}
    return out, per_as


def random_world(seed):
    """Tiny random world: 10.0.0.0/20 and 2001:db8::/44 with non-CIDR records, gaps, free space, nested and
    MOAS routes, AS0 and long VRPs (IPv4 up to /32, IPv6 up to /128) and n_collectors around the threshold;
    in about half of the worlds per family also a visible route around the prefix-length floor
    (10.0.0.0/7, /8 or /9; 2000::/15, 2001::/16 or 2001::/17) that covers the whole block."""
    r = random.Random(seed)
    recs, vrps, pairs = [], [], []
    coll = {4: r.choice([1, 4, 9]), 6: r.choice([0, 4, 7])}
    for af, base, u, (rlo, rhi), wfull in ((4, v4('10.0.0.0'), 12, (17, 24), 32),
                                           (6, v6hi('2001:db8::'), 20, (41, 48), 128)):
        cuts = sorted({base + r.randrange(1 << u) for _ in range(r.randint(0, 8))} | {base, base + (1 << u)})
        for a, b in zip(cuts, cuts[1:]):
            if r.random() < 0.8:
                free = r.random() < 0.25
                recs.append((af, r.choice(('arin', 'ripencc', 'apnic', 'iana')),
                             r.choice(('US', 'DE', 'JP', 'EU')), 'free' if free else 'delegated', a, b - 1))

        def text(lo, hi):
            L = r.randint(lo, hi)
            full = ((base + r.randrange(1 << u)) << (0 if af == 4 else 64)) | (r.getrandbits(64) if af == 6 else 0)
            full = full >> (wfull - L) << (wfull - L)
            return str(ipaddress.IPv4Network((full, L)) if af == 4 else ipaddress.IPv6Network((full, L))), L
        routes = [text(rlo, rhi) for _ in range(r.randint(0, 10))]
        for p, L in routes:
            for o in r.sample((0, 1, 2, 3, 64500), r.choice((1, 1, 2))):
                pairs.append((af, p, o, max(0, -(-coll[af] // 4) + r.choice((-1, 0, 0, 1, 5)))))
        for _ in range(r.randint(0, 10)):
            p, L = text(rlo - 1, r.choice((rhi, wfull)))
            vrps.append((p, min(wfull, L + r.choice((0, 0, 1, 8))), r.choice((0, 1, 2, 3))))
        for p, L in r.sample(routes, min(len(routes), 3)):
            vrps.append((p, min(wfull, L + r.choice((-1, 0, 2))), r.choice((0, 1, 2))))
    # drawn after everything above, so the worlds of earlier versions of this test are unchanged
    for af, floor in ((4, ipaddress.ip_network('10.0.0.0/8')), (6, ipaddress.ip_network('2001::/16'))):
        if r.random() < 0.5:
            n = r.choice((floor.supernet(), floor, next(floor.subnets())))
            pairs.append((af, str(n), r.choice((1, 64500)), -(-coll[af] // 4) + r.choice((0, 1))))
    return recs, vrps, sorted(set(pairs)), coll


@pytest.mark.parametrize('seed', range(24))
def test_compute_day_matches_brute_force(tmp_path, seed):
    recs, vrps, pairs, coll = random_world(seed)
    if len({(af, p, o) for af, p, o, _ in pairs}) != len(pairs):     # one row per (af, prefix, origin)
        pairs = list({(af, p, o): (af, p, o, nc) for af, p, o, nc in pairs}.values())
    geo, per_as = mini_world(str(tmp_path / 'r'), recs, vrps, pairs, (coll[4], coll[6]))
    exp_geo, exp_as = brute_force_compute_day(recs, vrps, pairs, coll)
    assert set(geo) == set(exp_geo)
    for k, e in exp_geo.items():
        assert {m: geo[k][m] for m in e} == e, k
    assert {k: {m: v[m] for m in ROUTE_M + SPACE_M} for k, v in per_as.items()} == exp_as


def test_snapshot_download_verifies_or_warns(tmp_path, monkeypatch, capsys):
    """download() checks bytes + sha256 from the manifest; a file the manifest does not describe is
    accepted with a WARNING (schema change upstream) instead of silently."""
    import hashlib
    body = {'peers.parquet': b'p' * 10, 'routes.parquet': b'r' * 20, 'route_peers.parquet': b'q' * 30}
    files = {n: {'bytes': len(b), 'sha256': hashlib.sha256(b).hexdigest()} for n, b in body.items()}
    manifest = {'date': '2026-10-26', 'complete': True, 'files': files}

    def fake_request(url, dest=None, timeout=0, retries=0):
        name = url.rsplit('/', 1)[1]
        data = json.dumps(manifest).encode() if name == 'manifest.json' else body[name]
        with open(dest, 'wb') as f:
            f.write(data)
        return dest
    monkeypatch.setattr(snapshot_pairs, '_request', fake_request)
    monkeypatch.setattr(snapshot_pairs.time, 'sleep', lambda s: None)   # whole-day retries pause 10-20 s
    day = dt.date(2026, 10, 26)
    snapshot_pairs.download(day, str(tmp_path / 'a'))
    assert 'WARNING' not in capsys.readouterr().err
    body['routes.parquet'] = b'x' * 20                       # same size, different content
    with pytest.raises(RuntimeError, match='sha256 mismatch'):
        snapshot_pairs.download(day, str(tmp_path / 'b'))
    del files['routes.parquet']
    snapshot_pairs.download(day, str(tmp_path / 'c'))
    assert 'routes.parquet: the manifest lists no bytes/sha256' in capsys.readouterr().err


def test_islands_with_duplicate_intervals_are_exact():
    """islands() over many duplicated (af, s, e1) rows (vint has one row per VRP and several VRPs share a
    prefix) equals a plain sort-and-merge in both families; touching intervals merge."""
    con = duckdb.connect()
    con.execute('SET threads=8')
    con.execute("""CREATE TABLE t AS
        SELECT CAST(CASE WHEN i % 2 = 0 THEN 4 ELSE 6 END AS UTINYINT) AS af,
               CAST(i * 10 + hash(i) % 3 AS HUGEINT) AS s,
               CAST(i * 10 + hash(i) % 3 + [1, 2, 5, 12][CAST(hash(i * 7) % 4 AS BIGINT) + 1] AS HUGEINT) AS e1
        FROM range(200000) r(i), range(3) d(j) WHERE j <= hash(i * 13) % 3""")
    compute_day.islands(con, 't', 'isl')
    for af in (4, 6):
        merged = []
        for s, e1 in con.execute(f'SELECT DISTINCT s, e1 FROM t WHERE af = {af} ORDER BY s, e1').fetchall():
            if merged and s <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], e1)
            else:
                merged.append([s, e1])
        got = con.execute(f'SELECT s, e1, cum FROM isl WHERE af = {af} ORDER BY s').fetchall()
        assert [(s, e1) for s, e1, _ in got] == [tuple(m) for m in merged]
        cum = 0
        for s, e1, c in got:
            assert c == cum
            cum += e1 - s
    con.close()
