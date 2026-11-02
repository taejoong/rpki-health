"""ipkeys: SQL macros agree with the Python reference; grid helpers follow the time grid (Wednesdays
2012-01-04..2025-12-31, then daily)."""
import datetime as dt
import ipaddress
import os
import random
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipelines', 'country'))
from crc import common, ipkeys  # noqa: E402


def _random_prefixes(n=4000, seed=7):
    rnd = random.Random(seed)
    out = ['0.0.0.0/1', '255.255.255.255/32', '10.0.0.0/8', '192.0.2.77/24', '2001:db8::/32',
           '2001:db8:1:2::/64', '2001:db8:1:2:3::/80', '::/1', '8000::/1', 'ffff:ffff:ffff:ffff::/64',
           '2001:db8::1/128', '2c0f:f000::/20', '2001:0:0:1::/48', '2a00::/12']
    for _ in range(n):
        if rnd.random() < 0.5:
            plen = rnd.randint(1, 32)
            addr = rnd.getrandbits(32)
            out.append(str(ipaddress.ip_network((addr, plen), strict=False)))
        else:
            plen = rnd.randint(1, 128)
            addr = rnd.getrandbits(128)
            out.append(str(ipaddress.ip_network((addr, plen), strict=False)))
    return out


def test_macros_match_python():
    con = duckdb.connect()
    ipkeys.register_macros(con)
    prefixes = _random_prefixes()
    con.execute("CREATE TABLE t(p VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?)", [(p,) for p in prefixes])
    rows = con.execute("SELECT p, ck_af(p), ck_len(p), ck_start(p), ck_end_u(p) FROM t").fetchall()
    for p, af, ln, start, end in rows:
        exp = ipkeys.parse_prefix(p)
        assert (af, start, end, ln) == exp, (p, (af, start, end, ln), exp)


def test_size_units():
    assert ipkeys.size_units(4, 24) == 256
    assert ipkeys.size_units(6, 48) == 1
    assert ipkeys.size_units(6, 32) == 65536
    assert ipkeys.size_units(6, 56) == 1 / 256
    assert ipkeys.size_units(6, 128) == ipkeys.size_units(6, 64)


def test_grid():
    g = common.grid('2026-01-03')
    assert g[0] == dt.date(2012, 1, 4)
    assert dt.date(2020, 1, 1) in g and dt.date(2025, 12, 31) in g
    assert g[-3:] == [dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(2026, 1, 3)]
    assert all((b - a).days == 7 for a, b in zip(g, g[1:]) if b < common.DAILY_FROM)
    assert all(common.is_grid_date(d) for d in g)
    assert not common.is_grid_date('2025-12-30')
    assert common.bgp_grid('2026-01-01')[0] == dt.date(2020, 1, 1)
    assert common.prev_grid_date('2026-10-26') == dt.date(2025, 10, 22)
    assert common.prev_grid_date('2027-03-01') == dt.date(2026, 3, 1)
    assert common.month_firsts([dt.date(2020, 1, 8), dt.date(2020, 1, 1), dt.date(2020, 2, 5)]) == \
        [dt.date(2020, 1, 1), dt.date(2020, 2, 5)]
