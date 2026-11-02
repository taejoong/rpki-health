"""Prefix <-> integer keys.

IPv4: key = 32-bit address, W = 32, unit = one address.
IPv6: key = upper 64 bits, W = 64, unit = one /64; prefixes longer than /64 are clamped to /64.
A prefix covers [start, end] inclusive, end = start + 2^(W - len) - 1.

`register_macros(con)` defines the same mapping as DuckDB SQL macros so per-day jobs never
convert prefixes row by row in Python:
  ck_af(prefix)          4 or 6
  ck_len(prefix)         prefix length as written (UTINYINT)
  ck_start(prefix)       start key (UBIGINT); host bits are cleared
  ck_end(prefix)         end key (UBIGINT, inclusive; uses the clamped length for IPv6)
Inputs must be canonical CIDR strings ('a.b.c.d/n', IPv6 hex groups with at most one '::').
IPv4-embedded IPv6 text ('::ffff:1.2.3.4/120') is not supported by the macros.
"""
import ipaddress

V6_UNITS_PER_48 = 1 << 16   # /64 units in one /48


def width(af):
    return 32 if af == 4 else 64


def parse_prefix(s):
    """'p/len' -> (af, start, end, length). length is the written prefix length."""
    net = ipaddress.ip_network(s.strip(), strict=False)
    if net.version == 4:
        start = int(net.network_address)
        return 4, start, start + (1 << (32 - net.prefixlen)) - 1, net.prefixlen
    plen = min(net.prefixlen, 64)
    start = (int(net.network_address) >> 64) & ~((1 << (64 - plen)) - 1)
    return 6, start, start + (1 << (64 - plen)) - 1, net.prefixlen


def size_units(af, length):
    """Space of a prefix in output units: IPv4 addresses, IPv6 /48s (float below /48)."""
    if af == 4:
        return float(1 << (32 - length))
    return (1 << (64 - min(length, 64))) / V6_UNITS_PER_48


_MACROS = r"""
CREATE OR REPLACE MACRO ck_af(p) AS CASE WHEN contains(p, ':') THEN 6 ELSE 4 END;
CREATE OR REPLACE MACRO ck_len(p) AS CAST(split_part(p, '/', 2) AS UTINYINT);
CREATE OR REPLACE MACRO ck_v4(a) AS
  (CAST(split_part(a, '.', 1) AS UBIGINT) << 24) + (CAST(split_part(a, '.', 2) AS UBIGINT) << 16)
  + (CAST(split_part(a, '.', 3) AS UBIGINT) << 8) + CAST(split_part(a, '.', 4) AS UBIGINT);
CREATE OR REPLACE MACRO ck_v6_head(a) AS
  CASE WHEN NOT contains(a, '::') THEN string_split(a, ':')
       WHEN split_part(a, '::', 1) = '' THEN []
       ELSE string_split(split_part(a, '::', 1), ':') END;
CREATE OR REPLACE MACRO ck_v6_tail(a) AS
  CASE WHEN NOT contains(a, '::') OR split_part(a, '::', 2) = '' THEN []
       ELSE string_split(split_part(a, '::', 2), ':') END;
CREATE OR REPLACE MACRO ck_v6_groups(a) AS
  list_concat(ck_v6_head(a),
              list_transform(range(8 - len(ck_v6_head(a)) - len(ck_v6_tail(a))), x -> '0'),
              ck_v6_tail(a));
CREATE OR REPLACE MACRO ck_v6_hi(a) AS
  (CAST('0x' || ck_v6_groups(a)[1] AS UBIGINT) << 48) + (CAST('0x' || ck_v6_groups(a)[2] AS UBIGINT) << 32)
  + (CAST('0x' || ck_v6_groups(a)[3] AS UBIGINT) << 16) + CAST('0x' || ck_v6_groups(a)[4] AS UBIGINT);
CREATE OR REPLACE MACRO ck_w(p) AS CASE WHEN contains(p, ':') THEN 64 ELSE 32 END;
CREATE OR REPLACE MACRO ck_clen(p) AS least(ck_len(p), ck_w(p));
CREATE OR REPLACE MACRO ck_raw(p) AS
  CASE WHEN contains(p, ':') THEN ck_v6_hi(lower(split_part(p, '/', 1)))
       ELSE ck_v4(split_part(p, '/', 1)) END;
CREATE OR REPLACE MACRO ck_start(p) AS
  CASE WHEN ck_clen(p) = 0 THEN 0::UBIGINT
       ELSE (ck_raw(p) >> (ck_w(p) - ck_clen(p))) << (ck_w(p) - ck_clen(p)) END;
CREATE OR REPLACE MACRO ck_end(p) AS
  CAST(ck_start(p) AS HUGEINT) + (CAST(1 AS HUGEINT) << (ck_w(p) - ck_clen(p))) - 1;
"""


def register_macros(con):
    for stmt in _MACROS.split(';'):
        if stmt.strip():
            con.execute(stmt)
    # ck_end above is HUGEINT so /0-/1 ends cannot overflow during arithmetic; expose a UBIGINT view
    con.execute("CREATE OR REPLACE MACRO ck_end_u(p) AS CAST(ck_end(p) AS UBIGINT)")
    return con
