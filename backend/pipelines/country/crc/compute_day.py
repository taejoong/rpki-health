"""One grid date -> daily/YYYY/D/{geo.parquet, as.parquet, done.json}.

Inputs for D: pairs_D (BGP dates >= 2020-01-01), the VRP set of D from inputs/vrp_intervals.parquet,
and the delegation snapshot of D (latest snap <= D) from inputs/deleg.parquet.

Everything runs as DuckDB SQL over integer keys (crc.ipkeys macros); no per-row Python.
- Visible pairs: n_collectors >= ceil(25% of the day's collectors of the family), and IPv4 /8../24 or
  IPv6 /16../48 (common.ROUTABLE_MIN_LEN / ROUTABLE_MAX_LEN). Shorter announcements are leaks; they are
  counted in done.json (inputs.short_pairs) and left out.
- RFC 6811: covering-length probes. For every visible prefix and every VRP length L <= its length
  that occurs among the VRPs, key = start >> (W - L); equi-join the VRPs on (af, L, key).
- Registration: ASOF join of a point (prefix start, interval start, VRP start) on record start_,
  then a check that the point is <= end_ (no record -> registry 'unknown', cc 'ZZ').
- Most-specific space: elementary intervals between all prefix boundaries (plus record boundaries
  for the country lens). For each prefix the nesting level is the running depth just after its
  open event (events sorted by position, closes before opens, shorter prefixes first). The owner
  of an elementary interval starting at x with depth d > 0 is the last prefix at level d that
  starts at or before x (ASOF join on (group, level)); depth 0 means unrouted and is dropped.
  This is exact for CIDR prefixes because two prefixes either nest or are disjoint.
- Registered coverage: VRP prefixes merged into disjoint islands (gaps-and-islands); C(x) = island
  length below x via an ASOF join with running sums; covered part of a record = C(end+1) - C(start).
- Level 'all' ("All RIRs") sums the five RIRs only: space registered to IANA (2002::/16, 6to4) or to no
  record stays in its 'registry'/'cc' rows but out of the world total.
IPv4 keys are addresses (W = 32); IPv6 keys are /64 units (W = 64). Interval arithmetic is HUGEINT;
space output is DOUBLE in IPv4 addresses and IPv6 /48s.

CLI (run from pipelines/country):
  python -m crc.compute_day [--root R] --date D
  python -m crc.compute_day [--root R] --from A --to B [--jobs N] [--only-missing]
  python -m crc.compute_day [--root R] --auto [--trailing 32] [--jobs N]
      --auto = the trailing N grid dates up to the latest pairs day, plus every missing grid date
      (32 = the longest bridged VRP gap, 31 days, plus the day the VRPs come back)
  options: --threads (per process), --memory-limit (per process), --tmp (DuckDB spill directory)
"""
import argparse
import concurrent.futures
import os
import shutil
import subprocess
import sys
import time

from . import VERSION, common, ipkeys
from .common import lit, log

BGP_METRICS = ('routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len', 'routes_nf',
               'space', 'space_valid', 'space_invalid', 'space_nf',
               'ases', 'ases_any_valid', 'ases_all_valid', 'ases_any_invalid')
REG_METRICS = ('deleg', 'deleg_cov', 'deleg_cov_noas0', 'vrps')
GEO_COLUMNS = ('date', 'level', 'key', 'af') + BGP_METRICS + REG_METRICS
AS_COLUMNS = ('date', 'cc', 'af', 'asn', 'routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len',
              'routes_nf', 'space', 'space_valid', 'space_invalid', 'space_nf')
DOUBLE_COLS = {'space', 'space_valid', 'space_invalid', 'space_nf', 'deleg', 'deleg_cov', 'deleg_cov_noas0'}
# --auto recomputes the trailing grid dates up to the latest pairs day: VRP bridging (gaps of up to 31 days,
# crc.intervals.TIER2_DAYS) and late RIPE NCC archive days change the VRP sets of past days once the VRPs
# come back, so the window is the longest bridged gap plus that day. run_daily.sh uses the same default.
TRAILING = 32


def _len_macro(name, by_af):
    return (f"CREATE OR REPLACE MACRO {name}(af) AS "
            f"CASE WHEN af = 4 THEN {int(by_af[4])} ELSE {int(by_af[6])} END;")


_MACROS = r"""
CREATE OR REPLACE MACRO cd_w(af) AS CASE WHEN af = 4 THEN 32 ELSE 64 END;
""" + _len_macro('cd_minlen', common.ROUTABLE_MIN_LEN) + '\n' + _len_macro('cd_maxlen', common.ROUTABLE_MAX_LEN) + r"""
CREATE OR REPLACE MACRO cd_unit(af) AS CASE WHEN af = 4 THEN 1.0 ELSE 65536.0 END;
CREATE OR REPLACE MACRO cd_v6_lo(a) AS
  (CAST('0x' || ck_v6_groups(a)[5] AS UBIGINT) << 48) + (CAST('0x' || ck_v6_groups(a)[6] AS UBIGINT) << 32)
  + (CAST('0x' || ck_v6_groups(a)[7] AS UBIGINT) << 16) + CAST('0x' || ck_v6_groups(a)[8] AS UBIGINT);
CREATE OR REPLACE MACRO cd_lo(p) AS
  CASE WHEN contains(p, ':') AND ck_len(p) > 64
       THEN (cd_v6_lo(lower(split_part(p, '/', 1))) >> (128 - ck_len(p))) << (128 - ck_len(p))
       ELSE CAST(0 AS UBIGINT) END;
CREATE OR REPLACE MACRO cd_probe(k, w, l) AS CASE WHEN l = 0 THEN CAST(0 AS UBIGINT) ELSE k >> (w - l) END;
"""


def register_macros(con):
    ipkeys.register_macros(con)
    for stmt in _MACROS.split(';'):
        if stmt.strip():
            con.execute(stmt)


def _levels(src, cols):
    """UNION ALL of src rows under the three levels: (level, key, cols...). 'all' = the five RIRs."""
    c = ', '.join(cols)
    rirs = ', '.join(lit(r) for r in common.REGISTRIES)
    return (f"SELECT 'cc' AS level, cc AS key, {c} FROM {src} "
            f"UNION ALL SELECT 'registry' AS level, registry AS key, {c} FROM {src} "
            f"UNION ALL SELECT 'all' AS level, 'all' AS key, {c} FROM {src} WHERE registry IN ({rirs})")


def owner_intervals(con, src, gcols, out, extra_points=None):
    """Most-specific owner of every routed elementary interval, per group.

    src: table with gcols..., s HUGEINT (start), e1 HUGEINT (end + 1), plen; (gcols, s, plen) unique.
    extra_points: optional table (gcols..., pos HUGEINT) of additional boundaries (record edges).
    out: TEMP TABLE (gcols..., pos, size, os, oplen) for every interval with depth > 0, where
         (os, oplen) identifies the owning prefix in src.
    """
    g = ', '.join(gcols)
    on = ' AND '.join(f'o.{c} = e.{c}' for c in gcols)
    pts = f"SELECT {g}, s AS pos FROM {src} UNION ALL SELECT {g}, e1 AS pos FROM {src}"
    if extra_points:
        pts += f" UNION ALL SELECT {g}, pos FROM {extra_points}"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE oi_run AS
        SELECT {g}, pos, kind, plen,
               CAST(sum(delta) OVER (PARTITION BY {g} ORDER BY pos, kind, plen
                                     ROWS UNBOUNDED PRECEDING) AS INTEGER) AS depth
        FROM (SELECT {g}, s AS pos, 1 AS kind, plen, 1 AS delta FROM {src}
              UNION ALL SELECT {g}, e1 AS pos, 0 AS kind, plen, -1 AS delta FROM {src}
              UNION ALL SELECT DISTINCT {g}, pos, 2 AS kind, 0 AS plen, 0 AS delta FROM ({pts}))""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE oi_lvl AS
        SELECT {g}, pos AS s, plen, depth AS level FROM oi_run WHERE kind = 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE oi_el AS
        SELECT * FROM (
          SELECT {g}, pos, depth, lead(pos) OVER (PARTITION BY {g} ORDER BY pos) AS nxt
          FROM oi_run WHERE kind = 2)
        WHERE depth > 0""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT {', '.join('e.' + c for c in gcols)}, e.pos, e.nxt - e.pos AS size, o.s AS os, o.plen AS oplen
        FROM oi_el e ASOF JOIN oi_lvl o ON {on} AND o.level = e.depth AND e.pos >= o.s""")
    bad = con.execute(f"SELECT count(*) FROM oi_el").fetchone()[0] - \
        con.execute(f"SELECT count(*) FROM {out}").fetchone()[0]
    if bad:
        raise RuntimeError(f'owner_intervals({src}): {bad} routed intervals without an owner')
    con.execute("DROP TABLE oi_run; DROP TABLE oi_lvl; DROP TABLE oi_el")


def islands(con, src, out):
    """Merge (af, s, e1) intervals into disjoint islands with the running length `cum` before each.
    Duplicates are removed first (several VRPs share a prefix): with ties in ORDER BY s, e1 the two
    window passes could order them differently and let an island absorb the gap before it."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT af, s, e1, coalesce(sum(e1 - s) OVER (PARTITION BY af ORDER BY s
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS cum
        FROM (SELECT af, min(s) AS s, max(e1) AS e1 FROM (
                SELECT *, sum(CASE WHEN prev_e1 IS NULL OR s > prev_e1 THEN 1 ELSE 0 END)
                          OVER (PARTITION BY af ORDER BY s, e1 ROWS UNBOUNDED PRECEDING) AS isl
                FROM (SELECT af, s, e1, max(e1) OVER (PARTITION BY af ORDER BY s, e1
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_e1
                      FROM (SELECT DISTINCT af, s, e1 FROM {src})))
              GROUP BY af, isl)""")


def _covered_sql(src, isl, x, name):
    """Add column `name` = island length strictly below src.x (C(x))."""
    return (f"SELECT q.*, coalesce(i.cum + least(q.{x}, i.e1) - i.s, 0) AS {name} "
            f"FROM {src} q ASOF LEFT JOIN {isl} i ON i.af = q.af AND q.{x} >= i.s")


def pick_deleg_snap(con, deleg_file, d):
    row = con.execute(f"SELECT max(snap) FROM read_parquet({lit(deleg_file)}) "
                      f"WHERE snap <= DATE {lit(d.isoformat())}").fetchone()
    return row[0]


def compute(con, d, root, write=True):
    """Compute one date on an open connection (macros registered). Returns the done.json dict."""
    t0 = time.time()
    d = common.to_date(d)
    ds = lit(d.isoformat())
    vrp_file, deleg_file = common.vrp_intervals_path(root), common.deleg_path(root)
    for p in (vrp_file, deleg_file):
        if not os.path.exists(p):
            raise FileNotFoundError(p)

    # ── pairs (BGP) ──────────────────────────────────────────────────────────
    meta = None
    if d >= common.BGP_FROM:
        meta = common.read_json(common.pairs_meta_path(d, root))
        if meta is None or not os.path.exists(common.pairs_path(d, root)):
            log(f'compute_day: WARNING {d}: no pairs file; BGP columns will be NULL')
            meta = None
    bgp = meta is not None

    # ── VRP set of D ─────────────────────────────────────────────────────────
    con.execute(f"""CREATE OR REPLACE TEMP TABLE vrp AS
        SELECT DISTINCT CAST(ck_af(prefix) AS UTINYINT) AS af, ck_len(prefix) AS vlen,
               ck_start(prefix) AS vstart, cd_lo(prefix) AS vlo,
               CAST(ck_end(prefix) AS HUGEINT) + 1 AS ve1,
               CAST(max_length AS UTINYINT) AS max_length, CAST(asn AS UBIGINT) AS asn
        FROM (SELECT DISTINCT prefix, max_length, asn FROM read_parquet({lit(vrp_file)})
              WHERE DATE {ds} BETWEEN first_day AND last_day)""")
    vrp_rows = con.execute("SELECT count(*) FROM vrp").fetchone()[0]

    # ── delegation snapshot of D ─────────────────────────────────────────────
    snap = pick_deleg_snap(con, deleg_file, d)
    if snap is None:
        log(f'compute_day: WARNING {d}: no delegation snapshot <= D; everything maps to unknown/ZZ')
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rec AS
        SELECT CAST(afi AS UTINYINT) AS af, registry,
               CASE WHEN status = 'free' OR cc IS NULL OR cc = '' THEN 'ZZ' ELSE upper(cc) END AS cc,
               status, CAST(start_ AS UBIGINT) AS rs, CAST(end_ AS UBIGINT) AS re,
               CAST(start_ AS HUGEINT) AS rsh, CAST(end_ AS HUGEINT) + 1 AS re1
        FROM read_parquet({lit(deleg_file)})
        WHERE snap = {('DATE ' + lit(snap.isoformat())) if snap else 'NULL'} AND afi IN (4, 6)""")
    overlaps = con.execute("""SELECT count(*) FROM (
        SELECT rs, max(re) OVER (PARTITION BY af ORDER BY rs, re ROWS BETWEEN UNBOUNDED PRECEDING
                                 AND 1 PRECEDING) AS prev_re FROM rec) WHERE prev_re >= rs""").fetchone()[0]
    if overlaps:
        log(f'compute_day: WARNING {d}: {overlaps} overlapping delegation records in snap {snap}')

    # ── registered lens: delegated space covered by the union of VRP prefixes ──
    con.execute("CREATE OR REPLACE TEMP TABLE vint AS "
                "SELECT af, CAST(vstart AS HUGEINT) AS s, ve1 AS e1, asn <> 0 AS nz FROM vrp")
    islands(con, '(SELECT af, s, e1 FROM vint)', 'isl_all')
    islands(con, '(SELECT af, s, e1 FROM vint WHERE nz)', 'isl_nz')
    con.execute(f"""CREATE OR REPLACE TEMP TABLE regrec AS
        WITH d0 AS (SELECT af, registry, cc, rsh AS a, re1 AS b FROM rec WHERE status = 'delegated'),
        d1 AS ({_covered_sql('d0', 'isl_all', 'a', 'ca')}),
        d2 AS ({_covered_sql('d1', 'isl_all', 'b', 'cb')}),
        d3 AS ({_covered_sql('d2', 'isl_nz', 'a', 'na')}),
        d4 AS ({_covered_sql('d3', 'isl_nz', 'b', 'nb')})
        SELECT af, registry, cc, b - a AS sz, cb - ca AS cov, nb - na AS cov_nz FROM d4""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE m_reg AS
        SELECT level, key, af, CAST(sum(sz) AS DOUBLE) / cd_unit(af) AS deleg,
               CAST(sum(cov) AS DOUBLE) / cd_unit(af) AS deleg_cov,
               CAST(sum(cov_nz) AS DOUBLE) / cd_unit(af) AS deleg_cov_noas0
        FROM ({_levels('regrec', ('af', 'sz', 'cov', 'cov_nz'))}) GROUP BY level, key, af""")

    # vrps: distinct VRPs by the record containing the VRP start
    con.execute(f"""CREATE OR REPLACE TEMP TABLE vrprec AS
        SELECT v.af, CASE WHEN r.re >= v.vstart THEN r.registry ELSE 'unknown' END AS registry,
               CASE WHEN r.re >= v.vstart THEN r.cc ELSE 'ZZ' END AS cc
        FROM vrp v ASOF LEFT JOIN rec r ON r.af = v.af AND v.vstart >= r.rs""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE m_vrps AS
        SELECT level, key, af, count(*) AS vrps FROM ({_levels('vrprec', ('af',))}) GROUP BY level, key, af""")

    info = {'pairs_src': None, 'pairs_generated_at': None, 'pairs_rows': None, 'visible_pairs': None,
            'short_pairs': None, 'collectors': None, 'threshold': None}
    tables = ['m_reg', 'm_vrps']
    if bgp:
        coll = {af: int((meta.get('collectors') or {}).get(str(af), 0) or 0) for af in (4, 6)}
        thr = {af: common.visibility_threshold(coll[af]) for af in (4, 6)}
        pf = lit(common.pairs_path(d, root))
        seen = f"origin_as IS NOT NULL AND n_collectors >= CASE WHEN af = 4 THEN {thr[4]} ELSE {thr[6]} END"
        # 1. visible pairs: seen by enough collectors, IPv4 /8../24 or IPv6 /16../48. The upper bound repeats
        #    the routable guard of the pairs files; the lower bound drops leaks (see common.ROUTABLE_MIN_LEN).
        con.execute(f"""CREATE OR REPLACE TEMP TABLE pv AS
            SELECT DISTINCT CAST(af AS UTINYINT) AS af, ck_len(prefix) AS plen, ck_start(prefix) AS pstart,
                   CAST(origin_as AS UBIGINT) AS origin
            FROM read_parquet({pf})
            WHERE {seen} AND ck_len(prefix) BETWEEN cd_minlen(af) AND cd_maxlen(af)""")
        # pairs that met the threshold but are shorter than the floor (route leaks; /0 included), not counted
        pairs_rows, short = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE {seen}
                AND ck_len(prefix) < cd_minlen(af)) FROM read_parquet({pf})""").fetchone()
        visible = con.execute("SELECT count(*) FROM pv").fetchone()[0]
        con.execute("CREATE OR REPLACE TEMP TABLE px AS SELECT DISTINCT af, plen, pstart FROM pv")
        # 2. RFC 6811 via covering-length probes
        con.execute("""CREATE OR REPLACE TEMP TABLE vk AS
            SELECT af, vlen, cd_probe(vstart, cd_w(af), vlen) AS k, max_length, asn
            FROM vrp WHERE vlen <= cd_maxlen(af)""")
        con.execute("CREATE OR REPLACE TEMP TABLE vl AS SELECT DISTINCT af, vlen FROM vk")
        con.execute("""CREATE OR REPLACE TEMP TABLE cov AS
            SELECT q.af, q.plen, q.pstart, v.asn, v.max_length
            FROM (SELECT x.af, x.plen, x.pstart, l.vlen, cd_probe(x.pstart, cd_w(x.af), l.vlen) AS k
                  FROM px x JOIN vl l ON l.af = x.af AND l.vlen <= x.plen) q
            JOIN vk v ON v.af = q.af AND v.vlen = q.vlen AND v.k = q.k""")
        con.execute("""CREATE OR REPLACE TEMP TABLE pst AS
            SELECT p.af, p.plen, p.pstart, p.origin,
                   CASE WHEN bool_or(c.asn = p.origin AND p.origin <> 0 AND p.plen <= c.max_length) THEN 'valid'
                        WHEN bool_or(c.asn = p.origin AND p.origin <> 0) THEN 'invalid_len'
                        WHEN count(c.asn) > 0 THEN 'invalid_as'
                        ELSE 'notfound' END AS state
            FROM pv p LEFT JOIN cov c ON c.af = p.af AND c.plen = p.plen AND c.pstart = p.pstart
            GROUP BY p.af, p.plen, p.pstart, p.origin""")
        # 3. registration of each prefix: the record containing its start
        con.execute("""CREATE OR REPLACE TEMP TABLE preg AS
            SELECT x.af, x.plen, x.pstart,
                   CASE WHEN r.re >= x.pstart THEN r.registry ELSE 'unknown' END AS registry,
                   CASE WHEN r.re >= x.pstart THEN r.cc ELSE 'ZZ' END AS cc
            FROM px x ASOF LEFT JOIN rec r ON r.af = x.af AND x.pstart >= r.rs""")
        con.execute("""CREATE OR REPLACE TEMP TABLE pk AS
            SELECT s.af, s.plen, s.pstart, s.origin, s.state, g.registry, g.cc
            FROM pst s JOIN preg g USING (af, plen, pstart)""")
        # 5./6. routes and origin-AS lenses
        con.execute(f"""CREATE OR REPLACE TEMP TABLE m_routes AS
            WITH lv AS ({_levels('pk', ('af', 'origin', 'state'))}),
            r AS (SELECT level, key, af, count(*) AS routes,
                         count(*) FILTER (WHERE state = 'valid') AS routes_valid,
                         count(*) FILTER (WHERE state = 'invalid_as') AS routes_inv_as,
                         count(*) FILTER (WHERE state = 'invalid_len') AS routes_inv_len,
                         count(*) FILTER (WHERE state = 'notfound') AS routes_nf
                  FROM lv GROUP BY level, key, af),
            o AS (SELECT level, key, af, origin, bool_or(state = 'valid') AS anyv,
                         bool_and(state = 'valid') AS allv,
                         bool_or(state IN ('invalid_as', 'invalid_len')) AS anyi
                  FROM lv GROUP BY level, key, af, origin),
            a AS (SELECT level, key, af, count(*) AS ases, count(*) FILTER (WHERE anyv) AS ases_any_valid,
                         count(*) FILTER (WHERE allv) AS ases_all_valid,
                         count(*) FILTER (WHERE anyi) AS ases_any_invalid
                  FROM o GROUP BY level, key, af)
            SELECT * FROM r JOIN a USING (level, key, af)""")
        # 7. routed space, most-specific attribution, split by delegation record
        con.execute("""CREATE OR REPLACE TEMP TABLE pxs AS
            SELECT af, plen, CAST(pstart AS HUGEINT) AS s,
                   CAST(pstart AS HUGEINT) + (CAST(1 AS HUGEINT) << (cd_w(af) - plen)) AS e1,
                   CASE WHEN bool_or(state = 'valid') THEN 'valid'
                        WHEN bool_or(state <> 'notfound') THEN 'invalid' ELSE 'notfound' END AS pstate
            FROM pst GROUP BY af, plen, pstart""")
        con.execute("""CREATE OR REPLACE TEMP TABLE recpts AS
            SELECT af, rsh AS pos FROM rec UNION ALL SELECT af, re1 AS pos FROM rec""")
        owner_intervals(con, 'pxs', ['af'], 'sp_el', extra_points='recpts')
        con.execute("""CREATE OR REPLACE TEMP TABLE sp_key AS
            SELECT e.af, e.size, p.pstate,
                   CASE WHEN r.re1 > e.pos THEN r.registry ELSE 'unknown' END AS registry,
                   CASE WHEN r.re1 > e.pos THEN r.cc ELSE 'ZZ' END AS cc
            FROM sp_el e JOIN pxs p ON p.af = e.af AND p.s = e.os AND p.plen = e.oplen
            ASOF LEFT JOIN rec r ON r.af = e.af AND e.pos >= r.rsh""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE m_space AS
            SELECT level, key, af,
                   CAST(sum(size) AS DOUBLE) / cd_unit(af) AS space,
                   CAST(coalesce(sum(size) FILTER (WHERE pstate = 'valid'), 0) AS DOUBLE) / cd_unit(af) AS space_valid,
                   CAST(coalesce(sum(size) FILTER (WHERE pstate = 'invalid'), 0) AS DOUBLE) / cd_unit(af) AS space_invalid,
                   CAST(coalesce(sum(size) FILTER (WHERE pstate = 'notfound'), 0) AS DOUBLE) / cd_unit(af) AS space_nf
            FROM ({_levels('sp_key', ('af', 'size', 'pstate'))}) GROUP BY level, key, af""")
        # 10. per-AS rows (cc level): routes by start record; space most-specific within (origin, cc)
        con.execute("""CREATE OR REPLACE TEMP TABLE as_r AS
            SELECT cc, af, origin, count(*) AS routes,
                   count(*) FILTER (WHERE state = 'valid') AS routes_valid,
                   count(*) FILTER (WHERE state = 'invalid_as') AS routes_inv_as,
                   count(*) FILTER (WHERE state = 'invalid_len') AS routes_inv_len,
                   count(*) FILTER (WHERE state = 'notfound') AS routes_nf
            FROM pk GROUP BY cc, af, origin""")
        con.execute("""CREATE OR REPLACE TEMP TABLE pas AS
            SELECT af, cc, origin, plen, CAST(pstart AS HUGEINT) AS s,
                   CAST(pstart AS HUGEINT) + (CAST(1 AS HUGEINT) << (cd_w(af) - plen)) AS e1, state
            FROM pk""")
        owner_intervals(con, 'pas', ['af', 'cc', 'origin'], 'as_el')
        con.execute("""CREATE OR REPLACE TEMP TABLE as_s AS
            SELECT e.af, e.cc, e.origin,
                   CAST(sum(e.size) AS DOUBLE) / cd_unit(e.af) AS space,
                   CAST(coalesce(sum(e.size) FILTER (WHERE p.state = 'valid'), 0) AS DOUBLE) / cd_unit(e.af) AS space_valid,
                   CAST(coalesce(sum(e.size) FILTER (WHERE p.state IN ('invalid_as', 'invalid_len')), 0) AS DOUBLE)
                     / cd_unit(e.af) AS space_invalid,
                   CAST(coalesce(sum(e.size) FILTER (WHERE p.state = 'notfound'), 0) AS DOUBLE) / cd_unit(e.af) AS space_nf
            FROM as_el e JOIN pas p ON p.af = e.af AND p.cc = e.cc AND p.origin = e.origin
                                   AND p.s = e.os AND p.plen = e.oplen
            GROUP BY e.af, e.cc, e.origin""")
        tables += ['m_routes', 'm_space']
        info.update(pairs_src=meta.get('src'), pairs_generated_at=meta.get('generated_at'),
                    pairs_rows=pairs_rows, visible_pairs=visible, short_pairs=short,
                    collectors={str(k): v for k, v in coll.items()},
                    threshold={str(k): v for k, v in thr.items()})

    # ── geo rows ─────────────────────────────────────────────────────────────
    keys = ' UNION '.join(f'SELECT level, key, af FROM {t}' for t in tables)

    def col(name, table, typ):
        if table in ('m_routes', 'm_space') and not bgp:
            return f'CAST(NULL AS {typ}) AS {name}'
        return f'CAST(coalesce({table}.{name}, 0) AS {typ}) AS {name}'
    src_of = {m: ('m_routes' if m.startswith('routes') or m.startswith('ases') else 'm_space')
              for m in BGP_METRICS}
    src_of.update({'deleg': 'm_reg', 'deleg_cov': 'm_reg', 'deleg_cov_noas0': 'm_reg', 'vrps': 'm_vrps'})
    sel = ', '.join(col(m, src_of[m], 'DOUBLE' if m in DOUBLE_COLS else 'BIGINT')
                    for m in BGP_METRICS + REG_METRICS)
    joins = ' '.join(f'LEFT JOIN {t} USING (level, key, af)' for t in tables)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE geo AS
        SELECT DATE {ds} AS date, k.level, k.key, CAST(k.af AS UTINYINT) AS af, {sel}
        FROM ({keys}) k {joins}""")
    geo_rows = con.execute("SELECT count(*) FROM geo").fetchone()[0]
    as_rows = 0
    if bgp:
        con.execute(f"""CREATE OR REPLACE TEMP TABLE asx AS
            SELECT DATE {ds} AS date, r.cc, CAST(r.af AS UTINYINT) AS af, CAST(r.origin AS UBIGINT) AS asn,
                   CAST(r.routes AS BIGINT) AS routes, CAST(r.routes_valid AS BIGINT) AS routes_valid,
                   CAST(r.routes_inv_as AS BIGINT) AS routes_inv_as,
                   CAST(r.routes_inv_len AS BIGINT) AS routes_inv_len, CAST(r.routes_nf AS BIGINT) AS routes_nf,
                   CAST(coalesce(s.space, 0) AS DOUBLE) AS space,
                   CAST(coalesce(s.space_valid, 0) AS DOUBLE) AS space_valid,
                   CAST(coalesce(s.space_invalid, 0) AS DOUBLE) AS space_invalid,
                   CAST(coalesce(s.space_nf, 0) AS DOUBLE) AS space_nf
            FROM as_r r LEFT JOIN as_s s ON s.af = r.af AND s.cc = r.cc AND s.origin = r.origin""")
        as_rows = con.execute("SELECT count(*) FROM asx").fetchone()[0]
    _check_finite(con, 'geo', DOUBLE_COLS)
    if bgp:
        _check_finite(con, 'asx', {'space', 'space_valid', 'space_invalid', 'space_nf'})

    done = {'date': d.isoformat(), 'version': VERSION,
            'inputs': {'pairs_src': info['pairs_src'], 'vrp_rows': vrp_rows,
                       'deleg_snap': snap.isoformat() if snap else None,
                       'pairs_generated_at': info['pairs_generated_at'],
                       'pairs_rows': info['pairs_rows'], 'visible_pairs': info['visible_pairs'],
                       'short_pairs': info['short_pairs'],
                       'collectors': info['collectors'], 'threshold': info['threshold']},
            'rows': geo_rows, 'as_rows': as_rows}
    if write:
        _write(con, d, root, bgp, done, t0)
    done['seconds'] = round(time.time() - t0, 2)
    return done


def _check_finite(con, table, cols):
    cond = ' OR '.join(f'isnan({c}) OR isinf({c})' for c in sorted(cols))
    n = con.execute(f"SELECT count(*) FROM {table} WHERE {cond}").fetchone()[0]
    if n:
        raise RuntimeError(f'{table}: {n} rows with NaN/Inf')


def _write(con, d, root, bgp, done, t0):
    """Write geo/as/done into a temp dir next to daily/YYYY/D, then swap it in."""
    final = common.daily_dir(d, root)
    parent = os.path.dirname(final)
    os.makedirs(parent, exist_ok=True)
    tmp = os.path.join(parent, f'.tmp-{d.isoformat()}-{os.getpid()}')
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    common.copy_to_parquet(con, f"SELECT {', '.join(GEO_COLUMNS)} FROM geo", os.path.join(tmp, 'geo.parquet'),
                           order_by='level, key, af')
    if bgp:
        common.copy_to_parquet(con, f"SELECT {', '.join(AS_COLUMNS)} FROM asx", os.path.join(tmp, 'as.parquet'),
                               order_by='cc, af, asn')
    done['seconds'] = round(time.time() - t0, 2)
    done['peak_rss_mb'] = common.peak_rss_mb()
    done['generated_at'] = common.utc_now_iso()
    common.atomic_write_json(os.path.join(tmp, 'done.json'), done)
    old = None
    if os.path.exists(final):
        old = os.path.join(parent, f'.old-{d.isoformat()}-{os.getpid()}')
        os.rename(final, old)
    os.rename(tmp, final)
    if old:
        shutil.rmtree(old, ignore_errors=True)


def compute_date(d, root, threads=16, memory_limit='20GB', tmp=None):
    con = common.connect(threads=threads, memory_limit=memory_limit, tmp=tmp, root=root)
    try:
        register_macros(con)
        done = compute(con, d, root)
    finally:
        con.close()
    inp = done['inputs']
    log(f"compute_day: {done['date']} rows={done['rows']} as_rows={done['as_rows']} "
        f"pairs={inp['pairs_src']}:{inp['visible_pairs']}/{inp['pairs_rows']} vrps={inp['vrp_rows']} "
        f"deleg={inp['deleg_snap']} {done['seconds']}s rss={common.peak_rss_mb()}MB"
        + (f" short_pairs={inp['short_pairs']} (left out)" if inp.get('short_pairs') else ''))
    return done


def is_missing(d, root):
    """No done.json, an older version, computed without pairs that exist now, or computed from a pairs
    file that has been rewritten since (snapshot regenerated upstream, backfill delivered again): the
    pairs json generated_at recorded in done.json no longer matches. A done.json written before that
    key existed is not judged by it."""
    done = common.read_json(common.done_path(d, root))
    if not done or done.get('version') != VERSION:
        return True
    if d >= common.BGP_FROM and os.path.exists(common.pairs_path(d, root)):
        meta = common.read_json(common.pairs_meta_path(d, root))
        inp = done.get('inputs') or {}
        if meta is not None and not inp.get('pairs_src'):
            return True
        if meta is not None and 'pairs_generated_at' in inp and inp['pairs_generated_at'] != meta.get('generated_at'):
            return True
    return False


def auto_dates(root, trailing=TRAILING):
    """Trailing grid dates up to the latest pairs day, plus every missing grid date before it."""
    days = common.pairs_days(root)
    if not days:
        raise RuntimeError(f'no pairs files under {root}/pairs')
    end = days[-1]
    g = common.grid(end)
    recent = set(g[-trailing:]) if trailing > 0 else set()
    return sorted(recent | {d for d in g if is_missing(d, root)})


def _run_parallel(dates, a):
    """--jobs N: one subprocess per date (crash isolation, memory freed per date)."""
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    failed = []

    def one(d):
        cmd = [sys.executable, '-m', 'crc.compute_day', '--root', a.root, '--date', d.isoformat(),
               '--threads', str(a.threads), '--memory-limit', a.memory_limit]
        if a.tmp:
            cmd += ['--tmp', a.tmp]
        return d, subprocess.run(cmd, cwd=here).returncode

    with concurrent.futures.ThreadPoolExecutor(max_workers=a.jobs) as ex:
        for d, rc in ex.map(one, dates):
            if rc != 0:
                failed.append(d)
    return failed


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--date')
    g.add_argument('--from', dest='dfrom')
    g.add_argument('--auto', action='store_true')
    ap.add_argument('--to', dest='dto')
    ap.add_argument('--trailing', type=int, default=TRAILING)
    ap.add_argument('--jobs', type=int, default=1)
    ap.add_argument('--only-missing', action='store_true')
    ap.add_argument('--threads', type=int)
    ap.add_argument('--memory-limit')
    ap.add_argument('--tmp')
    a = ap.parse_args(argv)
    # --jobs N children run with cwd=pipelines/country: pass them absolute paths (also expands
    # '--root=~/country-rpki', which the shell leaves alone)
    a.root = os.path.abspath(os.path.expanduser(a.root))
    if a.tmp:
        a.tmp = os.path.abspath(os.path.expanduser(a.tmp))
    a.jobs = max(1, a.jobs)
    a.threads = a.threads or max(1, 16 // a.jobs)
    a.memory_limit = a.memory_limit or f'{max(4, 40 // a.jobs)}GB'
    if a.date:
        dates = [common.to_date(a.date)]
        if not common.is_grid_date(dates[0]):
            log(f'compute_day: note: {dates[0]} is not a grid date (assemble ignores it)')
    elif a.dfrom:
        if not a.dto:
            ap.error('--from needs --to')
        dates = common.grid(a.dto, a.dfrom)
    else:
        dates = auto_dates(a.root, a.trailing)
    if a.only_missing:
        dates = [d for d in dates if is_missing(d, a.root)]
    if not dates:
        log('compute_day: nothing to do')
        return 0
    many = len(dates) > 1     # a single date (each --jobs child) logs just its result line
    if many:
        log(f'compute_day: {len(dates)} date(s) {dates[0]}..{dates[-1]} jobs={a.jobs} threads={a.threads} '
            f'memory_limit={a.memory_limit}')
    t0 = time.time()
    failed = []
    if a.jobs > 1 and len(dates) > 1:
        failed = _run_parallel(dates, a)
    else:
        for d in dates:
            try:
                compute_date(d, a.root, a.threads, a.memory_limit, a.tmp)
            except Exception as e:
                log(f'compute_day: ERROR {d}: {type(e).__name__}: {e}')
                failed.append(d)
    if many or failed:
        log(f'compute_day: {len(dates) - len(failed)}/{len(dates)} ok in {time.time() - t0:.0f}s')
    if failed:
        log(f'compute_day: failed: {", ".join(map(str, failed))}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
