"""One-off APNIC sub-TAL VRP day-intervals 2012-10-26..2018-04-04 -> inputs/vrp_subtal.parquet
(src 'apnic-subtal').

warehouse-apnic has no snapshots 2012-10-27..2018-04-04; that period lives in the sub-TAL
warehouses warehouse-apnic-{arin,iana,ripe} of the parser checkout (SUBTAL_BASE; the afrinic/lacnic
ones hold no ROAs). They have membership bitmaps (indexes/membership/ts=*/objects.roar) but no
presence runs, so:
1. per sub-TAL: (object_id, day) = membership ∩ ROA object ids, kept inside the EE window;
   runs over consecutive snapshot days (missing snapshot days are bridged like
   build_presence_runs); joined to roa_prefixes and merged per VRP (gaps-and-islands);
2. all three sub-TALs merged per VRP (overlapping or touching ranges);
3. two-tier bridging of crc.intervals with a calendar of the merged sub-TAL counts and the union
   of sub-TAL snapshot days (<= 3 days always, 4-31 days when every gap day is bad).

Needs pyroaring (imported lazily): run it with the parser's venv,
  <parser checkout>/.venv/bin/python -m crc.vrp_subtal [--root R] [--force]
The output never changes, so the step is skipped when the file exists (unless --force).
Output columns: afi, prefix, max_length, asn UBIGINT, first_day, last_day, src, bridged_days, rir.
"""
import argparse
import datetime as dt
import os
import sys
import time

from crc import common, intervals

# the parser next to this backend checkout, which holds the sub-TAL warehouses: an rpki-parser
# checkout, or parser/ in the rpki-health repository (the first of the two that exists)
_PARSER_DIRS = [os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), *['..'] * 4, d))
                for d in ('rpki-parser', 'parser')]
SUBTAL_BASE = next((p for p in _PARSER_DIRS if os.path.isdir(p)), _PARSER_DIRS[0])
SUBTALS = ('arin', 'iana', 'ripe')
GROUP = 'apnic-subtal'


def membership(wh, roa_ids_sql_rows):
    """(object_ids, days, snapshot_days) of ROA objects present per membership snapshot of one warehouse."""
    from pyroaring import BitMap  # only this one-off step needs it

    roa_ids = BitMap(x[0] for x in roa_ids_sql_rows)
    mroot = os.path.join(wh, 'indexes', 'membership')
    oid, day, snaps = [], [], []
    for n in sorted(os.listdir(mroot)):
        if not n.startswith('ts='):
            continue
        f = os.path.join(mroot, n, 'objects.roar')
        if not os.path.exists(f):
            continue
        with open(f, 'rb') as fh:
            bm = BitMap.deserialize(fh.read()) & roa_ids
        d = n[3:13]
        snaps.append(d)
        oid.extend(bm)
        day.extend([d] * len(bm))
    return oid, day, sorted(set(snaps))


def raw_subtal_sql(tables, pres, snapdays):
    """Raw VRP intervals of one sub-TAL warehouse from (object_id, d) presence and its snapshot days."""
    return f"""
WITH roa AS (SELECT object_id, any_value(asn) AS asn, CAST(min(ee_not_before) AS DATE) AS nb,
                    CAST(max(ee_not_after) AS DATE) AS na
             FROM read_parquet('{tables}/table=roa_objects/*.parquet') GROUP BY object_id),
ok AS (SELECT p.object_id, p.d FROM {pres} p JOIN roa USING (object_id) WHERE p.d BETWEEN roa.nb AND roa.na),
sid AS (SELECT d, row_number() OVER (ORDER BY d) AS k FROM (SELECT DISTINCT d FROM {snapdays})),
r AS (SELECT object_id, min(d) AS d_lo, max(d) AS d_hi
      FROM (SELECT ok.*, sid.k - row_number() OVER (PARTITION BY object_id ORDER BY ok.d) AS g
            FROM ok JOIN sid USING (d))
      GROUP BY object_id, g),
pfx AS (SELECT DISTINCT object_id, prefix, max_length, afi FROM read_parquet('{tables}/table=roa_prefixes/*.parquet')),
v AS (SELECT p.prefix, p.max_length, roa.asn, p.afi, r.d_lo, r.d_hi FROM r JOIN roa USING (object_id) JOIN pfx p USING (object_id))
""" + intervals.islands_sql('v', intervals.VRP_KEY, extra=('afi',))


def build(con, base=SUBTAL_BASE, subtals=SUBTALS, loader=membership):
    """Creates tables st_raw (merged across sub-TALs), st_cal, st_br. Returns per-sub-TAL stats."""
    stats = {}
    parts = []
    con.execute("CREATE OR REPLACE TABLE st_snapdays (grp VARCHAR, d DATE)")
    for s in subtals:
        t = time.time()
        wh = os.path.join(base, f'warehouse-apnic-{s}')
        tables = os.path.join(wh, 'tables')
        ids = con.execute(f"SELECT DISTINCT object_id FROM read_parquet('{tables}/table=roa_objects/*.parquet')").fetchall()
        oid, day, snaps = loader(wh, ids)
        con.execute(f"CREATE OR REPLACE TABLE pres_{s} AS SELECT unnest($1::INTEGER[]) AS object_id, "
                    f"CAST(unnest($2::VARCHAR[]) AS DATE) AS d", [oid, day])
        con.execute(f"CREATE OR REPLACE TABLE snap_{s} AS SELECT CAST(unnest($1::VARCHAR[]) AS DATE) AS d", [snaps])
        con.execute(f"INSERT INTO st_snapdays SELECT {common.lit(GROUP)}, d FROM snap_{s}")
        con.execute(f"CREATE OR REPLACE TABLE raw_{s} AS {raw_subtal_sql(tables, f'pres_{s}', f'snap_{s}')}")
        n = con.execute(f"SELECT count(*) FROM raw_{s}").fetchone()[0]
        stats[s] = {'object_days': len(oid), 'snapshot_days': len(snaps), 'raw_intervals': n,
                    'seconds': round(time.time() - t, 1)}
        common.log(f'vrp_subtal: apnic-{s}: ROA object-days {len(oid):,}, snapshot days {len(snaps):,}, '
                   f'raw VRP intervals {n:,} [{time.time() - t:.1f}s]')
        parts.append(f"SELECT prefix, max_length, asn, afi, first_day AS d_lo, last_day AS d_hi FROM raw_{s}")
    con.execute("CREATE OR REPLACE VIEW st_union AS " + " UNION ALL ".join(parts))
    con.execute(f"CREATE OR REPLACE TABLE st_raw AS SELECT {common.lit(GROUP)} AS grp, * FROM ("
                + intervals.islands_sql('st_union', intervals.VRP_KEY, extra=('afi',)) + ")")
    lo, hi = con.execute("SELECT min(first_day), max(last_day) FROM st_raw").fetchone()
    con.execute("CREATE OR REPLACE TABLE st_cal AS " + intervals.calendar_sql('st_raw', 'st_snapdays', lo, hi))
    con.execute("CREATE OR REPLACE TABLE st_br AS " + intervals.bridge_sql('st_raw', 'st_cal'))
    intervals.check_invariants(con, 'st_br', label='vrp_subtal')
    return stats


def output_select():
    return """SELECT CAST(afi AS UTINYINT) AS afi, prefix, CAST(max_length AS UTINYINT) AS max_length,
       CAST(asn AS UBIGINT) AS asn, first_day, last_day, 'apnic-subtal' AS src, bridged_days, 'apnic' AS rir
FROM st_br"""


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--force', action='store_true', help='rebuild even if the output exists')
    ap.add_argument('--subtal-base', default=SUBTAL_BASE)
    a = ap.parse_args(argv)
    out = os.path.join(common.inputs_dir(a.root), 'vrp_subtal.parquet')
    if os.path.exists(out) and not a.force:
        common.log(f'vrp_subtal: {out} exists; skipped (use --force to rebuild)')
        return 0
    t0 = time.time()
    con = common.connect(threads=16, memory_limit='48GB', root=a.root)
    stats = build(con, a.subtal_base)
    common.copy_to_parquet(con, output_select(), out, order_by='afi, prefix, max_length, asn, first_day')
    rows = con.execute(f"SELECT count(*) FROM read_parquet({common.lit(out)})").fetchone()[0]
    gaps = dict(zip(('tier1', 'tier2', 'gaps'), con.execute(intervals.gap_stats_sql('st_raw', 'st_cal')).fetchone()))
    yearly = {}
    for y in range(2013, 2019):
        d = f'{y}-01-01'
        yearly[d] = con.execute(f"SELECT count(*) FROM (SELECT DISTINCT prefix, max_length, asn FROM st_br "
                                f"WHERE DATE '{d}' BETWEEN first_day AND last_day)").fetchone()[0]
    meta = {
        'generated_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'src': 'apnic-subtal', 'rows': rows,
        'raw_rows_merged': con.execute("SELECT count(*) FROM st_raw").fetchone()[0],
        'distinct_vrps': con.execute("SELECT count(DISTINCT (prefix, max_length, asn)) FROM st_br").fetchone()[0],
        'first_day': con.execute("SELECT min(first_day) FROM st_br").fetchone()[0],
        'last_day': con.execute("SELECT max(last_day) FROM st_br").fetchone()[0],
        'gaps': gaps, 'subtals': stats, 'vrps_on': yearly,
        'seconds': round(time.time() - t0, 1), 'bytes': os.path.getsize(out),
    }
    common.atomic_write_json(os.path.join(common.inputs_dir(a.root), 'vrp_subtal.json'), meta)
    common.log(f'vrp_subtal: wrote {out} rows={rows:,} [{time.time() - t0:.1f}s]')
    return 0 if rows else 1


if __name__ == '__main__':
    sys.exit(main())
