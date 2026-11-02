"""Union of the VRP interval sources -> inputs/vrp_intervals.parquet.

Sources (all required unless --allow-missing):
  inputs/warehouse_intervals.parquet  src 'warehouse'     (crc.vrp_export)
  inputs/vrp_subtal.parquet           src 'apnic-subtal'  (crc.vrp_subtal)
  inputs/archive_intervals.parquet    src 'ripe-archive'  (crc.ripe_archive)
Rows are kept as they are (UNION ALL); rows of different src (or of different RIRs of one src)
may overlap, so consumers take SELECT DISTINCT afi, prefix, max_length, asn WHERE D BETWEEN
first_day AND last_day.

Checks: every prefix is parsed with ipaddress and rewritten to canonical CIDR when it is not
(host bits cleared, IPv6 lower-case compressed); afi is recomputed from the prefix; rows with an
unparseable prefix or a max_length outside [prefix length, 32|128] are dropped and counted.

Output columns exactly: afi UTINYINT, prefix VARCHAR, max_length UTINYINT, asn UBIGINT,
first_day DATE, last_day DATE, src VARCHAR, bridged_days INTEGER. Also inputs/vrp_intervals.json.
Prints per-src row counts and the VRP count of the latest day covered by every source.
CLI: python -m crc.vrp_merge [--root R]
"""
import argparse
import datetime as dt
import ipaddress
import os
import sys
import time

from crc import common

SOURCES = (('warehouse', 'warehouse_intervals.parquet'),
           ('apnic-subtal', 'vrp_subtal.parquet'),
           ('ripe-archive', 'archive_intervals.parquet'))
COLUMNS = ('afi', 'prefix', 'max_length', 'asn', 'first_day', 'last_day', 'src', 'bridged_days')


def canonical_map(prefixes):
    """{prefix: canonical or None (unparseable)} for the prefixes that are not canonical."""
    out = {}
    for p in prefixes:
        try:
            c = str(ipaddress.ip_network(p, strict=False))
        except ValueError:
            out[p] = None
            continue
        if c != p:
            out[p] = c
    return out


def merge(con, paths):
    """paths: [(src, parquet path)]. Creates table vrp_u with the VRP-interval columns (see the module
    docstring). Returns stats."""
    parts = [f"""SELECT prefix, max_length, asn, first_day, last_day, {common.lit(src)} AS src,
                        CAST(coalesce(bridged_days, 0) AS INTEGER) AS bridged_days
                 FROM read_parquet({common.lit(p)})""" for src, p in paths]
    con.execute("CREATE OR REPLACE TABLE vrp_in AS " + " UNION ALL ".join(parts))
    prefixes = [r[0] for r in con.execute("SELECT DISTINCT prefix FROM vrp_in").fetchall()]
    fix = canonical_map(prefixes)
    con.execute("CREATE OR REPLACE TABLE pfx_fix (prefix VARCHAR, canon VARCHAR)")
    if fix:
        con.execute("INSERT INTO pfx_fix SELECT unnest($1::VARCHAR[]), unnest($2::VARCHAR[])",
                    [list(fix), list(fix.values())])
    con.execute("""
        CREATE OR REPLACE TABLE vrp_c AS
        SELECT CASE WHEN f.prefix IS NULL THEN v.prefix ELSE f.canon END AS prefix,
               v.max_length, v.asn, v.first_day, v.last_day, v.src, v.bridged_days,
               f.prefix IS NOT NULL AND f.canon IS NULL AS unparseable
        FROM vrp_in v LEFT JOIN pfx_fix f USING (prefix)""")
    con.execute("""
        CREATE OR REPLACE TABLE vrp_u AS
        SELECT CAST(CASE WHEN contains(prefix, ':') THEN 6 ELSE 4 END AS UTINYINT) AS afi,
               prefix, CAST(max_length AS UTINYINT) AS max_length, CAST(asn AS UBIGINT) AS asn,
               first_day, last_day, src, bridged_days
        FROM vrp_c
        WHERE NOT unparseable AND first_day <= last_day
          AND max_length >= CAST(split_part(prefix, '/', 2) AS INTEGER)
          AND max_length <= CASE WHEN contains(prefix, ':') THEN 128 ELSE 32 END""")
    n_in = con.execute("SELECT count(*) FROM vrp_in").fetchone()[0]
    n_out = con.execute("SELECT count(*) FROM vrp_u").fetchone()[0]
    return {'rows_in': n_in, 'rows': n_out, 'dropped': n_in - n_out,
            'prefixes_rewritten': sum(1 for v in fix.values() if v is not None),
            'prefixes_unparseable': sum(1 for v in fix.values() if v is None),
            'distinct_prefixes': len(prefixes)}


def vrps_on(con, table, day, src=None):
    w = f" AND src = {common.lit(src)}" if src else ''
    return con.execute(f"SELECT count(*) FROM (SELECT DISTINCT afi, prefix, max_length, asn FROM {table} "
                       f"WHERE DATE '{day}' BETWEEN first_day AND last_day{w})").fetchone()[0]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--allow-missing', action='store_true', help='merge whatever sources exist')
    ap.add_argument('--threads', type=int, default=16)
    ap.add_argument('--memory-limit', default='48GB')
    a = ap.parse_args(argv)
    t0 = time.time()
    inp = common.inputs_dir(a.root)
    paths, missing = [], []
    for src, name in SOURCES:
        p = os.path.join(inp, name)
        (paths if os.path.exists(p) else missing).append((src, p))
    if missing and not a.allow_missing:
        raise SystemExit(f'vrp_merge: missing inputs {[p for _, p in missing]} (use --allow-missing)')
    if not paths:
        raise SystemExit('vrp_merge: no inputs')
    con = common.connect(threads=min(a.threads, 16), memory_limit=a.memory_limit, root=a.root)
    stats = merge(con, paths)
    out = os.path.join(inp, 'vrp_intervals.parquet')
    common.copy_to_parquet(con, 'SELECT ' + ', '.join(COLUMNS) + ' FROM vrp_u', out,
                           order_by='afi, prefix, max_length, asn, first_day, src')
    per_src = {s: {'rows': n, 'first_day': f, 'last_day': l} for s, n, f, l in con.execute(
        "SELECT src, count(*), min(first_day), max(last_day) FROM vrp_u GROUP BY src ORDER BY src").fetchall()}
    overall_last = max(v['last_day'] for v in per_src.values())
    live = [v['last_day'] for s, v in per_src.items() if s != 'apnic-subtal']   # sub-TAL is historical
    latest = min(live) if live else overall_last
    for s in per_src:
        per_src[s]['vrps_latest'] = vrps_on(con, 'vrp_u', latest, s)
    union_latest = vrps_on(con, 'vrp_u', latest)
    meta = {'generated_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            **stats, 'sources': per_src, 'missing_sources': [s for s, _ in missing],
            'latest_day': latest, 'vrps_latest_day': union_latest,
            'last_day': overall_last, 'vrps_last_day': vrps_on(con, 'vrp_u', overall_last),
            'seconds': round(time.time() - t0, 1), 'bytes': os.path.getsize(out)}
    common.atomic_write_json(os.path.join(inp, 'vrp_intervals.json'), meta)
    for s, v in per_src.items():
        print(f"{s:13s} rows={v['rows']:>10,} days {v['first_day']}..{v['last_day']} "
              f"vrps({latest})={v['vrps_latest']:,}")
    print(f"union rows={stats['rows']:,} (dropped {stats['dropped']}, prefixes rewritten "
          f"{stats['prefixes_rewritten']}); VRPs on {latest} (latest day of every recent source) = {union_latest:,}; "
          f"on {overall_last} = {meta['vrps_last_day']:,}")
    common.log(f'vrp_merge: wrote {out} [{time.time() - t0:.1f}s]')
    return 0


if __name__ == '__main__':
    sys.exit(main())
