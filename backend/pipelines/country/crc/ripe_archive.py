"""RIPE NCC RPKI repository archive -> inputs/archive_intervals.parquet  (src 'ripe-archive').

Source: https://ftp.ripe.net/rpki/<ta>.tal/YYYY/MM/DD/roas.csv.xz for the 5 TAs (written by RIPE
around 05:00-07:30 UTC of day DD). Columns URI,ASN,IP Prefix,Max Length,Not Before,Not After;
ASN like 'AS123'; a blank Max Length means the prefix length.

Steps (idempotent):
1. fetch: every missing <ta> file for every day in [--from, --to] into
   inputs/ripe_archive/YYYY/MM/DD/<ta>.roas.csv.xz (at most 4 parallel downloads, xz verified
   before it is kept; 404 = day not archived, skipped). Default --to = the latest day for which
   all 5 TAs are available.
2. parse: one compact per-day set inputs/ripe_archive/YYYY/MM/DD/vrps.parquet
   (d, ta, afi, prefix, max_length, asn); rebuilt only when a raw file of that day is newer.
3. intervals: per (ta, prefix, max_length, asn) runs of consecutive days, then gaps of <= 3 days
   are bridged (no bad-day tier for the archive) -> inputs/archive_intervals.parquet with columns
   afi, prefix, max_length, asn UBIGINT, first_day, last_day, src='ripe-archive', bridged_days,
   rir (the TA), plus inputs/archive_intervals.json.

CLI: python -m crc.ripe_archive [--root R] [--from 2026-04-08] [--to YYYY-MM-DD]
"""
import argparse
import concurrent.futures
import datetime as dt
import lzma
import os
import shutil
import sys
import tempfile
import time

from crc import common, fetch, intervals

TAS = ('afrinic', 'apnic', 'arin', 'lacnic', 'ripencc')
BASE_URL = 'https://ftp.ripe.net/rpki'
DEFAULT_FROM = dt.date(2026, 4, 8)
HEADER = ['URI', 'ASN', 'IP Prefix', 'Max Length', 'Not Before', 'Not After']


def day_url(day, ta):
    return f'{BASE_URL}/{ta}.tal/{day:%Y/%m/%d}/roas.csv.xz'


def archive_dir(root):
    return os.path.join(common.inputs_dir(root), 'ripe_archive')


def day_dir(root, day):
    return os.path.join(archive_dir(root), f'{day:%Y}', f'{day:%m}', f'{day:%d}')


def raw_path(root, day, ta):
    return os.path.join(day_dir(root, day), f'{ta}.roas.csv.xz')


def day_parquet(root, day):
    return os.path.join(day_dir(root, day), 'vrps.parquet')


def days_between(a, b):
    out, d = [], a
    while d <= b:
        out.append(d)
        d += dt.timedelta(days=1)
    return out


def latest_available(today=None, max_back=14):
    """Latest day <= today (UTC) for which all 5 TAs have roas.csv.xz (HEAD requests)."""
    today = today or dt.datetime.now(dt.timezone.utc).date()
    for k in range(max_back + 1):
        d = today - dt.timedelta(days=k)
        if all(fetch.exists(day_url(d, ta)) for ta in TAS):
            return d
    raise SystemExit(f'ripe_archive: no day with all 5 TAs in the last {max_back} days')


def fetch_days(root, days):
    jobs = [{'url': day_url(d, ta), 'dest': raw_path(root, d, ta)}
            for d in days for ta in TAS if not os.path.exists(raw_path(root, d, ta))]
    common.log(f'ripe_archive: {len(days)} days, {len(jobs)} files to fetch')
    res = fetch.download_many(jobs, label='ripe_archive') if jobs else []
    summ = fetch.summarize(res)
    missing = sorted(f"{j['url']}" for j, st, _, _ in res if st == 'missing')
    errors = [(j['url'], e) for j, st, _, e in res if st == 'error']
    return summ, missing, errors


def _decompress(src, dst):
    with lzma.open(src, 'rb') as f, open(dst, 'wb') as g:
        shutil.copyfileobj(f, g, 1 << 20)
    with open(dst, 'r', encoding='utf-8', errors='replace') as g:
        first = g.readline().strip().lstrip('﻿')
    return [c.strip().strip('"') for c in first.split(',')] if first else None


def parse_csv_sql(path, ta):
    """SQL for one roas.csv: ta, afi, prefix, max_length (UTINYINT), asn (UBIGINT), distinct."""
    src = (f"read_csv({common.lit(path)}, header=true, all_varchar=true, delim=',', quote='\"', "
           f"escape='\"', auto_detect=false, columns={{'URI': 'VARCHAR', 'ASN': 'VARCHAR', "
           f"'IP Prefix': 'VARCHAR', 'Max Length': 'VARCHAR', 'Not Before': 'VARCHAR', 'Not After': 'VARCHAR'}})")
    return f"""
SELECT DISTINCT {common.lit(ta)} AS ta,
       CAST(CASE WHEN contains(p, ':') THEN 6 ELSE 4 END AS UTINYINT) AS afi,
       p AS prefix,
       CAST(coalesce(nullif(ml, ''), split_part(p, '/', 2)) AS UTINYINT) AS max_length,
       CAST(regexp_replace(a, '^[Aa][Ss]', '') AS UBIGINT) AS asn
FROM (SELECT lower(trim("IP Prefix")) AS p, trim(coalesce("Max Length", '')) AS ml, trim("ASN") AS a FROM {src})
WHERE p <> ''"""


def convert_day(con, root, day, tmp_root):
    """Parse the day's raw files into vrps.parquet. Returns per-TA row counts."""
    files = [(ta, raw_path(root, day, ta)) for ta in TAS if os.path.exists(raw_path(root, day, ta))]
    tmpd = tempfile.mkdtemp(prefix=f'ripe-{day.isoformat()}-', dir=tmp_root)
    try:
        csvs = [(ta, src, os.path.join(tmpd, f'{ta}.csv')) for ta, src in files]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(csvs) or 1) as ex:
            headers = list(ex.map(lambda x: _decompress(x[1], x[2]), csvs))
        parts = []
        for (ta, src, csv), hdr in zip(csvs, headers):
            if hdr is None:
                common.log(f'ripe_archive: {src} is empty; skipped')
                continue
            if hdr != HEADER:
                raise SystemExit(f'ripe_archive: unexpected header in {src}: {hdr}')
            parts.append(parse_csv_sql(csv, ta))
        if not parts:
            return {}
        sql = (f"SELECT DATE '{day.isoformat()}' AS d, ta, afi, prefix, max_length, asn FROM ("
               + " UNION ALL ".join(parts) + ")")
        out = day_parquet(root, day)
        common.copy_to_parquet(con, sql, out, order_by='ta, afi, prefix, max_length, asn')
        return dict(con.execute(f"SELECT ta, count(*) FROM read_parquet({common.lit(out)}) GROUP BY ta").fetchall())
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)


def stale(root, day):
    pq = day_parquet(root, day)
    raws = [raw_path(root, day, ta) for ta in TAS if os.path.exists(raw_path(root, day, ta))]
    if not raws:
        return False
    if not os.path.exists(pq):
        return True
    t = os.path.getmtime(pq)
    return any(os.path.getmtime(r) > t for r in raws)


def raw_intervals_sql(days_src):
    """Per (ta, VRP): runs of consecutive calendar days in a table of per-day sets (d, ta, afi, prefix, max_length, asn)."""
    return f"""
SELECT ta, prefix, max_length, asn, any_value(afi) AS afi, min(d) AS first_day, max(d) AS last_day
FROM (SELECT *, d - CAST(row_number() OVER (PARTITION BY ta, prefix, max_length, asn ORDER BY d) AS INTEGER) AS g_
      FROM {days_src})
GROUP BY ta, prefix, max_length, asn, g_"""


def output_select(bridged_src):
    return f"""SELECT CAST(afi AS UTINYINT) AS afi, prefix, CAST(max_length AS UTINYINT) AS max_length,
       CAST(asn AS UBIGINT) AS asn, first_day, last_day, 'ripe-archive' AS src, bridged_days, ta AS rir
FROM {bridged_src}"""


def build_intervals(con, root, d_from, d_to):
    files = []
    for d in days_between(d_from, d_to):
        if os.path.exists(day_parquet(root, d)):
            files.append(day_parquet(root, d))
    if not files:
        raise SystemExit('ripe_archive: no parsed days to build intervals from')
    flist = '[' + ', '.join(common.lit(f) for f in files) + ']'
    t = time.time()
    # one TA at a time keeps the window over ~1/3 of the day-rows (peak memory)
    con.execute("CREATE OR REPLACE TABLE ra_raw (ta VARCHAR, prefix VARCHAR, max_length UTINYINT, asn UBIGINT, "
                "afi UTINYINT, first_day DATE, last_day DATE)")
    for ta in TAS:
        src = f"(SELECT * FROM read_parquet({flist}) WHERE ta = {common.lit(ta)})"
        con.execute(f"INSERT INTO ra_raw {raw_intervals_sql(src)}")
    con.execute("CREATE OR REPLACE TABLE ra_br AS " + intervals.bridge_sql('ra_raw', None, group_col='ta'))
    intervals.check_invariants(con, 'ra_br', group_col='ta', label='ripe_archive')
    out = os.path.join(common.inputs_dir(root), 'archive_intervals.parquet')
    common.copy_to_parquet(con, output_select('ra_br'), out, order_by='afi, prefix, max_length, asn, first_day, rir')
    stats = {
        'raw_rows': con.execute("SELECT count(*) FROM ra_raw").fetchone()[0],
        'rows': con.execute(f"SELECT count(*) FROM read_parquet({common.lit(out)})").fetchone()[0],
        'rows_by_ta': dict(con.execute(
            f"SELECT rir, count(*) FROM read_parquet({common.lit(out)}) GROUP BY rir ORDER BY rir").fetchall()),
        'gaps': dict(zip(('tier1', 'tier2', 'gaps'), con.execute(
            intervals.gap_stats_sql('ra_raw', None, group_col='ta')).fetchone())),
        'days_by_ta': dict(con.execute(
            f"SELECT ta, count(DISTINCT d) FROM read_parquet({flist}) GROUP BY ta ORDER BY ta").fetchall()),
        'seconds': round(time.time() - t, 1),
        'bytes': os.path.getsize(out),
    }
    last = con.execute(f"SELECT max(last_day) FROM read_parquet({common.lit(out)})").fetchone()[0]
    stats['first_day'] = con.execute(f"SELECT min(first_day) FROM read_parquet({common.lit(out)})").fetchone()[0]
    stats['last_day'] = last
    stats['vrps_last_day'] = con.execute(
        f"SELECT count(*) FROM (SELECT DISTINCT afi, prefix, max_length, asn FROM read_parquet({common.lit(out)}) "
        f"WHERE DATE '{last}' BETWEEN first_day AND last_day)").fetchone()[0]
    con.execute("DROP TABLE ra_raw")
    con.execute("DROP TABLE ra_br")
    return out, stats


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--from', dest='d_from', type=common.to_date, default=DEFAULT_FROM)
    ap.add_argument('--to', dest='d_to', type=common.to_date, default=None,
                    help='last day (default: latest day available for all 5 TAs)')
    ap.add_argument('--no-fetch', action='store_true', help='only (re)build from local files')
    ap.add_argument('--threads', type=int, default=16)
    ap.add_argument('--memory-limit', default='48GB')
    a = ap.parse_args(argv)
    t0 = time.time()
    if a.d_to is None:
        a.d_to = (max((d for d in days_between(a.d_from, dt.datetime.now(dt.timezone.utc).date())
                       if all(os.path.exists(raw_path(a.root, d, ta)) for ta in TAS)), default=a.d_from)
                  if a.no_fetch else latest_available())
    if a.d_to < a.d_from:
        raise SystemExit(f'ripe_archive: --to {a.d_to} is before --from {a.d_from}')
    days = days_between(a.d_from, a.d_to)
    summ, missing, errors = ({'ok': 0, 'present': 0, 'missing': 0, 'error': 0, 'bytes': 0}, [], [])
    if not a.no_fetch:
        summ, missing, errors = fetch_days(a.root, days)
        common.log(f'ripe_archive: fetch {summ}')
        for m in missing:
            common.log(f'ripe_archive: not archived (404): {m}')
    t_fetch = time.time() - t0
    con = common.connect(threads=min(a.threads, 16), memory_limit=a.memory_limit, root=a.root)
    t = time.time()
    tmp_root = common.tmp_dir(a.root)
    converted = 0
    for d in days:
        if stale(a.root, d):
            counts = convert_day(con, a.root, d, tmp_root)
            converted += 1
            if converted % 20 == 1:
                common.log(f'ripe_archive: parsed {d} {counts}')
    t_parse = time.time() - t
    common.log(f'ripe_archive: parsed {converted} day(s) [{t_parse:.1f}s]')
    out, stats = build_intervals(con, a.root, a.d_from, a.d_to)
    raw_bytes = sum(os.path.getsize(raw_path(a.root, d, ta)) for d in days for ta in TAS
                    if os.path.exists(raw_path(a.root, d, ta)))
    present_days = [d for d in days if all(os.path.exists(raw_path(a.root, d, ta)) for ta in TAS)]
    meta = {
        'generated_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'src': 'ripe-archive', 'from': a.d_from.isoformat(), 'to': a.d_to.isoformat(),
        'days_complete': len(present_days),
        'days_missing_any_ta': [d.isoformat() for d in days if d not in set(present_days)],
        'fetch': summ, 'not_archived': missing, 'fetch_errors': errors,
        'raw_bytes_on_disk': raw_bytes, 'parsed_days': converted,
        'seconds': {'fetch': round(t_fetch, 1), 'parse': round(t_parse, 1), 'intervals': stats.pop('seconds')},
        **stats,
    }
    common.atomic_write_json(os.path.join(common.inputs_dir(a.root), 'archive_intervals.json'), meta)
    common.log(f'ripe_archive: wrote {out} rows={stats["rows"]:,} days={len(present_days)} '
               f'last_day={stats["last_day"]} vrps_last_day={stats["vrps_last_day"]:,} [{time.time() - t0:.0f}s]')
    if errors:
        common.log(f'ripe_archive: {len(errors)} download error(s); rerun to retry')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
