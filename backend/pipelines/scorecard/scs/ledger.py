"""Observation ledger: what our collection observed, per RIR and per snapshot / UTC day.

Inputs (no presence table is read, so the presence lock is not needed; no read starts in [00:45, 01:20)):
- warehouse-<rir>/snapshots/ts=*/metadata.json: one directory per snapshot, the RIPE NCC archive's daily
  00:00 trees in era 1 (repo_id ftp.ripe.net), one rpki-client cycle every 20 minutes in era 2 (repo_id
  rpki-client); file_count = objects in the snapshot. A cycle still being loaded has no metadata yet.
- warehouse-<rir>/indexes/membership/ts=*: the membership index the presence tables are built from;
  sid = position in the sorted list of these directories, as in rpki-parser
  analysis/build_presence_runs.py (object_presence_runs.start_sid / end_sid).
- table=object_validity: dataset_ts of every cycle with RP verdict rows (from 2026-06-20). A cycle with
  no rejected object writes no row, so zero verdict rows is not proof of a missing verdict.

cycles.parquet, one row per snapshot: rir, ts (TIMESTAMP, UTC), src ('archive' | 'rpki-client'), sid,
  file_count, has_metadata, has_membership, ref_short, ref_long, partial, good, verdict_rows.
  ref_short = median file_count of the RIR's snapshots of the same source within +-12 h (era 2) or +-7
  days (archive: one snapshot a day); ref_long = the median within +-7 days.
  partial = file_count < 0.9 x ref_short (a file_count drop: some repositories missing
  from the snapshot). good = the snapshot is an observation: metadata and membership index present, and
  file_count >= 0.5 x ref_long for an archive snapshot (the day's only observation: ARIN 2024-05-15..17
  held 4-39% of the usual files) or >= 0.25 x ref_long for one of our cycles (its neighbours fill in the
  rest). A partial snapshot still observes the other repositories: the archive lacked TWNIC's
  repository on most days of 2021-06..2022-02 (APNIC at 72-89%), and until 2026-06-20 many of our cycles
  missed a whole repository for hours (LACNIC at about 49% of its files in a quarter of its April
  cycles, ARIN 10-15 cycles a day at 58-65%); a near-empty one observes nothing useful (2026-10-10..10-12:
  ARIN 1,592 files instead of about 270,000, RIPE NCC 18-360 instead of 135,000), and a 12-hour median
  follows such a stretch, hence the 7-day reference.
ledger.parquet, one row per RIR and UTC day from 2011-12-26 (the week of the first archive snapshot) to
  the cut day: rir, day, era (1 before 2026-04-08, 2 from then), transition (2026-03-30..04-19),
  cycles (snapshot directories), good_cycles, drops (partial snapshots), empty (snapshots with metadata
  and index that are not observations), missing_index (no metadata or no membership index), expected (1 in era 1, 72 in
  era 2), longest_gap_h (longest stretch of the day without a good snapshot, in hours: 24 in era 1 by
  construction, about 0.33 in a normal era-2 day), verdict_cycles (cycles of the day with
  object_validity rows; NULL before 2026-06-20), low = good_cycles < 0.9 x expected, gap = no good
  snapshot (era 1) or longest_gap_h > 3 (era 2), complete = the day ended before the cut.
ledger.json: generated_at, cut, per-RIR counts (snapshots, low and gap days, first/last snapshot), timings.
The status rules (scs/status.py) read ledger.parquet; modules can read both through metrics.Ctx.

CLI (run from pipelines/scorecard):
  python -m scs.ledger [--root R] [--warehouse-base B] [--cut 'YYYY-MM-DD HH:MM:SS']
"""
import argparse
import csv
import json
import os
import re
import sys
import time

from . import common
from .common import lit, log

LEDGER_FROM = common.HISTORY_FROM
ERA2_EXPECTED = 72                 # rpki_cycle.sh runs every 20 minutes
LOW_SHARE = 0.9                    # a day (week) is low below 90% of the expected cycles
DROP_SHORT = 0.9                   # partial snapshot (a file_count drop) below 0.9 x ref_short
EMPTY_ARCHIVE = 0.5                # an archive day below 0.5 x ref_long is not an observation
EMPTY_CYCLE = 0.25                 # one of our cycles below 0.25 x ref_long observed nothing useful
SHORT_ERA2, SHORT_ERA1, LONG = '12 hours', '7 days', '7 days'
GAP_HOURS = 3.0                    # era 2: a day with a stretch of more than 3 h without a good cycle
ARCHIVE_REPO = 'ftp.ripe.net'
_TS_RE = re.compile(r'^ts=(\d{4}-\d{2}-\d{2})(?:[ _](\d{2})[-:]?(\d{2})[-:]?(\d{2}))?$')


def norm_ts(name):
    """'ts=2026-10-29 09-00-01' -> '2026-10-29 09:00:01' (None for other names), as build_presence_runs."""
    m = _TS_RE.match(name.replace('_', ' '))
    if not m:
        return None
    return f"{m.group(1)} {m.group(2) or '00'}:{m.group(3) or '00'}:{m.group(4) or '00'}"


def scan_snapshots(rir, base=common.WAREHOUSE_BASE):
    """[(ts, src, file_count, has_metadata, has_membership, sid)] for every snapshot directory of a RIR
    (and every membership directory without a snapshot directory, which keeps the sid sequence exact)."""
    wh = common.warehouse_dir(rir, base)
    mem = {}
    mdir = os.path.join(wh, 'indexes', 'membership')
    if os.path.isdir(mdir):
        names = sorted(t for t in (norm_ts(e.name) for e in os.scandir(mdir) if e.is_dir()) if t)
        mem = {t: i for i, t in enumerate(names)}
    out = {}
    sdir = os.path.join(wh, 'snapshots')
    for e in (os.scandir(sdir) if os.path.isdir(sdir) else ()):
        t = norm_ts(e.name)
        if not t or not e.is_dir():
            continue
        repo, n, meta = None, None, False
        try:
            with open(os.path.join(e.path, 'metadata.json')) as f:
                m = json.load(f)
            repo, n, meta = m.get('repo_id'), m.get('file_count'), True
        except (OSError, ValueError):
            pass
        if repo is None:   # no (readable) metadata yet: the source follows from the label
            repo = ARCHIVE_REPO if t.endswith('00:00:00') and t[:10] < common.ERA2_FROM.isoformat() else 'rpki-client'
        src = 'archive' if repo == ARCHIVE_REPO else 'rpki-client'
        out[t] = (t, src, n if isinstance(n, int) else None, meta, t in mem, mem.get(t))
    for t, sid in mem.items():
        if t not in out:
            src = 'archive' if t.endswith('00:00:00') and t[:10] < common.ERA2_FROM.isoformat() else 'rpki-client'
            out[t] = (t, src, None, False, True, sid)
    return [out[t] for t in sorted(out)]


def verdict_rows(con, rir, base=common.WAREHOUSE_BASE, min_age_s=common.MIN_PART_AGE_S):
    """{dataset_ts: rows} of table=object_validity (empty when the table does not exist)."""
    files = common.part_files(rir, 'object_validity', base, min_age_s)
    if not files:
        return {}
    common.guard_window(f'warehouse-{rir} object_validity')
    rows = common.run(con, f"SELECT dataset_ts, count(*) FROM {common.read_sql(files, ['dataset_ts'])} "
                           f"WHERE dataset_ts IS NOT NULL GROUP BY dataset_ts").fetchall()
    return dict(rows)


def build(con, snaps, verdicts, cut, tmp):
    """Tables `cycles` and `ledger` in con from scan_snapshots / verdict_rows output per RIR.
    snaps: {rir: [...]}, verdicts: {rir: {dataset_ts: rows}}, cut: naive UTC datetime, tmp: a scratch
    directory (the snapshot list goes through a CSV file: list parameters are slow for 100K rows)."""
    os.makedirs(tmp, exist_ok=True)
    path = os.path.join(tmp, f'.ledger-{os.getpid()}.csv')
    try:
        with open(path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(('rir', 'ts', 'src', 'file_count', 'has_metadata', 'has_membership', 'sid', 'verdict_rows'))
            for rir in sorted(snaps):
                v = verdicts.get(rir) or {}
                for t, src, n, meta, has_mem, sid in snaps[rir]:
                    w.writerow((rir, t, src, '' if n is None else n, meta, has_mem, '' if sid is None else sid,
                                v.get(t, 0)))
        con.execute(f"""CREATE OR REPLACE TEMP TABLE snaps_raw AS SELECT * FROM read_csv({lit(path)}, header = true,
            columns = {{'rir': 'VARCHAR', 'ts': 'TIMESTAMP', 'src': 'VARCHAR', 'file_count': 'BIGINT',
                       'has_metadata': 'BOOLEAN', 'has_membership': 'BOOLEAN', 'sid': 'INTEGER',
                       'verdict_rows': 'BIGINT'}})""")
    finally:
        if os.path.exists(path):
            os.unlink(path)
    cut_lit = f"TIMESTAMP '{cut.strftime('%Y-%m-%d %H:%M:%S')}'"
    # reference medians per RIR and source (snapshots with metadata only); era 1 is one snapshot a day
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cycles0 AS
        WITH m AS (SELECT * FROM snaps_raw WHERE ts <= {cut_lit}),
        r AS (SELECT *,
                median(file_count) OVER (PARTITION BY rir, src ORDER BY ts
                    RANGE BETWEEN INTERVAL {SHORT_ERA2} PRECEDING AND INTERVAL {SHORT_ERA2} FOLLOWING) AS ref_12h,
                median(file_count) OVER (PARTITION BY rir, src ORDER BY ts
                    RANGE BETWEEN INTERVAL {LONG} PRECEDING AND INTERVAL {LONG} FOLLOWING) AS ref_7d
              FROM m WHERE file_count IS NOT NULL)
        SELECT m.rir, m.ts, m.src, m.sid, m.file_count, m.has_metadata, m.has_membership,
               CAST(CASE WHEN m.src = 'archive' THEN r.ref_7d ELSE r.ref_12h END AS DOUBLE) AS ref_short,
               CAST(r.ref_7d AS DOUBLE) AS ref_long, m.verdict_rows
        FROM m LEFT JOIN r ON r.rir = m.rir AND r.ts = m.ts""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cycles AS
        SELECT rir, ts, src, sid, file_count, has_metadata, has_membership, ref_short, ref_long,
               coalesce(file_count < {DROP_SHORT} * ref_short, false) AS partial,
               coalesce(has_metadata AND has_membership AND file_count > 0
                        AND file_count >= CASE WHEN src = 'archive' THEN {EMPTY_ARCHIVE} ELSE {EMPTY_CYCLE} END
                                          * ref_long, false) AS good,
               verdict_rows
        FROM cycles0""")
    con.execute('DROP TABLE cycles0')
    cut_day = cut.date()
    era2 = common.date_lit(common.ERA2_FROM)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ledger AS
        WITH days AS (
          SELECT r.rir, CAST(d.range AS DATE) AS day
          FROM (SELECT DISTINCT rir FROM snaps_raw) r,
               range(TIMESTAMP '{LEDGER_FROM.isoformat()}', TIMESTAMP '{cut_day.isoformat()}' + INTERVAL 1 DAY,
                     INTERVAL 1 DAY) d),
        per_day AS (
          SELECT rir, CAST(ts AS DATE) AS day, count(*) AS cycles, count(*) FILTER (WHERE good) AS good_cycles,
                 count(*) FILTER (WHERE partial) AS drops,
                 count(*) FILTER (WHERE has_metadata AND has_membership AND NOT good) AS empty,
                 count(*) FILTER (WHERE NOT has_metadata OR NOT has_membership) AS missing_index
          FROM cycles GROUP BY ALL),
        g AS (
          SELECT rir, ts AS t0, coalesce(lead(ts) OVER (PARTITION BY rir ORDER BY ts), {cut_lit}) AS t1
          FROM cycles WHERE good),
        gaps AS (
          SELECT d.rir, d.day,
                 max(epoch(least(g.t1, CAST(d.day AS TIMESTAMP) + INTERVAL 1 DAY)
                           - greatest(g.t0, CAST(d.day AS TIMESTAMP)))) / 3600.0 AS longest_gap_h
          FROM days d JOIN g ON g.rir = d.rir AND g.t0 < CAST(d.day AS TIMESTAMP) + INTERVAL 1 DAY
                            AND g.t1 > CAST(d.day AS TIMESTAMP)
          GROUP BY ALL),
        v AS (SELECT rir, CAST(ts AS DATE) AS day, count(*) FILTER (WHERE verdict_rows > 0) AS verdict_cycles
              FROM cycles GROUP BY ALL)
        SELECT d.rir, d.day, CAST(CASE WHEN d.day < {era2} THEN 1 ELSE 2 END AS UTINYINT) AS era,
               d.day BETWEEN {common.date_lit(common.TRANSITION_FROM)} AND {common.date_lit(common.TRANSITION_TO)} AS transition,
               CAST(coalesce(p.cycles, 0) AS INTEGER) AS cycles,
               CAST(coalesce(p.good_cycles, 0) AS INTEGER) AS good_cycles,
               CAST(coalesce(p.drops, 0) AS INTEGER) AS drops,
               CAST(coalesce(p.empty, 0) AS INTEGER) AS empty,
               CAST(coalesce(p.missing_index, 0) AS INTEGER) AS missing_index,
               CAST(CASE WHEN d.day < {era2} THEN 1 ELSE {ERA2_EXPECTED} END AS INTEGER) AS expected,
               round(least(coalesce(gp.longest_gap_h, 24.0), 24.0), 4) AS longest_gap_h,
               CAST(CASE WHEN d.day >= {common.date_lit(common.VERDICTS_FROM)} THEN coalesce(v.verdict_cycles, 0) END
                    AS INTEGER) AS verdict_cycles,
               coalesce(p.good_cycles, 0) < {LOW_SHARE} * CASE WHEN d.day < {era2} THEN 1 ELSE {ERA2_EXPECTED} END AS low,
               CASE WHEN d.day < {era2} THEN coalesce(p.good_cycles, 0) = 0
                    ELSE coalesce(gp.longest_gap_h, 24.0) > {GAP_HOURS} END AS gap,
               CAST(d.day AS TIMESTAMP) + INTERVAL 1 DAY <= {cut_lit} AS complete
        FROM days d LEFT JOIN per_day p USING (rir, day) LEFT JOIN gaps gp USING (rir, day)
             LEFT JOIN v USING (rir, day)""")


def summary(con):
    out = {}
    for r in con.execute("""SELECT rir, count(*), count(*) FILTER (WHERE low AND complete),
            count(*) FILTER (WHERE gap AND complete), sum(cycles), sum(good_cycles), min(day) FILTER (WHERE cycles > 0),
            max(day) FILTER (WHERE cycles > 0) FROM ledger GROUP BY rir ORDER BY rir""").fetchall():
        out[r[0]] = {'days': r[1], 'low_days': r[2], 'gap_days': r[3], 'snapshots': int(r[4]),
                     'good_snapshots': int(r[5]), 'first_day': str(r[6]), 'last_day': str(r[7])}
    return out


def write(con, root, cut, seconds, rirs):
    d = common.ledger_dir(root)
    common.copy_to_parquet(con, 'SELECT * FROM cycles', os.path.join(d, 'cycles.parquet'), order_by='rir, ts')
    common.copy_to_parquet(con, 'SELECT * FROM ledger', os.path.join(d, 'ledger.parquet'), order_by='rir, day')
    meta = {'generated_at': common.utc_now_iso(), 'cut': common.iso(cut), 'from': LEDGER_FROM.isoformat(),
            'rirs': list(rirs), 'by_rir': summary(con), 'seconds': seconds,
            'rules': {'expected_era2': ERA2_EXPECTED, 'low_share': LOW_SHARE, 'drop_short': DROP_SHORT,
                      'empty_archive': EMPTY_ARCHIVE, 'empty_cycle': EMPTY_CYCLE, 'short_window_era2': SHORT_ERA2,
                      'short_window_era1': SHORT_ERA1,
                      'long_window': LONG, 'gap_hours_era2': GAP_HOURS}}
    common.atomic_write_json(os.path.join(d, 'ledger.json'), meta)
    return meta


def run_ledger(root, base=common.WAREHOUSE_BASE, cut=None, rirs=common.REGISTRIES, min_age_s=common.MIN_PART_AGE_S,
               threads=common.DEFAULT_THREADS, memory_limit='2GB'):
    """Scan, build and write the ledger; returns ledger.json's content."""
    t0 = time.time()
    cut = common.parse_ts(cut) if cut else common.utc_now().replace(microsecond=0)
    con = common.connect(threads=threads, memory_limit=memory_limit, root=root)
    try:
        snaps, verdicts, seconds = {}, {}, {}
        for rir in rirs:
            common.guard_window(f'warehouse-{rir} snapshots')
            t = time.time()
            snaps[rir] = scan_snapshots(rir, base)
            verdicts[rir] = verdict_rows(con, rir, base, min_age_s)
            seconds[rir] = round(time.time() - t, 1)
            log(f'ledger: {rir} {len(snaps[rir]):,} snapshots, {len(verdicts[rir]):,} verdict cycles '
                f'[{seconds[rir]}s]')
        build(con, snaps, verdicts, cut, common.tmp_dir(root))
        seconds['total'] = round(time.time() - t0, 1)
        meta = write(con, root, cut, seconds, rirs)
    finally:
        con.close()
    for rir, s in meta['by_rir'].items():
        log(f"ledger: {rir} days {s['days']:,} low {s['low_days']:,} gap {s['gap_days']:,} snapshots "
            f"{s['snapshots']:,} ({s['good_snapshots']:,} good) {s['first_day']}..{s['last_day']}")
    log(f"ledger: wrote {common.ledger_dir(root)} cut={meta['cut']} [{seconds['total']}s, peak RSS "
        f"{common.peak_rss_mb()} MB]")
    return meta


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--warehouse-base', default=common.WAREHOUSE_BASE, help='directory holding warehouse-<rir>/')
    ap.add_argument('--cut', help="ledger end, UTC 'YYYY-MM-DD HH:MM:SS' (default: now)")
    a = ap.parse_args(argv)
    if not common.may_start():
        raise SystemExit('ledger: refusing to start between 00:45 and 01:20 UTC (presence build window)')
    common.renice(10)
    run_ledger(os.path.abspath(a.root), a.warehouse_base, a.cut)
    return 0


if __name__ == '__main__':
    sys.exit(main())
