"""Weekly run (or backfill) of the scorecard: as_of, plan, metric modules, then assemble and publish.

Steps (run_weekly.sh runs scs.ledger first):
1. as_of = the earliest of the five RIRs' latest object_presence_runs end, read under the shared presence
   lock (--as-of overrides it); W = the Monday of as_of's week (the incomplete week). A RIR whose presence
   ends more than 12 h before the newest one is a warning (its presence build failed?), and so is an as_of
   more than 36 h old or, in a weekly run without --as-of, one before this week's Monday (the week that
   just ended is not complete in the presence tables, so the newest week would repeat last week's). A
   ledger that ends before as_of stops the run (its later days would count as gaps, and the rows of the
   weeks closed now would be frozen as masked); --allow-stale-ledger runs anyway, with a warning. No
   state while a previous build exists is a warning (every metric's history is recomputed).
2. plan (assemble.plan_weeks): per metric, the trailing --weeks before W, open weeks, a full recompute
   after a def_version / host map / VERSION change, and --backfill FROM TO.
3. each metric module (scs/m_*.py) once per RIR and contiguous range of weeks (metrics.Ctx.call), in
   registry order; a module that is not implemented yet is skipped with a warning when the metrics were
   not named (an explicitly named one is an error).
4. assemble.build: status, merge with state/, gate, publish `current`, new state, release (weekly runs).
Prints one JSON line (assemble.summary) on stdout; logs go to stderr. Exit 0 = published (warnings
make run_weekly.sh report PARTIAL); any error = not published, `current` unchanged.

CLI (run from pipelines/scorecard):
  python -m scs.run [--root R] [--warehouse-base B] [--weeks 6] [--backfill FROM TO] [--metrics a,b]
                    [--threads 4] [--memory-limit-gb 6] [--no-release] [--allow-shrink] [--as-of TS]
                    [--allow-stale-ledger]
"""
import argparse
import datetime as dt
import os
import shutil
import sys
import time

from . import assemble, common, defs, hostmap, metrics
from .common import log

STALE_RIR_HOURS = 12
STALE_AS_OF_HOURS = 36


def presence_ends(root, base, rirs, lock_path, lock_wait_s, min_age_s=common.MIN_PART_AGE_S):
    """{rir: latest object_presence_runs end_ts (naive UTC) or None}, under the shared presence lock."""
    out = {}
    con = common.connect(threads=common.DEFAULT_THREADS, memory_limit='2GB', root=root)
    try:
        with common.presence_lock(lock_path, lock_wait_s):
            for rir in rirs:
                files = common.part_files(rir, 'object_presence_runs', base, min_age_s)
                if not files:
                    out[rir] = None
                    continue
                v = common.run(con, f"SELECT max(end_ts) FROM {common.read_sql(files, ['end_ts'])}").fetchone()[0]
                out[rir] = common.parse_ts(v) if v else None
    finally:
        con.close()
    return out


def as_of_from(ends, warnings):
    have = {r: v for r, v in ends.items() if v}
    if not have:
        raise SystemExit('scorecard: no presence runs in any warehouse; cannot set as_of')
    newest = max(have.values())
    for r in sorted(ends):
        v = ends[r]
        if v is None or newest - v > dt.timedelta(hours=STALE_RIR_HOURS):
            warnings.append(f'warehouse-{r} presence ends {common.iso(v)}, more than {STALE_RIR_HOURS} h before '
                            f'{common.iso(newest)} (presence build failed?); as_of follows it')
    return min(have.values())


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--warehouse-base', default=common.WAREHOUSE_BASE, help='directory holding warehouse-<rir>/')
    ap.add_argument('--weeks', type=int, default=6, help='trailing weeks recomputed before the current week')
    ap.add_argument('--backfill', nargs=2, metavar=('FROM', 'TO'), help='also compute the weeks of FROM..TO')
    ap.add_argument('--metrics', help='comma list of metric_ids (default: every non-stub metric)')
    ap.add_argument('--threads', type=int, default=common.DEFAULT_THREADS)
    ap.add_argument('--memory-limit-gb', type=float, default=common.DEFAULT_MEMORY_GB)
    ap.add_argument('--no-release', action='store_true', help='do not make releases/<YYYY-Www>/')
    ap.add_argument('--allow-shrink', action='store_true', help='publish even if a metric has fewer weeks')
    ap.add_argument('--as-of', help="input cut, UTC 'YYYY-MM-DD HH:MM:SS' (default: from the presence tables)")
    ap.add_argument('--allow-stale-ledger', action='store_true',
                    help='run even if the ledger ends before as_of (its later days count as gaps)')
    ap.add_argument('--lock', default=common.LOCK_PATH)
    ap.add_argument('--lock-wait', type=int, default=common.LOCK_WAIT_S, help='seconds to wait for the lock')
    a = ap.parse_args(argv)
    if a.weeks < 0:
        ap.error('--weeks must be >= 0')
    backfill = None
    if a.backfill:
        try:
            backfill = tuple(common.to_date(x) for x in a.backfill)
        except ValueError:
            ap.error('--backfill takes two dates YYYY-MM-DD')
        if backfill[1] < backfill[0]:
            ap.error('--backfill FROM is after TO')
    requested = [m.strip() for m in a.metrics.split(',') if m.strip()] if a.metrics else defs.default_metrics()
    unknown = [m for m in requested if m not in defs.METRICS]
    if unknown:
        ap.error(f'unknown metric(s): {", ".join(unknown)}')
    if not common.may_start():
        raise SystemExit('scorecard: refusing to start between 00:45 and 01:20 UTC (presence build window)')
    common.renice(10)
    defs.check()
    t0 = time.time()
    root = os.path.abspath(a.root)
    warnings = []
    missing = [m for m in requested if not metrics.module_available(defs.METRICS[m]['module'])]
    if missing and a.metrics:
        raise SystemExit(f'scorecard: metric module(s) not implemented: '
                         f'{", ".join(sorted({defs.METRICS[m]["module"] for m in missing}))}')
    if missing:
        warnings.append(f'metric modules not implemented yet, skipped: {", ".join(missing)}')
    todo = [m for m in requested if m not in missing]
    if not todo:
        raise SystemExit('scorecard: no metric to compute')
    hm = hostmap.load()
    ends = {}
    if a.as_of:
        as_of = common.parse_ts(a.as_of)
    else:
        ends = presence_ends(root, a.warehouse_base, common.REGISTRIES, a.lock, a.lock_wait)
        as_of = as_of_from(ends, warnings)
    now = common.utc_now()
    if now - as_of > dt.timedelta(hours=STALE_AS_OF_HOURS):
        warnings.append(f'as_of {common.iso(as_of)} is more than {STALE_AS_OF_HOURS} h old')
    this_monday = common.monday(now)
    if not a.as_of and not backfill and common.monday(as_of) < this_monday:
        warnings.append(f"as_of {common.iso(as_of)} is before this week's Monday {this_monday}: week "
                        f'{common.week_label(this_monday - dt.timedelta(days=7))} is not complete in the presence '
                        f'tables, so the newest week is '
                        f'{common.week_label(common.monday(as_of) - dt.timedelta(days=7))}')
    lmeta = common.read_json(os.path.join(common.ledger_dir(root), 'ledger.json'))
    if not lmeta:
        raise SystemExit(f'scorecard: no ledger under {common.ledger_dir(root)}; run `python -m scs.ledger` first')
    if common.parse_ts(lmeta['cut']) < as_of:
        stale = f"ledger ends {lmeta['cut']}, before as_of {common.iso(as_of)}: later days count as gaps"
        if not a.allow_stale_ledger:
            raise SystemExit(f'scorecard: {stale}, and the weeks closed now would be frozen as masked; run '
                             f'`python -m scs.ledger` first (or --allow-stale-ledger)')
        warnings.append(stale)
    state, state_dir = assemble.load_state(root)
    prev_manifest, prev_dir = assemble.load_previous(root)
    if state_dir is None and prev_manifest is not None:
        warnings.append(f'no state under {common.state_link(root)} although the previous build {prev_dir} exists: '
                        f'every metric recomputes and freezes its whole history again')
    ctx = metrics.Ctx(root, as_of, todo, base=a.warehouse_base, hostmap=hm, threads=a.threads,
                      memory_limit=f'{a.memory_limit_gb:g}GB', lock_path=a.lock, lock_wait_s=a.lock_wait,
                      code_commit=common.code_commit())
    log(f'run: as_of {common.iso(as_of)} current week {ctx.current_week} ({common.week_label(ctx.current_week)}), '
        f'metrics {", ".join(todo)}, code {ctx.code_commit}, host map v{hm.version} {hm.sha256[:12]}')
    plan = assemble.plan_weeks(todo, state, prev_manifest, ctx.current_week, a.weeks, hm, backfill)
    for m, (ws, why) in plan.items():
        rng = ', '.join(f'{x}..{y}' for x, y in common.week_ranges(ws)) or 'none'
        log(f'run: {m}: {len(ws)} week(s) {rng} ({"; ".join(why)})')
    ranges = assemble.module_ranges(plan)
    for module in dict.fromkeys(defs.METRICS[m]['module'] for m in todo):
        for rir in ctx.rirs:
            for wf, wt in ranges.get(module, []):
                ctx.call(module, rir, wf, wt)
    mode = 'backfill' if backfill else 'weekly'
    extra = {'presence_end_by_rir': {r: common.iso(v) for r, v in ends.items()},
             'weeks_option': a.weeks, 'backfill': [x.isoformat() for x in backfill] if backfill else None,
             'module_calls': [{k: (v.isoformat() if isinstance(v, dt.date) else v) for k, v in o.items() if k != 'files'}
                              for o in ctx.outputs]}
    manifest, _ = assemble.build(ctx, plan, state, state_dir, prev_manifest, prev_dir, mode=mode,
                                 release=not a.no_release, allow_shrink=a.allow_shrink, warnings=warnings,
                                 extra_manifest=extra)
    shutil.rmtree(ctx.work, ignore_errors=True)
    log(f'run: done in {time.time() - t0:.1f}s, peak RSS {common.peak_rss_mb()} MB')
    print(assemble.summary(manifest), flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
