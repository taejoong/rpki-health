"""Fresh module rows + frozen state -> builds/<YYYYMMDDTHHMMSSZ>/, atomic `current`, state/ and releases/.

Which weeks a run computes (plan_weeks), per requested metric:
- the trailing weeks W-N..W-1 (N = --weeks, default 6: the longest finalisation, ROA-EE revocation with a
  35-day horizon, is final at W+6), W = the week that contains as_of;
- every week that is not closed yet although it is in the previous build or after the metric's first
  closed week (a missed weekly run, a shorter --weeks earlier); a week is closed for a metric once every
  rule of the metric is final for it (as_of >= W + 7 x max final_after);
- after a change of the metric's def_version, of the host map (version or classification digest) or of
  the framework VERSION: its frozen rows are discarded and the whole history it had (closed weeks and previous build)
  is recomputed;
- --backfill FROM TO adds that range (computed in one grouped pass per module and RIR).
Metrics that are not requested keep their frozen rows and the previous build's other rows unchanged.

Rows of a build, per metric: the frozen rows of state/ (never recomputed, even when the week is computed
again because another statistic of it is not final yet), plus the fresh rows of the computed weeks that
are not closed and whose key is not frozen. Fresh rows get their status from scs/status.py, which also
drops rows that are not published yet. Rows that became final are added to the new state.

Files of a build:
  scorecard.parquet / scorecard.csv  metrics.OUT_COLS, sorted by week, metric_id, rir, population, operator,
                                     window, stat (the order is total, so reruns are byte-identical)
  histograms.parquet                 metrics.HIST_COLS, same order plus bin_lo
  ledger.parquet, annotations.csv    the observation ledger and annotations the statuses were set with
  manifest.json                      versions (VERSION, def_version per metric, host map version, sha256 and digest,
                                     code_commit, rpki-client release, duckdb), as_of, weeks per metric,
                                     counts by status, the definitions with their method texts, warnings
Publish (as pipelines/country/crc/assemble.py): write builds/<stamp>.tmp, fsync, rename to builds/<stamp>,
then `ln -s builds/<stamp> current.new && mv -T current.new current`; keep 3 builds, never the one
`current` points to; .tmp directories older than 6 h are removed. The gate refuses (current unchanged,
state unchanged) a build with NaN/inf values, duplicate keys, an unknown status, a metric whose newest
week lacks one of the RIRs, or a metric with fewer weeks than in the previous build (--allow-shrink).
State: states/<stamp>/{scorecard,histograms}.parquet + state.json, swapped in through the `state` symlink
the same way (keep 3); a `state` link whose target is gone is an error (restore it, or remove the link to
rebuild from scratch). Release (weekly runs, not --backfill): releases/<YYYY-Www>/ = hard links of the
build files except scorecard.csv, which the release holds gzip-compressed (scorecard.csv.gz, the same
rows), plus CHANGES.csv (rows added, removed, or with another value, status or n than in the previous
release, from the scorecard.parquet files) and release.json; a second weekly run in the same week makes
<YYYY-Www>-2, and so on. Releases are never modified.
"""
import datetime as dt
import json
import os
import re
import shutil
import time

import duckdb

from . import VERSION, common, defs, ledger as ledger_mod, metrics, status
from .common import lit, log

STAMP_RE = re.compile(r'^\d{8}T\d{6}Z$')
RELEASE_RE = re.compile(r'^(\d{4}-W\d{2})(?:-(\d+))?$')
REFDATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'refdata')
ANNOTATIONS_PATH = os.path.join(REFDATA, 'annotations.csv')
DAY = dt.timedelta(days=7)


# ── state ────────────────────────────────────────────────────────────────────
def load_state(root):
    """(state.json content or an empty state, the state directory or None). A `state` link that leads
    nowhere is an error: an empty state would recompute and freeze every metric's history again."""
    link = common.state_link(root)
    if os.path.lexists(link) and not os.path.isdir(link):
        raise RuntimeError(f'{link} -> {os.path.realpath(link)} is missing: restore it, or remove the link to '
                           f'rebuild the state from scratch')
    if not os.path.isdir(link):
        return {'version': VERSION, 'metrics': {}}, None
    d = os.path.realpath(link)
    meta = common.read_json(os.path.join(d, 'state.json'))
    if not meta:
        raise RuntimeError(f'{d}/state.json missing or empty')
    return meta, d


def closed_weeks(entry):
    return {w for a, b in (entry or {}).get('closed') or [] for w in common.weeks(a, b)}


def state_valid(meta, metric_id, hm):
    """True when the frozen rows of metric_id were made with the current definition, host map (version and
    classification digest) and VERSION."""
    e = (meta.get('metrics') or {}).get(metric_id)
    return bool(e) and e.get('version') == VERSION and e.get('def_version') == defs.METRICS[metric_id][
        'def_version'] and e.get('hostmap_version') == hm.version and e.get('hostmap_digest') == hm.digest


def load_previous(root):
    """(manifest, build directory) of the build `current` points to, or (None, None)."""
    cur = common.current_link(root)
    if not os.path.isdir(cur):
        return None, None
    d = os.path.realpath(cur)
    return common.read_json(os.path.join(d, 'manifest.json')), d


def plan_weeks(metric_ids, state, prev_manifest, current_week, trailing, hm, backfill=None):
    """{metric_id: (sorted weeks to compute, [reasons])} (see the module docstring)."""
    last = current_week - DAY
    out = {}
    for m in metric_ids:
        ws, why = set(), []
        if trailing > 0:
            ws |= set(common.weeks(current_week - trailing * DAY, last))
            why.append(f'trailing {trailing}')
        entry = (state.get('metrics') or {}).get(m)
        closed = closed_weeks(entry)
        pw = ((prev_manifest or {}).get('metrics') or {}).get(m, {}).get('weeks') or {}
        known = []
        if pw.get('first'):
            known.append(common.to_date(pw['first']))
        if closed:
            known.append(min(closed))
        if known:
            history = set(common.weeks(min(known), last))
            if state_valid(state, m, hm):
                open_ = history - closed
                if open_ - ws:
                    why.append(f'{len(open_ - ws)} open week(s) since {min(open_)}')
                ws |= open_
            elif entry or pw:
                why.append(f'recompute from {min(known)}: def_version, host map or VERSION changed')
                ws |= history
        if backfill:
            ws |= set(common.weeks(backfill[0], min(common.monday(backfill[1]), last)))
            why.append(f'backfill {common.monday(backfill[0])}..{common.monday(backfill[1])}')
        out[m] = (sorted(w for w in ws if w <= last), why)
    return out


def module_ranges(plan):
    """{module: [(week_from, week_to)]}: contiguous ranges of the union of its metrics' weeks."""
    by_mod = {}
    for m, (ws, _) in plan.items():
        by_mod.setdefault(defs.METRICS[m]['module'], set()).update(ws)
    return {mod: common.week_ranges(ws) for mod, ws in by_mod.items() if ws}


# ── build ────────────────────────────────────────────────────────────────────
def _table_from(con, name, files, cols):
    """TEMP TABLE name from parquet files (or empty with the given columns)."""
    if files:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT {metrics.q(c for c, _ in cols)} "
                    f"FROM read_parquet({common.files_sql(files)})")
    else:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} ({', '.join(f'{chr(34)}{c}{chr(34)} {t}' for c, t in cols)})")


def _key_join(a, b, key):
    return ' AND '.join(f'{a}."{k}" = {b}."{k}"' for k in key)


def _n(con, sql):
    return con.execute(sql).fetchone()[0]


def gate(con, rirs, prev_manifest, allow_shrink=False):
    """Refuse a build that is broken or would show less than the previous one."""
    p = []
    for t, cols in (('out', ('value', 'top1_share')), ('out_hist', ('bin_lo', 'bin_hi'))):
        cond = ' OR '.join(f'isnan({c}) OR isinf({c})' for c in cols)
        n = _n(con, f"SELECT count(*) FROM {t} WHERE {cond}")
        if n:
            p.append(f'{t}: {n} rows with NaN/inf')
    for t, key in (('out', metrics.KEY), ('out_hist', metrics.HIST_KEY)):
        n = _n(con, f"SELECT count(*) - count(DISTINCT ({metrics.q(key)})) FROM {t}")
        if n:
            p.append(f'{t}: {n} duplicate keys')
    bad = _n(con, f"SELECT count(*) FROM out WHERE status NOT IN ({', '.join(lit(s) for s in status.STATUSES)}) "
                  f"OR status IS NULL")
    if bad:
        p.append(f'{bad} rows with an unknown status')
    if not _n(con, 'SELECT count(*) FROM out'):
        p.append('no rows')
    for m, w, rs in con.execute("""SELECT metric_id, week, list(DISTINCT rir ORDER BY rir) FROM out
            WHERE (metric_id, week) IN (SELECT (metric_id, max(week)) FROM out GROUP BY metric_id)
            GROUP BY ALL ORDER BY ALL""").fetchall():
        missing = sorted(set(rirs) - set(rs))
        if missing:
            p.append(f'{m}: newest week {w} has no rows for {", ".join(missing)}')
    if prev_manifest and not allow_shrink:
        now = dict(con.execute("SELECT metric_id, count(DISTINCT week) FROM out GROUP BY ALL").fetchall())
        for m, info in (prev_manifest.get('metrics') or {}).items():
            before = (info.get('weeks') or {}).get('count') or 0
            if m in defs.METRICS and now.get(m, 0) < before:
                p.append(f'{m}: {now.get(m, 0)} weeks, fewer than the {before} of the previous build '
                         f'(--allow-shrink to publish anyway)')
    if p:
        raise RuntimeError('build not published: ' + '; '.join(p))


def build(ctx, plan, state, state_dir, prev_manifest, prev_dir, mode='weekly', release=True, allow_shrink=False,
          stamp=None, keep=3, warnings=(), extra_manifest=None):
    """Assemble, gate, publish; update state/ and make the release. Returns (manifest, build dir)."""
    t0 = time.time()
    root = ctx.root
    stamp = stamp or dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    bdir = os.path.join(common.builds_dir(root), stamp)
    if os.path.exists(bdir):
        raise RuntimeError(f'{bdir} exists')
    work = bdir + '.tmp'
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    warnings = list(warnings)
    computed = {m for m, (ws, _) in plan.items() if ws}
    con = ctx.connect()
    try:
        manifest, new_state = _assemble(con, ctx, plan, computed, state, state_dir, prev_manifest, prev_dir, work,
                                        stamp, mode, warnings, allow_shrink, extra_manifest)
        files = [os.path.join(work, f) for f in sorted(os.listdir(work))]
        common.fsync_paths(*files, work)
        os.rename(work, bdir)
        common.fsync_paths(common.builds_dir(root))
        publish(root, stamp)
    except BaseException:
        shutil.rmtree(work, ignore_errors=True)
        con.close()
        raise
    # after the swap a failure no longer fails the run: a state that was not written makes the next run
    # recompute the weeks that would have been closed, and a missing release is made by hand
    removed, state_stamp, rel, late = [], None, None, []
    try:
        removed = prune(root, keep)
    except Exception as e:      # noqa: BLE001
        late.append(f'old builds not pruned: {type(e).__name__}: {e}')
    try:
        state_stamp = write_state(con, root, stamp, new_state, keep)
    except Exception as e:      # noqa: BLE001 (reported as a warning, the run is PARTIAL)
        late.append(f'state not updated: {type(e).__name__}: {e}')
    finally:
        con.close()
    if mode == 'weekly' and release:
        try:
            rel = make_release(root, bdir, manifest)
        except Exception as e:  # noqa: BLE001
            late.append(f'release not made: {type(e).__name__}: {e}')
    manifest['build_warnings'] = manifest['build_warnings'] + late
    log(f"assemble: build {stamp} as_of={manifest['as_of']} rows={manifest['counts']['scorecard_rows']:,} "
        f"histogram_rows={manifest['counts']['histogram_rows']:,} state={state_stamp} release={rel} "
        f"removed={removed} [{time.time() - t0:.1f}s]")
    for w in manifest['build_warnings']:
        log(f'assemble: WARNING {w}')
    manifest['release'] = rel
    return manifest, bdir


def _assemble(con, ctx, plan, computed, state, state_dir, prev_manifest, prev_dir, work, stamp, mode, warnings,
              allow_shrink, extra_manifest):
    hm = ctx.hostmap
    registry = list(defs.METRICS)
    # 1. fresh rows of the computed (metric, week) pairs, with their status
    common.values_table(con, 'computed', [('metric_id', 'VARCHAR'), ('week', 'DATE')],
                        [(m, w) for m, (ws, _) in plan.items() for w in ws])
    _table_from(con, 'fresh_raw0', ctx.emitted('rows'), metrics.ROW_COLS)
    _table_from(con, 'fresh_hist0', ctx.emitted('hist'), metrics.HIST_COLS)
    for t in ('fresh_raw0', 'fresh_hist0'):
        con.execute(f"CREATE OR REPLACE TEMP TABLE {t[:-1]} AS SELECT * FROM {t} "
                    f"WHERE (metric_id, week) IN (SELECT (metric_id, week) FROM computed)")
    con.execute(f"CREATE OR REPLACE TEMP TABLE ledger AS SELECT * FROM read_parquet({lit(ctx._ledger_file('ledger'))})")
    status.annotations_table(con, ANNOTATIONS_PATH)
    problems = status.check_annotations(con)
    if problems:
        raise RuntimeError('annotations.csv: ' + '; '.join(problems))
    code_commit = ctx.code_commit or common.code_commit()
    status.apply(con, 'fresh_raw', 'fresh', ctx.as_of, hm.version, code_commit, metric_ids=set(registry))
    status.apply_hist(con, 'fresh_hist', 'fresh_h', ctx.as_of, metric_ids=set(registry))
    # 2. frozen rows: all metrics not computed now, and computed metrics whose state is still valid
    valid = {m for m in registry if state_valid(state, m, hm)}
    keep_state = sorted((set(registry) - computed) | (computed & valid))
    st_files = {k: os.path.join(state_dir, f'{k}.parquet') for k in ('scorecard', 'histograms')} if state_dir else {}
    _table_from(con, 'st0', [st_files['scorecard']] if st_files else [], metrics.OUT_COLS)
    _table_from(con, 'sth0', [st_files['histograms']] if st_files else [], metrics.HIST_COLS + (('frozen', 'BOOLEAN'),))
    ks = ', '.join(lit(m) for m in keep_state) or "''"
    con.execute(f"CREATE OR REPLACE TEMP TABLE st AS SELECT * FROM st0 WHERE metric_id IN ({ks})")
    con.execute(f"CREATE OR REPLACE TEMP TABLE sth AS SELECT * FROM sth0 WHERE metric_id IN ({ks})")
    # weeks closed before this run (all their rows are frozen in st: fresh rows of them are ignored) and the
    # weeks of computed metrics that are final now
    old_closed = {m: closed_weeks((state.get('metrics') or {}).get(m)) for m in keep_state}
    closed = dict(old_closed)
    for m in computed:
        mf = defs.max_final_after(m)
        newly = {w for w in plan[m][0] if ctx.as_of >= dt.datetime.combine(w, dt.time()) + mf * DAY}
        closed[m] = old_closed.get(m, set()) | newly
    common.values_table(con, 'old_closed', [('metric_id', 'VARCHAR'), ('week', 'DATE')],
                        [(m, w) for m, ws in old_closed.items() for w in sorted(ws)])
    # 3. previous build's rows of metrics that are not computed now (not closed, not frozen)
    carried = sorted(set(registry) - computed)
    prev_files = [os.path.join(prev_dir, 'scorecard.parquet')] if prev_dir and carried else []
    prev_hist = [os.path.join(prev_dir, 'histograms.parquet')] if prev_dir and carried else []
    _table_from(con, 'prev0', prev_files, metrics.OUT_COLS)
    _table_from(con, 'prevh0', prev_hist, metrics.HIST_COLS)
    cs = ', '.join(lit(m) for m in carried) or "''"
    # 4. merge: frozen rows, then fresh rows of weeks that were not closed (keys not frozen), then the previous
    #    build's rows of metrics not computed now
    not_closed = "(x.metric_id, x.week) NOT IN (SELECT (metric_id, week) FROM old_closed)"
    for out, st_t, fresh_t, prev_t, key, frozen in (('out', 'st', 'fresh', 'prev0', metrics.KEY, ''),
                                                    ('out_hist0', 'sth', 'fresh_h', 'prevh0', metrics.HIST_KEY,
                                                     ', false AS frozen')):
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {out} AS
            SELECT * FROM {st_t}
            UNION ALL BY NAME
            SELECT x.* FROM {fresh_t} x
            WHERE {not_closed} AND NOT EXISTS (SELECT 1 FROM {st_t} s WHERE {_key_join('s', 'x', key)})
            UNION ALL BY NAME
            SELECT x.*{frozen} FROM {prev_t} x
            WHERE x.metric_id IN ({cs}) AND {not_closed}
              AND NOT EXISTS (SELECT 1 FROM {st_t} s WHERE {_key_join('s', 'x', key)})""")
    con.execute(f"CREATE OR REPLACE TEMP TABLE out_hist AS SELECT {metrics.q(c for c, _ in metrics.HIST_COLS)} "
                f"FROM out_hist0")
    gate(con, ctx.rirs, prev_manifest, allow_shrink)
    # 5. files
    out_cols = metrics.q(c for c, _ in metrics.OUT_COLS)
    common.copy_to_parquet(con, f'SELECT {out_cols} FROM out', os.path.join(work, 'scorecard.parquet'),
                           order_by=metrics.order_sql())
    common.copy_to_csv(con, f'SELECT {out_cols} FROM out', os.path.join(work, 'scorecard.csv'),
                       order_by=metrics.order_sql())
    common.copy_to_parquet(con, 'SELECT * FROM out_hist', os.path.join(work, 'histograms.parquet'),
                           order_by=metrics.order_sql(metrics.HIST_KEY))
    shutil.copyfile(ctx._ledger_file('ledger'), os.path.join(work, 'ledger.parquet'))
    shutil.copyfile(ANNOTATIONS_PATH, os.path.join(work, 'annotations.csv'))
    manifest = _manifest(con, ctx, plan, computed, valid, stamp, mode, warnings, code_commit, extra_manifest)
    common.atomic_write_json(os.path.join(work, 'manifest.json'), manifest, sort_keys=False)
    # 6. the new state: frozen rows before plus the rows that are final now
    con.execute(f"""CREATE OR REPLACE TEMP TABLE new_st AS
        SELECT * FROM st UNION ALL BY NAME
        SELECT o.* FROM out o WHERE o.frozen AND NOT EXISTS (SELECT 1 FROM st s WHERE {_key_join('s', 'o', metrics.KEY)})""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE new_sth AS
        SELECT * FROM sth UNION ALL BY NAME
        SELECT o.* FROM out_hist0 o WHERE o.frozen
          AND NOT EXISTS (SELECT 1 FROM sth s WHERE {_key_join('s', 'o', metrics.HIST_KEY)})""")
    entries = {}
    for m in sorted(closed):
        if m in computed:
            e = {'version': VERSION, 'def_version': defs.METRICS[m]['def_version'], 'hostmap_version': hm.version,
                 'hostmap_digest': hm.digest, 'hostmap_sha256': hm.sha256}
        else:
            e = {k: v for k, v in ((state.get('metrics') or {}).get(m) or {}).items() if k != 'closed'}
        if not e and not closed[m]:
            continue
        e['closed'] = [[a.isoformat(), b.isoformat()] for a, b in common.week_ranges(closed[m])]
        entries[m] = e
    new_state = {'version': VERSION, 'build': stamp, 'as_of': common.iso(ctx.as_of), 'metrics': entries}
    return manifest, new_state


def _manifest(con, ctx, plan, computed, valid, stamp, mode, warnings, code_commit, extra):
    per = {}
    for m, w0, w1, nw, n in con.execute("""SELECT metric_id, CAST(min(week) AS VARCHAR), CAST(max(week) AS VARCHAR),
            count(DISTINCT week), count(*) FROM out GROUP BY ALL ORDER BY ALL""").fetchall():
        per[m] = {'weeks': {'first': w0, 'last': w1, 'count': nw}, 'rows': n}
    by_status = {}
    for m, s, n in con.execute("SELECT metric_id, status, count(*) FROM out GROUP BY ALL ORDER BY ALL").fetchall():
        by_status.setdefault(m, {})[s] = n
    for m in list(per):
        d = defs.METRICS[m]
        per[m].update({'def_version': d['def_version'], 'by_status': by_status.get(m, {}),
                       'computed': m in computed,
                       'computed_weeks': [[a.isoformat(), b.isoformat()]
                                          for a, b in common.week_ranges(plan.get(m, ([], []))[0])],
                       'computed_why': plan.get(m, ([], []))[1]})
    for m in computed - set(per):
        per[m] = {'weeks': {'first': None, 'last': None, 'count': 0}, 'rows': 0, 'computed': True,
                  'def_version': defs.METRICS[m]['def_version'],
                  'computed_weeks': [[a.isoformat(), b.isoformat()] for a, b in common.week_ranges(plan[m][0])],
                  'computed_why': plan[m][1]}
    for m in sorted(set(per) - computed - valid):
        warnings.append(f'{m}: frozen rows from another def_version, host map or VERSION; run with --metrics {m} '
                        f'to recompute its history')
    w0, w1, nw = con.execute("SELECT CAST(min(week) AS VARCHAR), CAST(max(week) AS VARCHAR), count(DISTINCT week) "
                             "FROM out").fetchone()
    lmeta = common.read_json(os.path.join(ctx.ledger_dir, 'ledger.json')) or {}
    ordered = [m for m in defs.METRICS if m in per]
    return {
        'name': 'rpki-scorecard', 'version': VERSION, 'build': stamp, 'generated_at': common.utc_now_iso(),
        'mode': mode, 'as_of': common.iso(ctx.as_of), 'current_week': ctx.current_week.isoformat(),
        'newest_week': w1, 'newest_week_label': common.week_label(w1) if w1 else None,
        'weeks': {'first': w0, 'last': w1, 'count': nw},
        'metrics': {m: per[m] for m in ordered},
        'counts': {'scorecard_rows': _n(con, 'SELECT count(*) FROM out'),
                   'histogram_rows': _n(con, 'SELECT count(*) FROM out_hist'),
                   'by_status': dict(con.execute("SELECT status, count(*) FROM out GROUP BY ALL ORDER BY ALL").fetchall())},
        'hostmap': ctx.hostmap.info(), 'code_commit': code_commit,
        'rpki_client_release': common.rpki_client_release(), 'duckdb': duckdb.__version__,
        'ledger': {'cut': lmeta.get('cut'), 'generated_at': lmeta.get('generated_at'), 'rules': lmeta.get('rules')},
        'inputs': {'warehouse_base': ctx.base, **(extra or {})},
        'method': defs.COMMON_METHOD,
        'definitions': defs.manifest_entries(ordered),
        'build_warnings': warnings,
    }


# ── publish, state, prune, release ───────────────────────────────────────────
def _swap_link(link, target):
    new = link + '.new'
    if os.path.lexists(new):
        os.unlink(new)
    os.symlink(target, new)
    os.replace(new, link)
    common.fsync_paths(os.path.dirname(link))


def publish(root, stamp):
    """current -> builds/<stamp> via a new symlink renamed over the old one (mv -T)."""
    _swap_link(common.current_link(root), os.path.join('builds', stamp))


def _prune_dir(d, target, keep):
    if not os.path.isdir(d):
        return []
    stamps = sorted((s for s in os.listdir(d) if STAMP_RE.match(s) and os.path.isdir(os.path.join(d, s))),
                    reverse=True)
    removed = []
    for s in stamps[keep:]:
        if s != target:
            shutil.rmtree(os.path.join(d, s))
            removed.append(s)
    now = time.time()
    for s in os.listdir(d):
        p = os.path.join(d, s)
        if s.endswith('.tmp') and os.path.isdir(p) and now - os.path.getmtime(p) > 6 * 3600:
            shutil.rmtree(p, ignore_errors=True)
    return removed


def prune(root, keep=3):
    """Delete all but the newest `keep` builds (never the current target) and stale .tmp dirs."""
    target = os.path.basename(os.path.realpath(common.current_link(root)))
    return _prune_dir(common.builds_dir(root), target, keep)


def write_state(con, root, stamp, new_state, keep=3):
    """states/<stamp>/{scorecard,histograms}.parquet + state.json, then `state` -> states/<stamp>."""
    sd = common.states_dir(root)
    d = os.path.join(sd, stamp)
    tmp = d + '.tmp'
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    out_cols = metrics.q(c for c, _ in metrics.OUT_COLS)
    common.copy_to_parquet(con, f'SELECT {out_cols} FROM new_st', os.path.join(tmp, 'scorecard.parquet'),
                           order_by=metrics.order_sql())
    common.copy_to_parquet(con, f"SELECT {metrics.q(c for c, _ in metrics.HIST_COLS)}, frozen FROM new_sth",
                           os.path.join(tmp, 'histograms.parquet'), order_by=metrics.order_sql(metrics.HIST_KEY))
    new_state['rows'] = _n(con, 'SELECT count(*) FROM new_st')
    common.atomic_write_json(os.path.join(tmp, 'state.json'), new_state)
    common.fsync_paths(*[os.path.join(tmp, f) for f in os.listdir(tmp)], tmp)
    os.rename(tmp, d)
    common.fsync_paths(sd)
    _swap_link(common.state_link(root), os.path.join('states', stamp))
    _prune_dir(sd, stamp, keep)
    return stamp


def _release_key(name):
    m = RELEASE_RE.match(name)
    return (m.group(1), int(m.group(2) or 1)) if m else None


def latest_release(root):
    rd = common.releases_dir(root)
    names = [n for n in os.listdir(rd) if _release_key(n) and os.path.isdir(os.path.join(rd, n))] \
        if os.path.isdir(rd) else []
    return max(names, key=_release_key) if names else None


def make_release(root, bdir, manifest):
    """releases/<YYYY-Www>[-n]/: hard links of the build files (scorecard.csv as scorecard.csv.gz),
    CHANGES.csv and release.json (immutable)."""
    label = manifest.get('newest_week_label')
    if not label:
        return None
    rd = common.releases_dir(root)
    os.makedirs(rd, exist_ok=True)
    prev = latest_release(root)
    name, n = label, 1
    while os.path.exists(os.path.join(rd, name)):
        n += 1
        name = f'{label}-{n}'
    tmp = os.path.join(rd, f'.tmp-{name}-{os.getpid()}')
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    try:
        for f in sorted(os.listdir(bdir)):
            if f == 'scorecard.csv':        # the largest file (about 190 MB); about 15x smaller gzipped
                continue
            try:
                os.link(os.path.join(bdir, f), os.path.join(tmp, f))
            except OSError:
                shutil.copy2(os.path.join(bdir, f), os.path.join(tmp, f))
        write_csv_gz(os.path.join(bdir, 'scorecard.parquet'), os.path.join(tmp, 'scorecard.csv.gz'), root)
        changes = write_changes(os.path.join(bdir, 'scorecard.parquet'),
                                os.path.join(rd, prev, 'scorecard.parquet') if prev else None,
                                os.path.join(tmp, 'CHANGES.csv'), root)
        common.atomic_write_json(os.path.join(tmp, 'release.json'), {
            'release': name, 'week': label, 'build': manifest['build'], 'as_of': manifest['as_of'],
            'previous_release': prev, 'created_at': common.utc_now_iso(), 'changes': changes,
            'rows': manifest['counts']['scorecard_rows']})
        common.fsync_paths(*[os.path.join(tmp, f) for f in os.listdir(tmp)], tmp)
        os.rename(tmp, os.path.join(rd, name))
        common.fsync_paths(rd)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return name


def write_csv_gz(parquet, path, root):
    """scorecard.csv.gz of a release: the rows of a build's scorecard.parquet as gzip-compressed CSV, in the
    order and format of the build's scorecard.csv."""
    con = common.connect(threads=2, memory_limit='2GB', root=root)
    try:
        common.copy_to_csv(con, f'SELECT * FROM read_parquet({lit(parquet)})', path, order_by=metrics.order_sql(),
                           compression='gzip')
    finally:
        con.close()


def write_changes(new, old, path, root):
    """CHANGES.csv: rows added or removed since the previous release, or with another value, status or n.
    Without a previous release the file has the header only. Returns counts per change kind."""
    con = common.connect(threads=2, memory_limit='2GB', root=root)
    try:
        key = metrics.KEY
        kc = ', '.join(f'coalesce(n."{k}", o."{k}") AS "{k}"' for k in key)
        old_src = f"read_parquet({lit(old)})" if old else f"(SELECT * FROM read_parquet({lit(new)}) WHERE false)"
        con.execute(f"""CREATE TEMP TABLE ch AS
            SELECT CASE WHEN o.metric_id IS NULL THEN 'added' WHEN n.metric_id IS NULL THEN 'removed'
                        ELSE 'changed' END AS change, {kc},
                   o.value AS old_value, n.value AS new_value, o.status AS old_status, n.status AS new_status,
                   o.n AS old_n, n.n AS new_n
            FROM read_parquet({lit(new)}) n FULL OUTER JOIN {old_src} o ON {_key_join('n', 'o', key)}
            WHERE {'true' if old else 'false'} AND (o.metric_id IS NULL OR n.metric_id IS NULL
                   OR n.value IS DISTINCT FROM o.value OR n.status IS DISTINCT FROM o.status OR n.n IS DISTINCT FROM o.n)""")
        common.copy_to_csv(con, 'SELECT * FROM ch', path, order_by=metrics.order_sql() + ', change')
        return dict(con.execute("SELECT change, count(*) FROM ch GROUP BY ALL ORDER BY ALL").fetchall())
    finally:
        con.close()


def summary(manifest):
    """The one-line JSON summary run_weekly.sh prints."""
    return json.dumps({'build': manifest['build'], 'mode': manifest['mode'], 'as_of': manifest['as_of'],
                       'newest_week': manifest['newest_week'], 'weeks': manifest['weeks']['count'],
                       'rows': manifest['counts']['scorecard_rows'], 'by_status': manifest['counts']['by_status'],
                       'metrics': {m: {'rows': v['rows'], 'computed_weeks': v.get('computed_weeks')}
                                   for m, v in manifest['metrics'].items()},
                       'release': manifest.get('release'), 'warnings': manifest['build_warnings']}, sort_keys=True)


def ledger_summary(root):
    return common.read_json(os.path.join(common.ledger_dir(root), 'ledger.json'))


__all__ = ['plan_weeks', 'module_ranges', 'build', 'publish', 'prune', 'make_release', 'load_state',
           'load_previous', 'summary', 'ledger_mod']
