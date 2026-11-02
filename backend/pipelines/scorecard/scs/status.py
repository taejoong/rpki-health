"""Status of every row: whether it is published, and its status, era, flags and frozen marker.

For a row of week W (the Monday) under the rule of its (metric, stat) (scs/defs.py):
- published when as_of >= W + 7 x publish_after days (publish_after 1 = the Monday run right after the
  week); unpublished rows are dropped (ROA-EE revocation first appears at W+2);
- final when as_of >= W + 7 x final_after days: the value no longer changes; final rows are frozen
  (frozen = true) into state/ and later runs keep them as they are;
- masked when the observation ledger shows that our collection was incomplete for the period:
    cycles    W's era-2 days have fewer than 90% of the expected cycles (72 a day), or one of its era-1
              days has no good archive snapshot (crl_window, mft_window, crl_empty);
    gap       a ledger gap day (era 1: no good snapshot; era 2: more than 3 hours without a good cycle)
              in [W, W + 7 + lookahead_days), counting only days before as_of's day (later days are not
              observed yet, which is why such rows stay provisional until final_after);
    end_gap   a gap on W's last day (point-in-time values at the week's end);
    verdicts  W's verdict cycles are fewer than 90% of its snapshot cycles, or fewer than 90% of its
              expected cycles were good (as for 'cycles': an outage week whose remaining cycles all have
              verdicts), or W starts before verdicts exist (2026-06-20);
  a day without a ledger row (before 2011-12-26, or not yet complete at the ledger's cut) counts as a
  gap; masked rows keep their values, consumers must check the status;
- insufficient when the metric is a sample (kind 'sample') and n < 30 or n_ca < 10;
- status = masked, else insufficient, else final, else provisional.
Rolling windows (13w, 52w) take the status of their last week W; ledger gaps in the window's earlier weeks
are listed in flags (ledger:gap_in_window) because the data of those weeks were complete when the window
was computed.

era: '1', 'transition' (ISO 2026-W14..W16) or '2' for W; 'mixed' for a rolling window spanning two eras.
flags, sorted and ';'-joined: ids of the annotations (refdata/annotations.csv) that overlap the row's period
[first week of the window, W + 7 (+ lookahead_days for 'gap' rules)) and match its rir, metric and
population; ledger:gap (a gap day in the mask period), ledger:low (a day of W below 90% of the expected
cycles), ledger:gap_in_window, era:transition.
Histogram rows get the published filter and the frozen marker only (their stat picks the rule).
"""
import datetime as dt

from . import common, defs
from .classify import POPULATIONS
from .common import date_lit, lit

WINDOW_WEEKS = {'1w': 1, '13w': 13, '52w': 52, 'end': 1}
STATUSES = ('provisional', 'final', 'masked', 'insufficient')
BAD_SHARE = 0.9


def _era_sql(w):
    return (f"CASE WHEN {w} + 6 < {date_lit(common.TRANSITION_FROM)} THEN '1' "
            f"WHEN {w} <= {date_lit(common.TRANSITION_TO)} THEN 'transition' ELSE '2' END")


def _rules(con, src, metric_ids):
    """TEMP TABLE scs_statrule (metric_id, stat, publish_after, final_after, mask, lookahead_days, min_n,
    min_n_ca, def_version) for every (metric, stat) of src."""
    pairs = con.execute(f"SELECT DISTINCT metric_id, stat FROM {src}").fetchall()
    rows = []
    for m, s in pairs:
        if m not in defs.METRICS or (metric_ids is not None and m not in metric_ids):
            raise ValueError(f'status: metric {m!r} is not in the requested registry')
        r, d = defs.rule_for(m, s), defs.METRICS[m]
        sample = d['kind'] == 'sample'
        rows.append((m, s, r['publish_after'], r['final_after'], r['mask'], r['lookahead_days'],
                     defs.MIN_N if sample else None, defs.MIN_N_CA if sample else None, d['def_version']))
    common.values_table(con, 'scs_statrule', [
        ('metric_id', 'VARCHAR'), ('stat', 'VARCHAR'), ('publish_after', 'INTEGER'), ('final_after', 'INTEGER'),
        ('mask', 'VARCHAR'), ('lookahead_days', 'INTEGER'), ('min_n', 'INTEGER'), ('min_n_ca', 'INTEGER'),
        ('def_version', 'INTEGER')], rows)


def _ts(as_of):
    return f"TIMESTAMP '{common.parse_ts(as_of).strftime('%Y-%m-%d %H:%M:%S')}'"


def apply_hist(con, src, out, as_of, metric_ids=None):
    """Histogram rows of src that are published as of as_of, plus `frozen`, into TEMP TABLE out."""
    _rules(con, src, metric_ids)
    a = _ts(as_of)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT h.*, CAST(h.week AS TIMESTAMP) + to_days(7 * u.final_after) <= {a} AS frozen
        FROM {src} h JOIN scs_statrule u USING (metric_id, stat)
        WHERE CAST(h.week AS TIMESTAMP) + to_days(7 * u.publish_after) <= {a}""")


def apply(con, src, out, as_of, hostmap_version, code_commit, ledger='ledger', annotations='annotations',
          metric_ids=None):
    """Published rows of src (metrics.ROW_COLS) with status, era, flags, as_of, def_version,
    hostmap_version, code_commit and frozen (metrics.OUT_COLS order) into TEMP TABLE out.
    ledger: a table like ledger.parquet; annotations: a table from annotations_table()."""
    _rules(con, src, metric_ids)
    a = _ts(as_of)
    as_day = date_lit(common.parse_ts(as_of).date())
    max_look = max([0] + [r['lookahead_days'] for m in defs.METRICS.values() for r in m['rules']])
    windows = ', '.join(f"({lit(w)}, {k})" for w, k in WINDOW_WEEKS.items())
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scs_r AS
        SELECT s.*, u.publish_after, u.final_after, u.mask, u.lookahead_days, u.min_n, u.min_n_ca, u.def_version,
               w.k AS span_weeks, CAST(s.week - 7 * (w.k - 1) AS DATE) AS first_week,
               CASE WHEN u.mask = 'gap' THEN u.lookahead_days ELSE 0 END AS look,
               CAST(s.week AS TIMESTAMP) + to_days(7 * u.final_after) <= {a} AS final_
        FROM {src} s JOIN scs_statrule u USING (metric_id, stat)
             JOIN (VALUES {windows}) w(win, k) ON w.win = s."window"
        WHERE CAST(s.week AS TIMESTAMP) + to_days(7 * u.publish_after) <= {a}""")
    # masks per (rir, week, rule): the days of the mask period that are before as_of's day
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scs_md AS
        SELECT n.rir, n.week, n.mask, n.look, CAST(d.i AS INTEGER) AS i, n.week + CAST(d.i AS INTEGER) AS day
        FROM (SELECT DISTINCT rir, week, mask, look FROM scs_r) n CROSS JOIN range(0, {7 + max_look}) d(i)
        WHERE d.i < 7 + n.look AND n.week + CAST(d.i AS INTEGER) < {as_day}""")
    v0 = date_lit(common.VERDICTS_FROM)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scs_mask AS
        SELECT md.rir, md.week, md.mask, md.look,
               count(*) FILTER (WHERE md.i < 7 AND (l.day IS NULL OR NOT l.complete)) AS w_missing,
               count(*) FILTER (WHERE l.day IS NULL OR NOT l.complete) AS p_missing,
               count(*) FILTER (WHERE md.i < 7 AND l.era = 1 AND l.good_cycles < 1) AS e1_bad,
               coalesce(sum(l.good_cycles) FILTER (WHERE md.i < 7 AND l.era = 2), 0) AS e2_good,
               coalesce(sum(l.expected) FILTER (WHERE md.i < 7 AND l.era = 2), 0) AS e2_expected,
               count(*) FILTER (WHERE l.gap AND l.complete) AS p_gap,
               count(*) FILTER (WHERE md.i = 6 AND (l.day IS NULL OR NOT l.complete OR l.gap)) AS end_gap,
               count(*) FILTER (WHERE md.i = 6) AS end_days,
               count(*) FILTER (WHERE md.i < 7 AND md.day < {v0}) AS pre_verdicts,
               coalesce(sum(l.verdict_cycles) FILTER (WHERE md.i < 7), 0) AS v_cycles,
               coalesce(sum(l.cycles) FILTER (WHERE md.i < 7 AND md.day >= {v0}), 0) AS v_snaps,
               count(*) FILTER (WHERE md.i < 7 AND l.low AND l.complete) AS w_low
        FROM scs_md md LEFT JOIN {ledger} l ON l.rir = md.rir AND l.day = md.day
        GROUP BY md.rir, md.week, md.mask, md.look""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scs_mask2 AS
        SELECT *, CASE mask
                    WHEN 'cycles' THEN w_missing > 0 OR e1_bad > 0 OR e2_good < {BAD_SHARE} * e2_expected
                    WHEN 'gap' THEN p_gap > 0 OR p_missing > 0
                    WHEN 'end_gap' THEN end_gap > 0 OR end_days = 0
                    WHEN 'verdicts' THEN pre_verdicts > 0 OR w_missing > 0 OR v_cycles < {BAD_SHARE} * v_snaps
                                         OR e2_good < {BAD_SHARE} * e2_expected
                    ELSE false END AS masked
        FROM scs_mask""")
    # gap days in the earlier weeks of rolling windows
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scs_win AS
        WITH lw AS (SELECT rir, CAST(date_trunc('week', day) AS DATE) AS week,
                           count(*) FILTER (WHERE gap AND complete) AS gap_days
                    FROM {ledger} GROUP BY ALL)
        SELECT n.rir, n.week, n.span_weeks, sum(lw.gap_days) AS gap_days
        FROM (SELECT DISTINCT rir, week, span_weeks, first_week FROM scs_r WHERE span_weeks > 1) n
             JOIN lw ON lw.rir = n.rir AND lw.week >= n.first_week AND lw.week < n.week
        GROUP BY ALL""")
    # annotation ids per (rir, period, metric, population)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scs_ann AS
        SELECT k.rir, k.week, k.first_week, k.look, k.metric_id, k.population, list(a.id ORDER BY a.id) AS ids
        FROM (SELECT DISTINCT rir, week, first_week, look, metric_id, population FROM scs_r) k
             JOIN {annotations} a
               ON a.date_from < k.week + 7 + k.look AND coalesce(a.date_to, DATE '9999-12-31') >= k.first_week
              AND (list_contains(a.rirs, 'all') OR list_contains(a.rirs, k.rir))
              AND (len(a.metrics) = 0 OR list_contains(a.metrics, k.metric_id))
              AND (len(a.populations) = 0 OR list_contains(a.populations, k.population))
        GROUP BY ALL""")
    tr0, tr1 = date_lit(common.TRANSITION_FROM), date_lit(common.TRANSITION_TO)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT r.week, r."window", r.rir, r.population, r.operator, r.metric_id, r.stat, r.value, r.n, r.n_ca,
               r.top1_share,
               CASE WHEN m.masked THEN 'masked'
                    WHEN r.min_n IS NOT NULL AND (coalesce(r.n, 0) < r.min_n OR coalesce(r.n_ca, 0) < r.min_n_ca)
                         THEN 'insufficient'
                    WHEN r.final_ THEN 'final' ELSE 'provisional' END AS status,
               CASE WHEN {_era_sql('r.first_week')} <> {_era_sql('r.week')} THEN 'mixed' ELSE {_era_sql('r.week')} END
                   AS era,
               array_to_string(list_sort(list_distinct(
                   coalesce(an.ids, [])
                   || CASE WHEN m.p_gap > 0 THEN ['ledger:gap'] ELSE [] END
                   || CASE WHEN m.w_low > 0 THEN ['ledger:low'] ELSE [] END
                   || CASE WHEN coalesce(wn.gap_days, 0) > 0 THEN ['ledger:gap_in_window'] ELSE [] END
                   || CASE WHEN r.first_week <= {tr1} AND r.week + 6 >= {tr0} THEN ['era:transition'] ELSE [] END)),
                   ';') AS flags,
               CAST({a} AS TIMESTAMPTZ) AS as_of, r.def_version, CAST({int(hostmap_version)} AS INTEGER) AS hostmap_version,
               {lit(code_commit)} AS code_commit, r.final_ AS frozen
        FROM scs_r r
             LEFT JOIN scs_mask2 m ON m.rir = r.rir AND m.week = r.week AND m.mask IS NOT DISTINCT FROM r.mask
                                  AND m.look = r.look
             LEFT JOIN scs_win wn ON wn.rir = r.rir AND wn.week = r.week AND wn.span_weeks = r.span_weeks
             LEFT JOIN scs_ann an ON an.rir = r.rir AND an.week = r.week AND an.first_week = r.first_week
                                 AND an.look = r.look AND an.metric_id = r.metric_id AND an.population = r.population""")
    for t in ('scs_r', 'scs_md', 'scs_mask', 'scs_mask2', 'scs_win', 'scs_ann'):
        con.execute(f'DROP TABLE IF EXISTS {t}')


def annotations_table(con, path, name='annotations'):
    """TEMP TABLE <name>(id, date_from, date_to, rirs, kind, metrics, populations, text, source) from
    refdata/annotations.csv (rir, metrics and populations are ';'-lists; empty metrics/populations = all,
    empty date_to = open-ended)."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
        SELECT id, CAST(date_from AS DATE) AS date_from, CAST(nullif(date_to, '') AS DATE) AS date_to,
               list_transform(string_split(rir, ';'), x -> trim(x)) AS rirs, kind,
               list_filter(list_transform(string_split(coalesce(metrics, ''), ';'), x -> trim(x)), x -> x <> '') AS metrics,
               list_filter(list_transform(string_split(coalesce(populations, ''), ';'), x -> trim(x)), x -> x <> '')
                   AS populations,
               text, source
        FROM read_csv({lit(path)}, header = true, all_varchar = true, quote = '"', escape = '"')""")
    return name


def check_annotations(con, name='annotations'):
    """Problems in the annotations table (empty list when it is consistent)."""
    out = []
    for i, n in con.execute(f"SELECT id, count(*) FROM {name} GROUP BY id HAVING count(*) > 1").fetchall():
        out.append(f'duplicate annotation id {i} ({n} rows)')
    for (i,) in con.execute(f"SELECT id FROM {name} WHERE id IS NULL OR date_from IS NULL OR "
                            f"(date_to IS NOT NULL AND date_to < date_from) OR kind NOT IN "
                            f"('collection', 'platform', 'software', 'archive') OR text IS NULL").fetchall():
        out.append(f'annotation {i}: missing id/date_from/text, date_to < date_from, or unknown kind')
    rirs = set(common.REGISTRIES) | {'all'}
    for i, rs, ms, ps in con.execute(f"SELECT id, rirs, metrics, populations FROM {name}").fetchall():
        if not rs or set(rs) - rirs:
            out.append(f'annotation {i}: unknown rir {sorted(set(rs or []) - rirs)}')
        if set(ms) - set(defs.METRICS):
            out.append(f'annotation {i}: unknown metric {sorted(set(ms) - set(defs.METRICS))}')
        if set(ps) - set(POPULATIONS):
            out.append(f'annotation {i}: unknown population {sorted(set(ps) - set(POPULATIONS))}')
    return out


def week_status_preview(as_of, week, metric_id, stat):
    """(published, final) of a (week, metric, stat) as of as_of, without the ledger (for logs and tests)."""
    r = defs.rule_for(metric_id, stat)
    a = common.parse_ts(as_of)
    w = dt.datetime.combine(common.to_date(week), dt.time())
    return (a >= w + dt.timedelta(days=7 * r['publish_after']), a >= w + dt.timedelta(days=7 * r['final_after']))
