"""Day-interval SQL shared by the VRP input steps (vrp_export, vrp_subtal, ripe_archive).

Method: per-VRP day intervals with two-tier gap bridging, plus per-publication-point (host) bridging:
- islands_sql: merge overlapping or touching day ranges per key (gaps-and-islands).
- valid_islands_sql: islands_sql that also keeps, per island, the validity of the objects at its two
  ends: valid_from = earliest EE not_before among the objects present on its first day, valid_until =
  latest end of validity (least(EE not_after, CRL revocation)) among the objects present on its last day.
- calendar_sql: per group (RIR) and UTC day: n(d) = raw VRP count, has_snap, the centred 61-day
  rolling median, bad = NOT has_snap OR n < 0.9 * median, cum_good = running count of good days.
- host_calendar_sql: per (group, host) and UTC day: n(d) = the host's raw VRP count, ref = least(max n
  over the 31 days before d, max n over the 31 days after d), dip = n < 0.5 * ref, and
  bad = NOT has_snap OR (dip AND ref >= 20 AND the publication point is missing that day), where the
  publication point is missing when the host dips in some group (RIR) in which its ref >= 150. One
  repository serves every trust anchor, so the RIR parts of a host fall out together: a part of 20-149
  VRPs is bad only on days when a part of >= 150 VRPs of the same host dips too. Only a dip far below the
  level seen both before AND after is bad, so ramps, permanent drops (repository migrations, real mass
  withdrawals) are never flagged, and neither are hosts whose parts all stay below 150 VRPs: in the
  RIPE NCC archive their collapses were mostly real repository outages (independent validators lacked
  the same ROAs), while the national registries' (>= 167 VRPs) were archive dropouts. (A centred median
  would miss a host that is missing on most days of the window, as TWNIC was in the archive in 2021.)
- merge_sql: islands_sql that also records the host starting and ending each merged row.
- bridge_sql: two-tier gap bridging of raw intervals of the same VRP:
    tier 1: always close gaps of <= 3 days;
    tier 2: close gaps of 4..31 days when every gap day is a bad day for that group (or, with a
            host calendar, when every gap day is bad for that host; with cross_col, only for a gap
            between rows of different hosts, as used per registrable domain).
  With guard=True (validity guard, host/domain calendars only) the host rule does not close a gap
  that holds a whole UTC day on which no object carrying the VRP was valid, while the archive saw the
  host (or domain) that day (n > 0): every object present on the day before the gap had expired (EE
  not_after) or been revoked (CRL) before that day, and no object present on the day after the gap
  had begun its EE validity (not_before) by the end of it. Such a gap is a ROA lapse (CNNIC 2021-04-30..
  05-26 and 2022-05-28..06-06: EE certificates expired while the archive kept fetching the repository),
  not a collection gap. Days on which the host was not seen at all do not count: a CA that re-issues a
  ROA during a dropout (old EE revoked, replacement replaced again before the archive returns) leaves a
  hole the archive cannot fill. The group (RIR) rule is never guarded, so the guard only takes back
  days the host rule added.
  bridged_days = number of gap days closed inside the output row.
- exact_bridged_sql: bridged_days recomputed as the days of a row not covered by the raw intervals
  (used after rows of several hosts were merged, where per-row sums would count a day twice).
- uncovered_sql: rows of one interval table whose days are not all inside another (superset check).
- invariant_sql / check_invariants: bridged rows of one (group, key) never overlap or touch,
  first_day <= last_day and bridged_days >= 0; the writers check this before writing.
The *_sql functions return SQL text; inputs are table names or parenthesised subqueries.
"""

VRP_KEY = ('prefix', 'max_length', 'asn')
TIER1_DAYS = 3
TIER2_DAYS = 31
BAD_SHARE = 0.9
MEDIAN_HALF_WINDOW = 30   # 61-day centred window
HOST_WINDOW = 31          # host calendar: ref = least(max n over [d-31, d-1], max n over [d+1, d+31])
HOST_BAD_SHARE = 0.5      # a host day is bad when n < 0.5 * ref (a dip) ...
HOST_MIN_REF = 150        # ... and the host dips in some RIR where its ref >= 150 VRPs (publication point missing)
HOST_PART_MIN_REF = 20    # ... and this RIR's part of the host has ref >= 20 VRPs
UNKNOWN_HOST = '?'        # objects without a known publication point; the ratio rule is off for them


def islands_sql(src, keys, lo='d_lo', hi='d_hi', extra=()):
    """One row per (keys, island): min(lo) AS first_day, max(hi) AS last_day.

    Ranges of the same key that overlap or touch (next lo <= previous max hi + 1) are merged.
    `extra` columns are carried with any_value().

    The island flag (inner window) and its running sum (outer window) are two separate sorts, so
    ORDER BY lo, hi must be a total order inside each partition: (keys, lo, hi) is deduplicated
    first. Without that, rows tying on (lo, hi) (several runs of one object on the same day,
    several objects carrying the same VRP) can be ordered differently by the two passes, and a
    tied row that starts a new island is counted into the previous one, which then spans the gap.
    """
    k = ', '.join(keys)
    ex = ''.join(f', any_value({c}) AS {c}' for c in extra)
    return f"""
SELECT {k}{ex}, min({lo}) AS first_day, max({hi}) AS last_day FROM (
  SELECT *, sum(CASE WHEN prev_hi_ IS NULL OR {lo} > prev_hi_ + 1 THEN 1 ELSE 0 END)
            OVER (PARTITION BY {k} ORDER BY {lo}, {hi} ROWS UNBOUNDED PRECEDING) AS isl_
  FROM (SELECT *, max({hi}) OVER (PARTITION BY {k} ORDER BY {lo}, {hi}
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_hi_
        FROM (SELECT {k}{ex}, {lo}, {hi} FROM {src} GROUP BY {k}, {lo}, {hi})))
GROUP BY {k}, isl_"""


def valid_islands_sql(src, keys, lo='d_lo', hi='d_hi', extra=(), vfrom='valid_from', vuntil='valid_until'):
    """islands_sql of `src` rows that carry a validity window (vfrom, vuntil TIMESTAMP), one row per
    object (or merged row) and day range.

    Output: keys..., extra..., first_day, last_day, valid_from = min(vfrom) over the rows starting on
    first_day, valid_until = max(vuntil) over the rows ending on last_day (the objects present on the
    island's first and last day; NULL values are skipped, and NULL when all are NULL, which the guard
    treats as unknown: no hole). Rows are deduplicated on (keys, lo, hi) first, as in islands_sql, so
    the result does not depend on the input order.
    """
    k = ', '.join(keys)
    ex = ''.join(f', any_value({c}) AS {c}' for c in extra)
    return f"""
SELECT {k}{ex}, min({lo}) AS first_day, max({hi}) AS last_day,
       min(vf_) FILTER (WHERE {lo} = isl_lo_) AS valid_from, max(vu_) FILTER (WHERE {hi} = isl_hi_) AS valid_until
FROM (
  SELECT *, min({lo}) OVER (PARTITION BY {k}, isl_) AS isl_lo_, max({hi}) OVER (PARTITION BY {k}, isl_) AS isl_hi_
  FROM (
    SELECT *, sum(CASE WHEN prev_hi_ IS NULL OR {lo} > prev_hi_ + 1 THEN 1 ELSE 0 END)
              OVER (PARTITION BY {k} ORDER BY {lo}, {hi} ROWS UNBOUNDED PRECEDING) AS isl_
    FROM (SELECT *, max({hi}) OVER (PARTITION BY {k} ORDER BY {lo}, {hi}
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_hi_
          FROM (SELECT {k}{ex}, {lo}, {hi}, min({vfrom}) AS vf_, max({vuntil}) AS vu_
                FROM {src} GROUP BY {k}, {lo}, {hi}))))
GROUP BY {k}, isl_"""


def hole_range_sql(prev_last='prev_last', first_day='first_day', prev_until='prev_until_', valid_from='valid_from'):
    """(first, last) SQL date expressions of the validity hole of the gap (prev_last, first_day): the whole UTC
    days d with date(prev_until) < d < date(valid_from) inside the gap, where prev_until = end of validity of
    the objects present on prev_last and valid_from = start of validity of those present on first_day.
    There is a hole when first <= last; NULL validity (unknown) gives NULL, i.e. no hole (greatest() and
    least() skip NULL arguments, so the NULL case is explicit)."""
    unknown = f"{prev_until} IS NULL OR {valid_from} IS NULL"
    return (f"(CASE WHEN {unknown} THEN NULL ELSE greatest({prev_last} + 1, CAST({prev_until} AS DATE) + 1) END)",
            f"(CASE WHEN {unknown} THEN NULL ELSE least({first_day} - 1, CAST({valid_from} AS DATE) - 1) END)")


def hole_days_sql(prev_last='prev_last', first_day='first_day', prev_until='prev_until_', valid_from='valid_from'):
    """SQL expression: number of days of the validity hole (hole_range_sql) of a gap, 0 if none."""
    a, b = hole_range_sql(prev_last, first_day, prev_until, valid_from)
    return f"coalesce(greatest(0, {b} - {a} + 1), 0)"


def calendar_sql(raw, snapdays, day_from, day_to, group_col='grp'):
    """Per (grp, d) for every day in [day_from, day_to] and every group present in `snapdays`.

    raw: (group_col, first_day, last_day) raw (unbridged) intervals.
    snapdays: (grp, d) days with at least one snapshot.
    Days before the first raw event carry n = 0.
    """
    return f"""
WITH ev AS (
  SELECT {group_col} AS grp, first_day AS d, 1 AS dn FROM {raw}
  UNION ALL SELECT {group_col} AS grp, last_day + 1 AS d, -1 AS dn FROM {raw}),
daily AS (SELECT grp, d, sum(dn) AS dn FROM ev GROUP BY grp, d),
run AS (SELECT grp, d, sum(dn) OVER (PARTITION BY grp ORDER BY d ROWS UNBOUNDED PRECEDING) AS n FROM daily),
days AS (SELECT g.grp, CAST(unnest(range(DATE '{day_from}', DATE '{day_to}' + INTERVAL 1 DAY, INTERVAL 1 DAY)) AS DATE) AS d
         FROM (SELECT DISTINCT grp FROM {snapdays}) g),
f AS (SELECT days.grp, days.d,
             coalesce(last_value(run.n IGNORE NULLS) OVER (PARTITION BY days.grp ORDER BY days.d ROWS UNBOUNDED PRECEDING), 0) AS n,
             sd.d IS NOT NULL AS has_snap
      FROM days LEFT JOIN run ON run.grp = days.grp AND run.d = days.d
                LEFT JOIN (SELECT DISTINCT grp, d FROM {snapdays}) sd ON sd.grp = days.grp AND sd.d = days.d),
m AS (SELECT *, median(n) OVER (PARTITION BY grp ORDER BY d
                                ROWS BETWEEN {MEDIAN_HALF_WINDOW} PRECEDING AND {MEDIAN_HALF_WINDOW} FOLLOWING) AS med
      FROM f)
SELECT grp, d, n, med, has_snap, (NOT has_snap OR n < {BAD_SHARE} * med) AS bad,
       sum(CASE WHEN (NOT has_snap OR n < {BAD_SHARE} * med) THEN 0 ELSE 1 END)
         OVER (PARTITION BY grp ORDER BY d ROWS UNBOUNDED PRECEDING) AS cum_good
FROM m"""


def host_calendar_sql(raw, snapdays, group_col='grp', sub_col='host', window=HOST_WINDOW,
                      share=HOST_BAD_SHARE, min_ref=HOST_MIN_REF, part_min_ref=HOST_PART_MIN_REF,
                      unknown=UNKNOWN_HOST):
    """Per (grp, sub, d) for every day of each sub-group's span [first raw day, last raw day].

    raw: (group_col, sub_col, keys..., first_day, last_day) raw intervals, merged per (group, sub, key),
    so n(d) = number of raw rows of (grp, sub) covering d = the sub-group's VRPs on d.
    snapdays: (grp, d) days on which the group (RIR) has at least one snapshot.
    pmax = max n over [d-window, d-1], fmax = max n over [d+1, d+window], 0 where the window holds no
    day of the span (outside the span n is 0); ref = least(pmax, fmax); dip = n < share * ref.
    missing(sub, d) = some group's part of the sub-group dips on d with ref >= min_ref (the publication
    point, or the domain, is missing that day; it serves every group/trust anchor).
    bad = NOT has_snap OR (dip AND ref >= part_min_ref AND missing(sub, d)); the ratio test is off for
    the `unknown` sub-group, so objects without a publication point keep the group-only rule. With
    min_ref = part_min_ref this is the per-(group, sub) rule ref >= min_ref AND dip.
    A gap of a VRP of (grp, sub) lies between two raw days of that sub-group, so the span holds
    every day bridge_sql looks up.
    cum_present = running count of days with n > 0 (the sub-group was seen; used by the validity guard).
    Output: grp, sub, d, n, pmax, fmax, ref, has_snap, bad, cum_good, cum_present.
    """
    unk = "'" + unknown.replace("'", "''") + "'"
    return f"""
WITH ev AS (
  SELECT {group_col} AS grp, {sub_col} AS sub, first_day AS d, 1 AS dn FROM {raw}
  UNION ALL SELECT {group_col} AS grp, {sub_col} AS sub, last_day + 1 AS d, -1 AS dn FROM {raw}),
daily AS (SELECT grp, sub, d, sum(dn) AS dn FROM ev GROUP BY grp, sub, d),
run AS (SELECT grp, sub, d, sum(dn) OVER (PARTITION BY grp, sub ORDER BY d ROWS UNBOUNDED PRECEDING) AS n FROM daily),
span AS (SELECT {group_col} AS grp, {sub_col} AS sub, min(first_day) AS lo, max(last_day) AS hi FROM {raw} GROUP BY ALL),
days AS (SELECT grp, sub, CAST(unnest(range(lo, hi + INTERVAL 1 DAY, INTERVAL 1 DAY)) AS DATE) AS d FROM span),
f AS (SELECT days.grp, days.sub, days.d,
             coalesce(last_value(run.n IGNORE NULLS) OVER (PARTITION BY days.grp, days.sub ORDER BY days.d
                                                           ROWS UNBOUNDED PRECEDING), 0) AS n,
             sd.d IS NOT NULL AS has_snap
      FROM days LEFT JOIN run ON run.grp = days.grp AND run.sub = days.sub AND run.d = days.d
                LEFT JOIN (SELECT DISTINCT grp, d FROM {snapdays}) sd ON sd.grp = days.grp AND sd.d = days.d),
w AS (SELECT *,
             coalesce(max(n) OVER (PARTITION BY grp, sub ORDER BY d ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING), 0) AS pmax,
             coalesce(max(n) OVER (PARTITION BY grp, sub ORDER BY d ROWS BETWEEN 1 FOLLOWING AND {window} FOLLOWING), 0) AS fmax
      FROM f),
x AS (SELECT *, least(pmax, fmax) AS ref, n < {share} * least(pmax, fmax) AS dip_ FROM w),
m AS (SELECT sub, d, bool_or(dip_ AND ref >= {min_ref}) AS missing_ FROM x GROUP BY sub, d),
b AS (SELECT x.*,
             (NOT has_snap OR (sub <> {unk} AND dip_ AND ref >= {part_min_ref} AND m.missing_)) AS bad
      FROM x JOIN m USING (sub, d))
SELECT grp, sub, d, n, pmax, fmax, ref, has_snap, bad,
       sum(CASE WHEN bad THEN 0 ELSE 1 END) OVER (PARTITION BY grp, sub ORDER BY d ROWS UNBOUNDED PRECEDING) AS cum_good,
       sum(CASE WHEN n > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY grp, sub ORDER BY d ROWS UNBOUNDED PRECEDING) AS cum_present
FROM b"""


def merge_sql(src, keys, carry, lo='first_day', hi='last_day', extra=('afi',), valid=False):
    """islands_sql that also records which `carry` value (host) starts and ends each island.

    Ranges of the same keys that overlap or touch are merged. Rows are deduplicated on
    (keys, carry, lo, hi) first, so ORDER BY lo, hi, carry is a total order inside each partition and
    the result does not depend on the input order (see islands_sql).
    Output: keys..., extra..., first_day, last_day, first_<carry> (smallest carry value among the rows
    starting on first_day), last_<carry> (smallest among the rows ending on last_day).
    valid=True: `src` also has valid_from / valid_until (valid_islands_sql, bridge_sql guard=True); the
    output keeps the earliest valid_from of the rows starting on first_day and the latest valid_until
    of the rows ending on last_day.
    """
    k = ', '.join(keys)
    ex = ''.join(f', any_value({c}) AS {c}' for c in extra)
    vsel = (",\n       min(valid_from) FILTER (WHERE {lo} = isl_lo_) AS valid_from,"
            " max(valid_until) FILTER (WHERE {hi} = isl_hi_) AS valid_until").format(lo=lo, hi=hi) if valid else ''
    vagg = ', min(valid_from) AS valid_from, max(valid_until) AS valid_until' if valid else ''
    return f"""
SELECT {k}{ex}, min({lo}) AS first_day, max({hi}) AS last_day,
       min({carry}) FILTER (WHERE {lo} = isl_lo_) AS first_{carry},
       min({carry}) FILTER (WHERE {hi} = isl_hi_) AS last_{carry}{vsel}
FROM (
  SELECT *, min({lo}) OVER (PARTITION BY {k}, isl_) AS isl_lo_, max({hi}) OVER (PARTITION BY {k}, isl_) AS isl_hi_
  FROM (
    SELECT *, sum(CASE WHEN prev_hi_ IS NULL OR {lo} > prev_hi_ + 1 THEN 1 ELSE 0 END)
              OVER (PARTITION BY {k} ORDER BY {lo}, {hi}, {carry} ROWS UNBOUNDED PRECEDING) AS isl_
    FROM (SELECT *, max({hi}) OVER (PARTITION BY {k} ORDER BY {lo}, {hi}, {carry}
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_hi_
          FROM (SELECT {k}{ex}, {carry}, {lo}, {hi}{vagg} FROM {src} GROUP BY {k}, {carry}, {lo}, {hi}))))
GROUP BY {k}, isl_"""


def _tier2_parts(cal, group_col, sub_col=None, sub_cal=None, cross_col=None, guard=False):
    """(condition 'every gap day is bad', joins) for the tier-2 test of bridge_sql / gap stats.
    With cross_col, the sub_cal test only counts for a gap between rows of different cross_col values
    (last_<cross_col> of the row before the gap <> first_<cross_col> of the row after it).
    With guard, the sub_cal test also needs a gap without a seen validity hole (_seen_hole_g() = 0; g has
    prev_last, prev_until_, first_day, valid_from). The group (cal) test is never guarded."""
    if not cal and not sub_cal:
        return None, ''
    conds, joins = [], []
    if cal:
        conds.append('coalesce(c1.cum_good - c0.cum_good = 0, false)')
        joins.append(f"LEFT JOIN {cal} c0 ON c0.grp = g.{group_col} AND c0.d = g.prev_last "
                     f"LEFT JOIN {cal} c1 ON c1.grp = g.{group_col} AND c1.d = g.first_day - 1")
    if sub_cal:
        cross = f' AND coalesce(g.prev_cross_ <> g.first_{cross_col}, false)' if cross_col else ''
        hole = f' AND {_seen_hole_g()} = 0' if guard else ''
        conds.append(f'(coalesce(h1.cum_good - h0.cum_good = 0, false){cross}{hole})')
        joins.append(f"LEFT JOIN {sub_cal} h0 ON h0.grp = g.{group_col} AND h0.sub = g.{sub_col} AND h0.d = g.prev_last "
                     f"LEFT JOIN {sub_cal} h1 ON h1.grp = g.{group_col} AND h1.sub = g.{sub_col} AND h1.d = g.first_day - 1")
        if guard:
            joins.append(_seen_hole_joins(sub_cal, group_col, sub_col))
    return '(' + ' OR '.join(conds) + ')', ' '.join(joins)


def _hole_g():
    return hole_days_sql('g.prev_last', 'g.first_day', 'g.prev_until_', 'g.valid_from')


def _seen_hole_joins(sub_cal, group_col, sub_col):
    """Joins q0/q1 on the sub-group calendar at the day before the hole and at its last day."""
    a, b = hole_range_sql('g.prev_last', 'g.first_day', 'g.prev_until_', 'g.valid_from')
    return (f"LEFT JOIN {sub_cal} q0 ON q0.grp = g.{group_col} AND q0.sub = g.{sub_col} AND q0.d = {a} - 1 "
            f"LEFT JOIN {sub_cal} q1 ON q1.grp = g.{group_col} AND q1.sub = g.{sub_col} AND q1.d = {b}")


def _seen_hole_g():
    """Days of the validity hole on which the archive saw the sub-group (host or domain: n > 0), from the
    q0/q1 joins: only then would a replacement object have been seen. During a total dropout of the
    host a re-issue (old EE revoked, replacement never seen) looks like a hole, so those days do not count
    (real data: of 205 such gaps, RPKIviews or the RIPE NCC archive had the VRP in 202: freerangecloud
    2023-05, CNNIC 2022-01, r.magellan.ipxo.com 2026; 2 TWNIC VRPs of 2022-01 were really absent)."""
    a, b = hole_range_sql('g.prev_last', 'g.first_day', 'g.prev_until_', 'g.valid_from')
    return f"(CASE WHEN {a} <= {b} THEN coalesce(q1.cum_present, 0) - coalesce(q0.cum_present, 0) ELSE 0 END)"


def bridge_sql(raw, cal=None, group_col='grp', keys=VRP_KEY, extra=('afi',),
               tier1=TIER1_DAYS, tier2=TIER2_DAYS, sub_col=None, sub_cal=None, cross_col=None, guard=False):
    """Bridge gaps between raw intervals of the same (group, keys), or (group, sub, keys) with sub_col.

    raw: (group_col, [sub_col,] keys..., extra..., first_day, last_day), already merged per key (no overlaps).
    cal: calendar table from calendar_sql (columns grp, d, cum_good) or None for tier 1 only.
    sub_col / sub_cal: bridge per (group, sub, key); a 4..31-day gap is also closed when every gap day
    is bad in sub_cal (host_calendar_sql: columns grp, sub, d, cum_good) for that (group, sub).
    cross_col: raw also has first_<cross_col> / last_<cross_col> (merge_sql); the sub_cal test then
    only closes a gap between rows of different cross_col values (a VRP that moved between hosts).
    guard (with sub_cal, from host_calendar_sql with cum_present): raw also has valid_from / valid_until
    (valid_islands_sql); the sub_cal test does not close a gap with a validity hole (hole_range_sql) that
    holds a day on which the sub-group was seen (n > 0). The output then keeps valid_from of its first
    raw row and valid_until of its last one.
    Output: group_col, [sub_col,] keys..., extra..., first_day, last_day, bridged_days INTEGER
    [, valid_from, valid_until].
    """
    k = ', '.join(keys)
    part = f'{group_col}, {sub_col}, {k}' if sub_col else f'{group_col}, {k}'
    ex = ''.join(f', any_value({c}) AS {c}' for c in extra)
    guard = bool(guard and sub_col and sub_cal)
    lag_cross = ''
    if sub_col and sub_cal:
        cond, joins = _tier2_parts(cal, group_col, sub_col, sub_cal, cross_col, guard)
        tier2_case = f"WHEN first_day - prev_last - 1 <= {tier2} AND {cond} THEN 0"
        if cross_col:
            lag_cross = f", lag(last_{cross_col}) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_cross_"
        if guard:
            lag_cross += f", lag(valid_until) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_until_"
    elif cal:
        tier2_case = (f"WHEN first_day - prev_last - 1 <= {tier2} AND c1.cum_good - c0.cum_good = 0 THEN 0")
        joins = (f"LEFT JOIN {cal} c0 ON c0.grp = g.{group_col} AND c0.d = g.prev_last "
                 f"LEFT JOIN {cal} c1 ON c1.grp = g.{group_col} AND c1.d = g.first_day - 1")
    else:
        tier2_case, joins = '', ''
    # arg_*_null: keep the first/last row's value even when it is NULL (arg_min skips NULL values)
    vsel = (",\n       arg_min_null(valid_from, first_day) AS valid_from, arg_max_null(valid_until, last_day) AS valid_until"
            if guard else '')
    return f"""
WITH g AS (SELECT *, lag(last_day) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_last{lag_cross} FROM {raw}),
f AS (SELECT g.*, (first_day - prev_last - 1) AS gap_,
             CASE WHEN prev_last IS NULL THEN 1
                  WHEN first_day - prev_last - 1 <= {tier1} THEN 0
                  {tier2_case}
                  ELSE 1 END AS new_isl_
      FROM g {joins})
SELECT {part}{ex}, min(first_day) AS first_day, max(last_day) AS last_day,
       CAST(sum(CASE WHEN new_isl_ = 0 THEN gap_ ELSE 0 END) AS INTEGER) AS bridged_days{vsel}
FROM (SELECT *, sum(new_isl_) OVER (PARTITION BY {part} ORDER BY first_day ROWS UNBOUNDED PRECEDING) AS isl_ FROM f)
GROUP BY {part}, isl_"""


def gap_stats_sql(raw, cal=None, group_col='grp', keys=VRP_KEY, tier1=TIER1_DAYS, tier2=TIER2_DAYS):
    """Counts of gaps closed by tier 1, by tier 2 and left open (diagnostics)."""
    k = ', '.join(keys)
    part = f'{group_col}, {k}'
    if cal:
        t2 = (f"count(*) FILTER (WHERE gap_ > {tier1} AND gap_ <= {tier2} AND c1.cum_good - c0.cum_good = 0)")
        joins = (f"LEFT JOIN {cal} c0 ON c0.grp = g.{group_col} AND c0.d = g.prev_last "
                 f"LEFT JOIN {cal} c1 ON c1.grp = g.{group_col} AND c1.d = g.first_day - 1")
    else:
        t2, joins = '0', ''
    return f"""
WITH g AS (SELECT *, lag(last_day) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_last FROM {raw}),
h AS (SELECT g.*, (first_day - prev_last - 1) AS gap_ FROM g WHERE prev_last IS NOT NULL)
SELECT count(*) FILTER (WHERE gap_ <= {tier1}) AS tier1, {t2} AS tier2, count(*) AS gaps
FROM h g {joins}"""


def hole_stats_sql(raw, cal, group_col='grp', keys=VRP_KEY, tier1=TIER1_DAYS, tier2=TIER2_DAYS):
    """Diagnostic for the (unguarded) group rule: the 4..31-day gaps it closes (every gap day bad for the
    group) that have a validity hole. raw has valid_from / valid_until (valid_islands_sql).
    One row: gaps, gap_days, hole_days."""
    part = f"{group_col}, {', '.join(keys)}"
    return f"""
WITH g AS (SELECT *, lag(last_day) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_last,
                  lag(valid_until) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_until_ FROM {raw}),
h AS (SELECT g.*, (first_day - prev_last - 1) AS gap_, {_hole_g()} AS hole_ FROM g WHERE prev_last IS NOT NULL)
SELECT count(*) AS gaps, coalesce(sum(gap_), 0) AS gap_days, coalesce(sum(hole_), 0) AS hole_days
FROM h g LEFT JOIN {cal} c0 ON c0.grp = g.{group_col} AND c0.d = g.prev_last
         LEFT JOIN {cal} c1 ON c1.grp = g.{group_col} AND c1.d = g.first_day - 1
WHERE gap_ > {tier1} AND gap_ <= {tier2} AND c1.cum_good - c0.cum_good = 0 AND hole_ > 0"""


def sub_gap_stats_sql(raw, cal, sub_cal, group_col='grp', sub_col='host', keys=VRP_KEY,
                      tier1=TIER1_DAYS, tier2=TIER2_DAYS, cross_col=None, guard=False):
    """Per (group, sub): gaps, tier1, tier2 (closed by tier 2: every gap day bad for the sub-group or
    for the group), tier2_sub_only (closed only because of sub_cal), tier2_days (gap days closed by
    tier 2), refused_hole / refused_hole_days (guard: gaps, and their days, that the sub_cal test alone
    would have closed but that have a seen validity hole; they stay open). cross_col and guard as in
    bridge_sql. Rows are ordered by group, sub."""
    part = f"{group_col}, {sub_col}, {', '.join(keys)}"
    cond_any, joins = _tier2_parts(cal, group_col, sub_col, sub_cal, cross_col, guard)
    cond_grp = 'coalesce(c1.cum_good - c0.cum_good = 0, false)' if cal else 'false'
    cross = f' AND coalesce(g.prev_cross_ <> g.first_{cross_col}, false)' if cross_col else ''
    cond_sub_any = f'(coalesce(h1.cum_good - h0.cum_good = 0, false){cross})' if sub_cal else 'false'
    seen = _seen_hole_g() if guard else '0'
    cond_sub = f'({cond_sub_any} AND {seen} = 0)' if guard else cond_sub_any
    cond_any = cond_any or 'false'
    lag_cross = (f", lag(last_{cross_col}) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_cross_"
                 if cross_col else '')
    if guard:
        lag_cross += f", lag(valid_until) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_until_"
    t2 = f"gap_ > {tier1} AND gap_ <= {tier2} AND {cond_any}"
    refused = f"gap_ > {tier1} AND gap_ <= {tier2} AND {cond_sub_any} AND NOT {cond_grp} AND {seen} > 0"
    return f"""
WITH g AS (SELECT *, lag(last_day) OVER (PARTITION BY {part} ORDER BY first_day) AS prev_last{lag_cross} FROM {raw}),
h AS (SELECT g.*, (first_day - prev_last - 1) AS gap_ FROM g WHERE prev_last IS NOT NULL)
SELECT g.{group_col} AS grp, g.{sub_col} AS sub, count(*) AS gaps,
       count(*) FILTER (WHERE gap_ <= {tier1}) AS tier1,
       count(*) FILTER (WHERE {t2}) AS tier2,
       count(*) FILTER (WHERE gap_ > {tier1} AND gap_ <= {tier2} AND {cond_sub} AND NOT {cond_grp}) AS tier2_sub_only,
       coalesce(sum(gap_) FILTER (WHERE {t2}), 0) AS tier2_days,
       count(*) FILTER (WHERE {refused}) AS refused_hole,
       coalesce(sum(gap_) FILTER (WHERE {refused}), 0) AS refused_hole_days
FROM h g {joins}
GROUP BY g.{group_col}, g.{sub_col} ORDER BY 1, 2"""


def exact_bridged_sql(bridged, raw, group_col='grp', keys=VRP_KEY, extra=('afi',)):
    """`bridged` rows with bridged_days = days of the row on which `raw` has no interval.

    raw: unbridged intervals of the same (group, keys), merged per key (no overlaps), each lying inside
    one row of `bridged` (bridging only adds days). Rows of `bridged` without any raw interval are
    dropped, so callers compare row counts.
    """
    part = (group_col, *keys)
    on = ' AND '.join(f'b.{c} = r.{c}' for c in part)
    cols = ', '.join(f'b.{c}' for c in (*part, *extra))
    return f"""
SELECT * EXCLUDE (present_), CAST((last_day - first_day + 1) - present_ AS INTEGER) AS bridged_days FROM (
  SELECT {cols}, b.first_day, b.last_day, sum(r.last_day - r.first_day + 1) AS present_
  FROM {raw} r ASOF JOIN {bridged} b ON {on} AND b.first_day <= r.first_day
  GROUP BY ALL)"""


def uncovered_sql(inner, outer, group_col='grp', keys=VRP_KEY):
    """One row (uncovered, rows): rows of `inner` with a day outside `outer` for the same (group, keys).

    `outer` rows of one (group, key) must not overlap (bridged output), so the only row that can
    contain an inner row is the one with the greatest first_day <= the inner row's first_day.
    uncovered = 0 means every (VRP, day) of `inner` is a (VRP, day) of `outer`.
    """
    on = ' AND '.join(f'o.{c} = i.{c}' for c in (group_col, *keys))
    return f"""
SELECT count(*) FILTER (WHERE o.last_day IS NULL OR o.last_day < i.last_day) AS uncovered, count(*) AS rows
FROM {inner} i ASOF LEFT JOIN {outer} o ON {on} AND o.first_day <= i.first_day"""


def invariant_sql(table, group_col='grp', keys=VRP_KEY):
    """Violations of the interval invariants of a bridged (or raw) table, as one row:
    overlapping = rows that overlap or touch an earlier row of the same (group, key) (islands and
    bridging must leave a gap of >= 1 day between rows), inverted = first_day > last_day,
    negative_bridged = bridged_days < 0. All three must be 0.
    group_col may name several columns ('rir, host')."""
    part = f"{group_col}, {', '.join(keys)}"
    return f"""
SELECT count(*) FILTER (WHERE first_day <= prev_max_ + 1) AS overlapping,
       count(*) FILTER (WHERE first_day > last_day) AS inverted,
       count(*) FILTER (WHERE bridged_days < 0) AS negative_bridged
FROM (SELECT first_day, last_day, bridged_days,
             max(last_day) OVER (PARTITION BY {part} ORDER BY first_day, last_day
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max_
      FROM {table})"""


def check_invariants(con, table, group_col='grp', keys=VRP_KEY, label='intervals'):
    """Raise SystemExit (nothing is written) if `table` violates the invariants of invariant_sql."""
    ov, inv, neg = con.execute(invariant_sql(table, group_col, keys)).fetchone()
    if ov or inv or neg:
        raise SystemExit(f'{label}: interval invariants violated in {table}: {ov} overlapping/touching rows, '
                         f'{inv} with first_day > last_day, {neg} with bridged_days < 0; not writing')
    return {'overlapping': ov, 'inverted': inv, 'negative_bridged': neg}
