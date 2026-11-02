"""Framework stub metric (stub_crl_count): CRLs per week and population, to run the pipeline end to end.

Not a scorecard metric (defs 'stub': True, only computed when named with --metrics). It shows the module
interface of scs/metrics.py in its smallest form: one grouped pass over a warehouse table with ctx.run(),
aggregation per (week, issuing key) before the population fan-out (classify.populations_sql on the CA's
own publication host from ca_keys), a zero row for population 'all' in every week of the range, n / n_ca /
top1_share per row, and a histogram.

  crls     value = n = CRL rows with thisUpdate in the week (one row per CRL object in table=crls);
           n_ca = their distinct issuing keys (crls.aki)
  ca_keys  value = n_ca
  window_h histogram: 24-hour bins of nextUpdate - thisUpdate, [0, 24) .. [168, 192), overflow [192, NULL)
"""
from . import classify
from .common import date_lit, lit

BIN_H, LAST_BIN = 24, 8


def compute(ctx, rir, week_from, week_to):
    t0, t1 = ctx.week_bounds(week_from, week_to)
    con = ctx.connect()
    try:
        ctx.load_hostmap(con)
        ctx.load_ca_keys(con)
        crls = ctx.src(rir, 'crls', ['aki', 'this_update', 'next_update'])
        # per (week, issuing key, window bin): week = Monday of thisUpdate in UTC
        ctx.run(con, f"""CREATE TEMP TABLE c AS
            SELECT scs_week(this_update) AS week, aki,
                   CAST(least(greatest(floor(epoch(next_update - this_update) / 3600.0 / {BIN_H}), 0), {LAST_BIN})
                        AS INTEGER) AS b,
                   count(*) AS n
            FROM {crls} WHERE this_update >= {t0} AND this_update < {t1} GROUP BY ALL""")
        con.execute(f"""CREATE TEMP TABLE cp AS {classify.populations_sql(
            '(SELECT c.*, k.host FROM c LEFT JOIN ca_keys k ON k.ski = c.aki)', 'host', rir)}""")
        con.execute("""CREATE TEMP TABLE agg AS
            WITH per_key AS (SELECT week, population, operator, aki, sum(n) AS n_key FROM cp GROUP BY ALL)
            SELECT week, population, operator, sum(n_key) AS n, count(*) AS n_ca,
                   max(n_key) / sum(n_key) AS top1_share
            FROM per_key GROUP BY ALL""")
        # population 'all' in every week of the range, also when the week has no CRL
        con.execute(f"""CREATE TEMP TABLE frame AS
            SELECT CAST(range AS DATE) AS week, 'all' AS population, '' AS operator
            FROM range(CAST({date_lit(week_from)} AS TIMESTAMP), CAST({date_lit(week_to)} AS TIMESTAMP) + INTERVAL 7 DAY,
                       INTERVAL 7 DAY)""")
        con.execute("""CREATE TEMP TABLE agg2 AS
            SELECT f.week, f.population, f.operator, coalesce(a.n, 0) AS n, coalesce(a.n_ca, 0) AS n_ca, a.top1_share
            FROM frame f LEFT JOIN agg a USING (week, population, operator)
            UNION ALL
            SELECT week, population, operator, n, n_ca, top1_share FROM agg WHERE population <> 'all'""")
        r = lit(rir)
        rows = f"""
            SELECT week, '1w' AS "window", {r} AS rir, population, operator, 'stub_crl_count' AS metric_id,
                   s.stat, CASE s.stat WHEN 'crls' THEN n ELSE n_ca END AS value, n, n_ca, top1_share
            FROM agg2 CROSS JOIN (VALUES ('crls'), ('ca_keys')) s(stat)"""
        hist = f"""
            SELECT week, '1w' AS "window", {r} AS rir, population, operator, 'stub_crl_count' AS metric_id,
                   'window_h' AS stat, b * {BIN_H} AS bin_lo, CASE WHEN b < {LAST_BIN} THEN (b + 1) * {BIN_H} END AS bin_hi,
                   sum(n) AS count
            FROM cp WHERE b IS NOT NULL GROUP BY ALL"""
        ctx.emit(con, rows, hist)
    finally:
        con.close()
