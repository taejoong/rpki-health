"""CRL and manifest freshness windows, empty CRLs: crl_window, mft_window, crl_empty.

Objects of a week: the CRLs (table=crls) and manifests (table=manifests) whose thisUpdate falls in the ISO
week (UTC), one row per object, i.e. every re-issue we observed (era 2: a cycle every 20 minutes; era 1:
one archive snapshot a day). The window is nextUpdate - thisUpdate in whole seconds, reported in hours.
A CRL without a nextUpdate has no window and counts in crl_empty only; manifest times are ISO text and
are cast, and a text that does not parse leaves no window (neither occurs in the warehouses on
2026-10-29). Issuing key: crls.aki for a CRL; for a manifest, the aki of its EE certificate
(object_signatures.signer_cert_id -> certs/is_ca=false). A manifest whose EE certificate is not in the
warehouse has no key (APNIC: 6,661 manifests of 2017-18): it counts in n and in population unknown, not
in n_ca.

Populations: the publication host of the issuing key is the SIA host of the key's newest CA
certificate in any of the five warehouses (ca_keys, scs/classify.py); a key without one, or whose CA
certificate has no rsync URI, takes the SIA host of its newest manifest EE certificate in this warehouse
(about 190 APNIC hosted keys of 2026 have no CA certificate in any warehouse); a key with neither is
unknown. Class and operator: classify.populations_sql (host map).

Statistics per (week, population, operator); n = objects, n_ca = issuing keys, top1_share = the largest
key's share of n, the same on every statistic of the (week, population, operator):
  crl_window, mft_window
    share_le24, share_le48, share_le72, share_le168   cumulative band shares: objects with a window of at
                    most b hours, exactly (a window above b by seconds or minutes, such as Krill's 24 h
                    plus jitter, counts above b)
    share_gt168     objects with a window of more than 168 h
    p50, p90        nearest-rank quantiles (= DuckDB quantile_disc): the smallest observed window w with at
                    least q x n objects at or below w, so a 48/72 h mix gives 48 or 72, never 60; params
                    quantile 'cont' (compare mode only) interpolates like quantile_cont instead
    max             the longest window
    p50_ca          one vote per issuing key: the median over keys of each key's median window (the same
                    quantile rule within the key's objects of the week and across the keys)
    share_le{b}_ca, share_gt168_ca   one vote per key: the mean over keys of the key's own share of its
                    objects in the band. A key that alternates 48 and 72 h counts half; classifying keys by
                    their median instead follows the parity of each key's weekly count (ARIN's hosted keys,
                    2026-06..10: 0.49-0.93 of keys at <= 48 h by median, 0.49-0.53 as mean shares)
  crl_empty
    share_empty     CRLs with revoked_count = 0 / CRLs
    share_empty_ca  keys all of whose CRLs of the week were empty / keys
  histogram window_h (crl_window, mft_window): objects per 1-hour bin [k, k + 1) for k = 0..199, then the
    overflow bin [200, NULL) (params hist_bin_h, hist_max_h); a negative window would count in the first bin.
Population 'all' has every statistic in every week of the range (n = 0 and values NULL in a week without
objects); the other populations have rows in the weeks where they have objects.

How: one grouped pass per table and RIR over the whole range of weeks (never a loop over weeks):
1. crl_counts: the CRLs with thisUpdate in the range, reduced to counts per (week, key, window) with the
   empty ones (RIPE NCC, all history: 63.7 million CRLs -> 8.7 million rows).
2. mft_counts: the manifests in the range (the text bounds on thisUpdate prune their part files), then
   their signature rows and EE certificates between explicit object_id and cert_id bounds (both ids grow
   with time, so a weekly run reads only the newest parts), reduced the same way.
3. key_hosts: the host of every key of both (the warehouse is read again only for keys without a CA host)
   and its populations.
4. window_stats / empty_stats from the counts: quantiles from cumulative counts (wquantile_sql), the
   per-key medians first. build() runs 1-4 for any period (e.g. a calendar year).
Cost, 2026-10-29 (threads 4, memory_limit 6GB; the run's CA key table, about 10 s and 3.3 GB RSS, apart):
- one RIR-week: 6-15 s, 0.9-2.2 GB RSS, mostly fixed cost (part-file footers of four tables, and the
  fallback's read of the whole EE certificate table where keys lack a CA host); a weekly run of the three
  metrics (6 weeks, five RIRs, through scs.run) took 84 s;
- all history (774 weeks): AFRINIC 9 s / 1.9 GB, APNIC 49 s / 5.7 GB, ARIN 51 s / 6.0 GB, LACNIC 60 s /
  6.7 GB, RIPE NCC 104 s / 7.2 GB RSS (its manifest join over 64 million rows: 41 s, 7.0 GB; DuckDB fills
  its memory limit there, and a 3 GB limit keeps that join to 4.0 GB at about 15% more time); the backfill
  of the three metrics through scs.run took 261 s.
"""
import datetime as dt

from . import classify, common
from .common import date_lit, lit

METRICS = ('crl_window', 'mft_window', 'crl_empty')
PERIOD = 'scs_week({ts})'          # period label of an object: the Monday of its thisUpdate's ISO week (UTC)
HOUR_S = 3600
MFT_EE_SIA = '%.mft%'              # manifest EE certificates: their SIA names the signed .mft object
GROUP = ('week', 'population', 'operator')


def compute(ctx, rir, week_from, week_to):
    want = [m for m in METRICS if m in ctx.metrics]
    if not want:
        return
    con = ctx.connect()
    try:
        out = build(ctx, con, rir, week_from, common.to_date(week_to) + dt.timedelta(days=7), want)
        # population 'all' in every week of the range, also when the week has no object
        con.execute(f"""CREATE OR REPLACE TEMP TABLE frame AS
            SELECT CAST(range AS DATE) AS week
            FROM range(CAST({date_lit(week_from)} AS TIMESTAMP), CAST({date_lit(week_to)} AS TIMESTAMP)
                       + INTERVAL 7 DAY, INTERVAL 7 DAY)""")
        for m in want:
            stats, dist = out[m]
            values = empty_values() if m == 'crl_empty' else window_values(ctx.params(m)['bands_h'])
            ctx.emit(con, rows_sql(stats, m, rir, values),
                     hist_sql(dist, m, rir, ctx.params(m)) if dist else None)
    finally:
        con.close()


def build(ctx, con, rir, t0, t1, metric_ids, period=PERIOD, params=None):
    """Count, classify and summarise the objects with thisUpdate in [t0, t1) (dates or naive-UTC
    datetimes) for metric_ids, grouped by `period` (SQL with {ts}; default the ISO week). params:
    {metric_id: params} instead of ctx.params(). Returns {metric_id: (statistics table, distribution
    table or None)}: TEMP TABLEs in con, the statistics one row per (week, population, operator)."""
    want = [m for m in METRICS if m in metric_ids]
    par = {m: (params or {}).get(m) or ctx.params(m) for m in want}
    ctx.load_hostmap(con)
    ctx.load_ca_keys(con)
    counts = []
    if 'crl_window' in want or 'crl_empty' in want:
        counts.append(crl_counts(ctx, con, rir, t0, t1, period))
    if 'mft_window' in want:
        counts.append(mft_counts(ctx, con, rir, t0, t1, period))
    key_hosts(ctx, con, rir, counts)
    out = {}
    for m in want:
        if m == 'crl_empty':
            out[m] = (empty_stats(con, 'crl_k', f'{m}_s'), None)
        else:
            out[m] = window_stats(con, 'crl_k' if m == 'crl_window' else 'mft_k', m, par[m])
    return out


# ── warehouse reads ──────────────────────────────────────────────────────────
def crl_counts(ctx, con, rir, t0, t1, period=PERIOD, out='crl_k'):
    """TEMP TABLE out (week, aki, w_s, n, n_empty): CRLs with thisUpdate in [t0, t1) per period, issuing
    key and window in seconds (NULL without a nextUpdate); n_empty = those with revoked_count = 0."""
    crls = ctx.src(rir, 'crls', ['aki', 'this_update', 'next_update', 'revoked_count'])
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT {period.format(ts='this_update')} AS week, aki,
               CAST(round(epoch(next_update - this_update)) AS BIGINT) AS w_s,
               count(*) AS n, count(*) FILTER (WHERE revoked_count = 0) AS n_empty
        FROM {crls} WHERE this_update >= {common.ts_lit(t0)} AND this_update < {common.ts_lit(t1)}
        GROUP BY ALL""")
    return out


def mft_counts(ctx, con, rir, t0, t1, period=PERIOD, out='mft_k'):
    """TEMP TABLE out (week, aki, w_s, n): manifests with thisUpdate in [t0, t1) per period, issuing key
    (the aki of the manifest's EE certificate; NULL when that certificate is not in the warehouse) and
    window in seconds. The text thisUpdate ('2026-10-29T07:46:24+00:00') is compared as text with a day
    of margin, which lets the reader skip part files, before the exact filter on its cast value."""
    m = ctx.src(rir, 'manifests', ['object_id', 'this_update', 'next_update'])
    lo_txt = lit((common.to_date(t0) - dt.timedelta(days=1)).isoformat())
    hi_txt = lit((common.to_date(t1) + dt.timedelta(days=1)).isoformat())
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE {out}_m AS
        SELECT object_id, {period.format(ts='tu')} AS week, CAST(round(epoch(nu - tu)) AS BIGINT) AS w_s
        FROM (SELECT object_id, TRY_CAST(this_update AS TIMESTAMPTZ) AS tu, TRY_CAST(next_update AS TIMESTAMPTZ) AS nu
              FROM {m} WHERE this_update >= {lo_txt} AND this_update < {hi_txt})
        WHERE tu >= {common.ts_lit(t0)} AND tu < {common.ts_lit(t1)}""")
    # signature rows, then EE certificates, each between the id bounds of what the previous step found
    lo, hi = con.execute(f"SELECT coalesce(min(object_id), 0), coalesce(max(object_id), -1) FROM {out}_m").fetchone()
    sg = ctx.src(rir, 'object_signatures', ['object_id', 'signer_cert_id'])
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE {out}_s AS
        SELECT m.week, m.w_s, s.signer_cert_id AS cert_id
        FROM {out}_m m LEFT JOIN (SELECT object_id, signer_cert_id FROM {sg} WHERE object_id BETWEEN {lo} AND {hi}) s
             USING (object_id)""")
    con.execute(f'DROP TABLE {out}_m')
    lo, hi = con.execute(f"SELECT coalesce(min(cert_id), 0), coalesce(max(cert_id), -1) FROM {out}_s").fetchone()
    ee = ctx.src(rir, 'certs/is_ca=false', ['cert_id', 'aki'])
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT s.week, e.aki, s.w_s, count(*) AS n
        FROM {out}_s s LEFT JOIN (SELECT cert_id, aki FROM {ee} WHERE cert_id BETWEEN {lo} AND {hi}) e USING (cert_id)
        GROUP BY ALL""")
    con.execute(f'DROP TABLE {out}_s')
    return out


def key_hosts(ctx, con, rir, counts):
    """TEMP TABLE keyhost (aki, host) for the keys of the count tables (a row with aki NULL when some
    object has no key) and TEMP TABLE kpop (aki, population, operator), their populations. host = the
    ca_keys host, else the SIA host of the key's newest manifest EE certificate in this warehouse."""
    union = ' UNION '.join(f'SELECT DISTINCT aki FROM {t}' for t in counts)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE key_ca AS
        SELECT u.aki, k.host AS ca_host FROM ({union}) u LEFT JOIN ca_keys k ON k.ski = u.aki""")
    if con.execute("SELECT count(*) FROM key_ca WHERE aki IS NOT NULL AND ca_host IS NULL").fetchone()[0]:
        ee = ctx.src(rir, 'certs/is_ca=false', ['cert_id', 'aki', 'sia', 'not_before'])
        # arg_max skips NULL hosts (an EE certificate without an rsync URI)
        ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE key_ee AS
            SELECT aki, arg_max(scs_host(sia), (not_before, cert_id)) AS host FROM {ee}
            WHERE aki IN (SELECT aki FROM key_ca WHERE aki IS NOT NULL AND ca_host IS NULL)
              AND sia LIKE {lit(MFT_EE_SIA)}
            GROUP BY aki""")
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE key_ee (aki VARCHAR, host VARCHAR)")
    con.execute("""CREATE OR REPLACE TEMP TABLE keyhost AS
        SELECT h.aki, coalesce(h.ca_host, e.host) AS host FROM key_ca h LEFT JOIN key_ee e ON e.aki = h.aki""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE kpop AS
        SELECT aki, population, operator FROM ({classify.populations_sql('keyhost', 'host', rir)})""")


# ── statistics ───────────────────────────────────────────────────────────────
def wquantile_sql(src, group, value, weight, ps, method='disc'):
    """One row per group of src: w_total (the summed weight) and q<p> for every p in ps (percent,
    integers) of `value` weighted by the counts in `weight`, without expanding the counts:
      'disc'  nearest rank, as DuckDB quantile_disc: the smallest value v with a cumulative weight of at
              least ceil(p / 100 x w_total) at v;
      'cont'  as DuckDB quantile_cont: the values at the 0-based positions floor(r) and floor(r) + 1 of
              the sorted values, r = p / 100 x (w_total - 1), interpolated linearly.
    Ties of `value` within a group may come in any order: the first row whose cumulative weight reaches
    a rank has the value of that rank whatever the order among equal values (integer arithmetic, exact)."""
    g = ', '.join(group)
    cols = []
    for p in ps:
        if method == 'disc':
            cols.append(f'min(v_) FILTER (WHERE cum_ >= ({p} * tot_ + 99) // 100) AS q{p}')
        elif method == 'cont':
            lo = f'min(v_) FILTER (WHERE cum_ > ({p} * (tot_ - 1)) // 100)'
            hi = f'min(v_) FILTER (WHERE cum_ > ({p} * (tot_ - 1)) // 100 + 1)'
            frac = f'(({p} * (any_value(tot_) - 1)) % 100) / 100.0'
            cols.append(f'{lo} + {frac} * (coalesce({hi}, {lo}) - {lo}) AS q{p}')
        else:
            raise ValueError(f'quantile method {method!r}: disc or cont')
    return f"""
SELECT {g}, any_value(tot_) AS w_total, {', '.join(cols)} FROM (
  SELECT {g}, {value} AS v_,
         sum({weight}) OVER (PARTITION BY {g} ORDER BY {value} ROWS UNBOUNDED PRECEDING) AS cum_,
         sum({weight}) OVER (PARTITION BY {g}) AS tot_
  FROM {src})
GROUP BY {g}"""


def window_stats(con, counts, name, params):
    """Window statistics of a count table (crl_k or mft_k) per (week, population, operator):
    TEMP TABLE <name>_s (n, n_ca, top1_share, le<b> / gt (objects in the bands), p50_s, p90_s, max_s
    (windows in seconds), p50_ca_s (median of the per-key medians), kle<b> / kgt (mean over keys of the
    key's share of objects in the band)) and the distribution <name>_d (week, population, operator, w_s,
    n). Returns (<name>_s, <name>_d)."""
    method, bands = params['quantile'], [int(b) for b in params['bands_h']]
    top = bands[-1] * HOUR_S
    d, kw, s = f'{name}_d', f'{name}_kw', f'{name}_s'
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {d} AS
        SELECT k.week, p.population, p.operator, k.w_s, sum(k.n) AS n
        FROM {counts} k JOIN kpop p ON p.aki IS NOT DISTINCT FROM k.aki
        WHERE k.w_s IS NOT NULL GROUP BY ALL""")
    bsum = ', '.join(f'coalesce(sum(n) FILTER (WHERE w_s <= {b * HOUR_S}), 0) AS le{b}' for b in bands) + \
        f', coalesce(sum(n) FILTER (WHERE w_s > {top}), 0) AS gt'
    # per (week, key): the key's objects with a window (w_total), its median window and its counts in the bands
    keyed = f'(SELECT * FROM {counts} WHERE w_s IS NOT NULL AND aki IS NOT NULL)'
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {kw} AS
        SELECT q.week, q.aki, q.w_total, q.q50, b.* EXCLUDE (week, aki)
        FROM ({wquantile_sql(keyed, ('week', 'aki'), 'w_s', 'n', (50,), method)}) q
             JOIN (SELECT week, aki, {bsum} FROM {keyed} GROUP BY ALL) b USING (week, aki)""")
    # a key's share as an exact DECIMAL, so that the mean over keys does not depend on the summation order
    kshare = ', '.join(f'avg(CAST(kw.{c} / kw.w_total AS DECIMAL(38, 18))) AS k{c}'
                       for c in [f'le{b}' for b in bands] + ['gt'])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {s} AS
        WITH o AS (SELECT week, population, operator, sum(n) AS n, max(w_s) AS max_s, {bsum}
                   FROM {d} GROUP BY ALL),
             q AS ({wquantile_sql(d, GROUP, 'w_s', 'n', (50, 90), method)}),
             k AS (SELECT kw.week, p.population, p.operator, count(*) AS n_ca, max(kw.w_total) AS top_n,
                          quantile_{method}(kw.q50, 0.5) AS p50_ca_s, {kshare}
                   FROM {kw} kw JOIN kpop p ON p.aki = kw.aki GROUP BY ALL)
        SELECT o.*, q.q50 AS p50_s, q.q90 AS p90_s, coalesce(k.n_ca, 0) AS n_ca, k.top_n / o.n AS top1_share,
               k.* EXCLUDE (week, population, operator, n_ca, top_n)
        FROM o JOIN q USING (week, population, operator) LEFT JOIN k USING (week, population, operator)""")
    return s, d


def empty_stats(con, counts, out):
    """TEMP TABLE out per (week, population, operator): n (CRLs), n_empty, n_ca, top1_share, k_empty
    (keys all of whose CRLs were empty)."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {out} AS
        WITH kw AS (SELECT week, aki, sum(n) AS n_key, sum(n_empty) AS e_key FROM {counts} GROUP BY ALL),
             o AS (SELECT kw.week, p.population, p.operator, sum(kw.n_key) AS n, sum(kw.e_key) AS n_empty
                   FROM kw JOIN kpop p ON p.aki IS NOT DISTINCT FROM kw.aki GROUP BY ALL),
             k AS (SELECT kw.week, p.population, p.operator, count(*) AS n_ca, max(kw.n_key) AS top_n,
                          count(*) FILTER (WHERE kw.e_key = kw.n_key) AS k_empty
                   FROM kw JOIN kpop p ON p.aki = kw.aki GROUP BY ALL)
        SELECT o.*, coalesce(k.n_ca, 0) AS n_ca, k.top_n / o.n AS top1_share, k.k_empty
        FROM o LEFT JOIN k USING (week, population, operator)""")
    return out


def window_values(bands):
    """[(stat, SQL value over a window_stats row)] in defs order (bands in hours)."""
    top = bands[-1]
    out = [(f'share_le{b}', f'le{b} / nullif(n, 0)') for b in bands]
    out += [(f'share_gt{top}', 'gt / nullif(n, 0)'), ('p50', f'p50_s / {HOUR_S}.0'), ('p90', f'p90_s / {HOUR_S}.0'),
            ('max', f'max_s / {HOUR_S}.0'), ('p50_ca', f'p50_ca_s / {HOUR_S}.0')]
    out += [(f'share_le{b}_ca', f'CAST(kle{b} AS DOUBLE)') for b in bands]
    out.append((f'share_gt{top}_ca', 'CAST(kgt AS DOUBLE)'))
    return out


def empty_values():
    return [('share_empty', 'n_empty / nullif(n, 0)'), ('share_empty_ca', 'k_empty / nullif(n_ca, 0)')]


# ── rows and histograms (metrics.ROW_COLS / HIST_COLS) ───────────────────────
def rows_sql(stats, metric_id, rir, values):
    """Long rows of a statistics table: every statistic of population 'all' in every week of TEMP TABLE
    frame (NULL values and n = 0 where the week has no object), the other populations where they have
    objects."""
    case = ' '.join(f'WHEN {lit(s)} THEN {e}' for s, e in values)
    names = ', '.join(f'({lit(s)})' for s, _ in values)
    return f"""
SELECT x.week, '1w' AS "window", {lit(rir)} AS rir, x.population, x.operator, {lit(metric_id)} AS metric_id,
       v.stat, CASE v.stat {case} END AS value, coalesce(x.n, 0) AS n, coalesce(x.n_ca, 0) AS n_ca, x.top1_share
FROM (SELECT f.week, 'all' AS population, '' AS operator, s.* EXCLUDE (week, population, operator)
      FROM frame f LEFT JOIN {stats} s ON s.week = f.week AND s.population = 'all'
      UNION ALL BY NAME
      SELECT * FROM {stats} WHERE population <> 'all') x
CROSS JOIN (VALUES {names}) v(stat)"""


def hist_sql(dist, metric_id, rir, params):
    """Histogram window_h of a distribution table: hist_bin_h-hour bins from 0 to hist_max_h h, then the
    overflow bin [hist_max_h, NULL)."""
    bh, top = int(params['hist_bin_h']), int(params['hist_max_h'])
    nb = top // bh
    return f"""
SELECT week, '1w' AS "window", {lit(rir)} AS rir, population, operator, {lit(metric_id)} AS metric_id,
       'window_h' AS stat, b * {bh} AS bin_lo, CASE WHEN b < {nb} THEN (b + 1) * {bh} END AS bin_hi, sum(n) AS count
FROM (SELECT *, least(greatest(w_s // {bh * HOUR_S}, 0), {nb}) AS b FROM {dist})
GROUP BY ALL"""
