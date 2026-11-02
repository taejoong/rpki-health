"""Re-sign pairs: roa_ee_revocation and resign_consumed from one pair table.

Pairs: the ROA objects of one semantic ROA
(semantic_hash: the same ASN and prefix set) that have a row in object_presence_stats, ordered by
coalesce(signing_time, ee_not_before), then object_id; a pair is two consecutive objects of that order,
unless the successor was first seen more than chain_break_days (30) after the predecessor was last seen.
A pair belongs to the ISO week (UTC) of the successor's signing time. Every object of every semantic ROA
with an object signed in the range is loaded, so a predecessor can be arbitrarily old. Population and CA
key: the SIA host and the aki of the predecessor's signer EE certificate (object_signatures.signer_cert_id
-> certs/is_ca=false); the key is what n_ca and top1_share count.

roa_ee_revocation (window 1w; RFC 9286 5.2: the CA MUST revoke the EE certificate of a replaced object
that has not expired):
  early        the predecessor's EE notAfter is more than early_days (7) after the successor's signing time
  evidence(H)  a CRL of the predecessor's issuing key (crls.aki = predecessor EE aki) with thisUpdate in
               [successor signing - lookback_days (1), successor signing + H]
  revoked(H)   the predecessor's ee_serial is in crl_serials.serial_dec of such a CRL; H = 7 and 35 days
               (horizons_days). Early pairs without evidence(H) count in no_evidence_hH and are left out
               of the revoked shares (revoked implies evidence: the listing CRL is one)
  strata       of the early pairs with evidence: samekey / keychange (successor EE aki = / <> the
               predecessor's), withdrawn / kept (predecessor last seen at most / more than
               withdrawn_after_days (1) after the successor was first seen)
  stats        pairs, early_pairs (counts); revoked_hH (pair-weighted), revoked_hH_ca (mean of the per-key
               shares), no_evidence_hH (share of the early pairs), revoked_hH_{samekey,keychange,withdrawn,
               kept}. n is the share's denominator (early pairs with evidence; early pairs for no_evidence),
               n_ca its distinct keys, top1_share the largest key's share of n.
resign_consumed (windows 1w, 13w, 52w):
  consumed     (successor signing - predecessor EE notBefore) / (predecessor EE notAfter - notBefore), for
               pairs with notAfter > notBefore and consumed in [0, 1.5] (consumed_range)
  interval     successor signing - predecessor signing, in days
  transient    the predecessor was never present at a daily sampling instant: 04:00 UTC of some day
               (daily_instant_utc; object_presence_runs start_ts <= instant <= end_ts) or a RIPE NCC archive
               snapshot (labelled 00:00 UTC, until archive_until = 2026-04-14: the archive's daily trees are
               the era-1 sampling, so objects first seen before 2026-04-08 pass by construction)
  stats        over the pairs whose predecessor is not transient: pairs, consumed_p10/p50/p90
               (quantile_cont), consumed_p50_ca (median over keys of the per-key median), share_ge075,
               share_gt100 (re-signed after the predecessor had expired), interval_p10/p50/p90_d; over all
               pairs, from the ISO week of 2026-04-08 (the first era-2 day) on: pairs_incl_transient,
               consumed_p50_incl_transient, share_ge075_incl_transient
  windows      the 13w / 52w row of week W pools the pairs of W and of the 12 / 51 weeks before; values are
               exact: every pair is expanded to the windows that contain it and aggregated with GROUP BY, in
               chunks of output weeks of at most ROW_BUDGET expanded rows (one chunk for a weekly run)
  histogram    consumed (1w): bins of 0.01, [0, 0.01) .. [1.49, 1.5] (1.5 itself in the last bin)

Every week of the range has a row of population 'all' for every statistic (n = 0 when there is nothing;
the *_incl_transient ones from the first era-2 week); the other populations have rows where they have
pairs (13w / 52w: in the window). Input cut (ctx.as_of, as in m_mft and m_ca): an object is in a chain
only if it was first seen by as_of, presence after as_of is not used (last seen and run ends are capped at
as_of) and evidence CRLs have thisUpdate <= as_of, so a rerun with the same as_of gives the same rows
while the ETL keeps adding CRLs, and every RIR is cut at the same instant (LACNIC's presence ends 20
minutes after the others'). A week's rows do not depend on the range it is computed with (a weekly run
and a backfill agree). Times are naive UTC (TIMESTAMPTZ subtraction goes through ICU and is two orders of
magnitude slower). CA-weighted means are summed in fixed point (HUGEINT, 1e-18), so reruns give the same
bits whatever the thread schedule.

Steps (one compute() call per RIR and range of weeks; the pair table is shared by both metrics):
  build_pairs   roa_objects (all of them) -> semantic ROAs with an object signed in [t0, t1) -> presence
                (stats, and the runs of objects first seen in era 2, under the shared presence lock) ->
                lead() -> signer EE key and host of both objects -> TEMP TABLE pairs
  revocation    early pairs; CRLs of their keys with thisUpdate in [first successor - 1 d, min(last
                successor + 35 d, as_of)], on the UTC days of the pairs' windows; evidence = the key's first
                CRL at or after successor - 1 d (ASOF JOIN); listing = the first such CRL that has the serial
                (crl_serials cut to the CRLs' crl_id range and to the pairs' serials before any join)
  consumed      pair values, transient flag, populations; expansion to (week, window) and GROUP BY
"""
import datetime as dt
import os
import time

from . import classify, common
from .common import date_lit, lit, log

REV, CONS = 'roa_ee_revocation', 'resign_consumed'
WINDOWS = (('1w', 1), ('13w', 13), ('52w', 52))
ROW_BUDGET = 12_000_000          # expanded (pair, window, population) rows per aggregation chunk
WEEK0 = dt.date(1970, 1, 5)      # a Monday: week index = days since WEEK0 // 7
FIXED = 10 ** 18                 # fixed-point scale of the CA-weighted means
QS = (0.1, 0.5, 0.9)


def compute(ctx, rir, week_from, week_to):
    """Rows of roa_ee_revocation and resign_consumed (those in ctx.metrics) for the weeks
    week_from..week_to (Mondays) of warehouse `rir`."""
    want_rev, want_cons = REV in ctx.metrics, CONS in ctx.metrics
    if not (want_rev or want_cons):
        return
    pr, pc = ctx.params(REV), ctx.params(CONS)
    if want_rev and want_cons and pr['chain_break_days'] != pc['chain_break_days']:
        raise ValueError(f'm_pairs: {REV} and {CONS} share one pair table, so their chain_break_days must agree')
    wf, wt = common.monday(week_from), common.monday(week_to)
    # the 52-week window of week_from starts 51 weeks earlier
    first = wf - dt.timedelta(weeks=max(k for _, k in WINDOWS) - 1) if want_cons else wf
    con = ctx.connect()
    try:
        ctx.load_hostmap(con)
        build_pairs(ctx, con, rir, first, wt + dt.timedelta(days=7),
                    chain_break_days=(pr if want_rev else pc)['chain_break_days'],
                    transient=pc if want_cons else None)
        if want_rev:
            revocation(ctx, con, rir, wf, wt, pr)
        if want_cons:
            consumed(ctx, con, rir, wf, wt, pc)
    finally:
        con.close()


# ── reading ──────────────────────────────────────────────────────────────────
def _src(ctx, rir, table, cols):
    """ctx.src() with the largest part first. DuckDB estimates a multi-file scan as the first file's rows
    times the number of files (and 20% for a range filter); in name order the first part of crl_serials is
    an old small one, so a 30-million-row scan was estimated at 25,000 rows and became the build side of a
    hash join (6.9 GB RSS for ARIN). Largest first makes the estimate an upper bound, so the small tables
    of the pairs are the build sides. The order of the parts changes no result."""
    files = ctx.files(rir, table)
    if not files:
        return ctx.src(rir, table, cols)        # raises FileNotFoundError

    def size(f):
        try:
            return os.stat(f).st_size
        except FileNotFoundError:
            return 0
    return common.read_sql(sorted(files, key=lambda f: (-size(f), f)), cols)


def _dt(t):
    """A date (00:00) or a naive-UTC datetime as a naive-UTC datetime."""
    return common.parse_ts(t) if isinstance(t, dt.datetime) else dt.datetime.combine(common.to_date(t), dt.time())


def _ts(t):
    """TIMESTAMP literal (naive UTC) of a date or a naive-UTC datetime."""
    return f"TIMESTAMP '{_dt(t).strftime('%Y-%m-%d %H:%M:%S')}'"


def _utc(col):
    """A TIMESTAMPTZ column as naive UTC TIMESTAMP (epoch arithmetic: exact and independent of TimeZone)."""
    return f'make_timestamp(epoch_us({col}))'


# ── pairs ────────────────────────────────────────────────────────────────────
def build_pairs(ctx, con, rir, t0, t1, chain_break_days=30, transient=None):
    """TEMP TABLE pairs: the re-sign pairs of warehouse `rir` whose successor was signed in [t0, t1) (dates
    or naive-UTC datetimes), one row per pair; times are naive UTC TIMESTAMPs:
      week (DATE, the Monday of the successor's signing time), oid / nid (predecessor / successor object_id;
      nid identifies the pair), st, nb, na, ee_serial (predecessor: signing time, EE notBefore / notAfter,
      EE serial), nst (successor signing time), fs, ls (predecessor first / last seen, ls at most as_of),
      nfs (successor first seen), kid, host (predecessor signer EE key and SIA host), nkid (successor signer
      EE key), daily (predecessor present at a daily sampling instant by as_of; NULL unless `transient`, the
      resign_consumed parameters, is given). Only objects first seen by ctx.as_of are in the chains. Keys
      are integers (TEMP TABLE pair_keys: kid, aki), which keeps the hash tables of the CRL joins and of
      the per-key aggregates small.
    Returns the number of pairs."""
    started = time.time()
    lo, hi, cut = _ts(t0), _ts(t1), _ts(ctx.as_of)
    roa = _src(ctx, rir, 'roa_objects',
               ['object_id', 'semantic_hash', 'signing_time', 'ee_not_before', 'ee_not_after', 'ee_serial'])
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE pr_obj AS
        SELECT object_id, semantic_hash, {_utc('coalesce(signing_time, ee_not_before)')} AS st,
               {_utc('ee_not_before')} AS nb, {_utc('ee_not_after')} AS na, ee_serial
        FROM {roa}""")
    # whole chains of the semantic ROAs with an object signed in the range
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pr_chain AS
        SELECT * FROM pr_obj
        WHERE semantic_hash IN (SELECT semantic_hash FROM pr_obj WHERE st >= {lo} AND st < {hi})""")
    con.execute('DROP TABLE pr_obj')
    era2 = _ts(common.ERA2_FROM)
    with ctx.presence_lock():
        held = time.time()
        stats = _src(ctx, rir, 'object_presence_stats', ['object_id', 'first_ts', 'last_ts'])
        ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE pr_ps AS
            SELECT * FROM (
                SELECT object_id, CAST(first_ts AS TIMESTAMP) AS fs, least(CAST(last_ts AS TIMESTAMP), {cut}) AS ls
                FROM {stats} WHERE object_id IN (SELECT object_id FROM pr_chain))
            WHERE fs <= {cut}""")
        if transient is not None:
            # runs of the objects first seen in era 2 (every earlier one was in an archive snapshot)
            runs = _src(ctx, rir, 'object_presence_runs', ['object_id', 'start_ts', 'end_ts'])
            ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE pr_runs AS
                SELECT * FROM (
                    SELECT object_id, CAST(start_ts AS TIMESTAMP) AS s, least(CAST(end_ts AS TIMESTAMP), {cut}) AS e
                    FROM {runs} WHERE object_id IN (SELECT object_id FROM pr_ps WHERE fs >= {era2}))
                WHERE s <= {cut}""")
        held = time.time() - held
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pr_seq AS
        SELECT * FROM (
            SELECT c.object_id AS oid, c.st, c.nb, c.na, c.ee_serial, p.fs, p.ls,
                   lead(c.object_id) OVER w AS nid, lead(c.st) OVER w AS nst, lead(p.fs) OVER w AS nfs
            FROM pr_chain c JOIN pr_ps p USING (object_id)
            WINDOW w AS (PARTITION BY c.semantic_hash ORDER BY c.st, c.object_id))
        WHERE nst >= {lo} AND nst < {hi} AND nfs <= ls + to_days({int(chain_break_days)})""")
    con.execute('DROP TABLE pr_chain')
    con.execute('DROP TABLE pr_ps')
    sig = _src(ctx, rir, 'object_signatures', ['object_id', 'signer_cert_id'])
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE pr_sig AS
        SELECT object_id, min(signer_cert_id) AS cert_id FROM {sig}
        WHERE object_id IN (SELECT oid FROM pr_seq UNION ALL SELECT nid FROM pr_seq) GROUP BY object_id""")
    ee = _src(ctx, rir, 'certs/is_ca=false', ['cert_id', 'aki', 'sia'])
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE pr_ee AS
        SELECT cert_id, min(aki) AS aki, min(scs_host(sia)) AS host FROM {ee}
        WHERE cert_id IN (SELECT cert_id FROM pr_sig) GROUP BY cert_id""")
    con.execute("""CREATE OR REPLACE TEMP TABLE pair_keys AS
        SELECT aki, CAST(row_number() OVER (ORDER BY aki) AS INTEGER) AS kid
        FROM (SELECT DISTINCT aki FROM pr_ee WHERE aki IS NOT NULL)""")
    daily = 'NULL::BOOLEAN'
    if transient is not None:
        con.execute(f"CREATE OR REPLACE TEMP TABLE pr_daily AS {daily_sql('pr_runs', transient)}")
        daily = f"(s.fs < {era2} OR s.oid IN (SELECT object_id FROM pr_daily))"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pairs AS
        SELECT CAST(date_trunc('week', s.nst) AS DATE) AS week, s.oid, s.nid, s.st, s.nb, s.na, s.ee_serial,
               s.nst, s.fs, s.ls, s.nfs, k1.kid, e1.host, k2.kid AS nkid, {daily} AS daily
        FROM pr_seq s
             LEFT JOIN pr_sig g1 ON g1.object_id = s.oid LEFT JOIN pr_ee e1 ON e1.cert_id = g1.cert_id
             LEFT JOIN pair_keys k1 ON k1.aki = e1.aki
             LEFT JOIN pr_sig g2 ON g2.object_id = s.nid LEFT JOIN pr_ee e2 ON e2.cert_id = g2.cert_id
             LEFT JOIN pair_keys k2 ON k2.aki = e2.aki""")
    for tab in ('pr_seq', 'pr_sig', 'pr_ee', 'pr_runs', 'pr_daily'):
        con.execute(f'DROP TABLE IF EXISTS {tab}')
    n = con.execute('SELECT count(*) FROM pairs').fetchone()[0]
    log(f'm_pairs: {rir}: {n:,} pairs, successors signed in [{t0}, {t1}) [{time.time() - started:.1f}s, presence '
        f'lock held {held:.1f}s, peak RSS {common.peak_rss_mb()} MB]')
    return n


def daily_sql(runs, p):
    """object_ids of `runs` (object_id, s, e: naive UTC) with a run that contains a daily sampling instant:
    p['daily_instant_utc'] ('HH:MM') of any day, or the 00:00 label of an archive snapshot up to
    p['archive_until'] (our cycles are never labelled 00:00:00, and a run that spans an archive label
    includes that snapshot, since a run breaks at every snapshot without the object)."""
    hh, mm = (int(x) for x in p['daily_instant_utc'].split(':'))
    inst = f"to_minutes({hh * 60 + mm})"
    day = "date_trunc('day', s)"
    nxt_inst = f"CASE WHEN {day} + {inst} >= s THEN {day} + {inst} ELSE {day} + {inst} + INTERVAL 1 DAY END"
    nxt_mid = f"CASE WHEN {day} = s THEN s ELSE {day} + INTERVAL 1 DAY END"
    return f"""SELECT DISTINCT object_id FROM {runs}
               WHERE {nxt_inst} <= e OR ({nxt_mid} <= e AND {nxt_mid} <= {_ts(p['archive_until'])})"""


# ── aggregation helpers ──────────────────────────────────────────────────────
def _frame(con, name, week_from, week_to):
    """TEMP TABLE name(week): the Mondays week_from..week_to."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
        SELECT CAST(range AS DATE) AS week
        FROM range(CAST({date_lit(week_from)} AS TIMESTAMP), CAST({date_lit(week_to)} AS TIMESTAMP) + INTERVAL 7 DAY,
                   INTERVAL 7 DAY)""")


def _ca_mean(s, n, key='kid'):
    """Mean over the keys with n > 0 of s / n, summed in fixed point (the same bits on every run)."""
    f = f"{n} > 0 AND {key} IS NOT NULL"
    return (f"CAST(sum((CAST({s} AS HUGEINT) * {FIXED}) // {n}) FILTER (WHERE {f}) AS DOUBLE) / {float(FIXED)!r} "
            f"/ nullif(count(*) FILTER (WHERE {f}), 0)")


# ── roa_ee_revocation ────────────────────────────────────────────────────────
def revocation_stats(horizons):
    """[(stat, subset, numerator, kind)] of roa_ee_revocation: kind 'count' (value = n), 'share'
    (pair-weighted: numerator pairs / subset pairs) or 'ca' (mean over keys of the per-key share); subset
    and numerator are conditions on rv_x."""
    out = [('pairs', 'true', None, 'count'), ('early_pairs', 'early', None, 'count')]
    for h in horizons:
        ev, rv = f'ev{h}', f'rv{h}'
        out += [(f'revoked_h{h}', f'early AND {ev}', rv, 'share'),
                (f'revoked_h{h}_ca', f'early AND {ev}', rv, 'ca'),
                (f'no_evidence_h{h}', 'early', f'NOT {ev}', 'share'),
                (f'revoked_h{h}_samekey', f'early AND {ev} AND NOT keychange', rv, 'share'),
                (f'revoked_h{h}_keychange', f'early AND {ev} AND keychange', rv, 'share'),
                (f'revoked_h{h}_withdrawn', f'early AND {ev} AND NOT kept', rv, 'share'),
                (f'revoked_h{h}_kept', f'early AND {ev} AND kept', rv, 'share')]
    return out


def revocation_flags(ctx, con, rir, t0, t1, p):
    """TEMP TABLE rv_x: the pairs of `pairs` whose successor was signed in [t0, t1), with early, keychange,
    kept, ev_tu (the first CRL of the key at or after successor - lookback_days), rv_tu (the first one of
    those, up to the longest horizon, that lists the predecessor's serial) and ev<H> / rv<H> for every H of
    p['horizons_days']; CRLs with thisUpdate after ctx.as_of are not used. Returns the horizons."""
    started = time.time()
    hs = sorted(int(h) for h in p['horizons_days'])
    back, early_d, kept_d = int(p['lookback_days']), int(p['early_days']), int(p['withdrawn_after_days'])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rv_p AS
        SELECT nid, week, kid, host, ee_serial, nst,
               nst - to_days({back}) AS lo, nst + to_days({max(hs)}) AS hi,
               coalesce(na - to_days({early_d}) > nst, false) AS early,
               kid IS DISTINCT FROM nkid AS keychange,
               coalesce(ls > nfs + to_days({kept_d}), false) AS kept
        FROM pairs WHERE nst >= {_ts(t0)} AND nst < {_ts(t1)}""")
    # the CRLs of the early pairs' keys on the UTC days of their windows [lo, hi]: every CRL of a pair's
    # window is there, so the first CRL at or after lo below is the same as among all the key's CRLs
    # whenever it falls in the window (a full-history pass keeps 49 of RIPE NCC's 64 million CRLs without
    # the day filter)
    con.execute("""CREATE OR REPLACE TEMP TABLE rv_day AS
        SELECT DISTINCT kid, CAST(d AS DATE) AS day
        FROM (SELECT kid, unnest(range(CAST(CAST(lo AS DATE) AS TIMESTAMP), CAST(CAST(hi AS DATE) AS TIMESTAMP)
                                       + INTERVAL 1 DAY, INTERVAL 1 DAY)) AS d
              FROM rv_p WHERE early)""")
    crls = _src(ctx, rir, 'crls', ['crl_id', 'aki', 'this_update'])
    c0, c1 = _dt(t0) - dt.timedelta(days=back), min(_dt(t1) + dt.timedelta(days=max(hs)), ctx.as_of)
    ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE rv_crl AS
        SELECT * FROM (
            SELECT c.crl_id, k.kid, {_utc('c.this_update')} AS tu
            FROM {crls} c JOIN (SELECT * FROM pair_keys WHERE kid IN (SELECT kid FROM rv_day)) k ON k.aki = c.aki
            WHERE c.this_update >= {common.ts_lit(c0)} AND c.this_update <= {common.ts_lit(c1)})
        WHERE (kid, CAST(tu AS DATE)) IN (SELECT kid, day FROM rv_day)""")
    # evidence: the key's first CRL at or after successor - lookback
    con.execute("""CREATE OR REPLACE TEMP TABLE rv_ev AS
        SELECT e.nid, c.tu AS ev_tu
        FROM (SELECT nid, kid, lo FROM rv_p WHERE early) e ASOF JOIN rv_crl c ON c.kid = e.kid AND e.lo <= c.tu""")
    # listing: the first CRL of the window that has the predecessor's serial. crl_serials repeats every entry
    # of every CRL version: the scan is cut to the crl_id range of the window's CRLs (row-group pruning;
    # crl_id is assigned in arrival order) and, by a semi-join, to the early pairs' serials before it is
    # joined with the window's CRLs and the pairs (small build sides, see _src)
    ids = con.execute('SELECT min(crl_id), max(crl_id) FROM rv_crl').fetchone()
    if ids[0] is None:
        con.execute('CREATE OR REPLACE TEMP TABLE rv_ls (nid INTEGER, rv_tu TIMESTAMP)')
    else:
        ser = _src(ctx, rir, 'crl_serials', ['crl_id', 'serial_dec'])
        ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE rv_ls AS
            WITH x AS (
                SELECT c.kid, s.serial_dec AS serial, c.tu
                FROM {ser} s JOIN rv_crl c ON c.crl_id = s.crl_id
                WHERE s.crl_id BETWEEN {int(ids[0])} AND {int(ids[1])}
                  AND s.serial_dec IN (SELECT ee_serial FROM rv_p WHERE early))
            SELECT e.nid, min(x.tu) AS rv_tu
            FROM x JOIN (SELECT nid, kid, ee_serial, lo, hi FROM rv_p WHERE early) e
                 ON e.kid = x.kid AND e.ee_serial = x.serial AND x.tu >= e.lo AND x.tu <= e.hi
            GROUP BY e.nid""")
    flags = ', '.join(f"coalesce(v.ev_tu <= p.nst + to_days({h}), false) AS ev{h}, "
                      f"coalesce(l.rv_tu <= p.nst + to_days({h}), false) AS rv{h}" for h in hs)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rv_x AS
        SELECT p.*, v.ev_tu, l.rv_tu, {flags}
        FROM rv_p p LEFT JOIN rv_ev v USING (nid) LEFT JOIN rv_ls l USING (nid)""")
    n_crl = con.execute('SELECT count(*) FROM rv_crl').fetchone()[0]
    for tab in ('rv_p', 'rv_day', 'rv_crl', 'rv_ev', 'rv_ls'):
        con.execute(f'DROP TABLE IF EXISTS {tab}')
    n, n_early, n_rv = con.execute(f'SELECT count(*), count(*) FILTER (WHERE early), '
                                   f'count(*) FILTER (WHERE rv{max(hs)}) FROM rv_x').fetchone()
    log(f'm_pairs: {rir}: {n:,} pairs, {n_early:,} early, {n_rv:,} revoked within {max(hs)} d, {n_crl:,} CRLs '
        f'[{time.time() - started:.1f}s, peak RSS {common.peak_rss_mb()} MB]')
    return hs


def revocation(ctx, con, rir, wf, wt, p):
    """Rows of roa_ee_revocation for the weeks wf..wt from TEMP TABLE pairs."""
    hs = revocation_flags(ctx, con, rir, wf, wt + dt.timedelta(days=7), p)
    specs = revocation_stats(hs)
    cols = {}                                   # one (n, s) column pair per distinct (subset, numerator)
    for _, sub, num, _k in specs:
        cols.setdefault((sub, num), len(cols))
    con.execute(f"CREATE OR REPLACE TEMP TABLE rv_pop AS {classify.populations_sql('rv_x', 'host', rir)}")
    per_key = ', '.join(f"count(*) FILTER (WHERE {sub}) AS n{i}"
                        + (f", count(*) FILTER (WHERE ({sub}) AND ({num})) AS s{i}" if num else '')
                        for (sub, num), i in cols.items())
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rv_k AS
        SELECT week, population, operator, kid, {per_key} FROM rv_pop GROUP BY ALL""")
    sel = []
    for stat, sub, num, kind in specs:
        i = cols[(sub, num)]
        n = f'sum(n{i})'
        value = {'count': n, 'share': f'sum(s{i}) / nullif({n}, 0)', 'ca': _ca_mean(f's{i}', f'n{i}')}[kind]
        sel.append(f"""SELECT week, population, operator, {lit(stat)} AS stat, {value} AS value, {n} AS n,
                   count(*) FILTER (WHERE n{i} > 0 AND kid IS NOT NULL) AS n_ca,
                   max(n{i}) FILTER (WHERE n{i} > 0 AND kid IS NOT NULL) / nullif({n}, 0) AS top1_share
            FROM rv_k GROUP BY week, population, operator""")
    con.execute(f"CREATE OR REPLACE TEMP TABLE rv_agg AS {' UNION ALL '.join(sel)}")
    _frame(con, 'rv_frame', wf, wt)
    stats = ', '.join(f'({lit(s)}, {lit(k)})' for s, _, _, k in specs)
    r = lit(rir)
    rows = f"""
        SELECT a.week, '1w' AS "window", {r} AS rir, a.population, a.operator, {lit(REV)} AS metric_id, a.stat,
               a.value, a.n, a.n_ca, a.top1_share
        FROM rv_agg a WHERE a.population <> 'all'
        UNION ALL
        SELECT f.week, '1w', {r}, 'all', '', {lit(REV)}, s.stat,
               coalesce(a.value, CASE WHEN s.kind = 'count' THEN 0 END), coalesce(a.n, 0), coalesce(a.n_ca, 0),
               a.top1_share
        FROM rv_frame f CROSS JOIN (VALUES {stats}) s(stat, kind)
             LEFT JOIN rv_agg a ON a.week = f.week AND a.stat = s.stat AND a.population = 'all'"""
    ctx.emit(con, rows)
    for tab in ('rv_x', 'rv_pop', 'rv_k', 'rv_agg', 'rv_frame'):
        con.execute(f'DROP TABLE IF EXISTS {tab}')


# ── resign_consumed ──────────────────────────────────────────────────────────
def _wk(d):
    """Week index of the Monday of d (days since WEEK0 // 7)."""
    return (common.monday(d) - WEEK0).days // 7


def consumed(ctx, con, rir, wf, wt, p):
    """Rows and histogram of resign_consumed for the weeks wf..wt from TEMP TABLE pairs (successors from
    51 weeks before wf)."""
    started = time.time()
    c_lo, c_hi = (float(x) for x in p['consumed_range'])
    na_gt_nb = 'na > nb' if p.get('require_na_gt_nb', True) else 'true'
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cs_p AS
        SELECT * FROM (
            SELECT datediff('day', {date_lit(WEEK0)}, week) // 7 AS wk, week, kid, host,
                   (epoch(nst) - epoch(nb)) / nullif(epoch(na) - epoch(nb), 0) AS consumed,
                   (epoch(nst) - epoch(st)) / 86400.0 AS interval_d,
                   NOT coalesce(daily, false) AS transient
            FROM pairs WHERE {na_gt_nb})
        WHERE consumed >= {c_lo!r} AND consumed <= {c_hi!r}""")
    con.execute(f"CREATE OR REPLACE TEMP TABLE cs_pop AS {classify.populations_sql('cs_p', 'host', rir)}")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cs_win AS
        SELECT * FROM (VALUES {', '.join(f'({lit(w)}, {k})' for w, k in WINDOWS)}) v(win, k)""")
    w0, w1 = _wk(wf), _wk(wt)
    # chunks of output weeks of at most ROW_BUDGET expanded rows (a week alone may exceed it)
    counts = dict(con.execute('SELECT wk, count(*) FROM cs_pop GROUP BY wk').fetchall())
    size_of = {w: sum(counts.get(x, 0) for _, k in WINDOWS for x in range(w - k + 1, w + 1)) for w in range(w0, w1 + 1)}
    chunks = []
    for w in range(w0, w1 + 1):
        if not chunks or chunks[-1][2] + size_of[w] > ROW_BUDGET:
            chunks.append([w, w, 0])
        chunks[-1][1] = w
        chunks[-1][2] += size_of[w]
    con.execute("""CREATE OR REPLACE TEMP TABLE cs_agg (wk BIGINT, win VARCHAR, population VARCHAR, operator VARCHAR,
        n BIGINT, qc DOUBLE[], qi DOUBLE[], ge075 BIGINT, gt100 BIGINT, n_all BIGINT, qc_all DOUBLE, ge075_all BIGINT,
        n_ca BIGINT, top1 DOUBLE, ca_p50 DOUBLE, n_ca_all BIGINT, top1_all DOUBLE)""")
    qs = '[' + ', '.join(repr(q) for q in QS) + ']'
    for c0, c1, _ in chunks:
        # every pair once per (output week, window) that contains it
        x = f"""(SELECT p.population, p.operator, p.kid, p.consumed, p.interval_d, p.transient, w.win,
                        unnest(range(greatest(p.wk, {c0}), least(p.wk + w.k - 1, {c1}) + 1)) AS wk
                 FROM cs_pop p CROSS JOIN cs_win w WHERE p.wk <= {c1} AND p.wk + w.k - 1 >= {c0})"""
        con.execute(f"""CREATE OR REPLACE TEMP TABLE cs_a AS
            SELECT wk, win, population, operator,
                   count(*) FILTER (WHERE NOT transient) AS n,
                   quantile_cont(consumed, {qs}) FILTER (WHERE NOT transient) AS qc,
                   quantile_cont(interval_d, {qs}) FILTER (WHERE NOT transient) AS qi,
                   count(*) FILTER (WHERE NOT transient AND consumed >= 0.75) AS ge075,
                   count(*) FILTER (WHERE NOT transient AND consumed > 1) AS gt100,
                   count(*) AS n_all, quantile_cont(consumed, 0.5) AS qc_all,
                   count(*) FILTER (WHERE consumed >= 0.75) AS ge075_all
            FROM {x} GROUP BY ALL""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE cs_k AS
            SELECT wk, win, population, operator,
                   count(*) FILTER (WHERE n_k > 0 AND kid IS NOT NULL) AS n_ca,
                   max(n_k) FILTER (WHERE n_k > 0 AND kid IS NOT NULL) / nullif(sum(n_k), 0) AS top1,
                   quantile_cont(m_k, 0.5) FILTER (WHERE n_k > 0 AND kid IS NOT NULL) AS ca_p50,
                   count(*) FILTER (WHERE kid IS NOT NULL) AS n_ca_all,
                   max(n_all_k) FILTER (WHERE kid IS NOT NULL) / sum(n_all_k) AS top1_all
            FROM (SELECT wk, win, population, operator, kid, count(*) FILTER (WHERE NOT transient) AS n_k,
                         quantile_cont(consumed, 0.5) FILTER (WHERE NOT transient) AS m_k, count(*) AS n_all_k
                  FROM {x} GROUP BY ALL)
            GROUP BY ALL""")
        con.execute("""INSERT INTO cs_agg
            SELECT a.wk, a.win, a.population, a.operator, a.n, a.qc, a.qi, a.ge075, a.gt100, a.n_all, a.qc_all,
                   a.ge075_all, k.n_ca, k.top1, k.ca_p50, k.n_ca_all, k.top1_all
            FROM cs_a a JOIN cs_k k USING (wk, win, population, operator)""")
    for tab in ('cs_a', 'cs_k'):
        con.execute(f'DROP TABLE IF EXISTS {tab}')
    log(f'm_pairs: {rir}: resign_consumed {sum(c[2] for c in chunks):,} expanded rows in {len(chunks)} chunk(s) '
        f'[{time.time() - started:.1f}s, peak RSS {common.peak_rss_mb()} MB]')
    # stat: value, and the (n, n_ca, top1) it is over
    main, alls = ('n', 'n_ca', 'top1'), ('n_all', 'n_ca_all', 'top1_all')
    specs = [('pairs', 'n', main), ('consumed_p10', 'qc[1]', main), ('consumed_p50', 'qc[2]', main),
             ('consumed_p90', 'qc[3]', main), ('consumed_p50_ca', 'ca_p50', main),
             ('share_ge075', 'ge075 / nullif(n, 0)', main), ('share_gt100', 'gt100 / nullif(n, 0)', main),
             ('interval_p10_d', 'qi[1]', main), ('interval_p50_d', 'qi[2]', main), ('interval_p90_d', 'qi[3]', main),
             ('pairs_incl_transient', 'n_all', alls), ('consumed_p50_incl_transient', 'qc_all', alls),
             ('share_ge075_incl_transient', 'ge075_all / nullif(n_all, 0)', alls)]
    _frame(con, 'cs_frame', wf, wt)
    era2_wk = _wk(common.ERA2_FROM)
    r = lit(rir)
    sel = []
    for stat, value, (n, nca, top) in specs:
        era2_only = stat.endswith('_incl_transient')     # emitted from the first week with era-2 data
        count = stat in ('pairs', 'pairs_incl_transient')
        sel.append(f"""
            SELECT {date_lit(WEEK0)} + CAST(7 * a.wk AS INTEGER) AS week, a.win AS "window", {r} AS rir,
                   a.population, a.operator, {lit(CONS)} AS metric_id, {lit(stat)} AS stat, {value} AS value,
                   a.{n} AS n, a.{nca} AS n_ca, CASE WHEN a.{n} > 0 THEN a.{top} END AS top1_share
            FROM cs_agg a WHERE a.population <> 'all'{f' AND a.wk >= {era2_wk}' if era2_only else ''}""")
        sel.append(f"""
            SELECT f.week, w.win, {r}, 'all', '', {lit(CONS)}, {lit(stat)},
                   {f'coalesce({value}, 0)' if count else value}, coalesce(a.{n}, 0), coalesce(a.{nca}, 0),
                   CASE WHEN a.{n} > 0 THEN a.{top} END
            FROM cs_frame f CROSS JOIN cs_win w
                 LEFT JOIN cs_agg a ON a.wk = datediff('day', {date_lit(WEEK0)}, f.week) // 7 AND a.win = w.win
                                   AND a.population = 'all'
            {f"WHERE datediff('day', {date_lit(WEEK0)}, f.week) // 7 >= {era2_wk}" if era2_only else ''}""")
    hist = f"""
        SELECT week, '1w' AS "window", {r} AS rir, population, operator, {lit(CONS)} AS metric_id,
               'consumed' AS stat, b / 100.0 AS bin_lo, (b + 1) / 100.0 AS bin_hi, count(*) AS count
        FROM (SELECT week, population, operator, CAST(least(floor(consumed * 100), 149) AS INTEGER) AS b
              FROM cs_pop WHERE NOT transient AND wk BETWEEN {w0} AND {w1})
        GROUP BY ALL"""
    ctx.emit(con, ' UNION ALL '.join(sel), hist)
    for tab in ('cs_p', 'cs_pop', 'cs_win', 'cs_agg', 'cs_frame'):
        con.execute(f'DROP TABLE IF EXISTS {tab}')
