"""CA certificates and the live inventory: ca_cert_lifetime, live_inventory.

ca_cert_lifetime, windows 1w, 13w and 52w (the certificates of weeks W-12..W and W-51..W pooled):
- newly issued non-root CA certificates: rows of the RIR's own table=certs/is_ca=true (the warehouse that
  parsed the certificate first: head rows sit in one warehouse, ids are global) with is_ca_flag, aki not
  NULL and <> ski, not_after > not_before and not_before in the week (UTC), observed by the input cut (a
  presence run in the RIR's warehouse that starts by as_of). Per week and key
  (ski) only the latest certificate counts (largest not_before, then cert_id): era 2 sees every re-issue,
  and registro.br re-issues each of its CA certificates about daily.
- duration = not_after - not_before in days (fractional); p10 / p50 / p90 with quantile_disc (an observed
  duration, never a value between two issuers' policies such as 364 and 365); modal_not_after = the most
  common not_after date (UTC; ties: the earliest) as the number YYYYMMDD, and its share of n; histogram
  duration_d in 7-day bins up to 4,200 days plus an overflow bin, 1w only.
- population: the issuer's class, i.e. the publication host of the parent key: ca_keys.host of the
  child's aki (the SIA host of the parent's newest certificate in any of the five warehouses). APNIC's own
  CA (rpki.apnic.net) issues about 422 days, the NIRs under it 364-365 days; the issuer's host tells them
  apart, the certificate's own SIA does not (ARIN issues the RPS-hosted CAs' certificates). A parent key
  without any CA certificate gives 'unknown'.
- n = certificates, n_ca = distinct certified keys (ski) and top1_share = the largest key's share of n
  (1/n in a 1w window; more in 13w/52w windows for keys re-issued in several weeks). Not the issuing key:
  the headline population is one issuer (the RIR's own CA, one or two keys), so issuing keys would make
  every rir_hosted row 'insufficient'.
- Fixed-expiry issuers (AFRINIC: every certificate until 2027-03-31; RIPE NCC hosted: 2027-07-01) make
  the 1w durations fall by about 7 days a week; the 52w window is the stable value (close to a calendar
  year, but in UTC ISO weeks). They re-issue in one batch just after 1 January 00:00 UTC, so a 52w window
  that contains no 1 January (the weeks of 2012-12-24, 2017-12-25 and 2023-12-25; next 2028-12-25) holds
  no batch, and its count and durations fall for that one week.
- window 'end' (point in time at the week's end, same issuer classes): live_certs = non-root CA
  certificates (not_after > not_before) present at the week's instant (below), and the shares of them
  whose not_after is within 30 / 90 days of the instant (live_expiring_30d / _90d; certificates already
  expired but still in the era-2 cache count as expiring). n_ca / top1_share by ski.

live_inventory (window 'end', census):
- ca_keys: non-root CA keys (is_ca_flag, aki not NULL and <> ski) with a certificate present at the
  instant, by their own publication host (the SIA host of the key's newest present certificate); value =
  n = n_ca, top1_share = 1/n.
- roas: ROA objects present at the instant (object_signatures.object_type 'ROA'), by the host of their
  signer EE certificate's SIA (object_signatures.signer_cert_id -> certs/is_ca=false); n_ca = distinct
  issuing keys (EE aki), top1_share = the largest key's share of the ROAs.
- Era-2 counts include files still in the rpki-client cache (stale, orphaned, expired); era 1 holds the
  archive's validated trees only.

The week's instant: the last snapshot of the RIR in [W, W + 7 days) that the observation ledger
(cycles.parquet) marks good and that is complete: not partial (>= 0.9 x the median file count of its
+-12 h) and >= 0.98 x the median of its +-7 days (COMPLETE_SHARE); else the last good one; else none: the
week then has 'all' rows with value NULL and n = 0 (masked by the ledger as well). Era 1: the archive's
daily tree of the last good day (normally Sunday's); era 2: normally the Sunday 23:40 cycle. The 7-day
reference catches a slow decline, which the 12-hour median follows: on 2026-10-09..10-12 APNIC fell to
66% and LACNIC to 47% of their files over three days, their 10-11 cycles are not 'partial', and cycles
just above 0.9 x the 7-day median already missed repositories (week 10-05 read 8-12% fewer live ROAs).
Normal snapshots are within 1% of that median; at 0.98 the instant moves back from the last good snapshot
in 2-5 era-2 weeks per RIR (the degraded April cycles and the week of 10-05; by up to 59 h) and in 0-26
of about 745 era-1 weeks. Present at the instant = an object_presence_runs run of the RIR's warehouse
with start_ts <= instant <= end_ts (the run labels are the ledger's snapshot labels). Presence decides
which warehouse an object is in; its attributes come from whichever warehouse holds its head rows (an
object first parsed under another trust anchor in era 1 and filed under this RIR's hosts in era 2). The
CA certificates and the ROA -> signer EE table of all five warehouses are read once per run and cached
in the run's work directory (CA_CERTS_FILE, ROA_EE_FILE).

Rows for population 'all' exist in every week and window of the range (value NULL / n = 0 when nothing
was observed); other populations only where they have objects. Keys, hosts and populations are integer
codes inside the grouped passes (a full-history pass joins about 10 M live certificate-weeks and up to
70 M live ROA-weeks per RIR).
"""
import datetime as dt
import os
import time

from . import classify, common
from .common import date_lit, lit, log

LIFETIME, INVENTORY = 'ca_cert_lifetime', 'live_inventory'
CA_CERTS_FILE = 'm_ca_certs.parquet'
ROA_EE_FILE = 'm_ca_roa_ee.parquet'
WINDOWS = (('1w', 1), ('13w', 13), ('52w', 52))
HIST_BIN_D, HIST_MAX_D = 7, 4200          # duration_d histogram: 7-day bins to 4,200 days, then overflow
COMPLETE_SHARE = 0.98                     # a complete snapshot: >= 0.98 x the 7-day median file count
LIFETIME_STATS = ('certs', 'duration_p10_d', 'duration_p50_d', 'duration_p90_d', 'modal_not_after',
                  'modal_not_after_share')
END_STATS = ('live_certs', 'live_expiring_30d', 'live_expiring_90d')
INVENTORY_STATS = ('ca_keys', 'roas')


def connect(ctx):
    """ctx.connect() without DuckDB's external file cache: it keeps the Parquet pages it read in the
    buffer pool, so scanning the EE certificates of ARIN (2.8 GB) or RIPE NCC (17 GB) would hold the
    process at memory_limit plus overhead (7.3 GB peak RSS at 6GB, 5.1 GB without the cache)."""
    con = ctx.connect()
    con.execute("SET enable_external_file_cache = false")
    return con


def non_root(a='c'):
    """A non-root CA certificate (alias a): CA flag set, aki present and not its own key."""
    return f"{a}.is_ca_flag AND {a}.aki IS NOT NULL AND {a}.aki <> {a}.ski"


# ── shared tables (all five warehouses, cached for the run) ──────────────────
def ca_certs_path(ctx):
    """ctx.work/m_ca_certs.parquet: every CA certificate (repository object) of the five warehouses:
    wh (the warehouse holding its head rows), object_id, cert_id, ski, aki, nb, na (TIMESTAMP, UTC), host
    (SIA host of the certificate itself), is_ca_flag. About 680,000 rows, 5-8 s; built once per run."""
    path = os.path.join(ctx.work, CA_CERTS_FILE)
    if os.path.exists(path):
        return path
    t = time.time()
    con = connect(ctx)
    try:
        con.execute("""CREATE TEMP TABLE c (wh VARCHAR, object_id INTEGER, cert_id INTEGER, ski VARCHAR, aki VARCHAR,
                                            nb TIMESTAMP, na TIMESTAMP, host VARCHAR, is_ca_flag BOOLEAN)""")
        for r in common.REGISTRIES:
            if not ctx.has_table(r, 'certs/is_ca=true'):
                continue
            ctx.guard(f'warehouse-{r} certs/is_ca=true')
            src = ctx.src(r, 'certs/is_ca=true', ['object_id', 'cert_id', 'ski', 'aki', 'not_before', 'not_after',
                                                  'sia', 'is_ca_flag'])
            ctx.run(con, f"""INSERT INTO c
                SELECT {lit(r)}, object_id, cert_id, ski, aki, CAST(not_before AS TIMESTAMP),
                       CAST(not_after AS TIMESTAMP), scs_host(sia), coalesce(is_ca_flag, false)
                FROM {src} WHERE object_id IS NOT NULL""")
        common.copy_to_parquet(con, 'SELECT * FROM c', path, order_by='object_id, wh, cert_id')
        n = con.execute('SELECT count(*) FROM c').fetchone()[0]
    finally:
        con.close()
    log(f'm_ca: {n:,} CA certificates of the five warehouses [{time.time() - t:.1f}s]')
    return path


def roa_ee_path(ctx):
    """ctx.work/m_ca_roa_ee.parquet: one row per ROA object of the five warehouses: object_id, aki (issuing
    key = the signer EE certificate's aki) and host (that EE certificate's SIA host), from the warehouse
    holding its head rows (object_signatures.object_type 'ROA' -> signer_cert_id -> certs/is_ca=false).
    About 4.5 M rows; one scan of each warehouse's EE certificates, 25-35 s in all (peak RSS about 5 GB,
    ARIN's 3 M ROA signers), built once per run."""
    path = os.path.join(ctx.work, ROA_EE_FILE)
    if os.path.exists(path):
        return path
    t = time.time()
    con = connect(ctx)
    try:
        con.execute("CREATE TEMP TABLE e (wh VARCHAR, object_id INTEGER, aki VARCHAR, host VARCHAR)")
        for r in common.REGISTRIES:
            if not (ctx.has_table(r, 'object_signatures') and ctx.has_table(r, 'certs/is_ca=false')):
                continue
            ctx.guard(f'warehouse-{r} object_signatures, certs/is_ca=false')
            sig = ctx.src(r, 'object_signatures', ['object_id', 'object_type', 'signer_cert_id'])
            ctx.run(con, f"""CREATE OR REPLACE TEMP TABLE s AS
                SELECT object_id, min(signer_cert_id) AS signer FROM {sig}
                WHERE object_type = 'ROA' AND object_id IS NOT NULL GROUP BY object_id""")
            ee = ctx.src(r, 'certs/is_ca=false', ['cert_id', 'aki', 'sia'])
            # an object id has head rows in one warehouse (the first that parsed it); keep the first anyway
            ctx.run(con, f"""INSERT INTO e
                SELECT {lit(r)}, s.object_id, c.aki, c.host
                FROM s LEFT JOIN (SELECT cert_id, min(aki) AS aki, min(scs_host(sia)) AS host FROM {ee}
                                  WHERE cert_id IN (SELECT signer FROM s) GROUP BY cert_id) c ON c.cert_id = s.signer
                WHERE s.object_id NOT IN (SELECT object_id FROM e)""")
        con.execute("DROP TABLE IF EXISTS s")
        common.copy_to_parquet(con, 'SELECT * FROM e', path, order_by='object_id')
        n = con.execute('SELECT count(*) FROM e').fetchone()[0]
    finally:
        con.close()
    log(f'm_ca: {n:,} ROA objects of the five warehouses with their signer EE [{time.time() - t:.1f}s]')
    return path


# ── SQL builders (inputs are table names) ────────────────────────────────────
def _ts(t):
    """TIMESTAMP literal (naive UTC) of a date (00:00) or datetime."""
    if not isinstance(t, dt.datetime):
        t = dt.datetime.combine(common.to_date(t), dt.time())
    return f"TIMESTAMP '{common.parse_ts(t).strftime('%Y-%m-%d %H:%M:%S')}'"


def instants_sql(cycles, rir, week_from, week_to, as_of):
    """(week, t): each week's instant (module docstring) from the ledger's cycles table; weeks without a
    good snapshot have no row."""
    end = common.monday(week_to) + dt.timedelta(days=7)
    complete = f"good AND NOT partial AND file_count >= {COMPLETE_SHARE} * ref_long"
    return f"""
SELECT scs_week(ts) AS week,
       coalesce(max(ts) FILTER (WHERE {complete}), max(ts) FILTER (WHERE good)) AS t
FROM {cycles}
WHERE rir = {lit(rir)} AND ts >= {_ts(common.monday(week_from))} AND ts < {_ts(end)} AND ts <= {_ts(as_of)}
GROUP BY ALL HAVING t IS NOT NULL"""


def live_sql(inst, runs):
    """(week, t, object_id) of the objects of `runs` (object_id, s, e) present at each week's instant."""
    return f"SELECT i.week, i.t, r.object_id FROM {inst} i JOIN {runs} r ON r.s <= i.t AND r.e >= i.t"


def new_certs_sql(certs, runs, rir, nb_from, nb_to, as_of):
    """Newly issued non-root CA certificates of warehouse `rir` with not_before in [nb_from, nb_to) and a
    presence run starting by as_of, the latest per (week, ski): week, ski, aki, nb, na, cert_id, dur_d."""
    return f"""
SELECT scs_week(c.nb) AS week, c.ski, c.aki, c.nb, c.na, c.cert_id, epoch(c.na - c.nb) / 86400.0 AS dur_d
FROM {certs} c
WHERE c.wh = {lit(rir)} AND {non_root('c')} AND c.na > c.nb AND c.nb >= {_ts(nb_from)} AND c.nb < {_ts(nb_to)}
  AND c.object_id IN (SELECT object_id FROM {runs} WHERE s <= {_ts(as_of)})
QUALIFY row_number() OVER (PARTITION BY scs_week(c.nb), c.ski ORDER BY c.nb DESC, c.cert_id DESC) = 1"""


def host_populations(con, rir, hosts_sql):
    """TEMP TABLES hdim(host, hid) over the hosts of hosts_sql ('' = no host) and hpop(hid, population,
    operator, gid): every reported population of each host in warehouse `rir` (classify.populations_sql),
    gid = a code per (population, operator)."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE hdim AS
        SELECT host, CAST(row_number() OVER (ORDER BY host) AS INTEGER) AS hid
        FROM (SELECT DISTINCT coalesce(host, '') AS host FROM ({hosts_sql}) UNION SELECT '')""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE hpop AS
        SELECT hid, population, operator,
               CAST(dense_rank() OVER (ORDER BY population, operator) AS SMALLINT) AS gid
        FROM ({classify.populations_sql('hdim', 'host', rir)})""")


def _frame(con, name, week_from, week_to, windows):
    """TEMP TABLE name(week, "window"): every week of the range x the given windows."""
    w = ', '.join(f'({lit(x)})' for x in windows)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
        SELECT CAST(range AS DATE) AS week, v.win AS "window"
        FROM range(CAST({date_lit(week_from)} AS TIMESTAMP), CAST({date_lit(week_to)} AS TIMESTAMP) + INTERVAL 7 DAY,
                   INTERVAL 7 DAY) CROSS JOIN (VALUES {w}) v(win)""")


def _with_frame(agg, frame, cols, keys=('week', '"window"')):
    """Rows of agg (keys, population, operator, cols) plus a row of NULLs for population 'all' in every
    row of frame (keys) that agg lacks."""
    on = ' AND '.join(f'a.{k} = f.{k}' for k in keys)
    return f"""
SELECT {', '.join(f'f.{k}' for k in keys)}, 'all' AS population, '' AS operator, {', '.join(f'a.{c}' for c in cols)}
FROM {frame} f LEFT JOIN {agg} a ON {on} AND a.population = 'all'
UNION ALL
SELECT {', '.join(keys)}, population, operator, {', '.join(cols)} FROM {agg} WHERE population <> 'all'"""


# ── ca_cert_lifetime ─────────────────────────────────────────────────────────
def _lifetime(ctx, con, rir, week_from, week_to):
    """TEMP TABLES life_rows and life_hist: newly issued certificates, windows 1w / 13w / 52w."""
    lb = week_from - dt.timedelta(days=7 * (WINDOWS[-1][1] - 1))
    ub = week_to + dt.timedelta(days=7)
    ctx.run(con, f"CREATE OR REPLACE TEMP TABLE newc AS {new_certs_sql('ca_certs', 'ca_runs', rir, lb, ub, ctx.as_of)}")
    # one row per (week, certificate, population of its issuer); key and population as integer codes
    con.execute("""CREATE OR REPLACE TEMP TABLE lt AS
        SELECT n.week, p.gid, CAST(dense_rank() OVER (ORDER BY n.ski) AS INTEGER) AS kid, n.dur_d,
               CAST(n.na AS DATE) AS na_day
        FROM newc n LEFT JOIN ca_keys k ON k.ski = n.aki JOIN hdim h ON h.host = coalesce(k.host, '')
             JOIN hpop p USING (hid)""")
    _frame(con, 'lframe', week_from, week_to, [w for w, _ in WINDOWS])
    wins = ', '.join(f'({lit(w)}, {k})' for w, k in WINDOWS)
    # every target week and window with the certificates of its weeks (week - 7 (k - 1) .. week)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE lw AS
        SELECT t.week, t.k, l.gid, l.kid, l.dur_d, l.na_day
        FROM (SELECT DISTINCT f.week, v.k FROM lframe f JOIN (VALUES {wins}) v(win, k) ON v.win = f."window") t
             JOIN lt l ON l.week <= t.week AND l.week >= t.week - 7 * (t.k - 1)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE lagg AS
        WITH q AS (SELECT week, k, gid, count(*) AS n, quantile_disc(dur_d, [0.1, 0.5, 0.9]) AS qs
                   FROM lw GROUP BY ALL),
        pk AS (SELECT week, k, gid, kid, count(*) AS c FROM lw GROUP BY ALL),
        kk AS (SELECT week, k, gid, count(*) AS n_ca, max(c) AS top FROM pk GROUP BY ALL),
        md AS (SELECT week, k, gid, na_day, count(*) AS c FROM lw GROUP BY week, k, gid, na_day
               QUALIFY row_number() OVER (PARTITION BY week, k, gid ORDER BY c DESC, na_day) = 1)
        SELECT q.week, v.win AS "window", p.population, p.operator, q.n, kk.n_ca, kk.top, q.qs,
               md.na_day AS mode_day, md.c AS mode_n
        FROM q JOIN kk USING (week, k, gid) JOIN md USING (week, k, gid)
             JOIN (SELECT DISTINCT gid, population, operator FROM hpop) p USING (gid)
             JOIN (VALUES {wins}) v(win, k) ON v.k = q.k""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE lagg2 AS
        {_with_frame('lagg', 'lframe', ('n', 'n_ca', 'top', 'qs', 'mode_day', 'mode_n'))}""")
    stats = ', '.join(f'({lit(s)})' for s in LIFETIME_STATS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE life_rows AS
        SELECT week, "window", {lit(rir)} AS rir, population, operator, {lit(LIFETIME)} AS metric_id, s.stat,
               CASE s.stat WHEN 'certs' THEN coalesce(n, 0)
                           WHEN 'duration_p10_d' THEN qs[1] WHEN 'duration_p50_d' THEN qs[2]
                           WHEN 'duration_p90_d' THEN qs[3]
                           WHEN 'modal_not_after' THEN year(mode_day) * 10000 + month(mode_day) * 100 + day(mode_day)
                           ELSE mode_n / n END AS value,
               coalesce(n, 0) AS n, coalesce(n_ca, 0) AS n_ca, top / n AS top1_share
        FROM lagg2 CROSS JOIN (VALUES {stats}) s(stat)""")
    nb = HIST_MAX_D // HIST_BIN_D
    con.execute(f"""CREATE OR REPLACE TEMP TABLE life_hist AS
        SELECT l.week, '1w' AS "window", {lit(rir)} AS rir, p.population, p.operator, {lit(LIFETIME)} AS metric_id,
               'duration_d' AS stat, l.b * {HIST_BIN_D} AS bin_lo,
               CASE WHEN l.b < {nb} THEN (l.b + 1) * {HIST_BIN_D} END AS bin_hi, count(*) AS count
        FROM (SELECT week, gid, CAST(least(greatest(floor(dur_d / {HIST_BIN_D}), 0), {nb}) AS INTEGER) AS b
              FROM lt WHERE week >= {date_lit(week_from)}) l
             JOIN (SELECT DISTINCT gid, population, operator FROM hpop) p USING (gid)
        GROUP BY ALL""")


def _lifetime_end(con, rir, week_from, week_to):
    """TEMP TABLE end_rows: live CA certificates at the week's instant by issuer class (window 'end')."""
    con.execute("""CREATE OR REPLACE TEMP TABLE eagg AS
        WITH lk AS (SELECT l.week, c.ihid, c.kid, count(*) AS c,
                           count(*) FILTER (WHERE c.na <= l.t + INTERVAL 30 DAY) AS c30,
                           count(*) FILTER (WHERE c.na <= l.t + INTERVAL 90 DAY) AS c90
                    FROM live l JOIN ca_live c USING (object_id) WHERE c.na > c.nb GROUP BY ALL),
        pk AS (SELECT lk.week, p.population, p.operator, lk.kid, sum(lk.c) AS c, sum(lk.c30) AS c30,
                      sum(lk.c90) AS c90
               FROM lk JOIN hpop p ON p.hid = lk.ihid GROUP BY ALL)
        SELECT week, 'end' AS "window", population, operator, sum(c) AS n, sum(c30) AS n30, sum(c90) AS n90,
               count(*) AS n_ca, max(c) AS top
        FROM pk GROUP BY ALL""")
    _frame(con, 'eframe', week_from, week_to, ['end'])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE eagg2 AS
        SELECT x.*, i.t IS NOT NULL AS observed
        FROM ({_with_frame('eagg', 'eframe', ('n', 'n30', 'n90', 'n_ca', 'top'))}) x LEFT JOIN inst i USING (week)""")
    stats = ', '.join(f'({lit(s)})' for s in END_STATS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE end_rows AS
        SELECT week, "window", {lit(rir)} AS rir, population, operator, {lit(LIFETIME)} AS metric_id, s.stat,
               CASE s.stat WHEN 'live_certs' THEN CASE WHEN observed THEN coalesce(n, 0) END
                           WHEN 'live_expiring_30d' THEN n30 / n ELSE n90 / n END AS value,
               coalesce(n, 0) AS n, coalesce(n_ca, 0) AS n_ca, top / n AS top1_share
        FROM eagg2 CROSS JOIN (VALUES {stats}) s(stat)""")


# ── live_inventory ───────────────────────────────────────────────────────────
def _inventory(con, rir, week_from, week_to, roas):
    """TEMP TABLE inv_rows: live CA keys by their own host, live ROA objects by their signer EE host."""
    con.execute("""CREATE OR REPLACE TEMP TABLE iagg AS
        WITH kh AS (SELECT l.week, c.kid, arg_max(c.ohid, (c.nb, c.cert_id)) AS hid
                    FROM live l JOIN ca_live c USING (object_id) GROUP BY ALL)
        SELECT kh.week, p.population, p.operator, 'ca_keys' AS stat, count(*) AS n, count(*) AS n_ca,
               CAST(1 AS DOUBLE) / count(*) AS top
        FROM kh JOIN hpop p USING (hid) GROUP BY ALL""")
    if roas:
        con.execute("""INSERT INTO iagg
            WITH lr AS (SELECT i.week, e.hid, e.akid, count(*) AS c
                        FROM inst i JOIN roa_runs r ON r.s <= i.t AND r.e >= i.t JOIN roa_e e USING (object_id)
                        GROUP BY ALL),
            pk AS (SELECT lr.week, p.population, p.operator, lr.akid, sum(lr.c) AS c
                   FROM lr JOIN hpop p USING (hid) GROUP BY ALL)
            SELECT week, population, operator, 'roas', sum(c), count(akid),
                   max(c) FILTER (WHERE akid IS NOT NULL) / sum(c)
            FROM pk GROUP BY ALL""")
    _frame(con, 'iframe', week_from, week_to, ['end'])
    stats = ', '.join(f'({lit(s)})' for s in INVENTORY_STATS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE inv_rows AS
        WITH f AS (SELECT f.week, f."window", s.stat, i.t IS NOT NULL AS observed
                   FROM iframe f CROSS JOIN (VALUES {stats}) s(stat) LEFT JOIN inst i USING (week))
        SELECT f.week, f."window", {lit(rir)} AS rir, 'all' AS population, '' AS operator,
               {lit(INVENTORY)} AS metric_id, f.stat, CASE WHEN f.observed THEN coalesce(a.n, 0) END AS value,
               coalesce(a.n, 0) AS n, coalesce(a.n_ca, 0) AS n_ca, a.top AS top1_share
        FROM f LEFT JOIN iagg a ON a.week = f.week AND a.stat = f.stat AND a.population = 'all'
        UNION ALL
        SELECT week, 'end', {lit(rir)}, population, operator, {lit(INVENTORY)}, stat, n, n, n_ca, top
        FROM iagg WHERE population <> 'all'""")


# ── module entry point ───────────────────────────────────────────────────────
def compute(ctx, rir, week_from, week_to):
    want_life, want_inv = LIFETIME in ctx.metrics, INVENTORY in ctx.metrics
    if not (want_life or want_inv):
        return
    for t in ('certs/is_ca=true', 'object_presence_runs') + (('object_signatures',) if want_inv else ()):
        if not ctx.has_table(rir, t):        # a missing table must not pass as a week without objects
            raise FileNotFoundError(f'm_ca: warehouse-{rir} has no table={t}')
    wf, wt = common.monday(week_from), common.monday(week_to)
    certs_path = ca_certs_path(ctx)
    roa_path = roa_ee_path(ctx) if want_inv else None
    con = connect(ctx)
    try:
        ctx.load_hostmap(con)
        ctx.load_ca_keys(con)
        ctx.load_cycles(con)
        con.execute(f"CREATE TEMP TABLE ca_certs AS SELECT * FROM read_parquet({lit(certs_path)})")
        con.execute(f"CREATE TEMP TABLE inst AS {instants_sql('cycles', rir, wf, wt, ctx.as_of)}")
        t_min, t_max = con.execute("SELECT CAST(min(t) AS VARCHAR), CAST(max(t) AS VARCHAR) FROM inst").fetchone()
        roas = want_inv and t_min is not None
        if roas:
            con.execute(f"CREATE TEMP TABLE roa_ee AS SELECT object_id, aki, host FROM read_parquet({lit(roa_path)})")
        # presence of this warehouse, copied out under the shared lock: every run of a CA certificate (first
        # seen by as_of; present at the instants) and the ROA runs that span an instant of the range
        t = time.time()
        with ctx.presence_lock():
            runs = ctx.src(rir, 'object_presence_runs', ['object_id', 'start_ts', 'end_ts'])
            ctx.run(con, f"""CREATE TEMP TABLE ca_runs AS
                SELECT object_id, CAST(start_ts AS TIMESTAMP) AS s, CAST(end_ts AS TIMESTAMP) AS e FROM {runs}
                WHERE object_id IN (SELECT object_id FROM ca_certs)""")
            if roas:
                ctx.run(con, f"""CREATE TEMP TABLE roa_runs AS
                    SELECT object_id, CAST(start_ts AS TIMESTAMP) AS s, CAST(end_ts AS TIMESTAMP) AS e FROM {runs}
                    WHERE object_id IN (SELECT object_id FROM roa_ee) AND end_ts >= {lit(t_min)}
                      AND start_ts <= {lit(t_max)}""")
        held = time.time() - t
        # CA certificates present at the instants: one head row per object id, key and hosts as codes
        con.execute(f"CREATE TEMP TABLE live AS {live_sql('inst', 'ca_runs')}")
        con.execute(f"""CREATE TEMP TABLE ca_live0 AS
            SELECT DISTINCT ON (c.object_id) c.object_id, c.cert_id, c.ski, c.nb, c.na, c.host AS own_host,
                   k.host AS issuer_host
            FROM ca_certs c LEFT JOIN ca_keys k ON k.ski = c.aki
            WHERE c.object_id IN (SELECT object_id FROM live) AND {non_root('c')}
            ORDER BY c.object_id, c.wh""")
        hosts = ["SELECT own_host AS host FROM ca_live0", "SELECT issuer_host FROM ca_live0",
                 "SELECT k.host FROM ca_certs c JOIN ca_keys k ON k.ski = c.aki WHERE c.wh = " + lit(rir)]
        if roas:
            hosts.append("SELECT DISTINCT host FROM roa_ee")
        host_populations(con, rir, ' UNION '.join(hosts))
        con.execute("""CREATE TEMP TABLE ca_live AS
            SELECT c.object_id, c.cert_id, CAST(dense_rank() OVER (ORDER BY c.ski) AS INTEGER) AS kid, c.nb, c.na,
                   o.hid AS ohid, i.hid AS ihid
            FROM ca_live0 c JOIN hdim o ON o.host = coalesce(c.own_host, '')
                 JOIN hdim i ON i.host = coalesce(c.issuer_host, '')""")
        if roas:
            con.execute("""CREATE TEMP TABLE roa_e AS
                SELECT e.object_id, h.hid,
                       CASE WHEN e.aki IS NOT NULL THEN CAST(dense_rank() OVER (ORDER BY e.aki) AS INTEGER) END AS akid
                FROM roa_ee e JOIN hdim h ON h.host = coalesce(e.host, '')
                WHERE e.object_id IN (SELECT object_id FROM roa_runs)""")
        parts = []
        if want_life:
            _lifetime(ctx, con, rir, wf, wt)
            _lifetime_end(con, rir, wf, wt)
            parts += ['life_rows', 'end_rows']
        if want_inv:
            _inventory(con, rir, wf, wt, roas)
            parts.append('inv_rows')
        rows = ' UNION ALL BY NAME '.join(f'SELECT * FROM {p}' for p in parts)
        ctx.emit(con, rows, 'life_hist' if want_life else None)
        log(f'm_ca: {rir} {wf}..{wt}: presence lock held {held:.1f}s')
    finally:
        con.close()
