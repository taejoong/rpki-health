"""Replacements of manifests and CRLs: mft_ee_revocation and late_reissue, era 2 only,
on directly observed replacements only.

Replacements. The manifests of one CA key (the aki of the manifest's EE certificate) in manifestNumber order
(numerical: the warehouse stores canonical decimal strings of up to 46 digits, so the order is by length,
then text), then thisUpdate and object id; the CRLs of one key (crls.aki) in crlNumber order, then thisUpdate
and crl_id. Two consecutive objects of a key are a replacement (predecessor, successor), counted in the ISO
week W of the successor's thisUpdate. The order is taken per week, among the key's objects issued in W and
those present in some cycle from the day before W to its end (they include every predecessor whose
replacement was observed, however old): a numbering that a CA restarted earlier cannot interleave with the
current one (two NIC.br keys in October 2026 numbered 65, 66, ... twice), a week gets the same pairs whatever
range of weeks is computed with it (a weekly run and a backfill agree), and the set does not change once the
week is over. It is directly observed when
  - the successor was first seen in the cycle right after the predecessor's last cycle
    (object_presence_stats: successor first_sid = predecessor last_sid + 1),
  - both cycles are observations: good rpki-client cycles of the observation ledger (scs/ledger.py,
    cycles.good) in which our collection fetched something (at least one of the five warehouses got a new
    object in that cycle), at most gap_hours apart (3 h, the ledger's gap: no stretch without a good cycle
    in between), and
  - the successor was first seen by as_of.
Only these count. Across a collection outage the predecessor's last cycle and the successor's first cycle
are still consecutive, but days apart, and the intermediate objects were never captured: such replacements
look late (38-78% "late" CRLs in the week of 2026-10-05) and are left out here; the week's
rows are also masked by the ledger (status rule 'gap'). The fetch condition covers what the ledger cannot
see: from 2026-10-09 14:00 to 10-12 12:20 UTC no warehouse got any new object (rpki-client only re-listed its
cache), yet APNIC's and LACNIC's cycles kept their file counts and are good in the ledger. Without it, the
replacements across that stretch read as 236 late APNIC keys and 5 late keys of LACNIC's hosted service in
the (unmasked) week of 2026-10-12; with it, none. In the rest of era 2 only 4 isolated cycles fetched nothing.

mft_ee_revocation (RFC 9286 §4.2.1, §5.1): directly observed manifest replacements whose successor's
thisUpdate is before the predecessor's nextUpdate (early replacements). Revoked: the serial of the
predecessor's EE certificate (object_signatures.signer_cert_id -> certs/is_ca=false.serial_dec) is listed
in crl_serials of a CRL of the same key whose thisUpdate is from 1 day before to 2 days after the
successor's thisUpdate (params evidence_days) and not after as_of. An early replacement without any such
CRL counts as not revoked: §4.2.1 asks for a new CRL with every early manifest. Stats per week, population
and operator: early_replacements (count), revoked (pair-weighted share), revoked_ca (mean over keys of
the per-key share), ca_always and ca_never (shares of keys that revoked all, or none, of their predecessor
EE certificates in the week).

late_reissue (RFC 9286 §4.2.1, §5.2), for manifests and for CRLs: every directly observed replacement:
replacements_mft / _crl (count), late_mft / _crl (share whose successor's thisUpdate is after the
predecessor's nextUpdate), late_mft_ca / _crl_ca (share of keys with at least one late replacement),
headroom_mft_p50_h / _crl_p50_h (quantile_disc median of predecessor nextUpdate - successor thisUpdate,
hours; negative when late).

Every row: n = replacements behind the value, n_ca = their distinct keys, top1_share = the largest key's
share of n. Populations by the CA's own publication host (ca_keys.host of the key, else the host of the
newest manifest EE certificate of the key; classify.py). Population 'all' gets a row for every stat and
week from 2026-04-20 (era 2, available_from) on, also when n = 0.

Inputs per RIR and range of weeks [t0, t1), in one grouped pass:
  1. manifests and CRLs with thisUpdate in [t0 - 8 d, t1), CRLs up to min(t1 + 2 d, as_of) for the
     revocation evidence (the bulk; row groups pruned on thisUpdate);
  2. under the presence lock, object_presence_stats (object_id, first_sid, last_sid) of every object present
     in a cycle from t0 - 1 d on and first seen by as_of, a check that the presence sids and the ledger's
     cycles name the same snapshots, and the objects first seen per cycle in each of the five warehouses
     (first_sid only: which cycles fetched anything);
  3. the manifests and CRLs among those present objects that the bulk does not hold: predecessors issued
     before t0 - 8 d (long-lived manifests and stale ones of CAs that stopped publishing, a few hundred per
     RIR; up to 189 days old in era 2, ARIN). A directly observed predecessor is present in the cycle right
     before its successor, so the bulk plus these hold every predecessor whatever its age; one unpruned scan
     of manifests and of crls;
  4. object_signatures -> certs/is_ca=false of the manifests (key, EE serial, EE host), the bulk part
     pruned on object_id / cert_id, the older ones in a second scan;
  5. crl_serials of the evidence CRLs of keys with early replacements, restricted to the predecessors' serials
     first (LACNIC's CRLs list about 700 serials each, 48M rows a week).
"""
import datetime as dt

from . import classify, common, defs
from .common import date_lit, lit, ts_lit

METRIC_IDS = ('mft_ee_revocation', 'late_reissue')
BULK_DAYS = 8                       # thisUpdate look-back of the bulk read; older predecessors come from step 3
MARGIN = dt.timedelta(days=1)       # presence window: objects present in a cycle from t0 - 1 day on
REVOCATION_STATS = ('early_replacements', 'revoked', 'revoked_ca', 'ca_always', 'ca_never')
LATE_STATS = {'replacements': 'replacements_{k}', 'late': 'late_{k}', 'late_ca': 'late_{k}_ca',
              'headroom_p50_h': 'headroom_{k}_p50_h'}      # late_reissue stat names for k = mft, crl


def _at(d):
    """Naive UTC datetime of a date at 00:00."""
    return dt.datetime.combine(common.to_date(d), dt.time())


def _ts(t):
    """TIMESTAMP literal (naive UTC) for the ledger's cycles.ts."""
    return f"TIMESTAMP '{t.strftime('%Y-%m-%d %H:%M:%S')}'"


def compute(ctx, rir, week_from, week_to):
    todo = [m for m in METRIC_IDS if m in ctx.metrics]
    if not todo:
        return
    first = min(common.monday(defs.METRICS[m]['available_from']) for m in todo)
    wf, wt = max(common.monday(week_from), first), common.monday(week_to)
    if wf > wt:
        return                      # the range lies before era 2: these metrics have no rows there
    p_rev, p_late = ctx.params('mft_ee_revocation'), ctx.params('late_reissue')
    w0, w1 = _at(wf), _at(wt) + dt.timedelta(days=7)
    ev_lo, ev_hi = p_rev['evidence_days']
    ev_end = min(w1 + dt.timedelta(days=ev_hi), ctx.as_of)
    con = ctx.connect()
    try:
        ctx.load_hostmap(con)
        ctx.load_ca_keys(con)
        ctx.load_cycles(con)
        con.execute(f"""CREATE TEMP TABLE cya AS SELECT sid, ts, src, good FROM cycles
            WHERE rir = {lit(rir)} AND sid IS NOT NULL""")
        con.execute("CREATE TEMP TABLE cy AS SELECT sid, ts, good FROM cya WHERE src = 'rpki-client'")
        sid_lo, sid_cut = con.execute(f"""SELECT min(sid) FILTER (WHERE ts >= {_ts(w0 - MARGIN)}),
            max(sid) FILTER (WHERE ts <= {_ts(ctx.as_of)}) FROM cy""").fetchone()
        if sid_lo is None or sid_cut is None or sid_lo > sid_cut:
            common.log(f'm_mft: {rir}: no rpki-client cycle in {w0 - MARGIN}..{ctx.as_of}; only empty rows')
            sid_lo, sid_cut = 0, -1
        _read(ctx, con, rir, w0, w1, ev_end, sid_lo, sid_cut)
        _pairs(con, wf, wt, p_rev['gap_hours'], p_late['gap_hours'])
        _revocation(ctx, con, rir, ev_lo, ev_hi, ev_end)
        _key_hosts(con)
        rows = []
        if 'mft_ee_revocation' in todo:
            rows.append(_revocation_rows(con, rir, wf, wt))
        if 'late_reissue' in todo:
            rows.append(_late_rows(con, rir, wf, wt))
        ctx.emit(con, ' UNION ALL '.join(f'({r})' for r in rows))
    finally:
        con.close()


def _read(ctx, con, rir, w0, w1, ev_end, sid_lo, sid_cut):
    """TEMP TABLES pr (presence), mo (manifests with key, EE serial and EE host) and co (CRLs) of every
    object that can be one side of a directly observed replacement whose successor is in [w0, w1), and the
    column cy.fetched."""
    b0 = w0 - dt.timedelta(days=BULK_DAYS)
    mcols = ['object_id', 'manifest_number', 'this_update', 'next_update']
    ccols = ['crl_id', 'aki', 'crl_number', 'this_update', 'next_update']
    msel = ("object_id, manifest_number AS num, TRY_CAST(this_update AS TIMESTAMPTZ) AS tu, "
            "TRY_CAST(next_update AS TIMESTAMPTZ) AS nu")
    csel = "crl_id AS object_id, aki AS key, crl_number AS num, this_update AS tu, next_update AS nu"
    # 1. the bulk: thisUpdate is an ISO-8601 UTC string (YYYY-MM-DDTHH:MM:SS+00:00), so text bounds prune
    ctx.run(con, f"""CREATE TEMP TABLE mb AS SELECT DISTINCT {msel} FROM {ctx.src(rir, 'manifests', mcols)}
        WHERE this_update >= {lit(b0.strftime('%Y-%m-%d'))} AND this_update < {lit(w1.strftime('%Y-%m-%d'))}""")
    ctx.run(con, f"""CREATE TEMP TABLE cb AS SELECT DISTINCT {csel} FROM {ctx.src(rir, 'crls', ccols)}
        WHERE this_update >= {ts_lit(b0)} AND this_update <= {ts_lit(ev_end)}""")
    # 2. presence of everything present from t0 - 1 day on (and first seen by as_of), under the lock
    with ctx.presence_lock():
        ps = ctx.src(rir, 'object_presence_stats', ['object_id', 'first_sid', 'last_sid', 'first_ts', 'last_ts'])
        ctx.run(con, f"""CREATE TEMP TABLE pr AS SELECT object_id, first_sid, last_sid FROM {ps}
            WHERE last_sid >= {sid_lo} AND first_sid <= {sid_cut}""")
        probe = con.execute("""SELECT list(object_id ORDER BY object_id) FROM (
            (SELECT object_id FROM pr ORDER BY first_sid DESC, object_id LIMIT 1)
            UNION ALL (SELECT object_id FROM pr ORDER BY last_sid, object_id LIMIT 1))""").fetchone()[0] or []
        seen = ctx.run(con, f"""SELECT first_sid, first_ts, last_sid, last_ts FROM {ps}
            WHERE object_id IN ({', '.join(str(int(i)) for i in probe) or 'NULL'})""").fetchall()
        # cycles that fetched anything: a new object in at least one warehouse. A cycle that only re-lists the
        # rpki-client cache keeps every file, so the ledger's file counts call it good.
        con.execute("CREATE TEMP TABLE new_n (rir VARCHAR, sid INTEGER, n BIGINT)")
        for r in common.REGISTRIES:
            lo_r, hi_r = con.execute(f"""SELECT min(sid), max(sid) FROM cycles WHERE rir = {lit(r)} AND sid IS NOT NULL
                AND ts BETWEEN (SELECT ts FROM cy WHERE sid = {sid_lo}) AND (SELECT ts FROM cy WHERE sid = {sid_cut})""").fetchone()
            if lo_r is not None and ctx.has_table(r, 'object_presence_stats'):
                ctx.run(con, f"""INSERT INTO new_n SELECT {lit(r)}, first_sid, count(*)
                    FROM {ctx.src(r, 'object_presence_stats', ['first_sid'])} WHERE first_sid BETWEEN {lo_r} AND {hi_r}
                    GROUP BY ALL""")
    _check_sids(con, rir, seen)
    con.execute("ALTER TABLE cy ADD COLUMN fetched BOOLEAN DEFAULT false")
    con.execute("""UPDATE cy SET fetched = true WHERE ts IN (
        SELECT c.ts FROM cycles c JOIN new_n n ON n.rir = c.rir AND n.sid = c.sid WHERE n.n > 0)""")
    # 3. present manifests and CRLs that the bulk does not hold (old predecessors, and old objects that take
    #    part in a key's order): unpruned scans
    con.execute("""CREATE TEMP TABLE xo AS SELECT p.object_id FROM pr p
        ANTI JOIN mb ON mb.object_id = p.object_id ANTI JOIN cb ON cb.object_id = p.object_id""")
    ctx.run(con, f"""CREATE TEMP TABLE mx AS SELECT DISTINCT {msel} FROM {ctx.src(rir, 'manifests', mcols)}
        WHERE object_id IN (SELECT object_id FROM xo)""")
    ctx.run(con, f"""CREATE TEMP TABLE cx AS SELECT DISTINCT {csel} FROM {ctx.src(rir, 'crls', ccols)}
        WHERE crl_id IN (SELECT object_id FROM xo)""")
    con.execute("CREATE TEMP TABLE m AS SELECT * FROM mb UNION SELECT * FROM mx")
    con.execute("CREATE TEMP TABLE co AS SELECT * FROM cb UNION SELECT * FROM cx")
    # 4. the manifests' EE certificates: the bulk part pruned on ids; the few older ones from a small id
    #    table (the scan then reads only the row groups that can hold them)
    lo = con.execute("SELECT coalesce(min(object_id), 0) FROM mb").fetchone()[0]
    sigs = ctx.src(rir, 'object_signatures', ['object_id', 'signer_cert_id'])
    ctx.run(con, f"""CREATE TEMP TABLE sg AS SELECT DISTINCT object_id, signer_cert_id FROM {sigs}
        WHERE object_id >= {lo} AND object_id IN (SELECT object_id FROM m WHERE object_id >= {lo})""")
    con.execute(f"CREATE TEMP TABLE old_m AS SELECT object_id FROM m WHERE object_id < {lo}")
    if con.execute("SELECT count(*) FROM old_m").fetchone()[0]:
        ctx.run(con, f"""INSERT INTO sg SELECT DISTINCT object_id, signer_cert_id FROM {sigs}
            WHERE object_id IN (SELECT object_id FROM old_m)""")
    clo = con.execute(f"SELECT coalesce(min(signer_cert_id), 0) FROM sg WHERE object_id >= {lo}").fetchone()[0]
    ees = ctx.src(rir, 'certs/is_ca=false', ['cert_id', 'aki', 'serial_dec', 'sia'])
    esel = "cert_id, aki, serial_dec, scs_host(sia) AS host"
    ctx.run(con, f"""CREATE TEMP TABLE ee AS SELECT DISTINCT {esel} FROM {ees}
        WHERE cert_id >= {clo} AND cert_id IN (SELECT signer_cert_id FROM sg WHERE signer_cert_id >= {clo})""")
    con.execute(f"CREATE TEMP TABLE old_c AS SELECT DISTINCT signer_cert_id FROM sg WHERE signer_cert_id < {clo}")
    if con.execute("SELECT count(*) FROM old_c").fetchone()[0]:
        ctx.run(con, f"""INSERT INTO ee SELECT DISTINCT {esel} FROM {ees}
            WHERE cert_id IN (SELECT signer_cert_id FROM old_c)""")
    con.execute("""CREATE TEMP TABLE mo AS SELECT DISTINCT m.object_id, e.aki AS key, m.num, m.tu, m.nu,
            e.serial_dec AS ser, e.host AS ee_host
        FROM m JOIN sg USING (object_id) JOIN ee e ON e.cert_id = sg.signer_cert_id WHERE e.aki IS NOT NULL""")
    for t in ('mb', 'cb', 'xo', 'mx', 'cx', 'm', 'sg', 'old_m', 'ee', 'old_c', 'new_n'):
        con.execute(f'DROP TABLE {t}')          # long ranges: free them before the pairs and the evidence


def _check_sids(con, rir, seen):
    """The presence build and the ledger number snapshots the same way (position in the sorted membership
    listing); a disagreement would pair cycles wrongly, so it stops the module."""
    for first_sid, first_ts, last_sid, last_ts in seen:
        for sid, ts in ((first_sid, first_ts), (last_sid, last_ts)):
            got = con.execute(f"SELECT strftime(ts, '%Y-%m-%d %H:%M:%S') FROM cycles WHERE rir = {lit(rir)} "
                              f"AND sid = {int(sid)}").fetchone()
            if not got or got[0] != str(ts)[:19]:
                raise RuntimeError(f'm_mft: {rir}: presence sid {sid} is {ts} but the ledger has '
                                   f'{got[0] if got else "no snapshot"}: rebuild the ledger (python -m scs.ledger)')


def _pairs(con, wf, wt, gap_rev, gap_late):
    """TEMP TABLES pairs_mft and pairs_crl: for every week W of the range, the replacements whose successor's
    thisUpdate is in W, with the predecessor's nextUpdate (and EE serial) and whether they were directly
    observed under each metric's gap_hours (direct_rev, direct_late). A key's objects are put in order per
    week, among those issued in W and those present in some cycle from the day before W to its end (which
    holds every predecessor whose replacement was observed, however old): a numbering that a CA restarted
    earlier cannot interleave with the current one, a week gets the same pairs whatever range of weeks is
    computed with it, and the set no longer changes once the week is over."""
    for kind, objs, ser in (('mft', 'mo', 'ser'), ('crl', 'co', 'CAST(NULL AS VARCHAR)')):
        con.execute(f"""CREATE TEMP TABLE w_{kind} AS
            SELECT DISTINCT week, key, object_id, num, tu FROM (
              SELECT scs_week(tu) AS week, key, object_id, num, tu FROM {objs}
              UNION ALL
              SELECT CAST(unnest(generate_series(CAST(greatest(scs_week(cf.ts), {date_lit(wf)}) AS TIMESTAMP),
                                                 CAST(least(scs_week(cl.ts + INTERVAL 1 DAY), {date_lit(wt)}) AS TIMESTAMP),
                                                 INTERVAL 7 DAY)) AS DATE) AS week, o.key, o.object_id, o.num, o.tu
              FROM {objs} o JOIN pr p USING (object_id) JOIN cya cf ON cf.sid = p.first_sid
                   JOIN cya cl ON cl.sid = p.last_sid)
            WHERE week BETWEEN {date_lit(wf)} AND {date_lit(wt)}""")
        con.execute(f"""CREATE TEMP TABLE pairs_{kind} AS
            WITH s AS (
              SELECT week, key, object_id, tu, lag(object_id) OVER w AS p_id
              FROM w_{kind} WINDOW w AS (PARTITION BY week, key ORDER BY length(num), num, tu, object_id)),
            j AS (
              SELECT s.key, s.object_id AS s_id, s.p_id, s.tu AS s_tu, v.nu AS p_nu, v.ser AS p_ser, s.week,
                     coalesce(sp.first_sid = pp.last_sid + 1 AND cs.good AND cp.good AND cs.fetched AND cp.fetched,
                              false) AS adjacent,
                     epoch(cs.ts - cp.ts) / 3600.0 AS step_h
              FROM s JOIN (SELECT object_id, min(nu) AS nu, min({ser}) AS ser FROM {objs} GROUP BY object_id) v
                          ON v.object_id = s.p_id
                     LEFT JOIN pr sp ON sp.object_id = s.object_id LEFT JOIN pr pp ON pp.object_id = s.p_id
                     LEFT JOIN cy cs ON cs.sid = sp.first_sid LEFT JOIN cy cp ON cp.sid = pp.last_sid
              WHERE s.tu >= s.week AND s.tu < s.week + INTERVAL 7 DAY AND v.nu IS NOT NULL)
            SELECT *, adjacent AND step_h <= {float(gap_rev)} AS direct_rev,
                   adjacent AND step_h <= {float(gap_late)} AS direct_late
            FROM j""")
        con.execute(f'DROP TABLE w_{kind}')


def _revocation(ctx, con, rir, ev_lo, ev_hi, ev_end):
    """TEMP TABLES early (directly observed early manifest replacements) and rv (their predecessors whose EE
    serial is on a CRL of the key in [succ + ev_lo days, succ + ev_hi days], up to as_of)."""
    con.execute("CREATE TEMP TABLE early AS SELECT * FROM pairs_mft WHERE direct_rev AND s_tu < p_nu")
    con.execute(f"""CREATE TEMP TABLE ev AS SELECT DISTINCT object_id AS crl_id, key, tu FROM co
        WHERE key IN (SELECT DISTINCT key FROM early)
          AND tu >= (SELECT min(s_tu) FROM early) + to_days({int(ev_lo)}) AND tu <= {ts_lit(ev_end)}""")
    lo, hi = con.execute("SELECT min(crl_id), max(crl_id) FROM ev").fetchone()
    if lo is None:
        con.execute("CREATE TEMP TABLE rv (p_id INTEGER)")
        return
    # the predecessors' serials first: a CRL lists every revoked serial that has not expired (LACNIC about 700
    # per CRL, 48M rows a week), and joining those rows to ev before filtering them held 7.6 GB
    ctx.run(con, f"""CREATE TEMP TABLE hit AS SELECT crl_id, serial_dec
        FROM {ctx.src(rir, 'crl_serials', ['crl_id', 'serial_dec'])}
        WHERE crl_id >= {int(lo)} AND crl_id <= {int(hi)} AND serial_dec IN (SELECT p_ser FROM early)
          AND crl_id IN (SELECT crl_id FROM ev)""")
    con.execute(f"""CREATE TEMP TABLE rv AS
        SELECT DISTINCT e.p_id
        FROM hit h JOIN ev ON ev.crl_id = h.crl_id JOIN early e ON e.key = ev.key AND e.p_ser = h.serial_dec
        WHERE ev.tu >= e.s_tu + to_days({int(ev_lo)}) AND ev.tu <= e.s_tu + to_days({int(ev_hi)})""")


def _key_hosts(con):
    """TEMP TABLE kh(key, host): the CA's own publication host (ca_keys), else that of the key's newest
    manifest EE certificate (a key without any CA certificate in the five warehouses)."""
    con.execute("""CREATE TEMP TABLE kh AS
        SELECT k.key, coalesce(c.host, m.host) AS host
        FROM (SELECT key FROM pairs_mft UNION SELECT key FROM pairs_crl) k
             LEFT JOIN ca_keys c ON c.ski = k.key
             LEFT JOIN (SELECT key, arg_max(ee_host, (tu, object_id)) AS host FROM mo GROUP BY key) m USING (key)""")


def _frame(wf, wt):
    """Every week of the range for population 'all' (rows even when n = 0)."""
    return f"""SELECT CAST(range AS DATE) AS week, 'all' AS population, '' AS operator
        FROM range(CAST({date_lit(wf)} AS TIMESTAMP), CAST({date_lit(wt)} AS TIMESTAMP) + INTERVAL 7 DAY, INTERVAL 7 DAY)"""


def _with_frame(agg, cols, wf, wt):
    """agg's rows plus a zero row (n = 0, n_ca = 0, values NULL) for 'all' in weeks without any."""
    keep = ', '.join(f'a.{c}' for c in cols)
    return f"""SELECT f.week, f.population, f.operator, coalesce(a.n, 0) AS n, coalesce(a.n_ca, 0) AS n_ca,
               a.top1_share, {keep}
        FROM ({_frame(wf, wt)}) f LEFT JOIN ({agg}) a USING (week, population, operator)
        UNION ALL
        SELECT week, population, operator, n, n_ca, top1_share, {', '.join(cols)} FROM ({agg}) WHERE population <> 'all'"""


def _revocation_rows(con, rir, wf, wt):
    """Rows of mft_ee_revocation (one per week, population, operator and stat)."""
    con.execute(f"""CREATE TEMP TABLE agg_rev AS
        WITH x AS (SELECT e.week, e.key, rv.p_id IS NOT NULL AS revoked, kh.host
                   FROM early e LEFT JOIN rv USING (p_id) JOIN kh USING (key)),
        k AS (SELECT week, population, operator, key, count(*) AS n_key, count(*) FILTER (WHERE revoked) AS r_key
              FROM ({classify.populations_sql('x', 'host', rir)}) GROUP BY ALL)
        SELECT week, population, operator, sum(n_key) AS n, count(*) AS n_ca, max(n_key) / sum(n_key) AS top1_share,
               sum(r_key) / sum(n_key) AS revoked, avg(r_key / n_key) AS revoked_ca,
               avg(CASE WHEN r_key = n_key THEN 1.0 ELSE 0.0 END) AS ca_always,
               avg(CASE WHEN r_key = 0 THEN 1.0 ELSE 0.0 END) AS ca_never
        FROM k GROUP BY ALL""")
    cols = ['revoked', 'revoked_ca', 'ca_always', 'ca_never']
    values = ' '.join(f"WHEN {lit(c)} THEN {c}" for c in cols)
    stats = ', '.join(f'({lit(s)})' for s in REVOCATION_STATS)
    return f"""
        SELECT week, '1w' AS "window", {lit(rir)} AS rir, population, operator, 'mft_ee_revocation' AS metric_id,
               s.stat, CASE s.stat WHEN 'early_replacements' THEN n {values} END AS value, n, n_ca, top1_share
        FROM ({_with_frame('SELECT * FROM agg_rev', cols, wf, wt)}) CROSS JOIN (VALUES {stats}) s(stat)"""


def _late_rows(con, rir, wf, wt):
    """Rows of late_reissue: the manifest and the CRL statistics side by side."""
    out = []
    for kind in ('mft', 'crl'):
        con.execute(f"""CREATE TEMP TABLE agg_{kind} AS
            WITH x AS (SELECT p.week, p.key, p.s_tu > p.p_nu AS late, epoch(p.p_nu - p.s_tu) / 3600.0 AS headroom_h,
                              kh.host
                       FROM pairs_{kind} p JOIN kh USING (key) WHERE p.direct_late),
            f AS ({classify.populations_sql('x', 'host', rir)}),
            k AS (SELECT week, population, operator, key, count(*) AS n_key, count(*) FILTER (WHERE late) AS l_key
                  FROM f GROUP BY ALL),
            h AS (SELECT week, population, operator, quantile_disc(headroom_h, 0.5) AS headroom_p50_h FROM f GROUP BY ALL)
            SELECT week, population, operator, sum(n_key) AS n, count(*) AS n_ca, max(n_key) / sum(n_key) AS top1_share,
                   sum(l_key) / sum(n_key) AS late, avg(CASE WHEN l_key > 0 THEN 1.0 ELSE 0.0 END) AS late_ca,
                   any_value(h.headroom_p50_h) AS headroom_p50_h
            FROM k JOIN h USING (week, population, operator) GROUP BY ALL""")
        cols = ['late', 'late_ca', 'headroom_p50_h']
        values = ' '.join(f"WHEN {lit(LATE_STATS[c].format(k=kind))} THEN {c}" for c in cols)
        stats = ', '.join(f'({lit(v.format(k=kind))})' for v in LATE_STATS.values())
        out.append(f"""
            SELECT week, '1w' AS "window", {lit(rir)} AS rir, population, operator, 'late_reissue' AS metric_id,
                   s.stat, CASE s.stat WHEN {lit(LATE_STATS['replacements'].format(k=kind))} THEN n {values} END AS value,
                   n, n_ca, top1_share
            FROM ({_with_frame(f'SELECT * FROM agg_{kind}', cols, wf, wt)}) CROSS JOIN (VALUES {stats}) s(stat)""")
    return ' UNION ALL '.join(f'({q})' for q in out)
