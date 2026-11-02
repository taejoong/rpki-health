"""Relying-party rejections: rp_rejected, era 2 from the first full week of verdicts
(2026-06-22; table=object_validity starts 2026-06-20 02:40).

Input: table=object_validity, one row per present object that our rpki-client run rejected in a cycle
(dataset_ts = the cycle label; state is always 2; rows can repeat). A cycle without any rejected object
writes no row, and CRL objects never get one (rpki-client 9.7 reports an expired CRL against the
manifest), so verdict coverage comes from the ledger (status rule 'verdicts': masked when the week's
verdict cycles are below 90% of its snapshot cycles, or fewer than 90% of its expected cycles were good),
not from these rows.

Per week (the cycle's ISO week, UTC), population and operator:
  objects                     distinct rejected objects (value = n)
  ca_keys                     distinct CA keys affected (value = n_ca; n = the objects)
  objects_type_<type>         distinct rejected objects of one type: roa, mft, cer, aspa, gbr (crl,
                              unknown when the object is in no head table of any warehouse)
  objects_reason_<reason>     distinct rejected objects with one reason code (rpki-client wording, lower
                              case, '-' as '_': expired, not_yet_valid, resource_violation, revoked, ...)
  objects_<type>_<reason>     both (an object rejected for two reasons in the week counts in both)
n_ca = distinct CA keys behind the value and top1_share = the largest key's share of n. The key of a
signed object (ROA, MFT, ASPA, GBR) is its issuing key, the aki of its EE certificate
(object_signatures.signer_cert_id -> certs/is_ca=false); the key of a rejected CA certificate is the
certificate's own key (ski): the CA whose certificate was rejected, whose subtree the relying party then
drops; a CRL's is crls.aki.
Populations: ROAs, ASPAs and GBRs by the host of their EE certificate's SIA (the publication
point of the object); manifests by the CA's own SIA host (ca_keys.host of the manifest EE's aki, else the
EE certificate's host); CA certificates by their own SIA host (the rejected CA's repository: an expired
certificate of a member-run CA on RIPE NCC PaaS counts under rir_paas, not under the hosted CA that
issued it); CRLs by ca_keys.host of crls.aki.
Head rows: an object id resolves in this warehouse's object_signatures, crls, or the CA certificates of
all five warehouses (scs.m_ca.ca_certs_path, cached for the run); ids left over (era-2 objects whose
head rows another warehouse parsed first: a few per RIR) are looked up in the other warehouses'
object_signatures and crls, and their EE certificates in the warehouse of the signature row.
Rows: population 'all' and the headline 'rir_hosted' have objects and ca_keys in every week (zero when
nothing was rejected); every other row exists only with n > 0, so a missing row is a zero.
"""
import datetime as dt

from . import classify, common, defs, m_ca
from .common import lit

METRIC = 'rp_rejected'
FRAME_POPULATIONS = (('all', ''), ('rir_hosted', ''))
LOOKUP_CHUNK = 500          # ids per literal IN list in the other warehouses (row-group pruning)


def available_from():
    return common.monday(defs.METRICS[METRIC]['available_from'])


def reason_sql(col):
    """Reason code -> stat suffix: lower case, every run of other characters as '_' ('unknown' if empty)."""
    s = f"trim(regexp_replace(lower(coalesce({col}, '')), '[^a-z0-9]+', '_', 'g'), '_')"
    return f"coalesce(nullif({s}, ''), 'unknown')"


def type_sql(col):
    """object_type -> stat suffix without '_' (ROA -> roa)."""
    return f"coalesce(nullif(regexp_replace(lower(coalesce({col}, '')), '[^a-z0-9]+', '', 'g'), ''), 'unknown')"


def _ids_in(ids):
    return ', '.join(str(int(i)) for i in ids)


UNRESOLVED = """SELECT object_id FROM ids WHERE object_id NOT IN (SELECT object_id FROM sig)
    AND object_id NOT IN (SELECT object_id FROM cer) AND object_id NOT IN (SELECT object_id FROM crl)
    ORDER BY object_id"""


def _resolve_elsewhere(ctx, con, rir):
    """Add the signature rows, then the CRLs, of ids that this warehouse does not hold (TEMP TABLES sig,
    crl), warehouse by warehouse until none is left: literal id lists, so that the scans skip the row
    groups that cannot hold them (each table still costs a pass over 14,000 part footers, 1-2 s)."""
    for table, cols, insert in (
            ('object_signatures', ['object_id', 'object_type', 'signer_cert_id'],
             "INSERT INTO sig SELECT object_id, min(object_type), min(signer_cert_id), {wh} FROM {src} "
             "WHERE object_id IN ({ids}) GROUP BY object_id"),
            ('crls', ['crl_id', 'aki'],
             "INSERT INTO crl SELECT crl_id, min(aki) FROM {src} WHERE crl_id IN ({ids}) GROUP BY crl_id")):
        for other in common.REGISTRIES:
            rest = [r[0] for r in con.execute(UNRESOLVED).fetchall()]
            if not rest:
                return
            if other == rir or not ctx.has_table(other, table):
                continue
            src = ctx.src(other, table, cols)
            for i in range(0, len(rest), LOOKUP_CHUNK):
                ctx.run(con, insert.format(wh=lit(other), src=src, ids=_ids_in(rest[i:i + LOOKUP_CHUNK])))


def _ee_certs(ctx, con, rir):
    """TEMP TABLE ee(wh, cert_id, aki, host): the EE certificates of the signature rows, each from the
    warehouse that holds the signature row."""
    con.execute("CREATE TEMP TABLE ee (wh VARCHAR, cert_id INTEGER, aki VARCHAR, host VARCHAR)")
    for (wh,) in con.execute("SELECT DISTINCT wh FROM sig ORDER BY wh").fetchall():
        if not ctx.has_table(wh, 'certs/is_ca=false'):
            continue
        e = ctx.src(wh, 'certs/is_ca=false', ['cert_id', 'aki', 'sia'])
        if wh == rir:
            ctx.run(con, f"""INSERT INTO ee SELECT {lit(wh)}, cert_id, min(aki), min(scs_host(sia)) FROM {e}
                             WHERE cert_id IN (SELECT signer FROM sig WHERE wh = {lit(wh)}) GROUP BY cert_id""")
            continue
        signers = [r[0] for r in con.execute(
            f"SELECT DISTINCT signer FROM sig WHERE wh = {lit(wh)} AND signer IS NOT NULL ORDER BY 1").fetchall()]
        for i in range(0, len(signers), LOOKUP_CHUNK):
            ctx.run(con, f"""INSERT INTO ee SELECT {lit(wh)}, cert_id, min(aki), min(scs_host(sia)) FROM {e}
                             WHERE cert_id IN ({_ids_in(signers[i:i + LOOKUP_CHUNK])}) GROUP BY cert_id""")


def compute(ctx, rir, week_from, week_to):
    if METRIC not in ctx.metrics:
        return
    first, last = max(common.monday(week_from), available_from()), common.monday(week_to)
    if first > last:
        return
    for t in ('object_validity', 'object_signatures', 'crls'):
        if not ctx.has_table(rir, t):        # a missing table must not pass as a week without rejections
            raise FileNotFoundError(f'm_health: warehouse-{rir} has no table={t}')
    end = last + dt.timedelta(days=7)
    certs_path = m_ca.ca_certs_path(ctx)
    con = m_ca.connect(ctx)
    try:
        ctx.load_hostmap(con)
        ctx.load_ca_keys(con)
        # distinct (week, object, reason) of the range; dataset_ts is the cycle label 'YYYY-MM-DD HH:MM:SS'
        ov = ctx.src(rir, 'object_validity', ['dataset_ts', 'object_id', 'state', 'reason_code'])
        ctx.run(con, f"""CREATE TEMP TABLE ov AS
            SELECT DISTINCT scs_week(CAST(dataset_ts AS TIMESTAMP)) AS week, object_id,
                   {reason_sql('reason_code')} AS reason
            FROM {ov}
            WHERE state = 2 AND object_id IS NOT NULL AND dataset_ts >= {lit(first.isoformat())}
              AND dataset_ts < {lit(end.isoformat())}""")
        con.execute("CREATE TEMP TABLE ids AS SELECT DISTINCT object_id FROM ov")
        # what each object is, with its key and host
        s = ctx.src(rir, 'object_signatures', ['object_id', 'object_type', 'signer_cert_id'])
        ctx.run(con, f"""CREATE TEMP TABLE sig AS
            SELECT object_id, min(object_type) AS otype, min(signer_cert_id) AS signer, {lit(rir)} AS wh FROM {s}
            WHERE object_id IN (SELECT object_id FROM ids) GROUP BY object_id""")
        con.execute(f"""CREATE TEMP TABLE cer AS
            SELECT DISTINCT ON (object_id) object_id, ski, host FROM read_parquet({lit(certs_path)})
            WHERE object_id IN (SELECT object_id FROM ids) AND object_id NOT IN (SELECT object_id FROM sig)
            ORDER BY object_id, wh""")
        c = ctx.src(rir, 'crls', ['crl_id', 'aki'])
        ctx.run(con, f"""CREATE TEMP TABLE crl AS
            SELECT crl_id AS object_id, min(aki) AS aki FROM {c}
            WHERE crl_id IN (SELECT object_id FROM ids) GROUP BY crl_id""")
        _resolve_elsewhere(ctx, con, rir)
        _ee_certs(ctx, con, rir)
        con.execute(f"""CREATE TEMP TABLE obj AS
            SELECT i.object_id,
                   CASE WHEN s.object_id IS NOT NULL THEN {type_sql('s.otype')} WHEN c.object_id IS NOT NULL THEN 'cer'
                        WHEN r.object_id IS NOT NULL THEN 'crl' ELSE 'unknown' END AS otype,
                   CASE WHEN s.object_id IS NOT NULL THEN e.aki WHEN c.object_id IS NOT NULL THEN c.ski
                        ELSE r.aki END AS key,
                   CASE WHEN s.object_id IS NOT NULL THEN
                            CASE WHEN {type_sql('s.otype')} = 'mft' THEN coalesce(k.host, e.host) ELSE e.host END
                        WHEN c.object_id IS NOT NULL THEN c.host ELSE kr.host END AS host
            FROM ids i LEFT JOIN sig s USING (object_id) LEFT JOIN ee e ON e.wh = s.wh AND e.cert_id = s.signer
                 LEFT JOIN ca_keys k ON k.ski = e.aki LEFT JOIN cer c ON c.object_id = i.object_id
                 LEFT JOIN crl r ON r.object_id = i.object_id LEFT JOIN ca_keys kr ON kr.ski = r.aki""")
        rj = '(SELECT ov.week, ov.object_id, ov.reason, o.otype, o.key, o.host FROM ov JOIN obj o USING (object_id))'
        con.execute(f"CREATE TEMP TABLE rp AS {classify.populations_sql(rj, 'host', rir)}")
        # one row per (week, population, operator, stat, object): the object's distinct appearances
        con.execute("""CREATE TEMP TABLE cells AS
            SELECT DISTINCT week, population, operator, stat, object_id, key FROM (
                SELECT week, population, operator, 'objects' AS stat, object_id, key FROM rp
                UNION ALL SELECT week, population, operator, 'objects_type_' || otype, object_id, key FROM rp
                UNION ALL SELECT week, population, operator, 'objects_reason_' || reason, object_id, key FROM rp
                UNION ALL
                SELECT week, population, operator, 'objects_' || otype || '_' || reason, object_id, key FROM rp)""")
        con.execute("""CREATE TEMP TABLE ragg AS
            WITH pk AS (SELECT week, population, operator, stat, key, count(*) AS c FROM cells GROUP BY ALL)
            SELECT week, population, operator, stat, sum(c) AS n, count(key) AS n_ca,
                   max(c) FILTER (WHERE key IS NOT NULL) / sum(c) AS top
            FROM pk GROUP BY ALL""")
        fp = ', '.join(f'({lit(p)}, {lit(o)})' for p, o in FRAME_POPULATIONS)
        r = lit(rir)
        con.execute(f"""CREATE TEMP TABLE frame AS
            SELECT CAST(range AS DATE) AS week, p.population, p.operator
            FROM range(CAST({common.date_lit(first)} AS TIMESTAMP), CAST({common.date_lit(last)} AS TIMESTAMP)
                       + INTERVAL 7 DAY, INTERVAL 7 DAY) CROSS JOIN (VALUES {fp}) p(population, operator)""")
        rows = f"""
            WITH o AS (SELECT * FROM ragg WHERE stat = 'objects'),
            base AS (SELECT f.week, f.population, f.operator, coalesce(o.n, 0) AS n, coalesce(o.n_ca, 0) AS n_ca, o.top
                     FROM frame f LEFT JOIN o USING (week, population, operator)
                     UNION ALL
                     SELECT o.week, o.population, o.operator, o.n, o.n_ca, o.top FROM o
                     WHERE NOT EXISTS (SELECT 1 FROM frame f WHERE f.week = o.week AND f.population = o.population
                                                                AND f.operator = o.operator))
            SELECT week, '1w' AS "window", {r} AS rir, population, operator, {lit(METRIC)} AS metric_id,
                   'objects' AS stat, n AS value, n, n_ca, top AS top1_share FROM base
            UNION ALL
            SELECT week, '1w', {r}, population, operator, {lit(METRIC)}, 'ca_keys', n_ca, n, n_ca, top FROM base
            UNION ALL
            SELECT week, '1w', {r}, population, operator, {lit(METRIC)}, stat, n, n, n_ca, top FROM ragg
            WHERE stat <> 'objects'"""
        ctx.emit(con, rows)
    finally:
        con.close()
