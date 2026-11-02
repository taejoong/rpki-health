"""Populations as SQL on top of the host map (scs/hostmap.py).

Which attribute classifies what:
- signed-object metrics (ROA re-sign pairs, ROA-EE revocation, cadence): the host of the SIA of the
  predecessor's signer EE certificate (object_signatures.signer_cert_id -> certs/is_ca=false.sia);
- CRLs, manifests, manifest-EE revocation, late re-issue: the CA's own SIA host, i.e. that of the CA
  certificate with ski = crls.aki (manifests: ski = the manifest EE's aki); when no CA certificate of the
  key is known, the host of the object's own EE certificate (manifests) or of the key's manifest EE
  certificates (CRLs carry no EE certificate);
- CA certificate lifetime: the issuer's class, the SIA host of the parent CA certificate
  (parent.ski = child.aki).

The CA side uses one table of CA keys from the CA certificates of all five warehouses (ca_keys_sql;
the run builds it once, metrics.Ctx.load_ca_keys loads it): a CA certificate sits in its parent's
repository, which era 2 can file under another RIR than the CA's own objects (rpki_cycle.sh TAL_MAP),
so a lookup in one warehouse misses some keys (0.1-0.6% of CRLs). A key's host is the
SIA host of its newest certificate (latest notBefore, then the highest cert_id), across all history.

Reported populations per RIR: every object counts in its class (rir_hosted, rir_paas,
nir_hosted, delegated, unknown) and in 'all', and nir_hosted / rir_paas objects also in a row per
operator (population = the class, operator = e.g. 'JPNIC'); operator is '' everywhere else. The
headline population is rir_hosted (for CA lifetime: issuer class rir_hosted, i.e. issued by the RIR's
own CA).

The *_sql functions return SQL text; inputs are table names or parenthesised subqueries.
"""
from . import common, hostmap

CLASSES = hostmap.CLASSES
OPERATOR_CLASSES = hostmap.OPERATOR_CLASSES
POPULATIONS = CLASSES + ('all',)
HEADLINE = 'rir_hosted'


def load_hostmap(con, hm, name='hostmap'):
    """TEMP TABLE <name>(host, rir, class, operator) from a loaded HostMap."""
    return common.values_table(con, name, [('host', 'VARCHAR'), ('rir', 'VARCHAR'), ('class', 'VARCHAR'),
                                           ('operator', 'VARCHAR')], hm.rows())


def ca_keys_sql(srcs):
    """One row per CA key over the CA certificates of several warehouses: ski, host (SIA host of the
    newest certificate of the key), aki (its issuer key), newest_not_before, certs (certificates of the
    key), warehouses (comma list). srcs: [(rir, SQL source of that warehouse's certs/is_ca=true)]."""
    parts = ' UNION ALL '.join(
        f"SELECT {common.lit(rir)} AS rir, cert_id, ski, aki, not_before, scs_host(sia) AS host "
        f"FROM {src} WHERE is_ca_flag AND ski IS NOT NULL" for rir, src in srcs)
    return f"""
SELECT ski, arg_max(host, (not_before, cert_id)) AS host, arg_max(aki, (not_before, cert_id)) AS aki,
       CAST(max(not_before) AS TIMESTAMP) AS newest_not_before, count(*) AS certs,
       string_agg(DISTINCT rir, ',' ORDER BY rir) AS warehouses
FROM ({parts}) GROUP BY ski"""


def classify_sql(src, host_col, rir, hostmap_table='hostmap'):
    """src plus pop_class and pop_operator, from the host in column host_col, for warehouse `rir`."""
    r = common.lit(rir)
    ops = ', '.join(common.lit(c) for c in OPERATOR_CLASSES)
    return f"""
SELECT s_.*,
       CASE WHEN s_.{host_col} IS NULL OR s_.{host_col} = '' THEN 'unknown'
            WHEN hm_.rir = {r} THEN hm_.class ELSE 'delegated' END AS pop_class,
       CASE WHEN hm_.rir = {r} AND hm_.class IN ({ops}) THEN hm_.operator ELSE '' END AS pop_operator
FROM {src} s_ LEFT JOIN {hostmap_table} hm_ ON hm_.host = s_.{host_col}"""


def expand_sql(src, class_col='pop_class', operator_col='pop_operator'):
    """One row per reported population of each row of src (which has class_col and operator_col):
    (class, ''), ('all', '') and, for nir_hosted / rir_paas with an operator, (class, operator).
    Adds the columns population and operator; aggregate with GROUP BY week, population, operator."""
    ops = ', '.join(common.lit(c) for c in OPERATOR_CLASSES)
    return f"""
SELECT s_.*, CASE v_.k WHEN 1 THEN 'all' ELSE s_.{class_col} END AS population,
       CASE v_.k WHEN 2 THEN s_.{operator_col} ELSE '' END AS operator
FROM {src} s_ CROSS JOIN (VALUES (0), (1), (2)) v_(k)
WHERE v_.k < 2 OR (s_.{class_col} IN ({ops}) AND s_.{operator_col} <> '')"""


def populations_sql(src, host_col, rir, hostmap_table='hostmap'):
    """classify_sql + expand_sql: src rows fanned out to their populations (columns pop_class,
    pop_operator, population, operator added)."""
    return expand_sql(f'({classify_sql(src, host_col, rir, hostmap_table)})')
