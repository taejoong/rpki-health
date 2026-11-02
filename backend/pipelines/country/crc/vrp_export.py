"""Warehouse VRP day-intervals (bridged) -> inputs/warehouse_intervals.parquet  (src 'warehouse').

Method (per-VRP day intervals with two-tier gap bridging, plus per-publication-point bridging):
1. A ROA object counts on UTC day d when one of its object_presence_runs overlaps d while its EE
   certificate is valid. Its publication point ("host") is the host name of its EE certificate's SIA:
   table=object_signatures (object_id -> signer_cert_id) -> table=certs/is_ca=false (cert_id -> sia,
   e.g. 'rsync://rpkica.twnic.tw/...roa'). (roa_objects.repo_id names the data source, not the
   publication point.) Runs are merged per object, then per (host, VRP); VRP = (prefix, max_length,
   asn). Each merged row also keeps the validity of the objects at its two ends (for the guard in 3):
   valid_from = earliest EE not_before of the objects present on its first day, valid_until = latest
   least(EE not_after, CRL revocation) of those present on its last day. Revocations come from
   table=crl_serials, matched to the EE certificate by (issuer DN, serial) (crl_serials.cert_issuer_dn,
   serial_dec = certs.issuer_dn, serial_dec); revoked_cert_id / table=ee_revocations resolve only a
   small part of them (e.g. ARIN 122,921 of 2,576,553 revoked ROA EE certificates).
2. Calendars from the raw (unbridged) intervals (crc.intervals):
   - per RIR (calendar_sql, unchanged): bad = no snapshot that day, or n < 0.9 x the centred 61-day
     median of the RIR's VRP count;
   - per (RIR, host) (host_calendar_sql): n = the host's VRPs that day, ref = least(max n over the 31
     days before, max n over the 31 days after), dip = n < 0.5 x ref; bad = no snapshot that day, or
     (dip and ref >= 20 and the publication point is missing: on that day the host dips in some RIR
     where its ref >= 150). Only a dip far below the level seen both before and after is bad, so
     ramps and permanent drops (repository migrations, real mass withdrawals) are never flagged. The
     150-VRP gate keeps the archive's dropouts of national registries (TWNIC, JPNIC, IDNIC, CNNIC,
     CERNET: ref >= 167 on every gap day), which the RIR calendar cannot see, and leaves out the
     collapses of small delegated CAs (ref <= 118 in every case checked), which independent validators
     (RPKIviews, NRO) showed to be real repository outages in 38 of 44 checked episodes. A part of 20-149
     VRPs of a host in one RIR is bad when the host's part in another RIR (>= 150) dips the same day: one
     repository serves every trust anchor (rpki.admin.freerangecloud.com 2023-05-27..06-02: ARIN part
     131, RIPE NCC part 208, both 0; RPKIviews had all of them).
   - per (RIR, domain) for domains with several hosts (same rule as per host, on the domain's VRP
     count). domain = the host's registrable domain (registrable_domain(): the last two labels, three
     under a ccTLD second level such as nic.ad.jp). A registry that moves its repository to a new
     host of its domain shows up here: IDNIC moved rpki-ca.idnic.net -> repo-rpki.idnic.net with 17
     days (2020-10-07..23) on which the archive had neither, while each host's own calendar only
     sees a permanent drop and a ramp, which are never bad.
3. Bridging (intervals.bridge_sql), three levels, each only adding days:
   a. per (RIR, host, VRP): close gaps of <= 3 days, and gaps of 4-31 days when every gap day is bad
      for the RIR, or when every gap day is bad for that host and the gap has no validity hole;
   b. merge the hosts of a domain per (RIR, domain, VRP) (gaps-and-islands, deterministic order
      first_day, last_day, host) and close gaps of 4-31 days between rows of DIFFERENT hosts when
      every gap day is bad for the RIR, or every gap day is bad for the domain and there is no
      validity hole;
   c. merge the domains per (RIR, VRP) and apply the RIR rule alone (<= 3 days, or 4-31 days all bad
      for the RIR) once more, so a VRP that moves between hosts keeps at least the continuity it had
      before (e.g. a 1-day gap at a migration).
   Validity hole (guard): a whole UTC day of the gap on which no object carrying the VRP was valid,
   while the archive saw the host (domain) that day (n > 0): every object present on the day before
   the gap had expired or been revoked before that day, and no object present on the day after the gap
   had started its EE validity by its end. That is a ROA lapse, not a collection gap (CNNIC 2021-04-30..
   05-26 and 2022-05-28..06-06: EE certificates expired on 2021-04-29 / 2022-05-27 while the archive kept
   fetching rpki.cnnic.cn, replacements signed weeks / days later). Days on which the host was not seen
   do not count: during a total dropout a CA can re-issue a ROA (old EE revoked) and replace it again
   before the archive returns, and that hole is only in the archive (of 205 such gaps, RPKIviews or the
   RIPE NCC archive had the VRP in 202: freerangecloud 2023-05, CNNIC 2022-01, r.magellan.ipxo.com 2026;
   2 TWNIC VRPs of 2022-01-08..22 were really absent and stay bridged). The RIR rule is not guarded,
   so the RIR-only bridging of the method before per-host bridging stays a subset of the output; its
   closures with a validity hole (rir_tier2_validity_holes) lie in collection outages (no snapshots,
   or the RIR's count collapsed), where the replacement objects were never seen.
   bridged_days = days of the row on which no host had the VRP. Tier 2 only closes a gap of a VRP
   that is present on both sides of it, so a VRP that really left a repository is never carried
   forward. Before writing, the step checks that every (VRP, day) of the RIR-only bridging (the
   method before per-host bridging) is also in the output, and the interval invariants; a violation
   stops it.
A warehouse without object_signatures or certs/is_ca=false (or without their columns) uses host '?'
for all its objects, which keeps RIR-only bridging there; this is logged as a WARNING and listed as
host_fallback_rirs in the JSON. A warehouse without crl_serials (or the certificate issuer/serial
columns) gets validity from the EE certificates alone (WARNING, validity_fallback_rirs).

The presence tables are rewritten in place by the 01:00 UTC cron build, so this step
- never reads the warehouse between 00:55 and 01:20 UTC: it refuses to start (or to begin
  reading after waiting for the lock) from 00:45 on, so that a ~3-minute run ends before 00:55,
  and aborts before reading the next warehouse if it is ever inside the window,
- holds a shared flock on /tmp/rpki_presence.lock (waits up to 40 min for it),
- runs at nice 10 with DuckDB threads <= 16, memory_limit <= 16GB, spilling to ROOT/tmp.
A RIR whose latest presence run ends more than 12 h before the newest one (its presence build
failed) is logged as a WARNING and listed in the JSON as stale_rirs.

Presence runs end at the last snapshot before the build (e.g. 00:40 of the build day), so the
build day itself is incomplete: rows are clipped to last_complete_day = (date of the latest run
end) - 1, and a row whose last_day = last_complete_day means "still present".

Outputs (atomic): inputs/warehouse_intervals.parquet
  afi UTINYINT, prefix VARCHAR, max_length UTINYINT, asn UBIGINT, first_day DATE, last_day DATE,
  src VARCHAR ('warehouse'), bridged_days INTEGER, rir VARCHAR (warehouse the row came from)
inputs/warehouse_calendar.parquet (rir, d, n, med, has_snap, bad, cum_good),
inputs/warehouse_host_calendar.parquet (rir, host, d, n, ref, has_snap, bad),
inputs/warehouse_domain_calendar.parquet (rir, domain, d, n, ref, has_snap, bad; domains with >= 2 hosts) and
inputs/warehouse_intervals.json (counts, per-host and per-domain tier-2 counts incl. gaps refused by the
validity guard, validity holes of the RIR rule, last_complete_day, timings).

CLI: python -m crc.vrp_export [--root R]
"""
import argparse
import contextlib
import datetime as dt
import fcntl
import os
import sys
import time

from crc import common, intervals

RIRS = common.REGISTRIES
WAREHOUSE_BASE = '/ssd-data'
LOCK_PATH = '/tmp/rpki_presence.lock'
LOCK_WAIT_S = 40 * 60
BLACKOUT = ((0, 55), (1, 20))      # UTC [00:55, 01:20): the presence build rewrites its tables
START_CUTOFF = (0, 45)             # no new read from 00:45: a run must be over before 00:55
STALE_RIR_HOURS = 12
MAX_THREADS = 16
MAX_MEMORY_GB = 16              # DuckDB fills it (18 GB peak RSS at 16 GB, 33.5 GB at 32 GB: 284 s vs 208 s)
CAL_FROM = dt.date(2012, 1, 1)
UNKNOWN_HOST = intervals.UNKNOWN_HOST
# tables (directory under tables/, columns) that map a ROA object to its publication point
HOST_TABLES = (('table=object_signatures', ('object_id', 'signer_cert_id')),
               ('table=certs/is_ca=false', ('cert_id', 'sia')))
# tables that give the CRL revocation time of a ROA's EE certificate (validity guard)
REVOCATION_TABLES = (('table=object_signatures', ('object_id', 'signer_cert_id')),
                     ('table=certs/is_ca=false', ('cert_id', 'issuer_dn', 'serial_dec')),
                     ('table=crl_serials', ('cert_issuer_dn', 'serial_dec', 'revocation_date')))
HOST_RE = r'^[a-z]+://([^/:]+)'
# second-level labels under which a ccTLD registers names (nic.ad.jp, infininet.com.au, cernet.edu.cn)
CC_SECOND_LEVEL = frozenset(('ac', 'ad', 'co', 'com', 'ed', 'edu', 'go', 'gov', 'gr', 'lg', 'mil', 'ne', 'net',
                             'or', 'org'))


def registrable_domain(host):
    """'rpki-ca.idnic.net' -> 'idnic.net', 'rpki-repository.nic.ad.jp' -> 'nic.ad.jp'.

    The last two DNS labels, or three when the TLD has two letters and the second-level label is a
    generic one (CC_SECOND_LEVEL). IP literals and single labels (and UNKNOWN_HOST) are returned as they
    are. An approximation of the public-suffix rule, used only to group a registry's hosts."""
    if host == UNKNOWN_HOST or not host or host.replace('.', '').isdigit() or '.' not in host:
        return host
    labels = host.split('.')
    if len(labels) >= 3 and len(labels[-1]) == 2 and labels[-2] in CC_SECOND_LEVEL:
        return '.'.join(labels[-3:])
    return '.'.join(labels[-2:])


def _minute(now):
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.hour * 60 + now.minute


def in_blackout(now=None):
    """True in [00:55, 01:20) UTC, when the warehouse must not be read at all."""
    (h0, m0), (h1, m1) = BLACKOUT
    return h0 * 60 + m0 <= _minute(now) < h1 * 60 + m1


def may_start(now=None):
    """False in [00:45, 01:20) UTC: a read started then could still be running at 00:55."""
    (h0, m0), (h1, m1) = START_CUTOFF, BLACKOUT[1]
    return not (h0 * 60 + m0 <= _minute(now) < h1 * 60 + m1)


def guard_window(what):
    if in_blackout():
        raise SystemExit(f'vrp_export: inside the 00:55-01:20 UTC presence build window; not reading {what}')


def renice(target=10):
    """Raise our niceness to `target` (never lowers it)."""
    try:
        cur = os.nice(0)
        if cur < target:
            os.nice(target - cur)
    except OSError:
        pass


@contextlib.contextmanager
def shared_lock(path=LOCK_PATH, wait_s=LOCK_WAIT_S, poll_s=5.0):
    """flock(LOCK_SH) on `path`, polling up to wait_s seconds (same lock as `flock -s` in cron)."""
    fd = os.open(path, os.O_RDONLY | os.O_CREAT, 0o664)
    try:
        deadline = time.monotonic() + wait_s
        waited = False
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if not waited:
                    common.log(f'vrp_export: waiting for shared lock on {path} (up to {wait_s}s)')
                    waited = True
                if time.monotonic() >= deadline:
                    raise SystemExit(f'vrp_export: no shared lock on {path} after {wait_s}s')
                time.sleep(poll_s)
        yield
    finally:
        os.close(fd)


def snapshot_days(base, rirs=RIRS):
    """[(rir, 'YYYY-MM-DD')] for every snapshots/ts=* directory of every warehouse."""
    out = []
    for rir in rirs:
        d = os.path.join(base, f'warehouse-{rir}', 'snapshots')
        for n in os.listdir(d):
            if n.startswith('ts='):
                out.append((rir, n[3:13]))
    return out


def host_tables_problem(con, tables, required=HOST_TABLES):
    """None when the warehouse has the `required` tables and columns (default: those that map ROA objects
    to publication points), else the reason why not."""
    for sub, cols in required:
        d = os.path.join(tables, sub)
        if not os.path.isdir(d) or not any(n.endswith('.parquet') for n in os.listdir(d)):
            return f'no {sub}/*.parquet'
        have = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({common.lit(d + '/*.parquet')}, hive_partitioning=false)").fetchall()}
        missing = [c for c in cols if c not in have]
        if missing:
            return f'{sub} lacks column(s) {", ".join(missing)}'
    return None


def object_certs_sql(tables, keys=True):
    """object_id -> host of the SIA of the EE certificate that signed the ROA object (lower-case), and with
    keys=True its (issuer_dn, serial_dec) for the CRL lookup: one scan of table=certs/is_ca=false for both
    (the largest table read here, 16 GB for RIPE NCC)."""
    sig = f"read_parquet('{tables}/table=object_signatures/*.parquet', hive_partitioning=false)"
    certs = f"read_parquet('{tables}/table=certs/is_ca=false/*.parquet', hive_partitioning=false)"
    roa = f"read_parquet('{tables}/table=roa_objects/*.parquet', hive_partitioning=false)"
    h = f"nullif(lower(regexp_extract(c.sia, '{HOST_RE}', 1)), '')"
    k = ', c.issuer_dn, c.serial_dec' if keys else ''
    return f"""
SELECT DISTINCT s.object_id, {h} AS host{k}
FROM {sig} s JOIN {certs} c ON c.cert_id = s.signer_cert_id
WHERE s.object_id IN (SELECT object_id FROM {roa})"""


def host_map_sql(objcerts):
    """object_id -> host from object_certs_sql. Objects with several signature or certificate rows take the
    smallest host (deterministic); n_hosts counts the distinct hosts seen (1 on every real warehouse)."""
    return f"SELECT object_id, min(host) AS host, count(DISTINCT host) AS n_hosts FROM {objcerts} GROUP BY object_id"


def ee_keys_sql(objcerts):
    """object_id -> (issuer_dn, serial_dec) of the EE certificate that signed the ROA object."""
    return (f"SELECT DISTINCT object_id, issuer_dn, serial_dec FROM {objcerts} "
            f"WHERE issuer_dn IS NOT NULL AND serial_dec IS NOT NULL")


def revocation_sql(tables, keys):
    """object_id -> rev = earliest CRL revocation_date of the EE certificate that signed the ROA object, for
    revoked objects only. keys: a table from ee_keys_sql. The CRL entry is matched by (issuer DN, serial):
    crl_serials.revoked_cert_id (and table=ee_revocations, built from it) is resolved only for
    certificates parsed in the same warehouse run as the CRL, a small part of the revocations.
    crl_serials repeats every entry in every CRL issued later (hundreds of millions of rows), so it is
    filtered by the keys and aggregated per (issuer, serial) before the join to the objects."""
    crl = f"read_parquet('{tables}/table=crl_serials/*.parquet', hive_partitioning=false)"
    return f"""
WITH r AS (SELECT cs.cert_issuer_dn AS issuer_dn, cs.serial_dec, min(cs.revocation_date) AS rev
           FROM {crl} cs SEMI JOIN (SELECT DISTINCT issuer_dn, serial_dec FROM {keys}) k
                ON k.issuer_dn = cs.cert_issuer_dn AND k.serial_dec = cs.serial_dec
           GROUP BY cs.cert_issuer_dn, cs.serial_dec)
SELECT k.object_id, CAST(min(r.rev) AS TIMESTAMP) AS rev
FROM {keys} k JOIN r ON r.issuer_dn = k.issuer_dn AND r.serial_dec = k.serial_dec
GROUP BY k.object_id"""


def raw_sql(tables, hosts=None, revoked=None):
    """Raw (unbridged) VRP day-intervals of one warehouse, merged per (host, VRP):
    host, prefix, max_length, asn, afi, first_day, last_day, valid_from, valid_until.
    hosts: a table (object_id, host) from host_map_sql, or None (every object gets host '?').
    revoked: a table (object_id, rev) from revocation_sql, or None (validity from the EE certificate only).
    valid_from / valid_until: EE not_before of the objects present on first_day (earliest), and
    least(EE not_after, revocation) of those present on last_day (latest)."""
    roa = f"""SELECT object_id, any_value(asn) AS asn,
                 CAST(min(ee_not_before) AS TIMESTAMP) AS nb, CAST(max(ee_not_after) AS TIMESTAMP) AS na
          FROM read_parquet('{tables}/table=roa_objects/*.parquet') GROUP BY object_id"""
    # presence run ∩ EE validity, as UTC days
    r = f"""SELECT object_id,
               CAST(greatest(CAST(p.start_ts AS TIMESTAMP), roa.nb) AS DATE) AS d_lo,
               CAST(least(CAST(p.end_ts AS TIMESTAMP), roa.na) AS DATE) AS d_hi
        FROM read_parquet('{tables}/table=object_presence_runs/*.parquet') p JOIN roa USING (object_id)
        WHERE greatest(CAST(p.start_ts AS TIMESTAMP), roa.nb) <= least(CAST(p.end_ts AS TIMESTAMP), roa.na)"""
    oi = intervals.islands_sql('r', ['object_id'])
    pfx = f"SELECT DISTINCT object_id, prefix, max_length, afi FROM read_parquet('{tables}/table=roa_prefixes/*.parquet')"
    unk = common.lit(UNKNOWN_HOST)
    if hosts:
        host, join = f"coalesce(h.host, {unk})", f" LEFT JOIN {hosts} h USING (object_id)"
    else:
        host, join = unk, ''
    if revoked:
        until, join = "least(roa.na, coalesce(rv.rev, roa.na))", join + f" LEFT JOIN {revoked} rv USING (object_id)"
    else:
        until = 'roa.na'
    v = f"""SELECT {host} AS host, p.prefix, p.max_length, roa.asn, p.afi, oi.first_day AS d_lo, oi.last_day AS d_hi,
                  roa.nb AS nb_, {until} AS vu_
           FROM oi JOIN roa USING (object_id) JOIN pfx p USING (object_id){join}"""
    return (f"WITH roa AS ({roa}), r AS ({r}), oi AS ({oi}), pfx AS ({pfx}), v AS ({v}) "
            + intervals.valid_islands_sql('v', ('host',) + intervals.VRP_KEY, extra=('afi',),
                                          vfrom='nb_', vuntil='vu_'))


def export(root, base=WAREHOUSE_BASE, threads=MAX_THREADS, memory_limit=f'{MAX_MEMORY_GB}GB', rirs=RIRS):
    """Read every warehouse into rawh_<rir> (per host, VRP) and raw_<rir> (per VRP, all hosts), both with
    the validity columns valid_from / valid_until.

    Returns (con, raw_rows, max_end, t_read, hosts): raw_rows = rows of raw_<rir> per RIR,
    hosts = {rir: {...counts...}} plus hosts['_fallback'] = {rir: reason} for warehouses without
    the host tables and hosts['_validity_fallback'] = {rir: reason} for warehouses without the
    revocation tables (validity from the EE certificates only)."""
    t0 = time.time()
    con = common.connect(threads=threads, memory_limit=memory_limit, root=root)
    guard_window('the snapshot listing')
    snaps = snapshot_days(base, rirs)
    con.execute("CREATE TABLE snapdays AS SELECT DISTINCT grp, CAST(d AS DATE) AS d "
                "FROM (SELECT unnest($1::VARCHAR[]) AS grp, unnest($2::VARCHAR[]) AS d)",
                [[a for a, _ in snaps], [b for _, b in snaps]])
    raw_rows, max_end, hosts, fallback, vfallback = {}, {}, {}, {}, {}
    for rir in rirs:
        guard_window(f'warehouse-{rir}')
        t = time.time()
        tables = os.path.join(base, f'warehouse-{rir}', 'tables')
        problem = host_tables_problem(con, tables)
        vproblem = problem or host_tables_problem(con, tables, REVOCATION_TABLES)
        hmap = rev = None
        if problem:
            fallback[rir] = problem
            common.log(f"vrp_export: WARNING warehouse-{rir}: {problem}; every object gets host "
                       f"'{UNKNOWN_HOST}' (RIR-only bridging for this warehouse)")
        else:
            hmap = f'hostmap_{rir}'
            con.execute(f"CREATE TEMP TABLE objcert_{rir} AS {object_certs_sql(tables, keys=not vproblem)}")
            con.execute(f"CREATE TABLE {hmap} AS {host_map_sql(f'objcert_{rir}')}")
        if vproblem:
            vfallback[rir] = vproblem
            common.log(f"vrp_export: WARNING warehouse-{rir}: {vproblem}; ROA validity from the EE certificates "
                       f"only (no CRL revocations) for the validity guard")
        else:
            rev = f'revoked_{rir}'
            con.execute(f"CREATE TEMP TABLE eekeys_{rir} AS {ee_keys_sql(f'objcert_{rir}')}")
            con.execute(f"CREATE TABLE {rev} AS {revocation_sql(tables, f'eekeys_{rir}')}")
            con.execute(f"DROP TABLE eekeys_{rir}")
        if hmap:
            con.execute(f"DROP TABLE objcert_{rir}")
        con.execute(f"CREATE TABLE rawh_{rir} AS SELECT {common.lit(rir)} AS rir, * FROM ({raw_sql(tables, hmap, rev)})")
        con.execute(f"CREATE TABLE raw_{rir} AS SELECT {common.lit(rir)} AS rir, * FROM ("
                    + intervals.valid_islands_sql(f'rawh_{rir}', intervals.VRP_KEY, lo='first_day', hi='last_day',
                                                  extra=('afi',)) + ")")
        raw_rows[rir] = con.execute(f"SELECT count(*) FROM raw_{rir}").fetchone()[0]
        max_end[rir] = con.execute(
            f"SELECT max(CAST(end_ts AS TIMESTAMP)) FROM read_parquet('{tables}/table=object_presence_runs/*.parquet')"
        ).fetchone()[0]
        h_rows, h_hosts, h_unknown = con.execute(
            f"SELECT count(*), count(DISTINCT host), count(*) FILTER (WHERE host = {common.lit(UNKNOWN_HOST)}) "
            f"FROM rawh_{rir}").fetchone()
        hosts[rir] = {'raw_rows_by_host': h_rows, 'hosts': h_hosts, 'raw_rows_unknown_host': h_unknown}
        if hmap:
            n_obj, multi = con.execute(f"SELECT count(*), count(*) FILTER (WHERE n_hosts > 1) FROM {hmap}").fetchone()
            hosts[rir].update(objects_mapped=n_obj, multi_host_objects=multi)
            if h_unknown:
                common.log(f"vrp_export: WARNING warehouse-{rir}: {h_unknown:,} raw rows have no publication point "
                           f"(host '{UNKNOWN_HOST}')")
        n_rev = con.execute(f"SELECT count(*) FROM {rev}").fetchone()[0] if rev else None
        hosts[rir]['revoked_objects'] = n_rev
        common.log(f'vrp_export: {rir} raw intervals {raw_rows[rir]:,} ({h_rows:,} per host, {h_hosts} hosts), '
                   f'revoked ROA objects {n_rev if n_rev is not None else "n/a"}, '
                   f'latest run end {max_end[rir]} [{time.time() - t:.1f}s]')
    hosts['_fallback'] = fallback
    hosts['_validity_fallback'] = vfallback
    t_read = time.time() - t0
    return con, raw_rows, max_end, t_read, hosts


def _union(con, name, prefix, rirs):
    con.execute(f"CREATE OR REPLACE VIEW {name} AS " + " UNION ALL ".join(f"SELECT * FROM {prefix}{r}" for r in rirs))


def _host_domains(con):
    """Tables host_domain (host -> registrable domain) and multi_dom (rir, domain with >= 2 hosts)."""
    hosts = [r[0] for r in con.execute("SELECT DISTINCT host FROM rawh_all ORDER BY host").fetchall()]
    con.execute("CREATE OR REPLACE TABLE host_domain AS "
                "SELECT unnest($1::VARCHAR[]) AS host, unnest($2::VARCHAR[]) AS domain",
                [hosts, [registrable_domain(h) for h in hosts]])
    con.execute("""CREATE OR REPLACE TABLE multi_dom AS
        SELECT r.rir, hd.domain, count(*) AS hosts FROM (SELECT DISTINCT rir, host FROM rawh_all) r
        JOIN host_domain hd USING (host) GROUP BY ALL HAVING count(*) >= 2""")


def bridge_and_write(con, root, raw_rows, max_end, t_read, rirs=RIRS, hosts=None):
    t = time.time()
    hosts = dict(hosts or {})
    fallback = hosts.pop('_fallback', {})
    vfallback = hosts.pop('_validity_fallback', {})
    k = intervals.VRP_KEY
    _union(con, 'raw_all', 'raw_', rirs)
    _union(con, 'rawh_all', 'rawh_', rirs)
    day_to = con.execute("SELECT max(last_day) FROM raw_all").fetchone()[0]
    day_from = min(CAL_FROM, con.execute("SELECT min(first_day) FROM raw_all").fetchone()[0])
    con.execute("CREATE TABLE cal AS " + intervals.calendar_sql('raw_all', 'snapdays', day_from, day_to, 'rir'))
    con.execute("CREATE TABLE hcal AS " + intervals.host_calendar_sql('rawh_all', 'snapdays', group_col='rir',
                                                                      sub_col='host'))
    _host_domains(con)
    # raw intervals and calendar per (rir, domain), for domains with several hosts only (one-host
    # domains cannot have a gap between different hosts)
    con.execute("CREATE TABLE rawd AS " + intervals.islands_sql(
        "(SELECT r.*, hd.domain FROM rawh_all r JOIN host_domain hd USING (host) "
        " JOIN multi_dom m ON m.rir = r.rir AND m.domain = hd.domain)",
        ('rir', 'domain') + k, lo='first_day', hi='last_day', extra=('afi',)))
    con.execute("CREATE TABLE dcal AS " + intervals.host_calendar_sql('rawd', 'snapdays', group_col='rir',
                                                                      sub_col='domain'))
    gaps, cross, host_stats, dom_stats, rir_holes = {}, {}, {}, {}, {}
    for rir in rirs:
        r = common.lit(rir)
        # reference: RIR-only bridging of the merged hosts (the method before per-host bridging)
        con.execute(f"CREATE TABLE ref_{rir} AS " + intervals.bridge_sql(f'raw_{rir}', 'cal', group_col='rir'))
        gaps[rir] = dict(zip(('tier1', 'tier2', 'gaps'), con.execute(
            intervals.gap_stats_sql(f'raw_{rir}', 'cal', group_col='rir')).fetchone()))
        # the RIR rule is not guarded; its closures with a validity hole are only counted
        n, gd, hd = con.execute(intervals.hole_stats_sql(f'raw_{rir}', 'cal', group_col='rir')).fetchone()
        rir_holes[rir] = {'gaps': n, 'gap_days': int(gd), 'hole_days': int(hd)}
        # a. per (rir, host, VRP): tier 1; tier 2 when every gap day is bad for the RIR, or every gap day is
        #    bad for the host and no whole gap day is without a valid object (validity guard)
        con.execute(f"CREATE TABLE brh_{rir} AS " + intervals.bridge_sql(
            f'rawh_{rir}', 'cal', group_col='rir', sub_col='host', sub_cal='hcal', guard=True))
        for g, h, n, t1, t2, t2h, t2d, rf, rfd in con.execute(intervals.sub_gap_stats_sql(
                f'rawh_{rir}', 'cal', 'hcal', group_col='rir', sub_col='host', guard=True)).fetchall():
            if t2 or rf:
                host_stats.setdefault(g, {})[h] = {'gaps': n, 'tier1': t1, 'tier2': t2,
                                                   'tier2_host_only': t2h, 'tier2_gap_days': int(t2d),
                                                   'refused_validity_hole': rf, 'refused_gap_days': int(rfd)}
        # b. merge the hosts of each domain (order first_day, last_day, host) and close gaps between
        #    rows of different hosts whose days are all bad for the domain (guarded) or for the RIR
        con.execute(f"CREATE TABLE mhd_{rir} AS SELECT {r} AS rir, * FROM (" + intervals.merge_sql(
            f"(SELECT b.*, hd.domain FROM brh_{rir} b JOIN host_domain hd USING (host))", ('domain',) + k,
            carry='host', valid=True) + ")")
        con.execute(f"CREATE TABLE brd_{rir} AS " + intervals.bridge_sql(
            f'mhd_{rir}', 'cal', group_col='rir', sub_col='domain', sub_cal='dcal', cross_col='host', guard=True))
        for g, dmn, n, t1, t2, t2d_only, t2d, rf, rfd in con.execute(intervals.sub_gap_stats_sql(
                f'mhd_{rir}', 'cal', 'dcal', group_col='rir', sub_col='domain', cross_col='host',
                guard=True)).fetchall():
            if t2d_only or rf:
                dom_stats.setdefault(g, {})[dmn] = {'gaps': n, 'tier2': t2, 'tier2_domain_only': t2d_only,
                                                    'tier2_gap_days': int(t2d), 'refused_validity_hole': rf,
                                                    'refused_gap_days': int(rfd)}
        # c. merge the domains per (rir, VRP) and apply the RIR rule once more
        con.execute(f"CREATE TABLE mh_{rir} AS SELECT {r} AS rir, * FROM ("
                    + intervals.islands_sql(f'brd_{rir}', k, lo='first_day', hi='last_day', extra=('afi',)) + ")")
        con.execute(f"CREATE TABLE brx_{rir} AS " + intervals.bridge_sql(f'mh_{rir}', 'cal', group_col='rir'))
        cross[rir] = dict(zip(('tier1', 'tier2', 'gaps'), con.execute(
            intervals.gap_stats_sql(f'mh_{rir}', 'cal', group_col='rir')).fetchone()))
        # bridged_days = days of the row without the VRP in any host's raw intervals
        con.execute(f"CREATE TABLE br_{rir} AS " + intervals.exact_bridged_sql(f'brx_{rir}', f'raw_{rir}', group_col='rir'))
        n_brx, n_br = (con.execute(f"SELECT count(*) FROM {x}_{rir}").fetchone()[0] for x in ('brx', 'br'))
        if n_brx != n_br:
            raise SystemExit(f'vrp_export: {rir}: {n_brx - n_br} bridged rows without a raw interval; not writing')
    for name in ('brh', 'brd', 'br', 'ref'):
        _union(con, f'{name}_all', f'{name}_', rirs)
    intervals.check_invariants(con, 'brh_all', group_col='rir, host', label='vrp_export (per host)')
    intervals.check_invariants(con, 'brd_all', group_col='rir, domain', label='vrp_export (per domain)')
    intervals.check_invariants(con, 'br_all', group_col='rir', label='vrp_export')
    superset = {}
    for name, inner in (('raw', 'raw_all'), ('rir_only_bridging', 'ref_all')):
        unc, n = con.execute(intervals.uncovered_sql(inner, 'br_all', group_col='rir')).fetchone()
        superset[name] = {'rows': n, 'uncovered': unc}
        if unc:
            raise SystemExit(f'vrp_export: {unc} rows of {inner} have days missing from the bridged output; not writing')
    t_bridge = time.time() - t
    bridged_rows = dict(con.execute("SELECT rir, count(*) FROM br_all GROUP BY rir").fetchall())
    latest_end = max(v for v in max_end.values() if v is not None)
    last_complete = latest_end.date() - dt.timedelta(days=1)
    stale = {r: (v.isoformat() if v else None) for r, v in max_end.items()
             if v is None or latest_end - v > dt.timedelta(hours=STALE_RIR_HOURS)}
    for r, v in stale.items():
        common.log(f'vrp_export: WARNING warehouse-{r} presence ends {v}, more than {STALE_RIR_HOURS} h before '
                   f'{latest_end} (presence build failed?); its VRPs end early until it catches up')
    common.log(f'vrp_export: bridged rows {sum(bridged_rows.values()):,} (raw {sum(raw_rows.values()):,}); '
               f'latest run end {latest_end} -> last_complete_day {last_complete} [{t_bridge:.1f}s]')
    inp = common.inputs_dir(root)
    lcd = f"DATE '{last_complete.isoformat()}'"
    out = os.path.join(inp, 'warehouse_intervals.parquet')
    common.copy_to_parquet(con, f"""
        SELECT CAST(afi AS UTINYINT) AS afi, prefix, CAST(max_length AS UTINYINT) AS max_length,
               CAST(asn AS UBIGINT) AS asn, first_day, least(last_day, {lcd}) AS last_day,
               'warehouse' AS src, bridged_days, rir
        FROM br_all WHERE first_day <= {lcd}""", out,
        order_by='afi, prefix, max_length, asn, first_day, rir')
    common.copy_to_parquet(con, f"SELECT grp AS rir, d, n, med, has_snap, bad, cum_good FROM cal WHERE d <= {lcd}",
                           os.path.join(inp, 'warehouse_calendar.parquet'), order_by='rir, d')
    for name, cal_t, col in (('host', 'hcal', 'host'), ('domain', 'dcal', 'domain')):
        common.copy_to_parquet(con, f"""
            SELECT grp AS rir, sub AS {col}, d, CAST(n AS BIGINT) AS n, CAST(ref AS BIGINT) AS ref, has_snap, bad
            FROM {cal_t} WHERE d <= {lcd}""", os.path.join(inp, f'warehouse_{name}_calendar.parquet'),
            order_by=f'rir, {col}, d')
    rows = con.execute(f"SELECT count(*) FROM read_parquet({common.lit(out)})").fetchone()[0]
    by_rir = dict(con.execute(f"SELECT rir, count(*) FROM read_parquet({common.lit(out)}) GROUP BY rir").fetchall())
    still = con.execute(f"SELECT count(*) FROM read_parquet({common.lit(out)}) WHERE last_day = {lcd}").fetchone()[0]
    bad_days = {r: {'bad': b, 'no_snapshot': n} for r, b, n in con.execute(
        f"SELECT grp, count(*) FILTER (WHERE bad), count(*) FILTER (WHERE NOT has_snap) FROM cal "
        f"WHERE d <= {lcd} GROUP BY grp").fetchall()}
    sub_days = {}
    for name, cal_t in (('host', 'hcal'), ('domain', 'dcal')):
        sub_days[name] = {r: {f'{name}s': h, 'days': n, 'bad': b, 'bad_with_snapshot': bs}
                          for r, h, n, b, bs in con.execute(
            f"SELECT grp, count(DISTINCT sub), count(*), count(*) FILTER (WHERE bad), "
            f"count(*) FILTER (WHERE bad AND has_snap) FROM {cal_t} WHERE d <= {lcd} GROUP BY grp ORDER BY grp").fetchall()}
    multi = {}
    for r, dmn, n in con.execute("SELECT rir, domain, hosts FROM multi_dom ORDER BY rir, domain").fetchall():
        multi.setdefault(r, {})[dmn] = n
    # VRP-days of the output and of the RIR-only reference, both clipped to last_complete_day
    vrp_days = {r: {'rir_only': int(a), 'per_host': int(b), 'added': int(b - a)} for r, a, b in con.execute(f"""
        SELECT rir, sum(ref_days), sum(new_days) FROM (
          SELECT rir, sum(least(last_day, {lcd}) - first_day + 1) AS ref_days, 0 AS new_days
          FROM ref_all WHERE first_day <= {lcd} GROUP BY rir
          UNION ALL
          SELECT rir, 0, sum(least(last_day, {lcd}) - first_day + 1) FROM br_all WHERE first_day <= {lcd} GROUP BY rir)
        GROUP BY rir ORDER BY rir""").fetchall()}
    for r, v in vrp_days.items():
        hs, ds = host_stats.get(r, {}), dom_stats.get(r, {})
        refused = sum(x['refused_validity_hole'] for x in (*hs.values(), *ds.values()))
        common.log(f"vrp_export: {r} VRP-days {v['per_host']:,} (RIR-only bridging {v['rir_only']:,}, "
                   f"+{v['added']:,} by per-host/domain bridging; tier-2 gaps on "
                   f"{sum(1 for x in hs.values() if x['tier2'])} hosts, cross-host on "
                   f"{sum(1 for x in ds.values() if x['tier2_domain_only'])} domains; {refused:,} gaps refused by "
                   f"the validity guard; RIR rule closes {rir_holes[r]['gaps']:,} gaps with a validity hole)")
    meta = {
        'generated_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'src': 'warehouse', 'rows': rows, 'rows_by_rir': by_rir,
        'rows_before_clip': sum(bridged_rows.values()), 'raw_rows_by_rir': raw_rows,
        'still_present_rows': still,
        'gaps_by_rir': gaps, 'calendar_days_by_rir': bad_days,
        'bridging': {'tier1_days': intervals.TIER1_DAYS, 'tier2_days': intervals.TIER2_DAYS,
                     'host_window_days': intervals.HOST_WINDOW, 'host_bad_share': intervals.HOST_BAD_SHARE,
                     'host_min_ref': intervals.HOST_MIN_REF, 'host_part_min_ref': intervals.HOST_PART_MIN_REF,
                     'validity_guard': 'host and domain tier 2 (not the RIR rule)'},
        'hosts_by_rir': hosts, 'host_fallback_rirs': fallback, 'validity_fallback_rirs': vfallback,
        'host_calendar_days_by_rir': sub_days['host'],
        'domain_calendar_days_by_rir': sub_days['domain'],
        'domains_with_several_hosts': multi,
        'tier2_by_host': host_stats,
        'tier2_cross_host_by_domain': dom_stats,
        'cross_host_gaps_by_rir': cross,
        'rir_tier2_validity_holes': rir_holes,
        'vrp_days_by_rir': vrp_days,
        'superset_check': superset,
        'latest_run_end_by_rir': {k: (v.isoformat() if v else None) for k, v in max_end.items()},
        'stale_rirs': stale,
        'last_complete_day': last_complete.isoformat(),
        'first_day': con.execute(f"SELECT min(first_day) FROM read_parquet({common.lit(out)})").fetchone()[0],
        'seconds': {'read': round(t_read, 1), 'bridge': round(t_bridge, 1)},
        'bytes': os.path.getsize(out),
    }
    common.atomic_write_json(os.path.join(inp, 'warehouse_intervals.json'), meta)
    common.log(f'vrp_export: wrote {out} rows={rows:,} still_present={still:,} bytes={meta["bytes"]:,}')
    return meta


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--warehouse-base', default=WAREHOUSE_BASE, help='directory holding warehouse-<rir>/')
    ap.add_argument('--lock', default=LOCK_PATH)
    ap.add_argument('--lock-wait', type=int, default=LOCK_WAIT_S, help='seconds to wait for the shared lock')
    ap.add_argument('--threads', type=int, default=MAX_THREADS)
    ap.add_argument('--memory-limit-gb', type=int, default=MAX_MEMORY_GB)
    a = ap.parse_args(argv)
    threads = max(1, min(a.threads, MAX_THREADS))
    mem = f'{max(1, min(a.memory_limit_gb, MAX_MEMORY_GB))}GB'
    if not may_start():
        raise SystemExit('vrp_export: refusing to start between 00:45 and 01:20 UTC '
                         '(the run must be over before the 00:55-01:20 presence build window)')
    renice(10)
    t0 = time.time()
    with shared_lock(a.lock, a.lock_wait):
        if not may_start():
            raise SystemExit('vrp_export: lock acquired between 00:45 and 01:20 UTC; not reading')
        con, raw_rows, max_end, t_read, hosts = export(a.root, a.warehouse_base, threads, mem)
        # everything below reads only the in-memory tables, but keep the lock: raw_* views are
        # materialised tables, so releasing earlier would also be safe.
        meta = bridge_and_write(con, a.root, raw_rows, max_end, t_read, hosts=hosts)
    common.log(f'vrp_export: done in {time.time() - t0:.1f}s')
    return 0 if meta['rows'] else 1


if __name__ == '__main__':
    sys.exit(main())
