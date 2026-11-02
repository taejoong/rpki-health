"""daily/* -> builds/<YYYYMMDDTHHMMSSZ>/ + atomic `current` symlink.

Files of a build (all Parquet except the manifest):
  manifest.json      version, as_of L, prev_date prev(L), grid, method/lenses/sources/notes (shown
                     verbatim by the UI), counts
  series.parquet     every geo row of every grid date <= L, sorted by level, key, af, date
  countries.parquet  one row per (cc, af) at L (cc != 'ZZ', routes > 0 or deleg > 0) with the
                     dominant registry and prev_<metric> at prev(L)
  ases.parquet       one row per (cc, af, asn) at L with name, as_cc, ratios, prev values and
                     first_valid (earliest grid date with routes_valid > 0)
  as_series.parquet  per-AS rows on the first grid date of every month (>= 2020-01) plus L
Publish: write builds/<stamp>.tmp, rename to builds/<stamp>, then
`ln -s builds/<stamp> current.new && mv -T current.new current` (symlink + rename); keep 3 builds.
Before the swap: no NaN/Inf, a sanity gate (the 'all' rows of L for both families, countries,
and AS rows when L has BGP data) and fsync of the files, so `current` never points at a broken or
half-written build. A failed build removes its .tmp directory and leaves `current` unchanged.

Freshness warnings (logged, listed as manifest "build_warnings" and in the printed summary, where
run_daily.sh turns them into a PARTIAL run): as_of older than yesterday (UTC), grid dates <= as_of
without daily data or computed by another crc VERSION (a recompute after a VERSION bump that has not
finished), recent BGP dates without pairs, a VRP source (inputs/vrp_intervals.json) that ends before
as_of (e.g. the RIPE NCC archive day is late), a delegation snapshot > 45 days old.

CLI (run from pipelines/country):
  python -m crc.assemble [--root R] [--as-of D] [--keep 3]
  --as-of defaults to the latest grid date with a done.json and BGP data.
  Prints one JSON line: build, as_of, prev_date, counts, warnings.
"""
import argparse
import datetime as dt
import json
import os
import re
import shutil
import sys
import time

from . import VERSION, common
from .common import lit, log
from .compute_day import BGP_METRICS, GEO_COLUMNS, REG_METRICS

STAMP_RE = re.compile(r'^\d{8}T\d{6}Z$')
METRICS = BGP_METRICS + REG_METRICS
PREV_COUNTRY = ('routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len', 'space', 'space_valid',
                'deleg', 'deleg_cov', 'ases', 'ases_any_valid')
AS_METRICS = ('routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len', 'routes_nf',
              'space', 'space_valid', 'space_invalid', 'space_nf')
AS_SERIES_COLS = ('cc', 'af', 'asn', 'date', 'routes', 'routes_valid', 'routes_inv_as', 'routes_inv_len',
                  'routes_nf', 'space', 'space_valid')

# Text shown by the page's Methodology section: manifest "method" (the frontend reads these keys in this
# order), "lenses", "sources" and "notes" ({from, to, text}; to = None: from that date on). Every number
# was re-checked on 2026-10-28 against the VERSION 2 build of that day (as_of 2026-10-26). VERSION 3:
# validation_and_roas and the notes from 2021-04-30, 2022-05-28 and 2025-12-26 rewritten, the Estonia
# note of 2026-01-29 dropped (the series now equals NRO there); checked against the VERSION 3 staging
# build of 2026-10-28 (as_of 2026-10-27).
METHOD = {
    'country': (
        'A country is the registered country of the address space, not where it is used. Each address '
        'belongs to the RIR delegation record that contains it: the NRO combined delegated statistics from '
        'October 2019, and the five RIR files before that. We use one snapshot per month (the first of the '
        "month), so space delegated or returned during a month moves at the next month's snapshot. Late in a "
        "month, an economy can therefore differ from NRO's daily figure by several points (for example "
        'Indonesia, about 5 points at the end of October 2026). EU and AP are '
        'RIR-wide codes. Free, reserved and unrecorded space is not listed. "All RIRs" is the five RIRs '
        'together; IANA-reserved space such as 2002::/16 (6to4) is left out.'),
    'routes': (
        'A route is a (prefix, origin AS) pair in the RIB dumps of the RouteViews and RIPE RIS collectors: '
        'one dump per collector every Wednesday in 2020-2025 (24-69 collectors), and daily from 2026 (about '
        '75 collectors). A route is visible when at least 25% of the collectors with data that day see it, '
        'rounded up. Three collectors that copy others are not counted. Only IPv4 prefixes from /8 to /24 and '
        'IPv6 prefixes from /16 to /48 count; shorter announcements are leaks. A route counts for the country '
        'of the record that holds its first address. "Routes" is the share of visible routes that are '
        'RPKI-valid. A block of routes sitting at the visibility threshold can move a country by a point or '
        "more from one day to the next (for example about 1,600 routes of CERNET's AS4538 in China in "
        'mid-2026).'),
    'invalid_routes': (
        'Networks that enforce route origin validation drop invalid routes, so invalid routes reach few '
        'collectors. The invalid share here (0.24% of visible IPv4 routes on 2026-10-26) is therefore several '
        'times lower than in monitors that count every announced route. The valid share changes little with '
        'the visibility rule.'),
    'routed_space': (
        'Each routed address takes the state of the most specific visible route that contains it: valid if '
        "one of that prefix's visible origins is valid, invalid if a ROA covers it but no origin is valid, and "
        'NotFound otherwise. Each address is counted once, IPv4 in addresses and IPv6 in /48s. In IPv6 a '
        'single large prefix can move a country by tens of points.'),
    'registered_space': (
        'The share of delegated space covered by at least one ROA, AS0 ROAs included. This is the NRO RPKI '
        "adoption definition, and the values reproduce NRO's per-economy figures closely: the median "
        'difference was 0.003 percentage points on 2026-10-26. The remaining differences have three causes: '
        'the monthly delegation snapshot; counting every ROA seen during the UTC day, while NRO uses one '
        "early-morning snapshot; and ROAs that span two economies' records, which count for both."),
    'origin_ases': (
        "The share of the ASes that originate visible routes in the country's space and have at least one "
        "valid route there. An AS that announces several countries' space counts in each. One organisation "
        "can dominate: most of China's IPv6 origin ASes are CERNET member institutions."),
    'validation_and_roas': (
        'Routes are validated with RFC 6811 against the VRPs of the same UTC day, meaning every VRP present '
        'at any time that day, so a ROA that existed for a few hours counts for the whole day. Until '
        '2026-04-07 the VRPs come from the daily snapshots of the RIPE NCC RPKI repository archive (APNIC '
        "from 2012-10-26 to 2018-04-04 from the archive's APNIC sub-TAL repositories). From 2026-04-08 they "
        'come from our own rpki-client runs every 20 minutes, merged with the RIPE NCC archive. Collection '
        'gaps of up to 3 days are bridged. Gaps of up to 31 days are bridged when our source missed all or '
        "much of an RIR's ROAs on those days, or missed the ROA's repository and that repository publishes "
        "at least 150 VRPs under one RIR, as the national registries' do. Only ROAs present on both sides of "
        'a gap are carried across it. Across a missed repository they are not carried over days on which the '
        'repository was fetched but they had expired or been revoked; the RIR-wide rule does not check this '
        '(see the note of 2015-12-01). Outages of smaller repositories are not bridged: in most cases we '
        'checked, other validators lacked the same ROAs.'),
    'rir_lines': (
        "These group space by the RIR that delegated it. NIST's RPKI Monitor groups by IANA /8 block, which "
        "puts legacy space in ARIN, so ARIN's routes-valid share is lower here (53% on 2026-10-26) than in "
        "NIST's ARIN line."),
    'time_grid': (
        'Every Wednesday from 2012-01-04 to 2025-12-31, then daily. Registered space is shown from 2012 and '
        'the BGP-based perspectives from 2020-01-01. The 1-year change compares with the latest grid date at '
        'least 365 days earlier.'),
    'as_table': (
        "The table counts an AS's visible routes in this country's registered space on the as-of day. The "
        "ASN Coverage page counts all of the AS's prefixes seen by three collectors over seven days, so its "
        'shares differ. "ROA since" is the first grid date with a valid route in this country, precise to '
        'the week before 2026.'),
}

LENSES = {
    'routes': {'label': 'Routes', 'numerator': 'routes_valid', 'denominator': 'routes',
               'from': '2020-01-01',
               'description': 'Share of visible routes (prefix-origin pairs) that are RPKI-valid.'},
    'space': {'label': 'Routed address space', 'numerator': 'space_valid', 'denominator': 'space',
              'from': '2020-01-01',
              'description': 'Share of routed address space whose most specific visible route is valid.'},
    'registered': {'label': 'Registered address space', 'numerator': 'deleg_cov', 'denominator': 'deleg',
                   'from': '2012-01-04',
                   'description': 'Share of delegated address space covered by at least one ROA, '
                                  'including AS0 ROAs.'},
    'ases': {'label': 'Origin ASes', 'numerator': 'ases_any_valid', 'denominator': 'ases',
             'from': '2020-01-01',
             'description': 'Share of origin ASes announcing the space that have at least one valid route.'},
}

SOURCES = [
    {'name': 'RouteViews and RIPE RIS RIB dumps (BGP warehouse of netsecurelab.org)',
     'url': 'https://rib.netsecurelab.org', 'use': 'routes: every Wednesday 2020-2025, daily from 2026'},
    {'name': 'RPKI warehouses of rpki.netsecurelab.org', 'url': 'https://rpki.netsecurelab.org',
     'use': 'VRPs per day from 2012: the RIPE NCC archive snapshots they hold, and our own rpki-client runs '
            'from 2026-04-08'},
    {'name': 'RIPE NCC RPKI repository archive', 'url': 'https://ftp.ripe.net/rpki/',
     'use': 'VRPs per day until 2026-04-07, merged with our own from 2026-04-08'},
    {'name': 'NRO combined delegated statistics', 'url': 'https://ftp.ripe.net/pub/stats/ripencc/nro-stats/',
     'use': 'registered country and RIR from 2019-10'},
    {'name': 'RIR delegated statistics (AFRINIC, APNIC, ARIN, LACNIC, RIPE NCC)',
     'url': 'https://www.nro.net/about/rirs/statistics/', 'use': 'registered country and RIR before 2019-10'},
    {'name': 'RIPE NCC AS names', 'url': 'https://ftp.ripe.net/ripe/asnames/asn.txt',
     'use': 'AS names and AS registration country'},
]

# One note per date range, in date order. The NIR-repository notes assume the per-repository bridging of
# crc.vrp_export; their windows are the days the RIPE NCC archive lacks the repository. Two cases are
# carried only in part and say so: IDNIC's repository move (2020-10) and TWNIC member CAs (2021-06..09).
_FLOOR = 'It is left out, like every prefix shorter than IPv4 /8 or IPv6 /16; counted, it would cut '
_NIR = "The RIPE NCC archive lacks {nir}'s whole repository {when}; its ROAs are carried across the {gaps}."
NOTES = [
    {'from': '2012-01-04', 'to': '2013-03-31',
     'text': "LACNIC's delegation file lists NIC.br's 2804::/16 as one Brazilian record until March 2013, so "
             "Brazil's and LACNIC's IPv6 registered-space denominators fall on 2013-04-03 (All RIRs IPv6 +2.3 pp)."},
    {'from': '2012-01-04', 'to': '2015-12-31',
     'text': "Registered-space coverage matches RIPE NCC's certification statistics and RIPEstat. Both come "
             'from the same RIPE NCC validation lineage, so the completeness of the early VRP archive is not '
             'independently verified.'},
    {'from': '2015-12-01', 'to': '2015-12-16',
     'text': 'The ROAs of AS10474 for 16 South African IPv4 prefixes were revoked on 2015-11-30 and re-issued '
             "from 2015-12-14. AFRINIC's VRP count dipped at the same time, so the RIR-wide gap rule carries "
             "them across, and South Africa's IPv4 registered-space coverage reads about 12.1% instead of about "
             '6.8% on the 2015-12-02, 12-09 and 12-16 grid dates.'},
    {'from': '2018-04-05', 'to': '2018-07-04',
     'text': 'APNIC moved to a single trust anchor. The RIPE NCC archive stops recording the old APNIC trust '
             'anchors on 2018-04-04, while the new one fills up over three months, so the APNIC dip in this '
             'period is most likely an artifact of the archive.'},
    {'from': '2020-01-15', 'to': '2020-01-15',
     'text': 'A very short announcement, 2a10::/12 from AS12654, passed the visibility rule. ' + _FLOOR +
             "RIPE NCC's IPv6 routed-space share from 55% to 3%."},
    {'from': '2020-04-21', 'to': '2020-04-22',
     'text': "Pakistan's IPv4 registered-space coverage drops about 68 pp (78.5% to 10.2%) on the 2020-04-22 "
             'grid date. PTCL (AS45595) re-issued its ROAs, and the archive shows no covering ROA between the '
             'last sighting of the old ones (2020-04-20) and the first of the new ones (2020-04-23).'},
    {'from': '2020-10-07', 'to': '2020-10-23',
     'text': 'IDNIC moved its repository from rpki-ca.idnic.net to repo-rpki.idnic.net, and the RIPE NCC '
             'archive has neither on these days. Only ROAs present on both sides of the gap are carried across '
             "it, and the new repository filled up over several weeks, so Indonesia's coverage is too low on "
             'the 2020-10-07, 10-14 and 10-21 grid dates (IPv4 registered space 8.4% instead of about 12%).'},
    {'from': '2021-04-30', 'to': '2021-05-26',
     'text': "About 400 of CNNIC's VRPs (148 ROAs) expired on 2021-04-29 and were re-issued on 2021-05-27, "
             "while the archive kept fetching CNNIC's repository. This lapse is real and is not bridged: "
             "China's IPv4 origin-AS share is about 17-18% on the May 2021 grid dates, against 20.7% on "
             '2021-04-28 and 21.9% on 2021-06-02.'},
    {'from': '2021-06-15', 'to': '2022-02-17',
     'text': _NIR.format(nir='TWNIC', when='on most days of this period, in gaps of up to 29 days', gaps='gaps') +
             " Until 2021-09-29 the archive also lacks some of TWNIC's member CAs on days when the rest of the "
             'repository is there. Those gaps are not bridged, so Taiwan is too low on 12 grid dates from '
             '2021-06-16 to 2021-09-29: IPv4 registered space 44.6-91.7% instead of about 97.8% (NRO shows '
             '97.8% on 2021-09-08, ours 87.2%). On 2021-07-28 IPv6 registered space is 10.5%, and the APNIC '
             'lines dip by about 2 points.'},
    {'from': '2021-12-15', 'to': '2021-12-15',
     'text': 'A very short announcement, 2000::/3 from AS131111, passed the visibility rule. ' + _FLOOR +
             "the IPv6 routed-space share of All RIRs from 50% to 1%."},
    {'from': '2022-02-13', 'to': '2022-02-17',
     'text': _NIR.format(nir='IDNIC', when='on these days', gaps='gap')},
    {'from': '2022-05-28', 'to': '2022-06-06',
     'text': "CNNIC's ROAs expired on 2022-05-27 and were re-issued between 05-31 and 06-07; other validators "
             "lacked them too. This lapse is real and is not bridged: China's IPv4 origin-AS share is 18.9% "
             'on 2022-06-01, against about 26.5% the weeks before and after.'},
    {'from': '2022-11-23', 'to': '2022-12-12',
     'text': _NIR.format(nir='IDNIC', when='on these days', gaps='gap')},
    {'from': '2023-04-17', 'to': '2023-07-10',
     'text': "Real: Telecom Argentina's ROAs for 2800:2000::/20 (AS7303 and two related ASes) were withdrawn "
             'after 2023-04-16, and the AS7303 ROA returned on 2023-07-11. Argentina\'s and LACNIC\'s IPv6 '
             'routed-space shares drop for these weeks (LACNIC from about 57% to 22%).'},
    {'from': '2023-07-14', 'to': '2023-07-17',
     'text': 'No RPKI snapshots for these days (also missing in the RIPE NCC archive). VRPs are '
             'bridged across the gap.'},
    {'from': '2024-12-17', 'to': '2024-12-24',
     'text': 'ARIN RPKI snapshots are missing. VRPs present on both sides are bridged across the gap, but '
             "about 900 IPv4 VRPs, mostly Amazon's, were replaced during it, so ARIN's IPv4 registered-space "
             'coverage on the 2024-12-18 grid date is about 1.4 pp low.'},
    {'from': '2025-01-08', 'to': '2025-01-08',
     'text': 'A very short announcement, 96.0.0.0/3 from AS9498, passed the visibility rule. ' + _FLOOR +
             'the IPv4 routed-space share of All RIRs from 50.3% to 49.3%.'},
    {'from': '2025-02-01', 'to': '2025-05-28',
     'text': _NIR.format(nir='JPNIC', when='in five windows: 2025-02-01 – 02-04, 02-27 – 03-07, 04-05 – 04-15, '
                                          '04-17 – 04-23 and 05-19 – 05-28', gaps='gaps')},
    {'from': '2025-02-19', 'to': '2025-02-19',
     'text': "Macao's IPv4 registered-space coverage drops from 99.1% to 48.1% for one week. CTM (AS4609) "
             "replaced about 780 VRPs with 10, and the RIPE NCC archive's snapshot of that day has neither the "
             "old nor the new ones. NRO's adoption statistic, taken earlier that day, shows 99.1%."},
    {'from': '2025-10-22', 'to': '2025-10-26',
     'text': _NIR.format(nir='TWNIC', when='on these days', gaps='gap')},
    {'from': '2025-12-26', 'to': '2026-01-13',
     'text': "The repository repo.rpki.space was unreachable: other validators and NRO lacked its roughly 70 "
             "VRPs too, so they are not bridged. Israel's IPv6 values dip on these days: origin ASes from "
             'about 80% to 65-66%, routes by about 7 points and registered space by about 0.8.'},
    {'from': '2026-01-01', 'to': '2026-01-01',
     'text': 'BGP source switch from the weekly archive (68 collectors) to the daily warehouse (75 '
             'collectors). All announced routes rise by about 3%, but the counted (visible) routes of All '
             'RIRs change by +0.05% (IPv4) and +0.3% (IPv6), and their IPv4 valid share by -0.06 pp. A few '
             'countries with many routes near the visibility threshold move by 1-5 pp.'},
    {'from': '2026-03-14', 'to': '2026-03-16',
     'text': 'A very short announcement, 2800::/6 from AS32098, passed the visibility rule on these days. ' +
             _FLOOR + 'the IPv6 routed-space share of All RIRs from 67% to 3%.'},
    {'from': '2026-03-19', 'to': '2026-03-19',
     'text': "Colombia's IPv4 registered-space coverage drops 13 pp for one day. This is real: AS13489 "
             'replaced about 40 VRPs, and the new ones were published a day after the old ones disappeared. '
             "NRO's adoption statistic shows the same drop."},
    {'from': '2026-04-08', 'to': None,
     'text': 'VRP source switch to our own rpki-client runs, merged with the RIPE NCC archive. Our runs miss '
             "CERNET's repository (about 12,300 APNIC VRPs) on several days from 2026-04-15, which are bridged, "
             'and on every day from 2026-05-15, when the archive restores them.'},
    {'from': '2026-04-08', 'to': '2026-04-08',
     'text': "AFRINIC's IPv6 routed-space share dips for one day (52% to 19%). This is real: 2c0e::/20 "
             '(AS24863) is missing from every RIB dump of that day.'},
    {'from': '2026-05-16', 'to': '2026-05-28',
     'text': 'Our APNIC snapshots missed the NIR repositories (IDNIC, JPNIC, CNNIC, TWNIC). '
             'Their VRPs are bridged across the gap, and the RIPE NCC archive adds the ROAs created during it.'},
    {'from': '2026-07-21', 'to': '2026-07-21',
     'text': "AFRINIC's IPv6 routed-space share dips for one day (54% to 22%). This is real: 2c0e::/20 "
             '(AS24863) is seen by only 13 of 74 collectors that day.'},
    {'from': '2026-10-04', 'to': '2026-10-12',
     'text': 'Outage of our RPKI collection. VRPs present before and after the gap are carried '
             'across it (bridged), so the series has no artificial drop.'},
]


def _files(paths):
    return '[' + ', '.join(lit(p) for p in paths) + ']'


def collect(root):
    """{date: done} for grid dates with done.json."""
    out = {}
    for d in common.done_days(root):
        if common.is_grid_date(d):
            done = common.read_json(common.done_path(d, root))
            if done:
                out[d] = done
    return out


def dominant_registry_sql(deleg_file, snap):
    """(cc, registry): most delegated IPv4 space, then IPv6, then ASNs (then name) at `snap`."""
    return f"""
      SELECT cc, registry FROM (
        SELECT cc, registry,
               coalesce(sum(CASE WHEN afi = 4 THEN CAST(end_ AS HUGEINT) - start_ + 1 END), 0) AS v4,
               coalesce(sum(CASE WHEN afi = 6 THEN CAST(end_ AS HUGEINT) - start_ + 1 END), 0) AS v6,
               coalesce(sum(CASE WHEN afi = 0 THEN CAST(end_ AS HUGEINT) - start_ + 1 END), 0) AS n_asn
        FROM read_parquet({lit(deleg_file)})
        WHERE snap = DATE {lit(snap.isoformat())} AND status = 'delegated' AND cc IS NOT NULL
          AND cc NOT IN ('', 'ZZ')
        GROUP BY cc, registry)
      QUALIFY row_number() OVER (PARTITION BY cc ORDER BY v4 DESC, v6 DESC, n_asn DESC, registry) = 1"""


def _check_no_nan(con, path):
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({lit(path)})").fetchall()
            if r[1] in ('DOUBLE', 'FLOAT')]
    if not cols:
        return
    cond = ' OR '.join(f'isnan({c}) OR isinf({c})' for c in cols)
    n = con.execute(f"SELECT count(*) FROM read_parquet({lit(path)}) WHERE {cond}").fetchone()[0]
    if n:
        raise RuntimeError(f'{path}: {n} rows with NaN/Inf')


def _gate(con, L, has_bgp):
    """Refuse to publish a build the site cannot show: missing world rows, no countries, no AS rows."""
    ds = f"DATE {lit(L.isoformat())}"
    afs = {r[0] for r in con.execute(f"SELECT DISTINCT af FROM series WHERE date = {ds} AND level = 'all' "
                                     "AND key = 'all'").fetchall()}
    problems = []
    if afs != {4, 6}:
        problems.append(f"series has 'all' rows for af {sorted(afs)} on {L}, expected [4, 6]")
    if has_bgp and con.execute(f"SELECT count(*) FROM series WHERE date = {ds} AND level = 'all' "
                               "AND routes IS NULL").fetchone()[0]:
        problems.append(f'{L} has pairs but its world routes are NULL')
    if not _n(con, "countries WHERE af = 4"):
        problems.append('countries.parquet has no IPv4 rows')
    if has_bgp and not _n(con, 'ases'):
        problems.append(f'{L} has BGP data but ases.parquet is empty')
    if problems:
        raise RuntimeError('build not published: ' + '; '.join(problems))


STALE_DAYS = 1         # as_of older than yesterday (UTC) is reported
RECENT_BGP = 14        # missing pairs are reported among the trailing grid dates only (older gaps are history)
DELEG_MAX_AGE = 45     # days between as_of and its delegation snapshot


def _fmt_dates(ds, n=5):
    ds = sorted(ds)
    return ', '.join(map(str, ds[:n])) + (f' ... {ds[-1]}' if len(ds) > n else '')


def freshness_warnings(root, L, dones, bgp_dates, snap, today=None):
    """Short sentences about inputs that are missing or late for a build with as_of L."""
    today = today or dt.datetime.now(dt.timezone.utc).date()
    out = []
    age = (today - L).days
    if age > STALE_DAYS:
        out.append(f'as_of {L} is {age} days old (no BGP snapshot for a later day yet)')
    g = common.grid(L)
    missing = [d for d in g if d not in dones]
    if missing:
        out.append(f'{len(missing)} grid date(s) <= as_of have no daily data: {_fmt_dates(missing)}')
    old = [d for d in g if d in dones and dones[d].get('version') != VERSION]
    if old:   # e.g. a manual assemble before the recompute after a VERSION bump finished
        out.append(f'{len(old)} grid date(s) <= as_of were computed by another crc VERSION than {VERSION}: '
                   f'{_fmt_dates(old)} (compute_day --only-missing recomputes them)')
    have = set(bgp_dates)
    no_bgp = [d for d in g[-RECENT_BGP:] if d >= common.BGP_FROM and d in dones and d not in have]
    if no_bgp:
        out.append(f'no BGP pairs for {_fmt_dates(no_bgp)} (BGP columns are NULL there)')
    vmeta = common.read_json(os.path.join(common.inputs_dir(root), 'vrp_intervals.json')) or {}
    for src, info in sorted((vmeta.get('sources') or {}).items()):
        last = (info or {}).get('last_day')
        if src != 'apnic-subtal' and last and common.to_date(last) < L:
            out.append(f'VRP source {src} ends on {last}: VRPs of {common.to_date(last) + dt.timedelta(days=1)}'
                       f'..{L} lack it until it catches up (the trailing recompute then fixes them)')
    if snap is None:
        out.append(f'no delegation snapshot <= {L}')
    elif (L - common.to_date(snap)).days > DELEG_MAX_AGE:
        out.append(f'delegation snapshot {snap} is {(L - common.to_date(snap)).days} days older than as_of')
    return out


def build(root, as_of=None, keep=3, stamp=None, threads=8, memory_limit='16GB', tmp=None, today=None):
    t0 = time.time()
    dones = collect(root)
    if not dones:
        raise RuntimeError(f'no computed grid dates under {root}/daily')
    bgp_dates = sorted(d for d, x in dones.items() if (x.get('inputs') or {}).get('pairs_src')
                       and os.path.exists(os.path.join(common.daily_dir(d, root), 'as.parquet')))
    L = common.to_date(as_of) if as_of else (bgp_dates[-1] if bgp_dates else None)
    if L is None:
        raise RuntimeError('no grid date with BGP data; pass --as-of')
    if L not in dones:
        raise RuntimeError(f'as-of {L} has no done.json')
    prev = common.prev_grid_date(L)
    dates = sorted(d for d in dones if d <= L)
    as_dates = [d for d in bgp_dates if d <= L]
    geo = {d: os.path.join(common.daily_dir(d, root), 'geo.parquet') for d in dates}
    asf = {d: os.path.join(common.daily_dir(d, root), 'as.parquet') for d in as_dates}
    deleg_file = common.deleg_path(root)
    names_file = common.asnames_path(root)

    stamp = stamp or dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    bdir = os.path.join(common.builds_dir(root), stamp)
    if os.path.exists(bdir):
        raise RuntimeError(f'{bdir} exists')
    work = bdir + '.tmp'
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    try:
        manifest = _write_build(root, work, L, prev, dates, as_dates, dones, bgp_dates, geo, asf, deleg_file,
                                names_file, threads, memory_limit, tmp, today)
        files = [os.path.join(work, f) for f in sorted(os.listdir(work))]
        common.fsync_paths(*files, work)
        os.rename(work, bdir)
        common.fsync_paths(common.builds_dir(root))
    except BaseException:
        shutil.rmtree(work, ignore_errors=True)
        raise
    publish(root, stamp)
    removed = prune(root, keep)
    counts = manifest['counts']
    log(f"assemble: build {stamp} as_of={L} prev={prev} dates={len(dates)} bgp_dates={len(as_dates)} "
        f"countries={counts['countries']} ases={counts['ases']} removed={removed} {time.time() - t0:.1f}s")
    for w in manifest['build_warnings']:
        log(f'assemble: WARNING {w}')
    return manifest, bdir


def _write_build(root, work, L, prev, dates, as_dates, dones, bgp_dates, geo, asf, deleg_file, names_file,
                 threads, memory_limit, tmp, today):
    """All build files into `work` (manifest last). Returns the manifest."""
    con = common.connect(threads=threads, memory_limit=memory_limit, tmp=tmp, root=root)
    try:
        # series: every geo row of every grid date <= L
        con.execute(f"CREATE TEMP TABLE series AS SELECT {', '.join(GEO_COLUMNS)} "
                    f"FROM read_parquet({_files(geo.values())})")
        common.copy_to_parquet(con, 'SELECT * FROM series', os.path.join(work, 'series.parquet'),
                               order_by='level, key, af, date')

        # countries at L
        snap = con.execute(f"SELECT max(snap) FROM read_parquet({lit(deleg_file)}) "
                           f"WHERE snap <= DATE {lit(L.isoformat())}").fetchone()[0]
        if snap is not None:
            con.execute(f"CREATE TEMP TABLE dom AS {dominant_registry_sql(deleg_file, snap)}")
        else:
            con.execute("CREATE TEMP TABLE dom (cc VARCHAR, registry VARCHAR)")
        cur_cols = ', '.join(f'c.{m}' for m in METRICS)
        prev_cols = ', '.join(f'p.{m} AS prev_{m}' for m in PREV_COUNTRY)
        prev_d = f"DATE {lit(prev.isoformat())}" if prev else 'NULL'
        con.execute(f"""CREATE TEMP TABLE countries AS
            SELECT c.key AS cc, c.af, dom.registry, {cur_cols}, {prev_cols}
            FROM (SELECT * FROM series WHERE date = DATE {lit(L.isoformat())} AND level = 'cc'
                    AND key <> 'ZZ' AND (coalesce(routes, 0) > 0 OR coalesce(deleg, 0) > 0)) c
            LEFT JOIN (SELECT * FROM series WHERE date = {prev_d} AND level = 'cc') p
                   ON p.key = c.key AND p.af = c.af
            LEFT JOIN dom ON dom.cc = c.key""")
        common.copy_to_parquet(con, 'SELECT * FROM countries', os.path.join(work, 'countries.parquet'),
                               order_by='cc, af')

        # ases at L
        as_schema = ("cc VARCHAR, af UTINYINT, asn UBIGINT, " +
                     ', '.join(f"{m} {'DOUBLE' if m.startswith('space') else 'BIGINT'}" for m in AS_METRICS))
        if L in asf:
            con.execute(f"CREATE TEMP TABLE as_cur AS SELECT * FROM read_parquet({lit(asf[L])})")
        else:
            con.execute(f"CREATE TEMP TABLE as_cur ({as_schema})")
        if prev in asf:
            con.execute(f"CREATE TEMP TABLE as_prev AS SELECT * FROM read_parquet({lit(asf[prev])})")
        else:
            con.execute(f"CREATE TEMP TABLE as_prev ({as_schema})")
        if asf:
            con.execute(f"""CREATE TEMP TABLE fv AS
                SELECT cc, af, asn, min(date) AS first_valid
                FROM read_parquet({_files(asf.values())}) WHERE routes_valid > 0 GROUP BY cc, af, asn""")
        else:
            con.execute("CREATE TEMP TABLE fv (cc VARCHAR, af UTINYINT, asn UBIGINT, first_valid DATE)")
        if os.path.exists(names_file):
            con.execute(f"""CREATE TEMP TABLE nm AS
                SELECT CAST(asn AS UBIGINT) AS asn, any_value(name) AS name,
                       any_value(CASE WHEN cc IN ('', 'ZZ') THEN NULL ELSE upper(cc) END) AS cc
                FROM read_parquet({lit(names_file)}) GROUP BY asn""")
        else:
            log(f'assemble: WARNING {names_file} missing; AS names will be NULL')
            con.execute("CREATE TEMP TABLE nm (asn UBIGINT, name VARCHAR, cc VARCHAR)")
        if snap is not None:
            con.execute(f"""CREATE TEMP TABLE dasn AS
                SELECT CAST(start_ AS UBIGINT) AS s, CAST(end_ AS UBIGINT) AS e,
                       CASE WHEN status = 'delegated' AND cc NOT IN ('', 'ZZ') THEN upper(cc) END AS cc
                FROM read_parquet({lit(deleg_file)}) WHERE snap = DATE {lit(snap.isoformat())} AND afi = 0""")
        else:
            con.execute("CREATE TEMP TABLE dasn (s UBIGINT, e UBIGINT, cc VARCHAR)")
        ascols = ', '.join(f'c.{m}' for m in AS_METRICS)
        con.execute(f"""CREATE TEMP TABLE ases AS
            SELECT c.cc, c.af, c.asn, nm.name,
                   coalesce(nm.cc, CASE WHEN da.e >= c.asn THEN da.cc END) AS as_cc,
                   {ascols},
                   CAST(c.routes_valid AS DOUBLE) / nullif(c.routes, 0) AS pct_valid,
                   CAST(c.space_valid AS DOUBLE) / nullif(c.space, 0) AS pct_space_valid,
                   p.routes AS prev_routes, p.routes_valid AS prev_routes_valid,
                   p.space AS prev_space, p.space_valid AS prev_space_valid, fv.first_valid
            FROM as_cur c
            LEFT JOIN as_prev p ON p.cc = c.cc AND p.af = c.af AND p.asn = c.asn
            LEFT JOIN fv ON fv.cc = c.cc AND fv.af = c.af AND fv.asn = c.asn
            LEFT JOIN nm ON nm.asn = c.asn
            ASOF LEFT JOIN dasn da ON c.asn >= da.s""")
        common.copy_to_parquet(con, 'SELECT * FROM ases', os.path.join(work, 'ases.parquet'),
                               order_by='cc, af, asn')

        # as_series: first grid date of every month (>= 2020-01) plus L
        series_dates = sorted(set(common.month_firsts([d for d in as_dates if d >= common.BGP_FROM]))
                              | ({L} if L in asf else set()))
        if series_dates:
            con.execute(f"""CREATE TEMP TABLE as_series AS SELECT {', '.join(AS_SERIES_COLS)}
                FROM read_parquet({_files(asf[d] for d in series_dates)})""")
        else:
            con.execute(f"CREATE TEMP TABLE as_series ({as_schema}, date DATE)")
        common.copy_to_parquet(con, f"SELECT {', '.join(AS_SERIES_COLS)} FROM as_series",
                               os.path.join(work, 'as_series.parquet'), order_by='cc, asn, af, date')

        for name in ('series', 'countries', 'ases', 'as_series'):
            _check_no_nan(con, os.path.join(work, f'{name}.parquet'))
        _gate(con, L, L in asf)
        counts = {
            'grid_dates': len(dates), 'bgp_dates': len(as_dates),
            'first_date': dates[0].isoformat(), 'series_rows': _n(con, 'series'),
            'countries': {str(af): n for af, n in con.execute(
                "SELECT af, count(*) FROM countries GROUP BY af ORDER BY af").fetchall()},
            'ases': _n(con, 'ases'), 'as_series_rows': _n(con, 'as_series'),
            'as_series_dates': len(series_dates),
        }
    finally:
        con.close()
    done_l = dones[L]
    manifest = {
        'version': VERSION, 'as_of': L.isoformat(), 'prev_date': prev.isoformat() if prev else None,
        'generated_at': common.utc_now_iso(),
        'grid': {'weekly_from': common.WEEKLY_FROM.isoformat(), 'daily_from': common.DAILY_FROM.isoformat()},
        'bgp_from': common.BGP_FROM.isoformat(),
        'method': METHOD, 'lenses': LENSES, 'sources': SOURCES, 'notes': NOTES, 'counts': counts,
        'as_of_inputs': done_l.get('inputs'),
        'build_warnings': freshness_warnings(root, L, dones, bgp_dates, snap, today),
    }
    # unsorted: the API passes the manifest through as it is, and the page reads the method keys in order
    common.atomic_write_json(os.path.join(work, 'manifest.json'), manifest, sort_keys=False)
    return manifest


def _n(con, table):
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def publish(root, stamp):
    """current -> builds/<stamp> via a new symlink renamed over the old one (mv -T)."""
    cur = common.current_link(root)
    new = cur + '.new'
    if os.path.lexists(new):
        os.unlink(new)
    os.symlink(os.path.join('builds', stamp), new)
    os.replace(new, cur)
    common.fsync_paths(os.path.dirname(cur))


def prune(root, keep=3):
    """Delete all but the newest `keep` builds (never the current target) and stale .tmp dirs."""
    bd = common.builds_dir(root)
    if not os.path.isdir(bd):
        return []
    target = os.path.basename(os.path.realpath(common.current_link(root)))
    stamps = sorted((s for s in os.listdir(bd) if STAMP_RE.match(s) and os.path.isdir(os.path.join(bd, s))),
                    reverse=True)
    removed = []
    for s in stamps[keep:]:
        if s != target:
            shutil.rmtree(os.path.join(bd, s))
            removed.append(s)
    now = time.time()
    for s in os.listdir(bd):
        p = os.path.join(bd, s)
        if s.endswith('.tmp') and os.path.isdir(p) and now - os.path.getmtime(p) > 6 * 3600:
            shutil.rmtree(p, ignore_errors=True)
    return removed


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--as-of')
    ap.add_argument('--keep', type=int, default=3)
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--memory-limit', default='16GB')
    ap.add_argument('--tmp')
    a = ap.parse_args(argv)
    manifest, bdir = build(a.root, as_of=a.as_of, keep=max(1, a.keep), threads=a.threads,
                           memory_limit=a.memory_limit, tmp=a.tmp)
    print(json.dumps({'build': os.path.basename(bdir), 'as_of': manifest['as_of'],
                      'prev_date': manifest['prev_date'], 'counts': manifest['counts'],
                      'warnings': manifest['build_warnings']}, sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main())
