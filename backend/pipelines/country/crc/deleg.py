"""Monthly delegation history -> inputs/deleg.parquet.

One snapshot per month (snap = first day of the month), taken from the first file published on or
after the 1st:
- 2019-10 onward: NRO https://ftp.ripe.net/pub/stats/ripencc/nro-stats/YYYYMMDD/combined-stat
  (first day directory of the month that has the file; NRO already resolves conflicts).
- before 2019-10: the five per-RIR files of that month (extended format when the first file of the
  month exists in extended form, else the legacy 7-field file), merged; rare overlaps between
  records are resolved by "most specific wins": inside an overlap the smallest range (then
  status delegated, then registry name) keeps the space; the rest of a larger record is kept.
Parsing: '#' comments, the version line and '|summary' lines are skipped; status allocated|assigned
-> 'delegated', available|reserved -> 'free'; cc upper-case, empty/'*'/free -> 'ZZ'. Ranges are
kept as ranges (IPv4: start + count - 1; IPv6: upper 64 bits, /64 units, prefixes longer than /64
clamped; ASN: start + count - 1).

Files (idempotent):
  inputs/deleg_raw/<rir>/<file>[.zst], inputs/deleg_raw/nro/YYYYMMDD.combined-stat.zst
      raw downloads (original .gz/.bz2 compression; uncompressed sources stored as zstd)
  inputs/deleg_raw/manifest.json   chosen file(s) per month (past months need no listing again)
  inputs/deleg_months/YYYY-MM.parquet + .json   parsed month (rebuilt when its files change)
  inputs/deleg.parquet  snap DATE, afi UTINYINT (0=ASN,4,6), registry, cc, status,
      start_ UBIGINT, end_ UBIGINT (inclusive; crc.ipkeys units; ASN numbers), src
      (src = '<kind>:<YYYYMMDD>' with kind nro-combined | rir-extended | rir-legacy)
  inputs/deleg.json     per-month counts, overlaps, files
CLI: python -m crc.deleg [--root R] [--from 2012-01] [--to YYYY-MM]
"""
import argparse
import calendar
import datetime as dt
import heapq
import json
import os
import re
import socket
import sys
import tempfile
import time

from crc import common, fetch

PARSER_VERSION = 2
NRO_FROM = (2019, 10)
NRO_BASE = 'https://ftp.ripe.net/pub/stats/ripencc/nro-stats/'
REGISTRY_SET = ('afrinic', 'apnic', 'arin', 'lacnic', 'ripencc', 'iana')
# 'ietf' and 'ianapool' are the status of IANA rows in NRO files before ~2020 (later 'reserved'/'available')
STATUS_MAP = {'allocated': 'delegated', 'assigned': 'delegated', 'available': 'free', 'reserved': 'free',
              'ietf': 'free', 'ianapool': 'free'}
AFI = {'asn': 0, 'ipv4': 4, 'ipv6': 6}
CC_RE = re.compile(r'^[A-Z]{2}$')
# RIPE NCC's extended files up to January 2012 leave cc empty on every allocation (2012-01-01:
# 21,213 IPv4 + 3,730 IPv6 records, ~42 /8s); the legacy file of the same day has them.
LEGACY_PREFERRED_BEFORE = {'ripencc': '20120201'}
SELECTION_VERSION = 2          # bump when the file-selection rule changes (re-resolves cached months)
NO_CC_WARN_SHARE = 0.01        # warn when > 1% of a file's delegated records have no country
# per-RIR archives: (year directory URL, extended name, legacy name); {d} = YYYYMMDD
RIR_SOURCES = {
    'afrinic': ('https://ftp.afrinic.net/pub/stats/afrinic/{y}/',
                'delegated-afrinic-extended-{d}', 'delegated-afrinic-{d}'),
    'apnic': ('https://ftp.apnic.net/stats/apnic/{y}/',
              'delegated-apnic-extended-{d}.gz', 'delegated-apnic-{d}.gz'),
    'arin': ('https://ftp.arin.net/pub/stats/arin/archive/{y}/',
             'delegated-arin-extended-{d}.gz', 'delegated-arin-{d}.gz'),
    'lacnic': ('https://ftp.lacnic.net/pub/stats/lacnic/archive/{y}/',
               'delegated-lacnic-extended-{d}', 'delegated-lacnic-{d}'),
    'ripencc': ('https://ftp.ripe.net/pub/stats/ripencc/{y}/',
                'delegated-ripencc-extended-{d}.bz2', 'delegated-ripencc-{d}.bz2'),
}


# ── parsing ───────────────────────────────────────────────────────────────────
def _v4(s):
    return int.from_bytes(socket.inet_pton(socket.AF_INET, s), 'big')


def _v6_hi(s):
    return int.from_bytes(socket.inet_pton(socket.AF_INET6, s)[:8], 'big')


def _asn(s):
    """asplain '327681' or asdot '5.1' (= 5 * 65536 + 1, used in old AFRINIC files)."""
    if '.' in s:
        hi, lo = s.split('.', 1)
        hi, lo = int(hi), int(lo)
        if not (0 <= hi < 65536 and 0 <= lo < 65536):
            raise ValueError(s)
        return hi * 65536 + lo
    return int(s)


def parse_record(fields, default_registry=None):
    """One record line split on '|' -> (afi, registry, cc, status, start, end) or a skip reason str."""
    if len(fields) < 7:
        return 'short'
    reg = fields[0].strip().lower()
    typ = fields[2].strip().lower()
    if typ not in AFI:
        return 'type'
    if reg not in REGISTRY_SET:
        if default_registry is None:
            return 'registry'
        reg = default_registry
    status = STATUS_MAP.get(fields[6].strip().lower())
    if status is None:
        return 'status:' + fields[6].strip().lower()
    cc = fields[1].strip().upper()
    if status == 'free' or not CC_RE.match(cc):
        cc = 'ZZ'
    start_s, value_s = fields[3].strip(), fields[4].strip()
    try:
        if typ == 'ipv4':
            start, n = _v4(start_s), int(value_s)
            end = start + n - 1
            if n <= 0 or end >= 1 << 32:
                return 'range'
        elif typ == 'ipv6':
            plen = int(value_s)
            if not 0 <= plen <= 128:
                return 'range'
            clen = min(plen, 64)
            start = _v6_hi(start_s) & ~((1 << (64 - clen)) - 1) & ((1 << 64) - 1)
            end = start + (1 << (64 - clen)) - 1
        else:
            start, n = _asn(start_s), int(value_s)
            end = start + n - 1
            if n <= 0 or start < 0 or end >= 1 << 32:
                return 'range'
    except (OSError, ValueError):
        return 'value'
    return AFI[typ], reg, cc, status, start, end


def parse_lines(lines, default_registry=None, header=None):
    """Iterate a delegated-stats file: returns (records, skipped Counter-like dict).

    Skips blank lines, '#' comments, the version line (first field starts with a digit: '2', '2.3',
    '2.1ap') and summary lines (6th field 'summary'). If `header` is a dict, header['declared'] is
    set to the record count the version line declares ('version|registry|serial|records|...').
    """
    recs, skipped = [], {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        f = line.split('|')
        if len(f) >= 6 and f[5].strip().lower() == 'summary':
            continue
        if f[0][:1].isdigit():   # version line: '2|nro|20261024|...', '2.1ap|apnic|...'
            if header is not None and 'declared' not in header and len(f) > 3 and f[3].strip().isdigit():
                header['declared'] = int(f[3])
            continue
        r = parse_record(f, default_registry)
        if isinstance(r, str):
            skipped[r] = skipped.get(r, 0) + 1
            continue
        recs.append(r)
    return recs, skipped


def resolve_overlaps(recs):
    """Most-specific-wins partition per afi.

    recs: tuples (afi, registry, cc, status, start, end, *extra). Returns (out, stats): no two
    records of the same afi overlap in out; inside an overlap the record with the smallest range
    wins (ties: 'delegated' before 'free', then registry, cc); a larger record keeps the parts that
    no more specific record claims (it is split, extra fields are copied to every piece).
    stats = {afi: {'overlapping_records': n, 'clusters': k}} counts the input records involved.
    """
    by_afi = {}
    for r in recs:
        by_afi.setdefault(r[0], []).append(r)
    out, stats = [], {}
    for afi, rs in sorted(by_afi.items()):
        rs.sort(key=lambda r: (r[4], -r[5]))
        clusters, cur, cur_end = [], [], -1
        for r in rs:
            if cur and r[4] <= cur_end:
                cur.append(r)
                cur_end = max(cur_end, r[5])
            else:
                if cur:
                    clusters.append(cur)
                cur, cur_end = [r], r[5]
        if cur:
            clusters.append(cur)
        n_ov = n_cl = 0
        for cl in clusters:
            if len(cl) == 1:
                out.append(cl[0])
                continue
            n_ov += len(cl)
            n_cl += 1
            out.extend(_paint(cl))
        stats[afi] = {'overlapping_records': n_ov, 'clusters': n_cl}
    return out, stats


def _prio(r):
    return (r[5] - r[4], 0 if r[3] == 'delegated' else 1, r[1], r[2])


def _paint(cl):
    """Partition one cluster of overlapping records (sorted by start) by most-specific-wins."""
    bounds = sorted({r[4] for r in cl} | {r[5] + 1 for r in cl})
    heap, j, pieces = [], 0, []
    for i in range(len(bounds) - 1):
        b, e = bounds[i], bounds[i + 1] - 1
        while j < len(cl) and cl[j][4] <= b:
            heapq.heappush(heap, (_prio(cl[j]), j))
            j += 1
        while heap and cl[heap[0][1]][5] < b:
            heapq.heappop(heap)
        if not heap:
            continue
        k = heap[0][1]
        if pieces and pieces[-1][2] == k and pieces[-1][1] == b - 1:
            pieces[-1][1] = e
        else:
            pieces.append([b, e, k])
    return [tuple(cl[k][:4]) + (b, e) + tuple(cl[k][6:]) for b, e, k in pieces]


def count_overlaps(recs):
    """Records whose range starts before the running max end of earlier records (same afi)."""
    n = 0
    by_afi = {}
    for r in recs:
        by_afi.setdefault(r[0], []).append((r[4], r[5]))
    for rs in by_afi.values():
        rs.sort()
        mx = -1
        for s, e in rs:
            if s <= mx:
                n += 1
            mx = max(mx, e)
    return n


# ── sources ───────────────────────────────────────────────────────────────────
def raw_dir(root):
    return os.path.join(common.inputs_dir(root), 'deleg_raw')


def months_dir(root):
    return os.path.join(common.inputs_dir(root), 'deleg_months')


def month_key(y, m):
    return f'{y:04d}-{m:02d}'


def parse_month(s):
    s = str(s)
    return int(s[:4]), int(s[5:7])


def month_range(a, b):
    (y, m), out = a, []
    while (y, m) <= b:
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def stored_name(name):
    """Local file name: keep .gz/.bz2/.xz; compress uncompressed sources."""
    return name if fetch.compression_of(name) else f'{name}.{fetch.RAW_COMPRESS}'


class Listings:
    """Cached directory listings for one run."""

    def __init__(self):
        self.cache = {}

    def get(self, url):
        if url not in self.cache:
            try:
                self.cache[url] = set(fetch.list_dir(url))
            except fetch.NotFound:
                self.cache[url] = set()
        return self.cache[url]


def find_rir_file(listings, rir, y, m):
    """First file of (rir, month) on or after the 1st: dict(url, name, date, kind) or None.

    On that day the extended file is preferred, except before LEGACY_PREFERRED_BEFORE[rir].
    """
    base, ext, leg = RIR_SOURCES[rir]
    names = listings.get(base.format(y=y))
    for day in range(1, calendar.monthrange(y, m)[1] + 1):
        d = f'{y:04d}{m:02d}{day:02d}'
        order = (('rir-extended', ext), ('rir-legacy', leg))
        if d < LEGACY_PREFERRED_BEFORE.get(rir, ''):
            order = order[::-1]
        for kind, pat in order:
            n = pat.format(d=d)
            if n in names:
                return {'registry': rir, 'url': base.format(y=y) + n, 'name': n, 'date': d, 'kind': kind}
    return None


def find_nro_file(listings, y, m):
    dirs = sorted(n.rstrip('/') for n in listings.get(NRO_BASE)
                  if re.match(r'^\d{8}/$', n) and n.startswith(f'{y:04d}{m:02d}'))
    for d in dirs:
        url = f'{NRO_BASE}{d}/combined-stat'
        if fetch.exists(url):
            return {'registry': 'nro', 'url': url, 'name': f'{d}.combined-stat', 'date': d, 'kind': 'nro-combined'}
    return None


def local_path(root, f):
    return os.path.join(raw_dir(root), f['registry'], stored_name(f['name']))


def load_manifest(root):
    p = os.path.join(raw_dir(root), 'manifest.json')
    try:
        with open(p) as fh:
            return json.load(fh)
    except (FileNotFoundError, ValueError):
        return {}


def resolve_month(root, listings, manifest, y, m):
    """List of source files for the month (from the manifest when complete locally), or None."""
    key = month_key(y, m)
    ent = manifest.get(key)
    if (ent and ent.get('v') == SELECTION_VERSION
            and all(os.path.exists(local_path(root, f)) for f in ent['files'])):
        return ent['files']
    if (y, m) >= NRO_FROM:
        f = find_nro_file(listings, y, m)
        return [f] if f else None
    files = []
    for rir in sorted(RIR_SOURCES):
        f = find_rir_file(listings, rir, y, m)
        if f is None:
            common.log(f'deleg: no {rir} file in {key}')
            return None
        files.append(f)
    return files


# ── month build ───────────────────────────────────────────────────────────────
class BadSourceFile(Exception):
    """A raw delegated file that does not look like a complete stats file (nothing is built from it)."""

    def __init__(self, path, problem):
        super().__init__(f'{path}: {problem}')
        self.path = path


DECLARED_TOLERANCE = 0.01   # parsed records may differ from the version line's count by 1 record or 1%


def file_problem(n_records, declared, registries=None):
    """None if a parsed file looks complete, else the reason. All 550 files of 2012-01..2026-10 pass
    (541 exactly; 9 AFRINIC/LACNIC files list one record more than they declare, e.g. an ASN both
    allocated and reserved). `registries` (NRO files only) must contain the five RIRs."""
    if n_records == 0:
        return 'no records (not a delegated-stats file?)'
    if declared is None:
        return 'no version line with a record count'
    if abs(n_records - declared) > max(1, DECLARED_TOLERANCE * declared):
        return f'{n_records:,} records parsed but the version line declares {declared:,}'
    if registries is not None and not set(common.REGISTRIES) <= set(registries):
        return f'registries {sorted(registries)} lack {sorted(set(common.REGISTRIES) - set(registries))}'
    return None


def build_month(con, root, y, m, files):
    """Parse the month's raw files -> deleg_months/YYYY-MM.parquet (+ .json). Returns the json dict.
    Raises BadSourceFile (nothing written) when a file fails file_problem()."""
    recs, skipped, file_info = [], {}, []
    for f in files:
        path = local_path(root, f)
        src = f"{f['kind']}:{f['date']}"
        default = None if f['registry'] == 'nro' else f['registry']
        hdr = {}
        with fetch.open_text(path) as fh:
            rs, sk = parse_lines(fh, default_registry=default, header=hdr)
        problem = file_problem(len(rs), hdr.get('declared'),
                               {r[1] for r in rs} if f['registry'] == 'nro' else None)
        if problem:
            raise BadSourceFile(path, problem)
        recs.extend(r + (src,) for r in rs)
        for k, v in sk.items():
            skipped[k] = skipped.get(k, 0) + v
        n_del = sum(1 for r in rs if r[3] == 'delegated' and r[1] != 'iana')
        n_nocc = sum(1 for r in rs if r[3] == 'delegated' and r[1] != 'iana' and r[2] == 'ZZ')
        if n_del and n_nocc > NO_CC_WARN_SHARE * n_del:
            common.log(f"deleg: WARNING {f['name']}: {n_nocc:,} of {n_del:,} delegated records have no country")
        file_info.append({'registry': f['registry'], 'name': f['name'], 'kind': f['kind'], 'date': f['date'],
                          'url': f['url'], 'records': len(rs), 'delegated_without_cc': n_nocc,
                          'bytes': os.path.getsize(path)})
    n_in = len(recs)
    overlaps_in = count_overlaps(recs)
    rows, ov_stats = resolve_overlaps(recs) if overlaps_in else (recs, {})
    overlaps_out = count_overlaps(rows)
    os.makedirs(months_dir(root), exist_ok=True)
    tmp_root = common.tmp_dir(root)
    os.makedirs(tmp_root, exist_ok=True)
    fd, tsv = tempfile.mkstemp(prefix=f'deleg-{month_key(y, m)}-', suffix='.tsv', dir=tmp_root)
    try:
        with os.fdopen(fd, 'w') as fh:
            for r in rows:
                fh.write('%d\t%s\t%s\t%s\t%d\t%d\t%s\n' % r)
        snap = f'{y:04d}-{m:02d}-01'
        sql = (f"SELECT DATE '{snap}' AS snap, CAST(afi AS UTINYINT) AS afi, registry, cc, status, "
               f"CAST(start_ AS UBIGINT) AS start_, CAST(end_ AS UBIGINT) AS end_, src "
               f"FROM read_csv({common.lit(tsv)}, delim='\t', header=false, auto_detect=false, "
               f"columns={{'afi': 'INTEGER', 'registry': 'VARCHAR', 'cc': 'VARCHAR', 'status': 'VARCHAR', "
               f"'start_': 'UBIGINT', 'end_': 'UBIGINT', 'src': 'VARCHAR'}})")
        pq = os.path.join(months_dir(root), f'{month_key(y, m)}.parquet')
        common.copy_to_parquet(con, sql, pq, order_by='afi, start_')
    finally:
        os.unlink(tsv)
    counts = {}
    for r in rows:
        k = {0: 'asn', 4: 'ipv4', 6: 'ipv6'}[r[0]]
        counts[k] = counts.get(k, 0) + 1
    info = {'month': month_key(y, m), 'parser': PARSER_VERSION, 'source': 'nro' if files[0]['registry'] == 'nro' else 'rir',
            'files': file_info, 'records_in': n_in, 'records': len(rows), 'records_by_type': counts,
            'overlapping_records_in': overlaps_in, 'overlap_resolution': {str(k): v for k, v in ov_stats.items()},
            'overlaps_out': overlaps_out, 'skipped': skipped}
    common.atomic_write_json(os.path.join(months_dir(root), f'{month_key(y, m)}.json'), info)
    return info


def month_is_current(root, y, m, files):
    j = os.path.join(months_dir(root), f'{month_key(y, m)}.json')
    pq = os.path.join(months_dir(root), f'{month_key(y, m)}.parquet')
    try:
        with open(j) as fh:
            info = json.load(fh)
    except (FileNotFoundError, ValueError):
        return None
    if not os.path.exists(pq) or info.get('parser') != PARSER_VERSION:
        return None
    if sorted(f['name'] for f in info['files']) != sorted(f['name'] for f in files):
        return None
    if any(os.path.getmtime(local_path(root, f)) > os.path.getmtime(pq) for f in files):
        return None
    return info


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    ap.add_argument('--from', dest='m_from', default='2012-01')
    ap.add_argument('--to', dest='m_to', default=None, help='YYYY-MM (default: current UTC month)')
    ap.add_argument('--threads', type=int, default=16)
    ap.add_argument('--memory-limit', default='48GB')
    a = ap.parse_args(argv)
    t0 = time.time()
    today = dt.datetime.now(dt.timezone.utc).date()
    m_from = parse_month(a.m_from)
    m_to = parse_month(a.m_to) if a.m_to else (today.year, today.month)
    months = month_range(m_from, m_to)
    listings = Listings()
    manifest = load_manifest(a.root)
    chosen, unresolved = {}, []
    for y, m in months:
        files = resolve_month(a.root, listings, manifest, y, m)
        if files is None:
            unresolved.append(month_key(y, m))
            continue
        chosen[(y, m)] = files
    jobs = []
    for files in chosen.values():
        for f in files:
            dest = local_path(a.root, f)
            if not os.path.exists(dest):
                jobs.append({'url': f['url'], 'dest': dest,
                             'compress': None if fetch.compression_of(f['name']) else fetch.RAW_COMPRESS})
    t_list = time.time() - t0
    common.log(f'deleg: {len(months)} months, {len(chosen)} resolved, {len(jobs)} files to fetch '
               f'[{t_list:.0f}s listing]')
    res = fetch.download_many(jobs, label='deleg') if jobs else []
    summ = fetch.summarize(res)
    errors = [(j['url'], e) for j, st, _, e in res if st in ('error', 'missing')]
    for u, e in errors:
        common.log(f'deleg: download failed {u}: {e}')
    bad = {k for k, files in chosen.items() for f in files if not os.path.exists(local_path(a.root, f))}
    for k in bad:
        unresolved.append(month_key(*k))
        del chosen[k]
    # remember resolved months (rewritten only when something changed: a no-op rerun touches nothing)
    before = json.dumps(manifest, sort_keys=True)
    for (y, m), files in chosen.items():
        manifest[month_key(y, m)] = {'v': SELECTION_VERSION, 'files': files}
    if json.dumps(manifest, sort_keys=True) != before or not os.path.exists(os.path.join(raw_dir(a.root), 'manifest.json')):
        common.atomic_write_json(os.path.join(raw_dir(a.root), 'manifest.json'), manifest)
    t_fetch = time.time() - t0 - t_list
    con = common.connect(threads=min(a.threads, 16), memory_limit=a.memory_limit, root=a.root)
    infos, built, bad_files = {}, 0, []
    t = time.time()
    for (y, m) in sorted(chosen):
        files = chosen[(y, m)]
        info = month_is_current(a.root, y, m, files)
        if info is None:
            try:
                info = build_month(con, a.root, y, m, files)
            except BadSourceFile as e:
                # the month is left out (its dates fall back to the previous snapshot) and the run
                # fails; the file is kept as <file>.bad for inspection, so the next run fetches it again
                common.log(f'deleg: ERROR {month_key(y, m)} not built: {e}; moved to {e.path}.bad')
                bad_files.append(str(e))
                unresolved.append(month_key(y, m))
                if os.path.exists(e.path):
                    os.replace(e.path, e.path + '.bad')
                continue
            built += 1
            common.log(f"deleg: {info['month']} {info['source']} records={info['records']:,} "
                       f"overlapping_in={info['overlapping_records_in']} out={info['overlaps_out']} "
                       f"skipped={info['skipped']}")
        infos[month_key(y, m)] = info
    t_parse = time.time() - t
    if not infos:
        raise SystemExit('deleg: no month available')
    out = os.path.join(common.inputs_dir(a.root), 'deleg.parquet')
    pqs = [os.path.join(months_dir(a.root), f'{k}.parquet') for k in sorted(infos)]
    newest = max(os.path.getmtime(p) for p in pqs)
    meta_path = os.path.join(common.inputs_dir(a.root), 'deleg.json')
    try:
        with open(meta_path) as fh:
            prev_months = json.load(fh).get('months_present')
    except (FileNotFoundError, ValueError):
        prev_months = None
    if (built == 0 and os.path.exists(out) and os.path.getmtime(out) >= newest
            and prev_months == sorted(infos)):
        common.log(f'deleg: {out} is up to date')
    else:
        flist = '[' + ', '.join(common.lit(p) for p in pqs) + ']'
        # month files are listed in month order and each is sorted by (afi, start_): keeping the
        # insertion order gives rows sorted by (snap, afi, start_) without a 90M-row sort
        con.execute("SET preserve_insertion_order=true")
        common.copy_to_parquet(con, f"SELECT snap, afi, registry, cc, status, start_, end_, src FROM read_parquet({flist})",
                               out)
        con.execute("SET preserve_insertion_order=false")
    rows = con.execute(f"SELECT count(*) FROM read_parquet({common.lit(out)})").fetchone()[0]
    meta = {
        'generated_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'rows': rows, 'months_present': sorted(infos), 'months_unresolved': sorted(set(unresolved)),
        'bad_files': bad_files, 'fetch': summ, 'fetch_errors': errors,
        'raw_bytes_on_disk': sum(os.path.getsize(local_path(a.root, f)) for fs in chosen.values() for f in fs
                                 if os.path.exists(local_path(a.root, f))),
        'months': {k: {kk: v[kk] for kk in ('source', 'records', 'records_by_type', 'overlapping_records_in',
                                            'overlaps_out', 'skipped')} | {'files': [f['name'] for f in v['files']]}
                   for k, v in infos.items()},
        'seconds': {'listing': round(t_list, 1), 'fetch': round(t_fetch, 1), 'parse': round(t_parse, 1),
                    'total': round(time.time() - t0, 1)},
        'bytes': os.path.getsize(out),
    }
    common.atomic_write_json(meta_path, meta)
    common.log(f'deleg: wrote {out} rows={rows:,} months={len(infos)} unresolved={meta["months_unresolved"]} '
               f'[{time.time() - t0:.0f}s]')
    past_unresolved = [k for k in meta['months_unresolved'] if k != month_key(*m_to)]
    return 1 if (errors or past_unresolved or bad_files) else 0


if __name__ == '__main__':
    sys.exit(main())
