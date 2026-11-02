"""BGP pairs for snapshot days: the BGP warehouse's daily snapshot over HTTPS -> pairs/YYYY/pairs_D.{parquet,json}.

The BGP warehouse writes the snapshot of day D at about 06:27 UTC on D+1. The rib API serves its files:
  GET {API}/snapshots                         list of days (date, complete, generated_at, ...)
  GET {API}/snapshots/{D}/files/{name}        manifest.json, routes.parquet, route_peers.parquet,
                                              peers.parquet
with the header X-API-Key read from ~/.config/bgp-warehouse.key. The key is never printed or logged.

The counts inside the snapshot's routes.parquet include three alias collectors (ix-br.gru,
ix-br2.gru, locix.fra are byte-identical copies of RouteViews collectors), so the counts are
recomputed from route_peers x peers x routes without them (the same rule as genji/bgp_country_pairs.py):
  n_peers      = rows of route_peers (non-alias peers) that carry the route
  n_collectors = distinct non-alias collectors among those peers
  n_ff         = those peers flagged genji_full_feed_v4 / genji_full_feed_v6 for the route's family
Rows kept: IPv4 <= /24, IPv6 <= /48, origin_as not NULL, 0.0.0.0/0 and ::/0 dropped.
json: collectors[af] = distinct non-alias collectors with >= 1 routable pair of that family.

CLI (run from pipelines/country):
  python -m crc.snapshot_pairs [--root R] --date D [--keep-raw]
  python -m crc.snapshot_pairs [--root R] --from A --to B [--keep-raw]
  python -m crc.snapshot_pairs [--root R] --catch-up    every listed snapshot day without a pairs
                                                        file (or whose snapshot was regenerated)
  python -m crc.snapshot_pairs --list                   print the available snapshot days
Raw files are stored under R/tmp/snap_raw/D/ and deleted after success unless --keep-raw.

Network failures: every request is retried with backoff (connection errors, timeouts, 429/5xx,
and bodies shorter than their Content-Length or cut off mid-stream); a day whose files do not
match the manifest's bytes/sha256 is downloaded again from a fresh manifest (DOWNLOAD_ATTEMPTS).
A partial download never replaces a file: it goes to <file>.part, which is removed on failure.
Exit status: 0 = nothing failed (days the API does not list are not failures in --catch-up),
1 = a day failed or the API could not be reached.
"""
import argparse
import datetime as dt
import hashlib
import http.client
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request

from . import VERSION, common
from .common import lit, log

API = os.environ.get('RIB_SNAPSHOT_API', 'https://rib.netsecurelab.org/api/v1')
KEY_FILE = os.path.expanduser(os.environ.get('RIB_API_KEY_FILE', '~/.config/bgp-warehouse.key'))
FILES = ('manifest.json', 'peers.parquet', 'routes.parquet', 'route_peers.parquet')
EXTRACTOR = f'crc.snapshot_pairs {VERSION}'
USER_AGENT = f'country-rpki/{VERSION} (rpki.netsecurelab.org pipeline)'
RETRIES = 5
DOWNLOAD_ATTEMPTS = 3      # whole-day attempts when a file does not match the manifest
# Plausibility floors, the same as genji/bgp_country_pairs.py: a day below them (a partly ingested
# snapshot) is not written, so it can never become a build's as_of; it is retried on the next run.
MIN_ROWS = {'4': 500_000, '6': 50_000}
MIN_COLLECTORS = 10


class NotAvailable(Exception):
    """The API has no snapshot (or file) for that day."""


class ShortRead(Exception):
    """The body ended before Content-Length bytes arrived (urllib does not raise for this)."""


class Mismatch(Exception):
    """A downloaded file does not match the manifest's bytes/sha256."""


def _api_key():
    with open(KEY_FILE) as f:
        key = f.read().strip()
    if not key:
        raise RuntimeError(f'empty API key file {KEY_FILE}')
    return key


def _request(url, dest=None, timeout=120, retries=RETRIES):
    """GET url with the API key. Returns bytes, or streams into `dest` (via dest.part + rename).

    Retries network errors, 429 and 5xx, short or cut-off bodies (Content-Length not reached,
    http.client.IncompleteRead) with exponential backoff. 404 -> NotAvailable.
    Error messages carry the URL and status only, never the request headers.
    """
    req = urllib.request.Request(url, headers={'X-API-Key': _api_key(), 'User-Agent': USER_AGENT})
    delay = 5
    part = dest + '.part' if dest is not None else None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                length = resp.headers.get('Content-Length')
                if dest is None:
                    data = resp.read()
                    if length is not None and len(data) != int(length):
                        raise ShortRead(f'{len(data)} of {length} bytes')
                    return data
                with open(part, 'wb') as f:
                    shutil.copyfileobj(resp, f, 1 << 20)
                    n = f.tell()
                if length is not None and n != int(length):
                    raise ShortRead(f'{n} of {length} bytes')
                os.replace(part, dest)
                return dest
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NotAvailable(f'{url}: 404') from None
            if e.code in (401, 403):
                raise RuntimeError(f'{url}: HTTP {e.code} (check the API key file {KEY_FILE})') from None
            if e.code != 429 and e.code < 500:
                raise RuntimeError(f'{url}: HTTP {e.code}') from None
            err = f'HTTP {e.code}'
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError, http.client.HTTPException,
                ShortRead) as e:
            err = f'{type(e).__name__}: {getattr(e, "reason", e)}'
        finally:
            if part is not None and os.path.exists(part):
                os.unlink(part)
        if attempt == retries:
            raise RuntimeError(f'{url}: giving up after {retries} attempts ({err})')
        log(f'snapshot_pairs: {url}: {err}; retry {attempt}/{retries - 1} in {delay}s')
        time.sleep(delay)
        delay = min(delay * 2, 120)


def list_snapshots():
    """[{date, complete, generated_at, ...}] as served by GET /snapshots, ascending by date."""
    data = json.loads(_request(f'{API}/snapshots', timeout=60))
    snaps = data.get('snapshots', data) if isinstance(data, dict) else data
    return sorted(snaps, key=lambda s: s['date'])


def _sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def download(day, raw_dir, attempts=DOWNLOAD_ATTEMPTS):
    """Fetch manifest + 3 parquet files of `day` into raw_dir; verify bytes/sha256 from the manifest.

    A file that does not match is deleted and the day is tried again from a fresh manifest (the
    snapshot may have been regenerated between the manifest and the file), up to `attempts` times.
    """
    for attempt in range(1, attempts + 1):
        try:
            return _download_once(day, raw_dir)
        except Mismatch as e:
            if attempt == attempts:
                raise RuntimeError(f'{e} (after {attempts} attempts)') from None
            log(f'snapshot_pairs: {e}; downloading {day} again ({attempt + 1}/{attempts})')
            time.sleep(10 * attempt)


def _download_once(day, raw_dir):
    os.makedirs(raw_dir, exist_ok=True)
    base = f'{API}/snapshots/{day.isoformat()}/files'
    mpath = os.path.join(raw_dir, 'manifest.json')
    _request(f'{base}/manifest.json', dest=mpath, timeout=60)
    with open(mpath) as f:
        manifest = json.load(f)
    if str(manifest.get('date')) != day.isoformat():
        raise RuntimeError(f'{day}: manifest is for {manifest.get("date")}')
    expect = manifest.get('files', {})
    for name in FILES[1:]:
        path = os.path.join(raw_dir, name)
        want = expect.get(name, {})
        if os.path.exists(path) and want.get('sha256') and os.path.getsize(path) == want.get('bytes') \
                and _sha256(path) == want['sha256']:
            continue  # complete file from an earlier --keep-raw or interrupted run
        t = time.time()
        _request(f'{base}/{name}', dest=path, timeout=600)
        size = os.path.getsize(path)
        if want.get('bytes') is not None and size != want['bytes']:
            os.unlink(path)
            raise Mismatch(f'{day} {name}: {size} bytes, manifest says {want["bytes"]}')
        if want.get('sha256') and _sha256(path) != want['sha256']:
            os.unlink(path)
            raise Mismatch(f'{day} {name}: sha256 mismatch')
        if not want.get('sha256') or want.get('bytes') is None:
            log(f'snapshot_pairs: WARNING {day} {name}: the manifest lists no bytes/sha256; file not verified')
        log(f'snapshot_pairs: {day} {name} {size / 1e6:.1f} MB in {time.time() - t:.1f}s')
    return manifest


def routable_sql(af='af', prefix='prefix'):
    plen = f"CAST(split_part({prefix}, '/', 2) AS INTEGER)"
    return (f"({plen} >= 1 AND (({af} = 4 AND {plen} <= {common.ROUTABLE_MAX_LEN[4]}) OR "
            f"({af} = 6 AND {plen} <= {common.ROUTABLE_MAX_LEN[6]})))")


def recount(con, raw_dir):
    """Alias-free recount of a snapshot directory into TEMP TABLE sp_pairs (+ per-af collectors).

    Distinct collectors are counted with per-route bitmasks (bit_or of 1 << collector index; one
    UBIGINT per 64 collectors), which equals count(DISTINCT collector) without a DISTINCT hash per
    group. Returns {"collectors": {"4": n, "6": n}, "rows": n, "rows_by_af": {...}}.
    """
    rp, pe, ro = (lit(os.path.join(raw_dir, f)) for f in ('route_peers.parquet', 'peers.parquet',
                                                         'routes.parquet'))
    aliases = ', '.join(lit(a) for a in common.ALIASES)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sp_coll AS
        SELECT collector, CAST(row_number() OVER (ORDER BY collector) - 1 AS INTEGER) AS idx
        FROM (SELECT DISTINCT collector FROM read_parquet({pe}) WHERE collector NOT IN ({aliases}))""")
    ncoll = con.execute("SELECT count(*) FROM sp_coll").fetchone()[0]
    nmask = max(1, (ncoll + 63) // 64)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sp_peer AS
        SELECT p.peer_id, c.idx, coalesce(p.genji_full_feed_v4, false) AS ff4,
               coalesce(p.genji_full_feed_v6, false) AS ff6
        FROM read_parquet({pe}) p JOIN sp_coll c USING (collector)""")
    masks = ', '.join(
        f"bit_or(CASE WHEN p.idx // 64 = {i} THEN CAST(1 AS UBIGINT) << (p.idx % 64) "
        f"ELSE CAST(0 AS UBIGINT) END) AS m{i}" for i in range(nmask))
    # one pass over route_peers; the join partner is the tiny peer table
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sp_agg AS
        SELECT rp.route_id, count(*) AS n, count(*) FILTER (WHERE p.ff4) AS n_ff4,
               count(*) FILTER (WHERE p.ff6) AS n_ff6, {masks}
        FROM read_parquet({rp}) rp JOIN sp_peer p USING (peer_id)
        GROUP BY rp.route_id""")
    mcols = [f'm{i}' for i in range(nmask)]
    mor = ', '.join(f'bit_or(a.{m}) AS {m}' for m in mcols)
    bitcount = ' + '.join(f'bit_count({m})' for m in mcols)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sp_pairs AS
        SELECT *, CAST({bitcount} AS USMALLINT) AS n_collectors FROM (
          SELECT CAST(r.af AS UTINYINT) AS af, r.prefix, CAST(r.origin_as AS UBIGINT) AS origin_as,
                 CAST(sum(a.n) AS UINTEGER) AS n_peers,
                 CAST(sum(CASE WHEN r.af = 4 THEN a.n_ff4 ELSE a.n_ff6 END) AS UINTEGER) AS n_ff, {mor}
          FROM read_parquet({ro}) r JOIN sp_agg a USING (route_id)
          WHERE r.origin_as IS NOT NULL AND {routable_sql('r.af', 'r.prefix')}
          GROUP BY r.af, r.prefix, r.origin_as)""")
    coll = {str(af): 0 for af in (4, 6)}
    rows_by_af = {str(af): 0 for af in (4, 6)}
    q = ("SELECT af, count(*), " + ' + '.join(f'bit_count(bit_or({m}))' for m in mcols)
         + " FROM sp_pairs GROUP BY af")
    for af, n, c in con.execute(q).fetchall():
        rows_by_af[str(af)] = int(n)
        coll[str(af)] = int(c)
    return {'collectors': coll, 'rows': sum(rows_by_af.values()), 'rows_by_af': rows_by_af}


def build_day(day, root, keep_raw=False, con=None, threads=8, memory_limit='16GB', listing=None):
    """Download + recount + atomic write of pairs for `day`. Returns the json meta."""
    day = common.to_date(day)
    t0 = time.time()
    raw_dir = os.path.join(common.tmp_dir(root), 'snap_raw', day.isoformat())
    manifest = download(day, raw_dir)
    own = con is None
    if own:
        con = common.connect(threads=threads, memory_limit=memory_limit, root=root)
    try:
        info = recount(con, raw_dir)
        low = [f"IPv{af}: {info['rows_by_af'][af]:,} pairs, {info['collectors'][af]} collectors"
               for af in ('4', '6')
               if info['rows_by_af'][af] < MIN_ROWS[af] or info['collectors'][af] < MIN_COLLECTORS]
        if low:
            raise RuntimeError(f'{day}: implausible snapshot, not written ({"; ".join(low)}; floors '
                               f'{MIN_ROWS["4"]:,}/{MIN_ROWS["6"]:,} pairs and {MIN_COLLECTORS} collectors '
                               f'per family)')
        meta = {
            'date': day.isoformat(), 'src': 'snapshot', 'collectors': info['collectors'],
            'rows': info['rows'], 'rows_by_af': info['rows_by_af'],
            'generated_at': common.utc_now_iso(), 'extractor': EXTRACTOR,
            'snapshot_generated_at': manifest.get('generated_at'),
            'snapshot_complete': manifest.get('complete'),
            'missing_collectors': sorted(m.get('collector') if isinstance(m, dict) else str(m)
                                         for m in manifest.get('missing_collectors', [])),
            'aliases_dropped': list(common.ALIASES),
        }
        common.copy_to_parquet(
            con, "SELECT af, prefix, origin_as, n_peers, n_collectors, n_ff, 'snapshot' AS src "
                 "FROM sp_pairs", common.pairs_path(day, root), order_by='af, prefix, origin_as')
        # the json marks the day as present (common.pairs_days): the parquet must be on disk first,
        # or a crash could leave a json next to an empty file (XFS) that fails every later compute
        common.fsync_paths(common.pairs_path(day, root), os.path.dirname(common.pairs_path(day, root)))
        common.atomic_write_json(common.pairs_meta_path(day, root), meta)
        common.fsync_paths(common.pairs_meta_path(day, root), os.path.dirname(common.pairs_path(day, root)))
        con.execute("DROP TABLE IF EXISTS sp_agg; DROP TABLE IF EXISTS sp_pairs; "
                    "DROP TABLE IF EXISTS sp_peer; DROP TABLE IF EXISTS sp_coll")
    finally:
        if own:
            con.close()
    if not keep_raw:
        shutil.rmtree(raw_dir, ignore_errors=True)
    if manifest.get('complete') is False:
        log(f'snapshot_pairs: WARNING {day}: the BGP warehouse marks this snapshot incomplete')
    log(f"snapshot_pairs: {day} rows={meta['rows']} v4={meta['rows_by_af']['4']} "
        f"v6={meta['rows_by_af']['6']} collectors={meta['collectors']} {time.time() - t0:.1f}s")
    return meta


def _days(a, b):
    a, b = common.to_date(a), common.to_date(b)
    return [a + dt.timedelta(days=i) for i in range((b - a).days + 1)]


def catch_up_days(root, listing, today=None):
    """Listed days without a pairs file, plus days whose snapshot was regenerated upstream.

    Logs a WARNING for unlisted days after the last pairs day and when yesterday (UTC) is not listed
    yet (the BGP warehouse writes the snapshot of D at about 06:27 UTC on D+1; a later run picks it up)."""
    today = today or dt.datetime.now(dt.timezone.utc).date()
    yesterday = today - dt.timedelta(days=1)
    if not any(common.to_date(s['date']) >= yesterday for s in listing):
        log(f'snapshot_pairs: WARNING the snapshot of {yesterday} is not listed yet (latest listed: '
            f'{listing[-1]["date"] if listing else "none"}); the build keeps the previous as_of')
    have = set(common.pairs_days(root))
    todo = []
    for s in listing:
        d = common.to_date(s['date'])
        if d not in have:
            todo.append(d)
            continue
        meta = common.read_json(common.pairs_meta_path(d, root), {}) or {}
        if meta.get('src') == 'snapshot' and s.get('generated_at') and \
                meta.get('snapshot_generated_at') != s.get('generated_at'):
            log(f'snapshot_pairs: {d} was regenerated upstream ({meta.get("snapshot_generated_at")} '
                f'-> {s.get("generated_at")}); refetching')
            todo.append(d)
    if listing and have:
        listed = {common.to_date(s['date']) for s in listing}
        last = max(have)
        latest = common.to_date(listing[-1]['date'])
        gaps = [d for d in _days(last + dt.timedelta(days=1), latest) if d not in listed] \
            if latest > last else []
        if gaps:
            log(f'snapshot_pairs: WARNING no snapshot listed for {", ".join(map(str, gaps))}')
    return sorted(set(todo))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--root', default=common.DEFAULT_ROOT)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--date')
    g.add_argument('--from', dest='dfrom')
    g.add_argument('--catch-up', action='store_true')
    g.add_argument('--list', action='store_true')
    ap.add_argument('--to', dest='dto')
    ap.add_argument('--keep-raw', action='store_true')
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--memory-limit', default='16GB')
    a = ap.parse_args(argv)
    if a.list:
        for s in list_snapshots():
            print(s['date'], 'complete' if s.get('complete') else 'INCOMPLETE', s.get('generated_at', ''))
        return 0
    if a.date:
        days = [common.to_date(a.date)]
    elif a.dfrom:
        if not a.dto:
            ap.error('--from needs --to')
        days = _days(a.dfrom, a.dto)
    else:
        try:
            listing = list_snapshots()
        except Exception as e:  # API down or key unreadable: one clear line, no traceback
            log(f'snapshot_pairs: ERROR cannot list snapshots: {type(e).__name__}: {e}')
            return 1
        days = catch_up_days(a.root, listing)
        if not days:
            log('snapshot_pairs: nothing to fetch')
    failed = []
    con = common.connect(threads=a.threads, memory_limit=a.memory_limit, root=a.root) if days else None
    try:
        for d in days:
            try:
                build_day(d, a.root, keep_raw=a.keep_raw, con=con)
            except NotAvailable as e:
                log(f'snapshot_pairs: WARNING {d}: not available ({e})')
                if a.date:
                    failed.append(d)
            except Exception as e:  # keep going over a range; report at the end
                log(f'snapshot_pairs: ERROR {d}: {e}')
                failed.append(d)
    finally:
        if con is not None:
            con.close()
    if failed:
        log(f'snapshot_pairs: failed days: {", ".join(map(str, failed))}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
