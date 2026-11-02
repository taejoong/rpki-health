"""HTTP helpers for the input steps (stdlib urllib only).

- Every request sends User-Agent 'rpki-netsecurelab-country/1' (no e-mail address or other
  identifying headers) and has a timeout.
- Transient failures (timeouts, connection errors, HTTP 408/425/429/5xx, short reads, failed
  integrity checks) are retried with exponential backoff; 404/410 mean "missing" and are not retried.
- download() skips files that already exist, writes to a temporary name in the target directory
  and renames only after the integrity check, so a present file is always complete.
- Integrity: the byte count must match Content-Length, and .xz/.bz2/.gz/.zst files must
  decompress to the end. Uncompressed sources can be stored compressed (compress='zst'|'gz').
- download_many() runs at most MAX_PARALLEL (4) downloads at a time.
"""
import bz2
import concurrent.futures
import gzip
import html
import io
import lzma
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from crc import common

try:  # Python >= 3.14
    from compression import zstd as _zstd
except ImportError:  # pragma: no cover - older Pythons fall back to gzip
    _zstd = None

USER_AGENT = 'rpki-netsecurelab-country/1'
MAX_PARALLEL = 4
TIMEOUT = 120
RETRIES = 4
BACKOFF = 5.0
CHUNK = 1 << 20
RETRY_HTTP = (408, 425, 429, 500, 502, 503, 504)
HAVE_ZSTD = _zstd is not None
RAW_COMPRESS = 'zst' if HAVE_ZSTD else 'gz'   # how uncompressed sources are kept on disk


class NotFound(Exception):
    """HTTP 404/410."""


class IntegrityError(Exception):
    """Short read or a compressed file that does not decompress to the end."""


def _sleep_backoff(url, attempt, retries, backoff, err, retry_after=None):
    wait = retry_after if retry_after is not None else backoff * (2 ** attempt)
    common.log(f'fetch: {url}: {err}; retry {attempt + 1}/{retries} in {wait:.0f}s')
    time.sleep(wait)


def _retry_after(err):
    try:
        v = err.headers.get('Retry-After') if err.headers else None
        return min(float(v), 300.0) if v else None
    except (TypeError, ValueError):
        return None


def open_url(url, method='GET', timeout=TIMEOUT):
    """One attempt; raises NotFound for 404/410, urllib errors otherwise."""
    req = urllib.request.Request(url, method=method, headers={'User-Agent': USER_AGENT})
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            e.close()
            raise NotFound(url) from None
        raise


def _with_retries(fn, url, retries=RETRIES, backoff=BACKOFF):
    for attempt in range(retries + 1):
        try:
            return fn()
        except NotFound:
            raise
        except urllib.error.HTTPError as e:
            if e.code not in RETRY_HTTP or attempt == retries:
                raise
            _sleep_backoff(url, attempt, retries, backoff, f'HTTP {e.code}', _retry_after(e))
        except (urllib.error.URLError, TimeoutError, ConnectionError, IntegrityError, OSError, EOFError) as e:
            if attempt == retries:
                raise
            _sleep_backoff(url, attempt, retries, backoff, f'{type(e).__name__}: {e}')
    raise AssertionError('unreachable')


def fetch_bytes(url, timeout=TIMEOUT, retries=RETRIES, backoff=BACKOFF):
    """GET url into memory (small files and listings). Raises NotFound on 404."""
    def once():
        with open_url(url, timeout=timeout) as r:
            data = r.read()
            n = r.headers.get('Content-Length')
            if n is not None and int(n) != len(data):
                raise IntegrityError(f'short read {len(data)} of {n} bytes')
            return data
    return _with_retries(once, url, retries, backoff)


def exists(url, timeout=60, retries=RETRIES, backoff=BACKOFF):
    """HEAD url: True if 2xx, False on 404/410."""
    def once():
        with open_url(url, method='HEAD', timeout=timeout):
            return True
    try:
        return _with_retries(once, url, retries, backoff)
    except NotFound:
        return False


def list_dir(url, **kw):
    """Names linked from an HTML directory listing (directories keep their trailing '/')."""
    text = fetch_bytes(url, **kw).decode('utf-8', 'replace')
    out = []
    for href in re.findall(r'href="([^"]+)"', text):
        name = urllib.parse.unquote(html.unescape(href))
        if name.startswith(('?', '/', '#')) or name.startswith('..') or '://' in name:
            continue
        if '/' in name.rstrip('/'):
            continue
        if name not in out:
            out.append(name)
    return out


def _open_decompressed(path):
    p = path.lower()
    if p.endswith('.xz'):
        return lzma.open(path, 'rb')
    if p.endswith('.bz2'):
        return bz2.open(path, 'rb')
    if p.endswith('.gz'):
        return gzip.open(path, 'rb')
    if p.endswith('.zst'):
        if not HAVE_ZSTD:
            raise IntegrityError(f'cannot verify {path}: no zstd support in this Python')
        return _zstd.open(path, 'rb')
    return None


def compression_of(path):
    p = path.lower()
    for ext in ('xz', 'bz2', 'gz', 'zst'):
        if p.endswith('.' + ext):
            return ext
    return None


def verify_file(path, name=None):
    """Decompress `path` to the end (format from `name` or `path`); returns the decompressed size.

    Raises IntegrityError for truncated or corrupt data. Uncompressed files return their size.
    """
    name = name or path
    kind = compression_of(name)
    if kind is None:
        return os.path.getsize(path)
    opener = {'xz': lzma.open, 'bz2': bz2.open, 'gz': gzip.open}.get(kind)
    try:
        if kind == 'zst':
            if not HAVE_ZSTD:
                raise IntegrityError(f'cannot verify {name}: no zstd support in this Python')
            f = _zstd.open(path, 'rb')
        else:
            f = opener(path, 'rb')
        n = 0
        with f:
            while True:
                b = f.read(CHUNK)
                if not b:
                    break
                n += len(b)
        return n
    except IntegrityError:
        raise
    except (EOFError, OSError, lzma.LZMAError, ValueError) as e:
        raise IntegrityError(f'{name}: {type(e).__name__}: {e}') from None
    except Exception as e:  # zstd errors have their own class
        if type(e).__name__.startswith('Zstd'):
            raise IntegrityError(f'{name}: {type(e).__name__}: {e}') from None
        raise


def _compressor(kind, fileobj):
    if kind == 'zst':
        if not HAVE_ZSTD:
            raise ValueError('zstd not available in this Python')
        return _zstd.ZstdFile(fileobj, 'wb', level=10)
    if kind == 'gz':
        return gzip.GzipFile(fileobj=fileobj, mode='wb', compresslevel=6, mtime=0)
    raise ValueError(f'unknown compression {kind!r}')


def download(url, dest, compress=None, timeout=TIMEOUT, retries=RETRIES, backoff=BACKOFF, verify=True):
    """Fetch url to dest unless dest exists. Returns (status, bytes_downloaded).

    status: 'present' (skipped), 'ok' (downloaded and verified), 'missing' (404/410).
    compress: None keeps the bytes as served; 'zst'/'gz' compresses an uncompressed source on
    the fly (dest should then end in .zst/.gz). Raises after `retries` failed attempts.
    """
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return 'present', 0
    d = os.path.dirname(dest) or '.'
    os.makedirs(d, exist_ok=True)

    def once():
        fd, tmp = tempfile.mkstemp(prefix='.tmp-', suffix='-' + os.path.basename(dest), dir=d)
        try:
            with open_url(url, timeout=timeout) as r, os.fdopen(fd, 'wb') as raw:
                expected = r.headers.get('Content-Length')
                out = _compressor(compress, raw) if compress else raw
                n = 0
                try:
                    while True:
                        b = r.read(CHUNK)
                        if not b:
                            break
                        out.write(b)
                        n += len(b)
                finally:
                    if out is not raw:
                        out.close()
            if expected is not None and int(expected) != n:
                raise IntegrityError(f'short read {n} of {expected} bytes')
            if verify:
                size = verify_file(tmp, dest)
                if compress and size != n:
                    raise IntegrityError(f'recompressed size {size} != downloaded {n}')
            os.chmod(tmp, 0o644)
            os.replace(tmp, dest)
            return n
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    try:
        n = _with_retries(once, url, retries, backoff)
    except NotFound:
        return 'missing', 0
    return 'ok', n


def download_many(jobs, workers=MAX_PARALLEL, label='fetch'):
    """jobs: iterable of dicts with keys url, dest (+ download() keyword args).

    Runs at most min(workers, MAX_PARALLEL) downloads at once. Returns a list of
    (job, status, nbytes, error) in input order; failed jobs have status 'error'. Progress is
    logged every 50 jobs.
    """
    jobs = list(jobs)
    workers = max(1, min(int(workers), MAX_PARALLEL))
    results = [None] * len(jobs)
    t0 = time.time()
    done = 0
    total_bytes = 0

    def run(i):
        j = dict(jobs[i])
        url, dest = j.pop('url'), j.pop('dest')
        try:
            st, n = download(url, dest, **j)
            return i, st, n, None
        except Exception as e:  # keep going; the caller decides
            return i, 'error', 0, f'{type(e).__name__}: {e}'

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for i, st, n, err in ex.map(run, range(len(jobs))):
            results[i] = (jobs[i], st, n, err)
            done += 1
            total_bytes += n
            if err:
                common.log(f'{label}: FAILED {jobs[i]["url"]}: {err}')
            if done % 50 == 0 or done == len(jobs):
                common.log(f'{label}: {done}/{len(jobs)} files, {total_bytes / 1e6:.1f} MB '
                           f'in {time.time() - t0:.0f}s')
    return results


def summarize(results):
    """{'ok': n, 'present': n, 'missing': n, 'error': n, 'bytes': total}."""
    out = {'ok': 0, 'present': 0, 'missing': 0, 'error': 0, 'bytes': 0}
    for _, st, n, _ in results:
        out[st] = out.get(st, 0) + 1
        out['bytes'] += n
    return out


def open_text(path):
    """Text reader for a local raw file, decompressing by extension (xz/bz2/gz/zst/none)."""
    f = _open_decompressed(path)
    if f is None:
        return open(path, 'r', encoding='utf-8', errors='replace', newline='')
    return io.TextIOWrapper(f, encoding='utf-8', errors='replace', newline='')
