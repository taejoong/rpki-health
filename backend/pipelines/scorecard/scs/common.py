"""Shared paths, ISO weeks, warehouse reads, DuckDB connection and small file helpers.

Data layout under ROOT (default /ssd-data/scorecard; SCORECARD_ROOT or --root):
  ledger/      ledger.parquet (per RIR and UTC day), cycles.parquet (per snapshot), ledger.json
  builds/      <YYYYMMDDTHHMMSSZ>/ published files; `current` -> builds/<stamp>
  states/      <stamp>/ frozen final rows; `state` -> states/<stamp>
  releases/    <YYYY-Www>/ immutable copies of weekly builds plus CHANGES.csv
  logs/, tmp/ (DuckDB spill directories, per-run work directories)

Warehouse reads, the same discipline as pipelines/country/crc/vrp_export.py:
- explicit lists of regular `*.parquet` part files more than 180 s old: every table directory also has a
  `<table>.parquet` symlink to its newest part (a glob would read that part twice), and the ETL writes
  parts in place, so a part younger than a few minutes can be incomplete;
- read_parquet(..., union_by_name=true, hive_partitioning=false): some old parts carry extra columns;
- no warehouse read starts in [00:45, 01:20) UTC, the window of the 01:00 presence build (`may_start`,
  checked again before every statement by `run`), and presence tables are read only under a shared flock
  on /tmp/rpki_presence.lock, held for as short as possible (`presence_lock`); only the presence build
  writes them, under the exclusive lock, so a presence part younger than 180 s listed under the shared
  lock is an error (a build has just finished), not a part to skip;
- nice 10, DuckDB threads <= 8 and memory_limit <= 8GB (peak RSS 24-38% above the limit in production
  runs), spill to ROOT/tmp/duckdb-<pid>-<n>, at most MAX_SPILL (300GB, SCORECARD_MAX_SPILL) of spill per
  connection, one warehouse statement (`run`) at most 10 minutes. Nothing is ever written under the
  warehouse base.
TimeZone is UTC on every connection; the backend venv has no pytz, so TIMESTAMPTZ values must be CAST to
TIMESTAMP (naive UTC), DATE or VARCHAR before they are fetched into Python.
"""
import contextlib
import datetime as dt
import fcntl
import json
import os
import resource
import subprocess
import sys
import tempfile
import threading
import time

import duckdb

DEFAULT_ROOT = os.environ.get('SCORECARD_ROOT', '/ssd-data/scorecard')
WAREHOUSE_BASE = os.environ.get('SCORECARD_WAREHOUSE_BASE', '/ssd-data')
RPKI_CLIENT_METRICS = os.environ.get('SCORECARD_RPKI_CLIENT_METRICS', '/ssd-data/rpki-validator/output/metrics')

REGISTRIES = ('afrinic', 'apnic', 'arin', 'lacnic', 'ripencc')
RIR_LABELS = {'afrinic': 'AFRINIC', 'apnic': 'APNIC', 'arin': 'ARIN', 'lacnic': 'LACNIC', 'ripencc': 'RIPE NCC'}

# Eras: era 1 = daily RIPE NCC archive trees until 2026-04-14, era 2 = our own
# rpki-client cycles from 2026-04-08. ISO weeks 2026-W14..W16 mix both (and hold the 04-03..04-08 archive
# gap and the irregular cadence of 04-08..04-19): they are flagged permanently.
ERA2_FROM = dt.date(2026, 4, 8)
TRANSITION_FROM, TRANSITION_TO = dt.date(2026, 3, 30), dt.date(2026, 4, 19)
VERDICTS_FROM = dt.date(2026, 6, 20)     # object_validity (rpki-client verdicts) exists from 2026-06-20 02:40
HISTORY_FROM = dt.date(2011, 12, 26)     # ISO week of the first archive snapshot (2012-01-01, a Sunday)

LOCK_PATH = '/tmp/rpki_presence.lock'
LOCK_WAIT_S = 40 * 60
START_CUTOFF = (0, 45)                   # UTC [00:45, 01:20): no warehouse read starts (presence build 01:00)
BLACKOUT_END = (1, 20)
MIN_PART_AGE_S = 180
MAX_THREADS = 8
MAX_MEMORY_GB = 8
DEFAULT_THREADS = 4
DEFAULT_MEMORY_GB = 6
QUERY_TIMEOUT_S = 600
SPILL_PREFIX = 'duckdb-'
MAX_SPILL = os.environ.get('SCORECARD_MAX_SPILL', '300GB')   # DuckDB max_temp_directory_size per connection


# ── ISO weeks ────────────────────────────────────────────────────────────────
def to_date(x):
    if isinstance(x, dt.datetime):
        return x.date()
    if isinstance(x, dt.date):
        return x
    return dt.date.fromisoformat(str(x)[:10])


def monday(d):
    """Monday of the ISO week that contains d (a date, datetime or 'YYYY-MM-DD')."""
    d = to_date(d)
    return d - dt.timedelta(days=d.weekday())


def week_label(w):
    """'2026-W43' (ISO year and week number) for the week starting on Monday w."""
    y, n, _ = to_date(w).isocalendar()
    return f'{y:04d}-W{n:02d}'


def weeks(first, last):
    """Mondays from monday(first) to monday(last), inclusive."""
    w, last = monday(first), monday(last)
    out = []
    while w <= last:
        out.append(w)
        w += dt.timedelta(days=7)
    return out


def week_ranges(ws):
    """Contiguous (first, last) Monday ranges of a set of weeks, ascending."""
    out = []
    for w in sorted(set(ws)):
        if out and w - out[-1][1] == dt.timedelta(days=7):
            out[-1][1] = w
        else:
            out.append([w, w])
    return [tuple(r) for r in out]


def week_era(w):
    """'1', 'transition' (ISO 2026-W14..W16) or '2' for the week starting on Monday w."""
    w = to_date(w)
    if w + dt.timedelta(days=6) < TRANSITION_FROM:
        return '1'
    if w <= TRANSITION_TO:
        return 'transition'
    return '2'


def utc_now():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def utc_now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def iso(ts):
    """Naive-UTC datetime -> '2026-10-26T00:40:01Z' (None stays None)."""
    return ts.strftime('%Y-%m-%dT%H:%M:%SZ') if ts else None


def parse_ts(s):
    """'2026-10-26T00:40:01Z' / '2026-10-26 00:40:01' -> naive UTC datetime."""
    if isinstance(s, dt.datetime):
        return s.replace(tzinfo=None) if s.tzinfo is None else s.astimezone(dt.timezone.utc).replace(tzinfo=None)
    s = str(s).strip().replace('T', ' ').rstrip('Z')
    if s.endswith('+00:00') or s.endswith('+00'):
        s = s[:s.rindex('+')]
    return dt.datetime.fromisoformat(s)


# ── SQL literals ─────────────────────────────────────────────────────────────
def lit(s):
    """SQL string literal."""
    return "'" + str(s).replace("'", "''") + "'"


def date_lit(d):
    return f"DATE '{to_date(d).isoformat()}'"


def ts_lit(t):
    """TIMESTAMPTZ literal in UTC for a date (00:00) or a naive-UTC datetime."""
    if isinstance(t, dt.datetime):
        return f"TIMESTAMPTZ '{parse_ts(t).strftime('%Y-%m-%d %H:%M:%S')}+00'"
    return f"TIMESTAMPTZ '{to_date(t).isoformat()} 00:00:00+00'"


def files_sql(paths):
    return '[' + ', '.join(lit(p) for p in paths) + ']'


def sql_value(v):
    """Python value -> SQL literal (None, bool, int, float, date, str)."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, int):
        return repr(v)
    if isinstance(v, float):
        return repr(v) if v == v and v not in (float('inf'), float('-inf')) else f"CAST('{v}' AS DOUBLE)"
    if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
        return date_lit(v)
    return lit(v)


def values_table(con, name, cols, rows, chunk=2000):
    """TEMP TABLE name (cols = [(column, type)]) from Python rows, inserted as VALUES literals (list
    parameters cost about 80 us per element)."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} ({', '.join(f'{chr(34)}{c}{chr(34)} {t}' for c, t in cols)})")
    rows = list(rows)
    for i in range(0, len(rows), chunk):
        vals = ', '.join('(' + ', '.join(sql_value(v) for v in r) + ')' for r in rows[i:i + chunk])
        con.execute(f"INSERT INTO {name} VALUES {vals}")
    return name


# ── paths ────────────────────────────────────────────────────────────────────
def builds_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'builds')


def current_link(root=DEFAULT_ROOT):
    return os.path.join(root, 'current')


def states_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'states')


def state_link(root=DEFAULT_ROOT):
    return os.path.join(root, 'state')


def releases_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'releases')


def ledger_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'ledger')


def tmp_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'tmp')


def warehouse_dir(rir, base=WAREHOUSE_BASE):
    return os.path.join(base, f'warehouse-{rir}')


def table_dir(rir, table, base=WAREHOUSE_BASE):
    """table: a directory name under tables/ without the 'table=' prefix, partition included where the
    table is partitioned: 'crls', 'certs/is_ca=true', 'certs/is_ca=false'."""
    return os.path.join(warehouse_dir(rir, base), 'tables', 'table=' + table)


# ── warehouse reads ──────────────────────────────────────────────────────────
_presence_held = 0      # > 0 while presence_lock() holds the shared presence lock


def part_files(rir, table, base=WAREHOUSE_BASE, min_age_s=MIN_PART_AGE_S, now=None):
    """Regular *.parquet files of one table directory (not recursive) last modified more than min_age_s
    ago, sorted. The <table>.parquet symlink to the newest part is skipped (it would be read twice), and so
    are parts the ETL may still be writing. A missing directory gives []. Under the shared presence lock a
    younger part of a presence table (object_presence_*) raises RuntimeError instead: only the presence
    build writes those, under the exclusive lock, so the part is complete and skipping it would silently
    drop the newest presence data."""
    d = table_dir(rir, table, base)
    now = time.time() if now is None else now
    out = []
    try:
        it = os.scandir(d)
    except FileNotFoundError:
        return out
    with it:
        for e in it:
            if not e.name.endswith('.parquet') or e.is_symlink() or not e.is_file(follow_symlinks=False):
                continue
            try:
                age = now - e.stat(follow_symlinks=False).st_mtime
            except FileNotFoundError:
                continue
            if age < min_age_s:
                if _presence_held and table.startswith('object_presence_'):
                    raise RuntimeError(f'warehouse-{rir} table={table}: {e.name} was written {age:.0f} s ago (less '
                                       f'than {min_age_s} s) although the shared presence lock is held: a presence '
                                       f'build has just finished; run again in a few minutes')
                continue
            out.append(e.path)
    out.sort()
    return out


def read_sql(paths, cols=None):
    """SQL fragment reading an explicit list of Parquet files ((SELECT cols FROM read_parquet(...)))."""
    src = f"read_parquet({files_sql(paths)}, union_by_name=true, hive_partitioning=false)"
    if cols:
        return f"(SELECT {', '.join(cols)} FROM {src})"
    return src


def _minute(now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.hour * 60 + now.minute


def may_start(now=None):
    """False in [00:45, 01:20) UTC: a warehouse read started then could overlap the 01:00 presence build."""
    (h0, m0), (h1, m1) = START_CUTOFF, BLACKOUT_END
    return not (h0 * 60 + m0 <= _minute(now) < h1 * 60 + m1)


def guard_window(what):
    """Stop before reading `what` inside [00:45, 01:20) UTC."""
    if not may_start():
        raise SystemExit(f'scorecard: inside the 00:45-01:20 UTC presence build window; not reading {what}')


def renice(target=10):
    """Raise our niceness to `target` (never lowers it)."""
    try:
        cur = os.nice(0)
        if cur < target:
            os.nice(target - cur)
    except OSError:
        pass


@contextlib.contextmanager
def presence_lock(path=LOCK_PATH, wait_s=LOCK_WAIT_S, poll_s=5.0):
    """flock(LOCK_SH) on the presence lock, polled with LOCK_NB every poll_s seconds for up to wait_s (the
    lock of app/db.py and crc/vrp_export.py; run_daily_build_presence.sh takes it exclusively at 01:00 with
    no timeout, and flock has no writer preference, so hold it only around presence-table reads). Inside
    [00:45, 01:20) UTC, before or after the wait, nothing is read. While it is held, part_files() refuses
    young presence parts instead of skipping them."""
    global _presence_held
    guard_window(f'presence tables (lock {path})')
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
                    log(f'scorecard: waiting for the shared lock on {path} (up to {wait_s}s)')
                    waited = True
                if time.monotonic() >= deadline:
                    raise SystemExit(f'scorecard: no shared lock on {path} after {wait_s}s')
                time.sleep(poll_s)
        guard_window(f'presence tables (lock {path} acquired)')
        _presence_held += 1
        try:
            yield
        finally:
            _presence_held -= 1
    finally:
        os.close(fd)


_spill_seq = iter(range(1 << 62))

MACROS = r"""
CREATE OR REPLACE TEMP MACRO scs_week(ts) AS CAST(date_trunc('week', ts) AS DATE);
CREATE OR REPLACE TEMP MACRO scs_host(sia) AS nullif(lower(regexp_extract(sia, 'rsync://([^/:,]+)', 1)), '');
"""


def connect(threads=DEFAULT_THREADS, memory_limit=f'{DEFAULT_MEMORY_GB}GB', tmp=None, root=DEFAULT_ROOT):
    """DuckDB connection: UTC, threads <= 8, memory_limit <= 8GB, spill to <tmp or ROOT/tmp>/duckdb-<pid>-<n>/
    (one directory per connection: DuckDB's spill files have fixed names and a closing instance deletes
    every duckdb_temp_* file of its directory, so two connections must never share one; DuckDB creates
    the directory on the first spill and removes it on close, run_weekly.sh removes those of killed runs),
    at most MAX_SPILL of spill (DuckDB's default is 90% of the free disk; a statement that needs more
    fails). Macros: scs_week(ts) = the Monday (DATE) of ts's ISO week in UTC; scs_host(sia) = lower-case
    host of the first rsync URI in an SIA string (NULL when there is none)."""
    threads = max(1, min(int(threads), MAX_THREADS))
    gb = float(str(memory_limit).upper().rstrip('B').rstrip('G'))
    memory_limit = f'{max(0.1, min(gb, MAX_MEMORY_GB)):g}GB'
    con = duckdb.connect()
    base = tmp or tmp_dir(root)
    os.makedirs(base, exist_ok=True)
    spill = os.path.join(base, f'{SPILL_PREFIX}{os.getpid()}-{next(_spill_seq)}')
    con.execute(f"SET threads={threads}; SET memory_limit={lit(memory_limit)}; SET temp_directory={lit(spill)}; "
                f"SET max_temp_directory_size={lit(MAX_SPILL)}; "
                "SET preserve_insertion_order=false; SET enable_progress_bar=false; SET TimeZone='UTC'")
    con.execute(MACROS)
    return con


def run(con, sql, params=None, timeout_s=QUERY_TIMEOUT_S):
    """con.execute(sql) that refuses to start inside [00:45, 01:20) UTC and interrupts the statement after
    timeout_s seconds (DuckDB raises an InterruptException). Returns the connection, like execute()."""
    guard_window('the warehouse (next statement)')
    timer = threading.Timer(timeout_s, con.interrupt)
    timer.daemon = True
    timer.start()
    try:
        return con.execute(sql, params) if params is not None else con.execute(sql)
    finally:
        timer.cancel()


# ── files ────────────────────────────────────────────────────────────────────
def log(*parts):
    ts = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(ts, *parts, file=sys.stderr, flush=True)


def fsync_paths(*paths):
    """fsync files and directories (after writing a file and after renaming into a directory)."""
    for p in paths:
        fd = os.open(p, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_write_text(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.tmp-', dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(text)
        os.chmod(tmp, 0o644)  # mkstemp creates 0600; outputs are read by other tools
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def atomic_write_json(path, obj, sort_keys=True):
    """sort_keys=False keeps the dict order (the build manifest lists method texts in registry order)."""
    atomic_write_text(path, json.dumps(obj, indent=1, sort_keys=sort_keys, default=str) + '\n')


def copy_to_parquet(con, select_sql, path, order_by=None):
    """COPY (select) TO path via a temp file in the same directory, then rename (atomic)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = os.path.join(os.path.dirname(path), '.tmp-' + os.path.basename(path))
    q = f"({select_sql}" + (f" ORDER BY {order_by}" if order_by else '') + ")"
    con.execute(f"COPY {q} TO {lit(tmp)} (FORMAT parquet, COMPRESSION zstd)")
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)
    return path


def copy_to_csv(con, select_sql, path, order_by=None, compression=None):
    """COPY (select) TO path as CSV with a header (compressed with compression='gzip'), via a temp file and
    a rename."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = os.path.join(os.path.dirname(path), '.tmp-' + os.path.basename(path))
    q = f"({select_sql}" + (f" ORDER BY {order_by}" if order_by else '') + ")"
    opts = 'FORMAT csv, HEADER true' + (f', COMPRESSION {compression}' if compression else '')
    con.execute(f"COPY {q} TO {lit(tmp)} ({opts})")
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)
    return path


def read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def peak_rss_mb():
    """Peak resident set size of this process (MB; Linux ru_maxrss is in KB)."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)


def code_commit(path=os.path.dirname(os.path.abspath(__file__))):
    """Short commit of the code checkout, '+dirty' when pipelines/scorecard has uncommitted changes,
    'unknown' outside a git checkout. --no-optional-locks: `git status` must not refresh (rewrite) the
    checkout's index, and a git killed at the timeout must not leave an index.lock behind."""
    pipe = os.path.dirname(path)
    try:
        head = subprocess.run(['git', '--no-optional-locks', '-C', pipe, 'rev-parse', '--short=12', 'HEAD'],
                              capture_output=True, text=True, timeout=20, check=True).stdout.strip()
        dirty = subprocess.run(['git', '--no-optional-locks', '-C', pipe, 'status', '--porcelain', '--', '.'],
                               capture_output=True, text=True, timeout=20, check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return 'unknown'
    return head + ('+dirty' if dirty else '')


def rpki_client_release(path=RPKI_CLIENT_METRICS):
    """rpki-client release from its OpenMetrics output (rpki_client_info{...,release="9.7"}), or None."""
    import re
    try:
        with open(path, errors='replace') as f:
            for line in f:
                if line.startswith('rpki_client_info'):
                    m = re.search(r'release="([^"]+)"', line)
                    return m.group(1) if m else None
    except OSError:
        return None
    return None
