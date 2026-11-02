"""Shared paths, time grid, DuckDB connection and small file helpers.

Data layout under ROOT (default /ssd-data/country-rpki, or COUNTRY_RPKI_ROOT):
  inputs/   vrp_intervals.parquet, deleg.parquet, asnames.parquet, raw downloads
  pairs/    YYYY/pairs_YYYY-MM-DD.parquet + .json (visible BGP pairs per grid date)
  daily/    YYYY/YYYY-MM-DD/{geo,as}.parquet + done.json (per-date aggregates)
  builds/   <stamp>/ served files; `current` -> builds/<stamp>
  logs/, tmp/ (DuckDB spill)
Additive changes only: other modules import these names.
"""
import datetime as dt
import json
import os
import sys
import tempfile

import duckdb

DEFAULT_ROOT = os.environ.get('COUNTRY_RPKI_ROOT', '/ssd-data/country-rpki')

REGISTRIES = ('afrinic', 'apnic', 'arin', 'lacnic', 'ripencc')
ALIASES = ('ix-br.gru', 'ix-br2.gru', 'locix.fra')  # byte-identical copies of other collectors

# Time grid: Wednesdays 2012-01-04 .. 2025-12-31, then every day from 2026-01-01.
WEEKLY_FROM = dt.date(2012, 1, 4)
DAILY_FROM = dt.date(2026, 1, 1)
BGP_FROM = dt.date(2020, 1, 1)          # BGP-based lenses start here (first grid Wednesday of 2020)
VISIBILITY_SHARE = 0.25                 # visible = seen by >= ceil(25% of that day's collectors, per af)


def to_date(x):
    if isinstance(x, dt.date):
        return x
    return dt.date.fromisoformat(str(x)[:10])


def is_grid_date(d):
    d = to_date(d)
    if d >= DAILY_FROM:
        return True
    return d >= WEEKLY_FROM and (d - WEEKLY_FROM).days % 7 == 0


def grid(end, start=WEEKLY_FROM):
    """All grid dates in [start, end], ascending."""
    end, start = to_date(end), to_date(start)
    out = []
    d = WEEKLY_FROM
    while d < DAILY_FROM and d <= end:
        if d >= start:
            out.append(d)
        d += dt.timedelta(days=7)
    d = max(DAILY_FROM, start)
    while d <= end:
        out.append(d)
        d += dt.timedelta(days=1)
    return out


def bgp_grid(end, start=BGP_FROM):
    return grid(end, max(to_date(start), BGP_FROM))


def prev_grid_date(latest):
    """Latest grid date <= latest - 365 days (the '1 year earlier' comparison point)."""
    target = to_date(latest) - dt.timedelta(days=365)
    if target < WEEKLY_FROM:
        return None
    if target >= DAILY_FROM:
        return target
    return target - dt.timedelta(days=(target - WEEKLY_FROM).days % 7)


def month_firsts(dates):
    """First date of each calendar month present in `dates` (input in any order)."""
    seen = {}
    for d in sorted(to_date(x) for x in dates):
        seen.setdefault((d.year, d.month), d)
    return [seen[k] for k in sorted(seen)]


# ── paths ────────────────────────────────────────────────────────────────────
def inputs_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'inputs')


def pairs_path(d, root=DEFAULT_ROOT):
    d = to_date(d)
    return os.path.join(root, 'pairs', f'{d.year:04d}', f'pairs_{d.isoformat()}.parquet')


def pairs_meta_path(d, root=DEFAULT_ROOT):
    return pairs_path(d, root)[:-len('.parquet')] + '.json'


def daily_dir(d, root=DEFAULT_ROOT):
    d = to_date(d)
    return os.path.join(root, 'daily', f'{d.year:04d}', d.isoformat())


def builds_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'builds')


def current_link(root=DEFAULT_ROOT):
    return os.path.join(root, 'current')


def tmp_dir(root=DEFAULT_ROOT):
    return os.path.join(root, 'tmp')


# ── helpers ──────────────────────────────────────────────────────────────────
def log(*parts):
    ts = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(ts, *parts, file=sys.stderr, flush=True)


def lit(s):
    """SQL string literal."""
    return "'" + str(s).replace("'", "''") + "'"


SPILL_PREFIX = 'duckdb-'
_spill_seq = iter(range(1 << 62))


def connect(threads=16, memory_limit='48GB', tmp=None, root=DEFAULT_ROOT):
    """DuckDB connection; spills go to <tmp or ROOT/tmp>/duckdb-<pid>-<n>/, one directory per connection.

    DuckDB's spill files have fixed names (duckdb_temp_storage_S32K-0.tmp, ...) and a closing instance
    deletes every duckdb_temp_* file in its temp_directory, so connections sharing one directory (the
    --jobs children of compute_day, a manual run next to the cron run) corrupt each other's spills.
    DuckDB creates the per-connection directory on the first spill and removes it again on close;
    run_daily.sh removes the ones left by killed processes.
    """
    con = duckdb.connect()
    base = tmp or tmp_dir(root)
    os.makedirs(base, exist_ok=True)
    spill = os.path.join(base, f'{SPILL_PREFIX}{os.getpid()}-{next(_spill_seq)}')
    con.execute(f"SET threads={int(threads)}; SET memory_limit={lit(memory_limit)}; "
                f"SET temp_directory={lit(spill)}; SET preserve_insertion_order=false; "
                "SET TimeZone='UTC'")
    return con


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
        os.chmod(tmp, 0o644)  # mkstemp creates 0600; outputs are read by the backend and other tools
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def atomic_write_json(path, obj, sort_keys=True):
    """sort_keys=False keeps the dict order (the build manifest: the page reads its method keys in order)."""
    atomic_write_text(path, json.dumps(obj, indent=1, sort_keys=sort_keys, default=str) + '\n')


def copy_to_parquet(con, select_sql, path, order_by=None):
    """COPY (select) TO path via a temp file in the same dir, then rename (atomic)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = os.path.join(os.path.dirname(path), '.tmp-' + os.path.basename(path))
    q = f"({select_sql}" + (f" ORDER BY {order_by}" if order_by else '') + ")"
    con.execute(f"COPY {q} TO {lit(tmp)} (FORMAT parquet, COMPRESSION zstd)")
    os.replace(tmp, path)
    return path


# ── compute-side helpers (additive; used by snapshot_pairs, compute_day, assemble) ──────────
ROUTABLE_MAX_LEN = {4: 24, 6: 48}       # routable = IPv4 <= /24, IPv6 <= /48 (and not /0)
# Shortest prefix that counts as a route (compute_day's visible-pair filter). The pairs files
# keep shorter announcements; compute_day drops them as leaks. Four passed the visibility rule and swamped
# the routed-space lens on the days they were seen: 2a10::/12 (2020-01-15), 2000::/3 (2021-12-15),
# 96.0.0.0/3 (2025-01-08) and 2800::/6 (2026-03-14..16). /8 keeps the legacy IPv4 /8s; /16 keeps
# 2002::/16 (6to4), 2003::/19 and every other RIR-sized block.
ROUTABLE_MIN_LEN = {4: 8, 6: 16}
DATE_DIR_LEN = len('YYYY-MM-DD')


def vrp_intervals_path(root=DEFAULT_ROOT):
    return os.path.join(inputs_dir(root), 'vrp_intervals.parquet')


def deleg_path(root=DEFAULT_ROOT):
    return os.path.join(inputs_dir(root), 'deleg.parquet')


def asnames_path(root=DEFAULT_ROOT):
    return os.path.join(inputs_dir(root), 'asnames.parquet')


def done_path(d, root=DEFAULT_ROOT):
    return os.path.join(daily_dir(d, root), 'done.json')


def visibility_threshold(n_collectors):
    """Minimum n_collectors of a visible pair: ceil(25% of that day's collectors of the family)."""
    import math
    return math.ceil(VISIBILITY_SHARE * int(n_collectors or 0))


def read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def pairs_days(root=DEFAULT_ROOT):
    """Dates that have both pairs_D.parquet and pairs_D.json under ROOT/pairs, ascending."""
    out = []
    base = os.path.join(root, 'pairs')
    if not os.path.isdir(base):
        return out
    for year in sorted(os.listdir(base)):
        ydir = os.path.join(base, year)
        if not (len(year) == 4 and year.isdigit() and os.path.isdir(ydir)):
            continue
        for name in os.listdir(ydir):
            if name.startswith('pairs_') and name.endswith('.json'):
                try:
                    d = dt.date.fromisoformat(name[len('pairs_'):-len('.json')])
                except ValueError:
                    continue
                if os.path.exists(pairs_path(d, root)):
                    out.append(d)
    return sorted(out)


def done_days(root=DEFAULT_ROOT):
    """Dates with a daily/YYYY/D/done.json, ascending."""
    out = []
    base = os.path.join(root, 'daily')
    if not os.path.isdir(base):
        return out
    for year in sorted(os.listdir(base)):
        ydir = os.path.join(base, year)
        if not (len(year) == 4 and year.isdigit() and os.path.isdir(ydir)):
            continue
        for name in os.listdir(ydir):
            if len(name) != DATE_DIR_LEN:
                continue
            try:
                d = dt.date.fromisoformat(name)
            except ValueError:
                continue
            if os.path.exists(os.path.join(ydir, name, 'done.json')):
                out.append(d)
    return sorted(out)


def utc_now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def peak_rss_mb():
    """Peak resident set size of this process (MB, Linux ru_maxrss is in KB)."""
    import resource
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)
