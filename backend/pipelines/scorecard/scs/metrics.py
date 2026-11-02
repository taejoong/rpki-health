"""Metric-module interface: what a module in scs/m_<name>.py receives and must emit.

A metric module computes the metrics that scs/defs.py assigns to it ('module': 'm_<name>') with one
function

    def compute(ctx, rir, week_from, week_to):
        '''Emit the rows of every week W (a Monday) in [week_from, week_to] for warehouse `rir`.'''

The framework calls it through Ctx.call() once per (module, RIR, contiguous range of weeks), RIR by RIR,
for the trailing weeks of a weekly run (default W-6..W-1) or for a whole --backfill range: one grouped
pass, never a loop over weeks. The module reads whatever it needs around the range itself (look-back for
13w/52w windows and re-sign predecessors, look-ahead for revocation horizons), computes all weeks with
GROUP BY week, and hands its rows to ctx.emit(). It returns nothing. It may skip metrics that are not in
ctx.metrics (rows of metrics that were not requested are dropped anyway).

Rows (ctx.emit(con, rows, hist=None): `rows` and `hist` are a SELECT statement or a table name in `con`;
column names must match exactly, order does not matter, types are cast):
  week        DATE     the Monday of the ISO week (UTC); for 13w/52w the last week of the window
  window      VARCHAR  '1w', '13w', '52w' or 'end' (point in time at the week's end); must be one of
                       the metric's defs 'windows'. `window` is a reserved word in DuckDB: always write
                       "window" in SQL
  rir         VARCHAR  the `rir` argument
  population  VARCHAR  rir_hosted, rir_paas, nir_hosted, delegated, unknown or all
  operator    VARCHAR  '' except on the per-operator rows of nir_hosted / rir_paas (e.g. 'JPNIC')
  metric_id   VARCHAR  a metric of this module (defs.METRICS[...]['module'] == the module's name)
  stat        VARCHAR  a statistic declared in the metric's defs 'stats' (exact, or a '^' regex family)
  value       DOUBLE   the statistic (NULL allowed, e.g. a median of nothing; never NaN or inf;
                       rounded to 10 decimals so reruns are byte-identical)
  n           BIGINT   objects / pairs / CRLs behind the value (>= 0, not NULL)
  n_ca        BIGINT   distinct CA keys (issuing keys: EE aki or crls.aki) behind the value (>= 0)
  top1_share  DOUBLE   the largest key's share of n, in [0, 1] (NULL when n = 0)
Emit a row for every (week, window, population 'all', stat) of the range (from the metric's defs
'available_from' on) even when n = 0, so that a silent read failure cannot pass as an empty week (the
publish gate wants every RIR in the newest week of every metric). scs/m_stub.py is a complete minimal
module: copy its shape.
Histograms (hist): week, window, rir, population, operator, metric_id, stat (a key of the metric's defs
  'histograms'), bin_lo DOUBLE, bin_hi DOUBLE (NULL = open-ended overflow bin), count BIGINT.
Keys are unique: (week, window, rir, population, operator, metric_id, stat) for rows, plus bin_lo for
histograms; emit() rejects duplicates, unknown names, weeks outside the range or not on a Monday, NaN/inf,
negative counts and an rir other than the argument (ValueError: a module bug).
The framework adds, per row (scs/status.py): status (provisional, final, masked, insufficient), era,
flags, as_of, def_version, hostmap_version, code_commit and frozen, and drops rows that are not published
yet (e.g. ROA-EE revocation of week W before W+2). Modules never set a status; masked and insufficient
rows keep their values.

ctx (Ctx below) carries:
  root, base           data root (SCORECARD_ROOT) and warehouse base (holding warehouse-<rir>/)
  as_of                input cut, naive UTC datetime (min over RIRs of the latest presence-run end);
                       no data after it is complete. ctx.as_of_sql is the same as a TIMESTAMPTZ literal
  current_week         the Monday of as_of's week (the incomplete week W)
  metrics, rirs        the metric_ids requested in this run, the RIRs of the run
  hostmap              scs.hostmap.HostMap (version, sha256, classify())
  mode                 'scorecard' (the default; 'compare' applies the overrides of defs.COMPARE_MODE,
                       empty in this release); ctx.params(metric_id) gives the definition's
                       parameters for the mode: read them instead of hard-coding constants
  connect()            a DuckDB connection (UTC, threads 4, memory_limit 6GB, own spill directory,
                       macros scs_week(ts) -> Monday DATE and scs_host(sia) -> lower-case rsync host);
                       open one per compute() call and close it before returning
  files(rir, table), has_table(rir, table), src(rir, table, cols=None), view(con, name, rir, table, cols)
                       warehouse tables as explicit lists of regular parts older than 180 s (table =
                       'crls', 'crl_serials', 'manifests', 'roa_objects', 'object_signatures',
                       'certs/is_ca=true', 'certs/is_ca=false', 'object_presence_runs', ...);
                       src() is a read_parquet(...) fragment, view() creates a TEMP VIEW
  run(con, sql)        execute with the 00:45-01:20 UTC guard and a 10-minute statement limit; use it for
                       every statement that reads the warehouse
  presence_lock()      `with ctx.presence_lock(): ...` around reads of object_presence_runs/_stats:
                       build the src()/view() inside the block, materialise what you need into TEMP
                       tables, leave the block (shared flock, keep it short: the 01:00 build waits);
                       inside it a presence part younger than 180 s raises instead of being skipped
  load_hostmap(con)    TEMP TABLE hostmap(host, rir, class, operator)
  load_ca_keys(con)    TEMP TABLE ca_keys(ski, host, aki, newest_not_before, certs, warehouses): every CA key
                       of the five warehouses with the SIA host of its newest certificate (built once per
                       run, cached in the run's work directory)
  load_ledger(con), load_cycles(con)  TEMP TABLE ledger / cycles (scs/ledger.py): per RIR-day and per
                       snapshot (sid, ts, good: e.g. 'directly observed' replacements = consecutive good
                       cycles)
  week_bounds(wf, wt)  (TIMESTAMPTZ literal of wf 00:00, of wt + 7 days 00:00) for range filters
How to classify populations (scs/classify.py): give every object a host column, then
  classify.populations_sql(src, host_col, rir) fans each row out to its populations (columns population and
  operator); aggregate with GROUP BY week, "window", population, operator. Host per metric family:
  signed objects = scs_host(EE sia) of the (predecessor's) signer EE certificate (object_signatures
  .signer_cert_id -> certs/is_ca=false.cert_id); CRLs/manifests = ca_keys.host of the issuing key
  (crls.aki / manifest EE aki), else the manifest EE's own host; CA lifetime = ca_keys.host of the
  parent key (child aki).
Tests: build a synthetic warehouse (tests/scorecard_world.py), make a Ctx with min_part_age_s=0 and a
fixed as_of, call ctx.call('m_<name>', rir, wf, wt) and read ctx.fetch('rows') / ctx.fetch('hist').
"""
import importlib
import importlib.util
import os
import re
import tempfile
import time

import duckdb

from . import classify, common, defs, hostmap as hostmap_mod
from .common import lit, log

ROW_COLS = (('week', 'DATE'), ('window', 'VARCHAR'), ('rir', 'VARCHAR'), ('population', 'VARCHAR'),
            ('operator', 'VARCHAR'), ('metric_id', 'VARCHAR'), ('stat', 'VARCHAR'), ('value', 'DOUBLE'),
            ('n', 'BIGINT'), ('n_ca', 'BIGINT'), ('top1_share', 'DOUBLE'))
HIST_COLS = (('week', 'DATE'), ('window', 'VARCHAR'), ('rir', 'VARCHAR'), ('population', 'VARCHAR'),
             ('operator', 'VARCHAR'), ('metric_id', 'VARCHAR'), ('stat', 'VARCHAR'), ('bin_lo', 'DOUBLE'),
             ('bin_hi', 'DOUBLE'), ('count', 'BIGINT'))
STATUS_COLS = (('status', 'VARCHAR'), ('era', 'VARCHAR'), ('flags', 'VARCHAR'), ('as_of', 'TIMESTAMPTZ'),
               ('def_version', 'INTEGER'), ('hostmap_version', 'INTEGER'), ('code_commit', 'VARCHAR'),
               ('frozen', 'BOOLEAN'))
OUT_COLS = ROW_COLS + STATUS_COLS
KEY = ('week', 'window', 'rir', 'population', 'operator', 'metric_id', 'stat')
HIST_KEY = KEY + ('bin_lo',)
ROUND = 10


def q(cols):
    """Quoted, comma-joined column list ("window" is reserved)."""
    return ', '.join(f'"{c}"' for c in cols)


def order_sql(key=KEY):
    """Deterministic order of output files: week, metric, rir, population, operator, window, stat (, bin)."""
    lead = ('week', 'metric_id', 'rir', 'population', 'operator', 'window', 'stat')
    return q(lead + tuple(k for k in key if k not in lead))


def module_metrics(module):
    return [m for m, d in defs.METRICS.items() if d['module'] == module]


def module_available(module):
    return importlib.util.find_spec(f'{__package__}.{module}') is not None


def import_module(module):
    return importlib.import_module(f'{__package__}.{module}')


class Ctx:
    """Everything a metric module gets (one per run; see the module docstring)."""

    def __init__(self, root, as_of, metrics, base=common.WAREHOUSE_BASE, rirs=common.REGISTRIES, hostmap=None,
                 threads=common.DEFAULT_THREADS, memory_limit=f'{common.DEFAULT_MEMORY_GB}GB', work=None,
                 min_part_age_s=common.MIN_PART_AGE_S, lock_path=common.LOCK_PATH, lock_wait_s=common.LOCK_WAIT_S,
                 mode='scorecard', ledger_dir=None, code_commit=None):
        self.root, self.base = root, base
        self.as_of = common.parse_ts(as_of)
        self.as_of_sql = common.ts_lit(self.as_of)
        self.current_week = common.monday(self.as_of)
        self.metrics = frozenset(metrics)
        unknown = self.metrics - set(defs.METRICS)
        if unknown:
            raise ValueError(f'unknown metric(s): {", ".join(sorted(unknown))}')
        self.rirs = tuple(rirs)
        self.hostmap = hostmap or hostmap_mod.load()
        self.threads, self.memory_limit = threads, memory_limit
        if not work:   # run-<pid>-<random>: run_weekly.sh removes those of processes that are gone
            os.makedirs(common.tmp_dir(root), exist_ok=True)
            work = tempfile.mkdtemp(prefix=f'run-{os.getpid()}-', dir=common.tmp_dir(root))
        self.work = work
        os.makedirs(os.path.join(self.work, 'fresh'), exist_ok=True)
        self.min_part_age_s, self.lock_path, self.lock_wait_s = min_part_age_s, lock_path, lock_wait_s
        self.mode = mode
        self.ledger_dir = ledger_dir or common.ledger_dir(root)
        self.code_commit = code_commit
        self.outputs = []          # [{module, rir, week_from, week_to, rows, hist, seconds, files}]
        self._call = None

    # ── warehouse ────────────────────────────────────────────────────────────
    def connect(self, threads=None, memory_limit=None):
        return common.connect(threads=threads or self.threads, memory_limit=memory_limit or self.memory_limit,
                              root=self.root)

    def files(self, rir, table):
        return common.part_files(rir, table, self.base, self.min_part_age_s)

    def has_table(self, rir, table):
        return bool(self.files(rir, table))

    def src(self, rir, table, cols=None):
        files = self.files(rir, table)
        if not files:
            raise FileNotFoundError(f'warehouse-{rir}: no parquet parts for table={table} under {self.base}')
        return common.read_sql(files, cols)

    def view(self, con, name, rir, table, cols=None):
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM {self.src(rir, table, cols)}")
        return name

    def run(self, con, sql, params=None):
        return common.run(con, sql, params)

    def guard(self, what):
        common.guard_window(what)

    def presence_lock(self):
        return common.presence_lock(self.lock_path, self.lock_wait_s)

    def params(self, metric_id):
        return defs.params(metric_id, self.mode)

    @staticmethod
    def week_bounds(week_from, week_to):
        import datetime as dt
        return common.ts_lit(common.monday(week_from)), common.ts_lit(common.monday(week_to) + dt.timedelta(days=7))

    # ── shared tables ────────────────────────────────────────────────────────
    def load_hostmap(self, con, name='hostmap'):
        return classify.load_hostmap(con, self.hostmap, name)

    def ca_keys_path(self):
        path = os.path.join(self.work, 'ca_keys.parquet')
        if not os.path.exists(path):
            t = time.time()
            con = self.connect()
            try:
                srcs = []
                for rir in common.REGISTRIES:
                    if self.has_table(rir, 'certs/is_ca=true'):
                        self.guard(f'warehouse-{rir} certs/is_ca=true')
                        srcs.append((rir, self.src(rir, 'certs/is_ca=true',
                                                   ['cert_id', 'ski', 'aki', 'not_before', 'sia', 'is_ca_flag'])))
                if srcs:
                    common.run(con, f"CREATE TEMP TABLE k AS {classify.ca_keys_sql(srcs)}")
                else:
                    con.execute("CREATE TEMP TABLE k (ski VARCHAR, host VARCHAR, aki VARCHAR, "
                                "newest_not_before TIMESTAMP, certs BIGINT, warehouses VARCHAR)")
                common.copy_to_parquet(con, 'SELECT * FROM k', path, order_by='ski')
                n = con.execute('SELECT count(*) FROM k').fetchone()[0]
            finally:
                con.close()
            log(f'ca_keys: {n:,} CA keys from {len(srcs)} warehouses [{time.time() - t:.1f}s]')
        return path

    def load_ca_keys(self, con, name='ca_keys'):
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM read_parquet({lit(self.ca_keys_path())})")
        return name

    def _ledger_file(self, name):
        path = os.path.join(self.ledger_dir, f'{name}.parquet')
        if not os.path.exists(path):
            raise FileNotFoundError(f'{path} missing: run `python -m scs.ledger` first')
        return path

    def load_ledger(self, con, name='ledger'):
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM read_parquet({lit(self._ledger_file('ledger'))})")
        return name

    def load_cycles(self, con, name='cycles'):
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM read_parquet({lit(self._ledger_file('cycles'))})")
        return name

    # ── calling modules and collecting their rows ────────────────────────────
    def call(self, module, rir, week_from, week_to):
        """Run module.compute(self, rir, week_from, week_to) and record what it emitted."""
        mod = import_module(module)
        wf, wt = common.monday(week_from), common.monday(week_to)
        if wt < wf:
            raise ValueError(f'{module}: week_to {wt} before week_from {wf}')
        self.guard(f'warehouse-{rir} ({module})')
        out = {'module': module, 'rir': rir, 'week_from': wf, 'week_to': wt, 'rows': 0, 'hist': 0, 'files': []}
        self._call = (module, rir, wf, wt, out)
        t = time.time()
        try:
            mod.compute(self, rir, wf, wt)
        finally:
            self._call = None
        out['seconds'] = round(time.time() - t, 1)
        self.outputs.append(out)
        log(f"{module}: {rir} {wf}..{wt} rows={out['rows']:,} hist={out['hist']:,} [{out['seconds']}s, "
            f"peak RSS {common.peak_rss_mb()} MB]")
        return out

    def emit(self, con, rows, hist=None):
        """Validate a module's rows (and histogram rows) and store them for the build (see the module
        docstring for the columns and the checks)."""
        if self._call is None:
            raise RuntimeError('Ctx.emit() outside Ctx.call()')
        module, rir, wf, wt, out = self._call
        for kind, query, cols, key in (('rows', rows, ROW_COLS, KEY), ('hist', hist, HIST_COLS, HIST_KEY)):
            if query is None:
                continue
            n, path = self._emit(con, kind, query, cols, key, module, rir, wf, wt, len(out['files']))
            out[kind] += n
            out['files'].append((kind, path))

    def _emit(self, con, kind, query, cols, key, module, rir, wf, wt, seq):
        query = query.strip()
        src = f'SELECT * FROM {query}' if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', query) else query
        names = [d[0] for d in con.execute(f'SELECT * FROM ({src}) LIMIT 0').description]
        want = [c for c, _ in cols]
        if sorted(names) != sorted(want):
            raise ValueError(f'{module} {kind}: columns {names} != {want}')
        rnd = {'value', 'top1_share', 'bin_lo', 'bin_hi'}
        casts = ', '.join((f'round(CAST("{c}" AS {t}), {ROUND})' if c in rnd else f'CAST("{c}" AS {t})') + f' AS "{c}"'
                          for c, t in cols)
        con.execute(f'CREATE OR REPLACE TEMP TABLE scs_emit AS SELECT {casts} FROM ({src})')
        problems = self._check(con, kind, key, module, rir, wf, wt)
        if problems:
            raise ValueError(f'{module} {kind} for {rir} {wf}..{wt}: ' + '; '.join(problems))
        n = con.execute('SELECT count(*) FROM scs_emit').fetchone()[0]
        path = os.path.join(self.work, 'fresh', f'{module}-{rir}-{wf}-{wt}-{seq:03d}-{kind}.parquet')
        common.copy_to_parquet(con, 'SELECT * FROM scs_emit', path, order_by=order_sql(key))
        con.execute('DROP TABLE scs_emit')
        return n, path

    def _check(self, con, kind, key, module, rir, wf, wt):
        own = set(module_metrics(module))
        p = []
        null_key = ' OR '.join(f'"{c}" IS NULL' for c in key)
        common_checks = (f"count(*) FILTER (WHERE rir <> {lit(rir)})",
                         f"count(*) FILTER (WHERE isodow(week) <> 1 OR week < {common.date_lit(wf)} "
                         f"OR week > {common.date_lit(wt)})",
                         f"count(*) FILTER (WHERE {null_key})",
                         f"count(*) - count(DISTINCT ({q(key)}))")
        if kind == 'rows':
            extra = ("count(*) FILTER (WHERE n IS NULL OR n < 0 OR n_ca IS NULL OR n_ca < 0)",
                     "count(*) FILTER (WHERE top1_share < 0 OR top1_share > 1 OR isnan(top1_share))",
                     "count(*) FILTER (WHERE isnan(value) OR isinf(value))")
            labels = ('n or n_ca NULL or negative', 'top1_share outside [0, 1]', 'value NaN or inf')
        else:
            extra = ("count(*) FILTER (WHERE count IS NULL OR count < 0)",
                     "count(*) FILTER (WHERE isnan(bin_lo) OR isinf(bin_lo) OR isnan(bin_hi) OR isinf(bin_hi) "
                     "OR bin_hi <= bin_lo)",
                     "0")
            labels = ('count NULL or negative', 'bin_lo/bin_hi NaN, inf or bin_hi <= bin_lo', '')
        vals = con.execute(f"SELECT {', '.join(common_checks + extra)} FROM scs_emit").fetchone()
        for v, label in zip(vals, (f'rir other than {rir}', f'week not a Monday in {wf}..{wt}',
                                   'NULL in a key column', 'duplicate keys') + labels):
            if v:
                p.append(f'{v} row(s): {label}')
        for m, w, s, pop, op in con.execute(
                'SELECT DISTINCT metric_id, "window", stat, population, operator FROM scs_emit ORDER BY ALL').fetchall():
            if m not in own:
                p.append(f'metric {m!r} does not belong to {module}')
                continue
            d = defs.METRICS[m]
            if kind == 'rows':
                if w not in d['windows']:
                    p.append(f'{m}: window {w!r} not in {list(d["windows"])}')
                if not defs.stat_allowed(m, s):
                    p.append(f'{m}: stat {s!r} not declared in defs')
            else:
                h = d['histograms'].get(s)
                if h is None:
                    p.append(f'{m}: histogram {s!r} not declared in defs')
                elif w not in h['windows']:
                    p.append(f'{m}: histogram {s!r} window {w!r} not in {list(h["windows"])}')
            if pop not in classify.POPULATIONS:
                p.append(f'unknown population {pop!r}')
            elif op and pop not in classify.OPERATOR_CLASSES:
                p.append(f'operator {op!r} on population {pop!r} (operators only on nir_hosted / rir_paas)')
            if len(p) > 20:
                break
        return p

    def emitted(self, kind='rows'):
        return [path for o in self.outputs for k, path in o['files'] if k == kind]

    def fetch(self, kind='rows'):
        """Everything emitted so far as a list of dicts, in key order (for tests and checks)."""
        files = self.emitted(kind)
        if not files:
            return []
        cols, key = (ROW_COLS, KEY) if kind == 'rows' else (HIST_COLS, HIST_KEY)
        con = duckdb.connect()
        try:
            cur = con.execute(f"SELECT {q(c for c, _ in cols)} FROM read_parquet({common.files_sql(files)}) "
                              f"ORDER BY {order_sql(key)}")
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]
        finally:
            con.close()
