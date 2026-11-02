# Country ROA deployment pipeline

Builds the data behind rpki.netsecurelab.org → Analysis → Countries: ROA deployment per RIR and
per registered country over time, under four perspectives (routes, routed address space,
registered address space, origin ASes), plus per-AS tables. The backend router
(`app/routers/country.py`) only reads the finished build under `ROOT/current`.

All code is Python + DuckDB (no pyarrow/numpy/pandas), run as modules from this directory with
the backend's virtual environment:

```
cd pipelines/country && $PY -m crc.<module> --help
```

The commands below write `$PY` for that interpreter: set it to the absolute path of the backend
venv's python (for example `PY=/path/to/rpki-backend/.venv/bin/python`) and run them from
`pipelines/country`. Cron may have no `python` on its PATH, and a system `python3` may have no
duckdb. `$SUBTAL_PY` is the python of the parser's venv (an rpki-parser checkout next to the backend
checkout, or `parser/` in the rpki-health repository), which has pyroaring; only `crc.vrp_subtal`
needs it.

`ROOT` is the data directory: `--root`, or `COUNTRY_RPKI_ROOT` (default `/ssd-data/country-rpki`).
The commands below write `$COUNTRY_RPKI_ROOT` for it, so export it in your shell first.

## Steps

| step | module | output | typical time |
|---|---|---|---|
| APNIC sub-TAL VRPs 2012-10..2018-04 (one-off, parser venv) | `crc.vrp_subtal` | `inputs/vrp_subtal.parquet` | 1.5 min, once |
| warehouse VRP day-intervals (holds `flock -s /tmp/rpki_presence.lock`) | `crc.vrp_export` | `inputs/warehouse_intervals.parquet` | ~3.5 min (145-230 s), 18 GB RSS |
| RIPE NCC archive roas.csv from 2026-04-08 | `crc.ripe_archive` | `inputs/archive_intervals.parquet` | seconds/day |
| union of the VRP sources | `crc.vrp_merge` | `inputs/vrp_intervals.parquet` | seconds |
| monthly delegation history 2012-01.. | `crc.deleg` | `inputs/deleg.parquet` | seconds/month |
| AS names | `crc.asnames` | `inputs/asnames.parquet` | seconds |
| BGP pairs of snapshot days (2026-10-25..) | `crc.snapshot_pairs` | `pairs/YYYY/pairs_D.{parquet,json}` | ~12 s/day |
| BGP pairs 2020-01-01..2026-10-24 (one-off on the BGP host) | `genji/bgp_country_pairs.py` | same layout | ~2 h weekly + 1.3 h daily 2026 |
| per-date aggregates | `crc.compute_day` | `daily/YYYY/D/{geo,as}.parquet, done.json` | ~10 s per BGP date, ~1 s before 2020 |
| build + publish | `crc.assemble` | `builds/<stamp>/`, `current` | seconds to a minute |

`run_daily.sh` runs all of it in that order (see below).

### Input steps (vrp_subtal, vrp_export, ripe_archive, vrp_merge, deleg, asnames)
All six are idempotent, write atomically (temp file + rename), log progress to stderr and exit
non-zero on failure. Downloads go through `crc/fetch.py` (urllib; User-Agent
`rpki-netsecurelab-country/1`; timeouts; retries with backoff; at most 4 parallel downloads;
existing files are skipped; xz/bz2/gz/zst files must decompress to the end before they are kept).
Gap bridging and gaps-and-islands SQL are shared in `crc/intervals.py`; vrp_export, vrp_subtal and
ripe_archive check before writing that bridged rows of one (RIR/TA, VRP) never overlap or touch and
that `bridged_days >= 0` (a violation stops the step, nothing is written).
- `vrp_subtal` needs pyroaring, which the backend venv does not have: run it with the parser's
  venv, `$SUBTAL_PY -m crc.vrp_subtal [--force]`
  (`run_daily.sh` uses `SUBTAL_PY`). Its output never changes and the step is skipped while it
  exists. Runs keep their gaps (they are not merged into one first..last row per VRP) and get the
  two-tier bridging; this matches RIPE's apnic-*.tal roas.csv exactly on 2016-06-01 (3,016 VRPs)
  and 2017-06-01 (10,026).
- `vrp_export` holds `flock -s /tmp/rpki_presence.lock` (waits up to 40 min), refuses to start
  (or to begin reading after the lock wait) 00:45-01:20 UTC so that it never reads 00:55-01:20,
  stops before the next warehouse if it is ever inside that window, and runs at nice 10 with 16
  threads / 16GB / spill to `ROOT/tmp` (DuckDB fills the limit: 18 GB peak RSS at 16GB, 34 GB at
  32GB). Its output is deterministic (two runs on the same presence build are
  byte-identical; CRLs ingested later in the day can add revocations, which only matter inside a
  bridged gap). A RIR whose presence lags the newest by > 12 h is logged as
  a WARNING (`stale_rirs` in the JSON). Rows are clipped
  to `last_complete_day` = (date of the latest presence-run end) - 1, so `last_day =
  last_complete_day` means "still present".
  - **Bridging per publication point.** Each ROA object is mapped to the host of its EE
    certificate's SIA (`table=object_signatures` -> `table=certs/is_ca=false`; `repo_id` is only
    the data source). Gaps of a VRP are closed at three levels, each only adding days:
    1. per (RIR, host): <= 3 days always; 4-31 days when every gap day is bad for the RIR, or when
       every gap day is bad for that host and the gap has no validity hole (below). A host day is
       bad when the RIR has no snapshot that day, or when n < 0.5 x ref (a dip) and ref >= 20 and
       the publication point is missing: the host dips that day in some RIR where its ref >= 150
       (`HOST_MIN_REF`; `HOST_PART_MIN_REF` = 20). n = the host's VRPs that day, ref = least(max n
       over the 31 days before, max n over the 31 days after). Only a dip far below the level seen
       both before and after counts, so ramps and permanent drops (migrations, real mass
       withdrawals) are never bad. The 150-VRP gate is set from the host calendar and independent
       validators: all 39 bridged episodes of national registries' repositories (TWNIC, JPNIC, IDNIC,
       CNNIC, CERNET; 2019-2026) have ref >= 167 on their gap days, while the 38 collapses of small
       delegated CAs that RPKIviews could check and found real (the repository was down for other
       validators too) all have ref <= 118. A host's parts in several RIRs count together, since
       one repository serves every trust anchor: rpki.admin.freerangecloud.com's ARIN part (131
       VRPs) is bridged on 2023-05-27..06-02 because its RIPE NCC part (208) vanished the same days
       (RPKIviews had all of them). This closes the archive's TWNIC, JPNIC and IDNIC dropouts, which
       are 3-8% of APNIC and invisible to the RIR calendar.
    2. per (RIR, registrable domain), for gaps between two DIFFERENT hosts of one domain: 4-31 days
       when every gap day is bad for the RIR, or for the domain (same rule on the domain's VRP count)
       without a validity hole. This covers a registry moving its repository to a new host, e.g.
       IDNIC rpki-ca.idnic.net -> repo-rpki.idnic.net with 17 days (2020-10-07..23) on which the
       archive had neither.
    3. per (RIR, VRP) over all hosts: the old RIR rule once more (<= 3 days, or 4-31 days all bad for
       the RIR), so a VRP that moves between hosts keeps at least its old continuity.
    Validity guard (levels 1 and 2, not the RIR rule): a gap is not closed by the host or domain
    rule when, on a whole UTC day of it on which the archive saw the host (n > 0), no object
    carrying the VRP was valid: every object present on the day before the gap had expired (EE
    not_after) or been revoked before that day, and every object present on the day after it began
    its EE validity (not_before) after that day. That is a ROA lapse, not a collection gap: CNNIC's
    EE certificates expired on 2021-04-29 and 2022-05-27 while the archive kept fetching
    rpki.cnnic.cn, and the replacements came on 2021-05-27 and 2022-05-31..06-07 (853 gaps). CRL
    revocations come from `table=crl_serials`, matched to the EE certificate by (issuer DN, serial);
    `revoked_cert_id` (and `table=ee_revocations`) is resolved for only a small part of them (ARIN:
    122,921 of 2,576,553 revoked ROA EE certificates). Days on which the host was not seen do not
    count: a CA that re-issues during a dropout leaves a hole only in the archive. Of the 205 gaps with
    such a hole, RPKIviews or the RIPE NCC archive had the VRP in 202 (freerangecloud 2023-05, CNNIC
    2022-01, r.magellan.ipxo.com 2026-04..06); 2 TWNIC VRPs of 2022-01-08..22 were really absent and
    stay bridged, 1 TWNIC VRP of 2021-10 has no reference. The RIR rule stays
    unguarded so that the RIR-only bridging remains a subset of the output: its 21,153 closures with
    a hole (`rir_tier2_validity_holes`) all lie in collection outages (2021-09, 2023-07-14..17,
    2024-12, 2026-05, ...) and all but 5 come from revocations of re-issued ROAs the archive never saw.
    `bridged_days` = days of the row on which no host had the VRP. The step refuses to write unless
    every (VRP, day) of the RIR-only bridging (the method before per-host bridging) is in the output.
    A warehouse without the two tables gets host `?` for every object (RIR rule only, WARNING,
    `host_fallback_rirs` in the JSON); one without `crl_serials` (or the certificates' issuer and
    serial) gets validity from the EE certificates only (WARNING, `validity_fallback_rirs`).
  - Also writes `inputs/warehouse_calendar.parquet` (RIR calendar: bad days per RIR, unchanged),
    `inputs/warehouse_host_calendar.parquet` (rir, host, d, n, ref, has_snap, bad),
    `inputs/warehouse_domain_calendar.parquet` (the same for domains with >= 2 hosts) and
    `inputs/warehouse_intervals.json` (per-host tier-2 counts `tier2_by_host` incl. the gaps the
    validity guard refused, domain closures `tier2_cross_host_by_domain`, VRP-days added per RIR
    `vrp_days_by_rir`, `rir_tier2_validity_holes`, revoked objects per RIR in `hosts_by_rir`,
    `superset_check`).
  - Not bridged by design: ROA re-issues whose new keys appear a day or more after the old ones
    vanish (MO 2025-02-19, PK 2020-04-22, CO 2026-03-19), ROA validity lapses (CNNIC 2021-05,
    2022-05/06), outages of publication points below 150 VRPs (also the few that were archive
    dropouts: rpki.co 2024-05, rsync.rp.ki 2026-01-24..27, rpki.nap.re 2024-02/04 per RPKIviews),
    gaps longer than 31 days, and VRPs absent on days their host is only partly missing. The last case remains for TWNIC in 2021-06..09: the
    archive lacks some member CAs (directories such as TWNICCA/TANET) on days when TWNIC is present,
    so TW registered space is still 87-92% instead of about 97.8% on several Wednesdays
    (44.6% on 2021-07-28).
- `ripe_archive` keeps the raw files under `inputs/ripe_archive/YYYY/MM/DD/<ta>.roas.csv.xz`
  (about 14 MB per day; 404 = not archived, e.g. 2026-06-13) and a parsed copy per day
  (`vrps.parquet`, rebuilt when a raw file is newer), then rebuilds all intervals (gaps <= 3 days
  bridged) in about 20 s / 8 GB RSS for 200 days. `--no-fetch` rebuilds from local files only.
- `vrp_merge` checks/canonicalises prefixes and writes `inputs/vrp_intervals.json` (per-source
  counts, VRPs on the latest day covered by both live sources).
- `deleg` remembers the chosen file(s) per month in `inputs/deleg_raw/manifest.json` (past months
  need no directory listing again), keeps raw files under `inputs/deleg_raw/<rir|nro>/` (.gz/.bz2
  as published, uncompressed sources as .zst) and parsed months in
  `inputs/deleg_months/YYYY-MM.parquet` (+ .json with counts and overlaps). Bump `PARSER_VERSION`
  in `crc/deleg.py` (or delete `inputs/deleg_months/`) to re-parse every month. NRO statuses
  `ietf`/`ianapool` (2019-2020 files) count as free; asdot ASNs (`5.1`) in old AFRINIC files are
  converted. RIPE NCC's extended files before 2012-02 have no cc on allocations, so 2012-01 uses
  RIPE's legacy file (`LEGACY_PREFERRED_BEFORE`); a file with > 1% delegated records without a
  cc is logged as a WARNING. A new month appears once its first file exists (NRO publishes
  around 11:00 UTC). A raw file that parses to no records, lacks the version line, differs from
  its declared record count by more than 1 record/1%, or (NRO) lacks one of the five RIRs is not
  used: the month is left out, the run exits 1 (also for the current month), and the file is
  moved aside as `<file>.bad` so that the next run downloads it again.
- `asnames` refreshes every run and refuses a download that parses to < 90% of the previous rows.

Measured on the production host (2026-10-27/28): backfill vrp_subtal 95 s, vrp_export 188-208 s with per-host
bridging and the validity guard (VERSION 3; 34 GB RSS at a 32GB limit; 18 GB at the default 16GB,
where the daily runs of 2026-10-28..11-02 took 145-230 s; 104-146 s / 12.9 GB without
the guard, 55 s / 7.8 GB with RIR-only bridging), ripe_archive 2026-04-08..10-27 12 min (2.77 GB
download), deleg 2012-01..2026-10 22 min (4.78 GB download, 0.96 GB kept on disk), asnames 35 s; a
daily rerun of all six steps takes about 3 min.

### snapshot_pairs
For day D the BGP host writes a snapshot at about 06:27 UTC on D+1. The module downloads
`https://rib.netsecurelab.org/api/v1/snapshots/D/files/{manifest.json,peers,routes,route_peers}.parquet`
with the `X-API-Key` header from `~/.config/bgp-warehouse.key` (never printed), checks bytes and
sha256 against the manifest, and recounts without the alias collectors (ix-br.gru, ix-br2.gru,
locix.fra): `n_peers` = peer rows, `n_collectors` = distinct non-alias collectors (bitmask
aggregate, identical to `count(DISTINCT collector)`), `n_ff` = full-feed peers of the family.
Only routable pairs are kept (IPv4 ≤ /24, IPv6 ≤ /48, origin not NULL, /0 dropped).
Raw files go to `ROOT/tmp/snap_raw/D/` and are deleted afterwards unless `--keep-raw`.
Network failures: each request is retried 5 times with backoff (connection errors, timeouts,
429/5xx, and bodies cut off before their Content-Length); a file that does not match the
manifest is deleted and the day is downloaded again from a fresh manifest (3 attempts). A partial
download never replaces a file (`<file>.part`, removed on failure), the pairs parquet is fsynced
before the json that marks the day present, and an unreachable API is one `ERROR` line (exit 1),
not a traceback. `--catch-up` logs a WARNING when yesterday's snapshot is not listed yet.
A day below the floors of `genji/bgp_country_pairs.py` (500k IPv4 / 50k IPv6 pairs, 10 collectors
per family; real days have about 1.29M / 318K and 75 / 74) is not written ("implausible
snapshot"), so a partly ingested snapshot can never become the published as_of; it is retried on
the next run.

```
$PY -m crc.snapshot_pairs --date 2026-10-26               # one day
$PY -m crc.snapshot_pairs --from 2026-10-25 --to 2026-10-31
$PY -m crc.snapshot_pairs --catch-up                      # every listed day without pairs (or regenerated upstream)
$PY -m crc.snapshot_pairs --list                          # what the API has
```

### compute_day
For grid date D: visible pairs (`n_collectors >= ceil(0.25 * collectors[af])`, IPv4 /8 to /24 and
IPv6 /16 to /48: `common.ROUTABLE_MIN_LEN` / `ROUTABLE_MAX_LEN`), RFC 6811 states
against the VRP set of D, registration by the delegation record containing the prefix start
(snapshot = latest monthly snap ≤ D), and the four lenses at three levels (`cc`, `registry`,
`all`). Routed space uses most-specific attribution over elementary intervals that are also cut
at delegation-record boundaries, so a route spanning two records is split exactly. Registered
coverage is the union of all VRP prefixes (with and without AS0) intersected with delegated
records; overlapping VRPs are never double counted. IPv4 in addresses, IPv6 in /48s (keys are /64
units). Dates before 2020-01-01, or BGP dates without a pairs file, get NULL BGP columns (a
warning is logged; `--only-missing`/`--auto` recompute them once the pairs file appears).
The pairs files keep announcements shorter than IPv4 /8 or IPv6 /16; compute_day leaves them out
as leaks and records how many met the visibility threshold in `done.json` (`inputs.short_pairs`,
also in its log line). In the whole history there are four, one visible pair each: 2a10::/12
(2020-01-15), 2000::/3 (2021-12-15), 96.0.0.0/3 (2025-01-08) and 2800::/6 (2026-03-14..16).
Counted, they collapse the routed-space share of every RIR whose space they cover (2800::/6 on
2026-03-14: All RIRs IPv6 67.0% → 3.4%, RIPE NCC 80.2% → 2.8%, LACNIC 66.3% → 0.8%). The floor
keeps the legacy IPv4 /8s, 2002::/16 and 2003::/19.

```
$PY -m crc.compute_day --date 2026-10-26
$PY -m crc.compute_day --from 2012-01-04 --to 2026-10-26 --jobs 4 --only-missing
$PY -m crc.compute_day --auto --trailing 32 --jobs 2      # what run_daily.sh does
```
`--threads` and `--memory-limit` are per process (defaults 16/jobs threads and 40GB/jobs);
`--tmp` sets the base of the per-connection DuckDB spill directories (default `ROOT/tmp`). Each
date is written into a temp directory and swapped in, so `daily/YYYY/D` is always complete.
`--jobs N` runs one subprocess per date (each logs one result line).

### assemble
Collects every grid date with a `done.json`, picks `as_of` L = latest grid date with BGP data
(`--as-of` to override), prev = latest grid date ≤ L − 365 days, and writes into
`builds/<YYYYMMDDTHHMMSSZ>.tmp/`: `series.parquet`, `countries.parquet`, `ases.parquet`,
`as_series.parquet`, `manifest.json` (method, lens, source and data-quality texts shown by the UI:
`METHOD`, `LENSES`, `SOURCES` and `NOTES` in `crc/assemble.py`; the page reads the method keys in
their order and lists the notes, one `{from, to, text}` per date range, by date).
The directory is renamed to `builds/<stamp>`, then `current` is swapped with
`ln -s builds/<stamp> current.new && mv -T current.new current` (symlink + rename). The newest 3
builds are kept (never the one `current` points to). Before the swap: no NaN/Inf, a gate that
refuses a build the site cannot show (no world rows for both families on as_of, no countries, no
AS rows on a BGP as_of) and fsync of the files. A failed build removes its `.tmp` directory and
leaves `current` alone. Rerunning on the same daily data gives byte-identical Parquet files.
Freshness warnings are logged as `assemble: WARNING ...` and listed in the manifest key
`build_warnings` and in the printed JSON (`run_daily.sh` turns them into PARTIAL): as_of older than
yesterday (UTC), grid dates ≤ as_of without daily data or computed by another crc `VERSION` (a
recompute after a bump that has not finished), recent BGP dates without pairs, a VRP source
of `inputs/vrp_intervals.json` that ends before as_of, a delegation snapshot > 45 days older.

## Data layout

```
ROOT/
  .lock                        flock of run_daily.sh
  inputs/                      vrp_intervals.parquet, deleg.parquet, asnames.parquet (+ raw downloads,
                               intermediate files of the input steps)
  pairs/YYYY/pairs_D.parquet   af, prefix, origin_as, n_peers, n_collectors, n_ff, src
  pairs/YYYY/pairs_D.json      date, src, collectors{4,6}, rows, rows_by_af, generated_at, extractor
  daily/YYYY/D/geo.parquet     date, level, key, af, routes.., space.., ases.., deleg.., vrps
  daily/YYYY/D/as.parquet      date, cc, af, asn, routes.., space..        (BGP dates only)
  daily/YYYY/D/done.json       date, inputs{pairs_src, vrp_rows, deleg_snap, ...}, rows, seconds, version
  builds/<stamp>/              manifest.json, series, countries, ases, as_series (.parquet)
  current -> builds/<stamp>
  logs/                        daily.log (cron)
  tmp/duckdb-<pid>-<n>/        DuckDB spill, one directory per connection (DuckDB removes it on close)
  tmp/snap_raw/D/              raw snapshot downloads (removed after success)
```
DuckDB spill files have fixed names and a closing DuckDB deletes every `duckdb_temp_*` file of its
temp directory, so processes must never share one spill directory: `common.connect` gives each
connection its own (two `--jobs` children spilling into one shared directory failed with
"Could not read enough bytes from file .../duckdb_temp_storage_DEFAULT-0.tmp").
Time grid: every Wednesday 2012-01-04..2025-12-31, then daily from 2026-01-01. BGP lenses from
2020-01-01.

## Daily run

`run_daily.sh [--root DIR] [--jobs N] [--trailing N]` (bash, `set -euo pipefail`,
`flock -n ROOT/.lock`, nice 10). The options override `COUNTRY_RPKI_ROOT`, `JOBS` (2) and
`TRAILING` (32); `PY` and `SUBTAL_PY` choose the interpreters. A relative root or interpreter path
is taken relative to the caller's directory (venv symlinks are kept). Steps:
0. remove leftovers of killed runs, only under this pipeline's own temp names: `daily/YYYY/.tmp-*`
   and `.old-*` and `pairs/YYYY/.tmp-*` older than 6 h, `tmp/snap_raw/D/` older than 3 days, and
   `tmp/duckdb-<pid>-<n>/` spill directories whose process is gone (nothing else under `ROOT/tmp`);
1. inputs: `vrp_subtal` (only if its output is missing; `SUBTAL_PY`, the parser's venv),
   `vrp_export`, `ripe_archive`, `vrp_merge`, `deleg`, `asnames`;
2. `snapshot_pairs --catch-up`;
3. `compute_day --auto --trailing 32 --jobs 2`: the trailing 32 grid dates up to the latest pairs
   day plus every grid date that is missing, computed without pairs that exist now, or from an
   older `VERSION`. VRP bridging closes gaps of up to 31 days only once the VRPs come back, and late
   RIPE NCC archive days fill in later, so the window is the longest bridged gap plus that day;
4. `assemble`;
5. one summary line on stdout: `<time> country-rpki daily: <STATUS> in <s>s[; failed: ...][; N
   warning(s)]; build: {"as_of", "build", "counts", "prev_date", "warnings"}`.

| status | exit | meaning |
|---|---|---|
| OK | 0 | published; every step succeeded and assemble reported no warning |
| PARTIAL | 1 | published, but an input refresh or `snapshot_pairs` failed (the previous files were used), or assemble reported freshness warnings |
| FAILED | 2 | `compute_day` or `assemble` failed, or the script was stopped; `current` is unchanged (the line names the step) |
| - | 75 | another run holds `ROOT/.lock`: one line on stderr, nothing done |
| - | 64 | bad option |

What happens when:
- the BGP host's snapshot of yesterday is late or missing: nothing new is fetched (`snapshot_pairs` warns
  "the snapshot of D is not listed yet"), as_of stays at the last snapshot day, assemble warns
  "as_of ... is N days old", the run is PARTIAL. The next run fetches every listed day it lacks
  (also days that appear late or were regenerated upstream) and recomputes them; a grid date whose
  snapshot never appears keeps NULL BGP columns (a gap in the BGP lines).
- the RIPE NCC archive day is late: `ripe_archive` stops at the latest day with all 5 TAs, so the
  VRP union lacks the archive (~12-14K APNIC/CERNET VRPs) on the newest days; assemble warns "VRP
  source ripe-archive ends on D", the run is PARTIAL, and the trailing recompute of a later run
  fixes those dates once the archive catches up.
- the network fails mid-download: see snapshot_pairs above; a day that still fails makes the run
  PARTIAL with the previous as_of, and the next run fetches it.
- a run was killed: files are only ever replaced by renames, so every file is complete; a date
  whose directory swap was interrupted has no done.json and is recomputed; step 0 of a later run
  removes the leftovers; a killed assemble leaves `current` on the previous build.
- a manual step still holds the lock when the daily run starts: the daily run exits 75 (see Backfill:
  run manual steps under the lock).

Schedule: once a day, after the BGP host's snapshot of the previous day (about 06:27 UTC) and the
RIPE NCC archive files (about 05:00-07:30 UTC) are available, and away from the parser's
presence build (00:55-01:20 UTC). Only exit 0 (OK) means a complete and fresh build, so alert on
any non-zero exit. A heartbeat monitor that is pinged only on exit 0 also works: a PARTIAL or
FAILED day then shows up as a missed ping. For example, at 07:45 UTC:
```
45 7 * * * /path/to/rpki-backend/pipelines/country/run_daily.sh >> /path/to/country-rpki/logs/daily.log 2>&1
```
The script has its own bash shebang, so cron's `/bin/sh` (dash) does not matter. Rotate the log
(for example weekly, with logrotate's `copytruncate`) so it does not grow forever.
`grep 'country-rpki daily:' daily.log` lists one line per run.

Measured end to end on the production host (2026-10-28 01:21 UTC, a scratch `--root` seeded with
copies of the real inputs and raw caches, one snapshot day to fetch): 170 s wall, peak RSS 8.0 GB
(vrp_export / ripe_archive), PARTIAL as expected at that hour (the BGP host's 10-27 snapshot is
written at ~06:27). Since the per-repository bridging and its validity guard (2026-10-28, VERSION 3)
vrp_export takes 2.5-4 minutes (145-230 s on 2026-10-28..11-02) and 18 GB peak RSS on the real
warehouses at its 16GB limit, about 2.5 minutes more than in the run below (the CRL tables it reads
hold 0.2-0.6 billion rows per RIR).
Steps: vrp_export 57 s, ripe_archive 20 s, vrp_merge 9 s, deleg < 1 s, asnames
35 s, snapshot_pairs 18 s per day, compute_day 29 s (14 dates, 2 with BGP data: 10 s each, 2.6 GB
RSS each), assemble 2 s. With the full history all 32 trailing dates have BGP data: about 10 s
each at 8 threads (2026-10-28: 9.9-11.0 s and 2.5-2.6 GB RSS for 2026-03-14..16 and 2026-10-26),
so about 3 minutes at `--jobs 2`. With assemble (about 4 s) and the 3.5-minute vrp_export a normal
production day takes about 8 minutes.

## Backfill

Run manual steps that write under `ROOT` (backfill, recompute, assemble) under the same lock as
the cron run, so the two never overlap: a cron run in the middle of a backfill would publish a
half-computed history, and two processes swapping the same `daily/YYYY/D` fail. Prefix the
commands below with `flock -n "$COUNTRY_RPKI_ROOT/.lock"` (refuse while the daily run is active)
or `flock "$COUNTRY_RPKI_ROOT/.lock"` (wait for it); a cron run that finds the lock taken skips
that day (exit 75).

1. Inputs (once): run the six input modules in the order above
   (`$SUBTAL_PY -m crc.vrp_subtal` for the sub-TAL step).
2. BGP pairs 2020-01-01..2026-10-24: `genji/bgp_country_pairs.py` on the BGP host (outside its
   05:55-06:50 UTC ingest window), output copied to `ROOT/pairs/YYYY/`; then
   `$PY -m crc.snapshot_pairs --catch-up`.
3. Per-date data for the whole grid (about 1,030 dates; ~613 with BGP data):
   ```
   flock -n "$COUNTRY_RPKI_ROOT/.lock" nice -n 10 $PY -m crc.compute_day --from 2012-01-04 \
       --to <latest pairs day> --jobs 4 --only-missing
   ```
   Measured on the production host: 8.8 s and 2.8 GB peak RSS per BGP date at 16 threads; with `--jobs 4`
   (4 threads each) 8 BGP dates took 30 s (about 3.8 s per date, 2.4 GB RSS per job), so the
   ~613 BGP dates take about 40 min and the ~420 earlier dates a few minutes. The VERSION 2
   recompute of the real root (2026-10-28, `--jobs 8 --threads 4 --memory-limit 12GB`, nice 10)
   took 1,221 s for all 1,030 dates: 9-19 s per BGP date (median 15 s), under 1.6 s before 2020,
   at most 2.5 GB RSS per job. The VERSION 3 recompute of a staging copy (same options) took 1,222 s
   for 1,031 dates (BGP dates 9.1-20.9 s, median 14.8 s; at most 2.45 GB RSS per job).
4. `$PY -m crc.assemble` (full history: about 4 s and 3 GB RSS).

## Recompute

(Under `flock "$COUNTRY_RPKI_ROOT/.lock"`, as for the backfill.)
- One date or a range: `$PY -m crc.compute_day --date D` / `--from A --to B [--jobs N]`, then
  `$PY -m crc.assemble`.
- Everything after a method change: bump `VERSION` in `crc/__init__.py`; `--auto` (or
  `--only-missing`) then treats every date as missing. The daily run would then recompute all
  ~1,030 grid dates at `--jobs 2`, so run that recompute by hand first (Backfill step 3, with more
  jobs) and then `assemble`. VERSION 2 (2026-10-28): the prefix-length floor and per-repository VRP
  bridging. VERSION 3 (2026-10-28): per-repository bridging only for publication points of >= 150
  VRPs and not across ROA validity lapses; run `vrp_export` and `vrp_merge` with the new code first.
- A BGP date that was computed before its pairs file existed is picked up automatically, and so is
  one whose pairs file was rewritten later (done.json records the pairs json `generated_at`).
- VRP inputs of past dates can change later: tier-2 bridging closes 4-31-day gaps only once the VRPs
  come back. `--auto` recomputes the trailing 32 grid dates (`TRAILING`), which covers the longest
  bridged gap plus the day the VRPs return. If daily runs were missed after an outage ended,
  recompute the outage window by hand (`--from A --to B`).
- Build an older view: `$PY -m crc.assemble --as-of 2026-06-30` (it still becomes `current`;
  run `assemble` again without `--as-of` to go back).
- A date whose files cannot be read (only after a power loss: compute_day renames but does not
  fsync, so XFS can leave empty files) makes every daily run FAILED: a JSONDecodeError in
  compute_day for an empty `done.json`, a DuckDB error naming the file for a parquet. Find them with
  `find ROOT/daily -size 0`, delete those `daily/YYYY/D` directories and rerun (`--auto` recomputes
  them).

## Checking a build

```
cat "$COUNTRY_RPKI_ROOT/current/manifest.json" | python3 -m json.tool | head -60
cd pipelines/country && $PY - "$COUNTRY_RPKI_ROOT/current" <<'EOF'
import duckdb, sys; c = duckdb.connect(); B = sys.argv[1]
print(c.sql(f"SELECT cc, af, registry, routes, round(routes_valid/routes, 4) v, round(space_valid/space, 4) sv,"
            f" round(deleg_cov/deleg, 4) reg, round(ases_any_valid/ases, 4) a FROM '{B}/countries.parquet'"
            f" WHERE cc IN ('US','KR','DE','BR','JP','IN','CN','ZA') ORDER BY cc, af"))
print(c.sql(f"SELECT key, af, round(routes_valid/routes, 4) FROM '{B}/series.parquet' WHERE level = 'all'"
            f" AND date = (SELECT max(date) FROM '{B}/series.parquet')"))
EOF
```
Expected on 2026-10-26 data: registered-space shares within about 0.001 of NRO economy adoption
(measured with the real merged inputs: US, KR, DE, JP, IN, CN, ZA within 0.0006 for both families,
CN IPv6 0.2342 exactly; BR +0.0007/+0.0035 because the reference is 2026-10-24),
routed IPv4 space covered within about 0.3 pp of RIPE world-roas, world IPv4 routes valid near
NIST's 71% (reference values read from these three sources on 2026-10-27). Also check:
`done.json` of the last dates (`pairs_src`, `vrp_rows`, `deleg_snap`), `readlink current`, and
`GET /api/analytics/country/meta` on the site.

## Tests

From the backend directory (`rpki-backend`, or `backend/` in the rpki-health repository):
```
.venv/bin/python -B -m pytest -q tests/test_country_pipeline_*.py
```
Synthetic worlds with hand-computed expected values for every lens and edge case (AS0, MOAS,
nested states, records split by a route, non-CIDR records, free/unknown space, IPv6 /48 units,
visibility threshold, the prefix-length floor, pre-2020 dates, assemble outputs and manifest
texts, symlink swap and retention), plus random worlds checked against a slow, literal
reimplementation of the compute_day rules (`tests/test_country_pipeline_compute.py`).
`test_country_pipeline_ops.py` covers operations: snapshot downloads against a local fake API with
cut-off bodies, 5xx, a regenerated file and an unreachable API; the plausibility floors; assemble
idempotency (byte-identical files), the publish gate, freshness warnings, publish/prune edge
cases; two processes spilling at once; and run_daily.sh with a stub interpreter (OK/PARTIAL/
FAILED/75 exit codes, relative root, venv symlink, stale-leftover cleanup, the trailing window).
