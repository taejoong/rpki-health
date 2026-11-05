# RPKI Operations Indicators

Weekly per-RIR operational metrics of the RPKI CA and repository services: CRL and manifest freshness,
revocation of replaced EE certificates, late re-issue, re-signing cadence, CA certificate lifetime,
relying-party rejections and the live inventory, per RIR and per publication class. Every value carries
its sample size, the number of CA keys behind it, the largest key's share, a status and the RFC
references of its definition. There are no grades. Supported by the ARIN Community Grant Program.

The pipeline's directory, files and settings keep the earlier working name `scorecard`.

Outputs stay internal (files under the data root); no API reads them yet.

All code is Python + DuckDB (no pyarrow/numpy/pandas), run as modules from this directory:

```
cd pipelines/scorecard && ../../.venv/bin/python -m scs.run --help     # also scs.ledger
```
(Only scs.run and scs.ledger have a command line; `-m scs.m_crl --help` etc. print nothing.)

The commands below write `$PY` for that interpreter and run from `pipelines/scorecard`. `ROOT` is
`/ssd-data/scorecard` (`--root`, or `SCORECARD_ROOT`); the warehouses are read from
`/ssd-data/warehouse-<rir>` (`--warehouse-base`, or `SCORECARD_WAREHOUSE_BASE`).

## Steps

| step | module | output | typical time |
|---|---|---|---|
| observation ledger (snapshot metadata, membership index names, verdict cycles; no presence table) | `scs.ledger` | `ledger/{ledger,cycles}.parquet`, `ledger.json` | 5-10 s, 1.1 GiB peak RSS (all history) |
| as_of, plan, metric modules, status, merge with state, publish, release | `scs.run` | `builds/<stamp>/`, `current`, `states/<stamp>/`, `state`, `releases/<YYYY-Www>/` | all ten metrics, 6 weeks: 5-6 min, 6.9-7.1 GiB peak RSS; all history (774 weeks): 15.5 min, 7.7 GiB |

`run_weekly.sh` runs both (see Weekly run). Inside `scs.run`:
1. **as_of** = the earliest of the five RIRs' latest `object_presence_runs` end, read under a shared
   `flock` on `/tmp/rpki_presence.lock` (released right after). W = the Monday of as_of's ISO week.
   `scs.run` stops when the ledger ends before as_of (`--allow-stale-ledger` runs anyway, with a
   warning): the ledger's later days would count as gaps, and the weeks the run closes would be frozen
   as masked. A weekly run without `--as-of` whose as_of is before this week's Monday warns: the week that just ended is
   not complete in the presence tables, so the newest week would repeat last week's.
2. **plan** (`scs/assemble.py`): per metric, the trailing `--weeks` weeks before W (default 6), every
   week that is not closed yet (missed runs), the whole history after a definition change, and
   `--backfill FROM TO`.
3. **metric modules** `scs/m_*.py`, once per RIR and contiguous range of weeks (one grouped pass, never a
   loop over weeks). The interface is documented in `scs/metrics.py`; the definitions (stats, windows,
   parameters, status rules, RFC anchors, method texts) are registered in `scs/defs.py`.
4. **status** (`scs/status.py`), **merge** with the frozen rows of `state/`, **publish gate**,
   atomic **publish** of `builds/<stamp>` and `current`, the new **state**, and (weekly runs only) an
   immutable **release**.

Warehouse reads follow the discipline of the country pipeline's VRP export: explicit lists of regular
`*.parquet` parts older than 180 s (the `<table>.parquet` symlink and parts being written are skipped;
only the presence build writes presence parts, under its exclusive lock, so a younger presence part seen
while the shared lock is held stops the run instead of being skipped), `union_by_name`, no read starts in
[00:45, 01:20) UTC (the 01:00 presence build), the presence lock is shared and held only while presence
tables are read, nice 10, DuckDB threads 4 and memory_limit 6GB (5.6 GiB; a run's peak RSS is 6.9-7.7
GiB, 24-38% above the limit, reached in m_mft on RIPE NCC), one spill directory per connection under
`ROOT/tmp` with at most 300GB of spill each (`SCORECARD_MAX_SPILL`, e.g. `500GB`; DuckDB's default would
be 90% of the free disk), at most 10 minutes per statement that reads the warehouses (`common.run`) and
3 hours for `scs.run` in `run_weekly.sh`.
Nothing is written under the warehouses.

## Layout

```
ROOT/
  .lock                       flock of run_weekly.sh
  ledger/ledger.parquet       per RIR and UTC day: cycles, good cycles, drops, longest gap, verdict cycles, low, gap
  ledger/cycles.parquet       per snapshot: ts, source, sid, file_count, references, partial, good, verdict rows
  builds/<stamp>/             scorecard.parquet + .csv, histograms.parquet, ledger.parquet, annotations.csv,
                              manifest.json
  current -> builds/<stamp>   (the newest 3 builds are kept, never the current one)
  states/<stamp>/             frozen final rows (scorecard.parquet, histograms.parquet) + state.json
  state -> states/<stamp>     (keep 3)
  releases/<YYYY-Www>/        immutable copy of a weekly build, its scorecard.csv gzipped (scorecard.csv.gz;
                              earlier releases hold scorecard.csv), + CHANGES.csv + release.json
  logs/weekly.log             cron log
  tmp/                        DuckDB spill (duckdb-<pid>-<n>/), per-run work directories (run-<pid>-<t>/)
```

`scorecard.parquet` (long format, one row per week, window, rir, population, operator, metric_id,
stat): `value, n, n_ca, top1_share, status, era, flags, as_of, def_version, hostmap_version,
code_commit, frozen`. `window` is '1w', '13w', '52w' or 'end' (point in time at the week's end); it is a
reserved word in DuckDB, so write `"window"` in SQL. `histograms.parquet`: week, window, rir,
population, operator, metric_id, stat, bin_lo, bin_hi (NULL = overflow bin), count.

## Weekly run

`run_weekly.sh [--root DIR] [--weeks N] [--backfill FROM TO] [--metrics LIST]` (bash,
`set -euo pipefail`, `flock -n ROOT/.lock`, nice 10). The options override `SCORECARD_ROOT`,
`WEEKS` (6) and `METRICS`; `PY` chooses the interpreter. A relative root or interpreter path is taken
relative to the caller's directory (venv symlinks are kept). Steps:
0. remove leftovers of killed runs (DuckDB spill and run directories of processes that are gone,
   release temp directories and ledger CSVs older than 6 h);
1. `scs.ledger` (a failure is FAILED: the previous ledger ends before the new as_of, which `scs.run`
   refuses);
2. `scs.run --weeks N [--backfill FROM TO] [--metrics LIST]`, at most 3 hours (`timeout --foreground
   --signal=TERM 3h`; a hung run would hold `ROOT/.lock` and make the following weeks exit 75);
3. one summary line on stdout: `<time> scorecard weekly: <STATUS> in <s>s[; N warning(s)]; build:
   {"as_of", "build", "by_status", "metrics", "mode", "newest_week", "release", "rows", "warnings",
   "weeks"}`.

| status | exit | meaning |
|---|---|---|
| OK | 0 | published; every step succeeded and no warning |
| PARTIAL | 1 | published, but the run reported warnings: a RIR's presence ends more than 12 h before the others; as_of more than 36 h old, or (weekly runs) before this week's Monday; no state although a previous build exists; a metric module file is missing; `--allow-stale-ledger` with a ledger that ends before as_of; old builds not pruned, state or release not written after the publish |
| FAILED | 2 | `scs.ledger` or `scs.run` failed or ran out of time, or the script stopped (also before the lock is taken); the line names the step and says whether `current` is unchanged or changed (then the state or the release may be missing) |
| - | 75 | another run holds `ROOT/.lock`: one line on stderr, nothing done |
| - | 64 | bad option |

Schedule: Monday 05:00 UTC, after the Monday 01:00 presence build (which completes Sunday) and far from
the 00:45-01:20 window. The heartbeat goes only on exit 0, so a PARTIAL or FAILED week shows up as a
missed heartbeat:
```
0 5 * * 1 /path/to/rpki-backend/pipelines/scorecard/run_weekly.sh >> /ssd-data/scorecard/logs/weekly.log 2>&1 && curl -sf -X POST "$HB/rpki-scorecard-weekly?host=$(hostname)" || true
```
The script has its own bash shebang, so cron's `/bin/sh` does not matter. `grep 'scorecard weekly:'
weekly.log` lists one line per run. Add the log to the logrotate list.

## Backfill

`run_weekly.sh` takes `ROOT/.lock` itself, so run it directly; wrapping it in `flock` on the same file
makes it exit 75 at once. Only `python -m scs.ledger` or `scs.run` started by hand need
`flock -n ROOT/.lock`; run `scs.ledger` first, since `scs.run` refuses a ledger that ends before as_of.
```
./run_weekly.sh --backfill 2011-12-26 <last Monday>
```
`--backfill FROM TO` computes FROM..TO (plus the trailing weeks) in one grouped pass per module and RIR,
publishes a build and freezes every week that is final; it makes no release (the next weekly release
lists the new rows as added). 2011-12-26 is the ISO week of the first archive snapshot (2012-01-01).
Backfilled weeks carry the as_of of the backfill: they were computed with full look-ahead, unlike the
values a weekly run showed at the time. Measured 2026-11-01 on the production warehouses (all ten
metrics, 2011-12-26..2026-10-19 = 774 weeks, five RIRs): 940 s end to end; scs.run 932 s (m_crl 219 s,
m_pairs 218 s, m_mft 374 s, m_ca 76 s, m_health 39 s, assemble 6 s), peak RSS 7.7 GiB (m_mft on RIPE
NCC); the cross-warehouse CA key table takes 6-7 s per run. A weekly run (6 weeks) takes 5-6 minutes
(296 s and 334 s for scs.run on 2026-11-01 and 2026-11-02) at 6.9-7.1 GiB peak RSS. (The log's "MB" are
MiB: 7879.4, 7071.8 and 7302.7.)

## Recompute

- A definition change: bump the metric's `def_version` in `scs/defs.py` with a dated note. The next run
  discards its frozen rows and recomputes its whole history (closed weeks and the previous build's
  weeks); other metrics keep their frozen rows. A change of `refdata/hostmap.json` (its
  `hostmap_version`, or any host, RIR, class or operator; not its evidence texts) or of the framework
  `VERSION` (`scs/__init__.py`) does the same for every metric computed. Run that recompute by hand first if it is large (Backfill), then let cron go on.
- One metric only: `run_weekly.sh --metrics <id>` (metrics not named keep their rows).
- Missed weekly runs need nothing: every week that is not closed yet is recomputed.
- A build that lost weeks against the previous one is refused by the publish gate; `scs.run
  --allow-shrink` publishes it anyway (e.g. after deliberately retiring weeks).
- A state frozen with a stale ledger (`scs.run --allow-stale-ledger` with a ledger that ends before
  as_of): the weeks that run closed are frozen as masked, and later runs keep them. Repair it with a
  framework `VERSION` bump (every metric recomputes its history), or point `state` back at the
  `states/<stamp>` from before that run (the newest 3 are kept) and run again.
- A `state` link whose target is gone stops `scs.run`: restore the target, or remove the link to rebuild
  the state from scratch (every metric then recomputes and freezes its whole history, with a warning
  while a previous build exists).

## Checking a build

```
cat /ssd-data/scorecard/current/manifest.json | python3 -m json.tool | head -80
$PY - <<'EOF'
import duckdb; c = duckdb.connect(); B = '/ssd-data/scorecard/current'
c.sql("SET TimeZone='UTC'")
print(c.sql(f"""SELECT metric_id, status, count(*) FROM '{B}/scorecard.parquet' GROUP BY ALL ORDER BY ALL"""))
print(c.sql(f"""SELECT rir, week, stat, value, n, n_ca, top1_share, status, flags FROM '{B}/scorecard.parquet'
               WHERE population = 'rir_hosted' AND "window" = '1w' AND week = (SELECT max(week) FROM '{B}/scorecard.parquet')
               ORDER BY ALL LIMIT 40"""))
EOF
grep 'scorecard weekly:' /ssd-data/scorecard/logs/weekly.log | tail -3
ls /ssd-data/scorecard/releases/ && head /ssd-data/scorecard/releases/<YYYY-Www>/CHANGES.csv
```
Also check `readlink current state`, the manifest's `build_warnings`, `inputs.presence_end_by_rir`,
`ledger.cut` (after as_of), and that the newest week has rows for all five RIRs (the gate refuses a
build without).

## Tests

```
cd backend && .venv/bin/python -B -m pytest -q tests/test_scorecard_*.py
```
`tests/scorecard_world.py` writes synthetic warehouses (Parquet parts with the `<table>.parquet`
symlink, snapshot and membership directories) and makes a `metrics.Ctx` for them.
`test_scorecard_framework.py`: registry and host map, SQL classification against the Python one,
CA keys across warehouses, part-file listing and the presence-lock and time-window rules (a young
presence part under the lock is an error), the connection limits, `git` without index writes, the
ledger on a synthetic snapshot directory (partial ingest, missing day, outage, gap > 3 h, near-empty and
partial cycles, missing index, verdict cycles, sid), status rules (W+1/W+2/W+6, masks, the verdict mask
of an outage week, insufficient, census, era, flags), the module-interface checks, frozen state with
trailing recompute, missed runs, a def_version change, carried metrics, idempotent assemble, the publish
gate, a prune failure after the publish, releases with CHANGES.csv and the gzipped CSV, the stub metric
end to end through `scs.run`, and `scs.run`'s checks (a ledger that ends before as_of, as_of before this
week's Monday, a dangling `state` link, no state beside a previous build). One file per metric module:
`test_scorecard_crl.py` (m_crl: band shares, nearest-rank quantiles and CA weighting, the window
histogram, manifest text times and keys, a manifest without a key, crl_empty, the interpolating
quantile option, one grouped pass equal to week-by-week computation, end to end with status),
`test_scorecard_pairs.py` (m_pairs: early pairs and concentration, revocation within 7 and 35 days,
rows in every week, validity consumed in 1w/13w/52w windows, bounds and histogram, the transient
rule in era 2 and at the archive end, the as_of cut, range independence, status rules, chunking),
`test_scorecard_mft.py` (m_mft: mft_ee_revocation and late_reissue values, the as_of bound, the
outage mask, era 2 only, presence and ledger numbering, range independence, determinism),
`test_scorecard_ca.py` (m_ca and m_health: CA certificate lifetime by issuer class, rolling windows,
the 'end' window, the live inventory by host, the week's instant, rp_rejected by type, reason and
population, zero rows, the verdict start, status). `test_scorecard_ops.py`: `run_weekly.sh` with a
stub interpreter (OK/PARTIAL/FAILED/75/64, a timeout, `current` changed before a failure, failures
before the lock, arguments, relative root, venv symlink, cleanup).

## Method

The manifest carries the method text of every metric (`definitions`) and the general notes
(`method`). In short:

- **Periods.** ISO weeks in UTC (Monday 00:00 to Monday 00:00); `week` is the Monday. Rolling windows
  of 13 and 52 weeks end with the week; 'end' is the state at the week's end.
- **Populations** (`refdata/hostmap.json`, exact host names, versioned): per RIR, `rir_hosted` (the
  headline: the RIR's hosted CA service), `rir_paas` (its publication service for member-run CAs),
  `nir_hosted` (National Internet Registry platforms, also per operator), `delegated` (any other host),
  `unknown` (no resolvable host) and `all`. Signed-object metrics are classified by the host of the
  (predecessor's) signer EE certificate, CRL and manifest metrics by the CA's own publication host, CA
  lifetime by the issuer's class. In era 2 a warehouse holds objects by publication host, so `all` is
  "published on hosts filed under this RIR", not the trust anchor's whole tree; RIR-hosted hosts are
  entirely under their own trust anchor.
- **Status.** `provisional` (may change), `final` (frozen), `masked` (our collection was incomplete for
  the period, per the observation ledger; values shown, not to be used), `insufficient` (sample
  metrics only: n < 30 or fewer than 10 CA keys; values shown; the censuses rp_rejected and
  live_inventory are never insufficient). A row of week W is published by the Monday run
  `publish_after` weeks after W and is final `final_after` weeks after W (the `rules` of each metric in
  `scs/defs.py`, also listed in the manifest): crl_window, mft_window and crl_empty W+1/W+1;
  resign_consumed and ca_cert_lifetime W+1/W+2; roa_ee_revocation W+2/W+6 for its 35-day-horizon
  statistics (`*_h35*`) and W+2/W+2 for all its other statistics; mft_ee_revocation W+1/W+2 for its
  shares (revoked, revoked_ca, ca_always, ca_never) and W+1/W+1 for early_replacements; late_reissue
  W+1/W+1; rp_rejected W+1/W+1; live_inventory W+1/W+1.
- **Observation ledger.** Era 1 (until 2026-04-07): one RIPE NCC archive snapshot a day; a day without
  a good snapshot (none, or one with less than half the median file count of its +-7 days) is a gap
  day. Era 2 (from 2026-04-08): our rpki-client cycles every 20 minutes; a cycle is good with at least a
  quarter of the +-7-day median file count; a day is low below 90% of 72 good cycles and a gap day when
  more than 3 hours pass without a good cycle. In both eras a partial snapshot (below 0.9 x the
  surrounding median: a repository missing) still counts as an observation; a near-empty one does not.
- **Annotations** (`refdata/annotations.csv`): archive holes, our outages and degraded cycles, era
  transition, platform changes and software versions; their ids appear in `flags` of every row whose
  period they overlap.
- **RFC anchors** (per metric in `scs/defs.py`): RFC 9286 §4.2.1, §5.1, §5.2, §6.3, §6.6 (manifests,
  replacement and revocation, staleness), RFC 6487 §4, §5 (certificate and CRL profiles), RFC 6489 (key
  rollover), RFC 9582 (ROAs), RFC 9589 (CMS signing time).
- **Caveats.** Era-2 collection sees every re-issue and keeps the full cache, so issuance-weighted values
  and populations differ from era 1; RP verdicts exist only from 2026-06-20, and a cycle without any
  rejected object writes no verdict row. The ledger judges a cycle by its file count, so a cycle that
  ran but fetched nothing new (rpki-client only re-listed its cache, as from 2026-10-09 14:00 to 10-12
  12:20 UTC) counts as good: the masks do not see such a stretch (rp_rejected of APNIC's week 2026-10-12
  is final), while m_mft leaves those cycles out by itself. Other published definitions of these
  metrics (e.g. calendar years in a given time zone) are not computed: the parameter overrides of the
  `compare` mode (`COMPARE_MODE` in `scs/defs.py`) are empty in this release.

One subsection per metric module follows (definitions, populations, status, caveats and measured
costs; the method paragraphs that go into the manifest live in `scs/defs.py`, the details in each
module's docstring). Every module emits rows for population `all` in every week of the range from the
metric's first week on, also when there is nothing (n = 0, NULL values; for rp_rejected the `objects`
and `ca_keys` rows), so a silent read failure cannot pass as an empty week; other populations have
rows only where they have objects.
Costs: production warehouses, threads 4, memory_limit 6GB; "weekly" = the cron run of 2026-11-02
(6 weeks, five RIRs), "all history" = the backfill of 2026-11-01 (774 weeks, five RIRs). The peak RSS
is that of the whole process so far (the log's "MB" are MiB; given here in GiB).

### CRL and manifest windows, empty CRLs (`scs/m_crl.py`: crl_window, mft_window, crl_empty)

- **Objects.** The CRLs (`table=crls`) and manifests (`table=manifests`) whose thisUpdate falls in the
  ISO week (UTC), one row per object we observed: every re-issue counts (era 2: a cycle every 20
  minutes; era 1: one archive snapshot a day). Window = nextUpdate - thisUpdate in whole seconds,
  reported in hours. A CRL without a nextUpdate has no window and counts in crl_empty only; manifest
  times are stored as text and cast.
- **crl_window, mft_window** (window `1w`, sample): `share_le24`, `share_le48`, `share_le72`,
  `share_le168` (cumulative shares of objects with a window of at most b hours, exactly: a window a few
  seconds over 24 h counts above 24), `share_gt168`; `p50`, `p90` (nearest rank, i.e. an observed
  window: a 48/72-hour mix gives 48 or 72, never 60), `max`; CA-weighted `p50_ca` (median over issuing
  keys of each key's median) and `share_le{24,48,72,168}_ca`, `share_gt168_ca` (mean over keys of each
  key's own share). Histogram `window_h`: 1-hour bins from 0 to 200 h, then an overflow bin [200, NULL).
- **crl_empty** (window `1w`, sample): `share_empty` (CRLs with revoked_count = 0), `share_empty_ca`
  (share of keys all of whose CRLs in the week were empty).
- **Keys and populations.** Issuing key: `crls.aki`; for a manifest, the aki of its EE certificate
  (`object_signatures.signer_cert_id` -> `certs/is_ca=false`). Population by the CA's own publication
  host: the SIA host of the key's newest CA certificate in any of the five warehouses, else the SIA host
  of its newest manifest EE certificate; a manifest whose EE certificate is not in the warehouse has no
  key (APNIC: 6,661 manifests of 2017-18) and counts in n and in population `unknown`, not in n_ca.
- **Status.** Published and final at W+1. Masked when the week's era-2 days have fewer than 90% of the
  expected good cycles (72 a day) or one of its era-1 days has no good archive snapshot.
- **Caveats.** Read the band shares; the median is a label: ARIN's hosted CRLs mix 48- and 72-hour
  windows in about equal numbers, so its weekly p50 flips between the two. Era 2 counts every re-issue
  and era 1 one object a day, so issuance weighting differs between the eras. An empty CRL is not an
  error by itself; read crl_empty together with mft_ee_revocation.
- **Cost.** Weekly: 52 s (7-14 s per RIR; the first call also builds the 6-second cross-warehouse CA key
  table), 3.2 GiB peak RSS. All history: 219 s (AFRINIC 15 s, APNIC 41 s, ARIN 41 s, LACNIC 42 s, RIPE
  NCC 81 s: 64 million CRLs and the manifest join, 7.0 GiB peak RSS).

### Re-sign pairs: ROA-EE revocation and validity consumed (`scs/m_pairs.py`: roa_ee_revocation, resign_consumed)

- **Pairs.** The ROA objects of one semantic ROA (`semantic_hash`: the same ASN and prefix set) that
  have a presence row, ordered by coalesce(signing_time, ee_not_before), then object_id; a pair is two
  consecutive objects, unless the successor was first seen more than 30 days after the predecessor was
  last seen (the chain breaks). A pair counts in the ISO week of the successor's signing time. Key and
  population: the aki and the SIA host of the predecessor's signer EE certificate. Input cut: only
  objects first seen by as_of, presence capped at as_of, evidence CRLs with thisUpdate <= as_of, so a
  rerun with the same as_of gives the same rows.
- **roa_ee_revocation** (window `1w`, sample): `pairs` and `early_pairs` (counts; a pair is early when
  the predecessor's EE certificate is valid for more than 7 more days at the successor's signing time).
  For H = 7 and H = 35 days: `revoked_hH` (share of the early pairs with evidence whose predecessor EE
  serial is listed on a CRL of the predecessor's issuing key with thisUpdate from 1 day before to H days
  after the successor's signing time), `revoked_hH_ca` (mean of the per-key shares), `no_evidence_hH`
  (share of early pairs without any CRL of that key in the window; they are left out of the revoked
  shares), and the strata `revoked_hH_samekey` / `_keychange` (successor signed by the same or by
  another key) and `revoked_hH_withdrawn` / `_kept` (predecessor gone within a day of the successor
  appearing, or kept). n is the share's denominator.
- **resign_consumed** (windows `1w`, `13w`, `52w`, sample): consumed = (successor signing time -
  predecessor EE notBefore) / (predecessor EE notAfter - notBefore), for pairs with notAfter > notBefore
  and consumed in [0, 1.5]; interval = successor signing time - predecessor signing time, in days. Pairs
  whose predecessor was never present at a daily sampling instant (04:00 UTC, or an archive snapshot up
  to 2026-04-14) are transient and left out. Statistics: `pairs`, `consumed_p10`, `consumed_p50`,
  `consumed_p90` (interpolated, quantile_cont), `consumed_p50_ca`, `share_ge075`, `share_gt100`
  (re-signed after the predecessor had expired), `interval_p10_d`, `interval_p50_d`, `interval_p90_d`,
  and from the ISO week of 2026-04-08 on `pairs_incl_transient`, `consumed_p50_incl_transient`,
  `share_ge075_incl_transient`. The 13w and 52w rows pool the pairs of the 13 or 52 weeks that end with
  W. Histogram `consumed` (1w): bins of 0.01 from 0 to 1.5.
- **Status.** roa_ee_revocation: the 35-day statistics (`*_h35*`) are published at W+2 and final at
  W+6, every other statistic W+2/W+2; masked by a ledger gap day in [W, W + 14 days), for the 35-day
  statistics in [W, W + 42 days). resign_consumed: W+1/W+2, masked by a gap day in [W, W + 14 days).
- **Caveats.** RFC 9286 §5.2 asks a CA to revoke the EE certificate of a replaced object that has not
  expired; a key rollover (RFC 6489) retires the old key instead, hence the samekey/keychange strata.
  The 7- and 35-day horizons bound the evidence; a share pooled over every CRL ever stored is a
  different, unbounded quantity. Weekly resign_consumed values follow each CA's schedule (RIPE NCC's
  hosted CA re-signs in one January batch), so the 13- and 52-week windows are the stable signal,
  except that a 52-week window that contains no 1 January (the weeks of 2012-12-24, 2017-12-25 and
  2023-12-25; next 2028-12-25) holds no January batch, and its values shift for that one week.
- **Cost.** Weekly: 69 s (ARIN 22 s for 1.19 million pairs of the 52-week look-back), 3.9 GiB peak RSS.
  All history: 218 s (ARIN 86 s: 2.63 million pairs, 283 million window rows in 26 chunks).

### Replacements: manifest-EE revocation and late re-issue (`scs/m_mft.py`: mft_ee_revocation, late_reissue)

- **Replacements.** Era 2 only: values from the week of 2026-04-20. The manifests of one CA key (the
  aki of the manifest EE certificate) in manifestNumber order, then thisUpdate; the CRLs of one key
  (`crls.aki`) in crlNumber order, then thisUpdate. Two consecutive objects are a replacement, counted
  in the ISO week of the successor's thisUpdate. Only directly observed replacements count: the
  successor was first seen in the cycle right after the predecessor's last cycle, both cycles are good
  in the ledger and fetched something (some warehouse got a new object in them), they are at most 3 h
  apart, and the successor was first seen by as_of.
- **mft_ee_revocation** (window `1w`, sample): `early_replacements` (successor issued before the
  predecessor's nextUpdate; count), `revoked` (share of them whose predecessor EE serial is on a CRL of
  the same key with thisUpdate from 1 day before to 2 days after the successor's thisUpdate, and not
  after as_of; no such CRL counts as not revoked), `revoked_ca` (mean of the per-key shares),
  `ca_always` and `ca_never` (shares of keys that revoked every, or none, of their replaced manifest EE
  certificates in the week).
- **late_reissue** (window `1w`, sample): `replacements_mft`, `replacements_crl` (counts); `late_mft`,
  `late_crl` (share of replacements whose successor thisUpdate is after the predecessor's nextUpdate);
  `late_mft_ca`, `late_crl_ca` (share of keys with at least one late replacement);
  `headroom_mft_p50_h`, `headroom_crl_p50_h` (median of predecessor nextUpdate - successor thisUpdate,
  in hours; negative when late).
- **Populations.** The CA's own publication host, as for m_crl.
- **Status.** late_reissue and `early_replacements`: W+1/W+1, masked by a ledger gap day in the week.
  The mft_ee_revocation shares (`revoked`, `revoked_ca`, `ca_always`, `ca_never`): published at W+1
  and final at W+2 (they wait for the 2-day evidence window), masked by a gap day in [W, W + 9 days).
- **Caveats.** Across our own collection outages and across cycles that only re-listed the cache (no
  warehouse got a new object from 2026-10-09 14:00 to 10-12 12:20 UTC) the intermediate objects were
  never captured; those replacements are left out instead of being read as late. Daily archive
  snapshots cannot observe replacements, hence no era-1 values.
- **Cost.** Weekly: 120 s (RIPE NCC 42 s); it sets the run's peak RSS, 7.1 GiB. All history: 374 s
  (LACNIC 137 s, RIPE NCC 128 s), 7.7 GiB peak RSS.

### CA certificates and the live inventory (`scs/m_ca.py`: ca_cert_lifetime, live_inventory)

- **ca_cert_lifetime** (windows `1w`, `13w`, `52w`, `end`, sample): newly issued non-root CA
  certificates of the RIR's warehouse (CA flag set, aki present and different from ski, notAfter after
  notBefore, notBefore in the window, observed by as_of), keeping the latest certificate per key (ski)
  in each week. `certs` (count), `duration_p10_d`, `duration_p50_d`, `duration_p90_d` (notAfter -
  notBefore in days, observed values: quantile_disc), `modal_not_after` (the most common notAfter date
  as the number YYYYMMDD; ties: the earliest) and `modal_not_after_share`. Window `end`: `live_certs`
  (non-root CA certificates present at the week's instant) and `live_expiring_30d`,
  `live_expiring_90d` (shares of them whose notAfter is within 30 or 90 days of the instant, already
  expired ones included). Histogram `duration_d` (1w): 7-day bins up to 4,200 days plus an overflow bin.
  Population: the issuer's class, the publication host of the parent key (the child's aki); the
  headline rir_hosted is what the RIR's own CA issued. n_ca counts the certified keys (ski), not the
  issuing keys.
- **live_inventory** (window `end`, census): `ca_keys` (non-root CA keys with a certificate present at
  the instant, by the key's own publication host; value = n = n_ca) and `roas` (ROA objects present at
  the instant, by the host of their signer EE certificate; n_ca = their issuing keys).
- **The week's instant.** The week's last snapshot that the ledger marks good, that is not partial and
  that holds at least 0.98 x the median file count of its +-7 days (else the last good one; none: value
  NULL): normally the Sunday 23:40 cycle in era 2, the archive's Sunday tree in era 1. Present = a
  presence run of the RIR's warehouse spanning the instant.
- **Status.** ca_cert_lifetime: W+1/W+2, masked by a ledger gap day in [W, W + 14 days).
  live_inventory: W+1/W+1, masked by a gap on the week's last day.
- **Caveats.** Issuers with one fixed expiry date (AFRINIC: 2027-03-31; RIPE NCC hosted: 2027-07-01)
  make the 1-week durations fall by about 7 days a week; use the 52-week window, which is close to a
  calendar year but in UTC ISO weeks. These issuers re-issue in one batch just after 1 January 00:00
  UTC, so a 52-week window that contains no 1 January (the weeks of 2012-12-24, 2017-12-25 and
  2023-12-25; next 2028-12-25) holds no batch, and its count and durations fall for that one week. NIR
  CAs follow their own policy (APNIC's own CA about 422 days; JPNIC, IDNIC and CNNIC about 365). Era-2
  counts include files still in the rpki-client cache (stale, orphaned or expired); era 1 has the
  archive's validated trees only.
- **Cost.** Weekly: 46 s, of which 34 s load the CA certificates (683,000) and the ROA-to-signer-EE
  table (4.55 million ROAs) of all five warehouses, once per run. All history: 76 s.

### Relying-party rejections (`scs/m_health.py`: rp_rejected)

- **Input.** `table=object_validity`: the objects our rpki-client run rejected in each cycle, from
  2026-06-20 02:40 UTC; values from the week of 2026-06-22. Census: never insufficient.
- **Statistics** (window `1w`, the ISO week of the cycle label): `objects` (distinct rejected objects),
  `ca_keys` (distinct CA keys affected: the issuing key of a rejected signed object, the own key of a
  rejected CA certificate), `objects_type_<type>` (roa, mft, cer, aspa, gbr; crl, unknown),
  `objects_reason_<reason>` (the rpki-client reason code, '-' written as '_', e.g. expired,
  not_yet_valid), `objects_<type>_<reason>`.
- **Populations.** ROAs, ASPAs and GBRs by their EE certificate's host; manifests by the CA's own host;
  CA certificates by their own host (an expired certificate of a member-run CA on RIPE NCC's
  publication service counts under rir_paas). Populations `all` and `rir_hosted` have `objects` and
  `ca_keys` in every week (zero when nothing was rejected); every other row exists only with n > 0, so a
  missing row is a zero.
- **Status.** W+1/W+1; masked when the week's verdict cycles are fewer than 90% of its snapshot cycles,
  or when fewer than 90% of its expected cycles (72 a day) were good. A cycle without any rejected
  object writes no verdict row, so at APNIC and AFRINIC the first condition also masks clean weeks. The
  second is def_version 2 (2026-11-02): before it, an outage week whose remaining cycles all had
  verdict rows was final; the next run recomputes the metric's history.
- **Caveats.** CRLs never get a verdict (rpki-client reports an expired CRL against the manifest);
  objects without an error line count as not rejected. Cycles that ran but fetched nothing count as
  good (see Caveats above), so a week of them is not masked. Reason codes follow the wording of the
  rpki-client release named in the manifest (9.7).
- **Cost.** Weekly: 40 s (RIPE NCC 15 s). All history: 39 s.

### Framework stub (`scs/m_stub.py`: stub_crl_count)

CRLs per week and population, the smallest complete module (see `scs/metrics.py`). Not a metric of
the weekly indicators: it runs only when named with `--metrics stub_crl_count`.
