# RPKI ETL Warehouse and Longitudinal Analysis

This repository contains a high-throughput ETL that ingests RPKI repository snapshots, deduplicates objects across time, and writes query-ready Parquet tables for analysis. The design minimizes storage by content-addressing objects and separating object identity from per-snapshot membership.

The warehouse is organized **per RIR**, for example `warehouse-afrinic`, `warehouse-ripencc`. Archive snapshots are loaded per trust anchor; the 20-minute cycle (`rpki_cycle.sh`) files each publication point of the rpki-client cache under one RIR, by host name or by the AIA of a sample object, so a warehouse holds the repositories filed under that RIR rather than the trust anchor's whole tree. You can analyze one warehouse in isolation or combine results later.

This is the warehouse component of RPKI Health; see [Part of RPKI Health](#part-of-rpki-health).

## Requirements

* Python 3.11 or newer. The tests run on Python 3.14.
* The packages in `requirements.txt`. They include `pyroaring` for the membership bitmaps (not on Windows; without it the ETL writes membership as JSON files).
* For the collection cycle (`rpki_cycle.sh`): rpki-client 9.x and OpenSSL.
* Most scripts in `analysis/` and `analysis2/` also need pandas, and some `analysis/` scripts need NumPy and Matplotlib. `requirements.txt` does not list them. `analysis/build_presence_runs.py` needs only `requirements.txt`.

## Quick start

Install the dependencies and load the five sample objects in `sample-crawl/` into a small warehouse:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

python rpki_etl.py single \
  --input sample-crawl \
  --dataset-ts "2025-09-26 00:00:00" \
  --repo-id rpki.arin.net --tal-id ARIN \
  --transport rsync --session-id 0 --serial 0 \
  --out-root warehouse-sample --kv warehouse-sample/kv.sqlite \
  --io-workers 2 --parse-workers 2

python analysis/build_presence_runs.py --warehouse warehouse-sample
```

The sample objects are public objects from ARIN's RPKI repository; see [sample-crawl/README.md](sample-crawl/README.md). The ETL reads every file under `--input`, so it also stores that README, as one object of unknown type.

Query the tables, for example with DuckDB:

```bash
python - <<'EOF'
import duckdb
print(duckdb.sql("""
    SELECT DISTINCT r.asn, p.prefix, p.max_length
    FROM read_parquet('warehouse-sample/tables/table=roa_objects/**/*.parquet') r
    JOIN read_parquet('warehouse-sample/tables/table=roa_prefixes/**/*.parquet') p USING (object_id)
    ORDER BY 1, 2
"""))
EOF
```

`DISTINCT` is needed because of the symlink described under [Known issues](#known-issues).

Run the tests with `python -m pytest -q`.

### Loading a historical archive

`rpki_etl.py bulk` walks a tree of timestamped snapshots. For example, for one month of AFRINIC snapshots from the RIPE NCC archive, unpacked under `/data/rpki-archive/unpacked`:

```bash
python rpki_etl.py bulk \
  --input-root /data/rpki-archive/unpacked/afrinic/afrinic.tal/2022/01 \
  --repo-id ftp.ripe.net --tal-id AFRINIC \
  --transport rrdp --session-id 0 --serial 0 \
  --out-root warehouse-afrinic --kv warehouse-shared/kv.sqlite \
  --io-workers 24 --parse-workers 32 \
  --zstd-level 1 --zstd-threads 0 \
  --force-time "00:00:00" --exclude-dir unvalidated \
  --skip-existing-membership \
  --cas-backend sqlite --cas-mode path \
  --cas-sqlite-root warehouse-afrinic/cas-sqlite \
  --cas-external-root /data/rpki-archive/unpacked \
  --flush-rows 250000 --verbose
```

`run_tal_etl.sh TAL PERIOD` runs a similar command for one TAL and a year, month or day. It expects the layout `$DATA_ROOT/<tal>/<tal>.tal/YYYY/MM/DD/`.

After the ETL finishes, build the longitudinal **presence runs**:

```bash
python analysis/build_presence_runs.py \
  --warehouse warehouse-afrinic \
  --batch-rows 200000
```

Presence runs are required for many day-by-day analyses.

## Data sources

* **History.** Daily snapshots of the RIPE NCC RPKI repository archive (<https://ftp.ripe.net/rpki/>), loaded with `rpki_etl.py bulk`, one snapshot per day. In production they cover 2012-01-01 (ARIN: 2012-09-24) to mid-April 2026. The scripts that downloaded and unpacked the archive are not part of this repository.
* **Live collection.** The cache of a local rpki-client run over the five RIR trust anchors (RRDP, with rsync as fallback), loaded with `rpki_etl.py single` by `rpki_cycle.sh`. Production has collected this way since 2026-04-08; cron now starts a cycle every 20 minutes.
* **RP validity.** rpki-client's per-file verdicts, as described under [RP-validity tagging](#rp-validity-tagging-valid-vs-invalid).

## Operational scripts

The shell scripts were written for the lab's collection host. `rpki_cycle.sh` and `run_daily_build_presence.sh` find the checkout from their own location and use its `.venv` (override with `PYTHON`); `run_tal_etl.sh` must be started from the checkout (it looks for `./rpki_etl.py` and writes `warehouse-<tal>/` there) and runs `python` from `PATH` unless `PYTHON` is set. rpki-client's cache and output under `/ssd-data/rpki-validator/`, the TAL directory (`TAL_DIR`, default `/etc/tals`), the archive root (`DATA_ROOT`, default `/ssd-data/rpki-objects/unpacked`) and the lock files under `/tmp` are that host's paths. Adapt them before you run the scripts anywhere else.

* `rpki_cycle.sh` runs one collection cycle: rpki-client, the validity map, sorting the cache's publication points by RIR, and `rpki_etl.py single` for each RIR. It appends one line per finished cycle to `STATUS_LOG` (default `~/rpki_cycle_status.log`). A cycle that starts while the previous one still runs exits at once.
* `run_daily_build_presence.sh` rebuilds the presence tables of the five RIR warehouses in this checkout. Production runs it daily at 01:00 UTC.
* `run_tal_etl.sh TAL PERIOD` bulk-loads archive snapshots for one TAL. Environment variables (`DATA_ROOT`, `KV_PATH` and others) set its paths.

[ARCHITECTURE.md](https://github.com/taejoong/rpki-health/blob/main/ARCHITECTURE.md) describes how production schedules and monitors these jobs.

## Pipeline at a glance

1. **Discover snapshots** under `--input-root`. Each snapshot is a point-in-time tree of repository files. The ETL prunes `unvalidated` subtrees when requested.
2. **Hash** every candidate file by SHA-256 of raw bytes. This identifies unique objects across days and collapses duplicates.
3. **Assign stable IDs** using `kv.sqlite` which maps `object_hash -> object_id` and stores the first seen metadata for that hash. This lets you reference objects efficiently everywhere else.
4. **CAS store** using either a file CAS or a sharded **SQLite CAS**. In SQLite mode you can store compressed bytes or just a **path reference** via `--cas-mode bytes|path`.
5. **Parse** objects to populate type-specific tables: ROAs, prefixes, certificates, manifests, CRLs, signatures.
6. **Membership index** per snapshot: a Roaring bitmap `indexes/membership/ts=YYYY-MM-DD HH-MM-SS/objects.roar` that marks which `object_id`s are present in that snapshot.
7. **Presence runs** (post-step): compress the per-snapshot membership into `object_presence_runs` with `[start_ts, end_ts]` intervals per object.

This separation lets you keep one copy of each unique object while still answering time-series questions quickly.

## RP-validity tagging (valid vs invalid)

The ETL ingests **every** object rpki-client downloads into its cache (i.e. *repo-present*),
not just the objects that pass Relying-Party (RP) validation. RP-validity is **time-varying**
(an object can be valid one cycle and stale/expired the next), but the head tables
(`roa_objects`, `manifests`, …) are **write-once**: one row per `object_hash`, frozen at first
sight. So validity cannot live on a head row — it would be stuck at whatever the verdict was the
first time the object was ever seen.

Instead, validity is recorded in a separate **`object_validity`** table keyed by
`(dataset_ts, object_id)`. Each cycle re-emits **one row per *present* object that rpki-client
rejected** (typically ~hundreds). An object that is present but *not* listed is valid by default.
The point-in-time counts are then:

* **repo-present(ts)** = membership bitmap cardinality at `ts` (every cached object).
* **invalid(ts)** = rows in `object_validity` for that `ts`.
* **RP-valid(ts)** = `repo-present(ts) − invalid(ts)` (≈ the figure a public validator like
  `rpki-client`'s openbgpd output reports).

This mirrors how the membership Roaring bitmaps already separate *time-varying presence* from
*write-once object identity*.

> **valid** here means *"rpki-client did not explicitly reject it"*. Orphan/unreachable cached
> objects with no error line stay valid, as do CMS-embedded EE certs (which have no file of
> their own). Treat absence from `object_validity` as "not rejected", not as positive proof.

### How it works (3 stages)

```
[1] rpki-client -v ...  2> rpki-client.stderr.log      # preserve per-file verdicts
[2] build_validity_map.py --stderr-log ... --cache-dir ...  --output validity-map.json
        parses each rejection line -> resolves the path to its cache file -> SHA-256
        => { "<object_hash>": {"state": 2, "reason": "<code>"}, ... }   (only rejects listed)
[3] rpki_etl.py single --validity-map validity-map.json
        after membership is computed, for each present object_hash found in the map
        emit an object_validity row (dataset_ts, object_id, state=2, reason, tal_id)
```

The join key is the **content SHA-256 (`object_hash`)**, not the path, because rpki-client's
cache layout (RRDP session-hash dirs vs rsync, dual-transport duplication) makes paths unstable.
`rpki_cycle.sh` wires this in and is **fail-safe**: if the map build fails it writes an empty
map, so no `object_validity` rows are written (everything reads as valid) and data collection
never stalls. Running `rpki_etl.py` without `--validity-map` likewise writes no validity rows,
so old partitions and other callers are unaffected.

### Reason codes

| `reason_code` | rpki-client 9.x message it derives from |
|---|---|
| `expired` | `CRL has expired`, `certificate has expired` |
| `not-yet-valid` | `certificate is not yet valid` |
| `no-manifest` | `no valid manifest available` (the `.mft` is usually absent → not hashable) |
| `ta-constraint` | `trust anchor constraints violation: ...` |
| `resource-violation` | `RFC 3779 resource not subset of parent's resources`, `missing IP or AS resources` |
| `bad-digest`, `stale-mft`, `revoked`, `failed-verification`, `failed-parse`, `invalid` | other / wording-drift fallbacks |

### Validating the map against rpki-client's own counters

rpki-client's JSON `metadata` carries aggregate reject counts that the per-file map should
reconcile against. On a representative production run:

* `failedmanifests = 131` = `CRL has expired` (104) + `no valid manifest available` (27).
* `failedaspas = 7` = the 7 `.asa` `trust anchor constraints violation` lines.

So a quick health check is: sum the map's manifest/aspa reasons and compare to the JSON
counters. A large divergence means the stderr wording drifted or the cache layout changed.

### Implementation bugs found (and why the parser is shaped this way)

Two non-obvious bugs surfaced only when validating against **real** rpki-client 9.x stderr (an
earlier draft was written against an assumed format). They are fixed; documented here so the
shape of `build_validity_map.py` is not "simplified" back into them:

1. **Wrong cache-path resolution.** rpki-client prints reject paths as
   `<host>/<repo>/<file>` with **no transport prefix**, but stores the bytes under
   `.rrdp/<session-hash>/<host>/...` (RRDP) or `.rsync/<host>/...` (rsync fallback) — *not* at
   `cache/<host>/...`. The resolver must therefore glob `.rrdp/*/<token>` (and try `.rsync/`)
   to locate the file before hashing. A naive `cache_dir / token` join resolves **zero** files.
2. **Missing rejection signals.** The reason classifier gates on a keyword whitelist (to keep
   informational lines like `pulling from .../ta.cer` out of the map). The initial whitelist
   missed three real messages — `no valid manifest available`, `trust anchor constraints
   violation`, and `missing IP or AS resources` — so those rejects were silently dropped. The
   gate now includes them. When upgrading rpki-client, re-check stderr wording against the
   reason table above and the `_REJECTION_SIGNALS`/`_REASON_RULES` lists.

Unit coverage: `tests/test_build_validity_map.py` (synthetic stderr + cache fixtures exercising
both layouts and the informational-line filter).

## Directory layout

```
warehouse-<tal>/
  cas/                 # file CAS if enabled
  cas-sqlite/          # SQLite CAS shards if enabled (bytes or path mode)
  tables/              # Parquet tables, append-only parts
    table=<name>/**.parquet
  indexes/
    membership/
      ts=YYYY-MM-DD HH-MM-SS/
        objects.roar  # Roaring bitmap of object_ids present in that snapshot
  kv.sqlite            # global mapping of object_hash -> object_id (and seen metadata)
  snapshots/           # ts=<snapshot>/metadata.json (+ errors.csv) for every snapshot loaded;
                       # the backend API and the scorecard ledger read them, do not delete
```

* **KV store (`kv.sqlite`)**: maps 64-hex `object_hash` to int `object_id`. If you share KV across TALs, identical objects share IDs across TALs. If you keep a separate KV per TAL, IDs are per-TAL and cannot be compared directly. Production shares one KV across all RIRs: `warehouse-shared/kv.sqlite`.
* **CAS backends**:

  * `--cas-backend file`: compressed per-object files on disk.
  * `--cas-backend sqlite`: sharded SQLite with `objects(key BLOB, cbytes BLOB, orig_len INT, type TEXT, path TEXT)`.
  * `--cas-mode path`: only store a path string in CAS; bytes live in your local mirror. This saves space and speeds ETL. Reads resolve the path, optionally rehash with `--cas-verify-on-read`.

## Table glossary

Each table resides under `tables/table=<name>/**.parquet`. The ETL writes new parts append-only. There are no in-place rewrites. The two presence tables are the exception: `analysis/build_presence_runs.py` rebuilds them.

### 1. `roa_objects`

* **Grain**: one row per ROA object.
* **Key columns**:

  * `object_id INT` - stable ID from KV.
  * `object_hash STRING` - 64-hex SHA-256 of raw bytes.
  * `semantic_hash STRING` - 64-hex SHA-256 of a canonicalized ROA payload; identical routing content under different EEs maps to the same semantic hash.
  * `asn BIGINT` - origin AS.
  * `ee_not_before TIMESTAMP`, `ee_not_after TIMESTAMP` - EE validity window in the object.
  * `signing_time TIMESTAMP` - CMS SigningTime if present.
  * `ee_serial STRING` - EE certificate serial if extracted.
  * `ee_public_key STRING` - the EE certificate's public key.
  * `repo_id STRING`, `tal_id STRING`.
* **Why it exists**: joins to `roa_prefixes` and supports dedup by `semantic_hash`. RP-validity is **not** a column here (head rows are write-once); see the `object_validity` table and [RP-validity tagging](#rp-validity-tagging-valid-vs-invalid).

### 2. `roa_prefixes`

* **Grain**: one row per prefix authorization inside a ROA.
* **Columns**:

  * `object_id INT` - parent ROA.
  * `afi UINT8` - 4 or 6.
  * `prefix STRING` - canonical CIDR like `a.b.c.d/len` or `xxxx::/len`.
  * `max_length UINT8`.

### 3. `certs`

* **Grain**: one row per certificate; includes CA and EE certs. The rows are split into the directories `table=certs/is_ca=true/` and `table=certs/is_ca=false/` (a `**/*.parquet` read in DuckDB adds the directory value as a text column `is_ca`). EE certificates embedded in signed objects have no file of their own, so their `object_id` is NULL.
* **Columns**:

  * `cert_id INT` - optional internal surrogate if present.
  * `object_id INT`, `cert_hash STRING`.
  * `is_ca_flag BOOL` - the certificate is a CA certificate.
  * `subject_dn STRING`, `issuer_dn STRING` - cleaned DNs without "Serial Number:" or "Common Name:" prefixes.
  * `serial_dec STRING`, `serial_bin STRING`.
  * `ski STRING`, `aki STRING`.
  * `not_before TIMESTAMP`, `not_after TIMESTAMP`.
  * `spki_alg STRING`, `spki_bits INT`, `spki_sha256 STRING`.
  * `resources_ip STRING`, `resources_asn STRING` - not filled in yet; see [Known issues](#known-issues).
  * `sia STRING`, `aia STRING`, `cdp STRING`.
  * `key_usage STRING`, `ext_key_usage STRING`, `basic_constraints STRING`.
  * `repo_id STRING`, `tal_id STRING`.

### 4. `manifests`

* **Grain**: one row per manifest object.
* **Columns**:

  * `object_id INT`
  * `manifest_number STRING`
  * `this_update STRING`, `next_update STRING` - ISO 8601 text such as `2025-09-26T01:00:04+00:00`; cast to TIMESTAMPTZ before comparing
  * `file_count INT`
  * No `repo_id` or `tal_id`: take them from the manifest's EE certificate, `certs/is_ca=false` via `object_signatures.signer_cert_id`.

### 5. `manifests_entries`

* **Grain**: one row per file listed on a manifest.
* **Columns**:

  * `object_id INT` - parent manifest.
  * `filename STRING`
  * `hash_alg STRING`
  * `file_hash STRING` - hex SHA-256; normalized formatting across tables.

### 6. `crls`

* **Grain**: one row per CRL header.
* **Columns**:

  * `crl_id INT` - equal to `object_id`; joins to `crl_serials`.
  * `object_id INT`
  * `issuer_dn STRING` - cleaned DN without "Serial Number:".
  * `aki STRING`
  * `crl_number STRING`
  * `this_update TIMESTAMP`, `next_update TIMESTAMP`
  * `signature_alg STRING`
  * `revoked_count INT`
  * `repo_id STRING`, `tal_id STRING`
  * No hash or issuer column: the CRL's SHA-256 is in `kv.sqlite`, and the issuing CA is the CA certificate whose `ski` = `aki`.

### 7. `crl_serials`

* **Grain**: one row per revoked serial on a CRL.
* **Columns**:

  * `crl_id INT` - join to `crls`.
  * `serial_dec STRING`, `serial_bin STRING`
  * `revocation_date TIMESTAMP`
  * `reason_code INT16` - if present
  * `invalidity_date TIMESTAMP` - if present
  * `cert_issuer_dn STRING` - cleaned; no "Serial Number:" and no registry suffixes like ", AFRINIC"
  * `revoked_cert_id INT` - if resolvable

### 8. `object_signatures`

* **Grain**: one row per signed object envelope.
* **Columns**:

  * `object_id INT`
  * `object_type STRING` - ROA, MFT, ASPA or GBR: CMS-signed objects only (CRLs and certificates have no row here)
  * `signer_cert_id INT` - the `certs.cert_id` of the EE certificate that signed the object, if it was resolved (the issuing CA key is that EE certificate's `aki`)
  * `content_type_oid STRING` - eContent OID, e.g. ROA 1.2.840.113549.1.9.16.1.24, GBR 1.2.840.113549.1.9.16.1.35
  * `digest_algorithm STRING`, `signature_algorithm STRING`
  * `signing_time TIMESTAMP`
  * `message_digest STRING`
  * No serial column: the EE serial is `certs.serial_dec` of `signer_cert_id`, or `roa_objects.ee_serial`.

### 9. `object_presence_runs`

* **Grain**: one row per run of consecutive snapshots in which an object is present.
* **Columns**:

  * `object_id INT`
  * `start_sid INT`, `end_sid INT` - snapshot indexes in time order; `end_sid` is inclusive
  * `start_ts STRING`, `end_ts STRING` - the matching snapshot times, `YYYY-MM-DD HH:MM:SS`; `end_ts` is inclusive
* **How it is built**: `analysis/build_presence_runs.py` reads every membership bitmap under `indexes/membership/ts=.../objects.roar` in time order and coalesces consecutive snapshots into a single `[start, end]` run. This preserves flicker patterns while keeping the table compact.

### 10. `object_presence_stats`

* **Grain**: one row per object.
* **Columns**:

  * `object_id INT`
  * `first_sid INT`, `last_sid INT`
  * `first_ts STRING`, `last_ts STRING` - the first and last snapshot in which the object was present
  * `runs_count INT`
  * `total_days INT` - the number of snapshots in which the object was present (despite the name, not days)
* **Use**: fast filters and sanity checks. The RPKI Health API takes first-seen and last-seen times from this table.

### 11. `object_validity`

* **Grain**: one row per *present-but-rejected* object **per cycle** (time-varying, unlike the
  write-once head tables).
* **Columns**:

  * `dataset_ts STRING` - cycle timestamp; together with `object_id` forms the primary key.
  * `object_id INT` - the rejected object (joins to any head table).
  * `state UINT8` - RP-validation verdict: `2` invalid. (Valid objects are *absent*; `0/1` reserved.)
  * `reason_code STRING` - rejection reason (e.g. `expired`, `no-manifest`); see [Reason codes](#reason-codes).
  * `tal_id STRING`.
* **How it is built**: each `single` run with `--validity-map` emits a row for every object_hash
  in the map that is present in this cycle's membership. **RP-valid(ts) = membership(ts) −
  object_validity rows(ts)**. See [RP-validity tagging](#rp-validity-tagging-valid-vs-invalid).

## Membership bitmaps

For each discovered snapshot time the ETL writes:

```
indexes/membership/ts=YYYY-MM-DD HH-MM-SS/objects.roar
```

This is a Roaring bitmap of `object_id`s present at that timestamp. When you need exact day sets, load the latest snapshot per day and use that bitmap. Many analysis scripts prefer bitmaps for speed. If `pyroaring` is not available, use `object_presence_runs` as a fallback.

## Why you must run build_presence_runs.py

The ETL writes per-snapshot membership only. Many longitudinal questions need continuous presence or flicker detection. `analysis/build_presence_runs.py`:

* lists every snapshot under `indexes/membership/` in time order
* loads each snapshot's `objects.roar`
* for each `object_id`, turns the snapshots it is present in into runs of consecutive snapshots, `[start_ts, end_ts]`
* writes `tables/table=object_presence_runs/` and `tables/table=object_presence_stats/`

Example:

```bash
python analysis/build_presence_runs.py \
  --warehouse warehouse-ripencc \
  --batch-rows 200000
```

`--batch-rows` sets the number of rows per Parquet part (default 500,000). Each run rebuilds both tables from all snapshots and overwrites their part files (`part-00000.parquet` upward). It does not delete leftover parts, so if a rebuild may write fewer parts than the previous one (for example after you raise `--batch-rows`), delete the two table directories first. In production, `run_daily_build_presence.sh` runs it every day for each RIR.

## Common queries

Count objects by type:

```sql
SELECT 'ROA' AS type, COUNT(*) FROM read_parquet('warehouse-afrinic/tables/table=roa_objects/**/*.parquet')
UNION ALL
SELECT 'MFT', COUNT(*) FROM read_parquet('warehouse-afrinic/tables/table=manifests/**/*.parquet')
UNION ALL
SELECT 'CRL', COUNT(*) FROM read_parquet('warehouse-afrinic/tables/table=crls/**/*.parquet')
UNION ALL
SELECT 'CER', COUNT(*) FROM read_parquet('warehouse-afrinic/tables/table=certs/**/*.parquet');
```

These counts include the newest part of each table twice; see [Known issues](#known-issues).

Daily ROA object counts using presence runs:

```sql
WITH roap AS (
  SELECT pr.object_id,
         CAST(pr.start_ts AS TIMESTAMP) AS s,
         CAST(pr.end_ts   AS TIMESTAMP) AS e
  FROM read_parquet('warehouse-afrinic/tables/table=object_presence_runs/**/*.parquet') pr
  JOIN read_parquet('warehouse-afrinic/tables/table=roa_objects/**/*.parquet') r
    ON pr.object_id = r.object_id
),
bounds AS (
  SELECT DATE_TRUNC('day', MIN(s)) s0, DATE_TRUNC('day', MAX(e)) e0 FROM roap
)
SELECT d.d AS date, COUNT(DISTINCT roap.object_id) AS roas
FROM bounds b,
LATERAL range(b.s0, b.e0 + INTERVAL 1 day, INTERVAL 1 day) d(d)
LEFT JOIN roap ON roap.s <= d.d AND roap.e >= d.d
GROUP BY 1 ORDER BY 1;
```

Sanity: number of distinct `object_id`s in membership for a date equals the count in presence runs overlapping that date.

## Performance tips

* Hashing: `--io-workers` and `--zstd-level 1` reduce latency for large trees.
* Parsing: `--parse-workers` near the number of physical cores; the ETL uses a process pool with a safe start method to avoid stalls after heavy thread use.
* Parquet: `--flush-rows 250000` or higher reduces file count. Files are append-only.
* CAS: use `--cas-backend sqlite --cas-mode path` with `--cas-external-root` pointing at your local mirror to avoid duplicated bytes.
* Membership: keep `--skip-existing-membership` on repeated runs for the same timestamps.

## Troubleshooting

* CRL headers missing but `crl_serials` present:

  * You likely ran in CAS path mode without parsing CRLs on the first pass, or skipped CRL header writes.
  * Use `tools/backfill_crl_headers_from_manifests_mp.py`. It resolves CRL file hashes from `manifests_entries` to `object_id`s via `kv.sqlite`, reads bytes from `cas-sqlite` or your mirror, parses CRLs, and writes `table=crls`.
* Mixed KV:

  * If you split KV per TAL, `object_id`s are TAL-local. Do not intersect bitmaps across TALs unless you also keep separate warehouses.

## Why this model

* Most snapshots share 90 percent of objects with the previous day. Content-addressing collapses duplicates while membership bitmaps and presence runs retain longitudinal structure.
* `semantic_hash` on ROAs separates packaging churn from routing semantics. This stabilizes many metrics, especially in the presence of frequent EE reissuance.

## Analysis scripts

`analysis/` holds the presence-run builder and longitudinal analyses (`a01` to `a24`), which write PNG and CSV files to `--out-dir`.

See `analysis2/` for a growing library of repository-only analyses, and [analysis2/README.md](analysis2/README.md):

* maxLength safety, AS0 hygiene, manifest timeliness, CRL hygiene, replace-and-revoke, CA key rollovers, signing-time adoption, ROA profile conformance, and more.
* Outputs are written to `analysis2/out/` as CSVs. Git ignores them, and CSV and PNG files in general.

Use `--warehouse warehouse-<tal>` to analyze one TAL. You can later merge results across TALs. See [Requirements](#requirements) for the extra packages these scripts need.

## Known issues

* `certs.resources_ip` and `certs.resources_asn` are not filled in. `parse_cert.py` reads the RFC 3779 extensions with asn1crypto, which does not decode them, so the columns hold an empty string (or null when the certificate has no such extension). Decode the resources from the certificate bytes instead; `tools/get_object.py` fetches them from the warehouse.
* In each table directory written by the ETL, `<table>.parquet` is a symlink to the newest part file. A `**/*.parquet` glob matches the symlink as well, so it reads the newest part twice. Deduplicate (for example on `object_id`) when you need exact counts.

## Part of RPKI Health

This component is part of RPKI Health, a dashboard and public API over these warehouses. The NetSecure Lab at Virginia Tech runs it at <https://rpki.netsecurelab.org> (also <https://rpki-health.netsecurelab.org>).

* Code: <https://github.com/taejoong/rpki-health> receives the public release snapshots. This component is its `parser/` directory; `backend/` holds the API and `frontend/` the web UI.
* Design: [ARCHITECTURE.md](https://github.com/taejoong/rpki-health/blob/main/ARCHITECTURE.md) explains how the parts fit together.
* Security: report vulnerabilities privately through GitHub's private vulnerability reporting on that repository (Security tab, "Report a vulnerability"), not in a public issue.

## License

BSD 3-Clause License. See [LICENSE](LICENSE).

The RPKI objects in `sample-crawl/` were published by ARIN and are included unchanged as test fixtures; see [sample-crawl/README.md](sample-crawl/README.md).

## Acknowledgments

The RPKI warehouse and its ETL were built with support from Virginia Tech and the U.S. National Science Foundation. The public release, the weekly refresh pipeline, the dashboard and the operator-facing work are supported by the ARIN Community Grant Program (<https://www.arin.net/about/community_grants/>).

Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation or ARIN.
