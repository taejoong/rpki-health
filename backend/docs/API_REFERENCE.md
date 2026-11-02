# API Reference

Base URL: `http://localhost:8000`

Interactive docs: `http://localhost:8000/docs`

---

## ROA Lookup & History

### GET `/roa/lookup`

Live VRPs whose prefix is exactly `prefix`: an exact string match against the stored canonical form, and the query is not normalized (`1.0.0.1/24` or upper-case IPv6 match nothing). A ROA for a covering, shorter prefix is not returned, whatever its maxLength; to validate the prefixes an ASN announces against covering VRPs, use `/roa/coverage`. Reads the in-memory `live_roas` table. By default only RP-valid VRPs are returned (`validity=valid`); `validity=all` adds the ones rpki-client rejected.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `prefix` | string | Yes | - | IP prefix (e.g. `1.0.0.0/24`, `2001:db8::/32`) |
| `asn` | integer | No | - | Filter by ASN |
| `rir` | string | No | - | Filter by RIR name |
| `validity` | string | No | `valid` | `valid` (RP-valid only) or `all` (everything present in the repositories) |

**Response:**

```json
{
  "query_prefix": "1.0.0.0/24",
  "query_asn": null,
  "count": 1,
  "results": [
    {
      "rir": "apnic",
      "asn": 13335,
      "prefix": "1.0.0.0/24",
      "max_length": 24,
      "afi": 4,
      "object_hash": "1a726de7d6cc...",
      "signing_time": "2026-03-01 11:20:20-05",
      "first_seen": "2026-03-02 00:00:00",
      "last_seen": "2026-04-08 12:30:01"
    }
  ]
}
```

---

### GET `/roa/by-asn`

List all current ROA prefixes for an ASN.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `asn` | integer | Yes | - | Autonomous System Number |
| `rir` | string | No | - | Filter by RIR |
| `afi` | integer | No | - | Address family: `4` (IPv4) or `6` (IPv6) |
| `limit` | integer | No | 1000 | Max results (1-10000) |
| `offset` | integer | No | 0 | Pagination offset |
| `validity` | string | No | `valid` | `valid` (RP-valid only) or `all` (everything present in the repositories) |

**Response:**

```json
{
  "query_asn": 13335,
  "count": 5595,
  "results": [
    {
      "rir": "apnic",
      "asn": 13335,
      "prefix": "1.0.0.0/24",
      "max_length": 24,
      "afi": 4,
      ...
    }
  ]
}
```

---

### GET `/roa/history`

Full lifecycle history of ROAs for a prefix (exact match) or ASN. Returns every ROA object that ever existed, with `first_seen`/`last_seen` timestamps showing creation and withdrawal. Reads the in-memory `roa_history` table, which startup and every reload build from the Parquet files.

At least one of `prefix` or `asn` is required.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `prefix` | string | No | - | IP prefix |
| `asn` | integer | No | - | ASN |
| `rir` | string | No | - | Filter by RIR (recommended for performance) |
| `limit` | integer | No | 5000 | Max results (1-50000) |

**Response:**

```json
{
  "query_prefix": "1.0.0.0/24",
  "query_asn": null,
  "count": 3,
  "truncated": false,
  "entries": [
    {
      "rir": "apnic",
      "asn": 13335,
      "prefix": "1.0.0.0/24",
      "max_length": 24,
      "afi": 4,
      "object_hash": "5c0d39a97706...",
      "semantic_hash": "76a8e486eda9...",
      "signing_time": "2018-03-16 13:00:05-04",
      "first_seen": "2018-04-05 00:00:00",
      "last_seen": "2021-02-11 00:00:00"
    },
    ...
  ]
}
```

**Notes:**
- `semantic_hash` stays the same across re-signs of identical content; `object_hash` changes each time.
- Use this to distinguish re-signing (same `semantic_hash`) from actual ROA modification (different `semantic_hash`).

---

## Analytics

### GET `/analytics/churn`

ROA churn: new and withdrawn ROA entries per period, counted on the in-memory `roa_history` table, one entry per (ROA object, prefix): a ROA with three prefixes counts three times. `first_seen` and `last_seen` come from `object_presence_stats`; an entry is withdrawn when its last_seen is before the newest last_seen of the RIR. The last period with withdrawals, and any after it, is left out (it collects everything that disappeared just before the data edge).

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `granularity` | string | No | `month` | `week` or `month` |
| `start` | string | No | - | Start period (e.g. `2024-01` or `2024-W01`) |
| `end` | string | No | - | End period |

**Response:**

```json
{
  "rir": "arin",
  "granularity": "month",
  "start": "2025-06",
  "end": "2025-12",
  "data": [
    {"period": "2025-06", "new": 60876, "withdrawn": 65470, "net": -4594},
    {"period": "2025-07", "new": 82327, "withdrawn": 84852, "net": -2525},
    ...
  ]
}
```

---

### GET `/analytics/lifespan`

Object lifespan statistics in days. Only considers withdrawn objects (not currently live).

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `object_type` | string | No | `roa` | `roa`, `cert`, `crl`, or `manifest` |

**Response:**

```json
{
  "results": [
    {
      "rir": "arin",
      "object_type": "roa",
      "count": 2071557,
      "avg_days": 83.4,
      "median_days": 79.0,
      "p10_days": 24.0,
      "p90_days": 80.0
    }
  ]
}
```

---

### GET `/analytics/signing-cadence`

CRL-based re-signing interval distribution. Measures the time between consecutive CRL `this_update` timestamps per CA (grouped by AKI), over the CRLs in the live window only (last seen on or after the day before the newest presence date, i.e. about the last one to two days); it is not a history of the CA's cadence.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `ca_aki` | string | No | - | Filter by CA's Authority Key Identifier |

**Response:**

```json
{
  "rir": "ripencc",
  "total_re_signs": 4843,
  "avg_interval_hours": 9.0,
  "median_interval_hours": 9.0,
  "buckets": [
    {"interval_hours": "0-6", "count": 4},
    {"interval_hours": "6-12", "count": 4810},
    {"interval_hours": "12-24", "count": 29},
    {"interval_hours": "24-48", "count": 0},
    {"interval_hours": "48-168", "count": 0},
    {"interval_hours": "168+", "count": 0}
  ]
}
```

---

### GET `/analytics/crl-staleness`

Find CRLs past their `next_update` time. Only considers currently live CRLs. Staleness = now - next_update.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `min_staleness_hours` | float | No | 0 | Minimum staleness threshold |
| `limit` | integer | No | 100 | Max results (1-1000) |

**Response:**

```json
{
  "rir": "ripencc",
  "total_live_crls": 27115,
  "stale_count": 5,
  "entries": [
    {
      "rir": "ripencc",
      "issuer_dn": "d6424e531a5a...",
      "aki": "d6424e531a5a...",
      "this_update": "2023-02-08 03:26:45-05",
      "next_update": "2023-02-09 06:42:45-05",
      "staleness_hours": 27720.9,
      "revoked_count": 0
    }
  ]
}
```

---

### GET `/analytics/maxlength-issues`

Find ROAs with excessively loose maxLength. A large gap between prefix length and maxLength enables sub-prefix hijacking.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `min_excess` | integer | No | 8 | Minimum excess bits (maxLength - prefixLength) |
| `limit` | integer | No | 100 | Max results (1-1000) |

**Response:**

```json
{
  "rir": "ripencc",
  "total_roas": 341715,
  "issues_count": 5,
  "entries": [
    {
      "rir": "ripencc",
      "prefix": "2a05:1a00::/26",
      "prefix_length": 26,
      "max_length": 128,
      "excess": 102,
      "asn": 43754
    }
  ]
}
```

---

### GET `/analytics/rir-comparison`

Compare a metric across all 5 RIRs over time: the number of distinct objects first seen in each period per RIR (`object_presence_stats.first_ts`), not the number of live objects.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `metric` | string | No | `roa_count` | `roa_count`, `aspa_count`, or `cert_count` |
| `granularity` | string | No | `month` | `week` or `month` |
| `start` | string | No | - | Start period |
| `end` | string | No | - | End period |

**Response:**

```json
{
  "metric": "roa_count",
  "granularity": "month",
  "data": [
    {"period": "2025-01", "afrinic": 8239, "apnic": 5153, "arin": 54767, "lacnic": 2361, "ripencc": 47187},
    ...
  ]
}
```

---

## CA Hierarchy

### GET `/ca/info`

Get details about a CA certificate. Provide either `ski` or `cert_id`.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `ski` | string | No | - | Subject Key Identifier |
| `cert_id` | integer | No | - | Certificate ID |

**Response:**

```json
{
  "rir": "ripencc",
  "cert_id": 89479216,
  "object_id": 175510433,
  "subject_dn": "ripe-ncc-ta",
  "ski": "e8552b1fd6d1a4f7e404c6d8e5680d1ebc163fc3",
  "aki": null,
  "not_before": "2026-03-18 05:17:45-04",
  "not_after": "2026-06-18 05:17:45-04",
  "resources_ip": "",
  "resources_asn": "",
  "first_seen": "2026-03-19 00:00:00",
  "last_seen": "2026-04-08 00:00:00"
}
```

---

### GET `/ca/children`

List the direct child CA certificates of a CA (child AKI = parent SKI and child issuer DN = parent subject DN), one per child key, the most recently observed instance. EE certificates are not listed.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `cert_id` | integer | Yes | - | Parent CA cert_id |
| `current` | boolean | No | true | Only include live children |
| `limit` | integer | No | 100 | Max results (1-1000) |

---

### GET `/ca/stats`

Count of live signed objects (ROA, MFT, ASPA, GBR) whose CMS signer certificate is `cert_id`, by type. Signed objects are signed by one-time EE certificates, never by the CA certificate itself, so for a CA's `cert_id` the result is `total_signed: 0` and an empty `by_type`.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `cert_id` | integer | Yes | - | Signer certificate ID |

---

## ASPA Observatory

### GET `/aspa/list`

List ASPA objects with customer-provider AS pairs.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | No | - | Filter by RIR |
| `customer_asn` | integer | No | - | Filter by customer ASN |
| `current` | boolean | No | true | Only live objects |
| `limit` | integer | No | 500 | Max results (1-10000) |
| `offset` | integer | No | 0 | Pagination offset |

**Response:**

```json
{
  "count": 877,
  "entries": [
    {
      "rir": "arin",
      "customer_asn": 53808,
      "provider_asn": 51847,
      "afi_limit": null,
      "signing_time": "2025-10-24 13:23:34-04",
      "first_seen": "2025-10-25 00:00:00",
      "last_seen": "2026-04-08 00:00:00"
    }
  ]
}
```

---

### GET `/aspa/stats`

ASPA deployment summary across all RIRs.

**Response:**

```json
{
  "total_live_objects": 1633,
  "total_customer_asns": 1593,
  "per_rir": {
    "afrinic": {"live_objects": 0, "customer_asns": 0, "provider_pairs": 0},
    "apnic": {"live_objects": 10, "customer_asns": 10, "provider_pairs": 45},
    "arin": {"live_objects": 318, "customer_asns": 313, "provider_pairs": 877},
    "lacnic": {"live_objects": 73, "customer_asns": 73, "provider_pairs": 216},
    "ripencc": {"live_objects": 1232, "customer_asns": 1198, "provider_pairs": 4519}
  }
}
```

---

### GET `/aspa/trend`

ASPA adoption trend over time.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `granularity` | string | No | `month` | `week` or `month` |

**Response:**

```json
[
  {"period": "2023-07", "new_objects": 1, "new_customer_asns": 1},
  {"period": "2026-03", "new_objects": 176, "new_customer_asns": 137},
  ...
]
```

---

## Change Detection

### GET `/alerts/recent`

Detect recent ROA changes within the last N days.

- **new**: `first_seen` within the window, whether or not the entry is still live (an entry that appeared and disappeared within the window is listed as new and as withdrawn)
- **withdrawn**: `last_seen` within the window and before the newest `last_seen` of the RIR

Entries are (ROA object, prefix) rows. The window ends at the RIR's newest `last_seen`, which the daily 01:00 UTC presence rebuild sets, not at the time of the request.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name (required for performance) |
| `asn` | integer | No | - | Filter by ASN |
| `days` | integer | No | 7 | Look back N days (1-90) |
| `limit` | integer | No | 500 | Max results (1-10000) |

**Response:**

```json
{
  "since": "30 days ago",
  "count": 5,
  "changes": [
    {
      "rir": "apnic",
      "change_type": "new",
      "asn": 13335,
      "prefix": "2400:cb00:ff00::/40",
      "max_length": 48,
      "object_hash": "1e94a613796a...",
      "first_seen": "2026-04-08 01:08:48",
      "last_seen": "2026-04-08 00:00:00"
    }
  ]
}
```

---

## Object Inventory & Meta

### GET `/objects/summary`

Count of objects by type (ROA, Certificate, CRL, Manifest, ASPA) for an RIR.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `rir` | string | Yes | - | RIR name |
| `current` | boolean | No | true | Only count live objects |

**Response:**

```json
{
  "rir": "ripencc",
  "snapshot": "2026-04-08 20-00-01",
  "objects": [
    {"object_type": "ROA", "count": 57878},
    {"object_type": "Certificate", "count": 22287},
    {"object_type": "CRL", "count": 49189},
    {"object_type": "Manifest", "count": 49189},
    {"object_type": "ASPA", "count": 1209}
  ]
}
```

---

### GET `/health`

API health check with warehouse freshness info.

**Response:**

```json
{
  "status": "ok",
  "live_roa_total": 833962,
  "warehouses": [
    {
      "rir": "afrinic",
      "latest_snapshot": "2026-04-08 20-15-01",
      "snapshot_count": 5205,
      "object_count": 15579,
      "unique_object_count": 15323,
      "live_roa_count": 30790
    },
    ...
  ]
}
```

---

### GET `/meta/warehouses`

Same as `/health` warehouses array, returned standalone.

---

## Performance Notes

- **In-memory tables** (`live_roas`, `roa_history`, `current_invalid`, `ca_certs`, `ca_resource_map`, built at startup and on every reload): `/roa/lookup`, `/roa/by-asn`, `/roa/history`, `/roa/export`, `/roa/coverage-trend`, `/analytics/churn`, `/analytics/maxlength-issues`, `/alerts/recent`, `/validity/*`, `/ca/roots`, `/ca/info`, `/ca/children`, `/ca/by-resource`. `/objects/summary` (default `current=true`), `/health` and `/meta/warehouses` answer from counts held in memory. Most other endpoints read Parquet files when they are called.
- **Startup**: about 6 minutes on the production warehouses (5 min 41 s on 2026-10-28, 6 min 47 s on 2026-10-29): it builds the five in-memory tables and caches the object counts.
- **Reload** (`POST /admin/reload`): usually 6 to 15 minutes, occasionally about 20 (2026-11-02), while the old tables keep serving.
