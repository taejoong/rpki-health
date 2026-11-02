"""Pydantic response models."""

from pydantic import BaseModel


class ROAEntry(BaseModel):
    rir: str
    asn: int
    prefix: str
    max_length: int
    afi: int  # 4=IPv4, 6=IPv6
    object_hash: str | None = None
    signing_time: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class PrefixLookupResponse(BaseModel):
    query_prefix: str
    query_asn: int | None = None
    count: int
    results: list[ROAEntry]


class ASNLookupResponse(BaseModel):
    query_asn: int
    count: int
    results: list[ROAEntry]


# --- Phase 2: History ---

class ROAHistoryEntry(BaseModel):
    rir: str
    asn: int
    prefix: str
    max_length: int
    afi: int
    object_hash: str | None = None
    semantic_hash: str | None = None
    signing_time: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class ROAHistoryResponse(BaseModel):
    query_prefix: str | None = None
    query_asn: int | None = None
    count: int
    truncated: bool = False
    entries: list[ROAHistoryEntry]


# --- Phase 2: Churn ---

class ChurnDataPoint(BaseModel):
    period: str  # YYYY-MM or YYYY-WNN
    new: int
    withdrawn: int
    net: int


class ChurnResponse(BaseModel):
    rir: str
    granularity: str
    start: str | None = None
    end: str | None = None
    data: list[ChurnDataPoint]


# --- Phase 2: Object Inventory ---

class ObjectCount(BaseModel):
    object_type: str
    count: int


class ObjectSummaryResponse(BaseModel):
    rir: str
    snapshot: str | None = None
    objects: list[ObjectCount]


# --- Phase 2: Lifespan ---

class LifespanStats(BaseModel):
    rir: str
    object_type: str
    count: int
    avg_days: float
    median_days: float
    p10_days: float
    p90_days: float


class LifespanResponse(BaseModel):
    results: list[LifespanStats]


# --- Phase 3: CA Explorer ---

class CAInfoResponse(BaseModel):
    rir: str
    cert_id: int
    object_id: int
    subject_dn: str | None = None
    ski: str | None = None
    aki: str | None = None
    not_before: str | None = None
    not_after: str | None = None
    resources_ip: str | None = None
    resources_asn: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class CAChildEntry(BaseModel):
    cert_id: int
    subject_dn: str | None = None
    ski: str | None = None
    is_ca: bool | None = None
    not_before: str | None = None
    not_after: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class CAStatsResponse(BaseModel):
    rir: str
    cert_id: int
    total_signed: int
    by_type: dict[str, int]


class CARootEntry(BaseModel):
    cert_id: int
    subject_dn: str | None = None
    ski: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class CAByResourceEntry(BaseModel):
    rir: str
    cert_id: int
    subject_dn: str | None = None
    ski: str | None = None
    not_after: str | None = None
    prefix_count: int
    last_seen: str | None = None
    all_valid: bool | None = None


class CAByResourceResponse(BaseModel):
    asn: int
    ca_count: int
    results: list[CAByResourceEntry]


# --- Phase 3: Signing Cadence ---

class CadenceBucket(BaseModel):
    interval_hours: str  # range label, e.g. "0-6", "6-12"
    count: int


class SigningCadenceResponse(BaseModel):
    rir: str
    signer_cert_id: int | None = None
    ca_aki: str | None = None
    total_re_signs: int
    avg_interval_hours: float
    median_interval_hours: float
    buckets: list[CadenceBucket]


# --- Phase 3: CRL Staleness ---

class StaleCRLEntry(BaseModel):
    rir: str
    issuer_dn: str
    aki: str | None = None
    this_update: str | None = None
    next_update: str | None = None
    staleness_hours: float
    revoked_count: int


class CRLStalenessResponse(BaseModel):
    rir: str
    total_live_crls: int
    stale_count: int
    entries: list[StaleCRLEntry]


# --- Phase 3: MaxLength Issues ---

class MaxLengthIssue(BaseModel):
    rir: str
    prefix: str
    prefix_length: int
    max_length: int
    excess: int
    asn: int


class MaxLengthResponse(BaseModel):
    rir: str
    total_roas: int
    issues_count: int
    entries: list[MaxLengthIssue]


# --- Phase 4: ASPA ---

class ASPAEntry(BaseModel):
    rir: str
    customer_asn: int
    provider_asn: int
    afi_limit: str | None = None
    signing_time: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class ASPAListResponse(BaseModel):
    count: int
    entries: list[ASPAEntry]


class ASPAStatsResponse(BaseModel):
    total_live_objects: int
    total_customer_asns: int
    per_rir: dict[str, dict]


class ASPATrendPoint(BaseModel):
    period: str
    new_objects: int
    new_customer_asns: int


# --- Phase 4: RIR Comparison ---

class RIRDataPoint(BaseModel):
    period: str
    afrinic: int = 0
    apnic: int = 0
    arin: int = 0
    lacnic: int = 0
    ripencc: int = 0


class RIRComparisonResponse(BaseModel):
    metric: str
    granularity: str
    data: list[RIRDataPoint]


# --- Phase 4: Change Alerts ---

class ROAChange(BaseModel):
    rir: str
    change_type: str  # "new", "withdrawn", "modified"
    asn: int
    prefix: str
    max_length: int
    object_hash: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class RecentChangesResponse(BaseModel):
    since: str
    count: int
    changes: list[ROAChange]


# --- Meta ---

class WarehouseInfo(BaseModel):
    rir: str
    latest_snapshot: str | None = None
    snapshot_count: int = 0
    object_count: int | None = None
    unique_object_count: int | None = None
    live_roa_count: int = 0   # distinct ROA objects
    live_vrp_count: int = 0   # distinct (asn, prefix, max_length) payloads


class HealthResponse(BaseModel):
    status: str
    live_roa_total: int       # distinct ROA objects
    live_vrp_total: int = 0   # distinct VRPs (asn, prefix, max_length)
    warehouses: list[WarehouseInfo]


class WarehouseListResponse(BaseModel):
    warehouses: list[WarehouseInfo]


# --- RP-validity ---

class ValidityReasonCount(BaseModel):
    reason_code: str
    count: int


class ValiditySummaryResponse(BaseModel):
    rir: str  # specific RIR or "all"
    latest_validation_ts: str | None = None  # MAX(dataset_ts); null until first cycle
    repo_present: int   # currently-live objects (everything rpki-client cached)
    rp_valid: int       # repo_present minus invalid
    invalid: int        # objects rpki-client rejected in the latest cycle
    by_reason: list[ValidityReasonCount]


class InvalidObjectEntry(BaseModel):
    rir: str
    object_id: int
    object_type: str            # ROA / Certificate / Manifest / ASPA / CRL / Unknown
    reason_code: str
    detail: str | None = None   # human label (ASN, subject_dn, issuer_dn, …)
    object_hash: str | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class InvalidObjectsResponse(BaseModel):
    rir: str                    # specific RIR or "all"
    reason: str | None = None   # reason_code filter, if any
    count: int                  # total matching before limit/offset
    truncated: bool = False
    entries: list[InvalidObjectEntry]
