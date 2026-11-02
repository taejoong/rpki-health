"""CA hierarchy explorer endpoints."""

from fastapi import APIRouter, HTTPException, Query

from app.config import RIRS, WAREHOUSE_DIRS
from app.db import _presence_src, get_con, live_cutoff, parquet_glob
from app.models import (
    CAByResourceEntry,
    CAByResourceResponse,
    CAChildEntry,
    CAInfoResponse,
    CARootEntry,
    CAStatsResponse,
)

router = APIRouter(prefix="/ca", tags=["CA"])


@router.get("/roots", response_model=list[CARootEntry])
def ca_roots(
    rir: str = Query(..., description="RIR name"),
):
    """List the trust-anchor root CA(s) for an RIR — the natural entry point.

    A TA root is a self-signed CA cert (AKI absent or equal to its own SKI).
    The cert is re-issued over time, so we dedup by SKI and keep the most
    recently observed instance, dropping long-dead historical roots (>90 days
    behind the warehouse's latest observation).
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}")

    con = get_con()

    rows = con.execute(
        """
        SELECT cert_id, subject_dn, ski, first_seen, last_seen
        FROM (
            SELECT cert_id, subject_dn, ski,
                   first_seen_ts AS first_seen, last_seen_ts AS last_seen,
                   ROW_NUMBER() OVER (
                       PARTITION BY ski ORDER BY last_seen_ts DESC
                   ) AS rn,
                   MAX(last_seen_ts::TIMESTAMP) OVER () AS max_seen
            FROM ca_certs
            WHERE rir = $1
              AND (aki IS NULL OR aki = '' OR aki = ski)
        )
        WHERE rn = 1
          AND last_seen::TIMESTAMP >= max_seen - INTERVAL 90 DAY
        ORDER BY last_seen DESC
        """,
        [rir],
    ).fetchall()

    return [
        CARootEntry(
            cert_id=r[0], subject_dn=r[1], ski=r[2],
            first_seen=r[3], last_seen=r[4],
        )
        for r in rows
    ]


@router.get("/info", response_model=CAInfoResponse)
def ca_info(
    rir: str = Query(..., description="RIR name"),
    ski: str | None = Query(None, description="Subject Key Identifier"),
    cert_id: int | None = Query(None, description="Certificate ID"),
):
    """Get info about a CA certificate."""
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}")
    if ski is None and cert_id is None:
        raise HTTPException(400, "Provide ski or cert_id")

    con = get_con()

    if ski:
        where = "ski = $2"
        params = [rir, ski]
    else:
        where = "cert_id = $2"
        params = [rir, cert_id]

    row = con.execute(
        f"""
        SELECT cert_id, object_id, subject_dn, ski, aki,
               not_before, not_after,
               resources_ip, resources_asn,
               first_seen_ts, last_seen_ts
        FROM ca_certs
        WHERE rir = $1 AND {where}
        ORDER BY last_seen_ts DESC
        LIMIT 1
        """,
        params,
    ).fetchone()

    if not row:
        raise HTTPException(404, "CA not found")

    return CAInfoResponse(
        rir=rir, cert_id=row[0], object_id=row[1], subject_dn=row[2],
        ski=row[3], aki=row[4], not_before=row[5], not_after=row[6],
        resources_ip=row[7], resources_asn=row[8],
        first_seen=row[9], last_seen=row[10],
    )


@router.get("/children", response_model=list[CAChildEntry])
def ca_children(
    rir: str = Query(..., description="RIR name"),
    cert_id: int = Query(..., description="Parent CA cert_id"),
    current: bool = Query(True, description="Only live children"),
    limit: int = Query(100, ge=1, le=1000),
):
    """List direct child CA certificates, derived live from the AKI→SKI link.

    We do NOT use the materialized cert_chain_edges table: it is heavily
    incomplete because the ETL only parses never-before-seen objects, so a stable
    parent cert is absent from the run that parses its children and no edge gets
    recorded. The certs table itself is complete, so we derive the hierarchy
    directly — a child's AKI equals its issuer's SKI (plus issuer_dn = parent
    subject_dn as a guard against SKI collisions). Re-issued children sharing a
    SKI are deduped to the most recently observed instance.
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}")

    con = get_con()
    cutoff = live_cutoff(rir)

    live_clause = f"AND c.last_seen_ts >= '{cutoff}'" if current else ""

    rows = con.execute(
        f"""
        WITH parent AS (
            SELECT ski, subject_dn FROM ca_certs
            WHERE rir = $1 AND cert_id = $2 AND ski IS NOT NULL
            LIMIT 1
        )
        SELECT cert_id, subject_dn, ski, TRUE AS is_ca,
               not_before, not_after, first_seen, last_seen
        FROM (
            SELECT c.cert_id, c.subject_dn, c.ski,
                   c.not_before, c.not_after,
                   c.first_seen_ts AS first_seen, c.last_seen_ts AS last_seen,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.ski ORDER BY c.last_seen_ts DESC
                   ) AS rn
            FROM ca_certs c
            JOIN parent p ON c.aki = p.ski AND c.issuer_dn = p.subject_dn
            WHERE c.rir = $1
              AND c.cert_id <> $2
              AND c.ski <> c.aki
              {live_clause}
        )
        WHERE rn = 1
        ORDER BY subject_dn
        LIMIT {limit}
        """,
        [rir, cert_id],
    ).fetchall()

    return [
        CAChildEntry(
            cert_id=r[0], subject_dn=r[1], ski=r[2], is_ca=r[3],
            not_before=r[4], not_after=r[5], first_seen=r[6], last_seen=r[7],
        )
        for r in rows
    ]


@router.get("/stats", response_model=CAStatsResponse)
def ca_stats(
    rir: str = Query(..., description="RIR name"),
    cert_id: int = Query(..., description="Signer certificate ID"),
):
    """Live signed objects (ROA, MFT, ASPA, GBR) whose CMS signer certificate is cert_id, by type.

    Signed objects are signed by one-time EE certificates, never by a CA
    certificate, so a CA's cert_id gives total_signed 0 and an empty by_type.
    """
    rir = rir.lower()
    if rir not in RIRS:
        raise HTTPException(400, f"Unknown RIR: {rir}")

    con = get_con()
    sigs_pq = parquet_glob(rir, "object_signatures")
    obj_fl = parquet_glob(rir, "object_presence_stats")
    cutoff = live_cutoff(rir)

    rows = con.execute(
        f"""
        SELECT s.object_type, COUNT(*) AS cnt
        FROM read_parquet('{sigs_pq}') s
        JOIN {_presence_src(obj_fl)} ofl ON s.object_id = ofl.object_id
        WHERE s.signer_cert_id = $1
          AND ofl.last_seen_ts >= '{cutoff}'
        GROUP BY s.object_type
        ORDER BY cnt DESC
        """,
        [cert_id],
    ).fetchall()

    type_counts = {r[0]: r[1] for r in rows}
    total = sum(type_counts.values())

    return CAStatsResponse(
        rir=rir, cert_id=cert_id, total_signed=total, by_type=type_counts,
    )


@router.get("/by-resource", response_model=CAByResourceResponse)
def ca_by_resource(
    asn: int = Query(..., description="ASN to find the covering CA(s) for"),
    limit: int = Query(200, ge=1, le=1000),
):
    """Reverse lookup: which CA(s) issued the live ROAs covering an ASN.

    Anchored on ROAs (CA certs themselves inherit resources, so they carry no
    explicit ASN/prefix). Each ROA is signed by a one-time EE cert whose issuer
    is the member CA; the precomputed ca_resource_map resolves that hop. Results
    are the issuing CA(s) ranked by how many of the ASN's prefixes they cover,
    each jumpable into the CA hierarchy via (rir, cert_id).
    """
    con = get_con()
    rows = con.execute(
        """
        SELECT rir, ca_cert_id, ca_subject_dn, ca_ski,
               MAX(ca_not_after) AS not_after,
               COUNT(DISTINCT prefix) AS prefix_count,
               MAX(last_seen_ts) AS last_seen,
               BOOL_AND(is_valid) AS all_valid
        FROM ca_resource_map
        WHERE asn = $1
        GROUP BY rir, ca_cert_id, ca_subject_dn, ca_ski
        ORDER BY prefix_count DESC, ca_cert_id
        LIMIT $2
        """,
        [asn, limit],
    ).fetchall()

    results = [
        CAByResourceEntry(
            rir=r[0], cert_id=r[1], subject_dn=r[2], ski=r[3],
            not_after=str(r[4]) if r[4] is not None else None,
            prefix_count=r[5],
            last_seen=str(r[6]) if r[6] is not None else None,
            all_valid=r[7],
        )
        for r in rows
    ]

    return CAByResourceResponse(asn=asn, ca_count=len(results), results=results)
