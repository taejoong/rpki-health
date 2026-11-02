"""Command line entry point for the RPKI ETL pipeline."""
from __future__ import annotations

import json
import logging
import multiprocessing as mp

import multiprocessing.util as mu

import os
import re
import sqlite3
from collections import Counter, defaultdict

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone, time as _time
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple


import click
from tqdm import tqdm


from cas import SQLiteKV, make_cas_backend
from index import write_membership
from parse_cms import detect_cms_object_type
from parse_crl import ParsedCRL, parse_crl
from tables import ParquetBatchWriter, schema_crls, schema_crl_serials
from util import clean_dn, compute_sha256, dataset_day_to_id
from workers import ParsedCertificateData, WorkerParseResult, parse_worker


import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd



_EASTERN_TZ = ZoneInfo("America/New_York")


def _timestamped_desc(label: str, extra: Optional[str] = None) -> str:
    """Return a tqdm description prefixed with the current Eastern timestamp."""

    timestamp = datetime.now(_EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    parts = [timestamp, label]
    if extra:
        parts.append(extra)

    return " ".join(parts)


# ``parse_inputs`` used to be a module-level list that tests could inspect
# after invoking the CLI. Maintain it as a compatibility shim so external
# tooling that still references ``rpki_etl.parse_inputs`` does not crash with
# a ``NameError`` now that the worker arguments are built inside helper
# functions.
parse_inputs: List[Tuple[str, str]] = []

logger = logging.getLogger("rpki_etl")

# Regexes for date/datetime detection
_RE_YMD = re.compile(r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})$")
_RE_YMD_HMS = re.compile(
    r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})[ _](?P<H>\d{2})[:\-](?P<M>\d{2})[:\-](?P<S>\d{2})$"
)
_RE_YYYY = re.compile(r"^(?P<y>\d{4})$")
_RE_MM = re.compile(r"^(?P<m>0[1-9]|1[0-2])$")
_RE_DD = re.compile(r"^(?P<d>0[1-9]|[12]\d|3[01])$")

@dataclass
class HashedObject:
    path: Path
    object_hash: Optional[str]
    detected_type: Optional[str]
    size: int
    error: Optional[str] = None


def _parse_hms(value: str) -> _time:
    parts = value.split(":")
    if len(parts) == 2:
        hour_str, minute_str = parts
        second_str = "0"
    elif len(parts) == 3:
        hour_str, minute_str, second_str = parts
    else:
        raise ValueError(f"invalid time string: {value!r}")

    hour = int(hour_str)
    minute = int(minute_str)
    second = int(second_str)
    if not (0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60):
        raise ValueError(f"invalid time: {value!r}")
    return _time(hour=hour, minute=minute, second=second, tzinfo=timezone.utc)


def _infer_date_from_path(
    path: Path,
) -> Optional[Tuple[int, int, int, Optional[int], Optional[int], Optional[int]]]:
    parts = list(path.parts)

    for comp in reversed(parts):
        match = _RE_YMD_HMS.match(comp)
        if match:
            return (
                int(match["y"]),
                int(match["m"]),
                int(match["d"]),
                int(match["H"]),
                int(match["M"]),
                int(match["S"]),
            )

    for comp in reversed(parts):
        match = _RE_YMD.match(comp)
        if match:
            return (
                int(match["y"]),
                int(match["m"]),
                int(match["d"]),
                None,
                None,
                None,
            )

    for index in range(len(parts) - 2):
        year = _RE_YYYY.match(parts[index])
        if not year:
            continue
        month = _RE_MM.match(parts[index + 1])
        day = _RE_DD.match(parts[index + 2])
        if year and month and day:
            return (
                int(year["y"]),
                int(month["m"]),
                int(day["d"]),
                None,
                None,
                None,
            )

    return None


def _infer_tal_id_from_path(path: Path) -> Optional[str]:
    """Return the TAL identifier if it can be deduced from *path*.

    The repository snapshots produced by the crawler are usually laid out as::

        /.../<tal-id>/<tal-id>.tal/<YYYY>/<MM>/...

    When ``--tal-id`` is omitted we look for the first path component that ends
    with ``.tal`` and treat its stem as the TAL identifier.  ``None`` is
    returned when no such component is present so the caller can surface a
    helpful error message.
    """

    for component in reversed(path.parts):
        if component.lower().endswith(".tal"):
            tal_id = Path(component).stem
            if tal_id:
                return tal_id
    return None


def _discover_files_grouped_by_ts(
    input_root: Path,
    default_time: _time,
    force_time: Optional[_time],
    exclude_set: Set[str],
) -> Tuple[Dict[str, List[Path]], List[Dict[str, str]], int]:
    ts_to_files: Dict[str, List[Path]] = defaultdict(list)
    errors: List[Dict[str, str]] = []
    pruned = 0

    for root, dirs, files in os.walk(input_root, topdown=True):
        before = len(dirs)
        dirs[:] = [d for d in dirs if d.lower() not in exclude_set]
        pruned += before - len(dirs)

        root_path = Path(root)
        for filename in files:
            file_path = root_path / filename
            inferred = _infer_date_from_path(file_path)
            if inferred is None:
                errors.append(
                    {
                        "path": file_path.as_posix(),
                        "error": "no date or datetime in path",
                        "snapshot": None,
                    }
                )
                continue

            year, month, day, hour, minute, second = inferred
            if force_time is not None:
                hour, minute, second = (
                    force_time.hour,
                    force_time.minute,
                    force_time.second,
                )
            elif hour is None or minute is None or second is None:
                hour, minute, second = (
                    default_time.hour,
                    default_time.minute,
                    default_time.second,
                )

            dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
            label = dt.strftime("%Y-%m-%d %H:%M:%S")
            ts_to_files[label].append(file_path)

    return ts_to_files, errors, pruned


def _hash_path(path: Path) -> Tuple[Optional[str], int, Path, Optional[str]]:
    try:
        data = path.read_bytes()
    except Exception as exc:  # pragma: no cover - defensive I/O
        return None, 0, path, str(exc)
    return compute_sha256(data), len(data), path, None


@dataclass
class CertIndexEntry:
    cert_id: int
    is_ca: bool
    subject_dn: str
    issuer_dn: str
    serial_dec: str
    serial_bin: str
    ski: Optional[str]
    aki: Optional[str]
    not_before: datetime
    not_after: datetime


@dataclass
class Snapshot:
    ts: datetime
    label: str
    dir: Path
    file_count: int = 0


_SNAPSHOT_PATTERNS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d_%H-%M-%S",
    "%Y-%m-%d",
]


def _parse_snapshot_ts(dirname: str) -> Optional[datetime]:
    for pattern in _SNAPSHOT_PATTERNS:
        try:
            dt = datetime.strptime(dirname, pattern)
        except ValueError:
            continue
        if pattern == "%Y-%m-%d":
            dt = dt.replace(hour=0, minute=0, second=0)
        return dt.replace(tzinfo=timezone.utc)
    return None


def _discover_snapshots(input_root: Path) -> List[Snapshot]:
    snapshots: List[Snapshot] = []
    for child in sorted(input_root.iterdir()):
        if not child.is_dir():
            continue
        ts = _parse_snapshot_ts(child.name)
        if ts is None:
            continue
        label = ts.strftime("%Y-%m-%d %H:%M:%S")
        snapshots.append(Snapshot(ts=ts, label=label, dir=child))
    snapshots.sort(key=lambda snap: snap.ts)
    return snapshots



def _hash_probe(path: Path) -> HashedObject:
    try:
        data = path.read_bytes()
    except Exception as exc:  # pragma: no cover - defensive I/O
        return HashedObject(path=path, object_hash=None, detected_type=None, size=0, error=str(exc))

    object_hash = compute_sha256(data)
    detected_type: Optional[str] = None
    cms_result = detect_cms_object_type(data)
    if cms_result is not None:
        detected_type = cms_result.object_type
    return HashedObject(
        path=path,
        object_hash=object_hash,
        detected_type=detected_type,
        size=len(data),
    )


# RP-validity states recorded in the object_validity table (see build_validity_map.py).
VALIDATION_UNKNOWN = 0
VALIDATION_VALID = 1
VALIDATION_INVALID = 2


def load_validity_map(
    path: Optional[Path],
) -> Dict[str, Tuple[int, Optional[str]]]:
    """Load the RP-validity sidecar keyed by object_hash.

    Expected JSON shape: ``{"<object_hash>": {"state": <int>, "reason": <str|null>}}``
    (a ``[state, reason]`` list is also accepted). Returns an empty map when
    ``path`` is None, so every parsed object defaults to valid.
    """
    if path is None:
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    result: Dict[str, Tuple[int, Optional[str]]] = {}
    for object_hash, rec in raw.items():
        if isinstance(rec, dict):
            state = int(rec.get("state", VALIDATION_INVALID))
            reason = rec.get("reason")
        elif isinstance(rec, (list, tuple)):
            state = int(rec[0])
            reason = rec[1] if len(rec) > 1 else None
        else:
            state = VALIDATION_INVALID
            reason = str(rec)
        result[object_hash] = (state, reason)
    return result


def _tally_detected_object_types(items: Sequence[HashedObject]) -> Counter[str]:
    """Return a case-normalized frequency table of detected CMS object types."""

    counts: Counter[str] = Counter()
    for item in items:
        detected = (item.detected_type or "unknown").upper()
        counts[detected] += 1
    return counts


def _log_detected_object_mix(items: Sequence[HashedObject]) -> None:
    """Emit a summary of the CMS object mix observed during the read stage."""

    counts = _tally_detected_object_types(items)
    total = sum(counts.values())
    roa_count = counts.get("ROA", 0)
    mft_count = counts.get("MFT", 0)
    other_count = total - roa_count - mft_count

    logger.info(
        "Detected %d ROAs, %d MFTs, and %d other objects before hashing (%d total)",
        roa_count,
        mft_count,
        other_count,
        total,
    )

    if other_count:
        other_types = {
            object_type: count
            for object_type, count in counts.items()
            if object_type not in {"ROA", "MFT"}
        }
        if other_types:
            breakdown = ", ".join(
                f"{object_type}: {count}"
                for object_type, count in sorted(other_types.items())
            )
            logger.info("Other object breakdown: %s", breakdown)

def _parse_object(args: Sequence[str]) -> WorkerParseResult:
    """Backward-compatible wrapper for the worker parser.

    The tests and some external tooling import ``_parse_object`` directly to
    exercise the parsing pipeline without spinning up multiprocessing workers.
    The function used to live in this module prior to the refactor that
    introduced :func:`parse_worker`. Reintroduce it as a thin wrapper so the
    public surface remains stable while keeping :func:`parse_worker` as the
    implementation detail consumed by the worker pool.
    """


    return parse_worker(args)


@dataclass
class PipelineState:
    repo_id: str
    tal_id: str
    kv: SQLiteKV
    flush_rows: int = 0
    crl_writer: Optional[ParquetBatchWriter] = None
    crl_serial_writer: Optional[ParquetBatchWriter] = None
    roa_prefix_rows: List[Dict[str, object]] = field(default_factory=list)
    roa_object_rows: List[Dict[str, object]] = field(default_factory=list)
    object_signature_rows: List[Dict[str, object]] = field(default_factory=list)
    roas_by_object: Dict[int, Dict[str, object]] = field(default_factory=dict)
    signer_by_object: Dict[int, Optional[int]] = field(default_factory=dict)
    manifest_rows: List[Dict[str, object]] = field(default_factory=list)
    manifest_entry_rows: List[Dict[str, object]] = field(default_factory=list)
    aspa_object_rows: List[Dict[str, object]] = field(default_factory=list)
    aspa_provider_rows: List[Dict[str, object]] = field(default_factory=list)
    gbr_object_rows: List[Dict[str, object]] = field(default_factory=list)
    crl_serial_rows: List[Dict[str, object]] = field(default_factory=list)
    cert_rows: Dict[int, Dict[str, object]] = field(default_factory=dict)
    cert_index: Dict[int, CertIndexEntry] = field(default_factory=dict)
    cert_hash_to_id: Dict[str, int] = field(default_factory=dict)
    certs_by_ski: Dict[str, List[int]] = field(default_factory=lambda: defaultdict(list))
    certs_by_subject_dn: Dict[str, List[int]] = field(default_factory=lambda: defaultdict(list))
    certs_by_issuer_serial: Dict[Tuple[str, str], List[int]] = field(
        default_factory=lambda: defaultdict(list)
    )
    certs_by_issuer_serial_dec: Dict[Tuple[str, str], List[int]] = field(
        default_factory=lambda: defaultdict(list)
    )
    ee_revocation_rows: List[Dict[str, object]] = field(default_factory=list)
    cert_chain_rows: List[Dict[str, object]] = field(default_factory=list)
    epoch: datetime = field(
        default_factory=lambda: datetime(1970, 1, 1, tzinfo=timezone.utc)
    )

    def _emit_stream_row(
        self,
        writer: Optional[ParquetBatchWriter],
        table_name: str,
        primary_key: Optional[str],
        row: Dict[str, object],
    ) -> None:
        if writer is None:
            return
        if primary_key is not None and not self.kv.mark_emitted(table_name, primary_key):
            return
        writer.append(row)
        if self.flush_rows > 0 and len(writer.rows) >= self.flush_rows:
            writer.flush()

    def flush_streaming_tables(self) -> None:
        for writer in (self.crl_writer, self.crl_serial_writer):
            if writer is not None:
                writer.flush()

    def register_parsed_certificate(
        self,
        data: Optional[ParsedCertificateData],
        object_id: Optional[int],
    ) -> Optional[int]:
        if data is None:
            return None

        cert_hash = data.cert_hash
        existing = self.cert_hash_to_id.get(cert_hash)
        if existing is None:
            existing = self.kv.get_or_create_cert_id(cert_hash)
            self.cert_hash_to_id[cert_hash] = existing
        parsed_cert = data.parsed
        not_before = _ensure_utc(parsed_cert.not_before)
        not_after = _ensure_utc(parsed_cert.not_after)
        if existing in self.cert_rows:
            row = self.cert_rows[existing]
            if object_id is not None and row.get("object_id") is None:
                row["object_id"] = object_id
            entry = self.cert_index[existing]
            if entry.not_before is None:
                entry.not_before = not_before
            if entry.not_after is None:
                entry.not_after = not_after
            return existing

        cert_id = existing
        row = {
            "cert_id": cert_id,
            "object_id": object_id,
            "cert_hash": cert_hash,
            "is_ca_flag": parsed_cert.is_ca,
            "subject_dn": parsed_cert.subject,
            "issuer_dn": parsed_cert.issuer,
            "serial_dec": parsed_cert.serial_dec,
            "serial_bin": parsed_cert.serial_bin,
            "ski": parsed_cert.ski,
            "aki": parsed_cert.aki,
            "not_before": not_before,
            "not_after": not_after,
            "spki_alg": parsed_cert.spki_algorithm,
            "spki_bits": parsed_cert.spki_bits,
            "spki_sha256": parsed_cert.spki_sha256,
            "resources_ip": parsed_cert.resources_ip,
            "resources_asn": parsed_cert.resources_asn,
            "sia": parsed_cert.sia,
            "aia": parsed_cert.aia,
            "cdp": parsed_cert.cdp,
            "key_usage": parsed_cert.key_usage,
            "ext_key_usage": parsed_cert.ext_key_usage,
            "basic_constraints": parsed_cert.basic_constraints,
            "repo_id": self.repo_id,
            "tal_id": self.tal_id,
        }
        self.cert_rows[cert_id] = row
        entry = CertIndexEntry(
            cert_id=cert_id,
            is_ca=parsed_cert.is_ca,
            subject_dn=parsed_cert.subject,
            issuer_dn=parsed_cert.issuer,
            serial_dec=parsed_cert.serial_dec,
            serial_bin=parsed_cert.serial_bin,
            ski=parsed_cert.ski,
            aki=parsed_cert.aki,
            not_before=not_before
            if not_before is not None
            else _ensure_utc(parsed_cert.not_before),
            not_after=not_after
            if not_after is not None
            else _ensure_utc(parsed_cert.not_after),
        )
        self.cert_index[cert_id] = entry
        if parsed_cert.ski is not None:
            self.certs_by_ski[parsed_cert.ski].append(cert_id)
        self.certs_by_subject_dn[parsed_cert.subject].append(cert_id)
        self.certs_by_issuer_serial[(parsed_cert.issuer, parsed_cert.serial_bin)].append(
            cert_id
        )
        self.certs_by_issuer_serial_dec[(parsed_cert.issuer, parsed_cert.serial_dec)].append(
            cert_id
        )
        self.cert_hash_to_id[cert_hash] = cert_id
        return cert_id

    def index_result_certificates(
        self,
        result: Optional[WorkerParseResult],
        object_id: Optional[int],
    ) -> None:
        if result is None:
            return
        self.register_parsed_certificate(result.certificate, object_id)
        if result.cms is not None:
            for cert_data in result.cms.embedded_certs:
                self.register_parsed_certificate(cert_data, None)
            self.register_parsed_certificate(result.cms.signer_cert, None)

    def select_best_candidate(
        self, candidates: List[CertIndexEntry], reference: datetime
    ) -> Optional[int]:
        if not candidates:
            return None
        covering = [
            c
            for c in candidates
            if c.not_before is not None
            and c.not_after is not None
            and c.not_before <= reference <= c.not_after
        ]
        if covering:
            candidates = covering
        candidates.sort(key=lambda c: c.not_before or self.epoch, reverse=True)
        return candidates[0].cert_id

    def resolve_crl_issuer(
        self, parsed_crl: ParsedCRL, reference: datetime
    ) -> Optional[int]:
        candidates: List[CertIndexEntry] = []
        if parsed_crl.authority_key_identifier is not None:
            for cert_id in self.certs_by_ski.get(parsed_crl.authority_key_identifier, []):
                candidate = self.cert_index[cert_id]
                if candidate.subject_dn == parsed_crl.issuer:
                    candidates.append(candidate)
        if not candidates:
            for cert_id in self.certs_by_subject_dn.get(parsed_crl.issuer, []):
                candidates.append(self.cert_index[cert_id])
        candidates = [c for c in candidates if c.is_ca]
        return self.select_best_candidate(candidates, reference)

    def resolve_revoked_cert_id(
        self,
        issuer_dn: str,
        serial_bin: str,
        serial_dec: str,
        reference: datetime,
    ) -> Optional[int]:
        candidates: List[CertIndexEntry] = []
        for cert_id in self.certs_by_issuer_serial.get((issuer_dn, serial_bin), []):
            candidates.append(self.cert_index[cert_id])
        if not candidates:
            for cert_id in self.certs_by_issuer_serial_dec.get(
                (issuer_dn, serial_dec), []
            ):
                candidates.append(self.cert_index[cert_id])
        candidates = [c for c in candidates if not c.is_ca]
        return self.select_best_candidate(candidates, reference)

    def process_result(
        self, result: Optional[WorkerParseResult], object_id: int, object_hash: str
    ) -> None:
        if result is None:
            return
        self.signer_by_object[object_id] = None
        if result.cms is not None:
            cms = result.cms
            signer_cert_id = None
            if cms.signer_cert is not None:
                signer_cert_id = self.cert_hash_to_id.get(cms.signer_cert.cert_hash)
            if signer_cert_id is None and cms.serial_number_bytes and cms.issuer_dn:
                for cert_id in self.certs_by_issuer_serial.get(
                    (cms.issuer_dn, cms.serial_number_bytes), []
                ):
                    signer_cert_id = cert_id
                    break
            if signer_cert_id is None and cms.serial_number and cms.issuer_dn:
                for cert_id in self.certs_by_issuer_serial_dec.get(
                    (cms.issuer_dn, cms.serial_number), []
                ):
                    signer_cert_id = cert_id
                    break
            if signer_cert_id is None and cms.subject_key_identifier:
                for cert_id in self.certs_by_ski.get(cms.subject_key_identifier, []):
                    signer_cert_id = cert_id
                    break
            self.signer_by_object[object_id] = signer_cert_id
            self.object_signature_rows.append(
                {
                    "object_id": object_id,
                    "object_type": cms.object_type,
                    "signer_cert_id": signer_cert_id,
                    "content_type_oid": cms.content_type_oid,
                    "digest_algorithm": cms.digest_algorithm,
                    "signature_algorithm": cms.signature_algorithm,
                    "signing_time": _ensure_utc(cms.signing_time),
                    "message_digest": cms.message_digest,
                }
            )
            if result.roa is not None:
                roa_info = result.roa
                self.roa_object_rows.append(
                    {
                        "object_id": object_id,
                        "object_hash": object_hash,
                        "semantic_hash": roa_info.semantic_hash,
                        "asn": roa_info.parsed.asn,
                        "ee_serial": roa_info.parsed.ee_serial,
                        "ee_not_before": _ensure_utc(roa_info.parsed.ee_not_before),
                        "ee_not_after": _ensure_utc(roa_info.parsed.ee_not_after),
                        "signing_time": _ensure_utc(roa_info.parsed.signing_time),
                        "ee_public_key": roa_info.parsed.ee_public_key,
                        "repo_id": self.repo_id,
                        "tal_id": self.tal_id,
                    }
                )
                self.roas_by_object[object_id] = {
                    "asn": roa_info.parsed.asn,
                    "semantic_hash": roa_info.semantic_hash,
                }
                for prefix in roa_info.parsed.prefixes:
                    self.roa_prefix_rows.append(
                        {
                            "object_id": object_id,
                            "afi": prefix.afi,
                            "prefix": prefix.prefix,
                            "max_length": prefix.max_length,
                        }
                    )
            if result.manifest is not None:
                manifest = result.manifest
                self.manifest_rows.append(
                    {
                        "object_id": object_id,
                        "manifest_number": manifest.manifest_number,
                        "this_update": manifest.this_update,
                        "next_update": manifest.next_update,
                        "file_count": len(manifest.files),
                    }
                )
                for filename, file_hash in manifest.files:
                    self.manifest_entry_rows.append(
                        {
                            "object_id": object_id,
                            "filename": filename,
                            "hash_alg": manifest.file_hash_alg,
                            "file_hash": file_hash,
                        }
                    )
            if result.aspa is not None:
                aspa_info = result.aspa
                self.aspa_object_rows.append(
                    {
                        "object_id": object_id,
                        "object_hash": object_hash,
                        "customer_asn": aspa_info.customer_asn,
                        "ee_serial": aspa_info.ee_serial,
                        "ee_not_before": _ensure_utc(aspa_info.ee_not_before),
                        "ee_not_after": _ensure_utc(aspa_info.ee_not_after),
                        "signing_time": _ensure_utc(aspa_info.signing_time),
                        "ee_public_key": aspa_info.ee_public_key,
                        "repo_id": self.repo_id,
                        "tal_id": self.tal_id,
                    }
                )
                for provider in aspa_info.providers:
                    afi_limit_str = None
                    if provider.afi_limit:
                        afi_limit_str = ",".join(str(afi) for afi in provider.afi_limit)
                    self.aspa_provider_rows.append(
                        {
                            "object_id": object_id,
                            "provider_asn": provider.provider_asn,
                            "afi_limit": afi_limit_str,
                        }
                    )
            if result.gbr is not None:
                gbr_info = result.gbr
                self.gbr_object_rows.append(
                    {
                        "object_id": object_id,
                        "object_hash": object_hash,
                        "vcard": gbr_info.vcard,
                        "hcard": gbr_info.hcard,
                        "fn": gbr_info.fn,
                        "org": gbr_info.org,
                        "adr": gbr_info.adr,
                        "tel": gbr_info.tel,
                        "email": gbr_info.email,
                        "ee_serial": gbr_info.ee_serial,
                        "ee_not_before": _ensure_utc(gbr_info.ee_not_before),
                        "ee_not_after": _ensure_utc(gbr_info.ee_not_after),
                        "signing_time": _ensure_utc(gbr_info.signing_time),
                        "ee_public_key": gbr_info.ee_public_key,
                        "repo_id": self.repo_id,
                        "tal_id": self.tal_id,
                    }
                )
        elif result.crl is not None:
            parsed_crl = result.crl
            this_update = _ensure_utc(parsed_crl.this_update) or self.epoch
            next_update_dt = _ensure_utc(parsed_crl.next_update)
            issuer_cert_id = self.resolve_crl_issuer(parsed_crl, this_update)
            issuer_dn = clean_dn(parsed_crl.issuer) or parsed_crl.issuer
            crl_id = object_id
            header_row = {
                "crl_id": crl_id,
                "object_id": object_id,
                "issuer_dn": issuer_dn,
                "crl_number": parsed_crl.crl_number,
                "this_update": this_update,
                "next_update": next_update_dt,
                "revoked_count": int(parsed_crl.revoked_count),
                "aki": parsed_crl.authority_key_identifier,
                "signature_alg": parsed_crl.signature_algorithm,
                "repo_id": self.repo_id,
                "tal_id": self.tal_id,
            }
            self._emit_stream_row(
                self.crl_writer,
                "crls",
                str(crl_id),
                header_row,
            )
            for revoked in parsed_crl.revoked_certificates:
                entry_issuer_dn_raw = revoked.certificate_issuer or parsed_crl.issuer
                entry_issuer_dn = clean_dn(entry_issuer_dn_raw)
                revocation_dt = _ensure_utc(revoked.revocation_date) or this_update
                revoked_cert_id = self.resolve_revoked_cert_id(
                    entry_issuer_dn or issuer_dn,
                    revoked.serial_bin,
                    revoked.serial_dec,
                    revocation_dt,
                )
                serial_row = {
                    "crl_id": crl_id,
                    "serial_dec": revoked.serial_dec,
                    "serial_bin": revoked.serial_bin,
                    "revocation_date": revocation_dt,
                    "reason_code": revoked.reason_code,
                    "invalidity_date": _ensure_utc(revoked.invalidity_date),
                    "cert_issuer_dn": entry_issuer_dn,
                    "revoked_cert_id": revoked_cert_id,
                }
                self.crl_serial_rows.append(serial_row)
                self._emit_stream_row(
                    self.crl_serial_writer,
                    "crl_serials",
                    f"{crl_id}:{revoked.serial_dec}",
                    serial_row,
                )

    def seed_issuer_index_from_certs(self, table_root: Path) -> None:
        """Seed the cert index with accumulated CA certs from prior runs.

        The chain-edge builder resolves a subject's issuer through
        ``certs_by_ski`` / ``cert_index``, but incremental parsing means a stable
        parent CA cert is usually NOT re-parsed in the current run (CAS dedup), so
        it is absent from the in-memory index and the child's edge is silently
        dropped. We load every historically-observed CA cert (``is_ca_flag =
        true`` — at most a few hundred thousand rows vs. tens of millions of EE
        certs, so cheap) into the issuer-lookup indexes. Current-run entries take
        precedence. Seeded CA certs also become subjects, so the builder emits the
        full CA→CA topology on the next run (edge dedup via ``kv.mark_emitted``
        keeps it idempotent). EE-cert edges remain current-run only.
        """
        certs_dir = table_root / "table=certs"
        try:
            next(certs_dir.rglob("*.parquet"))
        except StopIteration:
            return
        try:
            import duckdb
        except Exception:  # pragma: no cover - optional dependency missing
            return

        glob = str(certs_dir / "**" / "*.parquet").replace("'", "''")
        con = duckdb.connect()
        try:
            con.execute(f"PRAGMA threads={max(1, os.cpu_count() or 1)}")
            rows = con.execute(
                f"""
                SELECT cert_id, is_ca_flag, subject_dn, issuer_dn,
                       serial_dec, serial_bin, ski, aki, not_before, not_after
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY cert_id ORDER BY not_before DESC
                    ) AS rn
                    FROM read_parquet('{glob}')
                    WHERE is_ca_flag = true
                )
                WHERE rn = 1
                """
            ).fetchall()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Could not seed issuer index from certs: %s", exc)
            return
        finally:
            con.close()

        seeded = 0
        for (
            cert_id, is_ca, subject_dn, issuer_dn, serial_dec, serial_bin,
            ski, aki, not_before, not_after,
        ) in rows:
            if cert_id in self.cert_index:
                continue
            self.cert_index[cert_id] = CertIndexEntry(
                cert_id=cert_id,
                is_ca=bool(is_ca),
                subject_dn=subject_dn,
                issuer_dn=issuer_dn,
                serial_dec=serial_dec,
                serial_bin=serial_bin,
                ski=ski,
                aki=aki,
                not_before=_ensure_utc(not_before),
                not_after=_ensure_utc(not_after),
            )
            if ski is not None:
                self.certs_by_ski[ski].append(cert_id)
            if subject_dn is not None:
                self.certs_by_subject_dn[subject_dn].append(cert_id)
            seeded += 1
        logger.info(
            "Seeded %d accumulated CA certs into issuer index for chain edges",
            seeded,
        )

    def finalize(self) -> None:
        ee_revocation_map: Dict[int, datetime] = {}
        for entry in self.crl_serial_rows:
            cert_id = entry["revoked_cert_id"]
            revocation_date = entry["revocation_date"]
            if cert_id is None or revocation_date is None:
                continue
            current = ee_revocation_map.get(cert_id)
            if current is None or revocation_date < current:
                ee_revocation_map[cert_id] = revocation_date
        self.ee_revocation_rows = [
            {"cert_id": cert_id, "first_revocation": revocation_date}
            for cert_id, revocation_date in sorted(ee_revocation_map.items())
        ]

        cert_chain_rows: List[Dict[str, object]] = []
        seen_edges: set[Tuple[int, int]] = set()
        for cert_id in sorted(self.cert_index):
            entry = self.cert_index[cert_id]
            reference_time = entry.not_before or self.epoch
            candidates: List[CertIndexEntry] = []
            if entry.aki is not None:
                for issuer_id in self.certs_by_ski.get(entry.aki, []):
                    issuer_entry = self.cert_index[issuer_id]
                    if issuer_entry.subject_dn == entry.issuer_dn:
                        candidates.append(issuer_entry)
            if not candidates:
                for issuer_id in self.certs_by_subject_dn.get(entry.issuer_dn, []):
                    candidates.append(self.cert_index[issuer_id])
            candidates = [c for c in candidates if c.cert_id != cert_id]
            issuer_id = self.select_best_candidate(candidates, reference_time)
            if issuer_id is None:
                continue
            edge = (issuer_id, cert_id)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            cert_chain_rows.append(
                {
                    "issuer_cert_id": issuer_id,
                    "subject_cert_id": cert_id,
                    "via_aki": entry.aki,
                }
            )
        self.cert_chain_rows = cert_chain_rows
        self.flush_streaming_tables()

    def build_roas_signers_snapshot_rows(
        self, dataset_ts: str, member_ids: Iterable[int]
    ) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for object_id in member_ids:
            info = self.roas_by_object.get(object_id)
            if info is None:
                continue
            signer_cert_id = self.signer_by_object.get(object_id)
            cert_entry = (
                self.cert_index.get(signer_cert_id)
                if signer_cert_id is not None
                else None
            )
            rows.append(
                {
                    "dataset_ts": dataset_ts,
                    "object_id": object_id,
                    "asn": info["asn"],
                    "semantic_hash": info["semantic_hash"],
                    "signer_cert_id": signer_cert_id,
                    "signer_serial": cert_entry.serial_dec if cert_entry else None,
                    "signer_not_before": cert_entry.not_before if cert_entry else None,
                    "signer_not_after": cert_entry.not_after if cert_entry else None,
                }
            )
        return rows


def _make_table_writer(
    kv: SQLiteKV, table_root: Path, flush_rows: int
) -> Callable[
    [
        str,
        pa.Schema,
        Iterable[Dict[str, object]],
        Optional[Dict[str, str]],
        Optional[Callable[[Dict[str, object]], str]],
    ],
    None,
]:
    def _write_table(
        name: str,
        schema: pa.Schema,
        rows: Iterable[Dict[str, object]],
        partition_values: Optional[Dict[str, str]] = None,
        primary_key_fn: Optional[Callable[[Dict[str, object]], str]] = None,
    ) -> None:
        partition = {"table": name}
        if partition_values:
            partition.update(partition_values)
        writer: Optional[ParquetBatchWriter] = None
        for row in rows:
            if primary_key_fn is not None:
                primary_key = primary_key_fn(row)
                if not kv.mark_emitted(name, primary_key):
                    continue
            if writer is None:
                writer = ParquetBatchWriter(
                    root=table_root,
                    table_name=name,
                    schema=schema,
                    partition_values=partition,
                )
            writer.append(row)
            if flush_rows > 0 and len(writer.rows) >= flush_rows:
                writer.flush()
        if writer is not None:
            writer.flush()

    return _write_table


def _write_base_tables(
    state: PipelineState,
    kv: SQLiteKV,
    table_root: Path,
    flush_rows: int,
) -> None:
    table_root.mkdir(parents=True, exist_ok=True)
    write_table = _make_table_writer(kv, table_root, flush_rows)
    timestamp_us = pa.timestamp("us", tz="UTC")

    write_table(
        "roa_objects",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("object_hash", pa.string()),
                ("semantic_hash", pa.string()),
                ("asn", pa.int64()),
                ("ee_serial", pa.string()),
                ("ee_not_before", timestamp_us),
                ("ee_not_after", timestamp_us),
                ("signing_time", timestamp_us),
                ("ee_public_key", pa.string()),
                ("repo_id", pa.string()),
                ("tal_id", pa.string()),
            ]
        ),
        state.roa_object_rows,
        primary_key_fn=lambda row: str(row["object_id"]),
    )

    write_table(
        "roa_prefixes",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("afi", pa.uint8()),
                ("prefix", pa.string()),
                ("max_length", pa.uint8()),
            ]
        ),
        state.roa_prefix_rows,
        primary_key_fn=lambda row: f"{row['object_id']}:{row['afi']}:{row['prefix']}:{row['max_length']}",
    )

    write_table(
        "object_signatures",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("object_type", pa.string()),
                ("signer_cert_id", pa.int32()),
                ("content_type_oid", pa.string()),
                ("digest_algorithm", pa.string()),
                ("signature_algorithm", pa.string()),
                ("signing_time", timestamp_us),
                ("message_digest", pa.string()),
            ]
        ),
        state.object_signature_rows,
        primary_key_fn=lambda row: str(row["object_id"]),
    )

    cert_schema = pa.schema(
        [
            ("cert_id", pa.int32()),
            ("object_id", pa.int32()),
            ("cert_hash", pa.string()),
            ("is_ca_flag", pa.bool_()),
            ("subject_dn", pa.string()),
            ("issuer_dn", pa.string()),
            ("serial_dec", pa.string()),
            ("serial_bin", pa.string()),
            ("ski", pa.string()),
            ("aki", pa.string()),
            ("not_before", timestamp_us),
            ("not_after", timestamp_us),
            ("spki_alg", pa.string()),
            ("spki_bits", pa.int32()),
            ("spki_sha256", pa.string()),
            ("resources_ip", pa.string()),
            ("resources_asn", pa.string()),
            ("sia", pa.string()),
            ("aia", pa.string()),
            ("cdp", pa.string()),
            ("key_usage", pa.string()),
            ("ext_key_usage", pa.string()),
            ("basic_constraints", pa.string()),
            ("repo_id", pa.string()),
            ("tal_id", pa.string()),
        ]
    )
    cert_rows_list = [state.cert_rows[cid] for cid in sorted(state.cert_rows)]
    cert_rows_ca = [row for row in cert_rows_list if row["is_ca_flag"]]
    cert_rows_ee = [row for row in cert_rows_list if not row["is_ca_flag"]]

    write_table(
        "certs",
        cert_schema,
        cert_rows_ca,
        {"is_ca": "true"},
        primary_key_fn=lambda row: str(row["cert_id"]),
    )
    write_table(
        "certs",
        cert_schema,
        cert_rows_ee,
        {"is_ca": "false"},
        primary_key_fn=lambda row: str(row["cert_id"]),
    )

    write_table(
        "manifests",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("manifest_number", pa.string()),
                ("this_update", pa.string()),
                ("next_update", pa.string()),
                ("file_count", pa.int32()),
            ]
        ),
        state.manifest_rows,
        primary_key_fn=lambda row: str(row["object_id"]),
    )

    write_table(
        "manifests_entries",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("filename", pa.string()),
                ("hash_alg", pa.string()),
                ("file_hash", pa.string()),
            ]
        ),
        state.manifest_entry_rows,
        primary_key_fn=lambda row: f"{row['object_id']}:{row['filename']}:{row['file_hash']}",
    )

    write_table(
        "cert_chain_edges",
        pa.schema(
            [
                ("issuer_cert_id", pa.int32()),
                ("subject_cert_id", pa.int32()),
                ("via_aki", pa.string()),
            ]
        ),
        state.cert_chain_rows,
        primary_key_fn=lambda row: f"{row['issuer_cert_id']}:{row['subject_cert_id']}",
    )

    write_table(
        "ee_revocations",
        pa.schema(
            [
                ("cert_id", pa.int32()),
                ("first_revocation", timestamp_us),
            ]
        ),
        state.ee_revocation_rows,
        primary_key_fn=lambda row: str(row["cert_id"]),
    )

    write_table(
        "aspa_objects",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("object_hash", pa.string()),
                ("customer_asn", pa.int64()),
                ("ee_serial", pa.string()),
                ("ee_not_before", timestamp_us),
                ("ee_not_after", timestamp_us),
                ("signing_time", timestamp_us),
                ("ee_public_key", pa.string()),
                ("repo_id", pa.string()),
                ("tal_id", pa.string()),
            ]
        ),
        state.aspa_object_rows,
        primary_key_fn=lambda row: str(row["object_id"]),
    )

    write_table(
        "aspa_providers",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("provider_asn", pa.int64()),
                ("afi_limit", pa.string()),
            ]
        ),
        state.aspa_provider_rows,
        primary_key_fn=lambda row: f"{row['object_id']}:{row['provider_asn']}",
    )

    write_table(
        "gbr_objects",
        pa.schema(
            [
                ("object_id", pa.int32()),
                ("object_hash", pa.string()),
                ("vcard", pa.string()),
                ("hcard", pa.string()),
                ("fn", pa.string()),
                ("org", pa.string()),
                ("adr", pa.string()),
                ("tel", pa.string()),
                ("email", pa.string()),
                ("ee_serial", pa.string()),
                ("ee_not_before", timestamp_us),
                ("ee_not_after", timestamp_us),
                ("signing_time", timestamp_us),
                ("ee_public_key", pa.string()),
                ("repo_id", pa.string()),
                ("tal_id", pa.string()),
            ]
        ),
        state.gbr_object_rows,
        primary_key_fn=lambda row: str(row["object_id"]),
    )



def _write_roas_signers_rows(
    kv: SQLiteKV,
    table_root: Path,
    rows: Iterable[Dict[str, object]],
    flush_rows: int,
) -> None:
    pending_rows = list(rows)
    if not pending_rows:
        return
    write_table = _make_table_writer(kv, table_root, flush_rows)
    timestamp_us = pa.timestamp("us", tz="UTC")
    write_table(
        "roas_signers_snapshot",
        pa.schema(
            [
                ("dataset_ts", pa.string()),
                ("object_id", pa.int32()),
                ("asn", pa.int64()),
                ("semantic_hash", pa.string()),
                ("signer_cert_id", pa.int32()),
                ("signer_serial", pa.string()),
                ("signer_not_before", timestamp_us),
                ("signer_not_after", timestamp_us),
            ]
        ),
        pending_rows,
        primary_key_fn=lambda row: f"{row['dataset_ts']}:{row['object_id']}",
    )


def _build_object_validity_rows(
    validity_map: Dict[str, Tuple[int, Optional[str]]],
    hash_to_id: Dict[str, int],
    dataset_ts: str,
    tal_id: str,
) -> List[Dict[str, object]]:
    """Time-varying RP-validity verdicts for objects present this cycle.

    Head tables only record that an object was repo-present (write-once at
    first sight). RP-validity changes over time, so each cycle we re-emit one
    row per *present* object that rpki-client rejected, keyed by
    ``(dataset_ts, object_id)``. The point-in-time valid count is then
    ``membership(ts) - invalid(ts)``; an object absent here is valid by default.
    """
    rows: List[Dict[str, object]] = []
    for object_hash, (state, reason) in validity_map.items():
        object_id = hash_to_id.get(object_hash)
        if object_id is None:
            continue  # rejected object not present in this cycle's cache
        rows.append(
            {
                "dataset_ts": dataset_ts,
                "object_id": object_id,
                "state": state,
                "reason_code": reason,
                "tal_id": tal_id,
            }
        )
    return rows


def _write_object_validity_rows(
    kv: SQLiteKV,
    table_root: Path,
    rows: Iterable[Dict[str, object]],
    flush_rows: int,
) -> None:
    pending_rows = list(rows)
    if not pending_rows:
        return
    write_table = _make_table_writer(kv, table_root, flush_rows)
    write_table(
        "object_validity",
        pa.schema(
            [
                ("dataset_ts", pa.string()),
                ("object_id", pa.int32()),
                ("state", pa.uint8()),
                ("reason_code", pa.string()),
                ("tal_id", pa.string()),
            ]
        ),
        pending_rows,
        primary_key_fn=lambda row: f"{row['dataset_ts']}:{row['object_id']}",
    )


def _check_crl_invariant(table_root: Path) -> None:
    serials_dir = table_root / "table=crl_serials"
    headers_dir = table_root / "table=crls"
    if not serials_dir.exists():
        return
    try:
        next(serials_dir.rglob("*.parquet"))
    except StopIteration:
        return
    try:
        import duckdb
    except Exception:  # pragma: no cover - optional dependency missing
        return

    gl_serials = str(serials_dir / "**" / "*.parquet")
    gl_headers = str(headers_dir / "**" / "*.parquet")
    try:
        con = duckdb.connect()
        con.execute(f"PRAGMA threads={max(1, os.cpu_count() or 1)}")
        try:
            ser_cnt = con.execute(
                f"SELECT COUNT(DISTINCT crl_id) FROM read_parquet('{gl_serials}')"
            ).fetchone()[0]
        except Exception:
            ser_cnt = 0
        if ser_cnt == 0:
            return
        try:
            hdr_cnt = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{gl_headers}')"
            ).fetchone()[0]
        except Exception:
            hdr_cnt = 0
        if hdr_cnt == 0:
            logger.error(
                "CRL invariant failed: crl_serials exist but no crls headers were written"
            )
            return
        try:
            missing = con.execute(
                f"""
                WITH s AS (
                    SELECT DISTINCT crl_id FROM read_parquet('{gl_serials}')
                ),
                h AS (
                    SELECT DISTINCT COALESCE(crl_id, object_id) AS crl_id
                    FROM read_parquet('{gl_headers}')
                )
                SELECT COUNT(*) FROM (
                    SELECT crl_id FROM s EXCEPT SELECT crl_id FROM h
                )
                """
            ).fetchone()[0]
        except Exception:
            missing = 0
        if missing:
            logger.error(
                "CRL invariant: %d crl_id present in serials but missing in headers",
                missing,
            )
    except Exception:  # pragma: no cover - defensive
        pass
    finally:
        try:
            con.close()
        except Exception:
            pass


def _emit_object_first_last_parquet(kv: SQLiteKV, out_root: Path) -> None:
    rows = sorted(kv.dump_observed(), key=lambda entry: entry[0])
    if not rows:
        return

    schema = pa.schema(
        [
            ("object_id", pa.int32()),
            ("first_seen_ts", pa.string()),
            ("last_seen_ts", pa.string()),
        ]

    )
    table = pa.Table.from_pydict(
        {
            "object_id": pa.array((entry[0] for entry in rows), type=pa.int32()),
            "first_seen_ts": pa.array((entry[1] for entry in rows), type=pa.string()),
            "last_seen_ts": pa.array((entry[2] for entry in rows), type=pa.string()),
        },
        schema=schema,
    )

    target_dir = out_root / "tables" / "table=object_first_last"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / "object_first_last.parquet"
    tmp_path = target_dir / "object_first_last.tmp.parquet"

    pq.write_table(table, tmp_path, compression="zstd")
    try:
        if target_path.exists():
            target_path.unlink()
        tmp_path.rename(target_path)
    except Exception:
        import shutil

        shutil.copyfile(tmp_path, target_path)
        tmp_path.unlink(missing_ok=True)


def _cas_sqlite_fetch_bytes(cas_root: Path, object_hash: str) -> Optional[bytes]:
    shard_hex = object_hash[:2].lower()
    db_path = cas_root / f"shard-{shard_hex}.db"
    if not db_path.exists():
        return None
    key = bytes.fromhex(object_hash)
    con = sqlite3.connect(str(db_path))
    try:
        row = con.execute(
            "SELECT cbytes, path FROM objects WHERE key=? LIMIT 1",
            (sqlite3.Binary(key),),
        ).fetchone()
    finally:
        con.close()
    if not row:
        return None
    compressed, stored_path = row
    if stored_path:
        path_obj = Path(stored_path)
        if not path_obj.is_absolute():
            path_obj = (cas_root.parent / path_obj).resolve()
        try:
            return path_obj.read_bytes()
        except Exception:  # pragma: no cover - defensive I/O
            return None
    if compressed:
        try:
            return zstd.ZstdDecompressor().decompress(compressed)
        except Exception:  # pragma: no cover - defensive I/O
            return None
    return None


def _kv_lookup_hash(conn: sqlite3.Connection, object_id: int) -> Optional[str]:
    row = conn.execute(
        "SELECT object_hash FROM objects WHERE object_id=?",
        (object_id,),
    ).fetchone()
    return row[0] if row else None


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _ensure_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    help=(
        "Run the RPKI ETL pipeline against a single snapshot or a tree of"
        " snapshots. Use `single` for one crawl or `bulk` to walk a"
        " historical mirror."
    ),
)
def cli() -> None:
    """Command group that exposes the ETL entry points."""


@cli.command(
    "single",
    help=(
        "Process a single repository snapshot that already exists on disk."
        " This command hashes every object, stores it in the configured CAS,"
        " parses it, and writes the warehouse tables."
    ),
)
@click.option(
    "--input",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help=(
        "Required. Directory containing the unpacked snapshot to ingest."
        " Example: /data/rpki/runs/2024-03-01T12-00-00."
    ),
)
@click.option(
    "--dataset-ts",
    "dataset_ts",
    type=str,
    required=True,
    help=(
        "Required. Dataset timestamp label stored in the warehouse."
        " Example: '2024-03-01 12:00:00'."
    ),
)
@click.option(
    "--repo-id",
    type=str,
    required=True,
    help=(
        "Required. Repository identifier written to the tables (for"
        " example, ftp.ripe.net)."
    ),
)
@click.option(
    "--tal-id",
    type=str,
    default=None,
    help=(
        "Optional. TAL identifier associated with the snapshot (e.g. lacnic)."
        " When omitted it is inferred from the --input path."
    ),
)
@click.option(
    "--transport",
    type=str,
    required=True,
    help=(
        "Required. Transport protocol used to fetch the snapshot (rrdp or"
        " rsync)."
    ),
)
@click.option(
    "--session-id",
    type=str,
    required=True,
    help="Required. Session identifier reported by the repository.",
)
@click.option(
    "--serial",
    type=str,
    required=True,
    help="Required. Serial number of the RRDP/rsync snapshot.",
)
@click.option(
    "--out-root",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help=(
        "Required. Destination directory for warehouse outputs."
        " The command creates tables/, indexes/, and CAS folders underneath."
    ),
)
@click.option(
    "--kv",
    "kv_path",
    type=click.Path(path_type=Path),
    required=True,
    help=(
        "Required. Path to the SQLite key-value database that tracks object"
        " identifiers between runs."
    ),
)
@click.option(
    "--io-workers",
    default=16,
    show_default=True,
    help=(
        "Optional. Number of threads used for hashing inputs and writing"
        " objects into CAS."
    ),
)
@click.option(
    "--parse-workers",
    default=32,
    show_default=True,
    help=(
        "Optional. Number of forkserver worker processes spawned to parse"
        " the encoded objects."
    ),

)
@click.option(
    "--mp-start",
    type=click.Choice(["forkserver", "spawn", "fork"]),
    default="forkserver",
    show_default=True,
    help=(
        "Optional. Multiprocessing start method for the parse worker pool."
        " Use 'spawn' if forkserver is unavailable."
    ),
)
@click.option(
    "--mp-chunk",
    type=int,
    default=256,
    show_default=True,
    help=(
        "Optional. Chunk size passed to executor.map when dispatching parse"
        " tasks."
    ),
)
@click.option(
    "--mp-debug",
    is_flag=True,
    default=False,
    help="Optional. Enable multiprocessing debug logging to stderr.",
)
@click.option(
    "--zstd-level",
    default=3,
    show_default=True,
    help="Optional. Compression level used when storing bytes in CAS.",
)
@click.option(
    "--zstd-threads",
    default=0,
    show_default=True,
    help="Optional. Number of helper threads Zstandard should use (0=auto).",
)
@click.option(
    "--cas-backend",
    type=click.Choice(["sqlite", "file"]),
    default="sqlite",
    show_default=True,
    help=(
        "Optional. Content-addressed storage backend. 'sqlite' enables the"
        " sharded SQLite CAS, while 'file' keeps the legacy filesystem"
        " layout."
    ),
)
@click.option(
    "--cas-sqlite-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help=(
        "Optional. Directory for SQLite CAS shards. Defaults to"
        " <out-root>/cas-sqlite when omitted."
    ),
)
@click.option(
    "--cas-sqlite-shards",
    type=int,
    default=256,
    show_default=True,
    help="Optional. Number of SQLite shard databases to create under the CAS.",
)
@click.option(
    "--cas-mode",
    type=click.Choice(["bytes", "path"]),
    default="bytes",
    show_default=True,
    help=(
        "Optional. Storage mode for SQLite CAS: 'bytes' keeps compressed"
        " payloads, 'path' stores references into an external mirror."
    ),
)
@click.option(
    "--cas-external-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help=(
        "Optional. Base directory used to resolve relative paths when the"
        " CAS runs in path mode."
    ),
)
@click.option(
    "--cas-verify-on-read",
    is_flag=True,
    default=False,
    help=(
        "Optional. When set, recomputes the SHA-256 digest when reading"
        " objects from path-mode CAS entries."
    ),
)
@click.option(
    "--threads",
    type=int,
    help="Optional. Legacy alias for --io-workers; overrides that value.",
    default=None,
)

@click.option(
    "--flush-rows",
    type=int,
    default=250_000,
    show_default=True,
    help=(
        "Optional. Maximum number of rows buffered per table before"
        " flushing to Parquet. Example: 100000."
    ),
)
@click.option(
    "--no-snapshot-rows",
    is_flag=True,
    default=False,
    help="Optional. Skip writing optional snapshot summary rows.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Optional. Discover work but do not write CAS or tables.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Optional. Enable verbose logging output.",
)
@click.option(
    "--skip-first-last",
    is_flag=True,
    default=False,
    help="Optional. Skip writing the object_first_last Parquet table.",
)
@click.option(
    "--validity-map",
    "validity_map_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Optional. JSON sidecar mapping object_hash -> {state, reason} produced"
        " by build_validity_map.py. Objects absent from the map are treated as"
        " valid. When omitted, every object is tagged valid (state=1)."
    ),
)
def single(
    input_dir: Path,
    dataset_ts: str,
    repo_id: str,
    tal_id: Optional[str],
    transport: str,
    session_id: str,
    serial: str,
    out_root: Path,
    kv_path: Path,
    io_workers: int,
    parse_workers: int,
    mp_start: str,
    mp_chunk: int,
    mp_debug: bool,

    zstd_level: int,
    zstd_threads: int,
    cas_backend: str,
    cas_sqlite_root: Optional[Path],
    cas_sqlite_shards: int,
    cas_mode: str,
    cas_external_root: Optional[Path],
    cas_verify_on_read: bool,

    threads: Optional[int],
    flush_rows: int,
    no_snapshot_rows: bool,
    dry_run: bool,
    verbose: bool,
    skip_first_last: bool,
    validity_map_path: Optional[Path],
) -> None:
    _setup_logging(verbose)

    validity_map = load_validity_map(validity_map_path)

    if tal_id is None:
        inferred_tal = _infer_tal_id_from_path(input_dir)
        if inferred_tal is None:
            raise click.UsageError(
                "--tal-id was not provided and could not be inferred from --input"
            )
        tal_id = inferred_tal

    if mp_debug:
        mu.log_to_stderr()

    if threads is not None:
        io_workers = threads

    if cas_mode == "path" and cas_backend != "sqlite":
        raise click.UsageError("--cas-mode=path requires --cas-backend=sqlite")

    try:
        parsed_dataset_ts = datetime.strptime(dataset_ts, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:  # pragma: no cover - input validation
        raise click.BadParameter(
            "dataset-ts must be in 'YYYY-MM-DD HH:MM:SS' format"
        ) from exc

    dataset_ts_str = parsed_dataset_ts.strftime("%Y-%m-%d %H:%M:%S")
    dataset_day = parsed_dataset_ts.date().isoformat()
    dataset_id = dataset_day_to_id(dataset_day)
    dataset_ts_label = dataset_ts_str.replace(":", "-")

    table_root = out_root / "tables"
    index_root = out_root / "indexes" / "membership" / f"ts={dataset_ts_label}"
    snapshot_root = out_root / "snapshots" / f"ts={dataset_ts_label}"
    snapshot_root.mkdir(parents=True, exist_ok=True)


    store = make_cas_backend(
        cas_backend,
        out_root,
        compression_level=zstd_level,
        zstd_threads=zstd_threads,
        sqlite_root=cas_sqlite_root,
        sqlite_shards=cas_sqlite_shards,
        cas_mode=cas_mode,
        cas_external_root=cas_external_root,
        cas_verify_on_read=cas_verify_on_read,
    )
    kv = SQLiteKV(kv_path)

    errors: List[Dict[str, str]] = []

    files = [
        Path(os.path.join(root, f))
        for root, _dirs, filenames in os.walk(input_dir, followlinks=True)
        for f in filenames
    ]

    hashed_results: List[HashedObject] = []
    roa_count = 0
    mft_count = 0
    other_count = 0
    if files:
        with ThreadPoolExecutor(max_workers=io_workers) as executor:
            for item in tqdm(
                executor.map(_hash_probe, files, chunksize=64),
                total=len(files),
                desc=_timestamped_desc("Hashing"),
            ):
                if item.error or item.object_hash is None:
                    errors.append(
                        {
                            "path": str(item.path),
                            "error": item.error or "failed to hash object",
                        }
                    )
                    continue
                detected_type = (item.detected_type or "unknown").upper()
                if detected_type == "ROA":
                    roa_count += 1
                elif detected_type == "MFT":
                    mft_count += 1
                else:
                    other_count += 1
                hashed_results.append(item)


    if files:
        _log_detected_object_mix(hashed_results)


    unique_objects: Dict[str, HashedObject] = {}
    for item in hashed_results:
        unique_objects.setdefault(item.object_hash, item)

    unique_hashes = list(unique_objects.keys())
    hash_to_id = kv.get_or_create_many(unique_hashes)
    object_ids = [hash_to_id[item.object_hash] for item in hashed_results]

    cas_errors: List[Tuple[str, str]] = []

    def _store_object(item: HashedObject) -> None:
        if store.exists(item.object_hash):
            return
        try:
            if cas_mode == "path" and hasattr(store, "store_path"):
                store.store_path(item.object_hash, item.path)
            else:
                data = item.path.read_bytes()
                store.store(item.object_hash, data)
        except Exception as exc:  # pragma: no cover - defensive I/O
            cas_errors.append((str(item.path), f"CAS store failed: {exc}"))

    unique_items = [unique_objects[h] for h in unique_hashes]
    if unique_items:
        with ThreadPoolExecutor(max_workers=io_workers) as executor:
            list(
                tqdm(
                    executor.map(_store_object, unique_items, chunksize=64),
                    total=len(unique_items),
                    desc=_timestamped_desc("CAS store"),
                )
            )
    for path_str, message in cas_errors:
        errors.append({"path": path_str, "error": message})

    unique_items_sorted = sorted(
        unique_items,
        key=lambda item: hash_to_id[item.object_hash],
    )

    logger.info("CAS stored %d unique objects", len(unique_items_sorted))


    primitive_inputs: List[Tuple[str, str]] = [
        (item.object_hash, str(item.path))
        for item in unique_items_sorted
        if not kv.is_parsed(item.object_hash)
    ]
    # Update the legacy module-level hook for callers that still inspect it.
    global parse_inputs
    parse_inputs = primitive_inputs
    parsed_results: Dict[str, WorkerParseResult] = {}

    if not primitive_inputs:
        logger.info("No objects required parsing")
    else:
        logger.info(
            "Parsing %d objects with %d workers (start=%s, chunk=%d)",
            len(primitive_inputs),
            parse_workers,
            mp_start,
            mp_chunk,
        )
        try:
            probe_result = parse_worker(primitive_inputs[0])
            logger.debug("Parse probe detected %s", probe_result.cms.object_type if probe_result.cms else "unknown")
        except Exception as exc:
            logger.error("Parse probe failed before pool start: %s", exc)
            raise

        ctx = mp.get_context(mp_start)
        with ProcessPoolExecutor(
            max_workers=parse_workers, mp_context=ctx
        ) as executor:
            for result in tqdm(
                executor.map(parse_worker, primitive_inputs, chunksize=mp_chunk),
                total=len(primitive_inputs),
                desc=_timestamped_desc("Parsing"),
            ):
                parsed_results[result.object_hash] = result
                obj_path = unique_objects[result.object_hash].path
                for message in result.errors:
                    errors.append({"path": str(obj_path), "error": message})


        logger.info(
            "Parse stage completed for %d objects", len(parsed_results)
        )

    crl_writer: Optional[ParquetBatchWriter] = None
    crl_serial_writer: Optional[ParquetBatchWriter] = None
    if not dry_run:
        crl_writer = ParquetBatchWriter(
            root=table_root,
            table_name="crls",
            schema=schema_crls(),
            partition_values={"table": "crls"},
        )
        crl_serial_writer = ParquetBatchWriter(
            root=table_root,
            table_name="crl_serials",
            schema=schema_crl_serials(),
            partition_values={"table": "crl_serials"},
        )

    state = PipelineState(
        repo_id=repo_id,
        tal_id=tal_id,
        kv=kv,
        flush_rows=flush_rows,
        crl_writer=crl_writer,
        crl_serial_writer=crl_serial_writer,
    )

    for item in unique_items_sorted:
        result = parsed_results.get(item.object_hash)
        if result is None:
            continue
        object_id = hash_to_id[item.object_hash]
        state.index_result_certificates(result, object_id)

    for item in unique_items_sorted:
        result = parsed_results.get(item.object_hash)
        if result is None:
            continue
        object_id = hash_to_id[item.object_hash]
        state.process_result(result, object_id, item.object_hash)
        kv.mark_parsed(item.object_hash)

    state.seed_issuer_index_from_certs(table_root)
    state.finalize()

    if dry_run:
        logger.info("Dry-run complete: %s objects", len(object_ids))
        kv.close()
        return

    _write_base_tables(state, kv, table_root, flush_rows)
    _check_crl_invariant(table_root)

    membership_ids = sorted(set(object_ids))
    index_root.mkdir(parents=True, exist_ok=True)
    write_membership(str(index_root / "objects.roar"), membership_ids)
    kv.update_observed_many(dataset_ts_str, membership_ids)

    if not no_snapshot_rows:
        roas_rows = state.build_roas_signers_snapshot_rows(
            dataset_ts_str, membership_ids
        )
        _write_roas_signers_rows(kv, table_root, roas_rows, flush_rows)

    validity_rows = _build_object_validity_rows(
        validity_map, hash_to_id, dataset_ts_str, tal_id
    )
    _write_object_validity_rows(kv, table_root, validity_rows, flush_rows)
    logger.info(
        "object_validity: %d invalid object(s) present this cycle", len(validity_rows)
    )

    metadata = {
        "dataset_ts": dataset_ts_str,
        "dataset_day": dataset_day,
        "dataset_id": dataset_id,
        "repo_id": repo_id,
        "tal_id": tal_id,
        "transport": transport,
        "session_id": session_id,
        "serial": serial,
        "file_count": len(files),
        "object_count": len(object_ids),
        "unique_object_count": len(unique_hashes),
        "error_count": len(errors),
    }
    (snapshot_root / "metadata.json").write_text(json.dumps(metadata, indent=2))

    if errors:
        import csv

        error_path = snapshot_root / "errors.csv"
        with error_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["path", "error"])
            writer.writeheader()
            writer.writerows(errors)

    if not skip_first_last:
        _emit_object_first_last_parquet(kv, out_root)

    store.close()

    kv.close()
    logger.info("Processed %s files", len(files))


@cli.command(
    "bulk",
    help=(
        "Walk a repository mirror that contains many timestamped"
        " directories. Each discovered snapshot is hashed, stored in the"
        " configured CAS, parsed, and written to the warehouse."
    ),
)
@click.option(
    "--input-root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help=(
        "Required. Root directory that contains per-snapshot subfolders."
        " Example: /data/rpki/historical/rirs."
    ),

)
@click.option(
    "--repo-id",
    type=str,
    required=True,
    help="Required. Repository identifier written to every output row.",
)
@click.option(
    "--tal-id",
    type=str,
    default=None,
    help=(
        "Optional. TAL identifier associated with the mirror contents."
        " When omitted it is inferred from the --input-root path."
    ),
)
@click.option(
    "--transport",
    type=str,
    required=True,
    help="Required. Transport protocol used by the source (rrdp or rsync).",
)
@click.option(
    "--session-id",
    type=str,
    required=True,
    help="Required. RRDP session identifier reported by the publisher.",
)
@click.option(
    "--serial",
    type=str,
    required=True,
    help="Required. RRDP serial corresponding to the snapshot batch.",
)
@click.option(
    "--out-root",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help=(
        "Required. Destination directory for generated tables, indexes,"
        " and CAS data."
    ),
)
@click.option(
    "--kv",
    "kv_path",
    type=click.Path(path_type=Path),
    required=True,
    help="Required. Path to the persistent SQLite key-value database.",
)
@click.option(
    "--io-workers",
    default=16,
    show_default=True,
    help=(
        "Optional. Thread pool size for hashing inputs and writing CAS"
        " entries."
    ),
)
@click.option(
    "--parse-workers",
    default=32,
    show_default=True,
    help=(
        "Optional. Number of forkserver worker processes that parse"
        " objects in parallel."
    ),
)
@click.option(
    "--mp-start",
    type=click.Choice(["forkserver", "spawn", "fork"]),
    default="forkserver",
    show_default=True,
    help=(
        "Optional. Multiprocessing start method for the parse worker pool."
        " Use 'spawn' if forkserver is unavailable."
    ),
)
@click.option(
    "--mp-chunk",
    type=int,
    default=256,
    show_default=True,
    help=(
        "Optional. Chunk size passed to executor.map when dispatching parse"
        " tasks."
    ),
)
@click.option(
    "--mp-debug",
    is_flag=True,
    default=False,
    help="Optional. Enable multiprocessing debug logging to stderr.",
)
@click.option(
    "--zstd-level",
    default=3,
    show_default=True,
    help="Optional. Compression level used when writing CAS bytes.",
)
@click.option(
    "--zstd-threads",
    default=0,
    show_default=True,
    help="Optional. Helper threads for Zstandard compression (0=auto).",
)
@click.option(

    "--cas-backend",
    type=click.Choice(["sqlite", "file"]),
    default="sqlite",
    show_default=True,
    help=(
        "Optional. Content-addressed storage backend. Choose 'sqlite' for"
        " the sharded CAS or 'file' for the legacy filesystem CAS."
    ),
)
@click.option(
    "--cas-sqlite-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help=(
        "Optional. Location for SQLite CAS shards. Defaults to"
        " <out-root>/cas-sqlite when omitted."
    ),
)
@click.option(
    "--cas-sqlite-shards",
    type=int,
    default=256,
    show_default=True,
    help="Optional. Number of shard databases to create for the SQLite CAS.",
)
@click.option(
    "--cas-mode",
    type=click.Choice(["bytes", "path"]),
    default="bytes",
    show_default=True,
    help=(
        "Optional. SQLite CAS storage mode: 'bytes' keeps compressed"
        " payloads, 'path' records file paths into an external mirror."
    ),
)
@click.option(
    "--cas-external-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help=(
        "Optional. Base path used to resolve stored paths when running"
        " the CAS in path mode."
    ),
)
@click.option(
    "--cas-verify-on-read",
    is_flag=True,
    default=False,
    help=(
        "Optional. Recompute and verify the SHA-256 digest when reading"
        " objects stored via path mode."
    ),
)
@click.option(
    "--default-time",
    type=str,
    default="12:00:00",
    show_default=True,
    help="Time used when only a date is inferred",
)
@click.option(
    "--force-time",
    type=str,
    default=None,
    help=(
        "Optional. Override the inferred time component for every snapshot."
        " Example: 00:00:00."
    ),
)
@click.option(
    "--exclude-dir",
    multiple=True,
    default=("unvalidated",),
    show_default=True,
    help=(
        "Optional. Directory names to skip recursively (case-insensitive)."
        " Provide the option multiple times to extend the list."
    ),
)
@click.option(
    "--skip-existing-membership",
    is_flag=True,
    default=True,
    help=(
        "Optional. Skip membership index generation when the output already"
        " exists for a snapshot."
    ),
)
@click.option(
    "--flush-rows",
    type=int,
    default=250_000,
    show_default=True,
    help=(
        "Optional. Maximum number of buffered rows per table before"
        " writing to Parquet."
    ),
)
@click.option(
    "--no-snapshot-rows",
    is_flag=True,
    default=False,
    help="Optional. Skip generating optional snapshot summary rows.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Optional. Walk snapshots and log actions without writing output.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Optional. Enable verbose logging output.",
)

def bulk(
    input_root: Path,
    repo_id: str,
    tal_id: Optional[str],
    transport: str,
    session_id: str,
    serial: str,
    out_root: Path,
    kv_path: Path,
    io_workers: int,
    parse_workers: int,
    mp_start: str,
    mp_chunk: int,
    mp_debug: bool,

    zstd_level: int,
    zstd_threads: int,
    cas_backend: str,
    cas_sqlite_root: Optional[Path],
    cas_sqlite_shards: int,
    cas_mode: str,
    cas_external_root: Optional[Path],
    cas_verify_on_read: bool,
    default_time: str,
    force_time: Optional[str],
    exclude_dir: Tuple[str, ...],

    skip_existing_membership: bool,
    flush_rows: int,
    no_snapshot_rows: bool,
    dry_run: bool,
    verbose: bool,
) -> None:
    _setup_logging(verbose)

    if tal_id is None:
        inferred_tal = _infer_tal_id_from_path(input_root)
        if inferred_tal is None:
            raise click.UsageError(
                "--tal-id was not provided and could not be inferred from --input-root"
            )
        tal_id = inferred_tal


    if mp_debug:
        mu.log_to_stderr()

    if cas_mode == "path" and cas_backend != "sqlite":
        raise click.UsageError("--cas-mode=path requires --cas-backend=sqlite")

    try:
        default_hms = _parse_hms(default_time)
    except ValueError as exc:  # pragma: no cover - option validation
        raise click.UsageError(f"--default-time: {exc}") from exc

    force_hms: Optional[_time] = None
    if force_time is not None:
        try:
            force_hms = _parse_hms(force_time)
        except ValueError as exc:  # pragma: no cover - option validation
            raise click.UsageError(f"--force-time: {exc}") from exc

    exclude_set = {name.lower() for name in exclude_dir}
    ts_to_files, discovery_errors, pruned_count = _discover_files_grouped_by_ts(
        input_root, default_hms, force_hms, exclude_set
    )

    if not ts_to_files:
        logger.info(
            "No files with recognizable dates under %s (pruned %d directories)",
            input_root,
            pruned_count,
        )
        return

    if skip_existing_membership:
        filtered: Dict[str, List[Path]] = {}
        for ts_str, files in ts_to_files.items():
            ts_label = ts_str.replace(":", "-")
            membership_path = (
                out_root
                / "indexes"
                / "membership"
                / f"ts={ts_label}"
                / "objects.roar"
            )
            if membership_path.exists():
                logger.info("Skipping %s; membership already exists", ts_str)
                continue
            filtered[ts_str] = files
        ts_to_files = filtered
        if not ts_to_files:
            logger.info(
                "All timestamps already processed (pruned %d directories)",
                pruned_count,
            )
            return

    ts_items: List[Tuple[str, datetime, List[Path]]] = []
    for ts_str, files in ts_to_files.items():
        ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        ts_items.append((ts_str, ts_dt, files))
    ts_items.sort(key=lambda item: item[1])

    logger.info(
        "Discovered %s timestamps (pruned %d directories)",
        len(ts_items),
        pruned_count,
    )


    table_root = out_root / "tables"
    store = make_cas_backend(
        cas_backend,
        out_root,
        compression_level=zstd_level,
        zstd_threads=zstd_threads,
        sqlite_root=cas_sqlite_root,
        sqlite_shards=cas_sqlite_shards,
        cas_mode=cas_mode,
        cas_external_root=cas_external_root,
        cas_verify_on_read=cas_verify_on_read,
    )
    kv = SQLiteKV(kv_path)

    errors: List[Dict[str, str]] = list(discovery_errors)

    hash_to_path: Dict[str, Path] = {}
    ts_to_hashes: Dict[str, List[str]] = {}

    with ThreadPoolExecutor(max_workers=io_workers) as executor:
        for ts_str, _, files in ts_items:
            if not files:
                ts_to_hashes[ts_str] = []
                continue
            bucket: List[str] = []
            for object_hash, _size, path_obj, error in tqdm(
                executor.map(_hash_path, files, chunksize=128),
                total=len(files),
                desc=_timestamped_desc("Hashing", extra=ts_str),
            ):
                if error or object_hash is None:
                    errors.append(
                        {
                            "path": path_obj.as_posix(),
                            "error": error or "failed to hash object",
                            "snapshot": ts_str,
                        }
                    )
                    continue
                bucket.append(object_hash)
                if object_hash not in hash_to_path:
                    hash_to_path[object_hash] = path_obj
            ts_to_hashes[ts_str] = sorted(set(bucket))


    unique_hashes = list(hash_to_path.keys())
    hash_to_id = kv.get_or_create_many(unique_hashes)

    def _store_if_missing(object_hash: str) -> Optional[Dict[str, str]]:
        if store.exists(object_hash):
            return None
        path = hash_to_path[object_hash]
        try:
            if cas_mode == "path" and hasattr(store, "store_path"):
                store.store_path(object_hash, path)
            else:
                data = path.read_bytes()
                store.store(object_hash, data)
            return None
        except Exception as exc:  # pragma: no cover - defensive I/O

            return {
                "path": str(path),
                "error": f"CAS store failed: {exc}",
                "snapshot": None,
            }


    if unique_hashes:
        with ThreadPoolExecutor(max_workers=io_workers) as executor:
            for message in tqdm(
                executor.map(_store_if_missing, unique_hashes, chunksize=128),
                total=len(unique_hashes),
                desc=_timestamped_desc("CAS store (missing only)"),
            ):
                if message:
                    errors.append(message)

    unique_hashes_sorted = sorted(unique_hashes, key=lambda h: hash_to_id[h])
    primitive_inputs: List[Tuple[str, str]] = [
        (object_hash, str(hash_to_path[object_hash]))
        for object_hash in unique_hashes_sorted
        if not kv.is_parsed(object_hash)
    ]
    global parse_inputs
    parse_inputs = primitive_inputs

    logger.info(
        "CAS stored %d unique objects", len(unique_hashes_sorted)
    )

    logger.info(
        "CAS stored %d unique objects", len(unique_hashes_sorted)
    )


    parsed_results: Dict[str, WorkerParseResult] = {}
    if not primitive_inputs:
        logger.info("All objects were already parsed")
    else:
        logger.info(
            "Parsing %d unique objects with %d workers (start=%s, chunk=%d)",
            len(primitive_inputs),
            parse_workers,
            mp_start,
            mp_chunk,
        )
        try:
            probe_result = parse_worker(primitive_inputs[0])
            logger.debug(
                "Parse probe detected %s",
                probe_result.cms.object_type if probe_result.cms else "unknown",
            )
        except Exception as exc:
            logger.error("Parse probe failed before pool start: %s", exc)
            raise

        ctx = mp.get_context(mp_start)
        with ProcessPoolExecutor(
            max_workers=parse_workers, mp_context=ctx
        ) as executor:
            for result in tqdm(
                executor.map(parse_worker, primitive_inputs, chunksize=mp_chunk),
                total=len(primitive_inputs),
                desc=_timestamped_desc("Parsing unique objects"),
            ):
                parsed_results[result.object_hash] = result
                obj_path = hash_to_path[result.object_hash]
                for message in result.errors:

                    errors.append(
                        {
                            "path": obj_path.as_posix(),
                            "error": message,
                            "snapshot": None,
                        }
                    )
        logger.info(
            "Parse stage completed for %d unique objects", len(parsed_results)
        )

    crl_writer: Optional[ParquetBatchWriter] = None
    crl_serial_writer: Optional[ParquetBatchWriter] = None
    if not dry_run:
        crl_writer = ParquetBatchWriter(
            root=table_root,
            table_name="crls",
            schema=schema_crls(),
            partition_values={"table": "crls"},
        )
        crl_serial_writer = ParquetBatchWriter(
            root=table_root,
            table_name="crl_serials",
            schema=schema_crl_serials(),
            partition_values={"table": "crl_serials"},
        )

    state = PipelineState(
        repo_id=repo_id,
        tal_id=tal_id,
        kv=kv,
        flush_rows=flush_rows,
        crl_writer=crl_writer,
        crl_serial_writer=crl_serial_writer,
    )

    logger.info(
        "Parse stage completed for %d objects", len(parsed_results)
    )

    for object_hash in unique_hashes_sorted:
        result = parsed_results.get(object_hash)
        if result is None:
            continue
        object_id = hash_to_id[object_hash]
        state.index_result_certificates(result, object_id)

    for object_hash in unique_hashes_sorted:
        result = parsed_results.get(object_hash)
        if result is None:
            continue
        object_id = hash_to_id[object_hash]
        state.process_result(result, object_id, object_hash)
        kv.mark_parsed(object_hash)

    state.seed_issuer_index_from_certs(table_root)
    state.finalize()

    if dry_run:
        logger.info(
            "Dry-run complete: %s timestamps, %s unique objects",
            len(ts_items),

            len(unique_hashes),
        )
        store.close()

        kv.close()
        return

    _write_base_tables(state, kv, table_root, flush_rows)
    _check_crl_invariant(table_root)


    for ts_str, ts_dt, files in ts_items:

        ts_label = ts_str.replace(":", "-")
        index_root = out_root / "indexes" / "membership" / f"ts={ts_label}"
        index_root.mkdir(parents=True, exist_ok=True)
        member_hashes = ts_to_hashes.get(ts_str, [])
        member_ids = sorted(
            {hash_to_id[h] for h in member_hashes if h in hash_to_id}
        )
        write_membership(str(index_root / "objects.roar"), member_ids)

        kv.update_observed_many(ts_str, member_ids)

        if not no_snapshot_rows:
            roas_rows = state.build_roas_signers_snapshot_rows(ts_str, member_ids)
            _write_roas_signers_rows(kv, table_root, roas_rows, flush_rows)

        snapshot_root = out_root / "snapshots" / f"ts={ts_label}"
        snapshot_root.mkdir(parents=True, exist_ok=True)

        dataset_day = ts_dt.date().isoformat()
        metadata = {
            "dataset_ts": ts_str,
            "dataset_day": dataset_day,
            "dataset_id": dataset_day_to_id(dataset_day),

            "repo_id": repo_id,
            "tal_id": tal_id,
            "transport": transport,
            "session_id": session_id,
            "serial": serial,

            "file_count": len(files),
            "object_count": len(member_ids),
            "unique_object_count": len(unique_hashes),
            "error_count": sum(
                1 for entry in errors if entry.get("snapshot") == ts_str
            ),
        }
        (snapshot_root / "metadata.json").write_text(json.dumps(metadata, indent=2))

        snapshot_errors = [
            {"path": entry.get("path", ""), "error": entry.get("error", "")}
            for entry in errors
            if entry.get("snapshot") == ts_str
        ]

        if snapshot_errors:
            import csv

            error_path = snapshot_root / "errors.csv"
            with error_path.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["path", "error"])
                writer.writeheader()
                writer.writerows(snapshot_errors)


    _emit_object_first_last_parquet(kv, out_root)
    store.close()
    kv.close()
    logger.info(
        "Processed %s timestamps with %s unique objects",
        len(ts_items),

        len(unique_hashes),
    )


@cli.group()
def repair() -> None:
    """Maintenance helpers for repairing existing warehouse tables."""


@repair.command("crl-headers")
@click.option("--warehouse", type=click.Path(file_okay=False, path_type=Path), required=True)
@click.option("--kv", type=click.Path(file_okay=True, path_type=Path), required=True)
@click.option(
    "--cas-sqlite-root",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
)
@click.option("--limit", type=int, default=None)
@click.option("--batch", type=int, default=1000)
def repair_crl_headers(
    warehouse: Path,
    kv: Path,
    cas_sqlite_root: Path,
    limit: Optional[int],
    batch: int,
) -> None:
    """Backfill missing CRL headers based on existing serial rows."""

    warehouse = warehouse.resolve()
    kv = kv.resolve()
    cas_sqlite_root = cas_sqlite_root.resolve()
    table_root = warehouse / "tables"
    serials_dir = table_root / "table=crl_serials"
    if not serials_dir.exists():
        raise click.ClickException("No CRL serials found in the warehouse.")

    serial_glob = str(serials_dir / "**" / "*.parquet").replace("'", "''")
    headers_glob = str(table_root / "table=crls" / "**" / "*.parquet").replace("'", "''")
    try:
        import duckdb
    except Exception as exc:  # pragma: no cover - optional dependency missing
        raise click.ClickException(f"duckdb is required for repair operations: {exc}") from exc

    con = duckdb.connect()
    try:
        try:
            serial_rows = con.execute(
                f"SELECT DISTINCT crl_id::INT AS crl_id FROM read_parquet('{serial_glob}')"
            ).fetchall()
        except Exception as exc:
            raise click.ClickException(f"Failed to read CRL serials: {exc}") from exc
        try:
            header_rows = con.execute(
                f"SELECT DISTINCT COALESCE(crl_id, object_id)::INT AS crl_id FROM read_parquet('{headers_glob}')"
            ).fetchall()
        except Exception:
            header_rows = []
    finally:
        con.close()

    have = {int(row[0]) for row in header_rows if row and row[0] is not None}
    todo = [int(row[0]) for row in serial_rows if row and row[0] is not None and int(row[0]) not in have]
    if limit is not None:
        todo = todo[: max(limit, 0)]
    if not todo:
        click.echo("No missing CRL headers.")
        return

    writer = ParquetBatchWriter(
        root=table_root,
        table_name="crls",
        schema=schema_crls(),
        partition_values={"table": "crls"},
    )
    kv_conn = sqlite3.connect(str(kv))
    emitted = 0
    try:
        for crl_id in todo:
            object_hash = _kv_lookup_hash(kv_conn, crl_id)
            if not object_hash:
                continue
            raw = _cas_sqlite_fetch_bytes(cas_sqlite_root, object_hash)
            if not raw:
                continue
            try:
                parsed = parse_crl(raw)
            except Exception:
                continue
            header_row = {
                "crl_id": crl_id,
                "object_id": crl_id,
                "issuer_dn": clean_dn(parsed.issuer) or parsed.issuer,
                "crl_number": parsed.crl_number,
                "this_update": _ensure_utc(parsed.this_update),
                "next_update": _ensure_utc(parsed.next_update),
                "revoked_count": int(parsed.revoked_count),
                "aki": parsed.authority_key_identifier,
                "signature_alg": parsed.signature_algorithm,
                "repo_id": None,
                "tal_id": None,
            }
            writer.append(header_row)
            kv_conn.execute(
                "INSERT OR IGNORE INTO emitted_rows(table_name, primary_key) VALUES (?, ?)",
                ("crls", str(crl_id)),
            )
            emitted += 1
            if batch > 0 and len(writer.rows) >= batch:
                writer.flush()
        writer.flush()
        kv_conn.commit()
    finally:
        kv_conn.close()

    if emitted:
        _check_crl_invariant(table_root)
        click.echo(f"Repaired headers for {emitted} CRLs at {table_root / 'table=crls'}")
    else:
        click.echo("No headers were repaired (missing data in CAS or KV).")


main = single


if __name__ == "__main__":  # pragma: no cover
    cli()
