from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from parse_aspa import ASPAParsingError, ParsedASPA, parse_aspa_signed_object
from parse_cert import CertificateParsingError, ParsedCertificate, parse_certificate
from parse_cms import CMSParsingError, OID_TO_TYPE, detect_cms_object_type, extract_signer_metadata
from parse_crl import CRLParsingError, ParsedCRL, parse_crl
from parse_gbr import GBRParsingError, ParsedGBR, parse_gbr_signed_object
from parse_mft import ManifestParsingError, ParsedManifest, parse_manifest
from parse_roa import ROAParsingError, ParsedROA, compute_semantic_hash, parse_roa_signed_object
from util import compute_sha256


_KNOWN_OBJECT_TYPES = {"ROA", "MFT", "ASPA", "GBR", "CER", "CRL"}


@dataclass
class ParsedCertificateData:
    cert_hash: str
    parsed: ParsedCertificate


@dataclass
class WorkerROAResult:
    parsed: ParsedROA
    semantic_hash: str


@dataclass
class SignerInfoResult:
    object_type: str
    content_type_oid: Optional[str]
    digest_algorithm: Optional[str]
    signature_algorithm: Optional[str]
    signing_time: Optional[datetime]
    message_digest: Optional[str]
    issuer_dn: Optional[str]
    serial_number: Optional[str]
    serial_number_bytes: Optional[str]
    subject_key_identifier: Optional[str]
    signer_cert: Optional[ParsedCertificateData]
    embedded_certs: List[ParsedCertificateData]


@dataclass
class WorkerParseResult:
    object_hash: str
    cms: Optional[SignerInfoResult]
    roa: Optional[WorkerROAResult]
    manifest: Optional[ParsedManifest]
    aspa: Optional[ParsedASPA]
    gbr: Optional[ParsedGBR]
    certificate: Optional[ParsedCertificateData]
    crl: Optional[ParsedCRL]
    errors: List[str]


def _normalise_object_type(
    detected_type: Optional[str], content_type_oid: Optional[str]
) -> str:
    if detected_type:
        label = detected_type.upper()
        if label in _KNOWN_OBJECT_TYPES:
            return label
        if label != "UNKNOWN":
            return label
    if content_type_oid is not None:
        mapped = OID_TO_TYPE.get(content_type_oid)
        if mapped is not None:
            return mapped
    return "UNKNOWN"


def parse_worker(args: Sequence[str]) -> WorkerParseResult:
    if len(args) == 2:
        object_hash, path_str = args  # type: ignore[misc]
        detected_type: Optional[str] = None
    elif len(args) == 3:
        object_hash, path_str, detected_type = args  # type: ignore[misc]
    else:  # pragma: no cover - defensive argument handling
        raise ValueError(f"expected 2 or 3 arguments, received {len(args)}")

    path = Path(path_str)
    errors: List[str] = []

    try:
        raw_bytes = path.read_bytes()
    except Exception as exc:  # pragma: no cover - defensive I/O
        return WorkerParseResult(
            object_hash=object_hash,
            cms=None,
            roa=None,
            manifest=None,
            aspa=None,
            gbr=None,
            certificate=None,
            crl=None,
            errors=[f"failed to read object: {exc}"],
        )

    cms_info: Optional[SignerInfoResult] = None
    roa_result: Optional[WorkerROAResult] = None
    manifest_result: Optional[ParsedManifest] = None
    aspa_result: Optional[ParsedASPA] = None
    gbr_result: Optional[ParsedGBR] = None
    certificate_result: Optional[ParsedCertificateData] = None
    crl_result: Optional[ParsedCRL] = None

    cms_result = detect_cms_object_type(raw_bytes)
    if cms_result is not None:
        content_type_oid: Optional[str] = None
        digest_algorithm: Optional[str] = None
        signature_algorithm: Optional[str] = None
        signing_time: Optional[datetime] = None
        message_digest: Optional[str] = None
        issuer_dn: Optional[str] = None
        serial_number: Optional[str] = None
        serial_number_bytes: Optional[str] = None
        subject_key_identifier: Optional[str] = None
        signer_cert_data: Optional[ParsedCertificateData] = None
        embedded_certs: List[ParsedCertificateData] = []

        signer_metadata = None
        try:
            signer_metadata = extract_signer_metadata(cms_result.content_info)
        except CMSParsingError as exc:
            errors.append(f"CMS metadata error: {exc}")

        if signer_metadata is not None:
            content_type_oid = signer_metadata.content_type_oid
            digest_algorithm = signer_metadata.digest_algorithm
            signature_algorithm = signer_metadata.signature_algorithm
            signing_time = signer_metadata.signing_time
            message_digest = signer_metadata.message_digest
            issuer_dn = signer_metadata.issuer_dn
            serial_number = signer_metadata.serial_number
            serial_number_bytes = signer_metadata.serial_number_bytes
            subject_key_identifier = signer_metadata.subject_key_identifier
            signer_bytes = signer_metadata.signer_certificate
            if signer_bytes is not None:
                try:
                    parsed_cert = parse_certificate(signer_bytes)
                    signer_cert_data = ParsedCertificateData(
                        cert_hash=compute_sha256(signer_bytes),
                        parsed=parsed_cert,
                    )
                except CertificateParsingError as exc:
                    errors.append(f"Failed to parse signer certificate: {exc}")

        signed_data = None
        try:
            signed_data = cms_result.content_info["content"]
        except Exception:  # pragma: no cover - defensive
            signed_data = None

        if signed_data is not None and "certificates" in signed_data:
            for certificate_choice in signed_data["certificates"]:
                if certificate_choice.name != "certificate":
                    continue
                der_bytes = certificate_choice.chosen.dump()
                try:
                    parsed_cert = parse_certificate(der_bytes)
                except CertificateParsingError as exc:
                    errors.append(f"Failed to parse embedded certificate: {exc}")
                    continue
                embedded_certs.append(
                    ParsedCertificateData(
                        cert_hash=compute_sha256(der_bytes), parsed=parsed_cert
                    )
                )

        object_type_label = _normalise_object_type(
            cms_result.object_type, content_type_oid
        )
        cms_info = SignerInfoResult(
            object_type=object_type_label,
            content_type_oid=content_type_oid,
            digest_algorithm=digest_algorithm,
            signature_algorithm=signature_algorithm,
            signing_time=signing_time,
            message_digest=message_digest,
            issuer_dn=issuer_dn,
            serial_number=serial_number,
            serial_number_bytes=serial_number_bytes,
            subject_key_identifier=subject_key_identifier,
            signer_cert=signer_cert_data,
            embedded_certs=embedded_certs,
        )

        if object_type_label == "ROA":
            try:
                parsed_roa = parse_roa_signed_object(cms_result.content_info)
                roa_result = WorkerROAResult(
                    parsed=parsed_roa,
                    semantic_hash=compute_semantic_hash(parsed_roa),
                )
            except ROAParsingError as exc:
                errors.append(f"ROA parse error: {exc}")
        elif object_type_label == "MFT":
            try:
                manifest_result = parse_manifest(cms_result.content_info)
            except ManifestParsingError as exc:
                errors.append(f"Manifest parse error: {exc}")
        elif object_type_label == "ASPA":
            try:
                aspa_result = parse_aspa_signed_object(cms_result.content_info)
            except ASPAParsingError as exc:
                errors.append(f"ASPA parse error: {exc}")
        elif object_type_label == "GBR":
            try:
                gbr_result = parse_gbr_signed_object(cms_result.content_info)
            except GBRParsingError as exc:
                errors.append(f"GBR parse error: {exc}")
    else:
        try:
            parsed_cert = parse_certificate(raw_bytes)
        except CertificateParsingError:
            try:
                crl_result = parse_crl(raw_bytes)
            except CRLParsingError as exc:
                reason = detected_type or "unknown object"
                errors.append(f"Failed to parse {reason}: {exc}")
        else:
            certificate_result = ParsedCertificateData(
                cert_hash=compute_sha256(raw_bytes), parsed=parsed_cert
            )

    return WorkerParseResult(
        object_hash=object_hash,
        cms=cms_info,
        roa=roa_result,
        manifest=manifest_result,
        aspa=aspa_result,
        gbr=gbr_result,
        certificate=certificate_result,
        crl=crl_result,
        errors=errors,
    )
