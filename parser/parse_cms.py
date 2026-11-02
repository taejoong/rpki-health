"""CMS helpers for object type detection."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from asn1crypto import cms

from util import clean_dn

ROA_CONTENT_OID = "1.2.840.113549.1.9.16.1.24"
MFT_CONTENT_OID = "1.2.840.113549.1.9.16.1.26"
GBR_CONTENT_OID = "1.2.840.113549.1.9.16.1.35"
ASPA_CONTENT_OID = "1.2.840.113549.1.9.16.1.49"


@dataclass
class CMSParseResult:
    object_type: str
    content_info: cms.ContentInfo


@dataclass
class SignerMetadata:
    signer_certificate: Optional[bytes]
    content_type_oid: str
    digest_algorithm: Optional[str]
    signature_algorithm: Optional[str]
    signing_time: Optional[datetime]

    message_digest: Optional[str]
    issuer_dn: Optional[str]
    serial_number: Optional[str]
    serial_number_bytes: Optional[str]
    subject_key_identifier: Optional[str]


class CMSParsingError(RuntimeError):
    pass


OID_TO_TYPE = {
    ROA_CONTENT_OID: "ROA",
    MFT_CONTENT_OID: "MFT",
    GBR_CONTENT_OID: "GBR",
    ASPA_CONTENT_OID: "ASPA",
}


SIGNED_DATA_OID = "1.2.840.113549.1.7.2"


def detect_cms_object_type(raw_bytes: bytes) -> Optional[CMSParseResult]:
    """Attempt to parse *raw_bytes* as CMS and infer the object type."""

    try:
        content_info = cms.ContentInfo.load(raw_bytes, strict=False)
    except Exception:  # pragma: no cover - defensive
        return None

    try:
        content_type = content_info["content_type"].dotted
    except ValueError:
        return None

    if content_type != SIGNED_DATA_OID:
        return CMSParseResult("unknown", content_info)

    try:
        signed_data = content_info["content"]
        econtent_type = signed_data["encap_content_info"]["content_type"].dotted
    except ValueError:
        return None
    object_type = OID_TO_TYPE.get(econtent_type, "unknown")
    return CMSParseResult(object_type, content_info)


def _normalise_dn(value: str) -> str:
    return clean_dn(value)


def extract_signer_metadata(content_info: cms.ContentInfo) -> SignerMetadata:
    """Extract signer certificate and CMS attributes from *content_info*."""

    if content_info["content_type"].dotted != SIGNED_DATA_OID:
        raise CMSParsingError("ContentInfo is not SignedData")

    signed_data = content_info["content"]
    econtent_info = signed_data["encap_content_info"]
    content_type_oid = econtent_info["content_type"].dotted

    signer_certificate: Optional[bytes] = None
    digest_algorithm: Optional[str] = None
    signature_algorithm: Optional[str] = None
    signing_time: Optional[datetime] = None

    message_digest: Optional[str] = None
    issuer_dn: Optional[str] = None
    serial_number: Optional[str] = None
    serial_number_bytes: Optional[str] = None
    subject_key_identifier: Optional[str] = None

    certificates = signed_data["certificates"] if "certificates" in signed_data else []
    cert_choices: list[cms.CertificateChoices] = [c for c in certificates if c.name == "certificate"]

    signer_infos = signed_data["signer_infos"] if "signer_infos" in signed_data else []
    if signer_infos:
        signer_info = signer_infos[0]
        digest_algorithm = signer_info["digest_algorithm"]["algorithm"].dotted
        signature_algorithm = signer_info["signature_algorithm"]["algorithm"].dotted

        sid = signer_info["sid"]
        matched_cert: Optional[cms.Certificate] = None
        if sid.name == "issuer_and_serial_number":

            issuer_dn = _normalise_dn(sid.chosen["issuer"].human_friendly)
            serial_field = sid.chosen["serial_number"]
            serial_number = str(serial_field.native)
            serial_number_bytes = bytes(serial_field.contents).hex()

            for choice in cert_choices:
                cert = choice.chosen
                tbs = cert["tbs_certificate"]
                if (

                    _normalise_dn(tbs["issuer"].human_friendly) == issuer_dn
                    and tbs["serial_number"].native == serial_field.native
                ):
                    matched_cert = cert
                    break
        elif sid.name == "subject_key_identifier":

            subject_key_identifier = sid.chosen.native.hex()
            for choice in cert_choices:
                cert = choice.chosen
                extensions = (
                    {ext["extn_id"].dotted: ext for ext in cert["tbs_certificate"]["extensions"]}
                    if "extensions" in cert["tbs_certificate"]
                    else {}
                )
                ski = extensions.get("2.5.29.14")

                if ski is not None and ski["extn_value"].native.hex() == subject_key_identifier:
                    matched_cert = cert
                    break

        if matched_cert is not None:
            signer_certificate = matched_cert.dump()
        elif issuer_dn is None and sid.name == "issuer_and_serial_number":

            issuer_dn = _normalise_dn(sid.chosen["issuer"].human_friendly)

        if "signed_attrs" in signer_info:
            for attr in signer_info["signed_attrs"]:
                oid = attr["type"].dotted
                if oid == "1.2.840.113549.1.9.5":
                    try:
                        signing_time = attr["values"][0].native
                    except Exception:  # pragma: no cover - defensive
                        signing_time = None
                elif oid == "1.2.840.113549.1.9.4":
                    try:

                        message_digest = attr["values"][0].native.hex()
                    except Exception:  # pragma: no cover - defensive
                        message_digest = None

    return SignerMetadata(
        signer_certificate=signer_certificate,
        content_type_oid=content_type_oid,
        digest_algorithm=digest_algorithm,
        signature_algorithm=signature_algorithm,
        signing_time=signing_time,
        message_digest=message_digest,
        issuer_dn=issuer_dn,
        serial_number=serial_number,
        serial_number_bytes=serial_number_bytes,
        subject_key_identifier=subject_key_identifier,
    )
