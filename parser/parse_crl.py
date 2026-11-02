"""CRL parsing helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from asn1crypto import crl as crl_module

from util import clean_dn


@dataclass
class RevokedCertificate:
    serial_dec: str
    serial_bin: str
    revocation_date: datetime
    reason_code: Optional[int]
    invalidity_date: Optional[datetime]
    certificate_issuer: Optional[str]


@dataclass
class ParsedCRL:
    issuer: str

    authority_key_identifier: str | None
    crl_number: str | None
    this_update: datetime
    next_update: Optional[datetime]
    signature_algorithm: Optional[str]
    revoked_certificates: List[RevokedCertificate]

    @property
    def revoked_count(self) -> int:
        return len(self.revoked_certificates)


class CRLParsingError(RuntimeError):
    pass


CRL_NUMBER_OID = "2.5.29.20"
CRL_REASON_OID = "2.5.29.21"
INVALIDITY_DATE_OID = "2.5.29.24"
CERTIFICATE_ISSUER_OID = "2.5.29.29"
AKI_OID = "2.5.29.35"

_CRL_REASON_TO_CODE = {
    "unspecified": 0,
    "key_compromise": 1,
    "cA_compromise": 2,
    "affiliation_changed": 3,
    "superseded": 4,
    "cessation_of_operation": 5,
    "certificate_hold": 6,
    "remove_from_crl": 8,
    "privilege_withdrawn": 9,
    "aA_compromise": 10,
}


def _normalise_dn(value: str) -> str:
    return clean_dn(value)


def _to_hex(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    return value.hex()

def parse_crl(der_bytes: bytes) -> ParsedCRL:
    try:
        parsed = crl_module.CertificateList.load(der_bytes)
    except Exception as exc:  # pragma: no cover - defensive
        raise CRLParsingError("Failed to decode CRL") from exc

    try:
        tbs = parsed["tbs_cert_list"]

        issuer = _normalise_dn(tbs["issuer"].human_friendly)
        this_update = tbs["this_update"].native
        next_update = None
        if "next_update" in tbs and tbs["next_update"].native is not None:
            next_update = tbs["next_update"].native

        crl_number = None

        authority_key_identifier: Optional[bytes] = None
        extensions = {}
        if "crl_extensions" in tbs:
            extensions = {ext["extn_id"].dotted: ext for ext in tbs["crl_extensions"]}
        if CRL_NUMBER_OID in extensions:
            crl_number = str(extensions[CRL_NUMBER_OID]["extn_value"].parsed.native)
        if AKI_OID in extensions:
            aki_ext = extensions[AKI_OID]["extn_value"].parsed
            try:
                aki_native = aki_ext.native
            except Exception:  # pragma: no cover - defensive
                aki_native = None
            if isinstance(aki_native, dict):
                authority_key_identifier = aki_native.get("key_identifier")


        revoked_entries: List[RevokedCertificate] = []
        if "revoked_certificates" in tbs:
            for entry in tbs["revoked_certificates"]:
                serial_field = entry["user_certificate"]
                serial_dec = str(serial_field.native)

                serial_bin = bytes(serial_field.contents).hex()
                revocation_date = entry["revocation_date"].native
                reason_code = None
                invalidity_date = None
                certificate_issuer = None
                if "crl_entry_extensions" in entry:
                    extensions = {ext["extn_id"].dotted: ext for ext in entry["crl_entry_extensions"]}
                    if CRL_REASON_OID in extensions:
                        try:
                            reason_name = str(extensions[CRL_REASON_OID]["extn_value"].parsed.native)
                            reason_code = _CRL_REASON_TO_CODE.get(reason_name)
                        except Exception:  # pragma: no cover - defensive
                            reason_code = None
                    if INVALIDITY_DATE_OID in extensions:
                        try:
                            invalidity_date = extensions[INVALIDITY_DATE_OID]["extn_value"].parsed.native
                        except Exception:  # pragma: no cover - defensive
                            invalidity_date = None
                    if CERTIFICATE_ISSUER_OID in extensions:
                        try:

                            issuer_value = extensions[CERTIFICATE_ISSUER_OID]["extn_value"].parsed
                            try:
                                certificate_issuer = issuer_value.human_friendly  # type: ignore[attr-defined]
                            except Exception:
                                try:
                                    certificate_issuer = str(issuer_value.native)
                                except Exception:  # pragma: no cover - defensive
                                    certificate_issuer = None
                            if certificate_issuer is not None:
                                certificate_issuer = _normalise_dn(certificate_issuer)
                        except Exception:  # pragma: no cover - defensive
                            certificate_issuer = None
                revoked_entries.append(
                    RevokedCertificate(
                        serial_dec=serial_dec,
                        serial_bin=serial_bin,
                        revocation_date=revocation_date,
                        reason_code=reason_code,
                        invalidity_date=invalidity_date,
                        certificate_issuer=certificate_issuer,
                    )
                )
    except Exception as exc:  # pragma: no cover - defensive
        raise CRLParsingError("Failed to decode CRL body") from exc

    return ParsedCRL(
        issuer=issuer,

        authority_key_identifier=_to_hex(authority_key_identifier),
        crl_number=crl_number,
        this_update=this_update,
        next_update=next_update,
        signature_algorithm=str(parsed["signature_algorithm"]["algorithm"].dotted),
        revoked_certificates=revoked_entries,
    )
