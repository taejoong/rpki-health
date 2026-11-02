"""Resource certificate parsing helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from hashlib import sha256

from asn1crypto import x509

from util import clean_dn


@dataclass
class ParsedCertificate:
    subject: str
    issuer: str
    serial_dec: str
    serial_bin: str
    not_before: datetime
    not_after: datetime
    ski: str | None
    aki: str | None
    is_ca: bool
    spki_algorithm: str
    spki_bits: Optional[int]
    spki_sha256: str
    resources_ip: str | None
    resources_asn: str | None
    sia: str | None
    aia: str | None
    cdp: str | None
    key_usage: str | None
    ext_key_usage: str | None
    basic_constraints: str | None


class CertificateParsingError(RuntimeError):
    pass


IP_RESOURCES_OID = "1.3.6.1.5.5.7.1.7"
AS_RESOURCES_OID = "1.3.6.1.5.5.7.1.8"
SIA_OID = "1.3.6.1.5.5.7.1.11"
AIA_OID = "1.3.6.1.5.5.7.1.1"
SKI_OID = "2.5.29.14"
AKI_OID = "2.5.29.35"
CRL_DIST_POINTS_OID = "2.5.29.31"
KEY_USAGE_OID = "2.5.29.15"
EXT_KEY_USAGE_OID = "2.5.29.37"
BASIC_CONSTRAINTS_OID = "2.5.29.19"


def _extract_general_name(location) -> Optional[str]:
    """Return a human-friendly representation of a GeneralName value."""

    value = getattr(location, "human_friendly", None)
    if value is not None:
        return str(value)

    try:
        raw_value = location.native
    except Exception:  # pragma: no cover - defensive
        return None

    if raw_value is None:
        return None
    return str(raw_value)


def _format_general_names(ext) -> str:
    try:
        names = ext["extn_value"].parsed
    except Exception:  # pragma: no cover - defensive
        return "unavailable"
    values: list[str] = []
    for name in names:
        try:
            values.append(name.native)
        except Exception:  # pragma: no cover - defensive
            continue
    return ",".join(str(v) for v in values if v)


def _summarise_ip_resources(extension: x509.Extension) -> str:
    try:
        value = extension["extn_value"].parsed
    except Exception:  # pragma: no cover - defensive
        return "unavailable"

    summaries: list[str] = []
    for family in value:
        afi = int.from_bytes(family["address_family"].native[:2], "big")
        afi_label = "v4" if afi == 1 else "v6" if afi == 2 else f"afi{afi}"
        ranges = []
        for address in family["ip_address_choice"]:
            if address.name == "addressPrefix":
                prefix = address.chosen.native
                ranges.append(prefix)
            elif address.name == "addressRange":
                start = address.chosen["min"].native
                end = address.chosen["max"].native
                ranges.append(f"{start}-{end}")
        summaries.append(f"{afi_label}: {', '.join(ranges)}")
    return "; ".join(summaries)


def _summarise_as_resources(extension: x509.Extension) -> str:
    try:
        value = extension["extn_value"].parsed
    except Exception:  # pragma: no cover - defensive
        return "unavailable"

    ranges: list[str] = []
    for choice in value:
        for entry in choice["as_ids"]["as_id_choice"]:
            if entry.name == "id":
                ranges.append(str(entry.chosen.native))
            elif entry.name == "range":
                start = entry.chosen["min"].native
                end = entry.chosen["max"].native
                ranges.append(f"{start}-{end}")
    return ", ".join(ranges)


def _normalise_dn(value: str) -> str:
    return clean_dn(value)


def _to_hex(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    return value.hex()


def parse_certificate(der_bytes: bytes) -> ParsedCertificate:
    try:
        certificate = x509.Certificate.load(der_bytes)

        tbs = certificate["tbs_certificate"]

        subject = _normalise_dn(tbs["subject"].human_friendly)
        issuer = _normalise_dn(tbs["issuer"].human_friendly)
        serial_field = tbs["serial_number"]
        serial_dec = str(serial_field.native)
        serial_bin = bytes(serial_field.contents).hex()
        validity = tbs["validity"]
        not_before: datetime = validity["not_before"].native
        not_after: datetime = validity["not_after"].native

        extensions = {ext["extn_id"].dotted: ext for ext in tbs["extensions"]} if "extensions" in tbs else {}

        ski = extensions.get(SKI_OID)
        ski_bytes = ski["extn_value"].native if ski is not None else None

        aki = extensions.get(AKI_OID)

        aki_bytes: Optional[bytes] = None
        if aki is not None:
            ext = aki["extn_value"].parsed
            if "key_identifier" in ext:
                aki_bytes = ext["key_identifier"].native

        resources_ext = extensions.get(IP_RESOURCES_OID)
        resources_ip = _summarise_ip_resources(resources_ext) if resources_ext is not None else None

        asn_ext = extensions.get(AS_RESOURCES_OID)
        resources_asn = _summarise_as_resources(asn_ext) if asn_ext is not None else None

        sia_ext = extensions.get(SIA_OID)
        sia = None
        if sia_ext is not None:
            try:
                entries = []
                for access in sia_ext["extn_value"].parsed:

                    value = _extract_general_name(access["access_location"])
                    if value:
                        entries.append(value)
                if entries:
                    sia = ",".join(entries)

            except Exception:  # pragma: no cover - defensive
                sia = None

        aia_ext = extensions.get(AIA_OID)
        aia = None
        if aia_ext is not None:
            try:
                entries = []
                for access in aia_ext["extn_value"].parsed:

                    value = _extract_general_name(access["access_location"])
                    if value:
                        entries.append(value)
                if entries:
                    aia = ",".join(entries)

            except Exception:  # pragma: no cover - defensive
                aia = None

        cdp_ext = extensions.get(CRL_DIST_POINTS_OID)
        cdp = _format_general_names(cdp_ext) if cdp_ext is not None else None

        key_usage_ext = extensions.get(KEY_USAGE_OID)
        key_usage = None
        if key_usage_ext is not None:
            try:
                key_usage = ",".join(key_usage_ext["extn_value"].parsed.native)
            except Exception:  # pragma: no cover - defensive
                key_usage = None

        ext_key_usage_ext = extensions.get(EXT_KEY_USAGE_OID)
        ext_key_usage = None
        if ext_key_usage_ext is not None:
            try:
                ext_key_usage = ",".join(ext_key_usage_ext["extn_value"].parsed.native)
            except Exception:  # pragma: no cover - defensive
                ext_key_usage = None

        basic_constraints_ext = extensions.get(BASIC_CONSTRAINTS_OID)
        basic_constraints = None
        is_ca = False
        if basic_constraints_ext is not None:
            try:
                parsed_bc = basic_constraints_ext["extn_value"].parsed
                parsed_bc_native = parsed_bc.native
                if isinstance(parsed_bc_native, dict):
                    is_ca = bool(parsed_bc_native.get("ca", False))
                else:  # pragma: no cover - defensive safeguard
                    is_ca = bool(getattr(parsed_bc, "ca", False))
                basic_constraints = str(parsed_bc_native)
            except Exception:  # pragma: no cover - defensive
                basic_constraints = None

        spki = tbs["subject_public_key_info"]
        algorithm = spki["algorithm"]["algorithm"]
        try:
            spki_algorithm = str(algorithm.native)
        except Exception:  # pragma: no cover - defensive
            spki_algorithm = algorithm.dotted

        spki_bits: Optional[int] = None
        try:
            spki_bits = int(spki.bit_size)
        except Exception:  # pragma: no cover - defensive
            spki_bits = None

        spki_sha256 = sha256(spki.dump()).digest().hex()

        return ParsedCertificate(
            subject=subject,
            issuer=issuer,
            serial_dec=serial_dec,
            serial_bin=serial_bin,
            not_before=not_before,
            not_after=not_after,

            ski=_to_hex(ski_bytes),
            aki=_to_hex(aki_bytes),
            is_ca=is_ca,
            spki_algorithm=spki_algorithm,
            spki_bits=spki_bits,
            spki_sha256=spki_sha256,
            resources_ip=resources_ip,
            resources_asn=resources_asn,
            sia=sia,
            aia=aia,
            cdp=cdp,
            key_usage=key_usage,
            ext_key_usage=ext_key_usage,
            basic_constraints=basic_constraints,
        )
    except Exception as exc:  # pragma: no cover - defensive
        raise CertificateParsingError("Failed to decode certificate") from exc
