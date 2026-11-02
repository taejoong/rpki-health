"""ASPA parsing helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

try:  # pragma: no cover - optional dependency for parsing real objects
    from asn1crypto import cms, core
except ImportError:  # pragma: no cover - optional dependency
    cms = None  # type: ignore
    core = None  # type: ignore

ASPA_CONTENT_OID = "1.2.840.113549.1.9.16.1.49"


if core is not None:

    class ASPACustomerASID(core.Integer):
        pass

    class ASPAProviderASID(core.Integer):
        pass

    class ASPAAfiLimit(core.SequenceOf):
        _spec = core.Integer

    class ASPAProviderAS(core.Sequence):
        _fields = [
            ("providerASID", ASPAProviderASID),
            ("afiLimit", ASPAAfiLimit, {"optional": True}),
        ]

    class ASPAProviderASSet(core.SequenceOf):
        _spec = ASPAProviderAS

    class AutonomousSystemProviderAttestation(core.Sequence):
        _fields = [
            ("version", core.Integer, {"default": 0, "implicit": 0}),
            ("customerASID", ASPACustomerASID),
            ("providerASSet", ASPAProviderASSet),
        ]


@dataclass(frozen=True)
class ASPAProvider:
    provider_asn: int
    afi_limit: Optional[List[int]]


@dataclass(frozen=True)
class ParsedASPA:
    customer_asn: int
    providers: List[ASPAProvider]
    ee_serial: str | None
    ee_not_before: datetime | None
    ee_not_after: datetime | None
    signing_time: datetime | None
    ee_public_key: str | None


class ASPAParsingError(RuntimeError):
    """Raised when ASPA parsing fails."""


def parse_aspa_econtent(econtent: bytes) -> tuple[int, List[ASPAProvider]]:  # pragma: no cover - requires asn1crypto
    if core is None:
        raise ASPAParsingError("asn1crypto is required for ASPA parsing")
    try:
        aspa = AutonomousSystemProviderAttestation.load(econtent)
    except Exception as exc:
        raise ASPAParsingError("Failed to decode AutonomousSystemProviderAttestation") from exc

    customer_asn = int(aspa["customerASID"].native)
    providers: List[ASPAProvider] = []

    # Iterate over providerASSet and get native representation of each entry
    for provider_entry in aspa["providerASSet"]:
        provider_entry_native = provider_entry.native
        
        # Handle different cases: provider_entry_native can be an int, tuple, or dict
        if isinstance(provider_entry_native, int):
            # Simple case: just an ASN, no afiLimit
            provider_asn = provider_entry_native
            afi_limit = None
        elif isinstance(provider_entry_native, tuple):
            # Tuple case: (providerASID, afiLimit) where afiLimit is optional
            provider_asn = int(provider_entry_native[0])
            afi_limit = None
            if len(provider_entry_native) > 1 and provider_entry_native[1] is not None:
                afi_limit_list = provider_entry_native[1]
                if isinstance(afi_limit_list, list):
                    afi_limit = [int(x) for x in afi_limit_list]
                else:
                    afi_limit = [int(afi_limit_list)]
        elif isinstance(provider_entry_native, dict):
            # Dict case: has providerASID and optionally afiLimit
            # Try different possible key names
            if "providerASID" in provider_entry_native:
                provider_asn = int(provider_entry_native["providerASID"])
            elif 0 in provider_entry_native:  # Maybe indexed by position
                provider_asn = int(provider_entry_native[0])
            else:
                raise ASPAParsingError(f"Could not find providerASID in dict: {list(provider_entry_native.keys())}")
            afi_limit = None
            if "afiLimit" in provider_entry_native and provider_entry_native["afiLimit"] is not None:
                afi_limit_list = provider_entry_native["afiLimit"]
                if isinstance(afi_limit_list, list):
                    afi_limit = [int(x) for x in afi_limit_list]
                else:
                    afi_limit = [int(afi_limit_list)]
            elif 1 in provider_entry_native and provider_entry_native[1] is not None:
                afi_limit_list = provider_entry_native[1]
                if isinstance(afi_limit_list, list):
                    afi_limit = [int(x) for x in afi_limit_list]
                else:
                    afi_limit = [int(afi_limit_list)]
        else:
            raise ASPAParsingError(f"Unexpected provider entry structure: {type(provider_entry_native)}, value: {provider_entry_native}")
        providers.append(ASPAProvider(provider_asn=provider_asn, afi_limit=afi_limit))

    return customer_asn, providers


def parse_aspa_signed_object(content_info):  # pragma: no cover - requires asn1crypto
    if cms is None:
        raise ASPAParsingError("asn1crypto is required for ASPA parsing")
    if content_info["content_type"].dotted != "1.2.840.113549.1.7.2":
        raise ASPAParsingError("ContentInfo is not SignedData")
    signed_data = content_info["content"]
    econtent = signed_data["encap_content_info"]["content"].native
    if econtent is None:
        raise ASPAParsingError("SignedData is missing eContent")

    customer_asn, providers = parse_aspa_econtent(econtent)

    ee_serial: str | None = None
    ee_not_before: datetime | None = None
    ee_not_after: datetime | None = None
    ee_public_key: str | None = None

    certificates = signed_data["certificates"] if "certificates" in signed_data else []
    if certificates:
        for certificate_choice in certificates:
            if certificate_choice.name != "certificate":
                continue
            ee_cert = certificate_choice.chosen
            tbs_cert = ee_cert["tbs_certificate"]
            ee_serial = str(tbs_cert["serial_number"].native)
            validity = tbs_cert["validity"]
            ee_not_before = validity["not_before"].native
            ee_not_after = validity["not_after"].native
            public_key = tbs_cert["subject_public_key_info"]["public_key"]
            contents = getattr(public_key, "contents", b"")
            if contents:
                unused_bits = contents[0]
                key_bytes = contents[1:]
                if unused_bits and key_bytes:
                    mask = 0xFF & (0xFF << unused_bits)
                    key_bytes = key_bytes[:-1] + bytes([key_bytes[-1] & mask])
                ee_public_key = key_bytes.hex()
            break

    signing_time: datetime | None = None
    signer_infos = signed_data["signer_infos"] if "signer_infos" in signed_data else []
    if signer_infos:
        signer_info = signer_infos[0]
        if "signed_attrs" in signer_info:
            for attr in signer_info["signed_attrs"]:
                if attr["type"].dotted == "1.2.840.113549.1.9.5":
                    signing_time = attr["values"][0].native
                    break

    return ParsedASPA(
        customer_asn=customer_asn,
        providers=providers,
        ee_serial=ee_serial,
        ee_not_before=ee_not_before,
        ee_not_after=ee_not_after,
        signing_time=signing_time,
        ee_public_key=ee_public_key,
    )

