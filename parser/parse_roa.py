"""ROA parsing and semantic hashing helpers."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

try:  # pragma: no cover - optional dependency for parsing real objects
    from asn1crypto import cms, core
except ImportError:  # pragma: no cover - optional dependency
    cms = None  # type: ignore
    core = None  # type: ignore

from util import PrefixAuthorisation, build_prefix_authorizations, compute_sha256

ROA_CONTENT_OID = "1.2.840.113549.1.9.16.1.24"


if core is not None:

    class ROAIPAddress(core.Sequence):
        _fields = [
            ("address", core.BitString),
            ("maxLength", core.Integer, {"optional": True}),
        ]

    class ROAIPAddressFamily(core.Sequence):
        _fields = [
            ("addressFamily", core.OctetString),
            ("ipAddressChoice", core.SequenceOf, {"spec": ROAIPAddress}),
        ]

    class RouteOriginAttestation(core.Sequence):
        _fields = [
            ("version", core.Integer, {"default": 0, "implicit": 0}),
            ("asID", core.Integer),
            (
                "ipAddrBlocks",
                core.SequenceOf,
                {"spec": ROAIPAddressFamily},
            ),
        ]

@dataclass(frozen=True)
class ParsedROA:
    asn: int
    prefixes: list[PrefixAuthorisation]
    ee_serial: str | None
    ee_not_before: datetime | None
    ee_not_after: datetime | None
    signing_time: datetime | None

    ee_public_key: str | None


class ROAParsingError(RuntimeError):
    """Raised when ROA parsing fails."""


AFI_IPV4 = 1
AFI_IPV6 = 2


def _bitstring_to_prefix(bitstring, afi: int) -> Tuple[str, int]:  # pragma: no cover - requires asn1crypto
    if core is None:
        raise ROAParsingError("asn1crypto is required for ROA parsing")

    contents = bitstring.contents
    if not contents:
        raise ROAParsingError("Empty bitstring in ROA prefix")
    unused = contents[0]
    raw_bytes = contents[1:]
    prefix_bits = len(raw_bytes) * 8 - unused
    if afi == AFI_IPV4:
        width = 4
    elif afi == AFI_IPV6:
        width = 16
    else:
        raise ROAParsingError(f"Unsupported AFI: {afi}")

    padded = raw_bytes.ljust(width, b"\x00")
    mask = int.from_bytes(padded, "big")
    if prefix_bits > width * 8:
        raise ROAParsingError("Prefix length exceeds address width")
    shift = width * 8 - prefix_bits
    mask >>= shift
    mask <<= shift
    canonical_bytes = mask.to_bytes(width, "big")

    import ipaddress

    network = (
        ipaddress.IPv4Network((canonical_bytes, prefix_bits))
        if afi == AFI_IPV4
        else ipaddress.IPv6Network((canonical_bytes, prefix_bits))
    )
    return network.network_address.compressed, prefix_bits


def parse_roa_econtent(econtent: bytes) -> Tuple[int, list[PrefixAuthorisation]]:  # pragma: no cover - requires asn1crypto
    if core is None:
        raise ROAParsingError("asn1crypto is required for ROA parsing")
    try:
        roa = RouteOriginAttestation.load(econtent)
    except Exception as exc:
        raise ROAParsingError("Failed to decode RouteOriginAttestation") from exc

    asn = int(roa["asID"].native)
    authorisations: list[tuple[int, str, int]] = []

    for family in roa["ipAddrBlocks"]:
        afi_bytes: bytes = family["addressFamily"].native
        if len(afi_bytes) < 2:
            raise ROAParsingError("Invalid AFI encoding")
        afi = int.from_bytes(afi_bytes[:2], "big")
        if afi not in (AFI_IPV4, AFI_IPV6):
            continue
        afi_value = 4 if afi == AFI_IPV4 else 6
        for address in family["ipAddressChoice"]:
            prefix, prefix_length = _bitstring_to_prefix(address["address"], afi)
            max_length = prefix_length
            if "maxLength" in address and address["maxLength"].native is not None:
                max_length = int(address["maxLength"].native)
            authorisations.append((afi_value, f"{prefix}/{prefix_length}", max_length))

    prefixes = build_prefix_authorizations(authorisations)
    return asn, prefixes


def parse_roa_signed_object(content_info):  # pragma: no cover - requires asn1crypto
    if cms is None:
        raise ROAParsingError("asn1crypto is required for ROA parsing")
    if content_info["content_type"].dotted != "1.2.840.113549.1.7.2":
        raise ROAParsingError("ContentInfo is not SignedData")
    signed_data = content_info["content"]
    econtent = signed_data["encap_content_info"]["content"].native
    if econtent is None:
        raise ROAParsingError("SignedData is missing eContent")

    asn, prefixes = parse_roa_econtent(econtent)

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

    return ParsedROA(
        asn=asn,
        prefixes=prefixes,
        ee_serial=ee_serial,
        ee_not_before=ee_not_before,
        ee_not_after=ee_not_after,
        signing_time=signing_time,
        ee_public_key=ee_public_key,
    )


def serialise_semantic_model(parsed: ParsedROA) -> str:
    ordered = sorted(parsed.prefixes, key=PrefixAuthorisation.to_tuple)
    payload = {
        "asn": parsed.asn,
        "roa": [
            {"afi": p.afi, "prefix": p.prefix, "maxLength": p.max_length}
            for p in ordered
        ],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def compute_semantic_hash(parsed: ParsedROA) -> str:
    json_payload = serialise_semantic_model(parsed)
    return compute_sha256(json_payload.encode("utf-8"))
