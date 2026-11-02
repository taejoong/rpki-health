"""GBR parsing helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

try:  # pragma: no cover - optional dependency for parsing real objects
    from asn1crypto import cms, core
except ImportError:  # pragma: no cover - optional dependency
    cms = None  # type: ignore
    core = None  # type: ignore

GBR_CONTENT_OID = "1.2.840.113549.1.9.16.1.35"


if core is not None:

    class GBRVCard(core.OctetString):
        pass

    class GBRHCard(core.OctetString):
        pass

    class GBRFN(core.UTF8String):
        pass

    class GBRORG(core.UTF8String):
        pass

    class GBRADR(core.UTF8String):
        pass

    class GBRTEL(core.UTF8String):
        pass

    class GBREMAIL(core.IA5String):
        pass

    class GBRContact(core.Sequence):
        _fields = [
            ("vCard", GBRVCard, {"optional": True}),
            ("hCard", GBRHCard, {"optional": True}),
            ("fn", GBRFN, {"optional": True}),
            ("org", GBRORG, {"optional": True}),
            ("adr", GBRADR, {"optional": True}),
            ("tel", GBRTEL, {"optional": True}),
            ("email", GBREMAIL, {"optional": True}),
        ]

    class GhostbustersRecord(core.Sequence):
        _fields = [
            ("version", core.Integer, {"default": 0, "implicit": 0}),
            ("contact", GBRContact),
        ]


@dataclass(frozen=True)
class ParsedGBR:
    vcard: Optional[str]
    hcard: Optional[str]
    fn: Optional[str]
    org: Optional[str]
    adr: Optional[str]
    tel: Optional[str]
    email: Optional[str]
    ee_serial: str | None
    ee_not_before: datetime | None
    ee_not_after: datetime | None
    signing_time: datetime | None
    ee_public_key: str | None


class GBRParsingError(RuntimeError):
    """Raised when GBR parsing fails."""


def parse_gbr_econtent(econtent: bytes) -> ParsedGBR:  # pragma: no cover - requires asn1crypto
    if core is None:
        raise GBRParsingError("asn1crypto is required for GBR parsing")
    try:
        gbr = GhostbustersRecord.load(econtent)
    except Exception as exc:
        raise GBRParsingError("Failed to decode GhostbustersRecord") from exc

    contact = gbr["contact"]
    vcard = contact["vCard"].native.decode("utf-8", errors="replace") if "vCard" in contact and contact["vCard"].native else None
    hcard = contact["hCard"].native.decode("utf-8", errors="replace") if "hCard" in contact and contact["hCard"].native else None
    fn = contact["fn"].native if "fn" in contact and contact["fn"].native else None
    org = contact["org"].native if "org" in contact and contact["org"].native else None
    adr = contact["adr"].native if "adr" in contact and contact["adr"].native else None
    tel = contact["tel"].native if "tel" in contact and contact["tel"].native else None
    email = contact["email"].native if "email" in contact and contact["email"].native else None

    return ParsedGBR(
        vcard=vcard,
        hcard=hcard,
        fn=fn,
        org=org,
        adr=adr,
        tel=tel,
        email=email,
        ee_serial=None,
        ee_not_before=None,
        ee_not_after=None,
        signing_time=None,
        ee_public_key=None,
    )


def parse_gbr_signed_object(content_info):  # pragma: no cover - requires asn1crypto
    if cms is None:
        raise GBRParsingError("asn1crypto is required for GBR parsing")
    if content_info["content_type"].dotted != "1.2.840.113549.1.7.2":
        raise GBRParsingError("ContentInfo is not SignedData")
    signed_data = content_info["content"]
    econtent = signed_data["encap_content_info"]["content"].native
    if econtent is None:
        raise GBRParsingError("SignedData is missing eContent")

    gbr_data = parse_gbr_econtent(econtent)

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

    return ParsedGBR(
        vcard=gbr_data.vcard,
        hcard=gbr_data.hcard,
        fn=gbr_data.fn,
        org=gbr_data.org,
        adr=gbr_data.adr,
        tel=gbr_data.tel,
        email=gbr_data.email,
        ee_serial=ee_serial,
        ee_not_before=ee_not_before,
        ee_not_after=ee_not_after,
        signing_time=signing_time,
        ee_public_key=ee_public_key,
    )

