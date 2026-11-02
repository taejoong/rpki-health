"""Manifest parsing helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from asn1crypto import cms, core


class FileName(core.Choice):
    """Accept common encodings for manifest file identifiers."""

    _alternatives = [
        ("ia5", core.IA5String),
        ("utf8", core.UTF8String),
        ("printable", core.PrintableString),
    ]


class ManifestFileHash(core.OctetString):
    _bad_tag = 3

    @property
    def native(self):  # type: ignore[override]
        if self.tag == 3:
            contents = self.contents
            if not contents:
                return b""
            return contents[1:]
        return super().native


class FileAndHash(core.Sequence):
    _fields = [
        ("file", FileName),
        ("hash", ManifestFileHash),
    ]


class Manifest(core.Sequence):
    _fields = [
        ("version", core.Integer, {"default": 0, "implicit": 0}),
        ("manifestNumber", core.Integer),
        ("thisUpdate", core.GeneralizedTime),
        ("nextUpdate", core.GeneralizedTime),
        ("fileHashAlg", core.ObjectIdentifier),
        ("fileList", core.SequenceOf, {"spec": FileAndHash}),
    ]
@dataclass
class ParsedManifest:
    manifest_number: str
    this_update: str
    next_update: str
    file_hash_alg: str
    files: List[tuple[str, str]]


class ManifestParsingError(RuntimeError):
    pass


MANIFEST_OID = "1.2.840.113549.1.9.16.1.26"

_HASH_OID_TO_HEX_DIGITS: Dict[str, int] = {
    # SHA-224
    "2.16.840.1.101.3.4.2.4": 56,
    # SHA-256
    "2.16.840.1.101.3.4.2.1": 64,
    # SHA-384
    "2.16.840.1.101.3.4.2.2": 96,
    # SHA-512
    "2.16.840.1.101.3.4.2.3": 128,
}


def _format_hash(value: bytes, hash_alg_oid: str) -> str:
    expected_digits: Optional[int] = _HASH_OID_TO_HEX_DIGITS.get(hash_alg_oid)
    hex_value = value.hex()
    if expected_digits is not None:
        hex_value = hex_value.rjust(expected_digits, "0")
    return hex_value


def parse_manifest_econtent(econtent: bytes) -> ParsedManifest:
    try:
        manifest = Manifest.load(econtent)
    except Exception as exc:  # pragma: no cover - defensive
        raise ManifestParsingError("Failed to decode manifest") from exc


    hash_alg_oid = manifest["fileHashAlg"].dotted

    files = []
    for entry in manifest["fileList"]:
        filename = entry["file"].native
        hash_value = entry["hash"].native
        if not hash_value:
            raise ManifestParsingError("Manifest file hash is empty")

        files.append((filename, _format_hash(hash_value, hash_alg_oid)))

    return ParsedManifest(
        manifest_number=str(manifest["manifestNumber"].native),
        this_update=manifest["thisUpdate"].native.isoformat(),
        next_update=manifest["nextUpdate"].native.isoformat(),
        file_hash_alg=hash_alg_oid,
        files=files,
    )


def parse_manifest(content_info: cms.ContentInfo) -> ParsedManifest:
    signed_data = content_info["content"]
    econtent = signed_data["encap_content_info"]["content"].native
    if econtent is None:
        raise ManifestParsingError("SignedData is missing manifest content")
    return parse_manifest_econtent(econtent)
