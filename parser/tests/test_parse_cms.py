from __future__ import annotations

from asn1crypto import cms, core

from parse_cms import detect_cms_object_type

SIGNED_DATA_OID = "1.2.840.113549.1.7.2"
ROA_CONTENT_OID = "1.2.840.113549.1.9.16.1.24"


def _build_content_info(econtent_oid: str) -> cms.ContentInfo:
    signed_data = cms.SignedData(
        {
            "version": "v3",
            "digest_algorithms": cms.DigestAlgorithms([]),
            "encap_content_info": cms.EncapsulatedContentInfo(
                {
                    "content_type": cms.ContentType(econtent_oid),
                    "content": core.ParsableOctetString(b"payload"),
                }
            ),
            "certificates": cms.CertificateSet([]),
            "crls": None,
            "signer_infos": cms.SignerInfos([]),
        }
    )
    return cms.ContentInfo(
        {
            "content_type": cms.ContentType(SIGNED_DATA_OID),
            "content": signed_data,
        }
    )


def test_detect_cms_object_type_returns_expected_type():
    content_info = _build_content_info(ROA_CONTENT_OID)
    result = detect_cms_object_type(content_info.dump())

    assert result is not None
    assert result.object_type == "ROA"


def test_detect_cms_object_type_gracefully_handles_invalid_cms_bytes():
    assert detect_cms_object_type(b"not asn1") is None


def test_detect_cms_object_type_returns_none_for_constructed_oid():
    malformed = bytearray(_build_content_info(ROA_CONTENT_OID).dump())
    for index, value in enumerate(malformed):
        if value == 0x06:  # OBJECT IDENTIFIER tag
            malformed[index] = 0x26  # force constructed bit to reproduce crash
            break

    assert detect_cms_object_type(bytes(malformed)) is None
