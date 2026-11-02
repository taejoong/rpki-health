from __future__ import annotations

import datetime as dt

from parse_mft import FileAndHash, Manifest, ManifestParsingError, parse_manifest_econtent


def _build_manifest(**overrides):
    manifest = Manifest(
        {
            "version": 0,
            "manifestNumber": 42,
            "thisUpdate": "20240101000000Z",
            "nextUpdate": "20240102000000Z",
            "fileHashAlg": "2.16.840.1.101.3.4.2.1",
            "fileList": [
                FileAndHash(
                    {
                        "file": {"utf8": "test.roa"},
                        "hash": b"\x00" * 32,
                    }
                )
            ],
        }
    )
    for key, value in overrides.items():
        manifest[key] = value
    return manifest


def test_parse_manifest_econtent_round_trip():
    manifest = _build_manifest()
    parsed = parse_manifest_econtent(manifest.dump())

    assert parsed.manifest_number == "42"
    assert parsed.file_hash_alg == "2.16.840.1.101.3.4.2.1"
    assert parsed.files == [("test.roa", "00" * 32)]

    expected_update = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    expected_next_update = dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc)
    assert parsed.this_update == expected_update.isoformat()
    assert parsed.next_update == expected_next_update.isoformat()


def test_parse_manifest_econtent_invalid_payload():
    try:
        parse_manifest_econtent(b"not der")
    except ManifestParsingError as exc:
        assert "decode" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ManifestParsingError")


def test_parse_manifest_econtent_handles_ia5_strings():
    manifest = _build_manifest(
        fileList=[
            FileAndHash(
                {
                    "file": {"ia5": "rsync://example.com/repo/foo.roa"},
                    "hash": b"\xff" * 32,
                }
            )
        ]
    )

    parsed = parse_manifest_econtent(manifest.dump())

    assert parsed.files == [("rsync://example.com/repo/foo.roa", "ff" * 32)]


