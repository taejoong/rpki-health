"""Unit tests for build_validity_map: stderr parsing, path resolution, hashing."""
from __future__ import annotations

import hashlib
from pathlib import Path

import build_validity_map as bvm


def _write(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def test_classify_reason_real_messages():
    cases = {
        "rpki-client: a/b/X.mft: CRL has expired": "expired",
        "rpki-client: a/b/Y.cer: certificate has expired": "expired",
        "rpki-client: a/b/Z.cer: certificate is not yet valid": "not-yet-valid",
        "rpki-client: a/b/W.asa: trust anchor constraints violation: AS 1": "ta-constraint",
        "rpki-client: a/b/V.mft: no valid manifest available": "no-manifest",
        "rpki-client: a/b/U.roa: RFC 3779 resource not subset of parent's resources": "resource-violation",
        "rpki-client: a/b/T.cer: missing IP or AS resources": "resource-violation",
    }
    for line, reason in cases.items():
        assert bvm.classify_reason(line) == reason, line


def test_classify_reason_ignores_informational():
    # progress / fetch lines that happen to mention an object path are not rejections
    assert bvm.classify_reason("rpki-client: ta/ripe: pulling from https://x/ripe-ncc-ta.cer") is None
    assert bvm.classify_reason("rpki-client: all files parsed: generating output") is None


def test_extract_path_takes_object_token():
    line = "rpki-client: host/repo/uuid/0/ABCDEF.mft: CRL has expired"
    assert bvm.extract_path(line) == "host/repo/uuid/0/ABCDEF.mft"


def test_resolve_cache_file_rrdp_layout(tmp_path):
    token = "rpki.example.net/repo/foo/0/AA.mft"
    real = tmp_path / ".rrdp" / "SESSIONHASH" / token
    _write(real, b"manifest-bytes")
    resolved = bvm.resolve_cache_file(token, tmp_path)
    assert resolved == real


def test_resolve_cache_file_98_transport_prefixed_token(tmp_path):
    # rpki-client 9.8 prints the path WITH a .rrdp/<session-hash>/ prefix, but the
    # bytes may live in the flat layout (or a different session dir). The resolver
    # must strip the prefix and still find the file.
    tail = "rpki.example.net/repo/foo/0/AA.roa"
    printed = ".rrdp/2A58FCEC0000SESSIONHASH/" + tail
    real = tmp_path / tail  # stored flat, not under the printed session dir
    _write(real, b"expired-roa-bytes")
    assert bvm.resolve_cache_file(printed, tmp_path) == real
    # also resolves when the bytes live under a *different* session hash dir
    tail2 = "rpki.example.net/repo/bar/0/BB.roa"
    printed2 = ".rrdp/PRINTEDHASH/" + tail2
    real2 = tmp_path / ".rrdp" / "ACTUALHASH" / tail2
    _write(real2, b"other-bytes")
    assert bvm.resolve_cache_file(printed2, tmp_path) == real2


def test_build_map_end_to_end(tmp_path):
    cache = tmp_path / "cache"
    # An RRDP-stored manifest that rpki-client rejected (CRL expired).
    token = "rpki.example.net/repo/foo/0/AA.mft"
    h = _write(cache / ".rrdp" / "DEADBEEF" / token, b"the-rejected-bytes")
    # An rsync-fallback cert rejected as expired.
    token2 = "rpki.example.net/repo/foo/bar.cer"
    h2 = _write(cache / ".rsync" / token2, b"expired-cert-bytes")

    stderr = tmp_path / "rpki.stderr.log"
    stderr.write_text(
        "rpki-client: ta/ripe: pulling from https://x/ripe-ncc-ta.cer\n"
        f"rpki-client: {token}: CRL has expired\n"
        f"rpki-client: {token2}: certificate has expired\n"
        # absent file -> unresolved, must NOT appear in map
        "rpki-client: rpki.example.net/repo/foo/0/MISSING.mft: no valid manifest available\n"
        "rpki-client: all files parsed: generating output\n",
        encoding="utf-8",
    )

    validity, counts, unresolved = bvm.build_map(stderr, cache)

    assert validity == {
        h: {"state": bvm.VALIDATION_INVALID, "reason": "expired"},
        h2: {"state": bvm.VALIDATION_INVALID, "reason": "expired"},
    }
    assert counts["expired"] == 2
    assert unresolved == 1  # the MISSING.mft (no-valid-manifest) file is absent
