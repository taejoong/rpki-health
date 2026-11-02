#!/usr/bin/env python3
"""Build an RP-validity sidecar from rpki-client's per-file verdict stream.

rpki-client downloads *every* object it can reach into the cache, but only a
subset passes RP validation. The valid set is what ends up in the openbgpd/JSON
output; the objects it rejects are named on stderr (one line per file) and then
discarded. Our warehouse ETL parses the whole cache, so to distinguish
RP-valid from RP-invalid objects we capture those stderr rejection lines here.

Pipeline:
    rpki-client -v ... 2> rpki-client.stderr.log     (see rpki_cycle.sh)
    build_validity_map.py --stderr-log ... --cache-dir ... --output map.json
    rpki_etl.py single ... --validity-map map.json

Output JSON shape (consumed by rpki_etl.load_validity_map):
    { "<object_hash>": {"state": 2, "reason": "not-yet-valid"}, ... }

Only *rejected* objects appear in the map; anything absent is treated as valid
by the ETL. Matching is by content hash (SHA-256), which is stable across the
cache's rsync vs RRDP layouts and session-hash reshuffles -- so we resolve each
error path to its actual cache file and hash the bytes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from collections import Counter
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger("build_validity_map")

VALIDATION_INVALID = 2

# Object file extensions rpki-client manages in the cache.
_OBJ_EXT = r"(?:roa|mft|cer|crl|asa|gbr|tak|spl|sig)"
# Grab the longest non-whitespace token that ends in a known object extension.
_PATH_RE = re.compile(r"(?P<path>[^\s:]+\." + _OBJ_EXT + r")\b")

# Reason classification, checked in order. Each (substring, reason_code).
# Keyword-based so it survives rpki-client wording/version drift. The concrete
# rpki-client 9.x per-file messages observed in production stderr are:
#   "CRL has expired"                                  -> expired
#   "no valid manifest available"                      -> no-manifest
#   "certificate has expired"                          -> expired
#   "certificate is not yet valid"                     -> not-yet-valid
#   "trust anchor constraints violation: ..."          -> ta-constraint
#   "RFC 3779 resource not subset of parent's ..."     -> resource-violation
#   "missing IP or AS resources"                       -> resource-violation
_REASON_RULES: Tuple[Tuple[str, str], ...] = (
    ("not yet valid", "not-yet-valid"),
    ("bad message digest", "bad-digest"),
    ("mismatched digest", "bad-digest"),
    ("no valid manifest", "no-manifest"),
    ("trust anchor constraints", "ta-constraint"),
    ("stale", "stale-mft"),
    ("has expired", "expired"),
    ("expired", "expired"),
    ("revoked", "revoked"),
    ("not subset", "resource-violation"),
    ("RFC 3779", "resource-violation"),
    ("missing ip or as", "resource-violation"),
    ("failed verification", "failed-verification"),
    ("failed parse", "failed-parse"),
    ("parse error", "failed-parse"),
    ("parse failed", "failed-parse"),
)

# A line is only treated as a rejection if it carries one of these signals.
# (Gate keeps informational lines like "pulling from .../ta.cer" out of the map.)
_REJECTION_SIGNALS = (
    "fail",
    "invalid",
    "stale",
    "not yet valid",
    "expired",
    "bad message digest",
    "mismatched digest",
    "discarded",
    "revoked",
    "RFC 3779",
    "not subset",
    "parse error",
    "no valid manifest",
    "trust anchor constraints",
    "missing ip or as",
)


def classify_reason(line: str) -> Optional[str]:
    """Return a reason_code if the line denotes a rejection, else None."""
    low = line.lower()
    if not any(sig in low for sig in (s.lower() for s in _REJECTION_SIGNALS)):
        return None
    for needle, code in _REASON_RULES:
        if needle.lower() in low:
            return code
    # Rejection signal present but unclassified -> generic invalid.
    return "invalid"


def extract_path(line: str) -> Optional[str]:
    match = _PATH_RE.search(line)
    return match.group("path") if match else None


def _strip_transport_prefix(token: str) -> str:
    """Drop a leading ``.rrdp/<session-hash>/`` or ``.rsync/`` transport prefix.

    rpki-client 9.8 prints rejection paths *with* their transport prefix
    (``.rrdp/<session-hash>/<host>/...``), but the bytes for an object may live
    under a different session dir or the flat ``<host>/...`` tree. Stripping the
    prefix recovers the transport-agnostic ``<host>/<repo-path>/<file>`` tail so
    the resolver can probe every layout. Returns the token unchanged if no known
    prefix is present.
    """
    parts = token.split("/")
    if len(parts) > 2 and parts[0] == ".rrdp":
        return "/".join(parts[2:])  # drop ".rrdp" + <session-hash>
    if len(parts) > 1 and parts[0] == ".rsync":
        return "/".join(parts[1:])  # drop ".rsync"
    return token


def resolve_cache_file(token: str, cache_dir: Path) -> Optional[Path]:
    """Resolve an rpki-client error path to a real file under the cache.

    rpki-client prints object paths either bare (``<host>/<repo-path>/<file>``,
    older releases) or transport-prefixed (``.rrdp/<session-hash>/<host>/...`` in
    9.8). The bytes are stored under one of:
        .rrdp/<session-hash>/<host>/<repo-path>/<file>   (RRDP, the common case)
        .rsync/<host>/<repo-path>/<file>                 (rsync fallback)
        <host>/<repo-path>/<file>                         (flat layout)
    The printed prefix and the on-disk prefix need not match, so we strip any
    transport prefix to a bare ``<host>/...`` tail and probe all layouts.
    """
    if token.startswith("./"):
        token = token[2:]

    candidates = [token]
    tail = _strip_transport_prefix(token)
    if tail != token:
        candidates.append(tail)

    for cand in candidates:
        # 1) exact path as printed (covers flat layout and correct .rrdp/.rsync)
        if (cache_dir / cand).is_file():
            return cache_dir / cand
        # 2) rsync fallback tree
        if (cache_dir / ".rsync" / cand).is_file():
            return cache_dir / ".rsync" / cand

    # 3) RRDP: bytes live under .rrdp/<session-hash>/<tail>. The session hash is
    #    opaque (and may differ from the printed one), so glob across session
    #    dirs and take the first match.
    rrdp = cache_dir / ".rrdp"
    if rrdp.is_dir():
        for match in rrdp.glob(f"*/{tail}"):
            if match.is_file():
                return match
    return None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_map(
    stderr_log: Path, cache_dir: Path
) -> Tuple[Dict[str, Dict[str, object]], Counter, int]:
    """Parse the stderr log into {object_hash: {state, reason}}.

    Returns (map, reason_counts, unresolved_count).
    """
    validity: Dict[str, Dict[str, object]] = {}
    reason_counts: Counter = Counter()
    unresolved = 0
    seen_paths: set[str] = set()

    with open(stderr_log, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            reason = classify_reason(line)
            if reason is None:
                continue
            token = extract_path(line)
            if token is None:
                continue
            if token in seen_paths:
                continue
            seen_paths.add(token)
            cache_file = resolve_cache_file(token, cache_dir)
            if cache_file is None:
                unresolved += 1
                logger.debug("unresolved path: %s", token)
                continue
            try:
                object_hash = sha256_file(cache_file)
            except OSError as exc:  # pragma: no cover - defensive I/O
                unresolved += 1
                logger.debug("hash failed for %s: %s", cache_file, exc)
                continue
            validity[object_hash] = {"state": VALIDATION_INVALID, "reason": reason}
            reason_counts[reason] += 1

    return validity, reason_counts, unresolved


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stderr-log",
        type=Path,
        required=True,
        help="Path to the captured rpki-client -v stderr log.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        required=True,
        help="rpki-client cache directory the verdicts refer to.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Destination JSON sidecar path.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.stderr_log.is_file():
        logger.warning("stderr log not found: %s -- writing empty map", args.stderr_log)
        args.output.write_text("{}", encoding="utf-8")
        return

    validity, reason_counts, unresolved = build_map(args.stderr_log, args.cache_dir)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(validity, fh)

    logger.info(
        "validity map: %d invalid objects (unresolved paths: %d) -> %s",
        len(validity),
        unresolved,
        args.output,
    )
    for reason, count in reason_counts.most_common():
        logger.info("  %-20s %d", reason, count)


if __name__ == "__main__":
    main()
