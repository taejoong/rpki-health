"""Utility helpers for the RPKI ETL pipeline."""
from __future__ import annotations

import datetime as _dt
import hashlib
import ipaddress
import re
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple

__all__ = [
    "compute_sha256",
    "canonicalize_ip_prefix",
    "build_prefix_authorizations",
    "coalesce_day_intervals",
    "dataset_day_to_id",
    "clean_dn",
]



_SERIAL_TOKEN = re.compile(
    r"(?i)\bserial\s*number\s*[:=]\s*[0-9A-Fa-f]+\b\s*,?"
)
_CN_KEY = re.compile(r"(?i)\bcommon\s*name\s*[:=]\s*")
_CN_EQ = re.compile(r"(?i)\bcn\s*=\s*")
_DN_PREFIXES = ("Serial Number:", "Common Name :", "Common Name:", "CN=", "SN=")


def clean_dn(value: str | None) -> str | None:
    """Normalise human-friendly distinguished names for storage."""

    if value is None:
        return None

    s = value.strip()
    s = _SERIAL_TOKEN.sub("", s)
    for prefix in _DN_PREFIXES:
        if s.startswith(prefix):
            s = s[len(prefix) :].strip()
    s = _CN_KEY.sub("", s)
    s = _CN_EQ.sub("", s)

    if s.startswith("Common Name: "):
        s = s[len("Common Name: ") :]
    if s.startswith("Common Name : "):
        s = s[len("Common Name : ") :]

    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"(^\s*,\s*|\s*,\s*$)", "", s)
    return s.strip()


def compute_sha256(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def _normalize_ipv4(address: str) -> str:
    octets = address.split(".")
    if len(octets) != 4:
        raise ValueError
    return ".".join(str(int(part)) for part in octets)


def canonicalize_ip_prefix(prefix: str) -> str:
    try:
        network = ipaddress.ip_network(prefix, strict=False)
    except ValueError:
        if "/" not in prefix:
            raise
        address, length = prefix.split("/", 1)
        try:
            normalized = _normalize_ipv4(address)
        except ValueError as exc:
            raise ValueError(f"Invalid prefix: {prefix!r}") from exc
        network = ipaddress.ip_network(f"{normalized}/{length}", strict=False)
    return network.compressed


@dataclass(frozen=True)
class PrefixAuthorisation:
    afi: int
    prefix: str
    max_length: int

    def to_tuple(self) -> Tuple[int, str, int]:
        return (self.afi, self.prefix, self.max_length)


def build_prefix_authorizations(
    authorisations: Iterable[Tuple[int, str, int]]
) -> List[PrefixAuthorisation]:
    seen = set()
    result: List[PrefixAuthorisation] = []
    for afi, prefix, max_length in authorisations:
        if afi not in (4, 6):
            raise ValueError(f"Unsupported AFI {afi!r}; expected 4 or 6")
        canonical = canonicalize_ip_prefix(prefix)
        key = (afi, canonical, int(max_length))
        if key in seen:
            continue
        seen.add(key)
        result.append(PrefixAuthorisation(*key))

    result.sort(key=PrefixAuthorisation.to_tuple)
    return result


@dataclass
class Interval:
    start_day_id: int
    end_day_id: int

    def extend_to(self, new_day_id: int) -> None:
        if new_day_id < self.end_day_id:
            raise ValueError("Intervals must be appended in non-decreasing order")
        self.end_day_id = new_day_id


def coalesce_day_intervals(existing: Sequence[Interval], new_day_id: int) -> List[Interval]:
    if not existing:
        return [Interval(new_day_id, new_day_id)]

    intervals = list(existing)
    last = intervals[-1]
    if new_day_id == last.end_day_id + 1:
        last.extend_to(new_day_id)
        return intervals
    if new_day_id <= last.end_day_id:
        return intervals

    intervals.append(Interval(new_day_id, new_day_id))
    return intervals


def dataset_day_to_id(day: str) -> int:
    try:
        date = _dt.date.fromisoformat(day)
    except ValueError as exc:
        raise ValueError(f"Invalid dataset day: {day!r}") from exc
    epoch = _dt.date(1970, 1, 1)
    return (date - epoch).days
