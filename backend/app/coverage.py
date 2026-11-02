"""RPKI coverage of BGP-announced prefixes (RFC 6811 route origin validation).

/roa/coverage and /roa/org-coverage compare what an ASN currently originates
(BGP warehouse, rib.netsecurelab.org) with the live VRPs in ``live_roas``.
Each announced (prefix, origin AS) pair gets the state a validating router
would assign:

  valid      a VRP covers the prefix, names this origin AS, and its maxLength
             is >= the prefix length
  invalid    VRPs cover the prefix but none matches; reason "as" (no covering
             VRP names this AS) or "length" (one does, but the announcement is
             more specific than its maxLength)
  not_found  no VRP covers the prefix

"Currently originates" = seen with that origin by any of
BGP_COVERAGE_COLLECTORS in the last N daily RIBs, where the window ends at the
newest day all of those collectors have (ingest lags ~1-2 days). Prefixes
longer than /24 (IPv4) or /48 (IPv6) are left out: they are not globally
routable and show up only through single peers' internal or customer routes
(e.g. ~900 /26-/27s of AS3356 seen only via its sibling AS3549).
"""

import ipaddress
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import httpx

from app.config import (
    BGP_COVERAGE_COLLECTORS,
    BGP_WAREHOUSE_KEY_FILE,
    BGP_WAREHOUSE_URL,
)
from app.db import cache_get, cache_set

log = logging.getLogger(__name__)

VALID, INVALID, NOT_FOUND = "valid", "invalid", "not_found"

# Longest prefix counted as announced, per IP version
MAX_ROUTABLE_LEN = {4: 24, 6: 48}
# A collector whose newest RIB is this many days behind the freshest one is
# left out of the window instead of dragging every result into the past.
MAX_COLLECTOR_LAG_DAYS = 3

# Shared by all requests, so concurrent org-coverage calls cannot multiply the
# load on the BGP warehouse.
bgp_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bgp")


class BGPError(Exception):
    """The BGP warehouse could not answer (fully) for this request."""


def _api_key() -> str:
    try:
        return BGP_WAREHOUSE_KEY_FILE.read_text().strip()
    except FileNotFoundError:
        raise BGPError("BGP warehouse API key not configured")


# ── BGP side ──

def _collector_ends() -> dict:
    """Which coverage collectors are current, and the newest day they all have."""
    cached = cache_get("bgp_coverage_collectors")
    if cached is not None:
        return cached
    try:
        resp = httpx.get(
            f"{BGP_WAREHOUSE_URL}/api/collectors",
            headers={"X-API-Key": _api_key()},
            timeout=30.0,
        )
        resp.raise_for_status()
        ends = {c["collector"]: c.get("v2_to") for c in resp.json().get("collectors", [])}
    except (httpx.HTTPError, ValueError) as e:
        raise BGPError(f"BGP warehouse API error: {e}")
    days = {c: date.fromisoformat(ends[c]) for c in BGP_COVERAGE_COLLECTORS if ends.get(c)}
    if not days:
        raise BGPError("BGP warehouse has no data for any coverage collector")
    newest = max(days.values())
    used = [c for c in BGP_COVERAGE_COLLECTORS
            if c in days and (newest - days[c]).days <= MAX_COLLECTOR_LAG_DAYS]
    skipped = [c for c in BGP_COVERAGE_COLLECTORS if c not in used]
    if skipped:
        log.warning("Coverage: skipping lagging/missing collectors %s (newest RIB %s)", skipped, newest)
    lag = (datetime.now(timezone.utc).date() - newest).days
    if lag > MAX_COLLECTOR_LAG_DAYS:
        log.warning("Coverage: newest BGP RIB is %s, %d days old", newest, lag)
    result = {
        "to": min(days[c] for c in used).isoformat(),
        "collectors": used,
        "skipped_collectors": skipped,
    }
    cache_set("bgp_coverage_collectors", result)
    return result


def coverage_window(days: int) -> dict:
    """The last ``days`` daily RIBs that every current coverage collector has."""
    ends = _collector_ends()
    end = date.fromisoformat(ends["to"])
    start = end - timedelta(days=days - 1)
    return {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "days": days,
        "collectors": ends["collectors"],
        "skipped_collectors": ends["skipped_collectors"],
    }


def announced_prefixes(asn: int, family: str, window: dict, timeout: float = 120.0) -> list[str]:
    """Distinct prefixes ``asn`` originated in ``window`` across the coverage
    collectors. Raises BGPError if the API (or any collector query) failed, so a
    partial answer is never reported as low coverage."""
    try:
        resp = httpx.get(
            f"{BGP_WAREHOUSE_URL}/api/asn/prefixes",
            params={
                "asn": asn,
                "family": family,
                "collectors": ",".join(window["collectors"]),
                # A closed run ends at the first RIB it was absent from, so a
                # route last seen the day before ``from`` has end == from 00:00.
                "date_from": f"{window['from']} 00:00:01",
                "date_to": f"{window['to']} 23:59:59",
            },
            headers={"X-API-Key": _api_key()},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
    except (httpx.HTTPError, ValueError) as e:
        raise BGPError(f"BGP warehouse API error: {e}")
    if data.get("errors"):
        failed = sorted({e.get("collector", "?") for e in data["errors"]})
        raise BGPError(f"BGP warehouse query failed for {', '.join(failed)}")
    return [p["prefix"] for p in data.get("prefixes", [])]


def method(window: dict) -> dict:
    """How the numbers were computed, echoed in every coverage response."""
    return {
        "window": {k: window[k] for k in ("from", "to", "days")},
        "collectors": window["collectors"],
        "skipped_collectors": window["skipped_collectors"],
        "max_prefix_length": {"ipv4": MAX_ROUTABLE_LEN[4], "ipv6": MAX_ROUTABLE_LEN[6]},
        "matching": "RFC 6811 origin validation against live VRPs",
    }


# ── RPKI side ──

def _parse(prefixes) -> dict[str, ipaddress.IPv4Network | ipaddress.IPv6Network]:
    nets = {}
    for p in prefixes:
        try:
            nets[p] = ipaddress.ip_network(p, strict=False)
        except ValueError:
            continue  # not an IP prefix; cannot be validated
    return nets


def routable(prefixes) -> tuple[list[str], int]:
    """Split off prefixes longer than MAX_ROUTABLE_LEN -> (kept, number dropped)."""
    nets = _parse(prefixes)
    kept = [p for p, n in nets.items() if n.prefixlen <= MAX_ROUTABLE_LEN[n.version]]
    return kept, len(nets) - len(kept)


def _covering_keys(net) -> list[tuple[int, int, int]]:
    """(version, network int, length) of every prefix that contains ``net``,
    from /0 down to ``net`` itself."""
    bits, base = net.max_prefixlen, int(net.network_address)
    return [
        (net.version, base & (((1 << length) - 1) << (bits - length)), length)
        for length in range(net.prefixlen + 1)
    ]


def _key_str(key: tuple[int, int, int]) -> str:
    version, addr, length = key
    cls = ipaddress.IPv4Network if version == 4 else ipaddress.IPv6Network
    return str(cls((addr, length)))


class Validator:
    """RFC 6811 validation of announced prefixes against ``live_roas``.

    Loads every VRP that could cover any of ``prefixes`` with one query (live_roas
    prefix strings are canonical, so exact matches on candidate supernets find
    them). ``validity_sql`` is the roa router's filter (" AND is_valid" by default).
    """

    def __init__(self, con, prefixes, validity_sql: str):
        self.nets = _parse(prefixes)
        self._keys = {p: _covering_keys(n) for p, n in self.nets.items()}
        key_strs = {k: _key_str(k) for keys in self._keys.values() for k in keys}
        self._strs = key_strs
        self.vrps: dict[str, list[tuple[int, int]]] = {}
        if key_strs:
            # One space-joined string: binding a Python list of ~100k values as a
            # DuckDB LIST parameter takes seconds, string_split takes ~0.1 s.
            rows = con.execute(
                "SELECT DISTINCT prefix, asn, max_length FROM live_roas "
                f"WHERE prefix IN (SELECT unnest(string_split($cands, ' '))){validity_sql}",
                {"cands": " ".join(set(key_strs.values()))},
            ).fetchall()
            for prefix, vrp_asn, max_length in rows:
                self.vrps.setdefault(prefix, []).append((vrp_asn, max_length))

    def validate(self, prefix: str, origin: int) -> tuple[str, str | None, set[str]]:
        """-> (state, invalid reason, VRP prefixes of ``origin`` that made it valid)."""
        net = self.nets.get(prefix)
        if net is None:
            return NOT_FOUND, None, set()
        covered = origin_covers = False
        matched: set[str] = set()
        for key in self._keys[prefix]:
            vrp_prefix = self._strs[key]
            for vrp_asn, max_length in self.vrps.get(vrp_prefix, ()):
                covered = True
                if vrp_asn == origin:
                    origin_covers = True
                    if net.prefixlen <= max_length:
                        matched.add(vrp_prefix)
        if matched:
            return VALID, None, matched
        if covered:
            return INVALID, ("length" if origin_covers else "as"), matched
        return NOT_FOUND, None, matched


def tally(states: dict[str, str], nets: dict) -> dict:
    """Counts per address family plus overall, from {prefix: state}; every
    prefix in ``states`` must be a key of ``nets`` (Validator.nets)."""
    out = {}
    for fam, version in (("all", None), ("ipv4", 4), ("ipv6", 6)):
        sel = [s for p, s in states.items() if version is None or nets[p].version == version]
        n_valid = sel.count(VALID)
        out[fam] = {
            "announced": len(sel),
            "valid": n_valid,
            "invalid": sel.count(INVALID),
            "not_found": sel.count(NOT_FOUND),
            "coverage_pct": round(n_valid / len(sel) * 100, 2) if sel else 0.0,
        }
    return out
