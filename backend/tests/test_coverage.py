"""RFC 6811 validation in app.coverage against a tiny in-memory live_roas."""

import duckdb
import pytest

from app import coverage
from app.coverage import INVALID, NOT_FOUND, VALID

VRPS = [
    # rir, asn, prefix, max_length, afi, is_valid
    ("apnic", 13335, "1.0.0.0/22", 24, 4, True),
    ("arin", 0, "10.0.0.0/8", 8, 4, True),              # AS0: nothing under it is valid
    ("arin", 64500, "192.0.2.0/24", 24, 4, False),      # rejected by rpki-client
    ("arin", 64501, "198.51.0.0/16", 16, 4, True),
    ("arin", 64502, "198.51.100.0/24", 24, 4, True),
    ("arin", 13335, "2606:4700::/32", 48, 6, True),
]


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    c.execute(
        "CREATE TABLE live_roas (rir VARCHAR, asn BIGINT, prefix VARCHAR, "
        "max_length INTEGER, afi INTEGER, is_valid BOOLEAN)"
    )
    c.executemany("INSERT INTO live_roas VALUES (?, ?, ?, ?, ?, ?)", VRPS)
    return c


@pytest.mark.parametrize("prefix, origin, state, reason", [
    ("1.0.0.0/24", 13335, VALID, None),          # covered by a shorter VRP within maxLength
    ("1.0.0.0/22", 13335, VALID, None),          # exact match
    ("1.0.0.0/25", 13335, INVALID, "length"),    # longer than maxLength
    ("1.0.0.0/24", 64496, INVALID, "as"),        # covered, wrong origin
    ("1.0.4.0/24", 13335, NOT_FOUND, None),      # outside every VRP
    ("10.1.0.0/16", 64496, INVALID, "as"),       # AS0 VRP
    ("198.51.100.0/24", 64502, VALID, None),     # a mismatching cover elsewhere does not matter
    ("198.51.100.0/24", 64501, INVALID, "length"),
    ("2606:4700:10::/48", 13335, VALID, None),
    ("2606:4700:10::/56", 13335, INVALID, "length"),
    ("2001:db8::/32", 13335, NOT_FOUND, None),
])
def test_validate(con, prefix, origin, state, reason):
    v = coverage.Validator(con, [prefix], " AND is_valid")
    got_state, got_reason, _ = v.validate(prefix, origin)
    assert (got_state, got_reason) == (state, reason)


def test_rejected_roas_only_count_with_validity_all(con):
    strict = coverage.Validator(con, ["192.0.2.0/24"], " AND is_valid")
    assert strict.validate("192.0.2.0/24", 64500)[0] == NOT_FOUND
    everything = coverage.Validator(con, ["192.0.2.0/24"], "")
    assert everything.validate("192.0.2.0/24", 64500)[0] == VALID


def test_matched_vrps_and_tally(con):
    announced = ["1.0.0.0/24", "1.0.1.0/24", "1.0.0.0/25", "8.8.8.0/24", "2606:4700::/32", "bogus"]
    v = coverage.Validator(con, announced, " AND is_valid")
    assert "bogus" not in v.nets
    states, used = {}, set()
    for p in v.nets:
        state, _, matched = v.validate(p, 13335)
        states[p] = state
        used |= matched
    assert used == {"1.0.0.0/22", "2606:4700::/32"}
    t = coverage.tally(states, v.nets)
    assert t["all"] == {"announced": 5, "valid": 3, "invalid": 1, "not_found": 1, "coverage_pct": 60.0}
    assert t["ipv4"]["announced"] == 4 and t["ipv6"] == {
        "announced": 1, "valid": 1, "invalid": 0, "not_found": 0, "coverage_pct": 100.0}


def test_host_bits_and_default_route(con):
    v = coverage.Validator(con, ["1.0.0.1/24", "0.0.0.0/0"], " AND is_valid")
    assert v.validate("1.0.0.1/24", 13335)[0] == VALID
    assert v.validate("0.0.0.0/0", 13335)[0] == NOT_FOUND


def test_routable_drops_more_specifics():
    kept, dropped = coverage.routable(["1.0.0.0/24", "1.0.0.0/25", "2001:db8::/48", "2001:db8::/56", "junk"])
    assert kept == ["1.0.0.0/24", "2001:db8::/48"] and dropped == 2


class _Resp:
    def __init__(self, data, status=200):
        self._data, self.status_code = data, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise coverage.httpx.HTTPStatusError("boom", request=None, response=None)

    def json(self):
        return self._data


@pytest.fixture
def no_cache(monkeypatch):
    monkeypatch.setattr(coverage, "cache_get", lambda k: None)
    monkeypatch.setattr(coverage, "cache_set", lambda k, v: None)
    monkeypatch.setattr(coverage, "_api_key", lambda: "k")


def test_window_skips_lagging_collector(monkeypatch, no_cache):
    ends = {"route-views2": "2026-10-23", "route-views6": "2026-10-22", "rrc00": "2026-10-01"}
    monkeypatch.setattr(coverage.httpx, "get", lambda *a, **kw: _Resp(
        {"collectors": [{"collector": c, "v2_to": d} for c, d in ends.items()]}))
    w = coverage.coverage_window(7)
    assert (w["from"], w["to"]) == ("2026-10-16", "2026-10-22")
    assert w["collectors"] == ["route-views2", "route-views6"] and w["skipped_collectors"] == ["rrc00"]


def test_announced_prefixes_params_and_partial_errors(monkeypatch, no_cache):
    seen = {}
    window = {"from": "2026-10-17", "to": "2026-10-23", "days": 7,
              "collectors": ["route-views2", "rrc00"], "skipped_collectors": []}

    def fake_get(url, params=None, **kw):
        seen.update(params)
        return _Resp({"prefixes": [{"prefix": "1.0.0.0/24"}], "errors": []})

    monkeypatch.setattr(coverage.httpx, "get", fake_get)
    assert coverage.announced_prefixes(13335, "both", window) == ["1.0.0.0/24"]
    # strictly after the previous day's RIB, through the end of the last day
    assert seen["date_from"] == "2026-10-17 00:00:01" and seen["date_to"] == "2026-10-23 23:59:59"
    assert seen["collectors"] == "route-views2,rrc00"

    monkeypatch.setattr(coverage.httpx, "get", lambda *a, **kw: _Resp(
        {"prefixes": [], "errors": [{"collector": "rrc00", "family": "ipv6", "error": "x"}]}))
    with pytest.raises(coverage.BGPError, match="rrc00"):
        coverage.announced_prefixes(13335, "both", window)
