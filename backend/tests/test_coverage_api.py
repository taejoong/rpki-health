"""/roa/coverage and /roa/org-coverage responses keep the fields the live
frontend (main: Coverage.jsx, OrgCoverage.jsx) reads."""

import duckdb
import pytest
from fastapi.testclient import TestClient

from app import coverage
from app.main import app
from app.routers import roa

WINDOW = {"from": "2026-10-17", "to": "2026-10-23", "days": 7,
          "collectors": ["route-views2", "route-views6", "rrc00"], "skipped_collectors": []}
ANNOUNCED = {
    13335: ["1.0.0.0/24", "1.0.0.0/25", "1.0.4.0/24", "198.51.100.0/24", "2606:4700::/32"],
    64500: ["192.0.2.0/24"],
}


@pytest.fixture
def client(monkeypatch):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE live_roas (asn BIGINT, prefix VARCHAR, max_length INTEGER, afi INTEGER, is_valid BOOLEAN)")
    con.executemany("INSERT INTO live_roas VALUES (?, ?, ?, ?, ?)", [
        (13335, "1.0.0.0/22", 24, 4, True),
        (13335, "2606:4700::/32", 48, 6, True),
        (13335, "203.0.113.0/24", 24, 4, True),    # ROA, never announced
        (64501, "198.51.0.0/16", 16, 4, True),     # makes AS13335's 198.51.100.0/24 invalid
    ])
    monkeypatch.setattr(roa, "get_con", lambda: con.cursor())
    monkeypatch.setattr(roa, "cache_get", lambda k: None)
    monkeypatch.setattr(roa, "cache_set", lambda k, v: None)
    monkeypatch.setattr(coverage, "coverage_window", lambda days: dict(WINDOW, days=days))
    monkeypatch.setattr(coverage, "announced_prefixes", lambda asn, fam, w, timeout=0: ANNOUNCED.get(asn, []))
    return TestClient(app)


def test_coverage_fields(client):
    d = client.get("/roa/coverage", params={"asn": 13335}).json()
    s = d["summary"]
    # 1.0.0.0/25 is longer than /24: excluded, not counted as invalid
    assert s["excluded_more_specifics"] == 1
    assert (s["announced"], s["roa_covered"], s["invalid"], s["not_found"]) == (4, 2, 1, 1)
    assert s["unsigned"] == s["announced"] - s["roa_covered"] and s["coverage_pct"] == 50.0
    assert s["roa_only"] == 1 and d["roa_not_announced"] == ["203.0.113.0/24"]
    assert d["unsigned_prefixes"] == ["1.0.4.0/24"]                      # "No ROA" list: not_found only
    assert d["invalid_prefixes"] == [{"prefix": "198.51.100.0/24", "reason": "as"}]
    for fam in ("ipv4", "ipv6"):
        assert {"announced", "signed", "unsigned", "coverage_pct"} <= d[fam].keys()
        assert d[fam]["announced"] == d[fam]["signed"] + d[fam]["unsigned"]
    assert d["method"]["window"] == {"from": "2026-10-17", "to": "2026-10-23", "days": 7}


def test_org_coverage_fields_and_bgp_failure(client, monkeypatch):
    family = [{"asn_entries": [{"asn": 13335, "org_name": "Cloudflare"}, {"asn": 64500, "org_name": "Cloudflare"}]}]

    class R:
        def raise_for_status(self): pass
        def json(self): return family

    monkeypatch.setattr(roa.httpx, "get", lambda *a, **kw: R())
    d = client.get("/roa/org-coverage", params={"asn": 13335}).json()
    assert {"org_name", "total_roa_prefixes", "total_announced", "total_signed", "org_coverage_pct", "per_asn"} <= d.keys()
    assert d["total_announced"] == 5 and d["total_signed"] == 2 and d["bgp_errors"] == 0
    row = d["per_asn"][0]
    assert {"asn", "as_name", "roa_total", "announced", "signed", "coverage_pct", "bgp_error"} <= row.keys()

    def down(days):
        raise coverage.BGPError("BGP warehouse API error: down")

    monkeypatch.setattr(coverage, "coverage_window", down)
    d = client.get("/roa/org-coverage", params={"asn": 13335}).json()
    # degrades to the ROA-only table the UI labels "BGP unavailable"
    assert d["total_announced"] == 0 and d["bgp_errors"] == 2 and d["total_roa_prefixes"] == 3
    assert all(r["bgp_error"] for r in d["per_asn"])
