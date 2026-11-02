"""/analytics/country/*: lazy reads of a precomputed build (no warehouse needed).

Each test writes a small build with the file schemas that crc.assemble writes
(manifest.json, series, countries, ases, as_series) into tmp_path with DuckDB
COPY and points ``current`` at it the way crc.assemble publishes: a new
symlink renamed over the old one.
"""

import json
import os
from types import SimpleNamespace

import duckdb
import pytest
from fastapi.testclient import TestClient

from app import main
from app.main import app
from app.routers import country

NAN, INF = float("nan"), float("inf")
STAMP, STAMP2 = "20261027T071500Z", "20261028T071500Z"
AS_OF, PREV_DATE = "2026-10-26", "2025-10-22"
DATES = ("2019-12-25", PREV_DATE, "2026-10-25", AS_OF)  # grid dates; BGP lenses start 2020-01-01

# Column names as crc.assemble writes them (not imported from the router).
METRICS = (
    "routes", "routes_valid", "routes_inv_as", "routes_inv_len", "routes_nf",
    "space", "space_valid", "space_invalid", "space_nf",
    "ases", "ases_any_valid", "ases_all_valid", "ases_any_invalid",
    "deleg", "deleg_cov", "deleg_cov_noas0", "vrps",
)
FLOAT_METRICS = {"space", "space_valid", "space_invalid", "space_nf", "deleg", "deleg_cov", "deleg_cov_noas0"}
REGISTERED = {"deleg", "deleg_cov", "deleg_cov_noas0", "vrps"}  # the only lens before 2020
PREV = ("routes", "routes_valid", "routes_inv_as", "routes_inv_len", "space", "space_valid",
        "deleg", "deleg_cov", "ases", "ases_any_valid")
AS_FIELDS = (
    "asn", "name", "as_cc", "routes", "routes_valid", "routes_inv_as", "routes_inv_len", "routes_nf",
    "space", "space_valid", "space_invalid", "space_nf", "pct_valid", "pct_space_valid",
    "prev_routes", "prev_routes_valid", "prev_space", "prev_space_valid", "first_valid",
)
AS_TREND = ("routes", "routes_valid", "routes_inv_as", "routes_inv_len", "routes_nf", "space", "space_valid")

SERIES_COLS = [("date", "DATE"), ("level", "VARCHAR"), ("key", "VARCHAR"), ("af", "UTINYINT")] + [
    (m, "DOUBLE" if m in FLOAT_METRICS else "BIGINT") for m in METRICS]
COUNTRY_COLS = [("cc", "VARCHAR"), ("af", "UTINYINT"), ("registry", "VARCHAR")] + SERIES_COLS[4:] + [
    (f"prev_{p}", "DOUBLE" if p in FLOAT_METRICS else "BIGINT") for p in PREV]
AS_COLS = [("cc", "VARCHAR"), ("af", "UTINYINT"), ("asn", "UBIGINT"), ("name", "VARCHAR"), ("as_cc", "VARCHAR")] + [
    (f, "BIGINT") for f in AS_FIELDS[3:8]] + [(f, "DOUBLE") for f in AS_FIELDS[8:14]] + [
    ("prev_routes", "BIGINT"), ("prev_routes_valid", "BIGINT"), ("prev_space", "DOUBLE"),
    ("prev_space_valid", "DOUBLE"), ("first_valid", "DATE")]
AS_SERIES_COLS = [("cc", "VARCHAR"), ("af", "UTINYINT"), ("asn", "UBIGINT"), ("date", "DATE")] + [
    (m, "DOUBLE" if m in FLOAT_METRICS else "BIGINT") for m in AS_TREND]

# (level, key, af) -> base; the row of date i holds geo(base * (i + 1) + bump)
KEYS = {
    ("all", "all", 4): 50, ("all", "all", 6): 40,
    **{("registry", r, af): b
       for b, r in enumerate(("afrinic", "apnic", "arin", "lacnic", "ripencc", "iana", "unknown"), 1)
       for af in (4, 6)},
    ("cc", "KR", 4): 3, ("cc", "KR", 6): 2, ("cc", "US", 4): 20, ("cc", "US", 6): 15,
    ("cc", "EU", 4): 1, ("cc", "AQ", 4): 1, ("cc", "ZZ", 4): 7,
}
REGISTRY = {"KR": "apnic", "US": "arin", "EU": "ripencc", "AQ": "arin"}  # countries.parquet; ZZ excluded


def geo(scale, bgp=True):
    """The 17 metrics of one geo row; the BGP-based ones are NULL before 2020."""
    v = dict(zip(METRICS, (
        100 * scale, 60 * scale, 5 * scale, 3 * scale, 32 * scale,
        1000.0 * scale, 700.0 * scale, 50.0 * scale, 250.0 * scale,
        10 * scale, 6 * scale, 4 * scale, 1 * scale,
        2000.0 * scale, 1500.0 * scale, 1400.0 * scale, 80 * scale,
    )))
    if not bgp:
        v.update({k: None for k in METRICS if k not in REGISTERED})
    return v


def series_rows(bump):
    rows = []
    for (level, key, af), base in KEYS.items():
        for i, day in enumerate(DATES):
            if (key, af, i) == ("afrinic", 4, 0):
                continue  # a key with no row on a date: null in the columnar payload
            v = geo(base * (i + 1) + bump, bgp=day >= "2020-01-01")
            if (key, af) == ("KR", 4) and i == 2:
                v["space_valid"] = NAN
            if (key, af) == ("KR", 4) and i == 3:
                v["space_nf"] = INF
            rows.append((day, level, key, af, *v.values()))
    return rows


def country_rows(bump):
    rows = []
    for (level, cc, af), base in KEYS.items():
        if level != "cc" or cc == "ZZ":
            continue
        now, prev = geo(base * 4 + bump), geo(base * 2 + bump)  # as_of is DATES[3], prev_date DATES[1]
        if (cc, af) == ("KR", 4):
            now["space_nf"], prev["space"] = INF, NAN
        rows.append((cc, af, REGISTRY[cc], *now.values(), *(prev[p] for p in PREV)))
    return rows


def as_row(cc, af, asn, name, as_cc, routes, valid, space, space_valid, first_valid=None,
           prev=(None, None, None, None), **override):
    inv_as = (routes - valid) // 2
    v = dict(zip(AS_FIELDS, (
        asn, name, as_cc, routes, valid, inv_as, 0, routes - valid - inv_as,
        space, space_valid, 0.0, space - space_valid,
        valid / routes, space_valid / space if space else None, *prev, first_valid,
    )))
    v.update(override)
    return (cc, af, *v.values())


AS_ROWS = [
    as_row("KR", 4, 4766, "KIXS-AS-KR - Korea Telecom, KR", "KR", 50, 10, 5000.0, 1000.0, "2020-01-01",
           (40, 5, 4000.0, 500.0)),
    as_row("KR", 4, 3786, "LGDACOM - LG DACOM Corporation, KR", "KR", 30, 30, 3000.0, 3000.0, "2021-06-02",
           (25, 20, 2500.0, 2000.0)),
    as_row("KR", 4, 9318, "SKB-AS - SK Broadband Co Ltd, KR", "KR", 30, 0, 0.0, 0.0),
    as_row("KR", 4, 17858, "POWERVIS-AS-KR - LG POWERCOMM, KR", "KR", 5, 5, 500.0, 500.0, "2023-03-01",
           (5, 5, 500.0, 500.0), pct_space_valid=NAN, space_nf=INF),
    as_row("KR", 4, 13335, "CLOUDFLARENET - Cloudflare, Inc., US", "US", 2, 2, 512.0, 512.0, "2020-01-01"),
    as_row("KR", 4, 64500, "example_net 100% - Example, KR", "KR", 1, 0, 256.0, 0.0),
    as_row("KR", 4, 64501, None, None, 1, 1, 256.0, 256.0, AS_OF),
    as_row("KR", 6, 4766, "KIXS-AS-KR - Korea Telecom, KR", "KR", 7, 7, 3.0, 3.0, "2020-01-01"),
    as_row("KR", 6, 3786, "LGDACOM - LG DACOM Corporation, KR", "KR", 3, 0, 1.0, 0.0),
    as_row("US", 4, 7018, "ATT-INTERNET4 - AT&T Services, Inc., US", "US", 300, 120, 9e6, 4e6, "2020-01-01"),
]
AS_SERIES_ROWS = [
    ("KR", 4, 4766, "2020-01-01", 30, 0, 10, 0, 20, 3000.0, 0.0),
    ("KR", 4, 4766, "2026-10-01", 45, 8, 18, 0, 19, NAN, 800.0),
    ("KR", 4, 4766, AS_OF, 50, 10, 20, 0, 20, 5000.0, 1000.0),
    ("KR", 6, 4766, AS_OF, 7, 7, 0, 0, 0, 3.0, 3.0),
    ("KR", 4, 3786, "2021-06-02", 10, 10, 0, 0, 0, 1000.0, 1000.0),
    ("KR", 4, 3786, AS_OF, 30, 30, 0, 0, 0, 3000.0, 3000.0),
    ("US", 4, 7018, "2020-01-01", 250, 0, 0, 0, 250, 8e6, 0.0),
    ("US", 4, 7018, AS_OF, 300, 120, 90, 0, 90, 9e6, 4e6),
]


def manifest(as_of=AS_OF):
    return {
        "version": 1, "as_of": as_of, "prev_date": PREV_DATE, "generated_at": "2026-10-27T07:15:00Z",
        "grid": {"weekly_from": "2012-01-04", "daily_from": "2026-01-01"}, "bgp_from": "2020-01-01",
        "method": {"visibility": "n_collectors >= ceil(0.25 * collectors[af])", "visibility_share": NAN},
        "lenses": {"routes": {"label": "Routes", "numerator": "routes_valid", "denominator": "routes",
                              "from": "2020-01-01"}},
        "sources": [{"name": "RIPE NCC RPKI archive", "from": "2026-04-08"}],
        "notes": [{"from": "2026-10-04", "to": "2026-10-12", "text": "VRP outage bridged"}],
        "counts": {"series_rows": len(series_rows(0))},
    }


def sql_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return f"'{v}'" if isinstance(v, float) else str(v)  # 'nan' / 'inf' cast to DOUBLE


def copy_parquet(con, path, cols, rows, order_by):
    """Build column types -> temp table -> COPY to Parquet. The values are
    this module's constants, inlined as literals: binding ~2,000 parameters
    (or executemany) takes ~0.2 s per build."""
    con.execute("CREATE OR REPLACE TEMP TABLE t (" + ", ".join(f"{c} {t}" for c, t in cols) + ")")
    values = ", ".join(
        "(" + ", ".join(f"CAST({sql_literal(v)} AS {t})" for v, (_, t) in zip(r, cols)) + ")" for r in rows)
    con.execute(f"INSERT INTO t VALUES {values}")
    con.execute(f"COPY (SELECT * FROM t ORDER BY {order_by}) TO '{path}' (FORMAT parquet)")


def write_build(root, stamp, con, as_of=AS_OF, bump=0):
    d = root / "builds" / stamp
    d.mkdir(parents=True)
    (d / "manifest.json").write_text(json.dumps(manifest(as_of)))  # NaN stays a bare NaN token
    copy_parquet(con, d / "series.parquet", SERIES_COLS, series_rows(bump), "level, key, af, date")
    copy_parquet(con, d / "countries.parquet", COUNTRY_COLS, country_rows(bump), "cc, af")
    copy_parquet(con, d / "ases.parquet", AS_COLS, AS_ROWS, "cc, af, asn")
    copy_parquet(con, d / "as_series.parquet", AS_SERIES_COLS, AS_SERIES_ROWS, "cc, asn, af, date")
    return d


def publish(root, build):
    """ln -s builds/<stamp> current.new && mv -T current.new current"""
    tmp = root / "current.new"
    os.symlink(os.path.relpath(build, root), tmp)
    os.replace(tmp, root / "current")


@pytest.fixture
def env(tmp_path, monkeypatch):
    con = duckdb.connect(":memory:")
    root = tmp_path / "country-rpki"
    build = write_build(root, STAMP, con)
    publish(root, build)
    cache = {}
    monkeypatch.setattr(country, "COUNTRY_RPKI_DIR", root)
    monkeypatch.setattr(country, "get_con", lambda: con.cursor())
    monkeypatch.setattr(country, "cache_get", cache.get)
    monkeypatch.setattr(country, "cache_set", cache.__setitem__)
    # Many requests from one test client: lift the per-IP limit, and start from
    # (and leave behind) a clean limiter for the other test modules.
    monkeypatch.setattr(main, "RATE_LIMIT", 10**6)
    monkeypatch.setattr(main, "_rate_limits", main.OrderedDict())
    return SimpleNamespace(c=TestClient(app), root=root, con=con, cache=cache, build=build)


def get(env, path, **params):
    return env.c.get(f"/analytics/country/{path}", params=params)


def asns(d):
    return [r["asn"] for r in d["rows"]]


ENDPOINTS = [
    ("meta", {}), ("countries", {}), ("rir-trend", {}), ("trend", {"cc": "KR"}),
    ("ases", {"cc": "KR"}), ("as-trend", {"cc": "KR", "asn": 4766}),
]


def test_router_is_registered():
    assert {f"/analytics/country/{p}" for p, _ in ENDPOINTS} <= set(app.openapi()["paths"])


def test_meta_serves_the_manifest_fields(env):
    d = get(env, "meta").json()
    assert list(d) == ["version", "as_of", "prev_date", "generated_at", "grid", "bgp_from",
                       "method", "lenses", "sources", "notes"]
    assert (d["version"], d["as_of"], d["prev_date"], d["bgp_from"]) == (1, AS_OF, PREV_DATE, "2020-01-01")
    assert d["grid"] == {"weekly_from": "2012-01-04", "daily_from": "2026-01-01"}
    assert d["method"]["visibility_share"] is None  # NaN in manifest.json -> null
    assert d["lenses"]["routes"]["denominator"] == "routes"
    assert d["notes"][0]["from"] == "2026-10-04" and d["sources"]


def test_manifest_numbers_beyond_float_range_are_null(env):
    # json.load parses 1e999 to inf (parse_constant only sees the NaN/Infinity
    # tokens); served as is, /meta was a 500 instead of null.
    b = write_build(env.root, STAMP2, env.con)
    text = json.dumps(manifest()).replace('"visibility_share": NaN', '"visibility_share": 1e999, "low": -1e999')
    (b / "manifest.json").write_text(text)
    publish(env.root, b)
    r = get(env, "meta")
    assert r.status_code == 200, r.text
    assert (r.json()["method"]["visibility_share"], r.json()["method"]["low"]) == (None, None)


def test_countries_rows_with_prev(env):
    d = get(env, "countries").json()
    assert (d["version"], d["as_of"], d["prev_date"], d["af"]) == (1, AS_OF, PREV_DATE, 4)
    assert [r["cc"] for r in d["rows"]] == ["AQ", "EU", "KR", "US"]
    kr = d["rows"][2]
    assert list(kr) == ["cc", "registry", *METRICS, "prev"]
    assert kr["registry"] == "apnic"
    assert {m: kr[m] for m in METRICS} == {**geo(12), "space_nf": None}  # Inf -> null
    prev = geo(6)
    assert kr["prev"] == {**{p: prev[p] for p in PREV}, "space": None}  # NaN -> null
    d6 = get(env, "countries", af=6).json()
    assert d6["af"] == 6 and [r["cc"] for r in d6["rows"]] == ["KR", "US"]
    assert d6["rows"][0]["routes"] == 100 * 2 * 4
    assert get(env, "countries", af=5).status_code == 400
    assert get(env, "countries", af="x").status_code == 422


def test_rir_trend_is_columnar_and_aligned(env):
    d = get(env, "rir-trend").json()
    assert (d["version"], d["as_of"], d["af"]) == (1, AS_OF, 4)
    assert d["dates"] == list(DATES)
    assert list(d["series"]) == ["all", "afrinic", "apnic", "arin", "lacnic", "ripencc"]  # no iana/unknown
    for s in d["series"].values():
        assert list(s) == list(METRICS) and all(len(v) == len(DATES) for v in s.values())
    assert d["series"]["arin"]["routes"] == [None, 600, 900, 1200]  # BGP lenses start 2020-01-01
    assert d["series"]["arin"]["deleg"] == [6000.0, 12000.0, 18000.0, 24000.0]
    assert d["series"]["afrinic"]["deleg"] == [None, 4000.0, 6000.0, 8000.0]  # no row on the first date
    assert d["series"]["all"]["vrps"] == [80 * 50 * i for i in (1, 2, 3, 4)]
    d6 = get(env, "rir-trend", af=6).json()
    assert d6["af"] == 6 and d6["series"]["all"]["routes"][-1] == 100 * 40 * 4
    assert get(env, "rir-trend", af=0).status_code == 400


def test_trend(env):
    d = get(env, "trend", cc="kr").json()
    assert (d["version"], d["as_of"], d["cc"], d["af"], d["registry"]) == (1, AS_OF, "KR", 4, "apnic")
    assert d["dates"] == list(DATES) and list(d["metrics"]) == list(METRICS)
    assert d["metrics"]["routes"] == [None, 600, 900, 1200]
    assert d["metrics"]["deleg"][0] == 6000.0  # registered-space lens exists before 2020
    assert d["metrics"]["space_valid"] == [None, 4200.0, None, 8400.0]  # NaN -> null
    assert d["metrics"]["space_nf"][-1] is None  # Inf -> null
    assert get(env, "trend", cc="US", af=6).json()["metrics"]["routes"][-1] == 100 * 15 * 4
    # known country without rows in this family: 200 with empty arrays
    d = get(env, "trend", cc="AQ", af=6).json()
    assert (d["registry"], d["dates"], d["metrics"]["routes"]) == ("arin", [], [])
    # in the series but not in countries.parquet (ZZ: unregistered space)
    assert get(env, "trend", cc="ZZ").json()["registry"] is None
    assert get(env, "trend", cc="QQ").status_code == 404
    for bad in ("K", "KOR", "K1", "K'", "KR\n", ""):
        assert get(env, "trend", cc=bad).status_code == 400, bad
    assert get(env, "trend", cc="KR", af=5).status_code == 400


def test_ases_default_sort_and_fields(env):
    d = get(env, "ases", cc="KR").json()
    assert (d["version"], d["as_of"], d["prev_date"], d["cc"], d["af"]) == (1, AS_OF, PREV_DATE, "KR", 4)
    assert (d["total"], d["offset"], d["limit"]) == (7, 0, 100)
    assert asns(d) == [4766, 3786, 9318, 17858, 13335, 64500, 64501]  # routes desc, ties asn asc
    assert d["rows"][0] == {
        "asn": 4766, "name": "KIXS-AS-KR - Korea Telecom, KR", "as_cc": "KR",
        "routes": 50, "routes_valid": 10, "routes_inv_as": 20, "routes_inv_len": 0, "routes_nf": 20,
        "space": 5000.0, "space_valid": 1000.0, "space_invalid": 0.0, "space_nf": 4000.0,
        "pct_valid": 0.2, "pct_space_valid": 0.2,
        "prev_routes": 40, "prev_routes_valid": 5, "prev_space": 4000.0, "prev_space_valid": 500.0,
        "first_valid": "2020-01-01",
    }
    assert all(list(r) == list(AS_FIELDS) for r in d["rows"])
    skb, powervis = d["rows"][2], d["rows"][3]
    assert (skb["pct_space_valid"], skb["first_valid"], skb["prev_routes"]) == (None, None, None)
    assert (powervis["pct_space_valid"], powervis["space_nf"]) == (None, None)  # NaN/Inf -> null
    assert d["rows"][4]["as_cc"] == "US"


@pytest.mark.parametrize("sort,order,expected", [
    ("routes", "asc", [64500, 64501, 13335, 17858, 3786, 9318, 4766]),
    ("asn", "asc", [3786, 4766, 9318, 13335, 17858, 64500, 64501]),
    ("name", "asc", [13335, 64500, 4766, 3786, 17858, 9318, 64501]),  # case-insensitive, NULL last
    ("name", "DESC", [9318, 17858, 3786, 4766, 64500, 13335, 64501]),
    ("pct_space_valid", "desc", [3786, 13335, 64501, 4766, 64500, 9318, 17858]),  # NaN sorts as NULL
    ("pct_space_valid", "asc", [64500, 4766, 3786, 13335, 64501, 9318, 17858]),
    ("first_valid", "asc", [4766, 13335, 3786, 17858, 64501, 9318, 64500]),
])
def test_ases_sort(env, sort, order, expected):
    assert asns(get(env, "ases", cc="KR", sort=sort, order=order).json()) == expected


def test_ases_paging(env):
    d = get(env, "ases", cc="KR", limit=2, offset=1).json()
    assert (d["total"], d["offset"], d["limit"], asns(d)) == (7, 1, 2, [3786, 9318])
    d = get(env, "ases", cc="KR", offset=7).json()
    assert (d["total"], d["rows"]) == (7, [])
    d = get(env, "ases", cc="kr", af=6).json()
    assert (d["cc"], d["af"], d["total"], asns(d)) == ("KR", 6, 2, [4766, 3786])
    d = get(env, "ases", cc="QQ").json()  # no ASes: an empty table, not an error
    assert (d["total"], d["rows"]) == (0, [])
    assert get(env, "ases", cc="KR", limit=500).status_code == 200
    d = get(env, "ases", cc="KR", offset=country.MAX_OFFSET).json()  # the largest offset accepted
    assert (d["total"], d["offset"], d["rows"]) == (7, country.MAX_OFFSET, [])


@pytest.mark.parametrize("q,expected", [
    ("AS4766", [4766]), ("as 4766", [4766]), ("4766", [4766]), (" dacom ", [3786]),
    ("CLOUDFLARE", [13335]), ("_", [64500]), ("%", [64500]), ("100%", [64500]), ("zzz", []),
    ("", [4766, 3786, 9318, 17858, 13335, 64500, 64501]),
])
def test_ases_search(env, q, expected):
    d = get(env, "ases", cc="KR", q=q).json()
    assert asns(d) == expected and d["total"] == len(expected)


def test_ases_rejects_bad_params(env):
    for params, code in [
        ({"sort": "bogus"}, 400), ({"sort": 'asn" DESC; DROP TABLE t; --'}, 400), ({"sort": "cc"}, 400),
        ({"order": "sideways"}, 400), ({"af": 5}, 400), ({"cc": "K1"}, 400),
        ({"limit": 0}, 422), ({"limit": 501}, 422), ({"offset": -1}, 422), ({"q": "x" * 101}, 422),
        # was a 500: DuckDB's OFFSET is a BIGINT ("Type UINT64 ... out of range for ... INT64")
        ({"offset": 2**63}, 422), ({"offset": 10**30}, 422), ({"offset": country.MAX_OFFSET + 1}, 422),
    ]:
        assert get(env, "ases", **{"cc": "KR", **params}).status_code == code, params


def test_as_trend(env):
    d = get(env, "as-trend", cc="kr", asn=4766).json()
    assert (d["version"], d["as_of"], d["cc"], d["af"], d["asn"]) == (1, AS_OF, "KR", 4, 4766)
    assert d["dates"] == ["2020-01-01", "2026-10-01", AS_OF]
    assert list(d["metrics"]) == list(AS_TREND)
    assert d["metrics"]["routes"] == [30, 45, 50]
    assert d["metrics"]["space"] == [3000.0, None, 5000.0]  # NaN -> null
    assert get(env, "as-trend", cc="KR", af=6, asn=4766).json()["dates"] == [AS_OF]
    assert get(env, "as-trend", cc="KR", asn=7018).status_code == 404  # an AS of another country
    assert get(env, "as-trend", cc="KR", af=6, asn=13335).status_code == 404
    assert get(env, "as-trend", cc="KR", asn=-1).status_code == 422
    assert get(env, "as-trend", cc="KR", asn=2**32).status_code == 422
    assert get(env, "as-trend", cc="KR").status_code == 422  # asn is required
    assert get(env, "as-trend", cc="K", asn=4766).status_code == 400


def test_only_the_global_views_are_cached_per_build(env):
    for path, params in ENDPOINTS:
        assert get(env, path, **params).status_code == 200, path
    get(env, "countries", af=6)
    d = os.path.realpath(env.build)
    assert set(env.cache) == {f"country:meta:{d}", f"country:countries:{d}:4",
                              f"country:countries:{d}:6", f"country:rir-trend:{d}:4"}
    # The cache keeps the rendered body: a hit is served without re-encoding the dict.
    r = get(env, "rir-trend")
    assert (r.content, r.headers["content-type"]) == (env.cache[f"country:rir-trend:{d}:4"], "application/json")
    assert r.json()["series"]["arin"]["routes"] == [None, 600, 900, 1200]
    env.cache[f"country:rir-trend:{d}:4"] = b'{"from":"cache"}'
    assert get(env, "rir-trend").json() == {"from": "cache"}


def test_new_build_is_served_after_the_symlink_swap(env):
    assert get(env, "trend", cc="KR").json()["metrics"]["routes"][-1] == 1200
    assert get(env, "countries").json()["rows"][2]["routes"] == 1200  # now cached for build 1
    assert get(env, "rir-trend").json()["series"]["arin"]["routes"][-1] == 1200
    publish(env.root, write_build(env.root, STAMP2, env.con, as_of="2026-10-27", bump=1))
    assert get(env, "meta").json()["as_of"] == "2026-10-27"
    d = get(env, "trend", cc="KR").json()
    assert (d["as_of"], d["metrics"]["routes"][-1]) == ("2026-10-27", 1300)
    assert get(env, "countries").json()["rows"][2]["routes"] == 1300
    assert get(env, "rir-trend").json()["series"]["arin"]["routes"][-1] == 1300
    assert len([k for k in env.cache if k.startswith("country:countries:")]) == 2  # one per build


def test_manifest_is_read_once_per_build(env):
    assert get(env, "trend", cc="KR").json()["as_of"] == AS_OF
    (env.build / "manifest.json").write_text(json.dumps(manifest(as_of="1999-01-01")))
    assert get(env, "trend", cc="KR").json()["as_of"] == AS_OF  # same realpath(current): not re-read
    publish(env.root, env.build)  # re-pointing at the same build changes nothing
    assert get(env, "trend", cc="KR").json()["as_of"] == AS_OF
    publish(env.root, write_build(env.root, STAMP2, env.con, as_of="2026-10-27"))
    assert get(env, "trend", cc="KR").json()["as_of"] == "2026-10-27"


def test_503_until_the_first_build(env, monkeypatch, tmp_path):
    empty, dangling = tmp_path / "empty", tmp_path / "dangling"
    empty.mkdir()
    dangling.mkdir()
    os.symlink("builds/" + STAMP, dangling / "current")  # current points at a deleted build
    for root in (empty, tmp_path / "absent", dangling):
        monkeypatch.setattr(country, "COUNTRY_RPKI_DIR", root)
        for path, params in ENDPOINTS:
            r = get(env, path, **params)
            assert (r.status_code, r.json()) == (503, {"detail": "Country analytics not built yet"}), (root, path)


def test_unreadable_build_is_503(env):
    bad = env.root / "builds" / STAMP2
    bad.mkdir()
    publish(env.root, bad)  # no manifest.json
    r = get(env, "meta")
    assert (r.status_code, r.json()) == (503, {"detail": "Country analytics build unreadable"})
    b3 = write_build(env.root, "20261029T071500Z", env.con)
    os.remove(b3 / "as_series.parquet")
    publish(env.root, b3)
    r = get(env, "as-trend", cc="KR", asn=4766)
    assert (r.status_code, r.json()) == (503, {"detail": "Country analytics build unreadable"})
    assert get(env, "trend", cc="KR").status_code == 200
    # truncated or not Parquet at all: unreadable like a missing file (was a 500)
    b4 = write_build(env.root, "20261030T071500Z", env.con)
    publish(env.root, b4)
    for content in ((b4 / "ases.parquet").read_bytes()[:200], b"", b"not parquet" * 20):
        (b4 / "ases.parquet").write_bytes(content)
        r = get(env, "ases", cc="KR")
        assert (r.status_code, r.json()) == (503, {"detail": "Country analytics build unreadable"}), content[:20]
    assert get(env, "trend", cc="KR").status_code == 200
