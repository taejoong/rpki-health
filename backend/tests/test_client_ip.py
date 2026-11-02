"""client_ip(): X-Visitor-IP counts only with the proxy token, for the rate
limiter and the visitor log; rate_key() keys IPv6 clients on their /64;
over_limit() keeps the limiter's keys bounded (no warehouse needed)."""

import logging
import os
import time
from collections import OrderedDict

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from app import client_ip as cip
from app import main, tracking
from app.main import app

TOKEN = "s3cret"
PEER = "203.0.113.5"
EGRESS = "2a06:98c0:3600::103"  # CF-Connecting-IP on the Pages Function's own requests


@pytest.fixture
def token_file(tmp_path, monkeypatch):
    path = tmp_path / "rpki-proxy.token"
    path.write_text(TOKEN + "\n")
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", path)
    monkeypatch.setattr(cip, "_token_state", {"sig": "unread", "token": None})
    monkeypatch.setattr(cip, "_ignored", {"count": 0, "next": 0.0})
    return path


@pytest.fixture
def client(token_file, monkeypatch):
    monkeypatch.setattr(main, "_rate_limits", main.OrderedDict())
    logged = []
    monkeypatch.setattr(tracking, "log_event", lambda **kw: logged.append(kw["ip"]))
    c = TestClient(app)  # no context manager: lifespan (init_db) does not run
    c.logged = logged
    return c


def key_of(headers=None, peer=PEER):
    """client_ip() of a bare request from ``peer`` carrying ``headers``."""
    raw = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()]
    return cip.client_ip(Request({"type": "http", "headers": raw, "client": (peer, 40000) if peer else None}))


class CountingFile:
    """Stands in for PROXY_TOKEN_FILE and counts reads."""

    def __init__(self, path):
        self.path, self.reads = path, 0

    def stat(self):
        return self.path.stat()

    def read_text(self):
        self.reads += 1
        return self.path.read_text()


def test_visitor_ip_only_with_the_proxy_token(token_file):
    spoof = {"X-Visitor-IP": "192.0.2.1"}
    assert key_of(spoof) == PEER
    assert key_of({**spoof, "X-Proxy-Token": TOKEN}) == "192.0.2.1"
    # What the Pages Function sends: the token beats its own CF-Connecting-IP.
    pages = {**spoof, "X-Proxy-Token": TOKEN, "CF-Connecting-IP": EGRESS}
    assert key_of(pages) == "192.0.2.1"
    for wrong in ("wrong", "", TOKEN.upper(), TOKEN[:-1], TOKEN + "x", "sécret"):
        assert key_of({**spoof, "X-Proxy-Token": wrong}) == PEER
        assert key_of({**pages, "X-Proxy-Token": wrong}) == EGRESS


def test_cf_connecting_ip_then_peer_never_x_forwarded_for(token_file):
    assert key_of({"CF-Connecting-IP": "198.51.100.7", "X-Visitor-IP": "192.0.2.1"}) == "198.51.100.7"
    assert key_of({"X-Forwarded-For": "192.0.2.9, 10.0.0.1"}) == PEER
    assert key_of(peer=None) == "unknown"
    # The proxy without a visitor address (e.g. wrangler dev) falls through too.
    assert key_of({"X-Proxy-Token": TOKEN, "X-Visitor-IP": "", "CF-Connecting-IP": "198.51.100.7"}) == "198.51.100.7"


def test_missing_unreadable_or_empty_token_file_fails_closed(token_file, monkeypatch, tmp_path):
    h = {"X-Visitor-IP": "192.0.2.1", "X-Proxy-Token": TOKEN}
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", tmp_path / "absent")
    assert key_of(h) == PEER
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", tmp_path)  # a directory: the read fails
    assert key_of(h) == PEER
    token_file.write_text(" \n")
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", token_file)
    assert key_of(h) == PEER
    assert key_of({"X-Visitor-IP": "192.0.2.1", "X-Proxy-Token": ""}) == PEER


def test_token_read_once_and_rotated_without_restart(token_file, monkeypatch):
    f = CountingFile(token_file)
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", f)
    h = {"X-Visitor-IP": "192.0.2.1"}
    for _ in range(20):
        assert key_of({**h, "X-Proxy-Token": TOKEN}) == "192.0.2.1"
    assert f.reads == 1  # a stat per request, a read only when the file changes

    new = token_file.with_name("rotated.tmp")
    new.write_text("r0tated\n")
    os.replace(new, token_file)
    assert key_of({**h, "X-Proxy-Token": TOKEN}) == PEER
    assert key_of({**h, "X-Proxy-Token": "r0tated"}) == "192.0.2.1"
    token_file.write_text("rewritten-in-place\n")
    assert key_of({**h, "X-Proxy-Token": "rewritten-in-place"}) == "192.0.2.1"
    assert f.reads == 3
    # openssl rand -hex 32 > file keeps inode and size, so only ctime shows the
    # change; the sleep puts it on a later tick of a coarse filesystem clock.
    time.sleep(0.05)
    token_file.write_text("REWRITTEN-IN-PLACE\n")
    assert key_of({**h, "X-Proxy-Token": "REWRITTEN-IN-PLACE"}) == "192.0.2.1"
    assert f.reads == 4


def test_untrusted_visitor_ip_is_logged_without_values(token_file, monkeypatch, tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="app.client_ip")
    h = {"X-Visitor-IP": "192.0.2.1"}
    for _ in range(3):
        key_of({**h, "X-Proxy-Token": TOKEN})
    for _ in range(3):
        key_of({**h, "X-Proxy-Token": "guess-1"})  # one warning per WARN_EVERY s...
    cip._ignored["next"] = 0.0  # ... which has now passed
    key_of(h)
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", tmp_path / "absent")
    key_of({**h, "X-Proxy-Token": TOKEN})
    key_of({**h, "X-Proxy-Token": TOKEN})
    token_file.write_text("\n")
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", token_file)
    key_of({**h, "X-Proxy-Token": TOKEN})
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        ("INFO", f"Proxy token loaded from {token_file}"),
        ("WARNING", "X-Visitor-IP ignored on 1 request(s) since the last such warning (latest: wrong X-Proxy-Token)"),
        ("WARNING", "X-Visitor-IP ignored on 3 request(s) since the last such warning (latest: no X-Proxy-Token)"),
        ("WARNING", f"No proxy token ({tmp_path / 'absent'}: FileNotFoundError): X-Visitor-IP is ignored"),
        ("WARNING", f"No proxy token ({token_file}: empty): X-Visitor-IP is ignored"),
    ]
    assert TOKEN not in caplog.text and "guess-1" not in caplog.text


def test_rotating_visitor_ip_without_token_is_limited(client):
    for i in range(main.RATE_LIMIT):
        h = {"X-Visitor-IP": f"192.0.2.{i}", "X-Forwarded-For": f"198.51.100.{i}"}
        assert client.get("/no-such-path", headers=h).status_code == 404
    assert client.get("/no-such-path", headers={"X-Visitor-IP": "192.0.2.250"}).status_code == 429
    assert list(main._rate_limits) == ["testclient"]
    assert set(client.logged) == {"testclient"}  # the visitor log uses the same key


def test_proxy_token_gives_each_visitor_a_bucket(client):
    def get(visitor):
        h = {"X-Proxy-Token": TOKEN, "X-Visitor-IP": visitor, "CF-Connecting-IP": EGRESS}
        return client.get("/no-such-path", headers=h).status_code

    assert [get("192.0.2.1") for _ in range(main.RATE_LIMIT + 1)] == [404] * main.RATE_LIMIT + [429]
    assert get("192.0.2.2") == 404
    assert client.get("/no-such-path").status_code == 404
    assert list(main._rate_limits) == ["192.0.2.1", "192.0.2.2", "testclient"]
    assert set(client.logged) == {"192.0.2.1", "192.0.2.2", "testclient"}


def test_wrong_empty_or_missing_token_ignores_visitor_ip(client, monkeypatch, tmp_path):
    for tok in ("wrong", "", TOKEN.upper()):
        client.get("/no-such-path", headers={"X-Proxy-Token": tok, "X-Visitor-IP": "192.0.2.1"})
    monkeypatch.setattr(cip, "PROXY_TOKEN_FILE", tmp_path / "absent")
    client.get("/no-such-path", headers={"X-Proxy-Token": TOKEN, "X-Visitor-IP": "192.0.2.1"})
    assert list(main._rate_limits) == ["testclient"] and len(main._rate_limits["testclient"]) == 4
    assert client.logged == ["testclient"] * 4


def test_limiter_keys_stay_bounded(client, monkeypatch):
    monkeypatch.setattr(cip, "MAX_KEYS", 50)
    for i in range(200):
        h = {"X-Proxy-Token": TOKEN, "X-Visitor-IP": f"10.0.0.{i}"}
        assert client.get("/no-such-path", headers=h).status_code == 404
    assert len(main._rate_limits) == 50 and next(reversed(main._rate_limits)) == "10.0.0.199"


def test_over_limit_window():
    hits = OrderedDict()
    assert [cip.over_limit(hits, "a", 2, 60) for _ in range(3)] == [False, False, True]
    assert len(hits["a"]) == 2  # a refused request is not counted
    hits["a"] = [t - 61 for t in hits["a"]]  # ... and the key frees up once they leave the window
    assert not cip.over_limit(hits, "a", 2, 60)


def test_over_limit_drops_idle_keys_and_caps_the_rest():
    now = time.monotonic()
    hits = OrderedDict([("idle", [now - 61]), ("idle2", [now - 3600]), ("busy", [now])])
    cip.over_limit(hits, "new", 2, 60)
    assert list(hits) == ["busy", "new"]  # keys with no hit inside the window are gone

    hits = OrderedDict()
    for i in range(cip.MAX_KEYS):
        cip.over_limit(hits, f"k{i}", 2, 60)
    cip.over_limit(hits, "k0", 2, 60)  # seen again: now the most recent
    for i in range(1000):
        cip.over_limit(hits, f"extra{i}", 2, 60)
    assert len(hits) == cip.MAX_KEYS
    assert "k0" in hits and "k1000" not in hits and "k1001" in hits  # least recently seen go first


def test_rate_key_groups_ipv6_by_64():
    assert cip.rate_key("2001:db8:1:2:aaaa::1") == "2001:db8:1:2::/64"
    assert cip.rate_key("2001:DB8:1:2:ffff:ffff:ffff:ffff") == "2001:db8:1:2::/64"
    assert cip.rate_key("::ffff:192.0.2.7") == "192.0.2.7"
    assert cip.rate_key("192.0.2.7") == "192.0.2.7"
    assert cip.rate_key("testclient") == "testclient"  # not an address: as is


def test_rotating_ipv6_within_a_64_is_limited(client):
    def get(i):
        h = {"X-Proxy-Token": TOKEN, "X-Visitor-IP": f"2001:db8:5:6::{i:x}", "CF-Connecting-IP": EGRESS}
        return client.get("/no-such-path", headers=h).status_code

    assert [get(i) for i in range(1, main.RATE_LIMIT + 2)] == [404] * main.RATE_LIMIT + [429]
    assert list(main._rate_limits) == ["2001:db8:5:6::/64"]
    assert client.logged[0] == "2001:db8:5:6::1"  # the visitor log keeps the full address
