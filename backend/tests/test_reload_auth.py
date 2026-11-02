"""POST /admin/reload: token check and status codes (no warehouse needed)."""

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import meta


@pytest.fixture
def client(tmp_path, monkeypatch):
    token = tmp_path / "rpki-reload.token"
    token.write_text("s3cret\n")
    monkeypatch.setattr(meta, "RELOAD_TOKEN_FILE", token)
    monkeypatch.setattr(meta, "reload_running", lambda: False)
    calls = []

    def fake_reload():
        calls.append(1)
        return fake_reload.result

    fake_reload.result = {"status": "ok", "live_roas": 1}
    monkeypatch.setattr(meta, "hot_reload", fake_reload)
    c = TestClient(app)  # no context manager: lifespan (init_db) does not run
    c.calls, c.fake = calls, fake_reload
    return c


def test_requires_token(client):
    assert client.post("/admin/reload").status_code == 401
    assert client.post("/admin/reload", headers={"X-Reload-Token": "wrong"}).status_code == 401
    assert client.post("/admin/reload", headers={"X-Reload-Token": "sécret".encode("latin-1")}).status_code == 401
    assert client.calls == []


def test_missing_token_file(client, monkeypatch, tmp_path):
    monkeypatch.setattr(meta, "RELOAD_TOKEN_FILE", tmp_path / "absent")
    assert client.post("/admin/reload", headers={"X-Reload-Token": "s3cret"}).status_code == 503


def test_wait_reports_outcome(client):
    h = {"X-Reload-Token": "s3cret"}
    r = client.post("/admin/reload?wait=true", headers=h)
    assert r.status_code == 200 and r.json()["live_roas"] == 1
    client.fake.result = {"status": "error", "detail": "boom"}
    assert client.post("/admin/reload?wait=true", headers=h).status_code == 500
    client.fake.result = {"status": "already_running"}
    assert client.post("/admin/reload?wait=true", headers=h).status_code == 409


def test_async_and_already_running(client, monkeypatch):
    h = {"X-Reload-Token": "s3cret"}
    assert client.post("/admin/reload", headers=h).status_code == 202
    monkeypatch.setattr(meta, "reload_running", lambda: True)
    assert client.post("/admin/reload", headers=h).status_code == 409


def test_reload_is_not_rate_limited(client, monkeypatch):
    from app import main
    monkeypatch.setattr(main, "_rate_limits", main.OrderedDict())
    for _ in range(main.RATE_LIMIT):
        client.get("/no-such-path")
    assert client.get("/no-such-path").status_code == 429
    assert client.post("/admin/reload", headers={"X-Reload-Token": "s3cret"}).status_code == 202
