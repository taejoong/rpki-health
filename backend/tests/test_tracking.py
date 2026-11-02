"""The visitor log never stores credentials passed in a query string."""

from app import tracking


def test_secret_query_values_are_redacted():
    assert tracking._redact("token=abc123&x=1") == "token=redacted&x=1"
    assert tracking._redact("Password=hunter2&KEY=k&secret=s") == "Password=redacted&KEY=redacted&secret=redacted"
    assert tracking._redact("asn=13335&prefix=1.0.0.0%2F24") == "asn=13335&prefix=1.0.0.0%2F24"
    assert tracking._redact("tokens=1&monkey=2&token") == "tokens=1&monkey=2&token"  # names must match exactly
    assert tracking._redact("") == ""


def test_log_event_stores_the_redacted_query(tmp_path, monkeypatch):
    import sqlite3

    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.execute("CREATE TABLE events (ts TEXT, ip TEXT, country TEXT, ua TEXT, path TEXT, query TEXT)")
    monkeypatch.setattr(tracking, "_con", con)
    tracking.log_event(ip="192.0.2.1", country=None, ua=None, path="/x", query="token=abc&a=b")
    assert con.execute("SELECT query FROM events").fetchall() == [("token=redacted&a=b",)]
