"""Who a request comes from: the key for the rate limiter and the visitor log.

Visitors reach the API through the Cloudflare Pages Function (frontend
functions/api/[[path]].js), which passes the visitor's address as X-Visitor-IP.
Any client can send that header, so X-Visitor-IP counts only on requests that
also carry the Function's shared secret (X-Proxy-Token, PROXY_TOKEN_FILE).
Otherwise the address is CF-Connecting-IP, which Cloudflare's edge sets itself,
or the socket peer for loopback callers. The rate limiter keys IPv6 clients on
their /64 (rate_key), since one host can use any address of its /64.
X-Forwarded-For is not read here: its first element is whatever the client sent.
(uvicorn's proxy headers, on by default, do set a loopback peer from it, but
tunnel requests always carry CF-Connecting-IP, which comes first; a local caller
could send either header anyway.)
"""

import hmac
import ipaddress
import logging
import threading
import time
from collections import OrderedDict

from fastapi import Request

from app.config import PROXY_TOKEN_FILE

log = logging.getLogger("app.client_ip")

# Most distinct keys a hit log keeps (see over_limit).
MAX_KEYS = 20_000
# Seconds between "X-Visitor-IP ignored" warnings (see _ignored_visitor_ip).
WARN_EVERY = 300

_token_lock = threading.Lock()
_token_state: dict = {"sig": "unread", "token": None}
_ignored: dict = {"count": 0, "next": 0.0}
_hits_lock = threading.Lock()


def _proxy_token() -> str | None:
    """The shared proxy secret, or None when the file is missing, unreadable or
    empty (then X-Visitor-IP is never trusted).

    Only a stat per call: the file is read again when it changes, so a rotated
    token takes effect without a restart. Each change is logged, never the value.
    """
    try:
        st = PROXY_TOKEN_FILE.stat()
    except OSError as e:
        sig = type(e).__name__  # e.g. FileNotFoundError: nothing to read
    else:
        # ctime moves on any write or chmod, and replacing the file changes the inode.
        sig = (st.st_ino, st.st_size, st.st_ctime_ns)
    with _token_lock:
        if _token_state["sig"] != sig:
            token, problem = None, sig
            if isinstance(sig, tuple):
                try:
                    token = PROXY_TOKEN_FILE.read_text().strip() or None
                    problem = "empty"
                except (OSError, ValueError) as e:
                    problem = type(e).__name__
            if token:
                log.info("Proxy token loaded from %s", PROXY_TOKEN_FILE)
            else:
                log.warning("No proxy token (%s: %s): X-Visitor-IP is ignored", PROXY_TOKEN_FILE, problem)
            _token_state.update(sig=sig, token=token)
        return _token_state["token"]


def _ignored_visitor_ip(reason: str) -> None:
    """Warn, at most every WARN_EVERY seconds, that X-Visitor-IP was not trusted.
    Steady warnings mean the Pages Function and PROXY_TOKEN_FILE disagree, so
    every visitor of the site shares the Function's rate-limit key."""
    with _token_lock:
        _ignored["count"] += 1
        now = time.monotonic()
        if now < _ignored["next"]:
            return
        count = _ignored["count"]
        _ignored.update(count=0, next=now + WARN_EVERY)
    log.warning("X-Visitor-IP ignored on %d request(s) since the last such warning (latest: %s)", count, reason)


def client_ip(request: Request) -> str:
    """The address a request is logged under; rate_key() of it is rate-limited."""
    visitor = request.headers.get("X-Visitor-IP")
    if visitor:
        provided = request.headers.get("X-Proxy-Token")
        expected = _proxy_token() if provided else None
        if not provided:
            _ignored_visitor_ip("no X-Proxy-Token")
        elif not expected:
            _ignored_visitor_ip("no proxy token on this server")
        elif hmac.compare_digest(provided.encode(), expected.encode()):
            return visitor
        else:
            _ignored_visitor_ip("wrong X-Proxy-Token")
    return (
        request.headers.get("CF-Connecting-IP")
        or (request.client.host if request.client else "unknown")
    )


def rate_key(ip: str) -> str:
    """The rate-limit key of an address: an IPv6 address by its /64 (an IPv4-mapped
    one by its IPv4 address), an IPv4 address or anything unparsable as is."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return ip
    if addr.version == 4:
        return ip
    if addr.ipv4_mapped:
        return str(addr.ipv4_mapped)
    return str(ipaddress.IPv6Network((int(addr) >> 64 << 64, 64)))


def over_limit(hits: OrderedDict[str, list[float]], key: str, limit: int, window: float) -> bool:
    """Count a request from ``key``, or return True (not counted) when it already
    made ``limit`` requests in the last ``window`` seconds.

    ``hits`` (key -> request times) is kept as an LRU: keys whose requests have
    all left the window are dropped, and past MAX_KEYS the least recently seen
    key goes, so a flood of distinct keys cannot grow it without bound.
    """
    with _hits_lock:
        now = time.monotonic()
        recent = [t for t in hits.pop(key, ()) if now - t < window]
        limited = len(recent) >= limit
        if not limited:
            recent.append(now)
        hits[key] = recent  # re-inserted last: most recently seen
        while hits:
            oldest = next(iter(hits.values()))
            if len(hits) <= MAX_KEYS and oldest and now - oldest[-1] < window:
                break
            hits.popitem(last=False)
    return limited
