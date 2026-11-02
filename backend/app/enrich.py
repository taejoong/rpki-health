"""IP -> (asn, as_org) enrichment via Team Cymru's DNS origin service, cached in
the visitor-analytics DB (ip_meta table).

IPv4 via origin.asn.cymru.com and IPv6 via origin6.asn.cymru.com (empty -> ('',
'')). No external deps: a minimal raw-UDP DNS TXT resolver, so there is no
dig / dnspython requirement. Nothing here ever raises into the caller; on any
failure an IP resolves to ('', '') and is cached as such so it is not retried
on every dashboard load.
"""

import ipaddress
import random
import socket
import sqlite3
import struct
import threading
import time

from app.config import ANALYTICS_DB

_lock = threading.Lock()
_con: sqlite3.Connection | None = None
_asname: dict[str, str] = {}
_resolver_cache: str | None = None


def _db() -> sqlite3.Connection:
    global _con
    if _con is None:
        _con = sqlite3.connect(str(ANALYTICS_DB), check_same_thread=False)
        _con.execute("PRAGMA journal_mode=WAL")
        _con.execute(
            """CREATE TABLE IF NOT EXISTS ip_meta (
                 ip TEXT PRIMARY KEY,
                 asn TEXT DEFAULT '',
                 as_org TEXT DEFAULT '',
                 updated_ts INTEGER
               )"""
        )
        _con.commit()
    return _con


# ------------------------------------------------------------------ dns/TXT
def _resolver() -> str:
    global _resolver_cache
    if _resolver_cache:
        return _resolver_cache
    _resolver_cache = "8.8.8.8"
    try:
        for line in open("/etc/resolv.conf"):
            if line.startswith("nameserver"):
                ns = line.split()[1]
                if ":" not in ns:  # this helper is IPv4-only
                    _resolver_cache = ns
                    break
    except OSError:
        pass
    return _resolver_cache


def _skip_name(data: bytes, off: int) -> int:
    while off < len(data):
        ln = data[off]
        if ln == 0:
            return off + 1
        if ln & 0xC0 == 0xC0:
            return off + 2
        off += 1 + ln
    return off


def dns_txt(name: str, server: str | None = None, timeout: float = 2.0) -> list[str]:
    """Minimal DNS TXT lookup; avoids depending on dig / bind-utils being installed."""
    if server is None:
        server = _resolver()
    qid = random.randint(0, 0xFFFF)
    q = struct.pack(">HHHHHH", qid, 0x0100, 1, 0, 0, 0)
    for label in name.split("."):
        q += bytes([len(label)]) + label.encode()
    q += b"\x00" + struct.pack(">HH", 16, 1)  # TXT, IN
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(q, (server, 53))
        data, _ = s.recvfrom(4096)
    except (socket.timeout, OSError):
        return []
    finally:
        s.close()
    if len(data) < 12 or struct.unpack(">H", data[:2])[0] != qid:
        return []
    qd, an = struct.unpack(">HH", data[4:8])
    off = 12
    for _ in range(qd):  # skip question
        off = _skip_name(data, off) + 4
    out = []
    for _ in range(an):
        off = _skip_name(data, off)
        if off + 10 > len(data):
            break
        rtype, _, _, rdlen = struct.unpack(">HHIH", data[off:off + 10])
        off += 10
        rd = data[off:off + rdlen]
        off += rdlen
        if rtype == 16 and rd:
            txt, p = b"", 0
            while p < len(rd):
                ln = rd[p]
                txt += rd[p + 1:p + 1 + ln]
                p += 1 + ln
            out.append(txt.decode("utf-8", "replace"))
    return out


def _cymru(ip: str) -> tuple[str, str]:
    """(asn, as_org) for an IPv4/IPv6 via Team Cymru; ('', '') on failure."""
    if not ip:
        return ("", "")
    if ":" in ip:  # IPv6 -> expanded nibbles under origin6.asn.cymru.com
        try:
            nibbles = ipaddress.IPv6Address(ip).exploded.replace(":", "")
            rev = ".".join(reversed(nibbles))
        except Exception:
            return ("", "")
        qname = "%s.origin6.asn.cymru.com" % rev
    else:
        try:
            rev = ".".join(reversed(ip.split(".")))
        except Exception:
            return ("", "")
        qname = "%s.origin.asn.cymru.com" % rev
    txt = dns_txt(qname)
    if not txt:
        return ("", "")
    # "13335 | 104.16.0.0/12 | US | arin | 2010-07-09"
    f = [x.strip() for x in txt[0].split("|")]
    if not f or not f[0]:
        return ("", "")
    asn = f[0].split()[0]
    org = ""
    if asn:
        if asn not in _asname:
            t = dns_txt("AS%s.asn.cymru.com" % asn)
            g = [x.strip() for x in t[0].split("|")] if t else []
            # "13335 | US | arin | 2010-07-09 | CLOUDFLARENET, US"
            _asname[asn] = g[-1] if g else ""
        org = _asname[asn]
    return (asn, org)


def enrich_ips(ips, max_new: int = 150) -> dict[str, tuple[str, str]]:
    """Ensure up to `max_new` uncached IPv4s from `ips` are resolved and cached,
    then return {ip: (asn, as_org)} for every requested ip (unresolved -> ('','')).

    Bounded per call so a cold cache warms over a few dashboard loads instead of
    blocking one request on hundreds of DNS lookups. Heaviest-first ordering is
    the caller's job (pass ips already ordered by importance)."""
    want = [ip for ip in dict.fromkeys(ips) if ip]  # de-dup, keep order
    con = _db()
    with _lock:
        cached = {r[0]: (r[1] or "", r[2] or "")
                  for r in con.execute("SELECT ip, asn, as_org FROM ip_meta")}
    todo = [ip for ip in want if ip not in cached][:max_new]
    if todo:
        now = int(time.time())
        rows = []
        for ip in todo:
            try:
                asn, org = _cymru(ip)
            except Exception:
                asn, org = ("", "")
            cached[ip] = (asn, org)
            rows.append((ip, asn, org, now))
        try:
            with _lock:
                con.executemany(
                    "INSERT OR REPLACE INTO ip_meta (ip, asn, as_org, updated_ts) "
                    "VALUES (?, ?, ?, ?)",
                    rows,
                )
                con.commit()
        except Exception:
            pass
    return {ip: cached.get(ip, ("", "")) for ip in want}
