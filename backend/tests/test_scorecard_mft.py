"""Manifest-EE revocation and late re-issue (pipelines/scorecard/scs/m_mft.py) on a synthetic APNIC warehouse.

The world: rpki-client cycles every 20 minutes from 2026-10-10 00:00:01 to 2026-10-26 01:00:01, except an
outage (no cycle from 2026-10-14 00:00:01 to 10-15 11:40:01: the cycles 10-13 23:40:01 and 10-15 12:00:01 are
consecutive but 36 h 20 min apart) and one near-empty cycle (2026-10-21 12:00:01, 10 files instead of 1,000:
not a good cycle). One filler object is first seen in every cycle, so that the cycles fetch something,
except from 2026-10-24 06:00:01 to 11:40:01, where only the cache was re-listed (good cycles in the ledger, no
new object); warehouse-ripencc has the same cycles and one new object at 09:00:01, so that cycle fetched.
Weeks W1 = 2026-10-12 (masked: outage) and W2 = 2026-10-19; as_of 2026-10-26 00:40:01.
Every manifest's EE serial is its name in lower case; P -> S is a replacement (predecessor, successor).

key  host (class)                        replacements (successor thisUpdate in W2 unless noted)
ka   rpki.apnic.net (rir_hosted)         manifests 9 -> 10 -> 11 -> 12 -> 13 (numeric order: '10' < '9' as text),
                                         each 24 h, re-issued every 6 h: 4 direct early pairs, headroom 18.25,
                                         18, 18, 18 h. Revoked: a1 on CA1 at succ (yes), a2 on CA2 at succ (yes),
                                         a3 first on CA3 at succ + 2 d exactly (yes), a4 first on CA4 at succ + 2 d
                                         + 1 min (no): 3 of 4. CRLs 98 -> 99 -> 100 -> 101 -> 102: 4 direct pairs,
                                         headroom 42, 42, -6 (late: CA3 issued 6 h after CA2's nextUpdate), 41.983;
                                         97 -> 98 is a W1 pair (24 h) whose two CRLs are also in W2's set
kb   rpki-repository.nic.ad.jp (JPNIC)   B1 -> B2 early, headroom 12.1 h; its CRL lists nothing: not revoked
kc   no CA certificate; manifest EE on   C1 -> C2: C2 first seen in the near-empty cycle (not observed);
     rpki.cnnic.cn (CNNIC, fallback)     C2 -> C3 early, headroom 11.667 h, c2 on CC1 at succ: revoked
kd   krill.example.net (delegated)       D1 (thisUpdate 2026-07-01, object and EE ids older than the bulk, still
                                         present) -> D2: late by 110 d 9 h 45 min (headroom -2649.75 h); D2 -> D3
                                         with thisUpdate = nextUpdate (neither early nor late, headroom 0); CRLs
                                         DC1 (old) -> DC2: late, -2649.75 h
ke   rpki.apnic.net (rir_hosted), W1     E1 -> E2 across the outage (would be late: excluded); E2 -> E3 early,
                                         revoked (0.917 h); E3 -> E4 skips a cycle; E4 -> E5 overlap: excluded
kf   no CA certificate, EE without an    no manifest numbers (thisUpdate order): F0 -> F1 early (0.167 h), no
     rsync URI (unknown)                 CRL at all: not revoked; F1 -> F2: F2 first seen 01:00:01, after as_of
kg   rpki.apnic.net                      G1 -> G2 early (18.167 h); g1 listed by other keys (kz; kc's CRL inside
                                         G2's window) and by a kg CRL 1 d 1 min before the successor: not revoked
kh   rpki.apnic.net                      H1 -> H2 early (17.917 h); h1 on a CRL of 10-26 00:50, after as_of
ki   rpki.apnic.net                      I1 -> I2 early (17.917 h); i1 on a CRL exactly 1 d before succ: revoked
kj   rpki.apnic.net, W1                  J1 -> J2 across the outage, early (7-day manifests): excluded
kk   rpki.apnic.net                      K1 last seen in the re-listed stretch (11:40:01), K2 issued in it and first
                                         seen at 12:00:01: consecutive good cycles 20 min apart, but K1's last cycle
                                         fetched nothing: excluded (it would read late)
kl   rpki.apnic.net                      L1 last seen at 09:00:01 (APNIC got nothing new then, RIPE NCC did) -> L2:
                                         early (15.083 h), not revoked
km   rpki.apnic.net                      numbering restarted: 50 -> 51 on 10-10 (gone on 10-11), then 50 -> 51 again
                                         on 10-22: early (17.917 h), not revoked, in a W2-only call and in a W1..W2
                                         call alike
kn   rpki.apnic.net                      O1 issued in W1 (10-day manifest), last seen Monday 10-26 00:00:01 -> O2 issued
                                         Sunday 23:50, first seen 00:20:01: early (46.167 h), not revoked
ko   rpki.apnic.net                      P1 (number 1) -> S1 (3) consecutive in time, but an orphan manifest numbered 2
                                         (issued in W1, present until the as_of cycle) lies between them: not a pair,
                                         at every as_of
A3's manifest row is stored twice (two parts); the <table>.parquet symlinks are skipped.
"""
import datetime as dt
import os
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scorecard_world as W  # noqa: E402
from scs import assemble, common, defs, ledger, m_mft, metrics, status  # noqa: E402

D = dt.date
RIR = 'apnic'
AS_OF = '2026-10-26 00:40:01'
W1, W2 = D(2026, 10, 12), D(2026, 10, 19)
NEAR_EMPTY = '2026-10-21 12:00:01'
CYCLES = [c for c in W.cycles('2026-10-10', '2026-10-26')
          if not '2026-10-14 00:00:01' <= c < '2026-10-15 12:00:01' and c <= '2026-10-26 01:00:01']
SID = {c: i for i, c in enumerate(CYCLES)}         # the ledger's and the presence build's snapshot numbering
FIRST, LAST = CYCLES[0], CYCLES[-1]
CA_HOSTS = {'ka': 'rpki.apnic.net', 'kb': 'rpki-repository.nic.ad.jp', 'kd': 'krill.example.net',
            'ke': 'rpki.apnic.net', 'kg': 'rpki.apnic.net', 'kh': 'rpki.apnic.net', 'ki': 'rpki.apnic.net',
            'kj': 'rpki.apnic.net', 'kk': 'rpki.apnic.net', 'kl': 'rpki.apnic.net', 'km': 'rpki.apnic.net',
            'kn': 'rpki.apnic.net', 'ko': 'rpki.apnic.net'}
RELISTED = ('2026-10-24 06:00:01', '2026-10-24 11:40:01')   # cycles that only re-listed the cache (no new object)
OTHER = 'ripencc'
EE_SIA = {'kc': 'rsync://rpki.cnnic.cn/repo/kc/kc.mft,https://rrdp.cnnic.cn/notification.xml',
          'kf': 'https://only.example.net/kf/kf.mft'}
OLD = {'D1': (1001, 5001), 'DC1': (1002, None)}      # object id (and EE cert id) older than the bulk

MANIFESTS = [  # name, key, manifestNumber, thisUpdate, nextUpdate, first seen, last seen
    ('A1', 'ka', '9', '2026-10-19 00:05:00', '2026-10-20 00:05:00', '2026-10-19 00:20:01', '2026-10-19 05:40:01'),
    ('A2', 'ka', '10', '2026-10-19 05:50:00', '2026-10-20 05:50:00', '2026-10-19 06:00:01', '2026-10-19 11:40:01'),
    ('A3', 'ka', '11', '2026-10-19 11:50:00', '2026-10-20 11:50:00', '2026-10-19 12:00:01', '2026-10-19 17:40:01'),
    ('A4', 'ka', '12', '2026-10-19 17:50:00', '2026-10-20 17:50:00', '2026-10-19 18:00:01', '2026-10-19 23:40:01'),
    ('A5', 'ka', '13', '2026-10-19 23:50:00', '2026-10-20 23:50:00', '2026-10-20 00:00:01', LAST),
    ('B1', 'kb', '1', '2026-10-20 00:05:00', '2026-10-21 00:05:00', '2026-10-20 00:20:01', '2026-10-20 12:00:01'),
    ('B2', 'kb', '2', '2026-10-20 11:59:00', '2026-10-21 11:59:00', '2026-10-20 12:20:01', LAST),
    ('C1', 'kc', '5', '2026-10-21 00:05:00', '2026-10-22 00:05:00', '2026-10-21 00:20:01', '2026-10-21 11:40:01'),
    ('C2', 'kc', '6', '2026-10-21 11:45:00', '2026-10-22 11:45:00', NEAR_EMPTY, '2026-10-22 00:00:01'),
    ('C3', 'kc', '7', '2026-10-22 00:05:00', '2026-10-23 00:05:00', '2026-10-22 00:20:01', LAST),
    ('D1', 'kd', '100', '2026-07-01 00:00:00', '2026-07-02 00:00:00', FIRST, '2026-10-20 09:40:01'),
    ('D2', 'kd', '101', '2026-10-20 09:45:00', '2026-10-21 09:45:00', '2026-10-20 10:00:01', '2026-10-21 09:40:01'),
    ('D3', 'kd', '102', '2026-10-21 09:45:00', '2026-10-22 09:45:00', '2026-10-21 10:00:01', LAST),
    ('E1', 'ke', '1', '2026-10-13 20:00:00', '2026-10-14 20:00:00', '2026-10-13 20:20:01', '2026-10-13 23:40:01'),
    ('E2', 'ke', '2', '2026-10-15 11:00:00', '2026-10-16 11:00:00', '2026-10-15 12:00:01', '2026-10-16 10:00:01'),
    ('E3', 'ke', '3', '2026-10-16 10:05:00', '2026-10-17 10:05:00', '2026-10-16 10:20:01', '2026-10-16 20:00:01'),
    ('E4', 'ke', '4', '2026-10-16 20:30:00', '2026-10-17 20:30:00', '2026-10-16 20:40:01', '2026-10-17 08:00:01'),
    ('E5', 'ke', '5', '2026-10-17 07:30:00', '2026-10-18 07:30:00', '2026-10-17 07:40:01', LAST),
    ('F0', 'kf', None, '2026-10-23 00:00:00', '2026-10-24 00:00:00', '2026-10-23 00:20:01', '2026-10-23 23:40:01'),
    ('F1', 'kf', None, '2026-10-23 23:50:00', '2026-10-26 00:00:00', '2026-10-24 00:00:01', '2026-10-26 00:40:01'),
    ('F2', 'kf', None, '2026-10-25 23:50:00', '2026-10-26 23:50:00', LAST, LAST),
    ('G1', 'kg', '1', '2026-10-22 00:00:00', '2026-10-23 00:00:00', '2026-10-22 00:20:01', '2026-10-22 05:40:01'),
    ('G2', 'kg', '2', '2026-10-22 05:50:00', '2026-10-23 05:50:00', '2026-10-22 06:00:01', LAST),
    ('H1', 'kh', '1', '2026-10-25 12:00:00', '2026-10-26 12:00:00', '2026-10-25 12:20:01', '2026-10-25 18:00:01'),
    ('H2', 'kh', '2', '2026-10-25 18:05:00', '2026-10-26 18:05:00', '2026-10-25 18:20:01', LAST),
    ('I1', 'ki', '1', '2026-10-23 06:00:00', '2026-10-24 06:00:00', '2026-10-23 06:20:01', '2026-10-23 12:00:01'),
    ('I2', 'ki', '2', '2026-10-23 12:05:00', '2026-10-24 12:05:00', '2026-10-23 12:20:01', LAST),
    ('J1', 'kj', '1', '2026-10-13 23:00:00', '2026-10-20 23:00:00', '2026-10-13 23:20:01', '2026-10-13 23:40:01'),
    ('J2', 'kj', '2', '2026-10-15 11:30:00', '2026-10-22 11:30:00', '2026-10-15 12:00:01', LAST),
    ('K1', 'kk', '1', '2026-10-23 18:00:00', '2026-10-24 08:00:00', '2026-10-23 18:20:01', '2026-10-24 11:40:01'),
    ('K2', 'kk', '2', '2026-10-24 10:00:00', '2026-10-25 10:00:00', '2026-10-24 12:00:01', LAST),
    ('L1', 'kl', '1', '2026-10-24 00:10:00', '2026-10-25 00:10:00', '2026-10-24 00:20:01', '2026-10-24 09:00:01'),
    ('L2', 'kl', '2', '2026-10-24 09:05:00', '2026-10-25 09:05:00', '2026-10-24 09:20:01', LAST),
    ('M1', 'km', '50', '2026-10-10 00:30:00', '2026-10-11 00:30:00', '2026-10-10 00:40:01', '2026-10-10 12:00:01'),
    ('M2', 'km', '51', '2026-10-10 12:05:00', '2026-10-11 12:05:00', '2026-10-10 12:20:01', '2026-10-11 06:00:01'),
    ('N1', 'km', '50', '2026-10-22 10:00:00', '2026-10-23 10:00:00', '2026-10-22 10:20:01', '2026-10-22 16:00:01'),
    ('N2', 'km', '51', '2026-10-22 16:05:00', '2026-10-23 16:05:00', '2026-10-22 16:20:01', LAST),
    ('O1', 'kn', '1', '2026-10-17 22:00:00', '2026-10-27 22:00:00', '2026-10-17 22:20:01', '2026-10-26 00:00:01'),
    ('O2', 'kn', '2', '2026-10-25 23:50:00', '2026-10-28 23:50:00', '2026-10-26 00:20:01', LAST),
    ('P1', 'ko', '1', '2026-10-21 00:00:00', '2026-10-22 00:00:00', '2026-10-21 00:20:01', '2026-10-21 05:40:01'),
    ('Q1', 'ko', '2', '2026-10-15 13:00:00', '2026-10-30 00:00:00', '2026-10-15 13:20:01', AS_OF),
    ('S1', 'ko', '3', '2026-10-21 05:50:00', '2026-10-22 05:50:00', '2026-10-21 06:00:01', LAST),
]
CRLS = [  # name, key (aki), crlNumber, thisUpdate, nextUpdate, first seen, last seen (None: no presence), serials
    ('CAm1', 'ka', '97', '2026-10-17 23:50:00', '2026-10-19 23:50:00', '2026-10-18 00:00:01', '2026-10-18 23:40:01', []),
    ('CA0', 'ka', '98', '2026-10-18 23:50:00', '2026-10-20 23:50:00', '2026-10-19 00:00:01', '2026-10-19 05:40:01', []),
    ('CA1', 'ka', '99', '2026-10-19 05:50:00', '2026-10-21 05:50:00', '2026-10-19 06:00:01', '2026-10-19 11:40:01', ['a1']),
    ('CA2', 'ka', '100', '2026-10-19 11:50:00', '2026-10-21 11:50:00', '2026-10-19 12:00:01', '2026-10-21 17:40:01',
     ['a1', 'a2']),
    ('CA3', 'ka', '101', '2026-10-21 17:50:00', '2026-10-23 17:50:00', '2026-10-21 18:00:01', '2026-10-21 23:40:01',
     ['a1', 'a2', 'a3']),
    ('CA4', 'ka', '102', '2026-10-21 23:51:00', '2026-10-23 23:51:00', '2026-10-22 00:00:01', LAST,
     ['a1', 'a2', 'a3', 'a4']),
    ('CB1', 'kb', '1', '2026-10-20 11:59:00', '2026-10-22 11:59:00', '2026-10-20 12:20:01', LAST, []),
    ('CC1', 'kc', '1', '2026-10-22 00:05:00', '2026-10-24 00:05:00', '2026-10-22 00:20:01', LAST, ['c2', 'g1']),
    ('DC1', 'kd', '50', '2026-07-01 00:00:00', '2026-07-02 00:00:00', FIRST, '2026-10-20 09:40:01', []),
    ('DC2', 'kd', '51', '2026-10-20 09:45:00', '2026-10-21 09:45:00', '2026-10-20 10:00:01', LAST, ['d1']),
    ('CE1', 'ke', '1', '2026-10-16 10:05:00', '2026-10-18 10:05:00', '2026-10-16 10:20:01', LAST, ['e2']),
    ('CG0', 'kg', '1', '2026-10-21 05:49:00', '2026-10-23 05:49:00', None, None, ['g1']),
    ('KZ1', 'kz', '1', '2026-10-22 05:50:00', '2026-10-24 05:50:00', None, None, ['g1']),
    ('HC1', 'kh', '1', '2026-10-26 00:50:00', '2026-10-28 00:50:00', LAST, LAST, ['h1']),
    ('CI0', 'ki', '1', '2026-10-22 12:05:00', '2026-10-24 12:05:00', None, None, ['i1']),
]


def ids():
    """{name: (object id, EE cert id)}: ids in list order, the old objects below the bulk."""
    out, oid = {}, 200000
    for name, *_ in MANIFESTS + CRLS:
        oid += 1
        out[name] = OLD.get(name, (oid, oid + 400000))
    return out


def presence_row(oid, first, last, shift=dt.timedelta()):
    fmt = (lambda t: (dt.datetime.fromisoformat(t) + shift).strftime('%Y-%m-%d %H:%M:%S'))
    return {'object_id': oid, 'first_sid': SID[first], 'last_sid': SID[last], 'first_ts': fmt(first), 'last_ts': fmt(last),
            'runs_count': 1, 'total_days': SID[last] - SID[first] + 1}


def build_world(base, presence_shift=dt.timedelta()):
    ident = ids()
    W.write_table(base, RIR, 'certs/is_ca=true', [
        {'cert_id': 900 + i, 'ski': k, 'aki': 'apnic-ta', 'is_ca_flag': True, 'not_before': '2026-01-01 00:00:00',
         'not_after': '2027-01-01 00:00:00', 'sia': f'rsync://{h}/repo/{k}/,https://rrdp.{h}/n.xml,rsync://{h}/repo/{k}/{k}.mft'}
        for i, (k, h) in enumerate(sorted(CA_HOSTS.items()))])
    mft, sig, ee, pres = [], [], [], []
    for name, key, num, tu, nu, first, last in MANIFESTS:
        oid, cid = ident[name]
        mft.append({'object_id': oid, 'manifest_number': num, 'this_update': tu.replace(' ', 'T') + '+00:00',
                    'next_update': nu.replace(' ', 'T') + '+00:00', 'file_count': 2})
        sig.append({'object_id': oid, 'object_type': 'MFT', 'signer_cert_id': cid})
        sia = EE_SIA.get(key) or f'rsync://{CA_HOSTS[key]}/repo/{key}/{key}.mft,https://rrdp.{CA_HOSTS[key]}/n.xml'
        ee.append({'cert_id': cid, 'is_ca_flag': False, 'ski': f'ee-{name}', 'aki': key, 'serial_dec': name.lower(),
                   'not_before': tu, 'not_after': nu, 'sia': sia})
        pres.append(presence_row(oid, first, last, presence_shift))
    W.write_table(base, RIR, 'manifests', mft[:12], part=1)
    W.write_table(base, RIR, 'manifests', mft[12:] + [mft[2]], part=2)            # A3 stored twice
    W.write_table(base, RIR, 'object_signatures', sig)
    W.write_table(base, RIR, 'certs/is_ca=false', ee)
    crl, ser = [], []
    for name, key, num, tu, nu, first, last, serials in CRLS:
        oid = ident[name][0]
        crl.append({'crl_id': oid, 'object_id': oid, 'crl_number': num, 'this_update': tu, 'next_update': nu,
                    'revoked_count': len(serials), 'aki': key})
        ser += [{'crl_id': oid, 'serial_dec': s} for s in serials]
        if first:
            pres.append(presence_row(oid, first, last, presence_shift))
    W.write_table(base, RIR, 'crls', crl)
    W.write_table(base, RIR, 'crl_serials', ser)
    pres += [presence_row(800000 + i, c, c, presence_shift) for i, c in enumerate(CYCLES)
             if not RELISTED[0] <= c <= RELISTED[1]]                               # one new object per cycle
    W.write_table(base, RIR, 'object_presence_stats', pres)
    W.snapshots(base, RIR, CYCLES, file_count={NEAR_EMPTY: 10})
    W.write_table(base, OTHER, 'object_presence_stats', [presence_row(900001, '2026-10-24 09:00:01', LAST, presence_shift)])
    W.snapshots(base, OTHER, CYCLES)


@pytest.fixture(scope='module')
def world(tmp_path_factory):
    d = tmp_path_factory.mktemp('mft')
    base, root = str(d / 'wh'), str(d / 'root')
    build_world(base)
    ledger.run_ledger(root, base, cut='2026-10-26 05:00:00', rirs=(RIR, OTHER), min_age_s=0)
    return base, root


def run(world, as_of=AS_OF, wf=W1, wt=W2, metric_ids=m_mft.METRIC_IDS):
    base, root = world
    ctx = W.make_ctx(root, base, as_of, list(metric_ids), rirs=(RIR,))
    ctx.call('m_mft', RIR, wf, wt)
    return ctx, {(r['week'], r['population'], r['operator'], r['metric_id'], r['stat']): r for r in ctx.fetch('rows')}


def vals(rows, week, pop, metric, op=''):
    """{stat: (value, n, n_ca, top1_share)} of one row group, values rounded to 6 decimals."""
    r6 = (lambda x: None if x is None else round(x, 6))
    return {k[4]: (r6(v['value']), v['n'], v['n_ca'], r6(v['top1_share'])) for k, v in rows.items()
            if k[0] == week and k[1] == pop and k[2] == op and k[3] == metric}


def rev(n, n_ca, top1, revoked, revoked_ca, always, never):
    r6 = (lambda x: round(x, 6))
    return {'early_replacements': (n, n, n_ca, r6(top1)), 'revoked': (r6(revoked), n, n_ca, r6(top1)),
            'revoked_ca': (r6(revoked_ca), n, n_ca, r6(top1)), 'ca_always': (r6(always), n, n_ca, r6(top1)),
            'ca_never': (r6(never), n, n_ca, r6(top1))}


def late(kind, n, n_ca, top1, share, share_ca, headroom):
    r6 = (lambda x: None if x is None else round(x, 6))
    t = r6(top1)
    return {f'replacements_{kind}': (n, n, n_ca, t), f'late_{kind}': (r6(share), n, n_ca, t),
            f'late_{kind}_ca': (r6(share_ca), n, n_ca, t), f'headroom_{kind}_p50_h': (r6(headroom), n, n_ca, t)}


def merge(*ds):
    out = {}
    for d in ds:
        out.update(d)
    return out


# ── registry ──────────────────────────────────────────────────────────────────
def test_registry_entries():
    for m in m_mft.METRIC_IDS:
        d = defs.METRICS[m]
        assert d['module'] == 'm_mft' and d['available_from'] == defs.ERA2 == '2026-04-20'
        assert d['kind'] == 'sample' and d['classified_by'] == 'ca_host' and d['windows'] == ('1w',)
        # 'no ledger gap in between' uses the ledger's own gap: more than 3 h without a good cycle
        assert defs.params(m)['gap_hours'] == ledger.GAP_HOURS
    assert defs.params('mft_ee_revocation')['evidence_days'] == [-1, 2]
    assert set(defs.METRICS['mft_ee_revocation']['stats']) == set(m_mft.REVOCATION_STATS)
    assert set(defs.METRICS['late_reissue']['stats']) == {v.format(k=k) for v in m_mft.LATE_STATS.values()
                                                          for k in ('mft', 'crl')}
    assert defs.rule_for('late_reissue', 'late_crl') == defs.REPLACEMENT_RULE
    assert defs.rule_for('mft_ee_revocation', 'early_replacements') == defs.REPLACEMENT_RULE
    r = defs.rule_for('mft_ee_revocation', 'revoked')
    # the evidence window ends 2 days after the successor: final once that is observed (W+2), masked by a
    # ledger gap up to 2 days after the week
    assert (r['publish_after'], r['final_after'], r['mask'], r['lookahead_days']) == (1, 2, 'gap', 2)
    for s in ('revoked_ca', 'ca_always', 'ca_never'):
        assert defs.rule_for('mft_ee_revocation', s) is r


# ── values of the synthetic world ─────────────────────────────────────────────
def test_mft_ee_revocation_values(world):
    """Early, directly observed manifest replacements of W2 (successor thisUpdate 10-19..10-25):
    ka 4 (3 revoked), kb 1 (0), kc 1 (1), kf 1 (0), kg 1 (0), kh 1 (0), ki 1 (1), kl 1 (0), km 1 (0), kn 1 (0)
    = 13 pairs, 5 revoked, 10 keys; per-key shares .75, 0, 1, 0, 0, 0, 1, 0, 0, 0 (kk's replacement is not
    observed)."""
    _, rows = run(world)
    assert vals(rows, W2, 'all', 'mft_ee_revocation') == rev(13, 10, 4 / 13, 5 / 13, 2.75 / 10, 2 / 10, 7 / 10)
    assert vals(rows, W2, 'rir_hosted', 'mft_ee_revocation') == rev(10, 7, 4 / 10, 4 / 10, 1.75 / 7, 1 / 7, 5 / 7)
    assert vals(rows, W2, 'nir_hosted', 'mft_ee_revocation') == rev(2, 2, 1 / 2, 1 / 2, 1 / 2, 1 / 2, 1 / 2)
    assert vals(rows, W2, 'nir_hosted', 'mft_ee_revocation', 'JPNIC') == rev(1, 1, 1, 0, 0, 0, 1)
    assert vals(rows, W2, 'nir_hosted', 'mft_ee_revocation', 'CNNIC') == rev(1, 1, 1, 1, 1, 1, 0)   # EE-host fallback
    assert vals(rows, W2, 'unknown', 'mft_ee_revocation') == rev(1, 1, 1, 0, 0, 0, 1)
    assert vals(rows, W2, 'delegated', 'mft_ee_revocation') == {}           # kd has no early replacement
    # W1: ke E2 -> E3 only (E1 -> E2 and kj J1 -> J2 span the outage, E3 -> E4 skips a cycle, E4 -> E5 overlaps)
    assert vals(rows, W1, 'all', 'mft_ee_revocation') == rev(1, 1, 1, 1, 1, 1, 0)
    assert vals(rows, W1, 'rir_hosted', 'mft_ee_revocation') == rev(1, 1, 1, 1, 1, 1, 0)
    assert {k[1] for k in rows if k[0] == W1} == {'all', 'rir_hosted'}


def test_late_reissue_values(world):
    """W2 manifests: ka 4 (18.25, 18, 18, 18 h), kb 1 (12.1), kc 1 (11.667; C1 -> C2 not observed), kd 2 (D1 -> D2
    late, -2649.75 h; D2 -> D3 equal, 0), kf 1 (0.167), kg 1 (18.167), kh 1 (17.917), ki 1 (17.917), kl 1 (15.083),
    km 1 (17.917), kn 1 (46.167) = 15, 1 late, 11 keys (kk's K1 -> K2, late if it counted, is not observed);
    quantile_disc median = the 8th of 15 sorted values = 17.917. CRLs: ka 98 -> 99 -> 100 -> 101 -> 102 (42, 42,
    -6 late, 41.983) and kd DC1 -> DC2 (late, -2649.75) = 5, 2 late, 2 keys; median 41.983."""
    _, rows = run(world)
    mft_all = late('mft', 15, 11, 4 / 15, 1 / 15, 1 / 11, 17 + 55 / 60)
    crl_all = late('crl', 5, 2, 4 / 5, 2 / 5, 1.0, 41 + 59 / 60)
    assert vals(rows, W2, 'all', 'late_reissue') == merge(mft_all, crl_all)
    assert vals(rows, W2, 'rir_hosted', 'late_reissue') == merge(late('mft', 10, 7, 4 / 10, 0, 0, 18.0),
                                                                 late('crl', 4, 1, 1, 1 / 4, 1, 41 + 59 / 60))
    assert vals(rows, W2, 'delegated', 'late_reissue') == merge(late('mft', 2, 1, 1, 1 / 2, 1, -2649.75),
                                                                late('crl', 1, 1, 1, 1, 1, -2649.75))
    assert vals(rows, W2, 'nir_hosted', 'late_reissue') == late('mft', 2, 2, 1 / 2, 0, 0, 11 + 40 / 60)
    assert vals(rows, W2, 'nir_hosted', 'late_reissue', 'JPNIC') == late('mft', 1, 1, 1, 0, 0, 12.1)
    assert vals(rows, W2, 'nir_hosted', 'late_reissue', 'CNNIC') == late('mft', 1, 1, 1, 0, 0, 11 + 40 / 60)
    assert vals(rows, W2, 'unknown', 'late_reissue') == late('mft', 1, 1, 1, 0, 0, 10 / 60)
    # W1: one manifest replacement (E2 -> E3, 55 min of headroom) and one CRL replacement (97 -> 98, 24 h)
    w1 = merge(late('mft', 1, 1, 1, 0, 0, 55 / 60), late('crl', 1, 1, 1, 0, 0, 24.0))
    assert vals(rows, W1, 'all', 'late_reissue') == w1 and vals(rows, W1, 'rir_hosted', 'late_reissue') == w1


def test_as_of_bounds_successors_and_evidence(world):
    """With as_of 10-26 01:00:01, F1 -> F2 (F2 first seen at 01:00:01) is observed (early, not revoked, 10 min of
    headroom) and h1's CRL of 00:50 is evidence: 14 early replacements, 6 revoked; per-key shares ka .75, kb 0,
    kc 1, kf 0 (0 of 2), kg 0, kh 1, ki 1, kl 0, km 0, kn 0; 16 manifest replacements, median the 8th of 16 =
    17.917 h."""
    _, rows = run(world, as_of='2026-10-26 01:00:01')
    assert vals(rows, W2, 'all', 'mft_ee_revocation') == rev(14, 10, 4 / 14, 6 / 14, 3.75 / 10, 3 / 10, 6 / 10)
    assert vals(rows, W2, 'all', 'late_reissue') == merge(late('mft', 16, 11, 4 / 16, 1 / 16, 1 / 11, 17 + 55 / 60),
                                                          late('crl', 5, 2, 4 / 5, 2 / 5, 1.0, 41 + 59 / 60))


def test_status_masks_the_outage_week(world):
    """W1 holds the outage (gap days 10-14 and 10-15): every row is masked, also the zero CRL row. W2 has no gap
    day (the near-empty cycle leaves 40 minutes without a good cycle): its rows have n < 30 -> insufficient."""
    ctx, _ = run(world)
    con = common.connect(threads=1, memory_limit='1GB', root=world[1])
    try:
        con.execute(f"CREATE TEMP TABLE r AS SELECT * FROM read_parquet({common.files_sql(ctx.emitted('rows'))})")
        ctx.load_ledger(con)
        status.annotations_table(con, assemble.ANNOTATIONS_PATH)
        status.apply(con, 'r', 'out', AS_OF, 1, 'test')
        got = {(w, m, s): (st, fl) for w, m, s, st, fl in con.execute(
            "SELECT week, metric_id, stat, status, flags FROM out WHERE population = 'all'").fetchall()}
    finally:
        con.close()
    w1 = {k: v for k, v in got.items() if k[0] == W1}
    assert len(w1) == 13 and {v[0] for v in w1.values()} == {'masked'}
    assert all('ledger:gap' in v[1].split(';') for v in w1.values())
    assert {v[0] for k, v in got.items() if k[0] == W2} == {'insufficient'}


def status_rows(root, rows, as_of):
    con = common.connect(threads=1, memory_limit='1GB', root=root)
    try:
        common.values_table(con, 'r', [(c, t) for c, t in metrics.ROW_COLS], rows)
        con.execute(f"CREATE TEMP TABLE ledger AS SELECT * FROM read_parquet('{root}/ledger/ledger.parquet')")
        status.annotations_table(con, assemble.ANNOTATIONS_PATH)
        status.apply(con, 'r', 'out', as_of, 1, 'test')
        return dict(con.execute("SELECT stat, status FROM out").fetchall())
    finally:
        con.close()


@pytest.mark.parametrize('hole, as_of, want', [
    # W+1: the count and late re-issue are final, the revocation shares wait for their evidence window
    (None, '2026-10-26 00:40:01', {'early_replacements': 'final', 'revoked': 'provisional', 'ca_never': 'provisional',
                                   'late_mft': 'final'}),
    (None, '2026-11-02 00:40:01', {'early_replacements': 'final', 'revoked': 'final', 'ca_never': 'final',
                                   'late_mft': 'final'}),
    # a ledger gap on the Tuesday after the week (W + 8 d) lies in the 2-day evidence window only
    ('2026-10-27', '2026-11-02 00:40:01', {'early_replacements': 'final', 'revoked': 'masked', 'ca_never': 'masked',
                                           'late_mft': 'final'}),
    # a gap in the week masks everything
    ('2026-10-21', '2026-11-02 00:40:01', {'early_replacements': 'masked', 'revoked': 'masked', 'ca_never': 'masked',
                                           'late_mft': 'masked'}),
])
def test_status_rules(tmp_path, hole, as_of, want):
    """Rows of W2 with n = 100 and 20 keys (not insufficient) under the registry's rules: count and late re-issue
    final at W+1; the revocation shares final at W+2 and masked by a ledger gap up to W + 9 d."""
    root = str(tmp_path / 'root')
    cyc = [c for c in W.cycles('2026-10-12', '2026-11-03') if not (hole and c.startswith(hole) and '06:00' <= c[11:] < '12:00')]
    con = common.connect(threads=1, memory_limit='1GB', root=root)
    try:
        ledger.build(con, {RIR: [(c, 'rpki-client', 1000, True, True, i) for i, c in enumerate(cyc)]}, {},
                     common.parse_ts('2026-11-03 05:00:00'), common.tmp_dir(root))
        ledger.write(con, root, common.parse_ts('2026-11-03 05:00:00'), {}, (RIR,))
    finally:
        con.close()
    rows = [(W2, '1w', RIR, 'all', '', 'late_reissue' if s == 'late_mft' else 'mft_ee_revocation', s, 0.5, 100, 20, 0.1)
            for s in want]
    assert status_rows(root, rows, as_of) == want


def test_only_requested_metrics_and_era2_only(world):
    _, rows = run(world, metric_ids=('late_reissue',))
    assert {k[3] for k in rows} == {'late_reissue'}
    # weeks before 2026-04-20 (era 1, transition) have no rows; the first era-2 week has its zero rows for 'all'
    _, rows = run(world, wf=D(2026, 4, 6), wt=D(2026, 4, 13))
    assert rows == {}
    _, rows = run(world, wf=D(2026, 4, 13), wt=D(2026, 4, 20))
    assert {(k[0], k[1]) for k in rows} == {(D(2026, 4, 20), 'all')}
    assert len(rows) == 13 and {v['n'] for v in rows.values()} == {0}
    assert {k[4]: v['value'] for k, v in rows.items() if v['value'] is not None} == {
        'early_replacements': 0, 'replacements_mft': 0, 'replacements_crl': 0}


def test_presence_numbering_must_match_the_ledger(tmp_path):
    """The presence build and the ledger number snapshots alike; presence stamps 20 minutes off stop the module."""
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    build_world(base, presence_shift=dt.timedelta(minutes=20))
    ledger.run_ledger(root, base, cut='2026-10-26 05:00:00', rirs=(RIR, OTHER), min_age_s=0)
    with pytest.raises(RuntimeError, match='rebuild the ledger'):
        run((base, root))


def test_week_values_do_not_depend_on_the_range(world):
    """W2 computed alone and together with W1 (a weekly run and a backfill): the same rows. A pairing over all
    objects of the call would put km's old 50 and 51 (issued 10-10, in a W1..W2 call's look-back) between the
    new ones and lose N1 -> N2."""
    def w2(rows):
        return {k: v for k, v in rows.items() if k[0] == W2}
    _, alone = run(world, wf=W2, wt=W2)
    _, together = run(world, wf=W1, wt=W2)
    assert w2(alone) == w2(together) and len(w2(alone)) > 26


def test_rows_are_deterministic(world):
    """Two runs on the same inputs and as_of emit the same rows (the output files are written sorted)."""
    a, _ = run(world)
    b, _ = run(world)
    con = duckdb.connect()
    q = "SELECT * FROM read_parquet({}) ORDER BY ALL"
    assert con.execute(q.format(common.files_sql(a.emitted('rows')))).fetchall() == \
        con.execute(q.format(common.files_sql(b.emitted('rows')))).fetchall()
