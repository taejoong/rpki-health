"""Re-sign pair metrics of the weekly scorecard (pipelines/scorecard/scs/m_pairs.py): roa_ee_revocation and
resign_consumed on synthetic warehouses, through Ctx.call as the framework calls the module. Expected
values are worked out by hand in the comments: the pair rule (order, objects without presence, the 30-day
chain break), early vs late replacement, revocation inside and outside the 7- and 35-day horizons, the
1-day look-back, serials scoped by key, no_evidence, key change, kept vs withdrawn,
pair- and CA-weighted shares, populations, consumed and interval quantiles, the consumed and notAfter
filters, consumed > 1, rolling 13- and 52-week windows, the daily-instant (transient) rule in era 2 and
at the archive's end, the *_incl_transient statistics, the histogram, chunked aggregation and reruns.
"""
import datetime as dt
import os
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scorecard_world as W  # noqa: E402
from scs import assemble, common, defs, ledger, m_pairs, metrics, status  # noqa: E402

D = dt.date
HOSTED, PAAS, KRILL = 'rpki.arin.net', 'rpki-rps.arin.net', 'krill.example.net'
REV, CONS = 'roa_ee_revocation', 'resign_consumed'


def plus(ts, **kw):
    return (dt.datetime.fromisoformat(ts) + dt.timedelta(**kw)).strftime('%Y-%m-%d %H:%M:%S')


class Roas:
    """Rows of the tables behind re-sign pairs: ROA objects with their signer EE certificate and presence,
    CRLs with their serials. Times are UTC 'YYYY-MM-DD HH:MM:SS'."""

    def __init__(self):
        self.t = {k: [] for k in ('roa_objects', 'object_signatures', 'certs/is_ca=false', 'object_presence_stats',
                                  'object_presence_runs', 'crls', 'crl_serials')}
        self.cert_id, self.crl_id = 90000, 20000

    def roa(self, oid, h, st, key, na=None, nb=None, days=90, host=HOSTED, serial=None, fs=None, ls='2026-10-28 00:00:01',
            runs=None, presence=True, signer=True):
        """One ROA object: signing time st, EE validity [nb (= st), na (= nb + days)], signer EE of key `key`
        published on `host` (no object_signatures row when not `signer`); presence [fs (= st + 20 min), ls]
        in one run unless `runs` is given."""
        nb = nb or st
        na = na or plus(nb, days=days)
        serial = serial or str(oid)
        self.cert_id += 1
        self.t['roa_objects'].append({'object_id': oid, 'object_hash': f'o{oid}', 'semantic_hash': h, 'asn': 64500,
                                      'ee_serial': serial, 'ee_not_before': nb, 'ee_not_after': na, 'signing_time': st})
        if signer:
            self.t['object_signatures'].append({'object_id': oid, 'object_type': 'ROA', 'signer_cert_id': self.cert_id})
        self.t['certs/is_ca=false'].append({'cert_id': self.cert_id, 'is_ca_flag': False, 'ski': f'ee{oid}', 'aki': key,
                                            'serial_dec': serial, 'not_before': nb, 'not_after': na,
                                            'sia': f'rsync://{host}/repo/{key}/{oid}.roa,https://{host}/rrdp/notification.xml'})
        if presence:
            fs = fs or plus(st, minutes=20)
            self.t['object_presence_stats'].append({'object_id': oid, 'first_ts': fs, 'last_ts': ls})
            for s, e in (runs if runs is not None else [(fs, ls)]):
                self.t['object_presence_runs'].append({'object_id': oid, 'start_ts': s, 'end_ts': e})

    def crl(self, key, tu, serials=()):
        self.crl_id += 1
        self.t['crls'].append({'crl_id': self.crl_id, 'object_id': self.crl_id, 'aki': key, 'this_update': tu,
                               'next_update': plus(tu, days=2), 'revoked_count': len(serials)})
        for s in serials:
            self.t['crl_serials'].append({'crl_id': self.crl_id, 'serial_dec': s, 'revocation_date': tu})

    def write(self, base, rir='arin', skip=()):
        for table, rows in self.t.items():
            if table not in skip:
                W.write_table(base, rir, table, rows)


def run(tmp_path, world, metric_ids, wf, wt, rir='arin', name='w', skip=(), as_of='2026-11-30 00:40:01'):
    base = str(tmp_path / f'{name}-wh')
    if not os.path.isdir(base):
        world.write(base, rir, skip)
    ctx = W.make_ctx(str(tmp_path / f'{name}-root'), base, as_of, metric_ids, rirs=(rir,))
    ctx.call('m_pairs', rir, wf, wt)
    return ctx


def table(ctx, kind='rows'):
    key = ('week', 'window', 'population', 'operator', 'metric_id', 'stat') + (('bin_lo',) if kind == 'hist' else ())
    out = {}
    for r in ctx.fetch(kind):
        k = tuple(r[c] for c in key)
        assert k not in out
        out[k] = r
    return out


def vnc(rows, week, stat, pop='all', window='1w', metric=REV, op=''):
    """(value, n, n_ca, top1_share) of one row, values rounded to 10 decimals as emit() stores them."""
    r = rows[(week, window, pop, op, metric, stat)]
    return r['value'], r['n'], r['n_ca'], r['top1_share']


# ── roa_ee_revocation ────────────────────────────────────────────────────────
def revocation_world():
    """ARIN. Keys K1, K2, K4 on rpki.arin.net (rir_hosted), K3 on krill.example.net (delegated), K5 on
    rpki-rps.arin.net (rir_paas, operator ARIN). Pairs of week 2026-09-14 (successor signing -> pair):
      A  h1 K1  A1 notAfter 09-29 -> A2 09-15 10:00: early; CRL 09-16 lists A1 -> revoked within 7 d; withdrawn
      B  h2 K1  B1 notAfter 09-29 -> B2 09-15 12:00: early; first listed 09-30 (15 d) -> revoked within 35 d only
      C  h3 K2  C1 notAfter 10-08 -> C2 09-16 08:00: early; K2 has no CRL in [09-15 08:00, 09-23 08:00]
                (CRLs 09-10, 09-24, 09-26) -> no_evidence_h7; listed 09-26 -> revoked within 35 d; kept (C1 seen
                until 10-08)
      D  h4 K1 -> K4 (key change) D1 -> D2 09-16 06:00: early, evidence (K1 CRL 09-16), never listed by K1 (K3's
                CRL of 09-20 lists the same serial: serials count only on the predecessor key's CRLs)
      E  h5 K1  E1 notAfter 09-22 00:00 -> E2 09-15 00:00: exactly 7 days left, not early (pairs only)
      F  h6 K3  F1 notAfter 2027-08-01 -> F2 09-17 00:00: listed only on K3's CRL of 09-16 23:00, 1 h after the
                1-day look-back starts -> revoked within 7 d; kept (seen until 10-20)
      G  h7 K3  G1 -> G2 09-18 12:00: listed only on 09-16 23:00, before the look-back (09-17 12:00), not on
                09-20 -> evidence, not revoked; withdrawn
      I  h9 K1  I1 last seen 08-16 00:20:01, I2 first seen 09-15 00:20:01: exactly 30 days -> a pair; I1
                notAfter 09-16 -> not early
      J  h10 K1 J1 notAfter 09-23 14:00 -> (J2 has no presence: not in the chain) -> J3 09-16 02:00: 7.5 days
                left, early; evidence, not revoked
      L  h11 K5 L1 -> L2 09-17 00:00: early, listed 09-17 12:00 -> revoked within 7 d (rir_paas, ARIN)
    h8: H1 last seen 07-01, H2 first seen 09-15 (> 30 days later): the chain breaks, no pair.
    Week 2026-09-21: M h12 K2 M1 -> M2 09-22 04:00, early, listed 09-24 -> revoked. Week 2026-09-28: nothing."""
    w = Roas()
    w.roa(101, 'h1', '2026-07-01 00:00:00', 'K1', serial='1001', ls='2026-09-15 12:00:01')
    w.roa(102, 'h1', '2026-09-15 10:00:00', 'K1')
    w.roa(111, 'h2', '2026-07-01 00:00:00', 'K1', serial='1002', ls='2026-09-16 00:00:01')
    w.roa(112, 'h2', '2026-09-15 12:00:00', 'K1')
    w.roa(121, 'h3', '2026-07-10 00:00:00', 'K2', serial='2001', ls='2026-10-08 00:00:01')
    w.roa(122, 'h3', '2026-09-16 08:00:00', 'K2')
    w.roa(131, 'h4', '2026-07-01 00:00:00', 'K1', serial='1003', ls='2026-09-16 08:00:01')
    w.roa(132, 'h4', '2026-09-16 06:00:00', 'K4')
    w.roa(141, 'h5', '2026-06-22 00:00:00', 'K1', na='2026-09-22 00:00:00', serial='1004', ls='2026-09-16 00:00:01')
    w.roa(142, 'h5', '2026-09-15 00:00:00', 'K1')
    w.roa(151, 'h6', '2026-08-01 00:00:00', 'K3', na='2027-08-01 00:00:00', host=KRILL, serial='3001',
          ls='2026-10-20 00:00:01')
    w.roa(152, 'h6', '2026-09-17 00:00:00', 'K3', host=KRILL)
    w.roa(161, 'h7', '2026-08-01 00:00:00', 'K3', na='2027-08-01 00:00:00', host=KRILL, serial='3002',
          ls='2026-09-19 00:00:01')
    w.roa(162, 'h7', '2026-09-18 12:00:00', 'K3', host=KRILL)
    w.roa(171, 'h8', '2026-06-01 00:00:00', 'K1', ls='2026-07-01 00:00:01')
    w.roa(172, 'h8', '2026-09-15 00:00:00', 'K1')
    w.roa(181, 'h9', '2026-06-18 00:00:00', 'K1', ls='2026-08-16 00:20:01')
    w.roa(182, 'h9', '2026-09-15 00:00:00', 'K1', fs='2026-09-15 00:20:01')
    w.roa(191, 'h10', '2026-07-01 00:00:00', 'K1', na='2026-09-23 14:00:00', serial='1005', ls='2026-09-16 02:00:01')
    w.roa(192, 'h10', '2026-08-01 00:00:00', 'K1', presence=False)
    w.roa(193, 'h10', '2026-09-16 02:00:00', 'K1')
    w.roa(201, 'h11', '2026-08-01 00:00:00', 'K5', na='2027-08-01 00:00:00', host=PAAS, serial='5001',
          ls='2026-09-17 00:00:01')
    w.roa(202, 'h11', '2026-09-17 00:00:00', 'K5', host=PAAS)
    w.roa(211, 'h12', '2026-07-15 00:00:00', 'K2', serial='2002', ls='2026-09-22 06:00:01')
    w.roa(212, 'h12', '2026-09-22 04:00:00', 'K2')
    w.crl('K1', '2026-09-14 00:00:00')
    w.crl('K1', '2026-09-16 00:00:00', ['1001'])
    w.crl('K1', '2026-09-30 00:00:00', ['1001', '1002'])
    w.crl('K2', '2026-09-10 00:00:00')
    w.crl('K2', '2026-09-24 00:00:00', ['2002'])
    w.crl('K2', '2026-09-26 00:00:00', ['2001', '2002'])
    w.crl('K3', '2026-09-16 23:00:00', ['3001', '3002'])
    w.crl('K3', '2026-09-20 00:00:00', ['1003'])
    w.crl('K5', '2026-09-17 12:00:00', ['5001'])
    return w


@pytest.fixture(scope='module')
def rev(tmp_path_factory):
    # only roa_ee_revocation: the module must not need object_presence_runs for it
    ctx = run(tmp_path_factory.mktemp('rev'), revocation_world(), [REV], '2026-09-14', '2026-09-28',
              skip=('object_presence_runs',))
    return table(ctx)


def test_revocation_pairs_early_and_concentration(rev):
    w = D(2026, 9, 14)
    # pairs A B C D E F G I J L: K1 holds A B D E I J (6 of 10); keys K1 K2 K3 K5
    assert vnc(rev, w, 'pairs') == (10.0, 10, 4, 0.6)
    # E (exactly 7 days left: 'more than 7' is strict) and I (notAfter 09-16) are not early; K1 holds A B D J of 8
    assert vnc(rev, w, 'early_pairs') == (8.0, 8, 4, 0.5)
    assert vnc(rev, w, 'pairs', 'rir_hosted') == (7.0, 7, 2, round(6 / 7, 10))      # A B C D E I J
    assert vnc(rev, w, 'pairs', 'delegated') == (2.0, 2, 1, 1.0)                    # F G
    assert vnc(rev, w, 'pairs', 'rir_paas') == (1.0, 1, 1, 1.0)                     # L
    assert vnc(rev, w, 'pairs', 'rir_paas', op='ARIN') == (1.0, 1, 1, 1.0)
    assert ('2026-09-14', '1w', 'unknown', '', REV, 'pairs') not in rev


def test_revocation_within_7_days(rev):
    w = D(2026, 9, 14)
    # evidence within 7 d: A B D F G J L (C has none); revoked: A F L
    assert vnc(rev, w, 'revoked_h7') == (round(3 / 7, 10), 7, 3, round(4 / 7, 10))
    # CA-weighted: K1 1/4 (A of A B D J), K3 1/2 (F of F G), K5 1/1
    assert vnc(rev, w, 'revoked_h7_ca')[0] == round((1 / 4 + 1 / 2 + 1) / 3, 10)
    assert vnc(rev, w, 'no_evidence_h7') == (0.125, 8, 4, 0.5)
    # strata: same key A B F G J L (A F L revoked), key change D
    assert vnc(rev, w, 'revoked_h7_samekey') == (0.5, 6, 3, 0.5)
    assert vnc(rev, w, 'revoked_h7_keychange') == (0.0, 1, 1, 1.0)
    # kept F (revoked); withdrawn A B D G J L (A L revoked); K1 holds A B D J of the 6
    assert vnc(rev, w, 'revoked_h7_kept') == (1.0, 1, 1, 1.0)
    assert vnc(rev, w, 'revoked_h7_withdrawn') == (round(2 / 6, 10), 6, 3, round(4 / 6, 10))
    # hosted: evidence A B D J, revoked A; no evidence C
    assert vnc(rev, w, 'revoked_h7', 'rir_hosted') == (0.25, 4, 1, 1.0)
    assert vnc(rev, w, 'no_evidence_h7', 'rir_hosted') == (0.2, 5, 2, 0.8)
    assert vnc(rev, w, 'revoked_h7', 'delegated') == (0.5, 2, 1, 1.0)


def test_revocation_within_35_days(rev):
    w = D(2026, 9, 14)
    # C has evidence (09-24) within 35 d; revoked: A B C F L of the 8
    assert vnc(rev, w, 'revoked_h35') == (0.625, 8, 4, 0.5)
    # K1 2/4 (A B), K2 1/1 (C), K3 1/2 (F), K5 1/1
    assert vnc(rev, w, 'revoked_h35_ca')[0] == 0.75
    assert vnc(rev, w, 'no_evidence_h35') == (0.0, 8, 4, 0.5)
    assert vnc(rev, w, 'revoked_h35_samekey') == (round(5 / 7, 10), 7, 4, round(3 / 7, 10))
    assert vnc(rev, w, 'revoked_h35_keychange') == (0.0, 1, 1, 1.0)
    assert vnc(rev, w, 'revoked_h35_kept') == (1.0, 2, 2, 0.5)                      # C F
    assert vnc(rev, w, 'revoked_h35_withdrawn') == (0.5, 6, 3, round(4 / 6, 10))    # A B L of A B D G J L


def test_revocation_every_week_has_all_rows(rev):
    stats = [s for s, *_ in m_pairs.revocation_stats([7, 35])]
    assert sorted(stats) == sorted(defs.METRICS[REV]['stats'])
    for week in (D(2026, 9, 14), D(2026, 9, 21), D(2026, 9, 28)):
        for s in stats:
            assert (week, '1w', 'all', '', REV, s) in rev, (week, s)
    assert vnc(rev, D(2026, 9, 21), 'revoked_h7') == (1.0, 1, 1, 1.0)               # M
    # an empty week: counts 0, shares NULL
    assert vnc(rev, D(2026, 9, 28), 'pairs') == (0.0, 0, 0, None)
    assert vnc(rev, D(2026, 9, 28), 'revoked_h7') == (None, 0, 0, None)
    assert not [k for k in rev if k[0] == D(2026, 9, 28) and k[2] != 'all']
    assert {k[1] for k in rev} == {'1w'}


# ── resign_consumed ──────────────────────────────────────────────────────────
def consumed_world():
    """ARIN, era 1 (archive presence, labelled 00:00) and era 2. Pairs (successor signing: consumed, interval):
    week 2025-03-03:
      P1 K1 hosted  pred 2025-01-01 / 90 d, succ 03-04: 62/90 = 0.68889, 62 d
      P2 K1 hosted  pred 2024-12-15 / 90 d, succ 03-05: 80/90 = 0.88889, 80 d
      P3 K2 hosted  pred 2024-12-01 / 90 d, succ 03-06: 95/90 = 1.05556 (> 1), 95 d
      P4 K2 hosted  pred 2025-02-01 / 30 d, succ 03-07: 34/30 = 1.13333 (> 1), 34 d
      P5 K3 krill   pred 2024-03-07 / 365 d, succ 03-06: 364/365 = 0.99726, 364 d
      out: Q1 94/31 = 3.03 (> 1.5), Q2 notAfter = notBefore, Q3 notBefore after the successor (< 0)
    week 2025-02-10: X K2 pred 2025-01-01 / 30 d, succ 02-15: 45/30 = 1.5 (kept; last histogram bin)
    week 2025-01-06: R K1 pred 2024-11-07 / 90 d, succ 01-07: 61/90 = 0.67778 (13w and 52w of 03-03)
    week 2024-10-14: S K3 pred 2023-10-15 / 365 d, succ 10-15: 366/365 = 1.00274 (52w of 03-03 only)
    week 2026-10-12 (era 2, cycles every 20 minutes):
      T1 pred present 10-12 10:00:01..11:00:01 (no 04:00): transient; succ 13:00: 3 h/2160 h = 0.0013889
      T2 pred present 03:40:01..04:20:01 (covers 04:00): kept; succ 05:00: 1.5 h/2160 h = 0.0006944
      T3 pred present 04:00:01..04:20:01 (starts just after 04:00): transient; succ 05:30: 0.0006944
      T4 pred runs 10-11 10:00:01..12:00:01 and 10-13 03:00:01..05:00:01 (covers 10-13 04:00): kept;
         succ 10-13 06:00: 44 h/2160 h = 0.0203704
    week 2026-04-13 (transition):
      T5 pred present only in the archive snapshot 2026-04-13 00:00:00: kept; 90.5/181 = 0.5
      T6 pred present 04-15 23:40:01..04-16 00:00:01 (a midnight after the archive's end, no 04:00):
         transient; succ 04-16 01:00: 1.5 h/2160 h = 0.0006944
      T7 pred first seen 2026-03-01 (era 1), no presence run read: kept; 103/181 = 0.56906"""
    w = Roas()
    era1 = dict(fs='2025-01-01 00:00:00', ls='2025-12-31 00:00:00')

    def pair(oid, h, pst, succ, key, days=90, host=HOSTED, nb=None, na=None, p=None, s=None):
        w.roa(oid, h, pst, key, nb=nb, na=na, days=days, host=host, **(p or era1))
        w.roa(oid + 1, h, succ, key, host=host, **(s or era1))
    pair(301, 'c1', '2025-01-01 00:00:00', '2025-03-04 00:00:00', 'K1')
    pair(311, 'c2', '2024-12-15 00:00:00', '2025-03-05 00:00:00', 'K1')
    pair(321, 'c3', '2024-12-01 00:00:00', '2025-03-06 00:00:00', 'K2')
    pair(331, 'c4', '2025-02-01 00:00:00', '2025-03-07 00:00:00', 'K2', days=30)
    pair(341, 'c5', '2024-03-07 00:00:00', '2025-03-06 00:00:00', 'K3', days=365, host=KRILL)
    pair(351, 'c6', '2024-12-01 00:00:00', '2025-03-05 00:00:00', 'K1', days=31)
    pair(361, 'c7', '2025-02-01 00:00:00', '2025-03-05 00:00:00', 'K1', days=0)
    pair(371, 'c8', '2025-02-01 00:00:00', '2025-03-05 00:00:00', 'K1', nb='2025-03-10 00:00:00')
    pair(381, 'c9', '2024-11-07 00:00:00', '2025-01-07 00:00:00', 'K1')
    pair(391, 'c10', '2023-10-15 00:00:00', '2024-10-15 00:00:00', 'K3', days=365, host=KRILL,
         p=dict(fs='2023-10-15 00:00:00', ls='2024-10-20 00:00:00'), s=dict(fs='2024-10-16 00:00:00', ls='2025-12-31 00:00:00'))
    pair(401, 'c11', '2025-01-01 00:00:00', '2025-02-15 00:00:00', 'K2', days=30)
    e2 = dict(ls='2026-10-28 00:00:01')
    pair(501, 't1', '2026-10-12 10:00:00', '2026-10-12 13:00:00', 'K1',
         p=dict(fs='2026-10-12 10:00:01', ls='2026-10-12 11:00:01'), s=e2)
    pair(511, 't2', '2026-10-12 03:30:00', '2026-10-12 05:00:00', 'K1',
         p=dict(fs='2026-10-12 03:40:01', ls='2026-10-12 04:20:01'), s=e2)
    pair(521, 't3', '2026-10-12 04:00:00', '2026-10-12 05:30:00', 'K1',
         p=dict(fs='2026-10-12 04:00:01', ls='2026-10-12 04:20:01'), s=e2)
    pair(531, 't4', '2026-10-11 10:00:00', '2026-10-13 06:00:00', 'K2',
         p=dict(fs='2026-10-11 10:00:01', ls='2026-10-13 05:00:01',
                runs=[('2026-10-11 10:00:01', '2026-10-11 12:00:01'), ('2026-10-13 03:00:01', '2026-10-13 05:00:01')]),
         s=e2)
    pair(541, 't5', '2026-01-13 00:00:00', '2026-04-13 12:00:00', 'K1', days=181,
         p=dict(fs='2026-04-13 00:00:00', ls='2026-04-13 00:00:00'), s=e2)
    pair(551, 't6', '2026-04-15 23:30:00', '2026-04-16 01:00:00', 'K1',
         p=dict(fs='2026-04-15 23:40:01', ls='2026-04-16 00:00:01'), s=e2)
    pair(561, 't7', '2026-01-01 00:00:00', '2026-04-14 00:00:00', 'K2', days=181,
         p=dict(fs='2026-03-01 00:00:00', ls='2026-04-14 00:00:00', runs=[]), s=e2)
    return w


@pytest.fixture(scope='module')
def cons(tmp_path_factory):
    d = tmp_path_factory.mktemp('cons')
    w = consumed_world()
    out = {}
    for wf, wt in (('2025-02-10', '2025-03-10'), ('2026-04-13', '2026-04-13'), ('2026-10-12', '2026-10-12')):
        ctx = run(d, w, [CONS], wf, wt)
        out.update(table(ctx))
        out.update({('hist',) + k: v for k, v in table(ctx, 'hist').items()})
    return out


def cvnc(rows, week, stat, pop='all', window='1w'):
    return vnc(rows, week, stat, pop, window, metric=CONS)


def test_consumed_one_week(cons):
    w = D(2025, 3, 3)
    # P1..P5: 0.68889 0.88889 0.99726 1.05556 1.13333; K1 and K2 hold 2 each
    assert cvnc(cons, w, 'pairs') == (5.0, 5, 3, 0.4)
    assert cvnc(cons, w, 'consumed_p50')[0] == round(364 / 365, 10)
    assert cvnc(cons, w, 'consumed_p10')[0] == round(62 / 90 + 0.4 * (80 / 90 - 62 / 90), 10)       # 0.76889
    assert cvnc(cons, w, 'consumed_p90')[0] == round(95 / 90 + 0.6 * (34 / 30 - 95 / 90), 10)       # 1.10222
    # per-key medians K1 (62+80)/180 = 0.78889, K2 (95/90 + 34/30)/2 = 1.09444, K3 0.99726 -> 0.99726
    assert cvnc(cons, w, 'consumed_p50_ca')[0] == round(364 / 365, 10)
    assert cvnc(cons, w, 'share_ge075')[0] == 0.8
    assert cvnc(cons, w, 'share_gt100')[0] == 0.4                                                  # P3 P4
    # intervals 34 62 80 95 364 days
    assert [cvnc(cons, w, f'interval_p{q}_d')[0] for q in (10, 50, 90)] == [45.2, 80.0, 256.4]
    # hosted: P1..P4 -> median (80/90 + 95/90) / 2
    assert cvnc(cons, w, 'consumed_p50', 'rir_hosted') == (round(175 / 180, 10), 4, 2, 0.5)
    assert cvnc(cons, w, 'pairs', 'delegated') == (1.0, 1, 1, 1.0)
    # era 1: no *_incl_transient statistics
    assert not [k for k in cons if k[0] == w and k[-1].endswith('_incl_transient')]


def test_consumed_rolling_windows(cons):
    w = D(2025, 3, 3)
    # 13w (2024-12-09..2025-03-03): P1..P5, X 1.5, R 0.67778 -> 7 values, median 0.99726;
    # K1 R P1 P2 (median 0.68889), K2 P3 P4 X (1.13333), K3 P5 -> CA median 0.99726
    assert cvnc(cons, w, 'pairs', window='13w') == (7.0, 7, 3, round(3 / 7, 10))
    assert cvnc(cons, w, 'consumed_p50', window='13w')[0] == round(364 / 365, 10)
    assert cvnc(cons, w, 'consumed_p50_ca', window='13w')[0] == round(364 / 365, 10)
    assert cvnc(cons, w, 'share_gt100', window='13w')[0] == round(3 / 7, 10)
    # 52w (2024-03-11..2025-03-03) adds S 1.00274: 8 values, median (0.99726 + 1.00274) / 2 = 1.0; per key
    # K3 (P5, S) median 1.0, K1 0.68889, K2 1.13333 -> 1.0
    assert cvnc(cons, w, 'pairs', window='52w') == (8.0, 8, 3, 0.375)
    assert cvnc(cons, w, 'consumed_p50', window='52w')[0] == 1.0
    assert cvnc(cons, w, 'consumed_p50_ca', window='52w')[0] == 1.0
    # a week without pairs of its own: 1w empty, 13w the same 7 pairs (2024-12-16..2025-03-10)
    w2 = D(2025, 3, 10)
    assert cvnc(cons, w2, 'pairs') == (0.0, 0, 0, None)
    assert cvnc(cons, w2, 'consumed_p50') == (None, 0, 0, None)
    assert cvnc(cons, w2, 'pairs', window='13w') == (7.0, 7, 3, round(3 / 7, 10))
    assert cvnc(cons, w2, 'pairs', 'rir_hosted', window='13w') == (6.0, 6, 2, 0.5)
    assert not [k for k in cons if k[0] == w2 and k[1] == '1w' and k[2] != 'all']


def test_consumed_bounds_and_histogram(cons):
    # X: consumed exactly 1.5 is kept, in the last bin [1.49, 1.5]
    assert cvnc(cons, D(2025, 2, 10), 'pairs') == (1.0, 1, 1, 1.0)
    h = {k[-1]: v['count'] for k, v in cons.items()
         if k[0] == 'hist' and k[1] == D(2025, 2, 10) and k[3] == 'all'}
    assert h == {1.49: 1}
    assert cons[('hist', D(2025, 2, 10), '1w', 'all', '', CONS, 'consumed', 1.49)]['bin_hi'] == 1.5
    h = {k[-1]: v['count'] for k, v in cons.items() if k[0] == 'hist' and k[1] == D(2025, 3, 3) and k[3] == 'all'}
    assert h == {0.68: 1, 0.88: 1, 0.99: 1, 1.05: 1, 1.13: 1}                       # Q1..Q3 left out
    assert sum(v['count'] for k, v in cons.items() if k[0] == 'hist' and k[1] == D(2025, 3, 3)
               and k[3] == 'rir_hosted') == 4


def test_consumed_transient_rule_era2(cons):
    w = D(2026, 10, 12)
    # kept: T2 (0.00069) and T4 (0.02037); transient: T1 (0.00139), T3 (0.00069)
    assert cvnc(cons, w, 'pairs') == (2.0, 2, 2, 0.5)
    assert cvnc(cons, w, 'consumed_p50')[0] == round((1.5 / 2160 + 44 / 2160) / 2, 10)
    assert cvnc(cons, w, 'pairs_incl_transient') == (4.0, 4, 2, 0.75)
    assert cvnc(cons, w, 'consumed_p50_incl_transient')[0] == round((1.5 / 2160 + 3 / 2160) / 2, 10)
    assert cvnc(cons, w, 'share_ge075_incl_transient')[0] == 0.0
    h = sum(v['count'] for k, v in cons.items() if k[0] == 'hist' and k[1] == w and k[3] == 'all')
    assert h == 2                                                                    # histogram: kept pairs only


def test_consumed_transient_rule_at_the_archive_end(cons):
    w = D(2026, 4, 13)
    # T5 (archive snapshot 04-13 00:00:00) and T7 (first seen in era 1) are kept, T6 is transient
    assert cvnc(cons, w, 'pairs') == (2.0, 2, 2, 0.5)
    assert cvnc(cons, w, 'consumed_p50')[0] == round((0.5 + 103 / 181) / 2, 10)
    assert cvnc(cons, w, 'pairs_incl_transient')[:2] == (3.0, 3)
    assert cvnc(cons, w, 'consumed_p50_incl_transient')[0] == 0.5


def test_daily_instant_sql():
    con = duckdb.connect()
    runs = [(1, '2026-10-12 03:40:01', '2026-10-12 04:20:01'),   # covers 04:00
            (2, '2026-10-12 04:00:01', '2026-10-12 23:40:01'),   # starts after 04:00, ends before the next one
            (3, '2026-10-12 04:00:01', '2026-10-13 04:00:01'),   # covers 10-13 04:00
            (4, '2026-04-14 00:00:00', '2026-04-14 00:00:00'),   # the last archive snapshot
            (5, '2026-04-14 23:40:01', '2026-04-15 00:20:01'),   # a midnight after the archive's end
            (6, '2026-04-13 22:00:01', '2026-04-14 01:00:01'),   # spans the archive label 2026-04-14 00:00:00
            (7, '2026-10-12 04:00:00', '2026-10-12 04:00:00')]   # exactly the instant
    con.execute("CREATE TABLE r (object_id INTEGER, s TIMESTAMP, e TIMESTAMP)")
    con.executemany("INSERT INTO r VALUES (?, ?, ?)", runs)
    p = defs.params(CONS)
    assert p['daily_instant_utc'] == '04:00' and p['archive_until'] == '2026-04-14'
    got = sorted(x for (x,) in con.execute(m_pairs.daily_sql('r', p)).fetchall())
    assert got == [1, 3, 4, 6, 7]


# ── input cut (as_of) ────────────────────────────────────────────────────────
A_CUT, A_LATE = '2026-10-05 00:40:01', '2026-11-30 00:40:01'


def as_of_world():
    """ARIN, key K1 on rpki.arin.net; week 2026-09-28, the newest week of a run with as_of A = 2026-10-05
    00:40:01. Predecessors signed 2026-08-01 00:00 with 90 days (notAfter 10-30: every pair is early) unless
    noted; K1's CRLs: 10-01 00:00 (q1), 10-04 12:00 (q1), 10-06 00:00 (q1 r1).
      Q  Q2 09-29 10:00: q1 listed 10-01: revoked; Q1 last seen 09-30 10:00:01, before Q2 first seen + 1 d:
         withdrawn
      R  R2 09-30 00:00: r1 listed only on 10-06, after A: revoked as of 11-30 only; kept
      S  S2 10-04 23:30, first seen 10-05 01:00:01, after A: no pair as of A; as of 11-30 early, evidence
         10-04 12:00, not revoked, kept
      T  T2 10-04 00:00 first seen 00:50:01; T1 last seen 10-05 01:00:01: kept as of 11-30, withdrawn as of A
         (T1's last seen is capped at A < T2 first seen + 1 d); evidence 10-04 12:00, not revoked
      U  U1 signed 10-04 22:50, present 10-04 23:00:01..10-05 05:00:01; U2 10-04 23:50 first seen 10-05
         00:00:01: U1 covers the daily instant 10-05 04:00 as of 11-30 only (its run is cut at A), so it is
         transient as of A; evidence 10-04 12:00, not revoked, withdrawn
      V  V1 has no object_signatures row: no key (no evidence) and no host (population unknown); V2 10-02
    consumed = (successor - notBefore) / 90 d: Q 59.4167/90, R 60/90, S 64.9792/90, T 64/90, V 62/90, U 1 h /
    2160 h."""
    w = Roas()
    for oid, h, succ, pred_kw, succ_kw in (
            (701, 'q', '2026-09-29 10:00:00', dict(ls='2026-09-30 10:00:01'), {}),
            (711, 'r', '2026-09-30 00:00:00', {}, {}),
            (721, 's', '2026-10-04 23:30:00', {}, dict(fs='2026-10-05 01:00:01')),
            (731, 't', '2026-10-04 00:00:00', dict(ls='2026-10-05 01:00:01'), dict(fs='2026-10-04 00:50:01')),
            (751, 'v', '2026-10-02 00:00:00', dict(signer=False), {})):
        w.roa(oid, h, '2026-08-01 00:00:00', 'K1', serial=h + '1', **pred_kw)
        w.roa(oid + 1, h, succ, 'K1', **succ_kw)
    w.roa(741, 'u', '2026-10-04 22:50:00', 'K1', serial='u1', fs='2026-10-04 23:00:01', ls='2026-10-05 05:00:01')
    w.roa(742, 'u', '2026-10-04 23:50:00', 'K1', fs='2026-10-05 00:00:01')
    w.crl('K1', '2026-10-01 00:00:00', ['q1'])
    w.crl('K1', '2026-10-04 12:00:00', ['q1'])
    w.crl('K1', '2026-10-06 00:00:00', ['q1', 'r1'])
    return w


@pytest.fixture(scope='module')
def cut(tmp_path_factory):
    d, w = tmp_path_factory.mktemp('cut'), as_of_world()
    return {a: table(run(d, w, [REV, CONS], '2026-09-28', '2026-09-28', as_of=a)) for a in (A_CUT, A_LATE)}


def test_as_of_cuts_evidence_and_presence(cut):
    w, a, b = D(2026, 9, 28), cut[A_CUT], cut[A_LATE]
    # as of A: pairs Q R T U V (S2 not seen yet); V has no key, so K1 holds 4 of the 5
    assert vnc(a, w, 'pairs') == (5.0, 5, 1, 0.8)
    assert vnc(a, w, 'no_evidence_h7') == (0.2, 5, 1, 0.8)                         # V
    # evidence Q R T U; revoked Q only (R's listing CRL of 10-06 is after A), also within 35 d
    assert vnc(a, w, 'revoked_h7') == (0.25, 4, 1, 1.0)
    assert vnc(a, w, 'revoked_h35') == (0.25, 4, 1, 1.0)
    assert vnc(a, w, 'revoked_h7_kept') == (0.0, 1, 1, 1.0)                        # R
    assert vnc(a, w, 'revoked_h7_withdrawn') == (round(1 / 3, 10), 3, 1, 1.0)      # Q T U
    assert vnc(a, w, 'revoked_h7_keychange') == (None, 0, 0, None)                 # V (keyless) has no evidence
    assert vnc(a, w, 'pairs', 'unknown') == (1.0, 1, 0, None)
    # as of 11-30: S is a pair, R is revoked (10-06), T is kept
    assert vnc(b, w, 'pairs') == (6.0, 6, 1, round(5 / 6, 10))
    assert vnc(b, w, 'revoked_h7') == (0.4, 5, 1, 1.0)
    assert vnc(b, w, 'revoked_h7_kept') == (round(1 / 3, 10), 3, 1, 1.0)          # R S T
    assert vnc(b, w, 'revoked_h7_withdrawn') == (0.5, 2, 1, 1.0)                   # Q U
    # consumed: U is transient as of A only
    assert cvnc(a, w, 'pairs') == (4.0, 4, 1, 0.75)                                # Q R T V
    assert cvnc(a, w, 'consumed_p50')[0] == round(61 / 90, 10)                     # (60/90 + 62/90) / 2
    assert cvnc(a, w, 'pairs_incl_transient') == (5.0, 5, 1, 0.8)
    assert cvnc(a, w, 'consumed_p50_incl_transient')[0] == round(60 / 90, 10)
    assert cvnc(a, w, 'pairs', 'unknown') == (1.0, 1, 0, None)
    assert cvnc(b, w, 'pairs') == (6.0, 6, 1, round(5 / 6, 10))
    assert cvnc(b, w, 'consumed_p50')[0] == round(61 / 90, 10)                     # U 0.0005 Q R | V T S


def test_week_values_do_not_depend_on_the_range(tmp_path, rev, cons):
    """A week computed alone (a weekly run) and inside a longer range (a backfill) gets the same rows."""
    alone = table(run(tmp_path, revocation_world(), [REV], '2026-09-21', '2026-09-21', name='r',
                      skip=('object_presence_runs',)))
    assert alone == {k: v for k, v in rev.items() if k[0] == D(2026, 9, 21)} and len(alone) > 30
    alone = table(run(tmp_path, consumed_world(), [CONS], '2025-03-03', '2025-03-03', name='c'))
    assert alone == {k: v for k, v in cons.items() if k[0] == D(2025, 3, 3)} and len(alone) > 60


# ── status (scs/status.py with the registry's rules) ─────────────────────────
def test_registry_rules():
    """The 35-day horizon stats W+2 / W+6, every other revocation stat W+2 / W+2, both masked by a
    ledger gap in the week or the look-ahead; re-sign cadence W+1 / W+2, masked by a gap in the week or the
    7 days after it."""
    for s in defs.METRICS[REV]['stats']:
        r = defs.rule_for(REV, s)
        assert (r['publish_after'], r['final_after'], r['mask'], r['lookahead_days']) == (
            (2, 6, 'gap', 35) if '_h35' in s else (2, 2, 'gap', 7)), s
    for s in defs.METRICS[CONS]['stats']:
        assert defs.rule_for(CONS, s) == defs.LATE_ARRIVAL_RULE, s


@pytest.mark.parametrize('hole, as_of, want', [
    # W+1: the cadence is published (provisional), the revocation rows are not
    (None, '2026-09-21 00:40:01', {(CONS, 'consumed_p50'): 'provisional'}),
    # W+2: the cadence and the 7-day horizon are final, the 35-day horizon waits until W+6
    (None, '2026-09-28 00:40:01', {(CONS, 'consumed_p50'): 'final', (REV, 'pairs'): 'final',
                                   (REV, 'revoked_h7'): 'final', (REV, 'revoked_h35'): 'provisional',
                                   (REV, 'no_evidence_h35'): 'provisional'}),
    (None, '2026-10-26 00:40:01', {(CONS, 'consumed_p50'): 'final', (REV, 'pairs'): 'final',
                                   (REV, 'revoked_h7'): 'final', (REV, 'revoked_h35'): 'final',
                                   (REV, 'no_evidence_h35'): 'final'}),
    # a ledger gap on W + 11 d is in every look-ahead; one on W + 28 d only in the 35-day one
    ('2026-09-25', '2026-10-26 00:40:01', {(CONS, 'consumed_p50'): 'masked', (REV, 'pairs'): 'masked',
                                           (REV, 'revoked_h7'): 'masked', (REV, 'revoked_h35'): 'masked',
                                           (REV, 'no_evidence_h35'): 'masked'}),
    ('2026-10-12', '2026-10-26 00:40:01', {(CONS, 'consumed_p50'): 'final', (REV, 'pairs'): 'final',
                                           (REV, 'revoked_h7'): 'final', (REV, 'revoked_h35'): 'masked',
                                           (REV, 'no_evidence_h35'): 'masked'}),
])
def test_status_rules(tmp_path, hole, as_of, want):
    """Rows of the week 2026-09-14 (n = 100, 20 keys: not insufficient) through status.apply with a ledger of
    20-minute cycles from 09-07 to 10-27, less 6 hours of the `hole` day (a gap day: > 3 h without a cycle)."""
    root, rir = str(tmp_path / 'root'), 'arin'
    cyc = [c for c in W.cycles('2026-09-07', '2026-10-27') if not (hole and c.startswith(hole) and '06:00' <= c[11:] < '12:00')]
    stats = [(CONS, 'consumed_p50'), (REV, 'pairs'), (REV, 'revoked_h7'), (REV, 'revoked_h35'), (REV, 'no_evidence_h35')]
    con = common.connect(threads=1, memory_limit='1GB', root=root)
    try:
        cut_ts = common.parse_ts('2026-10-27 05:00:00')
        ledger.build(con, {rir: [(c, 'rpki-client', 1000, True, True, i) for i, c in enumerate(cyc)]}, {}, cut_ts,
                     common.tmp_dir(root))
        common.values_table(con, 'r', list(metrics.ROW_COLS),
                            [(D(2026, 9, 14), '1w', rir, 'all', '', m, s, 0.5, 100, 20, 0.1) for m, s in stats])
        status.annotations_table(con, assemble.ANNOTATIONS_PATH)
        status.apply(con, 'r', 'out', as_of, 1, 'test')
        got = {(m, s): x for m, s, x in con.execute('SELECT metric_id, stat, status FROM out').fetchall()}
    finally:
        con.close()
    assert got == want


# ── module behaviour ─────────────────────────────────────────────────────────
def test_both_metrics_share_one_pass_and_chunks_do_not_matter(tmp_path, monkeypatch):
    w = consumed_world()
    a = table(run(tmp_path, w, [REV, CONS], '2025-02-10', '2025-03-10', name='a'))
    assert {k[4] for k in a} == {REV, CONS}
    monkeypatch.setattr(m_pairs, 'ROW_BUDGET', 1)                 # one output week per chunk
    b = table(run(tmp_path, w, [REV, CONS], '2025-02-10', '2025-03-10', name='a'))
    assert a == b
    c = table(run(tmp_path, w, [CONS], '2025-02-10', '2025-03-10', name='a'))
    assert c == {k: v for k, v in a.items() if k[4] == CONS}


def test_rows_pass_the_interface_and_reruns_are_identical(tmp_path):
    w = revocation_world()
    one = run(tmp_path, w, [REV, CONS], '2026-09-14', '2026-09-28', name='r')
    two = run(tmp_path, w, [REV, CONS], '2026-09-14', '2026-09-28', name='r')
    assert one.fetch('rows') == two.fetch('rows') and one.fetch('hist') == two.fetch('hist')
    rows = one.fetch('rows')
    for r in rows:
        assert defs.stat_allowed(r['metric_id'], r['stat']) and r['window'] in defs.METRICS[r['metric_id']]['windows']
        assert r['n'] >= 0 and r['n_ca'] >= 0 and (r['top1_share'] is None) == (r['n'] == 0 or r['n_ca'] == 0)
    # every (week, window, stat) of both metrics has a row of population 'all'
    for m in (REV, CONS):
        for wk in (D(2026, 9, 14), D(2026, 9, 21), D(2026, 9, 28)):
            for win in defs.METRICS[m]['windows']:
                got = {r['stat'] for r in rows if (r['metric_id'], r['week'], r['window'], r['population']) == (m, wk, win, 'all')}
                assert got == set(defs.METRICS[m]['stats']), (m, wk, win)


def test_chain_break_days_must_agree(tmp_path, monkeypatch):
    p = dict(defs.METRICS[CONS]['params'], chain_break_days=60)
    monkeypatch.setitem(defs.METRICS[CONS], 'params', p)
    with pytest.raises(ValueError, match='chain_break_days'):
        run(tmp_path, revocation_world(), [REV, CONS], '2026-09-14', '2026-09-14', name='x')
