"""Scorecard metrics of scs/m_ca.py (ca_cert_lifetime, live_inventory) and scs/m_health.py (rp_rejected)
on a synthetic APNIC warehouse (plus an ARIN one that holds a few head rows), with the expected values
worked out by hand in the comments.

The week under test is W1 = 2026-10-12 (W0 = 2026-10-05); as_of 2026-10-19 00:40:01. Durations of the
newly issued certificates are whole days (not_before 00:00 or 06:00 or 12:00, not_after the same time).
quantile_disc(q) of n sorted values is the value at 1-based position ceil(n q) (DuckDB 1.4).

CA keys (ski) and their certificates in warehouse-apnic (object id, issuer key aki, own SIA host):
  parents   ta (1, root, rpki.apnic.net), ap (2, aki ta, rpki.apnic.net), jp (3, aki ap, JPNIC host),
            id (4: repo-rpki.idnic.net 2025-06; 5: rpki-rsync.idnic.net 2026-06, the newest), dg (6, aki ap,
            krill.dg.example); warehouse-arin: rps (7, rpki-rps.arin.net: a listed ARIN host, so
            'delegated' in APNIC's warehouse); zz has no certificate (issuer class 'unknown')
  W1 (2026-10-12..18), issuer class of the parent:
    rir_hosted (aki ap)  C1 k1 10-12 (+400 d, replaced in the week by) C1b k1 10-14 -> 2027-12-10 (422 d),
                         C2 k2 10-13 -> 2027-12-10 (423), C3 k3 10-15 -> 2027-07-01 (259),
                         C4 k4 10-16 -> 2027-07-01 (258), C5 k5 10-17 12:00 -> 11-10 12:00 (24)
    nir_hosted           J1 j1 (aki jp) 10-12 06:00 +365, J2 j2 (aki jp) 10-13 06:00 +365,
                         I1 i1 (aki id) 10-12 +364
    delegated            D1 d1 (aki dg) 10-12 +364, D2 d2 (aki rps) 10-13 +364
    unknown              U1 u1 (aki zz) 10-14 +364 (no rsync URI in its SIA: own host unknown)
    left out             X1 root, X2 aki = ski, X3 not_after = not_before, X4 is_ca_flag false,
                         X5 first seen after as_of, X6 never seen, X7 head rows in warehouse-arin
  earlier: E1 k1 10-06 +365 (W0), E5 k9 (aki dg) 10-07 +5000 (W0), E2 k6 2026-08-03 +300 (in W1's 13w
  window), E3 k7 2026-01-05 +500 (52w), E4 k8 2025-10-06 +700 (53 weeks before W1: in no window)
The ledger: cycles every 20 minutes from 2026-09-28; on 10-18 the 23:00 cycle is partial (800 files of
1,000) and 23:20 / 23:40 are near-empty (100), so W1's instant is 10-18 22:40:01; W0's 10-11 23:40:01;
week 09-28's 10-04 23:40:01; week 09-21 has none.
"""
import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scorecard_world as W  # noqa: E402
from scs import assemble, common, defs, ledger, m_ca, metrics, status  # noqa: E402

D = dt.date
AS_OF = '2026-10-19 00:40:01'
W1, W0 = D(2026, 10, 12), D(2026, 10, 5)
END = '2026-10-18 23:40:01'          # last cycle of the synthetic presence


def ts(d, hm='00:00'):
    return f'{d} {hm}:00'


def plus(d, hm, days):
    return (dt.datetime.fromisoformat(f'{d} {hm}') + dt.timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')


def cert(oid, ski, aki, nb, na, host, flag=True, rsync=True):
    sia = (f'rsync://{host}/repo/{ski}/,https://{host}/notify.xml' if rsync else f'https://{host}/notify.xml')
    return {'cert_id': oid, 'object_id': oid, 'cert_hash': f'h{oid}', 'is_ca_flag': flag, 'ski': ski, 'aki': aki,
            'not_before': nb, 'not_after': na, 'sia': sia}


AP, JP_HOST, IDN, IDO = 'rpki.apnic.net', 'rpki-repository.nic.ad.jp', 'rpki-rsync.idnic.net', 'repo-rpki.idnic.net'
APNIC_CERTS = [
    cert(1, 'ta', None, ts('2020-01-01'), ts('2030-01-01'), AP),
    cert(2, 'ap', 'ta', ts('2026-01-01'), ts('2027-01-01'), AP),
    cert(3, 'jp', 'ap', ts('2026-01-01'), ts('2027-01-01'), JP_HOST),
    cert(4, 'id', 'ap', ts('2025-06-01'), ts('2026-06-01'), IDO),
    cert(5, 'id', 'ap', ts('2026-06-01'), ts('2027-06-01'), IDN),
    cert(6, 'dg', 'ap', ts('2026-01-01'), ts('2027-01-01'), 'krill.dg.example'),
    # W1
    cert(101, 'k1', 'ap', ts('2026-10-12'), plus('2026-10-12', '00:00', 400), AP),
    cert(102, 'k1', 'ap', ts('2026-10-14'), ts('2027-12-10'), AP),
    cert(103, 'k2', 'ap', ts('2026-10-13'), ts('2027-12-10'), AP),
    cert(104, 'k3', 'ap', ts('2026-10-15'), ts('2027-07-01'), AP),
    cert(105, 'k4', 'ap', ts('2026-10-16'), ts('2027-07-01'), AP),
    cert(106, 'k5', 'ap', ts('2026-10-17', '12:00'), ts('2026-11-10', '12:00'), AP),
    cert(107, 'k2', 'ap', ts('2025-08-01'), ts('2027-08-01'), 'repo-old.example'),      # C2old, still present
    cert(111, 'j1', 'jp', ts('2026-10-12', '06:00'), plus('2026-10-12', '06:00', 365), JP_HOST),
    cert(112, 'j2', 'jp', ts('2026-10-13', '06:00'), plus('2026-10-13', '06:00', 365), JP_HOST),
    cert(121, 'i1', 'id', ts('2026-10-12'), plus('2026-10-12', '00:00', 364), IDN),
    cert(131, 'd1', 'dg', ts('2026-10-12'), plus('2026-10-12', '00:00', 364), 'krill.d1.example'),
    cert(132, 'd2', 'rps', ts('2026-10-13'), plus('2026-10-13', '00:00', 364), 'rpki-rps.arin.net'),
    cert(141, 'u1', 'zz', ts('2026-10-14'), plus('2026-10-14', '00:00', 364), 'u1.example', rsync=False),
    cert(151, 'x1', None, ts('2026-10-13'), ts('2027-10-13'), AP),
    cert(152, 'x2', 'x2', ts('2026-10-13'), ts('2027-10-13'), AP),
    cert(153, 'x3', 'ap', ts('2026-10-13'), ts('2026-10-13'), AP),
    cert(154, 'x4', 'ap', ts('2026-10-13'), ts('2027-10-13'), AP, flag=False),
    cert(155, 'x5', 'ap', ts('2026-10-15'), ts('2027-10-15'), AP),
    cert(156, 'x6', 'ap', ts('2026-10-15'), ts('2027-10-15'), AP),
    cert(157, 'x8', 'ap', ts('2025-10-01'), ts('2026-10-01'), AP),                         # expired, present
    cert(158, 'y1', 'ap', ts('2025-09-01'), ts('2027-09-01'), AP),                         # seen from 23:00 only
    # earlier weeks
    cert(201, 'k1', 'ap', ts('2026-10-06'), plus('2026-10-06', '00:00', 365), AP),        # E1
    cert(202, 'k6', 'ap', ts('2026-08-03'), plus('2026-08-03', '00:00', 300), AP),        # E2
    cert(203, 'k7', 'ap', ts('2026-01-05'), plus('2026-01-05', '00:00', 500), AP),        # E3
    cert(204, 'k8', 'ap', ts('2025-10-06'), plus('2025-10-06', '00:00', 700), AP),        # E4
    cert(205, 'k9', 'dg', ts('2026-10-07'), plus('2026-10-07', '00:00', 5000), 'krill.k9.example'),  # E5
]
ARIN_CERTS = [
    cert(7, 'rps', 'arin-ta', ts('2026-01-01'), ts('2027-01-01'), 'rpki-rps.arin.net'),
    cert(171, 'xa', 'ap', ts('2026-10-12'), ts('2027-10-12'), 'rsync.paas.rpki.ripe.net'),     # X7
]
ALL_TIME = ('2026-09-28 00:00:01', END)
RUNS = {1: ALL_TIME, 2: ALL_TIME, 3: ALL_TIME,
        101: ('2026-10-12 00:20:01', '2026-10-14 00:00:01'), 102: ('2026-10-14 00:20:01', END),
        103: ('2026-10-13 00:20:01', END), 104: ('2026-10-15 00:20:01', END), 105: ('2026-10-16 00:20:01', END),
        106: ('2026-10-17 12:20:01', END), 107: ALL_TIME,
        111: ('2026-10-12 06:20:01', END), 112: ('2026-10-13 06:20:01', END), 121: ('2026-10-12 00:20:01', END),
        131: ('2026-10-12 00:20:01', END), 132: ('2026-10-13 00:20:01', END), 141: ('2026-10-14 00:20:01', END),
        151: ('2026-10-13 00:20:01', END), 152: ('2026-10-13 00:20:01', END), 153: ('2026-10-13 00:20:01', END),
        154: ('2026-10-13 00:20:01', END), 155: ('2026-10-19 01:00:01', '2026-10-19 01:00:01'),
        157: ALL_TIME, 158: ('2026-10-18 23:00:01', END),
        201: ('2026-10-06 00:20:01', '2026-10-12 00:00:01'), 202: ('2026-09-28 00:00:01', '2026-10-01 00:00:01'),
        203: ('2026-09-28 00:00:01', '2026-09-30 00:00:01'), 204: ('2026-09-28 00:00:01', '2026-09-29 00:00:01'),
        205: ('2026-10-07 00:20:01', END), 171: ('2026-10-12 00:20:01', END),
        # ROAs (R6 = 306 has its head rows in warehouse-arin) and a manifest
        301: ('2026-10-01 00:00:01', END), 302: ('2026-10-01 00:00:01', END), 303: ('2026-10-01 00:00:01', END),
        304: ('2026-10-01 00:00:01', END), 305: ('2026-10-01 00:00:01', END), 306: ('2026-10-12 00:20:01', END),
        307: ('2026-10-01 00:00:01', '2026-10-15 00:00:01'), 308: ALL_TIME, 313: ('2026-10-01 00:00:01', END)}
# signed objects: object id -> (type, signer EE cert id, EE aki, EE host); 313's EE certificate is missing
APNIC_SIGNED = {301: ('ROA', 9301, 'k1', AP), 302: ('ROA', 9302, 'k1', AP), 303: ('ROA', 9303, 'k2', AP),
                304: ('ROA', 9304, 'j1', JP_HOST), 305: ('ROA', 9305, 'd1', 'krill.d1.example'),
                307: ('ROA', 9307, 'k3', AP), 308: ('MFT', 9308, 'k1', AP),
                309: ('MFT', 9309, 'zz9', 'krill.zz9.example'), 310: ('MFT', 9310, 'i1', 'krill.old.example'),
                311: ('ASPA', 9311, 'k2', AP), 313: ('ROA', 9313, None, None)}
ARIN_SIGNED = {306: ('ROA', 9306, 'd2', 'rpki-rps.arin.net')}


def write_signed(base, rir, signed):
    W.write_table(base, rir, 'object_signatures', [
        {'object_id': o, 'object_type': t, 'signer_cert_id': c} for o, (t, c, _, _) in signed.items()])
    W.write_table(base, rir, 'certs/is_ca=false', [
        {'cert_id': c, 'object_id': None, 'is_ca_flag': False, 'ski': f'ee{o}', 'aki': aki,
         'not_before': ts('2026-10-01'), 'not_after': ts('2027-10-01'), 'sia': f'rsync://{h}/repo/{o}.x'}
        for o, (_, c, aki, h) in signed.items() if aki])


def verdict(cycle, oid, reason, state=2):
    return {'dataset_ts': cycle, 'object_id': oid, 'state': state, 'reason_code': reason}


VERDICTS = [
    verdict('2026-06-16 10:00:01', 307, 'expired'),                                    # before 06-22: out
    verdict('2026-10-06 10:00:01', 307, 'expired'), verdict('2026-10-07 10:00:01', 308, 'expired'),     # W0
    verdict('2026-10-12 00:20:01', 307, 'expired'), verdict('2026-10-12 00:40:01', 307, 'expired'),
    verdict('2026-10-12 00:40:01', 307, 'expired'),                                    # a repeated row
    verdict('2026-10-13 10:00:01', 304, 'not-yet-valid'), verdict('2026-10-14 10:00:01', 304, 'expired'),
    verdict('2026-10-13 10:00:01', 308, 'expired'), verdict('2026-10-13 10:00:01', 309, 'expired'),
    verdict('2026-10-13 10:00:01', 310, 'expired'), verdict('2026-10-15 10:00:01', 157, 'expired'),
    verdict('2026-10-15 10:00:01', 171, 'expired'), verdict('2026-10-15 10:00:01', 306, 'resource-violation'),
    verdict('2026-10-16 10:00:01', 999, 'expired'), verdict('2026-10-16 10:00:01', 311, 'invalid'),
    verdict('2026-10-16 10:00:01', 320, 'expired'), verdict('2026-10-16 10:00:01', 312, 'expired', state=1),
    verdict('2026-10-19 00:20:01', 308, 'expired'),                                    # week 10-19: out
]


def build_world(base):
    W.write_table(base, 'apnic', 'certs/is_ca=true', APNIC_CERTS)
    W.write_table(base, 'arin', 'certs/is_ca=true', ARIN_CERTS)
    W.write_table(base, 'apnic', 'object_presence_runs', [
        {'object_id': o, 'start_ts': a, 'end_ts': b} for o, (a, b) in sorted(RUNS.items())])
    W.write_table(base, 'arin', 'object_presence_runs', [{'object_id': 171, 'start_ts': AS_OF, 'end_ts': AS_OF}])
    write_signed(base, 'apnic', APNIC_SIGNED)
    write_signed(base, 'arin', ARIN_SIGNED)
    W.write_table(base, 'apnic', 'crls', [{'crl_id': 320, 'object_id': 320, 'aki': 'k4', 'revoked_count': 0,
                                           'this_update': ts('2026-10-16'), 'next_update': ts('2026-10-17')}])
    W.write_table(base, 'apnic', 'object_validity', VERDICTS)


def write_ledger(root, special=None, verdict_cycles=()):
    """cycles.parquet / ledger.parquet for apnic: every 20 minutes from 2026-09-28 to 10-18, file counts
    1,000 except `special` {ts: file_count}; verdict rows in `verdict_cycles`."""
    special = {'2026-10-18 23:00:01': 800, '2026-10-18 23:20:01': 100, '2026-10-18 23:40:01': 100} \
        if special is None else special
    cyc = W.cycles('2026-09-28', '2026-10-18')
    snaps = {'apnic': [(c, 'rpki-client', special.get(c, 1000), True, True, i) for i, c in enumerate(cyc)]}
    con = common.connect(threads=2, memory_limit='1GB', root=root)
    try:
        ledger.build(con, snaps, {'apnic': {c: 1 for c in verdict_cycles}}, common.parse_ts(AS_OF),
                     common.tmp_dir(root))
        ledger.write(con, root, common.parse_ts(AS_OF), {}, ('apnic',))
    finally:
        con.close()


@pytest.fixture(scope='module')
def world(tmp_path_factory):
    """Rows and histogram of m_ca and m_health for apnic, weeks 2026-09-21..10-12 (m_health from 06-15)."""
    tmp = tmp_path_factory.mktemp('ca')
    base, root = str(tmp / 'wh'), str(tmp / 'root')
    build_world(base)
    write_ledger(root)
    ctx = W.make_ctx(root, base, AS_OF, ['ca_cert_lifetime', 'live_inventory', 'rp_rejected'], rirs=('apnic',))
    ctx.call('m_ca', 'apnic', '2026-09-21', '2026-10-12')
    ctx.call('m_health', 'apnic', '2026-06-15', '2026-10-12')
    rows = {(r['metric_id'], r['week'], r['window'], r['population'], r['operator'], r['stat']): r
            for r in ctx.fetch('rows')}
    hist = {(r['week'], r['population'], r['operator'], r['bin_lo']): (r['bin_hi'], r['count'])
            for r in ctx.fetch('hist')}
    return {'rows': rows, 'hist': hist, 'ctx': ctx, 'root': root, 'base': base}


def val(world, metric, week, window, pop, stat, op=''):
    r = world['rows'].get((metric, week, window, pop, op, stat))
    return r and (r['value'], r['n'], r['n_ca'], r['top1_share'])


def approx(x):
    return pytest.approx(x, abs=1e-9)


# ── ca_cert_lifetime ─────────────────────────────────────────────────────────
def test_lifetime_week_by_issuer_class(world):
    """W1, 1w. rir_hosted after the dedup (C1b replaces C1, both k1): durations 24, 258, 259, 422, 423:
    p10 = position ceil(0.5) = 1 -> 24, p50 = ceil(2.5) = 3 -> 259, p90 = ceil(4.5) = 5 -> 423; notAfter
    2027-12-10 x2 and 2027-07-01 x2 tie -> the earlier date, share 2/5; 5 keys, top1 1/5.
    nir_hosted: 364 (I1), 365, 365 (J1, J2); its operators JPNIC (365, 365) and IDNIC (364); delegated
    D1, D2 (the RPS parent is a listed ARIN host: delegated in APNIC's warehouse); unknown U1.
    all = 11 certificates: 24, 258, 259, 364 x4, 365 x2, 422, 423 -> p10 pos 2 = 258, p50 pos 6 = 364,
    p90 pos 10 = 422; six notAfter dates twice each -> 2027-07-01, 2/11."""
    m = 'ca_cert_lifetime'
    g = lambda pop, stat, op='': val(world, m, W1, '1w', pop, stat, op)  # noqa: E731
    assert g('rir_hosted', 'certs') == (5, 5, 5, approx(0.2))
    assert [g('rir_hosted', s)[0] for s in ('duration_p10_d', 'duration_p50_d', 'duration_p90_d')] == [24, 259, 423]
    assert g('rir_hosted', 'modal_not_after')[0] == 20270701 and g('rir_hosted', 'modal_not_after_share')[0] == 0.4
    assert g('nir_hosted', 'certs') == (3, 3, 3, approx(1 / 3))
    assert [g('nir_hosted', s)[0] for s in ('duration_p10_d', 'duration_p50_d', 'duration_p90_d')] == [364, 365, 365]
    assert g('nir_hosted', 'modal_not_after')[0] == 20271011
    assert g('nir_hosted', 'modal_not_after_share')[0] == approx(1 / 3)
    assert g('nir_hosted', 'certs', 'JPNIC')[:3] == (2, 2, 2) and g('nir_hosted', 'duration_p50_d', 'JPNIC')[0] == 365
    assert (g('nir_hosted', 'modal_not_after', 'JPNIC')[0], g('nir_hosted', 'modal_not_after_share', 'JPNIC')[0]) == \
        (20271012, 0.5)
    assert g('nir_hosted', 'certs', 'IDNIC')[:3] == (1, 1, 1) and g('nir_hosted', 'duration_p90_d', 'IDNIC')[0] == 364
    assert g('delegated', 'certs')[:3] == (2, 2, 2) and g('delegated', 'duration_p50_d')[0] == 364
    assert g('unknown', 'certs')[:3] == (1, 1, 1) and g('unknown', 'modal_not_after')[0] == 20271013
    assert g('all', 'certs') == (11, 11, 11, approx(1 / 11))
    assert [g('all', s)[0] for s in ('duration_p10_d', 'duration_p50_d', 'duration_p90_d')] == [258, 364, 422]
    assert g('all', 'modal_not_after')[0] == 20270701 and g('all', 'modal_not_after_share')[0] == approx(2 / 11)
    # no rir_paas certificate, and no operator rows outside nir_hosted / rir_paas
    pops = {(k[3], k[4]) for k in world['rows'] if k[0] == m and k[1] == W1 and k[2] == '1w'}
    assert pops == {('all', ''), ('rir_hosted', ''), ('nir_hosted', ''), ('nir_hosted', 'JPNIC'),
                    ('nir_hosted', 'IDNIC'), ('delegated', ''), ('unknown', '')}


def test_lifetime_rolling_windows_pool_weekly_dedups(world):
    """rir_hosted, W1. 13w (weeks 07-20..10-12): W1's five plus E1 (k1 again, 365, W0) and E2 (300,
    08-03): 24, 258, 259, 300, 365, 422, 423 -> p10 pos 1 = 24, p50 pos 4 = 300, p90 pos 7 = 423; 6 keys,
    k1 twice -> top1 2/7. 52w (from 2025-10-20) adds E3 (500, 2026-01-05) and the parents ap and jp
    (2026-01-01 -> 2027-01-01, 365 d; issued by ta / ap): 24, 258, 259, 300, 365 x3, 422, 423, 500 ->
    p50 pos 5 = 365, p90 pos 9 = 423; 9 keys, top1 2/10; notAfter 2027-01-01, -07-01 and -12-10 twice
    each -> 2027-01-01. E4 (2025-10-06) is in no window of W0 or W1. W0's own 1w holds E1 only; its 13w
    E1 + E2 (p50 pos 1 = 300, p90 pos 2 = 365); its 52w (from 2025-10-13) E1, E2, E3, ap, jp: 300, 365 x3,
    500."""
    m = 'ca_cert_lifetime'
    g = lambda w, win, stat, pop='rir_hosted': val(world, m, w, win, pop, stat)  # noqa: E731
    assert g(W1, '13w', 'certs') == (7, 7, 6, approx(2 / 7))
    assert [g(W1, '13w', s)[0] for s in ('duration_p10_d', 'duration_p50_d', 'duration_p90_d')] == [24, 300, 423]
    assert (g(W1, '13w', 'modal_not_after')[0], g(W1, '13w', 'modal_not_after_share')[0]) == (20270701, approx(2 / 7))
    assert g(W1, '52w', 'certs') == (10, 10, 9, 0.2)
    assert [g(W1, '52w', s)[0] for s in ('duration_p10_d', 'duration_p50_d', 'duration_p90_d')] == [24, 365, 423]
    assert (g(W1, '52w', 'modal_not_after')[0], g(W1, '52w', 'modal_not_after_share')[0]) == (20270101, 0.2)
    assert g(W0, '1w', 'certs') == (1, 1, 1, 1.0) and g(W0, '1w', 'modal_not_after')[0] == 20271006
    assert [g(W0, '13w', s)[0] for s in ('certs', 'duration_p50_d', 'duration_p90_d')] == [2, 300, 365]
    assert [g(W0, '52w', s)[0] for s in ('certs', 'duration_p10_d', 'duration_p50_d', 'duration_p90_d')] == \
        [5, 300, 365, 500]
    # delegated, W1 13w: D1, D2 and E5 (5,000 days, W0)
    assert [g(W1, '13w', s, 'delegated')[0] for s in ('certs', 'duration_p50_d', 'duration_p90_d')] == [3, 364, 5000]


def test_lifetime_frame_rows_and_histogram(world):
    """Weeks without certificates (09-21, 09-28) still have every 'all' 1w row: certs 0, the rest NULL;
    their 13w windows hold E2 (08-03); 09-28's 52w window (from 2025-10-06, its first week) E2, E3, ap, jp
    and E4 (700 days: p90 = position 5 of 5), 09-21's (from 2025-09-29) also X8 (2025-10-01; position 6 of
    6). The histogram is 1w only: W1 rir_hosted 24 -> [21, 28), 258 -> [252, 259), 259 -> [259, 266), 422
    and 423 -> [420, 427); E5's 5,000 days in W0 -> the overflow bin [4200, NULL)."""
    m = 'ca_cert_lifetime'
    for w, n52 in ((D(2026, 9, 21), 6), (D(2026, 9, 28), 5)):
        assert val(world, m, w, '1w', 'all', 'certs') == (0, 0, 0, None)
        assert val(world, m, w, '1w', 'all', 'duration_p50_d') == (None, 0, 0, None)
        assert val(world, m, w, '1w', 'all', 'modal_not_after') == (None, 0, 0, None)
        assert val(world, m, w, '1w', 'all', 'modal_not_after_share') == (None, 0, 0, None)
        assert val(world, m, w, '13w', 'all', 'certs')[:3] == (1, 1, 1)
        assert val(world, m, w, '52w', 'all', 'certs')[:3] == (n52, n52, n52)
        assert val(world, m, w, '52w', 'all', 'duration_p90_d')[0] == 700
    h = world['hist']
    hosted = {k[3]: v for k, v in h.items() if k[0] == W1 and k[1] == 'rir_hosted'}
    assert hosted == {21: (28, 1), 252: (259, 1), 259: (266, 1), 420: (427, 2)}
    assert h[(W0, 'delegated', '', 4200)] == (None, 1)
    assert sum(v[1] for k, v in h.items() if k[0] == W1 and k[1] == 'all') == 11
    assert not [k for k in h if k[0] < W0]                     # no certificates, no bins


def test_lifetime_end_live_certificates_by_issuer(world):
    """W1's instant is 10-18 22:40:01 (the last good, complete cycle). Present then, non-root, notAfter >
    notBefore: rir_hosted issuer: ap (its issuer ta is on rpki.apnic.net), jp, C1b, C2, C2old (k2 again),
    C3, C4, C5, X8 (expired 2026-10-01, still present) and X7 (head rows in warehouse-arin, present in
    APNIC's) = 10 certificates of 9 keys; nir_hosted J1, J2, I1; delegated D1, D2, E5; unknown U1: 17.
    Not live: C1 and E1 (replaced), Y1 (first seen in the partial 23:00 cycle), X1-X4 (root, self-signed,
    not_after = not_before, no CA flag). Expiring within 30 days (<= 11-17 22:40:01): C5 (11-10) and X8;
    within 90 days (<= 2027-01-16 22:40:01) also ap and jp (2027-01-01).
    W0 (10-11 23:40:01): ap, jp, C2old, X8, E1 (all rir_hosted issuer) and E5: 30 days X8, 90 days X8, ap,
    jp. Week 09-21 has no snapshot: live_certs NULL."""
    m = 'ca_cert_lifetime'
    g = lambda w, pop, stat, op='': val(world, m, w, 'end', pop, stat, op)  # noqa: E731
    assert g(W1, 'rir_hosted', 'live_certs') == (10, 10, 9, 0.2)
    assert g(W1, 'rir_hosted', 'live_expiring_30d')[0] == 0.2 and g(W1, 'rir_hosted', 'live_expiring_90d')[0] == 0.4
    assert g(W1, 'nir_hosted', 'live_certs')[:3] == (3, 3, 3) and g(W1, 'nir_hosted', 'live_expiring_90d')[0] == 0
    assert g(W1, 'nir_hosted', 'live_certs', 'JPNIC')[0] == 2 and g(W1, 'nir_hosted', 'live_certs', 'IDNIC')[0] == 1
    assert g(W1, 'delegated', 'live_certs')[:3] == (3, 3, 3) and g(W1, 'unknown', 'live_certs')[:3] == (1, 1, 1)
    assert g(W1, 'all', 'live_certs') == (17, 17, 16, approx(2 / 17))
    assert g(W1, 'all', 'live_expiring_30d')[0] == approx(2 / 17)
    assert g(W1, 'all', 'live_expiring_90d')[0] == approx(4 / 17)
    assert g(W0, 'all', 'live_certs')[:3] == (6, 6, 6) and g(W0, 'rir_hosted', 'live_certs')[0] == 5
    assert g(W0, 'all', 'live_expiring_30d')[0] == approx(1 / 6) and g(W0, 'all', 'live_expiring_90d')[0] == 0.5
    assert g(D(2026, 9, 21), 'all', 'live_certs') == (None, 0, 0, None)
    assert g(D(2026, 9, 21), 'all', 'live_expiring_30d') == (None, 0, 0, None)


# ── live_inventory ───────────────────────────────────────────────────────────
def test_inventory_ca_keys_by_own_host(world):
    """Live non-root keys at W1's instant by the own host of their newest present certificate: rir_hosted
    ap, k1, k2 (C2 is newer than C2old on repo-old.example), k3, k4, k5, x3 (not_after = not_before still
    counts as present), x8 = 8; nir_hosted jp, j1, j2 (JPNIC) and i1 (IDNIC) = 4; delegated d1, d2 (RPS:
    another RIR's host), k9, xa (PaaS of RIPE NCC; head rows in warehouse-arin) = 4; unknown u1 = 1 (no
    rsync URI): 17. W0: ap, x8, k1 (E1) rir_hosted; jp; k2 delegated (only C2old present then) and k9:
    6. Week 09-28: ap, jp, k2, x8 (E2 left on 10-01). 09-21: no snapshot."""
    m = 'live_inventory'
    g = lambda w, pop, op='': val(world, m, w, 'end', pop, 'ca_keys', op)  # noqa: E731
    assert g(W1, 'rir_hosted') == (8, 8, 8, approx(1 / 8))
    assert g(W1, 'nir_hosted')[:3] == (4, 4, 4) and g(W1, 'nir_hosted', 'JPNIC')[0] == 3
    assert g(W1, 'nir_hosted', 'IDNIC')[0] == 1
    assert g(W1, 'delegated')[0] == 4 and g(W1, 'unknown')[0] == 1 and g(W1, 'all') == (17, 17, 17, approx(1 / 17))
    assert (g(W0, 'all')[0], g(W0, 'rir_hosted')[0], g(W0, 'nir_hosted')[0], g(W0, 'delegated')[0]) == (6, 3, 1, 2)
    assert g(D(2026, 9, 28), 'all')[0] == 4
    assert g(D(2026, 9, 21), 'all') == (None, 0, 0, None)


def test_inventory_roas_by_signer_ee_host(world):
    """ROAs present at W1's instant: R1, R2 (k1), R3 (k2) on rpki.apnic.net; R4 (j1) JPNIC; R5 (d1) and R6
    (d2 on RPS; head rows in warehouse-arin) delegated; R8 without an EE certificate: unknown, no key.
    R7 left on 10-15; the manifest does not count. W0: R1-R5, R7 (k3, hosted), R8."""
    m = 'live_inventory'
    g = lambda w, pop, op='': val(world, m, w, 'end', pop, 'roas', op)  # noqa: E731
    assert g(W1, 'rir_hosted') == (3, 3, 2, approx(2 / 3))
    assert g(W1, 'nir_hosted', 'JPNIC')[:3] == (1, 1, 1)
    assert g(W1, 'delegated') == (2, 2, 2, 0.5)
    assert g(W1, 'unknown') == (1, 1, 0, None)
    assert g(W1, 'all') == (7, 7, 5, approx(2 / 7))
    assert g(W0, 'rir_hosted') == (4, 4, 3, 0.5) and g(W0, 'all')[:3] == (7, 7, 5)
    assert g(D(2026, 9, 21), 'all') == (None, 0, 0, None)


def test_instants_pick_the_last_good_complete_snapshot(tmp_path):
    """Week A (10-05): complete 20:00, 21:00 at 97% of its 7-day median (a slow decline the 12-hour median
    follows: not 'partial'), partial 22:00, near-empty 23:00 -> 20:00. Week B (10-12): only partial
    snapshots -> the last of them. Week C (10-19): no good snapshot -> no instant. Era 1 (week 2025-01-06):
    the daily tree of the last good day (Sunday's archive snapshot is not good, Saturday's counts). Nothing
    after as_of; another RIR's cycles do not count."""
    con = common.connect(threads=1, memory_limit='1GB', root=str(tmp_path))
    try:
        rows = [('apnic', '2026-10-11 20:00:01', True, False, 1000), ('apnic', '2026-10-11 21:00:01', True, False, 970),
                ('apnic', '2026-10-11 22:00:01', True, True, 1000), ('apnic', '2026-10-11 23:00:01', False, True, 1000),
                ('apnic', '2026-10-17 10:00:01', True, True, 1000), ('apnic', '2026-10-17 11:00:01', True, True, 1000),
                ('apnic', '2026-10-24 10:00:01', False, False, 1000),
                ('apnic', '2025-01-11 00:00:00', True, False, 1000), ('apnic', '2025-01-12 00:00:00', False, False, 1000),
                ('apnic', '2026-11-01 10:00:01', True, False, 1000), ('arin', '2026-10-11 23:40:01', True, False, 1000)]
        common.values_table(con, 'cyc0', [('rir', 'VARCHAR'), ('ts', 'TIMESTAMP'), ('good', 'BOOLEAN'),
                                          ('partial', 'BOOLEAN'), ('file_count', 'BIGINT')], rows)
        con.execute("CREATE TEMP TABLE cyc AS SELECT *, 1000.0 AS ref_long FROM cyc0")
        got = con.execute(f"""SELECT week, CAST(t AS VARCHAR) FROM ({m_ca.instants_sql('cyc', 'apnic', D(2025, 1, 6),
                              D(2026, 10, 26), '2026-10-31 00:40:01')}) ORDER BY week""").fetchall()
    finally:
        con.close()
    assert got == [(D(2025, 1, 6), '2025-01-11 00:00:00'), (D(2026, 10, 5), '2026-10-11 20:00:01'),
                   (D(2026, 10, 12), '2026-10-17 11:00:01')]


# ── rp_rejected ──────────────────────────────────────────────────────────────
def test_rp_rejected_week_by_type_reason_and_population(world):
    """W1, distinct objects (307 three times, 304 for two reasons, 312 has state 1 and is left out):
      307 ROA k3 hosted | 304 ROA j1 JPNIC (not-yet-valid, then expired) | 308 MFT k1 hosted (the CA's host)
      309 MFT zz9: no CA certificate -> its EE host krill.zz9.example: delegated
      310 MFT i1: CA host rpki-rsync.idnic.net (IDNIC) wins over its EE host krill.old.example
      157 CER x8 (own key, own host) hosted | 171 CER xa, head rows in warehouse-arin, PaaS host: delegated
      306 ROA d2, head rows (signature, EE) in warehouse-arin, RPS host: delegated
      999 in no head table: unknown, no key | 311 ASPA k2 hosted | 320 CRL k4 (crls.aki) hosted
    all: 11 objects of 10 keys; rir_hosted 5 (k3, k1, x8, k2, k4); nir_hosted 2; delegated 3; unknown 1."""
    m = 'rp_rejected'
    g = lambda pop, stat, op='': val(world, m, W1, '1w', pop, stat, op)  # noqa: E731
    assert g('all', 'objects') == (11, 11, 10, approx(1 / 11))
    assert g('all', 'ca_keys') == (10, 11, 10, approx(1 / 11))
    assert g('rir_hosted', 'objects') == (5, 5, 5, 0.2) and g('rir_hosted', 'ca_keys')[0] == 5
    assert g('nir_hosted', 'objects')[:3] == (2, 2, 2)
    assert g('nir_hosted', 'objects', 'JPNIC')[0] == 1 and g('nir_hosted', 'objects', 'IDNIC')[0] == 1
    assert g('delegated', 'objects')[:3] == (3, 3, 3)
    assert g('unknown', 'objects') == (1, 1, 0, None) and g('unknown', 'ca_keys') == (0, 1, 0, None)
    types = {k[5]: v['value'] for k, v in world['rows'].items()
             if k[0] == m and k[1] == W1 and k[3] == 'all' and k[5].startswith('objects_type_')}
    assert types == {'objects_type_roa': 3, 'objects_type_mft': 3, 'objects_type_cer': 2, 'objects_type_aspa': 1,
                     'objects_type_crl': 1, 'objects_type_unknown': 1}
    reasons = {k[5]: v['value'] for k, v in world['rows'].items()
               if k[0] == m and k[1] == W1 and k[3] == 'all' and k[5].startswith('objects_reason_')}
    assert reasons == {'objects_reason_expired': 9, 'objects_reason_not_yet_valid': 1,
                       'objects_reason_resource_violation': 1, 'objects_reason_invalid': 1}
    both = {k[5]: v['value'] for k, v in world['rows'].items() if k[0] == m and k[1] == W1 and k[3] == 'all'
            and not k[5].startswith(('objects_type_', 'objects_reason_')) and k[5] not in ('objects', 'ca_keys')}
    assert both == {'objects_roa_expired': 2, 'objects_roa_not_yet_valid': 1, 'objects_roa_resource_violation': 1,
                    'objects_mft_expired': 3, 'objects_cer_expired': 2, 'objects_aspa_invalid': 1,
                    'objects_crl_expired': 1, 'objects_unknown_expired': 1}
    assert g('rir_hosted', 'objects_type_mft')[:3] == (1, 1, 1) and g('delegated', 'objects_cer_expired')[0] == 1
    assert all(defs.stat_allowed(m, s) for s in list(types) + list(reasons) + list(both))


def test_rp_rejected_weeks_from_available_and_zero_rows(world):
    """Weeks start at 2026-06-22 (the 06-16 verdict is out); W0 holds 307 and 308 (both hosted); weeks
    without verdicts have zero rows for all and rir_hosted only; the verdict of week 10-19 is out of
    range."""
    m = 'rp_rejected'
    weeks = sorted({k[1] for k in world['rows'] if k[0] == m})
    assert weeks == common.weeks('2026-06-22', '2026-10-12')
    assert val(world, m, W0, '1w', 'all', 'objects') == (2, 2, 2, 0.5)
    assert val(world, m, W0, '1w', 'rir_hosted', 'objects_mft_expired')[0] == 1
    quiet = D(2026, 9, 28)
    got = {(k[3], k[4], k[5]): world['rows'][k]['value'] for k in world['rows'] if k[0] == m and k[1] == quiet}
    assert got == {('all', '', 'objects'): 0, ('all', '', 'ca_keys'): 0, ('rir_hosted', '', 'objects'): 0,
                   ('rir_hosted', '', 'ca_keys'): 0}


def test_rp_rejected_range_before_verdicts_emits_nothing(tmp_path):
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    build_world(base)
    ctx = W.make_ctx(root, base, AS_OF, ['rp_rejected'], rirs=('apnic',))
    out = ctx.call('m_health', 'apnic', '2026-05-04', '2026-06-15')
    assert out['rows'] == 0 and ctx.fetch('rows') == []


# ── module behaviour and status ──────────────────────────────────────────────
def test_only_requested_metrics_and_missing_tables(tmp_path):
    """m_ca computes only what ctx.metrics asks for (live_inventory alone reads no lifetime rows); a
    warehouse without its presence table is an error, not an empty week."""
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    build_world(base)
    write_ledger(root)
    ctx = W.make_ctx(root, base, AS_OF, ['live_inventory'], rirs=('apnic',))
    ctx.call('m_ca', 'apnic', '2026-10-12', '2026-10-12')
    assert {r['metric_id'] for r in ctx.fetch('rows')} == {'live_inventory'} and ctx.fetch('hist') == []
    W.write_table(base, 'lacnic', 'certs/is_ca=true', [])
    ctx2 = W.make_ctx(root, base, AS_OF, ['ca_cert_lifetime'], rirs=('lacnic',))
    with pytest.raises(FileNotFoundError, match='object_presence_runs'):
        ctx2.call('m_ca', 'lacnic', '2026-10-12', '2026-10-12')


def test_reruns_are_identical(world):
    """A second call on the same inputs emits the same rows (deterministic tie-breaks and ORDER BY)."""
    ctx = W.make_ctx(world['root'], world['base'], AS_OF, ['ca_cert_lifetime', 'live_inventory', 'rp_rejected'],
                     rirs=('apnic',))
    ctx.call('m_ca', 'apnic', '2026-09-21', '2026-10-12')
    ctx.call('m_health', 'apnic', '2026-06-15', '2026-10-12')
    again = {(r['metric_id'], r['week'], r['window'], r['population'], r['operator'], r['stat']): r
             for r in ctx.fetch('rows')}
    assert again == world['rows']


def test_status_of_these_metrics(world, tmp_path):
    """Status rules applied to the emitted rows (scs/status.py) at as_of with the synthetic ledger:
    ca_cert_lifetime is a sample (W1 rir_hosted has n = 5, all at the week's end n = 17: insufficient;
    W1 is not final before W+2); live_inventory is a census (never insufficient), final at W+1, masked
    by a gap on the week's last day (09-27: no snapshot); rp_rejected is masked when the verdict cycles of
    the week are below 90% of its snapshot cycles (this ledger has verdict rows in 10 cycles only)."""
    root = str(tmp_path / 'root')
    write_ledger(root, verdict_cycles=W.cycles('2026-10-13', '2026-10-13')[:10])
    con = common.connect(threads=2, memory_limit='1GB', root=root)
    try:
        common.values_table(con, 'r', metrics.ROW_COLS,
                            [tuple(r[c] for c, _ in metrics.ROW_COLS) for r in world['rows'].values()])
        con.execute(f"CREATE TEMP TABLE ledger AS SELECT * FROM read_parquet('{root}/ledger/ledger.parquet')")
        status.annotations_table(con, assemble.ANNOTATIONS_PATH)
        status.apply(con, 'r', 'out', AS_OF, 1, 'test')
        st = {(m, w, win, p, o, s): x for m, w, win, p, o, s, x in con.execute(
            'SELECT metric_id, week, "window", population, operator, stat, status FROM out').fetchall()}
    finally:
        con.close()
    assert st[('ca_cert_lifetime', W1, '1w', 'rir_hosted', '', 'duration_p50_d')] == 'insufficient'
    assert st[('ca_cert_lifetime', W1, 'end', 'all', '', 'live_certs')] == 'insufficient'      # 16 keys, n 17
    assert st[('live_inventory', W1, 'end', 'all', '', 'ca_keys')] == 'final'
    assert st[('live_inventory', W1, 'end', 'unknown', '', 'roas')] == 'final'                   # n = 1, census
    assert st[('live_inventory', D(2026, 9, 21), 'end', 'all', '', 'ca_keys')] == 'masked'      # no snapshot
    assert st[('ca_cert_lifetime', W0, '1w', 'all', '', 'certs')] == 'insufficient'
    assert st[('rp_rejected', W1, '1w', 'all', '', 'objects')] == 'masked'
