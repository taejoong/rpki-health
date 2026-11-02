"""CRL and manifest freshness windows and empty CRLs (pipelines/scorecard/scs/m_crl.py: crl_window,
mft_window, crl_empty) on synthetic warehouses, with the expected values worked out by hand in the
comments: week assignment in UTC, cumulative band shares with inclusive edges, nearest-rank quantiles,
per-key medians and CA-weighted shares, n / n_ca / top1_share, histogram bins, populations (CA host from
the newest CA certificate across warehouses, NIR operator rows, PaaS, delegated, the manifest-EE
fallback, unknown), manifests with text times and without an EE certificate, the zero rows of population
'all', compare-mode quantiles, one grouped pass = week by week, and the metrics end to end through scs.run
with their status (final at W+1, masked by an outage, insufficient).
"""
import datetime as dt
import json
import os
import random
import shutil
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scorecard_world as W  # noqa: E402
from scs import common, defs, ledger, m_crl, run  # noqa: E402

D = dt.date
W1, W2, W3 = D(2026, 10, 12), D(2026, 10, 19), D(2026, 10, 26)
H = 3600
ALL3 = ['crl_window', 'mft_window', 'crl_empty']


def ts(t):
    return t.strftime('%Y-%m-%d %H:%M:%S')


def crl(i, key, tu, secs=None, rc=0):
    """One CRL row: thisUpdate tu (datetime), window secs seconds (None: no nextUpdate), revoked_count rc."""
    return {'crl_id': i, 'object_id': i, 'aki': key, 'revoked_count': rc, 'this_update': ts(tu),
            'next_update': None if secs is None else ts(tu + dt.timedelta(seconds=secs))}


def ca(cert_id, ski, sia, nb='2026-01-01 00:00:00'):
    return {'cert_id': cert_id, 'ski': ski, 'aki': 'apnic-ta', 'is_ca_flag': True, 'not_before': nb,
            'not_after': '2027-01-01 00:00:00', 'sia': sia}


def ee(cert_id, aki, sia, nb):
    return {'cert_id': cert_id, 'aki': aki, 'is_ca_flag': False, 'sia': sia, 'not_before': nb,
            'not_after': '2027-01-01 00:00:00'}


def world(base):
    """warehouse-apnic (and one CA certificate in warehouse-arin). Issuing keys and their populations:
      kA, kB  CA certificate on rpki.apnic.net                      -> rir_hosted
      kJ      CA certificate on rpki-repository.nic.ad.jp           -> nir_hosted, operator JPNIC
      kP      older CA certificate (2025) on rpki.apnic.net here, newer one (2026) in warehouse-arin on
              rpki.sub.apnic.net: the newest wins across warehouses  -> rir_paas, operator APNIC
      kD      CA certificate on krill.example.net (not listed)      -> delegated
      kX      CA certificate on rpki.arin.net (listed for ARIN)     -> delegated in APNIC's warehouse
      kH      CA certificate without an rsync URI: its manifest EE certificate on rpki.apnic.net -> rir_hosted
      kO      no CA certificate: manifest EE certificates on rpki.cernet.edu.cn (09-01) and rpki.apnic.net
              (10-10, the newest .mft one), a newer ROA EE certificate elsewhere (ignored) -> rir_hosted
      kU      no CA certificate, no EE certificate                   -> unknown
    CRLs (thisUpdate; window; revoked_count), week W1 = 2026-10-12..18:
      kA 48 h (0), 72 h (2), 48 h (0), and one without nextUpdate (0)
      kB 48 h (0), 72 h (0)          kJ 720 h (0)          kO 24 h exactly (1)
      kH 24 h + 1 s (0)              kD 168 h (3)          kX 48 h - 1 s (0)
      kP 26 h (0)                    kU 168 h + 1 s (0)
    W2: kA 2026-10-19 00:00:00 48 h, kB 2026-10-25 23:59:59 24 h; W3 (10-26): none. Outside the range
    W1..W3: kA 2026-10-11 23:59:59 and 2026-11-02 00:00:00.
    Manifests (text times; EE certificate -> key), W1: kA 48 h, 72 h; kB 24 h and one stamped
    '2026-10-19T02:00:00+05:00' (= 10-18 21:00 UTC, in W1) of 24 h; kJ 720 h; kO 24 h; one 48 h manifest
    whose EE certificate is missing (no key). W2: kA 48 h. Outside: kA 2026-10-11T23:59:59 (48 h)."""
    W.write_table(base, 'apnic', 'certs/is_ca=true', [
        ca(1, 'kA', 'rsync://rpki.apnic.net/repo/A/,https://rrdp.apnic.net/notification.xml,rsync://rpki.apnic.net/repo/A/a.mft'),
        ca(2, 'kB', 'rsync://rpki.apnic.net/repo/B/'),
        ca(3, 'kJ', 'rsync://rpki-repository.nic.ad.jp/ap/J/'),
        ca(4, 'kD', 'rsync://krill.example.net/repo/D/'),
        ca(5, 'kX', 'rsync://rpki.arin.net/repository/X/'),
        ca(6, 'kH', 'https://rrdp.example.net/notification.xml'),
        ca(7, 'kP', 'rsync://rpki.apnic.net/repo/P/', nb='2025-01-01 00:00:00')])
    W.write_table(base, 'arin', 'certs/is_ca=true', [ca(8, 'kP', 'rsync://rpki.sub.apnic.net/repo/P/')])
    t = dt.datetime(2026, 10, 13, 3)
    rows = [crl(1, 'kA', t, 48 * H), crl(2, 'kA', t, 72 * H, rc=2), crl(3, 'kA', t + dt.timedelta(days=1), 48 * H),
            crl(4, 'kA', t + dt.timedelta(days=2), None),
            crl(5, 'kB', t, 48 * H), crl(6, 'kB', t, 72 * H), crl(7, 'kJ', t, 720 * H), crl(8, 'kO', t, 24 * H, rc=1),
            crl(9, 'kH', t, 24 * H + 1), crl(10, 'kD', t, 168 * H, rc=3), crl(11, 'kX', t, 48 * H - 1),
            crl(12, 'kP', t, 26 * H), crl(13, 'kU', t, 168 * H + 1),
            crl(14, 'kA', dt.datetime(2026, 10, 19), 48 * H), crl(15, 'kB', dt.datetime(2026, 10, 25, 23, 59, 59), 24 * H),
            crl(16, 'kA', dt.datetime(2026, 10, 11, 23, 59, 59), 48 * H), crl(17, 'kA', dt.datetime(2026, 11, 2), 48 * H)]
    W.write_table(base, 'apnic', 'crls', rows)
    mfts = [(101, 'kA', '2026-10-13T03:00:00+00:00', '2026-10-15T03:00:00+00:00'),
            (102, 'kA', '2026-10-14T03:00:00+00:00', '2026-10-17T03:00:00+00:00'),
            (103, 'kB', '2026-10-13T00:00:00+00:00', '2026-10-14T00:00:00+00:00'),
            (104, 'kB', '2026-10-19T02:00:00+05:00', '2026-10-20T02:00:00+05:00'),
            (105, 'kJ', '2026-10-16T00:00:00+00:00', '2026-11-15T00:00:00+00:00'),
            (106, 'kO', '2026-10-15T06:00:00+00:00', '2026-10-16T06:00:00+00:00'),
            (107, None, '2026-10-17T12:00:00+00:00', '2026-10-19T12:00:00+00:00'),
            (108, 'kA', '2026-10-19T00:00:00+00:00', '2026-10-21T00:00:00+00:00'),
            (109, 'kA', '2026-10-11T23:59:59+00:00', '2026-10-13T23:59:59+00:00')]
    W.write_table(base, 'apnic', 'manifests', [{'object_id': o, 'manifest_number': '1', 'this_update': a,
                                                'next_update': b} for o, _, a, b in mfts])
    W.write_table(base, 'apnic', 'object_signatures', [{'object_id': o, 'object_type': 'MFT', 'signer_cert_id': o + 500}
                                                       for o, _, _, _ in mfts])
    host = {'kA': 'rpki.apnic.net', 'kB': 'rpki.apnic.net', 'kJ': 'rpki-repository.nic.ad.jp', 'kO': 'rpki.apnic.net'}
    W.write_table(base, 'apnic', 'certs/is_ca=false', [
        ee(o + 500, k, f'rsync://{host[k]}/repo/{k}/{o}.mft,https://rrdp.example/n.xml', a[:10] + ' 00:00:00')
        for o, k, a, _ in mfts if k] + [
        ee(901, 'kO', 'rsync://rpki.cernet.edu.cn/repo/O/o.mft', '2026-09-01 00:00:00'),
        ee(902, 'kO', 'rsync://rpki.apnic.net/repo/O/o.mft', '2026-10-10 00:00:00'),
        ee(903, 'kO', 'rsync://elsewhere.example.org/repo/O/x.roa', '2026-10-11 00:00:00'),
        ee(904, 'kH', 'rsync://rpki.apnic.net/repo/H/h.mft', '2026-10-01 00:00:00')])


def compute(tmp_path, wf=W1, wt=W3, metric_ids=ALL3, **kw):
    base = str(tmp_path / 'wh')
    if not os.path.isdir(base):
        world(base)
    ctx = W.make_ctx(str(tmp_path / 'root'), base, '2026-11-02 00:40:01', metric_ids, rirs=('apnic',), **kw)
    ctx.call('m_crl', 'apnic', wf, wt)
    rows = {(r['metric_id'], r['week'], r['population'], r['operator'], r['stat']): r for r in ctx.fetch('rows')}
    return rows, ctx.fetch('hist')


def compute_in(tmp_path, week):
    """compute() for one week on the same warehouse, with its own root."""
    base = str(tmp_path / 'wh')
    ctx = W.make_ctx(str(tmp_path / f'root-{week}'), base, '2026-11-02 00:40:01', ALL3, rirs=('apnic',))
    ctx.call('m_crl', 'apnic', week, week)
    rows = {(r['metric_id'], r['week'], r['population'], r['operator'], r['stat']): r for r in ctx.fetch('rows')}
    return rows, ctx.fetch('hist')


def stats(rows, metric, week, pop, op=''):
    """{stat: value} plus n, n_ca, top1 of one (metric, week, population, operator)."""
    got = {k[4]: r for k, r in rows.items() if k[:4] == (metric, week, pop, op)}
    if not got:
        return None
    one = next(iter(got.values()))
    assert {(r['n'], r['n_ca'], r['top1_share']) for r in got.values()} == {(one['n'], one['n_ca'], one['top1_share'])}
    return {s: r['value'] for s, r in got.items()} | {'n': one['n'], 'n_ca': one['n_ca'], 'top1': one['top1_share']}


def r10(x):
    return round(x, 10)


def test_crl_window_bands_quantiles_and_ca_weighting(tmp_path):
    rows, _ = compute(tmp_path)
    # W1, all: 12 CRLs with a window (kA's CRL without nextUpdate is not one), 9 keys, kA 3 of 12.
    # Sorted windows (h): 24, 24+1s, 26, 48-1s, 48, 48, 48, 72, 72, 168, 168+1s, 720.
    a = stats(rows, 'crl_window', W1, 'all')
    assert (a['n'], a['n_ca'], a['top1']) == (12, 9, 0.25)
    assert a['share_le24'] == r10(1 / 12)            # 24 h exactly is in the band; 24 h + 1 s is not
    assert a['share_le48'] == r10(7 / 12)            # 24, 24+1s, 26, 48-1s, 48 x 3
    assert a['share_le72'] == 0.75 and a['share_le168'] == r10(10 / 12) and a['share_gt168'] == r10(2 / 12)
    assert a['p50'] == 48.0                          # nearest rank ceil(0.5 x 12) = 6th
    assert a['p90'] == r10(168 + 1 / 3600)           # ceil(0.9 x 12) = 11th
    assert a['max'] == 720.0
    # per-key medians (nearest rank): kA [48,48,72] -> 48, kB [48,72] -> 48 (not 60), kJ 720, kO 24,
    # kH 24+1s, kD 168, kX 48-1s, kP 26, kU 168+1s; sorted: 24, 24+1s, 26, 48-1s, 48, 48, 168, 168+1s, 720
    assert a['p50_ca'] == 48.0                        # 5th of 9
    # CA-weighted bands = mean over the 9 keys of each key's share of its CRLs in the band:
    # <= 48 h: kA 2/3, kB 1/2, kO, kH, kX, kP 1 each -> (2/3 + 1/2 + 4) / 9 = 31/54; <= 72 h: 6/9 (kJ, kD, kU 0)
    assert (a['share_le24_ca'], a['share_le48_ca'], a['share_le72_ca'], a['share_le168_ca'], a['share_gt168_ca']) == \
        (r10(1 / 9), r10(31 / 54), r10(6 / 9), r10(7 / 9), r10(2 / 9))
    # rir_hosted: kA, kB (CA certificates), kO (manifest EE fallback), kH (CA certificate without rsync URI)
    # 7 CRLs: 24, 24+1s, 48, 48, 48, 72, 72; key medians 24, 24+1s, 48, 48
    h = stats(rows, 'crl_window', W1, 'rir_hosted')
    assert (h['n'], h['n_ca'], h['top1']) == (7, 4, r10(3 / 7))
    assert (h['share_le24'], h['share_le48'], h['share_le72'], h['share_gt168']) == (r10(1 / 7), r10(5 / 7), 1.0, 0.0)
    assert (h['p50'], h['p90'], h['max']) == (48.0, 72.0, 72.0)   # 4th and 7th of 7
    assert h['p50_ca'] == r10(24 + 1 / 3600)                       # 2nd of 4: the lower median, observed
    assert (h['share_le24_ca'], h['share_le48_ca']) == (0.25, r10((2 / 3 + 1 / 2 + 1 + 1) / 4))   # 19/24
    # NIR (class row and operator row), PaaS via the newest CA certificate in warehouse-arin, delegated
    # (an unlisted host and another RIR's listed host), unknown
    for op in ('', 'JPNIC'):
        j = stats(rows, 'crl_window', W1, 'nir_hosted', op)
        assert (j['n'], j['n_ca'], j['p50'], j['max'], j['share_le168'], j['share_gt168']) == (1, 1, 720.0, 720.0, 0.0, 1.0)
    for op in ('', 'APNIC'):
        assert stats(rows, 'crl_window', W1, 'rir_paas', op)['p50'] == 26.0
    d = stats(rows, 'crl_window', W1, 'delegated')                # kX 48 h - 1 s, kD 168 h
    assert (d['n'], d['n_ca'], d['p50'], d['p90'], d['share_le48'], d['share_le168']) == \
        (2, 2, r10(48 - 1 / 3600), 168.0, 0.5, 1.0)
    u = stats(rows, 'crl_window', W1, 'unknown')
    assert (u['n'], u['n_ca'], u['p50'], u['share_gt168']) == (1, 1, r10(168 + 1 / 3600), 1.0)
    assert not [k for k in rows if k[1] == W1 and k[3] not in ('', 'JPNIC', 'APNIC')]
    assert {k[2] for k in rows if k[0] == 'crl_window' and k[1] == W1} == {'all', 'rir_hosted', 'nir_hosted', 'rir_paas',
                                                                           'delegated', 'unknown'}
    # W2: the week's first second (kA 48 h) and last second (kB 24 h); the CRLs before W1 and from
    # 2026-11-02 are outside the range
    b = stats(rows, 'crl_window', W2, 'all')
    assert (b['n'], b['n_ca'], b['top1'], b['p50'], b['p90'], b['max']) == (2, 2, 0.5, 24.0, 48.0, 48.0)
    # W3 has no CRL: population 'all' has every statistic with n = 0 and no value; no other population
    c = stats(rows, 'crl_window', W3, 'all')
    assert (c['n'], c['n_ca'], c['top1']) == (0, 0, None)
    assert set(c) - {'n', 'n_ca', 'top1'} == set(defs.METRICS['crl_window']['stats'])
    assert {v for k, v in c.items() if k not in ('n', 'n_ca')} == {None}
    assert {k[2] for k in rows if k[1] == W3} == {'all'}


def test_crl_window_histogram(tmp_path):
    _, hist = compute(tmp_path, metric_ids=['crl_window'])
    h = {(r['bin_lo'], r['bin_hi']): r['count'] for r in hist
         if (r['week'], r['population'], r['operator'], r['metric_id'], r['stat']) == (W1, 'all', '', 'crl_window', 'window_h')}
    # 1-hour bins [k, k + 1): 48 h - 1 s falls in [47, 48), 24 h and 24 h + 1 s in [24, 25), 720 h in the overflow
    assert h == {(24.0, 25.0): 2, (26.0, 27.0): 1, (47.0, 48.0): 1, (48.0, 49.0): 3, (72.0, 73.0): 2,
                 (168.0, 169.0): 2, (200.0, None): 1}
    assert sum(r['count'] for r in hist if r['population'] == 'nir_hosted' and r['operator'] == 'JPNIC') == 1
    assert {r['metric_id'] for r in hist} == {'crl_window'}


def test_mft_window_text_times_ee_keys_and_a_manifest_without_key(tmp_path):
    rows, hist = compute(tmp_path)
    # W1: kA 48, 72; kB 24 and 24 (stamped +05:00: 10-18 21:00 UTC); kJ 720; kO 24; no key 48 -> 7 manifests,
    # 4 keys (the keyless manifest counts in n only); sorted 24, 24, 24, 48, 48, 72, 720
    a = stats(rows, 'mft_window', W1, 'all')
    assert (a['n'], a['n_ca'], a['top1']) == (7, 4, r10(2 / 7))
    assert (a['share_le24'], a['share_le48'], a['share_le72'], a['share_le168'], a['share_gt168']) == \
        (r10(3 / 7), r10(5 / 7), r10(6 / 7), r10(6 / 7), r10(1 / 7))
    assert (a['p50'], a['p90'], a['max']) == (48.0, 720.0, 720.0)
    # key medians: kA [48, 72] -> 48, kB 24, kJ 720, kO 24 -> sorted 24, 24, 48, 720, 2nd = 24;
    # key shares <= 48 h: kA 1/2, kB 1, kJ 0, kO 1 -> mean 0.625
    assert a['p50_ca'] == 24.0 and (a['share_le24_ca'], a['share_le48_ca'], a['share_gt168_ca']) == (0.5, 0.625, 0.25)
    u = stats(rows, 'mft_window', W1, 'unknown')
    assert (u['n'], u['n_ca'], u['top1'], u['p50'], u['p50_ca'], u['share_le48_ca']) == (1, 0, None, 48.0, None, None)
    h = stats(rows, 'mft_window', W1, 'rir_hosted')               # kA, kB, kO (fallback): 24, 24, 24, 48, 72
    assert (h['n'], h['n_ca'], h['top1'], h['p50'], h['p90']) == (5, 3, 0.4, 24.0, 72.0)
    assert stats(rows, 'mft_window', W1, 'nir_hosted', 'JPNIC')['p50'] == 720.0
    assert stats(rows, 'mft_window', W2, 'all')['n'] == 1
    assert stats(rows, 'mft_window', W3, 'all')['n'] == 0
    assert sum(r['count'] for r in hist if r['metric_id'] == 'mft_window' and r['week'] == W1
               and r['population'] == 'all') == 7


def test_crl_empty(tmp_path):
    rows, hist = compute(tmp_path, metric_ids=['crl_empty'])
    # W1: 13 CRLs (with kA's CRL without nextUpdate); empty: kA 3 of 4, kB 2, kJ, kH, kX, kP, kU -> 10;
    # keys all empty: kB, kJ, kH, kX, kP, kU = 6 of 9; kA holds 4 of 13
    a = stats(rows, 'crl_empty', W1, 'all')
    assert (a['n'], a['n_ca'], a['top1'], a['share_empty'], a['share_empty_ca']) == \
        (13, 9, r10(4 / 13), r10(10 / 13), r10(6 / 9))
    h = stats(rows, 'crl_empty', W1, 'rir_hosted')                # kA 3/4, kB 2/2, kO 0/1, kH 1/1
    assert (h['n'], h['n_ca'], h['top1'], h['share_empty'], h['share_empty_ca']) == (8, 4, 0.5, 0.75, 0.5)
    assert stats(rows, 'crl_empty', W1, 'delegated')['share_empty'] == 0.5     # kD (3 revoked), kX
    c = stats(rows, 'crl_empty', W3, 'all')
    assert (c['n'], c['share_empty'], c['share_empty_ca']) == (0, None, None)
    assert hist == [] and {k[0] for k in rows} == {'crl_empty'}


def test_compare_mode_interpolates_like_quantile_cont(tmp_path, monkeypatch):
    """params quantile 'cont' (an override of the compare mode): p50 and the per-key medians interpolate,
    like DuckDB quantile_cont."""
    monkeypatch.setitem(defs.COMPARE_MODE, 'crl_window', {'quantile': 'cont'})
    rows, _ = compute(tmp_path, metric_ids=['crl_window'], mode='compare')
    d = stats(rows, 'crl_window', W1, 'delegated')                # [48 h - 1 s, 168 h]: halfway
    assert d['p50'] == r10((48 - 1 / 3600 + 168) / 2)
    h = stats(rows, 'crl_window', W1, 'rir_hosted')
    # key medians: kA [48, 48, 72] -> 48, kB [48, 72] -> 60, kO 24, kH 24+1s; median of 4 = (24+1s + 48) / 2
    assert h['p50_ca'] == r10((24 + 1 / 3600 + 48) / 2)
    assert h['share_le48_ca'] == r10(19 / 24)                     # band shares do not depend on the quantile rule
    assert h['p50'] == 48.0 and h['p90'] == 72.0                 # positions 3 and 5.4 of 0..6: 48, 72


def test_one_grouped_pass_equals_week_by_week(tmp_path):
    """W1..W3 in one call gives the rows of three one-week calls (including the manifest stamped
    '2026-10-19T02:00:00+05:00', which the text pre-filter of the W1 call must still read)."""
    one, h1 = compute(tmp_path)
    by_week, hw = {}, []
    for w in (W1, W2, W3):
        r, h = compute_in(tmp_path, w)
        by_week.update(r)
        hw += h
    assert one == by_week
    assert sorted(h1, key=str) == sorted(hw, key=str)


def test_metrics_that_were_not_requested_are_not_read(tmp_path):
    """crl_empty alone reads no manifest table: with the manifest parts removed it still runs."""
    base = str(tmp_path / 'wh')
    world(base)
    shutil.rmtree(common.table_dir('apnic', 'manifests', base))
    rows, _ = compute(tmp_path, metric_ids=['crl_empty'])
    assert stats(rows, 'crl_empty', W1, 'all')['n'] == 13
    with pytest.raises(FileNotFoundError, match='table=manifests'):
        compute(tmp_path, metric_ids=['mft_window'])


@pytest.mark.parametrize('method', ['disc', 'cont'])
def test_weighted_quantiles_match_duckdb(method):
    """wquantile_sql on (value, count) rows equals DuckDB's quantile_disc / quantile_cont on the expanded
    values, for groups with ties, single values and large counts."""
    rnd = random.Random(7)
    rows = []
    for g in range(60):
        for _ in range(rnd.randint(1, 8)):
            rows.append((g, rnd.choice([24, 48, 72, 168, 720, rnd.randint(0, 900)]) * H + rnd.choice([0, 0, 1, -1]),
                         rnd.choice([1, 1, 2, 3, 50])))
    con = duckdb.connect()
    con.execute("CREATE TABLE t (g INTEGER, v BIGINT, n BIGINT)")
    con.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
    got = con.execute(f"SELECT g, w_total, q10, q50, q90 FROM ({m_crl.wquantile_sql('t', ['g'], 'v', 'n', (10, 50, 90), method)}) "
                      "ORDER BY g").fetchall()
    want = con.execute(f"""SELECT g, count(*), quantile_{method}(v, 0.1), quantile_{method}(v, 0.5), quantile_{method}(v, 0.9)
        FROM (SELECT g, v FROM t, range(n)) GROUP BY g ORDER BY g""").fetchall()
    assert [(g, n) + tuple(round(float(x), 6) for x in q) for g, n, *q in got] == \
        [(g, n) + tuple(round(float(x), 6) for x in q) for g, n, *q in want]


# ── end to end through scs.run: rows, statuses, the zero rows of every RIR ───
HOSTS = {'afrinic': 'rpki.afrinic.net', 'apnic': 'rpki.apnic.net', 'arin': 'rpki.arin.net',
         'lacnic': 'repository.lacnic.net', 'ripencc': 'rpki.ripe.net'}


def e2e_world(base):
    """Per RIR: 12 hosted CA keys k0..k11, each with one CRL and one manifest a day 2026-09-28..11-01 at
    03:00 (window 48 h for k0..k5, 72 h for k6..k11; k0, k2, .. k10 empty CRLs, the others list one
    revoked certificate), and a key without any certificate (one empty 24-hour CRL a day: unknown).
    Presence ends 2026-11-02 00:40:01 (LACNIC 01:00:01). No snapshot directories: e2e_ledger() writes the
    observation ledger from the snapshot list."""
    for i, (rir, host) in enumerate(HOSTS.items()):
        W.write_table(base, rir, 'certs/is_ca=true', [
            {'cert_id': i * 100 + k, 'ski': f'{rir}-k{k}', 'aki': f'{rir}-ta', 'is_ca_flag': True,
             'not_before': '2026-01-01 00:00:00', 'sia': f'rsync://{host}/repo/{k}/'} for k in range(12)])
        crls, mfts, sigs, ees, oid = [], [], [], [], i * 100000
        for d in range(35):
            t = dt.datetime(2026, 9, 28, 3) + dt.timedelta(days=d)
            for k in range(13):
                oid += 1
                key = f'{rir}-k{k}' if k < 12 else f'{rir}-nocert'
                hours = 24 if k == 12 else 48 if k < 6 else 72
                crls.append(crl(oid, key, t, hours * H, rc=(k % 2 if k < 12 else 0)))
                if k < 12:
                    mfts.append({'object_id': oid + 50000, 'manifest_number': str(d), 'this_update': t.isoformat() + '+00:00',
                                 'next_update': (t + dt.timedelta(hours=hours)).isoformat() + '+00:00'})
                    sigs.append({'object_id': oid + 50000, 'object_type': 'MFT', 'signer_cert_id': oid + 70000})
                    ees.append(ee(oid + 70000, key, f'rsync://{host}/repo/{k}/m.mft', ts(t)))
        W.write_table(base, rir, 'crls', crls)
        W.write_table(base, rir, 'manifests', mfts)
        W.write_table(base, rir, 'object_signatures', sigs)
        W.write_table(base, rir, 'certs/is_ca=false', ees)
        end = '2026-11-02 01:00:01' if rir == 'lacnic' else '2026-11-02 00:40:01'
        W.write_table(base, rir, 'object_presence_runs',
                      [{'object_id': 1, 'start_sid': 0, 'end_sid': 9, 'start_ts': '2026-09-20 00:00:01', 'end_ts': end}])


def e2e_ledger(root):
    """The observation ledger of e2e_world, built from a snapshot list (no directories): cycles every 20
    minutes 2026-09-14 .. 2026-11-02 03:40, except a whole day without cycles at ARIN on 2026-10-06; cut
    2026-11-02 05:00."""
    snaps = {}
    for rir in HOSTS:
        cyc = [c for c in W.cycles('2026-09-14', '2026-11-02')[:-60] if not (rir == 'arin' and c[:10] == '2026-10-06')]
        snaps[rir] = [(c, 'rpki-client', 1000, True, True, i) for i, c in enumerate(cyc)]
    cut = common.parse_ts('2026-11-02 05:00:00')
    con = common.connect(threads=2, memory_limit='1GB', root=root)
    try:
        ledger.build(con, snaps, {}, cut, common.tmp_dir(root))
        ledger.write(con, root, cut, {}, tuple(HOSTS))
    finally:
        con.close()


def test_end_to_end_rows_and_status(tmp_path, capsys, monkeypatch):
    base, root = str(tmp_path / 'wh'), str(tmp_path / 'root')
    e2e_world(base)
    e2e_ledger(root)
    monkeypatch.setattr(common, 'may_start', lambda now=None: True)
    monkeypatch.setattr(common, 'MIN_PART_AGE_S', 0)
    monkeypatch.setattr(common, 'utc_now', lambda: dt.datetime(2026, 11, 2, 5, 0, 13))   # the Monday run
    argv = ['--root', root, '--warehouse-base', base, '--metrics', ','.join(ALL3), '--lock', str(tmp_path / 'lock')]
    assert run.main(argv) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out['newest_week'] == '2026-10-26' and out['weeks'] == 6 and out['warnings'] == []
    b = os.path.realpath(os.path.join(root, 'current'))
    con = duckdb.connect()
    r = {(x[0], x[1], x[2], x[3], x[4]): x[5:] for x in con.execute(
        f"""SELECT rir, metric_id, CAST(week AS VARCHAR), population, stat, value, n, n_ca, status
            FROM read_parquet('{b}/scorecard.parquet') WHERE operator = ''""").fetchall()}
    for rir in HOSTS:
        # hosted, week 10-19: 84 CRLs of 12 keys, half 48 h and half 72 h: nearest rank 42nd = 48, 76th = 72
        assert r[(rir, 'crl_window', '2026-10-19', 'rir_hosted', 'share_le48')] == (0.5, 84, 12, 'final')
        assert r[(rir, 'crl_window', '2026-10-19', 'rir_hosted', 'p50')][0] == 48.0
        assert r[(rir, 'crl_window', '2026-10-19', 'rir_hosted', 'p90')][0] == 72.0
        assert r[(rir, 'crl_window', '2026-10-19', 'rir_hosted', 'p50_ca')][0] == 48.0       # 6th of 12 key medians
        assert r[(rir, 'mft_window', '2026-10-19', 'rir_hosted', 'share_le48')] == (0.5, 84, 12, 'final')
        assert r[(rir, 'crl_empty', '2026-10-19', 'rir_hosted', 'share_empty')] == (0.5, 84, 12, 'final')
        assert r[(rir, 'crl_empty', '2026-10-19', 'all', 'share_empty')] == (round(49 / 91, 10), 91, 13, 'final')
        assert r[(rir, 'crl_window', '2026-10-19', 'unknown', 'p50')] == (24.0, 7, 1, 'insufficient')
        # the first trailing week (09-21) has no object: population 'all' only, n = 0
        assert r[(rir, 'crl_window', '2026-09-21', 'all', 'p50')] == (None, 0, 0, 'insufficient')
        assert not [k for k in r if k[0] == rir and k[2] == '2026-09-21' and k[3] != 'all']
    # ARIN's collection outage on 10-06: its week 10-05 is masked (every population), the others are not
    assert r[('arin', 'crl_window', '2026-10-05', 'rir_hosted', 'p50')][-1] == 'masked'
    assert r[('arin', 'crl_window', '2026-10-05', 'unknown', 'p50')][-1] == 'masked'
    assert r[('apnic', 'crl_window', '2026-10-05', 'rir_hosted', 'p50')][-1] == 'final'
    m = json.load(open(f'{b}/manifest.json'))
    assert list(m['definitions']) == ALL3
    h = con.execute(f"""SELECT bin_lo, bin_hi, count FROM read_parquet('{b}/histograms.parquet')
        WHERE rir = 'lacnic' AND population = 'rir_hosted' AND metric_id = 'crl_window' AND week = DATE '2026-10-19'
        ORDER BY bin_lo""").fetchall()
    assert h == [(48.0, 49.0, 42), (72.0, 73.0, 42)]
