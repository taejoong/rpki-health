"""Metric definition registry: one entry per metric_id, in the order the manifest lists them.

Entry fields:
  def_version   integer; bump it with a dated note when the definition changes: the next run recomputes
                that metric's whole history (state.json records the version of its frozen rows)
  module        the module under scs/ that computes it (scs.metrics documents the interface)
  title, unit   for the manifest
  kind          'sample' (a row is 'insufficient' when n < MIN_N or n_ca < MIN_N_CA) or 'census'
                (counts of everything there was; never insufficient)
  classified_by which attribute assigns an object to a population (scs/classify.py): signer_ee_host,
                ca_host, issuer_host, own_host or per_type
  windows       the values of the `window` column the metric emits: '1w' (the ISO week), '13w' and
                '52w' (rolling windows ending with the week), 'end' (point in time at the week's end)
  stats         {stat: description}; a key starting with '^' is a regular expression for a family
  histograms    {stat: {'desc', 'windows'}}: fixed-bin histograms in histograms.parquet
  params        the parameters of the definition (COMPARE_MODE, empty in this release, may override some
                of them)
  rules         status rules (applied by scs/status.py), first match on the stat wins; a rule without
                'stats' is the default: publish_after / final_after = weeks after the week W (W+k: the run
                on the Monday k weeks after W's Monday, i.e. as_of >= W + 7k days); mask = 'cycles'
                (era 2: good cycles of W < 90% of expected; era 1: a day of W without a good
                snapshot), 'gap' (a ledger gap day in [W, W + 7 + lookahead_days)), 'end_gap' (a gap
                on W's last day), 'verdicts' (verdict cycles of W < 90% of its snapshot cycles, or good
                cycles of W < 90% of expected) or None
  available_from first week with values (era-2-only metrics), None = all history
  rfc, method   RFC anchors and the method paragraph (manifest; no grades)
"""
import re

MIN_N, MIN_N_CA = 30, 10

# status rules (see scs/status.py)
WINDOWS_RULE = {'publish_after': 1, 'final_after': 1, 'mask': 'cycles', 'lookahead_days': 0}
LATE_ARRIVAL_RULE = {'publish_after': 1, 'final_after': 2, 'mask': 'gap', 'lookahead_days': 7}
H7_RULE = {'publish_after': 2, 'final_after': 2, 'mask': 'gap', 'lookahead_days': 7}
H35_RULE = {'stats': r'_h35(_|$)', 'publish_after': 2, 'final_after': 6, 'mask': 'gap', 'lookahead_days': 35}
REPLACEMENT_RULE = {'publish_after': 1, 'final_after': 1, 'mask': 'gap', 'lookahead_days': 0}
VERDICT_RULE = {'publish_after': 1, 'final_after': 1, 'mask': 'verdicts', 'lookahead_days': 0}
END_RULE = {'publish_after': 1, 'final_after': 1, 'mask': 'end_gap', 'lookahead_days': 0}

ERA2 = '2026-04-20'              # first full era-2 ISO week (2026-W17)
VERDICTS = '2026-06-22'          # first full week with verdicts (they start 2026-06-20 02:40)

_BANDS = (24, 48, 72, 168)


def _window_stats(what):
    out = {f'share_le{b}': f'share of {what} with a window <= {b} h' for b in _BANDS}
    out['share_gt168'] = f'share of {what} with a window > 168 h'
    out.update({'p50': 'median window, h (quantile_disc: an observed value)',
                'p90': '90th percentile, h (quantile_disc)', 'max': 'longest window, h',
                'p50_ca': 'CA-weighted median: median over issuing keys of the per-key median window, h'})
    out.update({f'share_le{b}_ca': f'CA-weighted: mean over issuing keys of the key\'s share of {what} with a '
                                   f'window <= {b} h' for b in _BANDS})
    out['share_gt168_ca'] = f'CA-weighted: mean over issuing keys of the key\'s share of {what} with a window > 168 h'
    return out


def _revocation_stats():
    out = {'pairs': 'pairs whose successor was signed in the week (count; value = n)',
           'early_pairs': 'pairs whose predecessor EE had > 7 days of validity left (count; value = n)'}
    for h in (7, 35):
        out.update({
            f'revoked_h{h}': f'share of early pairs with CRL evidence revoked within {h} d (pair-weighted)',
            f'revoked_h{h}_ca': f'mean over issuing keys of the per-key share revoked within {h} d (CA-weighted)',
            f'no_evidence_h{h}': f'share of early pairs without any CRL of the issuing key in the {h}-day window',
            f'revoked_h{h}_samekey': f'revoked within {h} d, successor signed by the same key',
            f'revoked_h{h}_keychange': f'revoked within {h} d, successor signed by another key',
            f'revoked_h{h}_withdrawn': f'revoked within {h} d, predecessor gone within a day of the successor',
            f'revoked_h{h}_kept': f'revoked within {h} d, predecessor still present > 1 day after the successor'})
    return out


METRICS = {
    'crl_window': {
        'def_version': 1, 'module': 'm_crl', 'title': 'CRL freshness window', 'unit': 'hours',
        'kind': 'sample', 'classified_by': 'ca_host', 'windows': ('1w',), 'available_from': None,
        'stats': _window_stats('CRLs'),
        'histograms': {'window_h': {'desc': '1-hour bins of nextUpdate - thisUpdate from 0 to 200 h, then one '
                                            'overflow bin [200, NULL)', 'windows': ('1w',)}},
        'params': {'bands_h': list(_BANDS), 'quantile': 'disc', 'hist_bin_h': 1, 'hist_max_h': 200,
                   'weight': 'one row per CRL object (every observed re-issue); CRLs without a nextUpdate left out',
                   'band_edges': 'window <= b h exactly (whole seconds, no tolerance)',
                   'ca_weight': "one vote per issuing key: the median of the per-key medians; the mean of the "
                                "keys' shares in a band",
                   'host': 'SIA host of the newest CA certificate of the key in any warehouse, else of its newest '
                           'manifest EE certificate in the warehouse'},
        'rules': [WINDOWS_RULE],
        'rfc': ['RFC 6487 §5', 'RFC 9286 §4.2.1', 'RFC 9286 §6.3', 'RFC 9286 §6.6'],
        'method': (
            'CRLs whose thisUpdate falls in the week (UTC), one row per CRL object we observed: every '
            're-issue counts. The window is nextUpdate - thisUpdate in hours, the time after which the CRL is '
            'stale unless it has been replaced. RFC 6487 §5 profiles the CRL; RFC 9286 §4.2.1 expects the '
            "manifest's nextUpdate to match the CRL's, and a relying party treats a manifest past nextUpdate as "
            'a failed fetch (§6.3) and keeps using cached data (§6.6). The stable signals are the cumulative '
            'band shares (<= 24, 48, 72, 168 h, edges included, and > 168 h); the median is a label: ARIN\'s '
            'hosted CRLs mix 48-hour and 72-hour windows in about equal numbers, so its weekly median flips '
            'between them. Quantiles are observed windows (nearest rank: the smallest window with at least half, '
            'or 90%, of the CRLs at or below it). CA-weighted values give each issuing key (crls.aki) one vote: '
            "p50_ca is the median of the per-key median windows, a _ca band share the mean of the keys' own "
            'shares in the band (a key alternating 48 and 72 h counts half; classifying keys by their median '
            "instead would follow the parity of each key's weekly CRL count). Populations by the CA's own "
            'publication host: the SIA host of the newest CA certificate of the key in any warehouse, else that '
            'of its newest manifest EE certificate (keys without a CA certificate). Era 2 observes every re-issue '
            '(cycles every 20 minutes), era 1 one archive snapshot a day, so issuance weighting differs between '
            'the eras.')},
    'mft_window': {
        'def_version': 1, 'module': 'm_crl', 'title': 'Manifest freshness window', 'unit': 'hours',
        'kind': 'sample', 'classified_by': 'ca_host', 'windows': ('1w',), 'available_from': None,
        'stats': _window_stats('manifests'),
        'histograms': {'window_h': {'desc': '1-hour bins of nextUpdate - thisUpdate from 0 to 200 h, then one '
                                            'overflow bin [200, NULL)', 'windows': ('1w',)}},
        'params': {'bands_h': list(_BANDS), 'quantile': 'disc', 'hist_bin_h': 1, 'hist_max_h': 200,
                   'weight': 'one row per manifest object (every observed re-issue)',
                   'band_edges': 'window <= b h exactly (whole seconds, no tolerance)',
                   'ca_weight': "one vote per issuing key: the median of the per-key medians; the mean of the "
                                "keys' shares in a band",
                   'host': 'SIA host of the newest CA certificate of the key in any warehouse, else of its newest '
                           'manifest EE certificate in the warehouse'},
        'rules': [WINDOWS_RULE],
        'rfc': ['RFC 9286 §4.2.1', 'RFC 9286 §6.3', 'RFC 9286 §6.6'],
        'method': (
            'The same as crl_window for manifests whose thisUpdate falls in the week (thisUpdate and nextUpdate '
            'are stored as text and parsed). A relying party treats a manifest past its nextUpdate as a failed '
            'fetch (RFC 9286 §6.3) and falls back to cached data (§6.6); a CA must issue the next manifest '
            'before nextUpdate (§4.2.1). Issuing key = the aki of the manifest\'s EE certificate; a manifest '
            'whose EE certificate is not in the warehouse has no key and counts in population unknown (APNIC '
            "2017-18). Populations by the CA's own publication host (the newest CA certificate with ski = that "
            "key; the key's newest manifest EE certificate when no CA certificate is known).")},
    'crl_empty': {
        'def_version': 1, 'module': 'm_crl', 'title': 'Empty CRLs', 'unit': 'share',
        'kind': 'sample', 'classified_by': 'ca_host', 'windows': ('1w',), 'available_from': None,
        'stats': {'share_empty': 'share of CRLs issued in the week with revoked_count = 0',
                  'share_empty_ca': 'share of issuing keys all of whose CRLs in the week were empty'},
        'histograms': {},
        'params': {'weight': 'one row per CRL object (every observed re-issue), with or without a nextUpdate',
                   'ca_rule': 'a key counts as empty when every CRL of the key in the week lists no revoked '
                              'certificate'},
        'rules': [WINDOWS_RULE],
        'rfc': ['RFC 6487 §5', 'RFC 9286 §4.2.1'],
        'method': (
            'The share of CRLs issued in the week (thisUpdate) that list no revoked certificate '
            '(revoked_count = 0), and the share of issuing keys all of whose CRLs in the week were empty. An '
            'empty CRL is not an error by itself, since a CA that replaced nothing has nothing to revoke; read '
            'it with mft_ee_revocation: a CA that issues a new manifest before the nextUpdate of the current one '
            'must revoke the old manifest\'s EE certificate (RFC 9286 §4.2.1), so its CRL cannot stay empty. '
            "Populations by the CA's own publication host (as crl_window). Issuance-weighted: era 2 counts "
            'every re-issue, which weights CAs that re-issue often more than era 1 did.')},
    'roa_ee_revocation': {
        'def_version': 1, 'module': 'm_pairs', 'title': 'Revocation of replaced ROA EE certificates',
        'unit': 'share', 'kind': 'sample', 'classified_by': 'signer_ee_host', 'windows': ('1w',),
        'available_from': None, 'stats': _revocation_stats(), 'histograms': {},
        'params': {'chain_break_days': 30, 'early_days': 7, 'horizons_days': [7, 35], 'lookback_days': 1,
                   'withdrawn_after_days': 1, 'order': 'coalesce(signing_time, ee_not_before), object_id',
                   'ca_weight': 'mean of per-key shares'},
        'rules': [H35_RULE, H7_RULE],
        'rfc': ['RFC 9286 §5.2', 'RFC 6487 §5', 'RFC 6489 §2'],
        'method': (
            'Re-sign pairs are consecutive ROA objects of one semantic ROA (the same ASN and prefix set), ordered '
            'by CMS signing time (then EE notBefore and object id); a chain breaks when the successor was first '
            'seen more than 30 days after the predecessor was last seen. A pair counts in the week of the '
            "successor's signing time, and is early when the predecessor's EE certificate had more than 7 days of "
            'validity left then. RFC 9286 §5.2: when an object is replaced, the CA MUST revoke the certificate of '
            'the replaced object if it is not expired. Revoked within H: the predecessor\'s EE serial is listed '
            "on a CRL of the predecessor's issuing key with thisUpdate from 1 day before the successor's signing "
            'time to H days after it, for H = 7 days (final two weeks after the week) and H = 35 days (final six '
            'weeks after). Early pairs without any CRL of that key in the window are no_evidence: reported '
            'separately, left out of the shares. Strata: successor signed by the same key or by another key (a '
            'RFC 6489 key rollover retires the old key instead), and predecessor withdrawn (gone within a day of '
            'the successor appearing) or kept until it expired. Pair-weighted and CA-weighted (mean of per-key '
            'shares) shares; n_ca and top1_share show how concentrated the pairs are. Populations by the host of '
            "the predecessor's signer EE certificate. A share pooled over every CRL ever stored is a "
            'different, unbounded quantity and is not computed here.')},
    'mft_ee_revocation': {
        'def_version': 1, 'module': 'm_mft', 'title': 'Revocation of replaced manifest EE certificates',
        'unit': 'share', 'kind': 'sample', 'classified_by': 'ca_host', 'windows': ('1w',),
        'available_from': ERA2,
        'stats': {'early_replacements': 'directly observed manifests replaced before their nextUpdate (count; '
                                        'value = n)',
                  'revoked': 'share of early replacements whose predecessor EE was revoked (pair-weighted)',
                  'revoked_ca': 'mean over issuing keys of the per-key share revoked (CA-weighted)',
                  'ca_always': 'share of issuing keys that revoked every early-replaced manifest EE in the week',
                  'ca_never': 'share of issuing keys that revoked none'},
        'histograms': {},
        'params': {'order': 'manifest_number (numerically), then this_update', 'evidence_days': [-1, 2],
                   'gap_hours': 3,
                   'direct_observation': 'successor first seen in the cycle right after the predecessor\'s last '
                                         'cycle; both cycles good in the ledger, fetching (a new object in some '
                                         'warehouse) and at most gap_hours apart (no ledger gap)'},
        # the shares wait for the evidence window (2 days after the successor): final at W+2, and masked by a
        # ledger gap up to 2 days after the week; the count of early replacements follows REPLACEMENT_RULE
        'rules': [{'stats': r'^(revoked|ca_)', 'publish_after': 1, 'final_after': 2, 'mask': 'gap',
                   'lookahead_days': 2},
                  REPLACEMENT_RULE],
        'rfc': ['RFC 9286 §4.2.1', 'RFC 9286 §5.1'],
        'method': (
            'Manifest replacements of one CA key (manifests ordered by manifestNumber, then thisUpdate) whose '
            "successor was issued before the predecessor's nextUpdate. RFC 9286 §4.2.1 and §5.1: a CA that issues "
            'a new manifest before the nextUpdate of the current one MUST revoke the EE certificate of the '
            "replaced manifest. Revoked: the predecessor's EE serial is on a CRL of the same key with thisUpdate "
            "from 1 day before to 2 days after the successor's thisUpdate; a replacement without any such CRL "
            'counts as not revoked (§4.2.1 asks for a new CRL with every early manifest). The shares are '
            'provisional until that window has been observed (final two weeks after the week). Era 2 only, and '
            "only directly observed replacements: the successor was first seen in the cycle right after the "
            "predecessor's last cycle, with no ledger gap between them and both cycles fetching new objects "
            '(daily snapshots, outages and cycles that only re-list the cache miss intermediate manifests). '
            'Pair-weighted and CA-weighted shares, and the shares of keys that revoked every / no '
            "replaced manifest EE in the week. Populations by the CA's own publication host.")},
    'late_reissue': {
        'def_version': 1, 'module': 'm_mft', 'title': 'Late re-issue of manifests and CRLs', 'unit': 'share',
        'kind': 'sample', 'classified_by': 'ca_host', 'windows': ('1w',), 'available_from': ERA2,
        'stats': {'replacements_mft': 'directly observed manifest replacements (count; value = n)',
                  'replacements_crl': 'directly observed CRL replacements (count; value = n)',
                  'late_mft': "share of manifest replacements issued after the predecessor's nextUpdate",
                  'late_crl': "share of CRL replacements issued after the predecessor's nextUpdate",
                  'late_mft_ca': 'share of issuing keys with at least one late manifest replacement',
                  'late_crl_ca': 'share of issuing keys with at least one late CRL replacement',
                  'headroom_mft_p50_h': 'median of predecessor nextUpdate - successor thisUpdate, manifests, h',
                  'headroom_crl_p50_h': 'median of predecessor nextUpdate - successor thisUpdate, CRLs, h'},
        'histograms': {},
        'params': {'order': {'mft': 'manifest_number (numerically), then this_update',
                             'crl': 'crl_number (numerically), then this_update'},
                   'gap_hours': 3, 'quantile': 'disc',
                   'direct_observation': 'successor first seen in the cycle right after the predecessor\'s last '
                                         'cycle; both cycles good in the ledger, fetching (a new object in some '
                                         'warehouse) and at most gap_hours apart (no ledger gap)'},
        'rules': [REPLACEMENT_RULE],
        'rfc': ['RFC 9286 §4.2.1', 'RFC 9286 §5.2'],
        'method': (
            'Directly observed replacements (era 2) of the manifests and of the CRLs of one CA key (ordered by '
            'manifestNumber or crlNumber, then thisUpdate): the share '
            "whose successor's thisUpdate is later than the predecessor's nextUpdate, i.e. the object went stale "
            'before it was replaced (RFC 9286 §4.2.1: a new manifest MUST be issued before nextUpdate; §5.2), '
            'and the median headroom (predecessor nextUpdate - successor thisUpdate) in hours. Only replacements '
            "whose successor was first seen in the cycle right after the predecessor's last cycle count, both "
            'cycles fetching new objects: across our own collection outages, and across cycles that only '
            're-listed the cache (2026-10-09..10-12), the intermediate objects were never captured, and '
            "replacements would look late. Populations by the CA's own publication host.")},
    'resign_consumed': {
        'def_version': 1, 'module': 'm_pairs', 'title': 'Validity consumed at re-signing', 'unit': 'fraction',
        'kind': 'sample', 'classified_by': 'signer_ee_host', 'windows': ('1w', '13w', '52w'),
        'available_from': None,
        'stats': {'pairs': 'pairs in the window (count; value = n)',
                  'consumed_p10': '10th percentile of validity consumed', 'consumed_p50': 'median validity consumed',
                  'consumed_p90': '90th percentile of validity consumed',
                  'consumed_p50_ca': 'CA-weighted: median over issuing keys of the per-key median',
                  'share_ge075': 'share of pairs with consumed >= 0.75',
                  'share_gt100': 'share of pairs re-signed after the predecessor expired (consumed > 1)',
                  'interval_p10_d': '10th percentile of the re-sign interval, days',
                  'interval_p50_d': 'median re-sign interval, days',
                  'interval_p90_d': '90th percentile of the re-sign interval, days',
                  'pairs_incl_transient': 'era 2: pairs including transient predecessors (count)',
                  'consumed_p50_incl_transient': 'era 2: median including transient predecessors',
                  'share_ge075_incl_transient': 'era 2: share >= 0.75 including transient predecessors'},
        'histograms': {'consumed': {'desc': '0.01 bins of validity consumed from 0 to 1.5', 'windows': ('1w',)}},
        'params': {'consumed_range': [0, 1.5], 'require_na_gt_nb': True, 'daily_instant_utc': '04:00',
                   'archive_until': '2026-04-14', 'chain_break_days': 30, 'quantile': 'cont',
                   'ca_weight': 'median of per-key medians'},
        'rules': [LATE_ARRIVAL_RULE],
        'rfc': ['RFC 6487 §4.6', 'RFC 9582', 'RFC 9589'],
        'method': (
            'The re-sign pairs of roa_ee_revocation with EE notAfter > notBefore and consumed in [0, 1.5], '
            "where consumed = (successor signing time - predecessor EE notBefore) / (predecessor EE notAfter - "
            'notBefore) and the interval is successor signing time - predecessor signing time, in days. Pairs '
            'whose predecessor was never present at a daily sampling instant (04:00 UTC) are left out, so the '
            'transient objects that 20-minute collection sees (replaced within hours) do not count; era-1 '
            'objects meet the rule by construction. The era-2 values with them are the *_incl_transient '
            'statistics. No RFC prescribes a re-signing cadence; consumed > 1 means the ROA was re-signed after '
            'its predecessor had expired, a lapse relying parties can see. Weekly values follow each CA\'s '
            'schedule (RIPE NCC hosted re-signs in one January batch), so the 13- and 52-week windows are the '
            'stable signal, except that a 52-week window that contains no 1 January (the weeks of 2012-12-24, '
            '2017-12-25 and 2023-12-25; next 2028-12-25) holds no January batch, and its values shift for that '
            "one week. Populations by the host of the predecessor's signer EE certificate.")},
    'ca_cert_lifetime': {
        'def_version': 1, 'module': 'm_ca', 'title': 'CA certificate lifetime', 'unit': 'days',
        'kind': 'sample', 'classified_by': 'issuer_host', 'windows': ('1w', '13w', '52w', 'end'),
        'available_from': None,
        'stats': {'certs': 'newly issued certificates after the per-key dedup (count; value = n)',
                  'duration_p10_d': '10th percentile of notAfter - notBefore, days (quantile_disc)',
                  'duration_p50_d': 'median notAfter - notBefore, days (quantile_disc: an observed duration)',
                  'duration_p90_d': '90th percentile of notAfter - notBefore, days (quantile_disc)',
                  'modal_not_after': 'most common notAfter date (UTC; ties: the earliest), as the number YYYYMMDD',
                  'modal_not_after_share': 'share of certificates with the modal notAfter date',
                  'live_certs': "window 'end': CA certificates present at the week's instant (count; value = n)",
                  'live_expiring_30d': "window 'end': share of live CA certificates whose notAfter is within 30 "
                                       'days of the instant (already expired ones included)',
                  'live_expiring_90d': "window 'end': share of live CA certificates whose notAfter is within 90 "
                                       'days of the instant (already expired ones included)'},
        'histograms': {'duration_d': {'desc': '7-day bins of notAfter - notBefore from 0 to 4,200 days, then '
                                              'one overflow bin [4200, NULL)', 'windows': ('1w',)}},
        'params': {'dedup': 'latest certificate per ski in each week (largest notBefore, then cert_id)',
                   'non_root': 'is_ca_flag AND aki IS NOT NULL AND aki <> ski', 'validity': 'not_after > not_before',
                   'observed': "a presence run in the RIR's warehouse that starts by as_of",
                   'issuer': 'ca_keys.host of the child aki (SIA host of the parent key\'s newest certificate)',
                   'quantile': 'disc', 'n_ca': 'distinct certified keys (ski), not the issuing key',
                   'hist_bin_d': 7, 'hist_max_d': 4200, 'expiring_days': [30, 90],
                   'instant': "the week's last snapshot that the ledger marks good, not partial and with >= "
                              "0.98 x the median file count of its 7 days (else the last good one); present = a "
                              "presence run spanning it"},
        'rules': [LATE_ARRIVAL_RULE],
        'rfc': ['RFC 6487 §4', 'RFC 6489'],
        'method': (
            'Newly issued non-root CA certificates (CA flag set, aki present and different from ski, notAfter '
            'after notBefore) with notBefore in the window (UTC), held and observed by the RIR\'s warehouse, '
            'keeping the latest certificate per key (ski) in each week (era 2 sees every re-issue: NIC.br '
            're-issues its CA certificates about daily). Duration = notAfter - notBefore in days, percentiles as '
            'observed values (quantile_disc); also the most common notAfter date (value YYYYMMDD) and its share. '
            "Population by the issuer's class: the publication host of the parent CA certificate (parent ski = "
            "child aki). The headline is issuer class rir_hosted, the certificates the RIR's own CA issued; "
            "certificates issued by NIR CAs follow the NIR's policy (APNIC itself issues about 422 days, JPNIC, "
            'IDNIC and CNNIC about 365). n_ca counts the certified keys (one issuer stands behind a whole '
            'population). Issuers with one fixed expiry date for every certificate (AFRINIC 2027-03-31, RIPE NCC '
            'hosted 2027-07-01) make the 1-week duration fall by about 7 days per week; the 52-week window is the '
            'stable value (close to a calendar year, but in UTC ISO weeks). These issuers re-issue in one batch '
            'just after 1 January 00:00 UTC, so a 52-week window that contains no 1 January (the weeks of '
            '2012-12-24, 2017-12-25 and 2023-12-25; next 2028-12-25) holds no batch, and its count and '
            "durations fall for that one week. At the week's end (window 'end', the "
            "week's last good snapshot): the shares of the CA certificates present then whose notAfter lies within "
            '30 and 90 days (certificates already expired but still in the era-2 cache included). RFC 6487 leaves '
            'the validity period to the issuer.')},
    # def_version 2 (2026-11-02): the 'verdicts' mask also masks a week with fewer than 90% of its expected
    # cycles good (an outage week whose remaining cycles all had verdicts was final); 1 (2026-10-29): first version
    'rp_rejected': {
        'def_version': 2, 'module': 'm_health', 'title': 'Objects rejected by the relying party',
        'unit': 'objects', 'kind': 'census', 'classified_by': 'per_type', 'windows': ('1w',),
        'available_from': VERDICTS,
        'stats': {'objects': 'distinct objects with a rejection verdict in the week (value = n)',
                  'ca_keys': 'distinct CA keys affected (value = n_ca; n = the objects): the issuing key (EE aki) '
                             'of a rejected signed object, the own key (ski) of a rejected CA certificate',
                  '^objects_type_[a-z0-9_]+$': 'distinct rejected objects of one type (roa, mft, cer, aspa, gbr; crl, '
                                               'unknown)',
                  '^objects_reason_[a-z0-9_]+$': "distinct rejected objects with one reason code ('-' as '_')",
                  '^objects_(?!type_|reason_)[a-z0-9]+_[a-z0-9_]+$': 'distinct rejected objects of one type with one '
                                                                    'reason: objects_<type>_<reason>, e.g. '
                                                                    'objects_mft_expired'},
        'histograms': {},
        'params': {'verdict_coverage_share': 0.9, 'week': 'ISO week of the cycle label (dataset_ts), UTC',
                   'zero_rows': "objects and ca_keys of populations 'all' and 'rir_hosted' in every week; other "
                                'rows only with n > 0 (a missing row is a zero)'},
        'rules': [VERDICT_RULE],
        'rfc': ['RFC 6487', 'RFC 9286 §6', 'RFC 9582'],
        'method': (
            'Objects that our relying party (rpki-client; release in the manifest) rejected while they were '
            'present, from its per-cycle verdicts (from 2026-06-20): distinct objects per week by object type, '
            'by reason and by both, and the distinct CA keys affected (the issuing key of a signed object, the '
            'own key of a rejected CA certificate). Objects without an error line have no verdict, and CRLs never '
            'get one (rpki-client reports an expired CRL against the manifest). A cycle without any rejected '
            'object writes no verdict row, so coverage comes from the ledger: a week whose verdict cycles are '
            'below 90% of its snapshot cycles is masked (at APNIC and AFRINIC, where many cycles have no '
            'rejection at all, this also masks clean weeks), and so is a week with fewer than 90% of its '
            'expected cycles (72 a day) good. After a gap in our own collection the cached '
            'manifests of many CAs have expired before the next fetch replaces them (the week of 2026-10-05). '
            "Populations: ROAs, ASPAs and GBRs by their signer EE certificate's host, manifests by the CA's own "
            'publication host, CA certificates by their own host (the CA whose certificate was rejected). Reason '
            'codes follow the wording of the rpki-client release.')},
    'live_inventory': {
        'def_version': 1, 'module': 'm_ca', 'title': 'Live CA keys and ROAs', 'unit': 'count', 'kind': 'census',
        'classified_by': 'own_host', 'windows': ('end',), 'available_from': None,
        'stats': {'ca_keys': "non-root CA keys with a certificate present at the week's instant (value = n = "
                             'n_ca; top1_share = 1/n)',
                  'roas': "ROA objects present at the week's instant (value = n; n_ca = their issuing keys)"},
        'histograms': {},
        'params': {'instant': "the week's last snapshot before its end (Monday 00:00 UTC) that the ledger marks "
                              'good, not partial and with >= 0.98 x the median file count of its 7 days, else the '
                              'last good one; none: value NULL',
                   'present': "a presence run of the RIR's warehouse spanning the instant; attributes from the "
                              'warehouse holding the head rows'},
        'rules': [END_RULE],
        'rfc': ['RFC 6487', 'RFC 9582'],
        'method': (
            "Context at the week's end, at the week's last good and complete snapshot (no repository missing; "
            "normally the archive's Sunday tree in era 1, the Sunday 23:40 cycle in era 2): live CA keys "
            "(non-root CA certificates present then, by the own publication host of the key's newest present "
            "certificate) and live ROA "
            "objects (by their signer EE certificate's host; n_ca = their issuing keys). Era-2 counts include "
            "files still in the rpki-client cache (stale or orphaned, e.g. expired ROAs); era 1 has the archive's "
            'validated tree only. A week without a good snapshot has no value.')},
    'stub_crl_count': {
        'def_version': 1, 'module': 'm_stub', 'title': 'CRLs per week (framework stub)', 'unit': 'count',
        'kind': 'sample', 'classified_by': 'ca_host', 'windows': ('1w',), 'available_from': None, 'stub': True,
        'stats': {'crls': 'CRLs with thisUpdate in the week (count; value = n)',
                  'ca_keys': 'distinct issuing keys of those CRLs'},
        'histograms': {'window_h': {'desc': '24-hour bins of nextUpdate - thisUpdate from 0 to 192 h, then one '
                                            'overflow bin [192, NULL)', 'windows': ('1w',)}},
        'params': {}, 'rules': [WINDOWS_RULE], 'rfc': [],
        'method': ('A trivial count that exercises the framework end to end (warehouse read, CA-key populations, '
                   'status rules, state, publish). Not a scorecard metric; runs only when requested.')},
}

# Text of the manifest that applies to every metric.
COMMON_METHOD = {
    'periods': ('ISO weeks in UTC, Monday 00:00 to the next Monday 00:00; `week` is the Monday. Rolling '
                "windows (13w, 52w) end with the week; 'end' is a point in time at the week's end."),
    'populations': ('Per RIR: rir_hosted (the RIR\'s hosted CA service), rir_paas (its publication service for '
                    'member-run CAs), nir_hosted (National Internet Registry platforms), delegated (any other '
                    'host), unknown (no resolvable host) and all, plus one row per operator for nir_hosted and '
                    'rir_paas. Exact host names (refdata/hostmap.json, version in every row). The headline is '
                    'rir_hosted. A warehouse holds objects by publication host in era 2 (rpki_cycle.sh files each '
                    'host under one trust anchor), so `all` is "published on hosts filed under this RIR", not the '
                    "trust anchor's whole tree; RIR-hosted hosts are entirely under their own trust anchor."),
    'status': ('provisional: may still change; final: will not change (frozen); masked: our collection was '
               'incomplete for the period (observation ledger), the values are shown but must not be used; '
               'insufficient: fewer than 30 objects or fewer than 10 issuing keys, values shown for '
               'transparency. Every row carries n, n_ca (distinct issuing keys) and top1_share (the largest '
               "key's share of n). Consumers must check status."),
    'eras': ('Era 1 (until 2026-04-07): daily snapshots of the RIPE NCC RPKI repository archive (validated '
             'trees). Era 2 (from 2026-04-08): our own rpki-client 9.7 runs every 20 minutes, keeping the full '
             'cache. ISO weeks 2026-W14..W16 (2026-03-30..04-19) mix both and are flagged; rolling windows that '
             "span both eras have era 'mixed'."),
    'as_of': ('Input cut: the earliest of the five RIRs\' latest presence-run ends. Rows keep the as_of, '
              'def_version, hostmap_version and code_commit of the run that computed them; frozen rows are never '
              'recomputed unless the definition, the host map or the framework version changes.'),
}

# Compare mode (mode 'compare'): parameter overrides per metric_id that reproduce other published
# definitions of a metric, e.g. calendar years in a given time zone instead of UTC ISO weeks. Empty in this
# release.
COMPARE_MODE = {}


def registry(include_stub=True):
    return {k: v for k, v in METRICS.items() if include_stub or not v.get('stub')}


def default_metrics():
    """Metrics a run computes when none are named: every non-stub metric."""
    return [k for k, v in METRICS.items() if not v.get('stub')]


def params(metric_id, mode='scorecard'):
    p = dict(METRICS[metric_id].get('params') or {})
    if mode == 'compare':
        p.update(COMPARE_MODE.get(metric_id) or {})
    return p


def rule_for(metric_id, stat):
    """The status rule of one (metric, stat): the first rule whose 'stats' regex matches, else the default."""
    for r in METRICS[metric_id]['rules']:
        if r.get('stats') is None or re.search(r['stats'], stat):
            return r
    raise KeyError(f'{metric_id}: no status rule for stat {stat!r}')


def max_final_after(metric_id):
    """Weeks after which every row of a week of this metric is final (the week is 'closed')."""
    return max(r['final_after'] for r in METRICS[metric_id]['rules'])


def stat_allowed(metric_id, stat):
    for s in METRICS[metric_id]['stats']:
        if (s.startswith('^') and re.match(s, stat)) or s == stat:
            return True
    return False


def manifest_entries(metric_ids):
    """The registry as JSON for the manifest (order kept)."""
    out = {}
    for k in metric_ids:
        v = METRICS[k]
        out[k] = {f: v.get(f) for f in ('def_version', 'title', 'unit', 'kind', 'classified_by', 'windows',
                                        'available_from', 'stats', 'histograms', 'params', 'rules', 'rfc', 'method')}
        out[k]['windows'] = list(v['windows'])
        out[k]['min_n'], out[k]['min_n_ca'] = (MIN_N, MIN_N_CA) if v['kind'] == 'sample' else (None, None)
    return out


def check():
    """Consistency of the registry (run by the tests and at the start of every run)."""
    seen_windows = {'1w', '13w', '52w', 'end'}
    for k, v in METRICS.items():
        assert re.fullmatch(r'[a-z][a-z0-9_]*', k), k
        assert isinstance(v['def_version'], int) and v['def_version'] >= 1, k
        assert v['kind'] in ('sample', 'census'), k
        assert set(v['windows']) <= seen_windows and v['windows'], k
        assert v['rules'] and v['rules'][-1].get('stats') is None, f'{k}: the last rule must be the default'
        for r in v['rules']:
            assert r['mask'] in ('cycles', 'gap', 'end_gap', 'verdicts', None), k
            assert 1 <= r['publish_after'] <= r['final_after'], k
        for h, hv in v['histograms'].items():
            assert set(hv['windows']) <= set(v['windows']), (k, h)
        assert re.fullmatch(r'm_[a-z0-9_]+', v['module']), k
    return True
