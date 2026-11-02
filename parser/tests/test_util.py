from util import (
    PrefixAuthorisation,
    build_prefix_authorizations,
    canonicalize_ip_prefix,
    coalesce_day_intervals,
    dataset_day_to_id,
)


def test_canonicalize_ip_prefix_ipv4():
    assert canonicalize_ip_prefix("010.000.000.000/8") == "10.0.0.0/8"


def test_build_prefix_authorizations_sorted_and_deduped():
    auths = build_prefix_authorizations(
        [
            (4, "192.0.2.0/24", 24),
            (6, "2001:0db8::/32", 32),
            (4, "192.0.2.0/24", 24),
        ]
    )
    assert [a.to_tuple() for a in auths] == [
        (4, "192.0.2.0/24", 24),
        (6, "2001:db8::/32", 32),
    ]


def test_coalesce_day_intervals():
    intervals = coalesce_day_intervals([], 10)
    assert intervals[0].start_day_id == 10
    assert intervals[0].end_day_id == 10

    intervals = coalesce_day_intervals(intervals, 11)
    assert intervals[-1].start_day_id == 10
    assert intervals[-1].end_day_id == 11

    intervals = coalesce_day_intervals(intervals, 15)
    assert intervals[-1].start_day_id == 15
    assert intervals[-1].end_day_id == 15


def test_dataset_day_to_id():
    assert dataset_day_to_id("1970-01-01") == 0
    assert dataset_day_to_id("1970-01-02") == 1
