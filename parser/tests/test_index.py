from index import MembershipIndex, PresenceRuns


def test_membership_index_sorted_unique():
    index = MembershipIndex(object_ids=[])
    index.add(1)
    index.add(2)
    index.add(2)
    assert index.object_ids == [1, 2]


def test_presence_runs_coalesce():
    runs = PresenceRuns()
    runs.add_observation(10)
    runs.add_observation(11)
    runs.add_observation(13)
    assert [(i.start_day_id, i.end_day_id) for i in runs.intervals] == [(10, 11), (13, 13)]
