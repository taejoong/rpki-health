"""Unit tests for the time-varying object_validity row builder."""

from rpki_etl import _build_object_validity_rows


def test_only_present_invalid_objects_are_emitted():
    # validity_map carries every object rpki-client rejected this cycle.
    validity_map = {
        "hash_present_a": (2, "expired"),
        "hash_present_b": (2, "no-manifest"),
        "hash_absent": (2, "stale-mft"),  # rejected but not in this cycle's cache
    }
    # hash_to_id holds the full present set (valid + invalid) for the cycle.
    hash_to_id = {
        "hash_present_a": 10,
        "hash_present_b": 20,
        "hash_valid_c": 30,  # present and valid -> no row
    }

    rows = _build_object_validity_rows(
        validity_map, hash_to_id, "2026-06-19 01:00:00", "afrinic"
    )

    by_id = {row["object_id"]: row for row in rows}
    # Absent rejected object and present-valid object both excluded.
    assert set(by_id) == {10, 20}
    assert by_id[10]["state"] == 2
    assert by_id[10]["reason_code"] == "expired"
    assert by_id[20]["reason_code"] == "no-manifest"
    assert all(row["dataset_ts"] == "2026-06-19 01:00:00" for row in rows)
    assert all(row["tal_id"] == "afrinic" for row in rows)


def test_empty_validity_map_emits_nothing():
    assert _build_object_validity_rows({}, {"h": 1}, "2026-06-19 01:00:00", "ripe") == []
