import logging
from collections import Counter
from pathlib import Path

from rpki_etl import HashedObject, _log_detected_object_mix, _tally_detected_object_types


def _make_item(object_type: str | None) -> HashedObject:
    return HashedObject(
        path=Path("dummy"),
        object_hash="hash",
        detected_type=object_type,
        size=0,
    )


def test_tally_detected_object_types_normalizes_case_and_nulls() -> None:
    items = [
        _make_item("roa"),
        _make_item("ROA"),
        _make_item("mft"),
        _make_item("cer"),
        _make_item(None),
    ]

    counts = _tally_detected_object_types(items)

    assert counts == Counter({"ROA": 2, "MFT": 1, "CER": 1, "UNKNOWN": 1})


def test_log_detected_object_mix_includes_breakdown(caplog) -> None:
    items = [
        _make_item("roa"),
        _make_item("mft"),
        _make_item("gbr"),
        _make_item("asa"),
    ]

    with caplog.at_level(logging.INFO, logger="rpki_etl"):
        _log_detected_object_mix(items)

    summary_message = "Detected 1 ROAs, 1 MFTs, and 2 other objects before hashing (4 total)"
    assert summary_message in caplog.text
    assert "Other object breakdown: ASA: 1, GBR: 1" in caplog.text
