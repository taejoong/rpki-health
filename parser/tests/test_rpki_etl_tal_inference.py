from pathlib import Path

from rpki_etl import _infer_tal_id_from_path


def test_infer_tal_id_from_nested_directory() -> None:
    path = Path("/ssd-data/rpki-objects/unpacked/afrinic/afrinic.tal/2025/02")
    assert _infer_tal_id_from_path(path) == "afrinic"


def test_infer_tal_id_from_path_without_marker() -> None:
    path = Path("/ssd-data/rpki-objects/unpacked/ripencc")
    assert _infer_tal_id_from_path(path) is None
