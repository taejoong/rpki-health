from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

pytest.importorskip("tqdm")
pytest.importorskip("pyarrow")

import rpki_etl


def test_sample_crawl_end_to_end(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    sample_dir = repo_root / "sample-crawl"

    if not sample_dir.exists():  # pragma: no cover - dataset provided externally
        pytest.skip("sample-crawl dataset not available")

    out_root = tmp_path / "warehouse"
    out_root.mkdir(parents=True, exist_ok=True)

    runner = CliRunner()
    result = runner.invoke(
        rpki_etl.main,
        [
            "--input",
            str(sample_dir),
            "--dataset-ts",
            "2025-09-25 00:00:00",
            "--repo-id",
            "sample.repo",
            "--tal-id",
            "sample.tal",
            "--transport",
            "rrdp",
            "--session-id",
            "abc",
            "--serial",
            "123",
            "--out-root",
            str(out_root),
            "--kv",
            str(out_root / "kv.sqlite"),
            "--threads",
            "1",
            "--dry-run",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
