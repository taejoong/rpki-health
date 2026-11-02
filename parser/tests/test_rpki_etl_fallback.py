from __future__ import annotations

import hashlib
import json
from base64 import b64decode
from pathlib import Path
from shutil import copy2

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from parse_crl import CRLParsingError, parse_crl
from rpki_etl import _parse_object, main


DER_CERT_BASE64 = (
    "MIIDBTCCAe2gAwIBAgIUAMEU4zeKdfInD5mxz2cIKwyJBtswDQYJKoZIhvcNAQELBQAwEjEQMA4GA1UE"
    "AwwHVGVzdCBDQTAeFw0yNTA5MjYxNzA1NDlaFw0yNjA5MjYxNzA1NDlaMBIxEDAOBgNVBAMMB1Rlc3Qg"
    "Q0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC39rkulh/CoZD/71ZbtKpFtUHhhnQyeDfd8"
    "2NxCqJpD7EyhemzmqfznC3rw6UEFkmAgkvmYC5pWfzfKA5hUxWPFmtLE+qGkpwoI2rO0rhCrQXD5Ob7dw"
    "k1o7Cltx/kD/dYmteMj5swk7S63oy1Tvfq+LCPaexwv2Vtz2RXm50sgJsxYOtuRL96FYqN6ds+WCteheL"
    "tPPxQZMx1LH3eZHjkdyANUp+ZIR02oqP7PNLIbDari0ZFc53XEOaFxxpub1OshkVniUuqsTRxKDahdKdS"
    "Co25jesuPfsU0Kd0EfNcnp3W0UrAmnJM9QZlWPTGtWfFVtQTFYGIzRuMNazVnhLFAgMBAAGjUzBRMB0GA"
    "1UdDgQWBBRt9TyHhBlF5oHRRR4gllc3+LWomDAfBgNVHSMEGDAWgBRt9TyHhBlF5oHRRR4gllc3+LWomD"
    "APBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAYnUAA9daCfBDrlkLgDgUdLx+UpWly1l4"
    "VtyGcQDDCHss8SSr0b1tZRL3W05yf/A+k/pl2O6X/BVSDfVVwEkMEhWPrnwvlMkQiAxGVpKKlZeiG+f2Co"
    "dbEwL/7cy4m28EDS1oSWd3yD5DZDqHeXD0f4dXJ1h8HkuLGs0biupV4dGsoQnxlkNSubMrJ9mcoinHZLJO"
    "xks/9/8Fn7Hknbckzj5zIXzOBNFYVCP48+Rvyin0cEIZrPsPWOsVOmIiojE1DNUVN6aAY3XOWC6WTBBONc"
    "Mtd6XUyMqqhOVibrgTe/X2IKyZfjbhPYV2DtbPvnixcB/zd92KKiGHMXjnVyM6f"
)

DER_CRL_BASE64 = (
    "MIIBazBVAgEBMA0GCSqGSIb3DQEBCwUAMBIxEDAOBgNVBAMMB1Rlc3QgQ0EXDTI1MDkyNjE3MDYwNFoXDT"
    "I1MTAyNjE3MDYwNFqgDzANMAsGA1UdFAQEAgIQADANBgkqhkiG9w0BAQsFAAOCAQEAt9b6pC7sgRiPu3st"
    "Iz3Ds97nPlGU5Q25dHspUe4Nuq6I9VsEKxT8cCPUHRrR67vR6hJmi8XN0gZq4N6wdrN1phDj338RniMyjp"
    "oBnSHWyf74T5mEX0TN+qN+c0ZJdFkwX5e2KwDnBwqr6uObca/ai7nodCEDo0hCAFjsyzNc/K2Ut7I8d7nQ"
    "lIuLGu3P3gIXQy/Bn2+VOo30bCDoHXB2sCd0rvmv2C/8/W6T/fDd0/xz4Tc43pInOEmy8TONJsCot5dtuM"
    "JcaDOajhLWGg6dSAB/ZR0FlTZeas4mIk7xg3QCOmQKnF7k2U8+wOCN2R2lqnu+FFYdF2Vva3YWSPiYig=="
)


def _write_der(path: Path, payload_base64: str) -> None:
    path.write_bytes(b64decode(payload_base64))


def test_truncated_crl_reports_recoverable_error(tmp_path: Path) -> None:
    truncated = b64decode(DER_CRL_BASE64)[:-8]
    bad_crl_path = tmp_path / "truncated.crl"
    bad_crl_path.write_bytes(truncated)

    with pytest.raises(CRLParsingError):
        parse_crl(truncated)

    result = _parse_object(("deadbeef", str(bad_crl_path), "CRL"))

    assert result.crl is None
    assert any("Failed to parse" in error for error in result.errors)


def test_parses_raw_certificate_and_crl(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    _write_der(input_dir / "test.cer", DER_CERT_BASE64)
    _write_der(input_dir / "test.crl", DER_CRL_BASE64)

    out_root = tmp_path / "out"
    kv_path = tmp_path / "kv.sqlite"

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--input",
            str(input_dir),
            "--dataset-ts",
            "2025-09-26 12:00:00",
            "--repo-id",
            "test-repo",
            "--tal-id",
            "test-tal",
            "--transport",
            "rsync",
            "--session-id",
            "session",
            "--serial",
            "123",
            "--out-root",
            str(out_root),
            "--kv",
            str(kv_path),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output


    cert_dir = out_root / "tables" / "table=certs"
    cert_paths = sorted(cert_dir.glob("is_ca=*/certs.parquet"))
    assert cert_paths, "Certificate partitions should be written"

    crl_table_path = out_root / "tables" / "table=crls" / "crls.parquet"


    cert_rows: list[dict] = []
    for cert_path in cert_paths:
        cert_rows.extend(pq.read_table(cert_path).to_pylist())
    assert crl_table_path.exists()

    
    crl_rows = pq.read_table(crl_table_path).to_pylist()

    assert any(row["subject_dn"] == "Test CA" for row in cert_rows)
    assert crl_rows and crl_rows[0]["issuer_dn"] == "Test CA"
    assert crl_rows[0]["crl_id"] == crl_rows[0]["object_id"]
    assert crl_rows[0]["repo_id"] == "test-repo"
    assert crl_rows[0]["tal_id"] == "test-tal"
    sample_cert = cert_rows[0]
    assert isinstance(sample_cert["cert_hash"], str)
    assert len(sample_cert["cert_hash"]) == 64
    if sample_cert["ski"] is not None:
        assert isinstance(sample_cert["ski"], str)
        assert all(c in "0123456789abcdef" for c in sample_cert["ski"])


def test_roa_records_include_ee_and_crl_revocations(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    sample_root = Path(__file__).resolve().parent.parent / "sample-crawl"
    roa_source = sample_root / "55fdfb6e-3332-3318-b7df-bae9c2dbd8cf.roa"
    crl_source = sample_root / "00621ba6-99e5-4a17-9087-51ef82e92263.crl"

    copy2(roa_source, input_dir / "sample.roa")
    copy2(crl_source, input_dir / "sample.crl")

    out_root = tmp_path / "out"
    kv_path = tmp_path / "kv.sqlite"

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--input",
            str(input_dir),

            "--dataset-ts",
            "2025-09-26 12:00:00",

            "--repo-id",
            "test-repo",
            "--tal-id",
            "test-tal",
            "--transport",
            "rsync",
            "--session-id",
            "session",
            "--serial",
            "123",
            "--out-root",
            str(out_root),
            "--kv",
            str(kv_path),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output

    table_root = out_root / "tables"


    roa_path = table_root / "table=roa_objects" / "roa_objects.parquet"
    roa_rows = pq.read_table(roa_path).to_pylist()
    assert roa_rows, "ROA objects should be emitted"
    roa = roa_rows[0]
    assert isinstance(roa["semantic_hash"], str) and len(roa["semantic_hash"]) == 64
    assert isinstance(roa["object_hash"], str) and len(roa["object_hash"]) == 64

    signatures_path = table_root / "table=object_signatures" / "object_signatures.parquet"
    signature_rows = pq.read_table(signatures_path).to_pylist()
    assert signature_rows, "Object signatures should be recorded"
    signature = signature_rows[0]
    assert signature["signer_cert_id"] is not None

    assert signature["object_type"] == "ROA"
    if signature["message_digest"] is not None:
        assert isinstance(signature["message_digest"], str)

    cert_dir = table_root / "table=certs"
    cert_rows: list[dict] = []
    for cert_path in cert_dir.glob("is_ca=*/certs.parquet"):

        cert_rows.extend(pq.read_table(cert_path).to_pylist())
    signer = next(row for row in cert_rows if row["cert_id"] == signature["signer_cert_id"])
    assert signer["is_ca_flag"] is False

    roas_snapshot_path = (
        table_root
        / "table=roas_signers_snapshot"
        / "roas_signers_snapshot.parquet"
    )
    roas_snapshot_rows = pq.read_table(roas_snapshot_path).to_pylist()
    assert roas_snapshot_rows, "Derived ROA signer snapshot should exist"
    first_snapshot = roas_snapshot_rows[0]
    assert first_snapshot["signer_cert_id"] == signature["signer_cert_id"]
    assert first_snapshot["dataset_ts"] == "2025-09-26 12:00:00"
    assert first_snapshot["signer_not_before"] is not None

    crl_path = table_root / "table=crls" / "crls.parquet"
    crl_rows = pq.read_table(crl_path).to_pylist()
    assert crl_rows, "CRL summary should exist"

    serials_path = table_root / "table=crl_serials" / "crl_serials.parquet"
    serial_rows = pq.read_table(serials_path).to_pylist()
    assert serial_rows, "CRL serials should be captured"
    assert crl_rows[0]["revoked_count"] == len(serial_rows)
    assert all(row["crl_id"] == crl_rows[0]["crl_id"] for row in serial_rows)
    assert crl_rows[0]["repo_id"] == "test-repo"
    assert crl_rows[0]["tal_id"] == "test-tal"
    assert all(row["serial_dec"] for row in serial_rows)
    assert all(isinstance(row["serial_bin"], str) for row in serial_rows)


def test_reusing_kv_across_tals_assigns_distinct_cert_ids(tmp_path: Path) -> None:
    sample_root = Path(__file__).resolve().parent.parent / "sample-crawl"
    first_roa = sample_root / "55fdfb6e-3332-3318-b7df-bae9c2dbd8cf.roa"
    second_roa = sample_root / "92e9ca67-b54e-3009-b93c-5a603a77d633.roa"

    kv_path = tmp_path / "kv.sqlite"

    def run_for_tal(tal_suffix: str, roa_path: Path) -> tuple[list[dict], list[dict]]:
        input_dir = tmp_path / f"input-{tal_suffix}"
        input_dir.mkdir()
        copy2(roa_path, input_dir / "sample.roa")

        out_root = tmp_path / f"out-{tal_suffix}"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--input",
                str(input_dir),
                "--dataset-ts",
                f"2025-09-2{tal_suffix} 12:00:00",
                "--repo-id",
                f"repo-{tal_suffix}",
                "--tal-id",
                f"tal-{tal_suffix}",
                "--transport",
                "rsync",
                "--session-id",
                f"session-{tal_suffix}",
                "--serial",
                f"12{tal_suffix}",
                "--out-root",
                str(out_root),
                "--kv",
                str(kv_path),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        table_root = out_root / "tables"
        cert_rows: list[dict] = []
        for cert_path in (table_root / "table=certs").glob("is_ca=*/certs.parquet"):
            cert_rows.extend(pq.read_table(cert_path).to_pylist())
        assert cert_rows, "certificate rows should be written"

        sig_path = table_root / "table=object_signatures" / "object_signatures.parquet"
        signatures = pq.read_table(sig_path).to_pylist()
        assert signatures, "object signatures should be emitted"

        return cert_rows, signatures

    first_cert_rows, first_signatures = run_for_tal("1", first_roa)
    second_cert_rows, second_signatures = run_for_tal("2", second_roa)

    first_ids = {row["cert_id"] for row in first_cert_rows}
    second_ids = {row["cert_id"] for row in second_cert_rows}

    assert first_ids, "first TAL should produce certificates"
    assert second_ids, "second TAL should produce certificates"
    assert first_ids.isdisjoint(second_ids)

    for signature in first_signatures:
        signer_id = signature["signer_cert_id"]
        if signer_id is not None:
            assert signer_id in first_ids

    for signature in second_signatures:
        signer_id = signature["signer_cert_id"]
        if signer_id is not None:
            assert signer_id in second_ids


def test_validity_map_emits_object_validity_rows(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    sample_root = Path(__file__).resolve().parent.parent / "sample-crawl"
    roa_source = sample_root / "55fdfb6e-3332-3318-b7df-bae9c2dbd8cf.roa"
    copy2(roa_source, input_dir / "sample.roa")

    # The ETL keys validity on the content SHA-256 of the object's raw bytes.
    object_hash = hashlib.sha256(roa_source.read_bytes()).hexdigest()
    validity_map_path = tmp_path / "validity-map.json"
    validity_map_path.write_text(
        json.dumps({object_hash: {"state": 2, "reason": "expired"}})
    )

    out_root = tmp_path / "out"
    kv_path = tmp_path / "kv.sqlite"

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--input",
            str(input_dir),
            "--dataset-ts",
            "2025-09-26 12:00:00",
            "--repo-id",
            "test-repo",
            "--tal-id",
            "test-tal",
            "--transport",
            "rsync",
            "--session-id",
            "session",
            "--serial",
            "123",
            "--out-root",
            str(out_root),
            "--kv",
            str(kv_path),
            "--validity-map",
            str(validity_map_path),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output

    table_root = out_root / "tables"
    validity_path = table_root / "table=object_validity" / "object_validity.parquet"
    assert validity_path.exists(), "object_validity table should be written with a map"
    validity_rows = pq.read_table(validity_path).to_pylist()
    assert len(validity_rows) == 1
    row = validity_rows[0]
    assert row["state"] == 2
    assert row["reason_code"] == "expired"
    assert row["dataset_ts"] == "2025-09-26 12:00:00"
    assert row["tal_id"] == "test-tal"

    # The rejected object_id must join to the present ROA head row.
    roa_path = table_root / "table=roa_objects" / "roa_objects.parquet"
    roa_rows = pq.read_table(roa_path).to_pylist()
    assert row["object_id"] in {r["object_id"] for r in roa_rows}

