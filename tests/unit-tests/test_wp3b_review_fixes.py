"""Tests for the WP3b review fixes: importers, metadata collector, CLI and outputs package."""

import importlib.util
import json
import logging
import os
import random
import statistics
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from forklift.cli import _value_statistics_kwargs, main
from forklift.engine.exceptions import ProcessingError
from forklift.engine.importers import redact_connection_string, scrub_secrets
from forklift.engine.importers.excel_importer import ExcelImporter
from forklift.engine.importers.output_location import (
    OutputLocation,
    discard_partial_output,
    unique_stem,
    validate_output_stem,
)
from forklift.engine.importers.sql_importer import SqlImporter
from forklift.metadata.output_metadata_collector import (
    MetadataWriteError,
    OutputMetadataCollector,
)
from forklift.outputs.manifest import ManifestGenerator
from forklift.outputs.metadata import MetadataGenerator

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"

PASSWORD = "Sup3r-S3cret!pw"
CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};Server=db.example.com,1433;Database=sales;"
    f"Uid=etl_user;Pwd={PASSWORD};Encrypt=yes"
)


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


@pytest.fixture
def s3_bucket(monkeypatch):
    """A moto S3 bucket named ``bkt`` with fake credentials in the environment."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bkt")
        yield client


def s3_keys(client):
    return sorted(o["Key"] for o in client.list_objects_v2(Bucket="bkt").get("Contents", []))


SQL_SCHEMA = pa.schema([("id", pa.int64()), ("name", pa.string())])


def sql_batch(values=(1, 2)):
    return pa.record_batch([list(values), [f"n{v}" for v in values]], schema=SQL_SCHEMA)


@contextmanager
def patched_sql(tables, data):
    """Patch the SQL schema importer/input handler; ``data[table]`` is a callable -> batches."""
    with patch("forklift.schema.sql_schema_importer.SqlSchemaImporter") as importer_cls:
        with patch("forklift.inputs.sql.SqlInputHandler") as handler_cls:
            importer_cls.return_value.get_table_list.return_value = tables
            handler = Mock()
            handler_cls.return_value = handler
            handler.__enter__ = Mock(return_value=handler)
            handler.__exit__ = Mock(return_value=None)
            handler.get_table_schema.return_value = SQL_SCHEMA
            handler.read_table_data.side_effect = lambda schema, table: data[table]()
            yield handler


def good_table():
    return [sql_batch((1, 2, 3))]


def failing_table():
    """Yield one batch (so a file is already open) and then fail with a PII-bearing message."""
    yield sql_batch((9, 9))
    raise RuntimeError("cannot convert value 'CELL-VALUE-123-SSN' to int")


# ---------------------------------------------------------------------------------------------
# 1. Connection string redaction
# ---------------------------------------------------------------------------------------------


class TestRedactConnectionString:
    def test_password_removed_but_provenance_kept(self):
        redacted = redact_connection_string(CONNECTION_STRING)

        assert PASSWORD not in redacted
        assert "Pwd=***" in redacted
        for kept in ("ODBC Driver 17 for SQL Server", "db.example.com,1433", "Database=sales"):
            assert kept in redacted
        assert "Uid=etl_user" in redacted

    @pytest.mark.parametrize(
        "key",
        ["PWD", "pwd", "Password", "PASSWD", "Pass", "PassPhrase", "Secret", "ClientSecret"]
        + ["Token", "AccessToken", "Key", "AccountKey", "api_key", "SSLKey"],
    )
    def test_secret_keys_are_redacted_case_insensitively(self, key):
        redacted = redact_connection_string(f"Server=h;{key}=hunter2;Database=d")

        assert "hunter2" not in redacted
        assert redacted == f"Server=h;{key}=***;Database=d"

    def test_braced_value_with_semicolon(self):
        redacted = redact_connection_string("Server=h;Pwd={a;b};Database=d")

        assert redacted == "Server=h;Pwd=***;Database=d"

    def test_braced_value_with_escaped_closing_brace(self):
        redacted = redact_connection_string("Pwd={ab}}cd;e};Server=h")

        assert redacted == "Pwd=***;Server=h"

    def test_unterminated_brace_fails_closed(self):
        redacted = redact_connection_string("Server=h;Pwd={never;closed;Database=d")

        assert "never" not in redacted and "closed" not in redacted

    def test_spaces_around_key_and_value(self):
        redacted = redact_connection_string("Server = h ; PWD = hunter2 ;Database=d")

        assert "hunter2" not in redacted
        assert "Database=d" in redacted

    def test_non_secret_keys_untouched(self):
        text = "Driver={SQLite3};Database=/tmp/x.db;Trusted_Connection=yes"

        assert redact_connection_string(text) == text

    def test_url_userinfo_and_query_secrets(self):
        redacted = redact_connection_string(
            "postgresql://alice:p%40ss@db.example.com:5432/sales?sslmode=require&password=zzz"
        )

        assert "p%40ss" not in redacted and "zzz" not in redacted
        assert redacted.startswith("postgresql://alice:***@db.example.com:5432/sales")
        assert "sslmode=require" in redacted

    def test_url_without_password_is_unchanged(self):
        text = "postgresql://alice@db.example.com/sales"

        assert redact_connection_string(text) == text

    def test_none_passes_through(self):
        assert redact_connection_string(None) is None

    def test_scrub_secrets_removes_connection_string_and_values_from_messages(self):
        message = f"Login failed for {CONNECTION_STRING} (password {PASSWORD} rejected)"

        scrubbed = scrub_secrets(message, CONNECTION_STRING)

        assert PASSWORD not in scrubbed
        assert "db.example.com,1433" in scrubbed

    def test_scrub_secrets_handles_braced_secret_in_unbraced_form(self):
        scrubbed = scrub_secrets("bad password a;bcd", "Server=h;Pwd={a;bcd}")

        assert "a;bcd" not in scrubbed


# ---------------------------------------------------------------------------------------------
# 1/2. SQL importer: secrets on disk, failures, success reporting
# ---------------------------------------------------------------------------------------------


class TestSqlImporterSecrets:
    def test_metadata_json_never_contains_the_password(self, tmp_path):
        out = tmp_path / "out"
        with patched_sql([("dbo", "users", "users")], {"users": good_table}):
            SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        metadata = json.loads((out / "metadata.json").read_text())
        recorded = metadata["input_config"]["connection_string"]
        assert "Pwd=***" in recorded and "db.example.com,1433" in recorded
        for path in out.iterdir():
            assert PASSWORD.encode() not in path.read_bytes()

    def test_failure_logs_do_not_leak_the_password(self, tmp_path, caplog):
        out = tmp_path / "out"
        with patched_sql([("dbo", "users", "users")], {}) as handler:
            handler.__enter__ = Mock(
                side_effect=ConnectionError(f"Failed to connect: {CONNECTION_STRING}")
            )
            with caplog.at_level(logging.DEBUG):
                with pytest.raises(ConnectionError):
                    SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        assert PASSWORD not in caplog.text
        assert "db.example.com,1433" in caplog.text


class TestSqlImporterTableFailures:
    TABLES = [("dbo", "users", "users"), ("dbo", "orders", "orders")]

    def test_failed_table_raises_and_leaves_no_partial_file(self, tmp_path, caplog):
        out = tmp_path / "out"
        data = {"users": good_table, "orders": failing_table}
        with patched_sql(self.TABLES, data):
            with caplog.at_level(logging.DEBUG):
                with pytest.raises(ProcessingError, match="1 of 2 tables failed") as exc_info:
                    SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        # The truncated parquet of the failed table is gone, the good one is complete
        assert not (out / "orders.parquet").exists()
        assert pq.read_table(out / "users.parquet").num_rows == 3

        # Failure is an error, never an "invalid row", and is attached to the partial results
        results = exc_info.value.results
        assert results.invalid_rows == 0
        assert results.total_rows == results.valid_rows == 3
        assert results.output_files == [str(out / "users.parquet")]
        assert results.errors == ["dbo.orders: RuntimeError"]

        # No data values in the exception, the results or the logs
        assert "CELL-VALUE-123-SSN" not in str(exc_info.value)
        assert "CELL-VALUE-123-SSN" not in caplog.text
        assert "CELL-VALUE-123-SSN" not in "".join(results.errors)

    def test_metadata_records_failed_table_without_listing_it_as_output(self, tmp_path):
        out = tmp_path / "out"
        data = {"users": good_table, "orders": failing_table}
        with patched_sql(self.TABLES, data):
            with pytest.raises(ProcessingError):
                SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        metadata = json.loads((out / "metadata.json").read_text())
        assert metadata["failed_tables"] == [
            {"schema": "dbo", "table": "orders", "error_type": "RuntimeError"}
        ]
        assert metadata["output_files"] == [str(out / "users.parquet")]
        assert metadata["processing_summary"]["total_tables_failed"] == 1
        assert metadata["processing_summary"]["total_rows"] == 3
        assert "CELL-VALUE-123-SSN" not in json.dumps(metadata)

    def test_remaining_tables_are_still_processed_after_a_failure(self, tmp_path):
        out = tmp_path / "out"
        tables = [("dbo", "orders", "orders"), ("dbo", "users", "users")]
        with patched_sql(tables, {"users": good_table, "orders": failing_table}):
            with pytest.raises(ProcessingError):
                SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        assert pq.read_table(out / "users.parquet").num_rows == 3

    def test_continue_on_error_returns_results_with_errors(self, tmp_path):
        out = tmp_path / "out"
        data = {"users": good_table, "orders": failing_table}
        with patched_sql(self.TABLES, data):
            results = SqlImporter.import_sql(
                CONNECTION_STRING, out, "schema.json", continue_on_error=True
            )

        assert results.errors == ["dbo.orders: RuntimeError"]
        assert results.invalid_rows == 0
        assert results.output_files == [str(out / "users.parquet")]
        assert not (out / "orders.parquet").exists()

    def test_failure_before_the_writer_exists_is_also_recorded(self, tmp_path):
        out = tmp_path / "out"
        with patched_sql(self.TABLES, {"users": good_table, "orders": good_table}) as handler:
            handler.get_table_schema.side_effect = [SQL_SCHEMA, KeyError("dbo.orders")]
            with pytest.raises(ProcessingError, match="dbo.orders \\(KeyError\\)"):
                SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        assert not (out / "orders.parquet").exists()

    def test_writers_with_abort_are_aborted_not_closed(self, tmp_path):
        out = tmp_path / "out"
        writer = Mock()
        writer.write_batch.side_effect = RuntimeError("disk full")
        with patched_sql([("dbo", "users", "users")], {"users": good_table}):
            with patch(
                "forklift.engine.importers.sql_importer.create_parquet_writer",
                return_value=writer,
            ):
                with pytest.raises(ProcessingError):
                    SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        writer.abort.assert_called_once_with()
        writer.close.assert_not_called()  # close() on an S3 writer would upload the partial file

    def test_discard_partial_output_removes_the_file_when_writer_has_no_abort(self, tmp_path):
        target = tmp_path / "t.parquet"
        writer = pq.ParquetWriter(target, SQL_SCHEMA)
        writer.write_batch(sql_batch())

        discard_partial_output(writer, target)

        assert not target.exists()
        assert writer.is_open is False

    def test_all_good_run_still_reports_success(self, tmp_path):
        out = tmp_path / "out"
        with patched_sql(self.TABLES, {"users": good_table, "orders": good_table}):
            results = SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        assert results.errors == []
        assert results.total_rows == 6
        metadata = json.loads((out / "metadata.json").read_text())
        assert metadata["failed_tables"] == []


# ---------------------------------------------------------------------------------------------
# 3. Output path safety and unique names
# ---------------------------------------------------------------------------------------------


class TestOutputPathSafety:
    @pytest.mark.parametrize(
        "name",
        ["../../escaped", "..", ".", "sub/dir", "a\\b", "/abs/path", "C:evil", "", "  ", "x\x00y"]
        + [" lead", "trail "],
    )
    def test_validate_output_stem_rejects_non_stems(self, name):
        with pytest.raises(ValueError):
            validate_output_stem(name)

    @pytest.mark.parametrize("name", ["users", "sales_orders", "Order Details", "my.table", "ü"])
    def test_validate_output_stem_accepts_plain_stems(self, name):
        assert validate_output_stem(name) == name

    @pytest.mark.parametrize("output_name", ["../../escaped", "../up", "/tmp/abs_out"])
    def test_sql_output_name_cannot_escape_the_output_directory(self, tmp_path, output_name):
        out = tmp_path / "a" / "b" / "out"
        tables = [("dbo", "users", output_name)]
        with patched_sql(tables, {"users": good_table}) as handler:
            with pytest.raises(ValueError):
                SqlImporter.import_sql(CONNECTION_STRING, out, "schema.json")

        # Rejected before the database was touched, nothing written anywhere
        handler.__enter__.assert_not_called()
        assert not list(tmp_path.rglob("*.parquet"))
        assert not Path("/tmp/abs_out.parquet").exists()

    def test_symlink_pointing_outside_is_rejected(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        outside = tmp_path / "outside.parquet"
        (out / "users.parquet").symlink_to(outside)

        with pytest.raises(ValueError, match="outside the output directory"):
            OutputLocation(out).target("users")

    def test_two_tables_cannot_share_an_output_file(self, tmp_path):
        tables = [("a", "users", "same"), ("b", "orders", "Same")]
        with patched_sql(tables, {}):
            with pytest.raises(ValueError, match="same output file name"):
                SqlImporter.import_sql(CONNECTION_STRING, tmp_path / "out", "schema.json")

    def test_unique_stem_is_deterministic_and_case_insensitive(self):
        used = set()

        names = ["a", "A", "a", "a_2"]

        # "A" collides case-insensitively; the later literal "a_2" collides with a generated name
        assert [unique_stem(n, used) for n in names] == ["a", "A_2", "a_3", "a_2_2"]


def excel_handler_returning(sheets):
    """Patch the Excel input handler so ``process_sheets`` yields ``(name, table)`` pairs."""
    patcher = patch("forklift.inputs.excel.ExcelInputHandler")
    handler_cls = patcher.start()
    handler = Mock()
    handler_cls.return_value = handler
    handler.get_sheet_info.return_value = {
        "sheet_count": len(sheets),
        "engine": "openpyxl",
        "sheet_names": [name for name, _ in sheets],
    }
    handler.process_sheets.return_value = list(sheets)
    return patcher


@pytest.fixture
def workbook(tmp_path):
    path = tmp_path / "book.xlsx"
    path.write_bytes(b"placeholder")  # the handler is patched; the file only has to exist
    return path


class TestExcelOutputNames:
    def test_sanitising_collisions_do_not_overwrite_each_other(self, tmp_path, workbook):
        names = ["Q1/Q2", "Q1\\Q2", "Q1:Q2", "Data.", "Data"]
        sheets = [(name, pa.table({"v": [i, i + 1]})) for i, name in enumerate(names)]
        patcher = excel_handler_returning(sheets)
        try:
            results = ExcelImporter.import_excel(workbook, tmp_path / "out")
        finally:
            patcher.stop()

        written = sorted(Path(p).name for p in results.output_files)
        assert written == [
            "book_Data.parquet",
            "book_Data_2.parquet",
            "book_Q1_Q2.parquet",
            "book_Q1_Q2_2.parquet",
            "book_Q1_Q2_3.parquet",
        ]
        assert len(list((tmp_path / "out").glob("*.parquet"))) == 5
        assert results.total_rows == 10
        # first occurrence keeps the plain name, later ones are numbered in workbook order
        assert pq.read_table(tmp_path / "out" / "book_Q1_Q2.parquet")["v"].to_pylist() == [0, 1]
        assert pq.read_table(tmp_path / "out" / "book_Q1_Q2_3.parquet")["v"].to_pylist() == [2, 3]
        assert pq.read_table(tmp_path / "out" / "book_Data_2.parquet")["v"].to_pylist() == [4, 5]

    def test_sheet_names_cannot_traverse_directories(self, tmp_path, workbook):
        sheets = [(name, pa.table({"v": [1]})) for name in ["../../evil", "..", "a/../../b"]]
        patcher = excel_handler_returning(sheets)
        try:
            results = ExcelImporter.import_excel(workbook, tmp_path / "x" / "out")
        finally:
            patcher.stop()

        out = (tmp_path / "x" / "out").resolve()
        assert len(results.output_files) == 3
        for path in results.output_files:
            assert Path(path).resolve().parent == out
        assert sorted(p.name for p in tmp_path.rglob("*.parquet")) == sorted(
            Path(p).name for p in results.output_files
        )

    def test_digit_sheet_option_falls_back_to_an_index(self):
        patcher = patch("forklift.inputs.excel.ExcelInputHandler")
        handler_cls = patcher.start()
        handler_cls.return_value.get_sheet_info.return_value = {"sheet_names": ["A", "B", "2024"]}
        try:
            by_name = ExcelImporter._create_default_excel_config(Path("x.xlsx"), sheet="2024")
            by_index = ExcelImporter._create_default_excel_config(Path("x.xlsx"), sheet="1")
        finally:
            patcher.stop()

        assert by_name.sheets[0].select == {"name": "2024"}  # a sheet name wins over an index
        assert by_index.sheets[0].select == {"index": 1}


# ---------------------------------------------------------------------------------------------
# 4. S3 output URIs
# ---------------------------------------------------------------------------------------------


class TestS3Outputs:
    def test_sql_import_writes_to_s3_not_to_a_local_s3_directory(
        self, tmp_path, monkeypatch, s3_bucket
    ):
        monkeypatch.chdir(tmp_path)
        with patched_sql([("dbo", "users", "users")], {"users": good_table}):
            results = SqlImporter.import_sql(CONNECTION_STRING, "s3://bkt/out/", "schema.json")

        assert s3_keys(s3_bucket) == ["out/metadata.json", "out/users.parquet"]
        assert results.output_files == ["s3://bkt/out/users.parquet"]
        assert not (tmp_path / "s3:").exists() and not list(tmp_path.iterdir())

        body = s3_bucket.get_object(Bucket="bkt", Key="out/users.parquet")["Body"].read()
        assert pq.read_table(pa.BufferReader(body)).num_rows == 3
        metadata = s3_bucket.get_object(Bucket="bkt", Key="out/metadata.json")["Body"].read()
        assert PASSWORD.encode() not in metadata

    def test_sql_failed_table_is_not_uploaded_to_s3(self, tmp_path, monkeypatch, s3_bucket):
        monkeypatch.chdir(tmp_path)
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(scratch))  # where S3 writers buffer files
        tables = [("dbo", "users", "users"), ("dbo", "orders", "orders")]
        with patched_sql(tables, {"users": good_table, "orders": failing_table}):
            with pytest.raises(ProcessingError):
                SqlImporter.import_sql(CONNECTION_STRING, "s3://bkt/out", "schema.json")

        assert s3_keys(s3_bucket) == ["out/metadata.json", "out/users.parquet"]
        assert list(scratch.iterdir()) == []  # the aborted writer's temp file is gone too

    def test_excel_import_writes_to_s3(self, tmp_path, monkeypatch, workbook, s3_bucket):
        monkeypatch.chdir(tmp_path)
        sheets = [("Sheet1", pa.table({"v": [1, 2]})), ("Sheet2", pa.table({"v": [3]}))]
        patcher = excel_handler_returning(sheets)
        try:
            results = ExcelImporter.import_excel(workbook, "s3://bkt/xl")
        finally:
            patcher.stop()

        assert s3_keys(s3_bucket) == ["xl/book_Sheet1.parquet", "xl/book_Sheet2.parquet"]
        assert results.output_files == [
            "s3://bkt/xl/book_Sheet1.parquet",
            "s3://bkt/xl/book_Sheet2.parquet",
        ]
        assert not (tmp_path / "s3:").exists()

    def test_collapsed_pathlib_s3_uri_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="collapsed by pathlib"):
            OutputLocation(Path("s3://bkt/out"))

        with patched_sql([("dbo", "users", "users")], {"users": good_table}):
            with pytest.raises(ValueError, match="collapsed by pathlib"):
                SqlImporter.import_sql(CONNECTION_STRING, Path("s3://bkt/out"), "schema.json")

    def test_s3_output_name_validation_still_applies(self):
        with pytest.raises(ValueError):
            OutputLocation("s3://bkt/out").target("../other")


# ---------------------------------------------------------------------------------------------
# 5. Output metadata collector
# ---------------------------------------------------------------------------------------------


def pii_batch():
    return pa.record_batch(
        [
            pa.array(["123-45-6789", "987-65-4321", "555-12-3456", "123-45-6789"]),
            pa.array([81234.5, 99876.25, 120500.0, 64321.75]),
            pa.array([81234, 99876, 120500, 64321]),
            pa.array(["2001-02-03", "1999-12-31", "2001-02-03", "1980-07-04"]),
            pa.array([None, "x", "y", "z"]),
        ],
        names=["ssn", "salary", "salary_int", "dob", "nick"],
    )


class TestMetadataNoValuesByDefault:
    LEAKS = ("123-45-6789", "987-65-4321", "81234", "99876", "120500", "64321")
    FORBIDDEN_KEYS = ("top_values", "min_value", "max_value", "median", "mode", "quantiles")

    def test_defaults_do_not_include_value_statistics(self):
        collector = OutputMetadataCollector()

        assert collector.include_value_statistics is False
        assert collector._value_counters == {} and collector._numeric_values == {}

    def test_saved_default_metadata_contains_no_cell_values(self, tmp_path):
        collector = OutputMetadataCollector()
        collector.add_batch(pii_batch())

        path = collector.save_metadata(tmp_path)
        text = Path(path).read_text()
        column_text = json.dumps(json.loads(text)["column_statistics"])

        for leak in self.LEAKS:
            assert leak not in text
        for key in self.FORBIDDEN_KEYS:
            assert f'"{key}"' not in column_text
        assert collector._value_counters == {} and collector._numeric_values == {}

    def test_default_keeps_counts_types_distinct_and_string_lengths(self):
        collector = OutputMetadataCollector()
        collector.add_batch(pii_batch())

        stats = collector.generate_metadata(None, {})["column_statistics"]

        assert stats["ssn"]["unique_values_count"] == 3
        assert stats["ssn"]["distinct_count_is_lower_bound"] is False
        assert stats["ssn"]["min_length"] == stats["ssn"]["max_length"] == 11
        assert stats["ssn"]["data_type"] == "string"
        assert stats["nick"]["null_count"] == 1 and stats["nick"]["non_null_count"] == 3
        assert stats["salary"]["numeric_statistics"].keys() == {
            "mean",
            "standard_deviation",
            "variance",
        }

    def test_opt_in_adds_value_statistics(self):
        collector = OutputMetadataCollector(include_value_statistics=True)
        collector.add_batch(pii_batch())

        stats = collector.generate_metadata(None, {})["column_statistics"]

        assert stats["ssn"]["top_values"][0] == {
            "value": "123-45-6789",
            "count": 2,
            "percentage": 50.0,
        }
        assert stats["salary"]["min_value"] == 64321.75
        assert stats["salary"]["max_value"] == 120500.0
        assert "quantiles" in stats["salary"]["numeric_statistics"]
        assert "median" in stats["salary"]["numeric_statistics"]
        assert "min_value" not in stats["ssn"]  # string lengths are never reported as values

    def test_profiling_config_records_the_choice(self):
        metadata = OutputMetadataCollector(include_value_statistics=True, sample_size=77)
        metadata.add_batch(pii_batch())

        config = metadata.generate_metadata(None, {})["profiling_config"]

        assert config["include_value_statistics"] is True and config["sample_size"] == 77


class TestMetadataStatisticsHonesty:
    def test_high_cardinality_column_has_no_misleading_ratios(self):
        collector = OutputMetadataCollector()
        for start in range(0, 100_000, 10_000):
            collector.add_batch(pa.record_batch([pa.array(range(start, start + 10_000))], ["id"]))

        metadata = collector.generate_metadata(None, {})
        stats = metadata["column_statistics"]["id"]

        assert stats["distinct_count_is_lower_bound"] is True
        assert stats["unique_values_count"] == 10_000  # the cap, honestly a lower bound
        assert stats["uniqueness_ratio"] is None
        assert stats["likely_categorical"] is None and stats["too_unique"] is None
        assert metadata["data_quality"]["likely_categorical_columns"] == []

    def test_exact_distinct_below_the_cap(self):
        collector = OutputMetadataCollector()
        collector.add_batch(pa.record_batch([pa.array(range(500))], ["id"]))

        stats = collector.generate_metadata(None, {})["column_statistics"]["id"]

        assert stats["distinct_count_is_lower_bound"] is False
        assert stats["uniqueness_ratio"] == 1.0 and stats["too_unique"] is True

    def test_cap_is_configurable_and_must_be_positive(self):
        collector = OutputMetadataCollector(max_distinct_tracked=10)
        collector.add_batch(pa.record_batch([pa.array(range(50))], ["id"]))

        stats = collector.generate_metadata(None, {})["column_statistics"]["id"]
        assert stats["unique_values_count"] == 10 and stats["distinct_count_is_lower_bound"]
        with pytest.raises(ValueError):
            OutputMetadataCollector(max_distinct_tracked=0)
        with pytest.raises(ValueError):
            OutputMetadataCollector(sample_size=0)

    def test_top_value_counts_cover_all_batches(self):
        collector = OutputMetadataCollector(include_value_statistics=True)
        collector.add_batch(pa.record_batch([pa.array(["A"] * 5)], ["c"]))
        # >1000 distinct values (the old counter cut-off) but far below the tracking cap
        for batch in range(30):
            values = [f"v{batch}_{i}" for i in range(100)] + ["A"] * 3
            collector.add_batch(pa.record_batch([pa.array(values)], ["c"]))

        stats = collector.generate_metadata(None, {})["column_statistics"]["c"]

        assert stats["top_values"][0]["value"] == "A"
        assert stats["top_values"][0]["count"] == 5 + 30 * 3
        assert stats["distinct_count_is_lower_bound"] is False

    def test_top_values_are_withheld_once_counts_would_be_inexact(self):
        collector = OutputMetadataCollector(include_value_statistics=True, max_distinct_tracked=20)
        collector.add_batch(
            pa.record_batch([pa.array(["A"] * 5 + [f"v{i}" for i in range(30)])], ["c"])
        )

        stats = collector.generate_metadata(None, {})["column_statistics"]["c"]

        assert "top_values" not in stats
        assert stats["top_values_unavailable"]

    def test_mean_and_std_are_exact_over_all_batches(self):
        rng = random.Random(3)
        values = [rng.uniform(-1e6, 1e6) for _ in range(25_000)]
        collector = OutputMetadataCollector()
        for start, size in [(0, 7), (7, 12_000), (12_007, 1), (12_008, 12_992)]:
            collector.add_batch(pa.record_batch([pa.array(values[start : start + size])], ["x"]))

        numeric = collector.generate_metadata(None, {})["column_statistics"]["x"][
            "numeric_statistics"
        ]

        assert numeric["mean"] == pytest.approx(statistics.fmean(values), abs=1e-3)
        assert numeric["standard_deviation"] == pytest.approx(statistics.stdev(values), rel=1e-6)

    def test_quantiles_and_median_are_not_biased_towards_batch_heads(self):
        collector = OutputMetadataCollector(include_value_statistics=True)
        for start in range(0, 100_000, 10_000):  # sequential data: old code saw 10 x first 1000
            collector.add_batch(pa.record_batch([pa.array(range(start, start + 10_000))], ["v"]))

        stats = collector.generate_metadata(None, {})["column_statistics"]["v"]
        numeric = stats["numeric_statistics"]

        assert stats["min_value"] == 0 and stats["max_value"] == 99_999
        assert numeric["mean"] == 49_999.5
        assert abs(numeric["median"] - 49_999.5) < 2_000  # old code: 45_499
        assert abs(numeric["quantiles"]["p99"] - 99_000) < 1_500  # old code: 90_899
        assert abs(numeric["quantiles"]["p25"] - 25_000) < 2_500
        assert numeric["quantiles_are_estimated"] is True
        assert numeric["sample_size"] == 10_000
        assert "mode" not in numeric  # a mode of a sample would be a guess

    def test_quantiles_are_exact_while_the_sample_holds_every_value(self):
        collector = OutputMetadataCollector(include_value_statistics=True, quantiles=[0.5, 1.0])
        collector.add_batch(pa.record_batch([pa.array([5, 1, 4, 2, 3])], ["v"]))

        numeric = collector.generate_metadata(None, {})["column_statistics"]["v"][
            "numeric_statistics"
        ]

        assert numeric["quantiles"] == {"p50": 3, "p100": 5}
        assert numeric["quantiles_are_estimated"] is False

    def test_sampling_is_deterministic_and_independent_of_batch_boundaries(self):
        data = list(range(30_000))

        def run(chunk):
            collector = OutputMetadataCollector(include_value_statistics=True, sample_size=500)
            for start in range(0, len(data), chunk):
                collector.add_batch(
                    pa.record_batch([pa.array(data[start : start + chunk])], ["v"])
                )
            return sorted(collector._numeric_values["v"].items)

        assert run(3_000) == run(3_000) == run(777)

    def test_quantile_labels_are_rounded_not_truncated(self):
        collector = OutputMetadataCollector(
            include_value_statistics=True, quantiles=[0.29, 0.58, 0.999, 0.07, 0.5]
        )
        collector.add_batch(pa.record_batch([pa.array(range(1000))], ["v"]))

        numeric = collector.generate_metadata(None, {})["column_statistics"]["v"][
            "numeric_statistics"
        ]

        assert set(numeric["quantiles"]) == {"p29", "p58", "p99.9", "p7", "p50"}

    def test_non_finite_floats_never_reach_the_json(self, tmp_path):
        nan, inf = float("nan"), float("inf")
        collector = OutputMetadataCollector(include_value_statistics=True)
        collector.add_batch(
            pa.record_batch(
                [
                    pa.array([1.0, nan, inf, -inf, 3.0, nan]),
                    pa.array([inf, -inf, nan, inf, -inf, nan]),  # only non-finite values
                    pa.array([nan, nan, nan, 1.0, 1.0, 2.0]),
                ],
                names=["mixed", "all_bad", "nan_dups"],
            )
        )

        path = collector.save_metadata(tmp_path)
        text = Path(path).read_text()
        # parse_constant is only called for NaN/Infinity/-Infinity tokens
        saved = json.loads(text, parse_constant=lambda token: pytest.fail(f"bad token {token}"))

        stats = saved["column_statistics"]
        assert stats["mixed"]["min_value"] == 1.0 and stats["mixed"]["max_value"] == 3.0
        assert stats["mixed"]["non_finite_count"] == 4
        assert stats["mixed"]["numeric_statistics"]["mean"] == 2.0
        assert "numeric_statistics" not in stats["all_bad"]
        assert stats["nan_dups"]["unique_values_count"] == 3  # NaN counted once, plus 1.0 and 2.0

    def test_generated_metadata_dict_is_nan_free_too(self):
        collector = OutputMetadataCollector()
        collector.add_batch(pa.record_batch([pa.array([float("nan"), float("inf")])], ["v"]))

        json.dumps(collector.generate_metadata(None, {}), allow_nan=False)

    @pytest.mark.parametrize("bad", [[-0.1], [1.5], [0.5, 2], [float("nan")], [float("inf")]])
    def test_out_of_range_quantiles_fail_at_config_time(self, bad):
        with pytest.raises(ValueError, match="quantiles"):
            OutputMetadataCollector(quantiles=bad)

    @pytest.mark.parametrize("bad", [["median"], [True], [None]])
    def test_non_numeric_quantiles_fail_at_config_time(self, bad):
        with pytest.raises(ValueError, match="quantiles"):
            OutputMetadataCollector(quantiles=bad)

    def test_boundary_quantiles_are_accepted(self):
        collector = OutputMetadataCollector(
            quantiles=[0, 0.0, 1, 1.0], include_value_statistics=True
        )
        collector.add_batch(pa.record_batch([pa.array([1, 2, 3])], ["v"]))

        numeric = collector.generate_metadata(None, {})["column_statistics"]["v"][
            "numeric_statistics"
        ]
        assert numeric["quantiles"] == {"p0": 1, "p100": 3}

    def test_nested_columns_do_not_break_collection(self):
        collector = OutputMetadataCollector(include_value_statistics=True)
        collector.add_batch(pa.record_batch([pa.array([[1, 2], [3], None])], ["lst"]))

        stats = collector.generate_metadata(None, {})["column_statistics"]["lst"]

        assert stats["null_count"] == 1
        assert stats["unique_values_count"] is None
        assert stats["distinct_count_is_lower_bound"] is True
        assert stats["uniqueness_ratio"] is None


class TestMetadataSaving:
    def make_collector(self):
        collector = OutputMetadataCollector()
        collector.add_batch(pa.record_batch([pa.array([1, 2, 3])], ["n"]))
        return collector

    def test_s3_destination_is_written_through_the_s3_handler(
        self, tmp_path, monkeypatch, s3_bucket
    ):
        monkeypatch.chdir(tmp_path)

        saved = self.make_collector().save_metadata("s3://bkt/run1/", "output_data_metadata.json")

        assert saved == "s3://bkt/run1/output_data_metadata.json"
        assert s3_keys(s3_bucket) == ["run1/output_data_metadata.json"]
        body = s3_bucket.get_object(Bucket="bkt", Key="run1/output_data_metadata.json")["Body"]
        assert json.loads(body.read())["data_summary"]["total_rows"] == 3
        assert not list(tmp_path.iterdir())  # no local "s3:" directory

    def test_collapsed_pathlib_s3_uri_is_rejected(self):
        with pytest.raises(ValueError, match="collapsed by pathlib"):
            self.make_collector().save_metadata(Path("s3://bkt/run1"))

    def test_write_failure_raises_and_is_logged_not_printed(self, tmp_path, caplog, capsys):
        blocker = tmp_path / "file_not_dir"
        blocker.write_text("x")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(MetadataWriteError, match="Could not write output metadata"):
                self.make_collector().save_metadata(blocker / "sub")

        assert "Failed to save output metadata" in caplog.text
        assert capsys.readouterr().out == ""

    def test_s3_failure_raises(self, monkeypatch, s3_bucket):
        with pytest.raises(MetadataWriteError):
            self.make_collector().save_metadata("s3://does-not-exist/x")

    def test_nothing_to_save_still_returns_none(self, tmp_path):
        assert OutputMetadataCollector().save_metadata(tmp_path) is None
        assert OutputMetadataCollector(enabled=False).save_metadata(tmp_path) is None


# ---------------------------------------------------------------------------------------------
# 6. CLI
# ---------------------------------------------------------------------------------------------


def run_cli(*args):
    env = dict(os.environ, PYTHONPATH=str(SRC_DIR))
    return subprocess.run(
        [sys.executable, "-m", "forklift", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )


class TestCliExitCodes:
    def test_fwf_exits_2_with_a_clear_message(self, tmp_path):
        proc = run_cli("ingest", "in.txt", "--dest", str(tmp_path), "--input-kind", "fwf")

        assert proc.returncode == 2
        assert "'fwf' is not implemented yet" in proc.stderr

    def test_output_file_without_path_exits_2(self):
        proc = run_cli("generate-schema", "in.csv", "--file-type", "csv", "--output", "file")

        assert proc.returncode == 2
        assert "--output-path is required" in proc.stderr

    def test_generate_schema_failure_exits_1(self, tmp_path):
        proc = run_cli("generate-schema", str(tmp_path / "missing.csv"), "--file-type", "csv")

        assert proc.returncode == 1
        assert proc.stderr.startswith("Error generating schema:")
        assert proc.stdout == ""

    def test_excel_ingest_failure_exits_non_zero(self, tmp_path):
        proc = run_cli(
            "ingest",
            str(tmp_path / "missing.xlsx"),
            "--dest",
            str(tmp_path),
            "--input-kind",
            "excel",
        )

        assert proc.returncode != 0
        assert "FileNotFoundError" in proc.stderr

    def test_excel_is_wired_to_import_excel_with_the_sheet_option(self):
        with patch("forklift.cli.import_excel") as import_excel:
            import_excel.return_value = Mock(
                errors=[],
                total_rows=3,
                valid_rows=3,
                invalid_rows=0,
                output_files=["o.parquet"],
                manifest_file=None,
                metadata_file=None,
            )
            main(["ingest", "b.xlsx", "--dest", "out", "--input-kind", "excel", "--sheet", "S"])

        import_excel.assert_called_once_with("b.xlsx", "out", None, sheet="S")

    def test_results_with_errors_exit_1(self):
        results = Mock(
            errors=["boom"],
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            output_files=[],
            manifest_file=None,
            metadata_file=None,
        )
        with patch("forklift.cli.import_excel", return_value=results):
            with pytest.raises(SystemExit) as exc_info:
                main(["ingest", "b.xlsx", "--dest", "out", "--input-kind", "excel"])

        assert exc_info.value.code == 1

    def test_real_excel_ingest_end_to_end(self, tmp_path, capsys):
        source = REPO_ROOT / "tests" / "test-files" / "excel" / "excel-data.xlsx"
        if not source.exists():
            pytest.skip("excel test file missing")
        main(["ingest", str(source), "--dest", str(tmp_path / "out"), "--input-kind", "excel"])

        assert "Processing complete" in capsys.readouterr().out
        assert list((tmp_path / "out").glob("*.parquet"))


class TestCliMetadataOutput:
    def test_metadata_output_works_against_the_real_generator(self, tmp_path, capsys):
        csv_file = tmp_path / "people.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n3,Cara\n")
        metadata_file = tmp_path / "meta.json"

        main(
            [
                "generate-schema",
                str(csv_file),
                "--file-type",
                "csv",
                "--output",
                "file",
                "--output-path",
                str(tmp_path / "schema.json"),
                "--metadata-output",
                str(metadata_file),
            ]
        )

        assert (tmp_path / "schema.json").exists()
        assert json.loads(metadata_file.read_text())  # non-empty metadata document
        assert f"Metadata file written to: {metadata_file}" in capsys.readouterr().out

    def test_metadata_output_with_no_metadata_warns_instead_of_failing(self, tmp_path, capsys):
        csv_file = tmp_path / "people.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        main(
            [
                "generate-schema",
                str(csv_file),
                "--file-type",
                "csv",
                "--no-metadata",
                "--metadata-output",
                str(tmp_path / "meta.json"),
            ]
        )

        assert "--metadata-output is ignored" in capsys.readouterr().err
        assert not (tmp_path / "meta.json").exists()


class TestCliValueStatsFlag:
    def test_flag_is_only_passed_to_configs_that_have_the_field(self):
        from dataclasses import dataclass

        @dataclass
        class WithField:
            include_value_statistics: bool = False

        @dataclass
        class WithoutField:
            other: int = 0

        assert _value_statistics_kwargs(WithField, True) == {"include_value_statistics": True}
        assert _value_statistics_kwargs(WithField, False) == {}
        assert _value_statistics_kwargs(WithoutField, True) == {}

    def test_unsupported_flag_warns(self, capsys):
        from dataclasses import dataclass

        @dataclass
        class WithoutField:
            other: int = 0

        _value_statistics_kwargs(WithoutField, True)

        assert "--include-value-stats is not supported" in capsys.readouterr().err

    def test_flag_is_accepted_by_both_commands(self):
        with patch("forklift.cli.ForkliftCore") as core:
            core.return_value.process_csv.return_value = Mock(
                errors=[], total_rows=0, valid_rows=0, invalid_rows=0, output_files=[]
            )
            main(
                ["ingest", "a.csv", "--dest", "o", "--input-kind", "csv", "--include-value-stats"]
            )
        with patch("forklift.cli.SchemaGenerator"):
            main(["generate-schema", "a.csv", "--file-type", "csv", "--include-value-stats"])

    def test_flag_reaches_the_configs_when_they_support_it(self):
        import dataclasses

        from forklift.engine.config import ImportConfig
        from forklift.schema.schema_generator import SchemaGenerationConfig

        for cls in (ImportConfig, SchemaGenerationConfig):
            assert "include_value_statistics" in {f.name for f in dataclasses.fields(cls)}
        with patch("forklift.cli.ForkliftCore") as core:
            core.return_value.process_csv.return_value = Mock(
                errors=[], total_rows=0, valid_rows=0, invalid_rows=0, output_files=[]
            )
            main(
                ["ingest", "a.csv", "--dest", "o", "--input-kind", "csv", "--include-value-stats"]
            )
        assert core.call_args[0][0].include_value_statistics is True

    def test_encoding_priority_limitation_is_documented(self, capsys):
        with pytest.raises(SystemExit):
            main(["ingest", "--help"])

        assert "Only the first one is used" in " ".join(capsys.readouterr().out.split())


class TestMainExample:
    def test_src_main_is_a_portable_runnable_example(self, tmp_path):
        path = SRC_DIR / "main.py"
        source = path.read_text()
        assert "/Users/" not in source
        assert 'header_mode="present"' not in source and "HeaderMode.PRESENT" in source

        spec = importlib.util.spec_from_file_location("forklift_example_main", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.REPO_ROOT = tmp_path  # write the output below tmp_path, read the repo's test data

        assert module.EXAMPLE_DIR.is_dir()
        assert module.main() == 0
        assert (tmp_path / "output" / "largecsv" / "data.parquet").exists()


# ---------------------------------------------------------------------------------------------
# 7. outputs package
# ---------------------------------------------------------------------------------------------


class TestOutputsPackage:
    def test_package_documents_that_it_is_unused_by_the_engine(self):
        import forklift.outputs

        assert "not used by the engine" in forklift.outputs.__doc__

    def test_manifest_does_not_report_a_corrupt_file_as_empty(self, tmp_path):
        corrupt = tmp_path / "corrupt.parquet"
        corrupt.write_bytes(b"not a parquet file")

        with pytest.raises(pa.ArrowInvalid):
            ManifestGenerator.create_manifest(tmp_path, [str(corrupt)])

    def test_manifest_still_tolerates_missing_files(self, tmp_path):
        path = ManifestGenerator.create_manifest(tmp_path, [str(tmp_path / "gone.parquet")])

        entry = json.loads(Path(path).read_text())["files"][0]
        assert entry["file_size"] == 0 and entry["record_count"] == 0

    def test_metadata_reads_only_the_parquet_footer(self, tmp_path):
        out = tmp_path / "a.parquet"
        pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), out)

        with patch("pyarrow.parquet.read_table", side_effect=AssertionError("full read")):
            path = MetadataGenerator.create_metadata(tmp_path, {"output_files": [str(out)]})

        stats = json.loads(Path(path).read_text())["column_statistics"]["a.parquet"]
        assert stats == {
            "num_columns": 2,
            "num_rows": 3,
            "column_names": ["id", "name"],
            "column_types": ["int64", "string"],
        }

    def test_metadata_logs_unreadable_files_instead_of_hiding_them(self, tmp_path, caplog):
        bad = tmp_path / "bad.parquet"
        bad.write_bytes(b"nope")

        with caplog.at_level(logging.WARNING):
            path = MetadataGenerator.create_metadata(tmp_path, {"output_files": [str(bad)]})

        assert json.loads(Path(path).read_text())["column_statistics"] == {}
        assert "Skipping column statistics" in caplog.text
