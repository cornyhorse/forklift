"""Progress and cancellation hooks, error codes, input sources and ``to_dict`` of the engine.

These are the seams ``forklift.jobs`` builds on: ``import_csv`` / ``import_excel`` /
``import_sql`` report every batch (sheet for Excel) to ``progress`` and stop when ``cancel``
says so, errors carry a stable ``error_code``, and a CSV input can be read from an
``InputSource`` stream instead of a path.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift import ImportCancelled, import_csv, import_excel
from forklift.engine.config import ImportConfig, ProcessingResults
from forklift.engine.exceptions import (
    CANCELLED,
    COLUMN_MISSING,
    CONSTRAINT_VIOLATION,
    ENCODING_ERROR,
    ERROR_CODES,
    INPUT_UNREADABLE,
    LIMIT_EXCEEDED,
    PERMISSION_DENIED,
    SCHEMA_INVALID,
    ImportInterrupted,
    LimitExceededError,
    ProcessingError,
    with_error_code,
)
from forklift.engine.forklift_core import ForkliftCore
from forklift.engine.importers.output_location import discard_finished_outputs
from forklift.engine.importers.sql_importer import SqlImporter
from forklift.engine.input_source import CountingReader, InputSource
from forklift.engine.processors.batch_processor import BatchProcessor
from forklift.engine.processors.extensions import ExtensionPipeline
from forklift.engine.processors.header_detector import HeaderDetector
from forklift.engine.progress import NO_HOOKS, ImportHooks
from forklift.io import UnifiedIOHandler

PEOPLE = "id,name,age\n" + "".join(f"{i},n{i},{20 + i}\n" for i in range(25))


def _csv(tmp_path, text=PEOPLE, name="people.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _schema(tmp_path, schema):
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(schema))
    return path


class _BytesSource(InputSource):
    """An input source over bytes; records how often it was opened."""

    def __init__(self, data: bytes, size="auto"):
        self.data = data
        self.name = "memory://people.csv"
        self.size = len(data) if size == "auto" else size
        self.opened = []

    def open(self):
        self.opened.append("open")
        return io.BytesIO(self.data)


class TestErrorCodes:
    def test_the_contract_lists_twelve_codes(self):
        assert len(ERROR_CODES) == len(set(ERROR_CODES)) == 12
        assert {CANCELLED, LIMIT_EXCEEDED, PERMISSION_DENIED} <= set(ERROR_CODES)

    def test_with_error_code_sets_a_missing_code_and_keeps_an_existing_one(self):
        error = with_error_code(ValueError("bad"), SCHEMA_INVALID)
        assert error.error_code == SCHEMA_INVALID
        assert with_error_code(error, COLUMN_MISSING).error_code == SCHEMA_INVALID

    def test_interruptions_carry_their_codes(self):
        assert ImportCancelled("x").error_code == CANCELLED
        assert LimitExceededError("x").error_code == LIMIT_EXCEEDED
        assert isinstance(ImportCancelled("x"), (ImportInterrupted, ProcessingError))
        assert ProcessingError("x").error_code is None


class TestImportHooks:
    def test_report_calls_progress_then_cancel(self):
        events, asked = [], []
        hooks = ImportHooks(events.append, lambda: asked.append(True) or False)

        hooks.report(10, 2, 300)

        assert events == [{"rows_read": 10, "rows_rejected": 2, "bytes_read": 300}]
        assert asked == [True]

    def test_cancel_returning_true_raises_import_cancelled(self):
        with pytest.raises(ImportCancelled, match="cancelled after 5 row"):
            ImportHooks(cancel=lambda: True).report(5)

    def test_no_hooks_do_nothing(self):
        NO_HOOKS.report(1, 0, 0)

    @pytest.mark.parametrize("name", ["progress", "cancel"])
    def test_callbacks_must_be_callable(self, name):
        with pytest.raises(TypeError, match=f"{name} must be callable, got str"):
            ImportHooks(**{name: "nope"})


class TestProcessingResultsToDict:
    def test_every_field_is_a_plain_copy(self):
        results = ProcessingResults(total_rows=3, warnings=["w"], validation_summary={"A": 1})

        data = results.to_dict()
        data["warnings"].append("changed")

        assert data["total_rows"] == 3 and data["validation_summary"] == {"A": 1}
        assert results.warnings == ["w"]
        assert ProcessingResults(**results.to_dict()) == results
        json.dumps(data)


class TestCsvProgressAndCancel:
    def test_progress_is_reported_at_every_batch_boundary(self, tmp_path):
        events = []
        results = import_csv(
            _csv(tmp_path), tmp_path / "out", batch_size=10, progress=events.append
        )

        assert results.total_rows == 25
        assert [e["rows_read"] for e in events] == [10, 20, 25]
        assert all(e["rows_rejected"] == 0 for e in events)
        size = (tmp_path / "people.csv").stat().st_size
        assert events[-1]["bytes_read"] == size

    def test_rejected_rows_are_counted(self, tmp_path):
        text = "id,age\n1,30\n2,old\n3,40\n"
        schema = _schema(
            tmp_path, {"properties": {"id": {"type": "integer"}, "age": {"type": "integer"}}}
        )
        events = []

        import_csv(_csv(tmp_path, text), tmp_path / "out", schema, progress=events.append)

        assert events[-1]["rows_read"] == 3 and events[-1]["rows_rejected"] == 1

    def test_cancel_stops_the_import_and_keeps_no_output(self, tmp_path):
        calls = []

        def cancel():
            calls.append(1)
            return len(calls) == 2  # after the second batch

        with pytest.raises(ImportCancelled) as caught:
            import_csv(_csv(tmp_path), tmp_path / "out", batch_size=10, cancel=cancel)

        assert caught.value.error_code == CANCELLED
        assert len(calls) == 2
        assert not (tmp_path / "out" / "data.parquet").exists()
        assert not (tmp_path / "out" / "manifest.json").exists()

    def test_a_progress_callback_can_stop_the_import(self, tmp_path):
        def progress(event):
            raise LimitExceededError("too many rows")

        with pytest.raises(LimitExceededError):
            import_csv(_csv(tmp_path), tmp_path / "out", batch_size=10, progress=progress)
        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_s3_client_is_used_for_the_import(self, tmp_path):
        client = MagicMock(name="s3")
        config = ImportConfig(_csv(tmp_path), tmp_path / "out", s3_client=client)

        core = ForkliftCore(config)
        core.process_csv()

        assert core.csv_processor.io_handler.s3_client is client
        assert "s3_client=" not in repr(config)


class TestCsvErrorCodes:
    def _code(self, tmp_path, *, text=PEOPLE, schema=None, **kwargs):
        schema_file = _schema(tmp_path, schema) if schema is not None else None
        with pytest.raises(Exception) as caught:
            import_csv(_csv(tmp_path, text), tmp_path / "out", schema_file, **kwargs)
        return caught.value

    def test_unreadable_schema_is_schema_invalid(self, tmp_path):
        (tmp_path / "schema.json").write_text("{not json")
        with pytest.raises(ValueError) as caught:
            import_csv(_csv(tmp_path), tmp_path / "out", tmp_path / "schema.json")
        assert caught.value.error_code == SCHEMA_INVALID

    def test_missing_header_is_input_unreadable(self, tmp_path):
        error = self._code(tmp_path, text="# a\n# b\n# c\nid\n1\n", header_search_rows=2)
        assert error.error_code == INPUT_UNREADABLE

    def test_required_column_missing_is_column_missing(self, tmp_path):
        error = self._code(tmp_path, schema={"properties": {"email": {}}, "required": ["email"]})
        assert error.error_code == COLUMN_MISSING
        assert "'email'" in str(error)

    def test_extension_referring_to_an_unknown_column_is_column_missing(self, tmp_path):
        schema = {"properties": {"id": {}}, "x-primaryKey": {"columns": ["nope"]}}
        assert self._code(tmp_path, schema=schema).error_code == COLUMN_MISSING

    def test_misconfigured_extension_is_schema_invalid(self, tmp_path):
        schema = {
            "properties": {"id": {}},
            "x-calculatedColumns": {"calculated": [{"name": "id", "expression": "1"}]},
        }
        assert self._code(tmp_path, schema=schema).error_code == SCHEMA_INVALID

    def test_undecodable_header_is_an_encoding_error(self, tmp_path):
        (tmp_path / "people.csv").write_bytes("na\xefme\n1\n".encode("latin-1"))
        with pytest.raises(ValueError) as caught:
            import_csv(tmp_path / "people.csv", tmp_path / "out")
        assert caught.value.error_code == ENCODING_ERROR

    def test_undecodable_data_is_an_encoding_error(self, tmp_path):
        # Far enough down that header detection (which decodes the start) does not see it
        data = b"id,name\n" + b"1,ok\n" * 5000 + b"2,caf\xe9\n"
        (tmp_path / "people.csv").write_bytes(data)
        with pytest.raises(pa.ArrowInvalid) as caught:
            import_csv(tmp_path / "people.csv", tmp_path / "out")
        assert caught.value.error_code == ENCODING_ERROR

    def test_binary_column_is_an_encoding_error(self, tmp_path):
        batch = pa.record_batch([pa.array([b"\xff"], type=pa.binary())], names=["blob"])
        processor = BatchProcessor(ImportConfig("i", "o"), UnifiedIOHandler())
        with pytest.raises(pa.ArrowInvalid) as caught:
            processor._check_no_binary_columns(batch)
        assert caught.value.error_code == ENCODING_ERROR

    def test_widening_passthrough_row_is_input_unreadable(self, tmp_path):
        text = "a,b\n1,2\n" * 3 + "1,2,3\n"
        processor = BatchProcessor(
            ImportConfig("i", "o", excess_column_mode="passthrough", batch_size=2),
            UnifiedIOHandler(),
        )
        records = [r.split(",") for r in text.strip().split("\n")]
        with pytest.raises(ValueError) as caught:
            list(processor._batches_from_records(records, ["a", "b"], 1, None))
        assert caught.value.error_code == INPUT_UNREADABLE

    def test_required_column_absent_from_a_batch_is_column_missing(self):
        from forklift.engine.processors.csv_processor import CSVProcessor

        batch = pa.record_batch([pa.array([1])], names=["id"])
        schema = pa.schema([pa.field("email", pa.string(), nullable=False)])
        with pytest.raises(ValueError) as caught:
            CSVProcessor()._validate_batch(batch, schema, ImportConfig("i", "o"))
        assert caught.value.error_code == COLUMN_MISSING

    def test_fail_fast_constraint_is_a_constraint_violation(self, tmp_path):
        text = "id\n1\n1\n"
        schema = {
            "properties": {"id": {"type": "integer"}},
            "x-primaryKey": {"columns": ["id"]},
            "x-constraintHandling": {"errorMode": "fail_fast"},
        }
        assert self._code(tmp_path, text=text, schema=schema).error_code == CONSTRAINT_VIOLATION

    def test_fail_complete_constraint_is_a_constraint_violation(self, tmp_path):
        text = "id\n1\n1\n"
        schema = {
            "properties": {"id": {"type": "integer"}},
            "x-primaryKey": {"columns": ["id"]},
            "x-constraintHandling": {"errorMode": "fail_complete"},
        }
        assert self._code(tmp_path, text=text, schema=schema).error_code == CONSTRAINT_VIOLATION

    def test_value_error_from_the_validation_stage_is_not_a_constraint_violation(self):
        validator = MagicMock()
        validator.process_batch.side_effect = ValueError("validator broke")
        pipeline = ExtensionPipeline(validator=validator)
        batch = pa.record_batch([pa.array([1])], names=["id"])

        with pytest.raises(ValueError) as caught:
            pipeline.post_convert(batch)
        assert getattr(caught.value, "error_code", None) is None


class TestInputSource:
    def test_csv_is_read_from_a_stream_source_like_a_file(self, tmp_path):
        source = _BytesSource(PEOPLE.encode())
        events = []
        config = ImportConfig("memory://people.csv", tmp_path / "out", batch_size=10)

        results = ForkliftCore(config, input_source=source, progress=events.append).process_csv()

        assert results.total_rows == 25
        assert pq.read_table(tmp_path / "out" / "data.parquet").num_rows == 25
        assert events[-1]["bytes_read"] == len(source.data)
        metadata = json.loads((tmp_path / "out" / "metadata.json").read_text())
        assert metadata["input_config"]["input_path"] == "memory://people.csv"

    def test_ragged_rows_reread_the_source_from_the_start(self, tmp_path):
        text = "a,b\n" + "1,2\n" * 3 + "1,2,3\n"
        source = _BytesSource(text.encode())
        config = ImportConfig("memory://x.csv", tmp_path / "out")

        results = ForkliftCore(config, input_source=source).process_csv()

        assert results.total_rows == 4 and results.truncated_rows == 1
        assert len(source.opened) >= 3  # header, Arrow pass, row reader pass

    def test_footer_detection_uses_the_row_reader_without_a_copy(self, tmp_path, monkeypatch):
        import tempfile

        def no_copies(*args, **kwargs):
            raise AssertionError("a streamed input must not be copied")

        monkeypatch.setattr(tempfile, "mkstemp", no_copies)
        text = "id,name\n1,a\n2,b\nTOTAL,2\n9,ignored\n"
        source = _BytesSource(text.encode())
        config = ImportConfig(
            "memory://x.csv",
            tmp_path / "out",
            footer_detection={"column_index": 0, "patterns": ["^TOTAL$"]},
        )

        results = ForkliftCore(config, input_source=source).process_csv()

        assert results.total_rows == 2
        # The row reader keeps columns without a schema type as text (as for S3 inputs)
        assert pq.read_table(tmp_path / "out" / "data.parquet").column("id").to_pylist() == [
            "1",
            "2",
        ]

    def test_footer_detection_with_no_columns_reads_nothing(self):
        source = _BytesSource(b"")
        processor = BatchProcessor(
            ImportConfig("x", "o", footer_detection={"stop_on_blank": True}),
            UnifiedIOHandler(),
            input_source=source,
        )
        assert list(processor.create_s3_batch_reader("x", [], -1, None)) == []
        assert source.opened == []

    def test_empty_source_yields_no_batches(self):
        source = _BytesSource(b"")
        processor = BatchProcessor(ImportConfig("x", "o"), UnifiedIOHandler(), input_source=source)
        assert list(processor.create_s3_batch_reader("x", ["a"], 0, None)) == []
        assert source.opened == []

    def test_source_of_unknown_size_is_read(self, tmp_path):
        source = _BytesSource(b"a\n1\n", size=None)
        config = ImportConfig("memory://x.csv", tmp_path / "out")
        assert ForkliftCore(config, input_source=source).process_csv().total_rows == 1

    def test_header_detector_reads_only_the_head_of_a_source(self):
        class HeadOnly(_BytesSource):
            def open(self):
                raise AssertionError("header detection must use open_head")

            def open_head(self):
                self.opened.append("head")
                return io.BytesIO(self.data)

        source = HeadOnly("﻿id,name\n1,a\n".encode("utf-8"))
        detector = HeaderDetector(ImportConfig("x", "o"), UnifiedIOHandler(), input_source=source)

        assert detector.detect_header_row("x") == (0, ["id", "name"])
        assert list(detector.rows("x")) == [["id", "name"], ["1", "a"]]
        assert source.opened == ["head", "head"]

    def test_default_open_head_is_open(self):
        source = _BytesSource(b"abc")
        assert source.open_head().read() == b"abc"


class TestCountingReader:
    def test_counts_bytes_from_streams_with_and_without_readinto(self):
        class ReadOnly:
            def __init__(self):
                self._data = io.BytesIO(b"hello world")
                self.closed = False

            def read(self, size):
                return self._data.read(size)

            def close(self):
                self.closed = True

        for stream in (io.BytesIO(b"hello world"), ReadOnly()):
            counter = CountingReader(stream)
            buffered = io.BufferedReader(counter)
            assert buffered.read() == b"hello world"
            assert counter.count == 11
            buffered.close()
            counter.close()  # closing twice is harmless


class TestExcelHooks:
    def _workbook(self, tmp_path):
        workbook = openpyxl.Workbook()
        workbook.active.title = "one"
        workbook.active.append(["id"])
        workbook.active.append([1])
        two = workbook.create_sheet("two")
        two.append(["id"])
        two.append([2])
        two.append([3])
        path = tmp_path / "book.xlsx"
        workbook.save(path)
        return path

    def test_progress_after_every_sheet(self, tmp_path):
        events = []
        import_excel(self._workbook(tmp_path), tmp_path / "out", progress=events.append)

        size = (tmp_path / "book.xlsx").stat().st_size
        assert events == [
            {"rows_read": 1, "rows_rejected": 0, "bytes_read": size},
            {"rows_read": 3, "rows_rejected": 0, "bytes_read": size},
        ]

    def test_cancel_removes_the_sheets_already_written(self, tmp_path):
        with pytest.raises(ImportCancelled):
            import_excel(self._workbook(tmp_path), tmp_path / "out", cancel=lambda: True)
        assert list((tmp_path / "out").iterdir()) == []

    def test_invalid_schema_is_schema_invalid(self, tmp_path):
        schema = _schema(tmp_path, {"x-excel": {"sheets": "not a list"}})
        with pytest.raises(ProcessingError) as caught:
            import_excel(self._workbook(tmp_path), tmp_path / "out", schema)
        assert caught.value.error_code == SCHEMA_INVALID


class TestDiscardFinishedOutputs:
    def test_local_and_s3_outputs_are_removed_and_failures_logged(self, tmp_path, caplog):
        local = tmp_path / "a.parquet"
        local.write_bytes(b"x")
        client = MagicMock()
        client._s3_client.delete_object.side_effect = [None, RuntimeError("denied")]

        with caplog.at_level(logging.WARNING):
            discard_finished_outputs(
                [local, tmp_path / "gone.parquet", "s3://b/k1", "s3://b/k2"], client
            )

        assert not local.exists()
        client._s3_client.delete_object.assert_any_call(Bucket="b", Key="k1")
        assert "Could not remove output s3://b/k2 (RuntimeError)" in caplog.text


SQL_SCHEMA = pa.schema([("id", pa.int64())])


def _sql_import(tmp_path, tables, read_table_data, **kwargs):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text("{}")
    schema_importer = MagicMock()
    schema_importer.get_table_list.return_value = tables
    schema_importer.get_selected_columns.return_value = None
    handler = MagicMock()
    handler.__enter__.return_value = handler
    handler.__exit__.return_value = None
    handler.get_table_schema.return_value = SQL_SCHEMA
    handler.read_table_data.side_effect = read_table_data
    with patch(
        "forklift.schema.sql_schema_importer.SqlSchemaImporter", return_value=schema_importer
    ), patch("forklift.inputs.sql.SqlInputHandler", return_value=handler):
        return SqlImporter.import_sql(
            "Driver=x;Pwd=secret", tmp_path / "out", schema_file, **kwargs
        )


def _batches(*sizes):
    def read(schema, table, columns=None):
        for size in sizes:
            yield pa.record_batch([pa.array(list(range(size)))], schema=SQL_SCHEMA)

    return read


class TestSqlHooks:
    def test_progress_after_every_batch_across_tables(self, tmp_path):
        events = []
        tables = [("public", "a", None), ("public", "b", None)]

        _sql_import(tmp_path, tables, _batches(2, 3), progress=events.append)

        assert [e["rows_read"] for e in events] == [2, 5, 7, 10]
        assert all(e["bytes_read"] == 0 and e["rows_rejected"] == 0 for e in events)

    def test_cancel_stops_everything_and_keeps_no_table(self, tmp_path):
        calls = []

        def cancel():
            calls.append(1)
            return len(calls) == 3  # in the second table

        tables = [("public", "a", None), ("public", "b", None)]
        with pytest.raises(ImportCancelled):
            _sql_import(tmp_path, tables, _batches(2, 3), cancel=cancel)

        assert list((tmp_path / "out").iterdir()) == []

    def test_failed_tables_with_one_code_give_that_code(self, tmp_path):
        def denied(schema, table, columns=None):
            raise with_error_code(PermissionError("no"), PERMISSION_DENIED)

        with pytest.raises(ProcessingError) as caught:
            _sql_import(tmp_path, [("public", "a", None)], denied)
        assert caught.value.error_code == PERMISSION_DENIED

    def test_failed_tables_with_different_codes_are_input_unreadable(self, tmp_path):
        def read(schema, table, columns=None):
            if table == "a":
                raise with_error_code(PermissionError("no"), PERMISSION_DENIED)
            raise RuntimeError("other")

        tables = [("public", "a", None), ("public", "b", None)]
        with pytest.raises(ProcessingError) as caught:
            _sql_import(tmp_path, tables, read)
        assert caught.value.error_code == INPUT_UNREADABLE

    def test_missing_schema_file_and_empty_table_list_are_schema_invalid(self, tmp_path):
        with pytest.raises(ProcessingError) as caught:
            SqlImporter.import_sql("Driver=x", tmp_path / "out", None)
        assert caught.value.error_code == SCHEMA_INVALID
        with pytest.raises(ValueError) as caught:
            _sql_import(tmp_path, [], _batches())
        assert caught.value.error_code == SCHEMA_INVALID

    def test_invalid_sql_schema_is_schema_invalid(self, tmp_path):
        schema_file = tmp_path / "schema.json"
        schema_file.write_text("{not json")
        with pytest.raises(ProcessingError) as caught:
            SqlImporter.import_sql("Driver=x", tmp_path / "out", schema_file)
        assert caught.value.error_code == SCHEMA_INVALID


def test_import_csv_accepts_a_path_object_for_the_output(tmp_path):
    results = import_csv(_csv(tmp_path), Path(tmp_path) / "out")
    assert results.total_rows == 25
