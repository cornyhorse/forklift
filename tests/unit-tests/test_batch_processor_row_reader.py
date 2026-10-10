"""Tests for BatchProcessor's row reader, footer copy and error classification."""

import json
import os
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.engine.config import ImportConfig
from forklift.engine.forklift_core import import_csv
from forklift.engine.processors import batch_processor
from forklift.engine.processors.batch_processor import BatchProcessor
from forklift.engine.processors.type_conversion import ColumnConverter
from forklift.io import UnifiedIOHandler


def _csv(tmp_path, text, name="in.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _int_schema(tmp_path):
    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps({"type": "object", "properties": {"n": {"type": "integer"}}}))
    return schema


def _processor(tmp_path, **config):
    config = ImportConfig(input_path=tmp_path / "in.csv", output_path=tmp_path / "out", **config)
    return BatchProcessor(config, UnifiedIOHandler())


@pytest.fixture
def temp_copies(monkeypatch):
    """Paths of the temporary footer-filtered copies created during the test."""
    created = []
    real_mkstemp = batch_processor.tempfile.mkstemp

    def mkstemp(*args, **kwargs):
        fd, path = real_mkstemp(*args, **kwargs)
        created.append(path)
        return fd, path

    monkeypatch.setattr(batch_processor.tempfile, "mkstemp", mkstemp)
    yield created
    for path in created:  # os.remove: the tests replace Path.unlink and os.unlink
        if os.path.exists(path):
            os.remove(path)


class TestFooterFilteredCopy:
    def test_header_only_file_gives_an_empty_data_file_with_the_header_columns(self, tmp_path):
        out = tmp_path / "out"

        results = import_csv(
            _csv(tmp_path, "a,b\n"), out, footer_detection={"stop_on_blank": True}
        )

        table = pq.read_table(out / "data.parquet")
        assert results.total_rows == 0
        assert table.num_rows == 0
        assert table.column_names == ["a", "b"]

    def test_header_row_beyond_the_end_of_the_file_yields_nothing(self, tmp_path, temp_copies):
        path = _csv(tmp_path, "a,b\n")
        processor = _processor(tmp_path, footer_detection={"stop_on_blank": True})

        batches = list(processor.create_batch_reader(path, ["a", "b"], 5, lambda row: False))

        assert batches == []
        assert len(temp_copies) == 1 and not os.path.exists(temp_copies[0])

    def test_copy_that_cannot_be_deleted_does_not_fail_the_read(
        self, tmp_path, temp_copies, monkeypatch
    ):
        path = _csv(tmp_path, "a\n1\n2\nTOTAL\n")
        processor = _processor(tmp_path, footer_detection={"stop_on_blank": True})
        real_unlink = pathlib.Path.unlink

        def unlink(self, *args, **kwargs):
            if str(self) in temp_copies:
                raise PermissionError("busy")
            return real_unlink(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "unlink", unlink)

        batches = list(processor.create_batch_reader(path, ["a"], 0, lambda row: row == ["TOTAL"]))

        assert [b.to_pydict() for b in batches] == [{"a": [1, 2]}]
        assert os.path.exists(temp_copies[0])  # left behind; removed by the fixture

    def test_failing_footer_check_propagates_even_if_the_copy_cannot_be_removed(
        self, tmp_path, temp_copies, monkeypatch
    ):
        path = _csv(tmp_path, "a\n1\n")
        processor = _processor(tmp_path, footer_detection={"stop_on_blank": True})

        def broken_footer_check(row):
            raise RuntimeError("footer rule failed")

        def unlink(path):
            raise PermissionError("busy")

        monkeypatch.setattr(batch_processor.os, "unlink", unlink)

        with pytest.raises(RuntimeError, match="footer rule failed"):
            list(processor.create_batch_reader(path, ["a"], 0, broken_footer_check))

        assert len(temp_copies) == 1


class TestBatchSize:
    @pytest.mark.parametrize("batch_size", [0, -5, True, None])
    def test_unusable_batch_size_falls_back_to_the_default(self, tmp_path, batch_size):
        out = tmp_path / "out"

        results = import_csv(_csv(tmp_path, "a\n1\n2\n3\n"), out, batch_size=batch_size)

        parquet = pq.ParquetFile(out / "data.parquet")
        assert results.valid_rows == 3
        assert parquet.metadata.num_row_groups == 1  # not split into batches of one row


class TestRejectHandler:
    def test_rows_that_do_not_convert_are_counted_without_a_handler(self, tmp_path):
        path = _csv(tmp_path, "n\n1\nx\n3\n")
        processor = BatchProcessor(
            ImportConfig(input_path=path, output_path=tmp_path / "out"),
            UnifiedIOHandler(),
            converter=ColumnConverter({"n": pa.int64()}),
        )

        batches = list(processor.create_batch_reader(path, ["n"], 0, None))

        assert [b.column("n").to_pylist() for b in batches] == [[1, 3]]
        assert processor.rejected_rows == 1
        assert processor.last_reject_reason is None


class _RowsIoHandler:
    """Stands in for the S3 reader: hands out parsed rows."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def csv_reader(self, path, **dialect):
        self.calls.append((path, dialect))
        return iter(self.rows)


class TestS3RowReader:
    @pytest.mark.parametrize("header_row_index", [-1, None])
    def test_file_without_header_keeps_its_first_row(self, tmp_path, header_row_index):
        io_handler = _RowsIoHandler([["1", "2"], ["3", "4"]])
        config = ImportConfig(input_path="s3://bucket/in.csv", output_path=tmp_path / "out")
        processor = BatchProcessor(config, io_handler)

        batches = list(
            processor.create_s3_batch_reader(
                "s3://bucket/in.csv", ["col_1", "col_2"], header_row_index, None
            )
        )

        assert [b.to_pydict() for b in batches] == [{"col_1": ["1", "3"], "col_2": ["2", "4"]}]
        assert io_handler.calls == [
            (
                "s3://bucket/in.csv",
                {"delimiter": ",", "quotechar": '"', "encoding": "utf-8-sig", "escapechar": None},
            )
        ]


class TestRowReaderBuffers:
    def test_rejected_wide_rows_are_flushed_whenever_a_batch_is_full(self, tmp_path):
        out = tmp_path / "out"

        results = import_csv(
            _csv(tmp_path, "a,b\n1,2,3\n4,5,6\n7,8,9\n10,11\n"),
            out,
            batch_size=2,
            excess_column_mode="reject",
        )

        bad = pq.read_table(out / "bad_rows.parquet")
        assert results.invalid_rows == 3 and results.valid_rows == 1
        assert bad.to_pydict() == {"a": ["1", "4", "7"], "b": ["2", "5", "8"]}
        assert pq.ParquetFile(out / "bad_rows.parquet").metadata.num_row_groups == 2
        # Without a schema the row reader keeps the text as read
        assert pq.read_table(out / "data.parquet").to_pydict() == {"a": ["10"], "b": ["11"]}

    def test_full_batch_without_a_usable_row_is_skipped(self, tmp_path):
        out = tmp_path / "out"

        # The wide last row sends the file through the row reader (TRUNCATE)
        results = import_csv(
            _csv(tmp_path, "n\nx\ny\n1\n2,extra\n"),
            out,
            schema_file=_int_schema(tmp_path),
            batch_size=2,
        )

        assert results.valid_rows == 2 and results.invalid_rows == 2
        assert results.truncated_rows == 1
        assert pq.read_table(out / "data.parquet").column("n").to_pylist() == [1, 2]
        assert pq.read_table(out / "bad_rows.parquet").column("n").to_pylist() == ["x", "y"]

    def test_last_partial_batch_without_a_usable_row_is_skipped(self, tmp_path):
        out = tmp_path / "out"

        results = import_csv(
            _csv(tmp_path, "n\n1\n2,extra\nx\n"),
            out,
            schema_file=_int_schema(tmp_path),
            batch_size=2,
        )

        assert results.valid_rows == 2 and results.invalid_rows == 1
        assert pq.read_table(out / "data.parquet").column("n").to_pylist() == [1, 2]
        assert pq.read_table(out / "bad_rows.parquet").column("n").to_pylist() == ["x"]


class TestCorruptRows:
    @pytest.mark.parametrize(
        "row", ["x,�,z", "bad\x01: text,b,c"], ids=["replacement-char", "control-char"]
    )
    def test_too_wide_row_with_corrupt_content_is_an_error_without_the_row(self, tmp_path, row):
        out = tmp_path / "out"

        with pytest.raises(pa.ArrowInvalid) as raised:
            import_csv(_csv(tmp_path, f"a,b\n1,2\n{row}\n"), out)

        message = str(raised.value)
        assert "Expected 2 columns, got 3" in message
        assert row not in message and "<row content redacted>" in message
        assert not (out / "data.parquet").exists()
