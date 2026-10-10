"""Tests for how CSVProcessor finishes, keeps or discards its parquet outputs."""

import json
import logging

import pytest

from forklift.engine.forklift_core import import_csv
from forklift.engine.processors import csv_processor
from forklift.io import create_parquet_writer
from forklift.processors.data_validation.data_validation_processor import (
    BadRowsThresholdExceededError,
)

LOGGER = "forklift.engine.processors.csv_processor"


def _write(path, text):
    path.write_text(text, encoding="utf-8")
    return str(path)


def _schema(tmp_path, schema):
    return _write(tmp_path / "schema.json", json.dumps(schema))


class _CloseFails:
    """Wraps a real parquet writer; ``close()`` finishes the file and then reports a failure."""

    def __init__(self, inner):
        self._inner = inner

    def write_table(self, table):
        self._inner.write_table(table)

    def close(self):
        self._inner.close()
        raise OSError("disk full")


def _writers_failing_on(name, monkeypatch, wrapper=_CloseFails):
    """Make the writer of output file ``name`` a ``wrapper`` around the real writer."""

    def factory(path, schema, **kwargs):
        writer = create_parquet_writer(path, schema, **kwargs)
        return wrapper(writer) if str(path).endswith(name) else writer

    monkeypatch.setattr(csv_processor, "create_parquet_writer", factory)


@pytest.fixture
def required_id(tmp_path):
    """Input with one valid row and one row whose required ``id`` is empty."""
    schema = _schema(
        tmp_path,
        {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
            "required": ["id"],
        },
    )
    return _write(tmp_path / "in.csv", "id,name\n1,a\n,b\n"), schema


class TestCloseFailures:
    def test_data_file_that_fails_to_close_discards_both_outputs(
        self, tmp_path, monkeypatch, required_id
    ):
        csv_path, schema = required_id
        _writers_failing_on("data.parquet", monkeypatch)
        out = tmp_path / "out"

        with pytest.raises(OSError, match="disk full"):
            import_csv(csv_path, out, schema_file=schema)

        assert not (out / "data.parquet").exists()
        assert not (out / "bad_rows.parquet").exists()

    def test_bad_rows_file_that_fails_to_close_also_removes_the_finished_data_file(
        self, tmp_path, monkeypatch, required_id
    ):
        csv_path, schema = required_id
        _writers_failing_on("bad_rows.parquet", monkeypatch)
        out = tmp_path / "out"

        with pytest.raises(OSError, match="disk full"):
            import_csv(csv_path, out, schema_file=schema)

        assert not (out / "data.parquet").exists()
        assert not (out / "bad_rows.parquet").exists()
        assert not (out / "metadata.json").exists()

    def test_threshold_stop_reports_the_original_error_when_bad_rows_cannot_be_kept(
        self, tmp_path, monkeypatch, caplog
    ):
        schema = _schema(
            tmp_path,
            {
                "type": "object",
                "properties": {"x": {"type": "integer"}},
                "x-validation": {"fieldValidations": {"x": {"range": {"min": 0}}}},
            },
        )
        csv_path = _write(tmp_path / "in.csv", "x\n1\n-1\n")
        _writers_failing_on("bad_rows.parquet", monkeypatch)
        out = tmp_path / "out"

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            with pytest.raises(BadRowsThresholdExceededError) as raised:
                import_csv(csv_path, out, schema_file=schema)

        assert "Could not keep the rejected rows" in caplog.text
        assert "All rows rejected by the checks are in" not in str(raised.value)
        assert getattr(raised.value, "bad_rows_file", None) is None
        assert not (out / "data.parquet").exists()
        assert not (out / "bad_rows.parquet").exists()


class TestDiscardingPartialOutputs:
    def test_writer_whose_abort_fails_is_logged_and_its_local_file_removed(
        self, tmp_path, monkeypatch, caplog
    ):
        class AbortFails:
            def __init__(self, inner):
                self._inner = inner

            def write_table(self, table):
                self._inner.write_table(table)

            def close(self):
                self._inner.close()

            def abort(self):
                self._inner.close()
                raise OSError("abort refused")

        _writers_failing_on("data.parquet", monkeypatch, wrapper=AbortFails)
        # The third row is wider than the header once data was written: the import fails
        csv_path = _write(tmp_path / "in.csv", "a,b\n1,2\n3,4\n5,6,7\n")
        out = tmp_path / "out"

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            with pytest.raises(ValueError, match="cannot add columns once data"):
                import_csv(csv_path, out, batch_size=1, excess_column_mode="passthrough")

        assert "Could not abort partial output" in caplog.text
        assert not (out / "data.parquet").exists()


class TestStaleOutputs:
    def test_stale_output_that_cannot_be_removed_is_logged_and_the_import_continues(
        self, tmp_path, caplog
    ):
        out = tmp_path / "out"
        (out / "bad_rows.parquet").mkdir(parents=True)  # not removable as a file
        csv_path = _write(tmp_path / "in.csv", "a\n1\n")

        with caplog.at_level(logging.WARNING, logger=LOGGER):
            results = import_csv(csv_path, out)

        assert "Could not remove output" in caplog.text
        assert results.valid_rows == 1
        assert results.output_files == [str(out / "data.parquet")]
        assert (out / "bad_rows.parquet").is_dir()
