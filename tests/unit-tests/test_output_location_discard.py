"""Tests for discard_partial_output in forklift.engine.importers.output_location."""

import logging
from unittest.mock import MagicMock

from forklift.engine.importers.output_location import discard_partial_output


class _WriterWithoutAbort:
    """An S3 style writer that predates ``abort()``: data sits in a temporary file."""

    def __init__(self, temp_path):
        self._writer = MagicMock()
        self._temp_path = temp_path
        self.closed_publicly = False

    def close(self):  # would upload the truncated file
        self.closed_publicly = True


class _FailingAbortWriter:
    def abort(self):
        raise OSError("bucket gone")


class TestDiscardWriterWithoutAbort:
    def test_temporary_file_is_removed_without_publishing(self, tmp_path):
        temp_file = tmp_path / "upload.parquet"
        temp_file.write_bytes(b"partial")
        writer = _WriterWithoutAbort(temp_file)

        discard_partial_output(writer, "s3://bucket/out/table.parquet")

        writer._writer.close.assert_called_once_with()
        assert not writer.closed_publicly
        assert not temp_file.exists()


class TestDiscardWhenAbortFails:
    def test_failure_is_logged_and_the_local_file_is_still_removed(self, tmp_path, caplog):
        target = tmp_path / "table.parquet"
        target.write_bytes(b"partial")

        with caplog.at_level(logging.WARNING, logger="forklift.engine.importers.output_location"):
            discard_partial_output(_FailingAbortWriter(), target)

        assert not target.exists()
        assert "Could not cleanly abort writer" in caplog.text
        assert "OSError" in caplog.text
        assert "bucket gone" not in caplog.text  # only the exception type is logged
