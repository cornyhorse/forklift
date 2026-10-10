"""Tests for how SqlImporter.import_sql handles empty and interrupted tables."""

import json
import logging
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from forklift.engine.importers.sql_importer import SqlImporter

SCHEMA = pa.schema([("id", pa.int64())])


def _run(tmp_path, read_table_data):
    """Import one table ``public.users`` whose rows come from ``read_table_data``."""
    schema_file = tmp_path / "schema.json"
    schema_file.write_text("{}")
    schema_importer = MagicMock()
    schema_importer.get_table_list.return_value = [("public", "users", None)]
    handler = MagicMock()
    handler.__enter__.return_value = handler
    handler.__exit__.return_value = None
    handler.get_table_schema.return_value = SCHEMA
    handler.read_table_data.side_effect = read_table_data
    with patch(
        "forklift.schema.sql_schema_importer.SqlSchemaImporter", return_value=schema_importer
    ), patch("forklift.inputs.sql.SqlInputHandler", return_value=handler):
        return SqlImporter.import_sql("Driver=x;Pwd=secret", tmp_path / "out", schema_file)


class TestEmptyTable:
    def test_table_without_rows_is_processed_but_not_listed_as_output(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="forklift.engine.importers.sql_importer"):
            results = _run(tmp_path, lambda schema, table: iter(()))

        assert results.total_rows == 0
        assert results.output_files == []
        assert results.errors == []
        assert "Table public.users contained no data" in caplog.text
        metadata = json.loads((tmp_path / "out" / "metadata.json").read_text())
        assert metadata["processing_summary"]["total_tables_processed"] == 1
        assert metadata["input_config"]["tables_processed"] == [["public", "users", None]]
        assert metadata["input_config"]["connection_string"] == "Driver=x;Pwd=***"


class TestInterruptedTable:
    def test_interrupt_removes_the_partial_file_and_is_not_recorded_as_a_failure(self, tmp_path):
        def rows_then_interrupt(schema, table):
            yield pa.record_batch([pa.array([1, 2])], schema=SCHEMA)
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            _run(tmp_path, rows_then_interrupt)

        out = tmp_path / "out"
        assert not (out / "public_users.parquet").exists()
        assert not (out / "metadata.json").exists()
