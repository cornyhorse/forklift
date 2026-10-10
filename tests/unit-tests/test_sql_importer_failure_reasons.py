"""The reason recorded with a table that fails: useful to the user, never the table's data.

Driver and Arrow messages can quote cell values, so they are never repeated. What is shown is
the exception class plus either forklift's own catalog-lookup message (which holds only names)
or the SQLSTATE, its meaning and the driver's numeric code.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from forklift.engine.exceptions import ProcessingError
from forklift.engine.importers.sql_importer import SqlImporter, _failure_reason
from forklift.inputs.sql.schema import TableLookupError


class DriverError(Exception):
    """Shaped like pyodbc.Error: args are (SQLSTATE, message)."""


DENIED = DriverError(
    "42501",
    "[42501] ERROR: permission denied for table payroll; (1) (SQLExecDirectW)",
)
CONVERSION = ValueError("Could not convert 'ana@example.com' with type str: tried int64")


class TestFailureReason:
    def test_catalog_lookup_failure_explains_itself(self):
        error = TableLookupError("Table 'hr.payroll' was not found in the database catalog")

        assert _failure_reason(error) == str(error)

    def test_known_sqlstate_is_named_with_its_meaning_and_the_driver_code(self):
        assert _failure_reason(DENIED) == "SQLSTATE 42501, insufficient privilege, driver error 1"

    def test_unknown_sqlstate_keeps_the_code_and_the_driver_error(self):
        error = DriverError(
            "HY000",
            "[HY000] Query execution was interrupted, maximum statement execution time "
            "exceeded (3024) (SQLExecDirectW)",
        )

        assert _failure_reason(error) == "SQLSTATE HY000, driver error 3024"

    def test_message_without_a_driver_code_gives_the_sqlstate_alone(self):
        assert _failure_reason(DriverError("25006", "read-only")) == (
            "SQLSTATE 25006, read-only transaction: the statement would have written"
        )

    @pytest.mark.parametrize(
        "error",
        [CONVERSION, DriverError("not-a-state", "x"), DriverError("42501"), RuntimeError()],
    )
    def test_anything_else_gives_no_reason_and_never_its_message(self, error):
        assert _failure_reason(error) == ""


def _import(tmp_path, failure):
    """Import ``public.ok`` and ``hr.payroll``; reading the second raises ``failure``."""
    schema_file = tmp_path / "schema.json"
    schema_file.write_text("{}")
    schema_importer = MagicMock()
    schema_importer.get_table_list.return_value = [
        ("public", "ok", None),
        ("hr", "payroll", None),
    ]
    handler = MagicMock()
    handler.__enter__.return_value = handler
    handler.__exit__.return_value = None
    handler.get_table_schema.return_value = pa.schema([("id", pa.int64())])

    def read_table_data(schema, table):
        if table == "payroll":
            raise failure
        return iter([pa.record_batch({"id": [1, 2]})])

    handler.read_table_data.side_effect = read_table_data
    with patch(
        "forklift.schema.sql_schema_importer.SqlSchemaImporter", return_value=schema_importer
    ), patch("forklift.inputs.sql.SqlInputHandler", return_value=handler):
        return SqlImporter.import_sql("Driver=x", tmp_path / "out", schema_file)


class TestFailedTableReports:
    def test_reason_reaches_the_error_the_results_and_the_metadata(self, tmp_path):
        with pytest.raises(ProcessingError) as raised:
            _import(tmp_path, DENIED)

        expected = "DriverError (SQLSTATE 42501, insufficient privilege, driver error 1)"
        assert str(raised.value) == (
            "1 of 2 tables failed: hr.payroll (DriverError: SQLSTATE 42501, insufficient "
            "privilege, driver error 1)"
        )
        assert raised.value.results.errors == [f"hr.payroll: {expected}"]
        metadata = json.loads((tmp_path / "out" / "metadata.json").read_text())
        assert metadata["failed_tables"] == [
            {
                "schema": "hr",
                "table": "payroll",
                "error_type": "DriverError",
                "reason": "SQLSTATE 42501, insufficient privilege, driver error 1",
            }
        ]

    def test_error_without_a_safe_reason_is_reported_by_class_only(self, tmp_path):
        with pytest.raises(ProcessingError) as raised:
            _import(tmp_path, CONVERSION)

        assert str(raised.value) == "1 of 2 tables failed: hr.payroll (ValueError)"
        assert raised.value.results.errors == ["hr.payroll: ValueError"]
        text = (tmp_path / "out" / "metadata.json").read_text()
        assert "ana@example.com" not in text
        assert json.loads(text)["failed_tables"][0]["reason"] is None
