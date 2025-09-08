"""Comprehensive tests for bad rows handler module."""

import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import json
from datetime import datetime

from forklift.processors.bad_rows_handler import (
    BadRowsHandler,
    BadRowsConfig
)
from forklift.processors.base import ValidationResult
from forklift.processors.constraint_validator import ConstraintViolation


class TestBadRowsConfig:
    """Test BadRowsConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = BadRowsConfig()

        assert config.output_path is None
        assert config.output_format == "parquet"
        assert config.include_original_data is True
        assert config.include_error_details is True
        assert config.max_bad_rows is None
        assert config.create_summary is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = BadRowsConfig(
            output_path="/tmp/bad_rows",
            output_format="csv",
            include_original_data=False,
            include_error_details=False,
            max_bad_rows=100,
            create_summary=False
        )

        assert config.output_path == "/tmp/bad_rows"
        assert config.output_format == "csv"
        assert config.include_original_data is False
        assert config.include_error_details is False
        assert config.max_bad_rows == 100
        assert config.create_summary is False


class TestBadRowsHandler:
    """Test BadRowsHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = BadRowsConfig()
        self.handler = BadRowsHandler(self.config)

    def test_init(self):
        """Test handler initialization."""
        assert self.handler.config == self.config
        assert self.handler.bad_rows == []
        assert self.handler.validation_errors == []
        assert self.handler.constraint_violations == []
        assert self.handler.row_count == 0
        assert self.handler.bad_row_count == 0

    @patch('forklift.processors.bad_rows_handler.datetime')
    def test_add_bad_row_basic(self, mock_datetime):
        """Test adding a bad row with basic data."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

        row_data = {"id": 1, "name": "test"}
        self.handler.add_bad_row(row_data, 0)

        assert len(self.handler.bad_rows) == 1
        assert self.handler.bad_row_count == 1

        bad_row = self.handler.bad_rows[0]
        assert bad_row["row_index"] == 0
        assert bad_row["timestamp"] == "2023-01-01T12:00:00"
        assert bad_row["original_data"] == row_data

    @patch('forklift.processors.bad_rows_handler.datetime')
    def test_add_bad_row_with_validation_errors(self, mock_datetime):
        """Test adding a bad row with validation errors."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

        row_data = {"id": "invalid", "name": ""}
        validation_results = [
            ValidationResult(False, "Invalid ID", "INVALID_ID"),
            ValidationResult(False, "Empty name", "EMPTY_NAME")
        ]

        self.handler.add_bad_row(row_data, 1, validation_results=validation_results)

        assert len(self.handler.bad_rows) == 1
        bad_row = self.handler.bad_rows[0]
        assert bad_row["row_index"] == 1
        assert len(bad_row["errors"]) == 2

    @patch('forklift.processors.bad_rows_handler.datetime')
    def test_add_bad_row_with_constraint_violations(self, mock_datetime):
        """Test adding a bad row with constraint violations."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

        row_data = {"id": -1, "age": 200}
        violations = [
            ConstraintViolation("id", "minimum", -1, "ID must be positive"),
            ConstraintViolation("age", "maximum", 200, "Age too high")
        ]

        self.handler.add_bad_row(row_data, 2, constraint_violations=violations)

        assert len(self.handler.bad_rows) == 1
        bad_row = self.handler.bad_rows[0]
        assert bad_row["row_index"] == 2

    def test_add_bad_row_max_limit(self):
        """Test adding bad rows with maximum limit."""
        config = BadRowsConfig(max_bad_rows=2)
        handler = BadRowsHandler(config)

        # Add rows up to limit
        for i in range(3):
            handler.add_bad_row({"id": i}, i)

        # Only 2 rows should be stored due to limit
        assert len(handler.bad_rows) == 2
        assert handler.bad_row_count == 2

    def test_add_bad_rows_from_batch(self):
        """Test adding bad rows from a batch."""
        batch = pa.record_batch({
            'id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Charlie', 'David']
        })

        invalid_indices = [1, 3]
        validation_results = [
            ValidationResult(False, "Error 1", "ERROR1", row_number=1),
            ValidationResult(False, "Error 2", "ERROR2", row_number=3)
        ]

        self.handler.add_bad_rows_from_batch(batch, invalid_indices, validation_results)

        assert len(self.handler.bad_rows) == 2
        assert self.handler.bad_row_count == 2

    def test_increment_row_count(self):
        """Test incrementing row count."""
        assert self.handler.row_count == 0

        self.handler.increment_row_count()
        assert self.handler.row_count == 1

        self.handler.increment_row_count(5)
        assert self.handler.row_count == 6

    def test_has_bad_rows(self):
        """Test checking if handler has bad rows."""
        assert self.handler.has_bad_rows() is False

        self.handler.add_bad_row({"id": 1}, 0)
        assert self.handler.has_bad_rows() is True

    def test_get_bad_row_count(self):
        """Test getting bad row count."""
        assert self.handler.get_bad_row_count() == 0

        self.handler.add_bad_row({"id": 1}, 0)
        self.handler.add_bad_row({"id": 2}, 1)
        assert self.handler.get_bad_row_count() == 2

    def test_get_summary(self):
        """Test getting summary statistics."""
        self.handler.row_count = 100
        self.handler.add_bad_row({"id": 1}, 0)
        self.handler.add_bad_row({"id": 2}, 1)

        summary = self.handler.get_summary()

        assert isinstance(summary, dict)
        assert summary["total_rows_processed"] == 100
        assert summary["bad_rows_count"] == 2
        assert "bad_row_percentage" in summary
        assert "validation_error_types" in summary

    @patch('forklift.processors.bad_rows_handler.Path.mkdir')
    @patch('forklift.processors.bad_rows_handler.BadRowsHandler._write_parquet')
    @patch('forklift.processors.bad_rows_handler.BadRowsHandler._write_summary')
    def test_write_bad_rows_parquet(self, mock_write_summary, mock_write_parquet, mock_mkdir):
        """Test writing bad rows to parquet format."""
        self.handler.add_bad_row({"id": 1}, 0)

        result_path = self.handler.write_bad_rows("/tmp/output")

        mock_mkdir.assert_called_once()
        mock_write_parquet.assert_called_once()
        mock_write_summary.assert_called_once()
        assert result_path is not None

    @patch('forklift.processors.bad_rows_handler.Path.mkdir')
    @patch('forklift.processors.bad_rows_handler.BadRowsHandler._write_csv')
    def test_write_bad_rows_csv(self, mock_write_csv, mock_mkdir):
        """Test writing bad rows to CSV format."""
        config = BadRowsConfig(output_format="csv")
        handler = BadRowsHandler(config)
        handler.add_bad_row({"id": 1}, 0)

        result_path = handler.write_bad_rows("/tmp/output")

        mock_write_csv.assert_called_once()
        assert result_path is not None

    @patch('forklift.processors.bad_rows_handler.Path.mkdir')
    @patch('forklift.processors.bad_rows_handler.BadRowsHandler._write_json')
    def test_write_bad_rows_json(self, mock_write_json, mock_mkdir):
        """Test writing bad rows to JSON format."""
        config = BadRowsConfig(output_format="json")
        handler = BadRowsHandler(config)
        handler.add_bad_row({"id": 1}, 0)

        result_path = handler.write_bad_rows("/tmp/output")

        mock_write_json.assert_called_once()
        assert result_path is not None

    def test_write_bad_rows_no_bad_rows(self):
        """Test writing when there are no bad rows."""
        result_path = self.handler.write_bad_rows("/tmp/output")
        assert result_path is None

    @patch('forklift.processors.bad_rows_handler.pq.write_table')
    @patch('forklift.processors.bad_rows_handler.pa.table')
    def test_write_parquet(self, mock_pa_table, mock_write_table):
        """Test writing parquet file."""
        self.handler.add_bad_row({"id": 1, "name": "test"}, 0)

        mock_table = MagicMock()
        mock_pa_table.return_value = mock_table

        file_path = Path("/tmp/bad_rows.parquet")
        self.handler._write_parquet(file_path)

        mock_pa_table.assert_called_once()
        mock_write_table.assert_called_once_with(mock_table, file_path)

    @patch('forklift.processors.bad_rows_handler.pv_csv.write_csv')
    @patch('forklift.processors.bad_rows_handler.pa.table')
    def test_write_csv(self, mock_pa_table, mock_write_csv):
        """Test writing CSV file."""
        self.handler.add_bad_row({"id": 1, "name": "test"}, 0)

        mock_table = MagicMock()
        mock_pa_table.return_value = mock_table

        file_path = Path("/tmp/bad_rows.csv")
        self.handler._write_csv(file_path)

        mock_pa_table.assert_called_once()
        mock_write_csv.assert_called_once_with(mock_table, file_path)

    @patch('builtins.open', new_callable=mock_open)
    @patch('forklift.processors.bad_rows_handler.json.dump')
    def test_write_json(self, mock_json_dump, mock_file_open):
        """Test writing JSON file."""
        self.handler.add_bad_row({"id": 1, "name": "test"}, 0)

        file_path = Path("/tmp/bad_rows.json")
        self.handler._write_json(file_path)

        mock_file_open.assert_called_once_with(file_path, 'w', encoding='utf-8')
        mock_json_dump.assert_called_once()

    @patch('builtins.open', new_callable=mock_open)
    @patch('forklift.processors.bad_rows_handler.json.dump')
    def test_write_summary(self, mock_json_dump, mock_file_open):
        """Test writing summary file."""
        self.handler.add_bad_row({"id": 1}, 0)

        file_path = Path("/tmp/summary.json")
        self.handler._write_summary(file_path)

        mock_file_open.assert_called_once_with(file_path, 'w', encoding='utf-8')
        mock_json_dump.assert_called_once()


class TestBadRowsHandlerIntegration:
    """Test bad rows handler integration scenarios."""

    def test_handler_workflow(self):
        """Test complete handler workflow."""
        config = BadRowsConfig(
            output_path="/tmp/bad_rows",
            include_original_data=True,
            include_error_details=True,
            create_summary=True
        )
        handler = BadRowsHandler(config)

        # Add some bad rows
        handler.add_bad_row({"id": "invalid"}, 0)
        handler.add_bad_row({"name": ""}, 1)
        handler.increment_row_count(10)

        # Check statistics
        assert handler.has_bad_rows()
        assert handler.get_bad_row_count() == 2

        summary = handler.get_summary()
        assert summary["total_rows_processed"] == 10
        assert summary["bad_rows_count"] == 2

    def test_config_without_original_data(self):
        """Test handler with config that excludes original data."""
        config = BadRowsConfig(include_original_data=False)
        handler = BadRowsHandler(config)

        handler.add_bad_row({"sensitive": "data"}, 0)

        bad_row = handler.bad_rows[0]
        assert "original_data" not in bad_row

    def test_config_without_error_details(self):
        """Test handler with config that excludes error details."""
        config = BadRowsConfig(include_error_details=False)
        handler = BadRowsHandler(config)

        validation_results = [ValidationResult(False, "Error", "ERROR")]
        handler.add_bad_row({"id": 1}, 0, validation_results=validation_results)

        bad_row = handler.bad_rows[0]
        assert "errors" not in bad_row

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.processors.bad_rows_handler import (
            BadRowsHandler,
            BadRowsConfig
        )

        assert BadRowsHandler is not None
        assert BadRowsConfig is not None

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.processors.bad_rows_handler as bad_rows_module

        assert bad_rows_module.__doc__ is not None
        assert "Bad rows handler" in bad_rows_module.__doc__
