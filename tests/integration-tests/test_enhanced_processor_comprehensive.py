"""Comprehensive tests for EnhancedDataProcessor to improve code coverage."""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pytest

from forklift.processors.bad_rows_handler import BadRowsConfig
from forklift.processors.base import ValidationResult
from forklift.processors.constraint_validator import ConstraintViolation
from forklift.processors.enhanced_processor import (
    EnhancedDataProcessor,
    _json_type_to_arrow_type,
    create_enhanced_processor_from_schema_file,
)


class TestEnhancedDataProcessor:
    """Test EnhancedDataProcessor class functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create a basic schema for testing
        self.test_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
                pa.field("age", pa.int64(), nullable=True),
                pa.field("email", pa.string(), nullable=True),
            ]
        )

        self.test_schema_dict = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0},
                "email": {"type": "string", "format": "email"},
            },
            "required": ["id"],
        }

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_initialization_with_defaults(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test processor initialization with default parameters."""
        # Mock the dependencies
        mock_create_config.return_value = Mock()
        mock_constraint_validator_instance = Mock()
        mock_constraint_validator.return_value = mock_constraint_validator_instance
        mock_schema_validator_instance = Mock()
        mock_schema_validator.return_value = mock_schema_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        assert processor.schema == self.test_schema
        assert processor.schema_dict == {}
        assert processor.strict_mode is True
        assert processor.error_mode == "bad_rows"  # Default error mode

        # Verify mocks were called correctly
        mock_schema_validator.assert_called_once_with(self.test_schema, True)
        mock_create_config.assert_called_once_with({})
        mock_constraint_validator.assert_called_once()

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_initialization_with_custom_configs(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test processor initialization with custom configurations."""
        # Mock dependencies
        mock_constraint_config = Mock()
        mock_bad_rows_config = BadRowsConfig(max_bad_rows=100)

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator.return_value = mock_constraint_validator_instance
        mock_schema_validator_instance = Mock()
        mock_schema_validator.return_value = mock_schema_validator_instance

        processor = EnhancedDataProcessor(
            schema=self.test_schema,
            schema_dict=self.test_schema_dict,
            constraint_config=mock_constraint_config,
            bad_rows_config=mock_bad_rows_config,
            strict_mode=False,
        )

        assert processor.schema == self.test_schema
        assert processor.schema_dict == self.test_schema_dict
        assert processor.strict_mode is False
        assert processor.constraint_config == mock_constraint_config
        assert processor.bad_rows_handler.config == mock_bad_rows_config

        # Should not call create_constraint_config_from_schema when config provided
        mock_create_config.assert_not_called()

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_extract_error_handling_mode_with_schema_dict(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test error handling mode extraction from schema dictionary."""
        # Mock dependencies
        mock_create_config.return_value = Mock()
        mock_constraint_validator.return_value = Mock()
        mock_schema_validator.return_value = Mock()

        schema_dict = {"x-constraintHandling": {"errorMode": "fail_fast"}}

        processor = EnhancedDataProcessor(self.test_schema, schema_dict=schema_dict)
        assert processor.error_mode == "fail_fast"

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_extract_error_handling_mode_default(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test default error handling mode when not specified in schema."""
        # Mock dependencies
        mock_create_config.return_value = Mock()
        mock_constraint_validator.return_value = Mock()
        mock_schema_validator.return_value = Mock()

        processor = EnhancedDataProcessor(self.test_schema, schema_dict={})
        assert processor.error_mode == "bad_rows"

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_process_batch_successful(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test successful batch processing."""
        # Setup mocks
        mock_create_config.return_value = Mock()

        # Mock schema validator
        mock_schema_validator_instance = Mock()
        schema_valid_batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "age": [25, 30],
                "email": ["alice@test.com", "bob@test.com"],
            }
        )
        schema_validation_results = [ValidationResult(True, None, None, None, None)]
        mock_schema_validator_instance.process_batch.return_value = (
            schema_valid_batch,
            schema_validation_results,
        )
        mock_schema_validator.return_value = mock_schema_validator_instance

        # Mock constraint validator
        mock_constraint_validator_instance = Mock()
        constraint_valid_batch = schema_valid_batch
        constraint_validation_results = [ValidationResult(True, None, None, None, None)]
        mock_constraint_validator_instance.process_batch.return_value = (
            constraint_valid_batch,
            constraint_validation_results,
        )
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        # Create test batch
        test_batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "age": [25, 30],
                "email": ["alice@test.com", "bob@test.com"],
            }
        )

        result_batch, validation_results = processor.process_batch(test_batch)

        assert result_batch == constraint_valid_batch
        assert len(validation_results) == 2  # Schema + constraint validation results
        assert processor.bad_rows_handler.row_count == 2

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_process_batch_with_validation_errors(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test batch processing with validation errors and bad rows."""
        # Setup mocks
        mock_create_config.return_value = Mock()

        # Mock schema validator with validation errors
        mock_schema_validator_instance = Mock()
        schema_valid_batch = pa.RecordBatch.from_pydict(
            {"id": [1], "name": ["Alice"], "age": [25], "email": ["alice@test.com"]}
        )
        schema_validation_results = [
            ValidationResult(False, "TYPE_ERROR", "Invalid type", "age", 1),
            ValidationResult(True, None, None, None, None),
        ]
        mock_schema_validator_instance.process_batch.return_value = (
            schema_valid_batch,
            schema_validation_results,
        )
        mock_schema_validator.return_value = mock_schema_validator_instance

        # Mock constraint validator with violations
        mock_constraint_validator_instance = Mock()
        constraint_violations = [
            ConstraintViolation(
                violation_type="CHECK_CONSTRAINT",
                error_message="Age must be positive",
                columns=["age"],
                values=[-5],
                constraint_name="age_positive",
                row_index=1,
            )
        ]
        mock_constraint_validator_instance.get_all_violations.return_value = constraint_violations
        constraint_validation_results = [
            ValidationResult(False, "CONSTRAINT_ERROR", "Constraint violation", "age", 1)
        ]
        mock_constraint_validator_instance.process_batch.return_value = (
            schema_valid_batch,
            constraint_validation_results,
        )
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        # Create test batch with invalid data
        test_batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "age": [25, -5],
                "email": ["alice@test.com", "bob@test.com"],
            }
        )

        result_batch, validation_results = processor.process_batch(test_batch)

        assert len(validation_results) == 3  # Schema + constraint validation results
        assert processor.bad_rows_handler.get_bad_row_count() == 1  # One bad row added

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_handle_bad_rows_with_invalid_indices(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test handling bad rows with invalid row indices."""
        # Setup mocks
        mock_create_config.return_value = Mock()
        mock_schema_validator.return_value = Mock()

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = [
            ConstraintViolation(
                violation_type="CHECK_CONSTRAINT",
                error_message="Invalid row",
                columns=["age"],
                values=[-5],
                constraint_name="age_positive",
                row_index=10,  # Index beyond batch size
            )
        ]
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        # Create small batch
        test_batch = pa.RecordBatch.from_pydict(
            {"id": [1], "name": ["Alice"], "age": [25], "email": ["alice@test.com"]}
        )

        # Create validation results with invalid row index
        validation_results = [
            ValidationResult(False, "ERROR", "Error", "age", 10)  # Index beyond batch
        ]

        # Call _handle_bad_rows directly
        processor._handle_bad_rows(test_batch, test_batch, validation_results)

        # Should not add any bad rows due to invalid indices
        assert processor.bad_rows_handler.get_bad_row_count() == 0

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_finalize_successful(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test successful finalization."""
        # Setup mocks
        mock_create_config.return_value = Mock()
        mock_schema_validator.return_value = Mock()

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator_instance.finalize.return_value = None  # No exception
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        results = processor.finalize()

        assert results["constraint_validation_passed"] is True
        assert results["has_bad_rows"] is False
        assert "processing_summary" in results
        assert "constraint_violations" in results

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_finalize_with_constraint_error_bad_rows_mode(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test finalization with constraint validation error in bad_rows mode."""
        # Setup mocks
        mock_constraint_config = Mock()
        mock_constraint_config.error_mode.value = "bad_rows"
        mock_create_config.return_value = mock_constraint_config
        mock_schema_validator.return_value = Mock()

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator_instance.finalize.side_effect = Exception("Constraint error")
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)
        processor.constraint_config = mock_constraint_config

        # Should not raise exception in bad_rows mode
        results = processor.finalize()

        assert results["constraint_validation_passed"] is False
        assert results["constraint_error"] == "Constraint error"

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_finalize_with_constraint_error_fail_mode(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test finalization with constraint validation error in fail mode."""
        # Setup mocks
        mock_constraint_config = Mock()
        mock_constraint_config.error_mode.value = "fail_fast"
        mock_create_config.return_value = mock_constraint_config
        mock_schema_validator.return_value = Mock()

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator_instance.finalize.side_effect = Exception("Constraint error")
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)
        processor.constraint_config = mock_constraint_config

        # Should raise exception in fail mode
        with pytest.raises(Exception, match="Constraint error"):
            processor.finalize()

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_finalize_with_bad_rows_output(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test finalization with bad rows output."""
        # Setup mocks
        mock_create_config.return_value = Mock()
        mock_schema_validator.return_value = Mock()

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator_instance.finalize.return_value = None
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        # Add a bad row
        processor.bad_rows_handler.add_bad_row({"id": 1, "name": "John"}, 0)

        with patch.object(processor.bad_rows_handler, "write_bad_rows") as mock_write:
            mock_write.return_value = Path("/tmp/bad_rows.parquet")

            results = processor.finalize()

            assert results["has_bad_rows"] is True
            assert results["bad_rows_file"] == "/tmp/bad_rows.parquet"
            mock_write.assert_called_once()

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_get_constraint_violations_summary(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test getting constraint violations summary."""
        # Setup mocks
        mock_create_config.return_value = Mock()
        mock_schema_validator.return_value = Mock()

        violations = [
            ConstraintViolation("CHECK", "Error 1", ["col1"], [1], "check1", 0),
            ConstraintViolation("CHECK", "Error 2", ["col2"], [2], "check2", 1),
            ConstraintViolation("UNIQUE", "Error 3", ["col3"], [3], "unique1", 2),
            ConstraintViolation("CHECK", "Error 4", ["col1"], [4], "check1", 3),
            ConstraintViolation("CHECK", "Error 5", ["col1"], [5], "check1", 4),
            ConstraintViolation(
                "CHECK", "Error 6", ["col1"], [6], "check1", 5
            ),  # Should be included in samples
        ]

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = violations
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        summary = processor.get_constraint_violations_summary()

        assert summary["total_violations"] == 6
        assert summary["violation_types"]["CHECK"] == 5
        assert summary["violation_types"]["UNIQUE"] == 1
        assert "check1" in summary["affected_constraints"]
        assert "check2" in summary["affected_constraints"]
        assert "unique1" in summary["affected_constraints"]
        assert len(summary["sample_violations"]) <= 10  # Max 5 per type

        # Check sample violations structure
        check_samples = [v for v in summary["sample_violations"] if v["type"] == "CHECK"]
        assert len(check_samples) <= 5

    def test_json_type_to_arrow_type_conversions(self):
        """Test JSON type to Arrow type conversions."""
        # Test integer
        assert _json_type_to_arrow_type({"type": "integer"}) == pa.int64()

        # Test number
        assert _json_type_to_arrow_type({"type": "number"}) == pa.float64()

        # Test boolean
        assert _json_type_to_arrow_type({"type": "boolean"}) == pa.bool_()

        # Test string
        assert _json_type_to_arrow_type({"type": "string"}) == pa.string()

        # Test string with date format
        assert _json_type_to_arrow_type({"type": "string", "format": "date"}) == pa.date32()

        # Test string with date-time format
        assert _json_type_to_arrow_type({"type": "string", "format": "date-time"}) == pa.timestamp(
            "us"
        )

        # Test array
        assert _json_type_to_arrow_type({"type": "array"}) == pa.list_(pa.string())

        # Test unknown type (defaults to string)
        assert _json_type_to_arrow_type({"type": "unknown"}) == pa.string()

        # Test missing type (defaults to string)
        assert _json_type_to_arrow_type({}) == pa.string()

    def test_create_enhanced_processor_from_schema_file(self):
        """Test creating enhanced processor from schema file."""
        schema_dict = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "email": {"type": "string", "format": "email"},
            },
            "required": ["id", "name"],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(schema_dict, f)
            schema_file_path = f.name

        try:
            with patch(
                "forklift.processors.enhanced_processor.EnhancedDataProcessor"
            ) as mock_processor:
                mock_processor_instance = Mock()
                mock_processor.return_value = mock_processor_instance

                result = create_enhanced_processor_from_schema_file(
                    schema_file_path,
                    bad_rows_output_path="/tmp/bad_rows.parquet",
                    error_mode="fail_fast",
                )

                assert result == mock_processor_instance

                # Verify the processor was called with correct arguments
                call_args = mock_processor.call_args
                assert call_args[1]["bad_rows_config"].output_path == "/tmp/bad_rows.parquet"

                # Verify schema dict was modified with error mode
                schema_arg = call_args[1]["schema_dict"]
                assert schema_arg["x-constraintHandling"]["errorMode"] == "fail_fast"

        finally:
            Path(schema_file_path).unlink()

    def test_create_enhanced_processor_from_schema_file_with_existing_constraint_handling(self):
        """Test creating processor from schema file with existing constraint handling config."""
        schema_dict = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "x-constraintHandling": {
                "errorMode": "bad_rows",
                "existingConfig": "should_be_preserved",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(schema_dict, f)
            schema_file_path = f.name

        try:
            with patch(
                "forklift.processors.enhanced_processor.EnhancedDataProcessor"
            ) as mock_processor:
                mock_processor_instance = Mock()
                mock_processor.return_value = mock_processor_instance

                result = create_enhanced_processor_from_schema_file(
                    schema_file_path, error_mode="fail_complete"
                )

                # Verify existing config was preserved but error mode was overridden
                call_args = mock_processor.call_args
                schema_arg = call_args[1]["schema_dict"]
                assert schema_arg["x-constraintHandling"]["errorMode"] == "fail_complete"
                assert (
                    schema_arg["x-constraintHandling"]["existingConfig"] == "should_be_preserved"
                )

        finally:
            Path(schema_file_path).unlink()

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_handle_bad_rows_with_invalid_values(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test handling bad rows with invalid values in batch."""
        # Setup mocks
        mock_create_config.return_value = Mock()
        mock_schema_validator.return_value = Mock()
        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        # Create batch with null/invalid values
        test_batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, None, 3],
                "name": ["Alice", "Bob", None],
                "age": [25, 30, None],
                "email": ["alice@test.com", None, "charlie@test.com"],
            }
        )

        # Create validation result for row index 1 (second row) which has None for id
        validation_results = [
            ValidationResult(
                is_valid=False,
                error_code="NULL_ERROR",
                error_message="Null value not allowed",
                column_name="id",
                row_index=1,
            )
        ]

        processor._handle_bad_rows(test_batch, test_batch, validation_results)

        # Should add one bad row
        assert processor.bad_rows_handler.get_bad_row_count() == 1

        # Check that the bad row was properly captured
        # The bad row should be stored in the bad_rows list
        assert len(processor.bad_rows_handler.bad_rows) == 1
        bad_row = processor.bad_rows_handler.bad_rows[0]

        # Check that null values are handled correctly
        # Row index 1 corresponds to [None, 'Bob', 30, None]
        assert bad_row["original_data"]["id"] is None
        assert bad_row["original_data"]["name"] == "Bob"
        assert bad_row["original_data"]["age"] == 30
        assert bad_row["original_data"]["email"] is None

    @patch("forklift.processors.enhanced_processor.SchemaValidator")
    @patch("forklift.processors.enhanced_processor.ConstraintValidator")
    @patch("forklift.processors.enhanced_processor.create_constraint_config_from_schema")
    def test_empty_batch_processing(
        self, mock_create_config, mock_constraint_validator, mock_schema_validator
    ):
        """Test processing empty batches."""
        # Setup mocks
        mock_create_config.return_value = Mock()

        mock_schema_validator_instance = Mock()
        empty_batch = pa.RecordBatch.from_pydict({"id": [], "name": [], "age": [], "email": []})
        mock_schema_validator_instance.process_batch.return_value = (empty_batch, [])
        mock_schema_validator.return_value = mock_schema_validator_instance

        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.process_batch.return_value = (empty_batch, [])
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        processor = EnhancedDataProcessor(self.test_schema)

        result_batch, validation_results = processor.process_batch(empty_batch)

        assert result_batch.num_rows == 0
        assert len(validation_results) == 0
        assert processor.bad_rows_handler.row_count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
