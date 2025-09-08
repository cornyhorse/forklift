"""Comprehensive tests for enhanced processor module."""

import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import json

from forklift.processors.enhanced_processor import (
    EnhancedDataProcessor,
    create_enhanced_processor_from_schema_file,
    _json_type_to_arrow_type
)
from forklift.processors.base import ValidationResult
from forklift.processors.constraint_validator import ConstraintConfig
from forklift.processors.bad_rows_handler import BadRowsConfig


class TestEnhancedDataProcessor:
    """Test EnhancedDataProcessor class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('age', pa.int32())
        ])

        self.schema_dict = {
            "type": "object",
            "properties": {
                "id": {"type": "integer", "minimum": 1},
                "name": {"type": "string", "minLength": 1},
                "age": {"type": "integer", "minimum": 0, "maximum": 150}
            }
        }

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_init_basic(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test basic initialization."""
        processor = EnhancedDataProcessor(self.schema)

        assert processor.schema == self.schema
        assert processor.schema_dict == {}
        assert processor.strict_mode is True

        # Verify components were initialized
        mock_schema.assert_called_once()
        mock_constraint.assert_called_once()
        mock_bad_rows.assert_called_once()

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_init_with_all_options(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test initialization with all options."""
        constraint_config = ConstraintConfig()
        bad_rows_config = BadRowsConfig()

        processor = EnhancedDataProcessor(
            schema=self.schema,
            schema_dict=self.schema_dict,
            constraint_config=constraint_config,
            bad_rows_config=bad_rows_config,
            strict_mode=False
        )

        assert processor.schema == self.schema
        assert processor.schema_dict == self.schema_dict
        assert processor.strict_mode is False
        assert processor.constraint_config == constraint_config

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_process_batch_success(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test successful batch processing."""
        # Create test batch
        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        }, schema=self.schema)

        # Mock validator responses
        mock_schema_instance = MagicMock()
        mock_schema_instance.process_batch.return_value = (batch, [])
        mock_schema.return_value = mock_schema_instance

        mock_constraint_instance = MagicMock()
        mock_constraint_instance.process_batch.return_value = (batch, [])
        mock_constraint.return_value = mock_constraint_instance

        mock_bad_rows_instance = MagicMock()
        mock_bad_rows.return_value = mock_bad_rows_instance

        processor = EnhancedDataProcessor(self.schema)
        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch == batch
        assert validation_results == []

        # Verify validators were called
        mock_schema_instance.process_batch.assert_called_once_with(batch)
        mock_constraint_instance.process_batch.assert_called_once_with(batch)

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_process_batch_with_validation_errors(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test batch processing with validation errors."""
        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        }, schema=self.schema)

        # Mock validation errors
        schema_error = ValidationResult(False, "Schema error", "SCHEMA_ERROR")
        constraint_error = ValidationResult(False, "Constraint error", "CONSTRAINT_ERROR")

        mock_schema_instance = MagicMock()
        mock_schema_instance.process_batch.return_value = (batch, [schema_error])
        mock_schema.return_value = mock_schema_instance

        mock_constraint_instance = MagicMock()
        mock_constraint_instance.process_batch.return_value = (batch, [constraint_error])
        mock_constraint.return_value = mock_constraint_instance

        mock_bad_rows_instance = MagicMock()
        mock_bad_rows.return_value = mock_bad_rows_instance

        processor = EnhancedDataProcessor(self.schema)
        processor._handle_bad_rows = MagicMock(return_value=batch)

        result_batch, validation_results = processor.process_batch(batch)

        assert len(validation_results) == 2
        assert schema_error in validation_results
        assert constraint_error in validation_results

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_handle_bad_rows(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test bad rows handling."""
        original_batch = pa.record_batch({
            'id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', '', 'David'],
            'age': [25, 30, -5, 35]
        }, schema=self.schema)

        valid_batch = pa.record_batch({
            'id': [1, 2, 4],
            'name': ['Alice', 'Bob', 'David'],
            'age': [25, 30, 35]
        }, schema=self.schema)

        validation_results = [
            ValidationResult(False, "Empty name", "EMPTY_NAME", row_number=2),
            ValidationResult(False, "Invalid age", "INVALID_AGE", row_number=2)
        ]

        mock_bad_rows_instance = MagicMock()
        mock_bad_rows_instance.handle_bad_rows.return_value = valid_batch
        mock_bad_rows.return_value = mock_bad_rows_instance

        processor = EnhancedDataProcessor(self.schema)

        result = processor._handle_bad_rows(original_batch, valid_batch, validation_results)

        assert result == valid_batch
        mock_bad_rows_instance.handle_bad_rows.assert_called_once()

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_extract_error_handling_mode(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test error handling mode extraction."""
        processor = EnhancedDataProcessor(self.schema)

        # Test default mode
        mode = processor._extract_error_handling_mode()
        assert mode in ["strict", "lenient", "ignore"]

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_finalize(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test finalize method."""
        mock_bad_rows_instance = MagicMock()
        mock_bad_rows_instance.get_summary.return_value = {"bad_rows": 5}
        mock_bad_rows.return_value = mock_bad_rows_instance

        processor = EnhancedDataProcessor(self.schema)

        summary = processor.finalize()

        assert isinstance(summary, dict)
        assert "bad_rows_summary" in summary
        assert "constraint_violations_summary" in summary
        assert "processing_summary" in summary

    @patch('forklift.processors.enhanced_processor.SchemaValidator')
    @patch('forklift.processors.enhanced_processor.ConstraintValidator')
    @patch('forklift.processors.enhanced_processor.BadRowsHandler')
    def test_get_constraint_violations_summary(self, mock_bad_rows, mock_constraint, mock_schema):
        """Test constraint violations summary."""
        mock_constraint_instance = MagicMock()
        mock_constraint_instance.get_violation_summary.return_value = {"violations": 10}
        mock_constraint.return_value = mock_constraint_instance

        processor = EnhancedDataProcessor(self.schema)

        summary = processor.get_constraint_violations_summary()

        assert isinstance(summary, dict)
        mock_constraint_instance.get_violation_summary.assert_called_once()


class TestCreateEnhancedProcessorFromSchemaFile:
    """Test create_enhanced_processor_from_schema_file function."""

    @patch('forklift.processors.enhanced_processor.Path.exists')
    @patch('forklift.processors.enhanced_processor.Path.open')
    @patch('forklift.processors.enhanced_processor.json.load')
    def test_create_from_schema_file_basic(self, mock_json_load, mock_open, mock_exists):
        """Test creating processor from schema file."""
        # Mock file existence and content
        mock_exists.return_value = True

        schema_content = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            }
        }
        mock_json_load.return_value = schema_content

        with patch('forklift.processors.enhanced_processor.EnhancedDataProcessor') as mock_processor:
            result = create_enhanced_processor_from_schema_file("schema.json")

            mock_processor.assert_called_once()

    @patch('forklift.processors.enhanced_processor.Path.exists')
    def test_create_from_schema_file_not_found(self, mock_exists):
        """Test creating processor from non-existent schema file."""
        mock_exists.return_value = False

        with pytest.raises(FileNotFoundError, match="Schema file not found"):
            create_enhanced_processor_from_schema_file("nonexistent.json")

    @patch('forklift.processors.enhanced_processor.Path.exists')
    @patch('forklift.processors.enhanced_processor.Path.open')
    @patch('forklift.processors.enhanced_processor.json.load')
    def test_create_from_schema_file_with_options(self, mock_json_load, mock_open, mock_exists):
        """Test creating processor from schema file with options."""
        mock_exists.return_value = True

        schema_content = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            }
        }
        mock_json_load.return_value = schema_content

        bad_rows_config = BadRowsConfig()

        with patch('forklift.processors.enhanced_processor.EnhancedDataProcessor') as mock_processor:
            result = create_enhanced_processor_from_schema_file(
                "schema.json",
                bad_rows_config=bad_rows_config,
                strict_mode=False
            )

            mock_processor.assert_called_once()


class TestJsonTypeToArrowType:
    """Test _json_type_to_arrow_type function."""

    def test_string_type(self):
        """Test string type conversion."""
        field_def = {"type": "string"}
        result = _json_type_to_arrow_type(field_def)
        assert result == pa.string()

    def test_integer_type(self):
        """Test integer type conversion."""
        field_def = {"type": "integer"}
        result = _json_type_to_arrow_type(field_def)
        assert result == pa.int64()

    def test_number_type(self):
        """Test number type conversion."""
        field_def = {"type": "number"}
        result = _json_type_to_arrow_type(field_def)
        assert result == pa.float64()

    def test_boolean_type(self):
        """Test boolean type conversion."""
        field_def = {"type": "boolean"}
        result = _json_type_to_arrow_type(field_def)
        assert result == pa.bool_()

    def test_array_type(self):
        """Test array type conversion."""
        field_def = {"type": "array", "items": {"type": "string"}}
        result = _json_type_to_arrow_type(field_def)
        assert isinstance(result, pa.ListType)

    def test_unknown_type(self):
        """Test unknown type conversion defaults to string."""
        field_def = {"type": "unknown"}
        result = _json_type_to_arrow_type(field_def)
        assert result == pa.string()

    def test_missing_type(self):
        """Test missing type defaults to string."""
        field_def = {}
        result = _json_type_to_arrow_type(field_def)
        assert result == pa.string()


class TestEnhancedProcessorIntegration:
    """Test enhanced processor integration scenarios."""

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.processors.enhanced_processor import (
            EnhancedDataProcessor,
            create_enhanced_processor_from_schema_file,
            _json_type_to_arrow_type
        )

        assert EnhancedDataProcessor is not None
        assert callable(create_enhanced_processor_from_schema_file)
        assert callable(_json_type_to_arrow_type)

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.processors.enhanced_processor as enhanced_module

        assert enhanced_module.__doc__ is not None
        assert "Enhanced data processor" in enhanced_module.__doc__
