"""Tests for FWF field validation functionality."""

import pytest
from unittest.mock import Mock, patch

from forklift.schema.fwf.validation.fields import FieldValidator


class TestFieldValidator:
    """Test cases for FieldValidator class."""

    def test_validate_traditional_fields_empty(self):
        """Test validation with empty fields array."""
        errors = FieldValidator.validate_traditional_fields([])

        assert len(errors) == 1
        assert "x-fwf.fields array is required and cannot be empty" in errors[0]

    def test_validate_traditional_fields_valid(self):
        """Test validation with valid traditional fields."""
        fields = [
            {"name": "id", "start": 1, "length": 5, "type": "string", "parquetType": "string"},
            {"name": "name", "start": 6, "length": 20, "type": "string", "parquetType": "string"}
        ]

        with patch('forklift.schema.fwf.validation.fields.ParquetTypeValidator') as mock_validator:
            mock_validator.is_valid_parquet_type.return_value = True

            errors = FieldValidator.validate_traditional_fields(fields)
            assert errors == []

    def test_validate_traditional_fields_invalid_dict(self):
        """Test validation with non-dictionary field."""
        fields = ["invalid_field"]

        errors = FieldValidator.validate_traditional_fields(fields)

        assert len(errors) == 1
        assert "Field 0 must be a dictionary" in errors[0]

    def test_validate_traditional_fields_overlapping_positions(self):
        """Test validation with overlapping field positions."""
        fields = [
            {"name": "field1", "start": 1, "length": 5},
            {"name": "field2", "start": 3, "length": 5}  # Overlaps with field1
        ]

        errors = FieldValidator.validate_traditional_fields(fields)

        assert any("overlaps with previous field positions" in error for error in errors)

    def test_validate_conditional_fields_missing_flag_column(self):
        """Test validation with missing flag column."""
        conditional_schemas = {"schemas": []}

        errors = FieldValidator.validate_conditional_fields(conditional_schemas)

        assert "conditionalSchemas.flagColumn is required" in errors

    def test_validate_conditional_fields_empty_schemas(self):
        """Test validation with empty schemas array."""
        conditional_schemas = {
            "flagColumn": {"name": "flag", "start": 1, "length": 1}
        }

        with patch('forklift.schema.fwf.validation.fields.FieldValidator._validate_single_field') as mock_validate:
            mock_validate.return_value = []

            errors = FieldValidator.validate_conditional_fields(conditional_schemas)

            assert "conditionalSchemas.schemas array is required and cannot be empty" in errors

    def test_validate_conditional_fields_valid(self):
        """Test validation with valid conditional fields."""
        conditional_schemas = {
            "flagColumn": {"name": "flag", "start": 1, "length": 1},
            "schemas": [
                {
                    "flagValue": "A",
                    "fields": [
                        {"name": "flag", "start": 1, "length": 1},
                        {"name": "data", "start": 2, "length": 10}
                    ]
                }
            ]
        }

        with patch('forklift.schema.fwf.validation.fields.FieldValidator._validate_single_field') as mock_validate:
            mock_validate.return_value = []

            errors = FieldValidator.validate_conditional_fields(conditional_schemas)
            assert errors == []

    def test_validate_conditional_fields_invalid_variant(self):
        """Test validation with invalid schema variant."""
        conditional_schemas = {
            "flagColumn": {"name": "flag", "start": 1, "length": 1},
            "schemas": ["invalid_variant"]
        }

        with patch('forklift.schema.fwf.validation.fields.FieldValidator._validate_single_field') as mock_validate:
            mock_validate.return_value = []

            errors = FieldValidator.validate_conditional_fields(conditional_schemas)

            assert "Schema variant 0 must be a dictionary" in errors

    def test_validate_conditional_fields_missing_flag_value(self):
        """Test validation with missing flag value in variant."""
        conditional_schemas = {
            "flagColumn": {"name": "flag", "start": 1, "length": 1},
            "schemas": [
                {
                    "fields": [{"name": "data", "start": 1, "length": 10}]
                }
            ]
        }

        with patch('forklift.schema.fwf.validation.fields.FieldValidator._validate_single_field') as mock_validate:
            mock_validate.return_value = []

            errors = FieldValidator.validate_conditional_fields(conditional_schemas)

            assert "Schema variant 0 missing required 'flagValue'" in errors

    def test_validate_conditional_fields_missing_fields(self):
        """Test validation with missing fields in variant."""
        conditional_schemas = {
            "flagColumn": {"name": "flag", "start": 1, "length": 1},
            "schemas": [
                {
                    "flagValue": "A"
                }
            ]
        }

        with patch('forklift.schema.fwf.validation.fields.FieldValidator._validate_single_field') as mock_validate:
            mock_validate.return_value = []

            errors = FieldValidator.validate_conditional_fields(conditional_schemas)

            assert "Schema variant 0 missing required 'fields' array" in errors

    def test_validate_single_field_missing_name(self):
        """Test single field validation with missing name."""
        field = {"start": 1, "length": 5}

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 missing required 'name'" in errors

    def test_validate_single_field_missing_start(self):
        """Test single field validation with missing start position."""
        field = {"name": "test", "length": 5}

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 missing required 'start' position" in errors

    def test_validate_single_field_invalid_start(self):
        """Test single field validation with invalid start position."""
        field = {"name": "test", "start": 0, "length": 5}

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 start position must be a positive integer" in errors

    def test_validate_single_field_missing_length(self):
        """Test single field validation with missing length."""
        field = {"name": "test", "start": 1}

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 missing required 'length'" in errors

    def test_validate_single_field_invalid_length(self):
        """Test single field validation with invalid length."""
        field = {"name": "test", "start": 1, "length": 0}

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 length must be a positive integer" in errors

    def test_validate_single_field_invalid_type(self):
        """Test single field validation with invalid type."""
        field = {"name": "test", "start": 1, "length": 5, "type": "invalid_type"}

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 invalid type 'invalid_type'" in errors

    def test_validate_single_field_valid_types(self):
        """Test single field validation with valid types."""
        valid_types = ["string", "integer", "number", "boolean"]

        for valid_type in valid_types:
            field = {"name": "test", "start": 1, "length": 5, "type": valid_type}

            errors = FieldValidator._validate_single_field(field, 0, set())

            # Should not have type-related errors
            assert not any("invalid type" in error for error in errors)

    def test_validate_single_field_invalid_parquet_type(self):
        """Test single field validation with invalid Parquet type."""
        field = {"name": "test", "start": 1, "length": 5, "parquetType": "invalid_parquet"}

        with patch('forklift.schema.fwf.validation.fields.ParquetTypeValidator') as mock_validator:
            mock_validator.is_valid_parquet_type.return_value = False

            errors = FieldValidator._validate_single_field(field, 0, set())

            assert "Field 0 invalid Parquet type 'invalid_parquet'" in errors

    def test_validate_single_field_valid_parquet_type(self):
        """Test single field validation with valid Parquet type."""
        field = {"name": "test", "start": 1, "length": 5, "parquetType": "string"}

        with patch('forklift.schema.fwf.validation.fields.ParquetTypeValidator') as mock_validator:
            mock_validator.is_valid_parquet_type.return_value = True

            errors = FieldValidator._validate_single_field(field, 0, set())

            # Should not have parquet type-related errors
            assert not any("invalid Parquet type" in error for error in errors)

    def test_validate_single_field_invalid_alignment(self):
        """Test single field validation with invalid alignment."""
        field = {"name": "test", "start": 1, "length": 5, "alignment": "invalid_align"}

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 invalid alignment 'invalid_align', must be 'left', 'right', or 'center'" in errors

    def test_validate_single_field_valid_alignments(self):
        """Test single field validation with valid alignments."""
        valid_alignments = ["left", "right", "center"]

        for alignment in valid_alignments:
            field = {"name": "test", "start": 1, "length": 5, "alignment": alignment}

            errors = FieldValidator._validate_single_field(field, 0, set())

            # Should not have alignment-related errors
            assert not any("invalid alignment" in error for error in errors)

    def test_validate_single_field_invalid_pad_char(self):
        """Test single field validation with invalid padding character."""
        field = {"name": "test", "start": 1, "length": 5, "padChar": "ab"}  # Multiple chars

        errors = FieldValidator._validate_single_field(field, 0, set())

        assert "Field 0 padChar must be a single character" in errors

    def test_validate_single_field_valid_pad_char(self):
        """Test single field validation with valid padding character."""
        field = {"name": "test", "start": 1, "length": 5, "padChar": " "}

        errors = FieldValidator._validate_single_field(field, 0, set())

        # Should not have padChar-related errors
        assert not any("padChar must be a single character" in error for error in errors)

    def test_validate_single_field_position_overlap(self):
        """Test single field validation with position overlap."""
        positions_used = {1, 2, 3}
        field = {"name": "test", "start": 2, "length": 3}  # Overlaps positions 2,3,4

        errors = FieldValidator._validate_single_field(field, 0, positions_used)

        assert "Field 0 overlaps with previous field positions" in errors

    def test_validate_single_field_no_overlap(self):
        """Test single field validation without position overlap."""
        positions_used = {1, 2, 3}
        field = {"name": "test", "start": 5, "length": 3}  # Uses positions 5,6,7

        errors = FieldValidator._validate_single_field(field, 0, positions_used)

        # Should not have overlap-related errors
        assert not any("overlaps with previous field positions" in error for error in errors)

    def test_validate_single_field_complete_valid(self):
        """Test single field validation with completely valid field."""
        field = {
            "name": "test_field",
            "start": 1,
            "length": 10,
            "type": "string",
            "parquetType": "string",
            "alignment": "left",
            "padChar": " "
        }

        with patch('forklift.schema.fwf.validation.fields.ParquetTypeValidator') as mock_validator:
            mock_validator.is_valid_parquet_type.return_value = True

            errors = FieldValidator._validate_single_field(field, 0, set())
            assert errors == []
