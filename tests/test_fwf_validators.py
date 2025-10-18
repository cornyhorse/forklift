"""Tests for FWF validation utilities."""

from unittest.mock import Mock

import pytest

from forklift.inputs.config import FwfConditionalSchema, FwfFieldSpec
from forklift.inputs.fwf.validators import (FwfFieldValidator,
                                            FwfSchemaValidator)


class TestFwfFieldValidator:
    """Test cases for FWF field validator."""

    def test_validate_field_spec_valid(self):
        """Test validation of valid field specifications."""
        field = FwfFieldSpec(name="test_field", start=1, length=10, parquet_type="string")

        # Should not raise any exception
        FwfFieldValidator.validate_field_spec(field)

    def test_validate_field_spec_negative_start(self):
        """Test validation with negative start position."""
        field = FwfFieldSpec(name="test_field", start=-1, length=10, parquet_type="string")

        with pytest.raises(ValueError, match="start.*cannot be negative"):
            FwfFieldValidator.validate_field_spec(field)

    def test_validate_field_spec_zero_start(self):
        """Test validation with zero start position (should be 1-based)."""
        field = FwfFieldSpec(name="test_field", start=0, length=10, parquet_type="string")

        with pytest.raises(ValueError, match="start.*must be greater than 0"):
            FwfFieldValidator.validate_field_spec(field)

    def test_validate_field_spec_zero_length(self):
        """Test validation when length is zero."""
        field = FwfFieldSpec(name="test_field", start=1, length=0, parquet_type="string")

        with pytest.raises(ValueError, match="length.*must be greater than 0"):
            FwfFieldValidator.validate_field_spec(field)

    def test_validate_field_spec_negative_length(self):
        """Test validation when length is negative."""
        field = FwfFieldSpec(name="test_field", start=1, length=-5, parquet_type="string")

        with pytest.raises(ValueError, match="length.*must be greater than 0"):
            FwfFieldValidator.validate_field_spec(field)

    def test_validate_field_spec_empty_name(self):
        """Test validation with empty field name."""
        field = FwfFieldSpec(name="", start=1, length=10, parquet_type="string")

        with pytest.raises(ValueError, match="Field name cannot be empty"):
            FwfFieldValidator.validate_field_spec(field)

    def test_validate_field_spec_none_name(self):
        """Test validation with None field name."""
        field = FwfFieldSpec(name=None, start=1, length=10, parquet_type="string")

        with pytest.raises(ValueError, match="Field name cannot be empty"):
            FwfFieldValidator.validate_field_spec(field)

    def test_validate_field_spec_invalid_type(self):
        """Test validation with invalid parquet type."""
        field = FwfFieldSpec(name="test_field", start=1, length=10, parquet_type="invalid_type")

        with pytest.raises(ValueError, match="Invalid data type: invalid_type"):
            FwfFieldValidator.validate_field_spec(field)

    def test_validate_data_type_valid_types(self):
        """Test validation of valid data types."""
        valid_types = [
            "string",
            "int32",
            "int64",
            "float32",
            "float64",
            "bool",
            "date32",
            "timestamp",
            "binary",
        ]

        for data_type in valid_types:
            # Should not raise exception
            FwfFieldValidator.validate_data_type(data_type)

    def test_validate_data_type_invalid_types(self):
        """Test validation of invalid data types."""
        invalid_types = ["unknown", "varchar", "number", "text", ""]

        for data_type in invalid_types:
            with pytest.raises(ValueError, match="Invalid data type"):
                FwfFieldValidator.validate_data_type(data_type)

    def test_validate_position_range_valid(self):
        """Test validation of valid position ranges."""
        # Should not raise exception
        FwfFieldValidator.validate_position_range(1, 10)
        FwfFieldValidator.validate_position_range(5, 15)
        FwfFieldValidator.validate_position_range(100, 200)

    def test_validate_position_range_zero_start(self):
        """Test validation with zero start position."""
        with pytest.raises(ValueError, match="Start position.*must be greater than 0"):
            FwfFieldValidator.validate_position_range(0, 10)

    def test_validate_position_range_negative_start(self):
        """Test validation with negative start position."""
        with pytest.raises(ValueError, match="Start position.*cannot be negative"):
            FwfFieldValidator.validate_position_range(-1, 10)

    def test_validate_position_range_zero_length(self):
        """Test validation with zero length."""
        with pytest.raises(ValueError, match="Length.*must be greater than 0"):
            FwfFieldValidator.validate_position_range(5, 0)

    def test_validate_position_range_negative_length(self):
        """Test validation with negative length."""
        with pytest.raises(ValueError, match="Length.*must be greater than 0"):
            FwfFieldValidator.validate_position_range(5, -5)


class TestFwfSchemaValidator:
    """Test cases for FWF schema validator."""

    def test_validate_schema_valid(self):
        """Test validation of valid schema."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=5, parquet_type="string"),
            FwfFieldSpec(name="field2", start=6, length=5, parquet_type="int32"),
            FwfFieldSpec(name="field3", start=11, length=5, parquet_type="float64"),
        ]

        schema = FwfConditionalSchema(flag_value="01", description="Test schema", fields=fields)

        # Should not raise exception
        FwfSchemaValidator.validate_schema(schema)

    def test_validate_schema_overlapping_fields(self):
        """Test validation with overlapping field positions."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=10, parquet_type="string"),
            FwfFieldSpec(
                name="field2", start=5, length=10, parquet_type="int32"
            ),  # Overlaps with field1
        ]

        schema = FwfConditionalSchema(flag_value="01", description="Test schema", fields=fields)

        with pytest.raises(ValueError, match="Field positions overlap"):
            FwfSchemaValidator.validate_schema(schema)

    def test_validate_schema_adjacent_fields(self):
        """Test validation with adjacent but non-overlapping fields."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=5, parquet_type="string"),
            FwfFieldSpec(
                name="field2", start=6, length=5, parquet_type="int32"
            ),  # Adjacent to field1
            FwfFieldSpec(
                name="field3", start=11, length=5, parquet_type="float64"
            ),  # Adjacent to field2
        ]

        schema = FwfConditionalSchema(flag_value="01", description="Test schema", fields=fields)

        # Adjacent fields should be valid (no overlap)
        FwfSchemaValidator.validate_schema(schema)

    def test_validate_schema_duplicate_names(self):
        """Test validation with duplicate field names."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=5, parquet_type="string"),
            FwfFieldSpec(name="field1", start=6, length=5, parquet_type="int32"),  # Duplicate name
        ]

        schema = FwfConditionalSchema(flag_value="01", description="Test schema", fields=fields)

        with pytest.raises(ValueError, match="Duplicate field name"):
            FwfSchemaValidator.validate_schema(schema)

    def test_validate_schema_empty_fields(self):
        """Test validation with empty field list."""
        schema = FwfConditionalSchema(flag_value="01", description="Test schema", fields=[])

        with pytest.raises(ValueError, match="Schema must have at least one field"):
            FwfSchemaValidator.validate_schema(schema)

    def test_validate_schema_none_fields(self):
        """Test validation with None field list."""
        schema = FwfConditionalSchema(flag_value="01", description="Test schema", fields=None)

        with pytest.raises(ValueError, match="Schema must have at least one field"):
            FwfSchemaValidator.validate_schema(schema)

    def test_validate_field_positions_no_overlap(self):
        """Test field position validation with no overlaps."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=5, parquet_type="string"),
            FwfFieldSpec(name="field2", start=10, length=5, parquet_type="int32"),
            FwfFieldSpec(name="field3", start=20, length=5, parquet_type="float64"),
        ]

        # Should not raise exception
        FwfSchemaValidator.validate_field_positions(fields)

    def test_validate_field_positions_with_overlap(self):
        """Test field position validation with overlaps."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=10, parquet_type="string"),
            FwfFieldSpec(
                name="field2", start=8, length=8, parquet_type="int32"
            ),  # Overlaps with field1
        ]

        with pytest.raises(ValueError, match="Field positions overlap"):
            FwfSchemaValidator.validate_field_positions(fields)

    def test_validate_field_names_unique(self):
        """Test field name validation with unique names."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=5, parquet_type="string"),
            FwfFieldSpec(name="field2", start=6, length=5, parquet_type="int32"),
            FwfFieldSpec(name="field3", start=11, length=5, parquet_type="float64"),
        ]

        # Should not raise exception
        FwfSchemaValidator.validate_field_names(fields)

    def test_validate_field_names_duplicate(self):
        """Test field name validation with duplicate names."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=5, parquet_type="string"),
            FwfFieldSpec(name="field2", start=6, length=5, parquet_type="int32"),
            FwfFieldSpec(name="field1", start=11, length=5, parquet_type="float64"),  # Duplicate
        ]

        with pytest.raises(ValueError, match="Duplicate field name"):
            FwfSchemaValidator.validate_field_names(fields)

    def test_validate_field_names_case_sensitive(self):
        """Test that field name validation is case sensitive."""
        fields = [
            FwfFieldSpec(name="field1", start=1, length=5, parquet_type="string"),
            FwfFieldSpec(name="Field1", start=6, length=5, parquet_type="int32"),  # Different case
            FwfFieldSpec(
                name="FIELD1", start=11, length=5, parquet_type="float64"
            ),  # Different case
        ]

        # Should be valid since field names are case sensitive
        FwfSchemaValidator.validate_field_names(fields)
