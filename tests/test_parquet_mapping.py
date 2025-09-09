"""Tests for Parquet mapping utilities."""

import pytest
from unittest.mock import patch

from forklift.schema.fwf.utils.parquet_mapping import ParquetMappingUtils


class TestParquetMappingUtils:
    """Test cases for ParquetMappingUtils class."""

    def test_validate_parquet_types_in_fields_empty_list(self):
        """Test validation with empty fields list."""
        result = ParquetMappingUtils.validate_parquet_types_in_fields([])
        assert result == []

    def test_validate_parquet_types_in_fields_no_parquet_type(self):
        """Test validation with fields that don't have parquetType."""
        fields = [
            {"name": "field1", "type": "string"},
            {"name": "field2", "width": 10}
        ]
        result = ParquetMappingUtils.validate_parquet_types_in_fields(fields)
        assert result == []

    def test_validate_parquet_types_in_fields_valid_parquet_types(self):
        """Test validation with valid Parquet types."""
        fields = [
            {"name": "field1", "parquetType": "string"},
            {"name": "field2", "parquetType": "int32"},
            {"name": "field3", "parquetType": "double"}
        ]

        with patch('forklift.schema.fwf.utils.parquet_mapping.ParquetTypeValidator.is_valid_parquet_type', return_value=True):
            result = ParquetMappingUtils.validate_parquet_types_in_fields(fields)
            assert result == []

    def test_validate_parquet_types_in_fields_invalid_parquet_types(self):
        """Test validation with invalid Parquet types."""
        fields = [
            {"name": "field1", "parquetType": "invalid_type"},
            {"name": "field2", "parquetType": "string"},
            {"name": "field3", "parquetType": "another_invalid"}
        ]

        def mock_validator(parquet_type):
            return parquet_type in ["string", "int32", "double"]

        with patch('forklift.schema.fwf.utils.parquet_mapping.ParquetTypeValidator.is_valid_parquet_type', side_effect=mock_validator):
            result = ParquetMappingUtils.validate_parquet_types_in_fields(fields)

            expected_errors = [
                "Field 0 invalid Parquet type 'invalid_type'",
                "Field 2 invalid Parquet type 'another_invalid'"
            ]
            assert result == expected_errors

    def test_validate_parquet_types_in_fields_mixed_field_types(self):
        """Test validation with mixed field types (dict and non-dict)."""
        fields = [
            {"name": "field1", "parquetType": "string"},
            "not_a_dict",
            {"name": "field3", "parquetType": "invalid_type"},
            None
        ]

        def mock_validator(parquet_type):
            return parquet_type == "string"

        with patch('forklift.schema.fwf.utils.parquet_mapping.ParquetTypeValidator.is_valid_parquet_type', side_effect=mock_validator):
            result = ParquetMappingUtils.validate_parquet_types_in_fields(fields)

            expected_errors = ["Field 2 invalid Parquet type 'invalid_type'"]
            assert result == expected_errors

    def test_validate_parquet_types_in_variants_empty_list(self):
        """Test validation with empty variants list."""
        result = ParquetMappingUtils.validate_parquet_types_in_variants([])
        assert result == []

    def test_validate_parquet_types_in_variants_no_fields(self):
        """Test validation with variants that have no fields."""
        variants = [
            {"condition": "some_condition"},
            {"condition": "another_condition", "fields": []}
        ]
        result = ParquetMappingUtils.validate_parquet_types_in_variants(variants)
        assert result == []

    def test_validate_parquet_types_in_variants_valid_parquet_types(self):
        """Test validation with valid Parquet types in variants."""
        variants = [
            {
                "condition": "condition1",
                "fields": [
                    {"name": "field1", "parquetType": "string"},
                    {"name": "field2", "parquetType": "int32"}
                ]
            },
            {
                "condition": "condition2",
                "fields": [
                    {"name": "field3", "parquetType": "double"}
                ]
            }
        ]

        with patch('forklift.schema.fwf.utils.parquet_mapping.ParquetTypeValidator.is_valid_parquet_type', return_value=True):
            result = ParquetMappingUtils.validate_parquet_types_in_variants(variants)
            assert result == []

    def test_validate_parquet_types_in_variants_invalid_parquet_types(self):
        """Test validation with invalid Parquet types in variants."""
        variants = [
            {
                "condition": "condition1",
                "fields": [
                    {"name": "field1", "parquetType": "invalid_type1"},
                    {"name": "field2", "parquetType": "string"}
                ]
            },
            {
                "condition": "condition2",
                "fields": [
                    {"name": "field3", "parquetType": "invalid_type2"},
                    {"name": "field4", "parquetType": "another_invalid"}
                ]
            }
        ]

        def mock_validator(parquet_type):
            return parquet_type in ["string", "int32", "double"]

        with patch('forklift.schema.fwf.utils.parquet_mapping.ParquetTypeValidator.is_valid_parquet_type', side_effect=mock_validator):
            result = ParquetMappingUtils.validate_parquet_types_in_variants(variants)

            expected_errors = [
                "Variant 0 field 0 invalid Parquet type 'invalid_type1'",
                "Variant 1 field 0 invalid Parquet type 'invalid_type2'",
                "Variant 1 field 1 invalid Parquet type 'another_invalid'"
            ]
            assert result == expected_errors

    def test_validate_parquet_types_in_variants_mixed_field_types(self):
        """Test validation with mixed field types in variants."""
        variants = [
            {
                "condition": "condition1",
                "fields": [
                    {"name": "field1", "parquetType": "string"},
                    "not_a_dict",
                    {"name": "field3", "parquetType": "invalid_type"},
                    None
                ]
            }
        ]

        def mock_validator(parquet_type):
            return parquet_type == "string"

        with patch('forklift.schema.fwf.utils.parquet_mapping.ParquetTypeValidator.is_valid_parquet_type', side_effect=mock_validator):
            result = ParquetMappingUtils.validate_parquet_types_in_variants(variants)

            expected_errors = ["Variant 0 field 2 invalid Parquet type 'invalid_type'"]
            assert result == expected_errors

    def test_validate_parquet_types_in_variants_no_parquet_type_field(self):
        """Test validation with fields that don't have parquetType in variants."""
        variants = [
            {
                "condition": "condition1",
                "fields": [
                    {"name": "field1", "type": "string"},
                    {"name": "field2", "width": 10}
                ]
            }
        ]
        result = ParquetMappingUtils.validate_parquet_types_in_variants(variants)
        assert result == []

    def test_validate_parquet_types_comprehensive_workflow(self):
        """Test a comprehensive workflow with both methods."""
        # Test fields validation
        fields = [
            {"name": "id", "parquetType": "int64"},
            {"name": "name", "parquetType": "string"},
            {"name": "invalid_field", "parquetType": "bad_type"}
        ]

        # Test variants validation
        variants = [
            {
                "condition": "version == 1",
                "fields": [
                    {"name": "old_field", "parquetType": "string"},
                    {"name": "bad_field", "parquetType": "bad_type"}
                ]
            }
        ]

        def mock_validator(parquet_type):
            return parquet_type in ["int64", "string", "double"]

        with patch('forklift.schema.fwf.utils.parquet_mapping.ParquetTypeValidator.is_valid_parquet_type', side_effect=mock_validator):
            field_errors = ParquetMappingUtils.validate_parquet_types_in_fields(fields)
            variant_errors = ParquetMappingUtils.validate_parquet_types_in_variants(variants)

            expected_field_errors = ["Field 2 invalid Parquet type 'bad_type'"]
            expected_variant_errors = ["Variant 0 field 1 invalid Parquet type 'bad_type'"]

            assert field_errors == expected_field_errors
            assert variant_errors == expected_variant_errors
