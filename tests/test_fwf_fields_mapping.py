"""Tests for FWF field mapping functionality."""

import pytest

from forklift.schema.fwf.fields.mapping import FieldMapper


class TestFieldMapper:
    """Test cases for FieldMapper class."""

    def test_get_all_possible_fields_traditional_no_conditional(self):
        """Test get_all_possible_fields with traditional fields (no conditional schemas)."""
        traditional_fields = [
            {"name": "id", "start": 1, "length": 5, "parquetType": "int32"},
            {"name": "name", "start": 6, "length": 20, "parquetType": "string"},
            {"name": "age", "start": 26, "length": 3, "parquetType": "int32"},
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=False,
            traditional_fields=traditional_fields,
            flag_column=None,
            schema_variants=[],
        )

        assert len(result) == 3
        assert "id" in result
        assert "name" in result
        assert "age" in result
        assert result["id"]["parquetType"] == "int32"
        assert result["name"]["parquetType"] == "string"
        assert result["age"]["parquetType"] == "int32"

    def test_get_all_possible_fields_traditional_empty_fields(self):
        """Test get_all_possible_fields with empty traditional fields."""
        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=False,
            traditional_fields=[],
            flag_column=None,
            schema_variants=[],
        )

        assert result == {}

    def test_get_all_possible_fields_traditional_fields_without_names(self):
        """Test get_all_possible_fields with traditional fields missing names."""
        traditional_fields = [
            {"start": 1, "length": 5, "parquetType": "int32"},  # No name
            {"name": "valid_field", "start": 6, "length": 10, "parquetType": "string"},
            {"name": "", "start": 16, "length": 5, "parquetType": "int32"},  # Empty name
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=False,
            traditional_fields=traditional_fields,
            flag_column=None,
            schema_variants=[],
        )

        assert len(result) == 1
        assert "valid_field" in result
        assert result["valid_field"]["parquetType"] == "string"

    def test_get_all_possible_fields_conditional_with_flag_column(self):
        """Test get_all_possible_fields with conditional schemas and flag column."""
        flag_column = {"name": "record_type", "start": 1, "length": 1, "parquetType": "string"}
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"name": "employee_id", "start": 2, "length": 6, "parquetType": "string"},
                    {"name": "salary", "start": 8, "length": 8, "parquetType": "double"},
                ],
            },
            {
                "flagValue": "B",
                "fields": [
                    {"name": "customer_id", "start": 2, "length": 6, "parquetType": "string"},
                    {"name": "credit_limit", "start": 8, "length": 8, "parquetType": "double"},
                ],
            },
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=flag_column,
            schema_variants=schema_variants,
        )

        assert len(result) == 5  # flag + 4 fields
        assert "record_type" in result
        assert "employee_id" in result
        assert "salary" in result
        assert "customer_id" in result
        assert "credit_limit" in result

        # Check flag column
        assert result["record_type"]["parquetType"] == "string"

        # Check variant metadata
        assert result["employee_id"]["_appears_in_variants"] == ["A"]
        assert result["customer_id"]["_appears_in_variants"] == ["B"]

    def test_get_all_possible_fields_conditional_no_flag_column(self):
        """Test get_all_possible_fields with conditional schemas but no flag column."""
        schema_variants = [
            {
                "flagValue": "X",
                "fields": [
                    {"name": "field1", "start": 1, "length": 5, "parquetType": "int32"},
                    {"name": "field2", "start": 6, "length": 10, "parquetType": "string"},
                ],
            }
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=None,
            schema_variants=schema_variants,
        )

        assert len(result) == 2
        assert "field1" in result
        assert "field2" in result
        assert result["field1"]["_appears_in_variants"] == ["X"]

    def test_get_all_possible_fields_conditional_flag_column_no_name(self):
        """Test get_all_possible_fields with flag column missing name."""
        flag_column = {"start": 1, "length": 1, "parquetType": "string"}  # No name
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"name": "test_field", "start": 2, "length": 5, "parquetType": "string"}
                ],
            }
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=flag_column,
            schema_variants=schema_variants,
        )

        assert len(result) == 1
        assert "test_field" in result

    def test_get_all_possible_fields_conditional_overlapping_fields(self):
        """Test get_all_possible_fields with overlapping fields across variants."""
        schema_variants = [
            {
                "flagValue": "TYPE1",
                "fields": [
                    {"name": "common_field", "start": 1, "length": 10, "parquetType": "string"},
                    {"name": "type1_specific", "start": 11, "length": 5, "parquetType": "int32"},
                ],
            },
            {
                "flagValue": "TYPE2",
                "fields": [
                    {"name": "common_field", "start": 1, "length": 10, "parquetType": "string"},
                    {"name": "type2_specific", "start": 11, "length": 5, "parquetType": "double"},
                ],
            },
            {
                "flagValue": "TYPE3",
                "fields": [
                    {"name": "common_field", "start": 1, "length": 10, "parquetType": "string"},
                    {"name": "type3_specific", "start": 11, "length": 5, "parquetType": "bool"},
                ],
            },
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=None,
            schema_variants=schema_variants,
        )

        assert len(result) == 4
        assert "common_field" in result
        assert "type1_specific" in result
        assert "type2_specific" in result
        assert "type3_specific" in result

        # Check that common_field appears in all variants
        assert set(result["common_field"]["_appears_in_variants"]) == {"TYPE1", "TYPE2", "TYPE3"}

        # Check that specific fields appear only in their respective variants
        assert result["type1_specific"]["_appears_in_variants"] == ["TYPE1"]
        assert result["type2_specific"]["_appears_in_variants"] == ["TYPE2"]
        assert result["type3_specific"]["_appears_in_variants"] == ["TYPE3"]

    def test_get_all_possible_fields_conditional_fields_without_names(self):
        """Test get_all_possible_fields with variant fields missing names."""
        schema_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"start": 1, "length": 5, "parquetType": "int32"},  # No name
                    {"name": "valid_field", "start": 6, "length": 10, "parquetType": "string"},
                    {"name": "", "start": 16, "length": 5, "parquetType": "int32"},  # Empty name
                ],
            }
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=None,
            schema_variants=schema_variants,
        )

        assert len(result) == 1
        assert "valid_field" in result

    def test_get_all_possible_fields_conditional_empty_variants(self):
        """Test get_all_possible_fields with empty schema variants."""
        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=None,
            schema_variants=[],
        )

        assert result == {}

    def test_get_all_possible_fields_conditional_variants_no_fields(self):
        """Test get_all_possible_fields with variants that have no fields."""
        # Test with variants that have no fields key, empty fields, but NOT None fields
        # because None fields would cause a TypeError in the current implementation
        schema_variants = [
            {"flagValue": "A"},  # No fields key
            {"flagValue": "B", "fields": []},  # Empty fields
        ]

        result = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=None,
            schema_variants=schema_variants,
        )

        assert result == {}

    def test_get_all_possible_fields_conditional_none_fields_error(self):
        """Test that None fields causes a TypeError as expected."""
        schema_variants = [{"flagValue": "C", "fields": None}]  # None fields causes TypeError

        with pytest.raises(TypeError, match="'NoneType' object is not iterable"):
            FieldMapper.get_all_possible_fields(
                has_conditional_schemas=True,
                traditional_fields=[],
                flag_column=None,
                schema_variants=schema_variants,
            )

    def test_get_unified_parquet_schema_with_flag_column(self):
        """Test get_unified_parquet_schema with flag column."""
        flag_column = {"name": "type_flag", "parquetType": "string"}
        all_fields = {
            "type_flag": {"parquetType": "string"},
            "common_field": {"parquetType": "int32", "_appears_in_variants": ["A", "B"]},
            "specific_field": {"parquetType": "double", "_appears_in_variants": ["A"]},
        }
        schema_variants = [{"flagValue": "A"}, {"flagValue": "B"}]

        result = FieldMapper.get_unified_parquet_schema(
            all_fields=all_fields, flag_column=flag_column, schema_variants=schema_variants
        )

        assert len(result) == 3
        assert result["type_flag"] == "string"
        assert result["common_field"] == "int32"  # Appears in all variants
        assert result["specific_field"] == "double"  # Doesn't appear in all variants

    def test_get_unified_parquet_schema_no_flag_column(self):
        """Test get_unified_parquet_schema without flag column."""
        all_fields = {
            "field1": {"parquetType": "string", "_appears_in_variants": ["X", "Y", "Z"]},
            "field2": {"parquetType": "int64", "_appears_in_variants": ["X"]},
        }
        schema_variants = [{"flagValue": "X"}, {"flagValue": "Y"}, {"flagValue": "Z"}]

        result = FieldMapper.get_unified_parquet_schema(
            all_fields=all_fields, flag_column=None, schema_variants=schema_variants
        )

        assert len(result) == 2
        assert result["field1"] == "string"  # Appears in all variants
        assert result["field2"] == "int64"  # Doesn't appear in all variants

    def test_get_unified_parquet_schema_default_types(self):
        """Test get_unified_parquet_schema with fields missing parquetType."""
        all_fields = {
            "field_with_type": {"parquetType": "double"},
            "field_without_type": {"_appears_in_variants": ["A"]},
            "field_empty_type": {"parquetType": "", "_appears_in_variants": ["A"]},
        }
        schema_variants = [{"flagValue": "A"}]

        result = FieldMapper.get_unified_parquet_schema(
            all_fields=all_fields, flag_column=None, schema_variants=schema_variants
        )

        assert result["field_with_type"] == "double"
        assert result["field_without_type"] == "string"  # Default
        # The current implementation returns empty string when parquetType is explicitly ""
        assert result["field_empty_type"] == ""  # Returns empty string as-is

    def test_get_unified_parquet_schema_fields_without_variants_metadata(self):
        """Test get_unified_parquet_schema with fields missing _appears_in_variants."""
        all_fields = {
            "field1": {"parquetType": "int32"},  # No _appears_in_variants
            "field2": {"parquetType": "string", "_appears_in_variants": []},  # Empty list
        }
        schema_variants = [{"flagValue": "A"}]

        result = FieldMapper.get_unified_parquet_schema(
            all_fields=all_fields, flag_column=None, schema_variants=schema_variants
        )

        assert result["field1"] == "int32"
        assert result["field2"] == "string"

    def test_get_unified_parquet_schema_empty_inputs(self):
        """Test get_unified_parquet_schema with empty inputs."""
        result = FieldMapper.get_unified_parquet_schema(
            all_fields={}, flag_column=None, schema_variants=[]
        )

        assert result == {}

    def test_get_unified_parquet_schema_flag_column_default_type(self):
        """Test get_unified_parquet_schema with flag column missing parquetType."""
        flag_column = {"name": "flag_field"}  # No parquetType
        all_fields = {"flag_field": {}}  # No parquetType

        result = FieldMapper.get_unified_parquet_schema(
            all_fields=all_fields, flag_column=flag_column, schema_variants=[]
        )

        assert result["flag_field"] == "string"  # Default type

    def test_field_mapper_class_methods(self):
        """Test that FieldMapper methods are static methods."""
        # Test that methods can be called on the class
        assert callable(FieldMapper.get_all_possible_fields)
        assert callable(FieldMapper.get_unified_parquet_schema)

        # Test that they work when called on an instance too
        mapper = FieldMapper()
        result = mapper.get_all_possible_fields(False, [], None, [])
        assert result == {}

    def test_comprehensive_field_mapping_scenario(self):
        """Test a comprehensive scenario combining both methods."""
        # Set up complex scenario
        flag_column = {"name": "record_type", "start": 1, "length": 1, "parquetType": "string"}
        schema_variants = [
            {
                "flagValue": "E",  # Employee record
                "fields": [
                    {"name": "employee_id", "start": 2, "length": 8, "parquetType": "string"},
                    {"name": "name", "start": 10, "length": 30, "parquetType": "string"},
                    {"name": "salary", "start": 40, "length": 10, "parquetType": "double"},
                    {"name": "department", "start": 50, "length": 10, "parquetType": "string"},
                ],
            },
            {
                "flagValue": "C",  # Customer record
                "fields": [
                    {"name": "customer_id", "start": 2, "length": 8, "parquetType": "string"},
                    {"name": "name", "start": 10, "length": 30, "parquetType": "string"},
                    {"name": "credit_limit", "start": 40, "length": 10, "parquetType": "double"},
                    {"name": "region", "start": 50, "length": 10, "parquetType": "string"},
                ],
            },
        ]

        # Get all possible fields
        all_fields = FieldMapper.get_all_possible_fields(
            has_conditional_schemas=True,
            traditional_fields=[],
            flag_column=flag_column,
            schema_variants=schema_variants,
        )

        # Should have flag column + all unique fields
        assert len(all_fields) == 8  # record_type + 7 unique fields (1 common + 6 variant-specific)
        assert "record_type" in all_fields
        assert "name" in all_fields  # Common field
        assert "employee_id" in all_fields
        assert "salary" in all_fields
        assert "department" in all_fields
        assert "customer_id" in all_fields
        assert "credit_limit" in all_fields
        assert "region" in all_fields

        # Check common field appears in both variants
        assert set(all_fields["name"]["_appears_in_variants"]) == {"E", "C"}

        # Get unified schema
        unified_schema = FieldMapper.get_unified_parquet_schema(
            all_fields=all_fields, flag_column=flag_column, schema_variants=schema_variants
        )

        assert len(unified_schema) == 8
        assert unified_schema["record_type"] == "string"
        assert unified_schema["name"] == "string"  # Common field
        assert unified_schema["employee_id"] == "string"  # Variant-specific
        assert unified_schema["customer_id"] == "string"  # Variant-specific
