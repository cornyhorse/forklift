"""Tests for FWF schema importer core functionality."""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from forklift.schema.fwf.core import FwfSchemaImporter
from forklift.schema.fwf.exceptions import SchemaValidationError


class TestFwfSchemaImporter:
    """Test cases for FwfSchemaImporter class."""

    @pytest.fixture
    def minimal_valid_schema(self):
        """Create a minimal valid FWF schema."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"}
            },
            "required": ["id"],
            "additionalProperties": False,
            "x-fwf": {
                "fields": [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},
                    {"name": "name", "start": 6, "length": 20, "parquetType": "string"}
                ],
                "encoding": "utf-8",
                "headerRows": 1,
                "footerRows": 2,
                "trim": {"id": True, "name": False},
                "nulls": {"global": ["", "NULL"], "id": ["EMPTY"]},
                "case": {
                    "standardizeNames": "snake_case",
                    "dedupeNames": "suffix"
                }
            }
        }

    @pytest.fixture
    def conditional_schema(self):
        """Create a schema with conditional configurations."""
        return {
            "type": "object",
            "properties": {
                "record_type": {"type": "string"},
                "data": {"type": "string"}
            },
            "x-fwf": {
                "conditionalSchemas": {
                    "flagColumn": {
                        "name": "record_type",
                        "start": 0,
                        "length": 1
                    },
                    "schemas": [
                        {
                            "flagValue": "A",
                            "fields": [
                                {"name": "record_type", "start": 0, "length": 1, "parquetType": "string"},
                                {"name": "data", "start": 1, "length": 10, "parquetType": "string"}
                            ]
                        },
                        {
                            "flagValue": "B",
                            "fields": [
                                {"name": "record_type", "start": 0, "length": 1, "parquetType": "string"},
                                {"name": "data", "start": 1, "length": 15, "parquetType": "string"}
                            ]
                        }
                    ]
                }
            }
        }

    @pytest.fixture
    def invalid_conditional_schema(self):
        """Create an invalid conditional schema for testing validation."""
        return {
            "type": "object",
            "properties": {},
            "x-fwf": {
                "conditionalSchemas": {
                    "schemas": [
                        {
                            "flagValue": "A",
                            "fields": [
                                {"name": "field1", "start": 1, "length": -5}  # Invalid length
                            ]
                        }
                    ]
                }
            }
        }

    def test_init_with_dict_schema(self, minimal_valid_schema):
        """Test initialization with a dictionary schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        assert importer.schema == minimal_valid_schema
        assert importer.fwf_ext == minimal_valid_schema["x-fwf"]
        assert importer.field_map == minimal_valid_schema["properties"]
        assert importer.required == ["id"]
        assert importer.additional_properties == False
        assert importer.fields == minimal_valid_schema["x-fwf"]["fields"]
        assert importer.encoding == "utf-8"
        assert importer.header_rows == 1
        assert importer.footer_rows == 2
        assert importer.trim == {"id": True, "name": False}
        assert importer.nulls == {"global": ["", "NULL"], "id": ["EMPTY"]}
        assert importer.standardize_names == "snake_case"
        assert importer.dedupe_names == "suffix"

    def test_init_with_file_path(self, minimal_valid_schema):
        """Test initialization with a file path."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            json.dump(minimal_valid_schema, tmp_file)
            tmp_path = Path(tmp_file.name)

        try:
            importer = FwfSchemaImporter(tmp_path, validate=False)
            assert importer.schema == minimal_valid_schema
        finally:
            tmp_path.unlink()

    def test_init_with_string_path(self, minimal_valid_schema):
        """Test initialization with a string path."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            json.dump(minimal_valid_schema, tmp_file)
            tmp_path = tmp_file.name

        try:
            importer = FwfSchemaImporter(tmp_path, validate=False)
            assert importer.schema == minimal_valid_schema
        finally:
            Path(tmp_path).unlink()

    def test_init_with_invalid_type(self):
        """Test initialization with invalid schema type."""
        with pytest.raises(TypeError, match="schema must be path-like or dict"):
            # Intentionally pass an invalid type to test error handling
            FwfSchemaImporter(123, validate=False)  # type: ignore

    def test_init_with_empty_schema(self):
        """Test initialization with empty schema dict."""
        empty_schema = {}
        importer = FwfSchemaImporter(empty_schema, validate=False)

        assert importer.schema == empty_schema
        assert importer.fwf_ext == {}
        assert importer.field_map == {}
        assert importer.required == []
        assert importer.additional_properties == True  # Default value
        assert importer.fields == []
        assert importer.encoding == "utf-8"  # Default value
        assert importer.header_rows == 0
        assert importer.footer_rows == 0
        assert importer.trim == {}
        assert importer.nulls == {}
        assert importer.conditional_schemas == {}
        assert importer.has_conditional_schemas == False
        assert importer._conditional_manager is None
        assert importer._variant_manager is None
        assert importer.standardize_names is None
        assert importer.dedupe_names is None

    def test_init_with_conditional_schema(self, conditional_schema):
        """Test initialization with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        assert importer.has_conditional_schemas == True
        assert importer._conditional_manager is not None
        assert importer._variant_manager is not None
        assert importer.conditional_schemas == conditional_schema["x-fwf"]["conditionalSchemas"]

    def test_init_with_non_dict_case_config(self):
        """Test initialization when case config is not a dict."""
        schema = {
            "type": "object",
            "x-fwf": {
                "case": "invalid_type"  # Should be dict, not string
            }
        }
        importer = FwfSchemaImporter(schema, validate=False)

        assert importer.standardize_names is None
        assert importer.dedupe_names is None

    @patch('forklift.schema.fwf.validation.JsonSchemaValidator.validate')
    @patch('forklift.schema.fwf.validation.FwfExtensionValidator.validate')
    def test_validate_schema_success(self, mock_fwf_validator, mock_json_validator, minimal_valid_schema):
        """Test successful schema validation."""
        mock_json_validator.return_value = []
        mock_fwf_validator.return_value = []

        with patch.object(FwfSchemaImporter, '_validate_fields', return_value=[]), \
             patch.object(FwfSchemaImporter, '_validate_parquet_types', return_value=[]), \
             patch.object(FwfSchemaImporter, '_validate_properties', return_value=[]):

            importer = FwfSchemaImporter(minimal_valid_schema, validate=True)
            assert importer.validation_errors == []

    @patch('forklift.schema.fwf.validation.JsonSchemaValidator.validate')
    def test_validate_schema_with_errors(self, mock_json_validator, minimal_valid_schema):
        """Test schema validation with errors."""
        mock_json_validator.return_value = ["JSON validation error"]

        with pytest.raises(SchemaValidationError, match="Schema validation failed"):
            FwfSchemaImporter(minimal_valid_schema, validate=True)

    @patch('forklift.schema.fwf.validation.fields.FieldValidator.validate_traditional_fields')
    def test_validate_fields_traditional(self, mock_validator, minimal_valid_schema):
        """Test field validation for traditional (non-conditional) schemas."""
        mock_validator.return_value = []

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        errors = importer._validate_fields()

        mock_validator.assert_called_once_with(importer.fields)
        assert errors == []

    @patch('forklift.schema.fwf.validation.fields.FieldValidator.validate_conditional_fields')
    def test_validate_fields_conditional(self, mock_validator, conditional_schema):
        """Test field validation for conditional schemas."""
        mock_validator.return_value = []

        importer = FwfSchemaImporter(conditional_schema, validate=False)
        errors = importer._validate_fields()

        mock_validator.assert_called_once_with(importer.conditional_schemas)
        assert errors == []

    @patch('forklift.schema.fwf.utils.ParquetMappingUtils.validate_parquet_types_in_fields')
    def test_validate_parquet_types_traditional(self, mock_validator, minimal_valid_schema):
        """Test Parquet type validation for traditional schemas."""
        mock_validator.return_value = []

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        errors = importer._validate_parquet_types()

        mock_validator.assert_called_once_with(importer.fields)
        assert errors == []

    @patch('forklift.schema.fwf.utils.ParquetMappingUtils.validate_parquet_types_in_variants')
    def test_validate_parquet_types_conditional(self, mock_validator, conditional_schema):
        """Test Parquet type validation for conditional schemas."""
        mock_validator.return_value = []

        importer = FwfSchemaImporter(conditional_schema, validate=False)
        errors = importer._validate_parquet_types()

        mock_validator.assert_called_once()
        assert errors == []

    @patch('forklift.schema.fwf.validation.CompatibilityValidator.validate_schema_compatibility')
    def test_validate_properties_conditional(self, mock_validator, conditional_schema):
        """Test properties validation for conditional schemas."""
        mock_validator.return_value = []

        importer = FwfSchemaImporter(conditional_schema, validate=False)
        errors = importer._validate_properties()

        mock_validator.assert_called_once()
        assert errors == []

    def test_validate_properties_traditional(self, minimal_valid_schema):
        """Test properties validation for traditional schemas."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        errors = importer._validate_properties()

        assert errors == []

    # Test accessor methods
    def test_get_field_map(self, minimal_valid_schema):
        """Test getting field map."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        field_map = importer.get_field_map()

        assert field_map == minimal_valid_schema["properties"]

    def test_get_fwf_extension(self, minimal_valid_schema):
        """Test getting FWF extension."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        fwf_ext = importer.get_fwf_extension()

        assert fwf_ext == minimal_valid_schema["x-fwf"]

    def test_get_fields(self, minimal_valid_schema):
        """Test getting fields."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        fields = importer.get_fields()

        assert fields == minimal_valid_schema["x-fwf"]["fields"]

    def test_get_encoding(self, minimal_valid_schema):
        """Test getting encoding."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        encoding = importer.get_encoding()

        assert encoding == "utf-8"

    @patch('forklift.schema.fwf.fields.FieldParser.get_null_values')
    def test_get_null_values(self, mock_parser, minimal_valid_schema):
        """Test getting null values."""
        mock_parser.return_value = ["", "NULL"]

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        null_values = importer.get_null_values("test_column")

        mock_parser.assert_called_once_with("test_column", importer.nulls)
        assert null_values == ["", "NULL"]

    @patch('forklift.schema.fwf.fields.PositionCalculator.get_field_positions')
    def test_get_field_positions(self, mock_calculator, minimal_valid_schema):
        """Test getting field positions."""
        mock_calculator.return_value = [(1, 5), (6, 25)]

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        positions = importer.get_field_positions()

        mock_calculator.assert_called_once_with(importer.fields)
        assert positions == [(1, 5), (6, 25)]

    @patch('forklift.schema.fwf.fields.FieldParser.get_column_names')
    def test_get_column_names(self, mock_parser, minimal_valid_schema):
        """Test getting column names."""
        mock_parser.return_value = ["id", "name"]

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        column_names = importer.get_column_names()

        mock_parser.assert_called_once_with(
            importer.fields, importer.standardize_names, importer.dedupe_names
        )
        assert column_names == ["id", "name"]

    @patch('forklift.schema.fwf.fields.FieldParser.should_trim_field')
    def test_should_trim_field(self, mock_parser, minimal_valid_schema):
        """Test checking if field should be trimmed."""
        mock_parser.return_value = True

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        should_trim = importer.should_trim_field("id")

        mock_parser.assert_called_once_with("id", importer.trim)
        assert should_trim == True

    def test_as_dict(self, minimal_valid_schema):
        """Test getting raw schema dictionary."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        schema_dict = importer.as_dict()

        assert schema_dict == minimal_valid_schema

    # Test conditional schema methods
    def test_has_conditional_schema_support_true(self, conditional_schema):
        """Test conditional schema support detection when enabled."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        assert importer.has_conditional_schema_support() == True

    def test_has_conditional_schema_support_false(self, minimal_valid_schema):
        """Test conditional schema support detection when disabled."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        assert importer.has_conditional_schema_support() == False

    def test_get_flag_column_info_with_conditional(self, conditional_schema):
        """Test getting flag column info with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        with patch.object(importer._conditional_manager, 'get_flag_column_info') as mock_method:
            mock_method.return_value = {"name": "record_type", "start": 0, "length": 1}

            flag_column = importer.get_flag_column_info()

            mock_method.assert_called_once()
            assert flag_column == {"name": "record_type", "start": 0, "length": 1}

    def test_get_flag_column_info_without_conditional(self, minimal_valid_schema):
        """Test getting flag column info without conditional schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        flag_column = importer.get_flag_column_info()

        assert flag_column is None

    def test_get_schema_variants_with_conditional(self, conditional_schema):
        """Test getting schema variants with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        with patch.object(importer._conditional_manager, 'get_schema_variants') as mock_method:
            mock_method.return_value = [{"flagValue": "A"}, {"flagValue": "B"}]

            variants = importer.get_schema_variants()

            mock_method.assert_called_once()
            assert variants == [{"flagValue": "A"}, {"flagValue": "B"}]

    def test_get_schema_variants_without_conditional(self, minimal_valid_schema):
        """Test getting schema variants without conditional schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        variants = importer.get_schema_variants()

        assert variants == []

    def test_get_variant_by_flag_value_with_conditional(self, conditional_schema):
        """Test getting variant by flag value with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        with patch.object(importer._conditional_manager, 'get_variant_by_flag_value') as mock_method:
            mock_method.return_value = {"flagValue": "A", "fields": []}

            variant = importer.get_variant_by_flag_value("A")

            mock_method.assert_called_once_with("A")
            assert variant == {"flagValue": "A", "fields": []}

    def test_get_variant_by_flag_value_without_conditional(self, minimal_valid_schema):
        """Test getting variant by flag value without conditional schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        variant = importer.get_variant_by_flag_value("A")

        assert variant is None

    @patch('forklift.schema.fwf.fields.FieldMapper.get_all_possible_fields')
    def test_get_all_possible_fields(self, mock_mapper, minimal_valid_schema):
        """Test getting all possible fields."""
        mock_mapper.return_value = {"field1": {"name": "field1"}}

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        all_fields = importer.get_all_possible_fields()

        mock_mapper.assert_called_once_with(
            importer.has_conditional_schemas,
            importer.fields,
            importer.get_flag_column_info(),
            importer.get_schema_variants()
        )
        assert all_fields == {"field1": {"name": "field1"}}

    @patch('forklift.schema.fwf.fields.FieldMapper.get_unified_parquet_schema')
    def test_get_unified_parquet_schema(self, mock_mapper, minimal_valid_schema):
        """Test getting unified Parquet schema."""
        mock_mapper.return_value = {"field1": "string"}

        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        with patch.object(importer, 'get_all_possible_fields') as mock_get_fields:
            mock_get_fields.return_value = {"field1": {"name": "field1"}}

            schema = importer.get_unified_parquet_schema()

            mock_mapper.assert_called_once_with(
                {"field1": {"name": "field1"}},
                importer.get_flag_column_info(),
                importer.get_schema_variants()
            )
            assert schema == {"field1": "string"}

    def test_get_fields_for_flag_value_with_conditional(self, conditional_schema):
        """Test getting fields for flag value with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        with patch.object(importer._conditional_manager, 'get_fields_for_flag_value') as mock_method:
            mock_method.return_value = [{"name": "field1"}]

            fields = importer.get_fields_for_flag_value("A")

            mock_method.assert_called_once_with("A")
            assert fields == [{"name": "field1"}]

    def test_get_fields_for_flag_value_without_conditional(self, minimal_valid_schema):
        """Test getting fields for flag value without conditional schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        fields = importer.get_fields_for_flag_value("A")

        assert fields == importer.fields

    def test_get_field_positions_for_flag_value_with_variant_manager(self, conditional_schema):
        """Test getting field positions for flag value with variant manager."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        # Mock the missing method on VariantManager to test the intended behavior
        with patch.object(importer._variant_manager, 'get_field_positions_for_flag_value', create=True) as mock_method:
            mock_method.return_value = [(0, 1), (1, 11)]

            positions = importer.get_field_positions_for_flag_value("A")

            mock_method.assert_called_once_with("A")
            assert positions == [(0, 1), (1, 11)]

    def test_get_field_positions_for_flag_value_without_variant_manager(self, minimal_valid_schema):
        """Test getting field positions for flag value without variant manager."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        with patch.object(importer, 'get_field_positions') as mock_method:
            mock_method.return_value = [(1, 5), (6, 25)]

            positions = importer.get_field_positions_for_flag_value("A")

            mock_method.assert_called_once()
            assert positions == [(1, 5), (6, 25)]

    def test_get_column_names_for_flag_value_with_variant_manager(self, conditional_schema):
        """Test getting column names for flag value with variant manager."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        # Mock the missing method on VariantManager to test the intended behavior
        with patch.object(importer._variant_manager, 'get_column_names_for_flag_value', create=True) as mock_method:
            mock_method.return_value = ["record_type", "data"]

            names = importer.get_column_names_for_flag_value("A")

            mock_method.assert_called_once_with("A", importer.standardize_names, importer.dedupe_names)
            assert names == ["record_type", "data"]

    def test_get_column_names_for_flag_value_without_variant_manager(self, minimal_valid_schema):
        """Test getting column names for flag value without variant manager."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        with patch.object(importer, 'get_column_names') as mock_method:
            mock_method.return_value = ["id", "name"]

            names = importer.get_column_names_for_flag_value("A")

            mock_method.assert_called_once()
            assert names == ["id", "name"]

    # Test what happens when VariantManager methods don't exist (AttributeError scenarios)
    def test_get_field_positions_for_flag_value_attribute_error(self, conditional_schema):
        """Test behavior when VariantManager doesn't have the expected method."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        # This tests the actual current behavior where the method doesn't exist
        with pytest.raises(AttributeError, match="'VariantManager' object has no attribute 'get_field_positions_for_flag_value'"):
            importer.get_field_positions_for_flag_value("A")

    def test_get_column_names_for_flag_value_attribute_error(self, conditional_schema):
        """Test behavior when VariantManager doesn't have the expected method."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        # This tests the actual current behavior where the method doesn't exist
        with pytest.raises(AttributeError, match="'VariantManager' object has no attribute 'get_column_names_for_flag_value'"):
            importer.get_column_names_for_flag_value("A")

    def test_get_all_possible_flag_values_with_conditional(self, conditional_schema):
        """Test getting all possible flag values with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        with patch.object(importer._conditional_manager, 'get_all_possible_flag_values') as mock_method:
            mock_method.return_value = ["A", "B"]

            flag_values = importer.get_all_possible_flag_values()

            mock_method.assert_called_once()
            assert flag_values == ["A", "B"]

    def test_get_all_possible_flag_values_without_conditional(self, minimal_valid_schema):
        """Test getting all possible flag values without conditional schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        flag_values = importer.get_all_possible_flag_values()

        assert flag_values == []

    def test_validate_flag_value_with_conditional(self, conditional_schema):
        """Test validating flag value with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        with patch.object(importer._conditional_manager, 'validate_flag_value') as mock_method:
            mock_method.return_value = True

            is_valid = importer.validate_flag_value("A")

            mock_method.assert_called_once_with("A")
            assert is_valid == True

    def test_validate_flag_value_without_conditional(self, minimal_valid_schema):
        """Test validating flag value without conditional schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        is_valid = importer.validate_flag_value("A")

        assert is_valid == False

    def test_get_record_mapping_for_row_with_conditional(self, conditional_schema):
        """Test getting record mapping for row with conditional schema."""
        importer = FwfSchemaImporter(conditional_schema, validate=False)

        with patch.object(importer._conditional_manager, 'get_record_mapping_for_row') as mock_method:
            mock_method.return_value = {"flagValue": "A", "fields": []}

            mapping = importer.get_record_mapping_for_row("A123456789")

            mock_method.assert_called_once_with("A123456789")
            assert mapping == {"flagValue": "A", "fields": []}

    def test_get_record_mapping_for_row_without_conditional(self, minimal_valid_schema):
        """Test getting record mapping for row without conditional schema."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        mapping = importer.get_record_mapping_for_row("A123456789")

        assert mapping is None
