"""Tests for FWF schema importer core functionality."""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

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
            "x-fwf": {
                "fields": [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},
                    {"name": "name", "start": 6, "length": 20, "parquetType": "string"}
                ],
                "encoding": "utf-8"
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
                    "flagColumn": "record_type",
                    "variants": {
                        "A": {
                            "fields": [
                                {"name": "record_type", "start": 1, "length": 1, "parquetType": "string"},
                                {"name": "data", "start": 2, "length": 10, "parquetType": "string"}
                            ]
                        },
                        "B": {
                            "fields": [
                                {"name": "record_type", "start": 1, "length": 1, "parquetType": "string"},
                                {"name": "data", "start": 2, "length": 15, "parquetType": "string"}
                            ]
                        }
                    }
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
        assert importer.additional_properties == True

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
            FwfSchemaImporter(123, validate=False)

    def test_extract_fwf_configurations(self, minimal_valid_schema):
        """Test extraction of FWF-specific configurations."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        assert importer.fields == minimal_valid_schema["x-fwf"]["fields"]
        assert importer.encoding == "utf-8"
        assert importer.trim == {}
        assert importer.nulls == {}
        assert importer.header_rows == 0
        assert importer.footer_rows == 0

    def test_extract_fwf_configurations_with_defaults(self):
        """Test extraction with default values when FWF extension is minimal."""
        schema = {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "x-fwf": {}
        }

        importer = FwfSchemaImporter(schema, validate=False)

        assert importer.fields == []
        assert importer.encoding == "utf-8"
        assert importer.trim == {}
        assert importer.nulls == {}
        assert importer.header_rows == 0
        assert importer.footer_rows == 0

    def test_conditional_schema_detection(self, conditional_schema):
        """Test detection and initialization of conditional schemas."""
        with patch('forklift.schema.fwf.core.ConditionalSchemaManager') as mock_manager:
            with patch('forklift.schema.fwf.core.VariantManager') as mock_variant:
                mock_manager_instance = Mock()
                mock_variant_instance = Mock()
                mock_manager.return_value = mock_manager_instance
                mock_variant.return_value = mock_variant_instance
                mock_manager_instance.get_schema_variants.return_value = {}
                mock_manager_instance.get_flag_column_info.return_value = {}

                importer = FwfSchemaImporter(conditional_schema, validate=False)

                assert importer.has_conditional_schemas == True
                assert importer._conditional_manager == mock_manager_instance
                assert importer._variant_manager == mock_variant_instance

    def test_no_conditional_schema(self, minimal_valid_schema):
        """Test initialization without conditional schemas."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        assert importer.has_conditional_schemas == False
        assert importer._conditional_manager is None
        assert importer._variant_manager is None

    def test_case_configuration_extraction(self):
        """Test extraction of case configuration."""
        schema = {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "x-fwf": {
                "case": {
                    "standardizeNames": "snake_case",
                    "dedupeNames": "suffix"
                }
            }
        }

        importer = FwfSchemaImporter(schema, validate=False)

        assert importer.standardize_names == "snake_case"
        assert importer.dedupe_names == "suffix"

    def test_case_configuration_defaults(self, minimal_valid_schema):
        """Test default case configuration values."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)

        assert importer.standardize_names is None
        assert importer.dedupe_names is None

    def test_validation_with_valid_schema(self, minimal_valid_schema):
        """Test schema validation with a valid schema."""
        with patch('forklift.schema.fwf.core.JsonSchemaValidator') as mock_json:
            with patch('forklift.schema.fwf.core.FwfExtensionValidator') as mock_fwf:
                with patch('forklift.schema.fwf.core.FieldValidator') as mock_field:
                    with patch('forklift.schema.fwf.core.ParquetMappingUtils') as mock_parquet:
                        # Mock all validators to return no errors
                        mock_json.validate.return_value = []
                        mock_fwf.validate.return_value = []
                        mock_field.validate_traditional_fields.return_value = []
                        mock_parquet.validate_parquet_types_in_fields.return_value = []

                        # Should not raise exception
                        importer = FwfSchemaImporter(minimal_valid_schema, validate=True)
                        assert importer.validation_errors == []

    def test_validation_with_errors(self, minimal_valid_schema):
        """Test schema validation with validation errors."""
        with patch('forklift.schema.fwf.core.JsonSchemaValidator') as mock_json:
            with patch('forklift.schema.fwf.core.FwfExtensionValidator') as mock_fwf:
                with patch('forklift.schema.fwf.core.FieldValidator') as mock_field:
                    with patch('forklift.schema.fwf.core.ParquetMappingUtils') as mock_parquet:
                        # Mock validators to return errors
                        mock_json.validate.return_value = ["JSON schema error"]
                        mock_fwf.validate.return_value = ["FWF extension error"]
                        mock_field.validate_traditional_fields.return_value = ["Field error"]
                        mock_parquet.validate_parquet_types_in_fields.return_value = ["Parquet error"]

                        with pytest.raises(SchemaValidationError) as exc_info:
                            FwfSchemaImporter(minimal_valid_schema, validate=True)

                        assert "Schema validation failed" in str(exc_info.value)
                        assert "JSON schema error" in str(exc_info.value)
                        assert "FWF extension error" in str(exc_info.value)

    def test_validation_conditional_fields(self, conditional_schema):
        """Test validation of conditional fields."""
        with patch('forklift.schema.fwf.core.ConditionalSchemaManager') as mock_manager:
            with patch('forklift.schema.fwf.core.VariantManager') as mock_variant:
                with patch('forklift.schema.fwf.core.JsonSchemaValidator') as mock_json:
                    with patch('forklift.schema.fwf.core.FwfExtensionValidator') as mock_fwf:
                        with patch('forklift.schema.fwf.core.FieldValidator') as mock_field:
                            with patch('forklift.schema.fwf.core.ParquetMappingUtils') as mock_parquet:
                                with patch('forklift.schema.fwf.core.CompatibilityValidator') as mock_compat:
                                    # Setup mocks
                                    mock_manager_instance = Mock()
                                    mock_variant_instance = Mock()
                                    mock_manager.return_value = mock_manager_instance
                                    mock_variant.return_value = mock_variant_instance
                                    mock_manager_instance.get_schema_variants.return_value = {}
                                    mock_manager_instance.get_flag_column_info.return_value = {}

                                    # Mock all validators to return no errors
                                    mock_json.validate.return_value = []
                                    mock_fwf.validate.return_value = []
                                    mock_field.validate_conditional_fields.return_value = []
                                    mock_parquet.validate_parquet_types_in_variants.return_value = []
                                    mock_compat.validate_schema_compatibility.return_value = []

                                    # Should not raise exception
                                    importer = FwfSchemaImporter(conditional_schema, validate=True)
                                    assert importer.validation_errors == []

    def test_get_field_map(self, minimal_valid_schema):
        """Test get_field_map method."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        field_map = importer.get_field_map()

        assert field_map == minimal_valid_schema["properties"]

    def test_get_fwf_extension(self, minimal_valid_schema):
        """Test get_fwf_extension method."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        fwf_ext = importer.get_fwf_extension()

        assert fwf_ext == minimal_valid_schema["x-fwf"]

    def test_get_fields(self, minimal_valid_schema):
        """Test get_fields method."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        fields = importer.get_fields()

        assert fields == minimal_valid_schema["x-fwf"]["fields"]

    def test_get_encoding(self, minimal_valid_schema):
        """Test get_encoding method."""
        importer = FwfSchemaImporter(minimal_valid_schema, validate=False)
        encoding = importer.get_encoding()

        assert encoding == "utf-8"

    def test_additional_properties_false(self):
        """Test additionalProperties set to false."""
        schema = {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "additionalProperties": False,
            "x-fwf": {}
        }

        importer = FwfSchemaImporter(schema, validate=False)
        assert importer.additional_properties == False

    def test_required_field_extraction(self):
        """Test extraction of required fields."""
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"}
            },
            "required": ["id", "name"],
            "x-fwf": {}
        }

        importer = FwfSchemaImporter(schema, validate=False)
        assert importer.required == ["id", "name"]

    def test_complete_fwf_configuration(self):
        """Test extraction of complete FWF configuration."""
        schema = {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "x-fwf": {
                "fields": [{"name": "id", "start": 1, "length": 5}],
                "encoding": "iso-8859-1",
                "trim": {"leading": True, "trailing": True},
                "nulls": {"values": ["", "NULL"]},
                "headerRows": 2,
                "footerRows": 1
            }
        }

        importer = FwfSchemaImporter(schema, validate=False)

        assert importer.encoding == "iso-8859-1"
        assert importer.trim == {"leading": True, "trailing": True}
        assert importer.nulls == {"values": ["", "NULL"]}
        assert importer.header_rows == 2
        assert importer.footer_rows == 1
