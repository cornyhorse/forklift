"""Tests for FWF utility functions."""

import pytest
import tempfile
import json
from pathlib import Path

from forklift.inputs.fwf_utils import create_fwf_config_from_schema, create_simple_fwf_config
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


class TestCreateSimpleFwfConfig:
    """Test the create_simple_fwf_config utility function."""

    def test_create_simple_config_basic(self):
        """Test creating a simple FWF configuration."""
        field_specs = [
            {'name': 'id', 'start': 1, 'length': 5, 'parquet_type': 'int64'},
            {'name': 'name', 'start': 6, 'length': 20, 'parquet_type': 'string'},
            {'name': 'amount', 'start': 26, 'length': 10, 'parquet_type': 'decimal128(10,2)'}
        ]

        config = create_simple_fwf_config(field_specs)

        assert isinstance(config, FwfInputConfig)
        assert len(config.fields) == 3

        # Check first field
        assert config.fields[0].name == 'id'
        assert config.fields[0].start == 1
        assert config.fields[0].length == 5
        assert config.fields[0].parquet_type == 'int64'

        # Check defaults
        assert config.encoding == 'utf-8'
        assert config.trim_whitespace is True
        assert config.skip_blank_lines is True

    def test_create_simple_config_with_options(self):
        """Test creating simple config with additional options."""
        field_specs = [
            {
                'name': 'id',
                'start': 1,
                'length': 5,
                'align': 'right',
                'pad': '0',
                'required': True,
                'trim': False
            }
        ]

        config = create_simple_fwf_config(
            field_specs,
            encoding='latin-1',
            trim_whitespace=False,
            skip_blank_lines=False,
            null_values={'global': ['NULL']},
            footer_detection={'mode': 'regex', 'pattern': '^TOTAL'}
        )

        assert config.encoding == 'latin-1'
        assert config.trim_whitespace is False
        assert config.skip_blank_lines is False
        assert config.null_values == {'global': ['NULL']}
        assert config.footer_detection == {'mode': 'regex', 'pattern': '^TOTAL'}

        # Check field with custom options
        field = config.fields[0]
        assert field.align == 'right'
        assert field.pad == '0'
        assert field.required is True
        assert field.trim is False

    def test_create_simple_config_empty_fields(self):
        """Test creating config with empty field list."""
        config = create_simple_fwf_config([])
        assert config.fields == []


class TestCreateFwfConfigFromSchema:
    """Test the create_fwf_config_from_schema utility function."""

    def test_file_not_found(self):
        """Test handling of non-existent schema files."""
        non_existent_path = Path("/non/existent/schema.json")

        with pytest.raises(FileNotFoundError, match="Schema file not found"):
            create_fwf_config_from_schema(non_existent_path)

    def test_schema_without_fwf_config(self):
        """Test handling of schema files without x-fwf configuration."""
        schema_data = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Schema",
            "type": "object",
            "properties": {
                "id": {"type": "integer"}
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema_data, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="does not contain x-fwf configuration"):
                create_fwf_config_from_schema(temp_path)
        finally:
            temp_path.unlink()

    def test_standard_fwf_schema(self):
        """Test creating config from standard FWF schema."""
        schema_data = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test FWF Schema",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            },
            "x-fwf": {
                "encoding": "utf-8",
                "trim": {"rstrip": True},
                "fields": [
                    {
                        "name": "id",
                        "start": 1,
                        "length": 5,
                        "align": "right",
                        "pad": "0",
                        "parquetType": "int64",
                        "required": True
                    },
                    {
                        "name": "name",
                        "start": 6,
                        "length": 20,
                        "align": "left",
                        "pad": " ",
                        "parquetType": "string",
                        "trim": True
                    }
                ],
                "nulls": {
                    "global": ["", "NULL"],
                    "perColumn": {
                        "name": ["UNKNOWN"]
                    }
                },
                "footer": {
                    "mode": "regex",
                    "pattern": "^TOTAL"
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema_data, f)
            temp_path = Path(f.name)

        try:
            config = create_fwf_config_from_schema(temp_path)

            assert isinstance(config, FwfInputConfig)
            assert config.encoding == "utf-8"
            assert config.trim_whitespace is True
            assert len(config.fields) == 2

            # Check first field
            id_field = config.fields[0]
            assert id_field.name == "id"
            assert id_field.start == 1
            assert id_field.length == 5
            assert id_field.align == "right"
            assert id_field.pad == "0"
            assert id_field.parquet_type == "int64"

            # Check second field
            name_field = config.fields[1]
            assert name_field.name == "name"
            assert name_field.start == 6
            assert name_field.length == 20
            assert name_field.align == "left"
            assert name_field.parquet_type == "string"
            assert name_field.trim is True

            # Check null values
            assert config.null_values["global"] == ["", "NULL"]
            assert config.null_values["perColumn"]["name"] == ["UNKNOWN"]

            # Check footer detection
            assert config.footer_detection["mode"] == "regex"
            assert config.footer_detection["pattern"] == "^TOTAL"

        finally:
            temp_path.unlink()

    def test_conditional_fwf_schema(self):
        """Test creating config from conditional FWF schema."""
        schema_data = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Test Conditional FWF Schema",
            "type": "object",
            "x-fwf": {
                "encoding": "utf-8",
                "trim": {"rstrip": True},
                "conditionalSchemas": {
                    "flagColumn": {
                        "name": "record_type",
                        "start": 1,
                        "length": 1,
                        "parquetType": "string"
                    },
                    "schemas": [
                        {
                            "flagValue": "A",
                            "description": "Type A records",
                            "fields": [
                                {
                                    "name": "record_type",
                                    "start": 1,
                                    "length": 1,
                                    "parquetType": "string"
                                },
                                {
                                    "name": "id",
                                    "start": 2,
                                    "length": 5,
                                    "align": "right",
                                    "pad": "0",
                                    "parquetType": "int64"
                                },
                                {
                                    "name": "data_a",
                                    "start": 7,
                                    "length": 10,
                                    "parquetType": "string"
                                }
                            ]
                        },
                        {
                            "flagValue": "B",
                            "description": "Type B records",
                            "fields": [
                                {
                                    "name": "record_type",
                                    "start": 1,
                                    "length": 1,
                                    "parquetType": "string"
                                },
                                {
                                    "name": "id",
                                    "start": 2,
                                    "length": 3,
                                    "parquetType": "int64"
                                },
                                {
                                    "name": "data_b",
                                    "start": 5,
                                    "length": 8,
                                    "parquetType": "string"
                                }
                            ]
                        }
                    ]
                },
                "nulls": {
                    "global": ["", "NULL"]
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema_data, f)
            temp_path = Path(f.name)

        try:
            config = create_fwf_config_from_schema(temp_path)

            assert isinstance(config, FwfInputConfig)
            assert config.encoding == "utf-8"
            assert config.fields is None  # Should be None for conditional

            # Check flag column
            assert config.flag_column.name == "record_type"
            assert config.flag_column.start == 1
            assert config.flag_column.length == 1
            assert config.flag_column.parquet_type == "string"

            # Check conditional schemas
            assert len(config.conditional_schemas) == 2

            # Check schema A
            schema_a = config.conditional_schemas[0]
            assert schema_a.flag_value == "A"
            assert schema_a.description == "Type A records"
            assert len(schema_a.fields) == 3
            assert schema_a.fields[1].name == "id"
            assert schema_a.fields[1].length == 5
            assert schema_a.fields[2].name == "data_a"

            # Check schema B
            schema_b = config.conditional_schemas[1]
            assert schema_b.flag_value == "B"
            assert schema_b.description == "Type B records"
            assert len(schema_b.fields) == 3
            assert schema_b.fields[1].name == "id"
            assert schema_b.fields[1].length == 3
            assert schema_b.fields[2].name == "data_b"

        finally:
            temp_path.unlink()

    def test_conditional_schema_missing_flag_column(self):
        """Test conditional schema without flag column specification."""
        schema_data = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Invalid Conditional Schema",
            "type": "object",
            "x-fwf": {
                "conditionalSchemas": {
                    "schemas": [
                        {
                            "flagValue": "A",
                            "fields": [{"name": "test", "start": 1, "length": 5}]
                        }
                    ]
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema_data, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="Conditional schemas require flagColumn specification"):
                create_fwf_config_from_schema(temp_path)
        finally:
            temp_path.unlink()

    def test_schema_with_minimal_fwf_config(self):
        """Test schema with minimal x-fwf configuration."""
        schema_data = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Minimal FWF Schema",
            "type": "object",
            "x-fwf": {
                "fields": [
                    {
                        "name": "test_field",
                        "start": 1,
                        "length": 10
                    }
                ]
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(schema_data, f)
            temp_path = Path(f.name)

        try:
            config = create_fwf_config_from_schema(temp_path)

            assert isinstance(config, FwfInputConfig)
            assert config.encoding == "utf-8"  # Should use default
            assert len(config.fields) == 1

            field = config.fields[0]
            assert field.name == "test_field"
            assert field.start == 1
            assert field.length == 10
            assert field.parquet_type == "string"  # Should use default

        finally:
            temp_path.unlink()

    def test_invalid_json_schema(self):
        """Test handling of invalid JSON in schema file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content {")
            temp_path = Path(f.name)

        try:
            with pytest.raises(json.JSONDecodeError):
                create_fwf_config_from_schema(temp_path)
        finally:
            temp_path.unlink()


class TestFwfUtilsIntegration:
    """Integration tests for FWF utilities with actual schema files."""

    def test_with_actual_schema_files(self):
        """Test with actual schema files from the project."""
        # Test with the standard FWF schema
        schema_path = Path("/Users/matt/PycharmProjects/forklift/schema-standards/20250826-fwf.json")

        if schema_path.exists():
            config = create_fwf_config_from_schema(schema_path)
            assert isinstance(config, FwfInputConfig)
            assert config.fields is not None
            assert len(config.fields) > 0

            # Should have standard fields like id, name, etc.
            field_names = [field.name for field in config.fields]
            assert "id" in field_names
            assert "name" in field_names

    def test_with_conditional_schema_file(self):
        """Test with the conditional FWF schema."""
        schema_path = Path("/Users/matt/PycharmProjects/forklift/schema-standards/20250826-fwf-conditional.json")

        if schema_path.exists():
            config = create_fwf_config_from_schema(schema_path)
            assert isinstance(config, FwfInputConfig)
            assert config.conditional_schemas is not None
            assert config.flag_column is not None
            assert len(config.conditional_schemas) > 0

            # Should have flag column for record type
            assert config.flag_column.name == "record_type"
