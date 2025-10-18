"""Tests for calculated columns factory functionality."""

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from forklift.processors.calculated_columns import (
    CalculatedColumn,
    CalculatedColumnsConfig,
    CalculatedColumnsProcessor,
    ConstantColumn,
    ExpressionColumn,
)
from forklift.processors.calculated_columns_factory import (
    _parse_data_type,
    create_calculated_columns_processor_from_metadata,
    create_calculated_columns_processor_from_schema,
    validate_calculated_columns_schema,
)


class TestParseDataType:
    """Test the _parse_data_type helper function."""

    def test_parse_data_type_none_and_empty(self):
        """Test parsing None and empty data type strings."""
        assert _parse_data_type(None) is None
        assert _parse_data_type("") is None
        # Whitespace-only strings get stripped to empty and fall back to string type
        assert _parse_data_type("   ") == pa.string()

    def test_parse_data_type_simple_types(self):
        """Test parsing simple data type strings."""
        # String types
        assert _parse_data_type("string") == pa.string()
        assert _parse_data_type("STRING") == pa.string()  # case insensitive

        # Integer types
        assert _parse_data_type("int64") == pa.int64()
        assert _parse_data_type("int32") == pa.int32()

        # Float types
        assert _parse_data_type("float64") == pa.float64()
        assert _parse_data_type("float32") == pa.float32()
        assert _parse_data_type("double") == pa.float64()  # alias

        # Boolean types
        assert _parse_data_type("bool") == pa.bool_()
        assert _parse_data_type("boolean") == pa.bool_()

        # Date types
        assert _parse_data_type("date32") == pa.date32()
        assert _parse_data_type("date64") == pa.date64()

        # Other types
        assert _parse_data_type("timestamp") == pa.timestamp("ns")
        assert _parse_data_type("binary") == pa.binary()

    def test_parse_data_type_complex_timestamp(self):
        """Test parsing complex timestamp types with units."""
        assert _parse_data_type("timestamp[s]") == pa.timestamp("s")
        assert _parse_data_type("timestamp[ms]") == pa.timestamp("ms")
        assert _parse_data_type("timestamp[us]") == pa.timestamp("us")
        assert _parse_data_type("timestamp[ns]") == pa.timestamp("ns")

    def test_parse_data_type_decimal128(self):
        """Test parsing decimal128 types with precision and scale."""
        assert _parse_data_type("decimal128(10,2)") == pa.decimal128(10, 2)
        assert _parse_data_type("decimal128(18,4)") == pa.decimal128(18, 4)

    def test_parse_data_type_decimal128_invalid(self):
        """Test parsing invalid decimal128 formats."""
        # Invalid format should fall back to string
        assert _parse_data_type("decimal128(invalid)") == pa.string()
        assert _parse_data_type("decimal128(10)") == pa.string()  # missing scale
        assert _parse_data_type("decimal128(10,2,3)") == pa.string()  # too many params

    def test_parse_data_type_list_types(self):
        """Test parsing list types with inner types."""
        assert _parse_data_type("list<string>") == pa.list_(pa.string())
        assert _parse_data_type("list<int64>") == pa.list_(pa.int64())
        assert _parse_data_type("list<float64>") == pa.list_(pa.float64())

    def test_parse_data_type_list_types_invalid_inner(self):
        """Test parsing list types with invalid inner types."""
        # Should create list with string inner type for unknown types
        result = _parse_data_type("list<unknown_type>")
        assert isinstance(result, pa.ListType)
        assert result.value_type == pa.string()

    def test_parse_data_type_list_types_empty_inner(self):
        """Test parsing list types with empty inner type."""
        # Empty inner type should result in None inner type, which should not create a list
        result = _parse_data_type("list<>")
        # Since inner type parsing returns None for empty string, this should fall back to string
        assert result == pa.string()

    def test_parse_data_type_unknown_type(self):
        """Test parsing unknown data type falls back to string."""
        assert _parse_data_type("unknown_type") == pa.string()
        assert _parse_data_type("custom_type") == pa.string()
        assert _parse_data_type("invalid") == pa.string()

    def test_parse_data_type_whitespace_handling(self):
        """Test that whitespace is properly handled."""
        assert _parse_data_type("  string  ") == pa.string()
        assert _parse_data_type("\tint64\n") == pa.int64()


class TestCreateCalculatedColumnsProcessorFromSchema:
    """Test the create_calculated_columns_processor_from_schema function."""

    def test_create_processor_empty_config(self):
        """Test creating processor with empty configuration."""
        assert create_calculated_columns_processor_from_schema({}) is None
        assert create_calculated_columns_processor_from_schema(None) is None

    def test_create_processor_no_columns_no_partitions(self):
        """Test creating processor with no columns and no partition columns."""
        config = {"failOnError": True, "addMetadata": False}
        assert create_calculated_columns_processor_from_schema(config) is None

    def test_create_processor_with_constants(self):
        """Test creating processor with constants."""
        config = {
            "constants": [
                {
                    "name": "PI",
                    "value": 3.14159,
                    "dataType": "float64",
                    "description": "The value of pi",
                },
                {"name": "DEFAULT_STRING", "value": "default", "dataType": "string"},
            ]
        }

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert isinstance(processor, CalculatedColumnsProcessor)
        assert len(processor.config.constants) == 2
        assert processor.config.constants[0].name == "PI"
        assert processor.config.constants[0].value == 3.14159

    def test_create_processor_with_expressions(self):
        """Test creating processor with expressions."""
        config = {
            "expressions": [
                {
                    "name": "full_name",
                    "expression": "first_name + ' ' + last_name",
                    "dataType": "string",
                    "description": "Full name concatenation",
                    "dependencies": ["first_name", "last_name"],
                },
                {
                    "name": "age_plus_ten",
                    "expression": "age + 10",
                    "dataType": "int64",
                    "dependencies": ["age"],
                },
            ]
        }

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert isinstance(processor, CalculatedColumnsProcessor)
        assert len(processor.config.expressions) == 2
        assert processor.config.expressions[0].name == "full_name"
        assert processor.config.expressions[0].dependencies == ["first_name", "last_name"]

    def test_create_processor_with_calculated_columns(self):
        """Test creating processor with calculated columns."""
        config = {
            "calculated": [
                {
                    "name": "total_amount",
                    "expression": "quantity * price",
                    "dataType": "float64",
                    "description": "Total amount calculation",
                    "dependencies": ["quantity", "price"],
                },
                {
                    "name": "discount_amount",
                    "function": "total_amount * 0.1",  # backward compatibility
                    "dataType": "float64",
                    "dependencies": ["total_amount"],
                },
            ]
        }

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert isinstance(processor, CalculatedColumnsProcessor)
        assert len(processor.config.calculated) == 2
        assert processor.config.calculated[0].name == "total_amount"
        assert processor.config.calculated[0].expression == "quantity * price"

        # Test backward compatibility with 'function' field
        assert processor.config.calculated[1].name == "discount_amount"
        assert processor.config.calculated[1].expression == "total_amount * 0.1"
        assert hasattr(processor.config.calculated[1], "function")

    def test_create_processor_with_function_field_only(self):
        """Test creating processor with calculated columns using only 'function' field."""
        config = {
            "calculated": [
                {
                    "name": "computed_value",
                    "function": "x + y",
                    "dataType": "int64",
                    "dependencies": ["x", "y"],
                }
            ]
        }

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert len(processor.config.calculated) == 1
        assert processor.config.calculated[0].expression == "x + y"
        assert processor.config.calculated[0].function == "x + y"

    def test_create_processor_with_expression_and_function_fields(self):
        """Test creating processor with both 'expression' and 'function' fields (expression takes precedence)."""
        config = {
            "calculated": [
                {
                    "name": "computed_value",
                    "expression": "a + b",
                    "function": "x + y",  # should be ignored when expression is present
                    "dataType": "int64",
                    "dependencies": ["a", "b"],
                }
            ]
        }

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert len(processor.config.calculated) == 1
        assert processor.config.calculated[0].expression == "a + b"
        assert processor.config.calculated[0].function == "x + y"  # function field is preserved

    def test_create_processor_with_partition_columns_only(self):
        """Test creating processor with only partition columns."""
        config = {"partitionColumns": ["year", "month", "day"]}

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert processor.config.partition_columns == ["year", "month", "day"]
        assert len(processor.config.columns) == 0  # No calculated columns

    def test_create_processor_with_all_types(self):
        """Test creating processor with all types of columns."""
        config = {
            "constants": [{"name": "VERSION", "value": "1.0", "dataType": "string"}],
            "expressions": [
                {
                    "name": "full_name",
                    "expression": "first + ' ' + last",
                    "dependencies": ["first", "last"],
                }
            ],
            "calculated": [
                {"name": "total", "expression": "price * qty", "dependencies": ["price", "qty"]}
            ],
            "partitionColumns": ["year", "month"],
            "failOnError": False,
            "addMetadata": True,
            "validateDependencies": False,
        }

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert len(processor.config.constants) == 1
        assert len(processor.config.expressions) == 1
        assert len(processor.config.calculated) == 1
        assert len(processor.config.partition_columns) == 2
        assert processor.config.fail_on_error is False
        assert processor.config.add_metadata is True
        assert processor.config.validate_dependencies is False

    def test_create_processor_default_config_values(self):
        """Test that default configuration values are applied correctly."""
        config = {"constants": [{"name": "TEST", "value": "test"}]}

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        # Test default values
        assert processor.config.fail_on_error is True  # default
        assert processor.config.add_metadata is False  # default
        assert processor.config.validate_dependencies is True  # default

    def test_create_processor_missing_optional_fields(self):
        """Test creating processor with missing optional fields."""
        config = {
            "constants": [{"name": "TEST", "value": "test"}],  # No dataType or description
            "expressions": [
                {
                    "name": "expr",
                    "expression": "x + 1",
                }  # No dataType, description, or dependencies
            ],
            "calculated": [
                {
                    "name": "calc",
                    "expression": "y * 2",
                }  # No dataType, description, or dependencies
            ],
        }

        processor = create_calculated_columns_processor_from_schema(config)
        assert processor is not None
        assert len(processor.config.constants) == 1
        assert len(processor.config.expressions) == 1
        assert len(processor.config.calculated) == 1

        # Check that missing optional fields are handled
        assert processor.config.constants[0].data_type is None
        assert processor.config.constants[0].description is None
        assert processor.config.expressions[0].dependencies == []


class TestCreateCalculatedColumnsProcessorFromMetadata:
    """Test the create_calculated_columns_processor_from_metadata function."""

    def test_create_processor_from_metadata_no_config(self):
        """Test creating processor from metadata with no calculated columns config."""
        metadata = {}
        assert create_calculated_columns_processor_from_metadata(metadata) is None

        metadata = {"other": "data"}
        assert create_calculated_columns_processor_from_metadata(metadata) is None

    def test_create_processor_from_metadata_with_config(self):
        """Test creating processor from metadata with calculated columns config."""
        metadata = {
            "x-calculatedColumns": {
                "constants": [{"name": "PI", "value": 3.14159, "dataType": "float64"}],
                "failOnError": True,
            }
        }

        processor = create_calculated_columns_processor_from_metadata(metadata)
        assert processor is not None
        assert isinstance(processor, CalculatedColumnsProcessor)
        assert len(processor.config.constants) == 1
        assert processor.config.constants[0].name == "PI"

    @patch(
        "forklift.processors.calculated_columns_factory.create_calculated_columns_processor_from_schema"
    )
    def test_create_processor_from_metadata_delegates_to_schema_function(
        self, mock_create_from_schema
    ):
        """Test that the metadata function properly delegates to the schema function."""
        mock_processor = Mock()
        mock_create_from_schema.return_value = mock_processor

        calculated_config = {"constants": [{"name": "TEST", "value": "test"}]}
        metadata = {"x-calculatedColumns": calculated_config}

        result = create_calculated_columns_processor_from_metadata(metadata)

        mock_create_from_schema.assert_called_once_with(calculated_config)
        assert result == mock_processor


class TestValidateCalculatedColumnsSchema:
    """Test the validate_calculated_columns_schema function."""

    def test_validate_schema_non_dict(self):
        """Test validation with non-dictionary schema."""
        errors = validate_calculated_columns_schema("not a dict")
        assert len(errors) == 1
        assert "Schema configuration must be a dictionary" in errors[0]

        errors = validate_calculated_columns_schema(None)
        assert len(errors) == 1
        assert "Schema configuration must be a dictionary" in errors[0]

    def test_validate_schema_empty_dict(self):
        """Test validation with empty dictionary (should be valid)."""
        errors = validate_calculated_columns_schema({})
        assert len(errors) == 0

    def test_validate_constants_valid(self):
        """Test validation of valid constants."""
        schema = {
            "constants": [{"name": "PI", "value": 3.14159}, {"name": "VERSION", "value": "1.0"}]
        }
        errors = validate_calculated_columns_schema(schema)
        assert len(errors) == 0

    def test_validate_constants_invalid(self):
        """Test validation of invalid constants."""
        schema = {
            "constants": [
                "not a dict",  # Invalid type
                {"value": 123},  # Missing name
                {"name": "TEST"},  # Missing value
                {},  # Missing both name and value
            ]
        }
        errors = validate_calculated_columns_schema(schema)
        assert len(errors) == 5
        assert "Constant at index 0 must be a dictionary" in errors[0]
        assert "Constant at index 1 missing required 'name' field" in errors[1]
        assert "Constant at index 2 missing required 'value' field" in errors[2]
        assert "Constant at index 3 missing required 'name' field" in errors[3]
        assert "Constant at index 3 missing required 'value' field" in errors[4]

    def test_validate_expressions_valid(self):
        """Test validation of valid expressions."""
        schema = {
            "expressions": [
                {"name": "full_name", "expression": "first + ' ' + last"},
                {"name": "age_plus_ten", "expression": "age + 10"},
            ]
        }
        errors = validate_calculated_columns_schema(schema)
        assert len(errors) == 0

    def test_validate_expressions_invalid(self):
        """Test validation of invalid expressions."""
        schema = {
            "expressions": [
                "not a dict",  # Invalid type
                {"expression": "x + 1"},  # Missing name
                {"name": "test"},  # Missing expression
                {},  # Missing both name and expression
            ]
        }
        errors = validate_calculated_columns_schema(schema)
        assert len(errors) == 5
        assert "Expression at index 0 must be a dictionary" in errors[0]
        assert "Expression at index 1 missing required 'name' field" in errors[1]
        assert "Expression at index 2 missing required 'expression' field" in errors[2]
        assert "Expression at index 3 missing required 'name' field" in errors[3]
        assert "Expression at index 3 missing required 'expression' field" in errors[4]

    def test_validate_calculated_columns_valid(self):
        """Test validation of valid calculated columns."""
        schema = {
            "calculated": [
                {"name": "total", "expression": "price * qty"},
                {"name": "discount", "function": "total * 0.1"},
                {"name": "both", "expression": "a + b", "function": "x + y"},
            ]
        }
        errors = validate_calculated_columns_schema(schema)
        assert len(errors) == 0

    def test_validate_calculated_columns_invalid(self):
        """Test validation of invalid calculated columns."""
        schema = {
            "calculated": [
                "not a dict",  # Invalid type
                {"name": "test"},  # Missing both expression and function
                {"expression": "x + 1"},  # Missing name
                {},  # Missing name and expression/function
            ]
        }
        errors = validate_calculated_columns_schema(schema)
        assert len(errors) == 5
        assert "Calculated column at index 0 must be a dictionary" in errors[0]
        assert (
            "Calculated column at index 1 missing required 'function' or 'expression' field"
            in errors[1]
        )
        assert "Calculated column at index 2 missing required 'name' field" in errors[2]
        assert "Calculated column at index 3 missing required 'name' field" in errors[3]
        assert (
            "Calculated column at index 3 missing required 'function' or 'expression' field"
            in errors[4]
        )

    def test_validate_mixed_valid_and_invalid(self):
        """Test validation with a mix of valid and invalid configurations."""
        schema = {
            "constants": [{"name": "VALID", "value": 1}, {"name": "INVALID"}],  # Missing value
            "expressions": [
                {"name": "valid_expr", "expression": "x + 1"},
                "invalid",
            ],  # Not a dict
            "calculated": [
                {"name": "valid_calc", "expression": "y * 2"},
                {"name": "invalid_calc"},  # Missing expression/function
            ],
        }
        errors = validate_calculated_columns_schema(schema)
        assert len(errors) == 3
        assert any(
            "Constant at index 1 missing required 'value' field" in error for error in errors
        )
        assert any("Expression at index 1 must be a dictionary" in error for error in errors)
        assert any(
            "Calculated column at index 1 missing required 'function' or 'expression' field"
            in error
            for error in errors
        )


class TestIntegration:
    """Integration tests for calculated columns factory functionality."""

    def test_end_to_end_processor_creation(self):
        """Test complete end-to-end processor creation and basic functionality."""
        schema_config = {
            "constants": [{"name": "TAX_RATE", "value": 0.08, "dataType": "float64"}],
            "expressions": [
                {
                    "name": "subtotal",
                    "expression": "price * quantity",
                    "dependencies": ["price", "quantity"],
                }
            ],
            "calculated": [
                {
                    "name": "tax_amount",
                    "expression": "subtotal * TAX_RATE",
                    "dependencies": ["subtotal"],
                }
            ],
            "partitionColumns": ["year", "month"],
            "failOnError": True,
            "addMetadata": True,
        }

        # Validate the schema first
        errors = validate_calculated_columns_schema(schema_config)
        assert len(errors) == 0

        # Create the processor
        processor = create_calculated_columns_processor_from_schema(schema_config)
        assert processor is not None
        assert isinstance(processor, CalculatedColumnsProcessor)

        # Verify the processor configuration
        assert len(processor.config.constants) == 1
        assert len(processor.config.expressions) == 1
        assert len(processor.config.calculated) == 1
        assert len(processor.config.partition_columns) == 2
        assert processor.config.fail_on_error is True
        assert processor.config.add_metadata is True

    def test_metadata_to_processor_workflow(self):
        """Test the workflow from metadata to processor creation."""
        metadata = {
            "x-calculatedColumns": {
                "constants": [{"name": "DEFAULT_STATUS", "value": "active", "dataType": "string"}],
                "expressions": [
                    {"name": "full_address", "expression": "street + ', ' + city + ', ' + state"}
                ],
            }
        }

        # Create processor from metadata
        processor = create_calculated_columns_processor_from_metadata(metadata)
        assert processor is not None

        # Verify configuration
        assert len(processor.config.constants) == 1
        assert len(processor.config.expressions) == 1
        assert processor.config.constants[0].name == "DEFAULT_STATUS"
        assert processor.config.expressions[0].name == "full_address"

    def test_complex_data_types_parsing(self):
        """Test complex data type parsing in a complete configuration."""
        schema_config = {
            "constants": [
                {"name": "RATES", "value": [0.1, 0.2, 0.3], "dataType": "list<float64>"},
                {"name": "PRECISION_VALUE", "value": 123.45, "dataType": "decimal128(10,2)"},
            ],
            "expressions": [
                {"name": "event_time", "expression": "now()", "dataType": "timestamp[ms]"}
            ],
        }

        processor = create_calculated_columns_processor_from_schema(schema_config)
        assert processor is not None

        # Verify data types were parsed correctly
        rates_constant = processor.config.constants[0]
        assert isinstance(rates_constant.data_type, pa.ListType)
        assert rates_constant.data_type.value_type == pa.float64()

        precision_constant = processor.config.constants[1]
        assert precision_constant.data_type == pa.decimal128(10, 2)

        event_expr = processor.config.expressions[0]
        assert event_expr.data_type == pa.timestamp("ms")
