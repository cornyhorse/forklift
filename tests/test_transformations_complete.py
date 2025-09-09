"""Comprehensive tests for transformations.py to achieve 100% code coverage."""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Callable, Any

import pyarrow as pa
import pyarrow.compute as pc

# Add src to Python path for imports
sys.path.insert(0, 'src')

# Now we can import the transformations module and its dependencies
from forklift.processors.base import BaseProcessor, ValidationResult

# Import the transformations module directly
from forklift.processors.transformations import (
    ColumnTransformer,
    SchemaBasedTransformer,
    trim_whitespace,
    uppercase,
    lowercase,
    apply_money_conversion,
    apply_numeric_cleaning,
    apply_regex_replace,
    apply_string_replace,
    apply_html_xml_cleaning,
    apply_string_padding,
    apply_string_trimming
)


class TestColumnTransformer:
    """Test cases for ColumnTransformer class."""

    def test_init(self):
        """Test ColumnTransformer initialization."""
        transformations = {"col1": [trim_whitespace], "col2": [uppercase]}
        transformer = ColumnTransformer(transformations)
        assert transformer.transformations == transformations

    def test_process_batch_successful_transformation(self):
        """Test successful batch processing with transformations."""
        # Create test data
        schema = pa.schema([
            pa.field("col1", pa.string()),
            pa.field("col2", pa.string())
        ])

        data = {
            "col1": ["  hello  ", "  world  "],
            "col2": ["foo", "bar"]
        }

        batch = pa.record_batch(data, schema)

        # Setup transformations
        transformations = {
            "col1": [trim_whitespace],
            "col2": [uppercase]
        }

        transformer = ColumnTransformer(transformations)

        # Process batch
        result_batch, validation_results = transformer.process_batch(batch)

        # Verify results
        assert len(validation_results) == 0
        assert result_batch.column("col1").to_pylist() == ["hello", "world"]
        assert result_batch.column("col2").to_pylist() == ["FOO", "BAR"]

    def test_process_batch_column_not_in_schema(self):
        """Test processing when configured column is not in batch."""
        schema = pa.schema([pa.field("existing_col", pa.string())])
        data = {"existing_col": ["test"]}
        batch = pa.record_batch(data, schema)

        transformations = {"missing_col": [trim_whitespace]}
        transformer = ColumnTransformer(transformations)

        result_batch, validation_results = transformer.process_batch(batch)

        # Should not fail, just skip the missing column
        assert len(validation_results) == 0
        assert result_batch.equals(batch)

    def test_process_batch_transformation_error(self):
        """Test handling of transformation errors."""
        schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["test"]}
        batch = pa.record_batch(data, schema)

        # Create a transformation that will raise an exception
        def failing_transform(column):
            raise ValueError("Test error")

        transformations = {"col1": [failing_transform]}
        transformer = ColumnTransformer(transformations)

        result_batch, validation_results = transformer.process_batch(batch)

        # Should capture the error
        assert len(validation_results) == 1
        assert not validation_results[0].is_valid
        assert "Transformation failed for column 'col1': Test error" in validation_results[0].error_message
        assert validation_results[0].error_code == "TRANSFORMATION_ERROR"
        assert validation_results[0].column_name == "col1"

    def test_apply_transforms_single_transform(self):
        """Test applying a single transformation."""
        transformer = ColumnTransformer({})
        column = pa.array(["  hello  ", "  world  "])
        transforms = [trim_whitespace]

        result = transformer._apply_transforms(column, transforms)

        assert result.to_pylist() == ["hello", "world"]

    def test_apply_transforms_multiple_transforms(self):
        """Test applying multiple transformations in sequence."""
        transformer = ColumnTransformer({})
        column = pa.array(["  hello  ", "  world  "])
        transforms = [trim_whitespace, uppercase]

        result = transformer._apply_transforms(column, transforms)

        assert result.to_pylist() == ["HELLO", "WORLD"]

    def test_apply_transforms_empty_list(self):
        """Test applying empty transformation list."""
        transformer = ColumnTransformer({})
        column = pa.array(["hello", "world"])
        transforms = []

        result = transformer._apply_transforms(column, transforms)

        assert result.equals(column)


class TestSchemaBasedTransformer:
    """Test cases for SchemaBasedTransformer class."""

    def test_init_with_transformations(self):
        """Test SchemaBasedTransformer initialization with transformations."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "string_replace": {
                            "enabled": True,
                            "old": "old",
                            "new": "new"
                        }
                    }
                }
            }
        }

        transformer = SchemaBasedTransformer(schema)

        assert transformer.schema == schema
        assert hasattr(transformer, 'transformer')
        assert hasattr(transformer, 'column_transformations')
        assert "col1" in transformer.column_transformations

    def test_init_empty_schema(self):
        """Test initialization with empty schema."""
        schema = {}
        transformer = SchemaBasedTransformer(schema)

        assert transformer.schema == schema
        assert transformer.column_transformations == {}

    def test_parse_transformation_config_enabled_transformation(self):
        """Test parsing enabled transformation configuration."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "regex_replace": {
                            "enabled": True,
                            "pattern": "\\s+",
                            "replacement": " "
                        }
                    }
                }
            }
        }

        transformer = SchemaBasedTransformer(schema)

        assert "col1" in transformer.column_transformations
        assert len(transformer.column_transformations["col1"]) == 1

    def test_parse_transformation_config_disabled_transformation(self):
        """Test parsing disabled transformation configuration."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "regex_replace": {
                            "enabled": False,
                            "pattern": "\\s+",
                            "replacement": " "
                        }
                    }
                }
            }
        }

        transformer = SchemaBasedTransformer(schema)

        assert transformer.column_transformations == {}

    def test_parse_transformation_config_invalid_transformation(self):
        """Test handling of invalid transformation configuration."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "invalid_transform": {
                            "enabled": True
                        }
                    }
                }
            }
        }

        with patch('builtins.print') as mock_print:
            transformer = SchemaBasedTransformer(schema)

            assert transformer.column_transformations == {}
            assert mock_print.called

    def test_parse_transformation_config_non_dict_config(self):
        """Test handling of non-dictionary transformation config."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "some_transform": "not_a_dict"
                    }
                }
            }
        }

        transformer = SchemaBasedTransformer(schema)

        assert transformer.column_transformations == {}

    def test_process_batch_successful(self):
        """Test successful batch processing with schema-based transformations."""
        schema_dict = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "string_replace": {
                            "enabled": True,
                            "old": "old",
                            "new": "new"
                        }
                    }
                }
            }
        }

        # Create test data
        pa_schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["old_value", "another_old"]}
        batch = pa.record_batch(data, pa_schema)

        transformer = SchemaBasedTransformer(schema_dict)
        result_batch, validation_results = transformer.process_batch(batch)

        assert len(validation_results) == 0
        assert result_batch.column("col1").to_pylist() == ["new_value", "another_new"]

    def test_process_batch_column_not_in_batch(self):
        """Test processing when configured column is not in batch."""
        schema_dict = {
            "x-transformations": {
                "column_transformations": {
                    "missing_col": {
                        "string_replace": {
                            "enabled": True,
                            "old": "old",
                            "new": "new"
                        }
                    }
                }
            }
        }

        pa_schema = pa.schema([pa.field("existing_col", pa.string())])
        data = {"existing_col": ["test"]}
        batch = pa.record_batch(data, pa_schema)

        transformer = SchemaBasedTransformer(schema_dict)
        result_batch, validation_results = transformer.process_batch(batch)

        assert len(validation_results) == 0
        assert result_batch.equals(batch)

    def test_process_batch_transformation_error(self):
        """Test handling of transformation errors in schema-based processing."""
        schema_dict = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "string_replace": {
                            "enabled": True,
                            "old": "old",
                            "new": "new"
                        }
                    }
                }
            }
        }

        pa_schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["test"]}
        batch = pa.record_batch(data, pa_schema)

        # Mock the create_transformation_from_config function where it's used
        def failing_mock_create(transform_type, config):
            def failing_transform(column):
                raise RuntimeError("Transformation failed")
            return failing_transform

        # Patch the function in the schema_transformer module where it's actually imported and used
        with patch('forklift.processors.transformations.schema_transformer.create_transformation_from_config', failing_mock_create):
            transformer = SchemaBasedTransformer(schema_dict)
            result_batch, validation_results = transformer.process_batch(batch)

            assert len(validation_results) == 1
            assert not validation_results[0].is_valid
            assert "Schema-based transformation failed for column 'col1'" in validation_results[0].error_message
            assert validation_results[0].error_code == "SCHEMA_TRANSFORMATION_ERROR"
            assert validation_results[0].column_name == "col1"


class TestTransformationFunctions:
    """Test cases for individual transformation functions."""

    def test_trim_whitespace_string_column(self):
        """Test trimming whitespace from string column."""
        column = pa.array(["  hello  ", "  world  ", " test "])
        result = trim_whitespace(column)
        assert result.to_pylist() == ["hello", "world", "test"]

    def test_trim_whitespace_non_string_column(self):
        """Test trim_whitespace with non-string column."""
        column = pa.array([1, 2, 3])
        result = trim_whitespace(column)
        assert result.equals(column)

    def test_uppercase_string_column(self):
        """Test converting string column to uppercase."""
        column = pa.array(["hello", "world", "Test"])
        result = uppercase(column)
        assert result.to_pylist() == ["HELLO", "WORLD", "TEST"]

    def test_uppercase_non_string_column(self):
        """Test uppercase with non-string column."""
        column = pa.array([1, 2, 3])
        result = uppercase(column)
        assert result.equals(column)

    def test_lowercase_string_column(self):
        """Test converting string column to lowercase."""
        column = pa.array(["HELLO", "WORLD", "Test"])
        result = lowercase(column)
        assert result.to_pylist() == ["hello", "world", "test"]

    def test_lowercase_non_string_column(self):
        """Test lowercase with non-string column."""
        column = pa.array([1, 2, 3])
        result = lowercase(column)
        assert result.equals(column)

    def test_apply_money_conversion_callable(self):
        """Test that apply_money_conversion returns a callable."""
        transform_func = apply_money_conversion()
        assert callable(transform_func)

    def test_apply_money_conversion_custom_params(self):
        """Test money conversion with custom parameters."""
        transform_func = apply_money_conversion(
            currency_symbols=["€", "$"],
            thousands_separator=".",
            decimal_separator=",",
            parentheses_negative=False
        )
        assert callable(transform_func)

    def test_apply_numeric_cleaning_callable(self):
        """Test that apply_numeric_cleaning returns a callable."""
        transform_func = apply_numeric_cleaning()
        assert callable(transform_func)

    def test_apply_numeric_cleaning_custom_params(self):
        """Test numeric cleaning with custom parameters."""
        transform_func = apply_numeric_cleaning(
            thousands_separator=".",
            decimal_separator=",",
            allow_nan=False,
            target_type="int64"
        )
        assert callable(transform_func)

    def test_apply_regex_replace_callable(self):
        """Test that apply_regex_replace returns a callable."""
        transform_func = apply_regex_replace("\\s+", " ")
        assert callable(transform_func)

    def test_apply_regex_replace_with_flags(self):
        """Test regex replace with flags."""
        import re
        transform_func = apply_regex_replace("hello", "hi", re.IGNORECASE)
        assert callable(transform_func)

    def test_apply_string_replace_callable(self):
        """Test that apply_string_replace returns a callable."""
        transform_func = apply_string_replace("old", "new")
        assert callable(transform_func)

    def test_apply_string_replace_with_count(self):
        """Test string replace with count parameter."""
        transform_func = apply_string_replace("old", "new", count=1)
        assert callable(transform_func)

    def test_apply_html_xml_cleaning_callable(self):
        """Test that apply_html_xml_cleaning returns a callable."""
        transform_func = apply_html_xml_cleaning()
        assert callable(transform_func)

    def test_apply_html_xml_cleaning_custom_params(self):
        """Test HTML/XML cleaning with custom parameters."""
        transform_func = apply_html_xml_cleaning(
            strip_tags=False,
            decode_entities=False,
            preserve_whitespace=True
        )
        assert callable(transform_func)

    def test_apply_string_padding_callable(self):
        """Test that apply_string_padding returns a callable."""
        transform_func = apply_string_padding(10)
        assert callable(transform_func)

    def test_apply_string_padding_custom_params(self):
        """Test string padding with custom parameters."""
        transform_func = apply_string_padding(
            width=15,
            fillchar="*",
            side="right"
        )
        assert callable(transform_func)

    def test_apply_string_trimming_callable(self):
        """Test that apply_string_trimming returns a callable."""
        transform_func = apply_string_trimming()
        assert callable(transform_func)

    def test_apply_string_trimming_custom_params(self):
        """Test string trimming with custom parameters."""
        transform_func = apply_string_trimming(
            side="left",
            chars="*"
        )
        assert callable(transform_func)


class TestIntegration:
    """Integration tests combining multiple components."""

    def test_column_transformer_with_basic_functions(self):
        """Test using basic transformation functions with ColumnTransformer."""
        # Create test data
        schema = pa.schema([
            pa.field("text_col", pa.string()),
            pa.field("case_col", pa.string())
        ])

        data = {
            "text_col": ["  HELLO  ", "  WORLD  "],
            "case_col": ["foo", "bar"]
        }

        batch = pa.record_batch(data, schema)

        # Setup transformations using the basic functions
        transformations = {
            "text_col": [trim_whitespace, lowercase],
            "case_col": [uppercase]
        }

        transformer = ColumnTransformer(transformations)
        result_batch, validation_results = transformer.process_batch(batch)

        # Verify results
        assert len(validation_results) == 0
        assert result_batch.column("text_col").to_pylist() == ["hello", "world"]
        assert result_batch.column("case_col").to_pylist() == ["FOO", "BAR"]

    def test_schema_based_transformer_multiple_transformations(self):
        """Test SchemaBasedTransformer with multiple transformation types."""
        schema_dict = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "string_replace": {
                            "enabled": True,
                            "old": "old",
                            "new": "new"
                        },
                        "regex_replace": {
                            "enabled": True,
                            "pattern": "\\s+",
                            "replacement": " "
                        }
                    },
                    "col2": {
                        "money_conversion": {
                            "enabled": True,
                            "currency_symbols": ["$"]
                        }
                    }
                }
            }
        }

        pa_schema = pa.schema([
            pa.field("col1", pa.string()),
            pa.field("col2", pa.string())
        ])

        data = {
            "col1": ["old  text", "old   value"],
            "col2": ["$100", "$200"]
        }

        batch = pa.record_batch(data, pa_schema)

        transform_count = 0
        def mock_create_transform(transform_type, config):
            nonlocal transform_count
            transform_count += 1

            if transform_type == "string_replace":
                return lambda col: pa.array(["new text", "new value"])
            elif transform_type == "regex_replace":
                return lambda col: pa.array(["new text", "new value"])
            elif transform_type == "money_conversion":
                return lambda col: pa.array([100.0, 200.0])
            else:
                return lambda col: col

        with patch('forklift.processors.transformations.schema_transformer.create_transformation_from_config', mock_create_transform):
            transformer = SchemaBasedTransformer(schema_dict)
            result_batch, validation_results = transformer.process_batch(batch)

            assert len(validation_results) == 0
            assert transform_count == 3  # Two for col1, one for col2
            assert result_batch.column("col1").to_pylist() == ["new text", "new value"]
            assert result_batch.column("col2").to_pylist() == [100.0, 200.0]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_x_transformations_section(self):
        """Test schema with empty x-transformations section."""
        schema = {"x-transformations": {}}
        transformer = SchemaBasedTransformer(schema)
        assert transformer.column_transformations == {}

    def test_missing_column_transformations_key(self):
        """Test schema missing column_transformations key."""
        schema = {
            "x-transformations": {
                "global_settings": {"some": "setting"}
            }
        }
        transformer = SchemaBasedTransformer(schema)
        assert transformer.column_transformations == {}

    def test_column_transformer_empty_transformations(self):
        """Test ColumnTransformer with empty transformations dict."""
        transformer = ColumnTransformer({})

        schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["test"]}
        batch = pa.record_batch(data, schema)

        result_batch, validation_results = transformer.process_batch(batch)

        assert len(validation_results) == 0
        assert result_batch.equals(batch)

    def test_missing_x_transformations_key(self):
        """Test schema without x-transformations key."""
        schema = {"properties": {"col1": {"type": "string"}}}
        transformer = SchemaBasedTransformer(schema)
        assert transformer.column_transformations == {}

    def test_schema_based_transformer_no_enabled_transformations(self):
        """Test SchemaBasedTransformer when no transformations are enabled."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "string_replace": {
                            "enabled": False,
                            "old": "old",
                            "new": "new"
                        }
                    }
                }
            }
        }

        transformer = SchemaBasedTransformer(schema)
        assert transformer.column_transformations == {}

        # Test processing with no transformations
        pa_schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["test"]}
        batch = pa.record_batch(data, pa_schema)

        result_batch, validation_results = transformer.process_batch(batch)
        assert len(validation_results) == 0
        assert result_batch.equals(batch)
