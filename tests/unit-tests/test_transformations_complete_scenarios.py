"""Comprehensive tests for transformations.py to achieve 100% code coverage."""

import sys
from typing import Any, Callable, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

# Add src to Python path for imports
sys.path.insert(0, "src")

from forklift.processors.base import ValidationResult
from forklift.processors.transformations import (
    ColumnTransformer,
    SchemaBasedTransformer,
    trim_whitespace,
)


class TestSchemaBasedTransformerSpecialTypes:
    """Test cases for SchemaBasedTransformer special type handling."""

    def test_ssn_special_type_transformation(self):
        """Test SSN special type transformation."""
        schema = {"properties": {"ssn_field": {"type": "string", "x-special-type": "ssn"}}}

        with patch("forklift.utils.transformations.SSNConfig") as mock_ssn_config:
            transformer = SchemaBasedTransformer(schema)

            # Verify SSN config was created with correct parameters
            mock_ssn_config.assert_called_once_with(
                format_with_dashes=True, zero_pad=True, validate=True, allow_invalid=False
            )

            # Verify transformation was added
            assert "ssn_field" in transformer.column_transformations
            assert len(transformer.column_transformations["ssn_field"]) == 1

    def test_zip_permissive_special_type_transformation(self):
        """Test ZIP permissive special type transformation."""
        schema = {
            "properties": {"zip_field": {"type": "string", "x-special-type": "zip-permissive"}}
        }

        with patch("forklift.utils.transformations.ZipCodeConfig") as mock_zip_config:
            transformer = SchemaBasedTransformer(schema)

            mock_zip_config.assert_called_once_with(
                zip_type="zip-permissive",
                format_with_dash=True,
                zero_pad=True,
                validate=True,
                allow_invalid=False,
            )

            assert "zip_field" in transformer.column_transformations
            assert len(transformer.column_transformations["zip_field"]) == 1

    def test_zip_5_special_type_transformation(self):
        """Test ZIP-5 special type transformation."""
        schema = {"properties": {"zip5_field": {"type": "string", "x-special-type": "zip-5"}}}

        with patch("forklift.utils.transformations.ZipCodeConfig") as mock_zip_config:
            transformer = SchemaBasedTransformer(schema)

            mock_zip_config.assert_called_once_with(
                zip_type="zip-5",
                format_with_dash=True,
                zero_pad=True,
                validate=True,
                allow_invalid=False,
            )

    def test_zip_9_special_type_transformation(self):
        """Test ZIP-9 special type transformation."""
        schema = {"properties": {"zip9_field": {"type": "string", "x-special-type": "zip-9"}}}

        with patch("forklift.utils.transformations.ZipCodeConfig") as mock_zip_config:
            transformer = SchemaBasedTransformer(schema)

            mock_zip_config.assert_called_once_with(
                zip_type="zip-9",
                format_with_dash=True,
                zero_pad=True,
                validate=True,
                allow_invalid=False,
            )

    def test_phone_special_type_transformation(self):
        """Test phone special type transformation."""
        schema = {"properties": {"phone_field": {"type": "string", "x-special-type": "phone"}}}

        with patch("forklift.utils.transformations.PhoneNumberConfig") as mock_phone_config:
            transformer = SchemaBasedTransformer(schema)

            mock_phone_config.assert_called_once_with(
                format_style="us-standard",
                use_parentheses=True,
                use_dashes=True,
                validate=True,
                allow_invalid=False,
            )

            assert "phone_field" in transformer.column_transformations
            assert len(transformer.column_transformations["phone_field"]) == 1

    def test_email_special_type_transformation(self):
        """Test email special type transformation."""
        schema = {"properties": {"email_field": {"type": "string", "x-special-type": "email"}}}

        with patch("forklift.utils.transformations.EmailConfig") as mock_email_config:
            transformer = SchemaBasedTransformer(schema)

            mock_email_config.assert_called_once_with(
                normalize_case=True,
                validate_format=True,
                allow_invalid=False,
                strip_whitespace=True,
                normalize_domain=True,
            )

            assert "email_field" in transformer.column_transformations
            assert len(transformer.column_transformations["email_field"]) == 1

    def test_ipv4_special_type_transformation(self):
        """Test IPv4 special type transformation."""
        schema = {"properties": {"ipv4_field": {"type": "string", "x-special-type": "ipv4"}}}

        with patch("forklift.utils.transformations.IPAddressConfig") as mock_ip_config:
            transformer = SchemaBasedTransformer(schema)

            mock_ip_config.assert_called_once_with(
                ip_version="ipv4",
                normalize_ipv6=True,
                validate=True,
                allow_invalid=False,
                compress_ipv6=True,
            )

            assert "ipv4_field" in transformer.column_transformations
            assert len(transformer.column_transformations["ipv4_field"]) == 1

    def test_ipv6_special_type_transformation(self):
        """Test IPv6 special type transformation."""
        schema = {"properties": {"ipv6_field": {"type": "string", "x-special-type": "ipv6"}}}

        with patch("forklift.utils.transformations.IPAddressConfig") as mock_ip_config:
            transformer = SchemaBasedTransformer(schema)

            mock_ip_config.assert_called_once_with(
                ip_version="ipv6",
                normalize_ipv6=True,
                validate=True,
                allow_invalid=False,
                compress_ipv6=True,
            )

    def test_ip_generic_special_type_transformation(self):
        """Test generic IP special type transformation."""
        schema = {"properties": {"ip_field": {"type": "string", "x-special-type": "ip"}}}

        with patch("forklift.utils.transformations.IPAddressConfig") as mock_ip_config:
            transformer = SchemaBasedTransformer(schema)

            mock_ip_config.assert_called_once_with(
                ip_version="both",
                normalize_ipv6=True,
                validate=True,
                allow_invalid=False,
                compress_ipv6=True,
            )

    def test_mac_address_special_type_transformation(self):
        """Test MAC address special type transformation."""
        schema = {"properties": {"mac_field": {"type": "string", "x-special-type": "mac-address"}}}

        with patch("forklift.utils.transformations.MACAddressConfig") as mock_mac_config:
            transformer = SchemaBasedTransformer(schema)

            mock_mac_config.assert_called_once_with(
                format_style="colon",
                case_style="lower",
                validate=True,
                allow_invalid=False,
                zero_pad=True,
            )

            assert "mac_field" in transformer.column_transformations
            assert len(transformer.column_transformations["mac_field"]) == 1

    def test_multiple_special_types_in_schema(self):
        """Test schema with multiple special type fields."""
        schema = {
            "properties": {
                "ssn_field": {"type": "string", "x-special-type": "ssn"},
                "email_field": {"type": "string", "x-special-type": "email"},
                "regular_field": {"type": "string"},
            }
        }

        with patch("forklift.utils.transformations.SSNConfig"):
            with patch("forklift.utils.transformations.EmailConfig"):
                transformer = SchemaBasedTransformer(schema)

                # Should have transformations for SSN and email, but not regular field
                assert "ssn_field" in transformer.column_transformations
                assert "email_field" in transformer.column_transformations
                assert "regular_field" not in transformer.column_transformations
                assert len(transformer.column_transformations) == 2

    def test_non_dict_property_ignored(self):
        """Test that non-dict properties are ignored."""
        schema = {
            "properties": {
                "string_field": "not_a_dict",
                "normal_field": {"type": "string", "x-special-type": "email"},
            }
        }

        with patch("forklift.utils.transformations.EmailConfig"):
            transformer = SchemaBasedTransformer(schema)

            # Should only process the dict property
            assert "string_field" not in transformer.column_transformations
            assert "normal_field" in transformer.column_transformations
            assert len(transformer.column_transformations) == 1

    def test_no_special_type_fields(self):
        """Test schema with no special type fields."""
        schema = {"properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}

        transformer = SchemaBasedTransformer(schema)

        # Should not add any transformations
        assert len(transformer.column_transformations) == 0

    def test_empty_properties_section(self):
        """Test schema with empty properties section."""
        schema = {"properties": {}}

        transformer = SchemaBasedTransformer(schema)
        assert len(transformer.column_transformations) == 0

    def test_no_properties_section(self):
        """Test schema with no properties section."""
        schema = {}

        transformer = SchemaBasedTransformer(schema)
        assert len(transformer.column_transformations) == 0


class TestSchemaBasedTransformerIntegration:
    """Integration tests for SchemaBasedTransformer."""

    @patch("forklift.utils.transformations.create_transformation_from_config")
    def test_parse_transformation_config_with_error(self, mock_create_transform):
        """Test parsing transformation config when creation fails."""
        mock_create_transform.side_effect = ValueError(
            "Unknown transformation type: invalid_transform"
        )

        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {"invalid_transform": {"enabled": True, "some_param": "value"}}
                }
            }
        }

        # Capture print output
        with patch("builtins.print") as mock_print:
            transformer = SchemaBasedTransformer(schema)

            # Should print warning and continue
            mock_print.assert_called_once_with(
                "Warning: Could not create transformation invalid_transform for column col1: Unknown transformation type: invalid_transform"
            )

            # Column should not be in transformations since creation failed
            assert "col1" not in transformer.column_transformations

    def test_schema_based_transformer_full_integration(self):
        """Test full SchemaBasedTransformer integration."""
        schema = {
            "properties": {"ssn_field": {"type": "string", "x-special-type": "ssn"}},
            "x-transformations": {
                "column_transformations": {
                    "text_field": {"string_replace": {"enabled": True, "old": "old", "new": "new"}}
                }
            },
        }

        with patch("forklift.utils.transformations.SSNConfig"):
            with patch(
                "forklift.utils.transformations.create_transformation_from_config"
            ) as mock_create:
                mock_transform = Mock()
                mock_create.return_value = mock_transform

                transformer = SchemaBasedTransformer(schema)

                # Should have transformations for both special type and explicit config
                assert "ssn_field" in transformer.column_transformations
                assert "text_field" in transformer.column_transformations

    def test_process_batch_with_transformations(self):
        """Test process_batch with actual transformations."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {"string_replace": {"enabled": True, "old": "old", "new": "new"}}
                }
            }
        }

        # Create test batch
        pa_schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["old_value", "another_old"]}
        batch = pa.record_batch(data, pa_schema)

        # Mock transformation function
        def mock_transform(column):
            return pc.replace_substring_regex(column, "old", "new")

        with patch(
            "forklift.utils.transformations.create_transformation_from_config"
        ) as mock_create:
            mock_create.return_value = mock_transform

            transformer = SchemaBasedTransformer(schema)
            result_batch, validation_results = transformer.process_batch(batch)

            # Verify transformation was applied
            assert len(validation_results) == 0
            assert result_batch.column("col1").to_pylist() == ["new_value", "another_new"]

    def test_process_batch_transformation_error(self):
        """Test process_batch when transformation fails."""
        # First create a schema that will actually create transformations
        schema = {"properties": {"col1": {"type": "string", "x-special-type": "ssn"}}}

        # Create test batch
        pa_schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["test"]}
        batch = pa.record_batch(data, pa_schema)

        # Mock the transformation to raise an error
        def failing_transform(column):
            raise ValueError("Transform failed")

        with patch("forklift.utils.transformations.SSNConfig"):
            transformer = SchemaBasedTransformer(schema)

            # Replace the transformation with one that fails
            transformer.column_transformations["col1"] = [failing_transform]

            result_batch, validation_results = transformer.process_batch(batch)

            # Should capture the error
            assert len(validation_results) == 1
            assert not validation_results[0].is_valid
            assert (
                "Schema-based transformation failed for column 'col1': Transform failed"
                in validation_results[0].error_message
            )
            assert validation_results[0].error_code == "SCHEMA_TRANSFORMATION_ERROR"
            assert validation_results[0].column_name == "col1"

    def test_disabled_transformation_not_processed(self):
        """Test that disabled transformations are not processed."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "string_replace": {
                            "enabled": False,  # Disabled transformation
                            "old": "old",
                            "new": "new",
                        }
                    }
                }
            }
        }

        transformer = SchemaBasedTransformer(schema)

        # Should not have any transformations since it's disabled
        assert "col1" not in transformer.column_transformations
        assert len(transformer.column_transformations) == 0

    def test_transformation_config_without_enabled_key(self):
        """Test transformation config that doesn't have enabled key."""
        schema = {
            "x-transformations": {
                "column_transformations": {
                    "col1": {
                        "string_replace": {
                            # No enabled key - should be treated as disabled
                            "old": "old",
                            "new": "new",
                        }
                    }
                }
            }
        }

        transformer = SchemaBasedTransformer(schema)

        # Should not have any transformations since enabled defaults to False
        assert "col1" not in transformer.column_transformations
        assert len(transformer.column_transformations) == 0

    def test_column_transformations_section_missing(self):
        """Test x-transformations without column_transformations section."""
        schema = {
            "x-transformations": {
                # Missing column_transformations section
            }
        }

        transformer = SchemaBasedTransformer(schema)
        assert len(transformer.column_transformations) == 0


class TestTrimWhitespaceFunction:
    """Test cases for trim_whitespace function."""

    def test_trim_whitespace_string_column(self):
        """Test trim_whitespace with string column."""
        column = pa.array(["  hello  ", "  world  ", "  test  "])
        result = trim_whitespace(column)
        assert result.to_pylist() == ["hello", "world", "test"]

    def test_trim_whitespace_non_string_column(self):
        """Test trim_whitespace with non-string column."""
        column = pa.array([1, 2, 3])
        result = trim_whitespace(column)
        assert result.equals(column)

    def test_trim_whitespace_with_nulls(self):
        """Test trim_whitespace with null values."""
        column = pa.array(["  hello  ", None, "  world  "])
        result = trim_whitespace(column)
        expected = ["hello", None, "world"]
        assert result.to_pylist() == expected


class TestUtilityTransformationFunctions:
    """Test cases for utility transformation functions."""

    def test_uppercase_function(self):
        """Test uppercase transformation function."""
        from forklift.processors.transformations import uppercase

        column = pa.array(["hello", "world", "Test"])
        result = uppercase(column)
        assert result.to_pylist() == ["HELLO", "WORLD", "TEST"]

    def test_uppercase_non_string_column(self):
        """Test uppercase with non-string column."""
        from forklift.processors.transformations import uppercase

        column = pa.array([1, 2, 3])
        result = uppercase(column)
        assert result.equals(column)

    def test_lowercase_function(self):
        """Test lowercase transformation function."""
        from forklift.processors.transformations import lowercase

        column = pa.array(["HELLO", "WORLD", "Test"])
        result = lowercase(column)
        assert result.to_pylist() == ["hello", "world", "test"]

    def test_lowercase_non_string_column(self):
        """Test lowercase with non-string column."""
        from forklift.processors.transformations import lowercase

        column = pa.array([1, 2, 3])
        result = lowercase(column)
        assert result.equals(column)

    @patch("forklift.processors.transformations.factories.MoneyTypeConfig")
    @patch("forklift.processors.transformations.factories.DataTransformer")
    def test_apply_money_conversion(self, mock_transformer_class, mock_config_class):
        """Test apply_money_conversion function."""
        from forklift.processors.transformations import apply_money_conversion

        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # Test function creation
        transform_func = apply_money_conversion(
            currency_symbols=["$", "€"],
            thousands_separator=",",
            decimal_separator=".",
            parentheses_negative=True,
        )

        # Verify config was created correctly
        mock_config_class.assert_called_once_with(
            currency_symbols=["$", "€"],
            thousands_separator=",",
            decimal_separator=".",
            parentheses_negative=True,
        )

        # Test applying the transformation
        test_column = pa.array(["$1,000.00", "$500"])
        transform_func(test_column)
        mock_transformer.apply_money_conversion.assert_called_once_with(test_column, mock_config)

    @patch("forklift.processors.transformations.factories.NumericCleaningConfig")
    @patch("forklift.processors.transformations.factories.DataTransformer")
    def test_apply_numeric_cleaning(self, mock_transformer_class, mock_config_class):
        """Test apply_numeric_cleaning function."""
        from forklift.processors.transformations import apply_numeric_cleaning

        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # Test function creation
        transform_func = apply_numeric_cleaning(
            thousands_separator=",", decimal_separator=".", allow_nan=True, target_type="double"
        )

        # Verify config was created correctly
        mock_config_class.assert_called_once_with(
            thousands_separator=",", decimal_separator=".", allow_nan=True
        )

        # Test applying the transformation
        test_column = pa.array(["1,000.50", "2,500"])
        transform_func(test_column)
        mock_transformer.apply_numeric_cleaning.assert_called_once_with(
            test_column, mock_config, "double"
        )

    @patch("forklift.processors.transformations.factories.RegexReplaceConfig")
    @patch("forklift.processors.transformations.factories.DataTransformer")
    def test_apply_regex_replace(self, mock_transformer_class, mock_config_class):
        """Test apply_regex_replace function."""
        from forklift.processors.transformations import apply_regex_replace

        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # Test function creation
        transform_func = apply_regex_replace(pattern=r"\d+", replacement="NUMBER", flags=0)

        # Verify config was created correctly
        mock_config_class.assert_called_once_with(pattern=r"\d+", replacement="NUMBER", flags=0)

        # Test applying the transformation
        test_column = pa.array(["test123", "abc456"])
        transform_func(test_column)
        mock_transformer.apply_regex_replace.assert_called_once_with(test_column, mock_config)

    @patch("forklift.processors.transformations.factories.StringReplaceConfig")
    @patch("forklift.processors.transformations.factories.DataTransformer")
    def test_apply_string_replace(self, mock_transformer_class, mock_config_class):
        """Test apply_string_replace function."""
        from forklift.processors.transformations import apply_string_replace

        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # Test function creation
        transform_func = apply_string_replace(old="old", new="new", count=-1)

        # Verify config was created correctly
        mock_config_class.assert_called_once_with(old="old", new="new", count=-1)

        # Test applying the transformation
        test_column = pa.array(["old_value", "another_old"])
        transform_func(test_column)
        mock_transformer.apply_string_replace.assert_called_once_with(test_column, mock_config)

    @patch("forklift.processors.transformations.factories.HTMLXMLConfig")
    @patch("forklift.processors.transformations.factories.DataTransformer")
    def test_apply_html_xml_cleaning(self, mock_transformer_class, mock_config_class):
        """Test apply_html_xml_cleaning function."""
        from forklift.processors.transformations import apply_html_xml_cleaning

        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # Test function creation
        transform_func = apply_html_xml_cleaning(
            strip_tags=True, decode_entities=True, preserve_whitespace=False
        )

        # Verify config was created correctly
        mock_config_class.assert_called_once_with(
            strip_tags=True, decode_entities=True, preserve_whitespace=False
        )

        # Test applying the transformation
        test_column = pa.array(["<b>hello</b>", "<i>world</i>"])
        transform_func(test_column)
        mock_transformer.apply_html_xml_cleaning.assert_called_once_with(test_column, mock_config)

    @patch("forklift.processors.transformations.factories.StringPaddingConfig")
    @patch("forklift.processors.transformations.factories.DataTransformer")
    def test_apply_string_padding(self, mock_transformer_class, mock_config_class):
        """Test apply_string_padding function."""
        from forklift.processors.transformations import apply_string_padding

        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # Test function creation
        transform_func = apply_string_padding(width=10, fillchar="0", side="left")

        # Verify config was created correctly
        mock_config_class.assert_called_once_with(width=10, fillchar="0", side="left")

        # Test applying the transformation
        test_column = pa.array(["123", "45"])
        transform_func(test_column)
        mock_transformer.apply_string_padding.assert_called_once_with(test_column, mock_config)

    @patch("forklift.processors.transformations.factories.DataTransformer")
    def test_apply_string_trimming(self, mock_transformer_class):
        """Test apply_string_trimming function."""
        from forklift.processors.transformations import apply_string_trimming

        mock_transformer = Mock()
        mock_transformer_class.return_value = mock_transformer

        # Test function creation
        transform_func = apply_string_trimming(side="both", chars=None)

        # Test applying the transformation
        test_column = pa.array(["  hello  ", "  world  "])
        transform_func(test_column)
        mock_transformer.apply_string_trimming.assert_called_once_with(test_column, "both", None)


class TestColumnTransformerEdgeCases:
    """Test edge cases for ColumnTransformer."""

    def test_column_transformer_with_empty_transformations(self):
        """Test ColumnTransformer with empty transformations dict."""
        transformer = ColumnTransformer({})

        # Create test batch
        schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["test"]}
        batch = pa.record_batch(data, schema)

        result_batch, validation_results = transformer.process_batch(batch)

        # Should return original batch unchanged
        assert result_batch.equals(batch)
        assert len(validation_results) == 0

    def test_column_transformer_with_multiple_transforms_per_column(self):
        """Test ColumnTransformer with multiple transformations per column."""
        from forklift.processors.transformations import trim_whitespace, uppercase

        transformations = {"col1": [trim_whitespace, uppercase]}
        transformer = ColumnTransformer(transformations)

        # Create test batch
        schema = pa.schema([pa.field("col1", pa.string())])
        data = {"col1": ["  hello  ", "  world  "]}
        batch = pa.record_batch(data, schema)

        result_batch, validation_results = transformer.process_batch(batch)

        # Should apply both transformations in sequence
        assert len(validation_results) == 0
        assert result_batch.column("col1").to_pylist() == ["HELLO", "WORLD"]
