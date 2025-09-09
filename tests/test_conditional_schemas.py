"""Tests for the ConditionalSchemaManager class."""

import pytest
from unittest.mock import patch, MagicMock
from forklift.schema.fwf.conditional.schemas import ConditionalSchemaManager


class TestConditionalSchemaManager:
    """Test cases for the ConditionalSchemaManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        # Sample conditional schemas configuration
        self.sample_conditional_schemas = {
            "flagColumn": {
                "name": "record_type",
                "start": 0,
                "length": 1
            },
            "schemas": [
                {
                    "flagValue": "H",
                    "fields": [
                        {"name": "header_field1", "start": 1, "length": 10},
                        {"name": "header_field2", "start": 11, "length": 20}
                    ]
                },
                {
                    "flagValue": "D",
                    "fields": [
                        {"name": "data_field1", "start": 1, "length": 15},
                        {"name": "data_field2", "start": 16, "length": 10}
                    ]
                },
                {
                    "flagValue": "T",
                    "fields": [
                        {"name": "trailer_field1", "start": 1, "length": 12}
                    ]
                }
            ]
        }

        # Configuration with missing flagColumn
        self.schemas_no_flag_column = {
            "schemas": [
                {
                    "flagValue": "A",
                    "fields": [{"name": "field1", "start": 1, "length": 5}]
                }
            ]
        }

        # Configuration with missing schemas
        self.schemas_no_variants = {
            "flagColumn": {
                "name": "type",
                "start": 0,
                "length": 1
            }
        }

    def test_init_with_complete_configuration(self):
        """Test initialization with complete conditional schemas configuration."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        assert manager.conditional_schemas == self.sample_conditional_schemas
        assert manager.flag_column == self.sample_conditional_schemas["flagColumn"]
        assert manager.schema_variants == self.sample_conditional_schemas["schemas"]

    def test_init_with_missing_flag_column(self):
        """Test initialization when flagColumn is missing."""
        manager = ConditionalSchemaManager(self.schemas_no_flag_column)

        assert manager.conditional_schemas == self.schemas_no_flag_column
        assert manager.flag_column is None
        assert manager.schema_variants == self.schemas_no_flag_column["schemas"]

    def test_init_with_missing_schemas(self):
        """Test initialization when schemas key is missing."""
        manager = ConditionalSchemaManager(self.schemas_no_variants)

        assert manager.conditional_schemas == self.schemas_no_variants
        assert manager.flag_column == self.schemas_no_variants["flagColumn"]
        assert manager.schema_variants == []  # Default to empty list

    def test_init_with_empty_configuration(self):
        """Test initialization with empty configuration."""
        empty_config = {}
        manager = ConditionalSchemaManager(empty_config)

        assert manager.conditional_schemas == empty_config
        assert manager.flag_column is None
        assert manager.schema_variants == []

    def test_get_flag_column_info_with_flag_column(self):
        """Test getting flag column info when it exists."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        flag_column_info = manager.get_flag_column_info()
        assert flag_column_info is not None
        assert flag_column_info["name"] == "record_type"
        assert flag_column_info["start"] == 0
        assert flag_column_info["length"] == 1

    def test_get_flag_column_info_without_flag_column(self):
        """Test getting flag column info when it doesn't exist."""
        manager = ConditionalSchemaManager(self.schemas_no_flag_column)

        flag_column_info = manager.get_flag_column_info()
        assert flag_column_info is None

    def test_get_schema_variants_with_variants(self):
        """Test getting schema variants when they exist."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        variants = manager.get_schema_variants()
        assert len(variants) == 3
        assert variants[0]["flagValue"] == "H"
        assert variants[1]["flagValue"] == "D"
        assert variants[2]["flagValue"] == "T"

    def test_get_schema_variants_without_variants(self):
        """Test getting schema variants when they don't exist."""
        manager = ConditionalSchemaManager(self.schemas_no_variants)

        variants = manager.get_schema_variants()
        assert variants == []

    def test_get_variant_by_flag_value_existing(self):
        """Test getting variant by existing flag value."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        variant = manager.get_variant_by_flag_value("H")
        assert variant is not None
        assert variant["flagValue"] == "H"
        assert len(variant["fields"]) == 2
        assert variant["fields"][0]["name"] == "header_field1"

    def test_get_variant_by_flag_value_nonexistent(self):
        """Test getting variant by non-existent flag value."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        variant = manager.get_variant_by_flag_value("X")
        assert variant is None

    def test_get_variant_by_flag_value_empty_variants(self):
        """Test getting variant when no variants exist."""
        manager = ConditionalSchemaManager(self.schemas_no_variants)

        variant = manager.get_variant_by_flag_value("H")
        assert variant is None

    def test_get_all_possible_flag_values(self):
        """Test getting all possible flag values."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        flag_values = manager.get_all_possible_flag_values()
        assert flag_values == ["H", "D", "T"]

    def test_get_all_possible_flag_values_empty_variants(self):
        """Test getting flag values when no variants exist."""
        manager = ConditionalSchemaManager(self.schemas_no_variants)

        flag_values = manager.get_all_possible_flag_values()
        assert flag_values == []

    def test_get_all_possible_flag_values_with_missing_flag_values(self):
        """Test getting flag values when some variants don't have flagValue."""
        schemas_with_missing = {
            "schemas": [
                {"flagValue": "A", "fields": []},
                {"fields": []},  # Missing flagValue
                {"flagValue": "C", "fields": []},
                {"flagValue": None, "fields": []}  # None flagValue
            ]
        }
        manager = ConditionalSchemaManager(schemas_with_missing)

        flag_values = manager.get_all_possible_flag_values()
        assert flag_values == ["A", "C"]

    def test_validate_flag_value_valid(self):
        """Test validating valid flag values."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        assert manager.validate_flag_value("H") is True
        assert manager.validate_flag_value("D") is True
        assert manager.validate_flag_value("T") is True

    def test_validate_flag_value_invalid(self):
        """Test validating invalid flag values."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        assert manager.validate_flag_value("X") is False
        assert manager.validate_flag_value("") is False
        assert manager.validate_flag_value("h") is False  # Case sensitive

    def test_validate_flag_value_empty_variants(self):
        """Test validating flag value when no variants exist."""
        manager = ConditionalSchemaManager(self.schemas_no_variants)

        assert manager.validate_flag_value("H") is False

    def test_get_fields_for_flag_value_existing(self):
        """Test getting fields for existing flag value."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        fields = manager.get_fields_for_flag_value("H")
        assert len(fields) == 2
        assert fields[0]["name"] == "header_field1"
        assert fields[1]["name"] == "header_field2"

    def test_get_fields_for_flag_value_nonexistent(self):
        """Test getting fields for non-existent flag value."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        fields = manager.get_fields_for_flag_value("X")
        assert fields == []

    def test_get_fields_for_flag_value_variant_without_fields(self):
        """Test getting fields for variant that has no fields key."""
        schemas_no_fields = {
            "schemas": [{"flagValue": "A"}]  # Missing fields key
        }
        manager = ConditionalSchemaManager(schemas_no_fields)

        fields = manager.get_fields_for_flag_value("A")
        assert fields == []

    @patch('forklift.schema.fwf.conditional.schemas.PositionCalculator.extract_flag_value_from_row')
    def test_get_record_mapping_for_row_with_valid_flag(self, mock_extract):
        """Test getting record mapping for row with valid flag value."""
        mock_extract.return_value = "H"
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        mapping = manager.get_record_mapping_for_row("H123456789012345678901234567890")

        assert mapping is not None
        assert mapping["flagValue"] == "H"
        assert mapping["variant"]["flagValue"] == "H"
        assert len(mapping["fields"]) == 2
        mock_extract.assert_called_once_with("H123456789012345678901234567890", manager.flag_column)

    @patch('forklift.schema.fwf.conditional.schemas.PositionCalculator.extract_flag_value_from_row')
    def test_get_record_mapping_for_row_with_invalid_flag(self, mock_extract):
        """Test getting record mapping for row with invalid flag value."""
        mock_extract.return_value = "X"
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        mapping = manager.get_record_mapping_for_row("X123456789012345678901234567890")

        assert mapping is None
        mock_extract.assert_called_once_with("X123456789012345678901234567890", manager.flag_column)

    @patch('forklift.schema.fwf.conditional.schemas.PositionCalculator.extract_flag_value_from_row')
    def test_get_record_mapping_for_row_with_none_flag_value(self, mock_extract):
        """Test getting record mapping when flag value extraction returns None."""
        mock_extract.return_value = None
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        mapping = manager.get_record_mapping_for_row("123456789012345678901234567890")

        assert mapping is None
        mock_extract.assert_called_once_with("123456789012345678901234567890", manager.flag_column)

    @patch('forklift.schema.fwf.conditional.schemas.PositionCalculator.extract_flag_value_from_row')
    def test_get_record_mapping_for_row_with_empty_flag_value(self, mock_extract):
        """Test getting record mapping when flag value extraction returns empty string."""
        mock_extract.return_value = ""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        mapping = manager.get_record_mapping_for_row("123456789012345678901234567890")

        assert mapping is None
        mock_extract.assert_called_once_with("123456789012345678901234567890", manager.flag_column)

    def test_get_record_mapping_for_row_without_flag_column(self):
        """Test getting record mapping when no flag column is configured."""
        manager = ConditionalSchemaManager(self.schemas_no_flag_column)

        mapping = manager.get_record_mapping_for_row("A123456789012345678901234567890")

        assert mapping is None

    def test_edge_case_empty_flag_value_in_variants(self):
        """Test handling of empty flag value in variants."""
        schemas_with_empty_flag = {
            "flagColumn": {"name": "type", "start": 0, "length": 1},
            "schemas": [
                {"flagValue": "", "fields": [{"name": "empty_field", "start": 1, "length": 5}]},
                {"flagValue": "A", "fields": [{"name": "normal_field", "start": 1, "length": 5}]}
            ]
        }
        manager = ConditionalSchemaManager(schemas_with_empty_flag)

        # Empty string should not be included in flag values (filtered out by truthiness)
        flag_values = manager.get_all_possible_flag_values()
        assert "" not in flag_values
        assert "A" in flag_values

        # But should still be retrievable by direct lookup
        variant = manager.get_variant_by_flag_value("")
        assert variant is not None
        assert variant["flagValue"] == ""

    def test_comprehensive_workflow(self):
        """Test a comprehensive workflow using multiple methods."""
        manager = ConditionalSchemaManager(self.sample_conditional_schemas)

        # Check flag column info
        flag_column = manager.get_flag_column_info()
        assert flag_column is not None
        assert flag_column["name"] == "record_type"

        # Get all variants
        variants = manager.get_schema_variants()
        assert len(variants) == 3

        # Get all flag values
        flag_values = manager.get_all_possible_flag_values()
        assert len(flag_values) == 3

        # Test each flag value
        for flag_value in flag_values:
            # Validate flag value
            assert manager.validate_flag_value(flag_value)

            # Get variant
            variant = manager.get_variant_by_flag_value(flag_value)
            assert variant is not None
            assert variant["flagValue"] == flag_value

            # Get fields
            fields = manager.get_fields_for_flag_value(flag_value)
            assert isinstance(fields, list)
            assert len(fields) > 0
