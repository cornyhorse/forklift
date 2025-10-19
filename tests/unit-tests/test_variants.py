"""Tests for the VariantManager class."""

import pytest

from forklift.schema.fwf.conditional.variants import VariantManager


class TestVariantManager:
    """Test cases for the VariantManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        # Sample schema variants for testing
        self.sample_variants = [
            {
                "flagValue": "A",
                "fields": [
                    {"name": "field1", "start": 1, "length": 10},
                    {"name": "field2", "start": 11, "length": 5},
                ],
            },
            {
                "flagValue": "B",
                "fields": [
                    {"name": "field3", "start": 1, "length": 8},
                    {"name": "field4", "start": 9, "length": 12},
                ],
            },
            {"flagValue": "C", "fields": [{"name": "field5", "start": 1, "length": 15}]},
        ]

        # Sample flag column info
        self.sample_flag_column = {"name": "record_type", "start": 0, "length": 1}

    def test_init_with_variants_and_flag_column(self):
        """Test initialization with variants and flag column info."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        assert manager.schema_variants == self.sample_variants
        assert manager.flag_column_info == self.sample_flag_column

    def test_init_with_none_variants(self):
        """Test initialization with None variants."""
        manager = VariantManager(None, self.sample_flag_column)

        assert manager.schema_variants == []
        assert manager.flag_column_info == self.sample_flag_column

    def test_init_with_none_flag_column(self):
        """Test initialization with None flag column info."""
        manager = VariantManager(self.sample_variants, None)

        assert manager.schema_variants == self.sample_variants
        assert manager.flag_column_info is None

    def test_init_with_empty_list(self):
        """Test initialization with empty variants list."""
        manager = VariantManager([], self.sample_flag_column)

        assert manager.schema_variants == []
        assert manager.flag_column_info == self.sample_flag_column

    def test_get_variant_by_flag_value_existing(self):
        """Test getting variant by existing flag value."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        variant = manager.get_variant_by_flag_value("A")
        assert variant is not None
        assert variant["flagValue"] == "A"
        assert len(variant["fields"]) == 2
        assert variant["fields"][0]["name"] == "field1"

    def test_get_variant_by_flag_value_nonexistent(self):
        """Test getting variant by non-existent flag value."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        variant = manager.get_variant_by_flag_value("X")
        assert variant is None

    def test_get_variant_by_flag_value_empty_variants(self):
        """Test getting variant when no variants exist."""
        manager = VariantManager([], self.sample_flag_column)

        variant = manager.get_variant_by_flag_value("A")
        assert variant is None

    def test_get_all_flag_values(self):
        """Test getting all flag values."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        flag_values = manager.get_all_flag_values()
        assert flag_values == ["A", "B", "C"]

    def test_get_all_flag_values_empty_variants(self):
        """Test getting flag values when no variants exist."""
        manager = VariantManager([], self.sample_flag_column)

        flag_values = manager.get_all_flag_values()
        assert flag_values == []

    def test_get_all_flag_values_with_missing_flag_values(self):
        """Test getting flag values when some variants don't have flagValue."""
        variants_with_missing = [
            {"flagValue": "A", "fields": []},
            {"fields": []},  # Missing flagValue
            {"flagValue": "C", "fields": []},
            {"flagValue": None, "fields": []},  # None flagValue
        ]
        manager = VariantManager(variants_with_missing, self.sample_flag_column)

        flag_values = manager.get_all_flag_values()
        assert flag_values == ["A", "C"]

    def test_get_variant_fields_existing_variant(self):
        """Test getting fields for existing variant."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        fields = manager.get_variant_fields("A")
        assert len(fields) == 2
        assert fields[0]["name"] == "field1"
        assert fields[1]["name"] == "field2"

    def test_get_variant_fields_nonexistent_variant(self):
        """Test getting fields for non-existent variant."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        fields = manager.get_variant_fields("X")
        assert fields == []

    def test_get_variant_fields_variant_without_fields(self):
        """Test getting fields for variant that has no fields key."""
        variants_no_fields = [{"flagValue": "A"}]
        manager = VariantManager(variants_no_fields, self.sample_flag_column)

        fields = manager.get_variant_fields("A")
        assert fields == []

    def test_has_variants_true(self):
        """Test has_variants when variants exist."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        assert manager.has_variants() is True

    def test_has_variants_false_empty_list(self):
        """Test has_variants with empty variants list."""
        manager = VariantManager([], self.sample_flag_column)

        assert manager.has_variants() is False

    def test_has_variants_false_none(self):
        """Test has_variants with None variants (converted to empty list)."""
        manager = VariantManager(None, self.sample_flag_column)

        assert manager.has_variants() is False

    def test_get_flag_column_name_with_flag_column(self):
        """Test getting flag column name when flag column info exists."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        name = manager.get_flag_column_name()
        assert name == "record_type"

    def test_get_flag_column_name_without_flag_column(self):
        """Test getting flag column name when flag column info is None."""
        manager = VariantManager(self.sample_variants, None)

        name = manager.get_flag_column_name()
        assert name is None

    def test_get_flag_column_name_missing_name_key(self):
        """Test getting flag column name when name key is missing."""
        flag_column_no_name = {"start": 0, "length": 1}
        manager = VariantManager(self.sample_variants, flag_column_no_name)

        name = manager.get_flag_column_name()
        assert name is None

    def test_get_flag_column_position_with_flag_column(self):
        """Test getting flag column position when flag column info exists."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        position = manager.get_flag_column_position()
        assert position == 0

    def test_get_flag_column_position_without_flag_column(self):
        """Test getting flag column position when flag column info is None."""
        manager = VariantManager(self.sample_variants, None)

        position = manager.get_flag_column_position()
        assert position is None

    def test_get_flag_column_position_missing_start_key(self):
        """Test getting flag column position when start key is missing."""
        flag_column_no_start = {"name": "record_type", "length": 1}
        manager = VariantManager(self.sample_variants, flag_column_no_start)

        position = manager.get_flag_column_position()
        assert position is None

    def test_validate_flag_value_valid(self):
        """Test validating existing flag value."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        assert manager.validate_flag_value("A") is True
        assert manager.validate_flag_value("B") is True
        assert manager.validate_flag_value("C") is True

    def test_validate_flag_value_invalid(self):
        """Test validating non-existent flag value."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        assert manager.validate_flag_value("X") is False
        assert manager.validate_flag_value("") is False
        assert manager.validate_flag_value("a") is False  # Case sensitive

    def test_validate_flag_value_empty_variants(self):
        """Test validating flag value when no variants exist."""
        manager = VariantManager([], self.sample_flag_column)

        assert manager.validate_flag_value("A") is False

    def test_edge_case_empty_strings_and_none_values(self):
        """Test edge cases with empty strings and None values."""
        edge_case_variants = [
            {"flagValue": "", "fields": []},  # Empty string flag value
            {"flagValue": "VALID", "fields": []},
        ]
        manager = VariantManager(edge_case_variants, self.sample_flag_column)

        # Empty string should be included in flag values
        flag_values = manager.get_all_flag_values()
        assert "" in flag_values
        assert "VALID" in flag_values

        # Should be able to get variant by empty string
        variant = manager.get_variant_by_flag_value("")
        assert variant is not None
        assert variant["flagValue"] == ""

        # Should validate empty string as valid
        assert manager.validate_flag_value("") is True

    def test_comprehensive_workflow(self):
        """Test a comprehensive workflow using multiple methods."""
        manager = VariantManager(self.sample_variants, self.sample_flag_column)

        # Check that we have variants
        assert manager.has_variants()

        # Get all flag values
        all_flags = manager.get_all_flag_values()
        assert len(all_flags) == 3

        # Validate each flag value
        for flag in all_flags:
            assert manager.validate_flag_value(flag)

            # Get variant and fields for each flag
            variant = manager.get_variant_by_flag_value(flag)
            assert variant is not None

            fields = manager.get_variant_fields(flag)
            assert isinstance(fields, list)

        # Check flag column info
        assert manager.get_flag_column_name() == "record_type"
        assert manager.get_flag_column_position() == 0
