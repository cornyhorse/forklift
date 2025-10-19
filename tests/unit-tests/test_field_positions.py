"""Tests for FWF field position calculation utilities."""

from forklift.schema.fwf.fields.positions import PositionCalculator


class TestPositionCalculator:
    """Test cases for PositionCalculator class."""

    def test_get_field_positions_basic(self):
        """Test basic field position calculation."""
        fields = [
            {"name": "field1", "start": 1, "length": 5},
            {"name": "field2", "start": 6, "length": 10},
            {"name": "field3", "start": 16, "length": 8},
        ]

        result = PositionCalculator.get_field_positions(fields)
        expected = [(0, 5), (5, 15), (15, 23)]  # 0-based indexing
        assert result == expected

    def test_get_field_positions_empty_list(self):
        """Test with empty fields list."""
        result = PositionCalculator.get_field_positions([])
        assert result == []

    def test_get_field_positions_missing_start(self):
        """Test with fields missing start attribute (defaults to 1)."""
        fields = [
            {"name": "field1", "length": 5},  # No start
            {"name": "field2", "start": 6, "length": 10},
        ]

        result = PositionCalculator.get_field_positions(fields)
        expected = [(0, 5), (5, 15)]  # Default start=1, converted to 0-based
        assert result == expected

    def test_get_field_positions_missing_length(self):
        """Test with fields missing length attribute (defaults to 1)."""
        fields = [
            {"name": "field1", "start": 1, "length": 5},
            {"name": "field2", "start": 6},  # No length
        ]

        result = PositionCalculator.get_field_positions(fields)
        expected = [(0, 5), (5, 6)]  # Default length=1
        assert result == expected

    def test_get_field_positions_missing_both(self):
        """Test with fields missing both start and length (both default to 1)."""
        fields = [
            {"name": "field1"},  # No start or length
            {"name": "field2", "start": 3, "length": 2},
        ]

        result = PositionCalculator.get_field_positions(fields)
        expected = [(0, 1), (2, 4)]  # Default start=1, length=1
        assert result == expected

    def test_get_field_positions_single_character_fields(self):
        """Test with single character fields."""
        fields = [
            {"name": "char1", "start": 1, "length": 1},
            {"name": "char2", "start": 2, "length": 1},
            {"name": "char3", "start": 3, "length": 1},
        ]

        result = PositionCalculator.get_field_positions(fields)
        expected = [(0, 1), (1, 2), (2, 3)]
        assert result == expected

    def test_get_field_positions_overlapping_fields(self):
        """Test with overlapping fields (edge case)."""
        fields = [
            {"name": "field1", "start": 1, "length": 10},
            {"name": "field2", "start": 5, "length": 8},  # Overlaps with field1
        ]

        result = PositionCalculator.get_field_positions(fields)
        expected = [(0, 10), (4, 12)]
        assert result == expected

    def test_get_field_positions_for_flag_value_basic(self):
        """Test basic flag value field positions."""
        flag_column = {"name": "flag", "start": 1, "length": 1}
        variant_fields = [
            {"name": "field1", "start": 2, "length": 5},
            {"name": "field2", "start": 7, "length": 10},
        ]

        result = PositionCalculator.get_field_positions_for_flag_value(flag_column, variant_fields)
        expected = [(0, 1), (1, 6), (6, 16)]  # Flag first, then variants
        assert result == expected

    def test_get_field_positions_for_flag_value_no_flag(self):
        """Test flag value field positions with no flag column."""
        variant_fields = [
            {"name": "field1", "start": 2, "length": 5},
            {"name": "field2", "start": 7, "length": 10},
        ]

        result = PositionCalculator.get_field_positions_for_flag_value(None, variant_fields)
        expected = [(1, 6), (6, 16)]  # Only variant fields
        assert result == expected

    def test_get_field_positions_for_flag_value_empty_variants(self):
        """Test flag value field positions with empty variant fields."""
        flag_column = {"name": "flag", "start": 1, "length": 1}

        result = PositionCalculator.get_field_positions_for_flag_value(flag_column, [])
        expected = [(0, 1)]  # Only flag column
        assert result == expected

    def test_get_field_positions_for_flag_value_missing_flag_start(self):
        """Test flag value positions with flag column missing start (defaults to 1)."""
        flag_column = {"name": "flag", "length": 2}  # No start
        variant_fields = [{"name": "field1", "start": 3, "length": 5}]

        result = PositionCalculator.get_field_positions_for_flag_value(flag_column, variant_fields)
        expected = [(0, 2), (2, 7)]  # Default start=1
        assert result == expected

    def test_get_field_positions_for_flag_value_missing_flag_length(self):
        """Test flag value positions with flag column missing length (defaults to 1)."""
        flag_column = {"name": "flag", "start": 1}  # No length
        variant_fields = [{"name": "field1", "start": 2, "length": 5}]

        result = PositionCalculator.get_field_positions_for_flag_value(flag_column, variant_fields)
        expected = [(0, 1), (1, 6)]  # Default length=1
        assert result == expected

    def test_get_field_positions_for_flag_value_missing_variant_start(self):
        """Test flag value positions with variant fields missing start."""
        flag_column = {"name": "flag", "start": 1, "length": 1}
        variant_fields = [
            {"name": "field1", "length": 5},  # No start
            {"name": "field2", "start": 6, "length": 3},
        ]

        result = PositionCalculator.get_field_positions_for_flag_value(flag_column, variant_fields)
        expected = [(0, 1), (0, 5), (5, 8)]  # Default start=1 for field1
        assert result == expected

    def test_get_field_positions_for_flag_value_missing_variant_length(self):
        """Test flag value positions with variant fields missing length."""
        flag_column = {"name": "flag", "start": 1, "length": 1}
        variant_fields = [
            {"name": "field1", "start": 2},  # No length
            {"name": "field2", "start": 3, "length": 5},
        ]

        result = PositionCalculator.get_field_positions_for_flag_value(flag_column, variant_fields)
        expected = [(0, 1), (1, 2), (2, 7)]  # Default length=1 for field1
        assert result == expected

    def test_extract_flag_value_from_row_basic(self):
        """Test basic flag value extraction from row."""
        row_data = "A12345JOHN DOE    "
        flag_column = {"name": "flag", "start": 1, "length": 1}

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result == "A"

    def test_extract_flag_value_from_row_multi_character(self):
        """Test flag value extraction with multi-character flag."""
        row_data = "AB12345JOHN DOE   "
        flag_column = {"name": "flag", "start": 1, "length": 2}

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result == "AB"

    def test_extract_flag_value_from_row_middle_position(self):
        """Test flag value extraction from middle of row."""
        row_data = "12345ABCDEFGH     "
        flag_column = {"name": "flag", "start": 6, "length": 3}

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result == "ABC"

    def test_extract_flag_value_from_row_with_whitespace(self):
        """Test flag value extraction with whitespace (should be stripped)."""
        row_data = "12345 B DEFGH     "
        flag_column = {"name": "flag", "start": 6, "length": 3}

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result == "B"

    def test_extract_flag_value_from_row_insufficient_length(self):
        """Test flag value extraction when row is too short."""
        row_data = "ABC"  # Only 3 characters
        flag_column = {"name": "flag", "start": 5, "length": 2}  # Starts at position 5

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result is None

    def test_extract_flag_value_from_row_exact_length(self):
        """Test flag value extraction when row has exact length."""
        row_data = "ABCDE"
        flag_column = {"name": "flag", "start": 4, "length": 2}  # Positions 4-5

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result is None  # Row length equals start + length

    def test_extract_flag_value_from_row_empty_row(self):
        """Test flag value extraction from empty row."""
        row_data = ""
        flag_column = {"name": "flag", "start": 1, "length": 1}

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result is None

    def test_extract_flag_value_from_row_missing_start(self):
        """Test flag value extraction with missing start (defaults to 1)."""
        row_data = "ABCDEF"
        flag_column = {"name": "flag", "length": 2}  # No start

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result == "AB"

    def test_extract_flag_value_from_row_missing_length(self):
        """Test flag value extraction with missing length (defaults to 1)."""
        row_data = "ABCDEF"
        flag_column = {"name": "flag", "start": 3}  # No length

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result == "C"

    def test_extract_flag_value_from_row_missing_both(self):
        """Test flag value extraction with missing start and length (both default)."""
        row_data = "ABCDEF"
        flag_column = {"name": "flag"}  # No start or length

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result == "A"

    def test_extract_flag_value_from_row_boundary_case(self):
        """Test flag value extraction at boundary (last possible position)."""
        row_data = "ABCDEF"
        flag_column = {"name": "flag", "start": 6, "length": 1}  # Last character

        result = PositionCalculator.extract_flag_value_from_row(row_data, flag_column)
        assert result is None  # Row length = 6, but need position 6 + length 1 = 7
