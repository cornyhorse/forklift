"""Tests for column name utilities."""

import pytest

from forklift.utils.column_name_utilities import (
    dedupe_column_names, standardize_postgres_column_name)


class TestDedupeColumnNames:
    """Test cases for the dedupe_column_names function."""

    def test_no_duplicates(self):
        """Test that unique names are returned unchanged."""
        names = ["id", "name", "email", "age"]
        result = dedupe_column_names(names)
        assert result == ["id", "name", "email", "age"]

    def test_simple_duplicates_suffix_method(self):
        """Test basic duplicate handling with suffix method."""
        names = ["id", "name", "name", "amount", "name"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["id", "name", "name_1", "amount", "name_2"]
        assert result == expected

    def test_simple_duplicates_default_method(self):
        """Test that suffix method is the default."""
        names = ["col", "col", "col"]
        result = dedupe_column_names(names)  # No method specified
        expected = ["col", "col_1", "col_2"]
        assert result == expected

    def test_prefix_method(self):
        """Test duplicate handling with prefix method."""
        names = ["name", "name", "name", "id"]
        result = dedupe_column_names(names, method="prefix")
        expected = ["name", "1_name", "2_name", "id"]
        assert result == expected

    def test_error_method_with_duplicates(self):
        """Test that error method raises ValueError on duplicates."""
        names = ["id", "name", "name"]
        with pytest.raises(ValueError, match="Duplicate column name detected: name"):
            dedupe_column_names(names, method="error")

    def test_error_method_no_duplicates(self):
        """Test that error method works fine with no duplicates."""
        names = ["id", "name", "email"]
        result = dedupe_column_names(names, method="error")
        assert result == ["id", "name", "email"]

    def test_empty_list(self):
        """Test handling of empty input list."""
        result = dedupe_column_names([])
        assert result == []

    def test_single_item(self):
        """Test handling of single item list."""
        result = dedupe_column_names(["column"])
        assert result == ["column"]

    def test_all_same_names(self):
        """Test when all names are identical."""
        names = ["col", "col", "col", "col"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["col", "col_1", "col_2", "col_3"]
        assert result == expected

    def test_complex_suffix_pattern(self):
        """Test complex suffix patterns to ensure proper incrementing."""
        # This tests the regex pattern matching for existing suffixes
        names = ["col", "col", "col_1", "col"]
        result = dedupe_column_names(names, method="suffix")
        # First "col" stays as is
        # Second "col" becomes "col_1"
        # "col_1" becomes "col_1_1" (treated as duplicate of itself since it appears again)
        # Fourth "col" becomes "col_2" (since "col_1" is taken)
        expected = ["col", "col_1", "col_1_1", "col_2"]
        assert result == expected

    def test_existing_suffixes_complex(self):
        """Test handling when names already have numeric suffixes."""
        names = ["data_1", "data_1", "data_2", "data"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["data_1", "data_1_1", "data_2", "data"]
        assert result == expected

    def test_multiple_consecutive_duplicates(self):
        """Test multiple consecutive duplicate names."""
        names = ["a", "a", "a", "b", "b", "c"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["a", "a_1", "a_2", "b", "b_1", "c"]
        assert result == expected

    def test_prefix_method_with_existing_prefixes(self):
        """Test prefix method when names might conflict with generated prefixes."""
        names = ["col", "1_col", "col", "col"]
        result = dedupe_column_names(names, method="prefix")
        # Each occurrence of "col" gets handled independently
        # "1_col" stays as is, then subsequent "col" duplicates get prefixes
        expected = ["col", "1_col", "2_col", "3_col"]
        assert result == expected

    def test_mixed_case_names(self):
        """Test that mixed case names are treated as distinct."""
        names = ["Name", "name", "NAME"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["Name", "name", "NAME"]
        assert result == expected

    def test_special_characters_in_names(self):
        """Test names with special characters."""
        names = ["col@1", "col@1", "col#2", "col#2"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["col@1", "col@1_1", "col#2", "col#2_1"]
        assert result == expected

    def test_numeric_only_names(self):
        """Test names that are purely numeric."""
        names = ["123", "123", "456"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["123", "123_1", "456"]
        assert result == expected

    def test_empty_string_names(self):
        """Test handling of empty string names."""
        names = ["", "", "col"]
        result = dedupe_column_names(names, method="suffix")
        expected = ["", "_1", "col"]
        assert result == expected

    def test_whitespace_names(self):
        """Test names with whitespace."""
        names = [" ", "  ", "col", " "]
        result = dedupe_column_names(names, method="suffix")
        # " " and "  " are different strings, so " " appears twice
        expected = [" ", "  ", "col", " _1"]
        assert result == expected

    def test_very_long_names(self):
        """Test with very long column names."""
        long_name = "a" * 100
        names = [long_name, long_name, "short"]
        result = dedupe_column_names(names, method="suffix")
        expected = [long_name, f"{long_name}_1", "short"]
        assert result == expected

    def test_invalid_method(self):
        """Test behavior with invalid deduplication method."""
        names = ["col", "col"]
        # Invalid method should default to suffix behavior
        result = dedupe_column_names(names, method="invalid")
        expected = ["col", "col_1"]
        assert result == expected


class TestStandardizePostgresColumnName:
    """Test cases for the standardize_postgres_column_name function."""

    def test_basic_lowercase_conversion(self):
        """Test basic uppercase to lowercase conversion."""
        result = standardize_postgres_column_name("NAME")
        assert result == "name"

    def test_mixed_case_conversion(self):
        """Test mixed case conversion."""
        result = standardize_postgres_column_name("FirstName")
        assert result == "firstname"

    def test_spaces_to_underscores(self):
        """Test conversion of spaces to underscores."""
        result = standardize_postgres_column_name("First Name")
        assert result == "first_name"

    def test_special_characters_replacement(self):
        """Test replacement of special characters with underscores."""
        result = standardize_postgres_column_name("col@#$%^&*()name")
        assert result == "col_name"

    def test_multiple_consecutive_special_chars(self):
        """Test that multiple consecutive special characters become single underscore."""
        result = standardize_postgres_column_name("col!!!???name")
        assert result == "col_name"

    def test_leading_trailing_special_chars(self):
        """Test stripping of leading and trailing special characters."""
        result = standardize_postgres_column_name("@@@column@@@")
        assert result == "column"

    def test_numbers_preserved(self):
        """Test that numbers are preserved in column names."""
        result = standardize_postgres_column_name("Col123Name456")
        assert result == "col123name456"

    def test_mixed_alphanumeric_special(self):
        """Test mixed alphanumeric and special characters."""
        result = standardize_postgres_column_name("User_ID#123")
        assert result == "user_id_123"

    def test_postgres_length_limit(self):
        """Test truncation to PostgreSQL 63 character limit."""
        long_name = "a" * 100
        result = standardize_postgres_column_name(long_name)
        assert len(result) == 63
        assert result == "a" * 63

    def test_exactly_63_characters(self):
        """Test name that is exactly 63 characters."""
        name_63 = "a" * 63
        result = standardize_postgres_column_name(name_63)
        assert len(result) == 63
        assert result == name_63

    def test_under_63_characters(self):
        """Test name under 63 characters remains unchanged."""
        name_50 = "a" * 50
        result = standardize_postgres_column_name(name_50)
        assert len(result) == 50
        assert result == name_50

    def test_empty_string(self):
        """Test handling of empty string."""
        result = standardize_postgres_column_name("")
        assert result == ""

    def test_whitespace_only(self):
        """Test handling of whitespace-only string."""
        result = standardize_postgres_column_name("   ")
        assert result == ""

    def test_only_special_characters(self):
        """Test string with only special characters."""
        result = standardize_postgres_column_name("@#$%^&*()")
        assert result == ""

    def test_leading_whitespace(self):
        """Test stripping of leading whitespace."""
        result = standardize_postgres_column_name("   column_name")
        assert result == "column_name"

    def test_trailing_whitespace(self):
        """Test stripping of trailing whitespace."""
        result = standardize_postgres_column_name("column_name   ")
        assert result == "column_name"

    def test_internal_whitespace(self):
        """Test conversion of internal whitespace to underscores."""
        result = standardize_postgres_column_name("first   middle   last")
        assert result == "first_middle_last"

    def test_mixed_whitespace_and_special_chars(self):
        """Test mixed whitespace and special characters."""
        result = standardize_postgres_column_name("  col@name#test  ")
        assert result == "col_name_test"

    def test_underscores_preserved(self):
        """Test that existing underscores are preserved properly."""
        result = standardize_postgres_column_name("already_good_name")
        assert result == "already_good_name"

    def test_consecutive_underscores_collapsed(self):
        """Test that consecutive underscores are collapsed to single underscore."""
        result = standardize_postgres_column_name("col____name")
        assert result == "col_name"

    def test_real_world_examples(self):
        """Test real-world column name examples."""
        test_cases = [
            ("Customer ID", "customer_id"),
            ("Product Name & Description", "product_name_description"),
            ("Sales Amount ($)", "sales_amount"),
            ("Date/Time Created", "date_time_created"),
            ("Email-Address", "email_address"),
            ("Phone# (Primary)", "phone_primary"),
            ("Status Code [Active]", "status_code_active"),
            ("% Complete", "complete"),
        ]

        for input_name, expected in test_cases:
            result = standardize_postgres_column_name(input_name)
            assert (
                result == expected
            ), f"Failed for '{input_name}': got '{result}', expected '{expected}'"

    def test_unicode_characters(self):
        """Test handling of unicode characters."""
        result = standardize_postgres_column_name("Café_Naïve")
        # Unicode characters should be replaced with underscores
        assert result == "caf_na_ve"

    def test_tabs_and_newlines(self):
        """Test handling of tabs and newlines."""
        result = standardize_postgres_column_name("col\tname\ntest")
        assert result == "col_name_test"

    def test_numeric_start(self):
        """Test names that start with numbers."""
        result = standardize_postgres_column_name("123Column")
        assert result == "123column"

    def test_long_name_with_special_chars_truncation(self):
        """Test that long names with special chars are properly truncated after processing."""
        # Create a name that will be longer than 63 chars after processing
        long_name_with_spaces = "very " * 20 + "long column name"
        result = standardize_postgres_column_name(long_name_with_spaces)

        # Should be processed (spaces to underscores) then truncated to 63 chars
        assert len(result) == 63
        assert result.startswith("very_very_very")
        assert "_" in result  # Should contain underscores from space conversion


class TestColumnNameUtilitiesIntegration:
    """Integration tests combining both functions."""

    def test_standardize_then_dedupe(self):
        """Test using standardize then dedupe in sequence."""
        raw_names = ["Customer ID", "CUSTOMER ID", "customer_id", "Product Name"]

        # First standardize all names
        standardized = [standardize_postgres_column_name(name) for name in raw_names]

        # Then dedupe the standardized names
        result = dedupe_column_names(standardized)

        # Should have: customer_id, customer_id_1, customer_id_2, product_name
        expected = ["customer_id", "customer_id_1", "customer_id_2", "product_name"]
        assert result == expected

    def test_dedupe_then_standardize(self):
        """Test using dedupe then standardize in sequence."""
        names = ["Name", "Name", "Age"]

        # First dedupe
        deduped = dedupe_column_names(names)  # ["Name", "Name_1", "Age"]

        # Then standardize
        result = [standardize_postgres_column_name(name) for name in deduped]

        expected = ["name", "name_1", "age"]
        assert result == expected

    def test_empty_inputs_integration(self):
        """Test both functions with empty inputs."""
        # Empty list through dedupe
        deduped = dedupe_column_names([])
        assert deduped == []

        # Empty string through standardize
        standardized = standardize_postgres_column_name("")
        assert standardized == ""

    def test_complex_real_world_scenario(self):
        """Test a complex real-world scenario with messy column names."""
        messy_names = [
            "Customer ID#",
            "Customer Name & Title",
            "Customer ID#",  # Duplicate
            "Sales Amount ($USD)",
            "Date/Time (Created)",
            "Status - Active/Inactive",
            "Product Description (Long Text)",
            "Customer Name & Title",  # Another duplicate
        ]

        # Process through both functions
        standardized = [standardize_postgres_column_name(name) for name in messy_names]
        final_result = dedupe_column_names(standardized)

        expected = [
            "customer_id",
            "customer_name_title",
            "customer_id_1",
            "sales_amount_usd",
            "date_time_created",
            "status_active_inactive",
            "product_description_long_text",
            "customer_name_title_1",
        ]

        assert final_result == expected
