"""Final tests to achieve 100% coverage for fwf.py."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from forklift.inputs.config import FwfConditionalSchema, FwfFieldSpec, FwfInputConfig
from forklift.inputs.fwf import FwfInputHandler


class TestFwfFinalCoverage:
    """Tests to cover the final remaining lines for 100% coverage."""

    def test_center_alignment_padding(self):
        """Test center alignment padding logic."""
        config = FwfInputConfig(
            fields=[
                FwfFieldSpec("center_field", 1, 10, align="center", pad="*", parquet_type="string")
            ]
        )
        handler = FwfInputHandler(config)

        # Test center alignment with short input requiring padding
        field = FwfFieldSpec("test", 1, 10, align="center", pad="*")
        result = handler.extract_field_value("ABC", field)
        # Should be centered: ***ABC****
        assert result == "***ABC****"

        # Test center alignment with odd padding
        field_odd = FwfFieldSpec("test", 1, 9, align="center", pad="-")
        result = handler.extract_field_value("XY", field_odd)
        # Should be centered: ---XY----
        assert result == "---XY----"

    def test_left_alignment_with_custom_padding_removal(self):
        """Test left alignment with custom padding character removal."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("left_field", 1, 5, align="left", pad="0", parquet_type="string")]
        )
        handler = FwfInputHandler(config)

        # Test left alignment with custom padding removal
        field = FwfFieldSpec("test", 1, 8, align="left", pad="0")
        result = handler.extract_field_value("ABC00000", field)
        # Should remove trailing padding characters
        assert result == "ABC"

    def test_field_extraction_no_trim(self):
        """Test field extraction without trimming."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("no_trim", 1, 10, trim=False, parquet_type="string")]
        )
        handler = FwfInputHandler(config)

        # Test extraction without trimming
        field = FwfFieldSpec("test", 1, 9, trim=False)
        result = handler.extract_field_value("  VALUE  ", field)
        # Should not trim whitespace when trim=False
        assert result == "  VALUE  "

    def test_convert_value_unknown_type(self):
        """Test convert_value with unknown/unsupported type."""
        config = FwfInputConfig(fields=[FwfFieldSpec("test", 1, 5, parquet_type="unknown_type")])
        handler = FwfInputHandler(config)

        # Test conversion with unknown type - should return original value
        result = handler.convert_value("test_value", "unknown_type")
        assert result == "test_value"

    def test_conditional_schema_without_matching_flag(self):
        """Test conditional schema processing when no flag matches."""
        flag_column = FwfFieldSpec("type", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema(
                "A",
                "Schema A",
                [
                    FwfFieldSpec("type", 1, 1, parquet_type="string"),
                    FwfFieldSpec("field1", 2, 5, parquet_type="string"),
                ],
            )
        ]

        config = FwfInputConfig(flag_column=flag_column, conditional_schemas=conditional_schemas)
        handler = FwfInputHandler(config)

        # Test line with flag that doesn't match any schema
        result = handler.parse_line("X12345")  # Flag 'X' doesn't match 'A'
        assert result is None

    def test_create_arrow_table_with_decimal_conversion_error(self):
        """Test arrow table creation with decimal conversion errors."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("decimal_field", 1, 10, parquet_type="decimal128(10,2)")]
        )
        handler = FwfInputHandler(config)

        # Create test data
        test_data = "invalid_dec"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
            f.write(test_data + "\n")
            temp_path = Path(f.name)

        try:
            table = handler.create_arrow_table(temp_path)
            # Should handle conversion error gracefully
            assert table.num_rows == 1
        finally:
            temp_path.unlink()

    def test_read_file_file_not_found(self):
        """Test read_file with non-existent file."""
        config = FwfInputConfig(fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")])
        handler = FwfInputHandler(config)

        # Test with non-existent file
        non_existent_path = Path("/tmp/non_existent_file.txt")
        with pytest.raises(FileNotFoundError):
            handler.read_file(non_existent_path)

    def test_create_arrow_table_file_not_found(self):
        """Test create_arrow_table with non-existent file."""
        config = FwfInputConfig(fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")])
        handler = FwfInputHandler(config)

        # Test with non-existent file
        non_existent_path = Path("/tmp/non_existent_file.txt")
        with pytest.raises(FileNotFoundError):
            handler.create_arrow_table(non_existent_path)

    def test_create_arrow_table_empty_records(self):
        """Test create_arrow_table with file that produces no records."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")], comment_patterns=["^#"]
        )
        handler = FwfInputHandler(config)

        # Create file with only comments (no actual records)
        test_data = ["# Comment line 1", "# Comment line 2"]

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
            for line in test_data:
                f.write(line + "\n")
            temp_path = Path(f.name)

        try:
            table = handler.create_arrow_table(temp_path)
            # Should create empty table with proper schema
            assert table.num_rows == 0
            assert len(table.schema) > 0  # Should have schema fields
        finally:
            temp_path.unlink()

    def test_arrow_table_string_fallback(self):
        """Test arrow table creation with type conversion that requires string fallback."""
        config = FwfInputConfig(fields=[FwfFieldSpec("problematic", 1, 10, parquet_type="int64")])
        handler = FwfInputHandler(config)

        # Create data that will cause issues
        test_data = "not_a_number"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
            f.write(test_data + "\n")
            temp_path = Path(f.name)

        try:
            # Force the array creation to fail for the original type
            original_array = pa.array
            call_count = [0]

            def mock_array_with_fallback(*args, **kwargs):
                call_count[0] += 1
                # Fail the first call (int64), succeed on string fallback
                if call_count[0] == 1 and kwargs.get("type") == pa.int64():
                    raise Exception("Type conversion failed")
                return original_array(*args, **kwargs)

            with patch("pyarrow.array", side_effect=mock_array_with_fallback):
                table = handler.create_arrow_table(temp_path)
                assert table.num_rows == 1
        finally:
            temp_path.unlink()

    def test_bool_conversion_false_cases(self):
        """Test boolean conversion for false cases."""
        config = FwfInputConfig(fields=[FwfFieldSpec("bool_field", 1, 5, parquet_type="bool")])
        handler = FwfInputHandler(config)

        # Test various false values
        false_values = ["false", "FALSE", "0", "no", "NO", "n", "N", "f", "F"]
        for value in false_values:
            result = handler.convert_value(value, "bool")
            assert result is False, f"Value '{value}' should convert to False"

        # Test value not in true list
        result = handler.convert_value("maybe", "bool")
        assert result is False
