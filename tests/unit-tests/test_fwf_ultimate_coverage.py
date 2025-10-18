"""Ultra-targeted tests to achieve 100% coverage for fwf.py."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from forklift.inputs.config import (FwfConditionalSchema, FwfFieldSpec,
                                    FwfInputConfig)
from forklift.inputs.fwf import FwfInputHandler


class TestFwfUltimateCoverage:
    """Tests to cover the final 6 remaining lines for 100% coverage."""

    def test_convert_value_exception_handling(self):
        """Test convert_value exception handling (line 295)."""
        config = FwfInputConfig(fields=[FwfFieldSpec("test", 1, 5, parquet_type="int64")])
        handler = FwfInputHandler(config)

        # Force a TypeError by mocking int() to raise an exception
        with patch("builtins.int", side_effect=TypeError("Mocked error")):
            result = handler.convert_value("123", "int64")
            # Should return original value when conversion fails
            assert result == "123"

    def test_comment_pattern_match(self):
        """Test comment pattern matching (line 335)."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")],
            comment_patterns=[r"^#.*", r"^//.*"],
        )
        handler = FwfInputHandler(config)

        # Test pattern that matches
        assert handler.is_comment_line("# This is a comment") is True
        assert handler.is_comment_line("// This is also a comment") is True

    def test_parse_line_no_fields_to_use(self):
        """Test parse_line when no fields are available (line 439)."""
        # Create a valid config first, then test the no fields scenario
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

        # Test with a line that has no matching conditional schema
        # This will make fields_to_use None, hitting the "if not fields_to_use:" check
        result = handler.parse_line("Z12345")  # Flag 'Z' doesn't match 'A'
        assert result is None

    def test_parse_line_null_field_value(self):
        """Test parse_line with null field value processing (line 453)."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")],
            null_values={"global": ["NULL"]},
        )
        handler = FwfInputHandler(config)

        # Test line that produces null value
        result = handler.parse_line("NULL ")
        assert result is not None
        assert result["test"] is None  # Should be None due to null processing

    def test_create_arrow_table_decimal_conversion_exception(self):
        """Test decimal conversion exception in create_arrow_table (lines 542, 544)."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("decimal_field", 1, 10, parquet_type="decimal128(10,2)")]
        )
        handler = FwfInputHandler(config)

        # Create test data that will trigger decimal conversion
        test_data = "123.45"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
            f.write(test_data + "\n")
            temp_path = Path(f.name)

        try:
            # Mock float() to raise an exception during decimal conversion
            original_float = float
            call_count = [0]

            def mock_float_with_exception(value):
                call_count[0] += 1
                # Raise exception on first call during decimal conversion
                if call_count[0] == 1 and value == "123.45":
                    raise ValueError("Mocked decimal conversion error")
                return original_float(value)

            with patch("builtins.float", side_effect=mock_float_with_exception):
                table = handler.create_arrow_table(temp_path)
                # Should handle the exception and set value to None
                assert table.num_rows == 1
        finally:
            temp_path.unlink()

    def test_create_arrow_table_bool_type_conversion(self):
        """Test bool type conversion in create_arrow_table."""
        config = FwfInputConfig(fields=[FwfFieldSpec("bool_field", 1, 5, parquet_type="bool")])
        handler = FwfInputHandler(config)

        # Create test data with boolean value
        test_data = "true "

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
            f.write(test_data + "\n")
            temp_path = Path(f.name)

        try:
            table = handler.create_arrow_table(temp_path)
            assert table.num_rows == 1
            # Verify the boolean conversion worked
            column_data = table.column("bool_field").to_pylist()
            assert column_data[0] is True
        finally:
            temp_path.unlink()

    def test_create_arrow_table_float_type_conversion(self):
        """Test float type conversion in create_arrow_table."""
        config = FwfInputConfig(fields=[FwfFieldSpec("float_field", 1, 10, parquet_type="float64")])
        handler = FwfInputHandler(config)

        # Create test data with float value
        test_data = "123.456"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
            f.write(test_data + "\n")
            temp_path = Path(f.name)

        try:
            table = handler.create_arrow_table(temp_path)
            assert table.num_rows == 1
            # Verify the float conversion worked
            column_data = table.column("float_field").to_pylist()
            assert abs(column_data[0] - 123.456) < 0.001
        finally:
            temp_path.unlink()
