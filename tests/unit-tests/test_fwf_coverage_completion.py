"""Additional tests to achieve 100% coverage for fwf.py."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


class TestFwfCoverageCompletion:
    """Tests to cover remaining edge cases and achieve 100% coverage."""

    def test_decimal_type_without_params(self):
        """Test decimal type conversion without parameters."""
        handler = FwfInputHandler(FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="decimal")
        ]))

        # Test the default decimal case without parentheses
        arrow_type = handler._get_arrow_type("decimal")
        assert isinstance(arrow_type, pa.Decimal128Type)
        assert arrow_type.precision == 10
        assert arrow_type.scale == 2

    def test_decimal_type_with_precision_only(self):
        """Test decimal type with precision only (no scale)."""
        handler = FwfInputHandler(FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="decimal128(15)")
        ]))

        # Test decimal with precision only
        arrow_type = handler._get_arrow_type("decimal128(15)")
        assert isinstance(arrow_type, pa.Decimal128Type)
        assert arrow_type.precision == 15
        assert arrow_type.scale == 2  # Default scale

    def test_conditional_schema_arrow_generation_edge_cases(self):
        """Test arrow schema generation with conditional schemas edge cases."""
        # Test with conditional schemas but no flag column (should be invalid but test the logic)
        flag_column = FwfFieldSpec("record_type", 1, 1, parquet_type="string")

        conditional_schemas = [
            FwfConditionalSchema("A", "Schema A", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("field1", 2, 10, parquet_type="string")
            ]),
            FwfConditionalSchema("B", "Schema B", [
                FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                FwfFieldSpec("field2", 2, 10, parquet_type="string"),
                FwfFieldSpec("field1", 12, 5, parquet_type="int64")  # Same name, different type
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )
        handler = FwfInputHandler(config)

        # Generate schema and verify unique field collection
        schema = handler.get_arrow_schema()
        field_names = [field.name for field in schema]

        # Should include flag column and unique fields from all schemas
        assert "record_type" in field_names
        assert "field1" in field_names
        assert "field2" in field_names
        assert "__line_number__" in field_names
        assert "__source_file__" in field_names

    def test_mixed_simple_and_conditional_fields(self):
        """Test schema generation with both simple fields and conditional schemas."""
        simple_fields = [
            FwfFieldSpec("global_field", 50, 10, parquet_type="string")
        ]

        flag_column = FwfFieldSpec("type", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema("X", "Schema X", [
                FwfFieldSpec("type", 1, 1, parquet_type="string"),
                FwfFieldSpec("conditional_field", 2, 10, parquet_type="string")
            ])
        ]

        config = FwfInputConfig(
            fields=simple_fields,
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )
        handler = FwfInputHandler(config)

        schema = handler.get_arrow_schema()
        field_names = [field.name for field in schema]

        # Should include simple fields, flag column, and conditional fields
        assert "global_field" in field_names
        assert "type" in field_names
        assert "conditional_field" in field_names

    def test_process_null_values_no_config_edge_cases(self):
        """Test null value processing edge cases."""
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="string")
        ])
        handler = FwfInputHandler(config)

        # Test with no null_values config - empty string should return None
        assert handler.process_null_values("", "test") is None
        assert handler.process_null_values("value", "test") == "value"

    def test_process_null_values_with_config(self):
        """Test null value processing with various configurations."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")],
            null_values={
                "global": ["NULL", ""],
                "perColumn": {
                    "test": ["MISSING", "N/A"]
                }
            }
        )
        handler = FwfInputHandler(config)

        # Test global null values
        assert handler.process_null_values("NULL", "test") is None
        assert handler.process_null_values("", "test") is None

        # Test per-column null values
        assert handler.process_null_values("MISSING", "test") is None
        assert handler.process_null_values("N/A", "test") is None

        # Test value not in null list
        assert handler.process_null_values("VALID", "test") == "VALID"

        # Test per-column null for different field
        assert handler.process_null_values("MISSING", "other_field") == "MISSING"

    def test_extract_field_value_edge_cases(self):
        """Test field extraction edge cases."""
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, align="right", pad="0", trim=True)
        ])
        handler = FwfInputHandler(config)

        # Test field that starts beyond line length
        field_beyond = FwfFieldSpec("beyond", 20, 5, parquet_type="string")
        result = handler.extract_field_value("short", field_beyond)
        assert result == ""

        # Test right-aligned field with all padding characters
        field_right_pad = FwfFieldSpec("right", 1, 5, align="right", pad="0", trim=True)
        result = handler.extract_field_value("00000", field_right_pad)
        assert result == "0"  # Should preserve one pad character

    def test_create_arrow_table_with_conditional_schema(self):
        """Test arrow table creation with conditional schemas."""
        flag_column = FwfFieldSpec("type", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema("H", "Header", [
                FwfFieldSpec("type", 1, 1, parquet_type="string"),
                FwfFieldSpec("header_field", 2, 10, parquet_type="string")
            ]),
            FwfConditionalSchema("D", "Detail", [
                FwfFieldSpec("type", 1, 1, parquet_type="string"),
                FwfFieldSpec("detail_field", 2, 10, parquet_type="int64")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )
        handler = FwfInputHandler(config)

        # Create test data
        test_data = [
            "Hheader_val",
            "D000000123"
        ]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            for line in test_data:
                f.write(line + '\n')
            temp_path = Path(f.name)

        try:
            table = handler.create_arrow_table(temp_path)

            # Verify table structure
            assert table.num_rows == 2
            schema_field_names = [field.name for field in table.schema]
            assert "type" in schema_field_names
            assert "header_field" in schema_field_names
            assert "detail_field" in schema_field_names
            assert "__line_number__" in schema_field_names
            assert "__source_file__" in schema_field_names
        finally:
            temp_path.unlink()

    def test_type_conversion_edge_cases(self):
        """Test type conversion edge cases."""
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="string")
        ])
        handler = FwfInputHandler(config)

        # Test conversion with empty string
        assert handler.convert_value("", "int64") is None
        assert handler.convert_value("", "float64") is None
        assert handler.convert_value("", "bool") is None

        # Test invalid conversions that should return original value
        assert handler.convert_value("invalid", "int64") == "invalid"
        assert handler.convert_value("invalid", "float64") == "invalid"

    def test_parse_line_with_footer_detection(self):
        """Test parse_line with footer row detection."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")],
            footer_detection={
                "mode": "regex",
                "pattern": r"^FOOTER.*"
            }
        )
        handler = FwfInputHandler(config)

        # Test footer row should return None
        result = handler.parse_line("FOOTER: summary info")
        assert result is None

        # Test non-footer row should parse normally
        result = handler.parse_line("DATA ")
        assert result is not None
        assert result["test"] == "DATA"

    def test_is_footer_row_no_pattern(self):
        """Test footer detection with mode but no pattern."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")],
            footer_detection={
                "mode": "regex"
                # No pattern specified
            }
        )
        handler = FwfInputHandler(config)

        # Should return False when pattern is missing
        assert handler.is_footer_row("FOOTER: test") is False

    def test_create_arrow_table_fallback_behavior(self):
        """Test arrow table creation with type conversion fallbacks."""
        config = FwfInputConfig(fields=[
            FwfFieldSpec("problematic", 1, 10, parquet_type="int64")
        ])
        handler = FwfInputHandler(config)

        # Create data that will cause type conversion issues
        test_data = "text_value"

        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write(test_data + '\n')
            temp_path = Path(f.name)

        try:
            # Mock the pa.array function to force an exception for testing fallback
            original_array = pa.array

            def mock_array_side_effect(*args, **kwargs):
                # Check if this is the first call with int64 type
                if kwargs.get('type') == pa.int64():
                    raise Exception("Simulated conversion error")
                else:
                    # Use original function for other calls
                    return original_array(*args, **kwargs)

            with patch('pyarrow.array', side_effect=mock_array_side_effect):
                table = handler.create_arrow_table(temp_path)

                # Should still create table with fallback behavior
                assert table.num_rows == 1
        finally:
            temp_path.unlink()

    def test_encoding_detection_import_error(self):
        """Test encoding detection when chardet import fails."""
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="string")
        ])
        handler = FwfInputHandler(config)

        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test data')
            temp_path = Path(f.name)

        try:
            # Mock import error for chardet
            with patch('builtins.__import__') as mock_import:
                def import_side_effect(name, *args, **kwargs):
                    if name == 'chardet':
                        raise ImportError("chardet not available")
                    return __import__(name, *args, **kwargs)

                mock_import.side_effect = import_side_effect

                # Should fall back to utf-8
                encoding = handler.detect_encoding(temp_path)
                assert encoding == 'utf-8'
        finally:
            temp_path.unlink()
