"""Comprehensive tests for the FWF inputs module."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pyarrow as pa

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema
from forklift.inputs.fwf_utils import create_fwf_config_from_schema, create_simple_fwf_config


class TestFwfFieldSpec:
    """Test the FwfFieldSpec dataclass."""

    def test_fwf_field_spec_defaults(self):
        """Test FwfFieldSpec with default values."""
        field = FwfFieldSpec(name="test_field", start=1, length=10)

        assert field.name == "test_field"
        assert field.start == 1
        assert field.length == 10
        assert field.align == "left"
        assert field.pad == " "
        assert field.parquet_type == "string"
        assert field.required is False
        assert field.trim is True

    def test_fwf_field_spec_custom_values(self):
        """Test FwfFieldSpec with custom values."""
        field = FwfFieldSpec(
            name="id",
            start=5,
            length=8,
            align="right",
            pad="0",
            parquet_type="int64",
            required=True,
            trim=False
        )

        assert field.name == "id"
        assert field.start == 5
        assert field.length == 8
        assert field.align == "right"
        assert field.pad == "0"
        assert field.parquet_type == "int64"
        assert field.required is True
        assert field.trim is False


class TestFwfConditionalSchema:
    """Test the FwfConditionalSchema dataclass."""

    def test_fwf_conditional_schema(self):
        """Test FwfConditionalSchema creation."""
        fields = [
            FwfFieldSpec("id", 1, 5),
            FwfFieldSpec("name", 6, 20)
        ]
        schema = FwfConditionalSchema(
            flag_value="A",
            description="Schema A",
            fields=fields
        )

        assert schema.flag_value == "A"
        assert schema.description == "Schema A"
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"


class TestFwfInputConfig:
    """Test the FwfInputConfig dataclass."""

    def test_fwf_input_config_defaults(self):
        """Test FwfInputConfig with default values."""
        config = FwfInputConfig()

        assert config.encoding == "utf-8"
        assert config.fields is None
        assert config.conditional_schemas is None
        assert config.flag_column is None
        assert config.trim_whitespace is True
        assert config.skip_blank_lines is True
        assert config.comment_patterns is None
        assert config.footer_detection is None
        assert config.null_values is None

    def test_fwf_input_config_with_fields(self):
        """Test FwfInputConfig with field specifications."""
        fields = [
            FwfFieldSpec("id", 1, 10, parquet_type="int64"),
            FwfFieldSpec("name", 11, 30, parquet_type="string")
        ]
        config = FwfInputConfig(fields=fields)

        assert len(config.fields) == 2
        assert config.fields[0].name == "id"
        assert config.fields[1].name == "name"

    def test_fwf_input_config_conditional(self):
        """Test FwfInputConfig with conditional schemas."""
        flag_column = FwfFieldSpec("type", 1, 1)
        conditional_schemas = [
            FwfConditionalSchema("A", "Type A", [
                FwfFieldSpec("id", 2, 5),
                FwfFieldSpec("data", 7, 10)
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )

        assert config.flag_column.name == "type"
        assert len(config.conditional_schemas) == 1
        assert config.conditional_schemas[0].flag_value == "A"


class TestFwfInputHandler:
    """Test the FwfInputHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.simple_fields = [
            FwfFieldSpec("id", 1, 5, align="right", pad="0", parquet_type="int64"),
            FwfFieldSpec("name", 6, 20, align="left", parquet_type="string"),
            FwfFieldSpec("amount", 26, 10, align="right", parquet_type="decimal128(10,2)")
        ]
        self.simple_config = FwfInputConfig(fields=self.simple_fields)

    def test_fwf_handler_initialization(self):
        """Test FwfInputHandler initialization."""
        handler = FwfInputHandler(self.simple_config)
        assert handler.config == self.simple_config

    def test_fwf_handler_validation_no_config(self):
        """Test validation fails when no fields or conditional schemas provided."""
        config = FwfInputConfig()
        with pytest.raises(ValueError, match="Either fields or conditional_schemas must be specified"):
            FwfInputHandler(config)

    def test_fwf_handler_validation_conditional_no_flag(self):
        """Test validation fails when conditional schemas provided without flag column."""
        conditional_schemas = [
            FwfConditionalSchema("A", "Type A", [FwfFieldSpec("id", 1, 5)])
        ]
        config = FwfInputConfig(conditional_schemas=conditional_schemas)

        with pytest.raises(ValueError, match="Flag column must be specified when using conditional schemas"):
            FwfInputHandler(config)

    def test_field_overlap_validation(self):
        """Test validation of overlapping fields."""
        overlapping_fields = [
            FwfFieldSpec("field1", 1, 10),
            FwfFieldSpec("field2", 5, 10)  # Overlaps with field1
        ]
        config = FwfInputConfig(fields=overlapping_fields)

        with pytest.raises(ValueError, match="overlaps with"):
            FwfInputHandler(config)

    def test_extract_field_value_basic(self):
        """Test basic field value extraction."""
        handler = FwfInputHandler(self.simple_config)
        # Properly aligned test data: ID(1-5) + Name(6-25) + Amount(26-35)
        line = "00123John Doe               1234.56"

        # Test ID field (positions 1-5, right-aligned, zero-padded)
        id_field = self.simple_fields[0]
        id_value = handler.extract_field_value(line, id_field)
        assert id_value == "123"  # Should strip leading zeros

        # Test name field (positions 6-25, left-aligned)
        name_field = self.simple_fields[1]
        name_value = handler.extract_field_value(line, name_field)
        assert name_value == "John Doe"

        # Test amount field (positions 26-35, right-aligned)
        amount_field = self.simple_fields[2]
        amount_value = handler.extract_field_value(line, amount_field)
        assert amount_value == "1234.56"

    def test_extract_field_value_short_line(self):
        """Test field extraction from short lines."""
        handler = FwfInputHandler(self.simple_config)
        short_line = "00123John"  # Only covers first two fields partially

        # Test ID field - should work fine
        id_value = handler.extract_field_value(short_line, self.simple_fields[0])
        assert id_value == "123"

        # Test name field - should be truncated
        name_value = handler.extract_field_value(short_line, self.simple_fields[1])
        assert name_value == "John"

        # Test amount field - should be empty and padded
        amount_value = handler.extract_field_value(short_line, self.simple_fields[2])
        assert amount_value == ""

    def test_extract_field_value_alignment(self):
        """Test field alignment and padding."""
        handler = FwfInputHandler(self.simple_config)

        # Test right alignment with zero padding
        right_field = FwfFieldSpec("test", 1, 5, align="right", pad="0")
        value = handler.extract_field_value("123  ", right_field)
        assert value == "123"

        # Test center alignment
        center_field = FwfFieldSpec("test", 1, 5, align="center", pad=" ")
        value = handler.extract_field_value("123  ", center_field)
        assert value == "123"

    def test_parse_line_simple(self):
        """Test parsing a simple line."""
        handler = FwfInputHandler(self.simple_config)
        # Properly aligned test data: ID(1-5) + Name(6-25) + Amount(26-35)
        line = "00123John Doe               1234.56"

        result = handler.parse_line(line)

        assert result is not None
        assert result["id"] == 123  # Should be integer since parquet_type="int64"
        assert result["name"] == "John Doe"
        assert result["amount"] == 1234.56  # Should be float since parquet_type="decimal128(10,2)"

    def test_parse_line_blank(self):
        """Test parsing blank lines."""
        handler = FwfInputHandler(self.simple_config)

        # Should return None for blank lines when skip_blank_lines is True
        result = handler.parse_line("")
        assert result is None

        result = handler.parse_line("   ")
        assert result is None

    def test_parse_line_comment(self):
        """Test parsing comment lines."""
        config = FwfInputConfig(
            fields=self.simple_fields,
            comment_patterns=["^#", "^//"]
        )
        handler = FwfInputHandler(config)

        # Should return None for comment lines
        result = handler.parse_line("# This is a comment")
        assert result is None

        result = handler.parse_line("// Another comment")
        assert result is None

    def test_null_value_processing(self):
        """Test null value processing."""
        config = FwfInputConfig(
            fields=self.simple_fields,
            null_values={
                "global": ["", "NULL", "N/A"],
                "perColumn": {
                    "name": ["UNKNOWN"]
                }
            }
        )
        handler = FwfInputHandler(config)

        # Test global null values
        assert handler.process_null_values("NULL", "id") is None
        assert handler.process_null_values("N/A", "name") is None

        # Test per-column null values
        assert handler.process_null_values("UNKNOWN", "name") is None
        assert handler.process_null_values("UNKNOWN", "id") == "UNKNOWN"  # Not null for id field

        # Test normal values
        assert handler.process_null_values("123", "id") == "123"

    def test_conditional_schema_detection(self):
        """Test conditional schema detection."""
        flag_column = FwfFieldSpec("type", 1, 1)
        conditional_schemas = [
            FwfConditionalSchema("A", "Type A", [
                FwfFieldSpec("type", 1, 1),
                FwfFieldSpec("id", 2, 5),
                FwfFieldSpec("data_a", 7, 10)
            ]),
            FwfConditionalSchema("B", "Type B", [
                FwfFieldSpec("type", 1, 1),
                FwfFieldSpec("id", 2, 3),
                FwfFieldSpec("data_b", 5, 8)
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )
        handler = FwfInputHandler(config)

        # Test schema A detection
        schema_a = handler.determine_schema("A12345data_here")
        assert schema_a is not None
        assert schema_a.flag_value == "A"

        # Test schema B detection
        schema_b = handler.determine_schema("B123data_b_here")
        assert schema_b is not None
        assert schema_b.flag_value == "B"

        # Test no match
        schema_none = handler.determine_schema("C123unknown")
        assert schema_none is None

    def test_conditional_schema_parsing(self):
        """Test parsing with conditional schemas."""
        flag_column = FwfFieldSpec("type", 1, 1)
        conditional_schemas = [
            FwfConditionalSchema("A", "Type A", [
                FwfFieldSpec("type", 1, 1),
                FwfFieldSpec("id", 2, 5, parquet_type="int64"),
                FwfFieldSpec("name", 7, 15, parquet_type="string")
            ]),
            FwfConditionalSchema("B", "Type B", [
                FwfFieldSpec("type", 1, 1),
                FwfFieldSpec("id", 2, 3, parquet_type="int64"),
                FwfFieldSpec("amount", 5, 10, parquet_type="decimal128(10,2)")  # Increased length
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )
        handler = FwfInputHandler(config)

        # Test parsing type A record
        result_a = handler.parse_line("A12345John Doe    ")
        assert result_a is not None
        assert result_a["type"] == "A"
        assert result_a["id"] == 12345  # Should be integer since parquet_type="int64"
        assert result_a["name"] == "John Doe"

        # Test parsing type B record - ensure amount field has enough characters
        result_b = handler.parse_line("B123  1234.56   ")
        assert result_b is not None
        assert result_b["type"] == "B"
        assert result_b["id"] == 123  # Should be integer since parquet_type="int64"
        assert result_b["amount"] == 1234.56  # Should be float since parquet_type="decimal128(10,2)"

        # Test unknown type (should return None)
        result_unknown = handler.parse_line("C123unknown")
        assert result_unknown is None

    def test_arrow_type_conversion(self):
        """Test PyArrow type conversion."""
        handler = FwfInputHandler(self.simple_config)

        # Test basic types
        assert handler._get_arrow_type("int64") == pa.int64()
        assert handler._get_arrow_type("string") == pa.string()
        assert handler._get_arrow_type("bool") == pa.bool_()
        assert handler._get_arrow_type("float32") == pa.float32()

        # Test decimal types
        decimal_type = handler._get_arrow_type("decimal128(10,2)")
        assert isinstance(decimal_type, pa.Decimal128Type)
        assert decimal_type.precision == 10
        assert decimal_type.scale == 2

        # Test list types
        list_type = handler._get_arrow_type("list<string>")
        assert isinstance(list_type, pa.ListType)

        # Test unknown type (should default to string)
        unknown_type = handler._get_arrow_type("unknown_type")
        assert unknown_type == pa.string()

    def test_arrow_schema_generation(self):
        """Test PyArrow schema generation."""
        handler = FwfInputHandler(self.simple_config)
        schema = handler.get_arrow_schema()

        # Check field count (3 data fields + 2 metadata fields)
        assert len(schema) == 5

        # Check data fields
        assert schema.field("id").type == pa.int64()
        assert schema.field("name").type == pa.string()
        assert schema.field("amount").type == pa.decimal128(10, 2)

        # Check metadata fields
        assert schema.field("__line_number__").type == pa.int64()
        assert schema.field("__source_file__").type == pa.string()

    @patch('chardet.detect')
    def test_encoding_detection(self, mock_detect):
        """Test encoding detection."""
        mock_detect.return_value = {'encoding': 'latin-1'}

        handler = FwfInputHandler(self.simple_config)

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test data')
            temp_path = Path(f.name)

        try:
            encoding = handler.detect_encoding(temp_path)
            assert encoding == 'latin-1'
            mock_detect.assert_called_once()
        finally:
            temp_path.unlink()

    def test_encoding_detection_no_chardet(self):
        """Test encoding detection when chardet is not available."""
        handler = FwfInputHandler(self.simple_config)

        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test data')
            temp_path = Path(f.name)

        try:
            # Mock ImportError when trying to import chardet
            with patch('builtins.__import__', side_effect=lambda name, *args: exec('raise ImportError()') if name == 'chardet' else __import__(name, *args)):
                # Should fall back to utf-8
                encoding = handler.detect_encoding(temp_path)
                assert encoding == 'utf-8'
        finally:
            temp_path.unlink()

    def test_read_file_integration(self):
        """Test full file reading integration."""
        # Create test data with proper field alignment: ID(1-5) + Name(6-25) + Amount(26-35)
        test_data = [
            "00123John Doe               1234.56",
            "00456Jane Smith             5678.90",
            "",  # Blank line (should be skipped)
            "00789Bob Johnson            9876.54"
        ]

        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            for line in test_data:
                f.write(line + '\n')
            temp_path = Path(f.name)

        try:
            handler = FwfInputHandler(self.simple_config)
            results = list(handler.read_file(temp_path))

            # Should have 3 records (blank line skipped)
            assert len(results) == 3

            # Check first record
            assert results[0]["id"] == 123  # Should be integer since parquet_type="int64"
            assert results[0]["name"] == "John Doe"
            assert results[0]["amount"] == 1234.56  # Should be float since parquet_type="decimal128(10,2)"
            assert results[0]["__line_number__"] == 1

            # Check second record
            assert results[1]["id"] == 456  # Should be integer since parquet_type="int64"
            assert results[1]["name"] == "Jane Smith"
            assert results[1]["amount"] == 5678.90  # Should be float since parquet_type="decimal128(10,2)"
            assert results[1]["__line_number__"] == 2

            # Check third record (line 4 due to skipped blank line)
            assert results[2]["id"] == 789  # Should be integer since parquet_type="int64"
            assert results[2]["name"] == "Bob Johnson"
            assert results[2]["amount"] == 9876.54  # Should be float since parquet_type="decimal128(10,2)"
            assert results[2]["__line_number__"] == 4

        finally:
            temp_path.unlink()

    def test_create_arrow_table(self):
        """Test PyArrow table creation."""
        # Create test data with proper field alignment: ID(1-5) + Name(6-25) + Amount(26-35)
        test_data = [
            "00123John Doe               1234.56",
            "00456Jane Smith             5678.90"
        ]

        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            for line in test_data:
                f.write(line + '\n')
            temp_path = Path(f.name)

        try:
            handler = FwfInputHandler(self.simple_config)
            table = handler.create_arrow_table(temp_path)

            # Check table structure
            assert isinstance(table, pa.Table)
            assert table.num_rows == 2
            assert table.num_columns == 5  # 3 data + 2 metadata

            # Convert to pandas for easier testing
            df = table.to_pandas()
            assert df.iloc[0]['id'] == 123  # Should be converted to int
            assert df.iloc[0]['name'] == 'John Doe'
            assert df.iloc[1]['id'] == 456
            assert df.iloc[1]['name'] == 'Jane Smith'

        finally:
            temp_path.unlink()

    def test_empty_file_handling(self):
        """Test handling of empty files."""
        # Create empty file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            temp_path = Path(f.name)

        try:
            handler = FwfInputHandler(self.simple_config)
            table = handler.create_arrow_table(temp_path)

            # Should return empty table with correct schema
            assert isinstance(table, pa.Table)
            assert table.num_rows == 0
            assert table.num_columns == 5  # Still has the schema columns

        finally:
            temp_path.unlink()

    def test_file_not_found(self):
        """Test handling of non-existent files."""
        handler = FwfInputHandler(self.simple_config)
        non_existent_path = Path("/non/existent/file.fwf")

        with pytest.raises(FileNotFoundError):
            list(handler.read_file(non_existent_path))

    def test_footer_detection(self):
        """Test footer detection functionality."""
        config = FwfInputConfig(
            fields=self.simple_fields,
            footer_detection={
                "mode": "regex",
                "pattern": "^TOTAL"
            }
        )
        handler = FwfInputHandler(config)

        # Test footer detection
        assert handler.is_footer_row("TOTAL: 12345") is True
        assert handler.is_footer_row("SUMMARY: Complete") is False
        assert handler.is_footer_row("00123John Doe") is False

        # Test with no footer config
        no_footer_config = FwfInputConfig(fields=self.simple_fields)
        no_footer_handler = FwfInputHandler(no_footer_config)
        assert no_footer_handler.is_footer_row("TOTAL: 12345") is False
