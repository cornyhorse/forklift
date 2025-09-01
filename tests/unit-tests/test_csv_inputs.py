"""Comprehensive tests for the CSV inputs module to improve code coverage."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import csv

from forklift.inputs.csv import CsvInputHandler
from forklift.inputs.config import CsvInputConfig


class TestCsvInputConfig:
    """Test the CsvInputConfig dataclass."""

    def test_csv_input_config_defaults(self):
        """Test CsvInputConfig with default values."""
        config = CsvInputConfig()

        assert config.delimiter == ","
        assert config.quote_char == '"'
        assert config.escape_char is None
        assert config.encoding == "utf-8"
        assert config.header_mode == "present"
        assert config.header_search_rows == 10
        assert config.skip_blank_lines is True
        assert config.comment_patterns is None
        assert config.footer_detection is None

    def test_csv_input_config_custom_values(self):
        """Test CsvInputConfig with custom values."""
        config = CsvInputConfig(
            delimiter=";",
            quote_char="'",
            escape_char="\\",
            encoding="latin-1",
            header_mode="auto",
            header_search_rows=5,
            skip_blank_lines=False,
            comment_patterns=["^#", "^//"],
            footer_detection={"mode": "regex", "pattern": "^TOTAL"}
        )

        assert config.delimiter == ";"
        assert config.quote_char == "'"
        assert config.escape_char == "\\"
        assert config.encoding == "latin-1"
        assert config.header_mode == "auto"
        assert config.header_search_rows == 5
        assert config.skip_blank_lines is False
        assert config.comment_patterns == ["^#", "^//"]
        assert config.footer_detection == {"mode": "regex", "pattern": "^TOTAL"}


class TestCsvInputHandler:
    """Test the CsvInputHandler class."""

    def test_csv_input_handler_initialization(self):
        """Test CsvInputHandler initialization."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        assert handler.config == config

    def test_detect_encoding_success(self):
        """Test encoding detection with a valid file."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Create a temporary file with UTF-8 content
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("name,age,email\n")
            f.write("Alice,25,alice@example.com\n")
            f.write("Bob,30,bob@example.com\n")
            temp_path = Path(f.name)

        try:
            encoding = handler.detect_encoding(temp_path)
            # Should detect UTF-8 or similar
            assert encoding in ['utf-8', 'ascii', 'UTF-8', 'ASCII']
        finally:
            temp_path.unlink()

    def test_detect_encoding_fallback(self):
        """Test encoding detection with chardet returning None."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("test content")
            temp_path = Path(f.name)

        try:
            # Mock chardet to return None encoding
            with patch('chardet.detect') as mock_detect:
                mock_detect.return_value = {}  # Empty dict, no 'encoding' key
                encoding = handler.detect_encoding(temp_path)
                assert encoding == 'utf-8'  # Should fallback to utf-8
        finally:
            temp_path.unlink()

    def test_find_header_row_simple(self):
        """Test finding header row in a simple CSV."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("name,age,email\n")
            f.write("Alice,25,alice@example.com\n")
            f.write("Bob,30,bob@example.com\n")
            temp_path = Path(f.name)

        try:
            header_row, column_names = handler.find_header_row(temp_path)
            assert header_row == 0
            assert column_names == ['name', 'age', 'email']
        finally:
            temp_path.unlink()

    def test_find_header_row_with_comments(self):
        """Test finding header row with comment rows."""
        config = CsvInputConfig(comment_patterns=["^#", "^//"])
        handler = CsvInputHandler(config)

        # Create a temporary CSV file with comments
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("# This is a comment\n")
            f.write("// Another comment\n")
            f.write("name,age,email\n")
            f.write("Alice,25,alice@example.com\n")
            temp_path = Path(f.name)

        try:
            header_row, column_names = handler.find_header_row(temp_path)
            assert header_row == 2
            assert column_names == ['name', 'age', 'email']
        finally:
            temp_path.unlink()

    def test_find_header_row_with_blank_lines(self):
        """Test finding header row with blank lines."""
        config = CsvInputConfig(skip_blank_lines=True)
        handler = CsvInputHandler(config)

        # Create a temporary CSV file with blank lines
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("\n")
            f.write("   \n")  # Blank line with spaces
            f.write("name,age,email\n")
            f.write("Alice,25,alice@example.com\n")
            temp_path = Path(f.name)

        try:
            header_row, column_names = handler.find_header_row(temp_path)
            assert header_row == 2
            assert column_names == ['name', 'age', 'email']
        finally:
            temp_path.unlink()

    def test_find_header_row_no_skip_blank_lines(self):
        """Test finding header row without skipping blank lines."""
        config = CsvInputConfig(skip_blank_lines=False)
        handler = CsvInputHandler(config)

        # Create a temporary CSV file with blank lines
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("\n")
            f.write("name,age,email\n")
            temp_path = Path(f.name)

        try:
            header_row, column_names = handler.find_header_row(temp_path)
            assert header_row == 0
            assert column_names == []  # Empty line produces empty list
        finally:
            temp_path.unlink()

    def test_find_header_row_search_limit(self):
        """Test header search with limited search rows."""
        config = CsvInputConfig(header_search_rows=2)
        handler = CsvInputHandler(config)

        # Create a temporary CSV file where header is beyond search limit
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("row1\n")
            f.write("row2\n")
            f.write("name,age,email\n")  # This won't be found due to search limit
            temp_path = Path(f.name)

        try:
            header_row, column_names = handler.find_header_row(temp_path)
            assert header_row == 0
            assert column_names == ['row1']  # First row becomes header
        finally:
            temp_path.unlink()

    def test_find_header_row_no_valid_header(self):
        """Test exception when no valid header row is found."""
        config = CsvInputConfig(header_search_rows=1, comment_patterns=["^.*"])  # All rows are comments
        handler = CsvInputHandler(config)

        # Create a temporary CSV file where all rows are comments
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("# comment row\n")
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="No valid header row found"):
                handler.find_header_row(temp_path)
        finally:
            temp_path.unlink()

    def test_is_comment_row_no_patterns(self):
        """Test comment row detection with no patterns configured."""
        config = CsvInputConfig(comment_patterns=None)
        handler = CsvInputHandler(config)

        result = handler._is_comment_row(["# comment", "data"])
        assert result is False

    def test_is_comment_row_empty_row(self):
        """Test comment row detection with empty row."""
        config = CsvInputConfig(comment_patterns=["^#"])
        handler = CsvInputHandler(config)

        result = handler._is_comment_row([])
        assert result is False

    def test_is_comment_row_match(self):
        """Test comment row detection with matching pattern."""
        config = CsvInputConfig(comment_patterns=["^#", "^//"])
        handler = CsvInputHandler(config)

        # Test matching patterns
        assert handler._is_comment_row(["# This is a comment"]) is True
        assert handler._is_comment_row(["// Another comment"]) is True
        # Note: whitespace is stripped, so "  # comment" becomes "# comment" which matches ^#
        assert handler._is_comment_row(["  # Indented comment"]) is True  # Matches ^# after strip
        assert handler._is_comment_row(["normal,data,row"]) is False

    def test_is_comment_row_whitespace_handling(self):
        """Test comment row detection with whitespace handling."""
        config = CsvInputConfig(comment_patterns=["^#"])
        handler = CsvInputHandler(config)

        # Test whitespace is stripped from first cell
        assert handler._is_comment_row(["  # comment  "]) is True
        assert handler._is_comment_row(["\t#comment\t"]) is True

    def test_create_arrow_reader(self):
        """Test creating PyArrow CSV reader."""
        config = CsvInputConfig(
            delimiter=",",
            quote_char='"',
            escape_char="\\",
            encoding="utf-8"
        )
        handler = CsvInputHandler(config)

        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("name,age,email\n")
            f.write("Alice,25,alice@example.com\n")
            f.write("Bob,30,bob@example.com\n")
            temp_path = Path(f.name)

        try:
            column_names = ['name', 'age', 'email']
            reader = handler.create_arrow_reader(temp_path, column_names, skip_rows=1)

            # Verify reader is created and can read data
            assert reader is not None
            batch = reader.read_next_batch()
            assert batch.num_columns == 3
            assert batch.num_rows == 2  # Two data rows after header
        finally:
            temp_path.unlink()

    def test_create_arrow_reader_with_skip_rows(self):
        """Test creating PyArrow CSV reader with skip_rows parameter."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("# Comment line\n")
            f.write("name,age,email\n")
            f.write("Alice,25,alice@example.com\n")
            temp_path = Path(f.name)

        try:
            column_names = ['name', 'age', 'email']
            reader = handler.create_arrow_reader(temp_path, column_names, skip_rows=2)

            # Should skip the comment and header lines
            assert reader is not None
            batch = reader.read_next_batch()
            assert batch.num_columns == 3
            assert batch.num_rows == 1  # Only one data row after skipping
        finally:
            temp_path.unlink()

    def test_create_arrow_reader_custom_config(self):
        """Test creating PyArrow CSV reader with custom configuration."""
        config = CsvInputConfig(
            delimiter=";",
            quote_char="'",
            escape_char="/",
            encoding="latin-1"
        )
        handler = CsvInputHandler(config)

        # Create a temporary CSV file with custom format
        with tempfile.NamedTemporaryFile(mode='w', encoding='latin-1', delete=False, suffix='.csv') as f:
            f.write("name;age;email\n")
            f.write("'Alice';25;'alice@example.com'\n")
            temp_path = Path(f.name)

        try:
            column_names = ['name', 'age', 'email']
            reader = handler.create_arrow_reader(temp_path, column_names, skip_rows=1)

            assert reader is not None
            batch = reader.read_next_batch()
            assert batch.num_columns == 3
            assert batch.num_rows == 1
        finally:
            temp_path.unlink()


class TestCsvInputHandlerEdgeCases:
    """Test edge cases and error conditions."""

    def test_find_header_row_empty_file(self):
        """Test finding header row in an empty file."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Create an empty temporary file
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="No valid header row found"):
                handler.find_header_row(temp_path)
        finally:
            temp_path.unlink()

    def test_find_header_with_different_delimiter(self):
        """Test finding header with different delimiter configuration."""
        config = CsvInputConfig(delimiter=";")
        handler = CsvInputHandler(config)

        # Create a temporary CSV file with semicolon delimiter
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("name;age;email\n")
            f.write("Alice;25;alice@example.com\n")
            temp_path = Path(f.name)

        try:
            header_row, column_names = handler.find_header_row(temp_path)
            assert header_row == 0
            assert column_names == ['name', 'age', 'email']
        finally:
            temp_path.unlink()

    def test_comment_patterns_multiple_matches(self):
        """Test comment detection with multiple patterns."""
        config = CsvInputConfig(comment_patterns=["^#", "^%", "^//"])
        handler = CsvInputHandler(config)

        # Test various comment patterns
        assert handler._is_comment_row(["# Hash comment"]) is True
        assert handler._is_comment_row(["% Percent comment"]) is True
        assert handler._is_comment_row(["// Slash comment"]) is True
        assert handler._is_comment_row(["Regular data"]) is False

    def test_header_with_whitespace_columns(self):
        """Test header detection with whitespace in column names."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Create a temporary CSV file with whitespace in headers
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("  name  , age ,  email  \n")
            f.write("Alice,25,alice@example.com\n")
            temp_path = Path(f.name)

        try:
            header_row, column_names = handler.find_header_row(temp_path)
            assert header_row == 0
            assert column_names == ['name', 'age', 'email']  # Whitespace should be stripped
        finally:
            temp_path.unlink()

    def test_encoding_detection_with_special_characters(self):
        """Test encoding detection with special characters."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Create a temporary file with special characters
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.csv') as f:
            f.write("name,description\n")
            f.write("Café,Français\n")
            f.write("Naïve,Résumé\n")
            temp_path = Path(f.name)

        try:
            encoding = handler.detect_encoding(temp_path)
            # Should successfully detect an encoding
            assert encoding is not None
            assert isinstance(encoding, str)
        finally:
            temp_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__])
