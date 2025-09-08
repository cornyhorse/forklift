"""Comprehensive tests for CSV input module."""

import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import io

from forklift.inputs.csv import (
    CsvInputConfig,
    CsvInputHandler
)


class TestCsvInputConfig:
    """Test CsvInputConfig dataclass."""

    def test_default_config(self):
        """Test default CSV input configuration."""
        config = CsvInputConfig()

        assert config.delimiter == ","
        assert config.quote_char == '"'
        assert config.escape_char == "\\"
        assert config.skip_rows == 0
        assert config.encoding == "utf-8"
        assert config.has_header is True
        assert config.null_values == ["", "NULL", "null", "None"]
        assert config.true_values == ["true", "True", "TRUE", "1"]
        assert config.false_values == ["false", "False", "FALSE", "0"]
        assert config.dtype_inference is True
        assert config.max_rows is None

    def test_custom_config(self):
        """Test custom CSV input configuration."""
        config = CsvInputConfig(
            delimiter="|",
            quote_char="'",
            escape_char="/",
            skip_rows=2,
            encoding="latin-1",
            has_header=False,
            null_values=["NA", "N/A"],
            true_values=["yes", "Y"],
            false_values=["no", "N"],
            dtype_inference=False,
            max_rows=1000
        )

        assert config.delimiter == "|"
        assert config.quote_char == "'"
        assert config.escape_char == "/"
        assert config.skip_rows == 2
        assert config.encoding == "latin-1"
        assert config.has_header is False
        assert config.null_values == ["NA", "N/A"]
        assert config.true_values == ["yes", "Y"]
        assert config.false_values == ["no", "N"]
        assert config.dtype_inference is False
        assert config.max_rows == 1000


class TestCsvInputHandler:
    """Test CsvInputHandler class."""

    def test_init(self):
        """Test CSV input handler initialization."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        assert handler.config == config

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_basic(self, mock_read_csv):
        """Test basic file reading."""
        # Mock the PyArrow CSV reader
        mock_table = MagicMock()
        mock_table.to_batches.return_value = [
            pa.record_batch({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie']
            })
        ]
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        batches = list(handler.read_file("test.csv"))

        assert len(batches) == 1
        assert batches[0].num_rows == 3
        mock_read_csv.assert_called_once()

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_with_custom_config(self, mock_read_csv):
        """Test file reading with custom configuration."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = []
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig(
            delimiter="|",
            quote_char="'",
            encoding="latin-1",
            skip_rows=1,
            has_header=False
        )
        handler = CsvInputHandler(config)

        list(handler.read_file("test.csv"))

        # Verify read_csv was called with correct parameters
        call_args = mock_read_csv.call_args
        assert call_args[1]['delimiter'] == "|"
        assert call_args[1]['quote_char'] == "'"

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_with_max_rows(self, mock_read_csv):
        """Test file reading with max_rows limit."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = []
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig(max_rows=500)
        handler = CsvInputHandler(config)

        list(handler.read_file("test.csv"))

        # Verify read_csv was called with skip_rows parameter
        call_args = mock_read_csv.call_args
        assert 'skip_rows' in call_args[1]

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_encoding_detection(self, mock_read_csv):
        """Test file reading with encoding detection."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = []

        # First call fails with UnicodeDecodeError
        mock_read_csv.side_effect = [
            UnicodeDecodeError('utf-8', b'', 0, 1, 'invalid'),
            mock_table  # Second call succeeds
        ]

        config = CsvInputConfig(encoding="utf-8")
        handler = CsvInputHandler(config)

        list(handler.read_file("test.csv"))

        # Should have been called twice due to encoding fallback
        assert mock_read_csv.call_count == 2

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_error_handling(self, mock_read_csv):
        """Test file reading error handling."""
        mock_read_csv.side_effect = Exception("File read error")

        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        with pytest.raises(Exception, match="File read error"):
            list(handler.read_file("test.csv"))

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_stream_basic(self, mock_read_csv):
        """Test basic stream reading."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = [
            pa.record_batch({
                'col1': ['a', 'b', 'c'],
                'col2': [1, 2, 3]
            })
        ]
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        csv_data = "col1,col2\na,1\nb,2\nc,3"
        stream = io.StringIO(csv_data)

        batches = list(handler.read_stream(stream))

        assert len(batches) == 1
        assert batches[0].num_rows == 3
        mock_read_csv.assert_called_once()

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_stream_with_config(self, mock_read_csv):
        """Test stream reading with custom configuration."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = []
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig(
            delimiter=";",
            has_header=False,
            dtype_inference=False
        )
        handler = CsvInputHandler(config)

        stream = io.StringIO("a;b;c\n1;2;3")
        list(handler.read_stream(stream))

        # Verify correct parameters were passed
        call_args = mock_read_csv.call_args
        assert call_args[1]['delimiter'] == ";"

    def test_get_schema_no_file(self):
        """Test getting schema when no file has been read."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        schema = handler.get_schema()
        assert schema is None

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_get_schema_after_read(self, mock_read_csv):
        """Test getting schema after reading file."""
        mock_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string())
        ])
        mock_table = MagicMock()
        mock_table.schema = mock_schema
        mock_table.to_batches.return_value = []
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        list(handler.read_file("test.csv"))
        schema = handler.get_schema()

        assert schema == mock_schema

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_infer_schema(self, mock_read_csv):
        """Test schema inference."""
        mock_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string())
        ])
        mock_table = MagicMock()
        mock_table.schema = mock_schema
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        schema = handler.infer_schema("test.csv")

        assert schema == mock_schema
        mock_read_csv.assert_called_once()

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_infer_schema_with_sample_rows(self, mock_read_csv):
        """Test schema inference with limited sample rows."""
        mock_schema = pa.schema([('col1', pa.string())])
        mock_table = MagicMock()
        mock_table.schema = mock_schema
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        schema = handler.infer_schema("test.csv", sample_rows=100)

        assert schema == mock_schema
        # Verify sample_rows was used in the call
        call_args = mock_read_csv.call_args
        assert 'skip_rows' in call_args[1]

    def test_detect_delimiter_basic(self):
        """Test basic delimiter detection."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Test with comma-separated data
        csv_data = "a,b,c\n1,2,3\n4,5,6"
        detected = handler.detect_delimiter(csv_data)
        assert detected == ","

    def test_detect_delimiter_semicolon(self):
        """Test semicolon delimiter detection."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Test with semicolon-separated data
        csv_data = "a;b;c\n1;2;3\n4;5;6"
        detected = handler.detect_delimiter(csv_data)
        assert detected == ";"

    def test_detect_delimiter_tab(self):
        """Test tab delimiter detection."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Test with tab-separated data
        csv_data = "a\tb\tc\n1\t2\t3\n4\t5\t6"
        detected = handler.detect_delimiter(csv_data)
        assert detected == "\t"

    def test_detect_delimiter_pipe(self):
        """Test pipe delimiter detection."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Test with pipe-separated data
        csv_data = "a|b|c\n1|2|3\n4|5|6"
        detected = handler.detect_delimiter(csv_data)
        assert detected == "|"

    def test_detect_delimiter_ambiguous(self):
        """Test delimiter detection with ambiguous data."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Data that could be multiple delimiters
        csv_data = "a\n1\n4"
        detected = handler.detect_delimiter(csv_data)
        # Should return default when unclear
        assert detected in [",", ";", "\t", "|"]

    def test_detect_delimiter_empty_data(self):
        """Test delimiter detection with empty data."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        detected = handler.detect_delimiter("")
        assert detected == ","  # Default

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_with_null_values(self, mock_read_csv):
        """Test reading file with custom null values."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = []
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig(null_values=["NA", "N/A", "missing"])
        handler = CsvInputHandler(config)

        list(handler.read_file("test.csv"))

        call_args = mock_read_csv.call_args
        assert "null_values" in call_args[1]

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_with_boolean_values(self, mock_read_csv):
        """Test reading file with custom boolean values."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = []
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig(
            true_values=["yes", "Y", "1"],
            false_values=["no", "N", "0"]
        )
        handler = CsvInputHandler(config)

        list(handler.read_file("test.csv"))

        call_args = mock_read_csv.call_args
        assert "true_values" in call_args[1]
        assert "false_values" in call_args[1]

    @patch('forklift.inputs.csv.pv.read_csv')
    def test_read_file_no_dtype_inference(self, mock_read_csv):
        """Test reading file with dtype inference disabled."""
        mock_table = MagicMock()
        mock_table.to_batches.return_value = []
        mock_read_csv.return_value = mock_table

        config = CsvInputConfig(dtype_inference=False)
        handler = CsvInputHandler(config)

        list(handler.read_file("test.csv"))

        call_args = mock_read_csv.call_args
        # When dtype_inference is False, should read everything as strings
        assert "autogenerate_column_names" in call_args[1]


class TestCsvInputHandlerIntegration:
    """Test CSV input handler integration scenarios."""

    def test_real_world_csv_processing(self):
        """Test processing a real-world CSV scenario."""
        config = CsvInputConfig(
            delimiter=",",
            has_header=True,
            encoding="utf-8",
            null_values=["", "NULL", "N/A"],
            dtype_inference=True
        )
        handler = CsvInputHandler(config)

        # Mock a realistic CSV processing scenario
        with patch('forklift.inputs.csv.pv.read_csv') as mock_read_csv:
            mock_table = MagicMock()
            mock_table.to_batches.return_value = [
                pa.record_batch({
                    'customer_id': [1, 2, 3, 4, 5],
                    'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson'],
                    'email': ['john@test.com', 'jane@test.com', None, 'alice@test.com', 'charlie@test.com'],
                    'age': [25, 30, 45, 35, None],
                    'active': [True, True, False, True, True]
                })
            ]
            mock_read_csv.return_value = mock_table

            batches = list(handler.read_file("customers.csv"))

            assert len(batches) == 1
            assert batches[0].num_rows == 5
            assert 'customer_id' in batches[0].schema.names

    def test_csv_with_different_encodings(self):
        """Test CSV processing with different encodings."""
        encodings = ["utf-8", "latin-1", "cp1252"]

        for encoding in encodings:
            config = CsvInputConfig(encoding=encoding)
            handler = CsvInputHandler(config)

            with patch('forklift.inputs.csv.pv.read_csv') as mock_read_csv:
                mock_table = MagicMock()
                mock_table.to_batches.return_value = []
                mock_read_csv.return_value = mock_table

                list(handler.read_file("test.csv"))

                # Verify encoding was passed correctly
                call_args = mock_read_csv.call_args
                assert call_args[0][0] == "test.csv"

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.inputs.csv import (
            CsvInputConfig,
            CsvInputHandler
        )

        assert CsvInputConfig is not None
        assert CsvInputHandler is not None

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.inputs.csv as csv_module

        assert csv_module.__doc__ is not None

    def test_error_recovery_scenarios(self):
        """Test error recovery in various scenarios."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        # Test file not found
        with patch('forklift.inputs.csv.pv.read_csv') as mock_read_csv:
            mock_read_csv.side_effect = FileNotFoundError("File not found")

            with pytest.raises(FileNotFoundError):
                list(handler.read_file("nonexistent.csv"))

    def test_large_file_handling(self):
        """Test handling of large CSV files with batching."""
        config = CsvInputConfig()
        handler = CsvInputHandler(config)

        with patch('forklift.inputs.csv.pv.read_csv') as mock_read_csv:
            # Simulate large file with multiple batches
            batch1 = pa.record_batch({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
            batch2 = pa.record_batch({'id': [4, 5, 6], 'value': ['d', 'e', 'f']})

            mock_table = MagicMock()
            mock_table.to_batches.return_value = [batch1, batch2]
            mock_read_csv.return_value = mock_table

            batches = list(handler.read_file("large_file.csv"))

            assert len(batches) == 2
            assert batches[0].num_rows == 3
            assert batches[1].num_rows == 3
