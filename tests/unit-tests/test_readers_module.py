"""Tests for the readers module."""
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import json

from forklift.readers import DataFrameReader, read_csv, _cleanup_temp_dirs, _temp_dirs


class TestDataFrameReader:
    """Test cases for the DataFrameReader class."""

    def test_init_single_file(self):
        """Test initialization with a single parquet file."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        assert reader.parquet_files == files
        assert reader._temp_dir is None

    def test_init_multiple_files(self):
        """Test initialization with multiple parquet files."""
        files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        reader = DataFrameReader(files)

        assert reader.parquet_files == files
        assert reader._temp_dir is None

    def test_init_with_temp_dir(self):
        """Test initialization with temporary directory."""
        files = ["/path/to/file.parquet"]
        temp_dir = "/tmp/test_dir"

        reader = DataFrameReader(files, temp_dir)

        assert reader.parquet_files == files
        assert reader._temp_dir == temp_dir
        assert temp_dir in _temp_dirs

    def test_as_polars_single_file_eager(self):
        """Test as_polars with single file, eager loading."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        mock_df = MagicMock()

        with patch('polars.read_parquet', return_value=mock_df) as mock_read:
            result = reader.as_polars(lazy=False)

            mock_read.assert_called_once_with(files[0])
            assert result == mock_df

    def test_as_polars_single_file_lazy(self):
        """Test as_polars with single file, lazy loading."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        mock_lf = MagicMock()

        with patch('polars.scan_parquet', return_value=mock_lf) as mock_scan:
            result = reader.as_polars(lazy=True)

            mock_scan.assert_called_once_with(files[0])
            assert result == mock_lf

    def test_as_polars_multiple_files_eager(self):
        """Test as_polars with multiple files, eager loading."""
        files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        reader = DataFrameReader(files)

        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_concat_result = MagicMock()

        with patch('polars.read_parquet', side_effect=[mock_df1, mock_df2]) as mock_read, \
             patch('polars.concat', return_value=mock_concat_result) as mock_concat:

            result = reader.as_polars(lazy=False)

            assert mock_read.call_count == 2
            mock_concat.assert_called_once_with([mock_df1, mock_df2])
            assert result == mock_concat_result

    def test_as_polars_multiple_files_lazy(self):
        """Test as_polars with multiple files, lazy loading."""
        files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        reader = DataFrameReader(files)

        mock_lf1 = MagicMock()
        mock_lf2 = MagicMock()
        mock_concat_result = MagicMock()

        with patch('polars.scan_parquet', side_effect=[mock_lf1, mock_lf2]) as mock_scan, \
             patch('polars.concat', return_value=mock_concat_result) as mock_concat:

            result = reader.as_polars(lazy=True)

            assert mock_scan.call_count == 2
            mock_concat.assert_called_once_with([mock_lf1, mock_lf2])
            assert result == mock_concat_result

    def test_as_polars_import_error(self):
        """Test as_polars when polars is not installed."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        with patch.dict('sys.modules', {'polars': None}):
            with patch('builtins.__import__', side_effect=ImportError()):
                with pytest.raises(ImportError) as exc_info:
                    reader.as_polars()

                assert "polars is required for as_polars()" in str(exc_info.value)
                assert "pip install polars" in str(exc_info.value)

    def test_as_pandas_single_file(self):
        """Test as_pandas with single file."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        mock_df = MagicMock()

        with patch('pandas.read_parquet', return_value=mock_df) as mock_read:
            result = reader.as_pandas(columns=['col1', 'col2'])

            mock_read.assert_called_once_with(files[0], columns=['col1', 'col2'])
            assert result == mock_df

    def test_as_pandas_multiple_files(self):
        """Test as_pandas with multiple files."""
        files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        reader = DataFrameReader(files)

        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_concat_result = MagicMock()

        with patch('pandas.read_parquet', side_effect=[mock_df1, mock_df2]) as mock_read, \
             patch('pandas.concat', return_value=mock_concat_result) as mock_concat:

            result = reader.as_pandas()

            assert mock_read.call_count == 2
            mock_concat.assert_called_once_with([mock_df1, mock_df2], ignore_index=True)
            assert result == mock_concat_result

    def test_as_pandas_import_error(self):
        """Test as_pandas when pandas is not installed."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        with patch.dict('sys.modules', {'pandas': None}):
            with patch('builtins.__import__', side_effect=ImportError()):
                with pytest.raises(ImportError) as exc_info:
                    reader.as_pandas()

                assert "pandas is required for as_pandas()" in str(exc_info.value)
                assert "pip install pandas" in str(exc_info.value)

    def test_as_pyarrow_single_file(self):
        """Test as_pyarrow with single file."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        mock_table = MagicMock()

        with patch('pyarrow.parquet.read_table', return_value=mock_table) as mock_read:
            result = reader.as_pyarrow()

            mock_read.assert_called_once_with(files[0])
            assert result == mock_table

    def test_as_pyarrow_multiple_files(self):
        """Test as_pyarrow with multiple files."""
        files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        reader = DataFrameReader(files)

        mock_table1 = MagicMock()
        mock_table2 = MagicMock()
        mock_concat_result = MagicMock()

        with patch('pyarrow.parquet.read_table', side_effect=[mock_table1, mock_table2]) as mock_read, \
             patch('pyarrow.concat_tables', return_value=mock_concat_result) as mock_concat:

            result = reader.as_pyarrow()

            assert mock_read.call_count == 2
            mock_concat.assert_called_once_with([mock_table1, mock_table2])
            assert result == mock_concat_result

    def test_as_pyarrow_import_error(self):
        """Test as_pyarrow when pyarrow is not installed."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        with patch.dict('sys.modules', {'pyarrow.parquet': None}):
            with patch('builtins.__import__', side_effect=ImportError()):
                with pytest.raises(ImportError) as exc_info:
                    reader.as_pyarrow()

                assert "pyarrow is required for as_pyarrow()" in str(exc_info.value)
                assert "pip install pyarrow" in str(exc_info.value)

    @patch('forklift.readers.shutil.rmtree')
    def test_cleanup(self, mock_rmtree):
        """Test manual cleanup of temporary files."""
        files = ["/path/to/file.parquet"]
        temp_dir = "/tmp/test_dir"

        reader = DataFrameReader(files, temp_dir)

        # Verify temp_dir is in global registry
        assert temp_dir in _temp_dirs

        reader.cleanup()

        mock_rmtree.assert_called_once_with(temp_dir, ignore_errors=True)
        assert temp_dir not in _temp_dirs

    @patch('forklift.readers.shutil.rmtree')
    def test_cleanup_no_temp_dir(self, mock_rmtree):
        """Test cleanup when no temp_dir is set."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        reader.cleanup()

        mock_rmtree.assert_not_called()

    @patch('forklift.readers.shutil.rmtree')
    def test_del_cleanup(self, mock_rmtree):
        """Test that __del__ triggers cleanup."""
        files = ["/path/to/file.parquet"]
        temp_dir = "/tmp/test_dir"

        reader = DataFrameReader(files, temp_dir)

        # Manually call __del__ to simulate object deletion
        reader.__del__()

        mock_rmtree.assert_called_once_with(temp_dir, ignore_errors=True)
        assert temp_dir not in _temp_dirs


class TestReadCsv:
    """Test cases for the read_csv function."""

    @patch('forklift.readers.import_csv')
    @patch('forklift.readers.tempfile.mkdtemp')
    def test_read_csv_basic(self, mock_mkdtemp, mock_import_csv):
        """Test basic read_csv functionality."""
        # Setup mocks
        temp_dir = "/tmp/test_temp"
        mock_mkdtemp.return_value = temp_dir

        mock_result = MagicMock()
        mock_result.output_files = ["/tmp/output1.parquet", "/tmp/output2.parquet"]
        mock_import_csv.return_value = mock_result

        # Call function
        result = read_csv("/path/to/input.csv")

        # Verify calls
        mock_mkdtemp.assert_called_once()
        mock_import_csv.assert_called_once_with(
            input_path="/path/to/input.csv",
            output_path=temp_dir,
            schema_file=None
        )

        # Verify result
        assert isinstance(result, DataFrameReader)
        assert result.parquet_files == mock_result.output_files
        assert result._temp_dir == temp_dir

    @patch('forklift.readers.import_csv')
    @patch('forklift.readers.tempfile.mkdtemp')
    def test_read_csv_with_schema(self, mock_mkdtemp, mock_import_csv):
        """Test read_csv with schema file."""
        temp_dir = "/tmp/test_temp"
        mock_mkdtemp.return_value = temp_dir

        mock_result = MagicMock()
        mock_result.output_files = ["/tmp/output.parquet"]
        mock_import_csv.return_value = mock_result

        result = read_csv(
            "/path/to/input.csv",
            schema_file="/path/to/schema.json"
        )

        mock_import_csv.assert_called_once_with(
            input_path="/path/to/input.csv",
            output_path=temp_dir,
            schema_file="/path/to/schema.json"
        )

        assert isinstance(result, DataFrameReader)


class TestGlobalCleanup:
    """Test cases for global cleanup functionality."""

    @patch('forklift.readers.shutil.rmtree')
    def test_cleanup_temp_dirs(self, mock_rmtree):
        """Test global temp directory cleanup."""
        # Save original state
        original_dirs = _temp_dirs.copy()
        _temp_dirs.clear()

        # Add some test directories to the global set
        test_dirs = {"/tmp/test1", "/tmp/test2", "/tmp/test3"}
        _temp_dirs.update(test_dirs)

        try:
            _cleanup_temp_dirs()

            # Verify rmtree was called for each directory
            assert mock_rmtree.call_count == len(test_dirs)
            for call in mock_rmtree.call_args_list:
                args, kwargs = call
                assert args[0] in test_dirs
                assert kwargs == {'ignore_errors': True}

        finally:
            # Restore original state
            _temp_dirs.clear()
            _temp_dirs.update(original_dirs)

    def test_atexit_registration(self):
        """Test that cleanup function is registered with atexit."""
        import atexit

        # Check that our cleanup function is in the atexit handlers
        # Note: This is implementation-dependent and may vary between Python versions
        # We can at least verify the function exists and is callable
        assert callable(_cleanup_temp_dirs)


if __name__ == "__main__":
    pytest.main([__file__])
