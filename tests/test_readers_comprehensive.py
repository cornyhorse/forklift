"""Comprehensive tests for forklift readers module to improve code coverage."""

import pytest
import tempfile
import shutil
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, call
import sys

# Import the module under test
from forklift.readers import (
    DataFrameReader,
    read_csv,
    read_excel,
    read_fwf,
    read_sql,
    _cleanup_temp_dirs,
    _temp_dirs
)


class TestDataFrameReaderComprehensive:
    """Comprehensive tests for DataFrameReader class."""

    def setup_method(self):
        """Clear temp dirs before each test."""
        _temp_dirs.clear()

    def test_init_with_temp_dir_registration(self):
        """Test that temp_dir is properly registered for cleanup."""
        files = ["/path/to/file.parquet"]
        temp_dir = "/tmp/test_cleanup"

        reader = DataFrameReader(files, temp_dir)

        assert reader._temp_dir == temp_dir
        assert temp_dir in _temp_dirs

    def test_cleanup_method(self):
        """Test manual cleanup method."""
        files = ["/path/to/file.parquet"]
        temp_dir = "/tmp/test_cleanup"

        with patch('shutil.rmtree') as mock_rmtree:
            reader = DataFrameReader(files, temp_dir)
            assert temp_dir in _temp_dirs

            reader.cleanup()

            mock_rmtree.assert_called_once_with(temp_dir, ignore_errors=True)
            assert temp_dir not in _temp_dirs

    def test_cleanup_method_no_temp_dir(self):
        """Test cleanup method when no temp_dir is set."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        with patch('shutil.rmtree') as mock_rmtree:
            reader.cleanup()
            mock_rmtree.assert_not_called()

    def test_del_method_calls_cleanup(self):
        """Test that __del__ method calls cleanup."""
        files = ["/path/to/file.parquet"]
        temp_dir = "/tmp/test_cleanup"

        with patch('shutil.rmtree') as mock_rmtree:
            reader = DataFrameReader(files, temp_dir)
            reader.__del__()

            mock_rmtree.assert_called_once_with(temp_dir, ignore_errors=True)

    def test_as_polars_lazy_multiple_files_concat(self):
        """Test as_polars with multiple files and lazy=True using concat."""
        files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        reader = DataFrameReader(files)

        mock_lf1 = MagicMock()
        mock_lf2 = MagicMock()
        mock_concat_result = MagicMock()

        with patch('polars.scan_parquet', side_effect=[mock_lf1, mock_lf2]) as mock_scan, \
             patch('polars.concat', return_value=mock_concat_result) as mock_concat:

            result = reader.as_polars(lazy=True)

            assert mock_scan.call_count == 2
            mock_scan.assert_has_calls([call(files[0]), call(files[1])])
            mock_concat.assert_called_once_with([mock_lf1, mock_lf2])
            assert result == mock_concat_result

    def test_as_pandas_with_kwargs(self):
        """Test as_pandas with additional keyword arguments."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        mock_df = MagicMock()

        with patch('pandas.read_parquet', return_value=mock_df) as mock_read:
            result = reader.as_pandas(columns=['col1'], engine='pyarrow')

            mock_read.assert_called_once_with(files[0], columns=['col1'], engine='pyarrow')
            assert result == mock_df

    def test_as_pyarrow_multiple_files_concat_tables(self):
        """Test as_pyarrow with multiple files using concat_tables."""
        files = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        reader = DataFrameReader(files)

        mock_table1 = MagicMock()
        mock_table2 = MagicMock()
        mock_concat_result = MagicMock()

        with patch('pyarrow.parquet.read_table', side_effect=[mock_table1, mock_table2]) as mock_read, \
             patch('pyarrow.concat_tables', return_value=mock_concat_result) as mock_concat:

            result = reader.as_pyarrow()

            assert mock_read.call_count == 2
            mock_read.assert_has_calls([call(files[0]), call(files[1])])
            mock_concat.assert_called_once_with([mock_table1, mock_table2])
            assert result == mock_concat_result


class TestReaderFunctions:
    """Test the main reader functions."""

    def test_read_csv_success(self):
        """Test successful read_csv operation."""
        input_path = "/path/to/input.csv"

        # Mock the import_csv function and its results
        mock_results = MagicMock()
        mock_results.output_files = ["/tmp/output.parquet"]

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir") as mock_mkdtemp, \
             patch('forklift.readers.import_csv', return_value=mock_results) as mock_import:

            reader = read_csv(input_path, schema_file="schema.json", encoding="utf-8")

            mock_mkdtemp.assert_called_once_with(prefix="forklift_reader_")
            # Both encoding and delimiter are passed to import_csv
            mock_import.assert_called_once_with(
                input_path=input_path,
                output_path="/tmp/test_dir",
                schema_file="schema.json",
                encoding="utf-8",
                delimiter=","
            )

            assert isinstance(reader, DataFrameReader)
            assert reader.parquet_files == ["/tmp/output.parquet"]
            assert reader._temp_dir == "/tmp/test_dir"

    def test_read_csv_exception_cleanup(self):
        """Test that read_csv cleans up temp directory on exception."""
        input_path = "/path/to/input.csv"

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir") as mock_mkdtemp, \
             patch('forklift.readers.import_csv', side_effect=Exception("Import failed")) as mock_import, \
             patch('shutil.rmtree') as mock_rmtree:

            with pytest.raises(Exception, match="Import failed"):
                read_csv(input_path)

            mock_rmtree.assert_called_once_with("/tmp/test_dir", ignore_errors=True)

    def test_read_excel_success(self):
        """Test successful read_excel operation."""
        input_path = "/path/to/input.xlsx"

        mock_results = MagicMock()
        mock_results.output_files = ["/tmp/output.parquet"]

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir") as mock_mkdtemp, \
             patch('forklift.readers.import_excel', return_value=mock_results) as mock_import:

            reader = read_excel(input_path, sheet="Sheet1", schema_file="schema.json")

            mock_mkdtemp.assert_called_once_with(prefix="forklift_reader_")
            # Now sheet parameter is properly passed to import_excel
            mock_import.assert_called_once_with(
                input_path=input_path,
                output_path="/tmp/test_dir",
                schema_file="schema.json",
                sheet="Sheet1"
            )

            assert isinstance(reader, DataFrameReader)
            assert reader.parquet_files == ["/tmp/output.parquet"]
            assert reader._temp_dir == "/tmp/test_dir"

    def test_read_excel_exception_cleanup(self):
        """Test that read_excel cleans up temp directory on exception."""
        input_path = "/path/to/input.xlsx"

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir"), \
             patch('forklift.readers.import_excel', side_effect=Exception("Excel import failed")), \
             patch('shutil.rmtree') as mock_rmtree:

            with pytest.raises(Exception, match="Excel import failed"):
                read_excel(input_path)

            mock_rmtree.assert_called_once_with("/tmp/test_dir", ignore_errors=True)

    def test_read_fwf_success(self):
        """Test successful read_fwf operation."""
        input_path = "/path/to/input.txt"
        schema_file = "/path/to/fwf_schema.json"

        mock_results = MagicMock()
        mock_results.output_files = ["/tmp/output.parquet"]

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir") as mock_mkdtemp, \
             patch('forklift.readers.import_fwf', return_value=mock_results) as mock_import:

            reader = read_fwf(input_path, schema_file, custom_arg="value")

            mock_mkdtemp.assert_called_once_with(prefix="forklift_reader_")
            mock_import.assert_called_once_with(
                input_path=input_path,
                output_path="/tmp/test_dir",
                schema_file=schema_file,
                custom_arg="value"
            )

            assert isinstance(reader, DataFrameReader)

    def test_read_fwf_exception_cleanup(self):
        """Test that read_fwf cleans up temp directory on exception."""
        input_path = "/path/to/input.txt"
        schema_file = "/path/to/fwf_schema.json"

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir"), \
             patch('forklift.readers.import_fwf', side_effect=Exception("FWF import failed")), \
             patch('shutil.rmtree') as mock_rmtree:

            with pytest.raises(Exception, match="FWF import failed"):
                read_fwf(input_path, schema_file)

            mock_rmtree.assert_called_once_with("/tmp/test_dir", ignore_errors=True)

    def test_read_sql_success(self):
        """Test successful read_sql operation."""
        input_path = "postgresql://user:pass@host/db"

        mock_results = MagicMock()
        mock_results.output_files = ["/tmp/output.parquet"]

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir") as mock_mkdtemp, \
             patch('forklift.readers.import_sql', return_value=mock_results) as mock_import:

            reader = read_sql(input_path, schema_file="sql_schema.json", query="SELECT * FROM table")

            mock_mkdtemp.assert_called_once_with(prefix="forklift_reader_")
            mock_import.assert_called_once_with(
                input_path=input_path,
                output_path="/tmp/test_dir",
                schema_file="sql_schema.json",
                query="SELECT * FROM table"
            )

            assert isinstance(reader, DataFrameReader)

    def test_read_sql_exception_cleanup(self):
        """Test that read_sql cleans up temp directory on exception."""
        input_path = "postgresql://user:pass@host/db"

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir"), \
             patch('forklift.readers.import_sql', side_effect=Exception("SQL import failed")), \
             patch('shutil.rmtree') as mock_rmtree:

            with pytest.raises(Exception, match="SQL import failed"):
                read_sql(input_path)

            mock_rmtree.assert_called_once_with("/tmp/test_dir", ignore_errors=True)


class TestGlobalCleanup:
    """Test global cleanup functionality."""

    def setup_method(self):
        """Clear temp dirs before each test."""
        _temp_dirs.clear()

    def test_cleanup_temp_dirs_empty(self):
        """Test cleanup when no temp directories are registered."""
        with patch('shutil.rmtree') as mock_rmtree:
            _cleanup_temp_dirs()
            mock_rmtree.assert_not_called()

    def test_cleanup_temp_dirs_with_directories(self):
        """Test cleanup with registered temp directories."""
        _temp_dirs.add("/tmp/dir1")
        _temp_dirs.add("/tmp/dir2")

        with patch('shutil.rmtree') as mock_rmtree:
            _cleanup_temp_dirs()

            assert mock_rmtree.call_count == 2
            mock_rmtree.assert_has_calls([
                call("/tmp/dir1", ignore_errors=True),
                call("/tmp/dir2", ignore_errors=True)
            ], any_order=True)

    def test_cleanup_temp_dirs_ignore_errors(self):
        """Test that cleanup calls rmtree with ignore_errors=True."""
        _temp_dirs.add("/tmp/nonexistent")

        with patch('shutil.rmtree') as mock_rmtree:
            # Should not raise an exception because ignore_errors=True
            _cleanup_temp_dirs()
            mock_rmtree.assert_called_once_with("/tmp/nonexistent", ignore_errors=True)


class TestImportErrorHandling:
    """Test import error handling for optional dependencies."""

    def test_polars_import_error_message(self):
        """Test polars import error contains installation instructions."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        with patch.dict('sys.modules', {'polars': None}):
            with patch('builtins.__import__', side_effect=ImportError("No module named 'polars'")):
                with pytest.raises(ImportError) as exc_info:
                    reader.as_polars()

                error_msg = str(exc_info.value)
                assert "polars is required for as_polars()" in error_msg
                assert "pip install polars" in error_msg

    def test_pandas_import_error_message(self):
        """Test pandas import error contains installation instructions."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        with patch.dict('sys.modules', {'pandas': None}):
            with patch('builtins.__import__', side_effect=ImportError("No module named 'pandas'")):
                with pytest.raises(ImportError) as exc_info:
                    reader.as_pandas()

                error_msg = str(exc_info.value)
                assert "pandas is required for as_pandas()" in error_msg
                assert "pip install pandas" in error_msg

    def test_pyarrow_import_error_message(self):
        """Test pyarrow import error contains installation instructions."""
        files = ["/path/to/file.parquet"]
        reader = DataFrameReader(files)

        with patch.dict('sys.modules', {'pyarrow.parquet': None, 'pyarrow': None}):
            with patch('builtins.__import__', side_effect=ImportError("No module named 'pyarrow'")):
                with pytest.raises(ImportError) as exc_info:
                    reader.as_pyarrow()

                error_msg = str(exc_info.value)
                assert "pyarrow is required for as_pyarrow()" in error_msg
                assert "pip install pyarrow" in error_msg


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_reader_with_empty_file_list(self):
        """Test DataFrameReader with empty file list."""
        reader = DataFrameReader([])

        # Should handle empty lists gracefully
        with patch('polars.concat', return_value=MagicMock()) as mock_concat:
            reader.as_polars()
            mock_concat.assert_called_once_with([])

    def test_reader_cleanup_multiple_calls(self):
        """Test that multiple cleanup calls don't cause issues."""
        files = ["/path/to/file.parquet"]
        temp_dir = "/tmp/test_cleanup"

        with patch('shutil.rmtree') as mock_rmtree:
            reader = DataFrameReader(files, temp_dir)

            # Call cleanup multiple times
            reader.cleanup()
            reader.cleanup()

            # Should only remove directory once (second call should be safe)
            mock_rmtree.assert_called_once_with(temp_dir, ignore_errors=True)

    def test_pathlib_path_input(self):
        """Test that Path objects are handled correctly."""
        input_path = Path("/path/to/input.csv")

        mock_results = MagicMock()
        mock_results.output_files = ["/tmp/output.parquet"]

        with patch('tempfile.mkdtemp', return_value="/tmp/test_dir"), \
             patch('forklift.readers.import_csv', return_value=mock_results) as mock_import:

            reader = read_csv(input_path)

            # Should pass Path object through correctly
            mock_import.assert_called_once()
            args, kwargs = mock_import.call_args
            assert kwargs['input_path'] == input_path
