"""Tests for Forklift readers module."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.readers import (DataFrameReader, _cleanup_temp_dirs, _temp_dirs,
                              read_csv, read_excel)


class TestDataFrameReader:
    """Test cases for DataFrameReader class."""

    @pytest.fixture
    def sample_parquet_file(self):
        """Create a sample Parquet file for testing."""
        # Create sample data
        table = pa.table(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )

        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            pq.write_table(table, tmp_file.name)
            yield tmp_file.name

        # Cleanup
        Path(tmp_file.name).unlink(missing_ok=True)

    @pytest.fixture
    def multiple_parquet_files(self):
        """Create multiple sample Parquet files for testing."""
        files = []

        # Create first file
        table1 = pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]})

        # Create second file
        table2 = pa.table({"id": [3, 4], "name": ["Charlie", "David"], "age": [35, 40]})

        for i, table in enumerate([table1, table2]):
            with tempfile.NamedTemporaryFile(suffix=f"_{i}.parquet", delete=False) as tmp_file:
                pq.write_table(table, tmp_file.name)
                files.append(tmp_file.name)

        yield files

        # Cleanup
        for file_path in files:
            Path(file_path).unlink(missing_ok=True)

    def test_init_single_file(self, sample_parquet_file):
        """Test DataFrameReader initialization with single file."""
        reader = DataFrameReader([sample_parquet_file])

        assert reader.parquet_files == [sample_parquet_file]
        assert reader._temp_dir is None

    def test_init_with_temp_dir(self, sample_parquet_file):
        """Test DataFrameReader initialization with temp directory."""
        temp_dir = "/tmp/test"
        reader = DataFrameReader([sample_parquet_file], temp_dir)

        assert reader.parquet_files == [sample_parquet_file]
        assert reader._temp_dir == temp_dir
        assert temp_dir in _temp_dirs

    def test_as_polars_single_file_eager(self, sample_parquet_file):
        """Test as_polars with single file, eager evaluation."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("polars.read_parquet") as mock_read:
            mock_df = Mock()
            mock_read.return_value = mock_df

            result = reader.as_polars(lazy=False)

            mock_read.assert_called_once_with(sample_parquet_file)
            assert result == mock_df

    def test_as_polars_single_file_lazy(self, sample_parquet_file):
        """Test as_polars with single file, lazy evaluation."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("polars.scan_parquet") as mock_scan:
            mock_lf = Mock()
            mock_scan.return_value = mock_lf

            result = reader.as_polars(lazy=True)

            mock_scan.assert_called_once_with(sample_parquet_file)
            assert result == mock_lf

    def test_as_polars_multiple_files_eager(self, multiple_parquet_files):
        """Test as_polars with multiple files, eager evaluation."""
        reader = DataFrameReader(multiple_parquet_files)

        with patch("polars.read_parquet") as mock_read:
            with patch("polars.concat") as mock_concat:
                mock_df1 = Mock()
                mock_df2 = Mock()
                mock_read.side_effect = [mock_df1, mock_df2]
                mock_result = Mock()
                mock_concat.return_value = mock_result

                result = reader.as_polars(lazy=False)

                assert mock_read.call_count == 2
                mock_concat.assert_called_once_with([mock_df1, mock_df2])
                assert result == mock_result

    def test_as_polars_multiple_files_lazy(self, multiple_parquet_files):
        """Test as_polars with multiple files, lazy evaluation."""
        reader = DataFrameReader(multiple_parquet_files)

        with patch("polars.scan_parquet") as mock_scan:
            with patch("polars.concat") as mock_concat:
                mock_lf1 = Mock()
                mock_lf2 = Mock()
                mock_scan.side_effect = [mock_lf1, mock_lf2]
                mock_result = Mock()
                mock_concat.return_value = mock_result

                result = reader.as_polars(lazy=True)

                assert mock_scan.call_count == 2
                mock_concat.assert_called_once_with([mock_lf1, mock_lf2])
                assert result == mock_result

    def test_as_polars_import_error(self, sample_parquet_file):
        """Test as_polars with missing polars package."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="polars is required for as_polars()"):
                reader.as_polars()

    def test_as_pandas_single_file(self, sample_parquet_file):
        """Test as_pandas with single file."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("pandas.read_parquet") as mock_read:
            mock_df = Mock()
            mock_read.return_value = mock_df

            result = reader.as_pandas()

            mock_read.assert_called_once_with(sample_parquet_file)
            assert result == mock_df

    def test_as_pandas_multiple_files(self, multiple_parquet_files):
        """Test as_pandas with multiple files."""
        reader = DataFrameReader(multiple_parquet_files)

        with patch("pandas.read_parquet") as mock_read:
            with patch("pandas.concat") as mock_concat:
                mock_df1 = Mock()
                mock_df2 = Mock()
                mock_read.side_effect = [mock_df1, mock_df2]
                mock_result = Mock()
                mock_concat.return_value = mock_result

                result = reader.as_pandas()

                assert mock_read.call_count == 2
                mock_concat.assert_called_once_with([mock_df1, mock_df2], ignore_index=True)
                assert result == mock_result

    def test_as_pandas_with_kwargs(self, sample_parquet_file):
        """Test as_pandas with additional kwargs."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("pandas.read_parquet") as mock_read:
            mock_df = Mock()
            mock_read.return_value = mock_df

            result = reader.as_pandas(columns=["id", "name"])

            mock_read.assert_called_once_with(sample_parquet_file, columns=["id", "name"])
            assert result == mock_df

    def test_as_pandas_import_error(self, sample_parquet_file):
        """Test as_pandas with missing pandas package."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="pandas is required for as_pandas()"):
                reader.as_pandas()

    def test_as_pyarrow_single_file(self, sample_parquet_file):
        """Test as_pyarrow with single file."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("pyarrow.parquet.read_table") as mock_read:
            mock_table = Mock()
            mock_read.return_value = mock_table

            result = reader.as_pyarrow()

            mock_read.assert_called_once_with(sample_parquet_file)
            assert result == mock_table

    def test_as_pyarrow_multiple_files(self, multiple_parquet_files):
        """Test as_pyarrow with multiple files."""
        reader = DataFrameReader(multiple_parquet_files)

        with patch("pyarrow.parquet.read_table") as mock_read:
            with patch("pyarrow.concat_tables") as mock_concat:
                mock_table1 = Mock()
                mock_table2 = Mock()
                mock_read.side_effect = [mock_table1, mock_table2]
                mock_result = Mock()
                mock_concat.return_value = mock_result

                result = reader.as_pyarrow()

                assert mock_read.call_count == 2
                mock_concat.assert_called_once_with([mock_table1, mock_table2])
                assert result == mock_result

    def test_as_pyarrow_import_error(self, sample_parquet_file):
        """Test as_pyarrow with missing pyarrow package."""
        reader = DataFrameReader([sample_parquet_file])

        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="pyarrow is required for as_pyarrow()"):
                reader.as_pyarrow()

    def test_cleanup(self):
        """Test manual cleanup of temporary directory."""
        temp_dir = "/tmp/test_cleanup"
        reader = DataFrameReader([], temp_dir)

        # Add to global registry
        _temp_dirs.add(temp_dir)

        with patch("shutil.rmtree") as mock_rmtree:
            reader.cleanup()

            mock_rmtree.assert_called_once_with(temp_dir, ignore_errors=True)
            assert temp_dir not in _temp_dirs

    def test_cleanup_no_temp_dir(self):
        """Test cleanup with no temp directory."""
        reader = DataFrameReader([])

        with patch("shutil.rmtree") as mock_rmtree:
            reader.cleanup()

            mock_rmtree.assert_not_called()

    def test_del_calls_cleanup(self):
        """Test that __del__ calls cleanup."""
        reader = DataFrameReader([])

        with patch.object(reader, "cleanup") as mock_cleanup:
            # Explicitly call __del__ instead of relying on garbage collection
            reader.__del__()

            mock_cleanup.assert_called_once()


class TestReaderFunctions:
    """Test cases for reader utility functions."""

    def test_read_csv_basic(self):
        """Test read_csv function with basic parameters."""
        with patch("forklift.readers.import_csv") as mock_import:
            with patch("tempfile.mkdtemp") as mock_mkdtemp:
                mock_mkdtemp.return_value = "/tmp/test"
                mock_results = Mock()
                mock_results.output_files = ["output.parquet"]
                mock_import.return_value = mock_results

                result = read_csv("input.csv")

                assert isinstance(result, DataFrameReader)
                assert result.parquet_files == ["output.parquet"]
                assert result._temp_dir == "/tmp/test"

    def test_read_csv_with_schema(self):
        """Test read_csv function with schema file."""
        with patch("forklift.readers.import_csv") as mock_import:
            with patch("tempfile.mkdtemp") as mock_mkdtemp:
                mock_mkdtemp.return_value = "/tmp/test"
                mock_results = Mock()
                mock_results.output_files = ["output.parquet"]
                mock_import.return_value = mock_results

                result = read_csv("input.csv", schema_file="schema.json")

                mock_import.assert_called_once_with(
                    input_path="input.csv",
                    output_path="/tmp/test",
                    schema_file="schema.json",
                    encoding="utf-8",
                    delimiter=",",
                )

    def test_read_csv_error_cleanup(self):
        """Test read_csv cleans up temp directory on error."""
        with patch("forklift.readers.import_csv") as mock_import:
            with patch("tempfile.mkdtemp") as mock_mkdtemp:
                with patch("shutil.rmtree") as mock_rmtree:
                    mock_mkdtemp.return_value = "/tmp/test"
                    mock_import.side_effect = Exception("Import failed")

                    with pytest.raises(Exception, match="Import failed"):
                        read_csv("input.csv")

                    mock_rmtree.assert_called_once_with("/tmp/test", ignore_errors=True)

    def test_read_excel_basic(self):
        """Test read_excel function with basic parameters."""
        with patch("forklift.readers.import_excel") as mock_import:
            with patch("tempfile.mkdtemp") as mock_mkdtemp:
                mock_mkdtemp.return_value = "/tmp/test"
                mock_results = Mock()
                mock_results.output_files = ["output.parquet"]
                mock_import.return_value = mock_results

                result = read_excel("input.xlsx")

                assert isinstance(result, DataFrameReader)
                assert result.parquet_files == ["output.parquet"]

    def test_cleanup_temp_dirs(self):
        """Test global cleanup function."""
        # Add some test directories
        test_dirs = {"/tmp/test1", "/tmp/test2"}
        _temp_dirs.update(test_dirs)

        with patch("shutil.rmtree") as mock_rmtree:
            _cleanup_temp_dirs()

            # Should call rmtree for each directory
            assert mock_rmtree.call_count == len(test_dirs)
            for temp_dir in test_dirs:
                mock_rmtree.assert_any_call(temp_dir, ignore_errors=True)
