"""Forklift readers for ad-hoc DataFrame usage."""

from __future__ import annotations
import tempfile
import shutil
import atexit
from pathlib import Path
from typing import Any, Dict, Optional, Union

# Import refactored components instead of old functions
from .inputs.csv import CsvInputHandler
from .inputs.excel import ExcelInputHandler
from .inputs.fwf import FwfInputHandler
from .inputs.sql import SqlInputHandler
from .inputs.config import CsvInputConfig, ExcelInputConfig, FwfInputConfig, SqlInputConfig

# Global registry of temporary directories for cleanup
_temp_dirs = set()

def _cleanup_temp_dirs():
    """Clean up all temporary directories on exit."""
    for temp_dir in _temp_dirs:
        shutil.rmtree(temp_dir, ignore_errors=True)

# Register cleanup function
atexit.register(_cleanup_temp_dirs)


# Backward compatibility wrapper functions for the old import_* functions
def import_csv(*args, **kwargs):
    """Backward compatibility wrapper for CSV import functionality."""
    raise NotImplementedError("import_csv has been refactored. Use CsvInputHandler from forklift.inputs.csv instead.")

def import_excel(*args, **kwargs):
    """Backward compatibility wrapper for Excel import functionality."""
    raise NotImplementedError("import_excel has been refactored. Use ExcelInputHandler from forklift.inputs.excel instead.")

def import_fwf(*args, **kwargs):
    """Backward compatibility wrapper for FWF import functionality."""
    raise NotImplementedError("import_fwf has been refactored. Use FwfInputHandler from forklift.inputs.fwf instead.")

def import_sql(*args, **kwargs):
    """Backward compatibility wrapper for SQL import functionality."""
    raise NotImplementedError("import_sql has been refactored. Use SqlInputHandler from forklift.inputs.sql instead.")


class DataFrameReader:
    """Reader that can convert processed data to Polars or Pandas DataFrames.

    This class manages temporary Parquet files created during processing and
    provides methods to convert them to popular DataFrame formats.
    """

    def __init__(self, parquet_files: list[str], temp_dir: Optional[str] = None):
        """Initialize with list of parquet files from processing.

        Args:
            parquet_files: List of paths to Parquet files
            temp_dir: Optional temporary directory path for cleanup
        """
        self.parquet_files = parquet_files
        self._temp_dir = temp_dir
        if temp_dir:
            _temp_dirs.add(temp_dir)

    def as_polars(self, lazy: bool = False) -> "polars.DataFrame | polars.LazyFrame":
        """Return data as a Polars DataFrame or LazyFrame.

        Args:
            lazy: If True, return LazyFrame for lazy evaluation (default: False)

        Returns:
            Polars DataFrame or LazyFrame

        Example:
            >>> df = reader.as_polars()  # Eager DataFrame
            >>> lf = reader.as_polars(lazy=True)  # Lazy evaluation
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars is required for as_polars(). Install with: pip install polars")

        if len(self.parquet_files) == 1:
            if lazy:
                return pl.scan_parquet(self.parquet_files[0])
            else:
                return pl.read_parquet(self.parquet_files[0])
        else:
            # For multiple files, use scan_parquet with glob pattern if possible
            if lazy:
                # Create LazyFrames and concatenate
                lazy_frames = [pl.scan_parquet(f) for f in self.parquet_files]
                return pl.concat(lazy_frames)
            else:
                # Read and concatenate eagerly
                dfs = [pl.read_parquet(f) for f in self.parquet_files]
                return pl.concat(dfs)

    def as_pandas(self, **kwargs) -> "pandas.DataFrame":
        """Return data as a Pandas DataFrame.

        Args:
            **kwargs: Additional arguments passed to pd.read_parquet()

        Returns:
            Pandas DataFrame
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for as_pandas(). Install with: pip install pandas")

        if len(self.parquet_files) == 1:
            return pd.read_parquet(self.parquet_files[0], **kwargs)
        else:
            # Concatenate multiple files
            dfs = [pd.read_parquet(f, **kwargs) for f in self.parquet_files]
            return pd.concat(dfs, ignore_index=True)

    def as_pyarrow(self) -> "pyarrow.Table":
        """Return data as a PyArrow Table.

        Returns:
            PyArrow Table
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow is required for as_pyarrow(). Install with: pip install pyarrow")

        if len(self.parquet_files) == 1:
            return pq.read_table(self.parquet_files[0])
        else:
            # Read and concatenate multiple tables
            tables = [pq.read_table(f) for f in self.parquet_files]
            import pyarrow as pa
            return pa.concat_tables(tables)

    def cleanup(self):
        """Manually clean up temporary files."""
        if self._temp_dir:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            _temp_dirs.discard(self._temp_dir)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup()


# Convenience functions for quick DataFrame reading
def read_csv(source_path: Union[str, Path], **kwargs) -> DataFrameReader:
    """Read CSV file and return DataFrameReader for DataFrame conversion.

    Args:
        source_path: Path to CSV file
        **kwargs: Additional arguments for CSV processing (delimiter, encoding, etc.)

    Returns:
        DataFrameReader instance for converting to various DataFrame formats

    Example:
        >>> reader = read_csv('data.csv')
        >>> df = reader.as_pandas()
        >>> reader.cleanup()
    """
    from .inputs.csv import CsvInputHandler
    from .inputs.config import CsvInputConfig

    # Create temporary directory for processing
    temp_dir = tempfile.mkdtemp()
    _temp_dirs.add(temp_dir)

    try:
        # Create CSV config from kwargs
        csv_config = CsvInputConfig(
            delimiter=kwargs.get('delimiter', ','),
            encoding=kwargs.get('encoding', 'utf-8'),
            has_header=kwargs.get('has_header', True),
            quote_char=kwargs.get('quote_char', '"'),
            escape_char=kwargs.get('escape_char', '\\'),
            null_values=kwargs.get('null_values', ["", "NULL", "null", "None"])
        )

        # Use the refactored CSV input handler
        csv_handler = CsvInputHandler(csv_config)

        # Read the CSV file using the proper methods
        source_path = Path(source_path)

        # Find header row if header is expected
        if csv_config.has_header:
            header_row_idx, column_names = csv_handler.find_header_row(source_path)
            skip_rows = header_row_idx + 1
        else:
            # Generate column names if no header
            with open(source_path, 'r', encoding=csv_config.encoding) as f:
                first_line = f.readline()
                num_cols = len(first_line.split(csv_config.delimiter))
                column_names = [f"column_{i}" for i in range(num_cols)]
            skip_rows = 0

        # Create arrow reader and read the table
        reader = csv_handler.create_arrow_reader(source_path, column_names, skip_rows)
        table = reader.read_all()

        # Write to temporary parquet file
        output_path = Path(temp_dir) / "data.parquet"

        import pyarrow.parquet as pq
        pq.write_table(table, output_path)

        return DataFrameReader([str(output_path)], temp_dir)

    except Exception as e:
        # Clean up on error
        shutil.rmtree(temp_dir, ignore_errors=True)
        _temp_dirs.discard(temp_dir)
        raise RuntimeError(f"Failed to read CSV file: {str(e)}")

def read_excel(source_path: Union[str, Path], **kwargs) -> DataFrameReader:
    """Read Excel file and return DataFrameReader for DataFrame conversion.

    Args:
        source_path: Path to Excel file
        **kwargs: Additional arguments for Excel processing

    Returns:
        DataFrameReader instance
    """
    raise NotImplementedError("read_excel has been refactored. Use ExcelInputHandler from forklift.inputs.excel directly.")

def read_fwf(source_path: Union[str, Path], **kwargs) -> DataFrameReader:
    """Read Fixed Width File and return DataFrameReader for DataFrame conversion.

    Args:
        source_path: Path to FWF file
        **kwargs: Additional arguments for FWF processing

    Returns:
        DataFrameReader instance
    """
    raise NotImplementedError("read_fwf has been refactored. Use FwfInputHandler from forklift.inputs.fwf directly.")

def read_sql(query: str, connection_string: str, **kwargs) -> DataFrameReader:
    """Execute SQL query and return DataFrameReader for DataFrame conversion.

    Args:
        query: SQL query to execute
        connection_string: Database connection string
        **kwargs: Additional arguments for SQL processing

    Returns:
        DataFrameReader instance
    """
    raise NotImplementedError("read_sql has been refactored. Use SqlInputHandler from forklift.inputs.sql directly.")
