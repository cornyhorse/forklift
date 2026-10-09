"""Forklift readers for ad-hoc DataFrame usage."""

from __future__ import annotations

import atexit
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Union

from .engine.forklift_core import import_csv, import_excel, import_fwf, import_sql

# Global registry of temporary directories for cleanup
_temp_dirs = set()


def _cleanup_temp_dirs():
    """Clean up all temporary directories on exit."""
    for temp_dir in _temp_dirs:
        shutil.rmtree(temp_dir, ignore_errors=True)


# Register cleanup function
atexit.register(_cleanup_temp_dirs)


def _data_files(results) -> list[str]:
    """Parquet files with the accepted data of a processing run (rejected rows left out)."""
    files = list(results.output_files)
    bad_rows_file = getattr(results, "bad_rows_file", None)
    if isinstance(bad_rows_file, str):
        files = [f for f in files if f != bad_rows_file]
    return files


class DataFrameReader:
    """Reader that can convert processed data to Polars or Pandas DataFrames.

    This class manages temporary Parquet files created during processing and
    provides methods to convert them to popular DataFrame formats.

    The temporary files live until ``close()`` is called (or the reader is used as a context
    manager) or, at the latest, until the interpreter exits. They are deliberately *not*
    removed when the reader is garbage collected, because a Polars ``LazyFrame`` returned by
    ``as_polars(lazy=True)`` keeps reading from them after the reader itself is gone.

    Example:
        >>> with fl.read_csv("data.csv") as reader:  # noqa: F821
        ...     df = reader.as_pandas()
    """

    def __init__(self, parquet_files: list[str], temp_dir: Optional[str] = None):
        """Initialize with list of parquet files from processing.

        Args:
            parquet_files: List of paths to Parquet files
            temp_dir: Optional temporary directory path for cleanup
        """
        self.parquet_files = parquet_files
        self._temp_dir = temp_dir
        self._closed = False
        if temp_dir:
            _temp_dirs.add(temp_dir)

    def _check_open(self) -> None:
        """Raise a clear error if the files were already deleted by close()."""
        if self._closed:
            raise ValueError(
                "This DataFrameReader is closed: its temporary files were deleted. "
                "Convert the data before calling close()."
            )

    def as_polars(self, lazy: bool = False) -> "polars.DataFrame | polars.LazyFrame":  # noqa: F821
        """Return data as a Polars DataFrame or LazyFrame.

        Args:
            lazy: If True, return LazyFrame for lazy evaluation (default: False)

        Returns:
            Polars DataFrame or LazyFrame

        Example:
            >>> df = reader.as_polars()  # noqa: F821
            >>> lf = reader.as_polars(lazy=True)  # noqa: F821
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "polars is required for as_polars(). Install with: pip install polars"
            )

        self._check_open()

        if not self.parquet_files:
            # Nothing was written (e.g. an empty input): an empty frame
            return pl.LazyFrame() if lazy else pl.DataFrame()

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

    def as_pandas(self, **kwargs) -> "pandas.DataFrame":  # noqa: F821
        """Return data as a Pandas DataFrame.

        Args:
            **kwargs: Additional arguments passed to pd.read_parquet()

        Returns:
            Pandas DataFrame
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for as_pandas(). Install with: pip install pandas"
            )

        self._check_open()

        if not self.parquet_files:
            return pd.DataFrame()

        if len(self.parquet_files) == 1:
            return pd.read_parquet(self.parquet_files[0], **kwargs)
        else:
            # Concatenate multiple files
            dfs = [pd.read_parquet(f, **kwargs) for f in self.parquet_files]
            return pd.concat(dfs, ignore_index=True)

    def as_pyarrow(self) -> "pyarrow.Table":  # noqa: F821
        """Return data as a PyArrow Table.

        Returns:
            PyArrow Table
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError(
                "pyarrow is required for as_pyarrow(). Install with: pip install pyarrow"
            )

        self._check_open()

        import pyarrow as pa

        if not self.parquet_files:
            return pa.table({})

        if len(self.parquet_files) == 1:
            return pq.read_table(self.parquet_files[0])
        else:
            # Concatenate multiple tables
            tables = [pq.read_table(f) for f in self.parquet_files]
            return pa.concat_tables(tables)

    def cleanup(self):
        """Manually clean up temporary files."""
        if self._temp_dir and self._temp_dir in _temp_dirs:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            _temp_dirs.discard(self._temp_dir)

    def close(self):
        """Delete the temporary files. Lazy frames created earlier become unusable."""
        self.cleanup()
        self._closed = True

    def __enter__(self) -> "DataFrameReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def read_csv(
    input_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    encoding: str = "utf-8",
    delimiter: str = ",",
    **kwargs,
) -> DataFrameReader:
    """
    Read CSV file and return a DataFrameReader for conversion to Polars/Pandas.

    This function processes the CSV through forklift's validation and cleaning
    pipeline, then returns a reader that can convert to various DataFrame formats.

    Args:
        input_path: Path to CSV file (local or S3)
        schema_file: Optional JSON schema file for validation
        encoding: Text encoding (default: utf-8)
        delimiter: Field delimiter (default: comma)
        **kwargs: Additional arguments passed to import_csv

    Returns:
        DataFrameReader that can be converted to Polars or Pandas DataFrames

    Example:
        >>> import forklift as fl
        >>> df = fl.read_csv("data.csv").as_polars()
        >>> df = fl.read_csv("data.csv", schema_file="schema.json").as_pandas()
        >>> lf = fl.read_csv("large_data.csv").as_polars(lazy=True)  # Lazy evaluation
    """
    # Create temporary directory for output
    temp_dir = tempfile.mkdtemp(prefix="forklift_reader_")

    try:
        # Process CSV to Parquet
        results = import_csv(
            input_path=input_path,
            output_path=temp_dir,
            schema_file=schema_file,
            encoding=encoding,
            delimiter=delimiter,
            **kwargs,
        )

        # Return reader with the generated parquet files
        return DataFrameReader(_data_files(results), temp_dir)

    except Exception as e:
        # Clean up temp directory on error
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e


def read_excel(
    input_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    sheet: Optional[str] = None,
    **kwargs,
) -> DataFrameReader:
    """
    Read Excel file and return a DataFrameReader for conversion to Polars/Pandas.

    Args:
        input_path: Path to Excel file (local or S3)
        schema_file: Optional JSON schema file for validation
        sheet: Specific sheet name to read
        **kwargs: Additional arguments passed to import_excel

    Returns:
        DataFrameReader that can be converted to Polars or Pandas DataFrames

    Example:
        >>> import forklift as fl
        >>> df = fl.read_excel("data.xlsx").as_polars()
        >>> df = fl.read_excel("data.xlsx", sheet="Sheet1").as_pandas()
    """
    # Create temporary directory for output
    temp_dir = tempfile.mkdtemp(prefix="forklift_reader_")

    try:
        # Process Excel to Parquet
        results = import_excel(
            input_path=input_path,
            output_path=temp_dir,
            schema_file=schema_file,
            sheet=sheet,
            **kwargs,
        )

        # Return reader with the generated parquet files
        return DataFrameReader(_data_files(results), temp_dir)

    except Exception as e:
        # Clean up temp directory on error
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e


def read_fwf(
    input_path: Union[str, Path], schema_file: Union[str, Path], **kwargs
) -> DataFrameReader:
    """
    Read Fixed-Width File and return a DataFrameReader for conversion to Polars/Pandas.

    Args:
        input_path: Path to FWF file (local or S3)
        schema_file: JSON schema file with FWF specifications (required)
        **kwargs: Additional arguments passed to import_fwf

    Returns:
        DataFrameReader that can be converted to Polars or Pandas DataFrames

    Example:
        >>> import forklift as fl
        >>> df = fl.read_fwf("data.txt", "fwf_schema.json").as_polars()
        >>> df = fl.read_fwf("data.txt", "fwf_schema.json").as_pandas()
    """
    # Create temporary directory for output
    temp_dir = tempfile.mkdtemp(prefix="forklift_reader_")

    try:
        # Process FWF to Parquet
        results = import_fwf(
            input_path=input_path, output_path=temp_dir, schema_file=schema_file, **kwargs
        )

        # Return reader with the generated parquet files
        return DataFrameReader(_data_files(results), temp_dir)

    except Exception as e:
        # Clean up temp directory on error
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e


def read_sql(
    input_path: Union[str, Path], schema_file: Optional[Union[str, Path]] = None, **kwargs
) -> DataFrameReader:
    """
    Read SQL database and return a DataFrameReader for conversion to Polars/Pandas.

    Args:
        input_path: Database connection string
        schema_file: Optional JSON schema file for validation
        **kwargs: Additional arguments passed to import_sql

    Returns:
        DataFrameReader that can be converted to Polars or Pandas DataFrames

    Example:
        >>> import forklift as fl
        >>> df = fl.read_sql("postgresql://user:pass@host/db").as_polars()
        >>> df = fl.read_sql("sqlite:///data.db", "sql_schema.json").as_pandas()
    """
    # Create temporary directory for output
    temp_dir = tempfile.mkdtemp(prefix="forklift_reader_")

    try:
        # Process SQL to Parquet
        results = import_sql(
            connection_string=input_path,
            output_path=temp_dir,
            schema_file=schema_file,
            **kwargs,
        )

        # Return reader with the generated parquet files
        return DataFrameReader(_data_files(results), temp_dir)

    except Exception as e:
        # Clean up temp directory on error
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise e
