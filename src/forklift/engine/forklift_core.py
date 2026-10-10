"""Core Forklift engine for streaming data import with PyArrow.

This module provides the core functionality for importing CSV files with PyArrow
streaming capabilities, including header detection, footer detection, validation,
and output generation. Now supports S3 streaming for both input and output.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

# Import extracted configuration classes
from .config import ExcessColumnMode, HeaderMode, ImportConfig, ProcessingResults

# Import exceptions
from .exceptions import ImportCancelled, ProcessingError

# Import format-specific importers
from .importers import ExcelImporter, SqlImporter
from .input_source import InputSource

# Import extracted processing components
from .processors import CSVProcessor
from .progress import CancelCallback, ImportHooks, ProgressCallback


class ForkliftCore:
    """Core engine for streaming data import with PyArrow.

    This class provides the main functionality for importing CSV files using
    PyArrow's streaming capabilities. It supports header detection, footer
    detection, schema validation, and various output formats.

    Args:
        config: ImportConfig instance with processing configuration
        progress: Called at every batch boundary with ``{"rows_read", "rows_rejected",
            "bytes_read"}`` (see ``forklift.engine.progress``)
        cancel: Asked after every batch whether to stop; True stops the import with
            ``ImportCancelled`` and discards its outputs
        input_source: Read the input from this :class:`~forklift.engine.input_source.InputSource`
            instead of opening ``config.input_path`` (which then only names the input)
    """

    def __init__(
        self,
        config: ImportConfig,
        *,
        progress: Optional[ProgressCallback] = None,
        cancel: Optional[CancelCallback] = None,
        input_source: Optional[InputSource] = None,
    ):
        """Initialize the ForkliftCore engine.

        Args:
            config: Configuration object containing processing parameters
            progress: Optional progress callback
            cancel: Optional cancellation callback
            input_source: Optional stream source for the input
        """
        self.config = config
        self.hooks = ImportHooks(progress, cancel)
        self.input_source = input_source
        self.csv_processor = CSVProcessor()

    def process_csv(self) -> ProcessingResults:
        """Process CSV file with streaming and validation.

        Main processing method that orchestrates the entire CSV import workflow
        including header detection, streaming processing, validation, and output generation.
        Now supports S3 streaming for both input and output.

        Returns:
            ProcessingResults object containing processing statistics and output paths

        Raises:
            Exception: Processing errors are recorded in ``results.errors`` and re-raised;
                      no partial output files are left behind (except that a run stopped by
                      the ``x-validation`` threshold keeps a finished ``bad_rows.parquet``)
        """
        return self.csv_processor.process(
            self.config, hooks=self.hooks, input_source=self.input_source
        )


# Public API functions
def import_csv(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    *,
    progress: Optional[ProgressCallback] = None,
    cancel: Optional[CancelCallback] = None,
    **kwargs,
) -> ProcessingResults:
    """Import CSV file with streaming and validation.

    High-level API function for importing CSV files using PyArrow streaming.
    Supports header detection, footer detection, schema validation, and various
    output formats including parquet files and metadata. Now supports S3 streaming
    for both input and output.

    Args:
        input_path: Path to input CSV file to process (local or S3 URI)
        output_path: Directory where output files will be created (local or S3 URI)
        schema_file: Optional path to JSON schema file for validation (local or S3 URI)
        progress: Called at every batch boundary with ``{"rows_read", "rows_rejected",
            "bytes_read"}``
        cancel: Asked after every batch whether to stop; returning True raises
            :class:`~forklift.engine.exceptions.ImportCancelled` and discards the outputs
        **kwargs: Additional configuration options passed to ImportConfig (``header_mode`` and
            ``excess_column_mode`` accept the enum or a string such as ``"absent"``;
            ``s3_client`` is the S3 client for ``s3://`` paths)

    Returns:
        ProcessingResults object containing statistics and output file paths

    Examples:
        Basic CSV import::

            results = import_csv("data.csv", "output/")

        With schema validation::

            results = import_csv(
                input_path="data.csv",
                output_path="output/",
                schema_file="schema.json"
            )

        S3 to S3 processing::

            results = import_csv(
                input_path="s3://bucket/data.csv",
                output_path="s3://bucket/output/",
                schema_file="s3://bucket/schema.json"
            )

        With footer detection::

            results = import_csv(
                input_path="data.csv",
                output_path="output/",
                footer_detection={"stop_on_blank": True}
            )

        An S3-compatible store, with progress reporting::

            from forklift.io import S3StreamingClient

            results = import_csv(
                "s3://bucket/data.csv",
                "s3://bucket/output/",
                s3_client=S3StreamingClient(endpoint_url="https://store.example.org"),
                progress=lambda event: print(event["rows_read"]),
            )
    """
    config = ImportConfig(
        input_path=input_path, output_path=output_path, schema_file=schema_file, **kwargs
    )

    engine = ForkliftCore(config, progress=progress, cancel=cancel)
    return engine.process_csv()


def import_fwf(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> ProcessingResults:
    """Import Fixed Width File (placeholder for future implementation).

    Args:
        input_path: Path to input FWF file (local or S3 URI)
        output_path: Directory for output files (local or S3 URI)
        schema_file: Optional JSON schema file (local or S3 URI)
        **kwargs: Additional configuration options

    Returns:
        ProcessingResults object

    Raises:
        NotImplementedError: This function is not yet implemented
    """
    raise NotImplementedError("FWF import not yet implemented")


def import_excel(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> ProcessingResults:
    """Import Excel file with multi-sheet support.

    Processes Excel files (.xlsx and .xls) with support for multiple sheets,
    custom column mappings, header detection, and data range specification.
    Uses an efficient approach of opening the file once and streaming sheets
    from the already opened workbook.

    Args:
        input_path: Path to input Excel file (local or S3 URI)
        output_path: Directory for output files (local or S3 URI)
        schema_file: Optional JSON schema file (local or S3 URI)
        **kwargs: Additional configuration options including:
            - sheet: Specific sheet name/index to process (overrides schema)
            - values_only: Read only cell values, ignoring formulas (default: True)
            - engine: Excel engine to use ('openpyxl' or 'xlrd', auto-detected)
            - date_system: Excel date system ('1900' or '1904', default: '1900')
            - s3_client: Optional client for ``s3://`` inputs, schemas and outputs
            - progress / cancel: Called after every sheet (see ``import_csv``)

    Returns:
        ProcessingResults object containing processing statistics and metadata

    Raises:
        FileNotFoundError: If input file doesn't exist
        ValueError: If Excel file format is unsupported or configuration is invalid
        ImportError: If required Excel engine libraries are not installed
        ProcessingError: If data processing fails
    """
    return ExcelImporter.import_excel(input_path, output_path, schema_file, **kwargs)


def import_sql(
    connection_string: str,
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> ProcessingResults:
    """Import data from SQL database with ODBC connectivity.

    Processes data from SQL databases (SQLite, PostgreSQL, MySQL, Oracle, SQL Server, etc.)
    using ODBC connections. Supports explicit table specification through schema files
    with one-to-one schema/table mapping for predictable configuration.

    Args:
        connection_string: ODBC connection string for database
        output_path: Directory for output files (local or S3 URI)
        schema_file: Required JSON schema file specifying tables to import (local or S3 URI)
        **kwargs: Additional configuration options including:
            - batch_size: Number of rows to fetch per batch (default: 10000)
            - query_timeout: Query timeout in seconds (default: 300)
            - connection_timeout: Connection timeout in seconds (default: 30)
            - use_quoted_identifiers: Accepted for compatibility; identifiers are always
              validated against the catalog and quoted
            - schema_name: Default schema name if not specified in table configs
            - enable_streaming: Whether to use streaming cursor (default: True)
            - null_values: Values to treat as NULL/None
            - continue_on_error: Return the results instead of raising when some tables
              fail (default: False; failed tables are listed in ``results.errors``)
            - s3_client: Optional client used when ``output_path`` is an S3 URI
            - progress / cancel: Called after every batch (see ``import_csv``); a cancelled
              import stops at once and keeps none of its tables

    Returns:
        ProcessingResults object containing processing statistics and metadata

    Raises:
        ImportError: If pyodbc is not installed
        ConnectionError: If database connection fails
        ProcessingError: If any table fails to process (partial results are attached as
            ``error.results``) or no schema file is provided
        ValueError: If schema file doesn't specify any tables

    Examples:
        Basic SQLite import with schema::

            results = import_sql(
                connection_string="Driver={SQLite3 ODBC Driver};Database=test.db",
                output_path="output/",
                schema_file="sql_schema.json"
            )

        PostgreSQL with custom configuration::

            results = import_sql(
                connection_string=(
                    "Driver={PostgreSQL ODBC Driver};Server=localhost;"
                    "Database=mydb;Uid=user;Pwd=pass"
                ),
                output_path="output/",
                schema_file="pg_schema.json",
                batch_size=5000,
                use_quoted_identifiers=True
            )

    Schema file example::

        {
          "x-sql": {
            "tables": [
              {
                "select": {
                  "schema": "public",
                  "name": "users"
                },
                "outputName": "users_data"
              },
              {
                "select": {
                  "schema": "sales",
                  "name": "orders"
                }
              }
            ]
          }
        }
    """
    return SqlImporter.import_sql(connection_string, output_path, schema_file, **kwargs)


# Re-export for backwards compatibility with tests
__all__ = [
    "ForkliftCore",
    "ProcessingError",
    "ImportCancelled",
    "import_csv",
    "import_fwf",
    "import_excel",
    "import_sql",
    "ImportConfig",
    "ProcessingResults",
    "HeaderMode",
    "ExcessColumnMode",
]
