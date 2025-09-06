"""Core Forklift engine for streaming data import with PyArrow.

This module provides the core functionality for importing CSV files with PyArrow
streaming capabilities, including header detection, footer detection, validation,
and output generation. Now supports S3 streaming for both input and output.
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Union

# Import extracted configuration classes
from .config import ImportConfig, ProcessingResults, HeaderMode, ExcessColumnMode

# Import extracted processing components
from .processors import CSVProcessor

# Import format-specific importers
from .importers import ExcelImporter, SqlImporter

# Import exceptions
from .exceptions import ProcessingError


class ForkliftCore:
    """Core engine for streaming data import with PyArrow.

    This class provides the main functionality for importing CSV files using
    PyArrow's streaming capabilities. It supports header detection, footer
    detection, schema validation, and various output formats.

    Args:
        config: ImportConfig instance with processing configuration
    """

    def __init__(self, config: ImportConfig):
        """Initialize the ForkliftCore engine.

        Args:
            config: Configuration object containing processing parameters
        """
        self.config = config
        self.csv_processor = CSVProcessor()

    def process_csv(self) -> ProcessingResults:
        """Process CSV file with streaming and validation.

        Main processing method that orchestrates the entire CSV import workflow
        including header detection, streaming processing, validation, and output generation.
        Now supports S3 streaming for both input and output.

        Returns:
            ProcessingResults object containing processing statistics and output paths

        Raises:
            Exception: Various exceptions may be raised during processing,
                      all are captured in the results.errors list
        """
        return self.csv_processor.process(self.config)


# Public API functions
def import_csv(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
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
        **kwargs: Additional configuration options passed to ImportConfig

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
    """
    config = ImportConfig(
        input_path=input_path,
        output_path=output_path,
        schema_file=schema_file,
        **kwargs
    )

    engine = ForkliftCore(config)
    return engine.process_csv()


def import_fwf(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs
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
    **kwargs
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
    **kwargs
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
            - use_quoted_identifiers: Whether to quote table/column names (default: False)
            - schema_name: Default schema name if not specified in table configs
            - enable_streaming: Whether to use streaming cursor (default: True)
            - null_values: Values to treat as NULL/None

    Returns:
        ProcessingResults object containing processing statistics and metadata

    Raises:
        ImportError: If pyodbc is not installed
        ConnectionError: If database connection fails
        ProcessingError: If data processing fails or no schema file provided
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
                connection_string="Driver={PostgreSQL ODBC Driver};Server=localhost;Database=mydb;Uid=user;Pwd=pass",
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
    'ForkliftCore',
    'ProcessingError',
    'import_csv',
    'import_fwf',
    'import_excel',
    'import_sql',
    'ImportConfig',
    'ProcessingResults',
    'HeaderMode',
    'ExcessColumnMode'
]
