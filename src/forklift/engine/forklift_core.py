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

    # Delegation methods for backwards compatibility with tests
    def _load_schema(self):
        """Delegate to schema processor."""
        schema_processor = SchemaProcessor(self.config, UnifiedIOHandler())
        return schema_processor.load_schema()

    def _json_schema_to_pyarrow(self, schema_dict):
        """Delegate to schema processor."""
        schema_processor = SchemaProcessor(self.config, UnifiedIOHandler())
        return schema_processor._json_schema_to_pyarrow(schema_dict)

    def _json_type_to_pyarrow(self, field_def):
        """Delegate to schema processor."""
        schema_processor = SchemaProcessor(self.config, UnifiedIOHandler())
        return schema_processor._json_type_to_pyarrow(field_def)

    def _detect_header_row(self, input_path):
        """Delegate to header detector."""
        io_handler = UnifiedIOHandler()
        header_detector = HeaderDetector(self.config, io_handler)
        return header_detector.detect_header_row(input_path)

    def _find_first_data_row(self, input_path):
        """Delegate to header detector."""
        io_handler = UnifiedIOHandler()
        header_detector = HeaderDetector(self.config, io_handler)
        return header_detector._find_first_data_row(input_path)

    def _auto_detect_header(self, input_path):
        """Delegate to header detector."""
        io_handler = UnifiedIOHandler()
        header_detector = HeaderDetector(self.config, io_handler)
        return header_detector._auto_detect_header(input_path)

    def _looks_like_header(self, row):
        """Delegate to header detector."""
        io_handler = UnifiedIOHandler()
        header_detector = HeaderDetector(self.config, io_handler)
        return header_detector._looks_like_header(row)

    def _is_comment_row(self, row):
        """Delegate to header detector."""
        io_handler = UnifiedIOHandler()
        header_detector = HeaderDetector(self.config, io_handler)
        return header_detector._is_comment_row(row)

    def _should_stop_for_footer(self, row):
        """Delegate to header detector."""
        io_handler = UnifiedIOHandler()
        header_detector = HeaderDetector(self.config, io_handler)
        return header_detector.should_stop_for_footer(row)

    def _create_batch_reader(self, file_path, column_names=None, header_row_index=None):
        """Delegate to batch processor."""
        io_handler = UnifiedIOHandler()
        batch_processor = BatchProcessor(self.config, io_handler)

        # Set default values if not provided
        if column_names is None:
            column_names = getattr(self, 'column_names', [])
        if header_row_index is None:
            header_row_index = getattr(self, 'header_row_index', 0)

        footer_detector_func = self._should_stop_for_footer
        return batch_processor.create_batch_reader(file_path, column_names, header_row_index, footer_detector_func)

    def _create_s3_batch_reader(self, input_path, column_names=None, header_row_index=None):
        """Delegate to batch processor."""
        io_handler = UnifiedIOHandler()
        batch_processor = BatchProcessor(self.config, io_handler)

        # Set default values if not provided
        if column_names is None:
            column_names = getattr(self, 'column_names', [])
        if header_row_index is None:
            header_row_index = getattr(self, 'header_row_index', 0)

        footer_detector_func = self._should_stop_for_footer
        return batch_processor.create_s3_batch_reader(input_path, column_names, header_row_index, footer_detector_func)

    def _handle_column_mismatch_reader(self, file_path, skip_rows, column_names=None):
        """Delegate to batch processor."""
        io_handler = UnifiedIOHandler()
        batch_processor = BatchProcessor(self.config, io_handler)

        # Set default values if not provided
        if column_names is None:
            column_names = getattr(self, 'column_names', [])

        return batch_processor._handle_column_mismatch_reader(file_path, skip_rows, column_names)

    def _convert_rows_to_batch(self, rows, num_columns, column_names=None):
        """Delegate to batch processor."""
        io_handler = UnifiedIOHandler()
        batch_processor = BatchProcessor(self.config, io_handler)

        # Set default values if not provided
        if column_names is None:
            column_names = getattr(self, 'column_names', [f'col_{i}' for i in range(num_columns)])

        return batch_processor._convert_rows_to_batch(rows, num_columns, column_names)

    def _create_filtered_file(self, file_path, skip_rows):
        """Delegate to batch processor."""
        io_handler = UnifiedIOHandler()
        batch_processor = BatchProcessor(self.config, io_handler)
        footer_detector_func = self._should_stop_for_footer
        return batch_processor._create_filtered_file(file_path, skip_rows, footer_detector_func)

    def _validate_batch(self, batch):
        """Delegate to CSV processor for validation."""
        # Create a temporary CSV processor for validation
        csv_processor = CSVProcessor()
        csv_processor.schema_processor = SchemaProcessor(self.config, UnifiedIOHandler())
        schema = csv_processor.schema_processor.load_schema()
        return csv_processor._validate_batch(batch, schema, self.config)

    def _process_batch_with_transformations(self, batch):
        """Delegate to CSV processor."""
        csv_processor = CSVProcessor()
        csv_processor.schema_processor = SchemaProcessor(self.config, UnifiedIOHandler())
        if csv_processor.schema_processor.has_row_hash_config():
            from ...processors.row_hash_factory import create_row_hash_processor_from_schema
            row_hash_config = csv_processor.schema_processor.get_row_hash_config()
            if row_hash_config:
                row_hash_processor = create_row_hash_processor_from_schema(row_hash_config)
                if row_hash_processor:
                    processed_batch, validation_results = row_hash_processor.process_batch(batch)
                    return processed_batch
        return batch

    def _write_batch_to_parquet(self, batch, writer):
        """Delegate to CSV processor."""
        csv_processor = CSVProcessor()
        return csv_processor._write_batch_to_parquet(batch, writer)

    def _create_manifest(self, output_path, files):
        """Delegate to CSV processor."""
        csv_processor = CSVProcessor()
        csv_processor.io_handler = UnifiedIOHandler()
        return csv_processor._create_s3_manifest(output_path, files)

    def _create_metadata(self, output_path, results):
        """Delegate to CSV processor."""
        csv_processor = CSVProcessor()
        csv_processor.io_handler = UnifiedIOHandler()
        csv_processor.schema_processor = SchemaProcessor(self.config, UnifiedIOHandler())
        return csv_processor._create_s3_metadata(output_path, results)

    # Import necessary classes for delegation
    @property
    def io_handler(self):
        """Get unified IO handler."""
        return UnifiedIOHandler()


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
