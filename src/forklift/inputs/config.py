"""Configuration classes for input operations."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class CsvInputConfig:
    """Configuration for CSV input processing.

    Args:
        delimiter: Field delimiter character (default: comma)
        quote_char: Quote character for fields (default: double quote)
        escape_char: Escape character for special characters (default: None)
        encoding: Text encoding of the input file (default: utf-8)
        header_mode: How to handle header detection (default: present)
        header_search_rows: Maximum rows to search for header (default: 10)
        skip_blank_lines: Whether to skip blank lines during processing
        comment_patterns: List of regex patterns for comment row detection
        footer_detection: Configuration for footer detection and stopping
    """
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: Optional[str] = None
    encoding: str = "utf-8"
    header_mode: str = "present"  # present, absent, auto
    header_search_rows: int = 10
    skip_blank_lines: bool = True
    comment_patterns: Optional[List[str]] = None
    footer_detection: Optional[Dict[str, Any]] = None


@dataclass
class FwfFieldSpec:
    """Specification for a single fixed-width field.

    Args:
        name: Field name
        start: Starting position (1-based)
        length: Field length in characters
        align: Field alignment ('left', 'right', 'center')
        pad: Padding character
        parquet_type: Target Parquet data type
        required: Whether field is required
        trim: Whether to trim whitespace
    """
    name: str
    start: int
    length: int
    align: str = "left"
    pad: str = " "
    parquet_type: str = "string"
    required: bool = False
    trim: bool = True


@dataclass
class FwfConditionalSchema:
    """Conditional schema specification for FWF files.

    Args:
        flag_value: Value that triggers this schema
        description: Human-readable description
        fields: List of field specifications for this schema
    """
    flag_value: str
    description: str
    fields: List[FwfFieldSpec]


@dataclass
class FwfInputConfig:
    """Configuration for Fixed Width File input processing.

    Args:
        encoding: Text encoding of the input file (default: utf-8)
        fields: List of field specifications for standard FWF
        conditional_schemas: Configuration for conditional FWF processing
        flag_column: Specification for the flag column in conditional mode
        trim_whitespace: Global setting for trimming whitespace
        skip_blank_lines: Whether to skip blank lines during processing
        comment_patterns: List of regex patterns for comment row detection
        footer_detection: Configuration for footer detection and stopping
        null_values: Dictionary of null value representations
    """
    encoding: str = "utf-8"
    fields: Optional[List[FwfFieldSpec]] = None
    conditional_schemas: Optional[List[FwfConditionalSchema]] = None
    flag_column: Optional[FwfFieldSpec] = None
    trim_whitespace: bool = True
    skip_blank_lines: bool = True
    comment_patterns: Optional[List[str]] = None
    footer_detection: Optional[Dict[str, Any]] = None
    null_values: Optional[Dict[str, List[str]]] = None


@dataclass
class ExcelSheetConfig:
    """Configuration for a single Excel sheet.

    Args:
        select: Sheet selection criteria (name, index, or regex)
        columns: List of column mappings for this sheet
        header: Header configuration for this sheet
        data_start_row: Row number where data starts (1-based)
        data_end_row: Row number where data ends (1-based, optional)
        skip_blank_rows: Whether to skip blank rows
        name_override: Override name for this sheet in output
    """
    select: Dict[str, Any]
    columns: Optional[List[Dict[str, Any]]] = None
    header: Optional[Dict[str, Any]] = None
    data_start_row: Optional[int] = None
    data_end_row: Optional[int] = None
    skip_blank_rows: bool = True
    name_override: Optional[str] = None


@dataclass
class ExcelInputConfig:
    """Configuration for Excel input processing.

    Args:
        encoding: Text encoding to use for string conversion (default: utf-8)
        sheets: List of sheet configurations to process
        values_only: Whether to read only cell values (ignoring formulas)
        date_system: Excel date system ('1900' or '1904')
        nulls: Null value configuration (global and per-column)
        keep_default_na: Whether to keep default pandas NA values
        na_values: Additional values to treat as NA/null
        skip_blank_lines: Whether to skip completely blank lines
        engine: Excel engine to use ('openpyxl' for .xlsx, 'xlrd' for .xls)
    """
    encoding: str = "utf-8"
    sheets: List[ExcelSheetConfig] = None
    values_only: bool = True
    date_system: str = "1900"  # 1900 or 1904
    nulls: Optional[Dict[str, Any]] = None
    keep_default_na: bool = True
    na_values: Optional[List[str]] = None
    skip_blank_lines: bool = True
    engine: Optional[str] = None  # Auto-detect based on file extension


@dataclass
class SqlInputConfig:
    """Configuration for SQL database input processing.

    Args:
        connection_string: Database connection string (ODBC format)
        batch_size: Number of rows to fetch per batch (default: 10000)
        query_timeout: Query timeout in seconds (default: 300)
        connection_timeout: Connection timeout in seconds (default: 30)
        fetch_size: Database cursor fetch size for memory management
        null_values: Values to treat as NULL/None
        date_formats: Custom date format strings for parsing
        timestamp_formats: Custom timestamp format strings for parsing
        use_quoted_identifiers: Whether to quote table/column names in queries
        schema_name: Default schema name if not specified in patterns
        enable_streaming: Whether to use streaming cursor for large result sets
        connection_params: Additional connection parameters as key-value pairs
    """
    connection_string: str
    batch_size: int = 10000
    query_timeout: int = 300
    connection_timeout: int = 30
    fetch_size: Optional[int] = None
    null_values: Optional[List[str]] = None
    date_formats: Optional[List[str]] = None
    timestamp_formats: Optional[List[str]] = None
    use_quoted_identifiers: bool = False
    schema_name: Optional[str] = None
    enable_streaming: bool = True
    connection_params: Optional[Dict[str, Any]] = None
