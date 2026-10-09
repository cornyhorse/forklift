"""Configuration classes for input operations."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def _footer_patterns(footer_detection: Optional[Dict[str, Any]]) -> List[str]:
    """Return the footer regex (as a list) when ``footer_detection`` is in regex mode."""
    if footer_detection and footer_detection.get("mode") == "regex":
        pattern = footer_detection.get("pattern")
        if pattern:
            return [pattern]
    return []


def _validate_regexes(patterns: Optional[List[str]], option: str) -> None:
    """Compile each regex once so a malformed pattern fails at config time.

    Args:
        patterns: Regex pattern strings (``None`` or empty is fine)
        option: Name of the option being validated, used in the error message

    Raises:
        ValueError: If a pattern is not a valid regular expression
    """
    for pattern in patterns or []:
        try:
            re.compile(pattern)
        except (re.error, TypeError) as e:
            raise ValueError(f"Invalid regular expression in {option}: {pattern!r} ({e})") from e


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

    def __post_init__(self) -> None:
        _validate_regexes(self.comment_patterns, "comment_patterns")
        _validate_regexes(_footer_patterns(self.footer_detection), "footer_detection.pattern")


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
        required: Whether field is required (a blank value is recorded as a parse error)
        trim: Whether to trim whitespace (also requires ``FwfInputConfig.trim_whitespace``)
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

    def __post_init__(self) -> None:
        _validate_regexes(self.comment_patterns, "comment_patterns")
        _validate_regexes(_footer_patterns(self.footer_detection), "footer_detection.pattern")


@dataclass
class ExcelSheetConfig:
    """Configuration for a single Excel sheet.

    Row numbers are 1-based sheet rows (as shown in Excel). ``header["row"]`` is the
    exception: it is a 0-based offset, so ``{"row": 0}`` is sheet row 1.

    Args:
        select: Sheet selection criteria (name, index (0-based), or regex)
        columns: Column mappings (``name``, ``position`` as letter or 1-based number,
            optional ``parquetType``); when given, only these columns are returned
        header: Header configuration: ``mode`` (present/absent/auto, default present),
            ``row`` (0-based), ``override`` (list of column names)
        data_start_row: First sheet row of data, inclusive (1-based); never earlier than
            the row after the header
        data_end_row: Last sheet row of data, inclusive (1-based, optional)
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

    def __post_init__(self) -> None:
        if isinstance(self.select, dict) and self.select.get("regex") is not None:
            _validate_regexes([self.select["regex"]], "select.regex")


@dataclass
class ExcelInputConfig:
    """Configuration for Excel input processing.

    Args:
        encoding: Text encoding to use for string conversion (default: utf-8)
        sheets: List of sheet configurations to process
        values_only: Whether to read cached cell values instead of formulas
        date_system: Excel date system ('1900' or '1904'); the workbook's own setting wins
        nulls: Null value configuration (``global`` list and ``perColumn`` dict of lists)
        keep_default_na: Whether empty cells/strings are treated as null in addition to
            ``nulls``/``na_values`` (the only built-in null token is the empty string)
        na_values: Additional values to treat as NA/null
        skip_blank_lines: Whether to skip completely blank lines
        engine: Excel engine to use ('openpyxl' for .xlsx, 'xlrd' for .xls)
        max_rows: Maximum rows read from one sheet (clear error when exceeded)
        max_cells: Maximum cells read from one sheet (clear error when exceeded)
        max_uncompressed_bytes: Maximum total uncompressed size of an .xlsx archive
        max_compression_ratio: Maximum uncompressed/compressed ratio of an .xlsx archive
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
    max_rows: int = 1_048_576  # Excel's own per-sheet row limit
    max_cells: int = 10_000_000
    max_uncompressed_bytes: int = 1024 * 1024 * 1024
    max_compression_ratio: float = 200.0


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
        use_quoted_identifiers: Accepted for backward compatibility only. Table and schema
            names are always quoted (and validated against the catalog) because they
            are interpolated into SQL text.
        schema_name: Default schema name if not specified in patterns
        enable_streaming: Whether to use streaming cursor for large result sets
        connection_params: Additional connection parameters as key-value pairs
        read_only: Ask the driver for a read-only connection (default: True)

    ``connection_string`` and ``connection_params`` may contain credentials and are
    excluded from ``repr()``.
    """

    connection_string: str = field(repr=False)
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
    connection_params: Optional[Dict[str, Any]] = field(default=None, repr=False)
    read_only: bool = True
