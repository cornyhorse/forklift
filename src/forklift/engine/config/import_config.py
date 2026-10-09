"""Import configuration class for Forklift engine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from .enums import ExcessColumnMode, HeaderMode

_E = TypeVar("_E", bound=Enum)


def _coerce_enum(enum_cls: Type[_E], value: Any, field_name: str) -> _E:
    """Convert a string (case-insensitive value or member name) into an enum member.

    Args:
        enum_cls: Enum class to convert to
        value: Enum member or string such as ``"present"`` / ``"PRESENT"``
        field_name: Name of the config field (used in the error message)

    Returns:
        The matching enum member

    Raises:
        ValueError: If the value is not a member of the enum
    """
    if isinstance(value, enum_cls):
        return value

    # Accept members of an equivalent enum (e.g. imported through another module path)
    raw = getattr(value, "value", value)
    if isinstance(raw, str):
        key = raw.strip().lower()
        for member in enum_cls:
            if key in (str(member.value).lower(), member.name.lower()):
                return member

    valid = ", ".join(repr(member.value) for member in enum_cls)
    raise ValueError(f"Invalid {field_name} {value!r}; valid values are: {valid}")


@dataclass
class ImportConfig:
    """Configuration for data import operations.

    ``header_mode`` and ``excess_column_mode`` accept either the enum member or its string
    value (case-insensitive, e.g. ``"absent"``); unknown values raise ``ValueError``.

    Args:
        input_path: Path to input file to process
        output_path: Directory where output files will be created
        schema_file: Optional path to JSON schema file for validation. When given, the
            schema's column types (``x-csv.parquetTypeMapping`` first, then the JSON type and
            format) are applied to the output; values that cannot be converted send their
            row to bad_rows.parquet. Columns not in the schema keep Arrow type inference
            on the local path and stay strings on the S3 path.
        batch_size: Upper bound on rows per batch handed to the Parquet writer (default:
            10000). The local Arrow reader produces batches by byte size (~1 MiB) and they are
            only split down to this size, so smaller batches can occur; the S3 reader and the
            fallback reader buffer exactly this many rows.
        encoding: Text encoding of the input file (default: utf-8). A UTF-8 BOM is ignored.
        header_mode: How to handle header detection (default: PRESENT)
        header_search_rows: Maximum rows to scan for the header (default: 10); a ValueError
            is raised if no header row is found inside that window
        skip_blank_lines: Only used while locating the header row (rows made up solely of
            delimiters/whitespace above the header are skipped). Completely empty lines in the
            data section are always skipped, whatever this is set to.
        comment_rows: List of regex patterns for comment rows. Only used while locating the
            header row: matching rows above the header are skipped; lines below the header
            are never treated as comments. With the default (None) a row that is a single
            ``#...`` cell counts as a comment; ``[]`` turns comment detection off.
        footer_detection: Configuration for footer detection and stopping
        delimiter: Field delimiter character (default: comma)
        quote_char: Quote character for fields (default: double quote)
        escape_char: Escape character for special characters (default: none)
        validate_schema: Whether to enforce the schema's ``required`` columns (null or empty
            string values send the row to bad_rows.parquet; a required column missing from the
            input raises ValueError)
        max_validation_errors: Reserved. Currently not enforced: every invalid row is written
            to bad_rows.parquet and processing continues.
        create_manifest: Whether to create manifest file
        create_metadata: Whether to create metadata file
        compression: Compression type for output files (default: snappy)
        excess_column_mode: How to handle rows with excess columns (default: TRUNCATE)
        include_value_statistics: Whether metadata may contain statistics that expose actual
            values (top/bottom values, min/max, quantiles, samples). Default False.
    """

    input_path: Union[str, Path]
    output_path: Union[str, Path]
    schema_file: Optional[Union[str, Path]] = None
    batch_size: int = 10000
    encoding: str = "utf-8"
    header_mode: HeaderMode = HeaderMode.PRESENT
    header_search_rows: int = 10
    skip_blank_lines: bool = True
    comment_rows: Optional[List[str]] = None  # Patterns to skip as comments
    footer_detection: Optional[Dict[str, Any]] = None

    # CSV specific
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: Optional[str] = None

    # Row handling options
    excess_column_mode: ExcessColumnMode = ExcessColumnMode.TRUNCATE

    # Validation options
    validate_schema: bool = True
    max_validation_errors: int = 1000

    # Output options
    create_manifest: bool = True
    create_metadata: bool = True
    compression: str = "snappy"
    include_value_statistics: bool = False

    def __post_init__(self) -> None:
        """Coerce string enum values and validate them."""
        self.header_mode = _coerce_enum(HeaderMode, self.header_mode, "header_mode")
        self.excess_column_mode = _coerce_enum(
            ExcessColumnMode, self.excess_column_mode, "excess_column_mode"
        )
