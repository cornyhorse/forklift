"""Import configuration class for Forklift engine."""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .enums import HeaderMode, ExcessColumnMode


@dataclass
class ImportConfig:
    """Configuration for data import operations.

    Args:
        input_path: Path to input file to process
        output_path: Directory where output files will be created
        schema_file: Optional path to JSON schema file for validation
        batch_size: Number of rows to process in each batch (default: 10000)
        encoding: Text encoding of the input file (default: utf-8)
        header_mode: How to handle header detection (default: PRESENT)
        header_search_rows: Maximum rows to search for header (default: 10)
        skip_blank_lines: Whether to skip blank lines during processing
        comment_rows: List of regex patterns for comment row detection
        footer_detection: Configuration for footer detection and stopping
        delimiter: Field delimiter character (default: comma)
        quote_char: Quote character for fields (default: double quote)
        escape_char: Escape character for special characters
        validate_schema: Whether to perform schema validation
        max_validation_errors: Maximum validation errors before stopping
        create_manifest: Whether to create manifest file
        create_metadata: Whether to create metadata file
        compression: Compression type for output files (default: snappy)
        excess_column_mode: How to handle rows with excess columns (default: TRUNCATE)
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
