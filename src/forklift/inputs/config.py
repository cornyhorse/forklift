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
