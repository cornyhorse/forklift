"""CSV input handler for reading and preprocessing CSV files."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, List, Pattern, Tuple

import pyarrow.csv as pv_csv

from ..utils.detect_encoding import detect_encoding as _detect_file_encoding
from .config import CsvInputConfig


class CsvInputHandler:
    """Handles CSV file input with header detection and preprocessing.

    This class provides functionality for reading CSV files with various
    configurations including header detection, comment handling, and
    encoding detection.

    Args:
        config: CsvInputConfig instance with processing configuration

    Attributes:
        config: The configuration object for this input handler
    """

    def __init__(self, config: CsvInputConfig):
        """Initialize the CSV input handler.

        Args:
            config: Configuration object containing CSV processing parameters
        """
        self.config = config
        self._regex_cache: Dict[str, Pattern[str]] = {}
        # Compile now so a malformed pattern is a configuration error, not a per-row failure
        for pattern in config.comment_patterns or []:
            self._compile(pattern)

    def _compile(self, pattern: str) -> Pattern[str]:
        """Compile (and cache) a configured comment regex.

        Raises:
            ValueError: If the pattern is not a valid regular expression
        """
        compiled = self._regex_cache.get(pattern)
        if compiled is None:
            try:
                compiled = re.compile(pattern)
            except (re.error, TypeError) as e:
                raise ValueError(
                    f"Invalid regular expression in comment_patterns: {pattern!r} ({e})"
                )
            self._regex_cache[pattern] = compiled
        return compiled

    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding.

        Uses chardet (or charset_normalizer) when installed, then confirms the guess by
        decoding the whole file; without either library utf-8, cp1252 and latin-1 are tried.

        Args:
            file_path: Path to the CSV file to analyze

        Returns:
            Detected encoding string (defaults to utf-8 if detection fails)
        """
        return _detect_file_encoding(file_path)

    def find_header_row(self, file_path: Path) -> Tuple[int, List[str]]:
        """Find the header row and extract column names.

        Honours ``header_mode``: ``absent`` returns ``(-1, [])`` (there is no header); with
        ``present`` the header is the first row that is neither a comment nor blank; with
        ``auto`` the first rows (up to ``header_search_rows``) are scanned for one that looks
        like a header (mostly text), falling back to the first candidate row. Quote and
        escape characters from the configuration are used when splitting rows.

        Args:
            file_path: Path to the CSV file to process

        Returns:
            Tuple of (header_row_index, column_names)

        Raises:
            ValueError: If no valid header row can be found
        """
        if self.config.header_mode == "absent":
            return -1, []

        # utf-8-sig drops a byte order mark, which would otherwise end up in the first name
        encoding = self.config.encoding
        if encoding and encoding.lower().replace("_", "-") in ("utf-8", "utf8"):
            encoding = "utf-8-sig"

        reader_kwargs = {"delimiter": self.config.delimiter}
        if self.config.quote_char:
            reader_kwargs["quotechar"] = self.config.quote_char
        else:
            reader_kwargs["quoting"] = csv.QUOTE_NONE
        if self.config.escape_char:
            reader_kwargs["escapechar"] = self.config.escape_char

        candidates: List[Tuple[int, List[str]]] = []
        with open(file_path, "r", encoding=encoding, newline="") as f:
            for idx, row in enumerate(csv.reader(f, **reader_kwargs)):
                if idx >= self.config.header_search_rows:
                    break

                if self._is_comment_row(row):
                    continue

                # A row without any content cannot be a header, whatever skip_blank_lines
                # (which concerns data rows) says
                if not any(cell.strip() for cell in row):
                    continue

                names = [col.strip() for col in row]
                if self.config.header_mode != "auto":
                    return idx, names
                candidates.append((idx, names))

        for idx, names in candidates:
            if self._looks_like_header(names):
                return idx, names
        if candidates:
            return candidates[0]

        raise ValueError("No valid header row found")

    @staticmethod
    def _looks_like_header(cells: List[str]) -> bool:
        """True when a row has more text cells than numeric ones."""
        text = numbers = 0
        for cell in cells:
            if not cell:
                continue
            try:
                float(cell)
                numbers += 1
            except ValueError:
                text += 1
        return text > numbers

    def _is_comment_row(self, row: List[str]) -> bool:
        """Check if row should be treated as a comment.

        Tests the first cell of the row against configured comment patterns
        to determine if the entire row should be skipped.

        Args:
            row: List of cell values from a CSV row

        Returns:
            True if row matches a comment pattern, False otherwise
        """
        if not self.config.comment_patterns or not row:
            return False

        first_cell = row[0].strip() if row else ""

        for pattern in self.config.comment_patterns:
            if self._compile(pattern).match(first_cell):
                return True

        return False

    def create_arrow_reader(
        self, file_path: Path, column_names: List[str], skip_rows: int = 0
    ) -> pv_csv.CSVStreamingReader:
        """Create PyArrow CSV streaming reader.

        Sets up a PyArrow CSV streaming reader with the configured options
        for efficient processing of large CSV files.

        Args:
            file_path: Path to the CSV file to read
            column_names: List of column names for the CSV
            skip_rows: Number of rows to skip from the beginning (default: 0)

        Returns:
            PyArrow CSVStreamingReader configured for the file
        """
        parse_options = pv_csv.ParseOptions(
            delimiter=self.config.delimiter,
            quote_char=self.config.quote_char,
            escape_char=self.config.escape_char,
        )

        read_options = pv_csv.ReadOptions(
            encoding=self.config.encoding,
            skip_rows=skip_rows,
            column_names=column_names,
        )

        convert_options = pv_csv.ConvertOptions(
            check_utf8=False,
        )

        return pv_csv.open_csv(
            file_path,
            parse_options=parse_options,
            read_options=read_options,
            convert_options=convert_options,
        )
