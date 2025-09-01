"""CSV input handler for reading and preprocessing CSV files."""

from __future__ import annotations
import csv
import re
from pathlib import Path
from typing import List, Tuple

import pyarrow.csv as pv_csv

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

    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet library.

        Reads the first 10KB of the file to detect the most likely encoding.

        Args:
            file_path: Path to the CSV file to analyze

        Returns:
            Detected encoding string (defaults to utf-8 if detection fails)

        Note:
            Requires the chardet library to be installed for encoding detection.
        """
        import chardet

        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # Read first 10KB
            result = chardet.detect(raw_data)
            return result.get('encoding', 'utf-8')

    def find_header_row(self, file_path: Path) -> Tuple[int, List[str]]:
        """Find the header row and extract column names.

        Searches through the file to locate the header row based on the
        configured header mode and comment patterns.

        Args:
            file_path: Path to the CSV file to process

        Returns:
            Tuple of (header_row_index, column_names)

        Raises:
            ValueError: If no valid header row can be found
        """
        with open(file_path, 'r', encoding=self.config.encoding) as f:
            reader = csv.reader(f, delimiter=self.config.delimiter)

            for idx, row in enumerate(reader):
                if idx >= self.config.header_search_rows:
                    break

                if self._is_comment_row(row):
                    continue

                if self.config.skip_blank_lines and not any(cell.strip() for cell in row):
                    continue

                return idx, [col.strip() for col in row]

        raise ValueError("No valid header row found")

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
            if re.match(pattern, first_cell):
                return True

        return False

    def create_arrow_reader(self, file_path: Path, column_names: List[str], skip_rows: int = 0) -> pv_csv.CSVStreamingReader:
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
