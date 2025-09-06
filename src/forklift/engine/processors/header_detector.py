"""Header detection logic for CSV processing."""

from __future__ import annotations
import re
from pathlib import Path
from typing import List, Tuple, Union

from ..config import HeaderMode, ImportConfig
from ...io import UnifiedIOHandler


class HeaderDetector:
    """Handles header detection for CSV files with various modes and patterns."""

    def __init__(self, config: ImportConfig, io_handler: UnifiedIOHandler):
        """Initialize header detector with configuration.

        Args:
            config: Import configuration with header detection settings
            io_handler: Unified I/O handler for file operations
        """
        self.config = config
        self.io_handler = io_handler

    def detect_header_row(self, input_path: Union[str, Path]) -> Tuple[int, List[str]]:
        """Detect header row location and extract column names.

        Uses the configured header mode to determine how to find and extract
        column names from the input file (local or S3).

        Args:
            input_path: Path to the input CSV file (local or S3 URI)

        Returns:
            Tuple of (header_row_index, column_names)

        Raises:
            ValueError: If header detection fails and no fallback is available
        """
        if self.config.header_mode == HeaderMode.ABSENT:
            # No header, use schema or generate names
            return -1, []

        elif self.config.header_mode == HeaderMode.PRESENT:
            # Header is expected at first non-comment row
            header_idx, columns = self._find_first_data_row(input_path)
            return header_idx, columns

        else:  # AUTO mode
            return self._auto_detect_header(input_path)

    def _find_first_data_row(self, input_path: Union[str, Path]) -> Tuple[int, List[str]]:
        """Find the first non-comment row and extract columns.

        Searches through the file to find the first row that is not a comment
        or blank line, treating it as the header row. Works with local files and S3.

        Args:
            input_path: Path to the input CSV file (local or S3 URI)

        Returns:
            Tuple of (row_index, column_names). Returns (-1, []) for empty files.
        """
        # Use unified I/O handler for S3 and local file support
        for idx, row in enumerate(self.io_handler.csv_reader(
            input_path,
            delimiter=self.config.delimiter,
            encoding=self.config.encoding
        )):
            if idx >= self.config.header_search_rows:
                break

            # Skip completely empty rows
            if not row:
                continue

            # Check for comment rows (lines starting with #)
            if row and row[0].strip().startswith('#'):
                continue

            if self._is_comment_row(row):
                continue

            if self.config.skip_blank_lines and not any(cell.strip() for cell in row):
                continue

            return idx, [col.strip() for col in row]

        # Handle empty files gracefully
        return -1, []

    def _auto_detect_header(self, input_path: Union[str, Path]) -> Tuple[int, List[str]]:
        """Auto-detect header row by looking for text patterns.

        Analyzes the first several rows to identify which one looks most like
        a header based on the ratio of text to numeric content. Works with local files and S3.

        Args:
            input_path: Path to the input CSV file (local or S3 URI)

        Returns:
            Tuple of (header_row_index, column_names)

        Raises:
            ValueError: If no suitable header row can be detected
        """
        rows = []

        # Use unified I/O handler for S3 and local file support
        for idx, row in enumerate(self.io_handler.csv_reader(
            input_path,
            delimiter=self.config.delimiter,
            encoding=self.config.encoding
        )):
            if idx >= self.config.header_search_rows:
                break

            if self._is_comment_row(row):
                continue

            rows.append((idx, row))

        # Look for a row that looks like headers (mostly text, few numbers)
        for idx, row in rows:
            if self._looks_like_header(row):
                return idx, [col.strip() for col in row]

        # Default to first row
        if rows:
            return rows[0][0], [col.strip() for col in rows[0][1]]

        # Handle empty files gracefully - return no header and empty columns
        return -1, []

    def _looks_like_header(self, row: List[str]) -> bool:
        """Determine if a row looks like a header row.

        Analyzes the content of a row to determine if it appears to be a header
        based on the ratio of text content to numeric content.

        Args:
            row: List of cell values from a CSV row

        Returns:
            True if row appears to be a header, False otherwise
        """
        if not row:
            return False

        text_count = 0
        number_count = 0

        for cell in row:
            cell = cell.strip()
            if not cell:
                continue

            try:
                float(cell)
                number_count += 1
            except ValueError:
                text_count += 1

        # Header likely if mostly text
        return text_count > number_count

    def _is_comment_row(self, row: List[str]) -> bool:
        """Check if row should be treated as a comment.

        Tests the first cell of the row against configured comment patterns
        to determine if the entire row should be skipped.

        Args:
            row: List of cell values from a CSV row

        Returns:
            True if row matches a comment pattern, False otherwise
        """
        if not self.config.comment_rows or not row:
            return False

        first_cell = row[0].strip() if row else ""

        for comment_pattern in self.config.comment_rows:
            if re.match(comment_pattern, first_cell):
                return True

        return False

    def should_stop_for_footer(self, row: List[str]) -> bool:
        """Check if we should stop processing due to footer detection.

        Tests the row against configured footer detection rules to determine
        if processing should stop before this row.

        Args:
            row: List of cell values from a CSV row

        Returns:
            True if footer detected and processing should stop, False otherwise
        """
        if not self.config.footer_detection:
            return False

        detection = self.config.footer_detection

        # Check for blank row stopping
        if detection.get("stop_on_blank", False):
            # Handle completely empty rows or rows with only empty strings
            if not row or not any(cell.strip() for cell in row):
                return True

        # Check for pattern in specific column
        if "column_index" in detection and "patterns" in detection:
            col_idx = detection["column_index"]
            if 0 <= col_idx < len(row):
                cell_value = row[col_idx].strip()
                for pattern in detection["patterns"]:
                    if re.match(pattern, cell_value):
                        return True

        return False
