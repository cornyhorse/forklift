"""Header detection logic for CSV processing."""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Iterator, List, Optional, Tuple, Union

from ...io import UnifiedIOHandler
from ..config import HeaderMode, ImportConfig
from ..input_source import CountingReader, InputSource
from .text_utils import read_encoding


class HeaderDetector:
    """Handles header detection for CSV files with various modes and patterns."""

    def __init__(
        self,
        config: ImportConfig,
        io_handler: UnifiedIOHandler,
        input_source: Optional[InputSource] = None,
    ):
        """Initialize header detector with configuration.

        Args:
            config: Import configuration with header detection settings
            io_handler: Unified I/O handler for file operations
            input_source: Stream source to read instead of the input path (only its first
                rows are read, through :meth:`InputSource.open_head`)
        """
        self.config = config
        self.io_handler = io_handler
        self.input_source = input_source

    def detect_header_row(
        self, input_path: Union[str, Path], schema_columns: Optional[List[str]] = None
    ) -> Tuple[int, List[str]]:
        """Detect header row location and extract column names.

        Uses the configured header mode to determine how to find and extract
        column names from the input file (local or S3).

        Args:
            input_path: Path to the input CSV file (local or S3 URI)
            schema_columns: Column names from the schema, used for ``HeaderMode.ABSENT``

        Returns:
            Tuple of (header_row_index, column_names). ``(-1, [])`` is returned only when the
            file has no usable row at all (empty, or only blank/comment lines). In ABSENT mode
            the index is always -1 and the names come from the schema, or are generated
            (``col_1``..``col_N``) from the first data row.

        Raises:
            ValueError: If no header row is found within ``header_search_rows`` rows
        """
        if self.config.header_mode == HeaderMode.ABSENT:
            # No header: the schema names the columns, otherwise generate names
            if schema_columns:
                return -1, list(schema_columns)
            return -1, self._generate_column_names(input_path)

        elif self.config.header_mode == HeaderMode.PRESENT:
            # Header is expected at first non-comment row
            header_idx, columns = self._find_first_data_row(input_path)
            return header_idx, columns

        else:  # AUTO mode
            return self._auto_detect_header(input_path)

    def rows(self, input_path: Union[str, Path]) -> Iterator[List[str]]:
        """The rows of the input from the first one, as header detection reads them.

        A UTF-8 byte order mark is dropped; blank lines are empty lists. Reading is lazy, so
        stopping early reads only the start of the input (``forklift.jobs`` previews use it).
        """
        return self._iter_rows(input_path)

    def _iter_rows(self, input_path: Union[str, Path]):
        """Rows of the file as lists of cells; a UTF-8 byte order mark is dropped."""
        if self.input_source is not None:
            return self._iter_source_rows(self.input_source)
        return self.io_handler.csv_reader(
            input_path,
            delimiter=self.config.delimiter,
            quotechar=self.config.quote_char,
            encoding=read_encoding(self.config.encoding),
            escapechar=self.config.escape_char,
        )

    def _iter_source_rows(self, source: InputSource) -> Iterator[List[str]]:
        """Rows from the start of a stream source (read lazily: only what is needed)."""
        stream = io.BufferedReader(CountingReader(source.open_head()))
        with io.TextIOWrapper(
            stream, encoding=read_encoding(self.config.encoding), newline=""
        ) as text:
            yield from csv.reader(
                text,
                delimiter=self.config.delimiter,
                quotechar=self.config.quote_char,
                escapechar=self.config.escape_char,
            )

    def _header_not_found(self) -> ValueError:
        """Error for a file whose first ``header_search_rows`` rows hold no header."""
        return ValueError(
            f"No header row found within the first {self.config.header_search_rows} rows "
            "(blank and comment rows are skipped). Increase header_search_rows, check the "
            "comment_rows patterns, or use header_mode='absent' if the file has no header."
        )

    def _generate_column_names(self, input_path: Union[str, Path]) -> List[str]:
        """Generate ``col_1``..``col_N`` from the width of the first data row."""
        for idx, row in enumerate(self._iter_rows(input_path)):
            if idx >= self.config.header_search_rows:
                raise self._header_not_found()
            if not row or self._is_comment_row(row):
                continue
            return [f"col_{i}" for i in range(1, len(row) + 1)]
        return []

    def _find_first_data_row(self, input_path: Union[str, Path]) -> Tuple[int, List[str]]:
        """Find the first non-comment row and extract columns.

        Searches through the file to find the first row that is not a comment
        or blank line, treating it as the header row. Works with local files and S3.
        Rows are only comments when they match ``comment_rows``; a header such as
        ``#,name,amount`` is a header.

        Args:
            input_path: Path to the input CSV file (local or S3 URI)

        Returns:
            Tuple of (row_index, column_names). Returns (-1, []) for empty files.

        Raises:
            ValueError: If no header is found within ``header_search_rows`` rows
        """
        # Use unified I/O handler for S3 and local file support
        for idx, row in enumerate(self._iter_rows(input_path)):
            if idx >= self.config.header_search_rows:
                raise self._header_not_found()

            # Skip completely empty rows
            if not row:
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
            Tuple of (header_row_index, column_names). (-1, []) for files without any row.

        Raises:
            ValueError: If the search window holds only blank/comment rows
        """
        rows = []

        # Use unified I/O handler for S3 and local file support
        for idx, row in enumerate(self._iter_rows(input_path)):
            if idx >= self.config.header_search_rows:
                if not rows:
                    raise self._header_not_found()
                break

            if not row or self._is_comment_row(row):
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
            row: Cell values of a non-empty CSV row (callers skip empty rows)

        Returns:
            True if row appears to be a header, False otherwise
        """
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
        to determine if the entire row should be skipped. When ``comment_rows`` is not
        configured (None) only a line that is a single ``#`` cell counts as a comment (the
        long-standing default for metadata lines such as ``# Generated: ...``); a row like
        ``#,name,amount`` is a header. ``comment_rows=[]`` switches comment detection off.

        Args:
            row: Cell values of a non-empty CSV row (callers skip empty rows)

        Returns:
            True if row matches a comment pattern, False otherwise
        """
        first_cell = row[0].strip()

        if self.config.comment_rows is None:
            return len(row) == 1 and first_cell.startswith("#")

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
