from __future__ import annotations
import csv
from typing import Iterable, Dict, Any, List, Optional, Iterator
from .base import BaseInput
from ..utils.encoding import open_text_auto
from ..utils.dedupe import dedupe_column_names
from ..utils.standardize import standardize_postgres_column_name

_PROLOGUE_PREFIXES = ("#",)
_FOOTER_PREFIXES = ("TOTAL", "SUMMARY")


def _skip_prologue_lines(file_handle, header_row: Optional[List[str]] = None,
                         max_scan_rows: Optional[int] = 100) -> None:
    """Advance a file handle past prologue lines.

    Prologue lines are blank lines, lines starting with one of
    :data:`_PROLOGUE_PREFIXES`, or (when ``header_row`` is provided) any line
    until a matching header row is encountered.

    :param file_handle: Seekable text file-like object opened for reading.
    :param header_row: Optional explicit header row tokens to match; when
        provided scanning continues until a matching row is found or the scan
        limit is exceeded.
    :param max_scan_rows: Maximum number of lines to scan (``None`` disables the
        limit).
    :raises ValueError: If ``header_row`` is provided but not found within the
        scan limit / file length.
    """
    line_count = 0
    while True:
        if max_scan_rows is not None and line_count >= max_scan_rows:
            if header_row:
                raise ValueError(f"Provided header_row not found in first {max_scan_rows} rows.")
            return
        header_candidate_position = file_handle.tell()
        current_line = file_handle.readline()
        if not current_line:
            if header_row:
                raise ValueError("Provided header_row not found in file.")
            return
        line_count += 1
        current_line_stripped = current_line.strip()
        if not current_line_stripped or current_line_stripped.startswith(_PROLOGUE_PREFIXES):
            continue
        if header_row:
            candidate_tokens = [cell.strip() for cell in current_line.split(",")]
            if candidate_tokens == header_row:
                file_handle.seek(header_candidate_position)
                break
            else:
                continue
        file_handle.seek(header_candidate_position)
        break


def get_csv_reader(file_handle: Any, delimiter: str) -> Iterator[List[str]]:
    """Create a CSV row iterator with consistent whitespace handling.

    Uses :func:`csv.reader` with ``skipinitialspace=True`` so that a space
    following a delimiter is ignored (useful for loosely formatted exports).

    :param file_handle: File-like object positioned at the first CSV row.
    :param delimiter: Single character delimiter.
    :return: Iterator yielding lists of raw string cells.
    """
    csv_reader = csv.reader(file_handle, delimiter=delimiter, skipinitialspace=True)
    return csv_reader


class CSVInput(BaseInput):
    def _prepare_csv_reader_and_fieldnames(self, file_handle):
        """Prepare a DictReader and deduplicated field name list.

        Determines header presence according to ``header_mode`` / overrides,
        skips any prologue lines, normalizes and deduplicates header names, and
        returns a configured :class:`csv.DictReader` plus the final field list.

        :param file_handle: Open file handle at beginning of file.
        :return: Tuple of (``DictReader``, ``List[str]`` field names).
        """
        header_mode = self.opts.get("header_mode", "auto")  # "auto", "present", "absent"
        if header_mode == "present":
            has_header = True
        elif header_mode == "absent":
            has_header = False
        else:
            has_header = self.opts.get("has_header", True)

        header_override: Optional[List[str]] = self.opts.get("header_override")
        header_scan_limit = self.opts.get("header_scan_limit", 100)
        delimiter = self.opts.get("delimiter") or ","
        header_row_for_detection = header_override if has_header and header_override else None

        try:
            try:
                _skip_prologue_lines(file_handle, header_row_for_detection, header_scan_limit)
            except ValueError:
                if header_mode == "auto" and header_row_for_detection:
                    file_handle.seek(0)
                    _skip_prologue_lines(file_handle, None, header_scan_limit)
            csv_reader = get_csv_reader(file_handle, delimiter)
            raw_header: List[str] = self._get_raw_header(csv_reader, has_header, header_override)
            normalized_headers = [standardize_postgres_column_name(header) for header in raw_header]
            fieldnames = dedupe_column_names(normalized_headers)
            dict_reader = csv.DictReader(
                file_handle if has_header else file_handle,
                fieldnames=fieldnames,
                delimiter=delimiter,
                skipinitialspace=True,
            )
            return dict_reader, fieldnames
        except Exception as e:  # pragma: no cover - defensive
            file_handle.close()
            raise e

    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        """Yield normalized row dictionaries from the CSV source.

        Applies: prologue skipping, header handling (present/absent/auto),
        delimiter selection, header normalization + deduplication, blank row
        filtering, footer keyword filtering, and consecutive duplicate row
        elimination.

        Recognized ``self.opts`` keys:

        * ``delimiter`` – field separator (default ``,``)
        * ``encoding_priority`` – ordered encodings to attempt
        * ``header_override`` – explicit header list when file lacks one
        * ``header_mode`` – ``present`` | ``absent`` | ``auto``
        * ``header_scan_limit`` – lines to scan for a matching header

        :return: Iterator of row dicts.
        """
        encoding_priority: List[str] = self.opts.get("encoding_priority") or ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
        file_handle = open_text_auto(self.source, encoding_priority)
        try:
            dict_reader, fieldnames = self._prepare_csv_reader_and_fieldnames(file_handle)
            previous_row_as_tuple = None
            first_column_name = fieldnames[0] if fieldnames else None
            for row_dict in dict_reader:
                first_column_value = (row_dict.get(first_column_name) or "").strip() if first_column_name else ""
                if any(first_column_value.startswith(footer_prefix) for footer_prefix in _FOOTER_PREFIXES):
                    continue
                def is_empty(value):
                    if value is None:
                        return True
                    if isinstance(value, list):
                        return all(is_empty(v) for v in value)
                    return str(value).strip() == ""
                if not any(not is_empty(cell_value) for cell_value in row_dict.values()):
                    continue
                current_row_as_tuple = tuple((column_name, row_dict.get(column_name, "")) for column_name in fieldnames)
                if previous_row_as_tuple is not None and current_row_as_tuple == previous_row_as_tuple:
                    continue
                previous_row_as_tuple = current_row_as_tuple
                yield row_dict
        finally:
            file_handle.close()

    def _get_raw_header(self, csv_reader: Iterator[List[str]], has_header: bool,
                        header_override: Optional[List[str]]) -> List[str]:
        """Return the raw header row (file or override).

        :param csv_reader: Iterator positioned at the first potential header row.
        :param has_header: Whether the file contains a header row.
        :param header_override: Explicit header list overriding the file header.
        :return: List of header names (possibly empty).
        :raises ValueError: If ``has_header`` is False and no ``header_override`` provided.
        """
        if has_header:
            try:
                file_header_row = next(csv_reader)
            except StopIteration:
                return []
            file_header_row = [header_cell.strip() for header_cell in file_header_row]
            return header_override if header_override else file_header_row
        else:
            if not header_override:
                raise ValueError("header_override required when has_header=False")
            return header_override

    def get_tables(self) -> list[dict]:
        """Return a single logical table describing the CSV file.

        :return: List with one element containing ``name`` and ``rows`` iterator.
        """
        return [{
            "name": self.source,
            "rows": self.iter_rows()
        }]
