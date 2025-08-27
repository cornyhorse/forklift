from __future__ import annotations
import csv
from typing import Iterable, Dict, Any, List, Optional, Iterator
from .base import BaseInput
from ..utils.encoding import open_text_auto

_PROLOGUE_PREFIXES = ("#",)
_FOOTER_PREFIXES = ("TOTAL", "SUMMARY")

import re


def _pgsafe(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:63]


def _dedupe_column_names(names: list[str]) -> list[str]:
    """
    Ensure all names in the given list are unique by appending numeric suffixes
    (e.g., "col", "col_1", "col_2", …) when duplicates appear.

    Example:
        Input:  ["id", "name", "name", "amount", "name"]
        Output: ["id", "name", "name_1", "amount", "name_2"]

    :param names: List of original names (possibly with duplicates).
    :return: List of deduplicated names with suffixes applied where needed.
    """
    seen_counts: dict[str, int] = {}
    deduped: list[str] = []
    used_names: set[str] = set()

    for name in names:
        base_name = name
        count = seen_counts.get(base_name, 0)

        if count == 0 and base_name not in used_names:
            deduped.append(base_name)
            seen_counts[base_name] = 1
            used_names.add(base_name)
        else:
            new_name = f"{base_name}_1"  # Start at _1 for first duplicate
            while new_name in used_names:
                # Find the last numeric suffix and increment it
                match = re.match(r"(.+?)(_\d+)+$", new_name)
                if match:
                    prefix = match.group(1)
                    suffixes = re.findall(r"_\d+", new_name)
                    last_num = int(suffixes[-1][1:]) + 1
                    new_name = f"{prefix}{''.join(suffixes[:-1])}_{last_num}"

            deduped.append(new_name)
            seen_counts[base_name] = count + 1
            used_names.add(new_name)

    return deduped


def _looks_like_header(tokens: list[str]) -> bool:
    return all(not any(ch.isdigit() for ch in t) for t in tokens)


def _skip_prologue_lines(file_handle, header_row: Optional[List[str]] = None,
                         max_scan_rows: Optional[int] = 100) -> None:
    """
    Advance the file_handle past any prologue lines, which are lines that are either blank,
    start with any of the prefixes defined in _PROLOGUE_PREFIXES, or (if header_row is provided)
    until a line matching the header_row is found.

    If header_row is provided, lines are read and compared (after splitting and stripping)
    to header_row. The file_handle will be positioned at the start of the header row if found.
    Only the first max_scan_rows lines are checked (default: 100), unless overridden.

    Args:
        file_handle: File-like object to advance.
        header_row: Optional list of header strings to match as the header row.
        max_scan_rows: Maximum number of rows to scan for the header row (None for unlimited).

    Raises:
        ValueError: If header_row is provided but not found in the file within scan limit.
    """
    scanned = 0
    while True:
        if max_scan_rows is not None and scanned >= max_scan_rows:
            if header_row:
                raise ValueError(f"Provided header_row not found in first {max_scan_rows} rows.")
            return
        position = file_handle.tell()
        line = file_handle.readline()
        if not line:
            if header_row:
                raise ValueError("Provided header_row not found in file.")
            return
        scanned += 1
        line_stripped = line.strip()
        if not line_stripped or line_stripped.startswith(_PROLOGUE_PREFIXES):
            continue
        if header_row:
            tokens = [cell.strip() for cell in line.split(",")]
            if tokens == header_row:
                file_handle.seek(position)
                break
            else:
                continue
        file_handle.seek(position)
        break


def get_csv_reader(file_handle: Any, delimiter: str) -> Iterator[List[str]]:
    """
    Create a CSV reader for the given file handle and delimiter.

            skipinitialspace=True is an option on Python’s built-in csv.reader.
            It tells the reader to ignore any whitespace immediately following the delimiter.

            e.g.
            ------------------
            import csv
            from io import StringIO

            data = "id, name ,age\n1, Alice , 30\n2,Bob,25"
            reader_default = list(csv.reader(StringIO(data), delimiter=","))
            reader_skipspace = list(csv.reader(StringIO(data), delimiter=",", skipinitialspace=True))

            print("Default:", reader_default)
            print("Skipinitialspace:", reader_skipspace)
            ------------------

            Default:          [['id', ' name ', 'age'], ['1', ' Alice ', ' 30'], ['2', 'Bob', '25']]
            Skipinitialspace: [['id', 'name ', 'age'], ['1', 'Alice ', '30'], ['2', 'Bob', '25']]
            ------------------
            Note in the above how "name" is read as " name " with the default reader, but as "name " with skipinitialspace=True.
            This is particularly useful when dealing with CSV files that may have inconsistent spacing after delimiters.

    Args:
        file_handle: A file-like object opened for reading text.
        delimiter: The character used to separate fields in the CSV file.

    Returns:
        A csv.reader object configured with the specified delimiter and skipinitialspace=True.
    """
    csv_reader = csv.reader(file_handle, delimiter=delimiter, skipinitialspace=True)
    return csv_reader


class CSVInput(BaseInput):
    def _prepare_csv_reader_and_fieldnames(self, file_handle):
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
        # Clarify header detection and override logic
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
            normalized_headers = [_pgsafe(header) for header in raw_header]
            fieldnames = _dedupe_column_names(normalized_headers)
            dict_reader = csv.DictReader(
                file_handle if has_header else file_handle,
                fieldnames=fieldnames,
                delimiter=delimiter,
                skipinitialspace=True,
            )
            return dict_reader, fieldnames
        except Exception as e:
            file_handle.close()
            raise e

    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Iterate over rows from the CSV source, skipping prologue lines, deduplicating consecutive identical rows,
        and skipping footer/summary rows. Handles explicit header handling via the 'header_mode' option:

        header_mode:
            - "present": File is expected to have a header row.
            - "absent": File does not have a header row; use header_override for field names.
            - "auto": Try to detect header row.

        Uses options from self.opts:
            - delimiter: CSV delimiter character (default: ',')
            - encoding_priority: List of encodings to try for file reading
            - header_override: Optional list of header names to use instead of the file's header row
            - header_mode: Explicit header handling mode
            - header_scan_limit: Maximum number of rows to scan for header row (default: 100)

        :return: An iterator of dictionaries mapping field names to values for each valid row.
        """
        encoding_priority: List[str] = self.opts.get("encoding_priority") or ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
        file_handle = open_text_auto(self.source, encoding_priority)
        try:
            dict_reader, fieldnames = self._prepare_csv_reader_and_fieldnames(file_handle)
            previous_row_tuple = None
            first_fieldname = fieldnames[0] if fieldnames else None
            for row in dict_reader:
                first_value = (row.get(first_fieldname) or "").strip() if first_fieldname else ""
                if any(first_value.startswith(prefix) for prefix in _FOOTER_PREFIXES):
                    continue
                def is_empty(val):
                    if val is None:
                        return True
                    if isinstance(val, list):
                        return all(is_empty(v) for v in val)
                    return (str(val).strip() == "")
                if not any(not is_empty(value) for value in row.values()):
                    continue
                row_tuple = tuple((key, row.get(key, "")) for key in fieldnames)
                if previous_row_tuple is not None and row_tuple == previous_row_tuple:
                    continue
                previous_row_tuple = row_tuple
                yield row
        finally:
            file_handle.close()

    def _get_raw_header(self, csv_reader: Iterator[List[str]], has_header: bool,
                        header_override: Optional[List[str]]) -> List[str]:
        """
        Determine the raw header row for the CSV file.

        Args:
            csv_reader: The CSV reader object positioned at the first data row.
            has_header: Whether the CSV file has a header row.
            header_override: Optional list of header names to use instead of the file's header row.

        Returns:
            List of header names, either from the file or overridden.

        Raises:
            ValueError: If has_header is False and header_override is not provided.
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
