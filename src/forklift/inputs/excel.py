"""Excel input handler for reading and preprocessing Excel files.

Sheets are read with ``openpyxl`` (read-only streaming mode, ``.xlsx``) or ``xlrd``
(``.xls``) and turned into PyArrow tables column-wise; no DataFrame library is involved.

Resource limits protect against untrusted workbooks: ``.xlsx`` archives are inspected
(without extracting) and refused when their uncompressed size or compression ratio is
beyond the configured limits, and sheets are read with row/cell caps. Sparse sheets are
never expanded to their bounding box: blank rows are skipped as they stream past and
trailing empty rows/columns are trimmed.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import re
import warnings
import zipfile
from itertools import zip_longest
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union

import pyarrow as pa
import pyarrow.compute as pc

from ..utils.column_name_utilities import dedupe_column_names
from .config import ExcelInputConfig, ExcelSheetConfig

# Suppress the NumPy reload warning that can occur with openpyxl
warnings.filterwarnings("ignore", message=".*NumPy module was reloaded.*", category=UserWarning)

_HEADER_MODES = ("present", "absent", "auto")
_MAX_ZIP_ENTRIES = 10_000
# Tiny archives cannot hurt, and highly repetitive tiny parts legitimately compress well.
_RATIO_CHECK_MIN_BYTES = 1024 * 1024


def _is_blank(value: Any) -> bool:
    """Return True for empty cells and whitespace-only text."""
    return value is None or (type(value) is str and not value.strip())


def _used_width(row: Sequence[Any]) -> int:
    """Width of ``row`` once trailing blank cells are ignored."""
    width = len(row)
    if width == 0 or row.count(None) == width:
        return 0
    while width and _is_blank(row[width - 1]):
        width -= 1
    return width


def _to_text(value: Any) -> Optional[str]:
    """Render a cell value as text (used for mixed-type columns and header cells)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e15:
        return str(int(value))
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    return str(value)


def _build_array(values: List[Any]) -> pa.Array:
    """Build an Arrow array from one column of cell values.

    Uniform columns keep their natural type; int/float mixes become float64; datetime/date
    mixes become timestamps; any other mix is rendered as text.
    """
    kinds = {type(v) for v in values if v is not None}
    try:
        if not kinds:
            return pa.array(values, type=pa.string())
        if kinds == {bool}:
            return pa.array(values, type=pa.bool_())
        if kinds == {int}:
            return pa.array(values, type=pa.int64())
        if kinds <= {int, float}:
            return pa.array(values, type=pa.float64())
        if kinds == {str}:
            return pa.array(values, type=pa.string())
        if kinds == {dt.datetime}:
            return pa.array(values, type=pa.timestamp("us"))
        if kinds == {dt.date}:
            return pa.array(values, type=pa.date32())
        if kinds == {dt.time}:
            return pa.array(values, type=pa.time64("us"))
        if kinds == {dt.timedelta}:
            return pa.array(values, type=pa.duration("us"))
        if kinds <= {dt.datetime, dt.date}:
            promoted = [
                (
                    dt.datetime.combine(v, dt.time())
                    if type(v) is dt.date  # noqa: E721 - datetime is a date subclass
                    else v
                )
                for v in values
            ]
            return pa.array(promoted, type=pa.timestamp("us"))
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
        pass  # e.g. an integer too large for int64: fall back to text
    return pa.array([_to_text(v) for v in values], type=pa.string())


def _arrow_type(parquet_type: str, column: str) -> pa.DataType:
    """Resolve a schema ``parquetType`` string to an Arrow type."""
    match = re.fullmatch(r"decimal(?:128)?\((\d+)\s*,\s*(\d+)\)", parquet_type.strip())
    if match:
        return pa.decimal128(int(match.group(1)), int(match.group(2)))
    try:
        return pa.type_for_alias(parquet_type.strip())
    except (ValueError, KeyError):
        raise ValueError(
            f"Unsupported parquetType '{parquet_type}' for column '{column}'"
        ) from None


def _column_index(position: Union[int, str], column: str) -> int:
    """Convert a column position (``"A"``/``"AB"`` or a 1-based number) to a 0-based index."""
    if isinstance(position, bool):
        raise ValueError(f"Invalid column position for '{column}'")
    if isinstance(position, int) or (isinstance(position, str) and position.strip().isdigit()):
        index = int(position)
    elif isinstance(position, str) and re.fullmatch(r"[A-Za-z]+", position.strip()):
        index = 0
        for char in position.strip().upper():
            index = index * 26 + (ord(char) - ord("A") + 1)
    else:
        raise ValueError(f"Invalid column position {position!r} for '{column}'")
    if index < 1:
        raise ValueError(f"Column position for '{column}' must be >= 1")
    return index - 1


class ExcelInputHandler:
    """Handles Excel file input with sheet selection and preprocessing.

    This class provides functionality for reading Excel files with various
    configurations including sheet selection by name/index/regex, header
    detection, and data extraction.

    Args:
        config: ExcelInputConfig instance with processing configuration

    Attributes:
        config: The configuration object for this input handler
        _workbook: The opened workbook object (openpyxl or xlrd)
        _engine: The Excel engine being used ('openpyxl' or 'xlrd')
    """

    def __init__(self, config: ExcelInputConfig):
        """Initialize the Excel input handler.

        Args:
            config: Configuration object containing Excel processing parameters
        """
        self.config = config
        self._workbook = None
        self._engine = None

    def __enter__(self) -> "ExcelInputHandler":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_workbook()

    def detect_engine(self, file_path: Path) -> str:
        """Detect the appropriate Excel engine based on file extension.

        Args:
            file_path: Path to the Excel file

        Returns:
            Engine name ('openpyxl' for .xlsx, 'xlrd' for .xls)

        Raises:
            ValueError: If file extension is not supported
        """
        if self.config.engine:
            return self.config.engine

        suffix = Path(file_path).suffix.lower()
        if suffix == ".xlsx":
            return "openpyxl"
        elif suffix == ".xls":
            return "xlrd"
        else:
            raise ValueError(f"Unsupported Excel file extension: {suffix}")

    def _check_xlsx_archive(self, file_path: Path) -> None:
        """Refuse zip bombs: inspect the archive directory without extracting anything.

        Raises:
            ValueError: If the file is not a zip archive, has too many entries, or its
                total uncompressed size / compression ratio exceeds the configured limits
        """
        cfg = self.config
        try:
            with zipfile.ZipFile(file_path) as archive:
                infos = archive.infolist()
        except zipfile.BadZipFile:
            raise ValueError(f"'{Path(file_path).name}' is not a valid .xlsx (zip) file") from None

        if len(infos) > _MAX_ZIP_ENTRIES:
            raise ValueError(
                f"Excel archive has {len(infos)} entries (limit {_MAX_ZIP_ENTRIES}); "
                "refusing to open it"
            )

        total_size = sum(info.file_size for info in infos)
        total_packed = sum(info.compress_size for info in infos)
        limit = cfg.max_uncompressed_bytes
        if limit is not None and total_size > limit:
            raise ValueError(
                f"Excel archive expands to {total_size} bytes, more than max_uncompressed_bytes="
                f"{limit}; refusing to open it (possible zip bomb)"
            )

        max_ratio = cfg.max_compression_ratio
        if max_ratio is not None:
            if (
                total_size >= _RATIO_CHECK_MIN_BYTES
                and total_size / max(total_packed, 1) > max_ratio
            ):
                raise ValueError(
                    f"Excel archive compression ratio exceeds max_compression_ratio={max_ratio}; "
                    "refusing to open it (possible zip bomb)"
                )
            for info in infos:
                if (
                    info.file_size >= _RATIO_CHECK_MIN_BYTES
                    and info.file_size / max(info.compress_size, 1) > max_ratio
                ):
                    raise ValueError(
                        f"Excel archive part '{info.filename}' compression ratio exceeds "
                        f"max_compression_ratio={max_ratio}; refusing to open it "
                        "(possible zip bomb)"
                    )

    def open_workbook(self, file_path: Path) -> None:
        """Open an Excel workbook using the appropriate engine.

        ``.xlsx`` files are opened in openpyxl's read-only streaming mode after the archive
        has been checked against the configured size/ratio limits.

        Args:
            file_path: Path to the Excel file to open

        Raises:
            ImportError: If required library for the engine is not found
            ValueError: If engine is not supported or the archive exceeds the limits
        """
        file_path = Path(file_path)
        self.close_workbook()
        self._engine = self.detect_engine(file_path)

        try:
            if self._engine == "openpyxl":
                import openpyxl

                self._check_xlsx_archive(file_path)
                self._workbook = openpyxl.load_workbook(
                    file_path, read_only=True, data_only=self.config.values_only
                )
            elif self._engine == "xlrd":
                import xlrd

                self._workbook = xlrd.open_workbook(str(file_path))
            else:
                raise ValueError(f"Unsupported engine: {self._engine}")
        except ImportError as e:
            raise ImportError(f"Required library for {self._engine} engine not found: {e}")

    def close_workbook(self) -> None:
        """Close the opened workbook if applicable."""
        if self._workbook is not None:
            # openpyxl workbooks have close(), xlrd books release_resources()
            closer = getattr(self._workbook, "close", None) or self._workbook.release_resources
            closer()
        self._workbook = None
        self._engine = None

    def get_sheet_names(self) -> List[str]:
        """Get the names of all sheets in the workbook.

        Returns:
            List of sheet names

        Raises:
            RuntimeError: If no workbook is currently opened
        """
        if not self._workbook:
            raise RuntimeError("Workbook not opened. Call open_workbook() first.")

        if self._engine == "openpyxl":
            return self._workbook.sheetnames
        elif self._engine == "xlrd":
            return self._workbook.sheet_names()
        else:
            raise ValueError(f"Unsupported engine: {self._engine}")

    def get_sheet_info(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Describe a workbook (engine and sheet names) without reading any sheet data.

        The workbook is opened to read its sheet list and closed again; any workbook this
        handler had open before is closed first.

        Args:
            file_path: Path to the Excel file

        Returns:
            Dictionary with ``engine``, ``sheet_count`` and ``sheet_names``
        """
        self.open_workbook(Path(file_path))
        try:
            names = list(self.get_sheet_names())
            return {"engine": self._engine, "sheet_count": len(names), "sheet_names": names}
        finally:
            self.close_workbook()

    def select_sheets(
        self, sheet_configs: List[ExcelSheetConfig]
    ) -> List[Tuple[str, ExcelSheetConfig]]:
        """Select sheets based on configuration criteria.

        Args:
            sheet_configs: List of sheet configuration objects

        Returns:
            List of tuples containing (sheet_name, sheet_config)

        Raises:
            RuntimeError: If no workbook is currently opened
            ValueError: If a named sheet / index / regex matches no sheet, or no sheets are
                selected at all
        """
        if not self._workbook:
            raise RuntimeError("Workbook not opened. Call open_workbook() first.")

        available_sheets = self.get_sheet_names()
        selected_sheets = []
        prefix = "No sheets selected based on configuration criteria"

        for config in sheet_configs:
            select_criteria = config.select or {}

            if "name" in select_criteria:
                # Select by exact name
                sheet_name = select_criteria["name"]
                if sheet_name not in available_sheets:
                    raise ValueError(f"{prefix}: sheet '{sheet_name}' not found in workbook")
                selected_sheets.append((sheet_name, config))

            elif "index" in select_criteria:
                # Select by index (0-based)
                index = select_criteria["index"]
                if (
                    isinstance(index, bool)
                    or not isinstance(index, int)
                    or not 0 <= index < len(available_sheets)
                ):
                    raise ValueError(
                        f"{prefix}: sheet index {index!r} is out of range "
                        f"(workbook has {len(available_sheets)} sheets)"
                    )
                selected_sheets.append((available_sheets[index], config))

            elif "regex" in select_criteria:
                # Select by regex pattern
                try:
                    pattern = re.compile(select_criteria["regex"])
                except (re.error, TypeError) as e:
                    raise ValueError(f"Invalid sheet selection regex: {e}") from e
                matching_sheets = [name for name in available_sheets if pattern.match(name)]
                if not matching_sheets:
                    raise ValueError(f"{prefix}: regex matched no sheet")
                for sheet_name in matching_sheets:
                    selected_sheets.append((sheet_name, config))

            else:
                raise ValueError(f"{prefix}: sheet selection needs 'name', 'index' or 'regex'")

        if not selected_sheets:
            raise ValueError(prefix)

        return selected_sheets

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    @staticmethod
    def _optional_row_number(value: Any, option: str) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{option} must be a positive integer (1-based sheet row)")
        return value

    def _layout(
        self, sheet_config: ExcelSheetConfig
    ) -> Tuple[str, Optional[int], Optional[int], int, Optional[int], Optional[List[Any]]]:
        """Resolve header/data row settings.

        Returns:
            ``(mode, header_row, explicit_start, data_start, data_end, override)`` where all
            rows are 1-based sheet rows and ``header_row`` is ``None`` without a header.
        """
        header = sheet_config.header
        if isinstance(header, int) and not isinstance(header, bool):
            header = {"row": header}
        if header is None:
            header = {}
        if not isinstance(header, dict):
            raise ValueError("header must be a mapping with optional 'mode', 'row' and 'override'")

        mode = header.get("mode") or "present"
        if mode not in _HEADER_MODES:
            raise ValueError(f"Invalid header mode {mode!r}; expected one of {_HEADER_MODES}")

        offset = header.get("row")
        if offset is None:
            offset = 0
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError(
                "header.row must be a non-negative integer (0-based; 0 = sheet row 1)"
            )

        override = header.get("override")
        if override is not None and not isinstance(override, (list, tuple)):
            raise ValueError("header.override must be a list of column names")

        explicit_start = self._optional_row_number(sheet_config.data_start_row, "data_start_row")
        data_end = self._optional_row_number(sheet_config.data_end_row, "data_end_row")
        if explicit_start is not None and data_end is not None and data_end < explicit_start:
            raise ValueError("data_end_row must not be before data_start_row")

        if mode == "absent":
            header_row, data_start = None, explicit_start or 1
        else:
            header_row = offset + 1
            # Data can never start on or before the header row.
            data_start = max(explicit_start or 0, header_row + 1)
        return mode, header_row, explicit_start, data_start, data_end, override

    def _iter_rows(
        self, sheet_name: str, first_row: int, last_row: Optional[int]
    ) -> Iterator[Tuple[int, Sequence[Any]]]:
        """Stream ``(sheet_row_number, values)`` from ``first_row`` to ``last_row`` (inclusive)."""
        if self._engine == "openpyxl":
            sheet = self._workbook[sheet_name]
            if not hasattr(sheet, "iter_rows"):
                raise ValueError(f"Sheet '{sheet_name}' is not a worksheet (e.g. a chart sheet)")
            # Trust the cells, not the <dimension> tag: a wrong or hostile tag would either
            # hide data or make openpyxl pad every row out to the declared bounding box.
            with contextlib.suppress(AttributeError):
                sheet.reset_dimensions()
            rows = sheet.iter_rows(min_row=first_row, max_row=last_row, values_only=True)
            with contextlib.closing(rows):
                for number, row in enumerate(rows, start=first_row):
                    yield number, row
        else:  # xlrd: open_workbook only keeps a workbook for an engine it supports
            import xlrd

            sheet = self._workbook.sheet_by_name(sheet_name)
            end = sheet.nrows if last_row is None else min(sheet.nrows, last_row)
            datemode = self._workbook.datemode
            for index in range(first_row - 1, end):
                yield index + 1, [
                    self._xlrd_value(xlrd, cell, datemode) for cell in sheet.row(index)
                ]

    @staticmethod
    def _xlrd_value(xlrd, cell, datemode: int) -> Any:
        ctype, value = cell.ctype, cell.value
        if ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
            return None
        if ctype == xlrd.XL_CELL_BOOLEAN:
            return bool(value)
        if ctype == xlrd.XL_CELL_ERROR:
            return xlrd.error_text_from_code.get(value, "#ERROR")
        if ctype == xlrd.XL_CELL_DATE:
            try:
                year, month, day, hour, minute, second = xlrd.xldate_as_tuple(value, datemode)
                if (year, month, day) == (0, 0, 0):
                    return dt.time(hour, minute, second)
                return dt.datetime(year, month, day, hour, minute, second)
            except (xlrd.XLDateError, ValueError):
                return value
        if ctype == xlrd.XL_CELL_NUMBER and float(value).is_integer() and abs(value) < 2**53:
            return int(value)
        return value

    @staticmethod
    def _looks_like_header(cells: Sequence[Any]) -> bool:
        """Heuristic for ``header.mode == "auto"``: a row of unique, non-numeric labels."""
        labels = [c for c in cells if not _is_blank(c)]
        return (
            bool(labels)
            and all(type(c) is str for c in labels)
            and len({c.strip() for c in labels}) == len(labels)
        )

    def _scan_sheet(
        self, sheet_name: str, sheet_config: ExcelSheetConfig
    ) -> Tuple[Optional[Sequence[Any]], List[Sequence[Any]], Optional[List[Any]]]:
        """Stream a sheet once, returning ``(header_cells, data_rows, header_override)``.

        Blank rows are never stored when skipped, stored rows are trimmed of trailing blank
        cells, and trailing blank rows are dropped, so sparse sheets stay small.
        """
        cfg = self.config
        mode, header_row, explicit_start, data_start, data_end, override = self._layout(
            sheet_config
        )
        skip_blank = bool(sheet_config.skip_blank_rows and cfg.skip_blank_lines)
        first_row = min(r for r in (header_row, data_start) if r is not None)

        header_cells: Optional[Sequence[Any]] = None
        kept: List[Sequence[Any]] = []
        scanned_cells = 0

        # _iter_rows stops at data_end; numbering is gap-free, so the max_rows check on the
        # sheet row number also bounds the number of rows kept.
        for number, row in self._iter_rows(sheet_name, first_row, data_end):
            if number > cfg.max_rows:
                raise ValueError(
                    f"Sheet '{sheet_name}' has data beyond row {cfg.max_rows} (max_rows); "
                    "raise max_rows or restrict data_end_row"
                )
            scanned_cells += len(row)
            if scanned_cells > cfg.max_cells:
                raise ValueError(
                    f"Sheet '{sheet_name}' exceeds max_cells={cfg.max_cells}; raise max_cells "
                    "or restrict data_end_row"
                )

            width = _used_width(row)
            if header_row is not None and number == header_row:
                header_cells = tuple(row[:width])
                if mode != "auto" or self._looks_like_header(header_cells):
                    continue
                # auto mode and this row is data, not a header
                header_cells, header_row = None, None
                data_start = explicit_start or number
            if number < data_start:
                continue

            if width == 0 and skip_blank:
                continue
            kept.append(tuple(row[:width]))

        while kept and not kept[-1]:
            kept.pop()  # trailing blank rows (only stored when skip_blank_rows is off)
        return header_cells, kept, override

    def _null_tokens(self, column: str) -> Tuple[Set[str], Set[float]]:
        """Text and numeric tokens that mean NULL for ``column``."""
        cfg = self.config
        nulls = cfg.nulls or {}
        per_column = (nulls.get("perColumn") or {}).get(column)
        tokens: List[Any] = list(cfg.na_values or [])
        tokens.extend(per_column if per_column is not None else (nulls.get("global") or []))
        if cfg.keep_default_na:
            tokens.append("")
        text = {str(t) for t in tokens}
        numeric = set()
        for token in text:
            try:
                numeric.add(float(token))
            except ValueError:
                pass
        return text, numeric

    @staticmethod
    def _apply_nulls(values: List[Any], text: Set[str], numeric: Set[float]) -> List[Any]:
        if not text:
            return values
        out = []
        for value in values:
            kind = type(value)
            if kind is str:
                if value in text or value.strip() in text:
                    value = None
            elif numeric and (kind is int or kind is float) and float(value) in numeric:
                value = None
            out.append(value)
        return out

    def _column_plan(
        self,
        sheet_config: ExcelSheetConfig,
        names: List[str],
        width: int,
    ) -> List[Tuple[str, Optional[int], Optional[str]]]:
        """Decide output ``(name, source_index, parquet_type)`` for each column."""
        specs = sheet_config.columns
        if not specs:
            return [(name, index, None) for index, name in enumerate(names)]

        plan: List[Tuple[str, Optional[int], Optional[str]]] = []
        for spec in specs:
            if isinstance(spec, str):
                spec = {"name": spec}
            if not isinstance(spec, dict) or not spec.get("name"):
                raise ValueError("Each column mapping needs a 'name'")
            name = str(spec["name"])
            position = spec.get("position")
            if position is not None:
                index = _column_index(position, name)
            elif name in names:
                index = names.index(name)
            else:
                raise ValueError(f"Column '{name}' not found in sheet header")
            plan.append((name, index if index < width else None, spec.get("parquetType")))

        out_names = [p[0] for p in plan]
        if len(set(out_names)) != len(out_names):
            raise ValueError("Duplicate output column names in sheet columns configuration")
        return plan

    def read_sheet_data(self, sheet_name: str, sheet_config: ExcelSheetConfig) -> pa.Table:
        """Read data from a specific sheet into an Arrow table.

        Args:
            sheet_name: Name of the sheet to read
            sheet_config: Configuration for reading this sheet

        Returns:
            PyArrow table with one column per sheet column. Header cells become the column
            names (blank ones ``col_<n>``, duplicates de-duplicated with a numeric suffix);
            without a header the names are ``col_1..col_N`` (or ``header.override``).

        Raises:
            RuntimeError: If no workbook is currently opened
            ValueError: For invalid settings or when the sheet exceeds ``max_rows``/``max_cells``
        """
        if not self._workbook:
            raise RuntimeError("Workbook not opened. Call open_workbook() first.")

        header_cells, rows, override = self._scan_sheet(sheet_name, sheet_config)
        override = list(override or [])

        width = max([len(r) for r in rows] + [len(header_cells or ()), len(override)])
        names: List[str] = []
        for index in range(width):
            label = override[index] if index < len(override) else None
            if _is_blank(label):
                label = header_cells[index] if header_cells and index < len(header_cells) else None
            names.append(f"col_{index + 1}" if _is_blank(label) else _to_text(label))
        names = dedupe_column_names(names)

        # Column-wise view of the stored rows (short rows padded with None).
        columns: List[Tuple[Any, ...]] = list(zip_longest(*rows)) if rows else []
        n_rows = len(rows)

        arrays, out_names = [], []
        for name, index, parquet_type in self._column_plan(sheet_config, names, width):
            values = list(columns[index]) if index is not None and index < len(columns) else []
            values = values or [None] * n_rows
            text, numeric = self._null_tokens(name)
            array = _build_array(self._apply_nulls(values, text, numeric))
            if parquet_type:
                target = _arrow_type(parquet_type, name)
                if array.type != target:
                    try:
                        array = pc.cast(array, target)
                    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
                        raise ValueError(
                            f"Column '{name}' cannot be converted to {parquet_type}"
                        ) from None
            arrays.append(array)
            out_names.append(name)

        if not arrays:
            return pa.table({})
        return pa.Table.from_arrays(arrays, names=out_names)

    def process_sheets(self, file_path: Union[str, Path]) -> Iterator[Tuple[str, pa.Table]]:
        """Read every configured sheet of a workbook.

        With ``config.sheets`` unset, all sheets are read with default settings.

        Args:
            file_path: Path to the Excel file

        Yields:
            ``(sheet_name, table)`` for each selected sheet (``name_override`` replaces the
            sheet name when configured)

        Raises:
            ValueError: If a configured sheet does not exist or a resource limit is exceeded
        """
        self.open_workbook(Path(file_path))
        try:
            sheet_configs = self.config.sheets
            if sheet_configs is None:
                sheet_configs = [
                    ExcelSheetConfig(select={"name": name}) for name in self.get_sheet_names()
                ]
            for sheet_name, sheet_config in self.select_sheets(sheet_configs):
                table = self.read_sheet_data(sheet_name, sheet_config)
                yield (sheet_config.name_override or sheet_name), table
        finally:
            self.close_workbook()
