"""
excel_input.py
===============

Implements ExcelInput for reading Excel files using pandas, supporting header modes and deduplication, and yielding rows as dictionaries.
"""
from __future__ import annotations
from typing import Iterable, Dict, Any, Optional
from .base import BaseInput
import pandas as pd
from forklift.utils.dedupe import dedupe_column_names

class ExcelInput(BaseInput):
    """
    Implements ExcelInput for reading Excel files using pandas, supporting header modes and deduplication, and yielding rows as dictionaries.

    :param source: Path to the Excel file.
    :type source: str
    :param tables: List of table/sheet definitions (from schema importer). Each dict should contain at least 'name' and optionally 'header_override'.
    :type tables: Optional[list[dict]]
    :param header_mode: Header mode, one of "auto", "present", or "absent".
    :type header_mode: str
    :param header_override: List of column names to override headers if header_mode is "absent" (used for single-sheet mode).
    :type header_override: Optional[list[str]]
    :param opts: Additional options passed to BaseInput.
    :type opts: Any
    """

    def __init__(self, source: str, tables: Optional[list[dict]] = None, header_mode: str = "auto", header_override: Optional[list[str]] = None, **opts: Any):
        """
        Initialize an ExcelInput instance.

        :param source: Path to the Excel file.
        :type source: str
        :param tables: List of table/sheet definitions (from schema importer).
        :type tables: Optional[list[dict]]
        :param header_mode: Header mode, one of "auto", "present", or "absent".
        :type header_mode: str
        :param header_override: List of column names to override headers if header_mode is "absent".
        :type header_override: Optional[list[str]]
        :param opts: Additional options passed to BaseInput.
        :type opts: Any
        """
        super().__init__(source, **opts)
        self.header_mode = header_mode
        self.header_override = header_override
        self.opts = opts
        self.tables = tables
        self._dfs = {}  # table_name -> DataFrame
        self._load_excel_multi()

    def _load_excel_multi(self):
        """
        Load all specified tables/sheets from the Excel file into DataFrames.

        :raises Exception: If loading a sheet fails.
        """
        if self.tables:
            for table in self.tables:
                sheet_name = table.get('name')
                header_override = table.get('header_override', None)
                try:
                    df = pd.read_excel(self.source, sheet_name=sheet_name, header=None if self.header_mode == "absent" else 0)
                except ImportError:
                    raise RuntimeError("pandas is required for ExcelInput. Please install pandas.")
                if self.header_mode == "absent" and header_override:
                    df.columns = dedupe_column_names(header_override)
                elif self.header_mode == "present" or self.header_mode == "auto":
                    df.columns = dedupe_column_names([str(col) for col in df.columns])
                self._dfs[sheet_name] = df
        else:
            # Fallback to single-sheet mode for backward compatibility
            sheet_name = self.opts.get('sheet_name', None)
            try:
                df = pd.read_excel(self.source, sheet_name=sheet_name, header=None if self.header_mode == "absent" else 0)
            except ImportError:
                raise RuntimeError("pandas is required for ExcelInput. Please install pandas.")
            if self.header_mode == "absent" and self.header_override:
                df.columns = dedupe_column_names(self.header_override)
            elif self.header_mode == "present" or self.header_mode == "auto":
                df.columns = dedupe_column_names([str(col) for col in df.columns])
            self._dfs[sheet_name or 'default'] = df

    def iter_rows(self, table_name: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        """
        Iterate over the rows of the specified table/sheet, yielding each row as a dictionary.
        If table_name is None, iterate over all tables, yielding rows with a '_table' key indicating the table name.

        :param table_name: Name of the table/sheet to iterate. If None, iterate all tables.
        :type table_name: Optional[str]
        :return: An iterable of dictionaries, one per row.
        :rtype: Iterable[Dict[str, Any]]
        """
        if table_name:
            df = self._dfs.get(table_name)
            if df is not None:
                for _, row in df.iterrows():
                    yield dict(row)
        else:
            for tname, df in self._dfs.items():
                for _, row in df.iterrows():
                    result = dict(row)
                    result['_table'] = tname
                    yield result

    def get_tables(self) -> list[dict]:
        """
        Return a list of table dicts for Excel input, one per sheet/table.
        Each dict contains 'name' (the sheet name) and 'rows' (an iterable of row dicts).

        :return: A list of table dicts.
        :rtype: list[dict]
        """
        return [
            {"name": tname, "rows": (dict(row) for _, row in df.iterrows())}
            for tname, df in self._dfs.items()
        ]
