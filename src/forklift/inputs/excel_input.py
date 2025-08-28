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
    """

    def __init__(self, source: str, sheet_name: Optional[str] = None, header_mode: str = "auto", header_override: Optional[list[str]] = None, **opts: Any):
        """
        Initialize an ExcelInput instance.

        :param source: Path to the Excel file.
        :type source: str
        :param sheet_name: Name of the sheet to read. If None, reads the first sheet.
        :type sheet_name: Optional[str]
        :param header_mode: Header mode, one of "auto", "present", or "absent".
        :type header_mode: str
        :param header_override: List of column names to override headers if header_mode is "absent".
        :type header_override: Optional[list[str]]
        :param opts: Additional options passed to BaseInput.
        :type opts: Any
        """
        super().__init__(source, **opts)
        self.sheet_name = sheet_name
        self.header_mode = header_mode
        self.header_override = header_override
        self.opts = opts
        self._df = None
        self._load_excel()

    def _load_excel(self):
        """
        Load the Excel file into a pandas DataFrame, handling header modes and column deduplication.

        :raises RuntimeError: If pandas is not installed.
        """
        try:
            self._df = pd.read_excel(self.source, sheet_name=self.sheet_name, header=None if self.header_mode == "absent" else 0)
        except ImportError:
            raise RuntimeError("pandas is required for ExcelInput. Please install pandas.")
        if self.header_mode == "absent" and self.header_override:
            self._df.columns = dedupe_column_names(self.header_override)
        elif self.header_mode == "present" or self.header_mode == "auto":
            self._df.columns = dedupe_column_names([str(col) for col in self._df.columns])

    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Iterate over the rows of the loaded Excel sheet, yielding each row as a dictionary.

        :return: An iterable of dictionaries, one per row.
        :rtype: Iterable[Dict[str, Any]]
        """
        for _, row in self._df.iterrows():
            yield dict(row)
