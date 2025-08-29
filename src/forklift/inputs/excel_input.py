"""
excel_input.py
===============

Implements ExcelInput for reading Excel files using polars only, supporting header modes and
column deduplication, and yielding rows as dictionaries.
"""
from __future__ import annotations
from typing import Iterable, Dict, Any, Optional
from .base import BaseInput
from forklift.utils.column_name_utilities import dedupe_column_names

try:  # pragma: no cover - import guard
    import polars as pl  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError("polars is required for ExcelInput. Install with 'pip install polars'.") from e


class ExcelInput(BaseInput):
    """
    ExcelInput reads Excel sheets using polars.

    :param source: Path to the Excel file.
    :param tables: Optional list of table/sheet definitions; each dict must have 'name' and may
                   include 'header_override'. If omitted, single-sheet mode is used.
    :param header_mode: One of "auto", "present", or "absent".
    :param header_override: Column names to apply when header_mode == "absent" (single-sheet mode).
    :param opts: Additional options passed to BaseInput (e.g., sheet_name for single-sheet mode).
    """

    def __init__(
            self,
            source: str,
            tables: Optional[list[dict]] = None,
            header_mode: str = "auto",
            header_override: Optional[list[str]] = None,
            **opts: Any,
    ):
        super().__init__(source, **opts)
        self.header_mode = header_mode
        self.header_override = header_override
        self.opts = opts
        self.tables = tables
        self._dfs: dict[str, pl.DataFrame] = {}
        self._load_excel_multi()

    # ---------------------- Loading ----------------------
    def _read_sheet(self, sheet_name: Optional[str], header_override: Optional[list[str]]) -> pl.DataFrame:
        """Read a single sheet into a polars DataFrame."""
        has_header = self.header_mode != "absent"
        try:
            df = pl.read_excel(
                self.source,
                sheet_name=sheet_name,  # type: ignore[arg-type]
                sheet_id=None if sheet_name is not None else 0,
                has_header=has_header,
            )
        except Exception as e:  # pragma: no cover
            raise RuntimeError(f"Failed reading Excel with polars: {e}") from e

        # Column handling
        if not has_header:
            if header_override:
                cols = dedupe_column_names(header_override)
                if len(cols) != len(df.columns):
                    raise ValueError(
                        f"Header override length {len(cols)} does not match column count {len(df.columns)} for sheet {sheet_name or 'default'}"
                    )
                df.columns = cols  # type: ignore[attr-defined]
            else:  # absent, no override: dedupe existing generated names
                df.columns = dedupe_column_names(df.columns)  # type: ignore[attr-defined]
        else:  # present/auto
            df.columns = dedupe_column_names([str(c) for c in df.columns])  # type: ignore[attr-defined]
        return df

    def _load_excel_multi(self) -> None:
        if self.tables:
            for table in self.tables:
                sheet_name = table.get("name")
                if sheet_name is None:
                    raise ValueError("Table definition missing 'name' for Excel sheet")
                header_override = table.get("header_override")
                self._dfs[sheet_name] = self._read_sheet(sheet_name, header_override)
        else:
            sheet_name = self.opts.get("sheet_name")
            self._dfs[sheet_name or "default"] = self._read_sheet(sheet_name, self.header_override)

    # ---------------------- Iteration ----------------------
    def _iter_dataframe_rows(self, df: pl.DataFrame) -> Iterable[Dict[str, Any]]:
        for row in df.iter_rows(named=True):  # type: ignore[attr-defined]
            yield dict(row)

    def iter_rows(self, table_name: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        if table_name:
            df = self._dfs.get(table_name)
            if df is not None:
                yield from self._iter_dataframe_rows(df)
        else:
            for tname, df in self._dfs.items():
                for row in self._iter_dataframe_rows(df):
                    row["_table"] = tname
                    yield row

    def get_tables(self) -> list[dict]:
        return [
            {"name": tname, "rows": (dict(r) for r in df.iter_rows(named=True))}  # type: ignore[attr-defined]
            for tname, df in self._dfs.items()
        ]
