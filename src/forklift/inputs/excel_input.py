from __future__ import annotations
from typing import Iterable, Dict, Any, Optional
from .base import BaseInput
import pandas as pd
import re

class ExcelInput(BaseInput):
    """Implements ExcelInput for reading Excel files using pandas, supporting header modes and deduplication, and yielding rows as dictionaries."""

    def __init__(self, source: str, sheet_name: Optional[str] = None, header_mode: str = "auto", header_override: Optional[list[str]] = None, **opts: Any):
        super().__init__(source, **opts)
        self.sheet_name = sheet_name
        self.header_mode = header_mode
        self.header_override = header_override
        self.opts = opts
        self._df = None
        self._load_excel()

    def _dedupe_column_names(self, names: list[str]) -> list[str]:
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
                new_name = f"{base_name}_1"
                while new_name in used_names:
                    match = re.match(r"(.+?)(_\d+)+$", new_name)
                    if match:
                        prefix = match.group(1)
                        num = int(new_name.split('_')[-1]) + 1
                        new_name = f"{prefix}_{num}"
                    else:
                        new_name = f"{base_name}_{seen_counts[base_name]}"
                    seen_counts[base_name] += 1
                deduped.append(new_name)
                used_names.add(new_name)
        return deduped

    def _load_excel(self):
        try:
            self._df = pd.read_excel(self.source, sheet_name=self.sheet_name, header=None if self.header_mode == "absent" else 0)
        except ImportError:
            raise RuntimeError("pandas is required for ExcelInput. Please install pandas.")
        if self.header_mode == "absent" and self.header_override:
            self._df.columns = self._dedupe_column_names(self.header_override)
        elif self.header_mode == "present" or self.header_mode == "auto":
            self._df.columns = self._dedupe_column_names([str(col) for col in self._df.columns])

    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        for _, row in self._df.iterrows():
            yield dict(row)
