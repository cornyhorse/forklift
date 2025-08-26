from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional

Row = Dict[str, Any]


@dataclass
class RowResult:
    row: Optional[Row]
    error: Optional[Exception]
