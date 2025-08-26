from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any
from ..types import RowResult, Row

class BaseOutput(ABC):
    def __init__(self, dest: str, schema: Dict[str, Any] | None, **opts: Any):
        self.dest = dest
        self.schema = schema or {}
        self.opts = opts

    @abstractmethod
    def open(self) -> None: ...
    @abstractmethod
    def write(self, row: Row) -> None: ...
    @abstractmethod
    def quarantine(self, rr: RowResult) -> None: ...
    @abstractmethod
    def close(self) -> None: ...