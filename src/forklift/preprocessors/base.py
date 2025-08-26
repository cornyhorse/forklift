from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any

Row = Dict[str, Any]


class Preprocessor(ABC):
    @abstractmethod
    def apply(self, row: Row) -> Row:
        ...
