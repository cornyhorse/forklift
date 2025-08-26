from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict

class Preprocessor(ABC):
    @abstractmethod
    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:
        ...