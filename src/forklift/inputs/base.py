from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any
from ..types import Row

class BaseInput(ABC):
    def __init__(self, source: str, **opts: Any):
        self.source = source
        self.opts = opts

    @abstractmethod
    def iter_rows(self) -> Iterable[Row]:
        ...

