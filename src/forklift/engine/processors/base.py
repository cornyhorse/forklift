# processors/base.py
from __future__ import annotations

from abc import ABC, abstractmethod

from ..config import ImportConfig, ProcessingResults


class BaseProcessor(ABC):
    @abstractmethod
    def process(self, config: ImportConfig) -> ProcessingResults:
        pass
