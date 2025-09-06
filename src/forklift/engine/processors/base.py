# processors/base.py
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import ImportConfig, ProcessingResults

class BaseProcessor(ABC):
    @abstractmethod
    def process(self, config: "ImportConfig") -> "ProcessingResults":
        pass
