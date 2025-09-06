# processors/base.py
from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    @abstractmethod
    def process(self, config: ImportConfig) -> ProcessingResults:
        pass
