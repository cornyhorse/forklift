"""Base processor interface for data processing operations."""

from abc import ABC, abstractmethod
from typing import Optional

from ..config import ImportConfig, ProcessingResults


class BaseProcessor(ABC):
    """Abstract base class for data processors."""

    @abstractmethod
    def process(self, config: ImportConfig) -> ProcessingResults:
        """Process data according to the provided configuration.

        Args:
            config: Configuration object containing processing parameters

        Returns:
            ProcessingResults object containing statistics and output paths
        """
        pass
