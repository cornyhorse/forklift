"""Base classes for schema validation processors."""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple
from dataclasses import dataclass

import pyarrow as pa


@dataclass
class ValidationResult:
    """Result of a validation operation.

    Attributes:
        is_valid: Whether the validation passed
        error_message: Error message if validation failed
        error_code: Code identifying the type of error
        row_index: Row index where error occurred (if applicable)
        column_name: Column name where error occurred (if applicable)
    """
    is_valid: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    row_index: Optional[int] = None
    column_name: Optional[str] = None


class BaseProcessor(ABC):
    """Abstract base class for data processors.

    All data processors should inherit from this class and implement
    the process_batch method.
    """

    @abstractmethod
    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch of data.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (processed_batch, validation_results)
        """
        pass
