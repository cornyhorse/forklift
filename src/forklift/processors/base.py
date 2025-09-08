"""Base classes for data processors in the Forklift pipeline."""

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
        row_number: Alternative name for row_index (backward compatibility)
        field_name: Alternative name for column_name (backward compatibility)
        actual_value: The actual value that caused the error
        expected_value: The expected value or constraint
    """
    is_valid: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    row_index: Optional[int] = None
    column_name: Optional[str] = None
    row_number: Optional[int] = None
    field_name: Optional[str] = None
    actual_value: Any = None
    expected_value: Any = None

    def __post_init__(self):
        """Map between different naming conventions for backward compatibility."""
        # Map row_number to row_index
        if self.row_number is not None and self.row_index is None:
            self.row_index = self.row_number
        elif self.row_index is not None and self.row_number is None:
            self.row_number = self.row_index

        # Map field_name to column_name
        if self.field_name is not None and self.column_name is None:
            self.column_name = self.field_name
        elif self.column_name is not None and self.field_name is None:
            self.field_name = self.column_name


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
