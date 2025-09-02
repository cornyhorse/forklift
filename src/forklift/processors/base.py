"""Base classes and validation results for data processors."""

from __future__ import annotations
from typing import List, Tuple, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

import pyarrow as pa


@dataclass
class ValidationResult:
    """Result of data validation operation.

    Attributes:
        is_valid: Whether the validation passed
        error_message: Human-readable error message (if validation failed)
        error_code: Machine-readable error code for categorization
        row_index: Index of the row that failed validation (if applicable)
        column_name: Name of the column that failed validation (if applicable)
    """
    is_valid: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    row_index: Optional[int] = None
    column_name: Optional[str] = None


class BaseProcessor(ABC):
    """Base class for all data processors.

    This abstract base class defines the interface that all data processors
    must implement. Processors take PyArrow RecordBatch objects and return
    processed data along with validation results.
    """

    @abstractmethod
    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch and return valid data and validation results.

        Args:
            batch: PyArrow RecordBatch containing data to process

        Returns:
            Tuple of (processed_batch, validation_results)

        Note:
            Implementations should handle both data transformation and validation,
            returning the processed data and any validation issues encountered.
        """
        pass
