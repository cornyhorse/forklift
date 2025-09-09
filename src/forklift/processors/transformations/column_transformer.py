"""Basic column transformation processor."""

from __future__ import annotations
from typing import Dict, List, Tuple, Callable

import pyarrow as pa

from ..base import BaseProcessor, ValidationResult


class ColumnTransformer(BaseProcessor):
    """Transforms column data (standardization, cleaning, etc.).

    This processor applies configurable transformations to column data,
    such as trimming whitespace, changing case, or applying custom
    transformation functions.

    Args:
        transformations: Dictionary mapping column names to lists of transformation functions

    Attributes:
        transformations: Dictionary of column transformations to apply
    """

    def __init__(self, transformations: Dict[str, List[Callable]]):
        """Initialize the column transformer.

        Args:
            transformations: Dictionary where keys are column names and values are
                           lists of transformation functions to apply in order
        """
        self.transformations = transformations

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply transformations to batch columns.

        Applies all configured transformations to their respective columns,
        returning the transformed batch along with any errors encountered.

        Args:
            batch: PyArrow RecordBatch to transform

        Returns:
            Tuple of (transformed_batch, validation_results) where transformed_batch
            contains the data with transformations applied and validation_results
            contains any transformation errors
        """
        validation_results = []

        # Apply transformations to each configured column
        for column_name, transforms in self.transformations.items():
            if column_name in batch.schema.names:
                column_index = batch.schema.get_field_index(column_name)
                column = batch.column(column_index)

                try:
                    transformed_column = self._apply_transforms(column, transforms)
                    batch = batch.set_column(column_index, column_name, transformed_column)
                except Exception as e:
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Transformation failed for column '{column_name}': {str(e)}",
                        error_code="TRANSFORMATION_ERROR",
                        column_name=column_name
                    ))

        return batch, validation_results

    def _apply_transforms(self, column: pa.Array, transforms: List[Callable]) -> pa.Array:
        """Apply a list of transformations to a column.

        Applies transformation functions in sequence to the column data.

        Args:
            column: PyArrow Array to transform
            transforms: List of transformation functions to apply

        Returns:
            PyArrow Array with transformations applied
        """
        result = column
        for transform in transforms:
            result = transform(result)
        return result