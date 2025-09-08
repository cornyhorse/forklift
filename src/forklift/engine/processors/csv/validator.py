"""Batch validation utilities for CSV processing."""

from typing import Tuple
import pyarrow as pa
import pyarrow.compute as pc
from ...config import ImportConfig


class BatchValidator:
    """Handles batch validation and separation of good/bad rows."""

    @staticmethod
    def validate_batch(batch: pa.RecordBatch, schema: pa.Schema, config: ImportConfig) -> Tuple[pa.RecordBatch, pa.RecordBatch]:
        """Validate batch and separate good/bad rows.

        Args:
            batch: Input batch to validate
            schema: Schema to validate against
            config: Import configuration

        Returns:
            Tuple of (valid_batch, invalid_batch)
        """
        if not config.validate_schema or not schema:
            # No validation, return all as good
            empty_batch = batch.slice(0, 0)  # Empty batch with same schema
            return batch, empty_batch

        # For now, let's simplify validation - just check for required fields
        num_rows = len(batch)
        valid_mask = pa.array([True] * num_rows)

        for i, field in enumerate(schema):
            if i >= batch.num_columns:
                continue

            column = batch.column(i)

            # Null validation for required fields
            if not field.nullable:
                null_mask = pc.is_valid(column)
                valid_mask = pc.and_(valid_mask, null_mask)

        # Split into valid and invalid batches
        valid_indices = pc.filter(
            pa.array(range(num_rows)),
            valid_mask
        )
        invalid_indices = pc.filter(
            pa.array(range(num_rows)),
            pc.invert(valid_mask)
        )

        if len(valid_indices) > 0:
            valid_batch = pc.take(batch, valid_indices)
        else:
            valid_batch = batch.slice(0, 0)  # Empty batch

        if len(invalid_indices) > 0:
            invalid_batch = pc.take(batch, invalid_indices)
        else:
            invalid_batch = batch.slice(0, 0)  # Empty batch

        return valid_batch, invalid_batch
