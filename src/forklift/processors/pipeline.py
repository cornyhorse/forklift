"""Pipeline for chaining multiple processors together."""

from __future__ import annotations
from typing import List, Tuple

import pyarrow as pa

from .base import BaseProcessor, ValidationResult


class ProcessorPipeline:
    """Pipeline for chaining multiple processors.

    This class allows multiple processors to be chained together in a
    pipeline, with data flowing through each processor in sequence.

    Args:
        processors: List of BaseProcessor instances to chain together

    Attributes:
        processors: List of processors in the pipeline
    """

    def __init__(self, processors: List[BaseProcessor]):
        """Initialize the processor pipeline.

        Args:
            processors: List of BaseProcessor instances that will process data in order
        """
        self.processors = processors

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch through all processors in sequence.

        Passes the batch through each processor in the pipeline, accumulating
        validation results and applying transformations sequentially.

        Args:
            batch: PyArrow RecordBatch to process through the pipeline

        Returns:
            Tuple of (final_batch, all_validation_results) where final_batch
            is the result of all transformations and all_validation_results
            contains validation results from all processors
        """
        current_batch = batch
        all_validation_results = []

        for processor in self.processors:
            current_batch, validation_results = processor.process_batch(current_batch)
            all_validation_results.extend(validation_results)

        return current_batch, all_validation_results
