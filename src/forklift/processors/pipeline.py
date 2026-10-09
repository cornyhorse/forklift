"""Pipeline for chaining multiple processors together."""

from __future__ import annotations

import inspect
from typing import Dict, List, Tuple

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
        self._accepts_input_batch: Dict[int, bool] = {}

    def _wants_input_batch(self, processor: BaseProcessor) -> bool:
        """Whether ``processor.process_batch`` takes the pipeline's original input batch."""
        key = id(processor)
        if key not in self._accepts_input_batch:
            try:
                parameters = inspect.signature(processor.process_batch).parameters
                self._accepts_input_batch[key] = "input_batch" in parameters
            except (TypeError, ValueError):
                self._accepts_input_batch[key] = False
        return self._accepts_input_batch[key]

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process batch through all processors in sequence.

        Passes the batch through each processor in the pipeline, accumulating
        validation results and applying transformations sequentially. Processors
        whose ``process_batch`` accepts an ``input_batch`` argument (for example
        ``RowHashProcessor``, which can hash the row before transformations) receive
        the batch as it entered the pipeline.

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
            if self._wants_input_batch(processor):
                current_batch, validation_results = processor.process_batch(
                    current_batch, input_batch=batch
                )
            else:
                current_batch, validation_results = processor.process_batch(current_batch)
            all_validation_results.extend(validation_results)

        return current_batch, all_validation_results
