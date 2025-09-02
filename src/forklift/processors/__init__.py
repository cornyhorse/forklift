"""Data processors for validation, transformation, and quality checks.

This module provides processor classes for validating, transforming, and
performing quality checks on data during the import process. Processors can
be chained together in pipelines for complex data processing workflows.
"""

# Base classes and validation results
from .base import BaseProcessor, ValidationResult

# Schema validation
from .schema_validator import SchemaValidator

# Data quality checks
from .quality import DataQualityProcessor

# Column transformations
from .transformations import (
    ColumnTransformer,
    trim_whitespace,
    uppercase,
    lowercase,
)

# Pipeline for chaining processors
from .pipeline import ProcessorPipeline

__all__ = [
    # Base classes
    "BaseProcessor",
    "ValidationResult",

    # Processors
    "SchemaValidator",
    "DataQualityProcessor",
    "ColumnTransformer",

    # Pipeline
    "ProcessorPipeline",

    # Common transformation functions
    "trim_whitespace",
    "uppercase",
    "lowercase",
]
