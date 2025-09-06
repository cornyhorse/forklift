"""Schema generation package for Forklift.

This package provides capabilities to analyze data files and generate
schema objects that conform to the Forklift schema standards.
"""

# Import from the new modular structure
from .generator.core import SchemaGenerator, SchemaGenerationConfig, OutputTarget, FileType
from .types.special_types import SpecialTypeDetector
from .types.data_types import DataTypeConverter
from .processors.metadata import MetadataGenerator
from .processors.json_schema import JSONSchemaProcessor
from .utils.formatters import SchemaFormatter

# Maintain backward compatibility - keep the original imports available
__all__ = [
    'SchemaGenerator',
    'SchemaGenerationConfig',
    'OutputTarget',
    'FileType',
    'SpecialTypeDetector',
    'DataTypeConverter',
    'MetadataGenerator',
    'JSONSchemaProcessor',
    'SchemaFormatter'
]
