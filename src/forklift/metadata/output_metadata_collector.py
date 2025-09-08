"""Output metadata collector for processing statistics and data profiling.

This module provides comprehensive metadata collection and analysis capabilities
for processed data batches. The original file has been refactored into a package
for better maintainability. All functionality is preserved for backward compatibility.
"""

# Import all components from the refactored package
from .output_metadata_collector.core import OutputMetadataCollector
from .output_metadata_collector.statistics import StatisticsCalculator
from .output_metadata_collector.type_analyzer import TypeAnalyzer
from .output_metadata_collector.quality_metrics import QualityMetricsGenerator
from .output_metadata_collector.serializer import MetadataSerializer

__all__ = [
    'OutputMetadataCollector',
    'StatisticsCalculator',
    'TypeAnalyzer',
    'QualityMetricsGenerator',
    'MetadataSerializer'
]
