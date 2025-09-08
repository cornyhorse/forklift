"""Output metadata collector package for processing statistics and data profiling.

This package provides comprehensive metadata collection and analysis capabilities
for processed data batches. The original OutputMetadataCollector class has been
refactored into focused modules for better maintainability.
"""

from .core import OutputMetadataCollector
from .statistics import StatisticsCalculator
from .type_analyzer import TypeAnalyzer
from .quality_metrics import QualityMetricsGenerator
from .serializer import MetadataSerializer

__all__ = [
    'OutputMetadataCollector',
    'StatisticsCalculator',
    'TypeAnalyzer',
    'QualityMetricsGenerator',
    'MetadataSerializer'
]
