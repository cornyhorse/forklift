from __future__ import annotations

from .config import HeaderMode, ImportConfig, ProcessingResults
from .exceptions import ProcessingError
from .forklift_core import ForkliftCore

# Export the main classes for backwards compatibility
__all__ = ["ForkliftCore", "ImportConfig", "ProcessingResults", "HeaderMode", "ProcessingError"]
