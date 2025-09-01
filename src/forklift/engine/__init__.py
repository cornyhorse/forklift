from __future__ import annotations
from .forklift_core import ForkliftCore, ImportConfig, ProcessingResults, HeaderMode
from typing import Any, Optional

# Export the main classes for backwards compatibility
__all__ = [
    "ForkliftCore",
    "ImportConfig",
    "ProcessingResults",
    "HeaderMode"
]
