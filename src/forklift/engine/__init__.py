from __future__ import annotations
from .forklift_core import ForkliftCore
from .config import ImportConfig, ProcessingResults, HeaderMode
from .exceptions import ProcessingError
from typing import Any, Optional

# Export the main classes for backwards compatibility
__all__ = [
    "ForkliftCore",
    "ImportConfig",
    "ProcessingResults",
    "HeaderMode",
    "ProcessingError"
]
