"""Input handlers for different file formats.

This module provides a clean interface to various input handler classes for reading
and preprocessing data from different file formats including CSV, Fixed Width Files,
Excel, and JSON files.

The module is organized with separate files for each responsibility:
- config.py: Configuration classes for input processing
- csv.py: CSV file input handling with header detection and preprocessing
- future_handlers.py: Placeholder handlers for future file format implementations
"""

# Import configuration
from .config import CsvInputConfig

# Import core input handlers
from .csv import CsvInputHandler

# Import future/placeholder handlers
from .future_handlers import FwfInputHandler, ExcelInputHandler, JsonInputHandler

# Define public API
__all__ = [
    # Configuration
    "CsvInputConfig",

    # Core handlers
    "CsvInputHandler",

    # Future handlers
    "FwfInputHandler",
    "ExcelInputHandler",
    "JsonInputHandler",
]
