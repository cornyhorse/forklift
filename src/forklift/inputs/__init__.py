"""Input handlers for different file formats.

This module provides a clean interface to various input handler classes for reading
and preprocessing data from different file formats including CSV, Fixed Width Files,
Excel, and JSON files.

The module is organized with separate files for each responsibility:
- config.py: Configuration classes for input processing
- csv.py: CSV file input handling with header detection and preprocessing
- excel.py: Excel file input handling with multi-sheet support
- fwf.py: Fixed Width File input handling
- future_handlers.py: Placeholder handlers for future file format implementations
"""

# Import configuration
from .config import CsvInputConfig, FwfInputConfig, FwfFieldSpec, FwfConditionalSchema, ExcelInputConfig, ExcelSheetConfig, SqlInputConfig

# Import core input handlers
from .csv import CsvInputHandler
from .fwf import FwfInputHandler
from .excel import ExcelInputHandler
from .sql import SqlInputHandler

# Define public API
__all__ = [
    # Core handlers
    "CsvInputHandler",
    "FwfInputHandler",
    "ExcelInputHandler",
    "SqlInputHandler",

    # Configuration
    "CsvInputConfig",
    "FwfInputConfig",
    "FwfFieldSpec",
    "FwfConditionalSchema",
    "ExcelInputConfig",
    "ExcelSheetConfig",
    "SqlInputConfig",
]
