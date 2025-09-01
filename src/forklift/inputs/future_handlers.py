"""Future input handlers for file types not yet fully implemented."""

from __future__ import annotations
from typing import Dict, Any
from pathlib import Path

# Import the actual FWF handler
from .fwf import FwfInputHandler
from .config import FwfInputConfig


class ExcelInputHandler:
    """Handles Excel file input (placeholder for future implementation).

    This class will provide functionality for reading Excel files (.xlsx, .xls)
    with support for multiple worksheets, cell formatting, and data type detection.

    Args:
        config: Dictionary containing Excel processing configuration

    Attributes:
        config: The configuration dictionary for this input handler
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the Excel input handler.

        Args:
            config: Configuration dictionary containing Excel processing parameters
        """
        self.config = config

    def read_file(self, file_path: str):
        """Read Excel file (placeholder).

        Args:
            file_path: Path to the Excel file to read

        Raises:
            NotImplementedError: This functionality is not yet implemented
        """
        raise NotImplementedError("Excel input handling not yet implemented")
