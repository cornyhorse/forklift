"""CSV-specific data processor implementation.

This module has been refactored into smaller, organized components.
The main CSVProcessor class is now imported from the csv package.
"""

# Import from the new organized package structure
from .csv import CSVProcessor

# Maintain backward compatibility by exposing the main class
__all__ = ['CSVProcessor']
