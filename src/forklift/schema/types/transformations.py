"""Transformation type definitions and analysis."""

import re
from typing import Dict, List, Optional, Any
import pyarrow as pa
import pandas as pd


class TransformationAnalyzer:
    """Analyzes data to suggest appropriate transformations."""

    @staticmethod
    def analyze_column_for_transformations(column_name: str, column_data: pa.Array, arrow_type: pa.DataType) -> Optional[Dict[str, Any]]:
        """Analyze a column to suggest appropriate transformations based on data patterns.

        Args:
            column_name: Name of the column
            column_data: PyArrow array containing column data
            arrow_type: PyArrow data type

        Returns:
            Optional[Dict]: Suggested transformations or None
        """
        suggestions = {}

        # Convert to pandas for analysis
        pandas_series = column_data.to_pandas()
        sample_values = pandas_series.dropna().head(10).astype(str).tolist()

        if not sample_values:
            return None

        # Check for money patterns
        money_patterns = [r'\$', r'€', r'£', r'¥', r'₹', r'₽', r'\(.*\)', r'\d+,\d+', r'\d+\.\d{2}$']
        if any(re.search(pattern, str(val)) for pattern in money_patterns for val in sample_values[:5]):
            suggestions["money_conversion"] = {
                "enabled": False,
                "currency_symbols": ["$", "€", "£", "¥", "₹", "₽", "¢"],
                "thousands_separator": ",",
                "decimal_separator": ".",
                "parentheses_negative": True,
                "strip_whitespace": True
            }

        # Check for numeric fields with separators
        if pa.types.is_string(arrow_type):
            numeric_with_separators = any(re.search(r'\d+[,\.]\d+', str(val)) for val in sample_values[:5])
            if numeric_with_separators:
                suggestions["numeric_cleaning"] = {
                    "enabled": False,
                    "thousands_separator": ",",
                    "decimal_separator": ".",
                    "allow_nan": True,
                    "target_type": "double"
                }

        # Check for HTML/XML content
        html_patterns = [r'<[^>]+>', r'&\w+;']
        if any(re.search(pattern, str(val)) for pattern in html_patterns for val in sample_values[:5]):
            suggestions["html_xml_cleaning"] = {
                "enabled": False,
                "strip_tags": True,
                "decode_entities": True,
                "preserve_whitespace": False
            }

        # Check for excessive whitespace
        if any(re.search(r'^\s+|\s+$|\s{2,}', str(val)) for val in sample_values[:5]):
            suggestions["string_trimming"] = {
                "enabled": False,
                "side": "both",
                "chars": None
            }
            suggestions["regex_replace"] = {
                "enabled": False,
                "pattern": r'\s+',
                "replacement": " ",
                "flags": 0
            }

        # Add standard string operations for string columns
        if pa.types.is_string(arrow_type) and len(sample_values) > 0:
            if "string_trimming" not in suggestions:
                suggestions["string_trimming"] = {
                    "enabled": False,
                    "side": "both",
                    "chars": None
                }

        return suggestions if suggestions else None

    @staticmethod
    def get_transformation_types_config() -> Dict[str, Any]:
        """Get configuration for all available transformation types.

        Returns:
            Dict: Transformation types configuration
        """
        return {
            "regex_replace": {
                "description": "Apply regex pattern replacements",
                "parameters": {
                    "pattern": "string",
                    "replacement": "string",
                    "flags": "int (re module flags)"
                }
            },
            "string_replace": {
                "description": "Simple string replacement (like Python str.replace)",
                "parameters": {
                    "old": "string",
                    "new": "string",
                    "count": "int (-1 for all)"
                }
            },
            "money_conversion": {
                "description": "Convert money strings to decimal values",
                "parameters": {
                    "currency_symbols": "array",
                    "thousands_separator": "string",
                    "decimal_separator": "string",
                    "parentheses_negative": "boolean",
                    "strip_whitespace": "boolean"
                }
            },
            "numeric_cleaning": {
                "description": "Clean numeric fields with separator handling",
                "parameters": {
                    "thousands_separator": "string",
                    "decimal_separator": "string",
                    "allow_nan": "boolean",
                    "target_type": "string (int64, double, etc.)"
                }
            },
            "string_padding": {
                "description": "Pad strings (lpad/rpad)",
                "parameters": {
                    "width": "int",
                    "fillchar": "string",
                    "side": "string (left, right, both)"
                }
            },
            "string_trimming": {
                "description": "Trim strings (lstrip/rstrip/strip)",
                "parameters": {
                    "side": "string (left, right, both)",
                    "chars": "string (null for whitespace)"
                }
            },
            "html_xml_cleaning": {
                "description": "Remove HTML/XML tags and decode entities",
                "parameters": {
                    "strip_tags": "boolean",
                    "decode_entities": "boolean",
                    "preserve_whitespace": "boolean"
                }
            }
        }
