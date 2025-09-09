"""Data type conversion utilities."""

from typing import Dict, Any
import pyarrow as pa


class DataTypeConverter:
    """Handles conversion between different data type representations."""

    @staticmethod
    def arrow_to_json_schema_type(arrow_type: pa.DataType) -> Dict[str, Any]:
        """Convert PyArrow type to JSON Schema type definition.

        Args:
            arrow_type: PyArrow data type

        Returns:
            Dict: JSON Schema type definition
        """
        if pa.types.is_integer(arrow_type):
            return {"type": "integer"}
        elif pa.types.is_floating(arrow_type):
            return {"type": "number"}
        elif pa.types.is_boolean(arrow_type):
            return {"type": "boolean"}
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return {"type": "string"}
        elif pa.types.is_date(arrow_type):
            return {"type": "string", "format": "date"}
        elif pa.types.is_timestamp(arrow_type):
            return {"type": "string", "format": "date-time"}
        elif pa.types.is_time(arrow_type):
            return {"type": "string", "format": "time"}
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return {"type": "string", "contentEncoding": "base64"}
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            if hasattr(arrow_type, 'value_type'):
                value_type = DataTypeConverter.arrow_to_json_schema_type(arrow_type.value_type)
            else:
                value_type = {"type": "string"}
            return {"type": "array", "items": value_type}
        elif pa.types.is_struct(arrow_type):
            return {"type": "object", "additionalProperties": True}
        elif pa.types.is_dictionary(arrow_type):
            return {"type": "string"}
        else:
            return {"type": "string"}

    @staticmethod
    def detect_numeric_patterns(sample_values: list) -> Dict[str, bool]:
        """Detect numeric patterns in string data.

        Args:
            sample_values: List of sample string values

        Returns:
            Dict: Pattern detection results
        """
        import re

        patterns = {
            'has_thousands_separator': False,
            'has_decimal_separator': False,
            'has_currency_symbols': False,
            'has_parentheses_negative': False
        }

        currency_pattern = r'[\$€£¥₹₽¢]'
        thousands_pattern = r'\d+,\d+'
        decimal_pattern = r'\d+\.\d+'
        parentheses_pattern = r'\(.*\)'

        for value in sample_values[:10]:  # Check first 10 values
            str_val = str(value)
            if re.search(currency_pattern, str_val):
                patterns['has_currency_symbols'] = True
            if re.search(thousands_pattern, str_val):
                patterns['has_thousands_separator'] = True
            if re.search(decimal_pattern, str_val):
                patterns['has_decimal_separator'] = True
            if re.search(parentheses_pattern, str_val):
                patterns['has_parentheses_negative'] = True

        return patterns
