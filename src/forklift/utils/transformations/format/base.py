"""Base classes for format transformers."""

from __future__ import annotations

import pyarrow as pa
import pandas as pd
from abc import ABC, abstractmethod
from typing import Any


class BaseFormatter(ABC):
    """Base class for all format transformers."""

    def __init__(self, config: Any):
        """Initialize formatter with configuration."""
        self.config = config

    @abstractmethod
    def format_value(self, value: str) -> str:
        """Format a single value according to the configuration.

        Args:
            value: The value to format

        Returns:
            The formatted value

        Raises:
            ValueError: If the value is invalid and validation is enabled
        """
        pass

    def apply_formatting(self, column: pa.Array) -> pa.Array:
        """Apply formatting to a PyArrow array column.

        Args:
            column: The PyArrow array to format

        Returns:
            A new PyArrow array with formatted values
        """
        if not pa.types.is_string(column.type):
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_value = self.format_value(str(value))
                formatted_values.append(formatted_value)
            except ValueError:
                if getattr(self.config, 'allow_invalid', False):
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)


class ValidationMixin:
    """Mixin class providing common validation utilities."""

    @staticmethod
    def has_letters(value: str) -> bool:
        """Check if value contains letters."""
        import re
        return bool(re.search(r'[a-zA-Z]', value))

    @staticmethod
    def extract_digits(value: str) -> str:
        """Extract only digits from a value."""
        import re
        return re.sub(r'\D', '', value)

    @staticmethod
    def validate_length(value: str, expected_length: int, allow_shorter: bool = False) -> bool:
        """Validate string length."""
        if allow_shorter:
            return len(value) <= expected_length
        return len(value) == expected_length
