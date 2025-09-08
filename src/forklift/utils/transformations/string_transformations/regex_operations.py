"""Regex and string replacement operations."""

from __future__ import annotations

import pyarrow as pa
from ..configs import RegexReplaceConfig, StringReplaceConfig


class RegexOperations:
    """Handles regex and string replacement operations."""

    @staticmethod
    def apply_regex_replace(column: pa.Array, config: RegexReplaceConfig) -> pa.Array:
        """Apply regex replace transformation to a string column."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        transformed_series = pandas_series.str.replace(
            config.pattern,
            config.replacement,
            regex=True,
            flags=config.flags
        )
        return pa.array(transformed_series)

    @staticmethod
    def apply_string_replace(column: pa.Array, config: StringReplaceConfig) -> pa.Array:
        """Apply simple string replace transformation."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        if config.count == -1:
            transformed_series = pandas_series.str.replace(config.old, config.new)
        else:
            transformed_series = pandas_series.str.replace(config.old, config.new, n=config.count)
        return pa.array(transformed_series)
