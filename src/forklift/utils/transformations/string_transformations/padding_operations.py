"""String padding and trimming operations."""

from __future__ import annotations
from typing import Optional

import pyarrow as pa
from ..configs import StringPaddingConfig


class PaddingOperations:
    """Handles string padding and trimming operations."""

    @staticmethod
    def apply_string_padding(column: pa.Array, config: StringPaddingConfig) -> pa.Array:
        """Apply string padding operations (lstrip, rstrip, lpad, rpad)."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()

        if config.side == "left":
            transformed_series = pandas_series.str.rjust(config.width, config.fillchar)
        elif config.side == "right":
            transformed_series = pandas_series.str.ljust(config.width, config.fillchar)
        elif config.side == "both":
            transformed_series = pandas_series.str.center(config.width, config.fillchar)
        else:
            transformed_series = pandas_series.str.rjust(config.width, config.fillchar)

        return pa.array(transformed_series)

    @staticmethod
    def apply_string_trimming(column: pa.Array, side: str = "both", chars: Optional[str] = None) -> pa.Array:
        """Apply string trimming operations (lstrip, rstrip, strip)."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()

        if side == "left":
            transformed_series = pandas_series.str.lstrip(chars)
        elif side == "right":
            transformed_series = pandas_series.str.rstrip(chars)
        elif side == "both":
            transformed_series = pandas_series.str.strip(chars)
        else:
            transformed_series = pandas_series.str.strip(chars)

        return pa.array(transformed_series)
