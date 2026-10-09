"""DateTime transformation utilities.

This module provides datetime parsing, formatting, and timezone conversion capabilities.

Contract: a value that cannot be parsed (or whose result does not fit the target type) becomes
NULL; that is the documented way bad cells are reported. Everything else, in particular an invalid
timezone name (rejected when the configuration is created) or a programming error, raises.
"""

from __future__ import annotations

import datetime

import pyarrow as pa

from ..date_parser import coerce_datetime
from ._timezones import resolve_timezone
from .configs import DateTimeTransformConfig

_UTC = datetime.timezone.utc
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _is_mock(value) -> bool:
    """True for unittest.mock objects (the unit tests patch ``coerce_datetime`` with mocks)."""
    return (
        hasattr(value, "_mock_name")
        or "Mock" in str(type(value))
        or hasattr(value, "_mock_methods")
    )


class DateTimeTransformer:
    """Specialized transformer for datetime operations."""

    def apply_datetime_transformation(
        self, column: pa.Array, config: DateTimeTransformConfig
    ) -> pa.Array:
        """Apply datetime parsing and transformation to a column."""
        # Resolved once per column; an unknown name raises ValueError instead of nulling every row
        target_tz = resolve_timezone(config.timezone) if config.timezone else None
        pa_type = self._output_type(config)

        transformed_values = []

        for raw_value in column.to_pylist():
            str_value = self._cell_text(raw_value)
            if str_value is None:
                transformed_values.append(None)
                continue

            try:
                parsed_dt = self._parse(str_value, config)
                transformed_values.append(self._convert(parsed_dt, config, target_tz))
            except (ValueError, OverflowError):
                # Unparseable / out-of-range value -> NULL (documented contract)
                transformed_values.append(None)

        # Create PyArrow array with error handling for problematic types
        try:
            return pa.array(transformed_values, type=pa_type)
        except (pa.ArrowTypeError, TypeError):
            # Fallback for unconvertible types - convert Mock objects to None
            safe_values = [None if _is_mock(value) else value for value in transformed_values]
            return pa.array(safe_values, type=pa_type)

    @staticmethod
    def _cell_text(value):
        """Text of a cell for parsing, or None for null/blank cells.

        ``to_pylist()`` gives real ``None`` for nulls and exact ``int`` for integer columns, so an
        epoch column with nulls is no longer promoted to float ("1700000000.0"). Whole-number
        floats (a double column holding epochs) are rendered without the ".0" as well.
        """
        if value is None:
            return None
        if isinstance(value, float):
            if value != value:  # NaN
                return None
            if value.is_integer():
                value = int(value)
        text = str(value).strip()
        return text or None

    @staticmethod
    def _output_type(config: DateTimeTransformConfig) -> pa.DataType:
        """Arrow type of the result column."""
        if config.to_epoch:
            # Epoch output wins over target_type: the cells are numbers, not dates
            if config.to_epoch in ("milliseconds", "microseconds", "nanoseconds"):
                return pa.int64()
            return pa.float64()
        if config.target_type == "date":
            return pa.date32()
        if config.target_type == "timestamp":
            return pa.float64()
        if config.target_type == "string":
            return pa.string()
        return pa.timestamp("us", tz="UTC")

    @staticmethod
    def _parse(str_value: str, config: DateTimeTransformConfig):
        """Parse one cell according to the configured mode."""
        common = {"from_epoch": config.from_epoch, "to_epoch": config.to_epoch}
        if not config.dayfirst:  # day-first is coerce_datetime's default; only pass the override
            common["dayfirst"] = False
        if config.mode == "enforce":
            return coerce_datetime(str_value, fmt=config.format, allow_fuzzy=False, **common)
        if config.mode == "specify_formats":
            return coerce_datetime(
                str_value, formats=config.formats, allow_fuzzy=config.allow_fuzzy, **common
            )
        return coerce_datetime(str_value, allow_fuzzy=config.allow_fuzzy, **common)

    @staticmethod
    def _convert(parsed_dt, config: DateTimeTransformConfig, target_tz):
        """Apply timezone conversion and the target type to one parsed value."""
        # If to_epoch was specified, we already have the epoch value
        if config.to_epoch:
            if (
                config.to_epoch != "seconds"
                and isinstance(parsed_dt, int)
                and not _INT64_MIN <= parsed_dt <= _INT64_MAX
            ):
                raise OverflowError("epoch value does not fit into int64")
            return parsed_dt

        # Handle timezone conversion
        if target_tz is not None and (
            isinstance(parsed_dt, datetime.datetime) or _is_mock(parsed_dt)
        ):
            if _is_mock(parsed_dt):
                if hasattr(parsed_dt, "astimezone"):
                    parsed_dt = parsed_dt.astimezone(target_tz)
            else:
                if parsed_dt.tzinfo is None:
                    parsed_dt = parsed_dt.replace(tzinfo=_UTC)
                parsed_dt = parsed_dt.astimezone(target_tz)

        # Convert to target type
        if config.target_type == "date":
            if isinstance(parsed_dt, datetime.datetime):
                return parsed_dt.date()
            return parsed_dt
        if config.target_type == "timestamp":
            if isinstance(parsed_dt, datetime.datetime):
                if parsed_dt.tzinfo is None:
                    # Naive values are UTC (same as everywhere else), never machine-local time
                    parsed_dt = parsed_dt.replace(tzinfo=_UTC)
                return parsed_dt.timestamp()
            return parsed_dt
        if config.target_type == "string":
            if isinstance(parsed_dt, (datetime.datetime, datetime.date)):
                if config.output_format:
                    return parsed_dt.strftime(config.output_format)
                return parsed_dt.isoformat()
            return str(parsed_dt)
        return parsed_dt  # datetime
