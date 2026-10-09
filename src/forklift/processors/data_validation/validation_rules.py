"""Individual validation rule implementations.

Error messages name the field and the rule but, by default, do not contain the offending cell
value (it may be personal data). Pass ``include_values=True`` (``include_values_in_errors`` in
``ValidationConfig``) to add it.
"""

from datetime import date, datetime
from decimal import InvalidOperation
from typing import Any, List, Optional

from .._regex import compile_pattern, pattern_matches
from .._values import compare_temporal, parse_temporal, to_decimal
from .validation_config import DateValidation, EnumValidation, RangeValidation, StringValidation


def _shown(value: Any, include_values: bool) -> str:
    """`` 'value'`` (with a leading space) when values are allowed in messages, else ``''``."""
    return f" '{value}'" if include_values else ""


class ValidationRules:
    """Collection of validation rule implementations."""

    @staticmethod
    def is_null_or_empty(value: Any) -> bool:
        """Check if a value is null or empty."""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        return False

    @staticmethod
    def check_range_config(range_val: RangeValidation) -> None:
        """Raise ``ValueError`` if the bounds of a range rule are not numbers or dates."""
        for bound in (range_val.min_value, range_val.max_value):
            if bound is not None:
                _convert_bound(bound, temporal=_is_temporal(bound))

    @staticmethod
    def validate_range(
        field_name: str, value: Any, range_val: RangeValidation, include_values: bool = False
    ) -> Optional[str]:
        """Validate value against range constraints (numeric or date).

        Numbers are compared as exact decimals (strings keep their precision, ``0.01`` equals
        ``Decimal("0.01")``), NaN never passes, and date bounds compare dates/datetimes/ISO
        strings. Invalid bounds raise ``ValueError`` (configuration error).
        """
        shown = _shown(value, include_values)
        bounds = (range_val.min_value, range_val.max_value)
        try:
            temporal = isinstance(value, (date, datetime)) or any(_is_temporal(b) for b in bounds)

            if temporal:
                value = parse_temporal(value)
                if value is None:
                    return (
                        f"Field '{field_name}' value{shown} "
                        f"cannot be converted to a date for range validation"
                    )
            else:
                try:
                    value = to_decimal(value)
                except (InvalidOperation, TypeError, ValueError):
                    return (
                        f"Field '{field_name}' value{shown} "
                        f"cannot be converted to numeric for range validation"
                    )
                if value.is_nan():
                    return f"Field '{field_name}' value{shown} is not a number (NaN)"

            min_val = _convert_bound(range_val.min_value, temporal)
            max_val = _convert_bound(range_val.max_value, temporal)

            if min_val is not None:
                order = _order(value, min_val)
                if range_val.inclusive:
                    if order < 0:
                        return (
                            f"Field '{field_name}' value{shown} is below minimum "
                            f"{range_val.min_value}"
                        )
                elif order <= 0:
                    return (
                        f"Field '{field_name}' value{shown} is not greater than "
                        f"{range_val.min_value}"
                    )

            if max_val is not None:
                order = _order(value, max_val)
                if range_val.inclusive:
                    if order > 0:
                        return (
                            f"Field '{field_name}' value{shown} is above maximum "
                            f"{range_val.max_value}"
                        )
                elif order >= 0:
                    return (
                        f"Field '{field_name}' value{shown} is not less than "
                        f"{range_val.max_value}"
                    )

            return None

        except ValueError:
            raise
        except Exception:
            return f"Field '{field_name}' range validation error"

    @staticmethod
    def validate_string(
        field_name: str,
        value: Any,
        string_val: StringValidation,
        include_values: bool = False,
    ) -> Optional[str]:
        """Validate string constraints."""
        if not isinstance(value, str):
            value = str(value)

        # Check empty
        if not string_val.allow_empty and value.strip() == "":
            return f"Field '{field_name}' cannot be empty"

        # Check minimum length
        if string_val.min_length is not None and len(value) < string_val.min_length:
            return (
                f"Field '{field_name}' length {len(value)} "
                f"is below minimum {string_val.min_length}"
            )

        # Check maximum length
        if string_val.max_length is not None and len(value) > string_val.max_length:
            return (
                f"Field '{field_name}' length {len(value)} exceeds maximum {string_val.max_length}"
            )

        # Check pattern (compiled and cached; invalid patterns raise ValueError)
        if string_val.pattern is not None:
            compiled = compile_pattern(string_val.pattern, string_val.allow_unsafe_regex)
            if not pattern_matches(compiled, value):
                return (
                    f"Field '{field_name}' value{_shown(value, include_values)} "
                    f"does not match required pattern"
                )

        return None

    @staticmethod
    def validate_enum(
        field_name: str, value: Any, enum_val: EnumValidation, include_values: bool = False
    ) -> Optional[str]:
        """Validate enumeration constraints."""
        allowed_values = enum_val.allowed_values
        message = (
            f"Field '{field_name}' value{_shown(value, include_values)} "
            f"not in allowed values: {allowed_values}"
        )

        if enum_val.case_sensitive:
            if value not in allowed_values:
                return message
        else:
            # Case-insensitive comparison
            value_lower = str(value).lower()
            allowed_lower = [str(v).lower() for v in allowed_values]
            if value_lower not in allowed_lower:
                return message

        return None

    @staticmethod
    def validate_date(
        field_name: str, value: Any, date_val: DateValidation, include_values: bool = False
    ) -> Optional[str]:
        """Validate date constraints.

        String values are parsed with ``date_val.formats`` (default ``%Y-%m-%d``).
        """
        formats = list(date_val.formats) if date_val.formats else []
        shown = _shown(value, include_values)

        if isinstance(value, str):
            # Explicit formats replace the ISO default (they are the accepted formats)
            parsed = parse_temporal(value, formats, allow_iso=not formats)
            if parsed is None:
                return f"Field '{field_name}' value{shown} is not a valid date"
            parsed_date = parsed.date() if isinstance(parsed, datetime) else parsed
        elif isinstance(value, (date, datetime)):
            parsed_date = value.date() if isinstance(value, datetime) else value
        else:
            return f"Field '{field_name}' value{shown} is not a valid date type"

        # Check date range
        if date_val.min_date:
            min_date = ValidationRules._config_date(date_val.min_date, formats, "min_date")
            if parsed_date < min_date:
                return f"Field '{field_name}' date{shown} is before minimum {min_date}"

        if date_val.max_date:
            max_date = ValidationRules._config_date(date_val.max_date, formats, "max_date")
            if parsed_date > max_date:
                return f"Field '{field_name}' date{shown} is after maximum {max_date}"

        return None

    @staticmethod
    def _config_date(text: Any, formats: List[str], name: str) -> date:
        parsed = parse_temporal(text, formats)
        if parsed is None:
            raise ValueError(f"DateValidation.{name} is not a valid date: '{text}'")
        return parsed.date() if isinstance(parsed, datetime) else parsed


def _order(value: Any, bound: Any) -> int:
    """-1/0/1 for dates (calendar/instant) and decimals alike."""
    if isinstance(bound, (date, datetime)):
        return compare_temporal(value, bound)
    return (value > bound) - (value < bound)


def _is_temporal(bound: Any) -> bool:
    """Whether a configured bound is a date/datetime or an ISO date string."""
    return bound is not None and (
        isinstance(bound, (date, datetime)) or parse_temporal(bound) is not None
    )


def _convert_bound(bound: Any, temporal: bool) -> Any:
    """Convert a configured range bound; ``None`` stays ``None``; bad bounds raise ValueError."""
    if bound is None:
        return None
    if temporal:
        parsed = parse_temporal(bound)
        if parsed is None:
            raise ValueError(f"Range bound '{bound}' is not a date")
        return parsed
    try:
        number = to_decimal(bound)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(
            f"Range bounds must be numbers or ISO dates, got {type(bound).__name__} '{bound}'"
        ) from None
    if number.is_nan():
        raise ValueError("Range bounds cannot be NaN")
    return number
