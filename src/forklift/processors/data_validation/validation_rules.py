"""Individual validation rule implementations."""

import re
from typing import Any, Optional
from datetime import datetime, date

from .validation_config import RangeValidation, StringValidation, EnumValidation, DateValidation


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
    def validate_range(field_name: str, value: Any, range_val: RangeValidation) -> Optional[str]:
        """Validate value against range constraints."""
        try:
            # Convert value to appropriate type for comparison
            if isinstance(value, str):
                # Try to parse as number if it looks like one
                try:
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    return f"Field '{field_name}' value '{value}' cannot be converted to numeric for range validation"

            # Check minimum
            if range_val.min_value is not None:
                min_val = range_val.min_value
                if isinstance(min_val, str):
                    min_val = float(min_val) if '.' in min_val else int(min_val)

                if range_val.inclusive:
                    if value < min_val:
                        return f"Field '{field_name}' value {value} is below minimum {min_val}"
                else:
                    if value <= min_val:
                        return f"Field '{field_name}' value {value} is not greater than {min_val}"

            # Check maximum
            if range_val.max_value is not None:
                max_val = range_val.max_value
                if isinstance(max_val, str):
                    max_val = float(max_val) if '.' in max_val else int(max_val)

                if range_val.inclusive:
                    if value > max_val:
                        return f"Field '{field_name}' value {value} is above maximum {max_val}"
                else:
                    if value >= max_val:
                        return f"Field '{field_name}' value {value} is not less than {max_val}"

            return None

        except Exception as e:
            return f"Field '{field_name}' range validation error: {str(e)}"

    @staticmethod
    def validate_string(field_name: str, value: Any, string_val: StringValidation) -> Optional[str]:
        """Validate string constraints."""
        if not isinstance(value, str):
            value = str(value)

        # Check minimum length
        if string_val.min_length is not None and len(value) < string_val.min_length:
            return f"Field '{field_name}' length {len(value)} is below minimum {string_val.min_length}"

        # Check maximum length
        if string_val.max_length is not None and len(value) > string_val.max_length:
            return f"Field '{field_name}' length {len(value)} exceeds maximum {string_val.max_length}"

        # Check pattern
        if string_val.pattern is not None:
            try:
                if not re.match(string_val.pattern, value):
                    return f"Field '{field_name}' value '{value}' does not match required pattern"
            except re.error as e:
                return f"Field '{field_name}' pattern validation error: {str(e)}"

        # Check empty
        if not string_val.allow_empty and value.strip() == "":
            return f"Field '{field_name}' cannot be empty"

        return None

    @staticmethod
    def validate_enum(field_name: str, value: Any, enum_val: EnumValidation) -> Optional[str]:
        """Validate enumeration constraints."""
        allowed_values = enum_val.allowed_values

        if enum_val.case_sensitive:
            if value not in allowed_values:
                return f"Field '{field_name}' value '{value}' not in allowed values: {allowed_values}"
        else:
            # Case-insensitive comparison
            value_lower = str(value).lower()
            allowed_lower = [str(v).lower() for v in allowed_values]
            if value_lower not in allowed_lower:
                return f"Field '{field_name}' value '{value}' not in allowed values: {allowed_values}"

        return None

    @staticmethod
    def validate_date(field_name: str, value: Any, date_val: DateValidation) -> Optional[str]:
        """Validate date constraints."""
        # This is a simplified date validation - in practice you'd want more robust date parsing
        if isinstance(value, str):
            try:
                # Try to parse the date
                parsed_date = datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return f"Field '{field_name}' value '{value}' is not a valid date"
        elif isinstance(value, (date, datetime)):
            parsed_date = value.date() if isinstance(value, datetime) else value
        else:
            return f"Field '{field_name}' value '{value}' is not a valid date type"

        # Check date range
        if date_val.min_date:
            min_date = datetime.strptime(date_val.min_date, "%Y-%m-%d").date()
            if parsed_date < min_date:
                return f"Field '{field_name}' date {parsed_date} is before minimum {min_date}"

        if date_val.max_date:
            max_date = datetime.strptime(date_val.max_date, "%Y-%m-%d").date()
            if parsed_date > max_date:
                return f"Field '{field_name}' date {parsed_date} is after maximum {max_date}"

        return None
