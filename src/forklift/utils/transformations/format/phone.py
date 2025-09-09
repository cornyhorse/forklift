"""Phone number formatting utilities."""

from __future__ import annotations

import re
from .base import BaseFormatter, ValidationMixin
from ..configs import PhoneNumberConfig


class PhoneNumberFormatter(BaseFormatter, ValidationMixin):
    """Formatter for phone numbers."""

    def __init__(self, config: PhoneNumberConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single phone number value according to the specified style."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty phone number value")

        digits_and_plus = re.sub(r'[^\d+]', '', original_value)
        digits_only = self.extract_digits(original_value)

        if not digits_only:
            raise ValueError("No digits found in phone number")

        if self.config.validate and self.has_letters(original_value):
            raise ValueError("Phone number contains letters")

        # Handle country code detection
        has_country_code, phone_digits = self._parse_country_code(digits_and_plus, digits_only)

        # Validate phone number length
        if self.config.validate:
            if len(phone_digits) < self.config.min_digits or len(phone_digits) > self.config.max_digits:
                if len(digits_only) == 11 and digits_only.startswith('1'):
                    if len(phone_digits) != 10:
                        raise ValueError(f"Phone number must have {self.config.min_digits}-{self.config.max_digits} digits, got {len(phone_digits)}")
                else:
                    raise ValueError(f"Phone number must have {self.config.min_digits}-{self.config.max_digits} digits, got {len(phone_digits)}")

        # Format according to style
        formatted_number = self._apply_format_style(phone_digits, digits_only, has_country_code, original_value)

        # Replace dashes with dots if requested
        if self.config.use_dots:
            formatted_number = formatted_number.replace('-', '.')

        return formatted_number

    def _parse_country_code(self, digits_and_plus: str, digits_only: str) -> tuple[bool, str]:
        """Parse and detect country code presence."""
        has_country_code = False
        phone_digits = digits_only

        if digits_and_plus.startswith('+1') and len(digits_only) == 11 and digits_only.startswith('1'):
            has_country_code = True
            phone_digits = digits_only[1:]
        elif not digits_and_plus.startswith('+') and len(digits_only) == 11 and digits_only.startswith('1'):
            has_country_code = True
            phone_digits = digits_only[1:]
        elif len(digits_only) == 10:
            has_country_code = False
            phone_digits = digits_only

        return has_country_code, phone_digits

    def _apply_format_style(self, phone_digits: str, digits_only: str, has_country_code: bool, original_value: str) -> str:
        """Apply the specified format style."""
        if self.config.format_style == "international":
            return self._format_international(phone_digits, digits_only, has_country_code)
        elif self.config.format_style == "us-standard":
            return self._format_us_standard(phone_digits, has_country_code)
        elif self.config.format_style == "digits-only":
            return self._format_digits_only(phone_digits, has_country_code)
        else:  # preserve
            return original_value

    def _format_international(self, phone_digits: str, digits_only: str, has_country_code: bool) -> str:
        """Format in international style."""
        if self.config.include_country_code or has_country_code:
            if phone_digits and len(phone_digits) == 10:
                return f"+1 {phone_digits}"
            else:
                return f"+1 {digits_only}"
        else:
            return phone_digits

    def _format_us_standard(self, phone_digits: str, has_country_code: bool) -> str:
        """Format in US standard style."""
        if len(phone_digits) == 10:
            if self.config.include_country_code or has_country_code:
                if self.config.use_parentheses:
                    return f"1({phone_digits[:3]}) {phone_digits[3:6]}-{phone_digits[6:]}"
                else:
                    return f"1-{phone_digits[:3]}-{phone_digits[3:6]}-{phone_digits[6:]}"
            else:
                if self.config.use_parentheses:
                    return f"({phone_digits[:3]}) {phone_digits[3:6]}-{phone_digits[6:]}"
                else:
                    return f"{phone_digits[:3]}-{phone_digits[3:6]}-{phone_digits[6:]}"
        else:
            return phone_digits

    def _format_digits_only(self, phone_digits: str, has_country_code: bool) -> str:
        """Format as digits only."""
        if self.config.include_country_code or has_country_code:
            return f"1{phone_digits}"
        else:
            return phone_digits
