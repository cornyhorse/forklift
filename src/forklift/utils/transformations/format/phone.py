"""Phone number formatting utilities."""

from __future__ import annotations

import re

from ..configs import PhoneNumberConfig
from .base import BaseFormatter, ValidationMixin


class PhoneNumberFormatter(BaseFormatter, ValidationMixin):
    """Formatter for phone numbers.

    US/Canadian (NANP) numbers are formatted according to the configured style. A number with an
    explicit ``+`` country code other than ``+1`` is treated as international: it is validated
    against the E.164 length limits (``min_digits``/``max_digits`` describe NANP numbers and do
    not apply) and written in E.164 form (``+442079460958``); a ``+1`` is never added to it.
    """

    # E.164: country code + national number has at most 15 digits; 7 is the practical minimum
    E164_MIN_DIGITS = 7
    E164_MAX_DIGITS = 15

    def __init__(self, config: PhoneNumberConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single phone number value according to the specified style."""
        # "5551234567.0" (a number column that went through float) is 5551234567
        original_value = self.strip_float_suffix(value.strip())

        if not original_value:
            raise ValueError("Empty phone number value")

        digits_and_plus = re.sub(r"[^\d+]", "", original_value)
        digits_only = self.extract_digits(original_value)

        if not digits_only:
            raise ValueError("No digits found in phone number")

        if self.config.validate and self.has_letters(original_value):
            raise ValueError("Phone number contains letters")

        if digits_and_plus.startswith("+") and not digits_and_plus.startswith("+1"):
            return self._format_international_number(digits_only, original_value)

        # Handle country code detection
        has_country_code, phone_digits = self._parse_country_code(digits_and_plus, digits_only)

        # Validate phone number length
        if self.config.validate:
            if (
                len(phone_digits) < self.config.min_digits
                or len(phone_digits) > self.config.max_digits
            ):
                if len(digits_only) == 11 and digits_only.startswith("1"):
                    if len(phone_digits) != 10:
                        raise ValueError(
                            f"Phone number must have {self.config.min_digits}-"
                            f"{self.config.max_digits} digits, got {len(phone_digits)}"
                        )
                else:
                    raise ValueError(
                        f"Phone number must have {self.config.min_digits}-"
                        f"{self.config.max_digits} digits, got {len(phone_digits)}"
                    )

        # Format according to style
        return self._apply_format_style(
            phone_digits, digits_only, has_country_code, original_value
        )

    def _format_international_number(self, digits: str, original_value: str) -> str:
        """Format a number with an explicit non-NANP country code (E.164 style)."""
        if self.config.validate:
            if digits.startswith("0"):
                raise ValueError("International phone number has an invalid country code")
            if not self.E164_MIN_DIGITS <= len(digits) <= self.E164_MAX_DIGITS:
                raise ValueError(
                    f"International phone number must have {self.E164_MIN_DIGITS}-"
                    f"{self.E164_MAX_DIGITS} digits including the country code, got {len(digits)}"
                )

        if self.config.format_style == "preserve":
            return original_value
        if self.config.format_style == "digits-only":
            return digits
        # "us-standard" cannot describe a foreign number; "international" is E.164
        return f"+{digits}"

    def _parse_country_code(self, digits_and_plus: str, digits_only: str) -> tuple[bool, str]:
        """Parse and detect country code presence."""
        has_country_code = False
        phone_digits = digits_only

        if (
            digits_and_plus.startswith("+1")
            and len(digits_only) == 11
            and digits_only.startswith("1")
        ):
            has_country_code = True
            phone_digits = digits_only[1:]
        elif (
            not digits_and_plus.startswith("+")
            and len(digits_only) == 11
            and digits_only.startswith("1")
        ):
            has_country_code = True
            phone_digits = digits_only[1:]
        elif len(digits_only) == 10:
            has_country_code = False
            phone_digits = digits_only

        return has_country_code, phone_digits

    def _apply_format_style(
        self, phone_digits: str, digits_only: str, has_country_code: bool, original_value: str
    ) -> str:
        """Apply the specified format style."""
        if self.config.format_style == "international":
            return self._format_international(phone_digits, digits_only, has_country_code)
        elif self.config.format_style == "us-standard":
            return self._format_us_standard(phone_digits, has_country_code)
        elif self.config.format_style == "digits-only":
            return self._format_digits_only(phone_digits, has_country_code)
        else:  # preserve
            return original_value

    def _format_international(
        self, phone_digits: str, digits_only: str, has_country_code: bool
    ) -> str:
        """Format in international style."""
        if (self.config.include_country_code or has_country_code) and len(phone_digits) == 10:
            return f"+1 {phone_digits}"
        # Not a 10-digit NANP number: never invent a "+1" country code for it
        return phone_digits

    def _format_us_standard(self, phone_digits: str, has_country_code: bool) -> str:
        """Format in US standard style."""
        if len(phone_digits) != 10:
            return phone_digits

        # One separator for the number groups: dots, dashes (default) or plain spaces
        if self.config.use_dots:
            separator = "."
        elif self.config.use_dashes:
            separator = "-"
        else:
            separator = " "

        area, prefix, line = phone_digits[:3], phone_digits[3:6], phone_digits[6:]
        with_country_code = self.config.include_country_code or has_country_code

        if self.config.use_parentheses:
            formatted = f"({area}) {prefix}{separator}{line}"
            return f"1{formatted}" if with_country_code else formatted

        formatted = f"{area}{separator}{prefix}{separator}{line}"
        return f"1{separator}{formatted}" if with_country_code else formatted

    def _format_digits_only(self, phone_digits: str, has_country_code: bool) -> str:
        """Format as digits only."""
        if (self.config.include_country_code or has_country_code) and len(phone_digits) == 10:
            return f"1{phone_digits}"
        return phone_digits
