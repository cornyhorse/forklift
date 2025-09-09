"""ZIP code formatting utilities."""

from __future__ import annotations

from .base import BaseFormatter, ValidationMixin
from ..configs import ZipCodeConfig


class ZipCodeFormatter(BaseFormatter, ValidationMixin):
    """Formatter for ZIP codes."""

    def __init__(self, config: ZipCodeConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single ZIP code value according to the specified type."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty ZIP code value")

        digits_only = self.extract_digits(original_value)

        if not digits_only:
            raise ValueError("No digits found in ZIP code")

        if self.config.validate and len(digits_only) < len(original_value) * 0.5:
            raise ValueError("ZIP code contains too many non-digit characters")

        if self.config.zip_type == "zip-5":
            return self._format_zip5(digits_only)
        elif self.config.zip_type == "zip-9":
            return self._format_zip9(digits_only)
        else:  # zip-permissive
            return self._format_zip_permissive(digits_only)

    def _format_zip5(self, digits_only: str) -> str:
        """Format as 5-digit ZIP code."""
        if self.config.zero_pad and len(digits_only) < 5:
            digits_only = digits_only.zfill(5)
        elif len(digits_only) > 5:
            digits_only = digits_only[:5]

        if self.config.validate and len(digits_only) != 5:
            raise ValueError(f"ZIP-5 must have exactly 5 digits, got {len(digits_only)}")

        return digits_only

    def _format_zip9(self, digits_only: str) -> str:
        """Format as 9-digit ZIP code."""
        if self.config.validate and len(digits_only) != 9:
            raise ValueError(f"ZIP-9 must have exactly 9 digits, got {len(digits_only)}")

        if self.config.zero_pad and len(digits_only) < 9:
            digits_only = digits_only.zfill(9)

        if self.config.format_with_dash and len(digits_only) == 9:
            return f"{digits_only[:5]}-{digits_only[5:]}"
        else:
            return digits_only

    def _format_zip_permissive(self, digits_only: str) -> str:
        """Format with permissive rules (5 or 9 digits)."""
        if self.config.validate:
            if len(digits_only) not in [5, 9]:
                raise ValueError(f"ZIP code must have 5 or 9 digits, got {len(digits_only)}")

        if self.config.zero_pad:
            if len(digits_only) <= 5:
                digits_only = digits_only.zfill(5)
            elif len(digits_only) <= 9:
                digits_only = digits_only.zfill(9)

        if len(digits_only) == 9 and self.config.format_with_dash:
            return f"{digits_only[:5]}-{digits_only[5:]}"
        else:
            return digits_only
