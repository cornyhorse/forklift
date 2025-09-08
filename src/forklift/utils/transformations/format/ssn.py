"""SSN (Social Security Number) formatting utilities."""

from __future__ import annotations

from .base import BaseFormatter, ValidationMixin
from ..configs import SSNConfig


class SSNFormatter(BaseFormatter, ValidationMixin):
    """Formatter for Social Security Numbers."""

    def __init__(self, config: SSNConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single SSN value to XXX-XX-XXXX format."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty SSN value")

        # Remove all non-digits
        digits_only = self.extract_digits(original_value)

        # Check if no digits found first (this should take priority)
        if not digits_only:
            raise ValueError("No digits found in SSN")

        # Check for letters in original (only after we know there are digits)
        if self.config.validate and self.has_letters(original_value):
            raise ValueError("SSN contains letters")

        # Validate length before zero padding
        if self.config.validate and len(digits_only) != 9:
            raise ValueError(f"SSN must have exactly 9 digits, got {len(digits_only)}")

        # Handle zero padding
        if self.config.zero_pad and len(digits_only) < 9:
            digits_only = digits_only.zfill(9)

        # Format with dashes
        if self.config.format_with_dashes:
            if len(digits_only) >= 9:
                return f"{digits_only[:3]}-{digits_only[3:5]}-{digits_only[5:]}"
            elif len(digits_only) >= 6:
                if len(digits_only) == 8:
                    return f"{digits_only[:3]}-{digits_only[3:5]}-{digits_only[5:]}"
                elif len(digits_only) == 7:
                    return f"{digits_only[:3]}-{digits_only[3:5]}-{digits_only[5:]}"
                else:  # exactly 6
                    return f"{digits_only[:2]}-{digits_only[2:4]}-{digits_only[4:]}"
            elif len(digits_only) >= 4:
                if len(digits_only) == 5:
                    return f"{digits_only[:2]}-{digits_only[2:4]}-{digits_only[4:]}"
                else:  # exactly 4
                    return f"{digits_only[:2]}-{digits_only[2:]}"
            elif len(digits_only) == 3:
                return digits_only  # Too short for dashes
            else:  # 1 or 2 digits
                return digits_only
        else:
            return digits_only
