"""Email address formatting utilities."""

from __future__ import annotations

import re

from ..configs import EmailConfig
from .base import BaseFormatter

# Dot-separated runs of local-part characters: no leading, trailing or doubled dots
_LOCAL_PART = r"[A-Za-z0-9_%+-]+(?:\.[A-Za-z0-9_%+-]+)*"
# Domain labels start and end with a letter/digit (hyphens only inside); the TLD is alphabetic
_DOMAIN_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
_EMAIL_PATTERN = re.compile(rf"{_LOCAL_PART}@(?:{_DOMAIN_LABEL}\.)+[A-Za-z]{{2,}}")

# RFC 5321 limits
_MAX_LOCAL_PART = 64
_MAX_ADDRESS = 254
_MAX_LABEL = 63


class EmailFormatter(BaseFormatter):
    """Formatter for email addresses."""

    def __init__(self, config: EmailConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single email value according to the specified rules."""
        if not value.strip():
            raise ValueError("Empty email value")

        formatted = value

        if self.config.normalize_case:
            formatted = formatted.lower()

        # Only strip when asked to: with strip_whitespace=False surrounding whitespace is kept and
        # then fails validation (or is returned as is when validation is off).
        if self.config.strip_whitespace:
            formatted = formatted.strip()

        if self.config.normalize_domain and "." in formatted:
            formatted = re.sub(r"\.+$", "", formatted)

        if self.config.validate_format and not self._is_valid(formatted):
            raise ValueError("Invalid email format")

        return formatted

    @staticmethod
    def _is_valid(address: str) -> bool:
        """Syntax check: dot-atom local part, label-based domain, RFC length limits."""
        if len(address) > _MAX_ADDRESS or not _EMAIL_PATTERN.fullmatch(address):
            return False
        local, _, domain = address.rpartition("@")
        return len(local) <= _MAX_LOCAL_PART and all(
            len(label) <= _MAX_LABEL for label in domain.split(".")
        )
