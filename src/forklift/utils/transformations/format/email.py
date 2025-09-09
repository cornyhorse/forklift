"""Email address formatting utilities."""

from __future__ import annotations

import re
from .base import BaseFormatter
from ..configs import EmailConfig


class EmailFormatter(BaseFormatter):
    """Formatter for email addresses."""

    def __init__(self, config: EmailConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single email value according to the specified rules."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty email value")

        if self.config.normalize_case:
            original_value = original_value.lower()

        if self.config.strip_whitespace:
            original_value = original_value.strip()

        if self.config.normalize_domain and '.' in original_value:
            original_value = re.sub(r'\.+$', '', original_value)

        if self.config.validate_format:
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(pattern, original_value):
                raise ValueError("Invalid email format")

        return original_value
