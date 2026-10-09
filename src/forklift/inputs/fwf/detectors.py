"""Detection utilities for FWF processing including encoding and schema detection."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Optional, Pattern

from ...utils.detect_encoding import detect_encoding as _detect_file_encoding
from ..config import FwfConditionalSchema, FwfFieldSpec, FwfInputConfig


class FwfEncodingDetector:
    """Handles file encoding detection."""

    @staticmethod
    def detect_encoding(file_path: Path) -> str:
        """Detect file encoding (chardet or charset_normalizer when installed).

        The detector's guess is only accepted after the whole file has been decoded with it;
        without a detector library, utf-8, cp1252 and latin-1 are tried in turn.

        Args:
            file_path: Path to file to analyze

        Returns:
            Detected encoding string (defaults to utf-8 if detection fails)
        """
        return _detect_file_encoding(file_path)


class FwfSchemaDetector:
    """Handles conditional schema detection and line classification."""

    def __init__(self, config: FwfInputConfig):
        """Initialize the schema detector.

        Args:
            config: FWF configuration containing conditional schemas
        """
        self.config = config
        self._regex_cache: Dict[str, Pattern[str]] = {}
        # Compile now so a malformed pattern is a configuration error, not a per-line failure
        for pattern in config.comment_patterns or []:
            self._compile(pattern, "comment_patterns")
        footer_pattern = self._footer_pattern()
        if footer_pattern:
            self._compile(footer_pattern, "footer_detection.pattern")

    def _compile(self, pattern: str, option: str) -> Pattern[str]:
        """Compile (and cache) a configured regular expression.

        Raises:
            ValueError: If the pattern is not a valid regular expression
        """
        compiled = self._regex_cache.get(pattern)
        if compiled is None:
            try:
                compiled = re.compile(pattern)
            except (re.error, TypeError) as e:
                raise ValueError(f"Invalid regular expression in {option}: {pattern!r} ({e})")
            self._regex_cache[pattern] = compiled
        return compiled

    def _footer_pattern(self) -> Optional[str]:
        footer = self.config.footer_detection
        if footer and footer.get("mode") == "regex" and footer.get("pattern"):
            return footer["pattern"]
        return None

    def detect_conditional_schema(self, line: str) -> Optional[FwfConditionalSchema]:
        """Detect which conditional schema applies to a line.

        Args:
            line: Line to analyze

        Returns:
            Matching conditional schema or None
        """
        if not self.config.conditional_schemas or not self.config.flag_column:
            return None

        # Extract flag value
        flag_value = self._extract_field_value(line, self.config.flag_column)

        # Find matching schema
        for schema in self.config.conditional_schemas:
            if schema.flag_value == flag_value:
                return schema

        return None

    def determine_schema(self, line: str) -> Optional[FwfConditionalSchema]:
        """Determine which schema to use for a line (alias for detect_conditional_schema).

        Args:
            line: Line to analyze

        Returns:
            Matching conditional schema or None
        """
        return self.detect_conditional_schema(line)

    def is_comment_line(self, line: str) -> bool:
        """Check if a line is a comment based on configured patterns.

        Args:
            line: Line to check

        Returns:
            True if line matches a comment pattern
        """
        if not self.config.comment_patterns:
            return False

        for pattern in self.config.comment_patterns:
            if self._compile(pattern, "comment_patterns").match(line):
                return True

        return False

    def is_comment_row(self, line: str) -> bool:
        """Alias for is_comment_line for backward compatibility."""
        return self.is_comment_line(line)

    def is_blank_line(self, line: str) -> bool:
        """Check if a line is blank or whitespace-only.

        Args:
            line: Line to check

        Returns:
            True if line is blank
        """
        return not line.strip()

    def is_footer_row(self, line: str) -> bool:
        """Check if a line is a footer based on configured patterns.

        Args:
            line: Line to check

        Returns:
            True if line matches footer detection pattern
        """
        pattern = self._footer_pattern()
        if pattern:
            return bool(self._compile(pattern, "footer_detection.pattern").match(line))

        return False

    def _extract_field_value(self, line: str, field: FwfFieldSpec) -> str:
        """Extract a field value from a line (simplified version for flag detection).

        Args:
            line: The input line to extract from
            field: Field specification

        Returns:
            Extracted field value as string
        """
        # Convert 1-based to 0-based indexing
        start_idx = field.start - 1
        end_idx = start_idx + field.length

        # Extract the raw field value, handling short lines
        if start_idx >= len(line):
            raw_value = ""
        else:
            raw_value = line[start_idx:end_idx]

        # Trim whitespace if configured
        if field.trim:
            raw_value = raw_value.strip()

        return raw_value
