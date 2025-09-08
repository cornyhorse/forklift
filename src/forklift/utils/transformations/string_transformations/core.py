"""Core StringTransformer class that coordinates all string operations."""

from __future__ import annotations
from typing import Optional

import pyarrow as pa
from ..configs import RegexReplaceConfig, StringReplaceConfig, StringCleaningConfig, StringPaddingConfig

from .regex_operations import RegexOperations
from .padding_operations import PaddingOperations
from .cleaning_operations import CleaningOperations
from .normalization_operations import NormalizationOperations
from .case_operations import CaseOperations


class StringTransformer:
    """Specialized transformer for string operations.

    This class coordinates various string transformation operations
    that have been split into focused modules for better maintainability.
    """

    def __init__(self):
        """Initialize the StringTransformer."""
        self.regex_ops = RegexOperations()
        self.padding_ops = PaddingOperations()
        self.cleaning_ops = CleaningOperations()

    def apply_regex_replace(self, column: pa.Array, config: RegexReplaceConfig) -> pa.Array:
        """Apply regex replace transformation to a string column."""
        return self.regex_ops.apply_regex_replace(column, config)

    def apply_string_replace(self, column: pa.Array, config: StringReplaceConfig) -> pa.Array:
        """Apply simple string replace transformation."""
        return self.regex_ops.apply_string_replace(column, config)

    def apply_string_padding(self, column: pa.Array, config: StringPaddingConfig) -> pa.Array:
        """Apply string padding operations (lstrip, rstrip, lpad, rpad)."""
        return self.padding_ops.apply_string_padding(column, config)

    def apply_string_trimming(self, column: pa.Array, side: str = "both", chars: Optional[str] = None) -> pa.Array:
        """Apply string trimming operations (lstrip, rstrip, strip)."""
        return self.padding_ops.apply_string_trimming(column, side, chars)

    def apply_string_cleaning(self, column: pa.Array, config: StringCleaningConfig) -> pa.Array:
        """Apply comprehensive string cleaning operations."""
        return self.cleaning_ops.apply_string_cleaning(column, config)

    # Backward compatibility methods for tests - delegate to static methods in separate modules
    def _fix_encoding_errors(self, text: str) -> str:
        """Fix common encoding errors. (Backward compatibility method)"""
        return NormalizationOperations.fix_encoding_errors(text)

    def _normalize_quotes(self, text: str) -> str:
        """Normalize smart quotes to ASCII quotes. (Backward compatibility method)"""
        return NormalizationOperations.normalize_quotes(text)

    def _normalize_dashes(self, text: str) -> str:
        """Normalize em/en dashes to hyphens. (Backward compatibility method)"""
        return NormalizationOperations.normalize_dashes(text)

    def _normalize_spaces(self, text: str) -> str:
        """Convert non-breaking spaces to regular spaces. (Backward compatibility method)"""
        return NormalizationOperations.normalize_spaces(text)

    def _remove_zero_width_chars(self, text: str, replace_with_space: bool = False) -> str:
        """Remove zero-width characters. (Backward compatibility method)"""
        return NormalizationOperations.remove_zero_width_chars(text, replace_with_space)

    def _remove_control_chars(self, text: str, preserve_newlines: bool = True, preserve_tabs: bool = False) -> str:
        """Remove control characters. (Backward compatibility method)"""
        return NormalizationOperations.remove_control_chars(text, preserve_newlines, preserve_tabs)

    def _remove_accents(self, text: str) -> str:
        """Remove diacritical marks. (Backward compatibility method)"""
        return NormalizationOperations.remove_accents(text)

    def _to_ascii_only(self, text: str) -> str:
        """Convert to ASCII-only characters. (Backward compatibility method)"""
        return NormalizationOperations.to_ascii_only(text)

    def _fix_case_issues(self, text: str, title_case_exceptions: list, acronyms: list) -> str:
        """Fix common case issues. (Backward compatibility method)"""
        return CaseOperations.fix_case_issues(text, title_case_exceptions, acronyms)
