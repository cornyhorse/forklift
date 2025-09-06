"""String transformation utilities.

This module provides string cleaning, formatting, and case transformation capabilities.
"""

from __future__ import annotations

import re
import unicodedata
import pyarrow as pa
import pandas as pd
from typing import Optional

from .configs import RegexReplaceConfig, StringReplaceConfig, StringCleaningConfig, StringPaddingConfig


class StringTransformer:
    """Specialized transformer for string operations."""

    def apply_regex_replace(self, column: pa.Array, config: RegexReplaceConfig) -> pa.Array:
        """Apply regex replace transformation to a string column."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        transformed_series = pandas_series.str.replace(
            config.pattern,
            config.replacement,
            regex=True,
            flags=config.flags
        )
        return pa.array(transformed_series)

    def apply_string_replace(self, column: pa.Array, config: StringReplaceConfig) -> pa.Array:
        """Apply simple string replace transformation."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        if config.count == -1:
            transformed_series = pandas_series.str.replace(config.old, config.new)
        else:
            transformed_series = pandas_series.str.replace(config.old, config.new, n=config.count)
        return pa.array(transformed_series)

    def apply_string_padding(self, column: pa.Array, config: StringPaddingConfig) -> pa.Array:
        """Apply string padding operations (lstrip, rstrip, lpad, rpad)."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()

        if config.side == "left":
            transformed_series = pandas_series.str.rjust(config.width, config.fillchar)
        elif config.side == "right":
            transformed_series = pandas_series.str.ljust(config.width, config.fillchar)
        elif config.side == "both":
            transformed_series = pandas_series.str.center(config.width, config.fillchar)
        else:
            transformed_series = pandas_series.str.rjust(config.width, config.fillchar)

        return pa.array(transformed_series)

    def apply_string_trimming(self, column: pa.Array, side: str = "both", chars: Optional[str] = None) -> pa.Array:
        """Apply string trimming operations (lstrip, rstrip, strip)."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()

        if side == "left":
            transformed_series = pandas_series.str.lstrip(chars)
        elif side == "right":
            transformed_series = pandas_series.str.rstrip(chars)
        elif side == "both":
            transformed_series = pandas_series.str.strip(chars)
        else:
            transformed_series = pandas_series.str.strip(chars)

        return pa.array(transformed_series)

    def apply_string_cleaning(self, column: pa.Array, config: StringCleaningConfig) -> pa.Array:
        """Apply comprehensive string cleaning operations."""
        if not pa.types.is_string(column.type):
            return column

        pandas_series = column.to_pandas()
        transformed_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                transformed_values.append(value)
                continue

            str_value = str(value)

            # Fix common encoding errors FIRST
            if config.fix_encoding_errors:
                str_value = self._fix_encoding_errors(str_value)

            # Unicode normalization
            if config.unicode_normalize:
                try:
                    str_value = unicodedata.normalize(config.unicode_normalize, str_value)
                except ValueError:
                    pass

            # Smart quotes and special characters
            if config.normalize_quotes:
                str_value = self._normalize_quotes(str_value)

            if config.normalize_dashes:
                str_value = self._normalize_dashes(str_value)

            if config.normalize_spaces:
                str_value = self._normalize_spaces(str_value)

            # Zero-width and control characters
            if config.remove_zero_width:
                replace_with_space = config.collapse_whitespace
                str_value = self._remove_zero_width_chars(str_value, replace_with_space=replace_with_space)

            # Tab handling
            if config.remove_tabs:
                str_value = str_value.replace('\t', '')
            elif '\t' in str_value:
                explicit_tab_replacement = (
                    config.tab_replacement != " " or config.collapse_whitespace
                )

                if explicit_tab_replacement:
                    str_value = str_value.replace('\t', config.tab_replacement)
                elif config.remove_control_chars and not config.preserve_tabs:
                    pass
                else:
                    str_value = str_value.replace('\t', config.tab_replacement)

            if config.remove_control_chars:
                preserve_tabs_for_removal = config.preserve_tabs
                str_value = self._remove_control_chars(str_value, config.preserve_newlines, preserve_tabs_for_removal)

            # Whitespace handling
            if config.collapse_whitespace:
                if config.tab_replacement != " " and len(config.tab_replacement) > 1:
                    placeholder = "\uE000"
                    str_value = str_value.replace(config.tab_replacement, placeholder)
                    str_value = re.sub(r'\s+', ' ', str_value)
                    str_value = str_value.replace(placeholder, config.tab_replacement)
                else:
                    str_value = re.sub(r'\s+', ' ', str_value)

            if config.strip_whitespace:
                str_value = str_value.strip()

            # Accent and ASCII handling
            if config.remove_accents or config.ascii_only:
                str_value = self._remove_accents(str_value)

            if config.ascii_only:
                str_value = self._to_ascii_only(str_value)

            # Case handling
            if config.fix_case_issues:
                str_value = self._fix_case_issues(str_value, config.title_case_exceptions, config.acronyms)

            if config.case_transform == 'upper':
                str_value = str_value.upper()
            elif config.case_transform == 'lower':
                str_value = str_value.lower()
            elif config.case_transform in {'title', 'proper'}:
                if config.case_transform == 'title':
                    parts = re.split(r'(\s+|-)', str_value)
                    transformed_parts = [part.title() if part.strip() else part for part in parts]
                    str_value = ''.join(transformed_parts)
                else:  # proper
                    str_value = str_value[0].upper() + str_value[1:].lower() if str_value else str_value

            # Custom case mapping
            if config.custom_case_mapping:
                for key, mapped_value in config.custom_case_mapping.items():
                    if config.case_mapping_mode == 'exact' and str_value == key:
                        str_value = mapped_value
                        break
                    elif config.case_mapping_mode == 'startswith' and str_value.startswith(key):
                        str_value = mapped_value + str_value[len(key):]
                        break
                    elif config.case_mapping_mode == 'endswith' and str_value.endswith(key):
                        str_value = str_value[:-len(key)] + mapped_value
                        break
                    elif config.case_mapping_mode == 'contains' and key in str_value:
                        str_value = str_value.replace(key, mapped_value)

            # Acronym handling
            if config.acronyms:
                for acronym in config.acronyms:
                    pattern = r'\b' + re.escape(acronym.lower()) + r'\b'
                    str_value = re.sub(pattern, acronym.upper(), str_value, flags=re.IGNORECASE)

            transformed_values.append(str_value)

        return pa.array(transformed_values)

    def _fix_encoding_errors(self, text: str) -> str:
        """Fix common encoding errors."""
        if 'Donâ€™t' in text:
            text = text.replace('Donâ€™t', "Don't")

        fixes = {
            'â€™': "'", 'â€œ': '"', 'â€': '"', 'â€"': '—', 'â€"': '-', 'â€¦': '…', 'âœ"': '✓',
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú', 'Ã±': 'ñ', 'Ã¼': 'ü',
            'Ã ': 'à', 'Ã¨': 'è', 'Ã¬': 'ì', 'Ã²': 'ò', 'Ã¹': 'ù', 'Â': '',
        }

        for wrong, right in fixes.items():
            if wrong in text:
                text = text.replace(wrong, right)

        return text

    def _normalize_quotes(self, text: str) -> str:
        """Normalize smart quotes to ASCII quotes."""
        quote_mappings = {
            '\u2018': "'", '\u2019': "'", '\u201A': "'", '\u201B': "'",
            '\u201C': '"', '\u201D': '"', '\u201E': '"', '\u201F': '"',
            '\u2039': "'", '\u203A': "'", '\u00AB': '"', '\u00BB': '"',
        }

        for smart_quote, ascii_quote in quote_mappings.items():
            text = text.replace(smart_quote, ascii_quote)

        return text

    def _normalize_dashes(self, text: str) -> str:
        """Normalize em/en dashes to hyphens."""
        dash_mappings = {
            '\u2013': '-',  # En dash
            '\u2014': '-',  # Em dash
            '\u2015': '-',  # Horizontal bar
            '\u2212': '-',  # Minus sign
        }

        for dash, hyphen in dash_mappings.items():
            text = text.replace(dash, hyphen)

        return text

    def _normalize_spaces(self, text: str) -> str:
        """Convert non-breaking spaces to regular spaces."""
        space_mappings = {
            '\u00A0': ' ',  # Non-breaking space
            '\u2000': ' ',  # En quad
            '\u2001': ' ',  # Em quad
            '\u2002': ' ',  # En space
            '\u2003': ' ',  # Em space
            '\u2004': ' ',  # Three-per-em space
            '\u2005': ' ',  # Four-per-em space
            '\u2006': ' ',  # Six-per-em space
            '\u2007': ' ',  # Figure space
            '\u2008': ' ',  # Punctuation space
            '\u2009': ' ',  # Thin space
            '\u200A': ' ',  # Hair space
            '\u202F': ' ',  # Narrow no-break space
            '\u205F': ' ',  # Medium mathematical space
            '\u3000': ' ',  # Ideographic space
        }

        for special_space, regular_space in space_mappings.items():
            text = text.replace(special_space, regular_space)

        return text

    def _remove_zero_width_chars(self, text: str, replace_with_space: bool = False) -> str:
        """Remove zero-width characters."""
        zero_width_chars = [
            '\u200B',  # Zero-width space
            '\u200C',  # Zero-width non-joiner
            '\u200D',  # Zero-width joiner
            '\uFEFF',  # Zero-width no-break space (BOM)
            '\u2060',  # Word joiner
        ]

        replacement = ' ' if replace_with_space else ''
        for char in zero_width_chars:
            text = text.replace(char, replacement)

        return text

    def _remove_control_chars(self, text: str, preserve_newlines: bool = True, preserve_tabs: bool = False) -> str:
        """Remove control characters."""
        result = []
        for char in text:
            code = ord(char)

            if code < 32:  # Control characters
                if preserve_newlines and char in '\n\r':
                    result.append(char)
                elif preserve_tabs and char == '\t':
                    result.append(char)
                # Skip other control characters
            elif code == 127:  # DEL character
                # Skip DEL character
                pass
            else:
                result.append(char)

        return ''.join(result)

    def _remove_accents(self, text: str) -> str:
        """Remove diacritical marks."""
        return ''.join(
            char for char in unicodedata.normalize('NFD', text)
            if unicodedata.category(char) != 'Mn'
        )

    def _to_ascii_only(self, text: str) -> str:
        """Convert to ASCII-only characters."""
        # First remove accents to ensure proper ASCII conversion
        text_no_accents = self._remove_accents(text)
        try:
            return text_no_accents.encode('ascii', 'ignore').decode('ascii')
        except (UnicodeError, UnicodeEncodeError):
            # Fallback: manually filter to ASCII characters
            return ''.join(char for char in text_no_accents if ord(char) < 128)

    def _fix_case_issues(self, text: str, title_case_exceptions: list, acronyms: list) -> str:
        """Fix common case issues."""
        # Don't process short text (less than 3 characters) or text that's not all uppercase
        if len(text) <= 2 or not text.isupper():
            return text

        # Default common acronyms that should remain uppercase
        default_acronyms = {
            'NASA', 'FBI', 'CIA', 'USA', 'UK', 'US', 'CEO', 'CTO', 'CFO', 'VP',
            'HR', 'IT', 'AI', 'API', 'URL', 'HTTP', 'HTTPS', 'SQL', 'HTML',
            'CSS', 'JS', 'XML', 'JSON', 'PDF', 'CSV', 'ZIP', 'HTTP', 'FTP',
            'TCP', 'IP', 'DNS', 'SSL', 'TLS', 'AWS', 'IBM', 'AMD', 'GPU',
            'CPU', 'RAM', 'SSD', 'HDD', 'USB', 'DVD', 'CD', 'TV', 'HD', 'UHD'
        }

        # Combine default acronyms with custom ones
        all_acronyms = default_acronyms.copy()
        if acronyms:
            all_acronyms.update(acronym.upper() for acronym in acronyms)

        # Fix multiple consecutive uppercase letters (except known acronyms)
        words = text.split()
        fixed_words = []

        for i, word in enumerate(words):
            # Remove punctuation for checking exceptions/acronyms
            word_clean = ''.join(c for c in word if c.isalpha())

            # Check if word is a known acronym
            if word_clean.upper() in all_acronyms:
                # Preserve acronym case but handle punctuation
                result = ""
                for char in word:
                    if char.isalpha():
                        result += char.upper()
                    else:
                        result += char
                fixed_words.append(result)
            elif i == 0:
                # First word is always capitalized, but handle hyphenated compound names
                if '-' in word:
                    # Handle hyphenated compound names even for first word
                    parts = word.split('-')
                    fixed_parts = []
                    for j, part in enumerate(parts):
                        part_clean = ''.join(c for c in part if c.isalpha())
                        if part_clean.upper() in all_acronyms:
                            fixed_parts.append(part.upper())
                        elif j == 0:
                            # Only the first part of a hyphenated compound gets title case
                            fixed_parts.append(part.title())
                        elif part_clean.lower() in title_case_exceptions:
                            fixed_parts.append(part.lower())
                        else:
                            # All other parts in compound names stay lowercase
                            fixed_parts.append(part.lower())
                    fixed_words.append('-'.join(fixed_parts))
                else:
                    # Regular first word - convert to title case
                    fixed_words.append(word.title())
            elif word_clean.lower() in title_case_exceptions:
                # Use lowercase for exception words (but not the first word)
                result = ""
                for char in word:
                    if char.isalpha():
                        result += char.lower()
                    else:
                        result += char
                fixed_words.append(result)
            else:
                # Convert to title case, but handle hyphenated compound names
                if '-' in word:
                    # Handle hyphenated compound names
                    parts = word.split('-')
                    fixed_parts = []
                    for j, part in enumerate(parts):
                        part_clean = ''.join(c for c in part if c.isalpha())
                        if part_clean.upper() in all_acronyms:
                            fixed_parts.append(part.upper())
                        elif j == 0:
                            # Only the first part of a hyphenated compound gets title case
                            fixed_parts.append(part.title())
                        elif part_clean.lower() in title_case_exceptions:
                            fixed_parts.append(part.lower())
                        else:
                            # All other parts in compound names stay lowercase
                            fixed_parts.append(part.lower())
                    fixed_words.append('-'.join(fixed_parts))
                else:
                    # Regular word - convert to title case
                    fixed_words.append(word.title())

        return ' '.join(fixed_words)
