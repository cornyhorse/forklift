"""String transformation utilities.

This module provides string cleaning, formatting, and case transformation capabilities.

All transformers work directly on Arrow data (no pandas): nulls are ``None``, results keep the
Arrow string type of the input column (``string`` stays ``string``, ``large_string`` stays
``large_string``) and both flavours are accepted as input.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc

from ._arrow_utils import is_string_like, string_array
from .configs import (
    RegexReplaceConfig,
    StringCleaningConfig,
    StringPaddingConfig,
    StringReplaceConfig,
)

# Characters (as seen after a wrong cp1252/latin-1 decode) that can follow the lead byte of a UTF-8
# multi-byte sequence: C1 controls, U+00A0..U+00BF and the cp1252 specials mapped from 0x80..0x9F.
_CP1252_SPECIALS = (
    "\u20ac\u201a\u0192\u201e\u2026\u2020\u2021\u02c6\u2030\u0160\u2039\u0152\u017d"
    "\u2018\u2019\u201c\u201d\u2022\u2013\u2014\u02dc\u2122\u0161\u203a\u0153\u017e\u0178"
)
# A UTF-8 lead byte (0xC2..0xF4) followed by a continuation byte, both read as cp1252/latin-1.
_MOJIBAKE_MARKER = re.compile("[\u00c2-\u00f4][\u0080-\u00bf" + _CP1252_SPECIALS + "]")
# A genuine repair of Western text never yields Syriac/Thaana/NKo letters or C1 controls; combining
# marks (category Mn/Me) are rejected too. If the round trip produces them, the "mojibake" was
# probably legitimate text such as ``Weiß“`` or ``Ö”``.
_IMPLAUSIBLE_REPAIR = re.compile("[\u0080-\u009f\u0700-\u07ff]")
_IMPLAUSIBLE_CATEGORIES = frozenset({"Mn", "Me", "Cn", "Co", "Cs"})

# Letters joined by apostrophes ("don't", "O'Brien") form one word for title casing.
_TITLE_WORD = re.compile("[^\\W\\d_]+(?:['\u2019][^\\W\\d_]+)*")
_ORDINAL_SUFFIXES = {"st", "nd", "rd", "th"}


def _as_array(result) -> pa.Array:
    """Return a plain ``pa.Array`` (kernels hand back a ChunkedArray for chunked input)."""
    if isinstance(result, pa.ChunkedArray):
        return result.combine_chunks()
    return result


class StringTransformer:
    """Specialized transformer for string operations."""

    def apply_regex_replace(self, column: pa.Array, config: RegexReplaceConfig) -> pa.Array:
        """Apply regex replace transformation to a string column.

        Uses the stdlib ``re`` engine (Python regex and replacement syntax). ``re`` has no
        timeout, so patterns must come from trusted schemas.
        """
        if not is_string_like(column.type):
            return column

        try:
            pattern = re.compile(config.pattern, config.flags)
            values = [
                None if value is None else pattern.sub(config.replacement, value)
                for value in column.to_pylist()
            ]
        except re.error as exc:
            raise ValueError(f"Invalid regex_replace configuration: {exc}") from exc
        return string_array(values, column.type)

    def apply_string_replace(self, column: pa.Array, config: StringReplaceConfig) -> pa.Array:
        """Apply simple (literal, non-regex) string replace transformation."""
        if not is_string_like(column.type):
            return column

        if config.old == "":
            # Python semantics for an empty needle (insert between characters). The Arrow kernel
            # does not terminate for an empty pattern, so this edge case stays in Python.
            count = config.count
            values = [
                None if value is None else value.replace(config.old, config.new, count)
                for value in column.to_pylist()
            ]
            return string_array(values, column.type)

        max_replacements = None if config.count < 0 else config.count
        return _as_array(
            pc.replace_substring(
                column,
                pattern=config.old,
                replacement=config.new,
                max_replacements=max_replacements,
            )
        )

    def apply_string_padding(self, column: pa.Array, config: StringPaddingConfig) -> pa.Array:
        """Apply string padding operations (lstrip, rstrip, lpad, rpad)."""
        if not is_string_like(column.type):
            return column

        width = max(config.width, 0)
        if config.side == "both":
            # Arrow's centre kernel puts the odd pad character on the other side than str.center
            values = [
                None if value is None else value.center(width, config.fillchar)
                for value in column.to_pylist()
            ]
            return string_array(values, column.type)
        if config.side == "right":
            return _as_array(pc.utf8_rpad(column, width=width, padding=config.fillchar))
        # "left" and (historically) any unknown side pad on the left
        return _as_array(pc.utf8_lpad(column, width=width, padding=config.fillchar))

    def apply_string_trimming(
        self, column: pa.Array, side: str = "both", chars: Optional[str] = None
    ) -> pa.Array:
        """Apply string trimming operations (lstrip, rstrip, strip)."""
        if not is_string_like(column.type):
            return column

        if chars == "":
            return column  # str.strip("") strips nothing

        if chars is None:
            if side == "left":
                result = pc.utf8_ltrim_whitespace(column)
            elif side == "right":
                result = pc.utf8_rtrim_whitespace(column)
            else:
                result = pc.utf8_trim_whitespace(column)
        else:
            if side == "left":
                result = pc.utf8_ltrim(column, characters=chars)
            elif side == "right":
                result = pc.utf8_rtrim(column, characters=chars)
            else:
                result = pc.utf8_trim(column, characters=chars)
        return _as_array(result)

    def apply_string_cleaning(self, column: pa.Array, config: StringCleaningConfig) -> pa.Array:
        """Apply comprehensive string cleaning operations."""
        if not is_string_like(column.type):
            return column

        transformed_values = []

        for value in column.to_pylist():
            if value is None:
                transformed_values.append(None)
                continue

            str_value = value

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
                str_value = self._remove_zero_width_chars(
                    str_value, replace_with_space=replace_with_space
                )

            # Tab handling
            if config.remove_tabs:
                str_value = str_value.replace("\t", "")
            elif "\t" in str_value:
                explicit_tab_replacement = (
                    config.tab_replacement != " " or config.collapse_whitespace
                )

                if explicit_tab_replacement:
                    str_value = str_value.replace("\t", config.tab_replacement)
                elif config.remove_control_chars and not config.preserve_tabs:
                    pass
                else:
                    str_value = str_value.replace("\t", config.tab_replacement)

            if config.remove_control_chars:
                preserve_tabs_for_removal = config.preserve_tabs
                str_value = self._remove_control_chars(
                    str_value, config.preserve_newlines, preserve_tabs_for_removal
                )

            # Whitespace handling
            if config.collapse_whitespace:
                if config.tab_replacement != " " and len(config.tab_replacement) > 1:
                    placeholder = "\ue000"
                    str_value = str_value.replace(config.tab_replacement, placeholder)
                    str_value = re.sub(r"\s+", " ", str_value)
                    str_value = str_value.replace(placeholder, config.tab_replacement)
                else:
                    str_value = re.sub(r"\s+", " ", str_value)

            if config.strip_whitespace:
                str_value = str_value.strip()

            # Accent and ASCII handling
            if config.remove_accents or config.ascii_only:
                str_value = self._remove_accents(str_value)

            if config.ascii_only:
                str_value = self._to_ascii_only(str_value)

            # Case handling
            if config.fix_case_issues:
                str_value = self._fix_case_issues(
                    str_value, config.title_case_exceptions, config.acronyms
                )

            if config.case_transform == "upper":
                str_value = str_value.upper()
            elif config.case_transform == "lower":
                str_value = str_value.lower()
            elif config.case_transform in {"title", "proper"}:
                if config.case_transform == "title":
                    parts = re.split(r"(\s+|-)", str_value)
                    transformed_parts = [
                        self._title_case(part) if part.strip() else part for part in parts
                    ]
                    str_value = "".join(transformed_parts)
                else:  # proper
                    str_value = (
                        str_value[0].upper() + str_value[1:].lower() if str_value else str_value
                    )

            # Custom case mapping
            if config.custom_case_mapping:
                for key, mapped_value in config.custom_case_mapping.items():
                    if config.case_mapping_mode == "exact" and str_value == key:
                        str_value = mapped_value
                        break
                    elif config.case_mapping_mode == "startswith" and str_value.startswith(key):
                        str_value = mapped_value + str_value[len(key) :]
                        break
                    elif config.case_mapping_mode == "endswith" and str_value.endswith(key):
                        str_value = str_value[: -len(key)] + mapped_value
                        break
                    elif config.case_mapping_mode == "contains" and key in str_value:
                        str_value = str_value.replace(key, mapped_value)

            # Acronym handling
            if config.acronyms:
                for acronym in config.acronyms:
                    pattern = r"\b" + re.escape(acronym.lower()) + r"\b"
                    str_value = re.sub(pattern, acronym.upper(), str_value, flags=re.IGNORECASE)

            transformed_values.append(str_value)

        return string_array(transformed_values, column.type)

    def _fix_encoding_errors(self, text: str) -> str:
        """Repair mojibake: UTF-8 text that was decoded as cp1252/latin-1.

        Only text that contains a typical mojibake pair (a UTF-8 lead byte followed by a
        continuation byte, e.g. ``Ã©`` or ``â€™``) is touched. The text is re-encoded with the
        wrong codec and decoded as UTF-8; if that round trip does not succeed (or produces
        implausible characters) the text is returned unchanged. Legitimate letters such as the
        ``Â`` in ``Âge`` or the ``Ã`` in ``IRMÃ DO`` are never deleted or rewritten. Repairs
        encoding only; quote/dash normalisation is a separate option.
        """
        if not text or not _MOJIBAKE_MARKER.search(text):
            return text

        raw = bytearray()
        for char in text:
            code = ord(char)
            if code < 0x80:
                raw.append(code)
                continue
            try:
                raw += char.encode("cp1252")
            except UnicodeEncodeError:
                if 0x80 <= code <= 0xFF:
                    raw.append(
                        code
                    )  # latin-1 style byte (e.g. C1 controls cp1252 leaves undefined)
                else:
                    return text
        try:
            fixed = bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            return text

        if _IMPLAUSIBLE_REPAIR.search(fixed) or any(
            unicodedata.category(char) in _IMPLAUSIBLE_CATEGORIES
            for char in fixed
            if ord(char) > 127
        ):
            return text
        return fixed

    def _title_case(self, text: str) -> str:
        """Title-case ``text`` word by word.

        Unlike ``str.title`` this does not capitalise after apostrophes in contractions
        (``don't`` -> ``Don't``, not ``Don'T``) or after digits in ordinals (``1st``, not
        ``1St``), while keeping name prefixes such as ``O'Brien`` and ``L'Oréal``.
        """

        def convert(match: "re.Match[str]") -> str:
            word = match.group(0)
            start = match.start()
            if start > 0 and text[start - 1].isdigit() and word.lower() in _ORDINAL_SUFFIXES:
                return word.lower()

            pieces = re.split("(['\u2019])", word)
            result = []
            previous = ""
            for index, piece in enumerate(pieces):
                if piece in ("'", "\u2019"):
                    result.append(piece)
                    continue
                # Capitalise the first segment and a segment after a one-letter prefix (O', D', L')
                if index == 0 or (len(previous) == 1 and len(piece) > 1):
                    result.append(piece[:1].upper() + piece[1:].lower())
                else:
                    result.append(piece.lower())
                previous = piece
            return "".join(result)

        return _TITLE_WORD.sub(convert, text)

    def _normalize_quotes(self, text: str) -> str:
        """Normalize smart quotes to ASCII quotes."""
        quote_mappings = {
            "\u2018": "'",
            "\u2019": "'",
            "\u201a": "'",
            "\u201b": "'",
            "\u201c": '"',
            "\u201d": '"',
            "\u201e": '"',
            "\u201f": '"',
            "\u2039": "'",
            "\u203a": "'",
            "\u00ab": '"',
            "\u00bb": '"',
        }

        for smart_quote, ascii_quote in quote_mappings.items():
            text = text.replace(smart_quote, ascii_quote)

        return text

    def _normalize_dashes(self, text: str) -> str:
        """Normalize em/en dashes to hyphens."""
        dash_mappings = {
            "\u2013": "-",  # En dash
            "\u2014": "-",  # Em dash
            "\u2015": "-",  # Horizontal bar
            "\u2212": "-",  # Minus sign
        }

        for dash, hyphen in dash_mappings.items():
            text = text.replace(dash, hyphen)

        return text

    def _normalize_spaces(self, text: str) -> str:
        """Convert non-breaking spaces to regular spaces."""
        space_mappings = {
            "\u00a0": " ",  # Non-breaking space
            "\u2000": " ",  # En quad
            "\u2001": " ",  # Em quad
            "\u2002": " ",  # En space
            "\u2003": " ",  # Em space
            "\u2004": " ",  # Three-per-em space
            "\u2005": " ",  # Four-per-em space
            "\u2006": " ",  # Six-per-em space
            "\u2007": " ",  # Figure space
            "\u2008": " ",  # Punctuation space
            "\u2009": " ",  # Thin space
            "\u200a": " ",  # Hair space
            "\u202f": " ",  # Narrow no-break space
            "\u205f": " ",  # Medium mathematical space
            "\u3000": " ",  # Ideographic space
        }

        for special_space, regular_space in space_mappings.items():
            text = text.replace(special_space, regular_space)

        return text

    def _remove_zero_width_chars(self, text: str, replace_with_space: bool = False) -> str:
        """Remove zero-width characters."""
        zero_width_chars = [
            "\u200b",  # Zero-width space
            "\u200c",  # Zero-width non-joiner
            "\u200d",  # Zero-width joiner
            "\ufeff",  # Zero-width no-break space (BOM)
            "\u2060",  # Word joiner
        ]

        replacement = " " if replace_with_space else ""
        for char in zero_width_chars:
            text = text.replace(char, replacement)

        return text

    def _remove_control_chars(
        self, text: str, preserve_newlines: bool = True, preserve_tabs: bool = False
    ) -> str:
        """Remove control characters."""
        result = []
        for char in text:
            code = ord(char)

            if code < 32:  # Control characters
                if preserve_newlines and char in "\n\r":
                    result.append(char)
                elif preserve_tabs and char == "\t":
                    result.append(char)
                # Skip other control characters
            elif code == 127:  # DEL character
                # Skip DEL character
                pass
            else:
                result.append(char)

        return "".join(result)

    def _remove_accents(self, text: str) -> str:
        """Remove diacritical marks."""
        return "".join(
            char
            for char in unicodedata.normalize("NFD", text)
            if unicodedata.category(char) != "Mn"
        )

    def _to_ascii_only(self, text: str) -> str:
        """Convert to ASCII-only characters."""
        # First remove accents to ensure proper ASCII conversion
        text_no_accents = self._remove_accents(text)
        try:
            return text_no_accents.encode("ascii", "ignore").decode("ascii")
        except (UnicodeError, UnicodeEncodeError):
            # Fallback: manually filter to ASCII characters
            return "".join(char for char in text_no_accents if ord(char) < 128)

    def _fix_case_issues(self, text: str, title_case_exceptions: list, acronyms: list) -> str:
        """Fix common case issues."""
        # Don't process short text (less than 3 characters) or text that's not all uppercase
        if len(text) <= 2 or not text.isupper():
            return text

        # Default common acronyms that should remain uppercase
        default_acronyms = {
            "NASA",
            "FBI",
            "CIA",
            "USA",
            "UK",
            "US",
            "CEO",
            "CTO",
            "CFO",
            "VP",
            "HR",
            "IT",
            "AI",
            "API",
            "URL",
            "HTTP",
            "HTTPS",
            "SQL",
            "HTML",
            "CSS",
            "JS",
            "XML",
            "JSON",
            "PDF",
            "CSV",
            "ZIP",
            "HTTP",
            "FTP",
            "TCP",
            "IP",
            "DNS",
            "SSL",
            "TLS",
            "AWS",
            "IBM",
            "AMD",
            "GPU",
            "CPU",
            "RAM",
            "SSD",
            "HDD",
            "USB",
            "DVD",
            "CD",
            "TV",
            "HD",
            "UHD",
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
            word_clean = "".join(c for c in word if c.isalpha())

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
                if "-" in word:
                    # Handle hyphenated compound names even for first word
                    parts = word.split("-")
                    fixed_parts = []
                    for j, part in enumerate(parts):
                        part_clean = "".join(c for c in part if c.isalpha())
                        if part_clean.upper() in all_acronyms:
                            fixed_parts.append(part.upper())
                        elif j == 0:
                            # Only the first part of a hyphenated compound gets title case
                            fixed_parts.append(self._title_case(part))
                        elif part_clean.lower() in title_case_exceptions:
                            fixed_parts.append(part.lower())
                        else:
                            # All other parts in compound names stay lowercase
                            fixed_parts.append(part.lower())
                    fixed_words.append("-".join(fixed_parts))
                else:
                    # Regular first word - convert to title case
                    fixed_words.append(self._title_case(word))
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
                if "-" in word:
                    # Handle hyphenated compound names
                    parts = word.split("-")
                    fixed_parts = []
                    for j, part in enumerate(parts):
                        part_clean = "".join(c for c in part if c.isalpha())
                        if part_clean.upper() in all_acronyms:
                            fixed_parts.append(part.upper())
                        elif j == 0:
                            # Only the first part of a hyphenated compound gets title case
                            fixed_parts.append(self._title_case(part))
                        elif part_clean.lower() in title_case_exceptions:
                            fixed_parts.append(part.lower())
                        else:
                            # All other parts in compound names stay lowercase
                            fixed_parts.append(part.lower())
                    fixed_words.append("-".join(fixed_parts))
                else:
                    # Regular word - convert to title case
                    fixed_words.append(self._title_case(word))

        return " ".join(fixed_words)
