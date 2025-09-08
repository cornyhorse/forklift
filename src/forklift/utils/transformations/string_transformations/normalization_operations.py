"""String normalization and character fixing operations."""

from __future__ import annotations
import unicodedata


class NormalizationOperations:
    """Handles string normalization, encoding fixes, and character standardization."""

    @staticmethod
    def fix_encoding_errors(text: str) -> str:
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

    @staticmethod
    def normalize_quotes(text: str) -> str:
        """Normalize smart quotes to ASCII quotes."""
        quote_mappings = {
            '\u2018': "'", '\u2019': "'", '\u201A': "'", '\u201B': "'",
            '\u201C': '"', '\u201D': '"', '\u201E': '"', '\u201F': '"',
            '\u2039': "'", '\u203A': "'", '\u00AB': '"', '\u00BB': '"',
        }

        for smart_quote, ascii_quote in quote_mappings.items():
            text = text.replace(smart_quote, ascii_quote)

        return text

    @staticmethod
    def normalize_dashes(text: str) -> str:
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

    @staticmethod
    def normalize_spaces(text: str) -> str:
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

    @staticmethod
    def remove_zero_width_chars(text: str, replace_with_space: bool = False) -> str:
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

    @staticmethod
    def remove_control_chars(text: str, preserve_newlines: bool = True, preserve_tabs: bool = False) -> str:
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

    @staticmethod
    def remove_accents(text: str) -> str:
        """Remove diacritical marks."""
        return ''.join(
            char for char in unicodedata.normalize('NFD', text)
            if unicodedata.category(char) != 'Mn'
        )

    @staticmethod
    def to_ascii_only(text: str) -> str:
        """Convert to ASCII-only characters."""
        # First remove accents to ensure proper ASCII conversion
        text_no_accents = NormalizationOperations.remove_accents(text)
        try:
            return text_no_accents.encode('ascii', 'ignore').decode('ascii')
        except (UnicodeError, UnicodeEncodeError):
            # Fallback: manually filter to ASCII characters
            return ''.join(char for char in text_no_accents if ord(char) < 128)
