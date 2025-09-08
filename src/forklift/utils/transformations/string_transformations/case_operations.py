"""Case transformation and acronym handling operations."""

from __future__ import annotations
import re


class CaseOperations:
    """Handles case transformations and acronym processing."""

    @staticmethod
    def fix_case_issues(text: str, title_case_exceptions: list, acronyms: list) -> str:
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
