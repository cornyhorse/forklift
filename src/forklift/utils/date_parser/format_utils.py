"""Format normalization and validation utilities."""

import datetime
import re
from typing import List, Optional


def normalize_format(fmt: str) -> str:
    """Normalize schema tokens to strptime format.

    Args:
        fmt: Format string (either strptime or schema tokens)

    Returns:
        Normalized strptime format string
    """
    # If already contains %, assume it's strptime format
    if '%' in fmt:
        return fmt

    # Start with the input format
    result = fmt

    # Replace tokens in order of specificity (longest first to avoid conflicts)
    # Year tokens (longest first)
    result = result.replace('YYYY', '%Y')
    result = result.replace('yyyy', '%Y')
    result = result.replace('Yyyy', '%Y')
    result = result.replace('YY', '%y')
    result = result.replace('yy', '%y')
    result = result.replace('Yy', '%y')

    # Month name tokens (longest first to avoid conflicts with MM)
    result = result.replace('MMMM', '%B')
    result = result.replace('mmmm', '%B')
    result = result.replace('Mmmm', '%B')
    result = result.replace('MMM', '%b')
    result = result.replace('mmm', '%b')
    result = result.replace('Mmm', '%b')

    # Microsecond tokens (before other tokens that might conflict)
    result = result.replace('ffffff', '%f')
    result = result.replace('fff', '%f')

    # Handle MM/mm tokens with context awareness
    # Split the format into date and time parts to handle MM differently
    # Common time separators that indicate the time part
    time_separators = [' ', 'T']
    time_part_start = -1

    for sep in time_separators:
        if sep in result:
            # Find where time part likely starts (after date part)
            parts = result.split(sep)
            if len(parts) >= 2:
                # Check if the part after separator contains time-like tokens
                time_part = sep.join(parts[1:])
                if any(token in time_part for token in ['HH', 'hh', 'H:', 'h:', 'SS', 'ss', 'S:', 's:']):
                    time_part_start = result.find(sep + time_part)
                    break

    if time_part_start > 0:
        # Split into date and time parts
        date_part = result[:time_part_start]
        time_part = result[time_part_start:]

        # Process date part: MM should be months
        date_part = date_part.replace('MM', '%m')
        date_part = date_part.replace('mm', '%m')
        date_part = date_part.replace('Mm', '%m')

        # Process time part: MM should be minutes
        time_part = time_part.replace('MM', '%M')
        time_part = time_part.replace('mm', '%M')
        time_part = time_part.replace('Mm', '%M')

        result = date_part + time_part
    else:
        # No clear time part, treat as date-only format
        result = result.replace('MM', '%m')
        result = result.replace('mm', '%m')
        result = result.replace('Mm', '%m')

    # Handle single M/m tokens with regex to avoid conflicts with existing % codes
    # Only replace standalone M/m that are not preceded by % and not followed by letters
    result = re.sub(r'(?<!%)M(?![a-zA-Z%])', '%m', result)
    result = re.sub(r'(?<!%)m(?![a-zA-Z%])', '%m', result)

    # Day tokens
    result = result.replace('DD', '%d')
    result = result.replace('dd', '%d')
    result = result.replace('Dd', '%d')
    # Single D/d
    result = re.sub(r'(?<!%)D(?![a-zA-Z%])', '%d', result)
    result = re.sub(r'(?<!%)d(?![a-zA-Z%])', '%d', result)

    # Hour tokens
    result = result.replace('HH', '%H')
    result = result.replace('hh', '%H')
    result = result.replace('Hh', '%H')
    # Single H/h
    result = re.sub(r'(?<!%)H(?![a-zA-Z%])', '%H', result)
    result = re.sub(r'(?<!%)h(?![a-zA-Z%])', '%H', result)

    # Second tokens
    result = result.replace('SS', '%S')
    result = result.replace('ss', '%S')
    result = result.replace('Ss', '%S')
    # Single S/s
    result = re.sub(r'(?<!%)S(?![a-zA-Z%])', '%S', result)
    result = re.sub(r'(?<!%)s(?![a-zA-Z%])', '%S', result)

    return result


def matches_format_exact(value: str, fmt: str) -> bool:
    """Check if a value matches a format exactly.

    Args:
        value: String to check
        fmt: Format string (strptime format)

    Returns:
        True if value matches format exactly
    """
    try:
        # Parse the value with the format
        parsed = datetime.datetime.strptime(value, fmt)

        # Format it back and compare
        try:
            reformatted = parsed.strftime(fmt)

            # Special handling for microseconds - allow flexible matching
            if '%f' in fmt:
                # For microseconds, we need to handle the case where the input
                # has fewer than 6 digits but strftime always outputs 6 digits

                # Find the microseconds part in both strings
                # Pattern to match microseconds (1-6 digits after a dot)
                microseconds_pattern = r'\.(\d{1,6})'

                original_match = re.search(microseconds_pattern, value)
                reformatted_match = re.search(microseconds_pattern, reformatted)

                if original_match and reformatted_match:
                    original_microseconds = original_match.group(1)
                    reformatted_microseconds = reformatted_match.group(1)

                    # Pad the original to 6 digits for comparison
                    original_padded = original_microseconds.ljust(6, '0')

                    if original_padded == reformatted_microseconds:
                        # Replace the microseconds part in both strings for comparison
                        value_normalized = re.sub(microseconds_pattern, f'.{original_padded}', value)
                        return value_normalized == reformatted

            return reformatted == value
        except (ValueError, TypeError):
            # strftime can fail for some formats/values
            return False

    except (ValueError, TypeError):
        return False


def try_strptime(value: str, formats: List[str]) -> Optional[datetime.datetime]:
    """Try to parse a string using multiple strptime formats.

    Args:
        value: String to parse
        formats: List of strptime format strings

    Returns:
        Parsed datetime or None if no format matches
    """
    for fmt in formats:
        try:
            return datetime.datetime.strptime(value, fmt)
        except (ValueError, TypeError):
            continue
    return None
