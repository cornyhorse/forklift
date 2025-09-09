"""Configuration classes for data transformations.

This module contains all the dataclass configurations used by the transformation utilities.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DateTimeTransformConfig:
    """Configuration for datetime parsing and transformation."""
    mode: str = "common_formats"  # "enforce", "specify_formats", "common_formats"
    format: Optional[str] = None  # Single format to enforce (enforce mode)
    formats: Optional[List[str]] = None  # List of allowed formats (specify_formats mode)
    allow_fuzzy: bool = False  # Allow fuzzy parsing with dateutil
    from_epoch: bool = False  # Treat input as epoch timestamp
    to_epoch: Optional[str] = None  # Convert output to epoch ("seconds", "milliseconds", etc.)
    target_type: str = "datetime"  # "datetime", "date", "timestamp", "string"
    output_format: Optional[str] = None  # Format for string output (if target_type is "string")
    timezone: Optional[str] = None  # Target timezone for output

    def __post_init__(self):
        valid_modes = ["enforce", "specify_formats", "common_formats"]
        if self.mode not in valid_modes:
            raise ValueError(f"Invalid mode: {self.mode}. Must be one of {valid_modes}")

        if self.mode == "enforce" and not self.format:
            raise ValueError("Format must be specified when mode is 'enforce'")

        if self.mode == "specify_formats" and not self.formats:
            raise ValueError("Formats list must be specified when mode is 'specify_formats'")

        valid_targets = ["datetime", "date", "timestamp", "string"]
        if self.target_type not in valid_targets:
            raise ValueError(f"Invalid target_type: {self.target_type}. Must be one of {valid_targets}")

        if self.to_epoch:
            valid_epoch_units = ["seconds", "milliseconds", "microseconds", "nanoseconds"]
            if self.to_epoch not in valid_epoch_units:
                raise ValueError(f"Invalid to_epoch unit: {self.to_epoch}. Must be one of {valid_epoch_units}")


@dataclass
class RegexReplaceConfig:
    """Configuration for regex replace operations."""
    pattern: str
    replacement: str
    flags: int = 0  # re.IGNORECASE, re.MULTILINE, etc.


@dataclass
class StringReplaceConfig:
    """Configuration for simple string replace operations."""
    old: str
    new: str
    count: int = -1  # -1 means replace all occurrences


@dataclass
class MoneyTypeConfig:
    """Configuration for money type conversions."""
    currency_symbols: List[str] = None
    thousands_separator: str = ","
    decimal_separator: str = "."
    parentheses_negative: bool = True
    strip_whitespace: bool = True

    def __post_init__(self):
        if self.currency_symbols is None:
            self.currency_symbols = ["$", "€", "£", "¥", "₹", "₽", "¢"]


@dataclass
class NumericCleaningConfig:
    """Configuration for numeric field cleaning."""
    thousands_separator: str = ","
    decimal_separator: str = "."
    allow_nan: bool = True
    nan_values: List[str] = None
    strip_whitespace: bool = True

    def __post_init__(self):
        if self.nan_values is None:
            self.nan_values = ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!"]


@dataclass
class StringPaddingConfig:
    """Configuration for string padding operations."""
    width: int
    fillchar: str = " "
    side: str = "left"  # "left", "right", "both"


@dataclass
class HTMLXMLConfig:
    """Configuration for HTML/XML cleaning."""
    strip_tags: bool = True
    decode_entities: bool = True
    preserve_whitespace: bool = False


@dataclass
class StringCleaningConfig:
    """Configuration for comprehensive string cleaning operations."""
    # Smart quotes and special characters
    normalize_quotes: bool = True  # Convert smart quotes to ASCII quotes
    normalize_dashes: bool = True  # Convert em/en dashes to hyphens
    normalize_spaces: bool = True  # Convert non-breaking spaces to regular spaces

    # Whitespace handling
    collapse_whitespace: bool = True  # Collapse multiple spaces to single space
    strip_whitespace: bool = True  # Strip leading/trailing whitespace
    remove_tabs: bool = False  # Convert tabs to spaces (if False) or remove (if True)
    tab_replacement: str = " "  # What to replace tabs with if not removing

    # Zero-width and control characters
    remove_zero_width: bool = True  # Remove zero-width characters (ZWSP, ZWNJ, etc.)
    remove_control_chars: bool = True  # Remove control characters (except common ones)
    preserve_newlines: bool = True  # Keep \n and \r\n when removing control chars
    preserve_tabs: bool = False  # Keep \t when removing control chars

    # Unicode normalization
    unicode_normalize: Optional[str] = "NFKC"  # Unicode normalization form (NFC, NFD, NFKC, NFKD)

    # Case handling
    fix_case_issues: bool = False  # Fix common case issues (e.g., multiple caps)
    case_transform: Optional[str] = None  # Case transformation: 'upper', 'lower', 'title', 'proper', or None
    title_case_exceptions: List[str] = None  # Words to not title case (e.g., ["of", "the", "and"])
    custom_case_mapping: Optional[Dict[str, str]] = None  # Custom case mappings (e.g., state codes: {"california": "CA"})
    case_mapping_mode: str = "exact"  # How to apply custom mappings: 'exact', 'contains', 'startswith', 'endswith'
    acronyms: Optional[List[str]] = None  # Custom acronyms to preserve in uppercase (e.g., ["NASA", "API", "CEO"])

    # Other cleaning
    remove_accents: bool = False  # Remove diacritical marks
    ascii_only: bool = False  # Convert to ASCII-only (implies remove_accents=True)
    fix_encoding_errors: bool = True  # Fix common encoding errors

    def __post_init__(self):
        if self.title_case_exceptions is None:
            self.title_case_exceptions = ["a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "so", "the", "to", "up", "yet"]

        if self.custom_case_mapping is None:
            self.custom_case_mapping = {}

        if self.acronyms is None:
            self.acronyms = []

        # Validate case_transform parameter
        valid_transforms = {None, 'upper', 'lower', 'title', 'proper'}
        if self.case_transform not in valid_transforms:
            raise ValueError(f"case_transform must be one of {valid_transforms}, got: {self.case_transform}")

        # Validate case_mapping_mode
        valid_modes = {'exact', 'contains', 'startswith', 'endswith'}
        if self.case_mapping_mode not in valid_modes:
            raise ValueError(f"case_mapping_mode must be one of {valid_modes}, got: {self.case_mapping_mode}")


@dataclass
class SSNConfig:
    """Configuration for Social Security Number formatting."""
    format_with_dashes: bool = True  # Format as XXX-XX-XXXX
    zero_pad: bool = True  # Zero-pad numbers with fewer than 9 digits
    validate: bool = True  # Validate that result has exactly 9 digits
    allow_invalid: bool = False  # If False, invalid SSNs become None


@dataclass
class ZipCodeConfig:
    """Configuration for ZIP code formatting."""
    zip_type: str = "zip-permissive"  # "zip-permissive", "zip-5", "zip-9"
    format_with_dash: bool = True  # Format ZIP+4 as XXXXX-XXXX
    zero_pad: bool = True  # Zero-pad ZIP codes
    validate: bool = True  # Validate ZIP code format
    allow_invalid: bool = False  # If False, invalid ZIP codes become None

    def __post_init__(self):
        valid_types = {"zip-permissive", "zip-5", "zip-9"}
        if self.zip_type not in valid_types:
            raise ValueError(f"zip_type must be one of {valid_types}, got: {self.zip_type}")


@dataclass
class PhoneNumberConfig:
    """Configuration for phone number formatting."""
    format_style: str = "us-standard"  # "us-standard", "international", "digits-only", "preserve"
    validate: bool = True  # Validate phone number format
    allow_invalid: bool = False  # If False, invalid phone numbers become None
    min_digits: int = 10  # Minimum number of digits required
    max_digits: int = 11  # Maximum number of digits allowed
    include_country_code: bool = False  # Include country code in output
    use_parentheses: bool = True  # Use parentheses for area code in US format
    use_dashes: bool = True  # Use dashes between number groups
    use_dots: bool = False  # Use dots instead of dashes as separators

    def __post_init__(self):
        valid_styles = {"us-standard", "international", "digits-only", "preserve"}
        if self.format_style not in valid_styles:
            raise ValueError(f"format_style must be one of {valid_styles}, got: {self.format_style}")


@dataclass
class EmailConfig:
    """Configuration for email formatting."""
    normalize_case: bool = True  # Convert to lowercase
    strip_whitespace: bool = True  # Remove leading/trailing whitespace
    normalize_domain: bool = True  # Normalize domain (remove trailing dots)
    validate_format: bool = True  # Validate email format with regex
    allow_invalid: bool = False  # If False, invalid emails become None


@dataclass
class IPAddressConfig:
    """Configuration for IP address formatting."""
    ip_version: str = "both"  # "ipv4", "ipv6", "both"
    normalize_ipv6: bool = True  # Normalize IPv6 addresses (expand/compress)
    compress_ipv6: bool = True  # Compress IPv6 addresses (remove leading zeros)
    validate: bool = True  # Validate IP address format
    allow_invalid: bool = False  # If False, invalid IP addresses become None

    def __post_init__(self):
        valid_versions = {"ipv4", "ipv6", "both"}
        if self.ip_version not in valid_versions:
            raise ValueError(f"ip_version must be one of {valid_versions}, got: {self.ip_version}")


@dataclass
class MACAddressConfig:
    """Configuration for MAC address formatting."""
    format_style: str = "colon"  # "colon", "dash", "dot", "none"
    case_style: str = "lower"  # "lower", "upper", "preserve"
    zero_pad: bool = True  # Zero-pad MAC addresses
    validate: bool = True  # Validate MAC address format
    allow_invalid: bool = False  # If False, invalid MAC addresses become None

    def __post_init__(self):
        valid_formats = {"colon", "dash", "dot", "none"}
        if self.format_style not in valid_formats:
            raise ValueError(f"format_style must be one of {valid_formats}, got: {self.format_style}")

        valid_cases = {"lower", "upper", "preserve"}
        if self.case_style not in valid_cases:
            raise ValueError(f"case_style must be one of {valid_cases}, got: {self.case_style}")
