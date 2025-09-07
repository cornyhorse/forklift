"""Base data transformation utilities and main DataTransformer class.

This module provides the core transformation infrastructure and the main DataTransformer
class that combines all transformation capabilities.
"""

from __future__ import annotations

import pyarrow as pa
import pandas as pd
from typing import Optional
from decimal import Decimal

from .configs import (
    RegexReplaceConfig, StringReplaceConfig, MoneyTypeConfig, NumericCleaningConfig,
    StringPaddingConfig, HTMLXMLConfig, DateTimeTransformConfig, StringCleaningConfig,
    SSNConfig, ZipCodeConfig, PhoneNumberConfig, EmailConfig, IPAddressConfig, MACAddressConfig
)
from .string_transformations import StringTransformer
from .numeric_transformations import NumericTransformer
from .datetime_transformations import DateTimeTransformer
from .format.transformer import FormatTransformer
from .html_xml_transformations import HTMLXMLTransformer


class DataTransformer:
    """Comprehensive data transformation engine for schema-driven cleaning.

    This class combines all transformation capabilities from specialized transformer classes
    and provides a unified interface for data transformations.
    """

    def __init__(self):
        """Initialize the data transformer with specialized transformers."""
        self.string_transformer = StringTransformer()
        self.numeric_transformer = NumericTransformer()
        self.datetime_transformer = DateTimeTransformer()
        self.format_transformer = FormatTransformer()
        self.html_xml_transformer = HTMLXMLTransformer()

    # String transformation methods
    def apply_regex_replace(self, column: pa.Array, config: RegexReplaceConfig) -> pa.Array:
        """Apply regex replace transformation to a string column."""
        return self.string_transformer.apply_regex_replace(column, config)

    def apply_string_replace(self, column: pa.Array, config: StringReplaceConfig) -> pa.Array:
        """Apply simple string replace transformation."""
        return self.string_transformer.apply_string_replace(column, config)

    def apply_string_cleaning(self, column: pa.Array, config: StringCleaningConfig) -> pa.Array:
        """Apply comprehensive string cleaning operations."""
        return self.string_transformer.apply_string_cleaning(column, config)

    def apply_string_padding(self, column: pa.Array, config: StringPaddingConfig) -> pa.Array:
        """Apply string padding operations (lstrip, rstrip, lpad, rpad)."""
        return self.string_transformer.apply_string_padding(column, config)

    def apply_string_trimming(self, column: pa.Array, side: str = "both", chars: Optional[str] = None) -> pa.Array:
        """Apply string trimming operations (lstrip, rstrip, strip)."""
        return self.string_transformer.apply_string_trimming(column, side, chars)

    # Numeric transformation methods
    def apply_money_conversion(self, column: pa.Array, config: MoneyTypeConfig) -> pa.Array:
        """Convert money strings to decimal values."""
        return self.numeric_transformer.apply_money_conversion(column, config)

    def apply_numeric_cleaning(self, column: pa.Array, config: NumericCleaningConfig, target_type: str = "double") -> pa.Array:
        """Clean numeric fields with configurable separators and NaN handling."""
        return self.numeric_transformer.apply_numeric_cleaning(column, config, target_type)

    # DateTime transformation methods
    def apply_datetime_transformation(self, column: pa.Array, config: DateTimeTransformConfig) -> pa.Array:
        """Apply datetime parsing and transformation to a column."""
        return self.datetime_transformer.apply_datetime_transformation(column, config)

    # Format transformation methods
    def apply_ssn_formatting(self, column: pa.Array, config: SSNConfig) -> pa.Array:
        """Format Social Security Numbers to XXX-XX-XXXX format."""
        return self.format_transformer.apply_ssn_formatting(column, config)

    def apply_zip_code_formatting(self, column: pa.Array, config: ZipCodeConfig) -> pa.Array:
        """Format ZIP codes according to the specified type."""
        return self.format_transformer.apply_zip_code_formatting(column, config)

    def apply_phone_number_formatting(self, column: pa.Array, config: PhoneNumberConfig) -> pa.Array:
        """Format phone numbers according to the specified style."""
        return self.format_transformer.apply_phone_number_formatting(column, config)

    def apply_email_formatting(self, column: pa.Array, config: EmailConfig) -> pa.Array:
        """Format email addresses according to the specified rules."""
        return self.format_transformer.apply_email_formatting(column, config)

    def apply_ip_address_formatting(self, column: pa.Array, config: IPAddressConfig) -> pa.Array:
        """Format IP addresses according to the specified rules."""
        return self.format_transformer.apply_ip_address_formatting(column, config)

    def apply_mac_address_formatting(self, column: pa.Array, config: MACAddressConfig) -> pa.Array:
        """Format MAC addresses according to the specified rules."""
        return self.format_transformer.apply_mac_address_formatting(column, config)

    # HTML/XML transformation methods
    def apply_html_xml_cleaning(self, column: pa.Array, config: HTMLXMLConfig) -> pa.Array:
        """Remove HTML/XML tags and decode entities."""
        return self.html_xml_transformer.apply_html_xml_cleaning(column, config)

    # Private methods for backward compatibility with existing tests
    # These delegate to the appropriate specialized transformers

    # String transformation private methods
    def _fix_encoding_errors(self, text: str) -> str:
        """Fix common encoding errors."""
        return self.string_transformer._fix_encoding_errors(text)

    def _normalize_quotes(self, text: str) -> str:
        """Normalize smart quotes to ASCII quotes."""
        return self.string_transformer._normalize_quotes(text)

    def _normalize_dashes(self, text: str) -> str:
        """Normalize em/en dashes to hyphens."""
        return self.string_transformer._normalize_dashes(text)

    def _normalize_spaces(self, text: str) -> str:
        """Convert non-breaking spaces to regular spaces."""
        return self.string_transformer._normalize_spaces(text)

    def _remove_zero_width_chars(self, text: str, replace_with_space: bool = False) -> str:
        """Remove zero-width characters."""
        return self.string_transformer._remove_zero_width_chars(text, replace_with_space)

    def _remove_control_chars(self, text: str, preserve_newlines: bool = True, preserve_tabs: bool = False) -> str:
        """Remove control characters."""
        return self.string_transformer._remove_control_chars(text, preserve_newlines, preserve_tabs)

    def _remove_accents(self, text: str) -> str:
        """Remove diacritical marks."""
        return self.string_transformer._remove_accents(text)

    def _to_ascii_only(self, text: str) -> str:
        """Convert to ASCII-only characters."""
        return self.string_transformer._to_ascii_only(text)

    def _fix_case_issues(self, text: str, title_case_exceptions: list, acronyms: list) -> str:
        """Fix common case issues."""
        return self.string_transformer._fix_case_issues(text, title_case_exceptions, acronyms)

    # Numeric transformation private methods
    def _clean_money_string(self, value: str, config: MoneyTypeConfig) -> Optional:
        """Clean a money string and convert to decimal."""
        return self.numeric_transformer._clean_money_string(value, config)

    def _clean_numeric_string(self, value: str, config: NumericCleaningConfig) -> Optional[str]:
        """Clean a numeric string for conversion."""
        return self.numeric_transformer._clean_numeric_string(value, config)

    # Format transformation private methods
    def _format_ssn(self, value: str, config: SSNConfig) -> str:
        """Format a single SSN value."""
        return self.format_transformer._format_ssn(value, config)

    def _format_zip_code(self, value: str, config: ZipCodeConfig) -> str:
        """Format a single ZIP code value."""
        return self.format_transformer._format_zip_code(value, config)

    def _format_phone_number(self, value: str, config: PhoneNumberConfig) -> str:
        """Format a single phone number value."""
        return self.format_transformer._format_phone_number(value, config)

    def _format_email(self, value: str, config: EmailConfig) -> str:
        """Format a single email value."""
        return self.format_transformer._format_email(value, config)

    def _format_ip_address(self, value: str, config: IPAddressConfig) -> str:
        """Format a single IP address value."""
        return self.format_transformer._format_ip_address(value, config)

    def _normalize_ipv6_address(self, ipv6_address: str, compress: bool = True) -> str:
        """Normalize an IPv6 address."""
        return self.format_transformer._normalize_ipv6_address(ipv6_address, compress)

    def _is_valid_ipv4(self, ip_address: str) -> bool:
        """Check if an IP address is a valid IPv4 address."""
        return self.format_transformer._is_valid_ipv4(ip_address)

    def _is_valid_ipv6(self, ip_address: str) -> bool:
        """Check if an IP address is a valid IPv6 address."""
        return self.format_transformer._is_valid_ipv6(ip_address)

    def _format_mac_address(self, value: str, config: MACAddressConfig) -> str:
        """Format a single MAC address value."""
        return self.format_transformer._format_mac_address(value, config)
