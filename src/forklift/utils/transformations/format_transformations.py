"""Format transformation utilities for structured data types.

This module provides formatting capabilities for SSN, ZIP codes, phone numbers,
email addresses, IP addresses, and MAC addresses.
"""

from __future__ import annotations

import re
import pyarrow as pa
import pandas as pd

from .configs import (
    SSNConfig, ZipCodeConfig, PhoneNumberConfig,
    EmailConfig, IPAddressConfig, MACAddressConfig
)


class FormatTransformer:
    """Specialized transformer for structured format operations."""

    def apply_ssn_formatting(self, column: pa.Array, config: SSNConfig) -> pa.Array:
        """Format Social Security Numbers to XXX-XX-XXXX format."""
        if not pa.types.is_string(column.type):
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_ssn = self._format_ssn(str(value), config)
                formatted_values.append(formatted_ssn)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_ssn(self, value: str, config: SSNConfig) -> str:
        """Format a single SSN value."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty SSN value")

        # Remove all non-digits
        digits_only = re.sub(r'\D', '', original_value)

        # Check for letters in original
        if config.validate and re.search(r'[a-zA-Z]', original_value):
            raise ValueError("SSN contains letters")

        if not digits_only:
            raise ValueError("No digits found in SSN")

        # Validate length before zero padding
        if config.validate and len(digits_only) != 9:
            raise ValueError(f"SSN must have exactly 9 digits, got {len(digits_only)}")

        # Handle zero padding
        if config.zero_pad and len(digits_only) < 9:
            digits_only = digits_only.zfill(9)

        # Format with dashes
        if config.format_with_dashes:
            if len(digits_only) >= 6:
                if len(digits_only) == 9:
                    return f"{digits_only[:3]}-{digits_only[3:5]}-{digits_only[5:]}"
                elif len(digits_only) == 6:
                    return f"{digits_only[:2]}-{digits_only[2:4]}-{digits_only[4:]}"
                else:
                    if len(digits_only) <= 3:
                        return digits_only
                    elif len(digits_only) <= 5:
                        return f"{digits_only[:2]}-{digits_only[2:]}"
                    else:
                        return f"{digits_only[:3]}-{digits_only[3:5]}-{digits_only[5:]}"
            else:
                if len(digits_only) <= 2:
                    return digits_only
                elif len(digits_only) <= 4:
                    return f"{digits_only[:2]}-{digits_only[2:]}"
                else:
                    return f"{digits_only[:2]}-{digits_only[2:4]}-{digits_only[4:]}"
        else:
            return digits_only

    def apply_zip_code_formatting(self, column: pa.Array, config: ZipCodeConfig) -> pa.Array:
        """Format ZIP codes according to the specified type."""
        if not pa.types.is_string(column.type):
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_zip = self._format_zip_code(str(value), config)
                formatted_values.append(formatted_zip)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_zip_code(self, value: str, config: ZipCodeConfig) -> str:
        """Format a single ZIP code value."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty ZIP code value")

        digits_only = re.sub(r'\D', '', original_value)

        if not digits_only:
            raise ValueError("No digits found in ZIP code")

        if config.validate and len(digits_only) < len(original_value) * 0.5:
            raise ValueError("ZIP code contains too many non-digit characters")

        if config.zip_type == "zip-5":
            if config.zero_pad and len(digits_only) < 5:
                digits_only = digits_only.zfill(5)
            elif len(digits_only) > 5:
                digits_only = digits_only[:5]

            if config.validate and len(digits_only) != 5:
                raise ValueError(f"ZIP-5 must have exactly 5 digits, got {len(digits_only)}")

            return digits_only

        elif config.zip_type == "zip-9":
            if config.validate and len(digits_only) != 9:
                raise ValueError(f"ZIP-9 must have exactly 9 digits, got {len(digits_only)}")

            if config.zero_pad and len(digits_only) < 9:
                digits_only = digits_only.zfill(9)

            if config.format_with_dash and len(digits_only) == 9:
                return f"{digits_only[:5]}-{digits_only[5:]}"
            else:
                return digits_only

        else:  # zip-permissive
            if config.validate:
                if len(digits_only) not in [5, 9]:
                    raise ValueError(f"ZIP code must have 5 or 9 digits, got {len(digits_only)}")

            if config.zero_pad:
                if len(digits_only) <= 5:
                    digits_only = digits_only.zfill(5)
                elif len(digits_only) <= 9:
                    digits_only = digits_only.zfill(9)

            if len(digits_only) == 9 and config.format_with_dash:
                return f"{digits_only[:5]}-{digits_only[5:]}"
            else:
                return digits_only

    def apply_phone_number_formatting(self, column: pa.Array, config: PhoneNumberConfig) -> pa.Array:
        """Format phone numbers according to the specified style."""
        if not pa.types.is_string(column.type):
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_phone = self._format_phone_number(str(value), config)
                formatted_values.append(formatted_phone)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_phone_number(self, value: str, config: PhoneNumberConfig) -> str:
        """Format a single phone number value."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty phone number value")

        digits_and_plus = re.sub(r'[^\d+]', '', original_value)
        digits_only = re.sub(r'[^\d]', '', original_value)

        if not digits_only:
            raise ValueError("No digits found in phone number")

        if config.validate and re.search(r'[a-zA-Z]', original_value):
            raise ValueError("Phone number contains letters")

        # Handle country code detection
        has_country_code = False
        phone_digits = digits_only

        if digits_and_plus.startswith('+1') and len(digits_only) == 11 and digits_only.startswith('1'):
            has_country_code = True
            phone_digits = digits_only[1:]
        elif not digits_and_plus.startswith('+') and len(digits_only) == 11 and digits_only.startswith('1'):
            has_country_code = True
            phone_digits = digits_only[1:]
        elif len(digits_only) == 10:
            has_country_code = False
            phone_digits = digits_only

        # Validate phone number length
        if config.validate:
            if len(phone_digits) < config.min_digits or len(phone_digits) > config.max_digits:
                if len(digits_only) == 11 and digits_only.startswith('1'):
                    if len(phone_digits) != 10:
                        raise ValueError(f"Phone number must have {config.min_digits}-{config.max_digits} digits, got {len(phone_digits)}")
                else:
                    raise ValueError(f"Phone number must have {config.min_digits}-{config.max_digits} digits, got {len(phone_digits)}")

        # Format according to style
        if config.format_style == "international":
            if config.include_country_code or has_country_code:
                if phone_digits and len(phone_digits) == 10:
                    formatted_number = f"+1 {phone_digits}"
                else:
                    formatted_number = f"+1 {digits_only}"
            else:
                formatted_number = phone_digits

        elif config.format_style == "us-standard":
            if len(phone_digits) == 10:
                if config.include_country_code or has_country_code:
                    if config.use_parentheses:
                        formatted_number = f"1({phone_digits[:3]}) {phone_digits[3:6]}-{phone_digits[6:]}"
                    else:
                        formatted_number = f"1-{phone_digits[:3]}-{phone_digits[3:6]}-{phone_digits[6:]}"
                else:
                    if config.use_parentheses:
                        formatted_number = f"({phone_digits[:3]}) {phone_digits[3:6]}-{phone_digits[6:]}"
                    else:
                        formatted_number = f"{phone_digits[:3]}-{phone_digits[3:6]}-{phone_digits[6:]}"
            else:
                formatted_number = phone_digits

        elif config.format_style == "digits-only":
            if config.include_country_code or has_country_code:
                formatted_number = f"1{phone_digits}"
            else:
                formatted_number = phone_digits
        else:  # preserve
            formatted_number = original_value

        # Replace dashes with dots if requested
        if config.use_dots:
            formatted_number = formatted_number.replace('-', '.')

        return formatted_number

    def apply_email_formatting(self, column: pa.Array, config: EmailConfig) -> pa.Array:
        """Format email addresses according to the specified rules."""
        if not pa.types.is_string(column.type):
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_email = self._format_email(str(value), config)
                formatted_values.append(formatted_email)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_email(self, value: str, config: EmailConfig) -> str:
        """Format a single email value."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty email value")

        if config.normalize_case:
            original_value = original_value.lower()

        if config.strip_whitespace:
            original_value = original_value.strip()

        if config.normalize_domain and '.' in original_value:
            original_value = re.sub(r'\.+$', '', original_value)

        if config.validate_format:
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(pattern, original_value):
                raise ValueError("Invalid email format")

        return original_value

    def apply_ip_address_formatting(self, column: pa.Array, config: IPAddressConfig) -> pa.Array:
        """Format IP addresses according to the specified rules."""
        if not pa.types.is_string(column.type):
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_ip = self._format_ip_address(str(value), config)
                formatted_values.append(formatted_ip)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_ip_address(self, value: str, config: IPAddressConfig) -> str:
        """Format a single IP address value."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty IP address value")

        # Normalize IPv6 if requested
        if config.ip_version in {"ipv6", "both"}:
            try:
                normalized_ipv6 = self._normalize_ipv6_address(original_value, config.compress_ipv6)
                if normalized_ipv6:
                    original_value = normalized_ipv6
            except Exception:
                if not config.allow_invalid:
                    raise

        # Validate IP address format
        if config.validate:
            if config.ip_version == "ipv4" and not self._is_valid_ipv4(original_value):
                raise ValueError("Invalid IPv4 address")
            elif config.ip_version == "ipv6" and not self._is_valid_ipv6(original_value):
                raise ValueError("Invalid IPv6 address")
            elif config.ip_version == "both" and not (self._is_valid_ipv4(original_value) or self._is_valid_ipv6(original_value)):
                raise ValueError("Invalid IP address")

        return original_value

    def _normalize_ipv6_address(self, ipv6_address: str, compress: bool = True) -> str:
        """Normalize an IPv6 address."""
        import ipaddress

        try:
            parsed_ip = ipaddress.IPv6Address(ipv6_address)
            expanded = parsed_ip.exploded

            if compress:
                compressed = str(ipaddress.IPv6Address(expanded))
                return compressed
            else:
                return expanded
        except (ValueError, Exception):
            return None

    def _is_valid_ipv4(self, ip_address: str) -> bool:
        """Check if an IP address is a valid IPv4 address."""
        import ipaddress

        try:
            ipaddress.IPv4Address(ip_address)
            return True
        except (ValueError, Exception):
            return False

    def _is_valid_ipv6(self, ip_address: str) -> bool:
        """Check if an IP address is a valid IPv6 address."""
        import ipaddress

        try:
            ipaddress.IPv6Address(ip_address)
            return True
        except (ValueError, Exception):
            return False

    def apply_mac_address_formatting(self, column: pa.Array, config: MACAddressConfig) -> pa.Array:
        """Format MAC addresses according to the specified rules."""
        if not pa.types.is_string(column.type):
            column = pa.compute.cast(column, pa.string())

        pandas_series = column.to_pandas()
        formatted_values = []

        for value in pandas_series:
            if pd.isna(value) or value is None:
                formatted_values.append(None)
                continue

            try:
                formatted_mac = self._format_mac_address(str(value), config)
                formatted_values.append(formatted_mac)
            except ValueError:
                if config.allow_invalid:
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(None)

        return pa.array(formatted_values)

    def _format_mac_address(self, value: str, config: MACAddressConfig) -> str:
        """Format a single MAC address value."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty MAC address value")

        # Remove all non-hexadecimal characters
        hex_only = re.sub(r'[^0-9A-Fa-f]', '', original_value)

        if not hex_only:
            raise ValueError("No hexadecimal digits found in MAC address")

        if len(hex_only) < 6:
            raise ValueError(f"MAC address must have at least 6 hexadecimal digits, got {len(hex_only)}")

        # Handle zero padding
        if config.zero_pad and len(hex_only) < 12:
            hex_only = hex_only.zfill(12)

        # Validate MAC address length
        if config.validate and len(hex_only) != 12:
            raise ValueError(f"MAC address must have exactly 12 hexadecimal digits, got {len(hex_only)}")

        # Truncate to 12 characters if needed
        if len(hex_only) > 12:
            hex_only = hex_only[:12]

        # Split into octets
        octets = [hex_only[i:i+2] for i in range(0, 12, 2)]

        # Format according to style
        if config.format_style == "colon":
            formatted_mac = ':'.join(octets)
        elif config.format_style == "dash":
            formatted_mac = '-'.join(octets)
        elif config.format_style == "dot":
            formatted_mac = '.'.join([''.join(octets[i:i+2]) for i in range(0, 6, 2)])
        else:  # none
            formatted_mac = ''.join(octets)

        # Apply case transformation
        if config.case_style == "upper":
            formatted_mac = formatted_mac.upper()
        elif config.case_style == "lower":
            formatted_mac = formatted_mac.lower()

        return formatted_mac
