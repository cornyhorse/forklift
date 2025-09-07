"""Backward compatibility transformer that maintains the original FormatTransformer interface."""

from __future__ import annotations

import pyarrow as pa
from .ssn import SSNFormatter
from .postal import ZipCodeFormatter
from .phone import PhoneNumberFormatter
from .email import EmailFormatter
from .network import IPAddressFormatter, MACAddressFormatter
from ..configs import (
    SSNConfig, ZipCodeConfig, PhoneNumberConfig,
    EmailConfig, IPAddressConfig, MACAddressConfig
)


class FormatTransformer:
    """Specialized transformer for structured format operations.

    This class maintains backward compatibility with the original interface
    while delegating to the new modular formatters.
    """

    def apply_ssn_formatting(self, column: pa.Array, config: SSNConfig) -> pa.Array:
        """Format Social Security Numbers to XXX-XX-XXXX format."""
        formatter = SSNFormatter(config)
        return formatter.apply_formatting(column)

    def apply_zip_code_formatting(self, column: pa.Array, config: ZipCodeConfig) -> pa.Array:
        """Format ZIP codes according to the specified type."""
        formatter = ZipCodeFormatter(config)
        return formatter.apply_formatting(column)

    def apply_phone_number_formatting(self, column: pa.Array, config: PhoneNumberConfig) -> pa.Array:
        """Format phone numbers according to the specified style."""
        formatter = PhoneNumberFormatter(config)
        return formatter.apply_formatting(column)

    def apply_email_formatting(self, column: pa.Array, config: EmailConfig) -> pa.Array:
        """Format email addresses according to the specified rules."""
        formatter = EmailFormatter(config)
        return formatter.apply_formatting(column)

    def apply_ip_address_formatting(self, column: pa.Array, config: IPAddressConfig) -> pa.Array:
        """Format IP addresses according to the specified rules."""
        formatter = IPAddressFormatter(config)
        return formatter.apply_formatting(column)

    def apply_mac_address_formatting(self, column: pa.Array, config: MACAddressConfig) -> pa.Array:
        """Format MAC addresses according to the specified rules."""
        formatter = MACAddressFormatter(config)
        return formatter.apply_formatting(column)

    # Legacy method aliases for backward compatibility
    def _format_ssn(self, value: str, config: SSNConfig) -> str:
        """Format a single SSN value."""
        formatter = SSNFormatter(config)
        return formatter.format_value(value)

    def _format_zip_code(self, value: str, config: ZipCodeConfig) -> str:
        """Format a single ZIP code value."""
        formatter = ZipCodeFormatter(config)
        return formatter.format_value(value)

    def _format_phone_number(self, value: str, config: PhoneNumberConfig) -> str:
        """Format a single phone number value."""
        formatter = PhoneNumberFormatter(config)
        return formatter.format_value(value)

    def _format_email(self, value: str, config: EmailConfig) -> str:
        """Format a single email value."""
        formatter = EmailFormatter(config)
        return formatter.format_value(value)

    def _format_ip_address(self, value: str, config: IPAddressConfig) -> str:
        """Format a single IP address value."""
        formatter = IPAddressFormatter(config)
        return formatter.format_value(value)

    def _format_mac_address(self, value: str, config: MACAddressConfig) -> str:
        """Format a single MAC address value."""
        formatter = MACAddressFormatter(config)
        return formatter.format_value(value)
