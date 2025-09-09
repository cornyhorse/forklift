"""Network address formatting utilities (IP and MAC addresses)."""

from __future__ import annotations

import re
from .base import BaseFormatter, ValidationMixin
from ..configs import IPAddressConfig, MACAddressConfig


class IPAddressFormatter(BaseFormatter):
    """Formatter for IP addresses."""

    def __init__(self, config: IPAddressConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single IP address value according to the specified rules."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty IP address value")

        # Normalize IPv6 if requested
        if self.config.ip_version in {"ipv6", "both"}:
            try:
                normalized_ipv6 = self._normalize_ipv6_address(original_value, self.config.compress_ipv6)
                if normalized_ipv6:
                    original_value = normalized_ipv6
            except Exception:
                if not self.config.allow_invalid:
                    raise

        # Validate IP address format
        if self.config.validate:
            if self.config.ip_version == "ipv4" and not self._is_valid_ipv4(original_value):
                raise ValueError("Invalid IPv4 address")
            elif self.config.ip_version == "ipv6" and not self._is_valid_ipv6(original_value):
                raise ValueError("Invalid IPv6 address")
            elif self.config.ip_version == "both" and not (self._is_valid_ipv4(original_value) or self._is_valid_ipv6(original_value)):
                raise ValueError("Invalid IP address")

        return original_value

    def _normalize_ipv6_address(self, ipv6_address: str, compress: bool = True) -> str | None:
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


class MACAddressFormatter(BaseFormatter, ValidationMixin):
    """Formatter for MAC addresses."""

    def __init__(self, config: MACAddressConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single MAC address value according to the specified rules."""
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
        if self.config.zero_pad and len(hex_only) < 12:
            hex_only = hex_only.zfill(12)

        # Validate MAC address length
        if self.config.validate and len(hex_only) != 12:
            raise ValueError(f"MAC address must have exactly 12 hexadecimal digits, got {len(hex_only)}")

        # Truncate to 12 characters if needed
        if len(hex_only) > 12:
            hex_only = hex_only[:12]

        # Split into octets
        octets = [hex_only[i:i+2] for i in range(0, 12, 2)]

        # Format according to style
        formatted_mac = self._apply_format_style(octets)

        # Apply case transformation
        if self.config.case_style == "upper":
            formatted_mac = formatted_mac.upper()
        elif self.config.case_style == "lower":
            formatted_mac = formatted_mac.lower()

        return formatted_mac

    def _apply_format_style(self, octets: list[str]) -> str:
        """Apply the specified MAC address format style."""
        if self.config.format_style == "colon":
            return ':'.join(octets)
        elif self.config.format_style == "dash":
            return '-'.join(octets)
        elif self.config.format_style == "dot":
            return '.'.join([''.join(octets[i:i+2]) for i in range(0, 6, 2)])
        else:  # none
            return ''.join(octets)
