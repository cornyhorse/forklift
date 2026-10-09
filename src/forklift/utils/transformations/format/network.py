"""Network address formatting utilities (IP and MAC addresses)."""

from __future__ import annotations

import re

from ..configs import IPAddressConfig, MACAddressConfig
from .base import BaseFormatter, ValidationMixin


class IPAddressFormatter(BaseFormatter):
    """Formatter for IP addresses."""

    def __init__(self, config: IPAddressConfig):
        super().__init__(config)

    def format_value(self, value: str) -> str:
        """Format a single IP address value according to the specified rules."""
        original_value = value.strip()

        if not original_value:
            raise ValueError("Empty IP address value")

        # Normalize IPv6 if requested (normalize_ipv6=False leaves the text as it is)
        if self.config.normalize_ipv6 and self.config.ip_version in {"ipv6", "both"}:
            normalized_ipv6 = self._normalize_ipv6_address(
                original_value, self.config.compress_ipv6
            )
            if normalized_ipv6:
                original_value = normalized_ipv6

        # Validate IP address format
        if self.config.validate:
            if self.config.ip_version == "ipv4" and not self._is_valid_ipv4(original_value):
                raise ValueError("Invalid IPv4 address")
            elif self.config.ip_version == "ipv6" and not self._is_valid_ipv6(original_value):
                raise ValueError("Invalid IPv6 address")
            elif self.config.ip_version == "both" and not (
                self._is_valid_ipv4(original_value) or self._is_valid_ipv6(original_value)
            ):
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

        if not re.search(r"[0-9A-Fa-f]", original_value):
            raise ValueError("No hexadecimal digits found in MAC address")

        octets = self._parse_octets(original_value)
        if octets is None:
            if not self.config.validate:
                return original_value  # no validation requested: pass the text through untouched
            raise ValueError(
                "MAC address must be exactly 12 hexadecimal digits "
                "(optionally separated by ':', '-', '.' or spaces)"
            )

        # Format according to style
        formatted_mac = self._apply_format_style(octets)

        # Apply case transformation
        if self.config.case_style == "upper":
            formatted_mac = formatted_mac.upper()
        elif self.config.case_style == "lower":
            formatted_mac = formatted_mac.lower()

        return formatted_mac

    def _parse_octets(self, text: str) -> list[str] | None:
        """Split a MAC address into its six octets, or None if it is not exactly 12 hex digits.

        Accepts the compact form (``001122334455``), one consistent separator between octets
        (``00:11:22:33:44:55``, ``00-11-...``, ``00 11 ...``, unpadded ``0:1a:2b:3:4:5`` when
        ``zero_pad`` is on) and three groups of four digits (``0011.2233.4455``). Short input is
        never padded into a different address and extra digits are never truncated.
        """
        if re.fullmatch(r"[0-9A-Fa-f]{12}", text):
            return [text[i : i + 2] for i in range(0, 12, 2)]

        separators = set(re.findall(r"[^0-9A-Fa-f]", text))
        if len(separators) != 1:
            return None
        separator = separators.pop()
        if separator not in ":-. ":
            return None

        groups = text.split(separator)
        if not all(re.fullmatch(r"[0-9A-Fa-f]{1,4}", group) for group in groups):
            return None

        if len(groups) == 3 and all(len(group) == 4 for group in groups):
            return [group[i : i + 2] for group in groups for i in (0, 2)]

        if len(groups) == 6 and all(len(group) <= 2 for group in groups):
            if any(len(group) == 1 for group in groups) and not self.config.zero_pad:
                return None  # unpadded octets are only accepted when zero padding is enabled
            return [group.zfill(2) for group in groups]
        return None

    def _apply_format_style(self, octets: list[str]) -> str:
        """Apply the specified MAC address format style."""
        if self.config.format_style == "colon":
            return ":".join(octets)
        elif self.config.format_style == "dash":
            return "-".join(octets)
        elif self.config.format_style == "dot":
            return ".".join(["".join(octets[i : i + 2]) for i in range(0, 6, 2)])
        else:  # none
            return "".join(octets)
