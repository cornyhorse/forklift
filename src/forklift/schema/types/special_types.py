"""Special type detection and handling."""

import ipaddress
import re
from typing import Any, Dict, List, Optional, Tuple

from ..utils.helpers import split_name_tokens


def _is_ip_address(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


class SpecialTypeDetector:
    """Detects and suggests special data types based on content patterns.

    Content patterns are matched against the *whole* (stripped) value and are deliberately
    strict: bare digit strings are ambiguous (a 9-digit id is not an SSN, a 10-digit epoch is
    not a phone number, a 5-digit count is not a ZIP code), so SSN and phone numbers require
    their separators and a plain 5-digit ZIP code is only recognised through the column name.
    """

    _OCTET = r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)"

    # Pattern definitions for special types
    PATTERNS = {
        "ssn": [r"\d{3}-\d{2}-\d{4}"],  # 123-45-6789
        "phone": [
            r"(?:\+?1[\s.-]?)?\(\d{3}\)\s*\d{3}[\s.-]\d{4}",  # (123) 456-7890 or (123)456-7890
            r"(?:\+?1[\s.-]?)?\d{3}[\s.-]\d{3}[\s.-]\d{4}",  # 123-456-7890, 123.456.7890
        ],
        "email": [r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"],
        "zip_code": [r"\d{5}-\d{4}"],  # ZIP+4; bare 5 digits is too ambiguous
        "ip_address": [
            rf"(?:{_OCTET}\.){{3}}{_OCTET}",  # IPv4
            r"(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}",  # IPv6 (full form)
        ],
        "mac_address": [
            r"(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}",
            r"(?:[0-9a-fA-F]{4}\.){2}[0-9a-fA-F]{4}",
        ],
    }

    # Extra validators for formats a regular expression cannot express (compressed IPv6 ...)
    _VALIDATORS = {"ip_address": _is_ip_address}

    # Column-name hints as sequences of whole name tokens (see ``split_name_tokens``): a
    # column called ``tip``, ``description`` or ``ship_date`` is not an IP address column.
    NAME_TOKEN_PATTERNS: Dict[str, List[Tuple[str, ...]]] = {
        "ssn": [("ssn",), ("social", "security")],
        "phone": [("phone",), ("telephone",), ("tel",)],
        "email": [("email",), ("e", "mail")],
        "zip_code": [("zip",), ("zipcode",), ("postal", "code"), ("postalcode",), ("postcode",)],
        "ip_address": [("ip",), ("ipaddress",), ("ipaddr",)],
        "mac_address": [("mac",), ("macaddress",), ("macaddr",)],
    }

    _COMPILED_PATTERNS = {
        special_type: [re.compile(pattern) for pattern in patterns]
        for special_type, patterns in PATTERNS.items()
    }

    @classmethod
    def detect_special_type(
        cls, column_name: str, sample_values: List[str], confidence_threshold: float = 0.7
    ) -> Optional[str]:
        """Detect special type based on column name and sample values.

        Args:
            column_name: Name of the column
            sample_values: Sample values from the column
            confidence_threshold: Minimum confidence required for detection

        Returns:
            Optional[str]: Detected special type or None
        """
        if not sample_values:
            return None

        # Check column name patterns first
        name_based_type = cls._detect_from_column_name(column_name)

        # Check content patterns
        content_based_type = cls._detect_from_content(sample_values, confidence_threshold)

        # Prefer content-based detection if both exist
        return content_based_type or name_based_type

    @classmethod
    def _detect_from_column_name(cls, column_name: str) -> Optional[str]:
        """Detect special type from whole word tokens of the column name."""
        tokens = split_name_tokens(column_name)

        for special_type, token_patterns in cls.NAME_TOKEN_PATTERNS.items():
            for pattern in token_patterns:
                width = len(pattern)
                for start in range(len(tokens) - width + 1):
                    if tuple(tokens[start : start + width]) == pattern:
                        return special_type

        return None

    @classmethod
    def _detect_from_content(
        cls, sample_values: List[str], confidence_threshold: float
    ) -> Optional[str]:
        """Detect special type from content patterns."""
        if not sample_values:
            return None

        # Filter out null/empty values
        valid_values = [str(v).strip() for v in sample_values if v is not None and str(v).strip()]
        if not valid_values:
            return None

        total_values = len(valid_values)

        for special_type, patterns in cls._COMPILED_PATTERNS.items():
            validator = cls._VALIDATORS.get(special_type)
            match_count = 0

            for value in valid_values:
                if any(pattern.fullmatch(value) for pattern in patterns) or (
                    validator is not None and validator(value)
                ):
                    match_count += 1

            confidence = match_count / total_values if total_values > 0 else 0

            if confidence >= confidence_threshold:
                return special_type

        return None

    @staticmethod
    def get_transformation_config(special_type: str) -> Dict[str, Any]:
        """Get default transformation configuration for a special type.

        Args:
            special_type: The detected special type

        Returns:
            Dict: Transformation configuration
        """
        configs = {
            "ssn": {
                "format_with_dashes": True,
                "zero_pad": True,
                "validate": True,
                "allow_invalid": False,
            },
            "phone": {
                "format_style": "us-standard",
                "use_parentheses": True,
                "use_dashes": True,
                "validate": True,
                "allow_invalid": False,
            },
            "email": {
                "normalize_case": True,
                "validate_format": True,
                "allow_invalid": False,
                "strip_whitespace": True,
                "normalize_domain": True,
            },
            "zip_code": {
                "zip_type": "zip-permissive",
                "format_with_dash": True,
                "zero_pad": True,
                "validate": True,
                "allow_invalid": False,
            },
            "ip_address": {
                "ip_version": "both",
                "normalize_ipv6": True,
                "validate": True,
                "allow_invalid": False,
                "compress_ipv6": True,
            },
            "mac_address": {
                "format_style": "colon",
                "case_style": "lower",
                "validate": True,
                "allow_invalid": False,
                "zero_pad": True,
            },
        }

        return configs.get(special_type, {})
