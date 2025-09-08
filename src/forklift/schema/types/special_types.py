"""Special type detection and handling."""

import re
from typing import Dict, List, Optional, Any
import pandas as pd


class SpecialTypeDetector:
    """Detects and suggests special data types based on content patterns."""

    # Pattern definitions for special types
    PATTERNS = {
        'ssn': [
            r'\b\d{3}-\d{2}-\d{4}\b',  # 123-45-6789
            r'\b\d{9}\b'                # 123456789
        ],
        'phone': [
            r'\(\d{3}\)\s*\d{3}-\d{4}',     # (123) 456-7890 or (123)456-7890
            r'\b\d{3}-\d{3}-\d{4}\b',        # 123-456-7890
            r'\b\d{10}\b'                     # 1234567890
        ],
        'email': [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        ],
        'zip_code': [
            r'\b\d{5}\b',           # 12345
            r'\b\d{5}-\d{4}\b'      # 12345-6789
        ],
        'ip_address': [
            r'\b(?:\d{1,3}\.){3}\d{1,3}\b',  # IPv4
            r'\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b'  # IPv6
        ],
        'mac_address': [
            r'\b(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}\b'
        ]
    }

    @classmethod
    def detect_special_type(cls, column_name: str, sample_values: List[str], confidence_threshold: float = 0.7) -> Optional[str]:
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
        """Detect special type from column name patterns."""
        name_lower = column_name.lower()

        name_patterns = {
            'ssn': ['ssn', 'social_security', 'social_security_number'],
            'phone': ['phone', 'telephone', 'phone_number', 'tel'],
            'email': ['email', 'email_address', 'e_mail'],
            'zip_code': ['zip', 'zipcode', 'postal_code', 'zip_code'],
            'ip_address': ['ip', 'ip_address', 'ip_addr'],
            'mac_address': ['mac', 'mac_address', 'mac_addr']
        }

        for special_type, patterns in name_patterns.items():
            if any(pattern in name_lower for pattern in patterns):
                return special_type

        return None

    @classmethod
    def _detect_from_content(cls, sample_values: List[str], confidence_threshold: float) -> Optional[str]:
        """Detect special type from content patterns."""
        if not sample_values:
            return None

        # Filter out null/empty values
        valid_values = [str(v) for v in sample_values if v and str(v).strip()]
        if not valid_values:
            return None

        total_values = len(valid_values)

        for special_type, patterns in cls.PATTERNS.items():
            match_count = 0

            for value in valid_values:
                if any(re.search(pattern, str(value)) for pattern in patterns):
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
            'ssn': {
                'format_with_dashes': True,
                'zero_pad': True,
                'validate': True,
                'allow_invalid': False
            },
            'phone': {
                'format_style': 'us-standard',
                'use_parentheses': True,
                'use_dashes': True,
                'validate': True,
                'allow_invalid': False
            },
            'email': {
                'normalize_case': True,
                'validate_format': True,
                'allow_invalid': False,
                'strip_whitespace': True,
                'normalize_domain': True
            },
            'zip_code': {
                'zip_type': 'zip-permissive',
                'format_with_dash': True,
                'zero_pad': True,
                'validate': True,
                'allow_invalid': False
            },
            'ip_address': {
                'ip_version': 'both',
                'normalize_ipv6': True,
                'validate': True,
                'allow_invalid': False,
                'compress_ipv6': True
            },
            'mac_address': {
                'format_style': 'colon',
                'case_style': 'lower',
                'validate': True,
                'allow_invalid': False,
                'zero_pad': True
            }
        }

        return configs.get(special_type, {})
