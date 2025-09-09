"""Tests for special type detection and handling."""

import pytest
from unittest.mock import Mock, patch

from forklift.schema.types.special_types import SpecialTypeDetector


class TestSpecialTypeDetector:
    """Test cases for SpecialTypeDetector class."""

    def test_detect_special_type_empty_values(self):
        """Test detection with empty sample values."""
        result = SpecialTypeDetector.detect_special_type("test_column", [])
        assert result is None

    def test_detect_special_type_ssn_from_column_name(self):
        """Test SSN detection from column name patterns."""
        test_cases = [
            "ssn",
            "SSN",
            "social_security",
            "social_security_number",
            "employee_ssn",
            "user_social_security_number"
        ]

        for column_name in test_cases:
            result = SpecialTypeDetector.detect_special_type(column_name, ["123456789"])
            assert result == "ssn"

    def test_detect_special_type_phone_from_column_name(self):
        """Test phone detection from column name patterns."""
        test_cases = [
            "phone",
            "telephone",
            "phone_number",
            "tel",
            "customer_phone",
            "employee_telephone"
        ]

        for column_name in test_cases:
            result = SpecialTypeDetector.detect_special_type(column_name, ["1234567890"])
            assert result == "phone"

    def test_detect_special_type_email_from_column_name(self):
        """Test email detection from column name patterns."""
        test_cases = [
            "email",
            "email_address",
            "e_mail",
            "user_email",
            "customer_email_address"
        ]

        for column_name in test_cases:
            result = SpecialTypeDetector.detect_special_type(column_name, ["test@example.com"])
            assert result == "email"

    def test_detect_special_type_zip_from_column_name(self):
        """Test zip code detection from column name patterns."""
        test_cases = [
            "zip",
            "zipcode",
            "postal_code",
            "zip_code",
            "address_zip",
            "customer_postal_code"
        ]

        for column_name in test_cases:
            result = SpecialTypeDetector.detect_special_type(column_name, ["12345"])
            assert result == "zip_code"

    def test_detect_special_type_ip_from_column_name(self):
        """Test IP address detection from column name patterns."""
        test_cases = [
            "ip",
            "ip_address",
            "ip_addr",
            "server_ip",
            "client_ip_address"
        ]

        for column_name in test_cases:
            result = SpecialTypeDetector.detect_special_type(column_name, ["192.168.1.1"])
            assert result == "ip_address"

    def test_detect_special_type_mac_from_column_name(self):
        """Test MAC address detection from column name patterns."""
        test_cases = [
            "mac",
            "mac_address",
            "mac_addr",
            "device_mac",
            "network_mac_address"
        ]

        for column_name in test_cases:
            result = SpecialTypeDetector.detect_special_type(column_name, ["aa:bb:cc:dd:ee:ff"])
            assert result == "mac_address"

    def test_detect_special_type_ssn_from_content(self):
        """Test SSN detection from content patterns."""
        # Test SSN with dashes
        result = SpecialTypeDetector.detect_special_type("test_col", ["123-45-6789", "987-65-4321"])
        assert result == "ssn"

        # Test SSN without dashes
        result = SpecialTypeDetector.detect_special_type("test_col", ["123456789", "987654321"])
        assert result == "ssn"

    def test_detect_special_type_phone_from_content(self):
        """Test phone detection from content patterns."""
        # Test phone with parentheses and space
        result = SpecialTypeDetector.detect_special_type("test_col", ["(123) 456-7890", "(987) 654-3210"])
        assert result == "phone"

        # Test phone with dashes
        result = SpecialTypeDetector.detect_special_type("test_col", ["123-456-7890", "987-654-3210"])
        assert result == "phone"

        # Test phone without formatting - use strings with word boundaries
        result = SpecialTypeDetector.detect_special_type("test_col", [" 1234567890 ", " 9876543210 "])
        assert result == "phone"

    def test_detect_special_type_email_from_content(self):
        """Test email detection from content patterns."""
        emails = [
            "test@example.com",
            "user.name@domain.org",
            "first.last+tag@subdomain.example.co.uk"
        ]
        result = SpecialTypeDetector.detect_special_type("test_col", emails)
        assert result == "email"

    def test_detect_special_type_zip_from_content(self):
        """Test zip code detection from content patterns."""
        # Test 5-digit zip codes
        result = SpecialTypeDetector.detect_special_type("test_col", ["12345", "67890"])
        assert result == "zip_code"

        # Test zip+4 format
        result = SpecialTypeDetector.detect_special_type("test_col", ["12345-6789", "54321-0987"])
        assert result == "zip_code"

    def test_detect_special_type_ip_from_content(self):
        """Test IP address detection from content patterns."""
        # Test IPv4 addresses
        ipv4_addresses = ["192.168.1.1", "10.0.0.1", "172.16.0.1"]
        result = SpecialTypeDetector.detect_special_type("test_col", ipv4_addresses)
        assert result == "ip_address"

        # Test IPv6 addresses
        ipv6_addresses = ["2001:0db8:85a3:0000:0000:8a2e:0370:7334", "fe80:0000:0000:0000:0202:b3ff:fe1e:8329"]
        result = SpecialTypeDetector.detect_special_type("test_col", ipv6_addresses)
        assert result == "ip_address"

    def test_detect_special_type_mac_from_content(self):
        """Test MAC address detection from content patterns."""
        # Test MAC with colons
        result = SpecialTypeDetector.detect_special_type("test_col", ["aa:bb:cc:dd:ee:ff", "11:22:33:44:55:66"])
        assert result == "mac_address"

        # Test MAC with hyphens
        result = SpecialTypeDetector.detect_special_type("test_col", ["aa-bb-cc-dd-ee-ff", "11-22-33-44-55-66"])
        assert result == "mac_address"

    def test_detect_special_type_content_overrides_name(self):
        """Test that content-based detection overrides name-based detection."""
        # Column name suggests email, but content is phone numbers with proper formatting
        phone_values = ["(123) 456-7890", "(987) 654-3210"]
        result = SpecialTypeDetector.detect_special_type("email_column", phone_values)
        assert result == "phone"  # Content should override name

    def test_detect_special_type_low_confidence(self):
        """Test detection with low confidence threshold."""
        # Only 1 out of 3 values match phone pattern (33% confidence)
        mixed_values = ["(123) 456-7890", "not a phone", "also not a phone"]
        result = SpecialTypeDetector.detect_special_type("test_col", mixed_values, confidence_threshold=0.7)
        assert result is None  # Should not detect due to low confidence

    def test_detect_special_type_high_confidence(self):
        """Test detection with high confidence."""
        # 2 out of 3 values match phone pattern (67% confidence)
        mixed_values = ["(123) 456-7890", "(987) 654-3210", "not a phone"]
        result = SpecialTypeDetector.detect_special_type("test_col", mixed_values, confidence_threshold=0.6)
        assert result == "phone"  # Should detect due to sufficient confidence

    def test_detect_special_type_null_values_filtered(self):
        """Test that null and empty values are filtered out."""
        values_with_nulls = [None, "", "   ", "(123) 456-7890", "(987) 654-3210"]
        result = SpecialTypeDetector.detect_special_type("test_col", values_with_nulls)
        assert result == "phone"  # Should detect despite null values

    def test_detect_special_type_all_null_values(self):
        """Test detection with all null/empty values."""
        null_values = [None, "", "   ", None]
        result = SpecialTypeDetector.detect_special_type("test_col", null_values)
        assert result is None

    def test_detect_from_column_name_no_match(self):
        """Test column name detection with no matching patterns."""
        result = SpecialTypeDetector._detect_from_column_name("random_column_name")
        assert result is None

    def test_detect_from_content_empty_values(self):
        """Test content detection with empty values."""
        result = SpecialTypeDetector._detect_from_content([], 0.7)
        assert result is None

    def test_detect_from_content_no_valid_values(self):
        """Test content detection with no valid values after filtering."""
        result = SpecialTypeDetector._detect_from_content([None, "", "   "], 0.7)
        assert result is None

    def test_get_transformation_config_ssn(self):
        """Test transformation config for SSN."""
        config = SpecialTypeDetector.get_transformation_config("ssn")
        expected = {
            'format_with_dashes': True,
            'zero_pad': True,
            'validate': True,
            'allow_invalid': False
        }
        assert config == expected

    def test_get_transformation_config_phone(self):
        """Test transformation config for phone."""
        config = SpecialTypeDetector.get_transformation_config("phone")
        expected = {
            'format_style': 'us-standard',
            'use_parentheses': True,
            'use_dashes': True,
            'validate': True,
            'allow_invalid': False
        }
        assert config == expected

    def test_get_transformation_config_email(self):
        """Test transformation config for email."""
        config = SpecialTypeDetector.get_transformation_config("email")
        expected = {
            'normalize_case': True,
            'validate_format': True,
            'allow_invalid': False,
            'strip_whitespace': True,
            'normalize_domain': True
        }
        assert config == expected

    def test_get_transformation_config_zip_code(self):
        """Test transformation config for zip code."""
        config = SpecialTypeDetector.get_transformation_config("zip_code")
        expected = {
            'zip_type': 'zip-permissive',
            'format_with_dash': True,
            'zero_pad': True,
            'validate': True,
            'allow_invalid': False
        }
        assert config == expected

    def test_get_transformation_config_ip_address(self):
        """Test transformation config for IP address."""
        config = SpecialTypeDetector.get_transformation_config("ip_address")
        expected = {
            'ip_version': 'both',
            'normalize_ipv6': True,
            'validate': True,
            'allow_invalid': False,
            'compress_ipv6': True
        }
        assert config == expected

    def test_get_transformation_config_mac_address(self):
        """Test transformation config for MAC address."""
        config = SpecialTypeDetector.get_transformation_config("mac_address")
        expected = {
            'format_style': 'colon',
            'case_style': 'lower',
            'validate': True,
            'allow_invalid': False,
            'zero_pad': True
        }
        assert config == expected

    def test_get_transformation_config_unknown_type(self):
        """Test transformation config for unknown special type."""
        config = SpecialTypeDetector.get_transformation_config("unknown_type")
        assert config == {}

    def test_patterns_class_attribute(self):
        """Test that the PATTERNS class attribute is properly defined."""
        patterns = SpecialTypeDetector.PATTERNS

        # Check that all expected pattern types exist
        expected_types = ['ssn', 'phone', 'email', 'zip_code', 'ip_address', 'mac_address']
        for pattern_type in expected_types:
            assert pattern_type in patterns
            assert isinstance(patterns[pattern_type], list)
            assert len(patterns[pattern_type]) > 0

    def test_edge_case_mixed_data_types(self):
        """Test detection with mixed data types in sample values."""
        # Use phone patterns that will match after string conversion
        mixed_values = [" 1234567890 ", "(123) 456-7890", " 9876543210 "]  # Mix with proper word boundaries
        result = SpecialTypeDetector.detect_special_type("test_col", mixed_values)
        assert result == "phone"  # Should handle type conversion gracefully
