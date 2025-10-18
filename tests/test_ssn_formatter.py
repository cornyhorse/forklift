"""Tests for SSN formatting utilities."""

from unittest.mock import Mock

import pytest

from forklift.utils.transformations.configs import SSNConfig
from forklift.utils.transformations.format.ssn import SSNFormatter


class TestSSNFormatter:
    """Test cases for SSNFormatter class."""

    @pytest.fixture
    def default_config(self):
        """Create default SSN configuration."""
        return SSNConfig(format_with_dashes=True, zero_pad=True, validate=True, allow_invalid=False)

    @pytest.fixture
    def permissive_config(self):
        """Create permissive SSN configuration."""
        return SSNConfig(
            format_with_dashes=True, zero_pad=False, validate=False, allow_invalid=True
        )

    def test_init(self, default_config):
        """Test SSNFormatter initialization."""
        formatter = SSNFormatter(default_config)
        assert formatter.config == default_config

    def test_format_value_valid_ssn_with_dashes(self, default_config):
        """Test formatting valid SSN that already has dashes."""
        formatter = SSNFormatter(default_config)
        result = formatter.format_value("123-45-6789")
        assert result == "123-45-6789"

    def test_format_value_valid_ssn_without_dashes(self, default_config):
        """Test formatting valid SSN without dashes."""
        formatter = SSNFormatter(default_config)
        result = formatter.format_value("123456789")
        assert result == "123-45-6789"

    def test_format_value_ssn_with_spaces(self, default_config):
        """Test formatting SSN with spaces."""
        formatter = SSNFormatter(default_config)
        result = formatter.format_value("123 45 6789")
        assert result == "123-45-6789"

    def test_format_value_ssn_mixed_formatting(self, default_config):
        """Test formatting SSN with mixed formatting."""
        formatter = SSNFormatter(default_config)
        result = formatter.format_value("123.45.6789")
        assert result == "123-45-6789"

    def test_format_value_empty_ssn(self, default_config):
        """Test formatting empty SSN raises error."""
        formatter = SSNFormatter(default_config)
        with pytest.raises(ValueError, match="Empty SSN value"):
            formatter.format_value("")

    def test_format_value_whitespace_only(self, default_config):
        """Test formatting whitespace-only SSN raises error."""
        formatter = SSNFormatter(default_config)
        with pytest.raises(ValueError, match="Empty SSN value"):
            formatter.format_value("   ")

    def test_format_value_contains_letters_validation_enabled(self, default_config):
        """Test formatting SSN with letters when validation is enabled."""
        formatter = SSNFormatter(default_config)
        with pytest.raises(ValueError, match="SSN contains letters"):
            formatter.format_value("123-45-ABCD")

    def test_format_value_contains_letters_validation_disabled(self, permissive_config):
        """Test formatting SSN with letters when validation is disabled."""
        formatter = SSNFormatter(permissive_config)
        # Should extract digits and format them
        result = formatter.format_value("123-45-A678")
        assert result == "123-45-678"  # Letters removed, formatted

    def test_format_value_no_digits(self, default_config):
        """Test formatting SSN with no digits."""
        formatter = SSNFormatter(default_config)
        with pytest.raises(ValueError, match="No digits found in SSN"):
            formatter.format_value("ABC-DE-FGHI")

    def test_format_value_wrong_length_validation_enabled(self, default_config):
        """Test formatting SSN with wrong length when validation is enabled."""
        formatter = SSNFormatter(default_config)
        with pytest.raises(ValueError, match="SSN must have exactly 9 digits, got 8"):
            formatter.format_value("12345678")

    def test_format_value_wrong_length_validation_disabled(self, permissive_config):
        """Test formatting SSN with wrong length when validation is disabled."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("12345678")
        assert result == "123-45-678"  # Formats available digits

    def test_format_value_short_ssn_with_zero_padding(self, default_config):
        """Test formatting short SSN with zero padding enabled."""
        formatter = SSNFormatter(default_config)
        # Should not zero pad if validation is enabled and length is wrong
        with pytest.raises(ValueError, match="SSN must have exactly 9 digits"):
            formatter.format_value("12345")

    def test_format_value_short_ssn_no_zero_padding(self, permissive_config):
        """Test formatting short SSN without zero padding."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("12345")
        assert result == "12-34-5"

    def test_format_value_no_dashes_format(self):
        """Test formatting SSN without dash formatting."""
        config = SSNConfig(
            format_with_dashes=False, zero_pad=True, validate=True, allow_invalid=False
        )
        formatter = SSNFormatter(config)
        result = formatter.format_value("123456789")
        assert result == "123456789"

    def test_format_value_6_digit_ssn_with_dashes(self, permissive_config):
        """Test formatting 6-digit SSN with dashes."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("123456")
        assert result == "12-34-56"

    def test_format_value_3_digit_ssn_with_dashes(self, permissive_config):
        """Test formatting 3-digit SSN with dashes."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("123")
        assert result == "123"  # Too short for dashes

    def test_format_value_4_digit_ssn_with_dashes(self, permissive_config):
        """Test formatting 4-digit SSN with dashes."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("1234")
        assert result == "12-34"

    def test_format_value_5_digit_ssn_with_dashes(self, permissive_config):
        """Test formatting 5-digit SSN with dashes."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("12345")
        assert result == "12-34-5"

    def test_format_value_7_digit_ssn_with_dashes(self, permissive_config):
        """Test formatting 7-digit SSN with dashes."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("1234567")
        assert result == "123-45-67"

    def test_format_value_8_digit_ssn_with_dashes(self, permissive_config):
        """Test formatting 8-digit SSN with dashes."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("12345678")
        assert result == "123-45-678"

    def test_format_value_2_digit_ssn_short_path(self, permissive_config):
        """Test formatting very short SSN (2 digits) - should take short path."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("12")
        assert result == "12"

    def test_format_value_3_digit_ssn_short_path(self, permissive_config):
        """Test formatting 3-digit SSN in short path."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("123")
        assert result == "123"

    def test_format_value_4_digit_ssn_short_path(self, permissive_config):
        """Test formatting 4-digit SSN in short path."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("1234")
        assert result == "12-34"

    def test_format_value_5_digit_ssn_short_path(self, permissive_config):
        """Test formatting 5-digit SSN in short path."""
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("12345")
        assert result == "12-34-5"

    def test_format_value_zero_padding_enabled_valid_length(self):
        """Test zero padding with valid 9-digit SSN."""
        config = SSNConfig(
            format_with_dashes=True,
            zero_pad=True,
            validate=False,  # Allow zero padding to work
            allow_invalid=True,
        )
        formatter = SSNFormatter(config)
        result = formatter.format_value("12345")
        # Should zero pad to 9 digits then format
        assert result == "000-01-2345"

    def test_format_value_zero_padding_disabled(self, permissive_config):
        """Test formatting without zero padding."""
        permissive_config.zero_pad = False
        formatter = SSNFormatter(permissive_config)
        result = formatter.format_value("12345")
        assert result == "12-34-5"  # No zero padding

    def test_inheritance_from_base_formatter(self, default_config):
        """Test that SSNFormatter inherits from BaseFormatter."""
        from forklift.utils.transformations.format.base import BaseFormatter

        formatter = SSNFormatter(default_config)
        assert isinstance(formatter, BaseFormatter)

    def test_inheritance_from_validation_mixin(self, default_config):
        """Test that SSNFormatter inherits from ValidationMixin."""
        from forklift.utils.transformations.format.base import ValidationMixin

        formatter = SSNFormatter(default_config)
        assert isinstance(formatter, ValidationMixin)

    def test_edge_case_all_zeros(self, default_config):
        """Test formatting SSN with all zeros."""
        formatter = SSNFormatter(default_config)
        result = formatter.format_value("000000000")
        assert result == "000-00-0000"

    def test_edge_case_leading_zeros(self, default_config):
        """Test formatting SSN with leading zeros."""
        formatter = SSNFormatter(default_config)
        result = formatter.format_value("001234567")
        assert result == "001-23-4567"
