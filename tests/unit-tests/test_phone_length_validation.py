"""Digit-count validation of ``transformations.format.phone.PhoneNumberFormatter``."""

import pytest

from forklift.utils.transformations.configs import PhoneNumberConfig
from forklift.utils.transformations.format.phone import PhoneNumberFormatter


class TestDigitCountLimits:
    def test_number_with_nanp_country_code_passes_a_stricter_minimum(self):
        # min_digits counts the national number (10 digits here); the leading "1" is accepted
        formatter = PhoneNumberFormatter(PhoneNumberConfig(min_digits=11))

        assert formatter.format_value("1-555-123-4567") == "1(555) 123-4567"

    def test_number_without_country_code_fails_a_stricter_minimum(self):
        formatter = PhoneNumberFormatter(PhoneNumberConfig(min_digits=11))

        with pytest.raises(ValueError, match="Phone number must have 11-11 digits, got 10"):
            formatter.format_value("555-123-4567")

    def test_number_above_the_maximum_is_rejected(self):
        formatter = PhoneNumberFormatter(PhoneNumberConfig())

        with pytest.raises(ValueError, match="Phone number must have 10-11 digits, got 12"):
            formatter.format_value("555-123-4567-89")

    def test_short_number_is_accepted_with_a_lower_minimum(self):
        formatter = PhoneNumberFormatter(PhoneNumberConfig(min_digits=7))

        # Not ten digits: the US layout does not apply, the digits are returned as they are
        assert formatter.format_value("555-1234") == "5551234"


class TestWithoutValidation:
    @pytest.mark.parametrize("value", ["12345", "123-45"])
    def test_short_number_is_returned_as_digits_in_us_style(self, value):
        formatter = PhoneNumberFormatter(PhoneNumberConfig(validate=False))

        assert formatter.format_value(value) == "12345"
