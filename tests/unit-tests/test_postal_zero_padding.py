"""Zero padding of ``transformations.format.postal.ZipCodeFormatter`` without validation."""

import pytest

from forklift.utils.transformations.configs import ZipCodeConfig
from forklift.utils.transformations.format.postal import ZipCodeFormatter


def format_zip(value, **options):
    return ZipCodeFormatter(ZipCodeConfig(**options)).format_value(value)


class TestZip9Padding:
    def test_short_zip9_is_padded_to_nine_digits(self):
        assert format_zip("1234567", zip_type="zip-9", validate=False) == "00123-4567"

    def test_short_zip9_is_kept_without_zero_pad(self):
        assert format_zip("1234567", zip_type="zip-9", validate=False, zero_pad=False) == (
            "1234567"
        )


class TestPermissivePadding:
    def test_short_code_is_kept_without_zero_pad(self):
        assert format_zip("2134", validate=False, zero_pad=False) == "2134"

    @pytest.mark.parametrize(("value", "expected"), [("2134", "02134"), ("1234567", "00123-4567")])
    def test_short_code_is_padded_to_the_next_valid_length(self, value, expected):
        assert format_zip(value, validate=False) == expected

    def test_code_longer_than_nine_digits_is_left_alone(self):
        assert format_zip("1234567890", validate=False) == "1234567890"
