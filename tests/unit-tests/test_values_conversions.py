"""_values: exact decimals, temporal parsing and comparison."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from forklift.processors._values import compare_temporal, parse_temporal, to_decimal


class TestToDecimal:
    def test_booleans_are_zero_and_one(self):
        assert to_decimal(True) == Decimal(1) and to_decimal(False) == Decimal(0)

    def test_an_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="unsupported numeric type list"):
            to_decimal([1])


class TestParseTemporal:
    def test_an_iso_looking_but_impossible_date_is_not_a_date(self):
        assert parse_temporal("2024-13-45") is None

    def test_an_iso_looking_value_can_still_match_a_format(self):
        assert parse_temporal("2024-31-01", ["%Y-%d-%m"]) == date(2024, 1, 31)


class TestCompareTemporal:
    def test_a_date_against_a_datetime_bound_compares_instants(self):
        bound = datetime(2024, 1, 1, 12, 0)

        assert compare_temporal(date(2024, 1, 1), bound) == -1
        assert compare_temporal(date(2024, 1, 2), bound) == 1

    def test_non_temporal_values_are_compared_as_they_are(self):
        assert compare_temporal(5, 3) == 1
        assert compare_temporal("a", "a") == 0
