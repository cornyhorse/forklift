import pytest
from forklift.utils.standardize import standardize_postgres_column_name


@pytest.mark.parametrize(
    "raw,expected",
    [
        # trims + lowercases + spaces→underscore
        ("  Total Revenue ($) 2024  ", "total_revenue_2024"),
        # collapse multiple separators to single underscore
        ("Name__With___Many____Underscores", "name_with_many_underscores"),
        # strip leading/trailing separators after cleanup
        ("__Leading--and--trailing__", "leading_and_trailing"),
        # non-ASCII letters are removed (become separators → underscores → collapsed)
        ("Ünicode Štring 你好", "nicode_tring"),
        # all-invalid becomes empty
        ("!!!", ""),
        ("", ""),
    ],
)
def test_pgsafe_core_cases(raw, expected):
    """
    Test standardize_postgres_column_name for a variety of input cases, including:
    - Trimming, lowercasing, and replacing spaces with underscores
    - Collapsing multiple separators
    - Removing non-ASCII characters
    - Handling empty and all-invalid strings
    Verifies that the output matches the expected PG-safe identifier.
    """
    assert standardize_postgres_column_name(raw) == expected


def test_pgsafe_em_dash_and_punctuation():
    """
    Test standardize_postgres_column_name for input containing em dashes and punctuation.
    Verifies that these are converted to underscores and collapsed.
    """
    assert standardize_postgres_column_name("weird—dash—chars!!!") == "weird_dash_chars"


def test_pgsafe_max_length_cap():
    """
    Test standardize_postgres_column_name for input exceeding the Postgres identifier length cap (63 chars).
    Verifies that the output is truncated to 63 characters and lowercased.
    """
    raw = "A" * 70
    out = standardize_postgres_column_name(raw)
    assert out == "a" * 63
    assert len(out) == 63
