import pytest
from forklift.inputs.csv_input import _pgsafe


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
    assert _pgsafe(raw) == expected


def test_pgsafe_em_dash_and_punctuation():
    # em dashes and punctuation become underscores then collapse
    assert _pgsafe("weird—dash—chars!!!") == "weird_dash_chars"


def test_pgsafe_max_length_cap():
    # Postgres identifier cap (we enforce 63)
    raw = "A" * 70
    out = _pgsafe(raw)
    assert out == "a" * 63
    assert len(out) == 63
