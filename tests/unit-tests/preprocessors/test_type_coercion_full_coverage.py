import pytest
from datetime import datetime
from decimal import Decimal
import polars as pl

from forklift.preprocessors.type_coercion import (
    TypeCoercion,
    _coerce_bool,
    _coerce_number,
    _coerce_integer,
    _coerce_decimal,
    _coerce_binary,
    _normalize_type,
    _strip_numeric_artifacts,
)

# ---- helper function coverage ----

def test_coerce_bool_variants_and_error():
    assert _coerce_bool(True) is True
    assert _coerce_bool(False) is False
    assert _coerce_bool(1) is True  # numeric branch
    assert _coerce_bool(0.0) is False  # numeric branch
    assert _coerce_bool("YeS") is True
    assert _coerce_bool("no") is False
    with pytest.raises(ValueError):
        _coerce_bool("maybe")

def test_strip_numeric_and_number_integer_decimal_paths():
    tok, neg = _strip_numeric_artifacts("(1,234.50)")
    assert tok == "1234.50" and neg is True
    assert _coerce_number("(123.4)") == -123.4
    assert _coerce_integer("(1,234.0)") == -1234
    dec = _coerce_decimal("(12.345)", scale=2)
    assert dec == Decimal("-12.35")
    assert _coerce_decimal("12.34") == Decimal("12.34")
    with pytest.raises(ValueError):
        _strip_numeric_artifacts("")
    with pytest.raises(ValueError):
        _coerce_decimal("not-a-decimal")

def test_coerce_binary_all_paths():
    assert _coerce_binary("0x4869") == b"Hi"
    assert _coerce_binary("SGk=") == b"Hi"
    with pytest.raises(ValueError):
        _coerce_binary("bad@@@")
    with pytest.raises(ValueError):
        _coerce_binary("")

# ---- _normalize_type coverage ----

def test_normalize_type_string_and_dict_variants():
    assert _normalize_type(None) == (None, {})
    assert _normalize_type("float")[0] == "number"
    assert _normalize_type("DOUBLE")[0] == "number"
    assert _normalize_type("timestamp")[0] == "datetime"
    assert _normalize_type("boolean")[0] == "boolean"
    assert _normalize_type("unknown") == (None, {})
    t, meta = _normalize_type({"type": "decimal", "scale": 3, "precision": 10})
    assert t == "decimal" and meta["scale"] == 3 and meta["precision"] == 10
    assert _normalize_type({"type": "string", "format": "date"})[0] == "date"
    assert _normalize_type({"type": "string", "format": "date-time"})[0] == "datetime"
    assert _normalize_type({"type": "binary"})[0] == "binary"
    assert _normalize_type({"type": "string"})[0] == "string"
    assert _normalize_type({"type": "float"})[0] == "number"
    assert _normalize_type({"type": "integer"})[0] == "integer"
    assert _normalize_type({"type": "weird"}) == (None, {})

# ---- TypeCoercion vectorized coverage ----

def test_type_coercion_vectorized_all_declared_types_and_paths():
    tc = TypeCoercion(types={
        "num": "number",
        "int": "integer",
        "d": {"type": "decimal", "scale": 2},
        "d2": {"type": "decimal"},
        "b": "boolean",
        "s": "string",
        "dt": {"type": "string", "format": "date-time"},
        "date_only": {"type": "string", "format": "date"},
        "bin_hex": "binary",
        "bin_b64": "binary",
        "unknown": "not-supported",
    })
    now_iso = "2024-03-02T01:02:03Z"
    df = pl.DataFrame([
        {
            "num": -1234.5,
            "int": -1234,
            "d": "12.345",
            "d2": Decimal("1.2300"),
            "b": 1,
            "s": 123,
            "dt": now_iso,
            "date_only": "2024/03/02",
            "bin_hex": "0x4869",
            "bin_b64": "SGk=",
            "unknown": "keep-me",
        }
    ])
    out = tc.process_dataframe(df)
    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["num"] == -1234.5
    assert row["int"] == -1234
    assert row["d"] == Decimal("12.35")
    assert row["d2"] == Decimal("1.2300")
    assert row["b"] is True
    assert row["s"] == "123"
    assert isinstance(row["dt"], datetime)
    assert row["date_only"].isoformat() == "2024-03-02"
    assert row["bin_hex"] == b"Hi"
    assert row["bin_b64"] == b"Hi"
    assert row["unknown"] == "keep-me"

def test_decimal_quantization_and_invalid_row_filtered():
    tc = TypeCoercion(types={"d": {"type": "decimal", "scale": 2}, "bad": {"type": "decimal"}})
    df = pl.DataFrame([
        {"d": 1.239, "bad": "1.2"},  # good row
    ])
    out = tc.process_dataframe(df)
    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["d"] == Decimal("1.24")
    assert row["bad"] == Decimal("1.2")
    # Invalid decimal row
    df_err = pl.DataFrame([
        {"d": [1,2,3]},
    ])
    out_err = tc.process_dataframe(df_err)
    assert out_err.height == 0
    assert len(tc._df_errors) >= 1


def test_binary_passthrough_and_boolean_invalid_filtered():
    tc = TypeCoercion(types={"bin": "binary", "flag": "boolean", "badbin": "binary"})
    df = pl.DataFrame([
        {"bin": b"Hi", "flag": "y", "badbin": "0x4869"},          # good row
        {"bin": b"Hi", "flag": "???", "badbin": 123},               # bad boolean & binary
    ])
    out = tc.process_dataframe(df)
    # One good row retained
    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["bin"] == b"Hi"
    assert row["flag"] is True
    # Errors recorded
    assert len(tc._df_errors) >= 1


def test_date_and_datetime_invalid_filtered():
    tc = TypeCoercion(types={"d": {"type": "string", "format": "date"}, "dt": {"type": "string", "format": "date-time"}})
    df = pl.DataFrame([
        {"d": 20240302},  # invalid date (non-string)
        {"dt": 123},      # invalid datetime
    ])
    out = tc.process_dataframe(df)
    assert out.height == 0
    assert len(tc._df_errors) == 2


def test_null_tokens_and_blank_to_none_vectorized():
    tc = TypeCoercion(types={"i": "integer"}, nulls={"i": ["N/A"]})
    df = pl.DataFrame([
        {"i": "N/A"},
        {"i": "   "},
    ])
    out = tc.process_dataframe(df)
    assert out.height == 2
    vals = [r["i"] for r in out.to_dicts()]
    assert vals == [None, None]


def test_datetime_pass_through_vectorized():
    import datetime as _dt
    dt_obj = _dt.datetime(2024, 3, 2, 1, 2, 3)
    tc = TypeCoercion(types={"dt": {"type": "string", "format": "date-time"}})
    df = pl.DataFrame([
        {"dt": dt_obj}
    ])
    out = tc.process_dataframe(df)
    assert out.height == 1
    assert out.to_dicts()[0]["dt"] == dt_obj
