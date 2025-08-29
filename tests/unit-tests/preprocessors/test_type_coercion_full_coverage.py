import pytest
from datetime import datetime
from decimal import Decimal

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
    # Comma should be removed by currency/sep regex
    assert tok == "1234.50" and neg is True
    assert _coerce_number("(123.4)") == -123.4
    assert _coerce_integer("(1,234.0)") == -1234
    # scale quantization + negative parentheses
    dec = _coerce_decimal("(12.345)", scale=2)
    assert dec == Decimal("-12.35")
    # no-scale path
    assert _coerce_decimal("12.34") == Decimal("12.34")
    with pytest.raises(ValueError):
        _strip_numeric_artifacts("")  # empty number
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

# ---- TypeCoercion.apply branch coverage ----

def test_type_coercion_apply_all_declared_types_and_paths():
    tc = TypeCoercion(types={
        "num": "number",
        "int": "integer",
        "d": {"type": "decimal", "scale": 2},
        "d2": {"type": "decimal"},  # non-string decimal no scale
        "b": "boolean",
        "s": "string",
        "dt": {"type": "string", "format": "date-time"},
        "date_only": {"type": "string", "format": "date"},
        "bin_hex": "binary",
        "bin_b64": "binary",
        "unknown": "not-supported",
    })
    now_iso = "2024-03-02T01:02:03Z"
    row = {
        "num": -1234.5,  # numeric (non-string) branch
        "int": -1234,    # integer (non-string) branch
        "d": "12.345",
        "d2": Decimal("1.2300"),  # decimal instance path
        "b": 1,  # boolean numeric branch
        "s": 123,  # non-string becomes str
        "dt": now_iso,
        "date_only": "2024/03/02",
        "bin_hex": "0x4869",
        "bin_b64": "SGk=",
        "unknown": "keep-me",
    }
    out = tc.apply(row)
    assert out["num"] == -1234.5
    assert out["int"] == -1234
    assert out["d"] == Decimal("12.35")
    assert out["d2"] == Decimal("1.2300")
    assert out["b"] is True
    assert out["s"] == "123"
    assert isinstance(out["dt"], datetime)
    assert out["date_only"] == "2024-03-02"
    assert out["bin_hex"] == b"Hi"
    assert out["bin_b64"] == b"Hi"
    assert out["unknown"] == "keep-me"


def test_decimal_non_string_input_quantization_and_error_branch():
    # Non-string decimal input gets quantized if scale meta present
    tc = TypeCoercion(types={"d": {"type": "decimal", "scale": 2}, "bad": {"type": "decimal"}})
    out = tc.apply({"d": 1.239, "bad": "1.2"})
    assert out["d"] == Decimal("1.24")
    # Trigger unsupported decimal value error path
    tc_err = TypeCoercion(types={"d": {"type": "decimal"}})
    with pytest.raises(ValueError) as exc:
        tc_err.apply({"d": [1,2,3]})  # list is unsupported
    assert "unsupported decimal value" in str(exc.value)


def test_binary_bytes_passthrough_and_boolean_error():
    tc = TypeCoercion(types={"bin": "binary", "flag": "boolean", "badbin": "binary"})
    out = tc.apply({"bin": b"Hi", "flag": "y", "badbin": "0x4869"})
    assert out["bin"] == b"Hi"
    assert out["flag"] is True
    # unsupported binary value (non-bytes, non-str)
    with pytest.raises(ValueError):
        tc.apply({"badbin": 123})
    with pytest.raises(ValueError):
        tc.apply({"flag": "???"})


def test_date_non_string_and_datetime_non_string_error():
    tc = TypeCoercion(types={"d": {"type": "string", "format": "date"}, "dt": {"type": "string", "format": "date-time"}})
    with pytest.raises(ValueError):
        tc.apply({"d": 20240302})  # non-string date
    with pytest.raises(ValueError):
        tc.apply({"dt": 123})  # unsupported datetime value


def test_process_wrapper_captures_error():
    tc = TypeCoercion(types={"i": "integer"})
    result = tc.process({"i": "abc"})  # not an int
    assert result["row"] == {"i": "abc"}
    assert isinstance(result["error"], Exception)


def test_null_tokens_and_blank_to_none():
    tc = TypeCoercion(types={"i": "integer"}, nulls={"i": ["N/A"]})
    out = tc.apply({"i": "N/A"})
    assert out["i"] is None
    out2 = tc.apply({"i": "   "})
    assert out2["i"] is None


def test_datetime_pass_through():
    import datetime as _dt
    dt_obj = _dt.datetime(2024, 3, 2, 1, 2, 3)
    tc = TypeCoercion(types={"dt": {"type": "string", "format": "date-time"}})
    out = tc.apply({"dt": dt_obj})
    assert out["dt"] is dt_obj  # pass-through branch

