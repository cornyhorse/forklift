from datetime import datetime, timezone
from decimal import Decimal
import pytest

from forklift.preprocessors.type_coercion import TypeCoercion


def test_integer_and_decimal_and_boolean_and_null_tokens():
    tc = TypeCoercion(types={
        "qty": "integer",
        "price": {"type": "decimal", "scale": 2},
        "flag": {"type": "boolean"},
    }, nulls={"flag": ["NULL"]})
    row = {"qty": "(1,234)", "price": "12.345", "flag": "NULL"}
    out = tc.apply(row)
    assert out["qty"] == -1234
    assert isinstance(out["price"], Decimal)
    assert out["price"] == Decimal("12.35")
    assert out["flag"] is None


def test_datetime_and_binary_hex_and_base64():
    tc = TypeCoercion(types={
        "ts": {"type": "string", "format": "date-time"},
        "blob_hex": {"type": "binary"},
        "blob_b64": {"type": "binary"},
    })
    row = {"ts": "2024-03-01T12:34:56Z", "blob_hex": "0x4869", "blob_b64": "SGk="}
    out = tc.apply(row)
    assert isinstance(out["ts"], datetime)
    # Compare normalized ISO (allow timezone aware vs naive if tz parsing differs)
    assert out["ts"].replace(tzinfo=timezone.utc).isoformat().startswith("2024-03-01T12:34:56")
    assert out["blob_hex"] == b"Hi"
    assert out["blob_b64"] == b"Hi"


def test_binary_invalid_raises():
    tc = TypeCoercion(types={"data": "binary"})
    with pytest.raises(ValueError):
        tc.apply({"data": "not-hex-or-b64"})
