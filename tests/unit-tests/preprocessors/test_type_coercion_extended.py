import polars as pl
from datetime import datetime, timezone
from decimal import Decimal
import pytest

from forklift.preprocessors.type_coercion import TypeCoercion


def test_integer_decimal_boolean_null_tokens_vectorized():
    tc = TypeCoercion(types={
        "qty": "integer",
        "price": {"type": "decimal", "scale": 2},
        "flag": {"type": "boolean"},
    }, nulls={"flag": ["NULL"]})
    df = pl.DataFrame([
        {"qty": "(1,234)", "price": "12.345", "flag": "NULL"},
    ])
    out = tc.process_dataframe(df)
    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["qty"] == -1234
    assert isinstance(row["price"], Decimal)
    assert row["price"] == Decimal("12.35")
    assert row["flag"] is None


def test_datetime_and_binary_hex_and_base64_vectorized():
    tc = TypeCoercion(types={
        "ts": {"type": "string", "format": "date-time"},
        "blob_hex": {"type": "binary"},
        "blob_b64": {"type": "binary"},
    })
    df = pl.DataFrame([
        {"ts": "2024-03-01T12:34:56Z", "blob_hex": "0x4869", "blob_b64": "SGk="}
    ])
    out = tc.process_dataframe(df)
    assert out.height == 1
    row = out.to_dicts()[0]
    assert isinstance(row["ts"], datetime)
    assert row["ts"].replace(tzinfo=timezone.utc).isoformat().startswith("2024-03-01T12:34:56")
    assert row["blob_hex"] == b"Hi"
    assert row["blob_b64"] == b"Hi"


def test_binary_invalid_row_filtered():
    tc = TypeCoercion(types={"data": "binary"})
    df = pl.DataFrame([
        {"data": "not-hex-or-b64"}
    ])
    out = tc.process_dataframe(df)
    assert out.height == 0
    assert len(tc._df_errors) == 1
    assert "bad binary" in str(tc._df_errors[0][1])
