"""FwfInputHandler: delegating helpers and the Arrow table built from parsed records."""

from __future__ import annotations

import decimal

import pyarrow as pa
import pytest

from forklift.inputs.config import FwfConditionalSchema, FwfFieldSpec, FwfInputConfig
from forklift.inputs.fwf import FwfInputHandler


@pytest.fixture
def two_record_types():
    """Record types A and B share field names but declare different types for them."""
    typed = FwfConditionalSchema(
        flag_value="A",
        description="typed",
        fields=[
            FwfFieldSpec("price", 2, 6, parquet_type="decimal(8,2)"),
            FwfFieldSpec("qty", 8, 5, parquet_type="int64"),
        ],
    )
    text = FwfConditionalSchema(
        flag_value="B",
        description="text",
        fields=[FwfFieldSpec("price", 2, 6), FwfFieldSpec("qty", 8, 5)],
    )
    flag = FwfFieldSpec("kind", 1, 1)
    return FwfInputHandler(FwfInputConfig(conditional_schemas=[typed, text], flag_column=flag))


class TestDelegatingHelpers:
    def test_detect_conditional_schema_returns_the_schema_for_the_flag(self, two_record_types):
        assert two_record_types.detect_conditional_schema("B   n/a hello").description == "text"
        assert two_record_types.detect_conditional_schema("Z") is None

    def test_is_blank_line_is_true_only_for_whitespace(self, two_record_types):
        assert two_record_types.is_blank_line(" \t ") is True
        assert two_record_types.is_blank_line("  x ") is False


class TestArrowTable:
    def test_values_of_a_text_record_that_do_not_fit_the_first_declared_type_become_null(
        self, two_record_types, tmp_path
    ):
        path = tmp_path / "mixed.txt"
        path.write_text("A12.50 00042\nBn/a   hello\n", encoding="utf-8")

        table = two_record_types.create_arrow_table(path)

        # The first declaration of a name decides its column type
        assert table.schema.field("price").type == pa.decimal128(8, 2)
        assert table.schema.field("qty").type == pa.int64()
        assert table["kind"].to_pylist() == ["A", "B"]
        assert table["price"].to_pylist() == [decimal.Decimal("12.50"), None]
        assert table["qty"].to_pylist() == [42, None]

    def test_narrow_integer_fields_keep_their_declared_type(self, tmp_path):
        path = tmp_path / "small.txt"
        path.write_text("  7\n 12\n", encoding="utf-8")
        config = FwfInputConfig(fields=[FwfFieldSpec("n", 1, 3, parquet_type="int32")])

        table = FwfInputHandler(config).create_arrow_table(path)

        assert table.schema.field("n").type == pa.int32()
        assert table["n"].to_pylist() == [7, 12]
