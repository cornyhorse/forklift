"""Tests for type parsing and string conversion in forklift.engine.processors.type_conversion."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.engine.forklift_core import import_csv
from forklift.engine.processors.type_conversion import (
    ColumnConverter,
    csv_target_type,
    parse_arrow_type,
    to_string_batch,
)


class TestParseArrowTypeRejects:
    @pytest.mark.parametrize("value", [None, 32, ["int32"]])
    def test_non_text_type_names_are_not_understood(self, value):
        assert parse_arrow_type(value) is None

    def test_decimal_precision_arrow_cannot_hold(self):
        assert parse_arrow_type("decimal128(50, 2)") is None

    @pytest.mark.parametrize(
        "name",
        [
            "dictionary<values=money, indices=int32>",
            "dictionary<values=string, indices=bigint>",
        ],
    )
    def test_dictionary_with_unknown_member_types(self, name):
        assert parse_arrow_type(name) is None

    @pytest.mark.parametrize("index", ["float64", "string", "bool"])
    def test_dictionary_with_non_integer_indices(self, index):
        assert parse_arrow_type(f"dictionary<values=string, indices={index}>") is None

    def test_import_keeps_text_for_a_dictionary_type_with_non_integer_indices(self, tmp_path):
        schema = {
            "type": "object",
            "properties": {"code": {"type": "string"}},
            "x-csv": {"parquetTypeMapping": {"code": "dictionary<values=string, indices=double>"}},
        }
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(schema))
        csv_path = tmp_path / "in.csv"
        csv_path.write_text("code\nA1\nB2\n")

        results = import_csv(csv_path, tmp_path / "out", schema_file=schema_file)

        table = pq.read_table(tmp_path / "out" / "data.parquet")
        assert results.valid_rows == 2
        assert table.schema.field("code").type == pa.string()
        assert table.column("code").to_pylist() == ["A1", "B2"]


class TestCsvTargetType:
    def test_unknown_type_is_read_as_text(self):
        assert csv_target_type(None) == pa.string()


class TestConvertAgainstEstablishedSchema:
    def test_column_missing_from_the_established_schema_keeps_its_type(self):
        batch = pa.RecordBatch.from_pydict(
            {"known": pa.array(["1", "2"]), "extra": pa.array(["x", "y"])}
        )
        established = pa.schema([("known", pa.int64())])

        converted, rejected = ColumnConverter().convert(batch, established)

        assert rejected is None
        assert converted.schema == pa.schema([("known", pa.int64()), ("extra", pa.string())])
        assert converted.to_pydict() == {"known": [1, 2], "extra": ["x", "y"]}


class TestToStringBatch:
    def test_values_arrow_cannot_cast_to_text_are_written_with_str(self):
        batch = pa.RecordBatch.from_pydict(
            {"tags": pa.array([[1, 2], None, []]), "n": pa.array([1, None, 3])}
        )

        result = to_string_batch(batch)

        assert result.schema == pa.schema([("tags", pa.string()), ("n", pa.string())])
        assert result.to_pydict() == {"tags": ["[1, 2]", None, "[]"], "n": ["1", None, "3"]}
