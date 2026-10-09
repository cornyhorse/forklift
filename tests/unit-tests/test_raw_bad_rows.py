"""Rejected rows are written as the input file had them, whichever stage rejected them."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift import import_csv
from forklift.engine.processors.type_conversion import (
    ColumnConverter,
    has_raw_columns,
    raw_rows,
    with_raw_columns,
)
from forklift.processors.row_hash import RowHashConfig, RowHashProcessor


def run(tmp_path, csv_text, schema, **kwargs):
    source = tmp_path / "in.csv"
    source.write_text(csv_text)
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(schema))
    out = tmp_path / "out"
    results = import_csv(
        input_path=str(source), output_path=str(out), schema_file=str(schema_file), **kwargs
    )
    return results, out


def bad_rows(out):
    return pq.read_table(out / "bad_rows.parquet").to_pylist()


PROPERTIES = {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "amount": {"type": "number"},
    "ssn": {"type": "string", "x-special-type": "ssn"},
}
CLEANING = {
    "column_transformations": {
        "name": {"string_cleaning": {"enabled": True, "case_transform": "title"}},
    }
}


class TestEveryRejectPathShowsTheFile:
    def test_validation_and_constraints(self, tmp_path):
        schema = {
            "properties": PROPERTIES,
            "x-transformations": CLEANING,
            "x-primaryKey": {"columns": ["id"]},
        }
        csv = "id,name,amount,ssn\n1,  ann  lee ,77000.00,123456789\n1,bob ray,5.50,987654321\n"

        _, out = run(tmp_path, csv, schema)

        (row,) = bad_rows(out)
        assert row["name"] == "bob ray"  # not "Bob Ray"
        assert row["amount"] == "5.50"  # not "5.5"
        assert row["ssn"] == "987654321"  # not "987-65-4321"
        assert row["_rejection_reason"].startswith("UNIQUE_VIOLATION")

    def test_a_value_that_does_not_convert(self, tmp_path):
        schema = {"properties": PROPERTIES, "x-transformations": CLEANING}
        csv = "id,name,amount,ssn\n1,ann,1.0,123456789\n2,  bob ray ,abc,987654321\n"

        results, out = run(tmp_path, csv, schema)

        (row,) = bad_rows(out)
        assert (row["id"], row["name"], row["amount"], row["ssn"]) == (
            "2",
            "  bob ray ",
            "abc",
            "987654321",
        )
        assert results.invalid_rows == 1

    def test_a_missing_required_value(self, tmp_path):
        schema = {"properties": PROPERTIES, "required": ["name"]}
        csv = "id,name,amount,ssn\n1,,007.50,123456789\n2,bob,1.0,987654321\n"

        _, out = run(tmp_path, csv, schema)

        (row,) = bad_rows(out)
        assert (row["id"], row["name"], row["amount"], row["ssn"]) == (
            "1",
            "",
            "007.50",
            "123456789",
        )

    def test_null_markers_show_as_the_marker_the_file_had(self, tmp_path):
        schema = {
            "properties": PROPERTIES,
            "x-csv": {"nulls": {"global": ["NA", "-"]}},
            "required": ["name"],
        }
        csv = "id,name,amount,ssn\n1,NA,-,123456789\n2,bob,1.0,987654321\n"

        _, out = run(tmp_path, csv, schema)

        (row,) = bad_rows(out)
        assert (row["name"], row["amount"]) == ("NA", "-")  # not empty / null

    def test_without_the_extensions(self, tmp_path):
        schema = {"properties": PROPERTIES, "required": ["name"]}
        csv = "id,name,amount,ssn\n1,,77000.00,123456789\n2,bob,x,987654321\n"

        _, out = run(tmp_path, csv, schema, apply_schema_extensions=False)

        rows = {r["id"]: r for r in bad_rows(out)}
        assert rows["1"]["amount"] == "77000.00" and rows["2"]["amount"] == "x"

    @pytest.mark.parametrize("batch_size", [1, 2, 100])
    def test_rows_from_every_batch(self, tmp_path, batch_size):
        schema = {"properties": PROPERTIES, "x-primaryKey": {"columns": ["id"]}}
        lines = [f"{i % 3},n{i},{i}.10,1" for i in range(12)]

        results, out = run(
            tmp_path,
            "id,name,amount,ssn\n" + "\n".join(lines) + "\n",
            schema,
            batch_size=batch_size,
        )

        shown = sorted(r["amount"] for r in bad_rows(out))
        assert shown == sorted(f"{i}.10" for i in range(3, 12))
        assert results.invalid_rows == 9

    def test_rows_with_too_many_fields_are_unchanged(self, tmp_path):
        schema = {"properties": PROPERTIES}
        csv = "id,name,amount,ssn\n1,ann,1.0,123456789\n2,bob,2.0,987654321,extra\n"

        _, out = run(tmp_path, csv, schema, excess_column_mode="reject")

        (row,) = bad_rows(out)
        assert (row["id"], row["name"]) == ("2", "bob")


class TestNothingElseChanges:
    def test_no_hidden_column_reaches_a_file(self, tmp_path):
        schema = {"properties": PROPERTIES, "required": ["name"], "x-transformations": CLEANING}
        csv = "id,name,amount,ssn\n1,,1.0,123456789\n2,bob,2.0,987654321\n"

        _, out = run(tmp_path, csv, schema)

        for name in ("data.parquet", "bad_rows.parquet"):
            names = pq.read_table(out / name).schema.names
            assert not [n for n in names if n.startswith("__forklift")], names
        assert pq.read_table(out / "data.parquet").schema.names == ["id", "name", "amount", "ssn"]

    def test_an_empty_output_has_the_schema_without_hidden_columns(self, tmp_path):
        schema = {"properties": PROPERTIES, "x-transformations": CLEANING}

        _, out = run(tmp_path, "id,name,amount,ssn\n", schema)

        assert pq.read_table(out / "data.parquet").schema.names == ["id", "name", "amount", "ssn"]

    def test_the_input_hash_covers_the_files_own_columns_only(self, tmp_path):
        schema = {
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
            "x-transformations": {
                "column_transformations": {
                    "name": {"string_cleaning": {"enabled": True, "case_transform": "upper"}}
                }
            },
            "x-rowHash": {"inputHashEnabled": True},
        }

        _, out = run(tmp_path, "id,name\n1,ann\n2,bob\n", schema)

        expected = RowHashProcessor(RowHashConfig(input_hash_enabled=True)).compute_input_hash(
            pa.RecordBatch.from_pydict({"id": ["1", "2"], "name": ["ann", "bob"]})
        )
        assert pq.read_table(out / "data.parquet").column("_input_hash").to_pylist() == (
            expected.to_pylist()
        )

    def test_good_rows_are_unaffected(self, tmp_path):
        schema = {"properties": PROPERTIES, "x-transformations": CLEANING}

        _, out = run(tmp_path, "id,name,amount,ssn\n1,  ann  lee ,7.50,123456789\n", schema)

        row = pq.read_table(out / "data.parquet").to_pylist()[0]
        assert row == {"id": 1, "name": "Ann Lee", "amount": 7.5, "ssn": "123-45-6789"}


class TestHelpers:
    def batch(self):
        return pa.RecordBatch.from_pydict({"a": ["1", "x"], "b": ["p", "q"]})

    def test_raw_copies_share_the_data_and_are_hidden(self):
        wrapped = with_raw_columns(self.batch())

        assert has_raw_columns(wrapped) and wrapped.num_columns == 4
        assert wrapped.schema.names[:2] == ["a", "b"]
        assert wrapped.column(2).buffers() == wrapped.column(0).buffers()  # no copy

    def test_raw_rows_returns_the_copies_in_the_shape_of_the_file(self):
        converter = ColumnConverter({"a": pa.int64()})
        converted, rejected = converter.convert(
            with_raw_columns(pa.RecordBatch.from_pydict({"a": ["1", "x"], "b": ["p", "q"]}))
        )

        assert converted.column("a").to_pylist() == [1]
        assert rejected.to_pydict() == {"a": ["x"], "b": ["q"]}
        assert rejected.schema.names == ["a", "b"] and set(map(str, rejected.schema.types)) == {
            "string"
        }

    def test_raw_rows_without_copies_casts_the_typed_values(self):
        typed = pa.RecordBatch.from_pydict({"a": [1, 2], "b": ["p", "q"]})

        assert raw_rows(typed).to_pydict() == {"a": ["1", "2"], "b": ["p", "q"]}

    def test_duplicate_or_reserved_names_fall_back_to_the_typed_values(self):
        duplicate = pa.RecordBatch.from_arrays(
            [pa.array(["1"]), pa.array(["2"])], names=["a", "a"]
        )
        reserved = pa.RecordBatch.from_pydict({"__forklift_x": ["1"]})

        assert not has_raw_columns(with_raw_columns(duplicate))
        assert not has_raw_columns(with_raw_columns(reserved))

    def test_the_converter_leaves_hidden_columns_alone(self):
        converter = ColumnConverter({"a": pa.int64()}, None)
        batch = with_raw_columns(pa.RecordBatch.from_pydict({"a": ["7"]}))

        converted, _ = converter.convert(batch)

        assert converted.schema.field("a").type == pa.int64()
        assert converted.column(1).type == pa.string() and converted.column(1).to_pylist() == ["7"]
