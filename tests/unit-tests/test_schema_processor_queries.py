"""Tests for SchemaProcessor's schema loading and configuration queries."""

import json

import pyarrow as pa
import pytest

from forklift.engine.config import ImportConfig
from forklift.engine.processors import SchemaProcessor
from forklift.io import UnifiedIOHandler


def _processor(tmp_path, schema=None):
    schema_file = None
    if schema is not None:
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(schema), encoding="utf-8")
    config = ImportConfig(
        input_path=tmp_path / "in.csv", output_path=tmp_path / "out", schema_file=schema_file
    )
    processor = SchemaProcessor(config, UnifiedIOHandler())
    processor.load_schema()
    return processor


class TestNullableTypeLists:
    @pytest.mark.parametrize(
        "json_type, expected",
        [
            (["integer", "null"], pa.int64()),
            (["null", "number"], pa.float64()),
            (["null"], pa.string()),
        ],
    )
    def test_non_null_entry_of_a_type_list_decides_the_type(self, tmp_path, json_type, expected):
        processor = _processor(tmp_path, {"properties": {"n": {"type": json_type}}})

        assert processor.schema.field("n").type == expected


class TestWithoutSchema:
    def test_queries_answer_with_empty_defaults(self, tmp_path):
        processor = _processor(tmp_path)

        assert processor.schema is None
        assert processor.get_required_columns() == []
        assert processor.get_metadata_config() == {}
        assert processor.has_row_hash_config() is False
        assert processor.get_row_hash_config() is None
        assert processor.get_column_names_from_schema() is None


class TestRowHashConfig:
    def test_row_hash_block_is_reported(self, tmp_path):
        row_hash = {"enabled": True, "columnName": "hash"}
        processor = _processor(tmp_path, {"properties": {"a": {}}, "x-rowHash": row_hash})

        assert processor.has_row_hash_config() is True
        assert processor.get_row_hash_config() == row_hash

    def test_schema_without_row_hash_block(self, tmp_path):
        processor = _processor(tmp_path, {"properties": {"a": {}}})

        assert processor.has_row_hash_config() is False
        assert processor.get_row_hash_config() is None
        assert processor.get_metadata_config() == {}
