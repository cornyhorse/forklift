"""``MetadataGenerator`` column statistics for unusual columns: infinite enum values,
extension types, all-null strings and Arrow kernels that fail."""

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from forklift.schema.processors.metadata import MetadataGenerator


class _Celsius(pa.ExtensionType):
    """Minimal extension type over float64 storage."""

    def __init__(self):
        super().__init__(pa.float64(), "forklift.test.celsius")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def _column_metadata(table, **config):
    metadata = MetadataGenerator().generate_metadata(table, config)
    return metadata, metadata["column_metadata"]


def _failing(*args, **kwargs):
    raise pa.ArrowInvalid("kernel failed on 'secret-value'")


class TestInfiniteEnumValues:
    def test_infinite_values_are_listed_as_text(self):
        table = pa.table({"reading": pa.array([float("inf")] * 25 + [1.0] * 5)})

        metadata, _ = _column_metadata(table, include_value_statistics=True)

        suggestion = metadata["enum_suggestions"]["reading"]
        assert suggestion["is_enum_candidate"] is True
        assert suggestion["suggested_enum_values"] == ["inf", 1.0]
        assert suggestion["recommendation"].endswith("with values: inf, 1.0")


class TestUnhashableColumns:
    def test_extension_column_reports_storage_type_without_distinct_values(self):
        storage = pa.array([1.5, 2.5, None])
        column = pa.ExtensionArray.from_storage(_Celsius(), storage)
        table = pa.table({"temperature": column})

        metadata, columns = _column_metadata(table)

        entry = columns["temperature"]
        assert entry["parquet_type"] == "double"
        assert entry["null_count"] == 1
        assert entry["non_null_count"] == 2
        assert "distinct_count" not in entry
        assert "uniqueness_ratio" not in entry
        assert metadata["enum_suggestions"] == {}

    def test_failed_distinct_count_leaves_out_uniqueness(self, monkeypatch):
        monkeypatch.setattr(pc, "count_distinct", _failing)
        table = pa.table({"code": pa.array(["a", "a", "b"])})

        metadata, columns = _column_metadata(table)

        assert "distinct_count" not in columns["code"]
        assert "uniqueness_ratio" not in columns["code"]
        assert columns["code"]["max_length"] == 1
        assert metadata["enum_suggestions"] == {}


class TestAllNullStringColumn:
    def test_lengths_are_unknown_and_every_value_counts_as_empty(self):
        table = pa.table({"comment": pa.array([None, None, None], pa.string())})

        _, columns = _column_metadata(table)

        entry = columns["comment"]
        assert entry["null_percentage"] == 100.0
        assert entry["min_length"] is None
        assert entry["max_length"] is None
        assert entry["avg_length"] is None
        assert entry["median_length"] is None
        assert entry["empty_strings"] == 3
        assert entry["contains_whitespace"] == 0
        assert entry["contains_numbers"] == 0
        assert entry["contains_special_chars"] == 0
        assert entry["non_ascii_count"] == 0


class TestFailingStatisticKernels:
    def test_string_statistics_failure_is_reported_without_data(self, monkeypatch):
        monkeypatch.setattr(pc, "utf8_length", _failing)
        table = pa.table({"name": pa.array(["Ann", "Bob"])})

        _, columns = _column_metadata(table)

        assert columns["name"]["error"] == "Failed to calculate string statistics (ArrowInvalid)"
        assert columns["name"]["distinct_count"] == 2

    def test_boolean_statistics_failure_is_reported_without_data(self, monkeypatch):
        monkeypatch.setattr(pc, "sum", _failing)
        table = pa.table({"active": pa.array([True, False, True])})

        _, columns = _column_metadata(table)

        assert columns["active"]["error"] == (
            "Failed to calculate boolean statistics (ArrowInvalid)"
        )
        assert "true_count" not in columns["active"]


@pytest.mark.parametrize("include_values", [False, True])
def test_enum_analysis_of_a_single_value_column(include_values):
    table = pa.table({"status": pa.array(["open"] * 40)})

    metadata, _ = _column_metadata(table, include_value_statistics=include_values)

    suggestion = metadata["enum_suggestions"]["status"]
    assert suggestion["distinct_count"] == 1
    assert suggestion["distribution_balance"] == "skewed"
