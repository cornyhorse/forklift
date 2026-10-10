"""Write-time validator: the basic/strict factories and the large-string check."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from forklift.processors.write_time_validator import (
    create_basic_write_validator,
    create_strict_write_validator,
)


def codes(results):
    return [r.error_code for r in results if not r.is_valid]


class TestBasicWriteValidator:
    def test_with_a_primary_key_it_checks_nulls_and_duplicates(self):
        validator = create_basic_write_validator(["id"])
        batch = pa.RecordBatch.from_pydict({"id": [1, 1, None]})

        _, results = validator.process_batch(batch)

        assert validator.config.primary_key_columns == ["id"]
        assert validator.config.max_null_percentage == 90.0
        assert codes(results) == ["NULL_PRIMARY_KEY", "DUPLICATE_PRIMARY_KEYS"]
        assert codes(validator.finalize()) == []

    def test_without_a_primary_key_only_the_row_count_is_judged(self):
        validator = create_basic_write_validator()

        _, results = validator.process_batch(pa.RecordBatch.from_pydict({"id": [1, 1]}))

        assert validator.config.primary_key_columns == []
        assert codes(results) == []
        assert codes(create_basic_write_validator().finalize()) == ["EMPTY_TABLE"]


class TestStrictWriteValidator:
    def test_it_enforces_schema_required_columns_and_null_share(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        validator = create_strict_write_validator(
            ["id"], required_columns=["id", "name"], expected_schema=schema
        )
        batch = pa.RecordBatch.from_pydict({"id": [1, 2], "name": ["a", None]}, schema=schema)

        _, results = validator.process_batch(batch)

        assert codes(results) == ["EXCESSIVE_NULLS"]  # 50% NULL > 10%
        assert results[0].column_name == "name"

    def test_without_arguments_it_does_not_compare_schemas(self):
        validator = create_strict_write_validator()

        assert validator.config.expected_schema is None
        assert validator.config.fail_on_schema_mismatch is False
        assert validator.config.check_null_percentages is True


class TestLargeStringCheck:
    def test_a_failing_length_computation_skips_the_check(self, monkeypatch):
        validator = create_basic_write_validator()

        def fail(_column):
            raise pa.ArrowInvalid("cannot compute")

        monkeypatch.setattr(pc, "utf8_length", fail)
        _, results = validator.process_batch(pa.RecordBatch.from_pydict({"s": ["x" * 10]}))

        assert results == []
