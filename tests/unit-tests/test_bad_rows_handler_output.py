"""BadRowsHandler: grouping of a batch's errors per row and the flattened file layouts."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.csv as pv_csv
import pyarrow.parquet as pq

from forklift.processors.bad_rows_handler import BadRowsConfig, BadRowsHandler
from forklift.processors.base import ValidationResult
from forklift.processors.constraint_validator import ConstraintViolation


def failure(row, code, column="id"):
    return ValidationResult(
        is_valid=False,
        error_message=f"{code} message",
        error_code=code,
        row_index=row,
        column_name=column,
    )


def unique_violation(row):
    return ConstraintViolation(
        violation_type="unique",
        error_message="duplicate key",
        columns=["id"],
        values=[],
        constraint_name="uq_id",
        row_index=row,
    )


class TestAddBadRow:
    def test_passed_results_are_not_reported_as_errors(self):
        handler = BadRowsHandler(BadRowsConfig())
        passed = ValidationResult(is_valid=True, error_code="OK", row_index=0)

        handler.add_bad_row({"id": 1}, 0, validation_results=[passed, failure(0, "BAD")])

        [row] = handler.bad_rows
        assert [error["error_code"] for error in row["errors"]] == ["BAD"]
        assert [result.error_code for result in handler.validation_errors] == ["BAD"]


class TestAddBadRowsFromBatch:
    def test_errors_and_violations_are_grouped_by_row(self):
        handler = BadRowsHandler(BadRowsConfig())
        handler.increment_row_count(10)  # rows of earlier batches
        batch = pa.RecordBatch.from_pydict({"id": [1, 1, None]})
        results = [
            failure(1, "FIRST"),
            failure(1, "SECOND"),
            ValidationResult(is_valid=False, error_code="BATCH_LEVEL"),  # no row
            ValidationResult(is_valid=True, row_index=2),
        ]

        handler.add_bad_rows_from_batch(
            batch, [1, 2], results, [unique_violation(1), unique_violation(1)]
        )

        first, second = handler.bad_rows
        assert first["row_index"] == 11 and first["original_data"] == {"id": 1}
        assert [e.get("error_code") or e["violation_type"] for e in first["errors"]] == [
            "FIRST",
            "SECOND",
            "unique",
            "unique",
        ]
        assert second["row_index"] == 12 and second["original_data"] == {"id": None}
        assert second["errors"] == []
        assert handler.get_summary()["constraint_violations"] == {"unique": 2}


class TestFlattenedLayouts:
    def make_handler(self, tmp_path, **config):
        handler = BadRowsHandler(
            BadRowsConfig(output_path=str(tmp_path / "bad"), create_summary=False, **config)
        )
        handler.add_bad_row(
            {"id": 7, "name": "=cmd"},
            3,
            validation_results=[failure(3, "TYPE")],
            constraint_violations=[unique_violation(3)],
        )
        return handler

    def test_parquet_without_original_data_has_only_the_error_columns(self, tmp_path):
        handler = self.make_handler(tmp_path, output_format="parquet", include_original_data=False)

        table = pq.read_table(handler.write_bad_rows())

        assert table.column_names == [
            "row_index",
            "timestamp",
            "error_messages",
            "error_codes",
            "error_types",
        ]
        row = table.to_pylist()[0]
        assert row["error_codes"] == "TYPE; unique"
        assert row["error_types"] == "validation_error; constraint_violation"

    def test_parquet_without_error_details_has_only_the_data(self, tmp_path):
        handler = self.make_handler(tmp_path, output_format="parquet", include_error_details=False)

        table = pq.read_table(handler.write_bad_rows())

        assert table.to_pylist()[0] == {
            "row_index": 3,
            "timestamp": table.column("timestamp")[0].as_py(),
            "original_id": 7,
            "original_name": "=cmd",
        }

    def test_csv_carries_the_messages_and_codes_of_every_error(self, tmp_path):
        handler = self.make_handler(tmp_path, output_format="csv")

        table = pv_csv.read_csv(handler.write_bad_rows())

        row = table.to_pylist()[0]
        assert row["original_name"] == "'=cmd"  # formula protection
        assert row["error_messages"] == "TYPE message; duplicate key"
        assert row["error_codes"] == "TYPE; unique"
        assert "error_types" not in table.column_names

    def test_csv_without_original_data_or_error_details(self, tmp_path):
        handler = self.make_handler(
            tmp_path, output_format="csv", include_original_data=False, include_error_details=False
        )

        table = pv_csv.read_csv(handler.write_bad_rows())

        assert table.column_names == ["row_index", "timestamp"]
        assert table.column("row_index").to_pylist() == [3]
