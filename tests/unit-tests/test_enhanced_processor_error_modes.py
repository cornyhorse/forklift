"""EnhancedDataProcessor: schema failures in the fail modes, row-less violations, summaries."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.processors.bad_rows_handler import BadRowsConfig
from forklift.processors.base import ValidationResult
from forklift.processors.constraint_validator import (
    ConstraintConfig,
    ConstraintValidator,
    ConstraintViolation,
    ErrorMode,
)
from forklift.processors.enhanced_processor import EnhancedDataProcessor

REQUIRED_ID = pa.schema([pa.field("id", pa.int64(), nullable=False)])


def processor(tmp_path, **constraint_config):
    return EnhancedDataProcessor(
        REQUIRED_ID,
        constraint_config=ConstraintConfig(**constraint_config),
        bad_rows_config=BadRowsConfig(output_path=str(tmp_path / "bad_rows.parquet")),
    )


class BatchLevelCheck(ConstraintValidator):
    """A validator that reports one violation about the whole batch (no row, no name)."""

    def process_batch(self, batch):
        violation = ConstraintViolation(
            violation_type="batch",
            error_message="batch-level",
            columns=[],
            values=[],
            constraint_name="",
        )
        self.batch_violations = [violation]
        self.violations.append(violation)
        self.violation_count += 1
        result = ValidationResult(
            is_valid=False, error_message="batch-level", error_code="BATCH_VIOLATION"
        )
        return batch, [result]


class TestSchemaFailuresInFailModes:
    def test_fail_fast_raises_on_the_first_batch_with_a_bad_row(self, tmp_path):
        enhanced = processor(tmp_path, error_mode=ErrorMode.FAIL_FAST)

        with pytest.raises(ValueError, match=r"Schema validation failed for 1 row\(s\)$"):
            enhanced.process_batch(pa.RecordBatch.from_pydict({"id": [1, None]}))

    def test_fail_complete_keeps_the_rows_and_raises_from_finalize(self, tmp_path):
        enhanced = processor(tmp_path, error_mode=ErrorMode.FAIL_COMPLETE)

        kept, _ = enhanced.process_batch(pa.RecordBatch.from_pydict({"id": [1, None, None]}))

        assert kept.num_rows == 3
        with pytest.raises(ValueError) as error:
            enhanced.finalize()
        assert str(error.value) == "Schema validation failed for 2 row(s) (fail_complete mode)"
        assert not (tmp_path / "bad_rows.parquet").exists()


class TestViolationsWithoutARow:
    def test_a_batch_level_violation_rejects_no_row(self, tmp_path):
        enhanced = processor(tmp_path)
        enhanced.constraint_validator = BatchLevelCheck(enhanced.constraint_config)

        kept, results = enhanced.process_batch(pa.RecordBatch.from_pydict({"id": [None, 2]}))

        assert kept.column("id").to_pylist() == [2]  # only the schema failure is removed
        batch_level = [r for r in results if r.error_code == "BATCH_VIOLATION"]
        assert len(batch_level) == 1 and batch_level[0].row_index is None
        assert [row["row_index"] for row in enhanced.bad_rows_handler.bad_rows] == [0]

        summary = enhanced.get_constraint_violations_summary()
        assert summary["violation_types"] == {"batch": 1}
        assert summary["affected_constraints"] == []
        assert summary["sample_violations"][0]["row_index"] is None

        results = enhanced.finalize()
        assert pq.read_table(results["bad_rows_file"]).column("row_index").to_pylist() == [0]


class TestConstraintViolationsSummary:
    def test_samples_are_limited_to_five_per_type(self, tmp_path):
        enhanced = processor(tmp_path, unique_constraints=["id"])

        enhanced.process_batch(pa.RecordBatch.from_pydict({"id": [1] * 8}))

        summary = enhanced.get_constraint_violations_summary()
        assert summary["total_violations"] == 7
        assert summary["violation_types"] == {"unique": 7}
        assert summary["affected_constraints"] == ["id_unique"]
        assert [s["row_index"] for s in summary["sample_violations"]] == [1, 2, 3, 4, 5]
