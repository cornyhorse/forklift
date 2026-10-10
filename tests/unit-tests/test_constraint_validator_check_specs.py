"""ConstraintValidator: check constraints that check nothing are skipped."""

from __future__ import annotations

import pyarrow as pa

from forklift.processors.constraint_validator import ConstraintConfig, ConstraintValidator


class TestInertCheckConstraints:
    def test_specs_without_a_check_key_or_not_a_dict_are_ignored(self):
        validator = ConstraintValidator(
            ConstraintConfig(
                check_constraints={
                    "documented_only": {"column": "a", "description": "no check"},
                    "not_a_dict": "a > 0",
                    "positive": {"column": "a", "min": 0},
                }
            )
        )

        kept, results = validator.process_batch(pa.RecordBatch.from_pydict({"a": [-1, 1]}))

        assert kept.column("a").to_pylist() == [1]
        assert [(r.error_code, r.row_index) for r in results] == [("RANGE_VIOLATION", 0)]
        assert [v.constraint_name for v in validator.get_all_violations()] == ["positive"]
