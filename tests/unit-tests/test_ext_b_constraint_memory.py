"""Tests for the bounded memory use and the error-mode coercion of ``ConstraintValidator``.

1. Exact violation total, bounded retention, no cell values by default
2. ``bad_rows`` / ``fail_complete`` / ``fail_fast`` behave the same whatever the retention limit
3. ``ConstraintConfig.error_mode`` accepts strings (case-insensitive) and rejects the rest
4. ``EnhancedDataProcessor`` keeps dropping every violating row with a small limit
"""

import pyarrow as pa
import pytest

from forklift.processors.bad_rows_handler import BadRowsConfig
from forklift.processors.constraint_validator import (
    DEFAULT_MAX_RETAINED_VIOLATIONS,
    ConstraintConfig,
    ConstraintValidator,
    ErrorMode,
    coerce_error_mode,
    create_constraint_config_from_schema,
)
from forklift.processors.enhanced_processor import EnhancedDataProcessor

SECRET = "SECRET-CELL-VALUE-123"


def _ids(*ids) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"id": list(ids)})


def _validator(error_mode=ErrorMode.BAD_ROWS, **kwargs) -> ConstraintValidator:
    return ConstraintValidator(
        ConstraintConfig(error_mode=error_mode, unique_constraints=["id"], **kwargs)
    )


def _run_duplicates(validator: ConstraintValidator, batches: int = 50):
    """Every batch repeats id 0 (a duplicate after the first batch) and adds one fresh id."""
    kept = []
    for n in range(batches):
        out, _ = validator.process_batch(_ids(0, n + 1, 0))
        kept.extend(out.column("id").to_pylist())
    return kept


# ---------------------------------------------------------------------------------------------
# 1. exact total, bounded retention, no values by default
# ---------------------------------------------------------------------------------------------


class TestBoundedRetention:
    def test_defaults(self):
        config = ConstraintConfig()
        assert config.max_retained_violations == DEFAULT_MAX_RETAINED_VIOLATIONS == 1000
        assert config.include_values is False

    def test_many_batches_keep_at_most_the_cap_but_count_exactly(self):
        validator = _validator(max_retained_violations=10)
        _run_duplicates(validator, batches=50)

        # batch 1: row 2 repeats id 0 (1); batches 2..50: rows 0 and 2 (2 each)
        expected_total = 1 + 49 * 2
        assert validator.violation_count == expected_total
        assert len(validator.violations) == 10
        assert validator.violations_truncated is True
        assert validator.get_all_violations() == validator.violations
        assert validator.rows_seen == 150

    def test_the_first_violations_are_the_ones_retained(self):
        validator = _validator(max_retained_violations=2)
        validator.process_batch(_ids(1, 1, 1))  # violations at rows 1 and 2
        validator.process_batch(_ids(1))  # a third one, not retained
        assert [v.row_index for v in validator.violations] == [1, 2]
        assert validator.violation_count == 3

    def test_default_cap_is_applied(self):
        validator = _validator()
        validator.process_batch(_ids(*([7] * 2500)))
        assert validator.violation_count == 2499
        assert len(validator.violations) == 1000
        assert len(validator.batch_violations) == 2499  # one batch is bounded by its size

    def test_cap_of_zero_retains_nothing(self):
        validator = _validator(max_retained_violations=0)
        validator.process_batch(_ids(1, 1))
        assert validator.violation_count == 1
        assert validator.violations == []
        assert len(validator.batch_violations) == 1

    def test_none_means_no_limit(self):
        validator = _validator(max_retained_violations=None)
        _run_duplicates(validator, batches=20)
        assert len(validator.violations) == validator.violation_count == 1 + 19 * 2
        assert validator.violations_truncated is False

    def test_under_the_cap_nothing_is_truncated(self):
        validator = _validator(max_retained_violations=100)
        validator.process_batch(_ids(1, 1))
        assert validator.violations_truncated is False
        assert len(validator.violations) == validator.violation_count == 1

    def test_batch_violations_always_hold_the_whole_last_batch(self):
        validator = _validator(max_retained_violations=1)
        validator.process_batch(_ids(1, 1, 1, 1))
        assert len(validator.violations) == 1
        assert [v.row_index for v in validator.batch_violations] == [1, 2, 3]

    def test_reset_clears_the_count(self):
        validator = _validator(max_retained_violations=1)
        validator.process_batch(_ids(1, 1, 1))
        validator.reset()
        assert validator.violation_count == 0
        assert validator.violations == []
        assert validator.violations_truncated is False
        out, _ = validator.process_batch(_ids(1, 1))  # keys were forgotten too
        assert out.column("id").to_pylist() == [1]
        assert validator.violation_count == 1

    @pytest.mark.parametrize("bad", [-1, 1.5, "10", True, [1]])
    def test_invalid_cap_raises(self, bad):
        with pytest.raises(ValueError, match="max_retained_violations"):
            ConstraintConfig(max_retained_violations=bad)

    @pytest.mark.parametrize("bad", [1, "yes", None])
    def test_invalid_include_values_raises(self, bad):
        with pytest.raises(ValueError, match="include_values"):
            ConstraintConfig(include_values=bad)


class TestViolationValues:
    CHECKS = {
        "age_range": {"column": "age", "max": 120},
        "name_enum": {"column": "name", "enum": ["ok"]},
        "name_pattern": {"column": "name", "pattern": "^o"},
        "name_length": {"column": "name", "maxLength": 2},
        "tag_nullable": {"column": "tag", "nullable": False},
    }

    def _batch(self) -> pa.RecordBatch:
        return pa.RecordBatch.from_pydict(
            {
                "id": [1, 3, 1],
                "age": [30, 150, 20],
                "name": ["ok", SECRET, "ok"],
                "tag": ["t", None, "t"],
            }
        )

    def _run(self, **kwargs):
        validator = ConstraintValidator(
            ConstraintConfig(check_constraints=self.CHECKS, unique_constraints=["id"], **kwargs)
        )
        validator.process_batch(self._batch())
        return validator

    def test_values_are_not_stored_by_default(self):
        validator = self._run()
        kinds = {v.violation_type for v in validator.violations}
        assert {"range", "enum", "pattern", "length", "null", "unique"} <= kinds
        assert all(v.values == [] for v in validator.violations)
        assert all(v.values == [] for v in validator.batch_violations)
        assert SECRET not in repr(validator.violations)
        assert SECRET not in repr(validator.batch_violations)

    def test_values_are_stored_when_asked(self):
        validator = self._run(include_values=True)
        by_type = {v.violation_type: v for v in validator.violations}
        assert by_type["range"].values == [150]
        assert by_type["enum"].values == [SECRET]
        assert by_type["null"].values == [None]
        assert by_type["unique"].values == [1]

    def test_composite_unique_key_values(self):
        validator = ConstraintValidator(
            ConstraintConfig(unique_constraints=[("a", "b")], include_values=True)
        )
        batch = pa.RecordBatch.from_pydict({"a": [1, 1], "b": ["x", "x"]})
        validator.process_batch(batch)
        assert validator.violations[0].values == [1, "x"]

        quiet = ConstraintValidator(ConstraintConfig(unique_constraints=[("a", "b")]))
        quiet.process_batch(batch)
        assert quiet.violations[0].values == []

    def test_validation_results_never_carry_values(self):
        validator = ConstraintValidator(
            ConstraintConfig(
                check_constraints=self.CHECKS, unique_constraints=["id"], include_values=True
            )
        )
        _, results = validator.process_batch(self._batch())
        assert results
        assert all(SECRET not in str(r.error_message) for r in results)


# ---------------------------------------------------------------------------------------------
# 2. behaviour is independent of the retention limit
# ---------------------------------------------------------------------------------------------


class TestModesIgnoreTheCap:
    @pytest.mark.parametrize("cap", [0, 1, 10, None])
    def test_bad_rows_drops_every_violating_row(self, cap):
        validator = _validator(max_retained_violations=cap)
        kept = _run_duplicates(validator, batches=50)
        # id 0 once, then each batch's fresh id
        assert kept == [0] + list(range(1, 51))
        assert validator.violation_count == 1 + 49 * 2

    def test_bad_rows_results_are_complete_for_every_batch(self):
        validator = _validator(max_retained_violations=1)
        for _ in range(5):
            batch = _ids(5, 5, 5)
            _, results = validator.process_batch(batch)
            assert len(results) == len(validator.batch_violations)
        # after the first batch every row of the batch is a duplicate
        assert len(results) == 3

    @pytest.mark.parametrize("cap", [0, 1, 10, None])
    def test_fail_complete_uses_the_exact_count(self, cap):
        validator = _validator(ErrorMode.FAIL_COMPLETE, max_retained_violations=cap)
        kept = _run_duplicates(validator, batches=50)
        assert len(kept) == 150  # every row is kept in this mode
        with pytest.raises(ValueError, match=f"failed with {1 + 49 * 2} violations"):
            validator.finalize()

    def test_fail_complete_without_violations_passes(self):
        validator = _validator(ErrorMode.FAIL_COMPLETE, max_retained_violations=0)
        validator.process_batch(_ids(1, 2, 3))
        validator.finalize()

    def test_bad_rows_finalize_never_raises(self):
        validator = _validator(max_retained_violations=0)
        validator.process_batch(_ids(1, 1))
        validator.finalize()

    @pytest.mark.parametrize("cap", [0, 1, None])
    def test_fail_fast_raises_on_the_first_violation(self, cap):
        validator = _validator(ErrorMode.FAIL_FAST, max_retained_violations=cap)
        validator.process_batch(_ids(1, 2))
        with pytest.raises(ValueError, match="violated"):
            validator.process_batch(_ids(2))
        assert validator.violation_count == 1

    def test_fail_fast_finalize_uses_the_count_even_with_nothing_retained(self):
        validator = _validator(ErrorMode.FAIL_FAST, max_retained_violations=0)
        validator.violation_count = 4  # as if violations had been seen and discarded
        with pytest.raises(ValueError, match="4 violations"):
            validator.finalize()


# ---------------------------------------------------------------------------------------------
# 3. error_mode coercion
# ---------------------------------------------------------------------------------------------


class TestErrorModeCoercion:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("bad_rows", ErrorMode.BAD_ROWS),
            ("fail_fast", ErrorMode.FAIL_FAST),
            ("fail_complete", ErrorMode.FAIL_COMPLETE),
            ("BAD_ROWS", ErrorMode.BAD_ROWS),
            ("Fail_Fast", ErrorMode.FAIL_FAST),
            ("  fail_complete ", ErrorMode.FAIL_COMPLETE),
            (ErrorMode.FAIL_FAST, ErrorMode.FAIL_FAST),
        ],
    )
    def test_config_stores_an_error_mode(self, value, expected):
        config = ConstraintConfig(error_mode=value)
        assert config.error_mode is expected
        assert coerce_error_mode(value) is expected

    @pytest.mark.parametrize("value", ["bad_row", "", "fail-fast", None, 3, ["bad_rows"]])
    def test_invalid_values_raise_and_name_the_valid_ones(self, value):
        with pytest.raises(ValueError, match=r"bad_rows \| fail_fast \| fail_complete"):
            ConstraintConfig(error_mode=value)

    def test_string_bad_rows_really_drops_rows(self):
        validator = ConstraintValidator(
            ConstraintConfig(error_mode="bad_rows", unique_constraints=["id"])
        )
        out, results = validator.process_batch(_ids(1, 1, 2, 1))
        assert out.column("id").to_pylist() == [1, 2]
        assert len(results) == 2

    def test_string_fail_fast_raises_from_process_batch(self):
        validator = ConstraintValidator(
            ConstraintConfig(error_mode="FAIL_FAST", unique_constraints=["id"])
        )
        with pytest.raises(ValueError, match="violated"):
            validator.process_batch(_ids(1, 1))

    def test_string_fail_complete_raises_from_finalize(self):
        validator = ConstraintValidator(
            ConstraintConfig(error_mode="fail_complete", unique_constraints=["id"])
        )
        out, _ = validator.process_batch(_ids(1, 1))
        assert out.num_rows == 2
        with pytest.raises(ValueError, match="1 violations"):
            validator.finalize()

    def test_default_is_unchanged(self):
        assert ConstraintConfig().error_mode is ErrorMode.BAD_ROWS

    def test_schema_loader_uses_the_same_coercion(self):
        def load(mode):
            return create_constraint_config_from_schema(
                {"x-constraintHandling": {"errorMode": mode}}
            ).error_mode

        assert load("fail_fast") is ErrorMode.FAIL_FAST
        assert load("FAIL_COMPLETE") is ErrorMode.FAIL_COMPLETE
        assert load(" Bad_Rows") is ErrorMode.BAD_ROWS
        for bad in ("bad_row", "", None, 5):
            with pytest.raises(
                ValueError, match=r"errorMode.*bad_rows \| fail_fast \| fail_complete"
            ):
                load(bad)


# ---------------------------------------------------------------------------------------------
# 4. EnhancedDataProcessor with a small retention limit
# ---------------------------------------------------------------------------------------------


class TestEnhancedProcessorWithSmallLimit:
    def _processor(self, tmp_path, cap):
        schema = pa.schema([("id", pa.int64())])
        return EnhancedDataProcessor(
            schema=schema,
            schema_dict={"x-constraintHandling": {"errorMode": "bad_rows"}},
            constraint_config=ConstraintConfig(
                unique_constraints=["id"], max_retained_violations=cap
            ),
            bad_rows_config=BadRowsConfig(output_path=str(tmp_path / "bad_rows.parquet")),
        )

    @pytest.mark.parametrize("cap", [0, 3, 1000])
    def test_every_violating_row_is_still_dropped_and_collected(self, tmp_path, cap):
        processor = self._processor(tmp_path, cap)
        kept = []
        for n in range(20):
            out, results = processor.process_batch(_ids(0, n + 1, 0))
            kept.extend(out.column("id").to_pylist())
            assert sorted(r.row_index for r in results if not r.is_valid) == (
                [2] if n == 0 else [0, 2]
            )
        assert kept == [0] + list(range(1, 21))
        assert processor.bad_rows_handler.get_bad_row_count() == 1 + 19 * 2
        assert processor.constraint_validator.violation_count == 1 + 19 * 2
