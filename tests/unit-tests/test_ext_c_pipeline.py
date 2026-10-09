"""The extension pipeline that runs the schema extensions on the engine's batches.

These tests build the pipeline from processor objects; ``test_ext_e2e.py`` drives the same
behaviour through ``import_csv`` with real schema files.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.engine.processors.extensions import (
    HIDDEN_PREFIX,
    ExtensionPipeline,
    strip_hidden_columns,
)
from forklift.processors.calculated_columns_factory import (
    create_calculated_columns_processor_from_schema,
)
from forklift.processors.column_mapper import ColumnMapper, ColumnMappingConfig
from forklift.processors.constraint_validator import (
    ConstraintConfig,
    ConstraintValidator,
    ErrorMode,
)
from forklift.processors.data_validation import (
    BadRowsConfig,
    DataValidationProcessor,
    FieldValidationRule,
    RangeValidation,
    ValidationConfig,
)
from forklift.processors.transformations import SchemaBasedTransformer


def people(**overrides):
    columns = {
        "id": [1, 2, 2, 3],
        "age": [30, 200, 41, 52],
        "name": ["Ann", "Bob", "Cy", "Di"],
    }
    columns.update(overrides)
    return pa.RecordBatch.from_pydict(columns)


def age_validator(maximum=150):
    return DataValidationProcessor(
        ValidationConfig(
            field_validations=[
                FieldValidationRule("age", range_validation=RangeValidation(0, maximum))
            ],
            bad_rows_config=BadRowsConfig(fail_on_exceed_threshold=False),
        )
    )


def unique_ids(error_mode=ErrorMode.BAD_ROWS):
    return ConstraintValidator(ConstraintConfig(unique_constraints=["id"], error_mode=error_mode))


class TestRowRejection:
    def test_without_row_dropping_stages_every_row_is_kept(self):
        pipeline = ExtensionPipeline(mapper=ColumnMapper(ColumnMappingConfig()))

        stage = pipeline.post_convert(people())

        assert len(stage.kept) == 4
        assert stage.rejected is None and stage.reasons == []
        assert not pipeline.rejects_rows

    def test_duplicate_key_is_rejected_with_a_reason_and_the_first_row_is_kept(self):
        pipeline = ExtensionPipeline(constraints=unique_ids())

        stage = pipeline.post_convert(people())

        assert stage.kept.column("name").to_pylist() == ["Ann", "Bob", "Di"]
        assert stage.rejected.column("name").to_pylist() == ["Cy"]
        assert len(stage.reasons) == 1 and "UNIQUE" in stage.reasons[0]
        assert "id" in stage.reasons[0]
        assert pipeline.rejects_rows

    def test_a_row_rejected_by_validation_does_not_claim_its_key(self):
        # Bob (age 200) is rejected by the age rule; Cy has the same id and must stay valid
        pipeline = ExtensionPipeline(validator=age_validator(), constraints=unique_ids())

        stage = pipeline.post_convert(people())

        assert stage.kept.column("name").to_pylist() == ["Ann", "Cy", "Di"]
        assert stage.rejected.column("name").to_pylist() == ["Bob"]

    def test_reasons_name_the_rule_but_never_the_value(self):
        pipeline = ExtensionPipeline(validator=age_validator())

        stage = pipeline.post_convert(people())

        assert stage.rejected.column("age").to_pylist() == [200]
        assert stage.reasons and all("200" not in reason for reason in stage.reasons)

    def test_rejected_rows_keep_the_names_and_values_they_entered_with(self):
        mapper = ColumnMapper(ColumnMappingConfig(explicit_mappings={"age": "years"}))
        pipeline = ExtensionPipeline(
            mapper=mapper,
            validator=DataValidationProcessor(
                ValidationConfig(
                    field_validations=[
                        FieldValidationRule("years", range_validation=RangeValidation(0, 150))
                    ],
                    bad_rows_config=BadRowsConfig(fail_on_exceed_threshold=False),
                )
            ),
        )

        stage = pipeline.post_convert(people())

        assert stage.kept.schema.names == ["id", "years", "name"]
        assert stage.rejected.schema.names == ["id", "age", "name"]  # the shape of the input

    def test_reasons_line_up_with_rejected_rows_across_stages(self):
        pipeline = ExtensionPipeline(validator=age_validator(), constraints=unique_ids())
        batch = people(id=[1, 1, 2, 2], age=[10, 10, 999, 10], name=["a", "b", "c", "d"])

        stage = pipeline.post_convert(batch)

        rejected = dict(zip(stage.rejected.column("name").to_pylist(), stage.reasons))
        assert set(rejected) == {"b", "c"}
        assert "UNIQUE" in rejected["b"]
        assert rejected["c"] and "UNIQUE" not in rejected["c"]
        assert stage.kept.column("name").to_pylist() == ["a", "d"]

    def test_state_is_kept_across_batches(self):
        pipeline = ExtensionPipeline(constraints=unique_ids())
        pipeline.post_convert(pa.RecordBatch.from_pydict({"id": [1, 2]}))

        stage = pipeline.post_convert(pa.RecordBatch.from_pydict({"id": [2, 3]}))

        assert stage.kept.column("id").to_pylist() == [3]
        assert stage.rejected.column("id").to_pylist() == [2]


class TestShapingStages:
    def test_mapping_then_calculated_columns(self):
        calculated = create_calculated_columns_processor_from_schema(
            {
                "expressions": [
                    {
                        "name": "double_age",
                        "expression": "years * 2",
                        "dataType": "int64",
                        "dependencies": ["years"],
                    }
                ]
            }
        )
        mapper = ColumnMapper(ColumnMappingConfig(explicit_mappings={"age": "years"}))
        pipeline = ExtensionPipeline(mapper=mapper, calculated=calculated)

        stage = pipeline.post_convert(people())

        assert stage.kept.schema.names == ["id", "years", "name", "double_age"]
        assert stage.kept.column("double_age").to_pylist() == [60, 400, 82, 104]

    def test_output_schema_is_what_a_real_batch_produces(self):
        calculated = create_calculated_columns_processor_from_schema(
            {
                "constants": [{"name": "source", "value": "csv", "dataType": "string"}],
            }
        )
        pipeline = ExtensionPipeline(calculated=calculated, constraints=unique_ids())
        batch = people()

        predicted = pipeline.output_schema(batch.schema)
        actual = pipeline.post_convert(batch).kept.schema

        assert predicted.equals(actual)


class TestPreStage:
    def test_transformations_clean_text_before_types_are_applied(self):
        schema = {
            "properties": {"name": {"type": "string"}},
            "x-transformations": {
                "column_transformations": {
                    "name": {"string_cleaning": {"enabled": True, "strip_whitespace": True}}
                }
            },
        }
        pipeline = ExtensionPipeline(transformer=SchemaBasedTransformer(schema))
        assert pipeline.has_pre_stage

        out = pipeline.pre_convert(pa.RecordBatch.from_pydict({"name": ["  Ann  ", "Bob"]}))

        assert out.column("name").to_pylist() == ["Ann", "Bob"]

    def test_no_pre_stage_without_transformations_or_row_hash(self):
        assert not ExtensionPipeline(constraints=unique_ids()).has_pre_stage


class TestBookkeeping:
    def test_summary_counts_problems_by_code_and_column(self):
        pipeline = ExtensionPipeline(constraints=unique_ids())

        pipeline.post_convert(people(id=[1, 1, 1, 2]))

        assert sum(pipeline.summary.values()) == 2
        assert any(key.startswith("UNIQUE") for key in pipeline.summary)

    def test_summary_never_holds_more_than_a_bounded_number_of_keys(self):
        pipeline = ExtensionPipeline()
        from forklift.processors.base import ValidationResult

        pipeline.record(
            [ValidationResult(False, "m", "CODE", column_name=f"c{i}") for i in range(500)]
        )

        assert len(pipeline.summary) <= 201
        assert sum(pipeline.summary.values()) == 500

    def test_fail_complete_keeps_every_row_and_raises_when_finished(self):
        pipeline = ExtensionPipeline(constraints=unique_ids(ErrorMode.FAIL_COMPLETE))

        stage = pipeline.post_convert(people())
        assert len(stage.kept) == 4 and stage.rejected is None

        with pytest.raises(ValueError):
            pipeline.finalize()

    def test_fail_fast_raises_on_the_first_violation(self):
        pipeline = ExtensionPipeline(constraints=unique_ids(ErrorMode.FAIL_FAST))

        with pytest.raises(ValueError):
            pipeline.post_convert(people())

    def test_describe_lists_applied_extensions_warnings_and_summary(self):
        pipeline = ExtensionPipeline(constraints=unique_ids(), warnings=["x-pii is ignored"])
        pipeline.post_convert(people())

        description = pipeline.describe()

        assert description["warnings"] == ["x-pii is ignored"]
        assert description["applied"] and description["validation_summary"]

    def test_strip_hidden_columns(self):
        batch = pa.RecordBatch.from_pydict({"id": [1], f"{HIDDEN_PREFIX}row_id": [1]})

        assert strip_hidden_columns(batch).schema.names == ["id"]
        assert strip_hidden_columns(strip_hidden_columns(batch)).schema.names == ["id"]


class TestEnhancedProcessorWithBoundedViolations:
    """The constraint validator keeps only some violations; the totals must stay exact."""

    def test_totals_and_per_batch_attribution_do_not_depend_on_the_cap(self):
        from forklift.processors.enhanced_processor import EnhancedDataProcessor

        config = ConstraintConfig(unique_constraints=["id"], max_retained_violations=3)
        processor = EnhancedDataProcessor(
            pa.schema([pa.field("id", pa.int64())]), constraint_config=config, strict_mode=False
        )
        batch = pa.RecordBatch.from_pydict({"id": [1] * 20})  # 19 duplicates of the first row

        kept, _ = processor.process_batch(batch)

        assert kept.num_rows == 1
        assert processor.finalize()["constraint_violations"] == 19
        summary = processor.get_constraint_violations_summary()
        assert summary["total_violations"] == 19 and summary["retained_violations"] == 3
        assert processor.bad_rows_handler.bad_row_count == 19
