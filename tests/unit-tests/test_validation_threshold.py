"""``x-validation`` bad-row threshold: judged once at the end of the input by default."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from forklift import import_csv
from forklift.processors.data_validation import (
    BadRowsConfig,
    DataValidationProcessor,
    FieldValidationRule,
    RangeValidation,
    ValidationConfig,
)
from forklift.processors.data_validation.data_validation_processor import (
    BadRowsThresholdExceededError,
)
from forklift.processors.schema_extensions import build_data_validator, unsupported_extension_keys


def processor(threshold_check="early", percent=10.0, fail=True):
    return DataValidationProcessor(
        ValidationConfig(
            field_validations=[
                FieldValidationRule("age", range_validation=RangeValidation(0, 150))
            ],
            bad_rows_config=BadRowsConfig(
                enabled=False,
                max_bad_rows_percent=percent,
                fail_on_exceed_threshold=fail,
                threshold_check=threshold_check,
            ),
        )
    )


def batch(ages):
    return pa.RecordBatch.from_pydict({"age": ages})


class TestProcessor:
    def test_early_raises_while_the_batches_are_processed(self):
        with pytest.raises(BadRowsThresholdExceededError, match="thresholdMode is 'early'"):
            processor("early").process_batch(batch([999, 999, 30, 30]))

    def test_end_of_file_waits_for_the_verdict(self):
        validator = processor("end_of_file")

        validator.process_batch(batch([999, 999, 30, 30]))  # 50 % so far: no error yet
        validator.process_batch(batch([30] * 96))  # 2 % of the 100 rows

        validator.check_threshold()  # under the limit: nothing happens

    def test_the_verdict_is_the_same_for_any_batching(self):
        ages = [999] * 3 + [30] * 97
        for size in (1, 5, 10, 100):
            validator = processor("end_of_file")
            for start in range(0, 100, size):
                validator.process_batch(batch(ages[start : start + size]))

            validator.check_threshold()

    def test_over_the_limit_at_the_end_explains_itself(self):
        validator = processor("end_of_file", percent=10)
        validator.process_batch(batch([999] * 20 + [30] * 80))

        with pytest.raises(BadRowsThresholdExceededError) as error:
            validator.check_threshold({"VALIDATION_ERROR:age": 20, "VALIDATION_ERROR:name": 3})

        text = str(error.value)
        assert "20 of 100 rows" in text and "(20.0%)" in text and "threshold (10%)" in text
        assert "VALIDATION_ERROR:age x20, VALIDATION_ERROR:name x3" in text
        assert "no output was kept" in text
        for hint in ("failOnExceedThreshold", "maxBadRowsPercent", "thresholdMode"):
            assert f"x-validation.badRowsHandling.{hint}" in text

    def test_only_the_largest_findings_are_listed(self):
        validator = processor("end_of_file", percent=0)
        validator.process_batch(batch([999]))

        with pytest.raises(BadRowsThresholdExceededError) as error:
            validator.check_threshold({f"VALIDATION_ERROR:c{i:02d}": i + 1 for i in range(14)})

        assert "(and 4 more)" in str(error.value) and "c13 x14" in str(error.value)

    def test_failing_can_be_switched_off(self):
        validator = processor("end_of_file", fail=False)
        validator.process_batch(batch([999] * 10))

        validator.check_threshold()

    def test_an_unknown_mode_is_refused(self):
        with pytest.raises(ValueError, match="threshold_check.*early, end_of_file"):
            BadRowsConfig(threshold_check="late")


class TestLoader:
    @staticmethod
    def schema(**bad_rows):
        return {
            "x-validation": {
                "badRowsHandling": bad_rows,
                "fieldValidations": {"age": {"range": {"min": 0, "max": 150}}},
            }
        }

    def test_end_of_file_is_the_default(self):
        validator = build_data_validator(self.schema())

        assert validator.config.bad_rows_config.threshold_check == "end_of_file"

    def test_early_is_an_option(self):
        validator = build_data_validator(self.schema(thresholdMode="early"))

        assert validator.config.bad_rows_config.threshold_check == "early"

    def test_a_wrong_value_names_the_choices(self):
        with pytest.raises(ValueError) as error:
            build_data_validator(self.schema(thresholdMode="sometimes"))

        text = str(error.value)
        assert "thresholdMode" in text and "'early'" in text and "'end_of_file'" in text
        assert "'sometimes'" in text

    def test_the_key_is_known(self):
        assert unsupported_extension_keys(self.schema(thresholdMode="early")) == []


def run(tmp_path, csv_text, **bad_rows):
    source = tmp_path / "in.csv"
    source.write_text(csv_text)
    schema = {
        "properties": {"id": {"type": "integer"}, "age": {"type": "integer"}},
        "x-validation": {
            "badRowsHandling": {"maxBadRowsPercent": 10, **bad_rows},
            "fieldValidations": {"age": {"range": {"min": 0, "max": 150}}},
        },
    }
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(schema))
    return import_csv(
        input_path=str(source),
        output_path=str(tmp_path / "out"),
        schema_file=str(schema_file),
        batch_size=10,
    )


def rows(bad_first):
    ages = [999 if (i < 3 if bad_first else i >= 97) else 30 for i in range(100)]
    return "id,age\n" + "".join(f"{i},{a}\n" for i, a in enumerate(ages))


class TestImport:
    @pytest.mark.parametrize("bad_first", [True, False])
    def test_three_percent_bad_passes_wherever_the_bad_rows_are(self, tmp_path, bad_first):
        results = run(tmp_path, rows(bad_first))

        assert (results.valid_rows, results.invalid_rows) == (97, 3)

    def test_early_mode_is_the_old_behaviour(self, tmp_path):
        with pytest.raises(BadRowsThresholdExceededError, match="first 10 rows"):
            run(tmp_path, rows(bad_first=True), thresholdMode="early")

        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_a_failing_file_is_checked_to_the_end_and_leaves_no_output(self, tmp_path):
        text = "id,age\n" + "".join(f"{i},{999 if i % 2 else 30}\n" for i in range(100))

        with pytest.raises(BadRowsThresholdExceededError) as error:
            run(tmp_path, text)

        assert "50 of 100 rows" in str(error.value) and "VALIDATION_ERROR:age x50" in str(
            error.value
        )
        assert not (tmp_path / "out" / "data.parquet").exists()
        assert not (tmp_path / "out" / "bad_rows.parquet").exists()

    def test_not_failing_keeps_the_output_and_the_rejected_rows(self, tmp_path):
        text = "id,age\n" + "".join(f"{i},{999 if i % 2 else 30}\n" for i in range(100))

        results = run(tmp_path, text, failOnExceedThreshold=False)

        assert (results.valid_rows, results.invalid_rows) == (50, 50)
        assert results.bad_rows_file
