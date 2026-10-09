"""Cross-package behaviour that no single package's tests can cover on their own."""

from __future__ import annotations

import datetime
import json
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import forklift as fl
from forklift.metadata import MetadataWriteError
from forklift.schema.generator.inference import DataTypeInferrer


def _write_people_csv(path):
    rows = ["name,ssn,salary"]
    rows += [f"p{i},{100 + i:03d}-45-{6000 + i},{50000 + i * 17}" for i in range(30)]
    path.write_text("\n".join(rows) + "\n")


def _column_stats(output_dir):
    metadata = json.loads((output_dir / "output_data_metadata.json").read_text())
    return metadata["column_statistics"]


class TestValueStatisticsPassThrough:
    def test_import_csv_omits_cell_values_from_metadata_by_default(self, tmp_path):
        source = tmp_path / "people.csv"
        _write_people_csv(source)

        fl.import_csv(input_path=str(source), output_path=str(tmp_path / "out"))

        text = (tmp_path / "out" / "output_data_metadata.json").read_text()
        assert "100-45-6000" not in text
        for column in _column_stats(tmp_path / "out").values():
            assert "top_values" not in column
            assert "min_value" not in column and "max_value" not in column

    def test_import_csv_includes_values_only_when_asked(self, tmp_path):
        source = tmp_path / "people.csv"
        _write_people_csv(source)

        fl.import_csv(
            input_path=str(source),
            output_path=str(tmp_path / "out"),
            include_value_statistics=True,
        )

        assert "top_values" in _column_stats(tmp_path / "out")["ssn"]


class TestMetadataFailureDoesNotDiscardFinishedRun:
    def test_data_is_kept_and_failure_is_reported(self, tmp_path):
        source = tmp_path / "people.csv"
        _write_people_csv(source)

        with patch(
            "forklift.metadata.output_metadata_collector.OutputMetadataCollector.save_metadata",
            side_effect=MetadataWriteError("Could not write output metadata"),
        ):
            results = fl.import_csv(input_path=str(source), output_path=str(tmp_path / "out"))

        assert pq.read_table(tmp_path / "out" / "data.parquet").num_rows == 30
        assert any("Could not write output metadata" in e for e in results.errors)


class TestSchemaGenerationSamplingOverS3:
    """WP4 made binary S3 reads a full seekable copy; CSV sampling must opt out of that."""

    def test_csv_sampling_requests_the_forward_only_stream(self):
        inference = DataTypeInferrer.__new__(DataTypeInferrer)
        seen = []

        class FakeIO:
            def open_for_read(self, path, encoding="utf-8", mode="r", **kwargs):
                seen.append(kwargs.get("seekable", True))
                raise RuntimeError("stop after recording the request")

        inference.io_handler = FakeIO()
        with pytest.raises(RuntimeError):
            with inference._binary_opener("s3://bucket/data.csv", seekable=False)():
                pass
        with pytest.raises(RuntimeError):
            with inference._binary_opener("s3://bucket/data.parquet")():
                pass

        assert seen == [False, True]

    def test_csv_schema_from_s3_object_is_correct(self, tmp_path):
        moto = pytest.importorskip("moto")
        import boto3

        from forklift.api import generate_schema_from_csv

        with moto.mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="bkt")
            body = "zip,amount\n" + "\n".join(f"{i:05d},{i}.5" for i in range(50)) + "\n"
            client.put_object(Bucket="bkt", Key="data.csv", Body=body.encode())

            with patch.dict(
                "os.environ",
                {
                    "AWS_ACCESS_KEY_ID": "x",
                    "AWS_SECRET_ACCESS_KEY": "x",
                    "AWS_DEFAULT_REGION": "us-east-1",
                },
            ):
                schema = generate_schema_from_csv("s3://bkt/data.csv", nrows=10)

        props = schema["properties"]
        assert props["zip"]["type"] == "string"  # leading zeros survive
        assert props["amount"]["type"] == "number"


class TestPostgresNamesStayWithinLimitAfterDedupe:
    def test_csv_importer_keeps_deduped_names_within_63_characters(self):
        from forklift.schema.csv_schema_importer import CsvSchemaImporter

        importer = CsvSchemaImporter(
            {
                "type": "object",
                "properties": {},
                "x-csv": {"case": {"standardizeNames": "postgres", "dedupeNames": "suffix"}},
            },
            validate=False,
        )
        long_name = "a_very_long_column_name_" * 4

        names = importer.standardize_column_names([long_name, long_name, long_name])

        assert len(set(names)) == 3
        assert all(len(n) <= 63 for n in names)


class TestShippedSchemaStandardsLoad:
    """The schema-standards files are what users copy; each must load with its importer."""

    STANDARDS = (
        pytest.importorskip("pathlib").Path(__file__).resolve().parents[2] / "schema-standards"
    )

    def test_csv_standard(self):
        from forklift.schema.csv_schema_importer import CsvSchemaImporter

        importer = CsvSchemaImporter(self.STANDARDS / "20250826-csv.json", validate=True)
        assert importer.validation_errors == []

    def test_excel_standard_defines_x_excel(self):
        from forklift.schema.excel_schema_importer import ExcelSchemaImporter

        importer = ExcelSchemaImporter(self.STANDARDS / "20250826-excel.json", validate=True)
        assert importer.validation_errors == []
        assert importer.sheets, "the Excel standard must describe at least one sheet"

    def test_sql_standard(self):
        from forklift.schema.sql_schema_importer import SqlSchemaImporter

        importer = SqlSchemaImporter(self.STANDARDS / "20250826-sql.json", validate=True)
        assert importer.validation_errors == []

    @pytest.mark.parametrize("name", ["20250826-fwf.json", "20250826-fwf-conditional.json"])
    def test_fwf_standards(self, name):
        from forklift.schema.fwf_schema_importer import FwfSchemaImporter

        FwfSchemaImporter(self.STANDARDS / name)  # raises on an invalid standard


class TestShortIdentifiersAreNotFabricated:
    """With validation on, short SSNs/ZIPs are invalid rather than padded into valid-looking ones."""

    @pytest.mark.parametrize("garbage", ["0", "1", "000", "SSN: 000", "12345", "12345678"])
    def test_validating_ssn_formatter_rejects_short_values(self, garbage):
        from forklift.utils.transformations.configs import SSNConfig
        from forklift.utils.transformations.format.ssn import SSNFormatter

        with pytest.raises(ValueError):
            SSNFormatter(SSNConfig()).format_value(garbage)

    def test_special_type_pipeline_reports_them_as_invalid(self):
        from forklift.utils.transformations.base import DataTransformer
        from forklift.utils.transformations.configs import SSNConfig

        column = pa.array(["123-45-6789", "garbage", None, "SSN: 000"])
        out = DataTransformer().apply_ssn_formatting(column, SSNConfig())

        assert out.to_pylist() == ["123-45-6789", None, None, None]


class TestSeparatorFactoriesUseTheDerivedPairing:
    def test_money_factory_with_only_a_decimal_comma(self):
        from forklift.processors.transformations.factories import apply_money_conversion

        convert = apply_money_conversion(decimal_separator=",")

        assert convert(pa.array(["12,50", "1.234,56"])).to_pylist() == [12.5, 1234.56]

    def test_numeric_factory_with_only_a_decimal_comma(self):
        from forklift.processors.transformations.factories import apply_numeric_cleaning

        clean = apply_numeric_cleaning(decimal_separator=",")

        assert clean(pa.array(["3,14"])).to_pylist() == [3.14]


class TestCalculatedColumnsDocumentationExamples:
    """The documented expressions must keep working; every JSON block must load and compile."""

    DOC = (
        pytest.importorskip("pathlib").Path(__file__).resolve().parents[2]
        / "docs"
        / "schemas"
        / "X_CALCULATED_COLUMNS_DOCUMENTATION.md"
    )

    def _blocks(self):
        import re

        return [
            json.loads(b) for b in re.findall(r"```json\n(.*?)\n```", self.DOC.read_text(), re.S)
        ]

    def test_every_documented_x_calculated_columns_block_builds_a_processor(self):
        from forklift.processors.calculated_columns_factory import (
            create_calculated_columns_processor_from_schema,
        )

        built = 0
        for block in self._blocks():
            config = block.get("x-calculatedColumns", block)
            if not any(k in config for k in ("constants", "expressions", "calculated")):
                continue
            assert create_calculated_columns_processor_from_schema(config) is not None
            built += 1
        assert built >= 5

    def test_documented_expressions_evaluate(self):
        from forklift.processors.calculated_columns.evaluator import ExpressionEvaluator

        batch = pa.RecordBatch.from_pydict(
            {
                "street": ["1 Main St"],
                "city": ["Springfield"],
                "state": ["IL"],
                "zip_code": ["62701"],
                "order_total": [200.0],
                "discount_percent": [10],
                "annual_spend": [7000],
                "nickname": [None],
                "first_name": ["Ann"],
                "signup_date": [datetime.date(2020, 1, 1)],
            }
        )
        evaluator = ExpressionEvaluator()

        def run(expr):
            return evaluator.evaluate_expression(batch, 0, expr)

        assert run("street + ', ' + city + ', ' + state + ' ' + zip_code") == (
            "1 Main St, Springfield, IL 62701"
        )
        assert run("order_total * (discount_percent / 100.0)") == 20.0
        assert (
            run(
                "'Gold' if annual_spend >= 10000 else ('Silver' if annual_spend >= 5000 else 'Bronze')"
            )
            == "Silver"
        )
        assert run("coalesce(nickname, first_name)") == "Ann"
        assert run("year(today()) - year(signup_date)") >= 5
        assert run("length(trim(zip_code)) == 5") is True


class TestCsvSamplingWithoutDefaultColumnType:
    """pyarrow < 19 has no ConvertOptions.default_column_type: the header must be read from
    the same stream (a forward-only S3 object is opened once) and every column kept a string."""

    @pytest.fixture(autouse=True)
    def _old_pyarrow(self, monkeypatch):
        import forklift.schema.generator.inference as inference

        monkeypatch.setattr(inference, "_SUPPORTS_DEFAULT_COLUMN_TYPE", False)

    def _sample(self, data: bytes, nrows=None, **kwargs):
        import io as _io

        opens = []

        class OneShot:
            def __init__(self):
                opens.append(1)
                self._buffer = _io.BytesIO(data)

            def __enter__(self):
                return self._buffer

            def __exit__(self, *exc):
                return False

        inferrer = DataTypeInferrer.__new__(DataTypeInferrer)
        table = inferrer._read_string_csv(
            OneShot, nrows, kwargs.get("delimiter", ","), kwargs.get("encoding", "utf-8")
        )
        return table, len(opens)

    def test_columns_stay_strings_and_the_stream_is_opened_once(self):
        table, opens = self._sample(b"zip,amount\n00123,1.5\n00456,2.5\n", nrows=10)

        assert table.column("zip").to_pylist() == ["00123", "00456"]
        assert table.column("amount").to_pylist() == ["1.5", "2.5"]
        assert opens == 1

    def test_bom_quoted_header_and_delimiter(self):
        data = '﻿"first, name";age\nAnn;007\n'.encode("utf-8")

        table, _ = self._sample(data, nrows=5, delimiter=";")

        assert table.column_names == ["first, name", "age"]
        assert table.column("age").to_pylist() == ["007"]

    def test_invalid_text_does_not_echo_data(self):
        with pytest.raises(ValueError, match="encoding") as excinfo:
            self._sample("a\nsecrét\n".encode("latin-1"))
        assert "secr" not in str(excinfo.value)
