"""Cross-package behaviour that no single package's tests can cover on their own."""

from __future__ import annotations

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
        from forklift.utils.transformations.configs import SSNConfig
        from forklift.utils.transformations.base import DataTransformer

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
