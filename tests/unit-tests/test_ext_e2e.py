"""End to end: ``import_csv`` applies the schema extensions.

Each test writes a small CSV and a schema file, runs the real import and reads the Parquet
output. ``test_ext_c_pipeline.py`` covers the pipeline on processor objects and
``test_ext_a_loaders.py`` the loaders.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift import import_csv
from forklift.cli import main as cli_main

REPO_ROOT = Path(__file__).resolve().parents[2]
STANDARD_CSV = REPO_ROOT / "schema-standards" / "20250826-csv.json"


# ----------------------------------------------------------------------------------- helpers


def run(tmp_path, csv_text, schema=None, *, name="in.csv", **kwargs):
    """Import ``csv_text`` with ``schema`` (a dict). Returns ``(results, output_dir)``."""
    source = tmp_path / name
    source.write_text(csv_text)
    out = tmp_path / "out"
    if schema is not None:
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(schema))
        kwargs["schema_file"] = str(schema_file)
    results = import_csv(input_path=str(source), output_path=str(out), **kwargs)
    return results, out


def data(out):
    return pq.read_table(out / "data.parquet")


def bad(out):
    return pq.read_table(out / "bad_rows.parquet")


def schema_of(properties, **extensions):
    document = {"type": "object", "properties": properties}
    document.update(extensions)
    return document


PEOPLE = "id,name,age\n1,Ann,30\n2,Bob,200\n3,Cy,41\n3,Di,52\n"
PEOPLE_PROPERTIES = {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "age": {"type": "integer"},
}


# ----------------------------------------------------------------------------- without extras


class TestNothingConfigured:
    def test_a_schema_without_extensions_behaves_as_before(self, tmp_path):
        results, out = run(tmp_path, PEOPLE, schema_of(PEOPLE_PROPERTIES))

        assert results.schema_extensions == [] and results.warnings == []
        assert data(out).num_rows == 4
        assert not (out / "bad_rows.parquet").exists()

    def test_no_schema_at_all(self, tmp_path):
        results, out = run(tmp_path, PEOPLE)

        assert results.schema_extensions == [] and data(out).num_rows == 4


# -------------------------------------------------------------------------- x-transformations


class TestTransformations:
    SCHEMA = schema_of(
        {"name": {"type": "string"}, "salary": {"type": "number"}},
        **{
            "x-transformations": {
                "column_transformations": {
                    "name": {
                        "string_cleaning": {
                            "enabled": True,
                            "strip_whitespace": True,
                            "case_transform": "upper",
                        }
                    },
                    "salary": {"money_conversion": {"enabled": True}},
                }
            }
        },
    )

    def test_text_is_cleaned_before_the_types_are_applied(self, tmp_path):
        results, out = run(
            tmp_path, 'name,salary\n"  ann ","$1,234.50"\nbob,($10.00)\n', self.SCHEMA
        )

        table = data(out)
        assert table.column("name").to_pylist() == ["ANN", "BOB"]
        assert table.column("salary").to_pylist() == [1234.5, -10.0]
        assert "x-transformations" in results.schema_extensions

    def test_null_markers_see_the_text_of_the_file_not_the_transformed_text(self, tmp_path):
        schema = dict(self.SCHEMA)
        schema["x-csv"] = {"nulls": {"perColumn": {"salary": ["0.00"]}}}

        _, out = run(tmp_path, "name,salary\nann,0.00\nbob,5.00\n", schema)

        assert data(out).column("salary").to_pylist() == [None, 5.0]

    def test_a_transformation_for_a_column_the_file_lacks_only_warns(self, tmp_path):
        schema = dict(self.SCHEMA)
        schema["x-transformations"] = {
            "column_transformations": {
                "name": self.SCHEMA["x-transformations"]["column_transformations"]["name"],
                "nickname": {"string_cleaning": {"enabled": True, "strip_whitespace": True}},
            }
        }

        results, out = run(tmp_path, "name,salary\nann,1\n", schema)

        assert data(out).column("name").to_pylist() == ["ANN"]
        assert any("nickname" in w and "not in the input" in w for w in results.warnings)

    def test_an_invalid_transformation_fails_before_any_output_is_written(self, tmp_path):
        schema = schema_of(
            {"name": {"type": "string"}},
            **{
                "x-transformations": {
                    "column_transformations": {
                        "name": {"string_cleaning": {"enabled": True, "no_such_option": 1}}
                    }
                }
            },
        )

        with pytest.raises(ValueError):
            run(tmp_path, "name\nann\n", schema)

        assert not (tmp_path / "out" / "data.parquet").exists()


# ------------------------------------------------------------------------------ x-columnMapping


class TestColumnMapping:
    def test_headers_are_renamed(self, tmp_path):
        schema = schema_of(
            {"FullName": {"type": "string"}, "Years": {"type": "integer"}},
            **{"x-columnMapping": {"explicitMappings": {"FullName": "name", "Years": "age"}}},
        )

        results, out = run(tmp_path, "FullName,Years\nAnn,30\n", schema)

        table = data(out)
        assert table.schema.names == ["name", "age"]
        assert table.column("age").to_pylist() == [30]  # typed by the property of the same file
        assert "x-columnMapping" in results.schema_extensions

    def test_later_stages_use_the_new_names(self, tmp_path):
        schema = schema_of(
            {"Years": {"type": "integer"}},
            **{
                "x-columnMapping": {"explicitMappings": {"Years": "age"}},
                "x-validation": {
                    "badRowsHandling": {"maxBadRowsPercent": 100},
                    "fieldValidations": {"age": {"range": {"min": 0, "max": 150}}},
                },
            },
        )

        _, out = run(tmp_path, "Years\n30\n200\n", schema)

        assert data(out).column("age").to_pylist() == [30]
        assert bad(out).column("Years").to_pylist() == ["200"]  # the input's own name

    def test_a_rename_onto_a_declared_property_is_called_out(self, tmp_path):
        schema = schema_of(
            {"age": {"type": "integer"}},
            **{"x-columnMapping": {"explicitMappings": {"Years": "age"}}},
        )

        results, _ = run(tmp_path, "Years\n30\n", schema)

        assert any("'Years'" in w and "'age'" in w for w in results.warnings)

    def test_colliding_names_are_refused(self, tmp_path):
        schema = schema_of(
            {"a": {"type": "string"}},
            **{"x-columnMapping": {"explicitMappings": {"a": "b"}}},
        )

        with pytest.raises(ValueError):
            run(tmp_path, "a,b\n1,2\n", schema)


# ------------------------------------------------------------------------- x-calculatedColumns


class TestCalculatedColumns:
    SCHEMA = schema_of(
        {"age": {"type": "integer"}},
        **{
            "x-calculatedColumns": {
                "constants": [
                    {"name": "source", "value": "csv", "dataType": "string"},
                    {"name": "loaded", "value": "2024-08-26", "dataType": "date32"},
                ],
                "expressions": [
                    {
                        "name": "next_age",
                        "expression": "age + 1",
                        "dataType": "int64",
                        "dependencies": ["age"],
                    }
                ],
            }
        },
    )

    def test_columns_are_added(self, tmp_path):
        results, out = run(tmp_path, "age\n30\n41\n", self.SCHEMA)

        table = data(out)
        assert table.schema.names == ["age", "source", "loaded", "next_age"]
        assert table.column("next_age").to_pylist() == [31, 42]
        assert table.column("source").to_pylist() == ["csv", "csv"]
        assert pa.types.is_date32(table.schema.field("loaded").type)
        assert "x-calculatedColumns" in results.schema_extensions

    def test_a_file_without_rows_still_has_the_calculated_columns(self, tmp_path):
        _, out = run(tmp_path, "age\n", self.SCHEMA)

        table = data(out)
        assert table.num_rows == 0
        assert table.schema.names == ["age", "source", "loaded", "next_age"]

    def test_a_calculated_column_cannot_overwrite_a_column_of_the_file(self, tmp_path):
        with pytest.raises(ValueError, match="overwrite"):
            run(tmp_path, "age,source\n30,x\n", self.SCHEMA)


# --------------------------------------------------------------------------------- x-validation


class TestValidation:
    SCHEMA = schema_of(
        PEOPLE_PROPERTIES,
        **{
            "x-validation": {
                "badRowsHandling": {"maxBadRowsPercent": 100},
                "fieldValidations": {"age": {"range": {"min": 0, "max": 150}}},
            }
        },
    )

    def test_rows_that_break_a_rule_go_to_bad_rows_with_the_reason(self, tmp_path):
        results, out = run(tmp_path, PEOPLE, self.SCHEMA)

        assert data(out).column("name").to_pylist() == ["Ann", "Cy", "Di"]
        rejected = bad(out)
        assert rejected.column("name").to_pylist() == ["Bob"]
        assert rejected.schema.names == ["id", "name", "age", "_rejection_reason"]
        reason = rejected.column("_rejection_reason")[0].as_py()
        assert "age" in reason and "200" not in reason
        assert results.invalid_rows == 1 and results.valid_rows == 3 and results.total_rows == 4
        assert results.bad_rows_file and results.validation_summary

    def test_no_reason_column_when_nothing_can_reject_rows(self, tmp_path):
        schema = schema_of(
            PEOPLE_PROPERTIES,
            **{"x-calculatedColumns": {"constants": [{"name": "c", "value": "x"}]}},
        )

        _, out = run(tmp_path, "id,name,age\n1,Ann,x\n", schema)

        assert bad(out).schema.names == ["id", "name", "age"]  # the type error only

    def test_too_many_bad_rows_abort_the_import_and_leave_no_output(self, tmp_path):
        schema = schema_of(
            PEOPLE_PROPERTIES,
            **{
                "x-validation": {
                    "badRowsHandling": {"maxBadRowsPercent": 10, "failOnExceedThreshold": True},
                    "fieldValidations": {"age": {"range": {"min": 0, "max": 10}}},
                }
            },
        )

        with pytest.raises(Exception, match="threshold"):
            run(tmp_path, PEOPLE, schema)

        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_rules_for_a_column_the_file_lacks_only_warn(self, tmp_path):
        schema = schema_of(
            {**PEOPLE_PROPERTIES, "score": {"type": "number"}},
            **{
                "x-validation": {
                    "badRowsHandling": {"maxBadRowsPercent": 100},
                    "fieldValidations": {
                        "age": {"range": {"min": 0, "max": 150}},
                        "score": {"required": True, "range": {"min": 0, "max": 10}},
                    },
                }
            },
        )

        results, out = run(tmp_path, PEOPLE, schema)

        assert data(out).num_rows == 3
        assert any("'score'" in w and "not checked" in w for w in results.warnings)

    def test_a_misspelled_column_is_an_error(self, tmp_path):
        schema = schema_of(
            PEOPLE_PROPERTIES,
            **{"x-validation": {"fieldValidations": {"agee": {"range": {"min": 0}}}}},
        )

        with pytest.raises(ValueError, match="agee"):
            run(tmp_path, PEOPLE, schema)

    def test_x_data_quality_reports_but_keeps_the_rows(self, tmp_path):
        schema = schema_of(
            PEOPLE_PROPERTIES,
            **{"x-dataQuality": {"fieldSpecificRules": {"age": {"max": 100}}}},
        )

        results, out = run(tmp_path, PEOPLE, schema)

        assert data(out).num_rows == 4
        assert any(key.endswith(":age") for key in results.validation_summary)
        assert "x-dataQuality" in results.schema_extensions


# ---------------------------------------------------------------------------- keys and constraints


class TestConstraints:
    def test_duplicate_primary_keys_keep_the_first_row(self, tmp_path):
        schema = schema_of(PEOPLE_PROPERTIES, **{"x-primaryKey": {"columns": ["id"]}})

        results, out = run(tmp_path, PEOPLE, schema)

        assert data(out).column("name").to_pylist() == ["Ann", "Bob", "Cy"]
        rejected = bad(out)
        assert rejected.column("name").to_pylist() == ["Di"]
        assert "id" in rejected.column("_rejection_reason")[0].as_py()
        assert results.invalid_rows == 1

    def test_null_primary_keys_are_rejected(self, tmp_path):
        schema = schema_of(PEOPLE_PROPERTIES, **{"x-primaryKey": {"columns": ["id"]}})

        _, out = run(tmp_path, "id,name,age\n1,Ann,1\n,Bob,2\n", schema)

        assert data(out).column("name").to_pylist() == ["Ann"]
        assert bad(out).column("name").to_pylist() == ["Bob"]

    def test_unique_constraints_across_columns(self, tmp_path):
        schema = schema_of(
            PEOPLE_PROPERTIES,
            **{"x-uniqueConstraints": [{"name": "u", "columns": ["name", "age"]}]},
        )

        _, out = run(tmp_path, "id,name,age\n1,Ann,1\n2,Ann,2\n3,Ann,1\n", schema)

        assert data(out).column("id").to_pylist() == [1, 2]
        assert bad(out).column("id").to_pylist() == ["3"]

    def test_property_constraints_reject_rows(self, tmp_path):
        schema = schema_of(
            {
                "id": {"type": "integer"},
                "name": {"type": "string", "maxLength": 3},
                "age": {"type": "integer", "minimum": 18},
            }
        )

        results, out = run(tmp_path, "id,name,age\n1,Ann,30\n2,Bobby,40\n3,Cy,5\n", schema)

        assert data(out).column("id").to_pylist() == [1]
        assert sorted(bad(out).column("id").to_pylist()) == ["2", "3"]
        assert results.invalid_rows == 2

    def test_state_is_kept_across_batches(self, tmp_path):
        schema = schema_of(PEOPLE_PROPERTIES, **{"x-primaryKey": {"columns": ["id"]}})
        rows = "".join(f"{i % 7},n{i},1\n" for i in range(40))

        results, out = run(tmp_path, "id,name,age\n" + rows, schema, batch_size=3)

        assert sorted(data(out).column("id").to_pylist()) == list(range(7))
        assert results.invalid_rows == 33 and results.total_rows == 40

    def test_a_key_column_that_is_not_in_the_file_is_an_error(self, tmp_path):
        schema = schema_of(PEOPLE_PROPERTIES, **{"x-primaryKey": {"columns": ["identifier"]}})

        with pytest.raises(ValueError, match="identifier"):
            run(tmp_path, PEOPLE, schema)

        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_key_columns_use_the_names_after_mapping(self, tmp_path):
        schema = schema_of(
            {"Id": {"type": "integer"}},
            **{
                "x-columnMapping": {"explicitMappings": {"Id": "id"}},
                "x-primaryKey": {"columns": ["id"]},
            },
        )

        _, out = run(tmp_path, "Id\n1\n1\n", schema)

        assert data(out).column("id").to_pylist() == [1]
        assert bad(out).num_rows == 1


class TestErrorModes:
    @staticmethod
    def schema(mode):
        return schema_of(
            PEOPLE_PROPERTIES,
            **{
                "x-primaryKey": {"columns": ["id"]},
                "x-constraintHandling": {"errorMode": mode},
            },
        )

    def test_fail_fast_stops_at_the_first_violation(self, tmp_path):
        with pytest.raises(ValueError):
            run(tmp_path, PEOPLE, self.schema("fail_fast"))

        assert not (tmp_path / "out" / "data.parquet").exists()
        assert not (tmp_path / "out" / "bad_rows.parquet").exists()

    def test_fail_complete_checks_everything_then_fails_without_output(self, tmp_path):
        with pytest.raises(ValueError):
            run(tmp_path, PEOPLE, self.schema("fail_complete"))

        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_bad_rows_is_the_default(self, tmp_path):
        results, out = run(tmp_path, PEOPLE, self.schema("bad_rows"))

        assert results.invalid_rows == 1 and data(out).num_rows == 3

    def test_an_unknown_mode_is_refused_before_processing(self, tmp_path):
        with pytest.raises(ValueError, match="errorMode"):
            run(tmp_path, PEOPLE, self.schema("ignore"))


# ------------------------------------------------------------------------------------ x-rowHash


class TestRowHash:
    SCHEMA = schema_of(
        PEOPLE_PROPERTIES,
        **{
            "x-primaryKey": {"columns": ["id"]},
            "x-rowHash": {
                "enabled": True,
                "inputHashEnabled": True,
                "rowNumberEnabled": True,
            },
        },
    )

    def test_hash_columns_are_added_and_row_numbers_point_into_the_file(self, tmp_path):
        results, out = run(tmp_path, PEOPLE, self.SCHEMA)

        table = data(out)
        assert table.schema.names == [
            "id",
            "name",
            "age",
            "row_hash",
            "_input_hash",
            "_rownum_in_source_file",
            "_rownum",
        ]
        # The duplicate id (file row 4) is not in the output; the others keep their file position
        assert table.column("_rownum_in_source_file").to_pylist() == [1, 2, 3]
        assert len(set(table.column("row_hash").to_pylist())) == 3
        assert len(set(table.column("_input_hash").to_pylist())) == 3
        assert "x-rowHash" in results.schema_extensions

    def test_no_internal_column_reaches_a_file(self, tmp_path):
        _, out = run(tmp_path, PEOPLE, self.SCHEMA)

        for name in ("data.parquet", "bad_rows.parquet"):
            assert not [n for n in pq.read_table(out / name).schema.names if "__forklift" in n]

    def test_row_numbers_are_global_across_batches(self, tmp_path):
        rows = "".join(f"{i},n{i},1\n" for i in range(10))

        _, out = run(tmp_path, "id,name,age\n" + rows, self.SCHEMA, batch_size=3)

        assert data(out).column("_rownum_in_source_file").to_pylist() == list(range(1, 11))

    def test_the_same_input_gives_the_same_hashes(self, tmp_path):
        _, first = run(tmp_path, PEOPLE, self.SCHEMA)
        hashes = data(first).column("row_hash").to_pylist()

        second_dir = tmp_path / "second"
        second_dir.mkdir()
        _, second = run(second_dir, PEOPLE, self.SCHEMA)

        assert data(second).column("row_hash").to_pylist() == hashes

    def test_a_header_name_that_clashes_with_the_hash_column_is_refused(self, tmp_path):
        schema = schema_of({}, **{"x-rowHash": {"enabled": True}})

        with pytest.raises(ValueError, match="row_hash"):
            run(tmp_path, "id,row_hash\n1,x\n", schema)

    def test_reserved_header_names_are_refused(self, tmp_path):
        with pytest.raises(ValueError, match="reserved"):
            run(tmp_path, "id,__forklift_row_id\n1,2\n", self.SCHEMA)


# --------------------------------------------------------------------------- switching it off


class TestOptOut:
    SCHEMA = schema_of(
        PEOPLE_PROPERTIES,
        **{
            "x-primaryKey": {"columns": ["id"]},
            "x-calculatedColumns": {"constants": [{"name": "c", "value": "x"}]},
            "x-columnMapping": {"explicitMappings": {"name": "full_name"}},
        },
    )

    def test_apply_schema_extensions_false_ignores_the_extensions(self, tmp_path):
        results, out = run(tmp_path, PEOPLE, self.SCHEMA, apply_schema_extensions=False)

        table = data(out)
        assert table.schema.names == ["id", "name", "age"]
        assert table.num_rows == 4
        assert results.schema_extensions == [] and results.warnings == []

    def test_types_and_required_still_apply(self, tmp_path):
        schema = dict(self.SCHEMA, required=["name"])

        results, out = run(
            tmp_path, "id,name,age\n1,,5\n2,Bob,6\n", schema, apply_schema_extensions=False
        )

        assert data(out).column("id").to_pylist() == [2]
        assert results.invalid_rows == 1

    def test_the_cli_flag(self, tmp_path, capsys):
        source = tmp_path / "in.csv"
        source.write_text(PEOPLE)
        schema = tmp_path / "schema.json"
        schema.write_text(json.dumps(self.SCHEMA))

        cli_main(
            [
                "ingest",
                str(source),
                "--dest",
                str(tmp_path / "off"),
                "--input-kind",
                "csv",
                "--schema",
                str(schema),
                "--no-schema-extensions",
            ]
        )
        cli_main(
            [
                "ingest",
                str(source),
                "--dest",
                str(tmp_path / "on"),
                "--input-kind",
                "csv",
                "--schema",
                str(schema),
            ]
        )

        assert pq.read_table(tmp_path / "off" / "data.parquet").num_rows == 4
        assert pq.read_table(tmp_path / "on" / "data.parquet").num_rows == 3
        assert "Schema extensions applied" in capsys.readouterr().out


# ----------------------------------------------------------------------------- warnings


class TestWarnings:
    def test_content_no_processor_reads_is_reported_and_does_not_stop_the_import(self, tmp_path):
        schema = schema_of(
            PEOPLE_PROPERTIES,
            **{
                "x-pii": {"fields": {"name": {"isPII": True}}},
                "x-validation": {"crossFieldValidations": [{"name": "n", "rule": "r"}]},
            },
        )

        results, out = run(tmp_path, PEOPLE, schema)

        assert data(out).num_rows == 4
        assert any(w.startswith("x-pii") for w in results.warnings)
        assert any("crossFieldValidations" in w for w in results.warnings)

    def test_warnings_are_printed_by_the_cli(self, tmp_path, capsys):
        source = tmp_path / "in.csv"
        source.write_text(PEOPLE)
        schema = tmp_path / "schema.json"
        schema.write_text(json.dumps(schema_of(PEOPLE_PROPERTIES, **{"x-pii": {"fields": {}}})))

        cli_main(
            [
                "ingest",
                str(source),
                "--dest",
                str(tmp_path / "out"),
                "--input-kind",
                "csv",
                "--schema",
                str(schema),
            ]
        )

        assert "x-pii" in capsys.readouterr().err

    def test_a_value_is_never_part_of_a_finding(self, tmp_path):
        schema = schema_of(
            PEOPLE_PROPERTIES,
            **{
                "x-validation": {
                    "badRowsHandling": {"maxBadRowsPercent": 100},
                    "fieldValidations": {"age": {"range": {"min": 0, "max": 150}}},
                },
                "x-primaryKey": {"columns": ["id"]},
            },
        )

        results, out = run(tmp_path, "id,name,age\n1,SECRETNAME,999\n1,Other,5\n", schema)

        reasons = " ".join(bad(out).column("_rejection_reason").to_pylist())
        assert "SECRETNAME" not in reasons and "999" not in reasons
        assert "SECRETNAME" not in json.dumps(results.validation_summary)


# ------------------------------------------------------------------------- the shipped standard


class TestShippedStandard:
    HEADER = (
        "id,name,age,salary,birth_date,created_timestamp,ssn,zip_code,phone_number,email_address"
    )
    FIRST_ROWS = [
        '1,  ann   LEE ,30,"$55,000.00",1/2/1990,2020-01-01 10:00:00,123456789,02134,'
        "5551234567, Ann@Example.COM",
        "2,bob ray,17,0.00,1985-03-04,2021-03-04T05:06:07Z,111-22-3333,10001,"
        "(555) 123-4567,bob@example.com",
        # same id as the row above
        "2,dup dan,70,120000,1980-01-01,2019-01-01 00:00:00,222-33-4444,10001,"
        "5551230000,dan@example.com",
    ]

    @staticmethod
    def more_rows(first, last):
        letters = "abcdefghijklmnopqrstuvwxyz"
        return [
            f"{i},{letters[i % 26]}ra smith,{20 + i},{50000 + i},1990-01-{i % 28 + 1:02d},"
            f"2020-01-01 10:00:00,{300000000 + i},02134,555123{4000 + i},p{i}@example.com"
            for i in range(first, last)
        ]

    @pytest.fixture()
    def imported(self, tmp_path):
        # 1 of 22 rows is bad: under the standard's 10 % threshold
        rows = [self.HEADER] + self.FIRST_ROWS + self.more_rows(3, 22)
        return run(tmp_path, "\n".join(rows) + "\n", json.loads(STANDARD_CSV.read_text()))

    def test_the_standard_runs(self, imported):
        results, out = imported

        assert results.total_rows == 22 and results.invalid_rows == 1
        table = data(out)
        assert table.num_rows == 21
        assert "x-pii is documentation only: no masking is applied" in results.warnings

    def test_it_cleans_renames_and_adds_columns(self, imported):
        _, out = imported
        first = data(out).slice(0, 1).to_pylist()[0]

        assert first["name"] == "Ann Lee"
        assert first["email_address"] == "ann@example.com"
        assert first["salary"] == 55000.0
        assert first["birth_date"].isoformat() == "1990-01-02"
        assert first["social_security_number"] == "123-45-6789"  # ssn -> mapped, special type
        assert first["phone_number"] == "(555) 123-4567"
        assert first["data_source"] == "census_2020"
        assert first["age_category"] == "adult"
        assert first["salary_tier"] == "mid"
        assert first["name_length"] == len("Ann Lee")

    def test_the_duplicate_id_is_rejected_with_its_reason(self, imported):
        _, out = imported
        rejected = bad(out)

        assert rejected.column("id").to_pylist() == ["2"]
        # In the shape of the input file, with the values as the validation stage saw them
        assert rejected.schema.names[:3] == ["id", "name", "age"]
        assert rejected.column("name").to_pylist() == ["Dup Dan"]
        assert rejected.column("ssn").to_pylist() == ["222-33-4444"]
        assert "id" in rejected.column("_rejection_reason")[0].as_py()

    def test_the_standard_has_nothing_that_no_processor_reads(self):
        from forklift.processors.schema_extensions import unsupported_extension_keys

        assert unsupported_extension_keys(json.loads(STANDARD_CSV.read_text())) == [
            "x-pii is documentation only: no masking is applied"
        ]
