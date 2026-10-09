"""Tests for the schema extension loaders (``forklift.processors.schema_extensions``).

Covers ``build_column_mapper``, ``build_constraint_validator``, ``build_data_validator``,
``build_quality_processor``, ``referenced_columns``, ``unsupported_extension_keys`` and the
supporting changes in ``ColumnMapper.output_names``, ``ConstraintValidator``,
``DataValidationProcessor`` and ``DataQualityProcessor``.
"""

import json
import os
from pathlib import Path

import pyarrow as pa
import pytest

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
from forklift.processors.data_validation.data_validation_processor import (
    BadRowsThresholdExceededError,
)
from forklift.processors.quality import DataQualityProcessor
from forklift.processors.schema_extensions import (
    build_column_mapper,
    build_constraint_validator,
    build_data_validator,
    build_quality_processor,
    referenced_columns,
    unsupported_extension_keys,
)

STANDARD_CSV = Path(__file__).resolve().parents[2] / "schema-standards" / "20250826-csv.json"


def make_batch(**columns):
    return pa.RecordBatch.from_pydict(columns)


def values(batch, name):
    return batch.column(name).to_pylist()


def rejected(results):
    """``(row_index, column_name, error_code)`` of every result, sorted."""
    return sorted((r.row_index, r.column_name, r.error_code) for r in results)


# ====================================================================== ColumnMapper.output_names

ALL_NAMES = ["A", "StateID", "first_name", "LastName", "XMLParser", "Col 1", "_rownum", "a1"]

OUTPUT_NAME_CONFIGS = {
    "explicit": dict(explicit_mappings={"A": "Alpha", "Col 1": "col_one"}),
    "explicit_case_insensitive": dict(
        explicit_mappings={"a": "Alpha", "stateid": "state"}, case_sensitive=False
    ),
    "snake_case": dict(naming_convention="snake_case"),
    "camelCase": dict(naming_convention="camelCase"),
    "PascalCase": dict(naming_convention="PascalCase"),
    "lowercase": dict(naming_convention="lowercase"),
    "UPPERCASE": dict(naming_convention="UPPERCASE"),
    "explicit_and_convention": dict(
        explicit_mappings={"A": "StateCode"}, naming_convention="snake_case"
    ),
    "custom_transform": dict(custom_transform=lambda name: name + "_x"),
    "all_three": dict(
        explicit_mappings={"A": "Alpha"},
        naming_convention="snake_case",
        custom_transform=lambda name: "c_" + name,
    ),
    "drop_unmapped": dict(
        explicit_mappings={"A": "Alpha", "StateID": "StateID"}, drop_unmapped=True
    ),
    "drop_unmapped_with_convention": dict(
        explicit_mappings={"LastName": "LastName"},
        naming_convention="snake_case",
        drop_unmapped=True,
    ),
    "drop_everything": dict(drop_unmapped=True),
    "nothing": dict(),
}


class TestColumnMapperOutputNames:
    @pytest.mark.parametrize("config_name", sorted(OUTPUT_NAME_CONFIGS))
    def test_output_names_match_process_batch(self, config_name):
        mapper = ColumnMapper(ColumnMappingConfig(**OUTPUT_NAME_CONFIGS[config_name]))
        batch = pa.RecordBatch.from_arrays(
            [pa.array([i]) for i, _ in enumerate(ALL_NAMES)], names=ALL_NAMES
        )

        planned = mapper.output_names(ALL_NAMES)
        mapped_batch, _ = mapper.process_batch(batch)

        assert list(planned) == ALL_NAMES
        assert mapped_batch.schema.names == [n for n in planned.values() if n is not None]
        # the data of every kept column moved with its name
        for source, output in planned.items():
            if output is not None:
                assert values(mapped_batch, output) == values(batch, source)

    def test_dropped_columns_map_to_none(self):
        mapper = ColumnMapper(
            ColumnMappingConfig(explicit_mappings={"A": "Alpha"}, drop_unmapped=True)
        )
        assert mapper.output_names(["A", "B", "C"]) == {"A": "Alpha", "B": None, "C": None}

    def test_accepts_any_sequence_and_does_not_need_data(self):
        mapper = ColumnMapper(ColumnMappingConfig(naming_convention="snake_case"))
        assert mapper.output_names(("FirstName", "LastName")) == {
            "FirstName": "first_name",
            "LastName": "last_name",
        }
        assert mapper.output_names([]) == {}

    def test_collision_raises_the_same_error_as_process_batch(self):
        mapper = ColumnMapper(ColumnMappingConfig(explicit_mappings={"A": "x", "B": "x"}))
        batch = pa.RecordBatch.from_arrays([pa.array([1]), pa.array([2])], names=["A", "B"])

        with pytest.raises(ValueError) as from_names:
            mapper.output_names(["A", "B"])
        with pytest.raises(ValueError) as from_batch:
            mapper.process_batch(batch)

        assert str(from_names.value) == str(from_batch.value)
        assert "duplicate output column names" in str(from_names.value)

    def test_collision_through_the_naming_convention(self):
        mapper = ColumnMapper(ColumnMappingConfig(naming_convention="snake_case"))
        with pytest.raises(ValueError, match="duplicate output column names"):
            mapper.output_names(["FirstName", "first_name"])

    def test_dropped_columns_do_not_collide(self):
        mapper = ColumnMapper(
            ColumnMappingConfig(explicit_mappings={"A": "x", "B": "x"}, drop_unmapped=True)
        )
        # only A is present: B's mapping is irrelevant
        assert mapper.output_names(["A", "C"]) == {"A": "x", "C": None}

    def test_duplicate_input_names_raise_like_process_batch(self):
        mapper = ColumnMapper(ColumnMappingConfig())
        batch = pa.RecordBatch.from_arrays([pa.array([1]), pa.array([2])], names=["a", "a"])
        with pytest.raises(ValueError):
            mapper.process_batch(batch)
        with pytest.raises(ValueError):
            mapper.output_names(["a", "a"])

    def test_bad_custom_transform_raises(self):
        mapper = ColumnMapper(ColumnMappingConfig(custom_transform=lambda name: None))
        with pytest.raises(ValueError, match="custom_transform"):
            mapper.output_names(["a"])


# ======================================================================== build_column_mapper


class TestBuildColumnMapper:
    def test_absent_or_empty_gives_none(self):
        assert build_column_mapper({}) is None
        assert build_column_mapper({"x-columnMapping": None}) is None
        assert build_column_mapper({"x-columnMapping": {}}) is None
        assert build_column_mapper({"x-columnMapping": {"description": "only docs"}}) is None
        assert (
            build_column_mapper(
                {"x-columnMapping": {"explicitMappings": {}, "caseSensitive": False}}
            )
            is None
        )

    def test_standard_shape_maps_onto_the_config(self):
        mapper = build_column_mapper(
            {
                "x-columnMapping": {
                    "explicitMappings": {"DOB": "birth_date"},
                    "namingConvention": "camelCase",
                    "caseSensitive": False,
                    "allowUnmapped": True,
                    "dropUnmapped": False,
                }
            }
        )
        config = mapper.config
        assert isinstance(mapper, ColumnMapper)
        assert config.explicit_mappings == {"DOB": "birth_date"}
        assert config.naming_convention == "camelCase"
        assert config.case_sensitive is False
        assert config.allow_unmapped is True
        assert config.drop_unmapped is False

    def test_renames_columns(self):
        mapper = build_column_mapper(
            {
                "x-columnMapping": {
                    "explicitMappings": {"Addr1": "address_line_1"},
                    "namingConvention": "snake_case",
                }
            }
        )
        batch = make_batch(Addr1=["x"], FirstName=["y"])
        mapped, results = mapper.process_batch(batch)
        assert mapped.schema.names == ["address_line_1", "first_name"]
        assert results == []

    @pytest.mark.parametrize(
        "convention", ["snake_case", "camelCase", "PascalCase", "lowercase", "UPPERCASE"]
    )
    def test_every_convention_is_accepted(self, convention):
        mapper = build_column_mapper({"x-columnMapping": {"namingConvention": convention}})
        assert mapper.config.naming_convention == convention

    def test_drop_unmapped(self):
        mapper = build_column_mapper(
            {"x-columnMapping": {"explicitMappings": {"A": "a"}, "dropUnmapped": True}}
        )
        assert mapper.output_names(["A", "B"]) == {"A": "a", "B": None}

    def test_allow_unmapped_false_drops_unmapped_columns(self):
        mapper = build_column_mapper(
            {"x-columnMapping": {"explicitMappings": {"A": "a"}, "allowUnmapped": False}}
        )
        assert mapper.output_names(["A", "B"]) == {"A": "a", "B": None}

    def test_drop_unmapped_alone_is_a_mapper(self):
        mapper = build_column_mapper({"x-columnMapping": {"dropUnmapped": True}})
        assert mapper.output_names(["A"]) == {"A": None}

    def test_identity_mapping_counts_as_mapped(self):
        mapper = build_column_mapper(
            {"x-columnMapping": {"explicitMappings": {"A": "A"}, "dropUnmapped": True}}
        )
        assert mapper.output_names(["A", "B"]) == {"A": "A", "B": None}

    def test_case_insensitive_mapping(self):
        mapper = build_column_mapper(
            {"x-columnMapping": {"explicitMappings": {"firstname": "fn"}, "caseSensitive": False}}
        )
        assert mapper.output_names(["FirstName"]) == {"FirstName": "fn"}

    def test_unsupported_keys_do_not_raise(self):
        mapper = build_column_mapper(
            {
                "x-columnMapping": {
                    "explicitMappings": {"A": "a"},
                    "standardizationRules": {"maxLength": 64},
                    "globalMappings": {"x": "y"},
                    "patternMappings": [],
                }
            }
        )
        assert mapper.output_names(["A"]) == {"A": "a"}

    @pytest.mark.parametrize(
        "section, message",
        [
            ("text", "x-columnMapping: must be an object"),
            ([], "x-columnMapping: must be an object"),
            ({"explicitMappings": ["A"]}, "x-columnMapping.explicitMappings: must be an object"),
            ({"explicitMappings": {"A": 1}}, "x-columnMapping.explicitMappings.A"),
            ({"explicitMappings": {"A": ""}}, "x-columnMapping.explicitMappings.A"),
            ({"explicitMappings": {"": "a"}}, "x-columnMapping.explicitMappings key"),
            ({"explicitMappings": {"A": None}}, "x-columnMapping.explicitMappings.A"),
            ({"namingConvention": "kebab-case"}, "x-columnMapping.namingConvention"),
            ({"namingConvention": ""}, "x-columnMapping.namingConvention"),
            ({"namingConvention": 3}, "x-columnMapping.namingConvention"),
            ({"caseSensitive": "no"}, "x-columnMapping.caseSensitive"),
            ({"allowUnmapped": 1}, "x-columnMapping.allowUnmapped"),
            ({"dropUnmapped": "true"}, "x-columnMapping.dropUnmapped"),
        ],
    )
    def test_invalid_configuration_raises(self, section, message):
        with pytest.raises(ValueError) as error:
            build_column_mapper({"x-columnMapping": section})
        assert message in str(error.value)

    def test_invalid_convention_names_the_valid_ones(self):
        with pytest.raises(ValueError) as error:
            build_column_mapper({"x-columnMapping": {"namingConvention": "nope"}})
        for valid in ("snake_case", "camelCase", "PascalCase", "lowercase", "UPPERCASE"):
            assert valid in str(error.value)

    def test_keys_that_differ_only_in_case_with_different_targets_are_ambiguous(self):
        with pytest.raises(ValueError, match="differ only in case"):
            build_column_mapper(
                {
                    "x-columnMapping": {
                        "explicitMappings": {"A": "x", "a": "y"},
                        "caseSensitive": False,
                    }
                }
            )
        # same target: harmless
        assert build_column_mapper(
            {
                "x-columnMapping": {
                    "explicitMappings": {"A": "x", "a": "x"},
                    "caseSensitive": False,
                }
            }
        )
        # case sensitive: two different headers
        assert build_column_mapper({"x-columnMapping": {"explicitMappings": {"A": "x", "a": "y"}}})

    def test_non_dict_schema_raises(self):
        with pytest.raises(ValueError, match="schema must be a dictionary"):
            build_column_mapper(["x-columnMapping"])


# ================================================================== build_constraint_validator


def pk_schema(**overrides):
    section = {"columns": ["id"], "type": "single", "enforceUniqueness": True, "allowNulls": False}
    section.update(overrides)
    return {"x-primaryKey": section}


class TestBuildConstraintValidatorNone:
    def test_absent_gives_none(self):
        assert build_constraint_validator({}) is None
        assert build_constraint_validator({"properties": {"a": {"type": "string"}}}) is None
        assert build_constraint_validator({"x-primaryKey": None}) is None
        assert build_constraint_validator({"x-uniqueConstraints": None}) is None
        assert build_constraint_validator({"x-uniqueConstraints": []}) is None

    def test_nothing_to_enforce_gives_none(self):
        schema = pk_schema(enforceUniqueness=False, allowNulls=True)
        assert build_constraint_validator(schema) is None

    def test_error_mode_alone_gives_none(self):
        assert (
            build_constraint_validator({"x-constraintHandling": {"errorMode": "fail_fast"}})
            is None
        )

    def test_invalid_error_mode_raises_even_without_constraints(self):
        with pytest.raises(ValueError, match="errorMode"):
            build_constraint_validator({"x-constraintHandling": {"errorMode": "nope"}})

    def test_properties_without_constraints_do_not_call_the_resolver(self):
        calls = []

        def resolve(name):
            calls.append(name)
            return name

        schema = {"properties": {"a": {"type": "string"}, "b": {"minimum": 1}}}
        build_constraint_validator(schema, resolve_column=resolve)
        assert calls == ["b"]


class TestPrimaryKey:
    def test_single_column_is_unique_and_not_null(self):
        validator = build_constraint_validator(pk_schema())
        assert isinstance(validator, ConstraintValidator)
        assert validator.config.unique_constraints == ["id"]
        assert [spec["column"] for spec in validator.config.check_constraints.values()] == ["id"]
        assert validator.config.check_constraints["primary_key_id_not_null"] == {
            "column": "id",
            "nullable": False,
        }

    def test_composite_key_is_a_tuple(self):
        validator = build_constraint_validator(
            pk_schema(columns=["order_id", "line"], type="composite")
        )
        assert validator.config.unique_constraints == [("order_id", "line")]

    def test_duplicates_are_rejected_with_attribution(self):
        validator = build_constraint_validator(pk_schema())
        batch = make_batch(id=[1, 2, 1, 3, 2], name=list("abcde"))

        kept, results = validator.process_batch(batch)

        assert values(kept, "id") == [1, 2, 3]
        assert values(kept, "name") == ["a", "b", "d"]
        assert rejected(results) == [(2, "id", "UNIQUE_VIOLATION"), (4, "id", "UNIQUE_VIOLATION")]

    def test_duplicates_across_batches(self):
        validator = build_constraint_validator(pk_schema())
        first, _ = validator.process_batch(make_batch(id=[1, 2]))
        second, results = validator.process_batch(make_batch(id=[3, 1]))
        assert values(first, "id") == [1, 2]
        assert values(second, "id") == [3]
        assert rejected(results) == [(1, "id", "UNIQUE_VIOLATION")]

    def test_null_primary_keys_are_rejected_in_bad_rows_mode(self):
        validator = build_constraint_validator(pk_schema())
        batch = make_batch(id=[1, None, 2, None], name=list("abcd"))

        kept, results = validator.process_batch(batch)

        assert values(kept, "id") == [1, 2]
        assert values(kept, "name") == ["a", "c"]
        assert rejected(results) == [(1, "id", "NULL_VIOLATION"), (3, "id", "NULL_VIOLATION")]

    def test_null_in_any_part_of_a_composite_key_is_rejected(self):
        validator = build_constraint_validator(pk_schema(columns=["a", "b"], type="composite"))
        batch = make_batch(a=[1, None, 2, 3], b=["x", "y", None, "z"])

        kept, results = validator.process_batch(batch)

        assert values(kept, "a") == [1, 3]
        assert rejected(results) == [(1, "a", "NULL_VIOLATION"), (2, "b", "NULL_VIOLATION")]

    def test_a_null_row_does_not_claim_its_other_key_parts(self):
        validator = build_constraint_validator(pk_schema(columns=["a", "b"], type="composite"))
        validator.process_batch(make_batch(a=[1], b=[None]))
        kept, results = validator.process_batch(make_batch(a=[1], b=["x"]))
        assert values(kept, "a") == [1]
        assert results == []

    def test_allow_nulls_keeps_null_keys_and_does_not_compare_them(self):
        validator = build_constraint_validator(pk_schema(allowNulls=True))
        assert "primary_key_id_not_null" not in validator.config.check_constraints

        kept, results = validator.process_batch(make_batch(id=[None, None, 1, 1]))

        assert values(kept, "id") == [None, None, 1]
        assert rejected(results) == [(3, "id", "UNIQUE_VIOLATION")]

    def test_enforce_uniqueness_false_still_rejects_nulls(self):
        validator = build_constraint_validator(pk_schema(enforceUniqueness=False))
        assert validator.config.unique_constraints == []

        kept, results = validator.process_batch(make_batch(id=[1, 1, None]))

        assert values(kept, "id") == [1, 1]
        assert rejected(results) == [(2, "id", "NULL_VIOLATION")]

    def test_type_is_optional_and_must_match_the_columns(self):
        schema = {"x-primaryKey": {"columns": ["a", "b"]}}
        assert build_constraint_validator(schema).config.unique_constraints == [("a", "b")]
        with pytest.raises(ValueError, match=r"x-primaryKey\.type"):
            build_constraint_validator(pk_schema(columns=["a", "b"], type="single"))
        with pytest.raises(ValueError, match=r"x-primaryKey\.type"):
            build_constraint_validator(pk_schema(columns=["a"], type="composite"))
        with pytest.raises(ValueError, match=r"x-primaryKey\.type"):
            build_constraint_validator(pk_schema(type="natural"))

    def test_unsupported_primary_key_keys_do_not_raise(self):
        validator = build_constraint_validator(pk_schema(description="d", description_detail="x"))
        assert validator is not None

    @pytest.mark.parametrize(
        "section, message",
        [
            ({}, r"x-primaryKey\.columns: is required"),
            ({"type": "single"}, r"x-primaryKey\.columns: is required"),
            ({"columns": []}, r"x-primaryKey\.columns: must not be empty"),
            ({"columns": "id"}, r"x-primaryKey\.columns: must be a list"),
            ({"columns": [1]}, r"x-primaryKey\.columns\[0\]: must be a non-empty string"),
            ({"columns": ["a", ""]}, r"x-primaryKey\.columns\[1\]"),
            ({"columns": ["a", None]}, r"x-primaryKey\.columns\[1\]"),
            ({"columns": ["a", "a"]}, r"x-primaryKey\.columns: column 'a' is listed more than"),
            ({"columns": ["id"], "enforceUniqueness": "yes"}, r"x-primaryKey\.enforceUniqueness"),
            ({"columns": ["id"], "allowNulls": 0}, r"x-primaryKey\.allowNulls"),
        ],
    )
    def test_invalid_primary_key_raises(self, section, message):
        with pytest.raises(ValueError, match=message):
            build_constraint_validator({"x-primaryKey": section})

    @pytest.mark.parametrize("value", ["id", ["id"], 5, True])
    def test_primary_key_must_be_an_object(self, value):
        with pytest.raises(ValueError, match="x-primaryKey: must be an object"):
            build_constraint_validator({"x-primaryKey": value})


class TestUniqueConstraints:
    def test_single_and_composite_constraints(self):
        schema = {
            "x-uniqueConstraints": [
                {"name": "u_email", "columns": ["email"], "description": "d"},
                {"name": "u_pair", "columns": ["a", "b"]},
            ]
        }
        validator = build_constraint_validator(schema)
        assert validator.config.unique_constraints == ["email", ("a", "b")]
        assert validator.config.check_constraints == {}

    def test_duplicates_are_rejected(self):
        schema = {"x-uniqueConstraints": [{"name": "u", "columns": ["a", "b"]}]}
        validator = build_constraint_validator(schema)
        batch = make_batch(a=[1, 1, 2, 1], b=["x", "y", "x", "x"])

        kept, results = validator.process_batch(batch)

        assert values(kept, "a") == [1, 1, 2]
        assert rejected(results) == [(3, "a", "UNIQUE_VIOLATION")]

    def test_null_keys_are_not_compared(self):
        schema = {"x-uniqueConstraints": [{"columns": ["a", "b"]}]}
        validator = build_constraint_validator(schema)

        kept, results = validator.process_batch(
            make_batch(a=[1, 1, None, None], b=[None, None, 1, 1])
        )

        assert kept.num_rows == 4
        assert results == []

    def test_name_is_optional(self):
        assert build_constraint_validator({"x-uniqueConstraints": [{"columns": ["a"]}]})

    def test_unsupported_item_keys_do_not_raise(self):
        schema = {
            "x-uniqueConstraints": [
                {
                    "name": "u",
                    "columns": ["a"],
                    "condition": "status = 'active'",
                    "ignoreNulls": False,
                    "caseSensitive": False,
                }
            ]
        }
        validator = build_constraint_validator(schema)
        assert validator.config.unique_constraints == ["a"]

    @pytest.mark.parametrize(
        "section, message",
        [
            ({"columns": ["a"]}, "x-uniqueConstraints: must be a list"),
            ("a", "x-uniqueConstraints: must be a list"),
            (["a"], r"x-uniqueConstraints\[0\]: must be an object"),
            ([{"name": "u"}], r"x-uniqueConstraints\[0\]\.columns: is required"),
            ([{"columns": []}], r"x-uniqueConstraints\[0\]\.columns: must not be empty"),
            ([{"columns": "a"}], r"x-uniqueConstraints\[0\]\.columns: must be a list"),
            ([{"columns": [1]}], r"x-uniqueConstraints\[0\]\.columns\[0\]"),
            ([{"columns": ["a"]}, {"columns": [None]}], r"x-uniqueConstraints\[1\]\.columns\[0\]"),
            ([{"columns": ["a"], "name": ""}], r"x-uniqueConstraints\[0\]\.name"),
            ([{"columns": ["a"], "name": 3}], r"x-uniqueConstraints\[0\]\.name"),
            (
                [{"columns": ["a"], "name": "u"}, {"columns": ["b"], "name": "u"}],
                r"x-uniqueConstraints\[1\]\.name: 'u' is used by more than one",
            ),
            ([{"columns": ["a"], "ignoreNulls": "yes"}], r"x-uniqueConstraints\[0\]\.ignoreNulls"),
            ([{"columns": ["a"], "caseSensitive": 1}], r"x-uniqueConstraints\[0\]\.caseSensitive"),
        ],
    )
    def test_invalid_configuration_raises(self, section, message):
        with pytest.raises(ValueError, match=message):
            build_constraint_validator({"x-uniqueConstraints": section})


class TestDeduplication:
    def test_primary_key_repeated_in_unique_constraints_is_applied_once(self):
        schema = pk_schema()
        schema["x-uniqueConstraints"] = [{"name": "u_id", "columns": ["id"]}]
        validator = build_constraint_validator(schema)
        assert validator.config.unique_constraints == ["id"]

        _, results = validator.process_batch(make_batch(id=[1, 1]))
        assert rejected(results) == [(1, "id", "UNIQUE_VIOLATION")]  # reported once, not twice

    def test_composite_key_repeated_in_any_order(self):
        schema = pk_schema(columns=["a", "b"], type="composite")
        schema["x-uniqueConstraints"] = [
            {"name": "same", "columns": ["a", "b"]},
            {"name": "swapped", "columns": ["b", "a"]},
            {"name": "other", "columns": ["b", "c"]},
        ]
        validator = build_constraint_validator(schema)
        assert validator.config.unique_constraints == [("a", "b"), ("b", "c")]

    def test_x_unique_property_repeated(self):
        schema = pk_schema()
        schema["properties"] = {
            "id": {"type": "integer", "x-unique": True},
            "e": {"x-unique": True},
        }
        schema["x-uniqueConstraints"] = [{"columns": ["e"]}]
        validator = build_constraint_validator(schema)
        assert validator.config.unique_constraints == ["id", "e"]

    def test_pk_and_different_unique_constraint_are_both_kept(self):
        schema = pk_schema()
        schema["x-uniqueConstraints"] = [{"columns": ["email"]}]
        assert build_constraint_validator(schema).config.unique_constraints == ["id", "email"]


class TestConstraintHandling:
    @pytest.mark.parametrize(
        "mode, expected",
        [
            ("bad_rows", ErrorMode.BAD_ROWS),
            ("fail_fast", ErrorMode.FAIL_FAST),
            ("fail_complete", ErrorMode.FAIL_COMPLETE),
        ],
    )
    def test_error_mode(self, mode, expected):
        schema = pk_schema()
        schema["x-constraintHandling"] = {"errorMode": mode}
        assert build_constraint_validator(schema).config.error_mode == expected

    def test_error_mode_is_case_insensitive(self):
        schema = pk_schema()
        schema["x-constraintHandling"] = {"errorMode": "FAIL_FAST"}
        assert build_constraint_validator(schema).config.error_mode == ErrorMode.FAIL_FAST

    def test_default_error_mode_is_bad_rows(self):
        assert build_constraint_validator(pk_schema()).config.error_mode == ErrorMode.BAD_ROWS
        schema = pk_schema()
        schema["x-constraintHandling"] = {"description": "no errorMode"}
        assert build_constraint_validator(schema).config.error_mode == ErrorMode.BAD_ROWS

    @pytest.mark.parametrize("mode", ["ignore", "transform", "bad_row", "", None, 5, ["x"]])
    def test_invalid_error_mode_names_the_key_and_the_valid_values(self, mode):
        schema = pk_schema()
        schema["x-constraintHandling"] = {"errorMode": mode}
        with pytest.raises(ValueError) as error:
            build_constraint_validator(schema)
        message = str(error.value)
        assert "x-constraintHandling" in message and "errorMode" in message
        for valid in ("bad_rows", "fail_fast", "fail_complete"):
            assert valid in message

    def test_constraint_handling_must_be_an_object(self):
        with pytest.raises(ValueError, match="x-constraintHandling: must be an object"):
            build_constraint_validator({"x-constraintHandling": "bad_rows", **pk_schema()})

    def test_other_keys_do_not_raise(self):
        schema = pk_schema()
        schema["x-constraintHandling"] = {
            "errorMode": "bad_rows",
            "primaryKeyViolations": {"duplicates": "keep_first"},
            "badRowsOutput": {"enabled": True},
            "validationOptions": {"maxErrorsPerRow": 3},
        }
        assert build_constraint_validator(schema) is not None

    def test_fail_fast_raises_on_the_first_violation(self):
        schema = pk_schema()
        schema["x-constraintHandling"] = {"errorMode": "fail_fast"}
        validator = build_constraint_validator(schema)
        with pytest.raises(ValueError, match="violated"):
            validator.process_batch(make_batch(id=[1, 1]))

    def test_fail_complete_keeps_rows_and_raises_at_the_end(self):
        schema = pk_schema()
        schema["x-constraintHandling"] = {"errorMode": "fail_complete"}
        validator = build_constraint_validator(schema)

        kept, results = validator.process_batch(make_batch(id=[1, 1, None]))

        assert kept.num_rows == 3
        assert rejected(results) == [(1, "id", "UNIQUE_VIOLATION"), (2, "id", "NULL_VIOLATION")]
        with pytest.raises(ValueError, match="2 violations"):
            validator.finalize()


class TestPropertyConstraints:
    def test_property_keywords_become_checks(self):
        schema = {
            "properties": {
                "age": {"type": "integer", "minimum": 0, "maximum": 150},
                "cat": {"type": "string", "enum": ["A", "B"]},
                "code": {"type": "string", "pattern": "^[A-Z]{2}$"},
                "name": {"type": "string", "minLength": 2, "maxLength": 4},
                "email": {"type": "string", "x-unique": True},
                "free": {"type": "string"},
            }
        }
        validator = build_constraint_validator(schema)
        assert sorted(validator.config.check_constraints) == [
            "age_range",
            "cat_enum",
            "code_pattern",
            "name_length",
        ]
        assert validator.config.unique_constraints == ["email"]

        batch = make_batch(
            age=[10, -1, 151, None],
            cat=["A", "C", "B", None],
            code=["AB", "ab", "AB", None],
            name=["abc", "a", "abcde", None],
            email=["x", "y", "z", None],
            free=list("abcd"),
        )
        kept, results = validator.process_batch(batch)

        # rows 0 and 3 (all NULL, NULLs pass the value constraints) pass
        assert values(kept, "age") == [10, None]
        assert {(r.row_index, r.column_name) for r in results} == {
            (1, "age"),
            (2, "age"),
            (1, "cat"),
            (1, "code"),
            (1, "name"),
            (2, "name"),
        }
        assert all(r.error_code.endswith("_VIOLATION") for r in results)

    def test_null_values_pass_the_value_constraints(self):
        validator = build_constraint_validator({"properties": {"age": {"minimum": 0}}})
        kept, results = validator.process_batch(make_batch(age=[None, 5]))
        assert kept.num_rows == 2 and results == []

    def test_property_without_a_column_in_the_batch_is_skipped(self):
        validator = build_constraint_validator({"properties": {"age": {"minimum": 0}}})
        kept, results = validator.process_batch(make_batch(other=[1]))
        assert kept.num_rows == 1 and results == []

    def test_boolean_property_schemas_are_ignored(self):
        schema = {"properties": {"any": True, "age": {"minimum": 1}}}
        assert build_constraint_validator(schema) is not None

    def test_date_bounds_as_strings(self):
        schema = {"properties": {"d": {"minimum": "2020-01-01", "maximum": "2020-12-31"}}}
        validator = build_constraint_validator(schema)
        import datetime

        batch = make_batch(
            d=pa.array([datetime.date(2019, 12, 31), datetime.date(2020, 6, 1)], type=pa.date32())
        )
        kept, results = validator.process_batch(batch)
        assert kept.num_rows == 1
        assert rejected(results) == [(0, "d", "RANGE_VIOLATION")]

    @pytest.mark.parametrize(
        "definition, message",
        [
            ({"minimum": "abc"}, r"properties\.p\.minimum: must be a number or an ISO date"),
            ({"minimum": True}, r"properties\.p\.minimum"),
            ({"maximum": [1]}, r"properties\.p\.maximum"),
            ({"minimum": float("nan")}, r"properties\.p\.minimum: must be a finite"),
            ({"minimum": 5, "maximum": 1}, r"properties\.p \(minimum/maximum\): min is greater"),
            (
                {"minimum": "2021-01-01", "maximum": "2020-01-01"},
                r"properties\.p \(minimum/maximum\): min is greater",
            ),
            ({"minimum": 1, "maximum": "2020-01-01"}, "both be numbers or both be dates"),
            ({"enum": "AB"}, r"properties\.p\.enum: must be a list"),
            ({"enum": []}, r"properties\.p\.enum: must not be empty"),
            ({"pattern": 5}, r"properties\.p\.pattern: must be a string"),
            ({"pattern": "[unclosed"}, r"properties\.p\.pattern: Invalid regular expression"),
            ({"pattern": "^(a+)+$"}, r"properties\.p\.pattern: .*nested unbounded"),
            ({"minLength": -1}, r"properties\.p\.minLength: must be >= 0"),
            ({"minLength": 1.5}, r"properties\.p\.minLength: must be an integer"),
            ({"maxLength": "3"}, r"properties\.p\.maxLength: must be an integer"),
            ({"minLength": 5, "maxLength": 2}, r"minLength \(5\) is greater than maxLength \(2\)"),
            ({"x-unique": "yes"}, r"properties\.p\.x-unique: must be true or false"),
        ],
    )
    def test_invalid_property_constraints_raise(self, definition, message):
        with pytest.raises(ValueError, match=message):
            build_constraint_validator({"properties": {"p": definition}})

    def test_properties_must_be_an_object(self):
        with pytest.raises(ValueError, match="properties: must be an object"):
            build_constraint_validator({"properties": ["a"]})

    def test_null_keywords_are_ignored(self):
        schema = {"properties": {"p": {"minimum": None, "enum": None, "pattern": None}}}
        # nothing is checked, but nothing breaks either
        validator = build_constraint_validator(schema)
        kept, results = validator.process_batch(make_batch(p=[1, 2]))
        assert kept.num_rows == 2 and results == []


class TestResolveColumn:
    HEADERS = {"Order ID": "order_id", "Line No": "line_no", "Age": "age", "E-Mail": "email"}

    @classmethod
    def resolve(cls, name):
        return cls.HEADERS.get(name, name)

    def test_primary_key_unique_and_property_names_are_resolved(self):
        schema = {
            "properties": {
                "Age": {"type": "integer", "minimum": 0},
                "E-Mail": {"type": "string", "x-unique": True},
            },
            "x-primaryKey": {"columns": ["Order ID", "Line No"]},
            "x-uniqueConstraints": [{"columns": ["Order ID", "E-Mail"]}],
        }
        validator = build_constraint_validator(schema, resolve_column=self.resolve)

        config = validator.config
        assert config.unique_constraints == [
            ("order_id", "line_no"),
            ("order_id", "email"),
            "email",
        ]
        assert config.check_constraints["Age_range"]["column"] == "age"
        assert config.check_constraints["primary_key_order_id_not_null"]["column"] == "order_id"
        assert config.check_constraints["primary_key_line_no_not_null"]["column"] == "line_no"

        batch = make_batch(
            order_id=[1, 1, 2, None],
            line_no=[1, 1, 1, 1],
            age=[5, 5, -3, 5],
            email=["a", "b", "c", "d"],
        )
        kept, results = validator.process_batch(batch)
        assert values(kept, "order_id") == [1]
        assert rejected(results) == [
            (1, "order_id", "UNIQUE_VIOLATION"),
            (2, "age", "RANGE_VIOLATION"),
            (3, "order_id", "NULL_VIOLATION"),
        ]

    def test_names_that_are_already_output_names_pass_through(self):
        schema = {"x-primaryKey": {"columns": ["order_id"]}}
        validator = build_constraint_validator(schema, resolve_column=self.resolve)
        assert validator.config.unique_constraints == ["order_id"]

    def test_two_columns_resolving_to_the_same_name_raise(self):
        schema = {"x-primaryKey": {"columns": ["a", "A"]}}
        with pytest.raises(ValueError, match="same output column"):
            build_constraint_validator(schema, resolve_column=str.lower)

    def test_resolver_must_return_names(self):
        with pytest.raises(ValueError, match="resolve_column"):
            build_constraint_validator(pk_schema(), resolve_column=lambda name: "")
        with pytest.raises(ValueError, match="resolve_column"):
            build_constraint_validator(pk_schema(), resolve_column=lambda name: None)


class TestConstraintValidatorBehaviour:
    def test_messages_do_not_contain_cell_values(self):
        schema = pk_schema(columns=["id", "name"], type="composite")
        schema["properties"] = {
            "code": {"enum": ["A"], "pattern": "^A$", "minLength": 5, "maxLength": 6},
            "age": {"minimum": 0, "maximum": 1},
        }
        validator = build_constraint_validator(schema)
        batch = make_batch(
            id=["secret-1", "secret-1", None],
            name=["secret-2", "secret-2", "secret-3"],
            code=["secret-4", "secret-5", "secret-6"],
            age=[99, 98, 97],
        )
        _, results = validator.process_batch(batch)

        assert results
        assert not any("secret" in r.error_message for r in results)
        assert not any(str(n) in r.error_message for r in results for n in (99, 98, 97))
        assert all(v.values == [] for v in validator.violations)

    def test_results_carry_row_index_code_and_column_for_every_rejected_row(self):
        schema = pk_schema()
        schema["properties"] = {"age": {"minimum": 0}}
        validator = build_constraint_validator(schema)
        batch = make_batch(id=[1, 2, 3, 3, None, 6], age=[1, -1, 1, 1, 1, 1])

        kept, results = validator.process_batch(batch)

        rejected_rows = {r.row_index for r in results}
        assert rejected_rows == {1, 3, 4}
        assert kept.num_rows == batch.num_rows - len(rejected_rows)
        for result in results:
            assert result.is_valid is False
            assert isinstance(result.row_index, int)
            assert result.error_code
            assert result.column_name in ("id", "age")

    def test_violations_are_bounded_but_counted(self):
        validator = build_constraint_validator(pk_schema())
        batch = pa.RecordBatch.from_arrays([pa.array([None] * 20, type=pa.int64())], names=["id"])

        total_results = 0
        for _ in range(60):
            kept, results = validator.process_batch(batch)
            assert kept.num_rows == 0
            total_results += len(results)

        assert total_results == 1200  # every batch is reported completely
        assert validator.violation_count == 1200
        assert len(validator.violations) == 1000  # but only 1000 are kept in memory
        assert validator.violations_truncated is True
        assert len(validator.batch_violations) == 20

    def test_loaded_validator_keeps_no_cell_values_and_bounded_violations(self):
        validator = build_constraint_validator(pk_schema())
        config = validator.config
        assert config.include_values is False
        assert config.max_retained_violations == 1000

        validator.process_batch(make_batch(id=[1, 1, None]))
        assert validator.violation_count == 2
        assert [v.values for v in validator.violations] == [[], []]
        assert validator.violations_truncated is False

    def test_finalize_counts_every_violation(self):
        schema = pk_schema()
        schema["x-constraintHandling"] = {"errorMode": "fail_complete"}
        validator = build_constraint_validator(schema)
        validator.process_batch(make_batch(id=[None, None, 1, 1]))
        with pytest.raises(ValueError, match="3 violations"):
            validator.finalize()

    def test_null_check_flags_exactly_the_null_rows(self):
        validator = ConstraintValidator(
            ConstraintConfig(check_constraints={"n": {"column": "id", "nullable": False}})
        )
        kept, results = validator.process_batch(make_batch(id=[None, 1, None, 2, 3]))
        assert [r.row_index for r in results] == [0, 2]
        assert values(kept, "id") == [1, 2, 3]
        kept, results = validator.process_batch(make_batch(id=[1, 2]))
        assert results == [] and kept.num_rows == 2


# ======================================================================= build_data_validator


def validation_schema(fields=None, **sections):
    section = {"fieldValidations": fields or {}}
    section.update(sections)
    return {"x-validation": section}


class TestBuildDataValidatorNone:
    def test_absent_or_empty_gives_none(self):
        assert build_data_validator({}) is None
        assert build_data_validator({"x-validation": None}) is None
        assert build_data_validator({"x-validation": {}}) is None
        assert build_data_validator(validation_schema({})) is None
        assert build_data_validator({"x-validation": {"description": "d"}}) is None

    def test_rules_that_check_nothing_give_none(self):
        schema = validation_schema(
            {
                "a": {"required": False, "unique": False},
                "b": {"range": {}, "stringValidation": {}, "onViolation": {"range": "bad_rows"}},
                "c": {"range": {"inclusive": True}},
                "d": {"stringValidation": {"allowEmpty": True}},
            }
        )
        assert build_data_validator(schema) is None

    def test_only_unsupported_blocks_give_none(self):
        schema = {
            "x-validation": {
                "crossFieldValidations": [{"name": "x", "rule": "a < b"}],
                "globalValidations": {"maxNullFieldsPerRow": 3},
                "badRowsHandling": {"maxBadRowsPercent": 5},
            }
        }
        assert build_data_validator(schema) is None


class TestFieldRules:
    def test_standard_names_map_to_the_dataclasses(self):
        schema = validation_schema(
            {
                "age": {
                    "required": True,
                    "unique": True,
                    "range": {"min": 0, "max": 150, "inclusive": False},
                    "stringValidation": {
                        "minLength": 1,
                        "maxLength": 9,
                        "pattern": "^a",
                        "allowEmpty": False,
                    },
                    "enumValidation": {"allowedValues": ["A", "B"], "caseSensitive": False},
                    "dateValidation": {
                        "minDate": "1900-01-01",
                        "maxDate": "2100-12-31",
                        "format": ["%Y-%m-%d", "%m/%d/%Y"],
                    },
                }
            }
        )
        processor = build_data_validator(schema)
        assert isinstance(processor, DataValidationProcessor)
        (rule,) = processor.config.field_validations
        assert rule.field_name == "age"
        assert rule.required is True and rule.unique is True
        assert (rule.range_validation.min_value, rule.range_validation.max_value) == (0, 150)
        assert rule.range_validation.inclusive is False
        sv = rule.string_validation
        assert (sv.min_length, sv.max_length, sv.pattern, sv.allow_empty) == (1, 9, "^a", False)
        assert rule.enum_validation.allowed_values == ["A", "B"]
        assert rule.enum_validation.case_sensitive is False
        dv = rule.date_validation
        assert (dv.min_date, dv.max_date) == ("1900-01-01", "2100-12-31")
        assert dv.formats == ["%Y-%m-%d", "%m/%d/%Y"]

    def test_single_format_string(self):
        schema = validation_schema({"d": {"dateValidation": {"format": "%d/%m/%Y"}}})
        (rule,) = build_data_validator(schema).config.field_validations
        assert rule.date_validation.formats == ["%d/%m/%Y"]

    def test_rules_reject_rows_with_attribution(self):
        schema = validation_schema(
            {
                "age": {"range": {"min": 0, "max": 150}},
                "name": {
                    "required": True,
                    "stringValidation": {"minLength": 2, "maxLength": 5, "pattern": "^[A-Z]"},
                },
                "cat": {"enumValidation": {"allowedValues": ["A", "B"], "caseSensitive": False}},
                "born": {
                    "dateValidation": {
                        "minDate": "1900-01-01",
                        "maxDate": "2100-12-31",
                        "format": ["%Y-%m-%d"],
                    }
                },
            },
            badRowsHandling={"maxBadRowsPercent": 100},
        )
        processor = build_data_validator(schema)
        batch = make_batch(
            age=[10, 0, 151, 20, 30, 40, 50],
            name=["Alice", "Bob", "Carl", None, "x", "lower", "Eve"],
            cat=["A", "b", "B", "A", "A", "A", "Z"],
            born=[
                "2000-01-01",
                "1999-12-31",
                "1800-01-01",
                "nope",
                "2000-01-01",
                "2000-01-01",
                None,
            ],
        )

        kept, results = processor.process_batch(batch)

        assert values(kept, "name") == ["Alice", "Bob"]
        assert rejected(results) == [
            (2, "age", "VALIDATION_ERROR"),
            (2, "born", "VALIDATION_ERROR"),
            (3, "born", "VALIDATION_ERROR"),
            (3, "name", "VALIDATION_ERROR"),
            (4, "name", "VALIDATION_ERROR"),
            (5, "name", "VALIDATION_ERROR"),
            (6, "cat", "VALIDATION_ERROR"),
        ]

    def test_every_error_of_a_rejected_row_has_row_index_code_and_column(self):
        schema = validation_schema(
            {
                "a": {"required": True},
                "b": {"unique": True},
                "c": {"range": {"min": 1}},
            },
            badRowsHandling={"maxBadRowsPercent": 100},
        )
        processor = build_data_validator(schema)
        _, results = processor.process_batch(
            make_batch(a=[None, "x", "y"], b=[7, 8, 8], c=[0, 5, 5])
        )
        assert rejected(results) == [
            (0, "a", "VALIDATION_ERROR"),
            (0, "c", "VALIDATION_ERROR"),
            (2, "b", "VALIDATION_ERROR"),
        ]
        assert all(r.is_valid is False and r.error_message for r in results)

    def test_exclusive_range(self):
        schema = validation_schema(
            {"x": {"range": {"min": 0, "max": 10, "inclusive": False}}},
            badRowsHandling={"maxBadRowsPercent": 100},
        )
        kept, results = build_data_validator(schema).process_batch(make_batch(x=[0, 5, 10]))
        assert values(kept, "x") == [5]

    def test_date_bounds_may_use_the_listed_formats(self):
        schema = validation_schema(
            {"d": {"dateValidation": {"minDate": "01/01/2020", "format": ["%m/%d/%Y"]}}},
            badRowsHandling={"maxBadRowsPercent": 100},
        )
        kept, _ = build_data_validator(schema).process_batch(
            make_batch(d=["12/31/2019", "01/02/2020"])
        )
        assert values(kept, "d") == ["01/02/2020"]

    def test_messages_do_not_contain_cell_values(self):
        schema = validation_schema(
            {
                "s": {"stringValidation": {"pattern": "^zzz"}},
                "e": {"enumValidation": {"allowedValues": ["ok"]}},
                "n": {"range": {"max": 1}},
                "d": {"dateValidation": {"maxDate": "2000-01-01"}},
                "u": {"unique": True},
            },
            badRowsHandling={"maxBadRowsPercent": 100},
        )
        processor = build_data_validator(schema)
        _, results = processor.process_batch(
            make_batch(
                s=["secret-s"],
                e=["secret-e"],
                n=[987654],
                d=["2030-05-05"],
                u=["secret-u"],
            )
        )
        assert len(results) == 4
        for result in results:
            for secret in ("secret", "987654", "2030-05-05"):
                assert secret not in result.error_message

    def test_unsupported_keys_do_not_raise(self):
        schema = validation_schema(
            {
                "a": {
                    "required": True,
                    "onViolation": {"required": "warning"},
                    "range": {"min": 0, "weird": 1},
                    "somethingNew": 1,
                }
            },
            crossFieldValidations=[{"name": "x"}],
            globalValidations={"maxNullFieldsPerRow": 3},
        )
        assert build_data_validator(schema) is not None

    @pytest.mark.parametrize(
        "rule, message",
        [
            ("text", r"x-validation\.fieldValidations\.f: must be an object"),
            ({"required": "yes"}, r"x-validation\.fieldValidations\.f\.required"),
            ({"unique": 1}, r"x-validation\.fieldValidations\.f\.unique"),
            ({"range": [1]}, r"x-validation\.fieldValidations\.f\.range: must be an object"),
            ({"range": {"min": "abc"}}, r"f\.range\.min: must be a number or an ISO date"),
            ({"range": {"min": True}}, r"f\.range\.min"),
            ({"range": {"max": [1]}}, r"f\.range\.max"),
            ({"range": {"min": 5, "max": 1}}, r"f\.range: min is greater than max"),
            (
                {"range": {"min": "2021-01-01", "max": "2020-01-01"}},
                r"f\.range: min is greater than max",
            ),
            ({"range": {"min": 1, "max": "2020-01-01"}}, "both be numbers or both be dates"),
            (
                {"range": {"min": 1, "max": 1, "inclusive": False}},
                r"f\.range: min equals max but the range is not inclusive",
            ),
            ({"range": {"min": 1, "inclusive": "yes"}}, r"f\.range\.inclusive"),
            ({"stringValidation": "abc"}, r"f\.stringValidation: must be an object"),
            (
                {"stringValidation": {"minLength": -1}},
                r"f\.stringValidation\.minLength: must be >= 0",
            ),
            ({"stringValidation": {"maxLength": 1.5}}, r"f\.stringValidation\.maxLength"),
            ({"stringValidation": {"maxLength": True}}, r"f\.stringValidation\.maxLength"),
            (
                {"stringValidation": {"minLength": 5, "maxLength": 2}},
                r"f\.stringValidation: minLength \(5\) is greater than maxLength \(2\)",
            ),
            (
                {"stringValidation": {"pattern": 3}},
                r"f\.stringValidation\.pattern: must be a string",
            ),
            (
                {"stringValidation": {"pattern": "[unclosed"}},
                r"f\.stringValidation\.pattern: Invalid regular expression",
            ),
            (
                {"stringValidation": {"pattern": "^(a+)+$"}},
                r"f\.stringValidation\.pattern: .*nested unbounded",
            ),
            ({"stringValidation": {"allowEmpty": "no"}}, r"f\.stringValidation\.allowEmpty"),
            ({"enumValidation": {}}, None),  # empty block: nothing configured
            (
                {"enumValidation": {"caseSensitive": False}},
                r"f\.enumValidation\.allowedValues: is required",
            ),
            (
                {"enumValidation": {"allowedValues": []}},
                r"f\.enumValidation\.allowedValues: must not be empty",
            ),
            (
                {"enumValidation": {"allowedValues": "AB"}},
                r"f\.enumValidation\.allowedValues: must be a list",
            ),
            (
                {"enumValidation": {"allowedValues": ["a"], "caseSensitive": "yes"}},
                r"f\.enumValidation\.caseSensitive",
            ),
            (
                {"dateValidation": {"minDate": 20200101}},
                r"f\.dateValidation\.minDate: must be a date string",
            ),
            (
                {"dateValidation": {"minDate": "not a date"}},
                r"f\.dateValidation\.minDate: is not a valid date",
            ),
            (
                {"dateValidation": {"minDate": "2021-01-01", "maxDate": "2020-01-01"}},
                r"f\.dateValidation: minDate is after maxDate",
            ),
            ({"dateValidation": {"format": []}}, r"f\.dateValidation\.format: must not be empty"),
            ({"dateValidation": {"format": [1]}}, r"f\.dateValidation\.format\[0\]"),
            ({"dateValidation": {"format": 5}}, r"f\.dateValidation\.format: must be a list"),
            ({"onViolation": "bad_rows"}, r"f\.onViolation: must be an object"),
        ],
    )
    def test_invalid_field_rule_raises(self, rule, message):
        schema = validation_schema({"f": dict(rule) if isinstance(rule, dict) else rule})
        if message is None:
            assert build_data_validator(schema) is None
            return
        with pytest.raises(ValueError, match=message):
            build_data_validator(schema)

    def test_section_shapes(self):
        with pytest.raises(ValueError, match="x-validation: must be an object"):
            build_data_validator({"x-validation": []})
        with pytest.raises(ValueError, match=r"x-validation\.fieldValidations: must be an object"):
            build_data_validator({"x-validation": {"fieldValidations": [{"field": "a"}]}})
        with pytest.raises(
            ValueError, match=r"x-validation\.crossFieldValidations: must be a list"
        ):
            build_data_validator({"x-validation": {"crossFieldValidations": {}}})
        with pytest.raises(
            ValueError, match=r"x-validation\.globalValidations: must be an object"
        ):
            build_data_validator({"x-validation": {"globalValidations": []}})
        with pytest.raises(ValueError, match=r"x-validation\.badRowsHandling: must be an object"):
            build_data_validator({"x-validation": {"badRowsHandling": True}})
        with pytest.raises(
            ValueError, match=r"x-validation\.uniquenessHandling: must be an object"
        ):
            build_data_validator({"x-validation": {"uniquenessHandling": "first_wins"}})


class TestUniquenessStrategy:
    FIELDS = {"id": {"unique": True}}

    @pytest.mark.parametrize(
        "strategy", ["first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"]
    )
    def test_every_strategy_is_accepted(self, strategy):
        schema = validation_schema(self.FIELDS, uniquenessHandling={"strategy": strategy})
        assert build_data_validator(schema).config.uniqueness_strategy == strategy

    def test_default_is_first_wins(self):
        assert build_data_validator(validation_schema(self.FIELDS)).config.uniqueness_strategy == (
            "first_wins"
        )
        schema = validation_schema(self.FIELDS, uniquenessHandling={"options": ["first_wins"]})
        assert build_data_validator(schema).config.uniqueness_strategy == "first_wins"

    @pytest.mark.parametrize("strategy", ["keep_first", "", "FIRST_WINS", 3, ["first_wins"]])
    def test_unknown_strategy_raises(self, strategy):
        schema = validation_schema(self.FIELDS, uniquenessHandling={"strategy": strategy})
        with pytest.raises(ValueError) as error:
            build_data_validator(schema)
        assert "x-validation.uniquenessHandling.strategy" in str(error.value)
        for valid in ("first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"):
            assert valid in str(error.value)

    def test_strategies_behave_differently(self):
        batch = make_batch(id=[1, 1, 2])

        def run(strategy):
            schema = validation_schema(self.FIELDS, uniquenessHandling={"strategy": strategy})
            schema["x-validation"]["badRowsHandling"] = {"maxBadRowsPercent": 100}
            kept, results = build_data_validator(schema).process_batch(batch)
            return [r.row_index for r in results], values(kept, "id")

        assert run("first_wins") == ([1], [1, 2])
        assert run("last_wins") == ([0], [1, 2])
        assert run("mark_all_duplicates") == ([0, 1], [2])
        assert run("fail_on_duplicate") == ([1], [1, 2])


class TestBadRowsHandling:
    FIELDS = {"x": {"range": {"min": 0}}}

    def test_the_processor_does_not_collect_rejected_rows(self):
        processor = build_data_validator(validation_schema(self.FIELDS))
        config = processor.config.bad_rows_config
        assert config.enabled is False
        assert config.include_original_row is False
        assert config.include_validation_errors is False

    def test_threshold_defaults(self):
        config = build_data_validator(validation_schema(self.FIELDS)).config.bad_rows_config
        assert config.max_bad_rows_percent == 10.0
        assert config.fail_on_exceed_threshold is True

    def test_threshold_is_configurable(self):
        schema = validation_schema(
            self.FIELDS, badRowsHandling={"maxBadRowsPercent": 25, "failOnExceedThreshold": False}
        )
        config = build_data_validator(schema).config.bad_rows_config
        assert config.max_bad_rows_percent == 25.0
        assert config.fail_on_exceed_threshold is False

    def test_default_threshold_is_judged_at_the_end(self):
        processor = build_data_validator(validation_schema(self.FIELDS))
        batch = make_batch(x=[1, 2, 3, 4, -1, 5, 6, 7, 8, -2])  # 20 % bad

        processor.process_batch(batch)  # no verdict while batches are still coming

        with pytest.raises(BadRowsThresholdExceededError, match=r"\(20.0%\) exceed threshold"):
            processor.check_threshold()

    def test_early_mode_stops_at_the_batch(self):
        schema = validation_schema(self.FIELDS, badRowsHandling={"thresholdMode": "early"})
        processor = build_data_validator(schema)
        with pytest.raises(BadRowsThresholdExceededError):
            processor.process_batch(make_batch(x=[1, 2, 3, 4, -1, 5, 6, 7, 8, -2]))

    def test_configured_threshold_is_applied(self):
        schema = validation_schema(self.FIELDS, badRowsHandling={"maxBadRowsPercent": 25})
        processor = build_data_validator(schema)
        kept, results = processor.process_batch(make_batch(x=[1, 2, 3, 4, -1, 5, 6, 7, 8, -2]))
        assert kept.num_rows == 8 and len(results) == 2
        processor.check_threshold()  # 20 % of 10 rows: within 25 %
        processor.process_batch(make_batch(x=[-1, -2, -3, -4, 1]))  # 6 of 15 rows
        with pytest.raises(BadRowsThresholdExceededError):
            processor.check_threshold()

    def test_fail_on_exceed_threshold_false_never_raises(self):
        schema = validation_schema(
            self.FIELDS, badRowsHandling={"maxBadRowsPercent": 1, "failOnExceedThreshold": False}
        )
        processor = build_data_validator(schema)
        kept, results = processor.process_batch(make_batch(x=[-1, -2, 3]))
        assert kept.num_rows == 1 and len(results) == 2

    def test_fifty_batches_use_constant_memory(self):
        schema = validation_schema(self.FIELDS, badRowsHandling={"maxBadRowsPercent": 10})
        processor = build_data_validator(schema)
        batch = make_batch(x=[1] * 19 + [-1])  # 5 % bad, below the threshold

        for _ in range(50):
            kept, results = processor.process_batch(batch)
            assert kept.num_rows == 19
            assert [r.row_index for r in results] == [19]  # reported every time

        handler = processor.bad_rows_handler
        assert handler.bad_rows == []  # nothing is accumulated ...
        assert processor.get_bad_rows_batch() is None
        # ... but the bookkeeping behind the threshold is complete
        assert handler.bad_row_total == 50
        assert processor.total_rows_processed == 1000
        summary = processor.get_validation_summary()
        assert summary["bad_rows_count"] == 50
        assert summary["bad_rows_percent"] == pytest.approx(5.0)

    def test_no_files_are_written(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        schema = validation_schema(
            self.FIELDS,
            badRowsHandling={
                "enabled": True,
                "outputPath": "my_bad_rows",
                "fileFormat": "parquet",
                "includeOriginalRow": True,
                "includeValidationErrors": True,
                "maxBadRowsPercent": 100,
            },
        )
        processor = build_data_validator(schema)
        processor.process_batch(make_batch(x=[-1, 1]))
        assert list(tmp_path.iterdir()) == []
        assert not os.path.exists("my_bad_rows") and not os.path.exists("bad_rows")

    @pytest.mark.parametrize("percent", [-1, 100.5, "10", True, {}, [10]])
    def test_invalid_percent_raises(self, percent):
        schema = validation_schema(self.FIELDS, badRowsHandling={"maxBadRowsPercent": percent})
        with pytest.raises(ValueError, match=r"x-validation\.badRowsHandling\.maxBadRowsPercent"):
            build_data_validator(schema)

    def test_percent_bounds_are_valid(self):
        for percent in (0, 0.0, 100, 100.0, 12.5):
            schema = validation_schema(self.FIELDS, badRowsHandling={"maxBadRowsPercent": percent})
            assert build_data_validator(schema).config.bad_rows_config.max_bad_rows_percent == (
                float(percent)
            )

    @pytest.mark.parametrize("key", ["failOnExceedThreshold", "enabled"])
    def test_flags_must_be_booleans(self, key):
        schema = validation_schema(self.FIELDS, badRowsHandling={key: "yes"})
        with pytest.raises(ValueError, match=rf"x-validation\.badRowsHandling\.{key}"):
            build_data_validator(schema)


class TestDataValidatorResolveColumn:
    def test_field_names_are_resolved(self):
        schema = validation_schema(
            {"Age": {"range": {"min": 0}}, "E-Mail": {"required": True}},
            badRowsHandling={"maxBadRowsPercent": 100},
        )
        processor = build_data_validator(
            schema, resolve_column=lambda name: {"Age": "age", "E-Mail": "email"}.get(name, name)
        )
        assert [r.field_name for r in processor.config.field_validations] == ["age", "email"]

        kept, results = processor.process_batch(make_batch(age=[-1, 4], email=["a", "b"]))
        assert values(kept, "age") == [4]
        assert rejected(results) == [(0, "age", "VALIDATION_ERROR")]

    def test_names_that_are_already_output_names_pass_through(self):
        schema = validation_schema({"age": {"range": {"min": 0}}})
        processor = build_data_validator(schema, resolve_column=lambda name: name)
        assert processor.config.field_validations[0].field_name == "age"

    def test_two_fields_resolving_to_one_column_raise(self):
        schema = validation_schema({"Age": {"required": True}, "AGE": {"unique": True}})
        with pytest.raises(ValueError, match="refer to the same column 'age'"):
            build_data_validator(schema, resolve_column=str.lower)

    def test_resolver_must_return_names(self):
        schema = validation_schema({"Age": {"required": True}})
        with pytest.raises(ValueError, match="resolve_column"):
            build_data_validator(schema, resolve_column=lambda name: 5)


# ===================================================================== build_quality_processor


class TestBuildQualityProcessor:
    def test_absent_or_nothing_to_apply_gives_none(self):
        assert build_quality_processor({}) is None
        assert build_quality_processor({"x-dataQuality": None}) is None
        assert build_quality_processor({"x-dataQuality": {}}) is None
        assert build_quality_processor({"x-dataQuality": {"fieldSpecificRules": {}}}) is None
        assert (
            build_quality_processor(
                {
                    "x-dataQuality": {
                        "completeness": {"enabled": True, "minimumFillRate": 0.9},
                        "fieldSpecificRules": {"a": {"dataType": "integer"}},
                        "fieldQualityRules": {"b": {"rules": ["not_null"], "severity": "error"}},
                    }
                }
            )
            is None
        )

    def test_disabled_extension_gives_none(self):
        schema = {"x-dataQuality": {"enabled": False, "fieldSpecificRules": {"a": {"min": 1}}}}
        assert build_quality_processor(schema) is None

    def test_standard_shape(self):
        schema = {
            "x-dataQuality": {
                "fieldSpecificRules": {
                    "age": {"min": 0, "max": 150, "dataType": "integer"},
                    "email": {"pattern": "^a", "required": False},
                    "phone": {"pattern": "^[0-9]+$", "standardizeFormat": True},
                    "nothing": {"dataType": "string"},
                }
            }
        }
        processor = build_quality_processor(schema)
        assert isinstance(processor, DataQualityProcessor)
        assert processor.rules == {
            "column_rules": {
                "age": {"min_value": 0, "max_value": 150},
                "email": {"pattern": "^a"},
                "phone": {"pattern": "^[0-9]+$"},
            }
        }

    def test_documentation_shape(self):
        schema = {
            "x-dataQuality": {
                "fieldQualityRules": {
                    "code": {
                        "rules": ["pattern_match"],
                        "severity": "error",
                        "parameters": {
                            "pattern": "^[A-Z]{2}\\d{6}$",
                            "min_length": 8,
                            "max_length": 8,
                            "lookup_table": "ignored",
                        },
                    },
                    "amount": {"parameters": {"min_value": 0.01, "max_value": 1000000.0}},
                    "empty": {"rules": ["not_null"]},
                }
            }
        }
        assert build_quality_processor(schema).rules == {
            "column_rules": {
                "code": {"pattern": "^[A-Z]{2}\\d{6}$", "min_length": 8, "max_length": 8},
                "amount": {"min_value": 0.01, "max_value": 1000000.0},
            }
        }

    def test_both_shapes_merge_per_column(self):
        schema = {
            "x-dataQuality": {
                "fieldSpecificRules": {"age": {"min": 0}},
                "fieldQualityRules": {"age": {"parameters": {"max_value": 150, "min_value": 0}}},
            }
        }
        assert build_quality_processor(schema).rules == {
            "column_rules": {"age": {"min_value": 0, "max_value": 150}}
        }

    def test_conflicting_shapes_raise(self):
        schema = {
            "x-dataQuality": {
                "fieldSpecificRules": {"age": {"min": 0}},
                "fieldQualityRules": {"age": {"parameters": {"min_value": 18}}},
            }
        }
        with pytest.raises(ValueError, match="conflicts"):
            build_quality_processor(schema)

    def test_reports_violations_and_does_not_change_the_batch(self):
        schema = {
            "x-dataQuality": {
                "fieldSpecificRules": {
                    "age": {"min": 0, "max": 150},
                    "name": {"pattern": "^[A-Z]"},
                },
                "fieldQualityRules": {"name": {"parameters": {"min_length": 2, "max_length": 5}}},
            }
        }
        processor = build_quality_processor(schema)
        batch = make_batch(age=[10, -1, 200], name=["Al", "bob", "Alexander"])

        out, results = processor.process_batch(batch)

        assert out is batch
        assert rejected(results) == [
            (1, "age", "MIN_VALUE_VIOLATION"),
            (1, "name", "PATTERN_VIOLATION"),
            (2, "age", "MAX_VALUE_VIOLATION"),
            (2, "name", "MAX_LENGTH_VIOLATION"),
        ]

    def test_messages_do_not_contain_cell_values(self):
        schema = {
            "x-dataQuality": {
                "fieldSpecificRules": {
                    "n": {"min": 0, "max": 5},
                    "s": {"pattern": "^zzz"},
                }
            }
        }
        processor = build_quality_processor(schema)
        _, results = processor.process_batch(
            make_batch(n=[-123456, 654321], s=["secret-a", "secret-b"])
        )
        assert len(results) == 4
        for result in results:
            for secret in ("123456", "654321", "secret"):
                assert secret not in result.error_message

    def test_resolve_column(self):
        schema = {"x-dataQuality": {"fieldSpecificRules": {"Age": {"min": 0}}}}
        processor = build_quality_processor(
            schema, resolve_column=lambda name: {"Age": "age"}.get(name, name)
        )
        assert processor.rules == {"column_rules": {"age": {"min_value": 0}}}
        _, results = processor.process_batch(make_batch(age=[-1]))
        assert rejected(results) == [(0, "age", "MIN_VALUE_VIOLATION")]

    def test_unsupported_blocks_do_not_raise(self):
        schema = {
            "x-dataQuality": {
                "description": "d",
                "qualityThresholds": {"completeness": {"minimum": 0.9}},
                "crossFieldValidation": [{"name": "x"}],
                "statisticalChecks": {"outlierDetection": {"enabled": True}},
                "reporting": {"generateReport": True},
                "fieldSpecificRules": {"a": {"min": 1}},
            }
        }
        assert build_quality_processor(schema) is not None

    @pytest.mark.parametrize(
        "section, message",
        [
            ([], "x-dataQuality: must be an object"),
            ({"enabled": "yes"}, r"x-dataQuality\.enabled"),
            ({"fieldSpecificRules": []}, r"x-dataQuality\.fieldSpecificRules: must be an object"),
            ({"fieldSpecificRules": {"a": 5}}, r"x-dataQuality\.fieldSpecificRules\.a: must be"),
            (
                {"fieldSpecificRules": {"a": {"min": "0"}}},
                r"fieldSpecificRules\.a\.min: must be a number",
            ),
            ({"fieldSpecificRules": {"a": {"max": True}}}, r"fieldSpecificRules\.a\.max"),
            (
                {"fieldSpecificRules": {"a": {"min": 5, "max": 1}}},
                r"fieldSpecificRules\.a: min_value \(5\) is greater than max_value \(1\)",
            ),
            (
                {"fieldSpecificRules": {"a": {"pattern": "[x"}}},
                r"fieldSpecificRules\.a\.pattern: Invalid",
            ),
            (
                {"fieldSpecificRules": {"a": {"pattern": "^(a+)+$"}}},
                r"fieldSpecificRules\.a\.pattern",
            ),
            (
                {"fieldQualityRules": {"a": {"parameters": []}}},
                r"fieldQualityRules\.a\.parameters: must be an object",
            ),
            (
                {"fieldQualityRules": {"a": {"parameters": {"min_length": -1}}}},
                r"fieldQualityRules\.a\.parameters\.min_length: must be >= 0",
            ),
            (
                {"fieldQualityRules": {"a": {"parameters": {"max_length": "3"}}}},
                r"fieldQualityRules\.a\.parameters\.max_length: must be an integer",
            ),
            (
                {"fieldQualityRules": {"a": {"parameters": {"min_length": 9, "max_length": 3}}}},
                r"min_length \(9\) is greater than max_length \(3\)",
            ),
            (
                {"fieldQualityRules": {"a": {"parameters": {"min_value": float("inf")}}}},
                r"parameters\.min_value: must be a finite number",
            ),
            ({"fieldQualityRules": {"a": 3}}, r"fieldQualityRules\.a: must be an object"),
        ],
    )
    def test_invalid_configuration_raises(self, section, message):
        with pytest.raises(ValueError, match=message):
            build_quality_processor({"x-dataQuality": section})


# =================================================================== processor-level behaviour


class TestRowAttribution:
    """Every row-dropping processor reports row_index, error_code and column_name."""

    def test_data_validation_processor_reports_the_column_of_every_error(self):
        config = ValidationConfig(
            field_validations=[
                FieldValidationRule(field_name="a", required=True),
                FieldValidationRule(field_name="b", unique=True),
                FieldValidationRule(field_name="c", range_validation=RangeValidation(min_value=1)),
                FieldValidationRule(field_name="missing"),
            ],
            bad_rows_config=BadRowsConfig(max_bad_rows_percent=100),
        )
        processor = DataValidationProcessor(config)
        _, results = processor.process_batch(
            make_batch(a=[None, "x", "y"], b=[7, 8, 8], c=[0, 5, 5])
        )
        assert rejected(results) == [
            (0, "a", "VALIDATION_ERROR"),
            (0, "c", "VALIDATION_ERROR"),
            (2, "b", "VALIDATION_ERROR"),
        ]

    @pytest.mark.parametrize(
        "strategy", ["first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"]
    )
    def test_uniqueness_errors_name_the_column(self, strategy):
        config = ValidationConfig(
            field_validations=[FieldValidationRule(field_name="id", unique=True)],
            bad_rows_config=BadRowsConfig(max_bad_rows_percent=100),
            uniqueness_strategy=strategy,
        )
        _, results = DataValidationProcessor(config).process_batch(make_batch(id=[1, 1, 2, 2]))
        assert results and all(r.column_name == "id" and r.row_index is not None for r in results)

    def test_messages_stay_plain_strings(self):
        config = ValidationConfig(
            field_validations=[FieldValidationRule(field_name="a", required=True)],
            bad_rows_config=BadRowsConfig(max_bad_rows_percent=100),
        )
        processor = DataValidationProcessor(config)
        is_valid, errors = processor._validate_row(make_batch(a=[None]), 0)
        assert is_valid is False
        assert errors == ["Field 'a' is required but is null/empty"]
        assert all(type(error).__mro__[1] is str for error in errors)

    def test_collecting_processor_still_collects(self):
        config = ValidationConfig(
            field_validations=[FieldValidationRule(field_name="a", required=True)],
            bad_rows_config=BadRowsConfig(max_bad_rows_percent=100),
        )
        processor = DataValidationProcessor(config)
        processor.process_batch(make_batch(a=[None, "x"]))
        assert len(processor.bad_rows_handler.bad_rows) == 1


class TestQualityIncludeValues:
    RULES = {"column_rules": {"s": {"pattern": "^zzz"}, "n": {"max_value": 1}}}

    def test_default_keeps_the_historic_messages(self):
        processor = DataQualityProcessor(self.RULES)
        _, results = processor.process_batch(make_batch(s=["abc"], n=[42]))
        messages = " ".join(r.error_message for r in results)
        assert "abc" in messages and "42" in messages

    def test_include_values_false_keeps_values_out(self):
        processor = DataQualityProcessor(self.RULES, include_values=False)
        _, results = processor.process_batch(make_batch(s=["abc"], n=[42]))
        assert len(results) == 2
        for result in results:
            assert "abc" not in result.error_message and "42" not in result.error_message
            assert result.row_index == 0 and result.column_name in ("s", "n")


# ======================================================================== referenced_columns


class TestReferencedColumns:
    def test_empty(self):
        assert referenced_columns({}) == {}
        assert referenced_columns({"properties": {"a": {"type": "string"}}}) == {}

    def test_all_extensions(self):
        schema = {
            "properties": {
                "id": {"type": "integer", "minimum": 1},
                "name": {"type": "string"},
                "tag": {"type": "string", "x-unique": True},
                "off": {"type": "string", "x-unique": False},
                "code": {"pattern": "^a", "minLength": 1},
                "cat": {"enum": ["a"]},
                "n": {"maximum": 3},
            },
            "x-primaryKey": {"columns": ["id", "Order ID"]},
            "x-uniqueConstraints": [{"columns": ["a", "b"]}, {"columns": ["b", "c"]}],
            "x-validation": {
                "fieldValidations": {
                    "age": {"range": {"min": 0}},
                    "noop": {},
                    "off": {"required": False},
                    "email": {"required": True},
                }
            },
            "x-dataQuality": {
                "fieldSpecificRules": {"age": {"min": 0}, "z": {"dataType": "integer"}},
                "fieldQualityRules": {"q": {"parameters": {"min_length": 1}}, "r": {"rules": []}},
            },
        }
        assert referenced_columns(schema) == {
            "x-primaryKey": ["id", "Order ID"],
            "x-uniqueConstraints": ["a", "b", "c"],
            "x-validation": ["age", "email"],
            "x-dataQuality": ["age", "q"],
            "properties": ["id", "tag", "code", "cat", "n"],
        }

    def test_names_are_not_resolved(self):
        schema = {"x-primaryKey": {"columns": ["Order ID"]}}
        assert referenced_columns(schema) == {"x-primaryKey": ["Order ID"]}

    def test_malformed_content_is_skipped(self):
        schema = {
            "properties": [],
            "x-primaryKey": "id",
            "x-uniqueConstraints": [1, {"columns": "a"}, {"columns": [None, 5]}],
            "x-validation": {"fieldValidations": []},
            "x-dataQuality": [],
        }
        assert referenced_columns(schema) == {}
        assert referenced_columns({"x-primaryKey": {"columns": ["a", 3, None]}}) == {
            "x-primaryKey": ["a"]
        }

    def test_invalid_rules_are_still_referenced(self):
        schema = {
            "x-validation": {"fieldValidations": {"age": {"range": {"min": 5, "max": 1}}}},
            "x-dataQuality": {"fieldSpecificRules": {"age": {"min": 5, "max": 1}}},
        }
        assert referenced_columns(schema) == {
            "x-validation": ["age"],
            "x-dataQuality": ["age"],
        }

    def test_disabled_quality_extension(self):
        schema = {"x-dataQuality": {"enabled": False, "fieldSpecificRules": {"a": {"min": 1}}}}
        assert referenced_columns(schema) == {}

    def test_shipped_standard(self):
        schema = json.loads(STANDARD_CSV.read_text())
        found = referenced_columns(schema)
        assert found["x-primaryKey"] == ["id"]
        assert found["x-uniqueConstraints"] == ["name", "birth_date"]
        assert "email_address" in found["x-validation"] and "zip_9" in found["x-validation"]
        assert found["x-dataQuality"] == ["age", "salary", "email_address", "phone_number"]
        assert "id" in found["properties"] and "name" in found["properties"]


# ================================================================= unsupported_extension_keys


class TestUnsupportedExtensionKeys:
    def test_no_extensions(self):
        assert unsupported_extension_keys({}) == []
        assert unsupported_extension_keys({"properties": {"a": {"type": "string"}}}) == []
        assert unsupported_extension_keys({"x-csv": {"delimiter": ","}, "x-rowHash": {}}) == []

    def test_non_dict_schema_gives_no_warnings(self):
        assert unsupported_extension_keys(None) == []
        assert unsupported_extension_keys([]) == []

    def test_everything_supported_gives_no_warnings(self):
        schema = {
            "x-transformations": {"description": "d", "column_transformations": {"a": {}}},
            "x-calculatedColumns": {
                "constants": [],
                "expressions": [],
                "calculated": [],
                "failOnError": True,
                "addMetadata": False,
                "validateDependencies": True,
            },
            "x-columnMapping": {
                "explicitMappings": {"A": "a"},
                "namingConvention": "snake_case",
                "caseSensitive": False,
                "allowUnmapped": True,
                "dropUnmapped": False,
            },
            "x-primaryKey": {
                "description": "d",
                "columns": ["id"],
                "type": "single",
                "enforceUniqueness": True,
                "allowNulls": False,
                "description_detail": "x",
            },
            "x-uniqueConstraints": [
                {
                    "name": "u",
                    "columns": ["a"],
                    "description": "d",
                    "ignoreNulls": True,
                    "caseSensitive": True,
                }
            ],
            "x-constraintHandling": {"errorMode": "fail_fast", "description": "d"},
            "x-validation": {
                "badRowsHandling": {
                    "enabled": True,
                    "maxBadRowsPercent": 5,
                    "failOnExceedThreshold": False,
                },
                "uniquenessHandling": {"strategy": "first_wins", "options": ["first_wins"]},
                "fieldValidations": {
                    "a": {
                        "required": True,
                        "unique": True,
                        "range": {"min": 1, "max": 2, "inclusive": True},
                        "stringValidation": {
                            "minLength": 1,
                            "maxLength": 2,
                            "pattern": "x",
                            "allowEmpty": True,
                        },
                        "enumValidation": {"allowedValues": [1], "caseSensitive": True},
                        "dateValidation": {"minDate": "2020-01-01", "format": ["%Y"]},
                        "onViolation": {"required": "bad_rows", "range": "bad_rows"},
                    }
                },
                "crossFieldValidations": [],
                "globalValidations": {},
            },
            "x-dataQuality": {
                "enabled": True,
                "fieldSpecificRules": {
                    "a": {"min": 1, "max": 2, "pattern": "x", "required": False}
                },
                "fieldQualityRules": {
                    "b": {
                        "parameters": {
                            "min_length": 1,
                            "max_length": 2,
                            "pattern": "x",
                            "min_value": 1,
                            "max_value": 2,
                        }
                    }
                },
            },
        }
        assert unsupported_extension_keys(schema) == []

    def test_pii(self):
        assert unsupported_extension_keys({"x-pii": {"fields": {}}}) == [
            "x-pii is documentation only: no masking is applied"
        ]

    def test_transformations(self):
        schema = {
            "x-transformations": {
                "description": "d",
                "column_transformations": {"a": {"money_conversion": {"enabled": True}}},
                "moneyType": {"currencySymbols": ["$"]},
                "stringPadding": {"enabled": False, "width": 3},
                "regexReplacements": [],
                "columnSpecific": {"a": {"transformations": ["moneyType"]}},
                "somethingElse": {"x": 1},
            }
        }
        assert unsupported_extension_keys(schema) == [
            "x-transformations.moneyType is not read "
            "(use x-transformations.column_transformations.<column>.money_conversion)",
            "x-transformations.columnSpecific is not read "
            "(use x-transformations.column_transformations.<column>.<transformation>)",
            "x-transformations.somethingElse is not read "
            "(use x-transformations.column_transformations.<column>.<transformation>)",
        ]

    @pytest.mark.parametrize(
        "key, step",
        [
            ("stringCleaning", "string_cleaning"),
            ("caseTransformation", "string_cleaning"),
            ("numericCleaning", "numeric_cleaning"),
            ("moneyType", "money_conversion"),
            ("dateTimeParsing", "datetime"),
            ("htmlXmlCleaning", "html_xml_cleaning"),
            ("stringPadding", "string_padding"),
            ("regexReplacements", "regex_replace"),
            ("stringReplacements", "string_replace"),
        ],
    )
    def test_transformation_hints(self, key, step):
        assert unsupported_extension_keys({"x-transformations": {key: {"a": 1}}}) == [
            f"x-transformations.{key} is not read "
            f"(use x-transformations.column_transformations.<column>.{step})"
        ]

    def test_calculated_columns(self):
        schema = {
            "x-calculatedColumns": {
                "description": "d",
                "constants": [{"name": "c", "value": 1}],
                "partitionColumns": ["c"],
                "indexColumns": ["id"],
                "options": {"validateDependencies": True},
                "other": 1,
            }
        }
        assert unsupported_extension_keys(schema) == [
            "x-calculatedColumns.partitionColumns is recorded only: the output is not partitioned",
            "x-calculatedColumns.indexColumns is not read and is ignored",
            "x-calculatedColumns.options is not read (use the top-level failOnError, "
            "addMetadata and validateDependencies keys)",
            "x-calculatedColumns.other is not read and is ignored",
        ]

    def test_empty_blocks_are_not_reported(self):
        schema = {
            "x-calculatedColumns": {"partitionColumns": [], "indexColumns": [], "options": {}},
            "x-columnMapping": {"standardizationRules": {}},
            "x-constraintHandling": {"badRowsOutput": None},
        }
        assert unsupported_extension_keys(schema) == []

    def test_column_mapping(self):
        schema = {
            "x-columnMapping": {
                "explicitMappings": {"A": "a"},
                "standardizationRules": {"maxLength": 64},
                "globalMappings": {"x": "y"},
                "tableMappings": {"t": {}},
                "patternMappings": [{"pattern": "a"}],
                "standardization": {"caseConversion": "snake_case"},
                "validation": {"requireMapping": True},
            }
        }
        assert unsupported_extension_keys(schema) == [
            "x-columnMapping.standardizationRules is not supported and is ignored",
            "x-columnMapping.globalMappings is not supported and is ignored",
            "x-columnMapping.tableMappings is not supported and is ignored",
            "x-columnMapping.patternMappings is not supported and is ignored",
            "x-columnMapping.standardization is not supported and is ignored",
            "x-columnMapping.validation is not supported and is ignored",
        ]

    def test_primary_key(self):
        schema = {"x-primaryKey": {"columns": ["id"], "type": "single", "sortOrder": "asc"}}
        assert unsupported_extension_keys(schema) == [
            "x-primaryKey.sortOrder is not supported and is ignored"
        ]

    def test_unique_constraints(self):
        schema = {
            "x-uniqueConstraints": [
                {"name": "ok", "columns": ["a"]},
                {
                    "name": "cond",
                    "columns": ["b"],
                    "condition": "status = 'active'",
                    "ignoreNulls": False,
                    "caseSensitive": False,
                    "deferrable": True,
                },
                {"columns": ["c"], "ignoreNulls": True, "caseSensitive": True},
            ]
        }
        assert unsupported_extension_keys(schema) == [
            "x-uniqueConstraints[1].condition is not supported and is ignored "
            "(the constraint applies to every row)",
            "x-uniqueConstraints[1].ignoreNulls=false is not supported and is ignored "
            "(NULL keys are never compared)",
            "x-uniqueConstraints[1].caseSensitive=false is not supported and is ignored "
            "(values are compared case-sensitively)",
            "x-uniqueConstraints[1].deferrable is not supported and is ignored",
        ]

    def test_constraint_handling(self):
        schema = {
            "x-constraintHandling": {
                "errorMode": "bad_rows",
                "description": "d",
                "primaryKeyViolations": {"duplicates": "bad_rows", "nulls": "bad_rows"},
                "uniqueConstraintViolations": "bad_rows",
                "notNullViolations": "fill_default",
                "badRowsOutput": {"enabled": False, "format": "json"},
                "validationOptions": {"maxErrorsPerRow": 10},
            }
        }
        suffix = " (errorMode applies to all constraints)"
        assert unsupported_extension_keys(schema) == [
            "x-constraintHandling.notNullViolations is not supported and is ignored" + suffix,
            "x-constraintHandling.badRowsOutput is not supported and is ignored" + suffix,
            "x-constraintHandling.validationOptions is not supported and is ignored" + suffix,
        ]

    def test_violation_handling_that_contradicts_the_error_mode_is_reported(self):
        schema = {
            "x-constraintHandling": {
                "errorMode": "fail_fast",
                "primaryKeyViolations": {"duplicates": "bad_rows"},
                "uniqueConstraintViolations": "fail_fast",
            }
        }
        assert unsupported_extension_keys(schema) == [
            "x-constraintHandling.primaryKeyViolations is not supported and is ignored "
            "(errorMode applies to all constraints)"
        ]

    def test_validation(self):
        schema = {
            "x-validation": {
                "description": "d",
                "badRowsHandling": {
                    "enabled": False,
                    "outputPath": "bad_rows",
                    "fileFormat": "parquet",
                    "includeOriginalRow": True,
                    "includeValidationErrors": True,
                    "maxBadRowsPercent": 10.0,
                    "compress": True,
                },
                "uniquenessHandling": {"strategy": "first_wins", "options": [], "scope": "file"},
                "fieldValidations": {
                    "age": {
                        "range": {"min": 0, "max": 5, "step": 1},
                        "onViolation": {"range": "warning"},
                        "mask": True,
                    },
                    "name": {"onViolation": {"required": "bad_rows"}, "required": True},
                },
                "crossFieldValidations": [{"name": "x"}],
                "globalValidations": {"maxNullFieldsPerRow": 3},
                "extra": {"a": 1},
            }
        }
        ignored = "is not supported and is ignored"
        assert unsupported_extension_keys(schema) == [
            f"x-validation.extra {ignored}",
            f"x-validation.badRowsHandling.outputPath {ignored} "
            "(rejected rows are written by the import, not by the validator)",
            f"x-validation.badRowsHandling.fileFormat {ignored} "
            "(rejected rows are written by the import, not by the validator)",
            f"x-validation.badRowsHandling.includeOriginalRow {ignored} "
            "(rejected rows are written by the import, not by the validator)",
            f"x-validation.badRowsHandling.includeValidationErrors {ignored} "
            "(rejected rows are written by the import, not by the validator)",
            f"x-validation.badRowsHandling.enabled=false {ignored} "
            "(rejected rows are always removed and reported)",
            f"x-validation.badRowsHandling.compress {ignored}",
            f"x-validation.uniquenessHandling.scope {ignored}",
            f"x-validation.fieldValidations.age.range.step {ignored}",
            f"x-validation.fieldValidations.age.onViolation {ignored} "
            "(a violation always rejects the row)",
            f"x-validation.fieldValidations.age.mask {ignored}",
            f"x-validation.crossFieldValidations {ignored}",
            f"x-validation.globalValidations {ignored}",
        ]

    def test_data_quality(self):
        schema = {
            "x-dataQuality": {
                "description": "d",
                "completeness": {"enabled": True, "minimumFillRate": 0.95},
                "uniqueness": {"enabled": False, "uniqueFields": ["id"]},
                "qualityThresholds": {"completeness": {"minimum": 0.9}},
                "reporting": {},
                "fieldSpecificRules": {
                    "age": {"min": 0, "dataType": "integer"},
                    "phone": {"pattern": "x", "standardizeFormat": True, "required": True},
                    "ok": {"required": False, "standardizeFormat": False},
                },
                "fieldQualityRules": {
                    "code": {
                        "rules": ["not_null"],
                        "severity": "error",
                        "parameters": {"min_length": 1, "blocked_domains": ["x"]},
                    }
                },
            }
        }
        applied = (
            "(only the parameters min_length, max_length, pattern, min_value and max_value "
            "are applied)"
        )
        ignored = "is not supported and is ignored"
        assert unsupported_extension_keys(schema) == [
            f"x-dataQuality.completeness {ignored}",
            f"x-dataQuality.qualityThresholds {ignored}",
            f"x-dataQuality.fieldSpecificRules.age.dataType {ignored}",
            f"x-dataQuality.fieldSpecificRules.phone.standardizeFormat {ignored}",
            f"x-dataQuality.fieldSpecificRules.phone.required {ignored}",
            f"x-dataQuality.fieldQualityRules.code.rules {ignored} {applied}",
            f"x-dataQuality.fieldQualityRules.code.severity {ignored} {applied}",
            f"x-dataQuality.fieldQualityRules.code.parameters.blocked_domains {ignored} {applied}",
        ]

    def test_disabled_data_quality_reports_nothing(self):
        schema = {"x-dataQuality": {"enabled": False, "completeness": {"minimumFillRate": 1}}}
        assert unsupported_extension_keys(schema) == []

    def test_malformed_content_never_raises(self):
        schema = {
            "x-pii": "yes",
            "x-transformations": "x",
            "x-calculatedColumns": [],
            "x-columnMapping": 3,
            "x-primaryKey": [],
            "x-uniqueConstraints": {"a": 1},
            "x-constraintHandling": "bad_rows",
            "x-validation": {
                "badRowsHandling": [],
                "uniquenessHandling": "x",
                "fieldValidations": {"a": 5, "b": {"onViolation": "x", "range": 3}},
                "crossFieldValidations": 3,
            },
            "x-dataQuality": {
                "fieldSpecificRules": [1],
                "fieldQualityRules": {"a": 1, "b": {"parameters": 2}},
            },
        }
        assert unsupported_extension_keys(schema) == [
            "x-pii is documentation only: no masking is applied",
            "x-validation.crossFieldValidations is not supported and is ignored",
        ]

    def test_order_is_stable(self):
        schema = {
            "x-dataQuality": {"completeness": {"a": 1}},
            "x-validation": {"globalValidations": {"a": 1}},
            "x-pii": {},
            "x-columnMapping": {"standardizationRules": {"a": 1}},
        }
        first = unsupported_extension_keys(schema)
        assert first == unsupported_extension_keys(schema)
        assert [w.split(".")[0].split(" ")[0] for w in first] == [
            "x-pii",
            "x-columnMapping",
            "x-validation",
            "x-dataQuality",
        ]

    def test_shipped_standard_only_reports_pii(self):
        schema = json.loads(STANDARD_CSV.read_text())

        # Everything else the standard contains is applied by the import
        assert unsupported_extension_keys(schema) == [
            "x-pii is documentation only: no masking is applied"
        ]

    def test_content_of_older_standards_is_reported(self):
        # Keys the shipped standard used to contain and that no processor ever read
        schema = {
            "x-pii": {"fields": {"name": {"isPII": True}}},
            "x-transformations": {"moneyType": {"currencySymbols": ["$"]}},
            "x-calculatedColumns": {
                "constants": [],
                "partitionColumns": ["data_source"],
                "options": {"skipIfExists": False},
            },
            "x-columnMapping": {"standardizationRules": {"maxLength": 64}},
            "x-constraintHandling": {"errorMode": "bad_rows", "badRowsOutput": {"enabled": True}},
            "x-validation": {
                "badRowsHandling": {"outputPath": "bad_rows"},
                "crossFieldValidations": [{"name": "n", "rule": "r"}],
                "globalValidations": [{"name": "n"}],
            },
            "x-dataQuality": {"completeness": {"enabled": True, "minimumFillRate": 0.9}},
        }

        warnings = unsupported_extension_keys(schema)

        assert len(warnings) == len(set(warnings))
        assert "x-pii is documentation only: no masking is applied" in warnings
        assert (
            "x-transformations.moneyType is not read "
            "(use x-transformations.column_transformations.<column>.money_conversion)" in warnings
        )
        assert "x-columnMapping.standardizationRules is not supported and is ignored" in warnings
        assert any(w.startswith("x-constraintHandling.badRowsOutput ") for w in warnings)
        assert any(w.startswith("x-validation.crossFieldValidations ") for w in warnings)
        assert any(w.startswith("x-validation.globalValidations ") for w in warnings)
        assert any(w.startswith("x-validation.badRowsHandling.outputPath ") for w in warnings)
        assert any(
            w.startswith("x-calculatedColumns.partitionColumns is recorded only") for w in warnings
        )
        assert any(w.startswith("x-dataQuality.completeness ") for w in warnings)
        # what the loaders do apply is not reported
        assert not any(w.startswith("x-primaryKey") for w in warnings)
        assert not any(w.startswith("x-uniqueConstraints") for w in warnings)
        assert not any("explicitMappings" in w for w in warnings)


# ============================================================================ end to end


@pytest.fixture(scope="module")
def schema():
    return json.loads(STANDARD_CSV.read_text())


class TestShippedStandard:
    def test_every_loader_accepts_the_standard(self, schema):
        mapper = build_column_mapper(schema)
        assert mapper.config.naming_convention == "snake_case"
        assert mapper.config.case_sensitive is False
        assert build_constraint_validator(schema).config.unique_constraints == [
            "id",
            ("name", "birth_date"),
        ]
        validator = build_data_validator(schema)
        assert {r.field_name for r in validator.config.field_validations} >= {
            "id",
            "email_address",
            "ssn",
        }
        assert build_quality_processor(schema).rules["column_rules"]["age"] == {
            "min_value": 0,
            "max_value": 150,
        }

    def test_standard_with_resolve_column(self, schema):
        mapper = build_column_mapper(schema)
        headers = ["FirstName", "DOB", "id"]
        outputs = mapper.output_names(headers)

        def resolve(name):
            return outputs.get(name) or name

        validator = build_constraint_validator(schema, resolve_column=resolve)
        assert (
            "name",
            "birth_date",
        ) in validator.config.unique_constraints  # already output names


class TestEngineLikePipeline:
    """The order the engine uses: mapping, validation, constraints - with resolved names."""

    SCHEMA = {
        "properties": {
            "Customer ID": {"type": "integer"},
            "Age": {"type": "integer", "minimum": 0},
            "Mail": {"type": "string"},
        },
        "x-columnMapping": {
            "explicitMappings": {"Mail": "email"},
            "namingConvention": "snake_case",
        },
        "x-primaryKey": {"columns": ["Customer ID"]},
        "x-uniqueConstraints": [{"name": "u_mail", "columns": ["email"]}],
        "x-validation": {
            "fieldValidations": {
                "Age": {"range": {"max": 120}},
                "email": {"stringValidation": {"pattern": "@"}},
            },
            "badRowsHandling": {"maxBadRowsPercent": 100},
        },
        "x-dataQuality": {"fieldSpecificRules": {"Age": {"min": 18}}},
    }

    def test_pipeline(self):
        schema = self.SCHEMA
        batch = make_batch(
            **{
                "Customer ID": [1, 2, 2, None, 5, 6],
                "Age": [30, 20, 20, 30, 130, -5],
                "Mail": ["a@x", "b@x", "c@x", "d@x", "e@x", "f@x"],
            }
        )

        mapper = build_column_mapper(schema)
        outputs = mapper.output_names(batch.schema.names)

        def resolve(name):
            return outputs.get(name) or name

        quality = build_quality_processor(schema, resolve_column=resolve)
        validator = build_data_validator(schema, resolve_column=resolve)
        constraints = build_constraint_validator(schema, resolve_column=resolve)

        mapped, _ = mapper.process_batch(batch)
        assert mapped.schema.names == ["customer_id", "age", "email"]

        mapped, quality_results = quality.process_batch(mapped)
        assert {(r.row_index, r.column_name) for r in quality_results} == {(5, "age")}
        assert mapped.num_rows == 6  # report only

        valid, validation_results = validator.process_batch(mapped)
        assert {r.row_index for r in validation_results} == {4}
        assert valid.num_rows == 5

        final, constraint_results = constraints.process_batch(valid)
        # positions are relative to the batch each processor received
        assert {(r.row_index, r.error_code) for r in constraint_results} == {
            (2, "UNIQUE_VIOLATION"),
            (3, "NULL_VIOLATION"),
            (4, "RANGE_VIOLATION"),
        }
        assert values(final, "customer_id") == [1, 2]
