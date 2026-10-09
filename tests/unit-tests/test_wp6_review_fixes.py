"""WP6 review fixes: schema importers (CSV, Excel, SQL, FWF) and the shared type validator."""

import ast
import copy
import os
import re
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

from forklift.schema.csv_schema_importer import CsvSchemaImporter
from forklift.schema.csv_schema_importer import SchemaValidationError as CsvError
from forklift.schema.excel_schema_importer import ExcelSchemaImporter
from forklift.schema.excel_schema_importer import SchemaValidationError as ExcelError
from forklift.schema.fwf.conditional.variants import VariantManager
from forklift.schema.fwf.exceptions import ConditionalSchemaError
from forklift.schema.fwf.exceptions import SchemaValidationError as FwfError
from forklift.schema.fwf.fields.mapping import FieldMapper
from forklift.schema.fwf.fields.positions import PositionCalculator
from forklift.schema.fwf.utils.column_names import ColumnNameProcessor
from forklift.schema.fwf.validation.fields import FieldValidator
from forklift.schema.fwf.validation.parquet_types import ParquetTypeValidator
from forklift.schema.fwf_schema_importer import FwfSchemaImporter
from forklift.schema.naming import camel_case_name, snake_case_name
from forklift.schema.sql_schema_importer import SchemaValidationError as SqlError
from forklift.schema.sql_schema_importer import (
    SqlSchemaImporter,
    output_name_problem,
    sql_identifier_problem,
)
from forklift.schema.types import (
    DataTypeConverter,
    arrow_to_parquet_type_string,
    is_valid_parquet_type,
    parse_parquet_type,
    unify_parquet_types,
)

SCHEMA = "https://json-schema.org/draft/2020-12/schema"
ID = "https://github.com/cornyhorse/forklift/schema-standards/test.json"


def base(**extra):
    return {"$schema": SCHEMA, "$id": ID, "title": "Test", "type": "object", **extra}


def csv_schema(**ext):
    return base(
        properties={"id": {"type": "integer"}, "name": {"type": "string"}},
        required=["id"],
        **{"x-csv": {"delimiter": ",", **ext}},
    )


def excel_schema(**ext):
    return base(
        properties={"id": {"type": "integer"}},
        **{"x-excel": {"sheets": [{"select": {"name": "Sheet1"}}], **ext}},
    )


def sql_schema(tables=None, **ext):
    return base(
        **{"x-sql": {"tables": tables or [{"select": {"schema": "dbo", "name": "users"}}], **ext}}
    )


def fwf_schema(fields=None, **ext):
    return base(
        properties={"id": {"type": "string"}},
        **{
            "x-fwf": {
                "fields": fields
                or [
                    {"name": "id", "start": 1, "length": 5, "parquetType": "string"},
                    {"name": "name", "start": 6, "length": 10, "parquetType": "string"},
                ],
                **ext,
            }
        },
    )


def fwf_conditional_schema(amt_h="int32", amt_d="double"):
    flag = {"name": "record_type", "start": 1, "length": 1, "parquetType": "string"}
    return base(
        properties={"record_type": {"type": "string"}},
        **{
            "x-fwf": {
                "conditionalSchemas": {
                    "flagColumn": dict(flag),
                    "schemas": [
                        {
                            "flagValue": "H",
                            "fields": [
                                dict(flag),
                                {"name": "amt", "start": 2, "length": 5, "parquetType": amt_h},
                                {"name": "hdr", "start": 7, "length": 3, "parquetType": "string"},
                            ],
                        },
                        {
                            "flagValue": "D",
                            "fields": [
                                dict(flag),
                                {"name": "amt", "start": 2, "length": 5, "parquetType": amt_d},
                            ],
                        },
                    ],
                }
            }
        },
    )


# --------------------------------------------------------------------------------------------
# Strict Parquet type validation (shared by all four importers)
# --------------------------------------------------------------------------------------------

VALID_TYPES = [
    "int8",
    "uint64",
    "float32",
    "double",
    "bool",
    "string",
    "large_string",
    "binary",
    "large_binary",
    "date32",
    "date64",
    "time32[s]",
    "time32[ms]",
    "time64[us]",
    "time64[ns]",
    "timestamp[s]",
    "timestamp[ns]",
    "timestamp[us, tz=UTC]",
    "timestamp[ms,tz=America/New_York]",
    "duration[ms]",
    "decimal128(10,2)",
    "decimal128(38, 18)",
    "decimal256(76,10)",
    "list<string>",
    "list<list<int32>>",
    "large_list<double>",
    "dictionary<values=string, indices=int32>",
    "struct",
]
INVALID_TYPES = [
    "timestamp[foo]",
    "timestamp[]",
    "timestamp",
    "decimal128(99,abc)",
    "decimal128(0,0)",
    "decimal128(39,2)",
    "decimal128(5,6)",
    "decimal128()",
    "decimal128(5)",
    "decimal256(77,0)",
    "list<>",
    "list<foo>",
    "list<string",
    "time32[us]",
    "time64[s]",
    "duration[invalid]",
    "dictionary<values=string, indices=string>",
    "dictionary<values=list<int32>, indices=int32>",
    "timestamp[us, tz=]",
    "int128",
    "",
    "   ",
    None,
    5,
    ["string"],
    {"a": 1},
]


class TestStrictParquetTypes:
    @pytest.mark.parametrize("parquet_type", VALID_TYPES)
    def test_valid_types_are_accepted_and_round_trip(self, parquet_type):
        assert is_valid_parquet_type(parquet_type)
        arrow_type = parse_parquet_type(parquet_type)
        assert parse_parquet_type(arrow_to_parquet_type_string(arrow_type)) == arrow_type

    @pytest.mark.parametrize("parquet_type", INVALID_TYPES)
    def test_malformed_types_are_rejected(self, parquet_type):
        assert not is_valid_parquet_type(parquet_type)
        with pytest.raises(ValueError):
            parse_parquet_type(parquet_type)

    def test_pathological_input_is_rejected_cheaply(self):
        assert not is_valid_parquet_type("list<" * 50 + "int32" + ">" * 50)
        assert not is_valid_parquet_type("x" * 10_000)

    @pytest.mark.parametrize("bad", ["timestamp[foo]", "decimal128(99,abc)", "list<>"])
    def test_every_importer_rejects_prefix_only_matches(self, bad):
        csv = csv_schema(parquetTypeMapping={"id": bad})
        with pytest.raises(CsvError, match="Invalid Parquet type"):
            CsvSchemaImporter(csv)

        excel = excel_schema()
        excel["x-excel"]["sheets"][0]["columns"] = [
            {"name": "id", "position": 1, "parquetType": bad}
        ]
        with pytest.raises(ExcelError, match="invalid Parquet type"):
            ExcelSchemaImporter(excel)

        sql = sql_schema([{"select": {"name": "t"}, "columns": {"c": {"parquetType": bad}}}])
        with pytest.raises(SqlError, match="invalid Parquet type"):
            SqlSchemaImporter(sql)

        fwf = fwf_schema([{"name": "id", "start": 1, "length": 5, "parquetType": bad}])
        with pytest.raises(FwfError, match="invalid Parquet type"):
            FwfSchemaImporter(fwf)

        assert not ParquetTypeValidator.is_valid_parquet_type(bad)

    def test_arrow_to_parquet_type_string_is_lossless(self):
        assert arrow_to_parquet_type_string(pa.decimal128(12, 4)) == "decimal128(12,4)"
        assert arrow_to_parquet_type_string(pa.timestamp("ns")) == "timestamp[ns]"
        assert (
            arrow_to_parquet_type_string(pa.timestamp("us", tz="UTC")) == "timestamp[us, tz=UTC]"
        )
        assert arrow_to_parquet_type_string(pa.float64()) == "double"
        assert arrow_to_parquet_type_string(pa.list_(pa.int16())) == "list<int16>"
        with pytest.raises(ValueError):
            arrow_to_parquet_type_string(pa.float16())

    def test_json_schema_mapping_no_longer_degrades_decimals_to_strings(self):
        assert DataTypeConverter.arrow_to_json_schema_type(pa.decimal128(10, 2)) == {
            "type": "number"
        }
        assert DataTypeConverter.arrow_to_json_schema_type(pa.decimal256(40, 2)) == {
            "type": "number"
        }

    @pytest.mark.parametrize(
        "types, expected",
        [
            (["int32", "double"], "double"),
            (["int8", "int32"], "int32"),
            (["int8", "uint8"], "int16"),
            (["uint32", "int32"], "int64"),
            (["uint64", "int64"], "double"),
            (["float32", "int16"], "float32"),
            (["float32", "int32"], "double"),
            (["decimal128(5,2)", "decimal128(10,5)"], "decimal128(10,5)"),
            (["decimal128(38,0)", "decimal128(10,10)"], "decimal256(48,10)"),
            (["date32", "timestamp[s]"], "timestamp[s]"),
            (["timestamp[s]", "timestamp[ns]"], "timestamp[ns]"),
            (["date32", "date64"], "date64"),
            (["duration[s]", "duration[ms]"], "duration[ms]"),
            (["string", "binary"], "binary"),
            (["string", "large_string"], "large_string"),
            (["string", "string"], "string"),
        ],
    )
    def test_unify_widens(self, types, expected):
        assert unify_parquet_types(types) == expected

    @pytest.mark.parametrize(
        "types",
        [
            ["string", "int32"],
            ["bool", "double"],
            ["timestamp[s]", "timestamp[s, tz=UTC]"],
            ["timestamp[s, tz=UTC]", "timestamp[s, tz=Europe/Paris]"],
            ["decimal128(5,2)", "double"],
            ["list<int32>", "int32"],
            ["int32", "not-a-type"],
            [["int32"], "int32"],
            [],
        ],
    )
    def test_unify_rejects_incompatible(self, types):
        assert unify_parquet_types(types) is None


# --------------------------------------------------------------------------------------------
# A1.1 Crashes instead of SchemaValidationError
# --------------------------------------------------------------------------------------------


class TestNullableTypes:
    def test_csv_accepts_nullable_type_array_and_anyof(self):
        schema = csv_schema()
        schema["properties"]["n"] = {"type": ["string", "null"], "maxLength": 5}
        schema["properties"]["a"] = {
            "anyOf": [{"type": "integer", "minimum": 0}, {"type": "null"}]
        }
        schema["properties"]["o"] = {"oneOf": [{"type": "number"}, {"type": "null"}]}
        CsvSchemaImporter(schema)

    def test_excel_accepts_nullable_type_array_and_anyof(self):
        schema = excel_schema()
        schema["properties"]["n"] = {"type": ["string", "null"]}
        schema["properties"]["a"] = {"anyOf": [{"type": "integer"}, {"type": "null"}]}
        schema["x-excel"]["sheets"][0]["columns"] = [
            {"name": "n", "position": "A", "type": ["string", "null"], "format": "date"},
            {"name": "a", "position": "B", "anyOf": [{"type": "integer"}, {"type": "null"}]},
        ]
        ExcelSchemaImporter(schema)

    def test_sql_accepts_nullable_column_types(self):
        schema = sql_schema(
            [
                {
                    "select": {"name": "t"},
                    "columns": {
                        "c": {"type": ["string", "null"], "maxLength": 10},
                        "d": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                    },
                }
            ]
        )
        SqlSchemaImporter(schema)

    def test_fwf_accepts_nullable_field_types(self):
        schema = fwf_schema(
            [{"name": "id", "start": 1, "length": 5, "type": ["string", "null"]}],
        )
        FwfSchemaImporter(schema)

    @pytest.mark.parametrize(
        "bad",
        [
            {"type": ["string", 5]},
            {"type": []},
            {"type": ["null"]},
            {"type": "null"},
            {"anyOf": []},
            {"anyOf": ["string"]},
            {"anyOf": [{"type": "null"}]},
            {"type": {"a": 1}},
            {},
        ],
    )
    def test_malformed_type_declarations_are_validation_errors(self, bad):
        schema = csv_schema()
        schema["properties"]["x"] = bad
        with pytest.raises(CsvError, match="[Ff]ield 'x'"):
            CsvSchemaImporter(schema)
        schema = excel_schema()
        schema["properties"]["x"] = bad
        with pytest.raises(ExcelError, match="[Ff]ield 'x'"):
            ExcelSchemaImporter(schema)


class TestMalformedSchemasRaiseValidationErrors:
    def test_sql_columns_list_is_reported_not_crash(self):
        schema = sql_schema([{"select": {"name": "t"}, "columns": [1, 2]}])
        with pytest.raises(SqlError, match="Table 0 columns must be an object"):
            SqlSchemaImporter(schema)

    @pytest.mark.parametrize("properties", [[], "x", 5, [{"a": 1}]])
    def test_properties_not_an_object(self, properties):
        with pytest.raises(CsvError, match="Properties must be a dictionary"):
            CsvSchemaImporter({**csv_schema(), "properties": properties})
        with pytest.raises(ExcelError, match="Properties must be a dictionary"):
            ExcelSchemaImporter({**excel_schema(), "properties": properties})
        with pytest.raises(FwfError, match="Properties must be a dictionary"):
            FwfSchemaImporter({**fwf_schema(), "properties": properties})

    @pytest.mark.parametrize("schema_id", [5, ["x"], {"a": 1}, True])
    def test_non_string_id(self, schema_id):
        for importer, schema, error in (
            (CsvSchemaImporter, csv_schema(), CsvError),
            (ExcelSchemaImporter, excel_schema(), ExcelError),
            (SqlSchemaImporter, sql_schema(), SqlError),
            (FwfSchemaImporter, fwf_schema(), FwfError),
        ):
            schema["$id"] = schema_id
            with pytest.raises(error, match=r"\$id"):
                importer(schema)

    @pytest.mark.parametrize("parquet_type", [5, ["string"], {"a": 1}, True])
    def test_non_string_parquet_type(self, parquet_type):
        with pytest.raises(CsvError, match="Invalid Parquet type"):
            CsvSchemaImporter(csv_schema(parquetTypeMapping={"id": parquet_type}))
        excel = excel_schema()
        excel["x-excel"]["sheets"][0]["columns"] = [
            {"name": "id", "position": 1, "parquetType": parquet_type}
        ]
        with pytest.raises(ExcelError, match="invalid Parquet type"):
            ExcelSchemaImporter(excel)
        sql = sql_schema(
            [{"select": {"name": "t"}, "columns": {"c": {"parquetType": parquet_type}}}]
        )
        with pytest.raises(SqlError, match="invalid Parquet type"):
            SqlSchemaImporter(sql)
        fwf = fwf_schema([{"name": "id", "start": 1, "length": 5, "parquetType": parquet_type}])
        with pytest.raises(FwfError, match="invalid Parquet type"):
            FwfSchemaImporter(fwf)

    def test_extension_must_be_an_object(self):
        for importer, key, error in (
            (CsvSchemaImporter, "x-csv", CsvError),
            (ExcelSchemaImporter, "x-excel", ExcelError),
            (SqlSchemaImporter, "x-sql", SqlError),
            (FwfSchemaImporter, "x-fwf", FwfError),
        ):
            with pytest.raises(error, match=f"{key} must be an object"):
                importer({**base(), key: ["not", "an", "object"]})

    def test_non_object_schema_file(self, tmp_path):
        path = tmp_path / "s.json"
        path.write_text("[1, 2]")
        for importer, error in (
            (CsvSchemaImporter, CsvError),
            (ExcelSchemaImporter, ExcelError),
            (SqlSchemaImporter, SqlError),
            (FwfSchemaImporter, FwfError),
        ):
            with pytest.raises(error, match="schema root must be a JSON object"):
                importer(path)

    @pytest.mark.parametrize("required", ["id", ["missing"], [1], {"id": True}, 5])
    def test_required_must_be_a_list_of_known_property_names(self, required):
        with pytest.raises(CsvError, match="required"):
            CsvSchemaImporter({**csv_schema(), "required": required})
        with pytest.raises(ExcelError, match="required"):
            ExcelSchemaImporter({**excel_schema(), "required": required})
        with pytest.raises(FwfError, match="required"):
            FwfSchemaImporter({**fwf_schema(), "required": required})

    def test_required_string_is_not_split_into_characters(self):
        importer = CsvSchemaImporter({**csv_schema(), "required": "id"}, validate=False)
        assert importer.required == []
        assert CsvSchemaImporter(csv_schema()).required == ["id"]

    def test_fwf_required_may_name_x_fwf_fields(self):
        schema = fwf_schema()
        schema["properties"] = {}
        schema["required"] = ["name"]
        FwfSchemaImporter(schema)

    def test_min_greater_than_max(self):
        schema = csv_schema()
        schema["properties"]["n"] = {"type": "integer", "minimum": 5, "maximum": 1}
        with pytest.raises(CsvError, match="minimum must not exceed maximum"):
            CsvSchemaImporter(schema)
        schema["properties"]["n"] = {"type": "string", "minLength": 5, "maxLength": 1}
        with pytest.raises(CsvError, match="minLength must not exceed maxLength"):
            CsvSchemaImporter(schema)
        schema = excel_schema()
        schema["properties"]["n"] = {"type": "number", "minimum": 5, "maximum": 1}
        with pytest.raises(ExcelError, match="minimum must not exceed maximum"):
            ExcelSchemaImporter(schema)
        sql = sql_schema(
            [
                {
                    "select": {"name": "t"},
                    "columns": {"c": {"type": "integer", "minimum": 5, "maximum": 1}},
                }
            ]
        )
        with pytest.raises(SqlError, match="minimum exceeds maximum"):
            SqlSchemaImporter(sql)

    def test_invalid_footer_regex(self):
        with pytest.raises(CsvError, match="footer.pattern"):
            CsvSchemaImporter(csv_schema(footer={"mode": "regex", "pattern": "(unclosed"}))
        excel = excel_schema()
        excel["x-excel"]["sheets"][0]["footer"] = {"mode": "regex", "pattern": "(unclosed"}
        with pytest.raises(ExcelError, match="footer.pattern"):
            ExcelSchemaImporter(excel)

    @pytest.mark.parametrize("delimiter", ["ab", "||", "", 5, [","]])
    def test_invalid_delimiter(self, delimiter):
        with pytest.raises(CsvError, match="Invalid delimiter specification"):
            CsvSchemaImporter(csv_schema(delimiter=delimiter))

    @pytest.mark.parametrize("delimiter", [",", ";", "\t", "|", "auto", ":"])
    def test_valid_delimiter(self, delimiter):
        CsvSchemaImporter(csv_schema(delimiter=delimiter))

    def test_invalid_field_regex_still_rejected(self):
        schema = csv_schema()
        schema["properties"]["name"] = {"type": "string", "pattern": "["}
        with pytest.raises(CsvError, match="Invalid regex pattern for field 'name'"):
            CsvSchemaImporter(schema)

    def test_unhashable_enum_like_values_do_not_crash(self):
        csv = csv_schema(
            header={"mode": ["x"]}, footer={"mode": ["x"]}, case={"standardizeNames": ["x"]}
        )
        with pytest.raises(CsvError):
            CsvSchemaImporter(csv)
        with pytest.raises(FwfError):
            FwfSchemaImporter(
                fwf_schema(case={"standardizeNames": ["x"], "dedupeNames": {"a": 1}})
            )
        with pytest.raises(FwfError):
            FwfSchemaImporter(
                fwf_schema([{"name": "a", "start": 1, "length": 1, "alignment": ["left"]}])
            )

    def test_absurd_field_extent_does_not_exhaust_memory(self):
        schema = fwf_schema([{"name": "a", "start": 1, "length": 10**12}])
        with pytest.raises(FwfError, match="maximum record width"):
            FwfSchemaImporter(schema)


def _mutations():
    return [None, 5, 1.5, True, "text", "", [], [1], ["a"], {}, {"a": 1}, [{"a": 1}]]


def _paths(node, prefix=()):
    yield prefix
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _paths(value, prefix + (key,))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _paths(value, prefix + (index,))


def _replace(root, path, value, delete=False):
    root = copy.deepcopy(root)
    if not path:
        return value
    parent = root
    for step in path[:-1]:
        parent = parent[step]
    if delete:
        if isinstance(parent, dict):
            del parent[path[-1]]
        else:
            parent.pop(path[-1])
    else:
        parent[path[-1]] = value
    return root


def _fuzz_documents():
    csv = csv_schema(
        encodingPriority=["utf-8", "latin-1"],
        quotechar='"',
        escapechar="\\",
        header={"mode": "stability_scan", "keywords": ["id"]},
        footer={"mode": "regex", "pattern": "^TOTAL"},
        nulls={"global": [""], "perColumn": {"id": ["NA"]}},
        case={"standardizeNames": "snake_case", "dedupeNames": "suffix"},
        parquetTypeMapping={"id": "int64", "name": "string"},
    )
    csv["properties"]["tags"] = {"type": ["array", "null"], "items": {"type": "string"}}
    csv["properties"]["opt"] = {"anyOf": [{"type": "integer", "minimum": 0}, {"type": "null"}]}
    excel = excel_schema(
        valuesOnly=True,
        dateSystem="1900",
        nulls={"global": [""], "perColumn": {}},
    )
    excel["x-excel"]["sheets"][0].update(
        {
            "header": {"row": 1, "mode": "present"},
            "dataStartRow": 2,
            "footer": {"mode": "regex", "pattern": "^TOTAL"},
            "columns": [
                {"name": "id", "position": "A", "type": "integer", "parquetType": "int64"},
                {"name": "when", "position": 2, "type": ["string", "null"], "format": "date"},
            ],
        }
    )
    sql = sql_schema(
        [
            {
                "select": {"schema": "dbo", "name": "Order Details"},
                "outputName": "orders",
                "required": ["id"],
                "columns": {
                    "id": {"type": "integer", "minimum": 0, "parquetType": "int32"},
                    "s": {"type": ["string", "null"], "maxLength": 5, "pattern": "^a"},
                },
            }
        ],
        parquetTypeMapping={"sqlToParquet": {"DECIMAL": "decimal128(18,4)"}},
    )
    fwf = fwf_schema(
        trim={"id": True},
        nulls={"global": [""], "perColumn": {}},
        headerRows=1,
        footerRows=0,
        encoding="utf-8",
        case={"standardizeNames": "postgres", "dedupeNames": "suffix"},
    )
    fwf["x-fwf"]["fields"][0].update({"type": "string", "alignment": "left", "padChar": " "})
    return [
        (CsvSchemaImporter, CsvError, csv),
        (ExcelSchemaImporter, ExcelError, excel),
        (SqlSchemaImporter, SqlError, sql),
        (FwfSchemaImporter, FwfError, fwf),
        (FwfSchemaImporter, FwfError, fwf_conditional_schema()),
    ]


class TestFuzzedSchemasNeverCrash:
    """Replacing any value of an otherwise valid schema by a wrong-typed one (or removing it) must
    either still load or raise the importer's SchemaValidationError - never TypeError/AttributeError
    and friends."""

    @pytest.mark.parametrize("index", range(5))
    def test_wrong_typed_values(self, index):
        importer, error, document = _fuzz_documents()[index]
        importer(copy.deepcopy(document))  # the unmutated document is valid
        for path in _paths(document):
            if not path:
                continue
            for value in _mutations():
                mutated = _replace(document, path, value)
                try:
                    importer(mutated)
                except error:
                    pass
                except Exception as exc:  # pragma: no cover - failure path
                    pytest.fail(f"{importer.__name__} raised {exc!r} for {path} = {value!r}")
            try:
                importer(_replace(document, path, None, delete=True))
            except error:
                pass
            except Exception as exc:  # pragma: no cover - failure path
                pytest.fail(f"{importer.__name__} raised {exc!r} when removing {path}")


# --------------------------------------------------------------------------------------------
# A1.2 Encodings
# --------------------------------------------------------------------------------------------

ENCODINGS = ["iso-8859-1", "ISO8859-1", "cp1250", "utf-16", "cp037", "latin-1", "UTF8", "ascii"]


class TestEncodings:
    @pytest.mark.parametrize("encoding", ENCODINGS)
    def test_csv_accepts_any_known_text_encoding(self, encoding):
        importer = CsvSchemaImporter(csv_schema(encodingPriority=[encoding, "utf-8"]))
        assert importer.get_encoding_priority() == [encoding, "utf-8"]

    @pytest.mark.parametrize("encoding", ENCODINGS)
    def test_fwf_accepts_any_known_text_encoding(self, encoding):
        importer = FwfSchemaImporter(fwf_schema(encoding=encoding))
        assert importer.get_encoding() == encoding

    @pytest.mark.parametrize("encoding", ["not-a-codec", "hex", "base64", "", 5, None, ["utf-8"]])
    def test_unknown_or_binary_codecs_rejected(self, encoding):
        with pytest.raises(CsvError, match="Invalid encoding"):
            CsvSchemaImporter(csv_schema(encodingPriority=[encoding]))
        with pytest.raises(FwfError, match="Invalid encoding"):
            FwfSchemaImporter(fwf_schema(encoding=encoding))


# --------------------------------------------------------------------------------------------
# A1.3 Excel generated schema (singular "sheet")
# --------------------------------------------------------------------------------------------


class TestExcelSingularSheet:
    def generated(self, sheet):
        # exactly the shape schema/processors/json_schema.py:generate_excel_extension emits
        return base(
            properties={"id": {"type": "integer"}},
            **{
                "x-excel": {
                    "sheet": sheet,
                    "header": {"mode": "present"},
                    "skipRows": 0,
                    "skipFooter": 0,
                    "nulls": {"global": ["", "NA"]},
                    "validation": {"enabled": True, "onError": "log", "maxErrors": 1000},
                }
            },
        )

    def test_sheet_name(self):
        importer = ExcelSchemaImporter(self.generated("Data"))
        assert importer.sheets == [{"select": {"name": "Data"}, "header": {"mode": "present"}}]
        assert importer.get_column_mapping() == {}

    def test_sheet_index(self):
        importer = ExcelSchemaImporter(self.generated(0))
        assert importer.sheets[0]["select"] == {"index": 0}

    def test_sheet_select_object(self):
        importer = ExcelSchemaImporter(self.generated({"regex": "^Q[1-4]$"}))
        assert importer.sheets[0]["select"] == {"regex": "^Q[1-4]$"}

    def test_plural_form_still_works(self):
        assert len(ExcelSchemaImporter(excel_schema()).sheets) == 1

    @pytest.mark.parametrize("sheet", [None, True, 1.5, "", [], -1])
    def test_bad_single_sheet(self, sheet):
        with pytest.raises(ExcelError, match="[Ss]heet"):
            ExcelSchemaImporter(self.generated(sheet))

    def test_both_forms_is_ambiguous(self):
        schema = excel_schema(sheet="Data")
        with pytest.raises(ExcelError, match="either 'sheets' or 'sheet'"):
            ExcelSchemaImporter(schema)

    def test_neither_form_is_an_error(self):
        with pytest.raises(ExcelError, match="x-excel.sheets array is required"):
            ExcelSchemaImporter(base(**{"x-excel": {"valuesOnly": True}}))

    def test_sheets_must_be_a_list(self):
        schema = base(**{"x-excel": {"sheets": {"select": {"name": "x"}}}})
        with pytest.raises(ExcelError, match="x-excel.sheets must be an array"):
            ExcelSchemaImporter(schema)

    def test_sheet_select_values_are_validated(self):
        for select in ({"name": 5}, {"index": -1}, {"index": True}, {"regex": "("}):
            schema = excel_schema()
            schema["x-excel"]["sheets"][0]["select"] = select
            with pytest.raises(ExcelError, match="select"):
                ExcelSchemaImporter(schema)

    def test_generated_schema_from_the_real_generator_loads(self, tmp_path):
        openpyxl = pytest.importorskip("openpyxl")
        from forklift.api import generate_schema_from_excel

        path = tmp_path / "book.xlsx"
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = "Data"
        sheet.append(["id", "name"])
        sheet.append([1, "a"])
        workbook.save(path)

        for sheet_name in (None, "Data"):
            schema = generate_schema_from_excel(input_path=str(path), sheet_name=sheet_name)
            assert ExcelSchemaImporter(schema).sheets


# --------------------------------------------------------------------------------------------
# A1.5 SQL schema importer
# --------------------------------------------------------------------------------------------

INJECTION_NAMES = [
    "users WHERE 1=0 UNION SELECT password FROM admin--",
    "users; DROP TABLE users",
    "users'",
    'users"',
    "users`",
    "users\\",
    "users--comment",
    "users/*x*/",
    "us\x00ers",
    "us\ners",
    "us\ters",
    "us ers",
    "",
    " users",
    "users ",
    "a" * 129,
    "[users]",
    "users)",
]


class TestSqlIdentifiers:
    @pytest.mark.parametrize("name", INJECTION_NAMES)
    def test_table_name_rejected_at_load(self, name):
        schema = sql_schema([{"select": {"schema": "dbo", "name": name}}])
        with pytest.raises(SqlError, match="Table 0 select.name"):
            SqlSchemaImporter(schema)

    @pytest.mark.parametrize("name", INJECTION_NAMES)
    def test_schema_name_rejected_at_load(self, name):
        schema = sql_schema([{"select": {"schema": name, "name": "users"}}])
        with pytest.raises(SqlError, match="Table 0 select.schema"):
            SqlSchemaImporter(schema)

    @pytest.mark.parametrize(
        "name",
        [
            "users",
            "Order Details",
            "_x",
            "2020_sales",
            "Ünïcode_Tàble",
            "表",
            "v1.0_data",
            "my-table",
            "#tmp",
            "a$b",
        ],
    )
    def test_plausible_identifiers_are_accepted(self, name):
        importer = SqlSchemaImporter(sql_schema([{"select": {"schema": "dbo", "name": name}}]))
        assert importer.get_table_list() == [("dbo", name, None)]

    def test_error_message_does_not_contain_control_characters(self):
        schema = sql_schema([{"select": {"name": "us\x00ers\nINSERT"}}])
        with pytest.raises(SqlError) as excinfo:
            SqlSchemaImporter(schema)
        assert "\x00" not in str(excinfo.value)
        assert "INSERT" not in str(excinfo.value)

    @pytest.mark.parametrize(
        "output_name",
        [
            "../../../tmp/evil",
            "..",
            "a/b",
            "a\\b",
            "/abs",
            "evil/../x",
            "name..x",
            "x.",
            ".hidden",
            "",
            "a b",
            "a" * 129,
            "x\x00y",
            5,
            ["x"],
        ],
    )
    def test_output_name_must_be_a_plain_stem(self, output_name):
        schema = sql_schema([{"select": {"name": "users"}, "outputName": output_name}])
        with pytest.raises(SqlError, match="Table 0 outputName"):
            SqlSchemaImporter(schema)

    @pytest.mark.parametrize(
        "output_name", ["users", "users_2024", "a.b", "a-b", "9lives", "x.parquet"]
    )
    def test_plain_output_names_are_accepted(self, output_name):
        schema = sql_schema([{"select": {"name": "users"}, "outputName": output_name}])
        assert SqlSchemaImporter(schema).get_table_list()[0][2] == output_name

    def test_null_output_name_is_allowed(self):
        schema = sql_schema([{"select": {"name": "users"}, "outputName": None}])
        assert SqlSchemaImporter(schema).get_table_list() == [("default", "users", None)]

    def test_helper_functions(self):
        assert sql_identifier_problem("Order Details") is None
        assert "comment" in sql_identifier_problem("a--b")
        assert sql_identifier_problem(5) == "must be a string"
        assert output_name_problem("ok_1") is None
        assert output_name_problem("../x")

    def test_pattern_only_selection_is_rejected(self):
        for pattern in ("*.*", "dbo.*", "dbo.users", "users"):
            schema = sql_schema([{"select": {"pattern": pattern}}])
            with pytest.raises(SqlError, match="pattern-based table selection is not supported"):
                SqlSchemaImporter(schema)

    def test_pattern_next_to_a_name_is_not_dropped(self):
        schema = sql_schema([{"select": {"name": "users", "pattern": "dbo.users"}}])
        assert SqlSchemaImporter(schema).get_table_list() == [("default", "users", None)]

    def test_invalid_pattern_message_is_unchanged(self):
        schema = sql_schema([{"select": {"pattern": "bad..pattern"}}])
        with pytest.raises(SqlError, match="invalid select.pattern 'bad..pattern'"):
            SqlSchemaImporter(schema)


class TestSqlTypeMapping:
    def test_defaults(self):
        mapping = SqlSchemaImporter(sql_schema()).get_sql_to_parquet_mapping()
        assert mapping["FLOAT"] == "double"
        assert mapping["REAL"] == "float32"
        assert mapping["DOUBLE"] == "double"
        assert mapping["SMALLINT"] == "int16"
        assert mapping["TIME"] == "time64[us]"
        assert mapping["DECIMAL"] == mapping["NUMERIC"] == "decimal128(38,9)"
        assert all(is_valid_parquet_type(value) for value in mapping.values())

    def test_partial_user_mapping_keeps_the_defaults(self):
        schema = sql_schema(parquetTypeMapping={"sqlToParquet": {"INTEGER": "int32"}})
        mapping = SqlSchemaImporter(schema).get_sql_to_parquet_mapping()
        assert mapping["INTEGER"] == "int32"
        assert mapping["VARCHAR"] == "string"
        assert mapping["BIGINT"] == "int64"
        assert len(mapping) >= len(SqlSchemaImporter.DEFAULT_SQL_TO_PARQUET_MAPPING)

    def test_user_keys_are_case_insensitive(self):
        schema = sql_schema(parquetTypeMapping={"sqlToParquet": {"varchar": "large_string"}})
        assert SqlSchemaImporter(schema).get_sql_to_parquet_mapping()["VARCHAR"] == "large_string"

    def test_defaults_are_not_shared_between_instances(self):
        first = SqlSchemaImporter(sql_schema())
        first.get_sql_to_parquet_mapping()["INTEGER"] = "int8"
        assert SqlSchemaImporter(sql_schema()).get_sql_to_parquet_mapping()["INTEGER"] == "int64"

    def test_user_mapping_values_are_validated(self):
        schema = sql_schema(parquetTypeMapping={"sqlToParquet": {"DECIMAL": "decimal128(99,x)"}})
        with pytest.raises(SqlError, match=r"sqlToParquet\['DECIMAL'\] invalid Parquet type"):
            SqlSchemaImporter(schema)
        schema = sql_schema(parquetTypeMapping={"sqlToParquet": ["DECIMAL"]})
        with pytest.raises(SqlError, match="sqlToParquet must be an object"):
            SqlSchemaImporter(schema)

    def test_decimal_uses_real_precision_and_scale(self):
        importer = SqlSchemaImporter(sql_schema())
        assert importer.get_parquet_type_for_sql_type("DECIMAL(18,4)") == "decimal128(18,4)"
        assert importer.get_parquet_type_for_sql_type("numeric", precision=12, scale=3) == (
            "decimal128(12,3)"
        )
        assert importer.get_parquet_type_for_sql_type("DECIMAL(12)") == "decimal128(12,0)"
        assert importer.get_parquet_type_for_sql_type("DECIMAL(50,2)") == "decimal256(50,2)"
        # unknown precision: documented fallback
        assert importer.get_parquet_type_for_sql_type("DECIMAL") == "decimal128(38,9)"
        # nonsense precision: fallback rather than an invalid type
        assert importer.get_parquet_type_for_sql_type("DECIMAL(5,9)") == "decimal128(38,9)"

    def test_decimal_override_flows_through(self):
        schema = sql_schema(parquetTypeMapping={"sqlToParquet": {"DECIMAL": "decimal128(18,4)"}})
        importer = SqlSchemaImporter(schema)
        assert importer.get_sql_to_parquet_mapping()["DECIMAL"] == "decimal128(18,4)"
        assert importer.get_parquet_type_for_sql_type("DECIMAL(10,2)") == "decimal128(18,4)"
        assert importer.get_parquet_type_for_sql_type("NUMERIC(10,2)") == "decimal128(10,2)"

    def test_other_types_and_unknown(self):
        importer = SqlSchemaImporter(sql_schema())
        assert importer.get_parquet_type_for_sql_type("varchar(255)") == "string"
        assert importer.get_parquet_type_for_sql_type("REAL") == "float32"
        assert importer.get_parquet_type_for_sql_type("MYSTERY") is None
        assert importer.get_parquet_type_for_sql_type("x; DROP") is None

    def test_get_table_list_skips_malformed_entries_when_unvalidated(self):
        schema = sql_schema([5, {"select": "x"}, {"select": {"name": "t"}}])
        assert SqlSchemaImporter(schema, validate=False).get_table_list() == [
            ("default", "t", None)
        ]


# --------------------------------------------------------------------------------------------
# A1.6 Column name styles
# --------------------------------------------------------------------------------------------


class TestColumnNameStyles:
    @pytest.mark.parametrize(
        "name, snake, camel",
        [
            ("User ID", "user_id", "userId"),
            ("customerName", "customer_name", "customerName"),
            ("CustomerID", "customer_id", "customerId"),
            ("HTTPServer", "http_server", "httpServer"),
            ("order_details", "order_details", "orderDetails"),
            ("  Order   Details ", "order_details", "orderDetails"),
            ("ID", "id", "id"),
            ("col1Name", "col1_name", "col1Name"),
            ("Prénom Client", "prénom_client", "prénomClient"),
            ("", "", ""),
        ],
    )
    def test_helpers(self, name, snake, camel):
        assert snake_case_name(name) == snake
        assert camel_case_name(name) == camel

    def test_csv_importer_applies_snake_and_camel_case(self):
        names = ["User ID", "First Name", "first_name"]
        snake = CsvSchemaImporter(csv_schema(case={"standardizeNames": "snake_case"}))
        assert snake.standardize_column_names(names) == ["user_id", "first_name", "first_name"]
        camel = CsvSchemaImporter(
            csv_schema(case={"standardizeNames": "camelCase", "dedupeNames": "suffix"})
        )
        assert camel.standardize_column_names(names) == ["userId", "firstName", "firstName_1"]

    def test_csv_importer_postgres_unchanged(self):
        importer = CsvSchemaImporter(csv_schema(case={"standardizeNames": "postgres"}))
        assert importer.standardize_column_names(["User ID"]) == ["user_id"]

    def test_fwf_column_name_processor(self):
        names = ["User ID", "firstName"]
        assert ColumnNameProcessor.standardize_column_names(names, "snake_case") == [
            "user_id",
            "first_name",
        ]
        assert ColumnNameProcessor.standardize_column_names(names, "camelCase") == [
            "userId",
            "firstName",
        ]

    def test_fwf_importer_column_names_use_the_style(self):
        schema = fwf_schema(
            [
                {"name": "Customer ID", "start": 1, "length": 5},
                {"name": "firstName", "start": 6, "length": 5},
            ],
            case={"standardizeNames": "snake_case"},
        )
        assert FwfSchemaImporter(schema).get_column_names() == ["customer_id", "first_name"]


# --------------------------------------------------------------------------------------------
# A1.7 FWF schema package
# --------------------------------------------------------------------------------------------


class TestFwfConditional:
    def test_variant_manager_positions_and_names(self):
        importer = FwfSchemaImporter(fwf_conditional_schema())
        assert importer.get_field_positions_for_flag_value("H") == [(0, 1), (1, 6), (6, 9)]
        assert importer.get_column_names_for_flag_value("H") == ["record_type", "amt", "hdr"]
        assert importer.get_field_positions_for_flag_value("D") == [(0, 1), (1, 6)]
        assert importer.get_column_names_for_flag_value("D") == ["record_type", "amt"]
        # names and positions stay aligned although each variant repeats the flag column
        for flag in ("H", "D"):
            assert len(importer.get_field_positions_for_flag_value(flag)) == len(
                importer.get_column_names_for_flag_value(flag)
            )

    def test_unknown_flag_value(self):
        importer = FwfSchemaImporter(fwf_conditional_schema())
        assert importer.get_field_positions_for_flag_value("Z") == []
        assert importer.get_column_names_for_flag_value("Z") == []

    def test_column_names_for_flag_value_applies_case_options(self):
        schema = fwf_conditional_schema()
        schema["x-fwf"]["case"] = {"standardizeNames": "camelCase"}
        importer = FwfSchemaImporter(schema)
        assert importer.get_column_names_for_flag_value("H") == ["recordType", "amt", "hdr"]

    def test_variant_manager_directly(self):
        schema = fwf_conditional_schema()["x-fwf"]["conditionalSchemas"]
        manager = VariantManager(schema["schemas"], schema["flagColumn"])
        assert manager.get_field_positions_for_flag_value("D") == [(0, 1), (1, 6)]
        assert manager.get_column_names_for_flag_value("D", "postgres", "suffix") == [
            "record_type",
            "amt",
        ]

    def test_flag_value_extraction_at_row_end(self):
        flag = {"start": 1, "length": 1}
        assert PositionCalculator.extract_flag_value_from_row("A", flag) == "A"
        assert PositionCalculator.extract_flag_value_from_row("", flag) is None
        last = {"start": 5, "length": 2}
        assert PositionCalculator.extract_flag_value_from_row("1234AB", last) == "AB"
        assert PositionCalculator.extract_flag_value_from_row("1234A", last) is None

    def test_record_mapping_for_flag_only_row(self):
        importer = FwfSchemaImporter(fwf_conditional_schema())
        mapping = importer.get_record_mapping_for_row("H")
        assert mapping is not None and mapping["flagValue"] == "H"
        assert importer.get_record_mapping_for_row("Dxxxxx")["flagValue"] == "D"
        assert importer.get_record_mapping_for_row("Z") is None


class TestFwfDuplicatesAndFlagColumn:
    def test_duplicate_field_names_are_rejected(self):
        fields = [
            {"name": "a", "start": 1, "length": 2},
            {"name": "a", "start": 3, "length": 2},
        ]
        assert FieldValidator.validate_traditional_fields(fields) == ["Field 1 duplicate name 'a'"]
        with pytest.raises(FwfError, match="duplicate name 'a'"):
            FwfSchemaImporter(fwf_schema(fields))

    def test_duplicates_allowed_when_dedupe_is_configured(self):
        fields = [
            {"name": "a", "start": 1, "length": 2},
            {"name": "a", "start": 3, "length": 2},
        ]
        importer = FwfSchemaImporter(fwf_schema(fields, case={"dedupeNames": "suffix"}))
        assert importer.get_column_names() == ["a", "a_1"]
        with pytest.raises(FwfError, match="duplicate name"):
            FwfSchemaImporter(fwf_schema(fields, case={"dedupeNames": "error"}))

    def test_duplicate_names_inside_a_variant(self):
        schema = fwf_conditional_schema()
        schema["x-fwf"]["conditionalSchemas"]["schemas"][0]["fields"].append(
            {"name": "amt", "start": 20, "length": 2, "parquetType": "int32"}
        )
        with pytest.raises(FwfError, match="variant 0 field 3 duplicate name 'amt'"):
            FwfSchemaImporter(schema)

    def test_same_name_in_different_variants_is_fine(self):
        FwfSchemaImporter(fwf_conditional_schema(amt_h="int32", amt_d="int64"))

    def test_variant_field_overlapping_the_flag_column_is_rejected(self):
        schema = fwf_conditional_schema()
        schema["x-fwf"]["conditionalSchemas"]["schemas"][1]["fields"].append(
            {"name": "bad", "start": 1, "length": 3, "parquetType": "string"}
        )
        with pytest.raises(FwfError, match="variant 1 field 2 overlaps with previous"):
            FwfSchemaImporter(schema)

    def test_variant_repeating_the_flag_column_elsewhere_is_rejected(self):
        schema = fwf_conditional_schema()
        schema["x-fwf"]["conditionalSchemas"]["schemas"][1]["fields"][0]["start"] = 9
        with pytest.raises(FwfError, match="redefines flag column 'record_type'"):
            FwfSchemaImporter(schema)

    def test_variants_that_do_not_repeat_the_flag_column_are_fine(self):
        schema = fwf_conditional_schema()
        for variant in schema["x-fwf"]["conditionalSchemas"]["schemas"]:
            del variant["fields"][0]
        importer = FwfSchemaImporter(schema)
        assert importer.get_column_names_for_flag_value("D") == ["record_type", "amt"]

    def test_overlap_check_between_regular_fields_is_unchanged(self):
        fields = [
            {"name": "a", "start": 1, "length": 5},
            {"name": "b", "start": 5, "length": 5},
            {"name": "c", "start": 10, "length": 5},
        ]
        assert FieldValidator.validate_traditional_fields(fields) == [
            "Field 1 overlaps with previous field positions"
        ]


class TestFwfUnifiedSchema:
    def test_variant_types_are_promoted(self):
        importer = FwfSchemaImporter(fwf_conditional_schema(amt_h="int32", amt_d="double"))
        unified = importer.get_unified_parquet_schema()
        assert unified["amt"] == "double"  # used to be the first variant's int32
        assert unified["record_type"] == "string"
        assert unified["hdr"] == "string"

    def test_order_of_variants_does_not_matter(self):
        schema = fwf_conditional_schema(amt_h="double", amt_d="int32")
        assert FwfSchemaImporter(schema).get_unified_parquet_schema()["amt"] == "double"

    def test_decimals_widen(self):
        schema = fwf_conditional_schema(amt_h="decimal128(5,2)", amt_d="decimal128(10,5)")
        assert FwfSchemaImporter(schema).get_unified_parquet_schema()["amt"] == "decimal128(10,5)"

    def test_incompatible_variant_types_are_rejected_at_load(self):
        with pytest.raises(FwfError, match="incompatible Parquet types"):
            FwfSchemaImporter(fwf_conditional_schema(amt_h="int32", amt_d="string"))

    def test_unvalidated_incompatible_types_raise_on_use(self):
        importer = FwfSchemaImporter(
            fwf_conditional_schema(amt_h="int32", amt_d="string"), validate=False
        )
        with pytest.raises(ConditionalSchemaError, match="incompatible Parquet types"):
            importer.get_unified_parquet_schema()

    def test_get_all_possible_fields_does_not_mutate_the_schema(self):
        schema = fwf_conditional_schema()
        snapshot = copy.deepcopy(schema)
        importer = FwfSchemaImporter(schema)

        first = importer.get_all_possible_fields()
        second = importer.get_all_possible_fields()

        assert schema == snapshot
        assert first == second
        assert first["amt"]["_appears_in_variants"] == ["H", "D"]
        assert second["record_type"]["_appears_in_variants"] == ["H", "D"]
        assert "_appears_in_variants" not in schema["x-fwf"]["conditionalSchemas"]["flagColumn"]

    def test_get_all_possible_fields_traditional_returns_copies(self):
        fields = [{"name": "a", "start": 1, "length": 1}]
        result = FieldMapper.get_all_possible_fields(False, fields, None, [])
        result["a"]["extra"] = True
        assert "extra" not in fields[0]

    def test_unified_schema_is_stable_across_calls(self):
        importer = FwfSchemaImporter(fwf_conditional_schema())
        assert importer.get_unified_parquet_schema() == importer.get_unified_parquet_schema()

    def test_compatibility_check_understands_timezones_and_decimals(self):
        assert ParquetTypeValidator.are_types_compatible(["timestamp[s]", "timestamp[ns]"])
        assert ParquetTypeValidator.are_types_compatible(
            ["timestamp[s, tz=UTC]", "timestamp[ns, tz=UTC]"]
        )
        assert not ParquetTypeValidator.are_types_compatible(
            ["timestamp[s, tz=UTC]", "timestamp[ns]"]
        )
        assert ParquetTypeValidator.are_types_compatible(["decimal128(5,2)", "decimal128(10,5)"])
        assert not ParquetTypeValidator.are_types_compatible(["decimal128(5,2)", "int32"])


# --------------------------------------------------------------------------------------------
# B / C. Packaging and CI configuration
# --------------------------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / (".git" + "hub") / "workflows"


def _requirement_names(lines):
    names = set()
    for line in lines:
        line = line.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        names.add(re.split(r"[\[<>=!~; ]", line, maxsplit=1)[0].lower().replace("_", "-"))
    return names


@pytest.fixture(scope="module")
def pyproject():
    tomllib = pytest.importorskip("tomllib")
    with open(REPO_ROOT / "pyproject.toml", "rb") as handle:
        return tomllib.load(handle)


class TestPackagingMetadata:
    def test_runtime_dependencies(self, pyproject):
        runtime = _requirement_names(pyproject["project"]["dependencies"])
        assert runtime == {
            "pyarrow",
            "jsonschema",
            "boto3",
            "botocore",
            "python-dateutil",
            "pytz",
            "chardet",
        }
        pyarrow_spec = next(
            dep for dep in pyproject["project"]["dependencies"] if dep.startswith("pyarrow")
        )
        assert ">=15" in pyarrow_spec and "<" not in pyarrow_spec  # <18 has no 3.13 wheels

    def test_pandas_is_not_a_runtime_dependency(self, pyproject):
        assert "pandas" not in _requirement_names(pyproject["project"]["dependencies"])
        extras = pyproject["project"]["optional-dependencies"]
        assert _requirement_names(extras["pandas"]) == {"pandas"}
        assert _requirement_names(extras["polars"]) == {"polars"}

    def test_extras(self, pyproject):
        extras = pyproject["project"]["optional-dependencies"]
        assert set(extras) == {"excel", "sql", "pandas", "polars", "clipboard", "all", "dev"}
        assert _requirement_names(extras["excel"]) == {"openpyxl", "xlrd"}
        assert _requirement_names(extras["sql"]) == {"pyodbc"}
        assert _requirement_names(extras["clipboard"]) == {"pyperclip"}
        assert _requirement_names(extras["all"]) == (
            _requirement_names(extras["excel"])
            | _requirement_names(extras["sql"])
            | _requirement_names(extras["pandas"])
            | _requirement_names(extras["polars"])
            | _requirement_names(extras["clipboard"])
        )
        assert "pyspark" not in " ".join(sum(extras.values(), []))  # nothing imports it
        assert {"pytest", "pytest-cov", "black", "isort", "flake8", "mypy", "pre-commit"} <= (
            _requirement_names(extras["dev"])
        )

    def test_python_313_classifier(self, pyproject):
        assert "Programming Language :: Python :: 3.13" in pyproject["project"]["classifiers"]

    def test_requirements_txt_is_runtime_only_and_matches_pyproject(self, pyproject):
        lines = (REPO_ROOT / "requirements.txt").read_text().splitlines()
        assert _requirement_names(lines) == _requirement_names(
            pyproject["project"]["dependencies"]
        )

    def test_requirements_dev_txt(self):
        text = (REPO_ROOT / "requirements-dev.txt").read_text()
        names = _requirement_names(text.splitlines())
        assert {"pytest", "black", "isort", "flake8", "build", "twine", "python-dotenv"} <= names
        assert "-r requirements.txt" in text
        runtime = _requirement_names((REPO_ROOT / "requirements.txt").read_text().splitlines())
        assert not runtime & {"pytest", "pytest-cov", "ruff", "mattstash", "twine", "build"}

    def test_every_third_party_import_in_src_is_declared(self, pyproject):
        """pytz and chardet used to be imported without being declared anywhere."""
        distribution = {"dateutil": "python-dateutil"}
        declared = _requirement_names(pyproject["project"]["dependencies"])
        for extra in pyproject["project"]["optional-dependencies"].values():
            declared |= _requirement_names(extra)
        if not hasattr(sys, "stdlib_module_names"):
            pytest.skip("needs sys.stdlib_module_names (Python 3.10+)")
        stdlib = set(sys.stdlib_module_names)

        imported = set()
        for path in (REPO_ROOT / "src").rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Import):
                    imported.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    imported.add(node.module.split(".")[0])
        third_party = {name for name in imported if name not in stdlib and name != "forklift"}
        third_party -= {"main", "src"}  # the demo script src/main.py, not a dependency
        missing = {name for name in third_party if distribution.get(name, name) not in declared}
        assert not missing, f"imported but not declared in pyproject.toml: {sorted(missing)}"

    def test_changelog_exists_with_unreleased_section(self):
        text = (REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        assert "## [Unreleased]" in text and "Keep a Changelog" in text

    def test_sdist_includes(self, pyproject):
        include = pyproject["tool"]["hatch"]["build"]["targets"]["sdist"]["include"]
        assert "/schema-standards" in include
        for entry in include:
            assert (REPO_ROOT / entry.lstrip("/")).exists(), f"sdist include {entry} is missing"

    def test_manifest_in_references_only_existing_things(self):
        text = (REPO_ROOT / "MANIFEST.in").read_text()
        for line in text.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0] in ("include", "recursive-include"):
                assert (
                    REPO_ROOT / parts[1]
                ).exists(), f"MANIFEST.in references missing {parts[1]}"
        assert "docker" not in text

    def test_pre_commit_config_lives_at_the_repo_root(self):
        assert (REPO_ROOT / ".pre-commit-config.yaml").is_file()
        assert not (WORKFLOW_DIR.parent / ".pre-commit-config.yaml").exists()

    def test_scripts_have_no_hardcoded_home_paths(self):
        for path in (REPO_ROOT / "scripts").rglob("*"):
            if path.is_file() and path.suffix in {".sh", ".py", ".md"}:
                assert "/Users/" not in path.read_text(encoding="utf-8"), path.name


@pytest.fixture(scope="module")
def workflows():
    yaml = pytest.importorskip("yaml")
    result = {}
    for path in WORKFLOW_DIR.glob("*.y*ml"):
        result[path.name] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return result


def _steps(workflow):
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            yield step


class TestWorkflows:
    def test_every_workflow_defaults_to_read_only(self, workflows):
        assert workflows
        for name, workflow in workflows.items():
            assert workflow["permissions"] == {"contents": "read"}, name

    def test_only_expected_jobs_have_their_own_permissions(self, workflows):
        grants = {}
        for name, workflow in workflows.items():
            for job_name, job in workflow["jobs"].items():
                if "permissions" in job:
                    grants[(name, job_name)] = job["permissions"]
        assert grants == {
            ("fast-test.yml", "autoformat"): {"contents": "write"},
            ("publish.yaml", "publish-pypi"): {"id-token": "write"},
        }

    def test_autoformat_only_on_pushes_to_non_default_branches(self, workflows):
        workflow = workflows["fast-test.yml"]
        condition = workflow["jobs"]["autoformat"]["if"]
        assert "github.event_name == 'push'" in condition
        assert "!= github.event.repository.default_branch" in condition
        assert "pull_request" not in condition
        # pull requests and the default branch only check
        check = workflow["jobs"]["format-check"]
        assert "pull_request" in check["if"]
        script = "\n".join(step.get("run", "") for step in check["steps"])
        assert "black --check" in script and "isort --check-only" in script
        assert "auto-commit" not in str(check)

    def test_actions_use_current_majors(self, workflows):
        for name, workflow in workflows.items():
            for old in ("setup-python@v4", "cache@v3", "codecov-action@v3"):
                assert old not in str(workflow), f"{name} still uses {old}"
        uses = {step["uses"] for wf in workflows.values() for step in _steps(wf) if "uses" in step}
        assert {"actions/setup-python@v5", "actions/cache@v4", "codecov/codecov-action@v4"} <= uses

    def test_canonical_test_workflow(self, workflows):
        test = workflows["test.yml"]
        assert test["jobs"]["test"]["strategy"]["matrix"]["python-version"] == ["3.12", "3.13"]
        text = str(test)
        assert "--cov" in text and "codecov" in text and "coverage-report" in text

    def test_publish_gate(self, workflows):
        publish = workflows["publish.yaml"]
        assert set(publish["jobs"]["publish-pypi"]["needs"]) == {"build", "test"}
        script = "\n".join(step.get("run", "") for step in _steps(publish))
        assert "pytest tests/unit-tests" in script
        assert "tomllib" in script and "TAG_NAME" in script  # tag vs pyproject.toml version
        # the tag is passed through the environment, never interpolated into the script
        assert "github.event.release.tag_name" in str(publish)
        assert "${{ github.event.release.tag_name }}" not in script

    def test_publish_version_check_script(self, workflows):
        tomllib = pytest.importorskip("tomllib")
        steps = workflows["publish.yaml"]["jobs"]["build"]["steps"]
        step = next(s for s in steps if s.get("name", "").startswith("Verify release tag"))
        script = step["run"].split("<<'PY'\n", 1)[1].rsplit("PY", 1)[0]
        version = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["version"]

        def run(tag):
            return subprocess.run(
                [sys.executable, "-c", script],
                cwd=REPO_ROOT,
                env={**os.environ, "TAG_NAME": tag},
                capture_output=True,
                text=True,
            )

        assert run(f"v{version}").returncode == 0
        assert run(version).returncode == 0
        assert run("v999.0.0").returncode != 0
        assert run(f"release-{version}").returncode != 0

    def test_dependabot_covers_actions_and_pip(self):
        yaml = pytest.importorskip("yaml")
        config = yaml.safe_load((WORKFLOW_DIR.parent / "dependabot.yml").read_text())
        ecosystems = {u["package-ecosystem"]: u["schedule"]["interval"] for u in config["updates"]}
        assert ecosystems == {"github-actions": "weekly", "pip": "weekly"}
