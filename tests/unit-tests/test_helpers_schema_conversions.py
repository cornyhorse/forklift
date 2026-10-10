"""Schema helper functions: structure checks, Arrow <-> Parquet type strings, quantile
validation and JSON-safe conversion of number-like objects."""

import pyarrow as pa
import pytest

from forklift.schema.utils.helpers import (
    SchemaValidationError,
    get_parquet_type_string,
    parquet_type_string_to_arrow,
    to_json_safe,
    validate_quantiles,
    validate_schema_structure,
)


class _Celsius(pa.ExtensionType):
    """Minimal extension type over float64 storage."""

    def __init__(self):
        super().__init__(pa.float64(), "forklift.test.celsius")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


class TestValidateSchemaStructure:
    def test_complete_schema_is_valid(self):
        schema = {"$schema": "https://json-schema.org/draft/2020-12/schema"}
        schema.update({"type": "object", "properties": {}})

        assert validate_schema_structure(schema) is True

    @pytest.mark.parametrize("missing", ["$schema", "type", "properties"])
    def test_missing_top_level_field_is_named(self, missing):
        schema = {"$schema": "s", "type": "object", "properties": {}}
        del schema[missing]

        with pytest.raises(SchemaValidationError) as excinfo:
            validate_schema_structure(schema)
        assert str(excinfo.value) == f"Missing required field: {missing}"

    def test_first_missing_field_is_reported(self):
        with pytest.raises(SchemaValidationError, match=r"Missing required field: \$schema"):
            validate_schema_structure({"properties": {}})

    def test_properties_must_be_a_dictionary(self):
        schema = {"$schema": "s", "type": "object", "properties": ["id"]}

        with pytest.raises(SchemaValidationError, match="Properties must be a dictionary"):
            validate_schema_structure(schema)


class TestParquetTypeStrings:
    def test_extension_type_is_described_by_its_storage_type(self):
        assert get_parquet_type_string(_Celsius()) == "double"

    def test_struct_type_string_parses_to_an_empty_struct(self):
        assert parquet_type_string_to_arrow("struct") == pa.struct([])


class TestValidateQuantiles:
    @pytest.mark.parametrize("quantiles", ["0.5", b"0.5", 0.5], ids=["str", "bytes", "number"])
    def test_non_list_input_is_rejected(self, quantiles):
        with pytest.raises(ValueError, match="quantiles must be a list of numbers"):
            validate_quantiles(quantiles)


class _NumberLike:
    """Stand-in for a numpy scalar: ``item()`` returns the Python value."""

    def __init__(self, value):
        self._value = value

    def item(self):
        return self._value


class _BrokenNumberLike:
    def item(self):
        raise RuntimeError("cannot convert")

    def __str__(self):
        return "broken-number"


class _Opaque:
    item = "not callable"

    def __str__(self):
        return "opaque"


class TestToJsonSafeNumberLikes:
    def test_item_value_is_converted_recursively(self):
        assert to_json_safe(_NumberLike(7)) == 7
        assert to_json_safe(_NumberLike(float("nan"))) is None

    def test_failing_item_falls_back_to_the_text_form(self):
        assert to_json_safe(_BrokenNumberLike()) == "broken-number"

    def test_object_without_callable_item_becomes_text(self):
        assert to_json_safe(_Opaque()) == "opaque"
        assert to_json_safe([_Opaque(), (1, 2)]) == ["opaque", [1, 2]]
