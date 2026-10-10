"""SqlTypeConverter: DB-API type codes that are not Python types, and schema importers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.inputs.sql import SqlTypeConverter


class TestPythonTypeCodes:
    @pytest.mark.parametrize("type_code", [None, "int", 42.0, object()])
    def test_type_code_that_is_not_a_class_maps_to_varchar(self, type_code):
        assert SqlTypeConverter.python_type_to_string(type_code) == "VARCHAR"

    def test_unrecognised_class_maps_to_varchar(self):
        assert SqlTypeConverter.python_type_to_string(str) == "VARCHAR"


class TestSchemaImporter:
    def test_schema_importer_does_not_change_the_sql_type_mapping(self):
        importer = type("Importer", (), {"parquet_type_mapping": {"INTEGER": "string"}})()
        converter = SqlTypeConverter(schema_importer=importer)
        assert converter.sql_type_to_pyarrow("INTEGER") == pa.int32()
        assert converter.sql_type_to_pyarrow("numeric", 12, 3) == pa.decimal128(12, 3)
