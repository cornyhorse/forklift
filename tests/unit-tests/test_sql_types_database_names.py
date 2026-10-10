"""SQL Server and Oracle type names, and the ODBC type codes that settle ambiguous ones.

The service tests in tests/integration-tests/services/test_sql_types.py read the same types
from real servers.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.inputs.sql.types import SqlTypeConverter, base_type_name

convert = SqlTypeConverter().sql_type_to_pyarrow


@pytest.mark.parametrize(
    "name, expected",
    [
        ("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"),
        ("datetime2(7)", "DATETIME2"),
        ("int identity", "INT"),
        ("  interval day(2) to second(6) ", "INTERVAL DAY TO SECOND"),
        ("varchar", "VARCHAR"),
    ],
)
def test_type_names_are_compared_without_parameters(name, expected):
    assert base_type_name(name) == expected


class TestOracleNames:
    def test_number_with_precision_is_a_decimal(self):
        assert convert("NUMBER", 10, 2) == pa.decimal128(10, 2)
        assert convert("NUMBER", 38, 0) == pa.decimal128(38, 0)

    def test_number_without_precision_is_a_double_as_the_driver_returns_it(self):
        assert convert("NUMBER") == pa.float64()

    @pytest.mark.parametrize(
        "name, expected",
        [
            ("BINARY_DOUBLE", pa.float64()),
            ("BINARY_FLOAT", pa.float32()),
            ("TIMESTAMP(3)", pa.timestamp("us")),
            ("TIMESTAMP(6) WITH TIME ZONE", pa.timestamp("us", tz="UTC")),
            ("TIMESTAMP(6) WITH LOCAL TIME ZONE", pa.timestamp("us", tz="UTC")),
            ("RAW", pa.binary()),
            ("LONG RAW", pa.binary()),
            ("CLOB", pa.string()),
            ("NVARCHAR2", pa.string()),
        ],
    )
    def test_oracle_types(self, name, expected):
        assert convert(name) == expected

    def test_date_returned_as_a_timestamp_keeps_its_time(self):
        # Oracle's DATE holds a time of day; the catalog says so with SQL_TYPE_TIMESTAMP (93)
        assert convert("DATE", odbc_type=93) == pa.timestamp("us")
        assert convert("DATE", odbc_type=91) == pa.date32()
        assert convert("DATE") == pa.date32()


class TestSqlServerNames:
    @pytest.mark.parametrize(
        "name, expected",
        [
            ("datetimeoffset", pa.timestamp("us", tz="UTC")),
            ("image", pa.binary()),
            ("rowversion", pa.binary()),
            ("money", pa.decimal128(19, 4)),
            ("uniqueidentifier", pa.string()),
            ("datetime2", pa.timestamp("us")),
        ],
    )
    def test_sql_server_types(self, name, expected):
        assert convert(name) == expected

    def test_timestamp_returned_as_binary_is_a_row_version(self):
        # SQL Server's "timestamp" is rowversion; SQLColumns reports SQL_BINARY (-2)
        assert convert("timestamp", 8, odbc_type=-2) == pa.binary()
        assert convert("timestamp") == pa.timestamp("us")
