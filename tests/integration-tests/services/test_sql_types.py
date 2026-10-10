"""How SQL Server and Oracle column types arrive in Parquet, read from real servers.

Each test creates a table with one column per type, a row of values and a row of NULLs, imports
it as a login that may only read it, and checks the Arrow type and the value of every column.
The types are those whose names or values the ODBC drivers report in ways forklift has to
translate: SQL Server's ``money``, ``datetime2``, ``uniqueidentifier``, ``datetimeoffset`` (which
pyodbc cannot read by itself) and ``timestamp`` (a row version, not a time); Oracle's ``NUMBER``
(with and without precision), ``DATE`` (which holds a time of day), time-zone timestamps,
``CLOB``, ``RAW`` and ``BOOLEAN`` (which pyodbc misreads by itself).
"""

from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from service_helpers import Database, MsSql, Oracle, sql_schema_file

from forklift import import_sql

pytestmark = pytest.mark.services

UTC = datetime.timezone.utc


def _import_types(database: Database, tmp_path) -> pa.Table:
    login = database.create_user()
    database.grant_select(login, "types")
    results = import_sql(
        database.login_connection_string(login),
        tmp_path / "out",
        sql_schema_file(tmp_path, database.namespace, ["types"]),
    )
    assert results.errors == []
    assert results.total_rows == 2
    return pq.read_table(tmp_path / "out" / "types.parquet")


def _check(table: pa.Table, expected):
    """``expected``: column -> (Arrow type, value in the first row); the second row is NULL."""
    columns = {name.lower(): name for name in table.column_names}
    assert sorted(columns) == sorted(["id", *expected])
    for name, (arrow_type, value) in expected.items():
        column = table.column(columns[name])
        assert column.type == arrow_type, name
        assert column.to_pylist() == [value, None], name


class TestSqlServerTypes:
    def test_sql_server_types(self, mssql: MsSql, tmp_path):
        mssql.admin(
            f"CREATE TABLE {mssql.table('types')} (id int IDENTITY PRIMARY KEY, "
            "tiny tinyint, big bigint, flag bit, price money, small_price smallmoney, "
            "amount decimal(12, 4), ratio float, approx real, created datetime2, "
            "legacy datetime, short smalldatetime, day date, at time, stamped datetimeoffset, "
            "guid uniqueidentifier, name nvarchar(max), code varchar(10), payload varbinary(16), "
            "version rowversion)",
            f"INSERT INTO {mssql.table('types')} (tiny, big, flag, price, small_price, amount, "
            "ratio, approx, created, legacy, short, day, at, stamped, guid, name, code, payload) "
            "VALUES (255, 9223372036854775807, 1, 922337203685477.5807, 214748.3647, "
            "12345678.1234, 1.5, 2.5, '2024-01-02 03:04:05.1234567', '2024-01-02 03:04:05.123', "
            "'2024-01-02 03:04:00', '2024-01-02', '03:04:05.123456', "
            "'2024-01-02 03:04:05.123 +02:00', '6F9619FF-8B86-D011-B42D-00C04FC964FF', "
            "N'Zoë', 'abc', 0xDEADBEEF)",
            f"INSERT INTO {mssql.table('types')} DEFAULT VALUES",
        )

        table = _import_types(mssql, tmp_path)

        version = table.column("version")
        assert version.type == pa.binary()
        assert all(len(value) == 8 for value in version.to_pylist())  # a row version per row
        _check(
            table.drop_columns(["version"]),
            {
                "tiny": (pa.int16(), 255),  # unsigned on SQL Server: int8 would overflow
                "big": (pa.int64(), 9223372036854775807),
                "flag": (pa.bool_(), True),
                "price": (pa.decimal128(19, 4), decimal.Decimal("922337203685477.5807")),
                "small_price": (pa.decimal128(10, 4), decimal.Decimal("214748.3647")),
                "amount": (pa.decimal128(12, 4), decimal.Decimal("12345678.1234")),
                "ratio": (pa.float64(), 1.5),
                "approx": (pa.float32(), 2.5),
                # datetime2 has 100 ns steps; pyodbc returns microseconds
                "created": (pa.timestamp("us"), datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)),
                "legacy": (pa.timestamp("us"), datetime.datetime(2024, 1, 2, 3, 4, 5, 123000)),
                "short": (pa.timestamp("us"), datetime.datetime(2024, 1, 2, 3, 4)),
                "day": (pa.date32(), datetime.date(2024, 1, 2)),
                "at": (pa.time64("us"), datetime.time(3, 4, 5, 123456)),
                "stamped": (
                    pa.timestamp("us", tz="UTC"),
                    datetime.datetime(2024, 1, 2, 1, 4, 5, 123000, tzinfo=UTC),
                ),
                "guid": (pa.string(), "6F9619FF-8B86-D011-B42D-00C04FC964FF"),
                "name": (pa.string(), "Zoë"),
                "code": (pa.string(), "abc"),
                "payload": (pa.binary(), b"\xde\xad\xbe\xef"),
            },
        )


class TestOracleTypes:
    def test_oracle_types(self, oracle: Oracle, tmp_path):
        oracle.admin(
            f"CREATE TABLE {oracle.table('types')} (id INTEGER PRIMARY KEY, big INTEGER, "
            "plain NUMBER, price NUMBER(10, 2), small NUMBER(5), ratio FLOAT, "
            "dbl BINARY_DOUBLE, flt BINARY_FLOAT, day DATE, at TIMESTAMP, at3 TIMESTAMP(3), "
            "at_tz TIMESTAMP WITH TIME ZONE, at_ltz TIMESTAMP WITH LOCAL TIME ZONE, "
            "name VARCHAR2(20), nname NVARCHAR2(20), code CHAR(3), body CLOB, "
            "raw_data RAW(16), blob_data BLOB, flag BOOLEAN, flag_off BOOLEAN)",
            f"INSERT INTO {oracle.table('types')} VALUES (1, "
            "123456789012345678901234567890, 1.5, 12.34, 12345, 2.5, 3.5, 4.5, "
            "DATE '2024-01-02' + 3/24, TIMESTAMP '2024-01-02 03:04:05.123456', "
            "TIMESTAMP '2024-01-02 03:04:05.123', TIMESTAMP '2024-01-02 03:04:05 +02:00', "
            "TIMESTAMP '2024-01-02 03:04:05 +02:00', 'abc', 'Zoë', 'ab', 'clob text', "
            "HEXTORAW('00FF'), HEXTORAW('DEADBEEF'), TRUE, FALSE)",
            f"INSERT INTO {oracle.table('types')} (id) VALUES (2)",
        )

        table = _import_types(oracle, tmp_path)

        # Oracle reports unquoted names in upper case; forklift keeps the catalog's spelling
        assert table.column_names[:3] == ["ID", "BIG", "PLAIN"]
        _check(
            table,
            {
                # INTEGER is NUMBER(*,0): 38 digits, read exactly
                "big": (
                    pa.decimal128(38, 0),
                    decimal.Decimal("123456789012345678901234567890"),
                ),
                # NUMBER without precision is a floating-point number; the driver sends a double
                "plain": (pa.float64(), 1.5),
                "price": (pa.decimal128(10, 2), decimal.Decimal("12.34")),
                "small": (pa.decimal128(5, 0), decimal.Decimal("12345")),
                "ratio": (pa.float64(), 2.5),
                "dbl": (pa.float64(), 3.5),
                "flt": (pa.float32(), 4.5),
                # DATE holds a time of day
                "day": (pa.timestamp("us"), datetime.datetime(2024, 1, 2, 3, 0)),
                "at": (pa.timestamp("us"), datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)),
                "at3": (pa.timestamp("us"), datetime.datetime(2024, 1, 2, 3, 4, 5, 123000)),
                "at_tz": (
                    pa.timestamp("us", tz="UTC"),
                    datetime.datetime(2024, 1, 2, 1, 4, 5, tzinfo=UTC),
                ),
                "at_ltz": (
                    pa.timestamp("us", tz="UTC"),
                    datetime.datetime(2024, 1, 2, 1, 4, 5, tzinfo=UTC),
                ),
                "name": (pa.string(), "abc"),
                "nname": (pa.string(), "Zoë"),
                "code": (pa.string(), "ab "),  # CHAR is blank-padded
                "body": (pa.string(), "clob text"),
                "raw_data": (pa.binary(), b"\x00\xff"),
                "blob_data": (pa.binary(), b"\xde\xad\xbe\xef"),
                "flag": (pa.bool_(), True),
                "flag_off": (pa.bool_(), False),
            },
        )
        assert table.column("ID").to_pylist() == [decimal.Decimal(1), decimal.Decimal(2)]
