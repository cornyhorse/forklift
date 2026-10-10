"""``forklift.outputs.sql.write_table`` against real PostgreSQL, MySQL, SQL Server and Oracle.

Every test runs on the four databases (the ``database`` fixture), as a login with exactly the
privileges the test grants, so the tests show what a load needs and what it guarantees:

* every mapped Arrow type is created as the documented column type and reads back unchanged;
* a load is all or nothing: a value that does not fit, a cancellation or a killed process
  leaves the table as it was, and a retry with the same ``job_id`` drops what an interrupted
  attempt left;
* a login that may not create the staging table, insert, delete or update is refused with an
  error that names the table, the mode and the privilege (and suggests ``staging="none"``
  where that helps), never cell values or passwords.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import subprocess
import sys
import textwrap
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from service_helpers import Database
from sql_target_helpers import column_types, read_back, staging_writer

from forklift.outputs.sql import (
    TableWriteCancelled,
    TableWriteError,
    staging_table_name,
    write_table,
)

pytestmark = pytest.mark.services

SRC = Path(__file__).resolve().parents[3] / "src"
SECRET = "tok-do-not-leak-91c2"
PLUS_TWO = dt.timezone(dt.timedelta(hours=2))
DIALECTS = {"postgres": "postgresql", "mysql": "mysql", "mssql": "sqlserver", "oracle": "oracle"}
UNBOUNDED_TYPES = {
    "postgres": ("TEXT", "BYTEA"),
    "mysql": ("LONGTEXT", "LONGBLOB"),
    "mssql": ("NVARCHAR(MAX)", "VARBINARY(MAX)"),
    "oracle": ("CLOB", "BLOB"),
}

# (column, kind, Arrow type) of the round-trip table, and its rows
COLUMNS = [
    ("id", "integer", pa.int32()),
    ("flag", "boolean", pa.bool_()),
    ("i8", "integer", pa.int8()),
    ("i16", "integer", pa.int16()),
    ("i32", "integer", pa.int32()),
    ("i64", "integer", pa.int64()),
    ("u8", "integer", pa.uint8()),
    ("u16", "integer", pa.uint16()),
    ("u32", "integer", pa.uint32()),
    ("u64", "integer", pa.uint64()),
    ("f32", "float", pa.float32()),
    ("f64", "float", pa.float64()),
    ("amount", "decimal", pa.decimal128(10, 2)),
    ("name", "string", pa.string()),
    ("payload", "binary", pa.binary()),
    ("fixed", "binary", pa.binary(4)),
    ("day", "date", pa.date32()),
    ("at", "timestamp", pa.timestamp("us")),
    ("at_utc", "timestamp_tz", pa.timestamp("us", tz="+02:00")),
    ("clock", "time", pa.time64("us")),
]
ROWS = [
    (
        1,
        True,
        -128,
        -32768,
        -(2**31),
        -(2**63),
        255,
        65535,
        2**32 - 1,
        2**64 - 1,
        1.5,
        1 / 3,
        Decimal("-12345678.90"),
        "héllo € 漢字",
        b"\x00\xff" * 3000,
        b"abcd",
        dt.date(1999, 12, 31),
        dt.datetime(2020, 2, 29, 23, 59, 59, 999999),
        dt.datetime(2021, 6, 30, 14, 0, 0, 1, tzinfo=PLUS_TWO),
        dt.time(23, 59, 59, 999999),
    ),
    (
        2,
        False,
        127,
        32767,
        2**31 - 1,
        2**63 - 1,
        0,
        0,
        0,
        0,
        -0.25,
        1e300,
        Decimal("99999999.99"),
        "x" * 4000,
        b"\x01",
        b"\x00\x00\x00\x00",
        dt.date(2024, 1, 1),
        dt.datetime(1970, 1, 1),
        dt.datetime(2000, 1, 1, 2, 0, tzinfo=PLUS_TWO),
        dt.time(0, 0),
    ),
    (3,) + (None,) * (len(COLUMNS) - 1),
]
EXPECTED_TYPES = {
    "postgres": [
        "integer", "boolean", "smallint", "smallint", "integer", "bigint", "smallint",
        "integer", "bigint", "numeric(20,0)", "real", "double precision", "numeric(10,2)",
        "text", "bytea", "bytea", "date", "timestamp(6) without time zone",
        "timestamp(6) with time zone", "time(6) without time zone",
    ],
    "mysql": [
        "int", "tinyint(1)", "tinyint", "smallint", "int", "bigint", "tinyint unsigned",
        "smallint unsigned", "int unsigned", "bigint unsigned", "float", "double",
        "decimal(10,2)", "longtext", "longblob", "binary(4)", "date", "datetime(6)",
        "datetime(6)", "time(6)",
    ],
    "mssql": [
        "int", "bit", "smallint", "smallint", "int", "bigint", "tinyint", "int", "bigint",
        "decimal(20,0)", "real", "float", "decimal(10,2)", "nvarchar(max)", "varbinary(max)",
        "binary(4)", "date", "datetime2", "datetimeoffset", "time",
    ],
    "oracle": [
        "number(10)", "number(1)", "number(3)", "number(5)", "number(10)", "number(19)",
        "number(3)", "number(5)", "number(10)", "number(20)", "binary_float", "binary_double",
        "number(10,2)", "varchar2(4000)", "blob", "raw(4)", "date", "timestamp(6)",
        "timestamp(6) with time zone", "interval day(0) to second(6)",
    ],
}  # fmt: skip


def _source(rows=ROWS) -> pa.Table:
    return pa.table(
        {
            name: pa.array([row[i] for row in rows], arrow_type)
            for i, (name, _, arrow_type) in enumerate(COLUMNS)
        }
    )


def _expected(rows=ROWS):
    """What reading the rows back gives: time zone-aware values in UTC, times in microseconds."""
    expected = []
    for row in rows:
        values = []
        for (_, kind, _), value in zip(COLUMNS, row):
            if value is None:
                values.append(None)
            elif kind == "boolean":
                values.append(int(value))
            elif kind == "timestamp_tz":
                values.append(value.astimezone(dt.timezone.utc).replace(tzinfo=None))
            elif kind == "time":
                values.append(
                    ((value.hour * 60 + value.minute) * 60 + value.second) * 10**6
                    + value.microsecond
                )
            else:
                values.append(value)
        expected.append(tuple(values))
    return expected


def _kinds():
    return [(name, kind) for name, kind, _ in COLUMNS]


def _events(ids, label="new") -> pa.Table:
    return pa.table({"id": pa.array(ids, pa.int64()), "label": [f"{label}-{i}" for i in ids]})


def _owner(database: Database, *tables: str):
    """A login that may create tables in the namespace and write the admin's ``tables``.

    PostgreSQL's schema owner owns only the tables it creates; elsewhere the namespace's owner
    may already write every table in it.
    """
    login = database.owner_login()
    if database.kind == "postgres":
        for table in tables:
            database.grant_write(login, table)
    return login


def _write(database: Database, login, source, table="events", **kwargs):
    kwargs.setdefault("schema_name", database.namespace)
    return write_table(source, database.login_connection_string(login), table, **kwargs)


def _seed_events(database: Database, ids=(1, 2, 3)) -> None:
    """The admin's table ``events`` (id primary key) with rows labelled ``old``."""
    database.admin(
        f"CREATE TABLE {database.table('events')} (id INTEGER PRIMARY KEY, label VARCHAR(50))",
        *[f"INSERT INTO {database.table('events')} VALUES ({i}, 'old-{i}')" for i in ids],
    )


def _events_rows(database: Database):
    return [(int(i), label) for i, label in database.rows("events")]


def _assert_only_tables(database: Database, *names: str) -> None:
    """No staging table (or anything else) was left behind."""
    assert [n.casefold() for n in database.table_names()] == sorted(n.casefold() for n in names)


class TestTypes:
    def test_every_mapped_type_is_created_as_documented_and_reads_back(self, database, tmp_path):
        login = database.owner_login()
        path = tmp_path / "data.parquet"
        pq.write_table(_source(), path)

        result = _write(database, login, path, "typed", mode="create", key_columns=["id"])

        assert (result.rows_written, result.created, result.mode) == (3, True, "create")
        assert result.database == DIALECTS[database.kind]
        types = column_types(database, "typed")
        assert [types[name] for name, _, _ in COLUMNS] == EXPECTED_TYPES[database.kind]
        assert read_back(database, "typed", _kinds(), "id") == _expected()

    def test_appending_to_the_created_table_keeps_every_value(self, database):
        login = database.owner_login()
        _write(database, login, _source(), "typed", mode="create")

        later = [(row[0] + 10,) + row[1:] for row in ROWS]
        result = _write(database, login, _source(later), "typed", mode="append")

        assert result.rows_written == 3 and not result.created
        assert read_back(database, "typed", _kinds(), "id") == _expected(ROWS + later)

    @pytest.mark.parametrize("staging", ["table", "none"])
    def test_long_text_and_binary_reach_unbounded_columns(self, database, staging):
        text_type, binary_type = UNBOUNDED_TYPES[database.kind]
        database.admin(
            f"CREATE TABLE {database.table('docs')} "
            f"(id INTEGER, body {text_type}, data {binary_type})"
        )
        login = _owner(database, "docs")
        body = "é€漢x" * 25000  # 100,000 characters, 225,000 bytes in UTF-8
        data = bytes(range(256)) * 400
        source = pa.table(
            {"id": [1, 2, 3], "body": [body, "short", None], "data": [data, b"\x00", None]}
        )

        _write(database, login, source, "docs", staging=staging)

        rows = [
            (int(i), text, None if raw is None else bytes(raw))
            for i, text, raw in database.rows("docs")
        ]
        assert rows == [(1, body, data), (2, "short", b"\x00"), (3, None, None)]

    def test_odd_names_are_quoted_and_found_again(self, database):
        login = database.owner_login()
        source = pa.table({"Order ID": [1, 2], "select": ["a", "b"], "Total $": [1.5, 2.5]})

        _write(database, login, source, "Odd Table", mode="create")
        result = _write(database, login, source, "odd table", mode="append")

        assert result.rows_written == 2
        assert result.table == f"{database.catalog_namespace}.Odd Table"
        # Oracle gives the plain lower-case name its convention (upper case), keeps the others
        select = "SELECT" if database.kind == "oracle" else "select"
        assert database.column_names("Odd Table") == ["Order ID", select, "Total $"]
        assert database.row_count("Odd Table") == 4

    def test_plain_lower_case_names_follow_the_database_convention(self, database):
        login = database.owner_login()

        result = _write(database, login, _events([1]), "events", mode="create")

        # Oracle stores unquoted names in upper case, so forklift creates EVENTS there
        expected = "EVENTS" if database.kind == "oracle" else "events"
        assert result.table == f"{database.catalog_namespace}.{expected}"
        assert database.table_names() == [expected]


class TestModes:
    @pytest.mark.parametrize("staging", ["table", "none"])
    def test_append_adds_rows(self, database, staging):
        _seed_events(database)
        login = _owner(database, "events")

        result = _write(database, login, _events([4, 5]), mode="append", staging=staging)

        assert result.rows_written == 2
        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")] + [
            (4, "new-4"),
            (5, "new-5"),
        ]
        _assert_only_tables(database, "events")

    @pytest.mark.parametrize("staging", ["table", "none"])
    def test_replace_makes_the_rows_the_sources(self, database, staging):
        _seed_events(database)
        login = _owner(database, "events")

        _write(database, login, _events([7, 8]), mode="replace", staging=staging)

        assert _events_rows(database) == [(7, "new-7"), (8, "new-8")]
        _assert_only_tables(database, "events")

    @pytest.mark.parametrize("staging", ["table", "none"])
    def test_upsert_updates_matching_keys_and_inserts_the_others(self, database, staging):
        _seed_events(database)
        login = _owner(database, "events")

        result = _write(
            database, login, _events([2, 3, 4]), mode="upsert", key_columns=["id"], staging=staging
        )

        assert result.rows_written == 3
        assert _events_rows(database) == [(1, "old-1"), (2, "new-2"), (3, "new-3"), (4, "new-4")]
        _assert_only_tables(database, "events")

    def test_upsert_creates_a_missing_table_with_the_key_as_primary_key(self, database):
        login = database.owner_login()

        _write(database, login, _events([1, 2]), mode="upsert", key_columns=["id"])
        _write(database, login, _events([2, 3], "again"), mode="upsert", key_columns=["id"])

        assert _events_rows(database) == [(1, "new-1"), (2, "again-2"), (3, "again-3")]
        with pytest.raises(TableWriteError) as raised:  # the primary key refuses a duplicate
            _write(database, login, _events([3]), mode="append")
        assert raised.value.sqlstate in ("23000", "23505")

    def test_upsert_without_staging_lets_the_last_row_of_a_key_win(self, database):
        _seed_events(database)
        login = _owner(database, "events")
        source = pa.table({"id": [5, 5, 1], "label": ["first", "last", "one"]})

        _write(database, login, source, mode="upsert", key_columns=["id"], staging="none")

        assert _events_rows(database) == [(1, "one"), (2, "old-2"), (3, "old-3"), (5, "last")]

    def test_upsert_through_staging_refuses_repeated_keys(self, database):
        _seed_events(database)
        login = _owner(database, "events")
        source = pa.table({"id": [5, 5, 6, 6, 7], "label": ["a", "b", "c", "d", "e"]})

        with pytest.raises(TableWriteError, match="2 key value"):
            _write(database, login, source, mode="upsert", key_columns=["id"])

        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]
        _assert_only_tables(database, "events")

    def test_create_refuses_an_existing_table(self, database):
        _seed_events(database)
        login = database.owner_login()

        with pytest.raises(TableWriteError, match="already exists"):
            _write(database, login, _events([9]), mode="create")

        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]


class TestColumnChecks:
    def test_missing_unfit_and_required_columns_are_reported_together(self, database):
        database.admin(
            f"CREATE TABLE {database.table('people')} "
            "(id INTEGER, name VARCHAR(20), born DATE NOT NULL)"
        )
        login = _owner(database, "people")
        source = pa.table({"id": [1], "name": [3.5], "nickname": ["x"]})

        with pytest.raises(TableWriteError) as raised:
            _write(database, login, source, "people", mode="append")

        message = str(raised.value).casefold()
        assert "no column for 'nickname'" in message
        assert "'name' (float values" in message
        assert "born" in message and "need a value in every row" in message
        assert database.row_count("people") == 0


class TestAllOrNothing:
    @pytest.mark.parametrize("staging", ["table", "none"])
    def test_a_value_that_does_not_fit_leaves_the_table_unchanged(self, database, staging):
        database.admin(
            f"CREATE TABLE {database.table('codes')} (id INTEGER, code VARCHAR(5))",
            f"INSERT INTO {database.table('codes')} VALUES (1, 'keep')",
        )
        login = _owner(database, "codes")
        codes = ["ok"] * 25
        codes[14] = SECRET  # row 15: longer than VARCHAR(5)
        source = pa.table({"id": list(range(2, 27)), "code": codes})

        with pytest.raises(TableWriteError) as raised:
            _write(database, login, source, "codes", batch_size=10, staging=staging)

        message = str(raised.value)
        assert SECRET not in message
        assert raised.value.sqlstate is not None
        if staging == "none":  # the direct insert fails on its batch
            assert "rows 11 to 20 of the source" in message
        else:  # the staging column holds any text; moving it into the table fails
            assert "insert the staged rows" in message
        assert [(int(i), c) for i, c in database.rows("codes")] == [(1, "keep")]
        _assert_only_tables(database, "codes")

    @pytest.mark.parametrize("mode", ["append", "replace", "upsert"])
    @pytest.mark.parametrize("staging", ["table", "none"])
    def test_cancel_mid_load_leaves_the_table_unchanged(self, database, mode, staging):
        _seed_events(database)
        login = _owner(database, "events")
        events = []

        with pytest.raises(TableWriteCancelled) as raised:
            _write(
                database,
                login,
                _events(list(range(10, 40))),
                mode=mode,
                key_columns=["id"],
                staging=staging,
                batch_size=10,
                progress=events.append,
                cancel=lambda: len(events) >= 2,
            )

        assert raised.value.error_code == "CANCELLED"
        assert "20 row(s)" in str(raised.value)
        assert events == [{"rows_written": 10}, {"rows_written": 20}]
        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]
        _assert_only_tables(database, "events")

    def test_a_retry_drops_the_staging_table_a_killed_attempt_left(self, database):
        login = database.owner_login()
        _write(database, login, _events([1]), mode="create")
        connection_string = database.login_connection_string(login)
        job_id = "job-7f3a"

        # The first attempt dies after its first batch, without any cleanup
        child = textwrap.dedent("""
            import os, sys
            import pyarrow as pa
            from forklift.outputs.sql import write_table
            source = pa.table({"id": pa.array(range(10, 40), pa.int64()),
                               "label": [f"new-{i}" for i in range(10, 40)]})
            def progress(event):
                os._exit(17)
            write_table(source, os.environ["TARGET"], "events", schema_name=sys.argv[1],
                        batch_size=10, job_id=sys.argv[2], progress=progress)
            """)
        environment = dict(os.environ, TARGET=connection_string, PYTHONPATH=str(SRC))
        killed = subprocess.run(
            [sys.executable, "-c", child, database.namespace, job_id],
            env=environment,
            timeout=300,
        )
        assert killed.returncode == 17
        table = database.catalog_name("events")
        staging = staging_table_name(job_id, database.catalog_namespace, table)
        assert database.table_exists(staging)  # the killed attempt left its staged batch
        assert _events_rows(database) == [(1, "new-1")]

        result = _write(
            database, login, _events(list(range(10, 40))), batch_size=10, job_id=job_id
        )

        assert result.rows_written == 30
        assert database.row_count("events") == 31  # the leftover batch was not published
        _assert_only_tables(database, "events")

    def test_a_killed_direct_load_is_rolled_back_by_the_database(self, database):
        _seed_events(database)
        login = _owner(database, "events")
        child = textwrap.dedent("""
            import os, sys
            import pyarrow as pa
            from forklift.outputs.sql import write_table
            source = pa.table({"id": pa.array(range(10, 40), pa.int64()),
                               "label": ["new"] * 30})
            seen = []
            def progress(event):
                seen.append(event)
                if len(seen) == 2:
                    os._exit(17)
            write_table(source, os.environ["TARGET"], "events", schema_name=sys.argv[1],
                        batch_size=10, staging="none", progress=progress)
            """)
        environment = dict(
            os.environ, TARGET=database.login_connection_string(login), PYTHONPATH=str(SRC)
        )

        killed = subprocess.run(
            [sys.executable, "-c", child, database.namespace], env=environment, timeout=300
        )

        assert killed.returncode == 17
        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]


class TestRestrictedLogins:
    def test_without_create_the_staging_table_is_refused_with_a_hint(self, database):
        _seed_events(database)
        login = database.create_user()
        database.grant(login, "events", "SELECT", "INSERT")

        with pytest.raises(TableWriteError) as raised:
            _write(database, login, _events([4]), mode="append")

        error = raised.value
        assert error.error_code == "PERMISSION_DENIED"
        assert error.mode == "append" and error.table.casefold().endswith(".events")
        assert "create the staging table" in str(error)
        assert error.privilege and "CREATE" in error.privilege
        assert 'staging="none"' in str(error)
        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]

        # The hint works: the same login appends without a staging table
        result = _write(database, login, _events([4]), mode="append", staging="none")
        assert result.rows_written == 1
        assert database.row_count("events") == 4

    def test_without_insert_the_load_is_refused(self, database):
        _seed_events(database)
        login = database.create_user()
        database.grant(login, "events", "SELECT")

        with pytest.raises(TableWriteError) as raised:
            _write(database, login, _events([4]), mode="append", staging="none")

        assert raised.value.error_code == "PERMISSION_DENIED"
        assert raised.value.privilege.startswith("INSERT on table")
        assert database.row_count("events") == 3

    def test_replace_without_delete_is_refused(self, database):
        _seed_events(database)
        login = database.create_user()
        database.grant(login, "events", "SELECT", "INSERT", "UPDATE")

        with pytest.raises(TableWriteError) as raised:
            _write(database, login, _events([4]), mode="replace", staging="none")

        assert raised.value.privilege.startswith("DELETE on table")
        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]

    def test_upsert_without_update_is_refused(self, database):
        _seed_events(database)
        login = database.create_user()
        database.grant(login, "events", "SELECT", "INSERT")

        with pytest.raises(TableWriteError) as raised:
            _write(
                database, login, _events([3, 4]), mode="upsert", key_columns=["id"], staging="none"
            )

        assert "UPDATE" in raised.value.privilege
        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]

    def test_upsert_and_replace_without_staging_need_only_table_privileges(self, database):
        _seed_events(database)
        login = database.create_user()
        database.grant(login, "events", "SELECT", "INSERT", "UPDATE", "DELETE")

        _write(database, login, _events([3, 4]), mode="upsert", key_columns=["id"], staging="none")
        assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "new-3"), (4, "new-4")]
        _write(database, login, _events([9]), mode="replace", staging="none")
        assert _events_rows(database) == [(9, "new-9")]

    @pytest.mark.parametrize(
        "mode, privilege", [("replace", "DELETE"), ("upsert", "UPDATE"), ("append", None)]
    )
    def test_a_staging_login_is_refused_only_when_publishing_needs_more(
        self, database, mode, privilege
    ):
        _seed_events(database)
        login = staging_writer(database, "events")
        source = _events([4, 5])

        if privilege is None:
            _write(database, login, source, mode=mode)
            assert database.row_count("events") == 5
        else:
            with pytest.raises(TableWriteError) as raised:
                _write(database, login, source, mode=mode, key_columns=["id"])
            assert raised.value.error_code == "PERMISSION_DENIED"
            assert privilege in raised.value.privilege
            assert _events_rows(database) == [(1, "old-1"), (2, "old-2"), (3, "old-3")]
        _assert_only_tables(database, "events")

    def test_a_wrong_password_is_never_echoed(self, database, caplog):
        login = database.create_user()
        connection_string = database.login_connection_string(login).replace(login.password, SECRET)

        with caplog.at_level(logging.DEBUG), pytest.raises(TableWriteError) as raised:
            write_table(_events([1]), connection_string, "events", schema_name=database.namespace)

        assert SECRET not in str(raised.value)
        assert SECRET not in caplog.text
        assert "Pwd=***" in str(raised.value)
        assert raised.value.action == "connect"
