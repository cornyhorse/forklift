"""``forklift.outputs.sql.write_table`` against a fake pyodbc.

The fake driver answers the catalog queries of each dialect from a small description of one
schema, records every statement, and fails the statements a test names, so these tests can walk
every step (and every way a step can fail) on all four dialects. The SQL itself runs against
real servers in tests/integration-tests/services/test_sql_targets.py.
"""

from __future__ import annotations

import logging
import re
import sys
import types
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.outputs.sql import (
    TableWriteCancelled,
    TableWriteError,
    TableWriteResult,
    staging_table_name,
    write_table,
)
from forklift.outputs.sql.dialects import dialect_for

SECRET = "s3cr3t-cell-value"
PASSWORD = "pw-do-not-echo"
CONNECTION = f"Driver={{X}};Server=db;Uid=bob;Pwd={PASSWORD}"
DBMS = {
    "postgresql": "PostgreSQL",
    "mysql": "MySQL",
    "sqlserver": "Microsoft SQL Server",
    "oracle": "Oracle",
}
# Catalog rows of a table (id integer key, label text) as each dialect's columns query shows it
EVENTS = {
    "postgresql": [
        ("id", "integer", "int4", True, False, "", ""),
        ("label", "text", "text", False, False, "", ""),
    ],
    "mysql": [
        ("id", "int", "int", "NO", None, ""),
        ("label", "varchar", "varchar(50)", "YES", None, ""),
    ],
    "sqlserver": [("id", "int", 0, 0, 0, 0), ("label", "nvarchar", 1, 0, 0, 0)],
    "oracle": [
        ("ID", "NUMBER", 10, 0, "N", None, "NO", "NO"),
        ("LABEL", "VARCHAR2", None, None, "Y", None, "NO", "NO"),
    ],
}


class FakeError(Exception):
    """pyodbc.Error"""


def driver_error(state="42000", native=None):
    code = f" ({native})" if native is not None else ""
    return FakeError(state, f"[{state}] driver message quoting {SECRET}{code} (SQLExecDirectW)")


def undecodable(message):
    """What pyodbc raises when the driver's message is not valid UTF-16 (here a lone surrogate)."""
    try:
        (message.encode("utf-16-le") + b"\x00\xd8A\x00").decode("utf-16-le")
    except UnicodeDecodeError as cause:
        error = SystemError("<class 'pyodbc.Error'> returned a result with an exception set")
        error.__cause__ = cause
        return error


class FakeDatabase:
    """One schema of a database, as the fake driver shows it, and what was done to it."""

    def __init__(self, name, schema="sales", tables=None):
        self.dbms = DBMS[name]
        self.dialect = dialect_for(self.dbms)
        self.schemas = [schema]
        self.current_schema = schema
        self.tables = dict(tables or {})  # name -> catalog column rows
        self.unique = []  # (index, column) rows of the unique-keys query
        self.counts = {"nulls": 0, "duplicates": 0}
        self.failures = []  # [fragment, error, matches to let through first]
        self.calls = []  # ("execute" | "executemany", sql, params)
        self.committed = []  # how many calls had been made at each commit
        self.commits = self.rollbacks = 0
        self.encoding = None
        self.closed = False
        self.connect_error = None
        self.getinfo = self.dbms
        self.close_error = self.rollback_error = self.cursor_close_error = None
        self.commit_error = self.cursor_error = None  # (error, fragment executed before)

    def fail(self, fragment, error=None, after=0):
        """Fail every statement that contains ``fragment``, after letting ``after`` through."""
        self.failures.append([fragment, error or driver_error(), after])

    def check(self, sql):
        for rule in self.failures:
            fragment, error, after = rule
            if fragment in sql:
                if after:
                    rule[2] -= 1
                else:
                    raise error

    def hook(self, failure):
        """Raise a (error, fragment) failure once a statement with the fragment ran."""
        if failure is not None and self.executed(failure[1]):
            raise failure[0]

    def sql(self):
        return [sql for _, sql, _ in self.calls]

    def executed(self, fragment):
        return [call for call in self.calls if fragment in call[1]]


class FakeCursor:
    def __init__(self, database):
        self.database = database
        self.fast_executemany = False
        self.result = []
        self.wrote = False

    def execute(self, sql, *params):
        database, dialect = self.database, self.database.dialect
        if len(params) == 1 and isinstance(params[0], (list, tuple)):
            params = params[0]  # the values of a multi-row statement
        database.calls.append(("execute", sql, list(params)))
        self.wrote = self.wrote or sql.startswith("INSERT")
        database.check(sql)
        if sql == dialect.current_schema_sql:
            self.result = [(database.current_schema,)] if database.current_schema != "" else []
        elif sql == dialect.schema_sql:
            self.result = [(s,) for s in database.schemas if s.lower() == params[0].lower()]
        elif sql == dialect.tables_sql:
            self.result = [(t,) for t in database.tables if t.lower() == params[1].lower()]
        elif sql == dialect.columns_sql:
            self.result = list(database.tables[params[1]])
        elif dialect.unique_keys_sql and sql == dialect.unique_keys_sql:
            self.result = list(database.unique)
        elif sql.startswith("SELECT COUNT(*) FROM (SELECT"):
            self.result = [(database.counts["duplicates"],)]
        elif sql.startswith("SELECT COUNT(*) FROM"):
            self.result = [(database.counts["nulls"],)]
        else:
            self.result = []

    def executemany(self, sql, params):
        self.database.calls.append(("executemany", sql, list(params)))
        self.database.check(sql)

    def fetchall(self):
        return self.result

    def close(self):
        if self.database.cursor_close_error and self.wrote:
            raise self.database.cursor_close_error


class FakeConnection:
    def __init__(self, database):
        self.database = database

    def getinfo(self, code):
        assert code == 17
        if isinstance(self.database.getinfo, Exception):
            raise self.database.getinfo
        return self.database.getinfo

    def setencoding(self, encoding):
        self.database.encoding = encoding

    def cursor(self):
        self.database.hook(self.database.cursor_error)
        return FakeCursor(self.database)

    def commit(self):
        self.database.hook(self.database.commit_error)
        self.database.commits += 1
        self.database.committed.append(len(self.database.calls))

    def rollback(self):
        self.database.rollbacks += 1
        if self.database.rollback_error:
            raise self.database.rollback_error

    def close(self):
        self.database.closed = True
        if self.database.close_error:
            raise self.database.close_error


@pytest.fixture
def pyodbc(monkeypatch):
    module = types.ModuleType("pyodbc")
    module.Error = FakeError
    module.SQL_DBMS_NAME = 17
    module.pooling = True
    module.database = None

    def connect(connection_string, **kwargs):
        module.connect_args = (connection_string, kwargs)
        if module.database.connect_error:
            raise module.database.connect_error
        return FakeConnection(module.database)

    module.connect = connect
    monkeypatch.setitem(sys.modules, "pyodbc", module)
    return module


def events(ids=(1, 2, 3)):
    return pa.table({"id": pa.array(ids, pa.int64()), "label": [f"e{i}" for i in ids]})


def write(pyodbc, database, source=None, table="events", **kwargs):
    pyodbc.database = database
    kwargs.setdefault("schema_name", "sales")
    kwargs.setdefault("job_id", "job-1")
    return write_table(events() if source is None else source, CONNECTION, table, **kwargs)


def staging(database, table="events"):
    """The quoted staging table of job-1 (table spelled as the catalog or new_name spells it)."""
    dialect = database.dialect
    name = dialect.new_name(staging_table_name("job-1", "sales", dialect.new_name(table)))
    return dialect.qualified("sales", name)


ALL = list(DBMS)


def existing(name):
    table = "EVENTS" if name == "oracle" else "events"
    return FakeDatabase(name, tables={table: EVENTS[name]})


# ---------------------------------------------------------------------------- arguments


class TestArguments:
    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"mode": "merge"}, "mode must be one of create, append, replace, upsert"),
            ({"staging": "temp"}, "staging must be 'table' or 'none'"),
            ({"batch_size": 0}, "batch_size must be a positive integer"),
            ({"batch_size": True}, "batch_size must be a positive integer"),
            ({"batch_size": "10"}, "batch_size must be a positive integer"),
            ({"schema_name": ""}, "schema_name must be a non-empty string or None"),
            ({"job_id": ""}, "job_id must be a non-empty string or None"),
            ({"key_columns": "id"}, "not a string"),
            ({"key_columns": [""]}, "non-empty column names"),
            ({"key_columns": ["id", "id"]}, "names a column twice"),
            ({"mode": "upsert"}, "mode 'upsert' needs key_columns"),
        ],
    )
    def test_invalid_arguments_are_refused_before_connecting(self, pyodbc, kwargs, message):
        with pytest.raises(ValueError, match=message):
            write_table(events(), CONNECTION, "events", **kwargs)
        assert not hasattr(pyodbc, "connect_args")

    @pytest.mark.parametrize("table", ["", None, 5])
    def test_the_table_must_be_named(self, table):
        with pytest.raises(ValueError, match="table must be a non-empty string"):
            write_table(events(), CONNECTION, table)

    @pytest.mark.parametrize("name", ["progress", "cancel"])
    def test_callbacks_must_be_callable(self, name):
        with pytest.raises(TypeError, match=f"{name} must be callable, got int"):
            write_table(events(), CONNECTION, "events", **{name: 5})

    def test_the_source_must_be_parquet_or_arrow(self):
        with pytest.raises(TypeError, match="got dict"):
            write_table({"id": [1]}, CONNECTION, "events")

    def test_unwritable_columns_are_refused_before_connecting(self, pyodbc):
        with pytest.raises(TableWriteError, match="'tags'"):
            write_table(pa.table({"tags": [[1]]}), CONNECTION, "events")
        assert not hasattr(pyodbc, "connect_args")

    def test_pyodbc_is_needed(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyodbc", None)
        with pytest.raises(ImportError, match=r"pip install forklift-etl\[sql\]"):
            write_table(events(), CONNECTION, "events")


class TestSources:
    def test_a_parquet_file_is_read_in_batches(self, pyodbc, tmp_path):
        path = tmp_path / "data.parquet"
        pq.write_table(events(range(5)), path)
        database = existing("postgresql")

        result = write(pyodbc, database, str(path), batch_size=2, staging="none")

        assert result.rows_written == 5
        inserts = database.executed("INSERT INTO")
        assert [len(params) for _, _, params in inserts] == [4, 4, 2]  # two values per row

    def test_a_path_object_works_too(self, pyodbc, tmp_path):
        path = tmp_path / "data.parquet"
        pq.write_table(events(), path)
        assert write(pyodbc, existing("postgresql"), path, staging="none").rows_written == 3

    def test_a_record_batch_reader_is_cut_into_batches(self, pyodbc):
        reader = pa.RecordBatchReader.from_batches(events().schema, events(range(5)).to_batches())
        progress = []

        write(pyodbc, existing("postgresql"), reader, batch_size=2, progress=progress.append)

        assert progress == [{"rows_written": 2}, {"rows_written": 4}, {"rows_written": 5}]


# ---------------------------------------------------------------------------- connecting


class TestConnecting:
    def test_the_connection_is_made_without_autocommit_or_pooling(self, pyodbc, monkeypatch):
        monkeypatch.delenv("NLS_LANG", raising=False)

        write(pyodbc, existing("postgresql"), connect_timeout=7)

        assert pyodbc.connect_args == (CONNECTION, {"autocommit": False, "timeout": 7})
        assert pyodbc.pooling is False
        # Oracle's client needs it for anything but ASCII; other drivers ignore it
        import os

        assert os.environ["NLS_LANG"] == ".AL32UTF8"

    def test_a_configured_nls_lang_is_kept(self, pyodbc, monkeypatch):
        monkeypatch.setenv("NLS_LANG", "GERMAN_GERMANY.AL32UTF8")
        write(pyodbc, existing("oracle"))
        import os

        assert os.environ["NLS_LANG"] == "GERMAN_GERMANY.AL32UTF8"

    def test_a_refused_login_never_echoes_the_password(self, pyodbc, caplog):
        database = existing("mysql")
        database.connect_error = FakeError(
            "28000",
            f"[28000] Access denied for user 'bob' using {PASSWORD} (1045) (SQLDriverConnect)",
        )

        with caplog.at_level(logging.DEBUG), pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        error = raised.value
        assert PASSWORD not in str(error) and PASSWORD not in caplog.text
        assert "Pwd=***" in str(error)
        assert "SQLSTATE 28000 (invalid authorization (login refused)), driver error 1045" in str(
            error
        )
        assert (error.action, error.error_code, error.table) == (
            "connect",
            "PERMISSION_DENIED",
            "sales.events",
        )

    def test_an_unreachable_server_is_worth_a_retry(self, pyodbc):
        database = existing("postgresql")
        database.connect_error = FakeError("08001", "could not connect (101) (SQLDriverConnect)")

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert raised.value.retryable and raised.value.error_code == "TARGET_WRITE_FAILED"

    @pytest.mark.parametrize("dbms, shown", [("SQLite", "'SQLite'"), (5, "5")])
    def test_other_databases_are_refused(self, pyodbc, dbms, shown):
        database = existing("postgresql")
        database.getinfo = dbms

        with pytest.raises(TableWriteError, match=f"reports itself as {shown}"):
            write(pyodbc, database)

        assert database.closed

    def test_a_driver_that_cannot_say_which_database_it_is(self, pyodbc):
        database = existing("postgresql")
        database.getinfo = FakeError("HYC00", "optional feature not implemented")

        with pytest.raises(TableWriteError, match="reports itself as ''"):
            write(pyodbc, database)

    def test_mysql_parameters_are_sent_as_utf8(self, pyodbc):
        database = existing("mysql")
        write(pyodbc, database)
        assert database.encoding == "utf-8"

    def test_the_session_is_prepared_first(self, pyodbc):
        database = existing("postgresql")

        write(pyodbc, database)

        assert database.sql()[0] == "SET TIME ZONE 'UTC'"

    def test_a_session_that_cannot_be_prepared(self, pyodbc):
        database = existing("mysql")
        database.fail("sql_mode")

        with pytest.raises(TableWriteError, match="could not prepare the session") as raised:
            write(pyodbc, database)

        assert SECRET not in str(raised.value)

    def test_closing_a_broken_connection_only_logs(self, pyodbc, caplog):
        database = existing("postgresql")
        database.close_error = FakeError("08S01", "gone")

        with caplog.at_level(logging.WARNING):
            assert write(pyodbc, database).rows_written == 3

        assert "Could not close the database connection cleanly" in caplog.text


# ---------------------------------------------------------------------------- planning


class TestNames:
    def test_names_that_cannot_be_identifiers(self, pyodbc):
        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, existing("postgresql"), table="bad\x00name")
        assert raised.value.error_code == "SPEC_INVALID"
        assert raised.value.table == "sales.bad\x00name"

    @pytest.mark.parametrize("schema_name, column", [("x" * 64, "id"), ("sales", "c" * 64)])
    def test_names_longer_than_the_database_allows(self, pyodbc, schema_name, column):
        source = pa.table({column: [1]})
        with pytest.raises(TableWriteError, match="PostgreSQL allows at most 63 bytes"):
            write(pyodbc, existing("postgresql"), source, schema_name=schema_name)

    def test_the_login_default_schema_is_used_without_schema_name(self, pyodbc):
        database = existing("postgresql")

        result = write(pyodbc, database, schema_name=None)

        assert result.table == "sales.events"

    @pytest.mark.parametrize("default", ["", None])
    def test_a_login_without_a_default_schema(self, pyodbc, default):
        database = existing("mysql")
        database.current_schema = default

        with pytest.raises(TableWriteError, match="no default schema on MySQL; pass schema_name"):
            write(pyodbc, database, schema_name=None)

    def test_schemas_and_tables_are_found_ignoring_case(self, pyodbc):
        database = FakeDatabase("oracle", schema="SALES", tables={"EVENTS": EVENTS["oracle"]})

        result = write(pyodbc, database, schema_name="Sales", table="Events")

        assert result.table == "SALES.EVENTS" and not result.created

    def test_an_unknown_schema(self, pyodbc):
        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, existing("postgresql"), schema_name="nope")
        assert "schema 'nope' does not exist, or the login cannot see it" in str(raised.value)
        assert raised.value.error_code == "SPEC_INVALID"

    def test_an_ambiguous_schema(self, pyodbc):
        database = existing("postgresql")
        database.schemas = ["Sales", "SALES"]

        with pytest.raises(TableWriteError, match=r"matches several schemas \(SALES, Sales\)"):
            write(pyodbc, database, schema_name="sales")

    def test_an_ambiguous_table(self, pyodbc):
        database = FakeDatabase("postgresql", tables={"Events": [], "EVENTS": []})

        with pytest.raises(
            TableWriteError, match=r"several tables ignoring case \(EVENTS, Events\)"
        ):
            write(pyodbc, database)

    def test_an_exact_name_wins_over_names_that_differ_in_case(self, pyodbc):
        database = FakeDatabase(
            "postgresql", tables={"events": EVENTS["postgresql"], "Events": []}
        )
        assert write(pyodbc, database).table == "sales.events"


class TestPlanningAnExistingTable:
    def test_create_refuses_an_existing_table(self, pyodbc):
        with pytest.raises(TableWriteError, match="already exists"):
            write(pyodbc, existing("postgresql"), mode="create")

    def test_missing_unfit_and_required_columns(self, pyodbc):
        columns = {
            "postgresql": [
                ("id", "integer", "int4", True, False, "", ""),
                ("total", "integer", "int4", False, False, "", "s"),
                ("code", "integer", "int4", False, False, "", ""),
                ("born", "date", "date", True, False, "", ""),
                ("note", "text", "text", True, True, "", ""),
            ]
        }
        database = FakeDatabase("postgresql", tables={"events": columns["postgresql"]})
        source = pa.table({"id": [1], "total": [2], "code": ["x"], "extra": [1.5]})

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database, source)

        message = str(raised.value)
        assert "the table has no column for 'extra' (its columns are id, total, code, born, " in (
            message
        )
        assert "'total' (the database computes total)" in message
        assert "'code' (string values; code is integer)" in message
        assert "the table's column(s) born need a value in every row" in message
        assert "note" not in message.split("need a value")[0].split("column(s)")[-1]
        assert database.executed("CREATE") == []

    def test_columns_are_matched_ignoring_case(self, pyodbc):
        database = existing("oracle")

        write(pyodbc, database, staging="none")

        assert 'INSERT INTO "sales"."EVENTS" ("ID", "LABEL")' in database.executed("INSERT")[0][1]

    def test_warnings_name_columns_never_values(self, pyodbc):
        columns = [
            ("naive", "TIMESTAMP(6) WITH TIME ZONE", None, None, "Y", None, "NO", "NO"),
            ("aware", "TIMESTAMP(6)", None, None, "Y", None, "NO", "NO"),
            ("day", "DATE", None, None, "Y", None, "NO", "NO"),
            ("doc", "XMLTYPE", None, None, "Y", None, "NO", "NO"),
        ]
        database = FakeDatabase("oracle", tables={"T": columns})
        source = pa.table(
            {
                "naive": pa.array([0], pa.timestamp("us")),
                "aware": pa.array([0], pa.timestamp("us", tz="UTC")),
                "day": pa.array([0], pa.timestamp("us")),
                "doc": [SECRET],
            }
        )

        result = write(pyodbc, database, source, table="t", staging="none")

        assert result.warnings == [
            "Column 'naive' holds timestamps without a time zone; they are written to naive "
            "(TIMESTAMP(6) WITH TIME ZONE) as UTC",
            "Column 'aware' holds time zone-aware timestamps; aware (TIMESTAMP(6)) has no time "
            "zone, so they are written in UTC",
            "day is an Oracle DATE, which keeps whole seconds: the fractional seconds of column "
            "'day' are dropped",
            "doc has type XMLTYPE, which forklift does not check: the database converts the "
            "text of column 'doc'",
        ]

    def test_mysql_tables_get_utc_timestamps_for_time_zone_aware_columns(self, pyodbc):
        source = pa.table({"at": pa.array([0], pa.timestamp("us", tz="Europe/Paris"))})

        result = write(pyodbc, FakeDatabase("mysql"), source, table="t", mode="create")

        assert result.warnings == [
            "Column 'at' holds time zone-aware timestamps; MySQL has no such type, so they are "
            "written in UTC to a DATETIME(6) column"
        ]
        assert write(pyodbc, FakeDatabase("oracle"), source, table="t").warnings == []

    def test_an_oracle_date_column_takes_dates_without_a_warning(self, pyodbc):
        database = FakeDatabase(
            "oracle", tables={"T": [("D", "DATE", None, None, "Y", None, "NO", "NO")]}
        )
        source = pa.table({"d": pa.array([0], pa.date32())})
        assert write(pyodbc, database, source, table="t").warnings == []

    def test_a_decimal_the_database_cannot_hold(self, pyodbc):
        source = pa.table({"amount": pa.array([Decimal(1)], pa.decimal128(38, 2))})
        with pytest.raises(TableWriteError, match="column 'amount': decimal\\(70, 2\\) does not"):
            write(
                pyodbc,
                FakeDatabase("mysql"),
                source.cast(pa.schema([("amount", pa.decimal256(70, 2))])),
            )

    @pytest.mark.parametrize("name", ["postgresql", "mysql"])
    def test_upsert_without_staging_needs_a_unique_key(self, pyodbc, name):
        database = existing(name)
        database.unique = [(1, "id"), (1, "label"), (2, "label")]

        with pytest.raises(TableWriteError, match="exactly the key columns \\(id\\)"):
            write(pyodbc, database, mode="upsert", key_columns=["id"], staging="none")

        database.unique.append((3, "id"))
        assert write(pyodbc, database, mode="upsert", key_columns=["id"], staging="none")

    def test_reading_unique_indexes_can_fail(self, pyodbc):
        database = existing("postgresql")
        database.fail("pg_index")

        with pytest.raises(TableWriteError, match="could not read the table's unique indexes"):
            write(pyodbc, database, mode="upsert", key_columns=["id"], staging="none")

    @pytest.mark.parametrize("name", ["sqlserver", "oracle"])
    def test_merge_needs_no_unique_key(self, pyodbc, name):
        database = existing(name)
        write(pyodbc, database, mode="upsert", key_columns=["id"], staging="none")
        assert database.executed("MERGE")


# ---------------------------------------------------------------------------- staged writes


class TestStagedWrites:
    @pytest.mark.parametrize("name", ["postgresql", "sqlserver"])
    def test_a_new_table_is_created_in_the_publishing_transaction(self, pyodbc, name):
        database = FakeDatabase(name)

        result = write(pyodbc, database, mode="create", key_columns=["id"])

        assert (result.rows_written, result.created, result.database) == (3, True, name)
        dialect, stage = database.dialect, staging(database)
        target = dialect.qualified("sales", "events")
        sql = database.sql()
        create_staging = next(s for s in sql if s.startswith(f"CREATE TABLE {stage}"))
        assert "PRIMARY KEY" not in create_staging  # keys are checked, then the table gets one
        publish = sql[sql.index(next(s for s in sql if s.startswith(f"CREATE TABLE {target}"))) :]
        assert publish[0].endswith(f"PRIMARY KEY ({dialect.quote('id')}))")
        assert publish[1].startswith(f"INSERT INTO {target}") and stage in publish[1]
        assert publish[2] == f"DROP TABLE {stage}"
        assert any(s.startswith("SELECT COUNT(*) FROM (SELECT") for s in sql)

    @pytest.mark.parametrize("name", ["mysql", "oracle"])
    def test_a_new_table_is_the_renamed_staging_table(self, pyodbc, name):
        database = FakeDatabase(name)

        result = write(pyodbc, database, mode="create", key_columns=["id"])

        assert result.created
        sql = database.sql()
        assert any(s.startswith(f"ALTER TABLE {staging(database)} ADD PRIMARY KEY") for s in sql)
        assert sql[-1] == database.dialect.rename_table_sql(
            "sales",
            staging(database).split(".")[1].strip('`"'),
            "events".upper() if name == "oracle" else "events",
        )
        assert not database.executed("DROP TABLE")

    def test_a_new_table_without_keys_skips_the_key_checks(self, pyodbc):
        database = FakeDatabase("mysql")

        write(pyodbc, database, mode="append")

        assert not database.executed("COUNT(*)")
        assert not database.executed("PRIMARY KEY")

    @pytest.mark.parametrize("name", ALL)
    @pytest.mark.parametrize(
        "mode, fragments",
        [
            ("append", ["INSERT INTO"]),
            ("replace", ["DELETE FROM", "INSERT INTO"]),
        ],
    )
    def test_rows_are_moved_into_an_existing_table(self, pyodbc, name, mode, fragments):
        database = existing(name)
        target = database.dialect.qualified("sales", "EVENTS" if name == "oracle" else "events")

        result = write(pyodbc, database, mode=mode)

        assert not result.created
        sql, stage = database.sql(), staging(database)
        published = [
            i for i, s in enumerate(sql) if s.startswith(("DELETE", f"INSERT INTO {target}"))
        ]
        assert [sql[i].split(" ")[0] for i in published] == [f.split(" ")[0] for f in fragments]
        assert sql[published[-1]].endswith(f"FROM {stage}")  # moved from the staging table
        drop = sql.index(database.dialect.drop_table_sql(stage))
        # one commit publishes the rows, after the last of them and before the staging table goes
        assert [n for n in database.committed if published[0] < n <= drop] == [published[-1] + 1]

    @pytest.mark.parametrize("name", ALL)
    def test_upsert_merges_or_updates_then_inserts(self, pyodbc, name):
        database = existing(name)

        write(pyodbc, database, mode="upsert", key_columns=["id"])

        statements = [
            s for s in database.sql() if s.startswith(("MERGE", "UPDATE", "INSERT INTO"))
        ]
        if name in ("sqlserver", "oracle"):
            assert statements[-1].startswith("MERGE")
        else:
            assert statements[-2].startswith("UPDATE") and "NOT EXISTS" in statements[-1]

    def test_a_leftover_staging_table_is_dropped_first(self, pyodbc, caplog):
        database = existing("postgresql")
        database.tables[staging(database).split(".")[1].strip('"')] = []

        with caplog.at_level(logging.INFO):
            write(pyodbc, database)

        assert database.sql()[5] == f"DROP TABLE {staging(database)}"
        assert "Dropped the staging table an earlier attempt left" in caplog.text

    def test_a_leftover_that_cannot_be_dropped(self, pyodbc):
        database = existing("mysql")
        database.tables[staging(database).split(".")[1].strip("`")] = []
        database.fail("DROP TABLE", driver_error("42000", 1142))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert raised.value.privilege == "DROP on database sales"
        assert "that an earlier attempt left" in str(raised.value)

    def test_reading_the_catalog_can_fail(self, pyodbc):
        database = existing("postgresql")
        database.fail("pg_class")

        with pytest.raises(
            TableWriteError, match="could not read the database catalog \\(to find"
        ):
            write(pyodbc, database)

    def test_looking_for_a_leftover_staging_table_can_fail(self, pyodbc):
        database = existing("postgresql")
        database.fail("relkind", driver_error("08S01"), after=1)  # the table is found first

        with pytest.raises(TableWriteError, match="to find a leftover staging table") as raised:
            write(pyodbc, database)

        assert raised.value.retryable

    @pytest.mark.parametrize(
        "tables, hint",
        [
            ({"events": EVENTS["postgresql"]}, 'or pass staging="none" to write straight into'),
            ({}, "The table does not exist yet, so it has to be created either way."),
        ],
    )
    def test_a_login_that_may_not_create_the_staging_table(self, pyodbc, tables, hint):
        database = FakeDatabase("postgresql", tables=tables)
        database.fail("CREATE TABLE", driver_error("42501"))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        error = raised.value
        assert (
            error.error_code == "PERMISSION_DENIED" and error.privilege == "CREATE on schema sales"
        )
        assert "refused to create the staging table sales.forklift_stg_job_1_" in str(error)
        assert "SQLSTATE 42501 (insufficient privilege)" in str(error)
        assert hint in str(error)
        assert SECRET not in str(error)

    def test_a_failure_that_is_not_a_refusal_gets_no_hint(self, pyodbc):
        database = existing("postgresql")
        database.fail("CREATE TABLE", driver_error("53100"))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert "could not create the staging table" in str(raised.value)
        assert "staging=" not in str(raised.value).split("failed:")[1]
        assert raised.value.privilege is None

    @pytest.mark.parametrize(
        "counts, message",
        [
            ((2, 0), "2 row(s) of the source have no value in key column(s) id"),
            ((0, 3), "3 key value(s) occur more than once"),
        ],
    )
    def test_keys_must_be_unique_and_present(self, pyodbc, counts, message):
        database = existing("postgresql")
        database.counts = {"nulls": counts[0], "duplicates": counts[1]}

        with pytest.raises(TableWriteError, match=re.escape(message)):
            write(pyodbc, database, mode="upsert", key_columns=["id"])

        assert database.sql()[-1] == f"DROP TABLE {staging(database)}"
        assert not database.executed("MERGE") and not database.executed("UPDATE")

    def test_checking_the_keys_can_fail(self, pyodbc):
        database = existing("sqlserver")
        database.fail("COUNT(*)")
        with pytest.raises(TableWriteError, match="check the key columns of the staged rows"):
            write(pyodbc, database, mode="upsert", key_columns=["id"])

    @pytest.mark.parametrize(
        "fragment, action, privilege",
        [
            (
                "DELETE FROM",
                "delete the rows of table sales.events",
                "DELETE on table sales.events",
            ),
            (
                "INSERT INTO [sales].[events]",
                "insert the staged rows",
                "INSERT on table sales.events",
            ),
        ],
    )
    def test_a_refused_publish_leaves_the_table_and_no_staging_table(
        self, pyodbc, fragment, action, privilege
    ):
        database = existing("sqlserver")
        database.fail(fragment, driver_error("42000", 229))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database, mode="replace")

        assert action in str(raised.value) and raised.value.privilege == privilege
        assert "driver error 229 (permission denied on the object)" in str(raised.value)
        assert database.rollbacks == 1
        assert database.sql()[-1] == f"DROP TABLE {staging(database)}"

    def test_a_refused_upsert_names_the_privileges_it_needs(self, pyodbc):
        database = existing("oracle")
        database.fail("MERGE", driver_error("HY000", "1031"))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database, mode="upsert", key_columns=["id"])

        assert raised.value.privilege == "SELECT, INSERT and UPDATE on table sales.EVENTS"
        assert "driver error 1031 (insufficient privileges)" in str(raised.value)

    def test_a_refused_rename(self, pyodbc):
        database = FakeDatabase("mysql")
        database.fail("RENAME TABLE", driver_error("42000", 1142))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert raised.value.privilege == "ALTER, DROP, CREATE and INSERT on database sales"
        assert database.sql()[-1] == f"DROP TABLE {staging(database)}"

    def test_a_failed_publish_commit(self, pyodbc):
        database = existing("postgresql")
        database.commit_error = (driver_error("40001"), 'INSERT INTO "sales"."events"')

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert "could not commit the transaction that publishes the rows" in str(raised.value)
        assert raised.value.retryable and "trying again may succeed" in str(raised.value)

    def test_a_staging_table_that_cannot_be_dropped_after_publishing_is_a_warning(self, pyodbc):
        database = existing("oracle")
        database.fail("PURGE", driver_error("HY000", "1031"))

        result = write(pyodbc, database)

        assert result.rows_written == 3
        (warning,) = result.warnings
        assert warning.startswith(
            "The rows were published, but the staging table sales.FORKLIFT_STG_JOB_1_"
        )
        assert "driver error 1031" in warning and SECRET not in warning

    def test_a_staging_table_that_cannot_be_dropped_after_a_failure(self, pyodbc, caplog):
        database = existing("postgresql")
        database.fail('INSERT INTO "sales"."events"', driver_error("23505"))
        database.fail("DROP TABLE", driver_error("08S01"))

        with caplog.at_level(logging.WARNING), pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        message = str(raised.value)
        assert "SQLSTATE 23505 (unique violation (a key already exists))" in message
        assert (
            f"The table {staging(database)} that this write created could not be dropped"
            in message
        )
        assert "a retry with the same job_id drops a staging table" in message
        assert "Could not drop" in caplog.text

    def test_other_failures_are_cleaned_up_and_raised_as_they_are(self, pyodbc, caplog):
        database = existing("postgresql")
        database.fail("DROP TABLE", driver_error("08S01"))

        def progress(event):
            raise KeyboardInterrupt

        with caplog.at_level(logging.WARNING), pytest.raises(KeyboardInterrupt):
            write(pyodbc, database, progress=progress)

        assert database.rollbacks == 1 and database.executed("DROP TABLE")
        assert "Could not drop" in caplog.text

    def test_a_rollback_on_a_lost_connection_only_logs(self, pyodbc, caplog):
        database = existing("postgresql")
        database.fail("SELECT COUNT(*)", driver_error("08S01"))
        database.rollback_error = driver_error("08S01")

        with caplog.at_level(logging.WARNING), pytest.raises(TableWriteError):
            write(pyodbc, database, mode="upsert", key_columns=["id"])

        assert "Could not roll back the transaction" in caplog.text

    def test_a_driver_error_pyodbc_cannot_decode(self, pyodbc):
        database = existing("oracle")
        database.fail("INSERT INTO", SystemError("<class 'pyodbc.Error'> returned a result"))

        with pytest.raises(TableWriteError, match="no SQLSTATE or driver code reported"):
            write(pyodbc, database)

    def test_an_undecodable_message_still_tells_a_refusal_by_its_ora_number(self, pyodbc):
        database = existing("oracle")
        database.fail("INSERT INTO", undecodable(f"[Oracle][ODBC][Ora]ORA-01031: {SECRET}"))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert raised.value.native_code == 1031 and raised.value.sqlstate is None
        assert raised.value.error_code == "PERMISSION_DENIED"
        assert SECRET not in str(raised.value)


# ---------------------------------------------------------------------------- loading


class TestLoading:
    def test_rows_go_in_multi_row_statements(self, pyodbc):
        database = existing("postgresql")

        write(pyodbc, database, events(range(1201)), staging="none", batch_size=1201)

        many, rest = [c for c in database.calls if c[1].startswith("INSERT")]
        assert many[0] == "executemany" and len(many[2]) == 2 and len(many[2][0]) == 1000
        assert rest[0] == "execute" and len(rest[2]) == 402  # 201 rows of two values

    def test_wide_tables_put_fewer_rows_in_a_statement(self, pyodbc):
        database = FakeDatabase("postgresql")
        source = pa.table({f"c{i}": [1] * 300 for i in range(200)})

        write(pyodbc, database, source, table="wide", staging="none")

        (call,) = [c for c in database.calls if c[1].startswith("INSERT")]
        assert call[0] == "executemany" and [len(p) for p in call[2]] == [30000, 30000]

    def test_sql_server_binds_parameter_arrays(self, pyodbc):
        database = existing("sqlserver")

        write(pyodbc, database, events(range(5)), staging="none")

        (call,) = [c for c in database.calls if c[1].startswith("INSERT")]
        assert call[0] == "executemany" and call[1].count("?") == 2
        assert call[2] == [(i, f"e{i}") for i in range(5)]

    def test_empty_batches_write_nothing(self, pyodbc):
        database = existing("sqlserver")
        empty = pa.Table.from_batches([], events().schema)

        assert write(pyodbc, database, empty, staging="none").rows_written == 0
        assert not database.executed("INSERT")

    def test_a_warning_is_given_once(self, pyodbc):
        database = FakeDatabase("postgresql")
        source = pa.table({"at": pa.array([1, 1001, 2001], pa.timestamp("ns"))})

        result = write(pyodbc, database, source, table="t", batch_size=1)

        assert result.warnings == [
            "Column 'at' has values finer than microseconds; they were truncated to microseconds"
        ]

    def test_values_are_converted_for_the_driver(self, pyodbc):
        database = existing("oracle")

        write(pyodbc, database, staging="none")

        (call,) = [c for c in database.calls if c[1].startswith("INSERT")]
        assert call[2][:2] == [Decimal(1), "e1"]

    def test_a_failed_batch_names_its_rows_and_never_their_values(self, pyodbc):
        database = existing("mysql")
        database.fail("INSERT INTO `sales`.`events`", driver_error("22001", 1406))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database, events(range(1, 6)), batch_size=2, staging="none")

        message = str(raised.value)
        assert "could not write rows 1 to 2 of the source to table sales.events" in message
        assert "driver error 1406 (a value is too long for its column)" in message
        assert SECRET not in message and "e1" not in message
        assert raised.value.privilege is None
        assert database.rollbacks == 1

    def test_a_cursor_that_cannot_be_opened(self, pyodbc):
        database = existing("postgresql")
        # the load's cursor is the first one after the table's columns were read
        database.cursor_error = (driver_error("08003"), "pg_attribute")

        with pytest.raises(TableWriteError, match="could not open a cursor to write to table"):
            write(pyodbc, database, staging="none")

    def test_a_cursor_that_cannot_be_closed_after_the_load(self, pyodbc):
        database = existing("postgresql")
        database.fail("INSERT", driver_error("08S01"))
        database.cursor_close_error = driver_error("08003")

        with pytest.raises(TableWriteError, match="SQLSTATE 08S01"):
            write(pyodbc, database, staging="none")


class TestUndecodableDriverMessages:
    """pyodbc raises SystemError when it cannot decode the driver's message (Oracle's driver sends
    undecodable bytes now and then); every place that handles a driver error handles it too."""

    def test_a_refused_login(self, pyodbc):
        database = existing("oracle")
        database.connect_error = undecodable("[Oracle][ODBC][Ora]ORA-01017: invalid login")

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert (raised.value.action, raised.value.native_code) == ("connect", 1017)

    def test_a_driver_that_cannot_say_which_database_it_is(self, pyodbc):
        database = existing("postgresql")
        database.getinfo = undecodable("no such information")

        with pytest.raises(TableWriteError, match="reports itself as ''"):
            write(pyodbc, database)

    def test_a_staging_table_that_cannot_be_dropped_after_publishing(self, pyodbc):
        database = existing("oracle")
        database.fail("PURGE", undecodable(f"[Oracle][ODBC][Ora]ORA-01031: {SECRET}"))

        result = write(pyodbc, database)

        assert result.rows_written == 3
        (warning,) = result.warnings
        assert "driver error 1031" in warning and SECRET not in warning

    def test_cleaning_up_after_a_failure(self, pyodbc, caplog):
        database = existing("postgresql")
        database.fail('INSERT INTO "sales"."events"', driver_error("23505"))
        database.fail("DROP TABLE", undecodable("lost"))
        database.rollback_error = undecodable("lost")

        with caplog.at_level(logging.WARNING), pytest.raises(TableWriteError) as raised:
            write(pyodbc, database)

        assert "SQLSTATE 23505" in str(raised.value)  # the error that ended the write
        assert "that this write created could not be dropped" in str(raised.value)
        assert "Could not roll back the transaction" in caplog.text

    def test_a_cursor_that_cannot_be_closed_after_the_load(self, pyodbc):
        database = existing("postgresql")
        database.fail("INSERT", driver_error("08S01"))
        database.cursor_close_error = undecodable("lost")

        with pytest.raises(TableWriteError, match="SQLSTATE 08S01"):
            write(pyodbc, database, staging="none")

    def test_closing_the_connection(self, pyodbc, caplog):
        database = existing("postgresql")
        database.close_error = undecodable("lost")

        with caplog.at_level(logging.WARNING):
            assert write(pyodbc, database).rows_written == 3

        assert "Could not close the database connection cleanly" in caplog.text


class TestDirectWrites:
    @pytest.mark.parametrize("name", ["postgresql", "sqlserver"])
    def test_a_new_table_is_created_inside_the_transaction(self, pyodbc, name):
        database = FakeDatabase(name)

        result = write(pyodbc, database, mode="create", key_columns=["id"], staging="none")

        assert result.created and result.staging == "none"
        sql = [s for s in database.sql() if s.startswith(("CREATE", "INSERT"))]
        assert sql[0].startswith("CREATE TABLE") and "PRIMARY KEY" in sql[0]
        assert sql[1].startswith("INSERT")
        assert not database.executed("DROP")

    @pytest.mark.parametrize("name", ["mysql", "oracle"])
    def test_a_new_table_is_dropped_when_the_load_fails(self, pyodbc, name):
        database = FakeDatabase(name)
        database.fail("INSERT", driver_error("22003"))

        with pytest.raises(TableWriteError):
            write(pyodbc, database, mode="create", staging="none")

        table = database.dialect.qualified("sales", database.dialect.new_name("events"))
        assert database.sql()[-1] == database.dialect.drop_table_sql(table)

    def test_a_login_that_may_not_create_the_table(self, pyodbc):
        database = FakeDatabase("sqlserver")
        database.fail("CREATE TABLE", driver_error("42000", 262))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database, staging="none")

        assert raised.value.privilege == "CREATE TABLE in the database and ALTER on schema sales"
        assert "driver error 262 (permission denied in the database)" in str(raised.value)

    def test_replace_deletes_inside_the_transaction(self, pyodbc):
        database = existing("mysql")

        write(pyodbc, database, mode="replace", staging="none")

        sql = [s for s in database.sql() if s.startswith(("DELETE", "INSERT"))]
        assert sql == ["DELETE FROM `sales`.`events`", sql[1]] and sql[1].startswith("INSERT")

    def test_a_refused_delete(self, pyodbc):
        database = existing("postgresql")
        database.fail("DELETE", driver_error("42501"))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database, mode="replace", staging="none")

        assert raised.value.privilege == "DELETE on table sales.events"

    @pytest.mark.parametrize("name", ["postgresql", "oracle"])
    def test_upsert_keeps_the_last_row_of_a_key_in_a_batch(self, pyodbc, name):
        database = existing(name)
        database.unique = [(1, "id")]
        source = pa.table({"id": [5, 6, 5], "label": ["first", "six", "last"]})

        write(pyodbc, database, source, mode="upsert", key_columns=["id"], staging="none")

        (call,) = [c for c in database.calls if c[1].startswith(("INSERT", "MERGE"))]
        assert call[2][1::2] == ["six", "last"]

    def test_mysql_upserts_need_no_deduplication(self, pyodbc):
        database = existing("mysql")
        database.unique = [("PRIMARY", "id")]
        source = pa.table({"id": [5, 5], "label": ["first", "last"]})

        write(pyodbc, database, source, mode="upsert", key_columns=["id"], staging="none")

        (call,) = [c for c in database.calls if "ON DUPLICATE KEY UPDATE" in c[1]]
        assert call[2] == [5, "first", 5, "last"]

    def test_a_refused_upsert(self, pyodbc):
        database = existing("sqlserver")
        database.fail("MERGE", driver_error("42000", 229))

        with pytest.raises(TableWriteError) as raised:
            write(pyodbc, database, mode="upsert", key_columns=["id"], staging="none")

        assert raised.value.privilege == "SELECT, INSERT and UPDATE on table sales.events"

    def test_a_failed_commit_rolls_back(self, pyodbc):
        database = existing("postgresql")
        database.commit_error = (driver_error("40P01"), "INSERT")

        with pytest.raises(TableWriteError, match="could not commit the transaction"):
            write(pyodbc, database, staging="none")

        assert database.rollbacks == 1


class TestOracleLongValues:
    def _database(self, *columns):
        rows = [("ID", "NUMBER", 10, 0, "Y", None, "NO", "NO")] + list(columns)
        return FakeDatabase("oracle", tables={"DOCS": rows})

    def test_rows_with_long_lob_values_are_inserted_one_by_one(self, pyodbc):
        database = self._database(("BODY", "CLOB", None, None, "Y", None, "NO", "NO"))
        source = pa.table({"id": [1, 2, 3, 4], "body": ["short", "x" * 9000, None, "y" * 9000]})

        write(pyodbc, database, source, table="docs", staging="none", batch_size=2)

        calls = [c for c in database.calls if c[1].startswith("INSERT")]
        assert [(kind, "FROM dual" in sql) for kind, sql, _ in calls] == [
            ("execute", True),
            ("executemany", False),
            ("execute", True),
            ("executemany", False),
        ]
        assert "VALUES (CAST(? AS NUMBER(19)), ?)" in calls[1][1]
        assert calls[1][2] == [(Decimal(2), "x" * 9000)] and calls[3][2] == [
            (Decimal(4), "y" * 9000)
        ]

    def test_a_staging_table_takes_long_values_too(self, pyodbc):
        database = self._database(("BODY", "CLOB", None, None, "Y", None, "NO", "NO"))
        source = pa.table({"id": [1], "body": ["x" * 9000]})

        write(pyodbc, database, source, table="docs", mode="upsert", key_columns=["id"])

        assert database.executed("VALUES (CAST(? AS NUMBER(19)), ?)")
        assert database.executed("MERGE")

    def test_upsert_without_staging_cannot_take_long_values(self, pyodbc):
        database = self._database(("BODY", "CLOB", None, None, "Y", None, "NO", "NO"))
        source = pa.table({"id": [1], "body": ["x" * 9000]})

        with pytest.raises(TableWriteError, match="upsert with staging='table'"):
            write(
                pyodbc,
                database,
                source,
                table="docs",
                mode="upsert",
                key_columns=["id"],
                staging="none",
            )

    def test_long_values_and_times_cannot_share_a_row(self, pyodbc):
        database = self._database(
            ("BODY", "BLOB", None, None, "Y", None, "NO", "NO"),
            ("AT", "INTERVAL DAY(0) TO SECOND(6)", None, None, "Y", None, "NO", "NO"),
        )
        source = pa.table(
            {"id": [1], "body": [b"\x00" * 40000], "at": pa.array([0], pa.time64("us"))}
        )

        with pytest.raises(TableWriteError, match="cannot bind the INTERVAL"):
            write(pyodbc, database, source, table="docs", staging="none")


class TestCancelAndProgress:
    @pytest.mark.parametrize(
        "staging_mode, where", [("table", "the staging table"), ("none", "the open transaction")]
    )
    def test_cancel_between_batches(self, pyodbc, staging_mode, where):
        database = existing("postgresql")
        progress = []

        with pytest.raises(TableWriteCancelled) as raised:
            write(
                pyodbc,
                database,
                events(range(6)),
                batch_size=2,
                staging=staging_mode,
                progress=progress.append,
                cancel=lambda: len(progress) == 2,
            )

        error = raised.value
        assert progress == [{"rows_written": 2}, {"rows_written": 4}]
        assert f"cancelled after 4 row(s) were written to {where}" in str(error)
        assert error.error_code == "CANCELLED" and error.mode == "append"
        assert database.rollbacks == 1
        assert not database.executed('INSERT INTO "sales"."events" ("id", "label") SELECT')

    def test_cancel_after_the_last_batch_still_publishes_nothing(self, pyodbc):
        database = existing("mysql")
        calls = []

        def cancel():
            calls.append(1)
            return len(calls) == 2  # asked before the only batch, then before publishing

        with pytest.raises(TableWriteCancelled, match="after 3 row"):
            write(pyodbc, database, cancel=cancel)

        assert database.sql()[-1] == f"DROP TABLE {staging(database)}"

    def test_cancel_before_committing_a_direct_write(self, pyodbc):
        database = FakeDatabase("oracle")
        calls = []

        with pytest.raises(TableWriteCancelled):
            write(
                pyodbc,
                database,
                staging="none",
                cancel=lambda: bool(calls.append(1)) or len(calls) == 2,
            )

        assert database.sql()[-1].startswith("DROP TABLE")  # the committed CREATE is undone


def test_the_result_as_a_dict(pyodbc):
    result = write(pyodbc, existing("postgresql"), mode="replace")

    assert isinstance(result, TableWriteResult)
    assert result.to_dict() == {
        "rows_written": 3,
        "table": "sales.events",
        "mode": "replace",
        "warnings": [],
        "database": "postgresql",
        "created": False,
        "staging": "table",
    }


def test_a_random_job_id_names_the_staging_table_without_one(pyodbc):
    database = existing("postgresql")
    pyodbc.database = database

    write_table(events(), CONNECTION, "events", schema_name="sales")

    (create,) = [s for s in database.sql() if s.startswith("CREATE TABLE")]
    assert create.startswith('CREATE TABLE "sales"."forklift_stg_')
    assert "job_1" not in create


@pytest.mark.parametrize(
    "job_id, readable",
    [("job-1", "job_1"), ("7F3A-B2", "7f3a_b2"), ("---", "job"), ("x" * 40, "x" * 24)],
)
def test_staging_table_names(job_id, readable):
    name = staging_table_name(job_id, "s", "t")
    assert name.startswith(f"forklift_stg_{readable}_") and len(name) <= 50
    assert name == staging_table_name(job_id, "s", "t")
    assert name != staging_table_name(job_id, "s", "u")
