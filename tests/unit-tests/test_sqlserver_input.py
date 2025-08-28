import types
import pytest
from types import SimpleNamespace
from urllib.parse import unquote_plus

from forklift.inputs.db.sqlserver_input import SQLServerInput

# --- Helpers for engine/inspector fakes ---
class FakeConnection:
    def __init__(self, results_map, raise_errors=False):
        self.results_map = results_map
        self.raise_errors = raise_errors
        self.closed = False
        self.calls = []

    def execute(self, stmt, params=None):  # stmt can be sqlalchemy.sql.elements.TextClause
        if self.raise_errors:
            raise Exception("boom")
        sql_text = " ".join(str(stmt).split())  # normalize whitespace
        self.calls.append(sql_text)
        for key, rows in self.results_map.items():
            if key in sql_text:
                return rows
        return []

    def close(self):
        self.closed = True

class FakeEngine:
    def __init__(self, connection):
        self._connection = connection
        self.dialect = SimpleNamespace(name="mssql")

    def connect(self):
        return self._connection

class FakeInspector:
    def __init__(self):
        self.schemas = ["dbo"]
        self.tables = {"dbo": ["existing_table"]}
        self.views = {"dbo": ["existing_view"]}

    # SQLAlchemy inspector API subset
    def get_schema_names(self):
        return list(self.schemas)

    def get_table_names(self, schema=None):
        return list(self.tables.get(schema, []))

    def get_view_names(self, schema=None):
        return list(self.views.get(schema, []))

# --- Patch utilities ---
@pytest.fixture
def patch_sqlalchemy(monkeypatch):
    inspector = FakeInspector()

    # Map substrings in queries to result row tuples; keys are normalized substrings
    results_map = {
        "INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA": [("dbo", "info_schema_view")],
        "FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id WHERE s.name": [("dbo", "sys_schema_view")],
        "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA": [("dbo", "base_table_from_tables")],
        # final all views (no WHERE s.name)
        "FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id": [("dbo", "final_sys_view")],
    }
    connection = FakeConnection(results_map)
    engine = FakeEngine(connection)

    # Patch create_engine & inspect in base_sql_input module (where imported)
    import forklift.inputs.base_sql_input as base_mod
    monkeypatch.setattr(base_mod, "create_engine", lambda src: engine)
    monkeypatch.setattr(base_mod, "inspect", lambda eng: inspector)
    return SimpleNamespace(engine=engine, inspector=inspector, connection=connection)

@pytest.fixture
def patched_input(patch_sqlalchemy):
    # Use an mssql style URL to trigger logic
    return SQLServerInput("mssql+pyodbc://user:pass@host/db")

# --- _patch_connection_string tests ---

def test_patch_connection_string_non_mssql():
    src = "postgresql://user:pass@host/db"
    assert SQLServerInput._patch_connection_string(src) == src

def test_patch_connection_string_bare_odbc_connect():
    src = "mssql+pyodbc://u:p@h/db?odbc_connect&x=1"
    patched = SQLServerInput._patch_connection_string(src)
    assert "odbc_connect=" in patched
    # Ensure default driver added & properly encoded plus preserved other param
    assert "DRIVER%3DODBC+Driver+18+for+SQL+Server" in patched
    assert "TrustServerCertificate%3Dyes" in patched
    assert "x=1" in patched

# --- additional wrapper path tests ---

def test_patch_connection_string_existing_odbc_value():
    # existing odbc_connect with minimal value missing driver/trust should be patched via wrapper
    src = "mssql+pyodbc://u:p@h/db?odbc_connect=Server=localhost;Database=test"
    out = SQLServerInput._patch_connection_string(src)
    from urllib.parse import unquote_plus
    decoded = unquote_plus(out.split("odbc_connect=")[1])
    assert "DRIVER=ODBC Driver 18 for SQL Server" in decoded
    assert "TrustServerCertificate=yes" in decoded


def test_patch_connection_string_driver_only():
    src = "mssql+pyodbc://u:p@h/db?driver=ODBC+Driver+17+for+SQL+Server"
    out = SQLServerInput._patch_connection_string(src)
    # driver param style function should ensure trust added and driver sanitized
    assert "driver=ODBC+Driver+17+for+SQL+Server" in out
    assert "TrustServerCertificate=yes" in out

def test_patch_connection_string_mssql_no_changes():
    src = "mssql+pyodbc://u:p@h/db?foo=bar"
    out = SQLServerInput._patch_connection_string(src)
    # no driver or odbc_connect so unchanged
    assert out == src


def test_patch_connection_string_uppercase_odbc_connect():
    src = "mssql+pyodbc://u:p@h/db?ODBC_CONNECT=Server=localhost;Database=db"
    out = SQLServerInput._patch_connection_string(src)
    from urllib.parse import unquote_plus
    decoded = unquote_plus(out.split("ODBC_CONNECT=")[1]) if "ODBC_CONNECT=" in out else unquote_plus(out.split("odbc_connect=")[1])
    assert "DRIVER=ODBC Driver 18 for SQL Server" in decoded
    assert "TrustServerCertificate=yes" in decoded

# --- _patch_odbc_connect_string branches ---

def test_patch_odbc_connect_empty_query():
    src = "mssql+pyodbc://u:p@h/db"
    out = SQLServerInput._patch_odbc_connect_string(src)
    assert "odbc_connect=" in out


def test_patch_odbc_connect_bare_param_only():
    src = "mssql+pyodbc://u:p@h/db?odbc_connect"
    out = SQLServerInput._patch_odbc_connect_string(src)
    assert "DRIVER%3DODBC+Driver+18+for+SQL+Server" in out


def test_patch_odbc_connect_existing_missing_driver_and_trust():
    # value lacks driver/trust -> both appended
    custom = "Server=localhost;Database=test"
    src = f"mssql+pyodbc://u:p@h/db?odbc_connect={custom}"
    out = SQLServerInput._patch_odbc_connect_string(src)
    decoded = unquote_plus(out.split("odbc_connect=")[1])
    assert "DRIVER=ODBC Driver 18 for SQL Server" in decoded
    assert "TrustServerCertificate=yes" in decoded
    assert "Server=localhost" in decoded


def test_patch_odbc_connect_existing_has_driver_no_trust():
    custom = "DRIVER={ODBC Driver 17 for SQL Server};Server=localhost"  # braces & spaces
    src = f"mssql+pyodbc://u:p@h/db?odbc_connect={custom}"
    out = SQLServerInput._patch_odbc_connect_string(src)
    decoded = unquote_plus(out.split("odbc_connect=")[1])
    # Accept either space or plus form (environment-dependent decoding)
    assert ("DRIVER=ODBC Driver 17 for SQL Server" in decoded) or ("DRIVER=ODBC+Driver+17+for+SQL+Server" in decoded)
    assert "TrustServerCertificate=yes" in decoded


def test_patch_odbc_connect_no_odbc_param_adds_default():
    src = "mssql+pyodbc://u:p@h/db?foo=bar"
    out = SQLServerInput._patch_odbc_connect_string(src)
    assert "foo=bar" in out
    assert "odbc_connect=" in out


def test_patch_odbc_connect_empty_value():
    src = "mssql+pyodbc://u:p@h/db?odbc_connect="
    out = SQLServerInput._patch_odbc_connect_string(src)
    decoded = unquote_plus(out.split("odbc_connect=")[1])
    assert "DRIVER=ODBC Driver 18 for SQL Server" in decoded
    assert "TrustServerCertificate=yes" in decoded


def test_patch_odbc_connect_existing_has_both():
    custom = "DRIVER={ODBC Driver 18 for SQL Server};TrustServerCertificate=yes;Server=localhost"
    src = f"mssql+pyodbc://u:p@h/db?odbc_connect={custom}"
    out = SQLServerInput._patch_odbc_connect_string(src)
    decoded = unquote_plus(out.split("odbc_connect=")[1])
    # Should not duplicate driver or trust entries (accounting for plus or space)
    driver_variants = ["DRIVER=ODBC Driver 18 for SQL Server", "DRIVER=ODBC+Driver+18+for+SQL+Server"]
    assert sum(decoded.count(v) for v in driver_variants) == 1
    assert decoded.count("TrustServerCertificate=yes") == 1

# --- _patch_driver_params ---

def test_patch_driver_params_missing_all():
    src = "mssql+pyodbc://u:p@h/db?foo=bar"
    out = SQLServerInput._patch_driver_params(src)
    assert "driver=ODBC+Driver+18+for+SQL+Server" in out
    assert "TrustServerCertificate=yes" in out


def test_patch_driver_params_existing_sanitized():
    src = "mssql+pyodbc://u:p@h/db?driver={ODBC Driver 17 for SQL Server}&TrustServerCertificate=yes"
    out = SQLServerInput._patch_driver_params(src)
    assert "driver=ODBC+Driver+17+for+SQL+Server" in out
    # trust already present, no duplicate
    assert out.count("TrustServerCertificate=yes") == 1


def test_patch_driver_params_only_driver():
    src = "mssql+pyodbc://u:p@h/db?driver=ODBC+Driver+17+for+SQL+Server"
    out = SQLServerInput._patch_driver_params(src)
    assert "driver=ODBC+Driver+17+for+SQL+Server" in out
    assert "TrustServerCertificate=yes" in out


def test_patch_driver_params_only_trust():
    src = "mssql+pyodbc://u:p@h/db?TrustServerCertificate=yes"
    out = SQLServerInput._patch_driver_params(src)
    assert "driver=ODBC+Driver+18+for+SQL+Server" in out
    assert out.count("TrustServerCertificate=yes") == 1


def test_patch_driver_params_empty_query():
    src = "mssql+pyodbc://u:p@h/db"
    out = SQLServerInput._patch_driver_params(src)
    assert "driver=ODBC+Driver+18+for+SQL+Server" in out
    assert "TrustServerCertificate=yes" in out


# --- _get_all_tables with fallbacks ---

def test_get_all_tables_with_fallbacks(patched_input):
    tables = patched_input._get_all_tables()
    # Expect original + fallback additions
    expected = {("dbo", x) for x in [
        "existing_table", "existing_view", "info_schema_view", "sys_schema_view", "base_table_from_tables", "final_sys_view"
    ]}
    assert set(tables) == expected

def test_get_all_tables_non_mssql(monkeypatch):
    # Provide engine with non-mssql dialect so fallbacks skipped
    class NonMSSQLEngine(FakeEngine):
        def __init__(self, connection):
            super().__init__(connection)
            self.dialect = SimpleNamespace(name="postgresql")
    inspector = FakeInspector()
    connection = FakeConnection({})
    engine = NonMSSQLEngine(connection)
    import forklift.inputs.base_sql_input as base_mod
    monkeypatch.setattr(base_mod, "create_engine", lambda src: engine)
    monkeypatch.setattr(base_mod, "inspect", lambda eng: inspector)
    inp = SQLServerInput("postgresql://user:pass@host/db")  # though class name, dialect drives logic
    tables = inp._get_all_tables()
    assert tables == [("dbo", "existing_table"), ("dbo", "existing_view")]

# --- Fallback exception path ---

def test_fallback_exception_path(monkeypatch):
    # Patch create_engine to return engine whose connection execute raises
    failing_conn = FakeConnection({}, raise_errors=True)
    engine = FakeEngine(failing_conn)
    import forklift.inputs.base_sql_input as base_mod
    monkeypatch.setattr(base_mod, "create_engine", lambda src: engine)
    monkeypatch.setattr(base_mod, "inspect", lambda eng: FakeInspector())
    inp = SQLServerInput("mssql+pyodbc://user:pass@host/db")
    # Should not raise despite execute failing in fallbacks
    tables = inp._get_all_tables()
    assert tables == [("dbo", "existing_table"), ("dbo", "existing_view")]

def test_fallback_connections_closed(monkeypatch):
    # Track each connection closed in fallbacks
    closed = []

    class TrackingConnection(FakeConnection):
        def __init__(self, results_map):
            super().__init__(results_map, raise_errors=False)
        def close(self):
            closed.append(self)
            super().close()

    # results for all four fallback queries
    results_map = {
        "INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA": [("dbo", "info_schema_view")],
        "FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id WHERE s.name": [("dbo", "sys_schema_view")],
        "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA": [("dbo", "base_table_from_tables")],
        "FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id": [("dbo", "final_sys_view")],
    }

    class TrackingEngine(FakeEngine):
        def connect(self):
            # new connection each call so close path definitely runs
            return TrackingConnection(results_map)

    import forklift.inputs.base_sql_input as base_mod
    inspector = FakeInspector()
    engine = TrackingEngine(FakeConnection(results_map))
    monkeypatch.setattr(base_mod, "create_engine", lambda src: engine)
    monkeypatch.setattr(base_mod, "inspect", lambda eng: inspector)

    inp = SQLServerInput("mssql+pyodbc://user:pass@host/db")
    # Force fallbacks to create their own connections (not reuse self.connection)
    inp.connection = None
    tables = inp._get_all_tables()
    # 4 fallback helpers each should have created & closed a connection
    assert len(closed) == 4
    # Ensure expected tables included
    expected_names = {"existing_table", "existing_view", "info_schema_view", "sys_schema_view", "base_table_from_tables", "final_sys_view"}
    assert {t[1] for t in tables} == expected_names

# --- iter_rows not implemented ---

def test_iter_rows_not_implemented(patched_input):
    with pytest.raises(NotImplementedError):
        list(patched_input.iter_rows())

# --- Additional coverage for bare odbc_connect amid other params ---

def test_patch_odbc_connect_bare_amid_params():
    # Ensures loop branch handling bare 'odbc_connect' param (lines 120-123) is exercised
    src = "mssql+pyodbc://u:p@h/db?odbc_connect&foo=bar"
    out = SQLServerInput._patch_odbc_connect_string(src)
    # default driver/trust encoded
    assert "odbc_connect=" in out
    assert "foo=bar" in out
    assert "DRIVER%3DODBC+Driver+18+for+SQL+Server" in out
    assert "TrustServerCertificate%3Dyes" in out

# --- Direct fallback helper close coverage ---

def test_individual_fallback_helpers_close_connections(patch_sqlalchemy):
    # Instantiate after patching
    inp = SQLServerInput("mssql+pyodbc://user:pass@host/db")
    # Force creation of new connection per helper call
    inp.connection = None
    tables = []
    conn_obj = patch_sqlalchemy.engine._connection
    # information_schema views
    assert not conn_obj.closed
    inp._add_views_from_information_schema(tables, "dbo")
    assert conn_obj.closed
    # reset flag to observe subsequent close calls (simulate reopened connection)
    conn_obj.closed = False
    inp._add_views_from_sys_views(tables, "dbo")
    assert conn_obj.closed
    conn_obj.closed = False
    inp._add_tables_and_views_from_information_schema_tables(tables, "dbo")
    assert conn_obj.closed
    conn_obj.closed = False
    inp._add_all_views_from_sys_views(tables)
    assert conn_obj.closed

