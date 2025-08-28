import os
import types
import pytest
from sqlalchemy.exc import SQLAlchemyError
from forklift.inputs.sql_input import SQLInput

def get_sqlite_conn_str():
    """
    Return the SQLite connection string for the test database.

    :return: SQLite connection string
    :rtype: str
    """
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../test-files/sqlite/sqlite.db'))
    return f'sqlite:///{db_path}'

def make_mock_inspector():
    inspector = types.SimpleNamespace()
    inspector.get_schema_names = lambda: ["public", "analytics"]
    inspector.get_table_names = lambda schema=None: ["users", "events"] if schema == "public" else ["reports"]
    inspector.get_view_names = lambda schema=None: ["user_view"] if schema == "public" else ["report_view"]
    return inspector

def test_sql_input_all_tables():
    """
    Test that all tables and views are copied when using the '*.*' glob pattern.

    - Asserts all expected tables/views are present.
    - Asserts rows are returned as dictionaries.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    for t in tables:
        assert isinstance(t["rows"], list)
        if t["rows"]:
            assert isinstance(t["rows"][0], dict)
            assert "_table" not in t["rows"][0]  # _table only in iter_rows

def test_sql_input_single_table():
    """
    Test that only the specified table is copied when using a single table glob pattern.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_sql_input_view():
    """
    Test that only the specified view is copied when using a view glob pattern.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["v_good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "v_good_customers"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_sql_input_nonexistent_table():
    """
    Test that no tables are copied when a non-existent table is specified in the glob pattern.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["does_not_exist"])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_default_all_tables():
    """
    Test that all tables and views are copied when no 'include' argument is specified (default behavior).
    """
    sql_input = SQLInput(source=get_sqlite_conn_str())  # No include specified
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    # Should include all tables/views
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert len(table_names) == 3  # Only these three

def test_sql_input_subset_tables():
    """
    Test that only the specified subset of tables are copied and others are excluded.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["good_customers", "purchases"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    # Should only include specified tables
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" not in table_names
    assert len(table_names) == 2

def test_sql_input_empty_pattern():
    """
    Test that an empty pattern results in no tables being copied.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=[""])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_invalid_pattern():
    """
    Test that an invalid pattern (e.g., malformed) results in no tables being copied.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["foo..bar"])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_empty_include_list():
    """
    Test that an empty include list results in no tables being copied.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=[])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_del():
    """
    Test that the destructor (__del__) runs without error.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["good_customers"])
    del sql_input  # Should not raise

def test_sql_input_non_sqlite_patterns(monkeypatch):
    """
    Test all glob pattern branches for a non-SQLite database using a mocked inspector.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["*.*", "public.*", "public.users", "users"])
    sql_input.is_sqlite = False
    sql_input.inspector = make_mock_inspector()
    # Patch Table and select in the correct module to avoid real DB access
    monkeypatch.setattr("forklift.inputs.sql_input.Table", lambda name, metadata, schema=None, autoload_with=None: types.SimpleNamespace())
    monkeypatch.setattr("forklift.inputs.sql_input.select", lambda table_obj: "SELECT *")
    sql_input.connection.execute = lambda stmt: [types.SimpleNamespace(_mapping={"id": 1, "name": "test"})]
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    # Should include all tables/views from both schemas
    assert "users" in table_names
    assert "events" in table_names
    assert "reports" in table_names
    assert "user_view" in table_names
    assert "report_view" in table_names

def test_sql_input_table_reflection_error(monkeypatch):
    """
    Test that a table reflection error is handled (exception raised).
    """
    sql_input = SQLInput(source=get_sqlite_conn_str(), include=["error_table"])
    sql_input.is_sqlite = True
    # Minimal inspector returns one table that matches the pattern
    inspector = types.SimpleNamespace()
    inspector.get_table_names = lambda: ["error_table"]
    inspector.get_view_names = lambda: []
    sql_input.inspector = inspector
    def raise_error(*args, **kwargs):
        raise SQLAlchemyError("Reflection failed")
    monkeypatch.setattr("forklift.inputs.sql_input.Table", raise_error)
    monkeypatch.setattr("forklift.inputs.sql_input.select", lambda table_obj: "SELECT *")
    sql_input.connection.execute = lambda stmt: [types.SimpleNamespace(_mapping={"id": 1, "name": "test"})]
    with pytest.raises(SQLAlchemyError):
        list(sql_input.iter_rows())

def test_sql_input_empty_database(monkeypatch):
    """
    Test that an empty database (no tables/views) results in no output.
    """
    sql_input = SQLInput(source=get_sqlite_conn_str())
    sql_input.is_sqlite = True
    inspector = types.SimpleNamespace()
    inspector.get_table_names = lambda: []
    inspector.get_view_names = lambda: []
    sql_input.inspector = inspector
    tables = sql_input.get_tables()
    assert tables == []
