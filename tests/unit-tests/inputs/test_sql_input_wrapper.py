from forklift.inputs.sql_input import SQLInput, get_sql_input
from forklift.inputs.base_sql_input import BaseSQLInput


def test_get_sql_input_fallback(monkeypatch):
    """Ensure an unrecognized connection scheme falls back to BaseSQLInput.

    We monkeypatch BaseSQLInput in the sql_input module so we don't need a real
    SQLAlchemy engine for an unknown driver scheme and to assert the fallback path executes.
    """
    created = {}

    class Dummy(BaseSQLInput):  # type: ignore
        def __init__(self, source, include=None, **opts):  # pragma: no cover - constructor side effects not needed
            created["called"] = True

    monkeypatch.setattr("forklift.inputs.sql_input.BaseSQLInput", Dummy)
    inst = get_sql_input("otherdb://some-host/dbname")  # scheme not matched by any explicit branch
    assert isinstance(inst, Dummy)
    assert created.get("called") is True


def test_sqlinput_wrapper_delegation(monkeypatch):
    """Validate that SQLInput wrapper delegates all public methods to the underlying concrete input instance."""
    calls = {}

    class Dummy(BaseSQLInput):  # type: ignore
        def __init__(self, source, include=None, **opts):  # pragma: no cover - nothing to do
            pass

        def _get_all_tables(self):
            calls["tables"] = True
            return [("schema", "name")]

        def iter_rows(self):
            calls["iter"] = True
            return iter([{"a": 1}])

        def get_tables(self):
            calls["get_tables"] = True
            return [{"schema": "schema", "name": "name", "rows": []}]

    # Patch factory used inside SQLInput.__init__ to return our dummy delegate
    monkeypatch.setattr("forklift.inputs.sql_input.get_sql_input", lambda source, include=None, **opts: Dummy(source, include, **opts))

    wrapper = SQLInput("dummy://")

    # Exercise each delegating method
    assert wrapper._get_all_tables() == [("schema", "name")]
    assert list(wrapper.iter_rows()) == [{"a": 1}]
    assert wrapper.get_tables() == [{"schema": "schema", "name": "name", "rows": []}]

    # Ensure all delegate methods were invoked
    assert set(calls.keys()) == {"tables", "iter", "get_tables"}


def test_get_sql_input_all_dialects(monkeypatch):
    """Cover every explicit dialect branch in get_sql_input by stubbing each class to avoid real DB connections."""
    constructed = {}

    class Stub:  # simple non-BaseSQLInput stub to avoid engine creation
        def __init__(self, source, include=None, **opts):
            constructed.setdefault(self.__class__.__name__, 0)
            constructed[self.__class__.__name__] += 1

    # Patch each concrete input symbol used by get_sql_input
    monkeypatch.setattr("forklift.inputs.sql_input.SQLServerInput", type("SQLServerStub", (Stub,), {}))
    monkeypatch.setattr("forklift.inputs.sql_input.SQLiteInput", type("SQLiteStub", (Stub,), {}))
    monkeypatch.setattr("forklift.inputs.sql_input.MySQLInput", type("MySQLStub", (Stub,), {}))
    monkeypatch.setattr("forklift.inputs.sql_input.OracleInput", type("OracleStub", (Stub,), {}))
    monkeypatch.setattr("forklift.inputs.sql_input.PostgresInput", type("PostgresStub", (Stub,), {}))

    # Exercise each branch (include both postgres forms)
    get_sql_input("mssql://server/db")
    get_sql_input("sqlite:///file.db")
    get_sql_input("mysql://server/db")
    get_sql_input("oracle://server/db")
    get_sql_input("postgres://server/db")
    get_sql_input("postgresql://server/db")

    # Ensure each stub constructed exactly once except Postgres which is hit twice (postgres, postgresql)
    assert constructed["SQLServerStub"] == 1
    assert constructed["SQLiteStub"] == 1
    assert constructed["MySQLStub"] == 1
    assert constructed["OracleStub"] == 1
    assert constructed["PostgresStub"] == 2

