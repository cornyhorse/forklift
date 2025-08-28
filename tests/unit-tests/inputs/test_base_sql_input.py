import pytest

from forklift.inputs.base_sql_input import BaseSQLInput


class EngineStub:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.connect_called = 0

    def connect(self):  # simulate connect success or failure
        self.connect_called += 1
        if self.should_fail:
            raise Exception("boom")
        return object()


class SqliteInspectorStub:
    def __init__(self, tables=None, views=None):
        self._tables = tables or ["t1", "t2"]
        self._views = views or ["v1"]

    # sqlite style (no schema)
    def get_table_names(self):
        return list(self._tables)

    def get_view_names(self):
        return list(self._views)


class NormalInspectorStub:
    def __init__(self, mapping=None):
        # mapping: schema -> {tables: [...], views: [...]}
        self._mapping = mapping or {
            "schema1": {"tables": ["t1", "t2"], "views": ["v1"]},
            "schema2": {"tables": ["t2", "solo"], "views": []},
        }

    def get_schema_names(self):
        return list(self._mapping.keys())

    def get_table_names(self, schema=None):
        return list(self._mapping[schema]["tables"]) if schema else []

    def get_view_names(self, schema=None):
        return list(self._mapping[schema]["views"]) if schema else []


@pytest.mark.parametrize("should_fail", [False, True])
def test_init_connection_and_include(monkeypatch, should_fail):
    # Patch create_engine to use stub capturing should_fail path for try/except
    engine = EngineStub(should_fail=should_fail)
    monkeypatch.setattr("forklift.inputs.base_sql_input.create_engine", lambda source: engine)
    # Provide inspector stub
    monkeypatch.setattr(
        "forklift.inputs.base_sql_input.inspect", lambda eng: NormalInspectorStub()
    )

    # include provided (non-None) so branch hit
    inp = BaseSQLInput("dummy://", include=["schema1.*"])  # connection attempted

    assert inp.engine is engine
    if should_fail:
        assert inp.connection is None  # except branch
    else:
        assert inp.connection is not None  # success branch

    # Provided include should be preserved, not replaced with ["*.*"]
    assert inp.include == ["schema1.*"]


def test_init_include_default(monkeypatch):
    # Cover default include None path
    engine = EngineStub()
    monkeypatch.setattr("forklift.inputs.base_sql_input.create_engine", lambda source: engine)
    monkeypatch.setattr(
        "forklift.inputs.base_sql_input.inspect", lambda eng: NormalInspectorStub()
    )

    inp = BaseSQLInput("dummy://")  # include None triggers default
    assert inp.include == ["*.*"]


def test_get_all_tables_sqlite(monkeypatch):
    # Patch for sqlite mode
    engine = EngineStub()
    monkeypatch.setattr("forklift.inputs.base_sql_input.create_engine", lambda source: engine)
    inspector = SqliteInspectorStub(tables=["a"], views=["b"])  # simple small set
    monkeypatch.setattr("forklift.inputs.base_sql_input.inspect", lambda eng: inspector)

    inp = BaseSQLInput("sqlite://")
    inp.is_sqlite = True  # toggle path

    assert sorted(inp._get_all_tables()) == [(None, "a"), (None, "b")]  # both tables + views


def test_get_all_tables_normal(monkeypatch):
    engine = EngineStub()
    monkeypatch.setattr("forklift.inputs.base_sql_input.create_engine", lambda source: engine)
    inspector = NormalInspectorStub(
        mapping={
            "s1": {"tables": ["t1"], "views": ["v1"]},
            "s2": {"tables": ["t2"], "views": []},
        }
    )
    monkeypatch.setattr("forklift.inputs.base_sql_input.inspect", lambda eng: inspector)

    inp = BaseSQLInput("dummy://")
    tables = set(inp._get_all_tables())
    # Expect every table and view with schema
    assert tables == {("s1", "t1"), ("s1", "v1"), ("s2", "t2")}


def test_get_tables_pattern_matching(monkeypatch):
    # Provide deterministic inspector content
    engine = EngineStub()
    monkeypatch.setattr("forklift.inputs.base_sql_input.create_engine", lambda source: engine)
    inspector = NormalInspectorStub(
        mapping={
            "s1": {"tables": ["t1", "x"], "views": ["v1"]},
            "s2": {"tables": ["t2", "x"], "views": ["v2"]},
        }
    )
    monkeypatch.setattr("forklift.inputs.base_sql_input.inspect", lambda eng: inspector)

    # Patterns exercise: empty string skip, schema.*, schema.table, plain name, *.* (all), duplicate pattern
    patterns = ["", "s1.*", "s2.t2", "x", "*.*", "s1.t1"]
    inp = BaseSQLInput("dummy://", include=patterns)
    result = inp.get_tables()
    # Convert to set of tuples for comparison
    result_set = {(r["schema"], r["name"]) for r in result}
    # Expect all tables/views present because of *.* plus others
    expected = {
        ("s1", "t1"), ("s1", "x"), ("s1", "v1"),
        ("s2", "t2"), ("s2", "x"), ("s2", "v2"),
    }
    assert result_set == expected
    # Ensure rows key included and empty list
    assert all(r["rows"] == [] for r in result)


def test_iter_rows_not_implemented(monkeypatch):
    engine = EngineStub()
    monkeypatch.setattr("forklift.inputs.base_sql_input.create_engine", lambda source: engine)
    monkeypatch.setattr(
        "forklift.inputs.base_sql_input.inspect", lambda eng: NormalInspectorStub()
    )
    inp = BaseSQLInput("dummy://")
    with pytest.raises(NotImplementedError):
        list(inp.iter_rows())

