from forklift.inputs.sql_backup_input import SQLBackupInput, get_sql_backup_input
from forklift.inputs.base_sql_backup_input import BaseSQLBackupInput
import pytest


def test_get_sql_backup_input_factory(monkeypatch, tmp_path):
    """Ensure factory returns a PostgresBackupInput (stubbed) and receives include + opts."""
    created = {}

    class Dummy(BaseSQLBackupInput):  # type: ignore
        def __init__(self, source, include=None, **opts):  # pragma: no cover - init side-effects tracked
            created['source'] = source
            created['include'] = include
            created['opts'] = opts
            # call BaseSQLBackupInput __init__ path but with a temp file to satisfy file existence
            super().__init__(source, include, **opts)

    # Patch PostgresBackupInput symbol used by factory
    monkeypatch.setattr("forklift.inputs.sql_backup_input.PostgresBackupInput", Dummy)

    # Create a temp .sql file for source
    sql_file = tmp_path / "sample.sql"
    sql_file.write_text("-- empty\n")

    inst = get_sql_backup_input(str(sql_file), include=["public.table"], extra=True)
    assert isinstance(inst, Dummy)
    assert created['source'] == str(sql_file)
    assert created['include'] == ["public.table"]
    assert created['opts']['extra'] is True


def test_sql_backup_input_wrapper_delegation(monkeypatch, tmp_path):
    """Validate that SQLBackupInput delegates to the underlying implementation and _get_all_tables covers code path."""
    calls = {}

    class Dummy(BaseSQLBackupInput):  # type: ignore
        def __init__(self, source, include=None, **opts):  # pragma: no cover
            # ensure a valid file
            super().__init__(source, include, **opts)
        def iter_rows(self):
            calls['iter'] = True
            return iter([{'a': 1}])
        def get_tables(self):
            calls['tables'] = True
            return [{"schema": "s", "name": "t", "rows": [{'a': 1}]}]

    monkeypatch.setattr("forklift.inputs.sql_backup_input.get_sql_backup_input", lambda source, include=None, **opts: Dummy(source, include, **opts))

    sql_file = tmp_path / "sample.sql"
    sql_file.write_text("-- empty\n")

    wrapper = SQLBackupInput(str(sql_file))
    assert list(wrapper.iter_rows()) == [{'a': 1}]
    tables = wrapper.get_tables()
    assert tables[0]['name'] == 't'
    assert wrapper._get_all_tables() == [("s", "t")]
    assert set(calls.keys()) == {'iter', 'tables'}


def test_sql_backup_input_include_passthrough(monkeypatch, tmp_path):
    """Assert include list is passed through to delegate when constructing wrapper."""
    captured = {}
    class Dummy(BaseSQLBackupInput):  # type: ignore
        def __init__(self, source, include=None, **opts):  # pragma: no cover
            captured['include'] = include
            super().__init__(source, include, **opts)
        def get_tables(self):
            return []
        def iter_rows(self):
            return iter([])

    monkeypatch.setattr("forklift.inputs.sql_backup_input.get_sql_backup_input", lambda source, include=None, **opts: Dummy(source, include, **opts))

    sql_file = tmp_path / "sample.sql"
    sql_file.write_text("-- empty\n")
    SQLBackupInput(str(sql_file), include=["schema.*", "other.table"])  # construct only
    assert captured['include'] == ["schema.*", "other.table"]

