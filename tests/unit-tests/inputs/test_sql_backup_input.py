import os
import pytest

from forklift.engine.registry import get_input_cls
from forklift.inputs.sql_backup_input import SQLBackupInput
from forklift.inputs.base_sql_backup_input import BaseSQLBackupInput


def _backup_path():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "test-files", "sql", "source-sql-ddl-and-data", "pg", "001-sales-alt-export.sql"))


def test_registry_lookup():
    cls = get_input_cls("sql_backup")
    assert cls is SQLBackupInput


def test_sql_backup_parse_tables_and_rows():
    path = _backup_path()
    inp = SQLBackupInput(path)
    tables = { (t["schema"], t["name"]): t for t in inp.get_tables() }
    # Expected tables from dump
    expected = {
        ("alt", "good_customers"): 2,
        ("alt", "purchases"): 1,
        ("sales", "good_customers"): 20,
        ("sales", "purchases"): 20,
    }
    for key, count in expected.items():
        assert key in tables, f"Missing table {key}"
        assert len(tables[key]["rows"]) == count

    # Spot check one row with escaped quote
    sales_gc = tables[("sales", "good_customers")]["rows"]
    ola = next(r for r in sales_gc if r["name"].startswith("Ola"))
    assert ola["name"] == "Ola O'Neil"
    assert isinstance(ola["active"], bool) and ola["active"] is True


@pytest.mark.parametrize("include,expected_tables", [
    (["alt.*"], {"good_customers", "purchases"}),
    (["sales.good_customers"], {"good_customers"}),
    (["*.*"], {"good_customers", "purchases"}),
    (["good_customers"], {"good_customers"}),  # bare table pattern
])
def test_sql_backup_include_filtering(include, expected_tables):
    path = _backup_path()
    inp = SQLBackupInput(path, include=include)
    tables = inp.get_tables()
    names = set(t["name"] for t in tables)
    assert names.issuperset(expected_tables)


def test_sql_backup_multiline_and_edge_cases(tmp_path):
    # Craft a dump with multi-line INSERT (unsupported by design now), mismatched INSERT, and incomplete trailing buffer
    sql = """
    -- comment line should be ignored
    CREATE TABLE sch.sample (
        id integer NOT NULL,
        amount numeric(10,2),
        note text,
        created_at text,
        CONSTRAINT sample_pkey PRIMARY KEY (id)
    );
    -- Insert before table to test ordering (will define another table later)
    INSERT INTO sch.pre (id, name) VALUES (1, 'a');
    -- Multi-line insert (should be ignored entirely under single-line-only policy)
    INSERT INTO sch.sample (id, amount, note, created_at) VALUES
    ( 10,
      12.34,
      'Line with comma, inside',
      '2024-01-01'
    );
    -- Mismatched column/value count (skip and recorded) -> only one value for two columns
    INSERT INTO sch.sample (id, amount) VALUES (99);
    -- Create table after insert (tests _ensure_table no-op when columns already known)
    CREATE TABLE sch.pre (
      id integer,
      name text
    );
    INSERT INTO sch.sample (id, amount, note, created_at) VALUES (11, 0, 'Trailing test', '2024-01-02');
    INSERT INTO sch.sample (id, amount, note, created_at) VALUES (12, 5, 'Unterminated start'
    """
    path = tmp_path / "dump.sql"
    path.write_text(sql)

    inp = SQLBackupInput(str(path))
    tables = {(t["schema"], t["name"]): t for t in inp.get_tables()}

    sample_rows = tables[("sch", "sample")]["rows"]
    ids = sorted(r["id"] for r in sample_rows)
    # Only the single-line INSERT with id 11 should be captured; id 10 multiline ignored; id 12 incomplete ignored
    assert ids == [11]

    pre_rows = tables[("sch", "pre")]["rows"]
    assert pre_rows == [{"id": 1, "name": "a"}]

    flat = list(inp.iter_rows())
    # flat rows: pre (1) + sample (1)
    assert len(flat) == 2


def test_base_sql_backup_internal_methods_for_coverage(tmp_path):
    sql = "CREATE TABLE s.t (col1 int, CONSTRAINT c PRIMARY KEY (col1));\nINSERT INTO s.t (col1) VALUES (1);"
    p = tmp_path / "a.sql"
    p.write_text(sql)
    inp = SQLBackupInput(str(p))
    # Access delegate (BaseSQLBackupInput) to test _coerce fallback and column backfill
    delegate: BaseSQLBackupInput = inp._delegate  # type: ignore
    # Manually invoke internal methods
    tbl_meta = delegate._ensure_table("x", "y", None)
    assert tbl_meta["columns"] == []
    delegate._ensure_table("x", "y", ["c1", "c2"])  # backfill columns branch
    assert delegate._tables[("x", "y")]["columns"] == ["c1", "c2"]
    # _coerce fallback + numeric + float + boolean + NULL + string
    assert delegate._coerce("NULL") is None
    assert delegate._coerce("true") is True
    assert delegate._coerce("123") == 123
    assert delegate._coerce("4.56") == 4.56
    assert delegate._coerce("'hi'") == "hi"
    assert delegate._coerce("weird_token") == "weird_token"
    assert delegate._coerce("NOW()") == "NOW()"


def test_sql_backup_file_not_found():
    with pytest.raises(FileNotFoundError):
        SQLBackupInput("/nonexistent/path/dump.sql")


def test_sql_backup_multiline_flag_raises(tmp_path):
    sql = "INSERT INTO s.t (id, name) VALUES (1, 'a');\n"
    p = tmp_path / "one.sql"
    p.write_text(sql)
    with pytest.raises(NotImplementedError):
        SQLBackupInput(str(p), multiline=True)
