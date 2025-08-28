import os
import pytest
from forklift.inputs.sql_backup_input import SQLBackupInput
from forklift.inputs.base_sql_backup_input import BaseSQLBackupInput


def _existing_backup_path():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "test-files", "sql", "source-sql-ddl-and-data", "pg", "001-sales-alt-export.sql"))


def test_sql_backup_blank_pattern_skip_then_match():
    # First pattern blank exercises _matches branch that continues on empty after strip
    path = _existing_backup_path()
    inp = SQLBackupInput(path, include=["  ", "sales.good_customers"])  # blank then concrete
    tables = {(t["schema"], t["name"]): t for t in inp.get_tables()}
    assert ("sales", "good_customers") in tables


def test_sql_backup_create_with_only_constraint_then_insert_and_single_column_create(tmp_path):
    # CREATE with only a constraint -> no columns captured, later INSERT backfills columns inside _parse (line 107)
    # Second CREATE has a single column so final segment path (lines 154-156) is exercised.
    sql = (
        "CREATE TABLE s.t (CONSTRAINT t_pk PRIMARY KEY (id));\n"
        "INSERT INTO s.t (id) VALUES (1);\n"
        "CREATE TABLE s.u (c1 int);\n"
        "INSERT INTO s.u (c1) VALUES (5);\n"
    )
    p = tmp_path / "c.sql"
    p.write_text(sql)
    inp = SQLBackupInput(str(p))
    tables = {(t["schema"], t["name"]): t for t in inp.get_tables()}
    assert tables[("s", "t")]["rows"] == [{"id": 1}]
    assert tables[("s", "t")]["rows"][0]["id"] == 1
    # columns should have been backfilled from INSERT
    assert tables[("s", "t")]["rows"][0].keys() == {"id"}
    # single column table captured via final segment path
    assert tables[("s", "u")]["rows"] == [{"c1": 5}]


def test_base_sql_backup_parse_values_trailing_paren(tmp_path):
    # Build minimal file for delegate
    sql = "INSERT INTO s.v (a,b) VALUES (1,2);\n"
    fp = tmp_path / "v.sql"
    fp.write_text(sql)
    inp = SQLBackupInput(str(fp))
    delegate: BaseSQLBackupInput = inp._delegate  # type: ignore
    # Directly exercise _parse_values branch removing trailing ')'
    assert delegate._parse_values("1,2)") == [1, 2]

