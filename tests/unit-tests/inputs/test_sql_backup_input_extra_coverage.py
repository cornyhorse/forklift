import os
from forklift.inputs.sql_backup_input import SQLBackupInput

def test_sql_backup_single_line_create_and_constraint_and_dedupe(tmp_path):
    sql = """
    -- Single-line create with constraint & parentheses in types
    CREATE TABLE s.ct (id int, amount numeric(10,2), note text, CONSTRAINT note_chk CHECK (length(note) > 0));
    -- valid row
    INSERT INTO s.ct (id, amount, note) VALUES (1, 12.50, 'Hi');
    -- duplicate row (should be deduped)
    INSERT INTO s.ct (id, amount, note) VALUES (1, 12.50, 'Hi');
    -- len mismatch (skip) only one value for two columns
    INSERT INTO s.ct (id, amount) VALUES (5);
    -- table with empty column list (unusual but ensures backfill branch) then insert provides columns
    CREATE TABLE s.empty ();
    INSERT INTO s.empty (col1) VALUES (42);
    """.strip()
    p = tmp_path / "extra.sql"
    p.write_text(sql)

    inp = SQLBackupInput(str(p))
    tables = {(t['schema'], t['name']): t for t in inp.get_tables()}

    # ct table columns (constraint removed, numeric type kept)
    ct = tables[('s','ct')]
    assert {'id','amount','note'} == set(r for r in ct['rows'][0].keys())
    # only one row due to dedupe
    assert len(ct['rows']) == 1
    assert ct['rows'][0]['amount'] == 12.50

    empty_tbl = tables[('s','empty')]
    assert empty_tbl['rows'][0]['col1'] == 42

    # skipped entry recorded for len mismatch
    skipped = inp._delegate.get_skipped()  # type: ignore
    assert any(s['reason'].startswith('len_mismatch') and s['name']=='ct' for s in skipped)

