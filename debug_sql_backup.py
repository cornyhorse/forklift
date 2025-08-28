from forklift.inputs.sql_backup_input import SQLBackupInput
from forklift.inputs.base_sql_backup_input import INSERT_RE
import textwrap, os, json, tempfile

def main():
    sql = textwrap.dedent("""
        -- comment line should be ignored
        CREATE TABLE sch.sample (
            id integer NOT NULL,
            amount numeric(10,2),
            note text,
            created_at text,
            CONSTRAINT sample_pkey PRIMARY KEY (id)
        );
        INSERT INTO sch.pre (id, name) VALUES (1, 'a');
        INSERT INTO sch.sample (id, amount, note, created_at) VALUES
        ( 10,
          12.34,
          'Line with comma, inside',
          '2024-01-01'
        );
        INSERT INTO sch.sample (id, amount) VALUES (99);
        CREATE TABLE sch.types(
          b boolean,
          n numeric(5,2),
          t text,
          f text,
          s text
        );
        INSERT INTO sch.types (b, n, t, f, s) VALUES (true, 3.14, NULL, NOW(), 'O''Reilly');
        CREATE TABLE sch.pre (
          id integer,
          name text
        );
        INSERT INTO sch.sample (id, amount, note, created_at) VALUES (11, 0, 'Trailing test', '2024-01-02');
        INSERT INTO sch.sample (id, amount, note, created_at) VALUES (12, 5, 'Unterminated start'
    """)
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, 'dump.sql')
        open(path,'w').write(sql)
        print('--- Lines & match info ---')
        for ln in open(path):
            ls = ln.strip()
            if not ls:
                continue
            if ls.lower().startswith('insert into sch.types'):
                m = INSERT_RE.search(ls)
                print(ls)
                print('Matched:', bool(m))
                if m:
                    print('Groups:', m.groups())
        inp = SQLBackupInput(path)
        tables = inp.get_tables()
        print('\n--- Parsed tables ---')
        print(json.dumps(tables, indent=2))

if __name__ == '__main__':
    main()
