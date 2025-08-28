from forklift.inputs.sql_backup_input import SQLBackupInput
import re, os, json
DUMP = 'tests/test-files/sql/source-sql-ddl-and-data/pg/001-sales-alt-export.sql'

HEAD = re.compile(r"INSERT\s+INTO\s+sales\.good_customers", re.IGNORECASE)

def main():
    path = os.path.abspath(DUMP)
    parser = SQLBackupInput(path)
    tables = {(t['schema'], t['name']): t for t in parser.get_tables()}
    gc = tables.get(('sales','good_customers'))
    if not gc:
        print('No sales.good_customers parsed')
        return
    ids = sorted(r['id'] for r in gc['rows'])
    print('Parsed row count:', len(ids))
    missing = [i for i in range(1,21) if i not in ids]
    print('Missing IDs:', missing)
    print('\nRaw INSERT lines for sales.good_customers:')
    with open(path,'r',encoding='utf-8',errors='ignore') as fh:
        for ln in fh:
            if HEAD.search(ln):
                print(ln.strip())

if __name__ == '__main__':
    main()

