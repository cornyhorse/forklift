from forklift.inputs.sql_backup_input import SQLBackupInput
import os

path = 'tests/test-files/sql/source-sql-ddl-and-data/pg/001-sales-alt-export.sql'
parser = SQLBackupInput(path)
ids = []
for t in parser.get_tables():
    if (t['schema'], t['name']) == ('sales','good_customers'):
        ids = sorted(r['id'] for r in t['rows'])
        break
print('Count:', len(ids))
print('IDs:', ids)
print('Missing:', [i for i in range(1,21) if i not in ids])

