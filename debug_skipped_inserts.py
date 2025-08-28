from forklift.inputs.sql_backup_input import SQLBackupInput
import os, json
path = 'tests/test-files/sql/source-sql-ddl-and-data/pg/001-sales-alt-export.sql'
parser = SQLBackupInput(path)
skipped = parser._delegate.get_skipped()  # type: ignore
print('Total skipped:', len(skipped))
for s in skipped:
    if s['schema']=='sales' and s['name']=='good_customers':
        print('\nSkipped sales.good_customers:')
        print(json.dumps(s, indent=2))

