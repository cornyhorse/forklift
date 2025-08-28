# forklift
![FORKLIFT.png](FORKLIFT.png)
Standardize ingestion & cleanup of messy tabular files (CSV / Excel / FWF / SQL DB / SQL Backup) into Parquet.

## Install
```bash
pip install forklift
# extras:
pip install "forklift[excel,s3,validate,dev]"
```

---

## CSV Input

- **Purpose:** Ingests CSV files, handling messy headers, prologues, footers, encoding, and column deduplication.
- **Implementation:** See [`src/forklift/inputs/csv_input.py`](src/forklift/inputs/csv_input.py)
- **Schema:** See [`schema-standards/20250826-csv.json`](schema-standards/20250826-csv.json)
- **Features:**
  - Prologue/footer skipping
  - Header detection and override
  - Encoding auto-detection
  - Column name deduplication
- **Usage:**
  - Use `CSVInput(source, ...)` to read rows as dictionaries.
  - Schema file defines expected columns and types.

---

## Excel Input

- **Purpose:** Ingests Excel files (XLS/XLSX), supporting multiple sheets, header modes, and column deduplication.
- **Implementation:** See [`src/forklift/inputs/excel_input.py`](src/forklift/inputs/excel_input.py)
- **Schema:** See [`schema-standards/20250826-excel.json`](schema-standards/20250826-excel.json)
- **Features:**
  - Multi-sheet support
  - Header modes: auto, present, absent
  - Header override per sheet
  - Column name deduplication
- **Usage:**
  - Use `ExcelInput(source, tables=[...], ...)` to read rows as dictionaries.
  - Schema file defines expected sheets, columns, and types.

---

## FWF Input

- **Purpose:** Ingests fixed-width formatted (FWF) files using a schema specification for column positions and types.
- **Implementation:** See [`src/forklift/inputs/fwf_input.py`](src/forklift/inputs/fwf_input.py)
- **Schema:** See [`schema-standards/20250826-fwf.json`](schema-standards/20250826-fwf.json)
- **Features:**
  - Schema-driven parsing (column positions, widths)
  - Yields rows as dictionaries
- **Usage:**
  - Use `FWFInput(source, fwf_spec=...)` to read rows as dictionaries.
  - Schema file defines column positions, widths, and types.

---

## SQL Input (Live Databases)

- **Purpose:** Ingests tables from SQL databases using SQLAlchemy, supporting glob-based schema/table selection.
- **Implementation:** See [`src/forklift/inputs/sql_input.py`](src/forklift/inputs/sql_input.py)
- **Schema:** See [`schema-standards/20250826-sql.json`](schema-standards/20250826-sql.json)
- **Features:**
  - Glob-based schema/table selection (e.g., `sales.*`, `*.*`)
  - Supports SQLite, PostgreSQL, MySQL, MS SQL Server
  - Yields rows as dictionaries
- **Usage:**
  - Use `SQLInput(source, include=[...], ...)` to read rows from matched tables.
  - Schema or `x-sql` block defines include patterns.
- **Limitations:**
  - **MS SQL Server:** Views might not export via some drivers; materialize as tables if needed.

---

## SQL Backup Input (pg_dump-style .sql files)

- **Purpose:** Ingests data from a PostgreSQL (pg_dump style) *SQL backup file* containing `CREATE TABLE` + single-line `INSERT` statements.
- **Implementation:** Wrapper: [`src/forklift/inputs/sql_backup_input.py`](src/forklift/inputs/sql_backup_input.py) delegating to Postgres parser.
- **Classes:**
  - `SQLBackupInput` (wrapper, like `SQLInput`)
  - `PostgresBackupInput` (current concrete implementation)
  - `BaseSQLBackupInput` (generic single-line parser)
- **Usage (Engine):**
  ```python
  from forklift.engine.engine import Engine
  eng = Engine(input_kind="sql_backup", output_kind="parquet")
  eng.run("/path/to/dump.sql", "out.parquet")
  ```
- **Direct Usage:**
  ```python
  from forklift.inputs.sql_backup_input import SQLBackupInput
  inp = SQLBackupInput("dump.sql", include=["public.*"])  # patterns optional
  for table in inp.get_tables():
      print(table["schema"], table["name"], len(table["rows"]))
  ```
- **Include Patterns:** Same semantics as live SQL (e.g., `schema.*`, `schema.table`, `*.*`). If omitted defaults to `*.*`.
- **Single-Line Constraint:** Only INSERT statements wholly on one line are parsed:
  ```sql
  INSERT INTO public.users (id, name) VALUES (1, 'Alice');  -- supported
  INSERT INTO public.users (id, name) VALUES
  (2, 'Bob');  -- ignored (multiline)
  ```
- **Multiline Flag:** Passing `multiline=True` (or `multi_line=True`) raises `NotImplementedError` deliberately, making the limitation explicit.
- **Skipped Rows Tracking:** Length mismatches (column vs value count) are tracked via `get_skipped()` on the delegate.
- **Not Parsed:** COPY blocks, multi-row VALUES batches, triggers, functions, constraints beyond column extraction.
- **When to Use:** For lightweight ETL or testing when you have a dump file but not a live database connection.

---
