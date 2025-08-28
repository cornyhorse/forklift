# forklift
![FORKLIFT.png](FORKLIFT.png)
Standardize ingestion & cleanup of messy tabular files (CSV/Excel/FWF/SQL) into Parquet.

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

## SQL Input

- **Purpose:** Ingests tables from SQL databases using SQLAlchemy, supporting glob-based schema/table selection.
- **Implementation:** See [`src/forklift/inputs/sql_input.py`](src/forklift/inputs/sql_input.py)
- **Schema:** See [`schema-standards/20250826-sql.json`](schema-standards/20250826-sql.json)
- **Features:**
  - Glob-based schema/table selection (e.g., `sales.*`, `*.*`)
  - Supports SQLite, PostgreSQL, MySQL, MS SQL Server
  - Yields rows as dictionaries
- **Usage:**
  - Use `SQLInput(source, include=[...], ...)` to read rows from matched tables.
  - Schema file defines expected tables, columns, and types.
- **Limitations:**
  - **MS SQL Server:** Due to ODBC/driver limitations, views are not exported—only tables are exported. If you need view data, materialize views as tables before ingestion.

---
