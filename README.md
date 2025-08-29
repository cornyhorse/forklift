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

- **Purpose:** Ingests Excel files (XLS/XLSX) using a Polars-based reader, supporting multiple sheets, header modes, and column deduplication.
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

## License

This project is licensed under the MIT License. See the LICENSE file for the full text.

## Third-Party Notices

forklift bundles or makes use of third-party open source software. Attribution and licenses for notable dependencies are provided below. This list isn't exhaustive; see each package's distribution for complete details.

### Polars

Polars is an in-memory DataFrame library used for high-performance data manipulation.

- Project: https://github.com/pola-rs/polars
- License: MIT License
- Copyright: (c) Polars contributors

Polars is distributed under the MIT License:

```
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Other Notable Dependencies (Direct)

| Package | Purpose (brief) | License |
|---------|-----------------|---------|
| pyarrow | Columnar memory / Parquet | Apache-2.0 |
| openpyxl | XLSX reading | MIT |
| xlrd | Legacy XLS reading | BSD-2-Clause |
| sqlalchemy | SQL abstraction | MIT |
| boto3 | AWS SDK | Apache-2.0 |
| smart-open | Stream I/O (S3, etc.) | MIT |
| jsonschema | JSON Schema validation | MIT |
| frictionless | Data package validation (optional) | MIT |
| pandera | DataFrame validation (optional) | MIT |
| python-dateutil | Date parsing | BSD-3-Clause |
| charset-normalizer | Encoding detection | MIT |
| typing-extensions | Typing backports | PSF-2.0 |
| psycopg2-binary | PostgreSQL driver | LGPL-3.0 (with exceptions) |
| pymysql | MySQL driver | MIT |
| pyodbc | ODBC driver bridge | MIT |
| oracledb | Oracle DB driver | BSD-3-Clause |
| pytest / pytest-cov | Testing | MIT |
| ruff | Linting | MIT |
| mypy | Static typing | MIT |
| build | PEP 517 builds | MIT |
| twine | Publishing | Apache-2.0 |
| polars | DataFrame engine | MIT |

(All names above are trademarks of their respective owners.)

Apache-2.0 components (e.g., pyarrow, boto3, twine) are permissive and compatible with MIT. If you redistribute binary builds bundling their code, include their LICENSE and any NOTICE files.

psycopg2-binary is distributed under the GNU LGPL 3.0 with a linking exception (see https://www.psycopg.org/). Dynamic linking keeps forklift under MIT; if you statically link or embed, ensure compliance with LGPL terms (provide relinkable objects or allow replacement).

Most other listed dependencies are under permissive MIT or BSD licenses requiring preservation of copyright notices.

This list covers direct dependencies declared in requirements and optional extras at the time of writing. Transitive dependencies bring their own licenses; consult tools like pip-licenses for a complete inventory when producing a distribution bundle.

If you believe additional attribution is required for a dependency, please open an issue.
