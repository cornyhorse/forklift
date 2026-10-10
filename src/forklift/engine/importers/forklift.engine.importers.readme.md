# Forklift Engine Importers

The `forklift.engine.importers` package provides format-specific importers for converting various data sources into Parquet format. This package currently supports Excel files and SQL databases through dedicated importer classes.

## Overview

The importers package contains two main components:

- **ExcelImporter**: Handles Excel file (.xlsx, .xls) import operations with multi-sheet support
- **SqlImporter**: Handles SQL database import operations with ODBC connectivity

Both importers output data in Apache Parquet format for efficient storage and processing, to a local
directory or an `s3://bucket/prefix` URI. They are the public `import_excel()` / `import_sql()`
functions of `forklift`. Neither runs the CSV row-validation pipeline: there is no `bad_rows.parquet`,
no manifest and no output-metadata (`output_data_metadata.json`) file, so `include_value_statistics`
does not apply to them. They do not apply the schema extensions that `import_csv` runs either
(`x-transformations`, `x-columnMapping`, `x-calculatedColumns`, `x-validation`, `x-primaryKey`,
`x-uniqueConstraints`, per-property constraints, `x-rowHash`): nothing under `forklift.engine.importers` or
`forklift.inputs` reads them. Excel and SQL are optional extras (`pip install "forklift-etl[excel]"`,
`pip install "forklift-etl[sql]"`).

## ExcelImporter

The `ExcelImporter` class provides functionality to import Excel files with support for multiple sheets, custom schemas, and various Excel-specific configurations.

### Key Features

- **Multi-sheet processing**: Import all sheets or specific sheets from Excel workbooks
- **Schema validation**: Optional schema-based configuration for precise data extraction
- **Streaming readers**: `.xlsx` is read with openpyxl in read-only mode, legacy `.xls` with xlrd (chosen from the file extension unless `engine` is given)
- **Resource limits**: `.xlsx` archives are checked before they are opened and sheets are read with row/cell caps (see below)
- **Automatic sanitization**: Safe filename generation for output files
- **Flexible sheet selection**: Select sheets by name or index; a name or index that does not exist raises `ValueError`
- **Local input only**: the workbook must be a local file (`FileNotFoundError` otherwise); only the output location may be on S3

### Usage

#### Basic Import (All Sheets)

```python
from forklift.engine.importers import ExcelImporter

results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    values_only=True
)
```

#### Import with Schema

```python
results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    schema_file="config/excel_schema.json",
    engine="openpyxl"
)
```

#### Import Specific Sheet

```python
results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    sheet="Sales Data"  # By name
)

# Or by index
results = ExcelImporter.import_excel(
    input_path="data/workbook.xlsx",
    output_path="output/",
    sheet=0  # First sheet
)
```

### Parameters

- **input_path** (Union[str, Path]): Path to the Excel file to import
- **output_path** (Union[str, Path]): Directory where Parquet files will be saved
- **schema_file** (Union[str, Path], optional): Path to Excel schema configuration file
- **values_only** (bool, optional): Whether to read only cell values (default: True)
- **engine** (str, optional): Excel engine to use (openpyxl, xlrd, etc.)
- **date_system** (str, optional): Excel date system ("1900" or "1904"); the workbook's own setting wins
- **sheet** (Union[str, int], optional): Specific sheet to process (name or index). A string of digits such as `"0"` (what `forklift ingest --sheet 0` passes) is a sheet name if a sheet has that name, otherwise a 0-based index. Ignored when a `schema_file` is given (the schema selects the sheets)
- **s3_client** (optional): Client used when `output_path` is an `s3://` URI

Which rows and cells are read is decided by the sheet settings (from a schema file, or the defaults: the
first row is the header, data follows it, blank rows are skipped). Values of the same sheet column keep
their Excel type (integers, floats, dates, booleans); a column that mixes types is written as text.
Only empty and whitespace-only cells count as null by default (`keep_default_na`): a cell reading
`NA` or `N/A` stays a string unless the schema lists it under `x-excel.nulls`. Formula cells without a
cached value are read as null.

The safety limits live on `ExcelInputConfig` (`max_rows` 1,048,576, `max_cells` 10,000,000,
`max_uncompressed_bytes` 1 GiB, `max_compression_ratio` 200): an `.xlsx` archive beyond the size/ratio
limits is refused before it is parsed, and a sheet beyond the row/cell caps raises `ValueError`. They
are not parameters of `import_excel()`; to change them use `ExcelInputHandler` directly:

```python
from forklift.inputs.config import ExcelInputConfig
from forklift.inputs.excel import ExcelInputHandler

handler = ExcelInputHandler(ExcelInputConfig(max_rows=200_000))
for sheet_name, table in handler.process_sheets("data/workbook.xlsx"):  # yields pyarrow Tables
    print(sheet_name, table.num_rows)
info = handler.get_sheet_info("data/workbook.xlsx")  # {"engine", "sheet_count", "sheet_names"}
```

Row numbering in a sheet configuration: `header.row` is a 0-based offset (`0` is sheet row 1), while
`dataStartRow` and `dataEndRow` are 1-based sheet rows, both inclusive. Data never starts on or before
the header row. `header.mode` is `present` (default), `absent` (columns are `col_1`..`col_N`, or
`header.override`) or `auto` (the row is the header only if it consists of unique, non-numeric labels).

### Output

Each sheet is saved as a separate Parquet file with the naming pattern: `{workbook_name}_{sheet_name}.parquet`

`output_path` may be a local directory or an `s3://bucket/prefix` URI (written through the S3 Parquet
writer; pass it as a `str`). Sheet names are sanitised to plain file names; when sanitising makes two
names collide (`Q1/Q2`, `Q1:Q2`, `Data.` vs `Data`) the later sheets get a `_2`, `_3`, ... suffix in
workbook order, so no sheet overwrites another. A name that is not a plain file name or would resolve
outside the output directory raises `ValueError`.

The method returns a `ProcessingResults` object containing:
- Total rows processed (`valid_rows` equals `total_rows`; `invalid_rows` is always 0 for Excel)
- Processing execution time
- List of output files created
- Any errors encountered

A schema file that fails validation raises `ProcessingError("Schema validation failed: ...")`.

## SqlImporter

The `SqlImporter` class provides functionality to import data from SQL databases using ODBC connectivity, with support for multiple tables and batch processing.

### Key Features

- **ODBC connectivity**: Connect to various SQL databases (SQL Server, PostgreSQL, MySQL, etc.)
- **Schema-driven processing**: Required schema file specifies which tables to import
- **Batch processing**: Configurable batch sizes for memory-efficient processing
- **Streaming support**: Enable streaming for large datasets
- **Connection management**: Automatic connection handling and cleanup
- **Flexible naming**: Custom output names for tables
- **Safe identifiers**: schema/table names are validated against the database catalog and always quoted
- **Read-only access**: connections are opened read-only by default
- **Failure isolation**: a failing table is aborted without leaving a partial file; the others still run

### Usage

#### Basic SQL Import

```python
from forklift.engine.importers import SqlImporter

results = SqlImporter.import_sql(
    connection_string="DRIVER={SQL Server};SERVER=localhost;DATABASE=mydb;UID=user;PWD=pass",
    output_path="output/",
    schema_file="config/sql_schema.json"
)
```

#### Import with Custom Configuration

```python
results = SqlImporter.import_sql(
    connection_string="Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=mydb;Uid=user;Pwd=pass",
    output_path="s3://my-bucket/exports/",
    schema_file="config/sql_schema.json",
    batch_size=50000,
    query_timeout=600,
    enable_streaming=True,
    continue_on_error=True,   # return the results even if some tables failed
)
```

### Parameters

- **connection_string** (str): ODBC connection string for the database
- **output_path** (Union[str, Path]): Directory where Parquet files will be saved
- **schema_file** (Union[str, Path]): **Required** - Path to SQL schema configuration file
- **batch_size** (int, optional): Number of rows per batch (default: 10000)
- **query_timeout** (int, optional): Query timeout in seconds (default: 300)
- **connection_timeout** (int, optional): Connection timeout in seconds (default: 30)
- **use_quoted_identifiers** (bool, optional): Accepted for compatibility only. Identifiers are always validated against the catalog and quoted, whatever this is set to
- **schema_name** (str, optional): Default schema name for tables
- **enable_streaming** (bool, optional): Enable streaming mode (default: True)
- **null_values** (list, optional): Custom null value representations
- **continue_on_error** (bool, optional): Return the results instead of raising when some tables fail (default: False)
- **s3_client** (optional): Client used when `output_path` is an `s3://` URI

`import_sql()` always asks the driver for a read-only connection (`SqlInputConfig.read_only`, default
`True`). A driver that rejects that attribute needs `read_only=False`, which is only available when you
build `SqlInputConfig` yourself and drive `SqlInputHandler` directly (see
`forklift.inputs.sql`). `SqlInputConfig.__repr__` omits the connection string and connection
parameters.

### Output

Each table is saved as a separate Parquet file. The naming convention depends on the schema configuration:
- If `output_name` is specified: `{output_name}.parquet`
- If schema name exists: `{schema_name}_{table_name}.parquet`
- Default: `{table_name}.parquet`

`output_path` may be a local directory or an `s3://bucket/prefix` URI. `outputName` must be a plain file
stem (no path separators) and the resulting file must stay inside the output directory, otherwise
`ValueError` is raised before the database is contacted; two tables mapping to the same file name are
rejected too.

Additionally, a `metadata.json` file is created (in the same local directory or S3 prefix) containing:
- Processing summary (tables processed/failed, row counts, execution time)
- Input configuration details; the connection string is **redacted** (`Pwd=***`, `user:***@host`) via
  `forklift.engine.importers.redact_connection_string`; `tables_processed` lists the tables that succeeded
- List of output files (successful tables only; a table without rows is not listed, although an empty Parquet file with its schema is still written)
- `failed_tables`: schema, table and exception class of each failed table (never data values)

Column types come from the database catalog: `INT` is int32, `BIGINT` int64, `SMALLINT` and `TINYINT`
int16 (SQL Server's `TINYINT` is unsigned), `REAL` float32, `FLOAT`/`DOUBLE` float64 (`FLOAT` is double
precision on SQL Server, SQLite and Oracle), `DECIMAL(p,s)` decimal128 with the column's real
precision and scale (a column wider than 38 digits becomes a string), `MONEY` decimal128(19,4),
`SMALLMONEY` decimal128(10,4), `DATE` date32, `DATETIME`/`TIMESTAMP` timestamp[us], `TIME` time64[us],
binary types binary, and anything else string. An identity suffix such as `int identity` is ignored.

### Table failures

A table that fails is aborted (writer discarded, no partial `.parquet` left, nothing uploaded to S3)
and recorded in `results.errors` as `schema.table: ExceptionClass` (never as `invalid_rows`). The other
tables are still processed. Afterwards a `ProcessingError` is raised (partial results are available as
`error.results`) unless `continue_on_error=True` is passed, in which case the results are returned.

## Error Handling

Both importers implement comprehensive error handling:

- **File validation**: Check for file existence and accessibility
- **Schema validation**: Validate schema files before processing. For SQL this includes the table names (identifier-like only), `outputName` (a plain file stem) and the rule that every table is listed explicitly: a `select` with only a `pattern` is rejected
- **Connection errors**: Handle database connectivity issues
- **Processing errors**: Capture and report data processing failures (class names only; messages from the database/Arrow layer may quote cell values and are not logged)
- **Resource cleanup**: Ensure proper cleanup of connections and file handles

## ProcessingResults

Both importers return a `ProcessingResults` object with the following attributes:

```python
class ProcessingResults:
    total_rows: int          # Total number of rows processed
    valid_rows: int          # Number of successfully processed rows
    invalid_rows: int        # Always 0 for Excel/SQL (failures are in errors, not rows)
    execution_time: float    # Processing time in seconds
    output_files: List[str]  # List of generated output file paths
    errors: List[str]        # Failed tables, as "schema.table: ExceptionClass"
```

The other fields of `ProcessingResults` (`warnings`, `validation_summary`, `schema_extensions`, ...) exist but stay empty here: they are filled by `import_csv`.

## Dependencies

The importers package relies on several core forklift modules:

- `forklift.inputs.excel`: Excel input handling and configuration
- `forklift.inputs.sql`: SQL input handling and configuration  
- `forklift.schema.excel_schema_importer`: Excel schema validation
- `forklift.schema.sql_schema_importer`: SQL schema validation
- `forklift.io`: Parquet writer functionality
- `forklift.engine.config`: Processing configuration and results

## Best Practices

### Excel Import

1. **Use schema files** for complex Excel structures with specific column mappings
2. **Specify sheet selection** when you only need specific sheets to improve performance
3. **Leave the engine on auto-detect** unless the extension does not match the content
4. **Keep values_only enabled** (the default) to read cached formula results

### SQL Import

1. **Always provide a schema file** - it's required and ensures explicit table selection
2. **Tune batch sizes** based on available memory and network conditions
3. **Enable streaming** for large datasets to manage memory usage
4. **Set appropriate timeouts** based on query complexity and network conditions
5. **Use a least-privilege account**: the connection is requested read-only, but the database user should not be able to write anyway

## Example Schema Files

### Excel Schema Example

An Excel schema is a JSON Schema document (`$schema`, `$id` under
`https://github.com/cornyhorse/forklift/schema-standards/`, `title` and `type: object` are required)
with an `x-excel` extension. `x-excel` takes either a `sheets` list, or a single `sheet` (a name or a
0-based index) that is expanded to a one-element list.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/sales-excel.json",
  "title": "Sales workbook",
  "type": "object",
  "properties": {
    "Date": {"type": "string", "format": "date"},
    "Product": {"type": "string"},
    "Amount": {"type": "number"}
  },
  "required": ["Product"],
  "x-excel": {
    "valuesOnly": true,
    "dateSystem": "1900",
    "sheets": [
      {
        "select": {"name": "Sales Data"},
        "header": {"mode": "present", "row": 0},
        "dataStartRow": 2,
        "skipBlankRows": true,
        "nameOverride": "sales",
        "columns": [
          {"name": "Date", "position": "A", "parquetType": "date32"},
          {"name": "Product", "position": "B"},
          {"name": "Amount", "position": "C", "parquetType": "double"}
        ]
      }
    ]
  }
}
```

`header.row` is 0-based (`0` = sheet row 1); `dataStartRow` is the 1-based sheet row where data starts.

### SQL Schema Example

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/crm-sql.json",
  "title": "CRM export",
  "type": "object",
  "x-sql": {
    "tables": [
      {
        "select": {"schema": "dbo", "name": "customers"},
        "outputName": "customer_data"
      },
      {
        "select": {"schema": "dbo", "name": "orders", "columns": ["id", "amount", "created_at"]}
      }
    ]
  }
}
```

`select.schema` and `select.name` must look like identifiers (letters, digits, `_`, space, `.`, `-`,
`$`, `#`, `@`; no quotes, semicolons or comment markers). `outputName` must be a plain file stem
(letters, digits, `_`, `-`, `.`).

`select.columns` (optional) lists the columns to read, in that order; without it every column is
read (`SELECT *`). Use it for a login with column-level grants, or to leave columns out. Column
names may hold any printable character except quotes, semicolons, backslashes and comment
markers; each is matched against the table's catalog columns (exact spelling first, then
ignoring case, so `id` finds Oracle's `ID`) and always quoted, and the Parquet columns carry the
catalog's spelling. When a table without a declaration is refused for a missing privilege, its
`reason` names the columns the login may read and the declaration to add, e.g.
`"select": {"schema": "dbo", "name": "orders", "columns": ["id", "amount"]}`.
