# Forklift API Reference

Complete API documentation for all Forklift functions and classes.

## Table of Contents

- [Import Functions](#import-functions)
- [Reader Functions](#reader-functions)
- [Schema Generation Functions](#schema-generation-functions)
- [Configuration Classes](#configuration-classes)
- [Result Classes](#result-classes)
- [Exception Classes](#exception-classes)

## Import Functions

These functions perform ETL operations, reading data and writing to Parquet files. They are importable from the top-level package (`from forklift import import_csv`).

### `import_csv()`

Import CSV data to Parquet with validation and processing.

```python
def import_csv(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> ProcessingResults
```

**Parameters:**
- `input_path`: Path to CSV file (local or S3 URI)
- `output_path`: Output directory path (local or S3 URI)
- `schema_file`: Optional path to JSON schema file (local or S3 URI)
- `**kwargs`: Any other [`ImportConfig`](#importconfig) field, for example `delimiter`, `encoding`, `header_mode`, `excess_column_mode`, `batch_size`, `footer_detection`, `include_value_statistics`. `header_mode` and `excess_column_mode` accept the enum member or a case-insensitive string (`"absent"`); an unknown value raises `ValueError`

**Returns:** [`ProcessingResults`](#processingresults) object with processing statistics

**What is written to `output_path`:**
- `data.parquet`: the accepted rows (an input with only a header row yields an empty file that carries the schema)
- `bad_rows.parquet`: rejected rows, only when there are some; all columns are strings, in the shape of the input columns
- `manifest.json`, `metadata.json` and `output_data_metadata.json` (see `create_manifest` / `create_metadata`)

Files with these names left by an earlier run are removed when a run starts. If the run fails, the exception is re-raised and no partial `data.parquet` / `bad_rows.parquet` remains.

**How a schema is applied:** the schema's types are applied to the output (`x-csv.parquetTypeMapping` first, otherwise the JSON `type`/`format`), so a `string` column keeps `00123` as written. A row whose value cannot be converted goes to `bad_rows.parquet`. `required` columns are matched by column name; a null or an empty string in one sends the row to `bad_rows.parquet`. Blank lines are skipped. The other `x-` extensions (`x-transformations`, `x-calculatedColumns`, `x-rowHash`, `x-columnMapping`, `x-primaryKey`, ...) are not executed by `import_csv`.

**Example:**
```python
import forklift

results = forklift.import_csv(
    input_path="data.csv",
    output_path="./output/",
    schema_file="schema.json"
)
print(f"Processed {results.total_rows} rows, {results.invalid_rows} rejected")
```

### `import_excel()`

Import Excel data to Parquet. Each sheet becomes `<workbook name>_<sheet name>.parquet`.

```python
def import_excel(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> ProcessingResults
```

**Parameters:**
- `input_path`: Path to a local `.xlsx` or `.xls` file
- `output_path`: Output directory path (local) or `s3://bucket/prefix`
- `schema_file`: Optional path to an Excel schema (`x-excel`); when given it selects the sheets
- `**kwargs`: `sheet` (name or 0-based index; default all sheets), `values_only` (default `True`), `engine` (`"openpyxl"` or `"xlrd"`, detected from the extension), `date_system` (`"1900"` / `"1904"`) and `s3_client`

Sheets are read with openpyxl in read-only streaming mode (`.xlsx`) or xlrd (`.xls`). `.xlsx` archives are checked against size and compression-ratio limits and sheets against row/cell caps before they are read (`ExcelInputConfig`: `max_uncompressed_bytes`, `max_compression_ratio`, `max_rows`, `max_cells`); exceeding one raises `ValueError`. A sheet name or index that does not exist raises `ValueError`. Only empty and whitespace-only cells are null by default. See the [importers readme](../../src/forklift/engine/importers/forklift.engine.importers.readme.md) for the sheet settings (`header.row` is 0-based, `dataStartRow` / `dataEndRow` are 1-based and inclusive).

**Returns:** `ProcessingResults` (`invalid_rows` is always 0; there is no bad rows file, manifest or output-metadata file)

### `import_fwf()`

```python
def import_fwf(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> ProcessingResults
```

Not implemented in the engine yet: it raises `NotImplementedError` (so does `read_fwf()`, and `forklift ingest --input-kind fwf` exits with status 2). The fixed-width parsing classes (`forklift.inputs.fwf`, `forklift.schema.fwf`) can be used directly.

### `import_sql()`

Import tables from a SQL database to Parquet. The schema file (`x-sql`) lists the tables; there is no free-form query.

```python
def import_sql(
    connection_string: str,
    output_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> ProcessingResults
```

**Parameters:**
- `connection_string`: ODBC connection string
- `output_path`: Output directory path (local) or `s3://bucket/prefix`
- `schema_file`: **Required**: JSON schema with an `x-sql.tables` list naming the tables (a missing schema raises `ProcessingError`; a `select` with only a `pattern` is rejected)
- `**kwargs`: `batch_size`, `query_timeout`, `connection_timeout`, `use_quoted_identifiers` (accepted for compatibility; identifiers are always quoted), `schema_name`, `enable_streaming`, `null_values`, `continue_on_error` and `s3_client`

Table and schema names are validated against the database catalog and always quoted, the connection is requested read-only, and the connection string is redacted (`Pwd=***`) in `metadata.json` and in logs. If a table fails, its partial file is removed, the others are still processed, and afterwards a `ProcessingError` is raised with the partial results on `error.results`; pass `continue_on_error=True` to get the results back instead (failed tables are in `results.errors` and under `failed_tables` in `metadata.json`).

**Returns:** `ProcessingResults`

## Reader Functions

These functions run the import pipeline into a temporary directory and return a [`DataFrameReader`](#dataframereader), which converts the result to PyArrow, pandas or polars. They are importable from the top-level package.

### `read_csv()`

```python
def read_csv(
    input_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    encoding: str = "utf-8",
    delimiter: str = ",",
    **kwargs,
) -> DataFrameReader
```

**Parameters:**
- `input_path`: Path to CSV file (local or S3 URI)
- `schema_file`: Optional JSON schema file
- `encoding`: File encoding (default: "utf-8")
- `delimiter`: Field delimiter (default: ",")
- `**kwargs`: Passed to `import_csv()` (any `ImportConfig` field, e.g. `header_mode`)

### `read_excel()`

```python
def read_excel(
    input_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    sheet: Optional[str] = None,
    **kwargs,
) -> DataFrameReader
```

`sheet` is a sheet name (or index); without it every sheet is read and the reader concatenates the per-sheet files, which only works when the sheets have the same columns. `**kwargs` are passed to `import_excel()`.

### `read_fwf()`

```python
def read_fwf(
    input_path: Union[str, Path], schema_file: Union[str, Path], **kwargs
) -> DataFrameReader
```

Raises `NotImplementedError` (see `import_fwf()`).

### `read_sql()`

```python
def read_sql(
    input_path: Union[str, Path],
    schema_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> DataFrameReader
```

`input_path` is the ODBC connection string; `schema_file` lists the tables (see `import_sql()`). `**kwargs` are passed to `import_sql()`.

### `DataFrameReader`

Returned by the `read_*` functions. It owns the temporary Parquet files the run produced.

```python
class DataFrameReader:
    parquet_files: list[str]

    def as_pyarrow(self) -> pyarrow.Table
    def as_pandas(self, **kwargs) -> pandas.DataFrame      # kwargs go to pandas.read_parquet
    def as_polars(self, lazy: bool = False) -> polars.DataFrame | polars.LazyFrame
    def close(self) -> None
    def cleanup(self) -> None
    def __enter__(self) -> DataFrameReader
    def __exit__(self, exc_type, exc_val, exc_tb) -> None
```

- Only the **data files** are read. Rows that were rejected (`bad_rows.parquet`) are not part of the result; use `import_csv()` and read `results.bad_rows_file` if you need them.
- `as_pandas()` and `as_polars()` need the optional `pandas` / `polars` packages (`pip install "forklift-etl[pandas]"`); nothing else in Forklift uses them, and an `ImportError` names the missing package. `as_pyarrow()` needs nothing extra.
- An input with a header but no rows gives an empty frame with the header's columns (typed from the schema where there is one); an input without any rows at all gives an empty frame without columns.
- `close()` (also called when the reader is used as a context manager) deletes the temporary files; afterwards the `as_*` methods raise `ValueError`. Convert before closing: a polars `LazyFrame` reads the files lazily and becomes unusable once they are deleted.
- Without `close()` the files live until the interpreter exits (they are removed by an `atexit` hook). They are deliberately not removed when the reader is garbage collected, because a lazy frame may still be using them.

```python
import forklift

with forklift.read_csv("sales.csv", schema_file="sales_schema.json") as reader:
    table = reader.as_pyarrow()        # always available
    df = reader.as_pandas()            # needs the pandas extra
```

## Schema Generation Functions

Schema generation reads the data with PyArrow only (openpyxl for Excel), streaming local files and `s3://` objects. Only local paths and `s3://` URIs are accepted; `http://`, `ftp://`, `file://` and similar inputs raise `ValueError`. Column types are inferred from the text of the sampled values (CSV), so leading zeros survive (`02134` stays a string), `NA` is not treated as null, `YYYY-MM-DD` columns become `date32` and ISO timestamps become `timestamp`. The result does not depend on `nrows` as long as the sample is representative. A column is only suggested as primary key if it is 100% unique in the sample.

`nrows` is a positive integer or `None` (the whole file or sheet). `0`, negative numbers, booleans and non-integers raise `ValueError`.

### `generate_schema_from_csv()`

Generate JSON schema from CSV file analysis.

```python
def generate_schema_from_csv(
    input_path: Union[str, Path],
    nrows: Optional[int] = None,
    delimiter: str = ",",
    encoding: str = "utf-8",
    include_sample_data: bool = False,
    infer_primary_key_from_metadata: bool = False,
    user_specified_primary_key: Optional[List[str]] = None,
    include_value_statistics: bool = False,
) -> Dict[str, Any]
```

**Parameters:**
- `input_path`: Path to CSV file (local or S3 URI)
- `nrows`: Number of rows to analyze (None = entire file)
- `delimiter`: CSV field delimiter
- `encoding`: File encoding
- `include_sample_data`: Add the first rows as `x-sample` (off by default: it copies cell values)
- `infer_primary_key_from_metadata`: Infer primary key automatically
- `user_specified_primary_key`: Manually specify primary key columns
- `include_value_statistics`: Include the statistics that embed cell values in `x-metadata`: `top_values`, `bottom_values`, `suggested_enum_values`, `min_value`, `max_value`, `median`, `range` and `quantiles`. Off by default because those values can be personal data. Without it `x-metadata` still has counts, null statistics, distinct counts, type information, mean/standard deviation/variance and string-length statistics. Enum *candidates* are still reported, without their values

**Returns:** Dictionary containing generated schema. `x-generation.source_file` and `x-metadata.table_metadata.source_file` hold the file name only, never the directory.

### `generate_schema_from_excel()`

Generate JSON schema from Excel file analysis.

```python
def generate_schema_from_excel(
    input_path: Union[str, Path],
    nrows: Optional[int] = 1000,
    sheet_name: Optional[str] = None,
    include_sample_data: bool = False,
    infer_primary_key_from_metadata: bool = False,
    user_specified_primary_key: Optional[List[str]] = None,
    include_value_statistics: bool = False,
) -> Dict[str, Any]
```

**Parameters:**
- `input_path`: Path to a `.xlsx` file (local or S3 URI); legacy `.xls` raises `ValueError` here
- `nrows`: Number of data rows to analyze (default 1000; `None` = entire sheet)
- `sheet_name`: Sheet name or 0-based index (default: first sheet)
- the remaining parameters are as for `generate_schema_from_csv()`

### `generate_schema_from_parquet()`

Generate JSON schema from Parquet file analysis.

```python
def generate_schema_from_parquet(
    input_path: Union[str, Path],
    nrows: Optional[int] = None,
    include_sample_data: bool = False,
    infer_primary_key_from_metadata: bool = False,
    user_specified_primary_key: Optional[List[str]] = None,
    include_value_statistics: bool = False,
) -> Dict[str, Any]
```

Parameters are as for `generate_schema_from_csv()`. Parquet type strings in the schema are faithful to the file (`decimal128(10,2)`, `timestamp[us, tz=UTC]`, ...).

### `generate_and_save_schema()`

Generate schema and save to file (local path or S3 URI).

```python
def generate_and_save_schema(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    file_type: str,
    nrows: Optional[int] = None,
    **kwargs,
) -> None
```

**Parameters:**
- `input_path`: Path to input data file
- `output_path`: Path for output schema file
- `file_type`: Type of input file ("csv", "excel", "parquet")
- `nrows`: Number of rows to analyze (None = whole file)
- `**kwargs`: Any other `SchemaGenerationConfig` field, e.g. `include_value_statistics=True`, `delimiter`, `sheet_name`

### `generate_and_copy_schema()`

Generate schema and copy to clipboard.

```python
def generate_and_copy_schema(
    input_path: Union[str, Path],
    file_type: str,
    nrows: Optional[int] = None,
    **kwargs,
) -> Dict[str, Any]
```

Returns the generated schema. **Requires:** the `pyperclip` package (`pip install "forklift-etl[clipboard]"`); without it the schema is printed to stdout instead.

### `SchemaGenerationConfig`

Dataclass behind the functions above (`forklift.schema.schema_generator`):

```python
@dataclass
class SchemaGenerationConfig:
    input_path: Union[str, Path]
    file_type: FileType                 # FileType.CSV / EXCEL / PARQUET
    nrows: Optional[int] = 1000         # None = whole file
    output_target: OutputTarget = OutputTarget.STDOUT   # STDOUT / FILE / CLIPBOARD
    output_path: Optional[Union[str, Path]] = None
    delimiter: str = ","
    encoding: str = "utf-8"
    sheet_name: Optional[str] = None
    include_sample_data: bool = False
    user_specified_primary_key: Optional[list] = None
    generate_metadata: bool = True
    metadata_output_path: Optional[Union[str, Path]] = None
    enum_threshold: float = 0.1
    uniqueness_threshold: float = 0.95
    top_n_values: int = 10
    quantiles: Optional[list] = None    # each within 0..1, otherwise ValueError
    infer_primary_key_from_metadata: bool = False
    include_value_statistics: bool = False
```

Note that the dataclass default for `nrows` is 1000, whereas `generate_schema_from_csv()` / `generate_schema_from_parquet()` and the CLI default to the whole file.

## Configuration Classes

### `ImportConfig`

Configuration for CSV import operations (`forklift.engine.config.ImportConfig`). `import_csv()` builds one from its keyword arguments.

```python
@dataclass
class ImportConfig:
    input_path: Union[str, Path]
    output_path: Union[str, Path]
    schema_file: Optional[Union[str, Path]] = None
    batch_size: int = 10000
    encoding: str = "utf-8"
    header_mode: HeaderMode = HeaderMode.PRESENT
    header_search_rows: int = 10
    skip_blank_lines: bool = True
    comment_rows: Optional[List[str]] = None
    footer_detection: Optional[Dict[str, Any]] = None
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: Optional[str] = None
    excess_column_mode: ExcessColumnMode = ExcessColumnMode.TRUNCATE
    validate_schema: bool = True
    max_validation_errors: int = 1000
    create_manifest: bool = True
    create_metadata: bool = True
    compression: str = "snappy"
    include_value_statistics: bool = False
```

**Attributes:**
- `batch_size`: Upper bound on rows per batch written. The local reader produces batches of about 1 MiB which are only split down to this size, so smaller batches can occur
- `header_mode`: Enum member or case-insensitive string (`"present"`, `"absent"`, `"auto"`)
- `header_search_rows`: Rows scanned for the header; no header inside the window raises `ValueError`
- `skip_blank_lines`: Only used while locating the header; empty lines in the data are always skipped
- `comment_rows`: Regex patterns for comment rows **above the header**. With `None` only a row consisting of a single `#...` cell is a comment (`#,name,amount` is a header); `[]` disables comment detection
- `footer_detection`: `{"stop_on_blank": True}` and/or `{"column_index": 0, "patterns": ["^Total"]}`
- `excess_column_mode`: Enum member or string. `TRUNCATE` cuts rows with more fields than the header (counted in `truncated_rows`), `REJECT` sends them to `bad_rows.parquet`, `PASSTHROUGH` keeps the extra fields as `col_N` columns (only possible before the first batch is written, otherwise `ValueError`)
- `validate_schema`: Enforce the schema's `required` columns
- `max_validation_errors`: Reserved; currently not enforced
- `create_manifest` / `create_metadata`: Write `manifest.json` / `metadata.json` and `output_data_metadata.json`
- `compression`: Parquet compression codec
- `include_value_statistics`: Let `output_data_metadata.json` contain statistics that expose cell values (`top_values`, numeric/temporal `min_value`/`max_value`, `median`, `mode`, `quantiles`). Off by default

### `HeaderMode`

Enum for header handling modes.

```python
class HeaderMode(Enum):
    PRESENT = "present"    # File has header row
    ABSENT = "absent"      # No header: schema names, or col_1..col_N
    AUTO = "auto"          # Auto-detect which row is the header
```

### `ExcessColumnMode`

Enum for rows that have more fields than the header.

```python
class ExcessColumnMode(Enum):
    TRUNCATE = "truncate"        # Cut to the header width, keep the row (default)
    REJECT = "reject"            # Write the row to bad_rows.parquet
    PASSTHROUGH = "passthrough"  # Keep every field, name extras col_N
```

### `SqlInputConfig` and `ExcelInputConfig`

Lower-level configuration for `forklift.inputs.sql.SqlInputHandler` and `forklift.inputs.excel.ExcelInputHandler` (`forklift.inputs.config`). Notable fields:

- `SqlInputConfig`: `connection_string`, `batch_size`, `query_timeout`, `connection_timeout`, `read_only` (default `True`), `null_values`, `use_quoted_identifiers` (compatibility only). `connection_string` and `connection_params` are left out of `repr()`
- `ExcelInputConfig`: `sheets`, `values_only`, `date_system`, `nulls`, `keep_default_na`, `na_values`, `engine`, and the resource limits `max_rows` (1,048,576), `max_cells` (10,000,000), `max_uncompressed_bytes` (1 GiB) and `max_compression_ratio` (200)

### `ConstraintConfig`

Configuration for the constraint validator class (`forklift.processors.constraint_validator`). It is used by calling the validator directly; `import_csv()` does not take it.

```python
@dataclass
class ConstraintConfig:
    error_mode: ErrorMode = ErrorMode.BAD_ROWS   # FAIL_FAST / FAIL_COMPLETE / BAD_ROWS
    check_constraints: Dict[str, Any] = None
    unique_constraints: List[str] = None
    foreign_key_constraints: Dict[str, Any] = None
```

## Result Classes

### `ProcessingResults`

Result object returned by the import functions (`forklift.engine.config.ProcessingResults`).

```python
@dataclass
class ProcessingResults:
    total_rows: int = 0               # Rows processed (valid + invalid)
    valid_rows: int = 0               # Rows written to the data file
    invalid_rows: int = 0             # Rows rejected (see bad_rows_file)
    output_files: List[str] = []      # Generated data files; CSV also lists bad_rows.parquet here
    manifest_file: Optional[str] = None
    metadata_file: Optional[str] = None
    execution_time: float = 0.0       # Seconds
    errors: List[str] = []            # Error messages
    bad_rows_file: Optional[str] = None   # Rejected rows (None if nothing was rejected)
    truncated_rows: int = 0           # Rows cut to the header width (TRUNCATE)
```

`errors` also records a failure to write `output_data_metadata.json`: the data files are already complete at that point, so the run does not raise. For `import_sql(..., continue_on_error=True)` it lists the failed tables.

## Exception Classes

Forklift raises standard exceptions (`ValueError` for invalid configuration or input, `FileNotFoundError`, `ImportError` for missing optional packages) plus a few of its own:

### `ProcessingError`

`forklift.engine.exceptions.ProcessingError` (also importable from `forklift.engine`). Raised by the Excel and SQL importers for invalid schemas and failed tables. `import_sql` attaches the partial `ProcessingResults` as `error.results`.

### `SchemaValidationError`

Raised when a schema file does not follow the schema standard. Every problem found is collected into one message, each with its location (for example `Sheet 0 column 1 invalid type 'foo'`). Each schema importer defines its own class: `forklift.schema.csv_schema_importer`, `forklift.schema.excel_schema_importer`, `forklift.schema.sql_schema_importer` and `forklift.schema.fwf.exceptions` (also re-exported by `forklift.schema.fwf_schema_importer`), so catch the one that belongs to the importer you call, or `Exception`.

### `MetadataWriteError`

`forklift.metadata.MetadataWriteError` (a `RuntimeError`). Raised by `OutputMetadataCollector.save_metadata()` when the metadata file cannot be serialised or written. The CSV processor catches it and records the message in `ProcessingResults.errors`.

## Usage Examples

### Complete Import Workflow

```python
import forklift

results = forklift.import_csv(
    input_path="large_dataset.csv",
    output_path="./output/",
    schema_file="schema.json",
    batch_size=50000,
    header_mode="auto",
    excess_column_mode="reject",
)

# Check results
print("Import completed:")
print(f"  Total rows: {results.total_rows}")
print(f"  Valid rows: {results.valid_rows}")
print(f"  Invalid rows: {results.invalid_rows}")
print(f"  Processing time: {results.execution_time:.2f}s")
print(f"  Output files: {results.output_files}")
if results.bad_rows_file:
    print(f"  Rejected rows: {results.bad_rows_file}")
for message in results.errors:
    print(f"  Warning: {message}")
```

### Schema Generation with Options

```python
import forklift

# Generate comprehensive schema
schema = forklift.generate_schema_from_csv(
    "customer_data.csv",
    nrows=10000,  # Analyze first 10k rows
    include_sample_data=False,  # No sample rows (the default)
    infer_primary_key_from_metadata=True
)

# Save with metadata; add include_value_statistics=True to also record top values,
# min/max and quantiles (they copy cell values into the schema)
forklift.generate_and_save_schema(
    input_path="customer_data.csv",
    output_path="customer_schema.json",
    file_type="csv",
    include_sample_data=False,
    infer_primary_key_from_metadata=True
)
```

This API reference provides documentation for the public Forklift functions and classes. For usage examples and workflows, see the [Usage Guide](../guides/USAGE.md).
