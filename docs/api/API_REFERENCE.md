# Forklift API Reference

Complete API documentation for all Forklift functions and classes.

## Table of Contents

- [Import Functions](#import-functions)
- [Running Jobs](#running-jobs)
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
    *,
    progress: Optional[Callable[[Dict[str, int]], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
    **kwargs,
) -> ProcessingResults
```

**Parameters:**
- `input_path`: Path to CSV file (local or S3 URI)
- `output_path`: Output directory path (local or S3 URI)
- `schema_file`: Optional path to JSON schema file (local or S3 URI)
- `progress`: Called after every batch with `{"rows_read", "rows_rejected", "bytes_read"}` (rows read so far, rows rejected so far, bytes of the input consumed so far; `bytes_read` is 0 for `s3://` inputs)
- `cancel`: Asked after every batch; returning True raises [`ImportCancelled`](#importcancelled-and-limitexceedederror) and leaves no output behind
- `**kwargs`: Any other [`ImportConfig`](#importconfig) field, for example `delimiter`, `encoding`, `header_mode`, `excess_column_mode`, `batch_size`, `footer_detection`, `include_value_statistics`, `apply_schema_extensions`, `s3_client` (a `forklift.io.S3StreamingClient` for `s3://` inputs, schemas and outputs, for example one with `endpoint_url=` for an S3-compatible store). `header_mode` and `excess_column_mode` accept the enum member or a case-insensitive string (`"absent"`); an unknown value raises `ValueError`

**Returns:** [`ProcessingResults`](#processingresults) object with processing statistics

**What is written to `output_path`:**
- `data.parquet`: the accepted rows (an input with only a header row yields an empty file that carries the schema)
- `bad_rows.parquet`: rejected rows, only when there are some; all columns are strings, in the shape of the input columns. When the schema configures `x-validation` or a constraint it has an extra last column `_rejection_reason` (`CODE` or `CODE:column`, several joined by `; `, never a cell value; rows rejected by type conversion, `required` or excess fields get `type_conversion_failed`, `required_value_missing` or `too_many_fields`)
- `manifest.json`, `metadata.json` (which also records `schema_extensions`, `validation_summary` and `warnings`) and `output_data_metadata.json` (see `create_manifest` / `create_metadata`)

Files with these names left by an earlier run are removed when a run starts. If the run fails, the exception is re-raised and no partial `data.parquet` / `bad_rows.parquet` remains (except after `BadRowsThresholdExceededError`, which keeps a finished `bad_rows.parquet`, see below). Where the engine knows what went wrong the exception carries an [`error_code`](#error-codes).

**How a schema is applied:** the schema's types are applied to the output (`x-csv.parquetTypeMapping` first, otherwise the JSON `type`/`format`), so a `string` column keeps `00123` as written. A row whose value cannot be converted goes to `bad_rows.parquet`. `required` columns are matched by column name; a null or an empty string in one sends the row to `bad_rows.parquet`. Blank lines are skipped.

**Schema extensions:** unless `apply_schema_extensions=False`, `import_csv` also runs `x-transformations` (and the automatic `x-special-type` formatting), `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality` (findings only), `x-validation`, `x-primaryKey`, `x-uniqueConstraints`, the per-property constraints (`minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `enum`, `x-unique`), `x-constraintHandling.errorMode` and `x-rowHash` on every batch. They apply to CSV only: `import_excel`, `import_sql` and `import_fwf` apply none, and `x-pii` is not acted on. Stage order, the supported keys and a worked example are in [Applying Schema Extensions](../guides/USAGE.md#applying-schema-extensions); the key tables are in [docs/schemas](../schemas/README.md). Extension content that no processor reads is returned in `ProcessingResults.warnings`; an extension that is configured incorrectly or names a column that does not exist raises `ValueError` before any output is written (the guide has the exact rules).

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
- `**kwargs`: `sheet` (name or 0-based index; default all sheets), `values_only` (default `True`), `engine` (`"openpyxl"` or `"xlrd"`, detected from the extension), `date_system` (`"1900"` / `"1904"`), `s3_client`, and `progress` / `cancel` (called after every sheet, as for [`import_csv`](#import_csv); a cancelled import removes the sheets it had written)

Sheets are read with openpyxl in read-only streaming mode (`.xlsx`) or xlrd (`.xls`). `.xlsx` archives are checked against size and compression-ratio limits and sheets against row/cell caps before they are read (`ExcelInputConfig`: `max_uncompressed_bytes`, `max_compression_ratio`, `max_rows`, `max_cells`); exceeding one raises `ValueError`. A sheet name or index that does not exist raises `ValueError`. Only empty and whitespace-only cells are null by default. See the [importers readme](../../src/forklift/engine/importers/forklift.engine.importers.readme.md) for the sheet settings (`header.row` is 0-based, `dataStartRow` / `dataEndRow` are 1-based and inclusive).

**Returns:** `ProcessingResults` (`invalid_rows` is always 0; there is no bad rows file, manifest or output-metadata file). The schema's `x-...` extensions that `import_csv` runs (transformations, mapping, validation, constraints, ...) are not applied here, and `schema_extensions`, `validation_summary` and `warnings` stay empty

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
- `schema_file`: **Required**: JSON schema with an `x-sql.tables` list naming the tables (a missing schema raises `ProcessingError`; a `select` with only a `pattern` is rejected). A table's `select.columns` lists the columns to read, in order (for logins with column-level grants); without it every column is read
- `**kwargs`: `batch_size`, `query_timeout`, `connection_timeout`, `use_quoted_identifiers` (accepted for compatibility; identifiers are always quoted), `schema_name`, `enable_streaming`, `null_values`, `continue_on_error`, `s3_client`, and `progress` / `cancel` (called after every batch, as for [`import_csv`](#import_csv); a cancelled import stops at once and keeps none of its tables)

Table, schema and column names are validated against the database catalog and always quoted, the session is made read-only where the database allows it (PostgreSQL, MySQL/MariaDB and SQLite sessions; Oracle transactions; SQL Server cannot, and a warning says so), and the connection string is redacted (`Pwd=***`) in `metadata.json` and in logs. A table refused for a missing privilege fails with a reason that names the columns the login may read and the `select.columns` declaration that imports them. If a table fails, its partial file is removed, the others are still processed, and afterwards a `ProcessingError` is raised with the partial results on `error.results`; pass `continue_on_error=True` to get the results back instead (failed tables are in `results.errors` and under `failed_tables` in `metadata.json`).

**Returns:** `ProcessingResults`

## Running Jobs

A job spec runs the engine declaratively; it is the code path of `forklift run-job` and of the service's workers. The contract is published as JSON Schema in `contracts/jobspec.schema.json` and `contracts/jobresult.schema.json`; the [jobs package readme](../../src/forklift/jobs/forklift.jobs.readme.md) documents every field, kind and artifact.

### `run_job()`

```python
def run_job(
    spec: Union[JobSpec, Mapping[str, Any]],
    *,
    base_dir: Union[str, Path],
    allowed_url_hosts: Iterable[str] = (),
    progress: Optional[Callable[[Dict[str, int]], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
    s3_client: Optional[S3StreamingClient] = None,
) -> JobResult
```

Importable as `forklift.run_job` and `forklift.jobs.run_job`.

**Parameters:**
- `spec`: A `JobSpec`, or its JSON form as a dict (checked first; an invalid one is a `failed` result with code `SPEC_INVALID`)
- `base_dir`: Directory every `file` location is relative to; no `file` location may lead out of it
- `allowed_url_hosts`: Hosts (or `host:port`) a `presigned_url` input may point at (default: none)
- `progress`: Called at every batch boundary with `{"rows_read", "rows_rejected", "bytes_read"}`, plus `"rows_written"` while a `sql_table` output is loaded
- `cancel`: Asked after every batch; True ends the job with `status: "cancelled"`
- `s3_client`: Client for `s3` locations (default: boto3's credential chain)

**Returns:** a [`JobResult`](#jobspec-and-jobresult). Job failures are results, never exceptions; only wrong arguments raise (`ValueError` when `base_dir` is not a directory or `allowed_url_hosts` is a string, `TypeError` for a callback that is not callable).

**Kinds:** `run` (CSV, Excel and SQL inputs; `fwf` gives `SPEC_INVALID` because fixed-width import is not implemented), `preview` (CSV, Excel; `preview.json`), `validate_schema` (CSV; `report.json`) and `generate_schema` (CSV, Excel; `schema.json`).

```python
from forklift import run_job

result = run_job(
    {
        "spec_version": 1,
        "job_id": "check-1",
        "kind": "validate_schema",
        "input": {"format": "csv", "location": {"type": "file", "path": "people.csv"}},
        "schema": {"properties": {"id": {"type": "integer"}}, "required": ["id"]},
        "options": {"sample_rows": 500},
    },
    base_dir="./data",
)
print(result.status, result.warnings, [a.path for a in result.artifacts])  # out/report.json
```

### `JobSpec` and `JobResult`

`forklift.jobs.JobSpec` and `forklift.jobs.JobResult` are plain dataclasses (also exported from `forklift`).

- `JobSpec.from_dict(document)` checks a document against the contract and builds the spec; it raises `forklift.jobs.ContractError` (a `ValueError`) listing every problem with the path of its field, for example `input.options.delimeter: unknown field (did you mean 'delimiter'?)`. `JobResult.from_dict()` works the same way.
- `to_dict()` returns the JSON form (optional fields that are unset are left out). `repr()` hides connection strings and presigned URLs.
- Top-level spec fields: `spec_version` (1), `job_id`, `kind`, `input` (`format`, `location`, `options`), `schema` (inline), `output` (`location`, `compression`, `artifacts`), `options` (`apply_schema_extensions`, `batch_size`, `include_value_statistics`, `preview_rows`, `preview_max_bytes`, `sample_rows`, `infer_primary_key`) and `limits` (`max_input_bytes`, `max_seconds`, `max_rows`).
- Locations: `file` (`path`), `s3` (`uri`), `presigned_url` (`url`, `size`, `etag`; CSV inputs only), `sql` (`connection_string`), `sql_table` (`connection_string`, `table`, `schema_name`, `mode`, `key_columns`, `staging`).
- Result fields: `spec_version`, `job_id`, `status` (`succeeded`, `failed`, `cancelled`), `counts`, `schema_extensions`, `validation_summary`, `warnings`, `artifacts` (`kind`, `path`, `rows`, `bytes`, `sha256`) and `error` (`code`, `message`, `retryable`).

`python -m forklift.jobs.contract` regenerates the JSON Schemas from the dataclasses (`--check` exits with 1 when the checked-in files are out of date).

### Error codes

`forklift.engine.exceptions.ERROR_CODES` (also `forklift.jobs.ERROR_CODES`): `SPEC_INVALID`, `SCHEMA_INVALID`, `INPUT_UNREADABLE`, `ENCODING_ERROR`, `COLUMN_MISSING`, `BAD_ROWS_THRESHOLD_EXCEEDED`, `CONSTRAINT_VIOLATION`, `LIMIT_EXCEEDED`, `PERMISSION_DENIED`, `TARGET_WRITE_FAILED`, `CANCELLED`, `INTERNAL`.

Exceptions the import functions raise carry an `error_code` attribute with one of these codes where the engine knows what went wrong (a schema that cannot be loaded, a missing header, a required column that is not in the input, undecodable bytes, a violated `fail_fast` constraint, failed SQL tables, ...); the type and message of the exception are unchanged. `run_job` maps every other exception by its type.

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
    apply_schema_extensions: bool = True
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
- `apply_schema_extensions`: Run the schema's `x-...` extensions (`x-transformations`, `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality`, `x-validation`, `x-primaryKey`, `x-uniqueConstraints`, per-property constraints, `x-constraintHandling`, `x-rowHash`) on every batch. On by default; `False` (CLI: `--no-schema-extensions`) ignores them, while types, null markers and `required` still apply. CSV imports only

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

Configuration for the constraint validator class (`forklift.processors.constraint_validator`). You use it by calling the validator directly. `import_csv()` takes no `ConstraintConfig`: it builds its validator from the schema (`x-primaryKey`, `x-uniqueConstraints`, the per-property constraints and `x-constraintHandling.errorMode`) with `forklift.processors.schema_extensions.build_constraint_validator`.

```python
@dataclass
class ConstraintConfig:
    error_mode: ErrorMode = ErrorMode.BAD_ROWS   # FAIL_FAST / FAIL_COMPLETE / BAD_ROWS
    check_constraints: Dict[str, Any] = None
    unique_constraints: List[str] = None
    foreign_key_constraints: Dict[str, Any] = None
    max_retained_violations: Optional[int] = 1000  # None = keep every violation
    include_values: bool = False                  # keep offending cell values in violations
```

`error_mode` also accepts the strings `"bad_rows"`, `"fail_fast"` and `"fail_complete"` (case-insensitive); anything else raises `ValueError`.

**Memory use on large inputs.** The validator keeps an exact running total (`violation_count`; `finalize()` and the `fail_complete` mode use it) but retains at most `max_retained_violations` `ConstraintViolation` objects in `violations` / `get_all_violations()` (the first ones; `violations_truncated` says whether some were left out). `batch_violations` always holds every violation of the last batch, and `bad_rows` mode drops every violating row whatever the limit. Retained violations carry an empty `values` list unless `include_values=True` (cell values may be personal data). The state that checks `unique_constraints` is inherently proportional to the number of distinct keys seen so far (one entry per distinct key per constraint), so memory grows with the number of distinct keys, not with the number of rows or violations.

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
    warnings: List[str] = []          # Notes that do not stop the import (ignored schema content, ...)
    validation_summary: Dict[str, int] = {}   # Findings of the schema extensions per CODE or CODE:column
    schema_extensions: List[str] = [] # Extensions that were applied
```

`warnings`, `validation_summary` and `schema_extensions` are filled by `import_csv` when the schema has extensions to apply. `validation_summary` counts rejected rows, values nulled by `x-special-type`, `x-dataQuality` findings and so on, per `CODE` or `CODE:column` (for example `UNIQUE_VIOLATION:id`, `INVALID_SPECIAL_VALUE:ssn`); it never contains cell values. The CLI prints all three, and `metadata.json` records them.

`errors` also records a failure to write `output_data_metadata.json`: the data files are already complete at that point, so the run does not raise. For `import_sql(..., continue_on_error=True)` it lists the failed tables.

`results.to_dict()` returns every field as plain JSON values (copies of the lists and dicts); `ProcessingResults(**results.to_dict())` rebuilds the object.

## Exception Classes

Forklift raises standard exceptions (`ValueError` for invalid configuration or input, `FileNotFoundError`, `ImportError` for missing optional packages) plus a few of its own:

### `ProcessingError`

`forklift.engine.exceptions.ProcessingError` (also importable from `forklift.engine`). Raised by the Excel and SQL importers for invalid schemas and failed tables. `import_sql` attaches the partial `ProcessingResults` as `error.results`.

### `ImportCancelled` and `LimitExceededError`

`forklift.engine.exceptions.ImportCancelled` (also `forklift.ImportCancelled`) is raised when the `cancel` callback of `import_csv`, `import_excel` or `import_sql` returns True; `LimitExceededError` is raised by `run_job` from its progress callback when a `limits` value is exceeded. Both are `ImportInterrupted` (a `ProcessingError`) and stop the whole import at once: no output of it is kept (the SQL importer does not go on with its other tables, the Excel importer removes the sheets it had written). Their `error_code` is `CANCELLED` and `LIMIT_EXCEEDED`.

### Errors from the schema extensions

`import_csv` raises `ValueError` for an extension that is configured incorrectly (wrong type, unknown `errorMode`, invalid regular expression, a column name that exists neither in the file nor in `properties`, a calculated or hash column that would replace an existing column, ...) before any output is written, and for a violation under `x-constraintHandling.errorMode` `fail_fast` / `fail_complete`. `BadRowsThresholdExceededError` (`forklift.processors.data_validation.data_validation_processor`, a `RuntimeError`) is raised when `x-validation` rejects more than `badRowsHandling.maxBadRowsPercent` of the rows that reached it and `failOnExceedThreshold` is true; by default the verdict is given after the whole input was checked (`badRowsHandling.thresholdMode`: `end_of_file`, or `early` to stop at the first batch over the limit), and the message lists the findings by rule. In all these cases no `data.parquet` is left behind, and neither is `bad_rows.parquet` except after `BadRowsThresholdExceededError`: that error keeps `bad_rows.parquet` (finished and readable) so that the rejected rows can be inspected, and exposes its path as `error.bad_rows_file` (`error.whole_input_checked` says whether every row was checked; with `early` the file holds only the rows rejected before the import stopped).

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
