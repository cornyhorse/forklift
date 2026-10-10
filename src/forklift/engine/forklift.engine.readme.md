# Forklift Engine

## Overview

The **Forklift Engine** is a high-performance data processing framework designed for streaming import, validation, and transformation of various data formats. Built around Apache PyArrow for memory-efficient processing, Forklift provides a unified interface for importing data from CSV, Excel, and SQL sources with comprehensive validation, error handling, and metadata generation.

## Core Purpose

Forklift Engine serves as the central orchestration layer for data import operations, providing:

- **Streaming Data Processing**: Memory-efficient handling of large datasets using PyArrow streaming
- **Multi-Format Support**: Unified API for CSV, Excel, and SQL data sources
- **Schema Validation**: Comprehensive data validation against JSON schemas
- **Schema Extensions (CSV)**: The schema's `x-...` extensions (transformations, column mapping, calculated columns, validation, primary/unique keys, constraints, row hashes) are run on every batch
- **Error Handling**: Separation of valid and invalid data with detailed error reporting
- **Cloud Integration**: Native support for S3 input/output with streaming capabilities
- **Metadata Generation**: Automatic creation of manifests, metadata, and processing reports

## Architecture

The Forklift Engine follows a modular architecture with distinct responsibilities:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   forklift_core │───▶│   Importers     │───▶│   Processors    │
│   (Orchestrator)│    │  (Data Sources) │    │ (Transformation)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Config       │    │   Excel/SQL     │    │   Validation    │
│  (Settings)     │    │   Importers     │    │   & Output      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Core Components

#### 1. **forklift_core.py** - The Engine Orchestrator

The `ForkliftCore` class serves as the central orchestration engine that:

- **Coordinates Processing Workflow**: Manages the end-to-end data import pipeline
- **Provides Unified API**: Offers consistent interface across different data formats
- **Handles Configuration**: Integrates with the configuration system for flexible processing
- **Manages Resources**: Ensures proper initialization and cleanup of processing components

**Key Responsibilities:**
```python
class ForkliftCore:
    def __init__(self, config: ImportConfig, *, progress=None, cancel=None, input_source=None)
    def process_csv(self) -> ProcessingResults
```

The core engine delegates format-specific processing to specialized components while maintaining consistent error handling and result reporting across all data sources.

#### 2. **Public API Functions**

The engine exposes high-level functions for different data formats:

- **`import_csv()`**: Streaming CSV processing with PyArrow (schema typing, bad rows, manifest and metadata)
- **`import_excel()`**: Multi-sheet Excel file processing (one Parquet file per sheet; local input, local or S3 output)
- **`import_sql()`**: Database import with ODBC connectivity (one Parquet file per table listed in the schema file)
- **`import_fwf()`**: Fixed-width file processing (not implemented in the engine yet: it raises `NotImplementedError`, and `forklift ingest --input-kind fwf` exits with status 2)

Each function provides a simplified interface while supporting advanced configuration through keyword arguments.
They are exported from the top-level package (`from forklift import import_csv, import_excel, import_sql`).

#### 3. **Seams for jobs and services**

These small modules are what `forklift.jobs` (`run_job`, `forklift run-job`) builds on; library users can use them directly too.

- **`progress.py`**: `ImportHooks`. Every importer takes `progress=` and `cancel=`: `progress({"rows_read", "rows_rejected", "bytes_read"})` is called at every batch boundary (per batch for CSV and SQL, per sheet for Excel) and `cancel()` right after it; True raises `ImportCancelled` and the import keeps no output (SQL does not go on with its other tables, Excel removes the sheets it had written). A progress callback may raise an `ImportInterrupted` (such as `LimitExceededError`) to stop the import the same way.
- **`exceptions.py`**: the stable error codes (`ERROR_CODES`, the job contract's codes) and `with_error_code()`. The importers set `error_code` on the exceptions they understand: `SCHEMA_INVALID` (schema cannot be loaded, an extension is misconfigured), `INPUT_UNREADABLE` (no header row, a row too wide for `passthrough`, failed SQL tables), `ENCODING_ERROR`, `COLUMN_MISSING` (a required or referenced column is not in the input), `CONSTRAINT_VIOLATION` (`fail_fast` / `fail_complete`), `PERMISSION_DENIED` (SQL tables refused for privileges), `CANCELLED`, `LIMIT_EXCEEDED`. The type and the message of each exception stay as they were.
- **`input_source.py`**: `InputSource`, a CSV input read as forward-only byte streams instead of by path (`ForkliftCore(config, input_source=...)`; `config.input_path` then only names the input). Header detection reads it through `open_head()`, the main read and the row reader for ragged rows each open a new stream, and footer detection uses the row reader, so no copy is written. `forklift.jobs` streams presigned URLs this way; `import_csv` never turns a URL string into a source. `CountingReader` counts the bytes behind `bytes_read`.
- **`ImportConfig.s3_client`** and **`ProcessingResults.to_dict()`**: an explicit S3 client for `import_csv` (for S3-compatible stores), and the results as plain JSON values.

## Processing Pipeline

### 1. **Initialization Phase**
```
Input Configuration → Validation → Component Setup → Resource Allocation
```

- Configuration validation and setup
- Schema loading and parsing (if provided)
- Processor component initialization
- Output directory preparation

### 2. **Data Import Phase**
```
Source Detection → Format-Specific Import → Streaming Setup → Header Detection
```

- **CSV**: PyArrow streaming reader with configurable batching
- **Excel**: Multi-sheet processing with workbook optimization
- **SQL**: ODBC streaming with batch fetching

### 3. **Processing Phase**
```
Batch Processing → Schema Validation → Schema Extensions (CSV) → Error Separation → Output Generation
```

- Stream data in configurable batches for memory efficiency
- Apply schema validation to each batch (types, null markers, `required`)
- CSV only: run the schema extensions (see [Schema extensions](#schema-extensions-csv))
- Separate valid and invalid data into different output streams
- Generate Parquet files with compression

### 4. **Finalization Phase**
```
Metadata Generation → Manifest Creation → Resource Cleanup → Results Reporting
```

- Create comprehensive metadata about processed data
- Generate file manifests for output tracking
- Clean up temporary resources and connections
- Return detailed processing results

## Key Features

### Streaming Architecture

Forklift uses PyArrow's streaming capabilities to process large datasets efficiently:

```python
# Memory-efficient processing of large files
results = import_csv(
    input_path="large_dataset.csv",
    output_path="output/",
    batch_size=50000  # Process in 50K row batches
)
```

### Schema-Driven Validation

Schema types are applied to the output (`string` columns keep their text, so `00123` stays `00123`).
A row that has a value which cannot be converted, or an empty/null value in a `required` column
(matched by column name), is written to `bad_rows.parquet` (all-string columns in the shape of the
input) instead of stopping the run:

```python
# Schema validation with error separation
results = import_csv(
    input_path="data.csv",
    output_path="output/",
    schema_file="validation_schema.json",
)

# Access validation results
print(f"Valid rows: {results.valid_rows}")
print(f"Invalid rows: {results.invalid_rows}")
print(f"Bad rows file: {results.bad_rows_file}")  # None when nothing was rejected
```

### Schema Extensions (CSV)

`import_csv` (`ForkliftCore.process_csv`) also runs the `x-...` extensions of the schema. `CSVProcessor` builds an
`ExtensionPipeline` (`processors/extensions.py`) from the schema and the file's header before any output is
written, and runs it on every batch. Excel, SQL and fixed-width imports do not use it. Per batch:

```
raw text from the reader
  PRE   (header names)  x-rowHash input hash / row numbers (hidden columns, only if asked for)
                        x-csv null markers -> NULL
                        x-transformations + automatic x-special-type formatting
  engine                type conversion (properties, x-csv.parquetTypeMapping)  -> bad_rows
                        required                                                -> bad_rows
  POST  (output names)  x-columnMapping (renames) -> x-calculatedColumns (appends) -> x-dataQuality (findings only)
                        -> x-validation (rows -> bad_rows) -> constraints (x-primaryKey, x-uniqueConstraints,
                        per-property constraints, x-constraintHandling.errorMode; rows -> bad_rows)
                        -> x-rowHash (hash and metadata columns, last)
```

`properties`, `required`, `x-csv` and `x-transformations` use the header names of the file; every stage after
`x-columnMapping` uses the output names. `bad_rows.parquet` keeps the input's column names and gains a last
`_rejection_reason` column when `x-validation` or any constraint is configured. The pipeline's findings
come back on `ProcessingResults` (`schema_extensions`, `validation_summary`, `warnings`).
`ImportConfig(apply_schema_extensions=False)` switches the whole pipeline off. Misconfiguration (an invalid
extension, a column that does not exist, a calculated column that would replace an existing one) raises
`ValueError` before any output is written. The supported keys, error modes and a worked example are in the
[usage guide](../../../docs/guides/USAGE.md#applying-schema-extensions); the processors are described in the
[processors readme](processors/forklift.engine.processors.readme.md) and the
[extension loaders readme](../processors/forklift.processors.readme.md).

### Multi-Format Support

Unified interface across different data sources:

```python
# CSV processing
csv_results = import_csv("data.csv", "output/")

# Excel processing with multi-sheet support
excel_results = import_excel("workbook.xlsx", "output/")

# SQL database import (ODBC connection string; the schema file lists the tables)
sql_results = import_sql(
    connection_string="DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;...",
    output_path="output/",
    schema_file="sql_schema.json"
)
```

### Cloud Integration

Native S3 support for input and output operations:

```python
# S3 to S3 processing
results = import_csv(
    input_path="s3://input-bucket/data.csv",
    output_path="s3://output-bucket/processed/",
    schema_file="s3://config-bucket/schema.json"
)
```

### Advanced Configuration

Flexible configuration system supporting various processing modes:

```python
# Advanced CSV configuration
from forklift.engine import HeaderMode
from forklift.engine.config import ExcessColumnMode

results = import_csv(
    input_path="complex.csv",
    output_path="output/",
    delimiter="|",
    encoding="latin-1",
    header_mode=HeaderMode.AUTO,                  # or "auto"
    excess_column_mode=ExcessColumnMode.REJECT,   # or "reject"
    footer_detection={"column_index": 0, "patterns": ["^Total:", "^Summary:"]},
    compression="gzip"
)
```

## Configuration System

The engine uses a comprehensive configuration system through `ImportConfig`:

### Core Settings
- **File Paths**: Input/output locations (local or S3)
- **Processing Options**: Batch size (an upper bound), encoding, delimiters
- **Validation Settings**: Schema file, `validate_schema`, `apply_schema_extensions` (CSV: run the schema's `x-...` extensions, default on)
- **Output Configuration**: Compression, manifest and metadata generation, `include_value_statistics`

`header_mode` and `excess_column_mode` accept the enum member or a case-insensitive string
(`"absent"`); an unknown value raises a `ValueError` listing the valid ones. See the
[configuration readme](config/forklift.engine.config.readme.md) for every option.

### Header Detection
- **PRESENT**: First non-blank, non-comment row is the header; no header within `header_search_rows` rows raises `ValueError`
- **ABSENT**: No headers, use the schema's column names, or generate `col_1`..`col_N`
- **AUTO**: Automatic header detection using content analysis

### Error Handling
- **ExcessColumnMode**: TRUNCATE (default, counted in `truncated_rows`), REJECT (to `bad_rows.parquet`), or PASSTHROUGH extra fields
- **Validation Limits**: `max_validation_errors` is reserved and not enforced
- **Bad Data Separation**: Invalid rows written to `bad_rows.parquet`

## Output Generation

### Primary Outputs (CSV import)
- **`data.parquet`**: Compressed columnar data file (an input with a header but no rows still yields an empty file carrying the schema)
- **`metadata.json`**: Processing statistics and configuration, plus `schema_extensions`, `validation_summary` and `warnings`
- **`output_data_metadata.json`**: Column statistics of the output (after the schema extensions, so calculated columns are included); no cell values unless `include_value_statistics=True`
- **`manifest.json`**: List of generated output files

Stale `data.parquet` and `bad_rows.parquet` files of an earlier run are removed when a run starts. If a
run fails, no partial data or bad rows file is left behind. The exception is a run that stops because
`x-validation` rejected more rows than `maxBadRowsPercent` allows: it keeps a finished `bad_rows.parquet`
(and names it in the error) so the rejected rows can be inspected; `data.parquet` is not kept.

The engine writes all of these itself (with S3 support through `forklift.io`); the `forklift.outputs` package is not used by it.

### Error Outputs
- **`bad_rows.parquet`**: Rejected rows as strings, in the shape of the input columns (`results.bad_rows_file`). When the schema configures `x-validation` or a constraint it has a last column `_rejection_reason` (`type_conversion_failed`, `required_value_missing`, `too_many_fields`, or `CODE` / `CODE:column` such as `UNIQUE_VIOLATION:id`; never a cell value)
- **`results.errors`**: Messages for failures (Arrow messages are stripped of row content)
- **`results.warnings`**, **`results.validation_summary`**, **`results.schema_extensions`**: ignored schema content, finding counts per `CODE` / `CODE:column`, and the extensions that ran (CSV)
- **Processing Logs**: Execution statistics and timing

## Performance Optimization

### Memory Management
- **Streaming Processing**: Process data in configurable batches
- **PyArrow Integration**: Efficient columnar data handling
- **Resource Cleanup**: Automatic cleanup of temporary resources

### Scalability Features
- **Batch Processing**: Configurable batch sizes for optimal memory usage
- **S3 Streaming**: Efficient cloud storage integration
- **Lazy Loading**: Components initialized only when needed

### Performance Tuning
```python
# Optimize for large files
results = import_csv(
    input_path="huge_dataset.csv",
    output_path="output/",
    batch_size=100000,      # Upper bound on rows per written batch
    validate_schema=False,   # Skip the required-column check for trusted data
    compression="snappy"     # Fast compression
)
```

## Error Handling and Resilience

### Graceful Error Recovery
- **Partial Processing**: Continue processing despite individual row failures
- **Error Separation**: Invalid data preserved for investigation
- **Detailed Reporting**: Comprehensive error information in results

### Exception Management
```python
from forklift import import_csv

try:
    results = import_csv("data.csv", "output/")
    if results.invalid_rows > 0:
        print(f"Processing completed with {results.invalid_rows} invalid rows")
        # Invalid data available in results.bad_rows_file
except (ValueError, OSError) as e:
    print(f"Processing failed: {e}")  # e.g. unreadable file, no header, bad encoding
```

`import_csv` re-raises the original exception (`ValueError`, `FileNotFoundError`, a pyarrow error, ...).
`import_excel` and `import_sql` raise `ProcessingError` for schema problems; `import_sql` also raises it
when a table fails (partial results are attached as `error.results`) unless `continue_on_error=True`.

## Integration Examples

### Basic Usage
```python
from forklift import import_csv, import_excel, import_sql

# Simple CSV import
results = import_csv("data.csv", "output/")

# Excel with schema validation
results = import_excel(
    input_path="workbook.xlsx",
    output_path="output/",
    schema_file="excel_schema.json"
)

# SQL database import
results = import_sql(
    connection_string="postgresql://user:pass@localhost:5432/db",
    output_path="output/",
    schema_file="sql_schema.json"
)
```

### Advanced Configuration
```python
from forklift.engine import ForkliftCore, ImportConfig, HeaderMode

# Custom configuration
config = ImportConfig(
    input_path="complex_data.csv",
    output_path="s3://my-bucket/processed/",
    schema_file="validation_schema.json",
    header_mode=HeaderMode.AUTO,
    batch_size=25000,
    create_manifest=True,
)

# Direct engine usage
engine = ForkliftCore(config)
results = engine.process_csv()
```

## Extension Points

The Forklift Engine is designed for extensibility:

### Custom Processors
- Implement `BaseProcessor` interface for new data sources
- Add custom validation logic through processor extensions
- Integrate with existing streaming architecture

### Configuration Extensions
- Extend `ImportConfig` for format-specific options
- Add custom validation rules through schema extensions
- Implement custom error handling strategies

### Output Format Support
- Add new output formats through processor extensions
- Implement custom metadata generation
- Support additional compression algorithms

## Dependencies and Requirements

### Core Dependencies (installed with `pip install forklift-etl`)
- **PyArrow**: High-performance columnar processing (all data handling, including CSV parsing and Parquet writing)
- **boto3**: AWS S3 integration
- **jsonschema**, **python-dateutil**, **pytz**, **chardet**, **charset-normalizer**

### Optional Dependencies (extras)
- **openpyxl/xlrd** (`[excel]`): Excel file processing (`.xlsx` / `.xls`)
- **pyodbc** (`[sql]`): Database connectivity for SQL import (needs the unixODBC runtime library)
- **pandas** / **polars** (`[pandas]`, `[polars]`): only for `DataFrameReader.as_pandas()` / `as_polars()`; the engine never imports them
- **pyperclip** (`[clipboard]`): copy a generated schema to the clipboard

## Future Roadmap

### Planned Enhancements
- **Fixed-Width File Support**: Complete implementation of `import_fwf()`
- **Additional Formats**: JSON, XML, and other structured data formats
- **Schema Extensions beyond CSV**: `import_excel`, `import_sql` and `import_fwf` apply none of the `x-...` extensions yet; `x-pii` masking does not exist for any import
- **Performance Optimizations**: Further streaming and parallel processing improvements

### Integration Opportunities
- **Data Catalog Integration**: Automatic schema registry updates
- **Monitoring Integration**: Enhanced observability and metrics
- **Workflow Orchestration**: Integration with data pipeline frameworks
