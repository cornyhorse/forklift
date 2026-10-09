# Forklift

A powerful data processing and schema generation tool with PyArrow streaming, validation, and S3 support.

![Forklift Logo](FORKLIFT.png)

## Overview

Forklift is a comprehensive data processing tool that provides:

- **High-performance data import** with PyArrow streaming for CSV, Excel, FWF, and SQL sources
- **Intelligent schema generation** that analyzes your data and creates standardized schema definitions  
- **Robust validation** with configurable error handling and constraint validation
- **S3 streaming support** for both input and output operations
- **Parquet output** with metadata and manifest files; `pandas`/`polars` DataFrames on request through the readers

## Key Features

### 🚀 **Data Import & Processing**
- Stream large files efficiently with PyArrow
- Support for CSV, Excel, Fixed-Width Files (FWF), and SQL sources
- Configurable batch processing with memory optimization
- Comprehensive validation with detailed error reporting
- S3 integration for cloud-native workflows

### 🔍 **Schema Generation**
- **Intelligent schema inference** from data analysis
- **Privacy-first approach** - no sample rows and no raw cell values (top/bottom values, min/max, quantiles, enum value lists) in the generated schema or the output metadata unless you opt in with `include_sample_data` / `include_value_statistics`
- **Multiple file format support** - CSV, Excel, Parquet
- **Flexible output options** - stdout, file, or clipboard
- **Standards-compliant schemas** following JSON Schema with Forklift extensions

### 🛡️ **Validation & Quality**
- JSON Schema validation with custom extensions
- Primary key inference and enforcement
- Constraint validation (unique, not-null, primary key)
- Data type validation and conversion
- Configurable error handling modes (fail-fast, fail-complete, bad-rows)

## Installation

```bash
pip install forklift-etl
```

The core install is lean: it depends on `pyarrow` (>= 16, no upper cap, so current Python releases
including 3.13 and 3.14 work), `jsonschema`, `boto3`/`botocore`, `python-dateutil`, `pytz`, `chardet` and
`charset-normalizer`.

### Optional Dependencies (extras)

Input and output formats that need extra packages are installed as extras:

```bash
# Excel (.xlsx via openpyxl, legacy .xls via xlrd)
pip install "forklift-etl[excel]"

# SQL sources (pyodbc; also needs the unixODBC runtime library on your system)
pip install "forklift-etl[sql]"

# DataFrame hand-off formats
pip install "forklift-etl[pandas]"
pip install "forklift-etl[polars]"

# Copy generated schemas to the clipboard
pip install "forklift-etl[clipboard]"

# Several at once, or everything
pip install "forklift-etl[excel,sql,pandas,polars]"
pip install "forklift-etl[all]"
```

> **pandas and polars are optional output formats only.** Forklift processes all data with PyArrow;
> `pandas`/`polars` are imported lazily, only when you ask a reader result for a DataFrame
> (`as_pandas()` / `as_polars()`), and an `ImportError` tells you what to install if it is missing.

### Development

```bash
git clone https://github.com/cornyhorse/forklift.git
cd forklift
pip install -e ".[all,dev]"      # or: pip install -r requirements-dev.txt (also adds release tooling)
pre-commit install               # hooks are configured in .pre-commit-config.yaml
```

## Quick Start

### Data Import

```python
from forklift import import_csv

# Import CSV to Parquet with validation
results = import_csv(
    input_path="data.csv",
    output_path="./output/",
    schema_file="schema.json",
)

print(f"{results.valid_rows} rows imported, {results.invalid_rows} rejected")
if results.bad_rows_file:
    print(f"Rejected rows are in {results.bad_rows_file}")
```

With a schema, the declared types are applied to the output (a `string` column keeps `00123` as
written). Rows with a value that does not convert, or an empty value in a `required` column, go to
`bad_rows.parquet` instead of stopping the run. `import_excel(input_path, output_path, schema_file=None,
sheet=None)` writes one Parquet file per sheet, and `import_sql(connection_string, output_path,
schema_file)` one per table listed in the schema file. `import_fwf` is not implemented yet and raises
`NotImplementedError`.

### Schema Generation

```python
import forklift

# Generate schema from CSV (analyzes entire file by default)
schema = forklift.generate_schema_from_csv("data.csv")

# Generate with limited row analysis
schema = forklift.generate_schema_from_csv("data.csv", nrows=1000)

# Save schema to file
forklift.generate_and_save_schema(
    input_path="data.csv",
    output_path="schema.json",
    file_type="csv"
)

# Generate with primary key inference
schema = forklift.generate_schema_from_csv(
    "data.csv",
    infer_primary_key_from_metadata=True
)
```

Types are inferred from the text of every sampled value, so identifiers with leading zeros stay
strings and `NA` is not treated as null. The generated `x-metadata` holds counts, null statistics,
distinct counts and string-length statistics only; the statistics that copy cell values (top and
bottom values, `suggested_enum_values`, min/max/median/quantiles) are added only with
`include_value_statistics=True`, because those values can be personal data.

### Reading Data for Analysis

The `read_*` functions run the same pipeline as the `import_*` functions into a temporary directory and
return a `DataFrameReader`; convert it with `as_pyarrow()`, `as_pandas()` or `as_polars()` (the last
two need the optional `pandas` / `polars` packages).

```python
import forklift

# Read CSV into a DataFrame for analysis
df = forklift.read_csv("data.csv").as_polars()

# Read Excel with a specific sheet
df = forklift.read_excel("data.xlsx", sheet="Sheet1").as_pandas()

# Delete the temporary Parquet files when you are done
with forklift.read_csv("data.csv", schema_file="schema.json") as reader:
    table = reader.as_pyarrow()
```

Only accepted rows are returned: rows that were rejected (see `bad_rows.parquet` above) are not part of
the DataFrame. `read_fwf` raises `NotImplementedError` until fixed-width import exists in the engine.

## CLI Usage

### Data Import

```bash
# Import CSV with schema validation
forklift ingest data.csv --dest ./output/ --input-kind csv --schema schema.json

# Import from S3
forklift ingest s3://bucket/data.csv --dest s3://bucket/output/ --input-kind csv

# Import Excel file
forklift ingest data.xlsx --dest ./output/ --input-kind excel --sheet "Sheet1"

# Include statistics that expose real cell values (top values, min/max, quantiles) in the
# output metadata. Off by default because the metadata can hold personal data.
forklift ingest data.csv --dest ./output/ --input-kind csv --include-value-stats

# Fixed-width files are not implemented in the engine yet: this exits with status 2
forklift ingest data.txt --dest ./output/ --input-kind fwf --fwf-spec schema.json
```

Exit codes: `0` on success, `1` when processing fails (or `results.errors` is not empty), `2` for usage
errors and for input kinds that are not implemented. `--sheet` (Excel only) takes a sheet name, or a
0-based index if no sheet has that name; without it every sheet is imported. `--encoding-priority`
accepts a list but only its first entry is used.

### Schema Generation

```bash
# Generate schema from CSV (analyzes entire file by default)
forklift generate-schema data.csv --file-type csv

# Generate with limited row analysis
forklift generate-schema data.csv --file-type csv --nrows 1000

# Save to file
forklift generate-schema data.csv --file-type csv --output file --output-path schema.json

# Include sample data for development (explicit opt-in)
forklift generate-schema data.csv --file-type csv --include-sample

# Copy to clipboard
forklift generate-schema data.csv --file-type csv --output clipboard

# Excel files
forklift generate-schema data.xlsx --file-type excel --sheet "Sheet1"

# Parquet files
forklift generate-schema data.parquet --file-type parquet

# With primary key inference
forklift generate-schema data.csv --file-type csv --infer-primary-key

# Write the column metadata to its own file (without top/bottom values, min/max or quantiles ...)
forklift generate-schema data.csv --file-type csv --metadata-output metadata.json

# ... and opt in to the value-bearing statistics (they can contain personal data)
forklift generate-schema data.csv --file-type csv --include-value-stats
```

`--nrows` defaults to the whole file. Only local paths and `s3://` URIs are accepted as `source`
(`http://`, `ftp://`, `file://` ... are rejected).

## Core Components

- **Import Engine**: High-performance data processing with PyArrow
- **Schema Generator**: Intelligent schema inference and generation
- **Validation System**: Constraint validation and error handling
- **Processors**: Pluggable data transformation components
- **I/O Operations**: S3 and local file system support

## Documentation

For detailed documentation, see the [`docs/`](docs/) directory:

- **[Usage Guide](docs/guides/USAGE.md)** - Comprehensive usage examples and workflows
- **[Schema Standards](docs/schemas/SCHEMA_STANDARDS.md)** - JSON Schema format and extensions
- **[API Reference](docs/api/API_REFERENCE.md)** - Complete API documentation
- **[Constraint Validation](docs/integration/CONSTRAINT_VALIDATION_IMPLEMENTATION.md)** - Validation features
- **[S3 Integration](docs/aws/S3_TESTING.md)** - S3 usage and testing

## Examples

See the [`examples/`](examples/) directory for comprehensive examples:

- **[getting_started.py](examples/getting_started.py)** - **Start here!** Complete introduction to CSV processing with schema validation, including basic usage, complete schema validation, and passthrough mode for processing subsets of columns
- **calculated_columns_demo.py** - Calculated columns functionality
- **constraint_validation_demo.py** - Constraint validation examples
- **validation_demo.py** - Data validation with bad rows handling
- **datetime_features_example.py** - Date/time processing examples
- And more...

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
