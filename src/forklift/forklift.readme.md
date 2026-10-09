# Forklift Core Module Documentation

## Overview

This document provides detailed information about the core forklift package modules and how they integrate to provide a comprehensive data processing and schema generation solution. The forklift package is designed as a high-performance, streaming-first data processing tool with intelligent schema inference capabilities.

## Architecture Overview

The forklift package consists of three primary user-facing modules that work together to provide a complete data processing ecosystem:

- **`api.py`** - Programmatic Python API for schema generation
- **`cli.py`** - Command-line interface for data processing and schema generation
- **`readers.py`** - DataFrame conversion utilities for data analysis workflows

The import functions (`import_csv`, `import_excel`, `import_fwf`, `import_sql`) live in `engine/forklift_core.py` and are re-exported by the package; `import_fwf` raises `NotImplementedError` because fixed-width import is not wired into the engine yet.

## Core Modules

### api.py - Programmatic Schema Generation

The `api.py` module provides a clean Python API for generating Forklift schemas programmatically. This module is designed for integration into other Python applications and data pipelines.

#### Key Functions

**`generate_schema_from_csv()`**
- Analyzes CSV files to generate JSON Schema definitions
- Supports both local files and S3 URIs
- Configurable row analysis (default: entire file for accuracy; `nrows` is a positive integer or `None`)
- Privacy-first approach: sample rows (`include_sample_data`) and value-bearing statistics (`include_value_statistics`: top/bottom values, min/max, quantiles, enum value lists) are separate opt-ins
- Primary key inference capabilities (only columns that are 100% unique in the sample)

**`generate_schema_from_excel()`**
- Excel file schema generation with sheet selection (`.xlsx` only; the default sample is 1000 rows, `nrows=None` reads the whole sheet)
- Optimized for memory efficiency with configurable row limits
- Support for both local and S3-hosted Excel files

**`generate_schema_from_parquet()`**, **`generate_and_save_schema()`**, **`generate_and_copy_schema()`**
- Parquet input, saving to a file (local or S3) and copying to the clipboard (needs the `clipboard` extra)

#### Usage Examples

```python
import forklift

# Generate schema from entire CSV file (recommended for accuracy)
schema = forklift.generate_schema_from_csv("data.csv")

# Limited analysis for large files
schema = forklift.generate_schema_from_csv("data.csv", nrows=10000)

# With primary key inference
schema = forklift.generate_schema_from_csv(
    "data.csv", 
    infer_primary_key_from_metadata=True
)

# Manual primary key specification
schema = forklift.generate_schema_from_csv(
    "data.csv",
    user_specified_primary_key=["user_id", "timestamp"]
)
```

### cli.py - Command-Line Interface

The `cli.py` module provides a comprehensive command-line interface with two primary commands: `ingest` and `generate-schema`.

#### Ingest Command

The ingest command handles data processing and conversion:
- **Input kinds**: `csv`, `excel` and `fwf` (`fwf` is not implemented yet and exits with status 2)
- **Validation**: JSON Schema types and `required` columns for CSV; bad rows go to `bad_rows.parquet`
- **Output**: High-performance Parquet files with manifest and metadata files
- **Cloud support**: Native S3 streaming for both input and output
- **Exit codes**: `0` success, `1` processing failed (or the run reported errors), `2` usage errors and not-implemented input kinds
- `--include-value-stats` adds value-bearing statistics (top values, min/max, quantiles) to the CSV output metadata; `--sheet` (Excel) takes a sheet name or a 0-based index; `--encoding-priority` accepts several encodings but only the first is used; `--pre` (preprocessors) only prints a warning

```bash
# Basic CSV ingestion with validation
forklift ingest data.csv --dest ./output/ --input-kind csv --schema schema.json

# S3 to S3 processing
forklift ingest s3://bucket/data.csv --dest s3://bucket/output/ --input-kind csv

# Excel processing with sheet selection
forklift ingest data.xlsx --dest ./output/ --input-kind excel --sheet "Sheet1"
```

#### Generate-Schema Command

The schema generation command provides flexible schema creation:
- **Multiple output targets**: stdout, file, clipboard
- **Configurable analysis depth**: Control row analysis for performance
- **Metadata generation**: Statistical metadata for data profiling (`--metadata-output` writes it to its own file; `--no-metadata` turns it off)
- **Privacy controls**: Sample data (`--include-sample`) and value statistics (`--include-value-stats`) are explicit opt-ins
- **Analysis depth**: `--nrows` defaults to the whole file

```bash
# Generate schema with full file analysis
forklift generate-schema data.csv --file-type csv

# Limited analysis for performance
forklift generate-schema data.csv --file-type csv --nrows 5000

# Save to file with metadata
forklift generate-schema data.csv --file-type csv --output file --output-path schema.json

# Include sample data (explicit opt-in for development)
forklift generate-schema data.csv --file-type csv --include-sample
```

### readers.py - DataFrame Integration

The `readers.py` module provides seamless integration with popular DataFrame libraries, enabling data scientists and analysts to easily incorporate forklift's processing capabilities into their workflows.

#### DataFrameReader Class

`read_csv`, `read_excel`, `read_fwf` and `read_sql` run the corresponding `import_*` function into a temporary directory and return a `DataFrameReader`, which manages the temporary Parquet files and converts them to DataFrame formats:

**Key Features:**
- **Output formats**: `as_pyarrow()` always works; `as_polars()` and `as_pandas()` import polars / pandas lazily and are optional (`pip install "forklift-etl[polars]"` / `[pandas]`). Nothing else in Forklift uses pandas or polars
- **Lazy evaluation**: `as_polars(lazy=True)` returns a LazyFrame
- **Only accepted rows**: rows that were rejected (`bad_rows.parquet`) are not part of the result
- **Memory management**: `close()` (or `with` ...) deletes the temporary files; otherwise they are removed when the interpreter exits. They are not removed when the reader is garbage collected, because a LazyFrame may still read from them
- **Multiple files**: results made of several Parquet files are concatenated; a header-only input gives an empty frame

#### Usage Examples

```python
import forklift
import polars as pl

with forklift.read_csv("large_dataset.csv", schema_file="schema.json") as reader:
    # Convert to Polars DataFrame
    df = reader.as_polars()

    # Convert to Pandas for existing workflows
    pandas_df = reader.as_pandas()

# Lazy evaluation: collect before closing the reader
reader = forklift.read_csv("large_dataset.csv")
result = reader.as_polars(lazy=True).filter(pl.col("amount") > 1000).collect()
reader.close()
```

## Integration in the Forklift Ecosystem

### Data Processing Pipeline

The forklift package fits into a comprehensive data processing ecosystem:

1. **Schema Generation** (`api.py`, `cli.py`)
   - Analyze source data to understand structure and types
   - Generate standardized JSON Schema definitions
   - Infer relationships and constraints

2. **Data Validation & Processing** (`cli.py`, `engine/`)
   - Apply the schema's types and required columns, setting rejected rows aside
   - Stream processing for memory efficiency

3. **Data Analysis** (`readers.py`)
   - Convert processed data to analysis-ready formats
   - Hand the result to pyarrow, pandas or polars

### Design Principles

**Streaming-First Architecture**
- PyArrow streaming for memory-efficient processing of large files
- S3 native streaming without local downloads
- Configurable batch sizes for optimal performance

**Privacy and Security**
- No sample rows and no raw cell values (top/bottom values, min/max, quantiles, enum value lists) in schemas or output metadata by default
- Explicit opt-in for sample data (`include_sample_data`) and value statistics (`include_value_statistics`)
- Only file names, not directories, are recorded as provenance; error messages carry row numbers and column names, not cell content
- Connection strings are redacted in SQL import metadata, S3 writers publish nothing when a run fails

**Standards Compliance**
- JSON Schema with Forklift extensions
- Standardized metadata formats
- Consistent error reporting

**Flexibility and Extensibility**
- Modular design for custom integrations
- Local and S3 inputs and outputs, Parquet output

## Common Workflows

### Schema-Driven Data Processing

```python
# 1. Generate schema from sample data
schema = forklift.generate_schema_from_csv("sample.csv", nrows=10000)

# 2. Refine schema as needed (manually edit JSON)
# 3. Process full dataset with validated schema
import json
with open("schema.json", "w") as f:
    json.dump(schema, f, indent=2)   # then edit the file as needed
results = forklift.import_csv("full_dataset.csv", "output/", schema_file="schema.json")

# 4. Read the Parquet result for analysis
import pyarrow.parquet as pq
table = pq.read_table(results.output_files[0])   # or: forklift.read_csv(..., schema_file=...).as_polars()
```

### Cloud-Native Processing

```bash
# Generate schema from S3 data
forklift generate-schema s3://bucket/sample.csv --file-type csv --output file --output-path schema.json

# Process full dataset
forklift ingest s3://bucket/full_data.csv --dest s3://bucket/processed/ --input-kind csv --schema schema.json
```

### Development and Production

```python
# Development: Include sample data for exploration
dev_schema = forklift.generate_schema_from_csv("data.csv", include_sample_data=True)

# Production: Clean schema without sensitive data
prod_schema = forklift.generate_schema_from_csv("data.csv", include_sample_data=False)
```

This modular architecture ensures that forklift can adapt to various use cases, from one-off data analysis to production data pipelines, while maintaining high performance and data privacy standards.
