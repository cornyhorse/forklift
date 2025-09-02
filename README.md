# forklift
![FORKLIFT.png](FORKLIFT.png)

A high-performance data processing tool that standardizes ingestion and cleanup of messy tabular files (CSV, Excel, FWF, SQL) into optimized Parquet format using PyArrow streaming.

## Overview

Forklift is designed to handle real-world data processing challenges including:
- 🧹 **Data Cleaning**: Automatic header detection, encoding detection, prologue/footer handling
- 🚀 **High Performance**: PyArrow streaming for memory-efficient processing of large files
- 🔧 **Schema Validation**: JSON Schema-based validation with comprehensive error reporting
- ☁️ **Cloud Native**: S3 streaming support for input and output
- 📊 **Multiple Formats**: Support for CSV, Excel (XLS/XLSX), Fixed-Width Files (FWF), and SQL databases
- 🛡️ **Robust Processing**: Advanced error handling and data quality validation
- 🐻‍❄️ **Ad-hoc Analysis**: Direct integration with Polars and Pandas for immediate DataFrame usage

## Install

```bash
pip install forklift

# For ad-hoc DataFrame usage, install the desired DataFrame library:
pip install polars  # For high-performance Rust-based DataFrames
pip install pandas  # For traditional Python DataFrames
```

## Quick Start

### Command Line Interface

```bash
# Basic CSV processing
forklift ingest input.csv --dest output/ --input-kind csv --schema schema.json

# Excel processing with specific sheet
forklift ingest data.xlsx --dest output/ --input-kind excel --sheet "Sales Data"

# Fixed-width file processing
forklift ingest data.txt --dest output/ --input-kind fwf --fwf-spec fwf_schema.json
```

### Python API

#### ETL Pipeline (Parquet Output)
```python
import forklift as fl

# CSV processing to Parquet files
results = fl.import_csv(
    input_path="data.csv",
    output_path="output/",
    schema_file="schema.json"
)

# Excel processing to Parquet files
results = fl.import_excel(
    input_path="data.xlsx",
    output_path="output/",
    schema_file="schema.json"
)

# Fixed-width file processing
results = fl.import_fwf(
    input_path="data.txt",
    output_path="output/",
    schema_file="schema.json"
)

# SQL database processing
results = fl.import_sql(
    input_path="postgresql://user:pass@host/db",
    output_path="output/",
    schema_file="schema.json"
)
```

#### Ad-Hoc DataFrame Loading
```python
import forklift as fl

# Load directly to Polars (fastest for large datasets)
df = fl.read_csv("data.csv").as_polars()
df = fl.read_excel("data.xlsx").as_polars()

# Load directly to Pandas (for compatibility)
df = fl.read_csv("data.csv").as_pandas()
df = fl.read_excel("data.xlsx").as_pandas()

# Lazy evaluation for memory efficiency with large files
lazy_df = fl.read_csv("huge_dataset.csv").as_polars(lazy=True)
result = lazy_df.filter(pl.col("amount") > 1000).collect()

# PyArrow Tables for columnar processing
table = fl.read_csv("data.csv").as_pyarrow()

# All formats support schema validation and data cleaning
df = fl.read_csv("messy_data.csv", schema_file="schema.json").as_polars()
```

## Core Architecture

### Engine (`src/forklift/engine/`)
- **ForkliftCore**: Main processing engine with PyArrow streaming
- **ImportConfig**: Comprehensive configuration management
- **HeaderMode/ExcessColumnMode**: Enums for processing behavior control

### Input Processors (`src/forklift/inputs/`)
- **CSV**: Advanced CSV processing with encoding detection and header analysis
- **Excel**: Multi-sheet Excel processing (XLS/XLSX) via Pandas
- **FWF**: Fixed-width file processing with position-based parsing
- **SQL**: Live database connectivity with glob-based table selection

### Data Processors (`src/forklift/processors/`)
- **Schema Validation**: PyArrow schema validation and type coercion
- **Data Quality**: Configurable quality checks and validation rules
- **Transformations**: Column transformations and data cleaning
- **Pipeline**: Chaining multiple processors for complex workflows

### Schema Management (`src/forklift/schema/`)
- **JSON Schema 2020-12**: Standards-compliant schema validation
- **Format-specific importers**: CSV, Excel, FWF, and SQL schema processors
- **Validation**: Comprehensive schema and data validation with detailed error reporting

### Output Generation (`src/forklift/outputs/`)
- **Parquet**: Optimized Parquet file generation with metadata
- **Manifest**: Processing manifests for auditability
- **Metadata**: Rich metadata generation for downstream processing

### Utilities (`src/forklift/utils/`)
- **Encoding Detection**: Automatic encoding detection for text files
- **Date Parsing**: Intelligent date/time parsing and normalization
- **Column Utilities**: Column name deduplication and standardization
- **Row Validation**: Advanced row-level validation and error handling

### I/O Layer (`src/forklift/io/`)
- **S3 Streaming**: Native S3 support for input and output
- **Unified I/O**: Consistent interface for local and cloud storage

## Input Format Support

### CSV Input
- **Features**:
  - Prologue/footer detection and skipping
  - Automatic header detection with configurable search depth
  - Encoding auto-detection (UTF-8, Latin-1, etc.)
  - Column name deduplication
  - Comment row filtering
  - Excess column handling (truncate or reject)
- **Schema**: JSON Schema with `x-csv` extensions
- **Use Cases**: Log files, exports, data dumps with headers/footers

### Excel Input  
- **Features**:
  - Multi-sheet processing with Pandas backend
  - Header modes: auto-detect, present, absent
  - Per-sheet header overrides
  - Column name deduplication
  - Legacy XLS and modern XLSX support
- **Schema**: JSON Schema with `x-excel` extensions
- **Use Cases**: Financial reports, operational data, multi-table workbooks

### Fixed-Width Files (FWF)
- **Features**:
  - Schema-driven parsing with exact column positions
  - Variable and fixed-width column support
  - Type conversion and validation
  - Configurable null value handling
- **Schema**: JSON Schema with `x-fwf` position specifications
- **Use Cases**: Mainframe exports, legacy system data, formatted reports

### SQL Database Input
- **Features**:
  - Live database connectivity (PostgreSQL, MySQL, SQLite, SQL Server, Oracle)
  - Explicit table specification (`schema.table_name`, `table_name`)
  - Streaming extraction for large tables
  - Connection pooling and optimization
  - Support for different database naming conventions
- **Schema**: JSON Schema with `x-sql` table specifications
- **Use Cases**: Data warehouse extraction, database migration, ETL pipelines

## Processing Features

### Header Detection
```python
from forklift.engine.forklift_core import HeaderMode

# Auto-detect header location
config.header_mode = HeaderMode.AUTO

# Explicit header handling
config.header_mode = HeaderMode.PRESENT  # File has header
config.header_mode = HeaderMode.ABSENT   # No header, use schema
```

### Data Validation
- JSON Schema 2020-12 compliance
- PyArrow-based type validation and coercion
- Range and format validation
- Custom validation rules via processors
- Detailed error reporting with row/column context

### Performance Optimization
- PyArrow streaming for memory efficiency
- Configurable batch processing
- Optimized Parquet output with compression
- Lazy evaluation support for large datasets

### Cloud Integration
```python
# S3 input and output
results = fl.import_csv(
    input_path="s3://bucket/data.csv",
    output_path="s3://bucket/processed/",
    schema_file="s3://bucket/schemas/schema.json"
)
```

## Schema Standards

Forklift uses JSON Schema 2020-12 with format-specific extensions:

### Required Schema Structure
```json
{
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/example.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Example Schema",
  "type": "object",
  "properties": {
    "column_name": { "type": "string" },
    "numeric_column": { "type": "number" },
    "date_column": { "type": "string", "format": "date" }
  },
  "x-csv": {
    "delimiter": ",",
    "nulls": { "global": ["", "NULL", "N/A"] }
  }
}
```

### SQL Schema Example
```json
{
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/sql_example.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SQL Database Schema",
  "type": "object",
  "properties": {
    "customer_id": { "type": "integer" },
    "customer_name": { "type": "string" },
    "created_date": { "type": "string", "format": "date" }
  },
  "x-sql": {
    "tables": [
      {
        "select": {
          "schema": "sales",
          "name": "customers"
        },
        "outputName": "customers"
      },
      {
        "select": {
          "schema": "inventory", 
          "name": "products"
        },
        "outputName": "products"
      }
    ]
  }
}
```

### Format Extensions
- **`x-csv`**: CSV-specific configuration (delimiter, nulls, encoding)
- **`x-excel`**: Excel-specific configuration (sheets, headers)
- **`x-fwf`**: Fixed-width configuration (positions, widths)
- **`x-sql`**: SQL-specific configuration (explicit table specifications)

## Output Formats

### Parquet Files
- Snappy compression by default
- Schema preservation and type optimization
- Metadata embedding for lineage tracking
- Optimized for both row-based and columnar access

### Manifest Files
- Processing statistics and metrics
- Input/output file inventory
- Validation summary and error counts
- Processing timestamps and performance data

### Metadata Files
- Schema information and transformations
- Data quality metrics
- Processing configuration snapshot
- Lineage and provenance tracking

## Error Handling

Forklift provides comprehensive error handling and reporting:

- **Schema Validation Errors**: Detailed field-level validation failures
- **Data Processing Errors**: Row-level errors with context
- **I/O Errors**: File access and network issues
- **Performance Monitoring**: Processing statistics and bottleneck identification

## Real-World Examples

### Large Dataset Processing with Polars

```python
import forklift as fl
import polars as pl

# Process large CSV with lazy evaluation
lf = fl.read_csv("sales_2024.csv", schema_file="sales_schema.json").as_polars(lazy=True)

# Efficient aggregation at Rust speed
monthly_totals = (
    lf
    .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
    .with_columns(pl.col("date").dt.month().alias("month"))
    .group_by("month")
    .agg([
        pl.col("amount").sum().alias("total_sales"),
        pl.col("customer_id").n_unique().alias("unique_customers")
    ])
    .collect()
)
```

### ETL Pipeline with Full Processing

```python
from forklift.engine.forklift_core import ImportConfig, HeaderMode

config = ImportConfig(
    input_path="large_dataset.csv",
    output_path="processed/",
    schema_file="schema.json",
    batch_size=50000,  # Optimize for large files
    header_mode=HeaderMode.AUTO,
    encoding="utf-8"
)

results = fl.import_csv(**config.__dict__)
print(f"Processed {results.total_rows:,} rows in {results.execution_time:.2f}s")
```

### Multi-Format Data Integration

```python
import forklift as fl
import polars as pl

# Read from different sources and combine
sales_csv = fl.read_csv("sales.csv").as_polars(lazy=True)
customer_excel = fl.read_excel("customers.xlsx").as_polars(lazy=True)
product_db = fl.read_sql("postgresql://host/db", 
                        schema_file="sql_schema.json").as_polars(lazy=True)

# Join efficiently in Rust layer
result = (
    sales_csv
    .join(customer_excel, on="customer_id")
    .join(product_db, on="product_id")
    .collect()
)
```

## Development

### Project Structure
```
forklift/
├── src/forklift/          # Main package
│   ├── engine/            # Core processing engine
│   ├── inputs/            # Input format processors
│   ├── outputs/           # Output generators
│   ├── processors/        # Data processors and validation
│   ├── schema/            # Schema management
│   ├── utils/             # Utility functions
│   ├── io/                # I/O abstractions
│   └── readers.py         # Ad-hoc DataFrame readers
├── tests/                 # Test suite
├── schema-standards/      # Reference schemas
└── docs/                  # Documentation
```

### Running Tests
```bash
pytest tests/ -v --cov=forklift
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Dependencies

Forklift builds on excellent open-source libraries:

| Package | Purpose | License |
|---------|---------|---------|
| PyArrow | Columnar processing & Parquet | Apache-2.0 |
| Pandas | Data manipulation & Excel support | BSD-3-Clause |
| Polars | High-performance DataFrames (optional) | MIT |
| JSONSchema | Schema validation | MIT |
| Click | CLI framework | BSD-3-Clause |
| Boto3 | AWS S3 integration | Apache-2.0 |

See the full dependency list in `pyproject.toml` for complete details.
