# Forklift

A powerful data processing and schema generation tool with PyArrow streaming, validation, and S3 support.

![Forklift Logo](FORKLIFT.png)

## Overview

Forklift is a comprehensive data processing tool that provides:

- **High-performance data import** with PyArrow streaming for CSV, Excel, FWF, and SQL sources
- **Intelligent schema generation** that analyzes your data and creates standardized schema definitions  
- **Robust validation** with configurable error handling and constraint validation
- **S3 streaming support** for both input and output operations
- **Multiple output formats** including Parquet, with comprehensive metadata and manifests

## Key Features

### 🚀 **Data Import & Processing**
- Stream large files efficiently with PyArrow
- Support for CSV, Excel, Fixed-Width Files (FWF), and SQL sources
- Configurable batch processing with memory optimization
- Comprehensive validation with detailed error reporting
- S3 integration for cloud-native workflows

### 🔍 **Schema Generation**
- **Intelligent schema inference** from data analysis
- **Privacy-first approach** - no sensitive sample data included by default
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
pip install forklift
```

### Optional Dependencies

```bash
# For Excel support
pip install openpyxl

# For clipboard functionality
pip install pyperclip
```

## Quick Start

### Data Import

```python
import forklift

# Import CSV to Parquet with validation
from forklift import import_csv

results = import_csv(
    source="data.csv",
    destination="./output/",
    schema_path="schema.json"
)

print(f"Import completed successfully!")
```

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

### Reading Data for Analysis

```python
import forklift

# Read CSV into DataFrame for analysis
df = forklift.read_csv("data.csv")

# Read Excel with specific sheet
df = forklift.read_excel("data.xlsx", sheet_name="Sheet1")

# Read Fixed-Width File with schema
df = forklift.read_fwf("data.txt", schema_path="fwf_schema.json")
```

## CLI Usage

### Data Import

```bash
# Import CSV with schema validation
forklift ingest data.csv --dest ./output/ --input-kind csv --schema schema.json

# Import from S3
forklift ingest s3://bucket/data.csv --dest s3://bucket/output/ --input-kind csv

# Import Excel file
forklift ingest data.xlsx --dest ./output/ --input-kind excel --sheet "Sheet1"

# Import Fixed-Width File
forklift ingest data.txt --dest ./output/ --input-kind fwf --fwf-spec schema.json
```

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
```

## Core Components

- **Import Engine**: High-performance data processing with PyArrow
- **Schema Generator**: Intelligent schema inference and generation
- **Validation System**: Constraint validation and error handling
- **Processors**: Pluggable data transformation components
- **I/O Operations**: S3 and local file system support

## Documentation

For detailed documentation, see the [`docs/`](docs/) directory:

- **[Usage Guide](docs/USAGE.md)** - Comprehensive usage examples and workflows
- **[Schema Standards](docs/SCHEMA_STANDARDS.md)** - JSON Schema format and extensions
- **[API Reference](docs/API_REFERENCE.md)** - Complete API documentation
- **[Constraint Validation](docs/CONSTRAINT_VALIDATION_IMPLEMENTATION.md)** - Validation features
- **[S3 Integration](docs/S3_TESTING.md)** - S3 usage and testing

## Examples

See the [`examples/`](examples/) directory for comprehensive examples:

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
