# Forklift Core Module Documentation

## Overview

This document provides detailed information about the core forklift package modules and how they integrate to provide a comprehensive data processing and schema generation solution. The forklift package is designed as a high-performance, streaming-first data processing tool with intelligent schema inference capabilities.

## Architecture Overview

The forklift package consists of three primary user-facing modules that work together to provide a complete data processing ecosystem:

- **`api.py`** - Programmatic Python API for schema generation
- **`cli.py`** - Command-line interface for data processing and schema generation
- **`readers.py`** - DataFrame conversion utilities for data analysis workflows

## Core Modules

### api.py - Programmatic Schema Generation

The `api.py` module provides a clean Python API for generating Forklift schemas programmatically. This module is designed for integration into other Python applications and data pipelines.

#### Key Functions

**`generate_schema_from_csv()`**
- Analyzes CSV files to generate JSON Schema definitions
- Supports both local files and S3 URIs
- Configurable row analysis (default: entire file for accuracy)
- Privacy-first approach with optional sample data inclusion
- Primary key inference capabilities

**`generate_schema_from_excel()`**
- Excel file schema generation with sheet selection
- Optimized for memory efficiency with configurable row limits
- Support for both local and S3-hosted Excel files

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
- **Multi-format support**: CSV, Excel, Fixed-Width Files (FWF)
- **Validation**: JSON Schema-based validation with configurable error handling
- **Output**: High-performance Parquet files with comprehensive metadata
- **Cloud support**: Native S3 streaming for both input and output
- **Preprocessing**: Configurable data transformation pipelines

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
- **Metadata generation**: Rich statistical metadata for data profiling
- **Privacy controls**: Optional sample data inclusion with explicit opt-in

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

The `DataFrameReader` class manages the conversion of processed Parquet files to DataFrame formats:

**Key Features:**
- **Multi-library support**: Both Polars and Pandas integration
- **Lazy evaluation**: Polars LazyFrame support for efficient processing
- **Memory management**: Automatic cleanup of temporary files
- **Batch processing**: Handles multiple Parquet files from large datasets

#### Usage Examples

```python
import forklift

# Process data and get reader
reader = forklift.process_csv("large_dataset.csv", schema="schema.json")

# Convert to Polars DataFrame (recommended for performance)
df = reader.as_polars()

# Use lazy evaluation for large datasets
lazy_df = reader.as_polars(lazy=True)
result = lazy_df.filter(pl.col("amount") > 1000).collect()

# Convert to Pandas for existing workflows
pandas_df = reader.as_pandas()
```

## Integration in the Forklift Ecosystem

### Data Processing Pipeline

The forklift package fits into a comprehensive data processing ecosystem:

1. **Schema Generation** (`api.py`, `cli.py`)
   - Analyze source data to understand structure and types
   - Generate standardized JSON Schema definitions
   - Infer relationships and constraints

2. **Data Validation & Processing** (`cli.py`, `engine/`)
   - Validate data against schemas with configurable error handling
   - Apply transformations and cleaning operations
   - Stream processing for memory efficiency

3. **Data Analysis** (`readers.py`)
   - Convert processed data to analysis-ready formats
   - Support both exploratory analysis (Pandas) and production workflows (Polars)

### Design Principles

**Streaming-First Architecture**
- PyArrow streaming for memory-efficient processing of large files
- S3 native streaming without local downloads
- Configurable batch sizes for optimal performance

**Privacy and Security**
- No sensitive data in schemas by default
- Explicit opt-in for sample data inclusion
- Support for secure S3 operations

**Standards Compliance**
- JSON Schema with Forklift extensions
- Standardized metadata formats
- Consistent error reporting

**Flexibility and Extensibility**
- Modular design for custom integrations
- Configurable preprocessing pipelines
- Multiple output formats and targets

## Common Workflows

### Schema-Driven Data Processing

```python
# 1. Generate schema from sample data
schema = forklift.generate_schema_from_csv("sample.csv", nrows=10000)

# 2. Refine schema as needed (manually edit JSON)
# 3. Process full dataset with validated schema
results = forklift.import_csv("full_dataset.csv", schema=schema)

# 4. Convert to DataFrame for analysis
df = results.as_polars()
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
