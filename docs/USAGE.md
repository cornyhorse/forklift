# Forklift Usage Guide

This guide provides comprehensive examples and workflows for using Forklift effectively.

## Table of Contents

- [Data Import Workflows](#data-import-workflows)
- [Schema Generation](#schema-generation)
- [Data Reading and Analysis](#data-reading-and-analysis)
- [Validation and Error Handling](#validation-and-error-handling)
- [Working with Different File Formats](#working-with-different-file-formats)
- [S3 Integration](#s3-integration)
- [Advanced Features](#advanced-features)

## Data Import Workflows

### Basic CSV Import

```python
import forklift

# Simple CSV import to Parquet
results = forklift.import_csv(
    source="sales_data.csv",
    destination="./output/"
)
```

### Import with Schema Validation

```python
import forklift

# Import with schema validation
results = forklift.import_csv(
    source="sales_data.csv",
    destination="./output/",
    schema_path="sales_schema.json"
)

# Check results
print(f"Total rows processed: {results.total_rows}")
print(f"Valid rows: {results.valid_rows}")
print(f"Invalid rows: {results.invalid_rows}")
```

### Import with Preprocessing

```python
import forklift

# Import with data processors
results = forklift.import_csv(
    source="sales_data.csv",
    destination="./output/",
    preprocessors=["string_cleaning", "date_standardization"]
)
```

### Excel Import

```python
import forklift

# Import specific Excel sheet
results = forklift.import_excel(
    source="financial_data.xlsx",
    destination="./output/",
    sheet_name="Q4_Results"
)
```

### Fixed-Width File Import

```python
import forklift

# Import FWF with schema specification
results = forklift.import_fwf(
    source="mainframe_export.txt",
    destination="./output/",
    schema_path="fwf_schema.json"
)
```

## Schema Generation

### Basic Schema Generation

```python
import forklift

# Generate schema from CSV
schema = forklift.generate_schema_from_csv("customer_data.csv")

# Pretty print the schema
import json
print(json.dumps(schema, indent=2))
```

### Schema Generation with Analysis Options

```python
import forklift

# Generate schema with limited row analysis for large files
schema = forklift.generate_schema_from_csv(
    "large_dataset.csv",
    nrows=10000  # Analyze first 10,000 rows
)

# Generate with primary key inference
schema = forklift.generate_schema_from_csv(
    "customer_data.csv",
    infer_primary_key_from_metadata=True
)

# Manually specify primary key
schema = forklift.generate_schema_from_csv(
    "customer_data.csv",
    user_specified_primary_key=["customer_id"]
)
```

### Schema Generation with All Output Options

```python
import forklift

# Generate schema to stdout (default)
schema = forklift.generate_schema_from_csv("customer_data.csv")
print(schema)

# Generate schema and save to file
forklift.generate_and_save_schema(
    input_path="products.csv",
    output_path="products_schema.json",
    file_type="csv"
)

# Generate schema and copy to clipboard (requires pyperclip)
forklift.generate_and_copy_schema(
    input_path="products.csv",
    file_type="csv"
)

# Generate with all metadata options
schema = forklift.generate_schema_from_csv(
    "data.csv",
    nrows=5000,                         # Analyze first 5000 rows
    include_sample_data=True,           # Include sample data (opt-in)
    infer_primary_key_from_metadata=True, # Auto-infer primary key
    include_metadata=True,              # Rich metadata
    enum_threshold=0.1,                 # Suggest enums for low-cardinality
    uniqueness_threshold=0.95,          # Flag highly unique fields
    top_n_values=10                     # Include top/bottom values
)
```

### CLI Schema Generation to Different Outputs

```bash
# Generate to stdout (default)
forklift generate-schema data.csv --file-type csv

# Generate and save to file
forklift generate-schema data.csv --file-type csv --output file --output-path schema.json

# Generate and copy to clipboard
forklift generate-schema data.csv --file-type csv --output clipboard

# Generate with all options
forklift generate-schema data.csv \
  --file-type csv \
  --nrows 5000 \
  --include-sample \
  --infer-primary-key \
  --enum-threshold 0.1 \
  --uniqueness-threshold 0.95 \
  --top-n-values 15 \
  --output file \
  --output-path detailed_schema.json

# Excel schema generation
forklift generate-schema financial_data.xlsx \
  --file-type excel \
  --sheet "Summary" \
  --output file \
  --output-path excel_schema.json

# Parquet schema generation
forklift generate-schema existing_data.parquet \
  --file-type parquet \
  --output clipboard
```

## Data Reading and Analysis

### Reading for Quick Analysis

```python
import forklift

# Read CSV into pandas DataFrame
df = forklift.read_csv("sales_data.csv")
print(df.head())
print(df.info())

# Read with specific encoding
df = forklift.read_csv("legacy_data.csv", encoding="latin-1")

# Read with custom delimiter
df = forklift.read_csv("pipe_delimited.txt", delimiter="|")
```

### Reading Excel Files

```python
import forklift

# Read specific sheet
df = forklift.read_excel("quarterly_report.xlsx", sheet_name="Q1")

# Read with header customization
df = forklift.read_excel(
    "data_with_metadata.xlsx",
    sheet_name="Data",
    skip_rows=3  # Skip metadata rows
)
```

### Reading Fixed-Width Files

```python
import forklift

# Read FWF with schema
df = forklift.read_fwf("mainframe_data.txt", schema_path="fwf_schema.json")

# Read with inline field specifications
df = forklift.read_fwf(
    "simple_fwf.txt",
    field_specs=[
        {"name": "id", "start": 1, "length": 5},
        {"name": "name", "start": 6, "length": 20},
        {"name": "amount", "start": 26, "length": 10}
    ]
)
```

### Loading Data into Different DataFrame Libraries

Forklift's reader functions return a `DataFrameReader` object that can be converted to various dataframe formats with built-in optimization and validation.

```python
import forklift

# Read and convert to Pandas DataFrame
df_pandas = forklift.read_csv("sales_data.csv").as_pandas()
print(df_pandas.head())

# Read and convert to Polars DataFrame
df_polars = forklift.read_csv("sales_data.csv").as_polars()
print(df_polars.head())

# Read and convert to Polars LazyFrame for lazy evaluation
lf_polars = forklift.read_csv("large_dataset.csv").as_polars(lazy=True)
result = lf_polars.filter(pl.col("amount") > 100).collect()

# Read and convert to PyArrow Table
table_arrow = forklift.read_csv("sales_data.csv").as_pyarrow()
print(table_arrow.schema)
```

### DataFrame Conversion Examples by Format

#### CSV to Different Formats

```python
import forklift

# With schema validation during reading
reader = forklift.read_csv("customer_data.csv", schema_file="customer_schema.json")

# Convert to pandas for analysis
df_pandas = reader.as_pandas()
print(f"Pandas DataFrame: {df_pandas.shape}")
print(df_pandas.dtypes)

# Convert to polars for performance
df_polars = reader.as_polars()
print(f"Polars DataFrame: {df_polars.shape}")
print(df_polars.dtypes)

# Convert to pyarrow for columnar operations
table_arrow = reader.as_pyarrow()
print(f"PyArrow Table: {table_arrow.num_rows} rows, {table_arrow.num_columns} columns")
```

#### Excel to Different Formats

```python
import forklift

# Excel with sheet specification
reader = forklift.read_excel("financial_data.xlsx", sheet="Q4_Results")

# Convert to pandas (common for Excel analysis)
df = reader.as_pandas()
print(df.describe())

# Convert to polars for faster processing
df_polars = reader.as_polars()
aggregated = df_polars.group_by("category").agg([
    pl.col("amount").sum().alias("total_amount"),
    pl.col("amount").mean().alias("avg_amount")
])
```

#### Fixed-Width Files to Different Formats

```python
import forklift

# FWF processing with multi-record support
reader = forklift.read_fwf("mainframe_export.txt", schema_file="fwf_schema.json")

# Convert to polars for efficient processing
df_polars = reader.as_polars()
print(f"Processed {df_polars.height} records")

# Convert to pandas for compatibility with existing workflows
df_pandas = reader.as_pandas()
df_pandas.to_csv("converted_from_fwf.csv", index=False)
```

### Lazy Processing with Polars

```python
import forklift
import polars as pl

# Read large file with lazy evaluation
reader = forklift.read_csv("very_large_file.csv")
lazy_frame = reader.as_polars(lazy=True)

# Chain operations without loading full dataset
result = (
    lazy_frame
    .filter(pl.col("status") == "active")
    .with_columns([
        pl.col("amount").cast(pl.Float64),
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    ])
    .group_by("category")
    .agg([
        pl.col("amount").sum().alias("total"),
        pl.col("amount").count().alias("count")
    ])
    .sort("total", descending=True)
    .collect()  # Execute the lazy operations
)

print(result)
```

### Working with Spark (via PyArrow)

```python
import forklift
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("ForkliftData").getOrCreate()

# Read data through Forklift and convert to PyArrow
reader = forklift.read_csv("large_dataset.csv", schema_file="schema.json")
arrow_table = reader.as_pyarrow()

# Convert PyArrow table to Spark DataFrame
# Note: Requires PyArrow integration in Spark
spark_df = spark.createDataFrame(arrow_table.to_pandas())

# Or use arrow integration (if available)
# spark_df = spark.createDataFrame(arrow_table.to_pylist())

print(f"Spark DataFrame with {spark_df.count()} rows")
spark_df.show(5)

# Process with Spark
result = spark_df.groupBy("category").agg({"amount": "sum", "id": "count"})
result.show()
```

### Advanced DataFrame Integration

#### Custom Processing Pipeline

```python
import forklift
import polars as pl

def process_sales_data(file_path: str, schema_path: str):
    """Process sales data with validation and return clean dataframe."""
    
    # Read with validation
    reader = forklift.read_csv(file_path, schema_file=schema_path)
    
    # Convert to polars for efficient processing
    df = reader.as_polars()
    
    # Clean and transform data
    cleaned_df = (
        df
        .with_columns([
            # Clean currency columns
            pl.col("amount").str.replace_all(r"[$,]", "").cast(pl.Float64),
            # Standardize dates
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
            # Add calculated columns
            (pl.col("amount") * pl.col("tax_rate")).alias("tax_amount")
        ])
        .filter(pl.col("amount") > 0)  # Remove invalid amounts
        .sort("date")
    )
    
    return cleaned_df

# Use the pipeline
sales_df = process_sales_data("sales.csv", "sales_schema.json")
print(f"Processed {sales_df.height} valid sales records")
```

#### Memory-Efficient Large File Processing

```python
import forklift
import polars as pl

def process_large_file_efficiently(file_path: str):
    """Process large files using lazy evaluation."""
    
    # Read with forklift validation, convert to lazy polars
    reader = forklift.read_csv(file_path)
    lazy_df = reader.as_polars(lazy=True)
    
    # Define processing pipeline (not executed yet)
    pipeline = (
        lazy_df
        .filter(pl.col("status").is_in(["active", "pending"]))
        .with_columns([
            pl.col("created_date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("amount").cast(pl.Float64)
        ])
        .group_by([pl.col("created_date").dt.year(), "category"])
        .agg([
            pl.col("amount").sum().alias("total_amount"),
            pl.col("id").count().alias("record_count")
        ])
    )
    
    # Execute only when needed
    result = pipeline.collect()
    return result

# Process without loading full file into memory
summary = process_large_file_efficiently("huge_dataset.csv")
print(summary)
```

### Chaining with Data Analysis Libraries

```python
import forklift
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns

# Read and process data
df = forklift.read_csv("sales_data.csv").as_polars()

# Quick polars analysis
summary = df.group_by("region").agg([
    pl.col("sales").sum().alias("total_sales"),
    pl.col("sales").mean().alias("avg_sales")
])

# Convert to pandas for visualization
pandas_df = summary.to_pandas()

# Create visualization
plt.figure(figsize=(10, 6))
sns.barplot(data=pandas_df, x="region", y="total_sales")
plt.title("Sales by Region")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

## Validation and Error Handling

### Basic Validation

```python
import forklift

# Import with validation (default: log errors and continue)
results = forklift.import_csv(
    source="customer_data.csv",
    destination="./output/",
    schema_path="customer_schema.json"
)

# Check for validation issues
if results.invalid_rows > 0:
    print(f"Found {results.invalid_rows} invalid rows")
    print("Check the bad_rows file for details")
```

### Error Handling Modes

```python
import forklift
from forklift.engine.forklift_core import ImportConfig, ErrorHandlingMode

# Fail fast on first error
config = ImportConfig(
    error_handling_mode=ErrorHandlingMode.FAIL_FAST
)
results = forklift.import_csv(
    source="data.csv",
    destination="./output/",
    schema_path="schema.json",
    config=config
)

# Collect all errors then fail
config = ImportConfig(
    error_handling_mode=ErrorHandlingMode.FAIL_COMPLETE
)

# Continue processing, save bad rows to separate file
config = ImportConfig(
    error_handling_mode=ErrorHandlingMode.BAD_ROWS,
    bad_rows_path="./bad_rows/"
)
```

### Constraint Validation

```python
import forklift
from forklift.processors.constraint_validator import ConstraintConfig

# Configure constraint validation
constraint_config = ConstraintConfig(
    enforce_primary_key=True,
    enforce_unique_constraints=True,
    enforce_not_null=True
)

results = forklift.import_csv(
    source="users.csv",
    destination="./output/",
    schema_path="users_schema.json",
    constraint_config=constraint_config
)
```

## Working with Different File Formats

### CSV Files

```python
import forklift

# Basic CSV processing
df = forklift.read_csv("data.csv")

# CSV with custom options
df = forklift.read_csv(
    "custom_data.csv",
    delimiter=";",
    encoding="utf-8-sig",
    header_mode="present"
)

# Import CSV with preprocessing
results = forklift.import_csv(
    source="raw_data.csv",
    destination="./cleaned/",
    preprocessors=["string_cleaning", "date_parsing"]
)
```

### Excel Files

```python
import forklift

# Read multiple sheets
sheets = ["Sales", "Marketing", "Finance"]
dataframes = {}
for sheet in sheets:
    dataframes[sheet] = forklift.read_excel("quarterly_data.xlsx", sheet_name=sheet)

# Import Excel with sheet specification
results = forklift.import_excel(
    source="financial_data.xlsx",
    destination="./output/",
    sheet_name="Transactions",
    schema_path="transaction_schema.json"
)
```

### Fixed-Width Files

```python
import forklift

# FWF with multi-schema support (different record types)
results = forklift.import_fwf(
    source="complex_fwf.txt",
    destination="./output/",
    schema_path="multi_schema.json"  # Contains multiple record type definitions
)

# FWF with flag-based record identification
df = forklift.read_fwf(
    "variable_fwf.txt",
    schema_path="flag_based_schema.json"
)
```

## S3 Integration

### Reading from S3

```python
import forklift

# Read CSV from S3
df = forklift.read_csv("s3://my-bucket/data/sales.csv")

# Import from S3 to local
results = forklift.import_csv(
    source="s3://source-bucket/raw/data.csv",
    destination="./local_output/"
)

# Import from S3 to S3
results = forklift.import_csv(
    source="s3://source-bucket/raw/data.csv",
    destination="s3://dest-bucket/processed/"
)
```

### Schema Generation from S3

```python
import forklift

# Generate schema from S3 file
schema = forklift.generate_schema_from_csv("s3://my-bucket/sample/data.csv")

# Save schema to S3
forklift.generate_and_save_schema(
    input_path="s3://my-bucket/data/customers.csv",
    output_path="s3://my-bucket/schemas/customers_schema.json",
    file_type="csv"
)
```

## Advanced Features

### Custom Processors

```python
import forklift
from forklift.processors import BaseProcessor

# Use built-in processors
results = forklift.import_csv(
    source="messy_data.csv",
    destination="./clean/",
    preprocessors=[
        "string_cleaning",      # Clean whitespace, normalize text
        "date_standardization", # Standardize date formats
        "calculated_columns"    # Add computed fields
    ]
)
```

### Metadata Generation

```python
import forklift

# Generate schema with rich metadata
schema = forklift.generate_schema_from_csv(
    "customer_data.csv",
    include_metadata=True,
    enum_threshold=0.1,        # Suggest enums for low-cardinality fields
    uniqueness_threshold=0.95, # Flag highly unique fields
    top_n_values=10           # Include top/bottom values
)

# Access metadata
metadata = schema.get("x-metadata", {})
for field, field_metadata in metadata.items():
    print(f"{field}: {field_metadata.get('distinct_count')} unique values")
```

### Batch Processing

```python
import forklift
from pathlib import Path

# Process multiple files
data_dir = Path("./raw_data/")
output_dir = Path("./processed/")

for csv_file in data_dir.glob("*.csv"):
    print(f"Processing {csv_file.name}...")
    
    results = forklift.import_csv(
        source=str(csv_file),
        destination=str(output_dir),
        schema_path="common_schema.json"
    )
    
    print(f"Completed: {results.valid_rows} rows processed")
```

### Performance Optimization

```python
import forklift
from forklift.engine.forklift_core import ImportConfig

# Configure for large files
config = ImportConfig(
    batch_size=50000,         # Process in larger batches
    memory_limit_mb=2048,     # Increase memory limit
    use_streaming=True        # Enable streaming for very large files
)

results = forklift.import_csv(
    source="very_large_file.csv",
    destination="./output/",
    config=config
)
```

## CLI Workflows

### Basic CLI Usage

```bash
# Generate schema
forklift generate-schema data.csv --file-type csv --output file --output-path schema.json

# Import with validation
forklift ingest data.csv --dest ./output/ --input-kind csv --schema schema.json

# Import from S3
forklift ingest s3://bucket/data.csv --dest s3://bucket/processed/ --input-kind csv
```

### Advanced CLI Options

```bash
# Schema generation with options
forklift generate-schema large_file.csv \
  --file-type csv \
  --nrows 10000 \
  --infer-primary-key \
  --include-sample \
  --output file \
  --output-path detailed_schema.json

# Import with preprocessing
forklift ingest messy_data.csv \
  --dest ./clean/ \
  --input-kind csv \
  --pre string_cleaning date_standardization \
  --header-mode present \
  --encoding-priority utf-8 latin-1
```

This usage guide provides comprehensive examples for most common Forklift workflows. For specific API details, see the [API Reference](API_REFERENCE.md).
