# Forklift

A powerful data import and schema generation tool with PyArrow streaming, validation, and S3 support.

![Forklift Logo](FORKLIFT.png)

## Overview

Forklift is a comprehensive data processing tool that provides:

- **High-performance data import** with PyArrow streaming for CSV, Excel, FWF, and SQL sources
- **Intelligent schema generation** that analyzes your data and creates standardized schema definitions
- **Robust validation** with configurable error handling and reporting
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
- **Intelligent schema inference** from sample data (default: 1000 rows)
- **Privacy-first approach** - no sensitive sample data included by default
- **Multiple file format support** - CSV, Excel, Parquet
- **Flexible output options** - stdout, file, or clipboard
- **Standards-compliant schemas** following Forklift schema-standards format

### 🛡️ **Validation & Quality**
- JSON Schema validation with custom extensions
- Primary key inference and enforcement
- Data type validation and conversion
- Configurable null handling and error thresholds
- Detailed processing reports and manifests

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
results = forklift.import_csv(
    input_path="data.csv",
    output_path="./output/",
    schema_file="schema.json"
)

print(f"Processed {results.total_rows} rows")
print(f"Valid: {results.valid_rows}, Invalid: {results.invalid_rows}")
```

### Schema Generation

```python
import forklift

# Generate schema from CSV (analyzes first 1000 rows by default)
schema = forklift.generate_schema_from_csv("data.csv")

# Save schema to file
forklift.generate_and_save_schema(
    input_path="data.csv",
    output_path="schema.json",
    file_type="csv"
)

# Generate with sample data for development (opt-in for privacy)
schema = forklift.generate_schema_from_csv(
    "data.csv", 
    include_sample_data=True
)
```

## CLI Usage

### Data Import

```bash
# Import CSV with schema validation
forklift ingest data.csv --dest ./output/ --input-kind csv --schema schema.json

# Import from S3
forklift ingest s3://bucket/data.csv --dest s3://bucket/output/ --input-kind csv
```

### Schema Generation

```bash
# Generate schema (1000 rows, no sample data - privacy-safe default)
forklift generate-schema data.csv --file-type csv

# Generate with custom row limit
forklift generate-schema data.csv --file-type csv --nrows 5000

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
```

## Schema Standards

Forklift generates schemas that follow a standardized format with powerful extensions:

### Base JSON Schema Structure

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/20250903-csv.json",
  "title": "Forklift CSV Schema - Generated",
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "email": {"type": "string", "format": "email"}
  },
  "required": ["id", "name"]
}
```

### Forklift Extensions

#### `x-primaryKey` - Primary Key Configuration
```json
{
  "x-primaryKey": {
    "description": "Primary key configuration",
    "columns": ["id"],
    "type": "single",
    "enforceUniqueness": true,
    "allowNulls": false
  }
}
```

#### `x-csv` - CSV Processing Configuration
```json
{
  "x-csv": {
    "encodingPriority": ["utf-8", "utf-8-sig", "latin-1"],
    "delimiter": ",",
    "quotechar": "\"",
    "header": {"mode": "present"},
    "nulls": {
      "global": ["", "NA", "NULL"],
      "perColumn": {"salary": ["", "0.00"]}
    },
    "dataTypes": {
      "id": "int64",
      "name": "string",
      "salary": "double"
    },
    "validation": {
      "enabled": true,
      "onError": "log",
      "maxErrors": 1000
    }
  }
}
```

#### `x-excel` - Excel Processing Configuration
```json
{
  "x-excel": {
    "sheet": "Sheet1",
    "header": {"mode": "present"},
    "skipRows": 0,
    "nulls": {"global": ["", "NA", "NULL"]}
  }
}
```

#### `x-sample` - Sample Data (Optional)
⚠️ **Privacy Note**: Sample data is NOT included by default to protect sensitive information.

```json
{
  "x-sample": {
    "description": "Sample data from first 3 rows",
    "rows": [
      {"id": 1, "name": "John", "email": "john@example.com"},
      {"id": 2, "name": "Jane", "email": "jane@example.com"}
    ]
  }
}
```

To include sample data, explicitly request it:
- **CLI**: `--include-sample`
- **API**: `include_sample_data=True`

#### `x-generation` - Generation Metadata
```json
{
  "x-generation": {
    "generated_at": "2025-09-03T17:06:09.795225",
    "source_file": "data.csv",
    "rows_analyzed": 1000,
    "generator_version": "1.0.0"
  }
}
```

#### `x-transformations` - Data Transformation Configuration
⚠️ **New Feature**: Comprehensive data cleaning and transformation capabilities.

**🛡️ Safety-First Approach: "Suggested but Disabled by Default"**

When Forklift analyzes your data, it intelligently detects patterns and suggests appropriate transformations, but **all suggestions are disabled by default** for maximum safety:

- **🔍 Smart Detection**: Automatically identifies money formats, HTML tags, numeric separators, etc.
- **🛡️ No Auto-Changes**: Your data is never modified without explicit permission
- **✅ Easy Activation**: Simply change `"enabled": false` to `"enabled": true` when ready
- **🚀 Production Safe**: Generated schemas won't accidentally transform data in production

**Example: Money Detection**
```json
// Schema generator detects: "$1,234.56", "(500.00)", "€99.99"
{
  "price": {
    "money_conversion": {
      "enabled": false,  // ← SAFE: Detected but won't run automatically
      "currency_symbols": ["$", "€"],
      "parentheses_negative": true,
      "thousands_separator": ","
      // All parameters pre-configured based on your actual data!
    }
  }
}
```

**To Use the Transformation:**
```json
{
  "price": {
    "money_conversion": {
      "enabled": true,   // ← NOW it will convert "$100" to 100.0
      "currency_symbols": ["$", "€"],
      "parentheses_negative": true
    }
  }
}
```

This approach gives you **intelligent assistance without risk** - like having a smart assistant that says *"I noticed your data has these patterns and prepared transformations for you, but I won't apply them unless you explicitly say so."*

```json
````

## Configuration Options

### Schema Generation Configuration

```python
from forklift.schema.schema_generator import SchemaGenerationConfig, FileType, OutputTarget

config = SchemaGenerationConfig(
    input_path="data.csv",
    file_type=FileType.CSV,
    nrows=1000,                    # Default: 1000 rows
    output_target=OutputTarget.STDOUT,
    delimiter=",",
    encoding="utf-8",
    include_sample_data=False,     # Default: False (privacy-safe)
    infer_primary_key=True
)
```

### Import Configuration

```python
from forklift.engine.forklift_core import ImportConfig, HeaderMode

config = ImportConfig(
    input_path="data.csv",
    output_path="./output/",
    schema_file="schema.json",
    batch_size=10000,
    encoding="utf-8",
    header_mode=HeaderMode.PRESENT,
    validate_schema=True,
    create_manifest=True,
    create_metadata=True
)
```

## API Reference

### Schema Generation Functions

```python
# CSV schema generation
schema = forklift.generate_schema_from_csv(
    input_path: str,
    nrows: int = 1000,
    delimiter: str = ",",
    encoding: str = "utf-8",
    include_sample_data: bool = False,
    infer_primary_key: bool = True
)

# Excel schema generation
schema = forklift.generate_schema_from_excel(
    input_path: str,
    nrows: int = 1000,
    sheet_name: str = None,
    include_sample_data: bool = False,
    infer_primary_key: bool = True
)

# Parquet schema generation
schema = forklift.generate_schema_from_parquet(
    input_path: str,
    nrows: int = 1000,
    include_sample_data: bool = False,
    infer_primary_key: bool = True
)

# Convenience functions
forklift.generate_and_save_schema(input_path, output_path, file_type, **kwargs)
forklift.generate_and_copy_schema(input_path, file_type, **kwargs)
```

### Data Import Functions

```python
# Import functions
results = forklift.import_csv(input_path, output_path, schema_file=None, **kwargs)
results = forklift.import_excel(input_path, output_path, schema_file=None, **kwargs)
results = forklift.import_fwf(input_path, output_path, schema_file=None, **kwargs)
results = forklift.import_sql(connection, query, output_path, **kwargs)

# DataFrame readers (for ad-hoc analysis)
df = forklift.read_csv(input_path, **kwargs)
df = forklift.read_excel(input_path, **kwargs)
df = forklift.read_fwf(input_path, **kwargs)
df = forklift.read_sql(connection, query, **kwargs)
```

## S3 Integration

Forklift provides seamless S3 integration for both input and output:

```python
# Read from S3, write to local
results = forklift.import_csv(
    input_path="s3://bucket/data.csv",
    output_path="./local_output/"
)

# Read from local, write to S3
results = forklift.import_csv(
    input_path="data.csv",
    output_path="s3://bucket/output/"
)

# Full S3 workflow
results = forklift.import_csv(
    input_path="s3://input-bucket/data.csv",
    output_path="s3://output-bucket/processed/",
    schema_file="s3://config-bucket/schemas/data-schema.json"
)

# Schema generation from S3
schema = forklift.generate_schema_from_csv("s3://bucket/data.csv")
```

## Privacy & Security

### Sample Data Protection

Forklift takes a **privacy-first approach** to schema generation:

- **Default behavior**: No sample data included in generated schemas
- **Explicit opt-in**: Use `--include-sample` or `include_sample_data=True` when needed
- **Development workflow**: Include sample data only during development/testing

### Best Practices

1. **Production schemas**: Always use default settings (no sample data)
2. **Development schemas**: Explicitly request sample data when needed for testing
3. **Sensitive data**: Never commit schemas with sample data to version control
4. **Row limits**: Use appropriate `nrows` values to balance accuracy vs. performance

## Output Files

When processing data, Forklift creates comprehensive output:

### Parquet Files
- High-performance columnar format
- Optimized for analytics workloads
- Schema embedded in file metadata

### Manifest Files
```json
{
  "processing_summary": {
    "total_rows": 10000,
    "valid_rows": 9995,
    "invalid_rows": 5,
    "start_time": "2025-09-03T17:00:00Z",
    "end_time": "2025-09-03T17:00:30Z"
  },
  "output_files": ["data.parquet"],
  "schema_file": "schema.json"
}
```

### Metadata Files
```json
{
  "source": {
    "file_path": "data.csv",
    "file_size": 1048576,
    "encoding": "utf-8"
  },
  "processing": {
    "batch_size": 10000,
    "validation_enabled": true,
    "error_threshold": 1000
  },
  "schema": {
    "version": "1.0.0",
    "columns": 15,
    "primary_key": ["id"]
  }
}
```

## Error Handling

Forklift provides comprehensive error handling and reporting:

### Validation Errors
- Detailed error messages with row numbers
- Configurable error thresholds
- Continue processing or fail-fast options

### File Format Errors
- Encoding detection and fallback
- Malformed data handling
- Graceful degradation for partial files

### Example Error Handling

```python
from forklift import import_csv
from forklift.engine.forklift_core import ProcessingError

try:
    results = import_csv("data.csv", "./output/", validate_schema=True)
    
    if results.invalid_rows > 0:
        print(f"Warning: {results.invalid_rows} rows failed validation")
        # Check error details in manifest file
        
except ProcessingError as e:
    print(f"Processing failed: {e}")
except FileNotFoundError:
    print("Input file not found")
```

## Performance Considerations

### Row Sampling for Schema Generation

- **Default**: 1000 rows provides good balance of accuracy vs. speed
- **Large files**: Consider smaller samples (500-2000 rows) for very large datasets
- **Small files**: Full file analysis when under 1000 rows
- **Complex data**: Increase sample size for files with high variability

### Memory Management

- **Batch processing**: Default 10,000 rows per batch
- **Large files**: Reduce batch size if memory constrained
- **Streaming**: PyArrow streaming keeps memory usage constant

### S3 Optimization

- **Regional placement**: Keep data and compute in same AWS region
- **Parallel uploads**: Forklift automatically optimizes S3 transfers
- **Compression**: Use Snappy compression for Parquet output (default)

## Contributing

Forklift is open source and welcomes contributions:

1. **Issues**: Report bugs or request features
2. **Pull requests**: Submit improvements or fixes
3. **Documentation**: Help improve guides and examples
4. **Testing**: Add test cases for edge cases

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.

---

**Need help?** Check out the [documentation](docs/) or open an issue on GitHub.
