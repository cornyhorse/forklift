# Forklift Schema Package

The Forklift Schema package is a comprehensive data analysis and schema generation system that forms the foundation of Forklift's intelligent data processing capabilities. This package analyzes data files to automatically generate standardized JSON Schema definitions with Forklift-specific extensions for data validation, processing configuration, and metadata enrichment.

## Overview

The schema package serves as the intelligence layer of Forklift, bridging the gap between raw data files and structured, validated data processing workflows. It automatically discovers data patterns, infers types, detects relationships, and generates comprehensive schema definitions that drive the entire Forklift processing pipeline.

### Key Capabilities

- **Intelligent Data Analysis**: Automatically analyzes CSV, Excel, and Parquet files to understand data patterns, types, and relationships
- **Standards-Compliant Schema Generation**: Produces JSON Schema Draft 2020-12 compliant schemas with Forklift extensions
- **Metadata Enrichment**: Generates statistical metadata including null statistics, distinct counts, uniqueness analysis and string-length profiles. Statistics that embed raw cell values (top/bottom values, enum value lists, min/max/median/quantiles) are opt-in via `include_value_statistics`
- **Primary Key Detection**: Automatically identifies potential primary keys and unique constraints
- **Special Type Detection**: Recognizes common data patterns like SSNs, ZIP codes, phone numbers, and email addresses
- **Configuration Generation**: Creates file format-specific processing configurations (CSV delimiters, Excel sheets, etc.)

## Architecture

The schema package follows a modular architecture with clear separation of concerns:

```
forklift/schema/
├── generator/          # Core schema generation orchestration
│   ├── core.py        # Main SchemaGenerator class and configuration
│   ├── inference.py   # Data type inference and analysis
│   └── validation.py  # Schema validation and constraint checking
├── processors/        # Specialized processing modules
│   ├── json_schema.py # JSON Schema generation and formatting
│   ├── metadata.py    # Statistical metadata generation
│   └── config_parser.py # Configuration file processing
├── types/            # Data type handling and conversion
│   ├── data_types.py # PyArrow type conversion and mapping
│   └── special_types.py # Special pattern detection (SSN, ZIP, etc.)
├── utils/            # Utility functions
│   └── formatters.py # Schema output formatting
└── fwf/              # Fixed-width file specific components
```

## Core Components

### SchemaGenerator (generator/core.py)

The main orchestrator that coordinates all schema generation activities:

```python
from forklift.schema import SchemaGenerator, SchemaGenerationConfig, FileType, OutputTarget

config = SchemaGenerationConfig(
    input_path="data.csv",
    file_type=FileType.CSV,
    nrows=1000,  # Analyze first 1000 rows
    output_target=OutputTarget.FILE,
    output_path="schema.json",
    infer_primary_key_from_metadata=True,
    include_value_statistics=False,  # default: no raw cell values in the schema
)

generator = SchemaGenerator(config)
schema = generator.generate_schema()
```

**Key Features:**
- Configurable data sampling for large files (`nrows`; `None` analyses the whole file)
- Multiple output targets (stdout, file, clipboard)
- Flexible file type support
- Primary key inference capabilities

### Data Type Inference (generator/inference.py)

Samples are read with PyArrow only (no pandas):

- **CSV**: streamed with `pyarrow.csv.open_csv` as all-string columns and closed after `nrows` rows (local files and `s3://` objects alike, honouring the configured encoding and quoted newlines). Forklift's own type inference then runs over the strings, so the result depends only on the rows that were sampled, not on how Arrow happened to block the file (a file with fewer rows than `nrows` gives the same schema for any `nrows`, including `None`; `nrows` must be a positive integer or `None`, anything else raises `ValueError`):
  - `integer` only for `^-?(0|[1-9]\d*)$`, so identifiers such as `02134` or `00123` stay strings (values beyond 64 bits stay strings too)
  - `number`, `boolean` (`true`/`false`), `date` (`YYYY-MM-DD`, becomes `date32` / `format: date`) and `timestamp` (`YYYY-MM-DD[T ]HH:MM[:SS[.f]]`, optionally with a UTC offset, becomes `timestamp[unit]` / `format: date-time`) are detected from patterns; everything else is `string`
  - Parquet type strings keep their parameters: `decimal128(10,2)`, `timestamp[us, tz=UTC]`, `duration[ns]`, `list<int64>`
  - nulls are the empty string and `NULL`, `null`, `N/A`, `n/a`, `#N/A`, `NaN`, `nan`. `NA` is deliberately *not* a null token (it is a valid value)
- **Excel**: `openpyxl` in read-only mode, limited to `nrows + 1` rows; cells keep their Excel types. Legacy `.xls` workbooks are not supported
- **Parquet**: only as many row groups as `nrows` requires are read; the file's own types are kept
- Only local paths and `s3://` URIs are accepted: `http://`, `ftp://`, `file://` and other URL-like inputs raise `ValueError`
- A primary key is only inferred for a column that is non-null, 100% unique in the sample and has a key-like name (a whole word token such as `id`, `key`, `pk`, `uuid`)

### Metadata Generation (processors/metadata.py)

Generates a statistical profile for each column with `pyarrow.compute`. By default the profile contains **no raw cell values**: counts, null/NaN statistics, type information, distinct counts and uniqueness ratios, mean/standard deviation/variance, outlier counts and string-length statistics.

```json
{
  "x-metadata": {
    "column_metadata": {
      "status": {
        "name": "status",
        "parquet_type": "string",
        "null_count": 12,
        "distinct_count": 3,
        "uniqueness_ratio": 0.003,
        "min_length": 6,
        "max_length": 8
      },
      "score": {
        "null_count": 0,
        "distinct_count": 90,
        "mean": 55.5,
        "std_dev": 12.1,
        "variance": 146.4
      }
    },
    "enum_suggestions": {
      "status": {"is_enum_candidate": true, "distinct_count": 3, "confidence": "high"}
    }
  }
}
```

With `include_value_statistics=True` the profile additionally contains `top_values`, `bottom_values`, `suggested_enum_values`, `min_value`, `max_value`, `median`, `range` and `quantiles` (`quantile_25`, `quantile_99_5`, ...). Those fields copy cell values into the schema, so enable them only for data that may be shared. Sample rows (`x-sample`) are controlled separately by `include_sample_data`. Undefined statistics (for example the standard deviation of a single row) are `null`; the output is strict JSON without `NaN`/`Infinity`. The source file is recorded by file name only.

### Special Type Detection (types/special_types.py)

`SpecialTypeDetector.detect_special_type(column_name, sample_values)` recognises SSNs, phone numbers, email addresses, ZIP codes, IP addresses and MAC addresses:

- **Column names** are matched on whole word tokens (split on non-alphanumerics and camelCase), so `client_ip`, `userEmail` and `zipCode` match while `description`, `ship_date`, `tip`, `hotel` or `machine` do not
- **Content** must match the whole value and be unambiguous: SSNs and phone numbers need their separators (`123-45-6789`, `(123) 456-7890`), so 9-digit ids, 10-digit epoch timestamps and 5-digit counts are *not* classified from content alone (a `zip`/`ssn`/`phone` column name still is)

## Integration with Forklift Core

The schema package is deeply integrated with Forklift's core processing engine:

### Data Import Pipeline

1. **Schema Generation**: Analyzes input files to create processing schemas
2. **Validation Configuration**: Generated schemas drive validation rules
3. **Type Conversion**: Schema data types guide PyArrow type mapping
4. **Error Handling**: Schema constraints determine validation behavior

### Processing Configuration

Generated schemas include a format-specific extension (`x-csv`, `x-excel`) that documents the layout that was analysed:

```json
{
  "x-csv": {
    "encodingPriority": ["utf-8", "utf-8-sig", "utf-8", "latin-1"],
    "delimiter": ",",
    "quotechar": "\"",
    "nulls": {
      "global": ["", "NA", "N/A", "-", "NULL", "null"],
      "perColumn": {}
    },
    "dataTypes": {
      "customer_id": "int64",
      "name": "string",
      "signup_date": "date32"
    }
  }
}
```

`forklift.import_csv()` takes its read settings (`delimiter`, `encoding`, `header_mode`, ...) from `ImportConfig`, not from `x-csv`; from a schema file it uses the column types (`x-csv.parquetTypeMapping`, otherwise each property's JSON `type`/`format`), `x-csv.nulls`, `required` and `x-metadata-generation`. The generated `nulls.global` list is a suggestion: it includes `NA`, which the sampler itself does not treat as null.

### Validation Framework

Schemas provide the foundation for Forklift's multi-layer validation:

- **Type Validation**: Ensures data conforms to inferred types
- **Constraint Validation**: Enforces primary keys, unique constraints, and not-null rules
- **Format Validation**: Validates special patterns (emails, phone numbers, etc.)
- **Range Validation**: Checks numeric and date ranges

## Schema Standards Compliance

The schema package generates schemas that follow the [Forklift Schema Standards](../../../docs/schemas/SCHEMA_STANDARDS.md):

### Base JSON Schema

All schemas follow JSON Schema Draft 2020-12 specification with proper `$schema`, `$id`, and standard validation keywords.

### Forklift Extensions

Custom `x-` prefixed properties provide Forklift-specific functionality:

- **x-primaryKey**: Primary key definitions and constraints (user-specified or inferred)
- **x-metadata**: Statistical metadata for each field (value statistics only with `include_value_statistics`)
- **x-csv/x-excel**: Format-specific processing configurations
- **x-transformations**: Suggested cleaning steps per column (`column_transformations`)
- **x-generation**: When and from which file (name only) the schema was generated
- **x-sample**: Sample rows, only with `include_sample_data`

`x-uniqueConstraints` and the other constraint/quality extensions are part of the schema standard but are not generated.

## Usage Patterns

### Command Line Interface

```bash
# Generate schema from CSV (the whole file is analysed unless --nrows is given)
forklift generate-schema data.csv --file-type csv --output file --output-path schema.json

# Generate with primary key inference and value statistics (they copy cell values into the schema)
forklift generate-schema data.csv --file-type csv --infer-primary-key --include-value-stats \
  --output file --output-path schema.json

# Analyze Excel file with specific sheet (name)
forklift generate-schema data.xlsx --file-type excel --sheet "CustomerData" \
  --output file --output-path schema.json
```

### Programmatic Usage

```python
# Basic schema generation
from forklift.schema import SchemaGenerator, SchemaGenerationConfig, FileType

config = SchemaGenerationConfig(
    input_path="data.csv",
    file_type=FileType.CSV
)
generator = SchemaGenerator(config)
schema = generator.generate_schema()

# Advanced usage with custom configuration
config = SchemaGenerationConfig(
    input_path="s3://bucket/data.csv",
    file_type=FileType.CSV,
    nrows=5000,
    generate_metadata=True,
    infer_primary_key_from_metadata=True,
    enum_threshold=0.05,
    uniqueness_threshold=0.98
)
schema = SchemaGenerator(config).generate_schema()
```

## Performance and Scalability

The schema package is designed for efficient analysis of large datasets:

- **Streaming Analysis**: CSV, Excel and Parquet samples are streamed with PyArrow/openpyxl and reading stops after `nrows` rows
- **Configurable Sampling**: Analyzes the first `nrows` rows (default 1000 for `SchemaGenerationConfig` and `generate_schema_from_excel`; the `generate_schema_from_csv`/`generate_schema_from_parquet` API functions and the CLI default to `None`, i.e. the whole file)
- **S3 Integration**: Supports direct analysis of cloud-stored files; CSV is streamed and the read stops after `nrows` rows, Parquet and Excel are first copied to a temporary local file because they need random access

## Quality Assurance

The schema generation process includes multiple quality checks:

- **Data Quality Metrics**: Calculates completeness, uniqueness, and distribution metrics
- **Constraint Detection**: Identifies potential primary keys and unique constraints
- **Format Validation**: Ensures generated schemas are valid JSON Schema
- **Consistency Checks**: Validates that inferred types match actual data patterns

## Related Documentation

- **[Schema Standards](../../../docs/schemas/SCHEMA_STANDARDS.md)**: Complete specification of Forklift schema format
- **[Usage Guide](../../../docs/guides/USAGE.md)**: Comprehensive examples and workflows
- **[API Reference](../../../docs/api/API_REFERENCE.md)**: Detailed API documentation
- **[Constraint Validation](../../../docs/integration/CONSTRAINT_VALIDATION_IMPLEMENTATION.md)**: Validation system details

## Examples

### Basic CSV Analysis

```python
from forklift.schema import SchemaGenerator, SchemaGenerationConfig, FileType

# Analyze a customer data file
config = SchemaGenerationConfig(
    input_path="customers.csv",
    file_type=FileType.CSV,
    nrows=1000,
    generate_metadata=True
)

generator = SchemaGenerator(config)
schema = generator.generate_schema()

# Schema will include:
# - Inferred data types for all columns
# - Statistical metadata (null counts, distinct counts, string lengths; value statistics only
#   with include_value_statistics=True)
# - Detected special types (emails, phone numbers)
# - CSV processing configuration
```

### Excel Multi-Sheet Analysis

```python
# Analyze specific Excel sheet
config = SchemaGenerationConfig(
    input_path="financial_data.xlsx",
    file_type=FileType.EXCEL,
    sheet_name="Q1_Sales",
    infer_primary_key_from_metadata=True
)

schema = SchemaGenerator(config).generate_schema()

# Schema will include:
# - Excel-specific processing configuration
# - Primary key inference based on uniqueness analysis
# - Rich metadata for numerical and categorical columns
```

The Forklift Schema package transforms raw data analysis into actionable, standardized schema definitions that power the entire Forklift data processing ecosystem. By combining intelligent inference with comprehensive metadata generation, it enables automated, reliable, and scalable data processing workflows.
