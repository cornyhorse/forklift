# Forklift Schema Package

The Forklift Schema package is a comprehensive data analysis and schema generation system that forms the foundation of Forklift's intelligent data processing capabilities. This package analyzes data files to automatically generate standardized JSON Schema definitions with Forklift-specific extensions for data validation, processing configuration, and metadata enrichment.

## Overview

The schema package serves as the intelligence layer of Forklift, bridging the gap between raw data files and structured, validated data processing workflows. It automatically discovers data patterns, infers types, detects relationships, and generates comprehensive schema definitions that drive the entire Forklift processing pipeline.

### Key Capabilities

- **Intelligent Data Analysis**: Automatically analyzes CSV, Excel, and Parquet files to understand data patterns, types, and relationships
- **Standards-Compliant Schema Generation**: Produces JSON Schema Draft 2020-12 compliant schemas with Forklift extensions
- **Metadata Enrichment**: Generates rich statistical metadata including value distributions, uniqueness analysis, and data quality metrics
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
    infer_primary_key_from_metadata=True
)

generator = SchemaGenerator(config)
schema = generator.generate_schema()
```

**Key Features:**
- Configurable data sampling for large files
- Multiple output targets (stdout, file, clipboard)
- Flexible file type support
- Primary key inference capabilities

### Data Type Inference (generator/inference.py)

Sophisticated type inference that goes beyond basic type detection:

- **Numeric Types**: Distinguishes between integers, floats, and discovers optimal bit widths
- **Temporal Types**: Detects date/datetime patterns and formats
- **String Analysis**: Identifies categorical vs. free-text fields
- **Special Patterns**: Recognizes structured data like emails, phone numbers, SSNs

### Metadata Generation (processors/metadata.py)

Generates comprehensive statistical profiles for each column:

```json
{
  "x-metadata": {
    "customer_id": {
      "distinct_count": 1247,
      "null_count": 0,
      "min_value": 1,
      "max_value": 1247,
      "is_potentially_unique": true,
      "uniqueness_score": 1.0
    },
    "status": {
      "distinct_count": 3,
      "null_count": 12,
      "value_counts": {
        "active": 890,
        "inactive": 245,
        "pending": 100
      },
      "suggested_enum": true
    }
  }
}
```

### Special Type Detection (types/special_types.py)

Automatically identifies common data patterns:

- **Government IDs**: SSNs, EINs, driver's license numbers
- **Geographic Data**: ZIP codes, postal codes, state abbreviations
- **Contact Information**: Phone numbers, email addresses
- **Financial Data**: Credit card numbers, bank account numbers
- **Web Data**: URLs, IP addresses

## Integration with Forklift Core

The schema package is deeply integrated with Forklift's core processing engine:

### Data Import Pipeline

1. **Schema Generation**: Analyzes input files to create processing schemas
2. **Validation Configuration**: Generated schemas drive validation rules
3. **Type Conversion**: Schema data types guide PyArrow type mapping
4. **Error Handling**: Schema constraints determine validation behavior

### Processing Configuration

Generated schemas include format-specific processing instructions:

```json
{
  "x-csv": {
    "delimiter": ",",
    "encoding": "utf-8",
    "nulls": {
      "global": ["", "NA", "NULL"],
      "perColumn": {
        "salary": ["0.00", "N/A"]
      }
    },
    "dataTypes": {
      "customer_id": "int64",
      "name": "string",
      "signup_date": "date32"
    }
  }
}
```

### Validation Framework

Schemas provide the foundation for Forklift's multi-layer validation:

- **Type Validation**: Ensures data conforms to inferred types
- **Constraint Validation**: Enforces primary keys, unique constraints, and not-null rules
- **Format Validation**: Validates special patterns (emails, phone numbers, etc.)
- **Range Validation**: Checks numeric and date ranges

## Schema Standards Compliance

The schema package generates schemas that fully comply with [Forklift Schema Standards](../docs/SCHEMA_STANDARDS.md):

### Base JSON Schema

All schemas follow JSON Schema Draft 2020-12 specification with proper `$schema`, `$id`, and standard validation keywords.

### Forklift Extensions

Custom `x-` prefixed properties provide Forklift-specific functionality:

- **x-primaryKey**: Primary key definitions and constraints
- **x-uniqueConstraints**: Additional unique constraint definitions
- **x-metadata**: Rich statistical metadata for each field
- **x-csv/x-excel**: Format-specific processing configurations

## Usage Patterns

### Command Line Interface

```bash
# Generate schema from CSV
forklift schema generate data.csv --output schema.json

# Generate with metadata and primary key inference
forklift schema generate data.csv --metadata --infer-pk --output schema.json

# Analyze Excel file with specific sheet
forklift schema generate data.xlsx --sheet "CustomerData" --output schema.json
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
schema = generator.generate_schema()
```

## Performance and Scalability

The schema package is designed for efficient analysis of large datasets:

- **Streaming Analysis**: Uses PyArrow for memory-efficient data sampling
- **Configurable Sampling**: Analyzes representative samples rather than entire files
- **S3 Integration**: Supports direct analysis of cloud-stored files
- **Batch Processing**: Processes data in configurable batch sizes

## Quality Assurance

The schema generation process includes multiple quality checks:

- **Data Quality Metrics**: Calculates completeness, uniqueness, and distribution metrics
- **Constraint Detection**: Identifies potential primary keys and unique constraints
- **Format Validation**: Ensures generated schemas are valid JSON Schema
- **Consistency Checks**: Validates that inferred types match actual data patterns

## Related Documentation

- **[Schema Standards](../docs/SCHEMA_STANDARDS.md)**: Complete specification of Forklift schema format
- **[Usage Guide](../docs/USAGE.md)**: Comprehensive examples and workflows
- **[API Reference](../docs/API_REFERENCE.md)**: Detailed API documentation
- **[Constraint Validation](../docs/CONSTRAINT_VALIDATION_IMPLEMENTATION.md)**: Validation system details

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
# - Statistical metadata (min, max, distinct counts)
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

schema = generator.generate_schema()

# Schema will include:
# - Excel-specific processing configuration
# - Primary key inference based on uniqueness analysis
# - Rich metadata for numerical and categorical columns
```

The Forklift Schema package transforms raw data analysis into actionable, standardized schema definitions that power the entire Forklift data processing ecosystem. By combining intelligent inference with comprehensive metadata generation, it enables automated, reliable, and scalable data processing workflows.
