# Forklift Schema Validator Package

## Overview

The `forklift.processors.schema_validator` package is a core component of the Forklift data processing pipeline that provides robust schema validation capabilities for PyArrow record batches. This package enables data quality assurance by validating incoming data against predefined schema definitions with configurable validation modes and constraint checking.

## Package Context in Forklift Application

Forklift is a comprehensive data processing and ETL tool that provides high-performance data import, schema generation, and validation capabilities. The schema validator package fits into the broader Forklift architecture as follows:

### Position in the Data Pipeline

```
Data Sources (CSV, Excel, FWF, SQL, S3) 
    ↓
Forklift Readers (forklift.readers)
    ↓
Processing Pipeline (forklift.processors)
    ├── Schema Validator ← **This Package**
    ├── Column Mappers
    ├── Transformations
    ├── Calculated Columns
    └── Data Quality Processors
    ↓
Output Writers (Parquet, S3, etc.)
```

### Integration Points

- **Schema Generation**: Works with `forklift.api` schema generation functions to validate data against auto-generated schemas
- **Data Import Pipeline**: Used by `forklift.engine.forklift_core` import functions (`import_csv`, `import_excel`, etc.) for data validation
- **Processor Architecture**: Extends `forklift.processors.base.BaseProcessor` for consistent integration with other processors
- **Error Handling**: Integrates with Forklift's error handling modes (fail-fast, fail-complete, bad-rows)

## Package Architecture

The schema validator package is organized into modular components:

```
schema_validator/
├── __init__.py           # Package interface and exports
├── base_local.py         # Local base classes (ValidationResult, BaseProcessor)  
├── config.py            # Configuration classes and enums
├── schema.py            # Schema definition classes
├── type_converter.py    # Type conversion utilities
├── core.py              # Main validation logic
├── constraints.py       # Constraint validation implementations
└── utils.py             # Utility functions
```

## Core Components Documentation

### 1. `__init__.py` - Package Interface

**Purpose**: Defines the public API for the schema validator package.

**Key Exports**:
- `SchemaValidator` - Main validation class
- `SchemaValidatorConfig` - Configuration options
- `SchemaValidationMode`, `NullabilityMode` - Validation behavior enums
- `ColumnSchema` - Schema definition class
- Utility functions for creating validators and schemas

### 2. `base_local.py` - Base Classes

**Purpose**: Provides foundational classes for validation operations.

**Key Classes**:
- **`ValidationResult`**: Data class representing the outcome of a validation operation
  - `is_valid`: Boolean indicating validation success/failure
  - `error_message`: Human-readable error description
  - `error_code`: Machine-readable error identifier
  - `row_index`, `column_name`: Location information for errors

- **`BaseProcessor`**: Abstract base class for data processors
  - `process_batch()`: Abstract method for processing PyArrow RecordBatches
  - Returns tuple of (processed_batch, validation_results)

### 3. `config.py` - Configuration Management

**Purpose**: Defines configuration classes and enums for customizing validation behavior.

**Key Components**:

- **`SchemaValidationMode`** (Enum):
  - `STRICT`: All columns must match schema exactly
  - `PERMISSIVE`: Allow extra columns not in schema
  - `COERCE`: Attempt type coercion when possible

- **`NullabilityMode`** (Enum):
  - `ERROR`: Raise validation errors for null violations
  - `WARNING`: Log warnings but continue processing
  - `IGNORE`: Ignore nullability constraints

- **`SchemaValidatorConfig`** (DataClass):
  - Validation behavior settings (mode, nullability handling)
  - Type coercion options
  - Column order checking
  - Case sensitivity settings
  - Row count validation thresholds

### 4. `schema.py` - Schema Definitions

**Purpose**: Defines schema structure for individual columns.

**Key Classes**:
- **`ColumnSchema`**: Represents a single column's schema definition
  - `name`: Column name
  - `data_type`: Expected data type (string representation)
  - `nullable`: Whether column can contain null values
  - `constraints`: Dictionary of validation constraints
  - `description`: Optional column description

### 5. `type_converter.py` - Type System

**Purpose**: Handles conversion between different type representations and PyArrow types.

**Key Methods**:
- **`string_to_arrow_type()`**: Converts string type names to PyArrow DataTypes
- **`convert_arrow_schema_to_dict()`**: Converts PyArrow Schema to internal dictionary format
- **`convert_dict_to_arrow_schema()`**: Converts internal format to PyArrow Schema
- **`is_numeric_type()`**: Checks if a type is numeric
- **`is_type_compatible()`**: Validates type compatibility
- **`can_coerce_type()`**: Determines if type coercion is possible

**Supported Type Mappings**:
- Integer types: `int`, `integer`, `int64`, `int32`
- Float types: `float`, `double`, `float64`, `float32`
- String types: `string`, `str`, `text`
- Boolean: `bool`, `boolean`
- Temporal: `date`, `datetime`, `timestamp`

### 6. `core.py` - Main Validation Engine

**Purpose**: Contains the core `SchemaValidator` class that orchestrates all validation operations.

**Key Class**: `SchemaValidator`

**Initialization**:
- Accepts schema definition (dictionary or PyArrow Schema)
- Configurable validation behavior via `SchemaValidatorConfig`
- Backward compatibility with legacy boolean `strict_mode` parameter

**Validation Pipeline**:
1. **Batch Structure Validation**: Checks for null batches, empty batches
2. **Column Presence Validation**: Verifies required columns exist, handles extra columns
3. **Data Type Validation**: Ensures column types match schema expectations
4. **Nullability Validation**: Enforces null/non-null constraints
5. **Constraint Validation**: Applies business rule constraints
6. **Row Count Validation**: Validates minimum/maximum row counts

**Key Methods**:
- **`process_batch()`**: Main processing method implementing BaseProcessor interface
- **`get_schema_summary()`**: Returns summary statistics about the schema
- **`reset_cache()`**: Clears internal validation cache for performance

### 7. `constraints.py` - Constraint Validation

**Purpose**: Implements specific constraint validation logic.

**Key Class**: `ConstraintValidator`

**Constraint Types**:
- **Range Constraints**: Validates numeric min/max values
- **Enum Constraints**: Validates values against allowed lists
- **Pattern Constraints**: Validates strings against regex patterns
- **Length Constraints**: Validates string length min/max bounds

**Validation Methods**:
- **`validate_range_constraints()`**: Numeric range validation
- **`validate_enum_constraints()`**: Enumeration validation
- **`validate_pattern_constraints()`**: Regex pattern matching
- **`validate_length_constraints()`**: String length validation

### 8. `utils.py` - Utility Functions

**Purpose**: Provides helper functions for common schema validation tasks.

**Key Functions**:
- **`create_schema_validator_from_json()`**: Creates validator from JSON schema
- **`create_schema_from_batch()`**: Generates schema definition from PyArrow RecordBatch
  - Analyzes existing data to infer schema structure
  - Configurable nullability detection
  - Adds metadata about schema creation

## Usage Examples

### Basic Schema Validation

```python
from forklift.processors.schema_validator import SchemaValidator, SchemaValidatorConfig

# Define schema
schema_def = {
    "columns": [
        {"name": "id", "type": "int64", "nullable": False},
        {"name": "name", "type": "string", "nullable": False},
        {"name": "age", "type": "int32", "nullable": True, "constraints": {"min": 0, "max": 150}}
    ]
}

# Create validator with configuration
config = SchemaValidatorConfig(
    validation_mode=SchemaValidationMode.STRICT,
    nullability_mode=NullabilityMode.ERROR
)

validator = SchemaValidator(schema_def, config)

# Process data batch
processed_batch, results = validator.process_batch(record_batch)

# Check validation results
for result in results:
    if not result.is_valid:
        print(f"Validation error: {result.error_message}")
```

### Constraint Validation

```python
# Schema with various constraints
schema_with_constraints = {
    "columns": [
        {
            "name": "email",
            "type": "string",
            "nullable": False,
            "constraints": {
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            }
        },
        {
            "name": "status",
            "type": "string",
            "nullable": False,
            "constraints": {
                "enum": ["active", "inactive", "pending"]
            }
        },
        {
            "name": "score",
            "type": "float64",
            "nullable": True,
            "constraints": {
                "min": 0.0,
                "max": 100.0
            }
        }
    ]
}
```

### Schema Generation from Data

```python
from forklift.processors.schema_validator.utils import create_schema_from_batch

# Generate schema from existing data
schema_def = create_schema_from_batch(record_batch, include_nullability=True)

# Create validator from generated schema
validator = SchemaValidator(schema_def)
```

## Error Handling and Validation Results

The package provides detailed error reporting through `ValidationResult` objects:

### Error Codes
- `NULL_BATCH`: Batch is null or invalid
- `EMPTY_BATCH`: Batch is empty when data is required
- `MISSING_COLUMN`: Required column is missing
- `EXTRA_COLUMN`: Unexpected column found (strict mode)
- `TYPE_MISMATCH`: Column type doesn't match schema
- `NULL_IN_REQUIRED_FIELD`: Null value in non-nullable column
- `MIN_VALUE_VIOLATION`, `MAX_VALUE_VIOLATION`: Range constraint violations
- `ENUM_VIOLATION`: Value not in allowed enumeration
- `PATTERN_VIOLATION`: String doesn't match regex pattern
- `MIN_LENGTH_VIOLATION`, `MAX_LENGTH_VIOLATION`: Length constraint violations

### Integration with Forklift Error Handling

The schema validator integrates with Forklift's error handling modes:
- **Fail-fast**: Stop processing on first validation error
- **Fail-complete**: Collect all validation errors before stopping
- **Bad-rows**: Continue processing, logging bad rows for review

## Performance Considerations

- **Validation Caching**: Type compatibility results are cached for performance
- **PyArrow Compute**: Uses PyArrow's vectorized compute functions for efficient validation
- **Batch Processing**: Designed for streaming large datasets in manageable batches
- **Memory Efficiency**: Minimal memory overhead during validation operations

## Integration with Forklift Components

### Schema Generation API
Works seamlessly with `forklift.api` functions:
```python
import forklift
from forklift.processors.schema_validator import create_schema_validator_from_json

# Generate schema
schema = forklift.generate_schema_from_csv("data.csv")

# Create validator
validator = create_schema_validator_from_json(schema)
```

### Data Import Pipeline
Automatically used in Forklift import functions:
```python
import forklift

# Schema validation happens automatically
results = forklift.import_csv(
    source="data.csv",
    destination="./output/",
    schema_path="schema.json"  # Validates against this schema
)
```

This package is essential for ensuring data quality and consistency in the Forklift data processing pipeline, providing comprehensive validation capabilities while maintaining high performance and flexibility.
