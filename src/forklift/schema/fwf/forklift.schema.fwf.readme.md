# forklift.schema.fwf

The `forklift.schema.fwf` package provides comprehensive support for Fixed Width File (FWF) schema handling within the Forklift data processing framework. This package enables parsing, validation, and processing of fixed-width formatted data files using JSON Schema extensions.

## Overview

Fixed Width Files are a common data format where each field occupies a specific number of characters at predetermined positions within each record. This format is prevalent in legacy systems, mainframe outputs, and financial data exchanges. The FWF package provides robust tools to handle these files with modern data processing capabilities.

## Package Architecture

The FWF package is organized into specialized subpackages, each handling specific aspects of fixed-width file processing:

### Core Components

- **`core.py`**: Contains the main `FwfSchemaImporter` class that orchestrates all FWF processing
- **`exceptions.py`**: Defines FWF-specific exceptions and error handling

### Subpackages

#### [`fields`](./forklift.schema.fwf.fields.readme.md)
Handles field-level operations including:
- Field position calculation and validation
- Column name processing and standardization
- Field mapping and configuration management

#### [`validation`](./forklift.schema.fwf.validation.readme.md)
Provides comprehensive validation capabilities:
- JSON Schema compliance validation
- FWF extension validation (`x-fwf`)
- Field-level validation and type checking
- Parquet type mapping validation
- Cross-format compatibility validation

#### [`conditional`](./forklift.schema.fwf.conditional.readme.md)
Manages conditional schema processing:
- Multi-schema support within single files
- Flag-based schema selection
- Dynamic schema switching based on record types

#### [`utils`](./forklift.schema.fwf.utils.readme.md)
Provides utility functions and helper classes:
- Column name standardization and deduplication
- Parquet format mapping utilities
- Common processing functions

## Key Features

### Schema Standards Compliance
- Follows the FWF schema standard defined in `schema-standards/20250826-fwf.json`
- Supports the `x-fwf` JSON Schema extension for FWF-specific configurations
- Validates schema structure and field definitions

### Field Position Management
- Automatic calculation of field start and end positions
- Validation of position consistency and overlap detection
- Support for variable-length fields and padding

### Data Type Handling
- Comprehensive mapping from FWF field types to Parquet data types
- Type validation and compatibility checking
- Support for complex data types and transformations

### Flexible Configuration
- Support for multiple naming conventions (PostgreSQL, snake_case, camelCase)
- Configurable field alignment, padding, and trimming options
- Extensible architecture for custom processing requirements

### Multi-Schema Support
- Handle files with multiple record types using conditional schemas
- Flag-based automatic schema selection
- Dynamic processing of heterogeneous data structures

## Usage Example

```python
from forklift.schema.fwf import FwfSchemaImporter

# Load and validate a FWF schema
importer = FwfSchemaImporter("path/to/schema.json")

# Access field configurations
fields = importer.get_fields()
column_names = importer.get_column_names()

# Get Parquet type mappings
parquet_schema = importer.get_parquet_schema()
```

## Schema Structure

FWF schemas extend standard JSON Schema with the `x-fwf` extension:

```json
{
  "type": "object",
  "properties": { ... },
  "x-fwf": {
    "fields": [
      {
        "name": "field_name",
        "start": 1,
        "length": 10,
        "type": "string",
        "alignment": "left",
        "padding": " "
      }
    ]
  }
}
```

## Integration Points

The FWF package integrates with other Forklift components:
- **Data Processing Pipeline**: Provides schema information for data transformation
- **Validation Framework**: Ensures data quality and compliance
- **Output Generation**: Maps to various output formats including Parquet
- **Error Handling**: Comprehensive error reporting and debugging support

## Error Handling

The package provides detailed error reporting through:
- Schema validation errors with specific field and position information
- Type conversion errors with suggested corrections
- Position overlap detection with conflict resolution guidance
- Comprehensive logging for debugging and monitoring

This FWF package enables robust processing of fixed-width files while maintaining data integrity, type safety, and compatibility with modern data processing workflows.
