# forklift.schema.fwf.utils

The `utils` subpackage provides utility functions and helper classes for Fixed Width File (FWF) schema processing. This package contains common functionality used across the FWF module for data transformation, column processing, and format mapping.

## Components

### ColumnNameProcessor (`column_names.py`)
Handles column name processing and standardization:
- Standardizes column names according to various naming conventions
- Supports PostgreSQL, snake_case, and camelCase naming standards
- Provides deduplication functionality for duplicate column names
- Handles special character processing and sanitization

### ParquetMappingUtils (`parquet_mapping.py`)
Provides utilities for mapping FWF schemas to Parquet format:
- Maps FWF field types to appropriate Parquet data types
- Handles type conversion and compatibility checks
- Provides metadata mapping for Parquet schema generation
- Supports complex type mappings and nested structures

## Usage

The utils subpackage provides common functionality that is shared across other FWF subpackages. It is primarily used internally by the `FwfSchemaImporter` and validation components to ensure consistent data processing and format compatibility.

## Key Features

- **Name Standardization**: Multiple naming convention support with configurable rules
- **Deduplication**: Intelligent handling of duplicate column names
- **Type Mapping**: Comprehensive FWF to Parquet type conversion
- **Format Compatibility**: Ensures proper data format handling across different output types
- **Utility Functions**: Common helper functions for schema processing

## Naming Conventions Supported

- **postgres**: PostgreSQL-compatible column names (lowercase, underscores)
- **snake_case**: Python-style snake_case naming
- **camelCase**: JavaScript-style camelCase naming
- **Custom**: Extensible framework for additional naming conventions
