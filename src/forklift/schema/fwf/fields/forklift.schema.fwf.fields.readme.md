# forklift.schema.fwf.fields

The `fields` subpackage provides core field handling functionality for Fixed Width File (FWF) schema processing. This package contains utilities for parsing field definitions, calculating field positions, and mapping field configurations.

## Components

### FieldParser (`parser.py`)
Handles parsing and processing of FWF field configurations with support for:
- Column name extraction and standardization
- Field validation and normalization  
- Integration with column name processing utilities

### PositionCalculator (`positions.py`)
Manages field position calculations and validation for fixed-width files:
- Calculates field start and end positions
- Validates position consistency and overlaps
- Handles position-based field extraction logic

### FieldMapper (`mapping.py`)
Provides field mapping and transformation utilities:
- Maps field definitions to internal representations
- Handles field type conversions
- Manages field metadata and configuration mapping

## Usage

The fields subpackage is primarily used internally by the `FwfSchemaImporter` class to process field definitions from FWF schema files. It ensures that field positions are correctly calculated, field names are properly standardized, and field configurations are validated for consistency.

## Key Features

- **Position Validation**: Ensures fixed-width field positions don't overlap
- **Name Standardization**: Supports multiple column naming conventions (postgres, snake_case, camelCase)
- **Type Safety**: Validates field type mappings and configurations
- **Error Handling**: Provides detailed error messages for field configuration issues
