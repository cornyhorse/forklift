# forklift.schema.fwf.conditional

The `conditional` subpackage provides functionality for handling conditional schemas and schema variations within Fixed Width File (FWF) processing. This package enables dynamic schema selection based on flag values or conditions within the data.

## Components

### ConditionalSchemaManager (`schemas.py`)
Manages conditional schema configurations and operations:
- Handles multiple schema variants within a single FWF file
- Manages flag column configurations for schema selection
- Provides methods to retrieve specific schema variants based on conditions
- Validates conditional schema structures and configurations

### VariantManager (`variants.py`)
Manages schema variant operations and processing:
- Handles variant-specific field configurations
- Manages variant selection logic and processing
- Provides utilities for variant comparison and validation
- Supports dynamic schema switching based on data content

## Usage

The conditional subpackage is used when FWF files contain multiple record types or formats that require different schema handling. This is common in legacy data files where different record types are identified by flag values in specific positions.

## Key Features

- **Multi-Schema Support**: Handle multiple schema variants within a single file
- **Flag-Based Selection**: Automatically select schemas based on flag column values
- **Dynamic Processing**: Switch between schemas during file processing
- **Variant Validation**: Ensure all schema variants are properly configured
- **Conditional Logic**: Support complex conditional schema selection rules

## Example Use Case

A legacy mainframe file might contain different record types (header, detail, trailer) each with different field layouts. The conditional module allows you to define separate schemas for each record type and automatically apply the correct schema based on a record type indicator field.
