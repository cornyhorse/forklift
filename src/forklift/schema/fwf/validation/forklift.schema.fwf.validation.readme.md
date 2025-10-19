# forklift.schema.fwf.validation

The `validation` subpackage provides comprehensive validation functionality for Fixed Width File (FWF) schemas. This package ensures schema compliance, type safety, and compatibility with various data formats and standards.

## Components

### JsonSchemaValidator (`json_schema.py`)
Validates JSON Schema compliance and structure:
- Ensures schema follows JSON Schema specifications
- Validates required properties and structure
- Checks for proper schema formatting and syntax

### FwfExtensionValidator (`fwf_extension.py`)
Validates FWF-specific schema extensions (`x-fwf`):
- Validates FWF extension structure and required fields
- Ensures proper field definitions and configurations
- Validates FWF-specific properties like alignment, padding, and trimming

### FieldValidator (`fields.py`)
Provides field-level validation for FWF schemas:
- Validates individual field configurations
- Ensures field position consistency
- Checks field type definitions and constraints

### ParquetTypeValidator (`parquet_types.py`)
Handles Parquet data type mapping and validation:
- Maps FWF field types to Parquet data types
- Validates type compatibility and conversions
- Ensures proper data type handling for Parquet output

### CompatibilityValidator (`compatibility.py`)
Ensures cross-format compatibility and standards compliance:
- Validates compatibility between different schema versions
- Checks for breaking changes in schema updates
- Ensures backward compatibility with existing implementations

## Usage

The validation subpackage is used by the `FwfSchemaImporter` to perform comprehensive validation of FWF schemas during import and processing. It ensures that schemas are well-formed, compliant with standards, and compatible with the target data formats.

## Key Features

- **Multi-layer Validation**: Validates at JSON Schema, FWF extension, and field levels
- **Type Safety**: Ensures proper data type mapping and compatibility
- **Standards Compliance**: Validates against FWF schema standards
- **Detailed Error Reporting**: Provides specific error messages for validation failures
- **Parquet Integration**: Specialized validation for Parquet output compatibility
