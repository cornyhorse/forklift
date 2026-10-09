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
- Accepts any text encoding Python's `codecs` module knows for `encoding`

### FieldValidator (`fields.py`)
Provides field-level validation for FWF schemas:
- Validates individual field configurations
- Ensures field position consistency
- Checks field type definitions and constraints

### ParquetTypeValidator (`parquet_types.py`)
Handles Parquet data type mapping and validation:
- Validates the Parquet type grammar strictly: time units (`s`, `ms`, `us`, `ns`), `decimal128(p,s)` parameters, `list<...>` and `dictionary<values=..., indices=...>` are parsed, not just prefix-matched
- Validates type compatibility across conditional variants (see below)
- Ensures proper data type handling for Parquet output

### CompatibilityValidator (`compatibility.py`)
Checks fields that appear in more than one conditional schema variant:
- The declared Parquet types of one column across variants must be unifiable (all numeric, all decimal, all date/timestamp with the same time zone, all duration, or all string/binary); `int32` and `double` unify to `double`, `int64` and `string` do not
- Overlapping positions of the same column in different variants are only an error when the types are incompatible

## Usage

The validation subpackage is used by the `FwfSchemaImporter` to perform comprehensive validation of FWF schemas during import and processing. It ensures that schemas are well-formed, compliant with standards, and compatible with the target data formats.

## Key Features

- **Multi-layer Validation**: Validates at JSON Schema, FWF extension, and field levels
- **Type Safety**: Ensures proper data type mapping and compatibility
- **Standards Compliance**: Validates against FWF schema standards
- **Detailed Error Reporting**: Provides specific error messages for validation failures
- **Parquet Integration**: Specialized validation for Parquet output compatibility
