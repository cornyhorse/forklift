# Schema Documentation

This folder contains comprehensive documentation for Forklift's schema system, data validation, and transformation capabilities.

## Contents

### [SCHEMA_STANDARDS.md](SCHEMA_STANDARDS.md)
**Complete reference for Forklift schema configuration**
- JSON Schema extensions and custom properties
- File format configurations (CSV, Excel, FWF; the JSON and Parquet blocks are illustrative, no code reads them)
- Data type transformations (string, numeric, datetime, format-specific)
- Validation rules and constraint definitions
- Processing configuration options
- Comprehensive examples and use cases

### [README.md](README.md)
**Index of the per-extension pages** (`x-csv`, `x-fwf`, `x-transformations`, `x-validation`, `x-metadata-generation`, ...), including the table of which schema parts the CSV engine (`import_csv`) applies, the order of the stages, the naming rule for renamed columns and what `bad_rows.parquet`, the `warnings` and the `validation_summary` of a run contain. Excel, SQL and fixed-width imports do not apply the `x-...` processing extensions.

### [X_VALIDATION_DOCUMENTATION.md](X_VALIDATION_DOCUMENTATION.md)
**`x-validation`**: field rules that reject rows (`required`, `unique`, `range`, `stringValidation`, `enumValidation`, `dateValidation`), the uniqueness strategies and the bad-rows threshold. The other `X_*_DOCUMENTATION.md` pages describe one extension each in the same style.

## Planned Additions

The following documentation is recommended to expand this section:

### Schema Design Patterns
- **schema-design-patterns.md**: Common schema patterns for different data types
- **schema-migration.md**: Guidelines for evolving schemas over time
- **schema-validation-best-practices.md**: Best practices for validation rules

### Data Type Guides
- **data-types-reference.md**: Complete reference for all supported data types
- **transformation-cookbook.md**: Recipe-style transformation examples
- **custom-transformations.md**: Guide for creating custom transformation functions

### Validation Documentation
- **constraint-types.md**: Detailed documentation of all constraint types
- **validation-strategies.md**: Different approaches to data validation
- **error-handling-patterns.md**: How to handle validation errors effectively

### Format-Specific Guides
- **csv-processing-guide.md**: Advanced CSV processing techniques
- **excel-integration.md**: Working with complex Excel files
- **fixed-width-files.md**: Comprehensive FWF processing guide
- **json-data-handling.md**: JSON processing best practices

This organization allows for comprehensive schema documentation while maintaining clear separation of concerns.
