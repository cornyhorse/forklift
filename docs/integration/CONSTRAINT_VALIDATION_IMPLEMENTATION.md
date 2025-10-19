# Constraint Validation and Bad Rows Handling Implementation

## Overview

This document describes the comprehensive constraint validation and bad rows handling functionality implemented in the Forklift codebase that addresses data quality requirements for handling unique constraints, primary keys, and not-null violations according to schema standards.

## Key Components Implemented

### 1. Constraint Validator (`constraint_validator.py`)

**Features:**
- **Primary Key Validation**: Detects duplicate primary keys and null values in primary key columns
- **Unique Constraint Validation**: Enforces unique constraints on single or multiple column combinations
- **Not-Null Validation**: Validates required fields and not-null constraints
- **Flexible Error Handling**: Three modes for handling constraint violations

**Error Handling Modes:**
- `FAIL_FAST`: Stop processing immediately on first constraint violation
- `FAIL_COMPLETE`: Process all rows, collect all violations, then fail at the end
- `BAD_ROWS`: Continue processing, filter out invalid rows to a separate bad rows file

### 2. Bad Rows Handler (`bad_rows_handler.py`)

**Features:**
- **Flexible Output Formats**: Support for Parquet, CSV, and JSON output
- **Comprehensive Error Details**: Includes original data, validation errors, and constraint violations
- **Summary Generation**: Creates detailed summaries of data quality issues
- **Configurable Limits**: Optional maximum bad rows collection to prevent memory issues

### 3. Enhanced Data Processor (`enhanced_processor.py`)

**Features:**
- **Integrated Processing**: Combines schema validation, constraint checking, and bad rows handling
- **Schema-Driven Configuration**: Automatically configures constraints from schema files
- **Comprehensive Reporting**: Provides detailed processing summaries and violation reports

## Schema Standards Integration

### Enhanced CSV Schema Standard

The CSV schema standard (`20250826-csv.json`) includes comprehensive constraint handling configuration:

```json
{
  "x-primaryKey": {
    "columns": ["id"],
    "enforceUniqueness": true,
    "allowNulls": false
  },
  
  "x-constraintHandling": {
    "errorMode": "bad_rows",
    "primaryKeyViolations": {
      "duplicates": "bad_rows",
      "nulls": "bad_rows"
    },
    "uniqueConstraintViolations": "bad_rows",
    "notNullViolations": "bad_rows",
    "badRowsOutput": {
      "enabled": true,
      "format": "parquet",
      "includeOriginalData": true,
      "includeErrorDetails": true,
      "maxBadRows": null,
      "createSummary": true
    }
  },
  
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["name", "birth_date"],
      "description": "Ensure unique combination of name and birth date"
    }
  ]
}
```

## How This Addresses Common Data Quality Requirements

### 1. Primary Key and Unique Constraint Handling

**Problem**: How should rows with unique constraints or primary key violations be handled?

**Solution**: 
- Detects duplicate primary keys and unique constraint violations in real-time
- Tracks seen values across batches to ensure global uniqueness
- Configurable handling: reject file, add to bad rows, or continue processing

### 2. Not-Null Constraint Handling

**Problem**: Handling null values in primary key columns and required fields

**Solution**:
- Validates not-null constraints on required fields
- Specific handling for null values in primary key columns
- Configurable whether to allow nulls in primary keys

### 3. Flexible Error Handling

**Problem**: Need to either fail the file, continue processing to see ALL failing rows, or add them to bad rows

**Solution**: Three distinct error handling modes:
- **Fail Fast**: Stop on first violation (immediate feedback)
- **Fail Complete**: Collect all violations then fail (see all problems)
- **Bad Rows**: Continue processing, separate bad rows (production-ready)

### 4. Schema-Driven Configuration

**Problem**: Behavior should be configurable through schema definition files

**Solution**:
- Constraint definitions in schema files using `x-primaryKey`, `x-uniqueConstraints`
- Error handling mode specification in `x-constraintHandling`
- Automatic configuration parsing from schema dictionaries

## Usage Examples

### Basic Constraint Validation

```python
from forklift.processors.constraint_validator import ConstraintValidator, ConstraintConfig

config = ConstraintConfig(
    primary_key_columns=['id'],
    unique_constraints=[['email'], ['name', 'birth_date']],
    not_null_columns=['id', 'name', 'email'],
    error_mode=ConstraintErrorMode.BAD_ROWS
)

validator = ConstraintValidator(config)
valid_batch, validation_results = validator.process_batch(batch)
```

### Schema-Driven Processing

```python
from forklift.processors.enhanced_processor import EnhancedDataProcessor

processor = EnhancedDataProcessor(
    schema=arrow_schema,
    schema_dict=schema_dict,  # From JSON schema file
    bad_rows_config=BadRowsConfig(output_path="bad_rows.parquet")
)

valid_batch, results = processor.process_batch(batch)
summary = processor.finalize()  # Returns processing summary
```

### Bad Rows Output

The bad rows handler creates comprehensive output files:

**Bad Rows File** (Parquet/CSV/JSON):
- Original row data
- Detailed error information
- Constraint violation details
- Row indices for traceability

**Summary File** (JSON):
- Total rows processed
- Bad rows count and percentage
- Violation type breakdown
- Processing timestamps

## File Support

This implementation works with all supported file types:
- **CSV**: Full constraint validation with configurable delimiters and encoding
- **Excel**: Multi-sheet support with constraint validation
- **SQL**: Query result validation
- **Fixed-Width**: Positional data constraint checking

## Integration Points

The constraint validation integrates seamlessly with existing Forklift components:

1. **Schema Generator**: Can infer constraints from data patterns
2. **Data Transformations**: Applied before constraint validation
3. **Output Writers**: Receives only valid data after constraint filtering
4. **Metadata Collection**: Includes constraint violation statistics

## Performance Considerations

- **Memory Efficient**: Streams processing with configurable bad rows limits
- **Scalable**: Tracks constraints using efficient set operations
- **Configurable**: Optional constraint validation for performance-critical scenarios

This implementation provides a robust, production-ready solution for handling data quality issues according to schema standards while maintaining flexibility in error handling approaches.
