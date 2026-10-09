# Constraint Validation and Bad Rows Handling Implementation

## Overview

This document describes the comprehensive constraint validation and bad rows handling functionality implemented in the Forklift codebase that addresses data quality requirements for handling unique constraints, primary keys, and not-null violations according to schema standards.

> **Relationship to `import_csv`.** `forklift.import_csv()` runs the constraint validator on CSV files:
> it builds a `ConstraintValidator` from the schema with
> `forklift.processors.schema_extensions.build_constraint_validator` (`x-primaryKey`,
> `x-uniqueConstraints`, `x-constraintHandling.errorMode` and the per-property `minimum`, `maximum`,
> `minLength`, `maxLength`, `pattern`, `enum`, `x-unique`) and runs it on every batch, after
> `x-validation`. In `bad_rows` mode the violating rows go to `bad_rows.parquet` (all-string columns in
> the shape of the input, plus a last column `_rejection_reason` such as `UNIQUE_VIOLATION:id`) and are
> counted in `ProcessingResults.invalid_rows`; the findings are counted per reason in
> `ProcessingResults.validation_summary`. `fail_fast` and `fail_complete` make the import raise
> `ValueError` and leave no output behind. The bad rows handler and the enhanced processor described
> below are **not** used by `import_csv`: the engine writes `bad_rows.parquet` itself. Excel, SQL and
> fixed-width imports apply no constraints. See [Applying Schema Extensions](../guides/USAGE.md#applying-schema-extensions).

## Key Components Implemented

### 1. Constraint Validator (`constraint_validator.py`)

**Features:**
- **Unique Constraints**: single columns or tuples of columns (`ConstraintConfig.unique_constraints`); a key with a NULL part is never compared; the first row of a key wins
- **Check Constraints**: `ConstraintConfig.check_constraints` maps a name to a `column` plus `min`/`max`, `enum`, `pattern` (unanchored search), `minLength`/`maxLength` and `nullable: false`
- **Primary Key**: not a separate setting of the class: `build_constraint_validator` turns `x-primaryKey` into a unique constraint (`enforceUniqueness`, default true) plus a `nullable: false` check per key column (`allowNulls`, default false)
- **Flexible Error Handling**: Three modes for handling constraint violations
- **Bounded memory**: at most `max_retained_violations` (1000) violations are kept, without cell values; `violation_count` is exact

**Error Handling Modes** (`ConstraintConfig.error_mode`, an `ErrorMode` or its string):
- `FAIL_FAST`: Stop processing immediately on first constraint violation (`ValueError`)
- `FAIL_COMPLETE`: Process all rows, collect all violations, then fail at the end (`finalize()` raises `ValueError`)
- `BAD_ROWS`: Continue processing; the violating rows are removed from the returned batch and reported as `ValidationResult`s (`NULL_VIOLATION`, `UNIQUE_VIOLATION`, `RANGE_VIOLATION`, `ENUM_VIOLATION`, `PATTERN_VIOLATION`, `LENGTH_VIOLATION`, each with `row_index` and `column_name`)

### 2. Bad Rows Handler (`bad_rows_handler.py`)

Not used by `import_csv`. It collects bad rows when you drive the processors yourself.

**Features:**
- **Flexible Output Formats**: Support for Parquet, CSV, and JSON output
- **Comprehensive Error Details**: Includes original data (cell values), validation errors, and constraint violations
- **Summary Generation**: Creates detailed summaries of data quality issues
- **Configurable Limits**: Optional maximum bad rows collection to prevent memory issues

### 3. Enhanced Data Processor (`enhanced_processor.py`)

Not used by `import_csv`.

**Features:**
- **Integrated Processing**: Combines schema validation, constraint checking, and bad rows handling
- **Schema-Driven Configuration**: builds its constraint configuration from the schema dictionary with `create_constraint_config_from_schema`, which reads the per-property constraints and `x-constraintHandling.errorMode` but **not** `x-primaryKey` / `x-uniqueConstraints`; pass `constraint_config=build_constraint_validator(schema).config` to enforce those as well
- **Comprehensive Reporting**: Provides detailed processing summaries and violation reports

## Schema Standards Integration

### Enhanced CSV Schema Standard

The CSV schema standard (`20250826-csv.json`) configures the constraints that `import_csv` applies:

```json
{
  "x-primaryKey": {
    "columns": ["id"],
    "enforceUniqueness": true,
    "allowNulls": false
  },

  "x-constraintHandling": {
    "errorMode": "bad_rows"
  },

  "x-uniqueConstraints": [
    {
      "name": "unique_name_birth_date",
      "columns": ["name", "birth_date"],
      "description": "Ensure unique combination of name and birth date"
    }
  ]
}
```

What is read: `x-primaryKey` (`columns`, `type`, `enforceUniqueness`, `allowNulls`), `x-uniqueConstraints[]` (`name`, `columns`) and `x-constraintHandling.errorMode` (`bad_rows`, `fail_fast`, `fail_complete`; anything else, such as the `ignore` or `transform` of older documentation, raises `ValueError`). The other `x-constraintHandling` keys (`primaryKeyViolations`, `uniqueConstraintViolations`, `notNullViolations`, `badRowsOutput`, `validationOptions`) are not implemented. `primaryKeyViolations`, `uniqueConstraintViolations` and `notNullViolations` are accepted silently when they ask for the same as `errorMode`; every other value or key is reported in `ProcessingResults.warnings`.

## How This Addresses Common Data Quality Requirements

### 1. Primary Key and Unique Constraint Handling

**Problem**: How should rows with unique constraints or primary key violations be handled?

**Solution**: 
- Detects duplicate primary keys and unique constraint violations in real-time
- Tracks seen values across batches to ensure global uniqueness
- Configurable handling: fail the import (`fail_fast`, `fail_complete`) or set the violating rows aside (`bad_rows`); the first row of a key is kept and later duplicates are rejected

### 2. Not-Null Constraint Handling

**Problem**: Handling null values in primary key columns and required fields

**Solution**:
- `required` columns are checked by the import itself (null or empty string sends the row to `bad_rows.parquet` with the reason `required_value_missing`); `nullable: false` checks are available on the validator
- Rows with a NULL in a primary key column are rejected (`NULL_VIOLATION:<column>`)
- `allowNulls: true` lets NULL keys through

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
import pyarrow as pa
from forklift.processors.constraint_validator import ConstraintValidator, ConstraintConfig

batch = pa.RecordBatch.from_pydict({"id": [1, 2, 2, None], "name": ["a", "b", "c", "d"]})

config = ConstraintConfig(
    unique_constraints=["id"],                                        # or ("name", "birth_date") for a composite key
    check_constraints={"id_required": {"column": "id", "nullable": False}},
    error_mode="bad_rows",
)

validator = ConstraintValidator(config)
valid_batch, validation_results = validator.process_batch(batch)

print(valid_batch.to_pydict())    # {'id': [1, 2], 'name': ['a', 'b']}
print([(r.row_index, r.error_code, r.column_name) for r in validation_results])
# [(3, 'NULL_VIOLATION', 'id'), (2, 'UNIQUE_VIOLATION', 'id')]
```

### Schema-Driven Processing

`build_constraint_validator` builds the same validator from a schema dictionary, and is what `import_csv` uses:

```python
from forklift.processors.schema_extensions import build_constraint_validator

schema = {"x-primaryKey": {"columns": ["id"]}, "x-constraintHandling": {"errorMode": "bad_rows"}}
validator = build_constraint_validator(schema)           # None if the schema has nothing to check
valid_batch, validation_results = validator.process_batch(batch)   # same result as above
```

With `"errorMode": "fail_complete"` the same batch is returned unchanged (4 rows) and `validator.finalize()` raises `ValueError: Constraint validation failed with 2 violations`; with `"fail_fast"` `process_batch` raises at the first violation.

`EnhancedDataProcessor` combines schema validation, the constraint validator and a bad rows handler. It reads the per-property constraints and `errorMode` from `schema_dict`; hand it the configuration of `build_constraint_validator` to enforce `x-primaryKey` / `x-uniqueConstraints` too:

```python
from forklift.processors.bad_rows_handler import BadRowsConfig
from forklift.processors.enhanced_processor import EnhancedDataProcessor

processor = EnhancedDataProcessor(
    schema=pa.schema([("id", pa.int64()), ("name", pa.string())]),
    schema_dict=schema,
    constraint_config=validator.config,
    bad_rows_config=BadRowsConfig(output_path="bad_rows.parquet"),
)

valid_batch, results = processor.process_batch(batch)
summary = processor.finalize()  # processing summary; writes bad_rows.parquet when there are bad rows
```

### Bad Rows Output

The bad rows handler (not used by `import_csv`, whose `bad_rows.parquet` is described at the top of this page) creates these output files:

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

`import_csv` is the only import that runs the constraint validator. `import_excel` and `import_sql` apply no constraints and `import_fwf` is not implemented. The classes themselves work on any PyArrow batch, so you can run them on the tables that `ExcelInputHandler`, `SqlInputHandler` or `FwfInputHandler` produce.

## Integration Points

How the constraint validation fits into `import_csv`:

1. **Schema Generator**: infers only `x-primaryKey`, and an inferred key is enforced when you import with the generated schema
2. **Data Transformations**: `x-transformations`, `x-special-type` formatting, type conversion, `x-columnMapping`, `x-calculatedColumns` and `x-validation` run before the constraints (the constraints see the output column names)
3. **Output Writers**: `data.parquet` receives only the rows that passed; `x-rowHash` columns are added to those rows afterwards
4. **Metadata Collection**: the findings are counted per reason in `ProcessingResults.validation_summary` and recorded in `metadata.json`

## Performance Considerations

- **Memory Efficient**: batches are streamed; the validator retains at most `max_retained_violations` violations (1000) and no cell values, while `violation_count` and `validation_summary` stay exact. The set of unique keys seen grows with the number of distinct keys
- **Scalable**: uniqueness is tracked with hash sets of the keys
- **Configurable**: `import_csv(..., apply_schema_extensions=False)` skips all schema extensions, including the constraints

This implementation provides a robust, production-ready solution for handling data quality issues according to schema standards while maintaining flexibility in error handling approaches.
