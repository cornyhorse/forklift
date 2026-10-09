# Forklift Data Validation Package

## Overview

The `forklift.processors.data_validation` package provides comprehensive data validation functionality as part of Forklift's data processing pipeline system. This package implements field-level validation rules with sophisticated bad row handling, allowing for robust data quality enforcement during data import and processing operations.

## Package Context in Forklift

Forklift is a high-performance data processing tool that provides PyArrow-based streaming, schema generation, validation, and S3 support. The data validation package fits into Forklift's processor architecture as follows:

- **Base Architecture**: All processors inherit from `BaseProcessor` and implement the `process_batch()` method
- **Pipeline Integration**: Processors can be chained together using `ProcessorPipeline` for complex workflows
- **Streaming Processing**: Works with PyArrow RecordBatch objects for memory-efficient processing of large datasets
- **Validation Ecosystem**: Complements other validation processors like `SchemaValidator` and `DataQualityProcessor`

The data validation package specifically handles field-level validation rules while other processors handle schema validation, data quality checks, and transformations.

## Package Architecture

```
data_validation/
├── __init__.py                    # Package exports and imports
├── data_validation_processor.py   # Main processor implementation
├── validation_config.py          # Configuration classes
├── validation_rules.py           # Individual validation rule implementations
└── bad_rows_handler.py           # Bad row collection and processing
```

## Core Components

### 1. DataValidationProcessor (`data_validation_processor.py`)

**Purpose**: Main processor class that enforces field-level validation rules with bad row handling.

**Key Features**:
- Inherits from `BaseProcessor` for pipeline compatibility
- Processes PyArrow RecordBatch objects in streaming fashion
- Separates valid rows from invalid rows during processing
- Tracks unique values for uniqueness constraints
- Provides validation summaries and statistics

**Main Method**:
```python
def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]
```

**Validation Types Supported**:
- **Required field validation**: Null/empty checks
- **Unique field validation**: Duplicate detection with configurable strategies
- **Range validation**: Min/max constraints for numeric and date fields
- **String validation**: Length and pattern matching constraints
- **Enum validation**: Allowed values checking
- **Date validation**: Date format and range validation

**Backward Compatibility**: Maintains compatibility with legacy test interfaces through property wrappers and method aliases.

### 2. ValidationConfig (`validation_config.py`)

**Purpose**: Configuration classes that define validation rules and bad row handling behavior.

**Configuration Classes**:

#### `FieldValidationRule`
Defines validation rules for individual fields:
- `field_name`: Target field name
- `required`: Whether field is required (not null/empty)
- `unique`: Whether field values must be unique
- `range_validation`: Numeric/date range constraints
- `string_validation`: String length and pattern constraints
- `enum_validation`: Allowed values constraints
- `date_validation`: Date format and range constraints

#### `ValidationConfig`
Main configuration container:
- `field_validations`: List of field validation rules
- `bad_rows_config`: Bad row handling configuration
- `uniqueness_strategy`: How to handle duplicate values
  - `"first_wins"`: Keep first occurrence, mark subsequent as bad
  - `"last_wins"`: Keep last occurrence, mark previous as bad
  - `"fail_on_duplicate"`: Mark all duplicates as bad
  - `"mark_all_duplicates"`: Mark all instances of duplicated values as bad

#### `BadRowsConfig`
Configuration for bad row handling:
- `enabled`: Whether to collect bad rows
- `output_path`: Where to write bad rows
- `file_format`: Output format (parquet, csv, etc.)
- `include_original_row`: Include original row data
- `include_validation_errors`: Add error details to bad rows
- `max_bad_rows_percent`: Threshold for failing processing
- `fail_on_exceed_threshold`: Whether to fail when threshold exceeded

#### Validation-Specific Configs
- `RangeValidation`: Min/max value constraints with inclusive/exclusive options
- `StringValidation`: Length constraints and regex pattern matching
- `EnumValidation`: Allowed values with case sensitivity options
- `DateValidation`: Date range constraints with format specifications

### 3. ValidationRules (`validation_rules.py`)

**Purpose**: Static utility class containing individual validation rule implementations.

**Key Methods**:

#### `is_null_or_empty(value) -> bool`
Determines if a value is null or empty (handles None and empty strings).

#### `validate_range(field_name, value, range_val) -> Optional[str]`
Validates numeric and date values against min/max constraints:
- Handles string-to-number conversion
- Supports inclusive/exclusive range checking
- Returns error message or None if valid

#### `validate_string(field_name, value, string_val) -> Optional[str]`
Validates string constraints:
- Length validation (min/max)
- Regex pattern matching
- Empty string handling

#### `validate_enum(field_name, value, enum_val) -> Optional[str]`
Validates enumeration constraints:
- Case-sensitive or case-insensitive matching
- Returns descriptive error messages with allowed values

#### `validate_date(field_name, value, date_val) -> Optional[str]`
Validates date constraints:
- Date parsing from strings or datetime objects
- Date range validation
- Format validation

### 4. BadRowsHandler (`bad_rows_handler.py`)

**Purpose**: Manages collection, processing, and output of rows that fail validation.

**Key Features**:
- **Row Collection**: Collects bad rows with original data and error information
- **Metadata Enhancement**: Adds validation error details, timestamps, and error counts
- **Type Inference**: Infers appropriate PyArrow data types for bad row output
- **Threshold Management**: Tracks bad row percentages and threshold violations
- **Output Generation**: Creates PyArrow RecordBatch for bad row output

**Key Methods**:

#### `add_bad_row(batch, row_idx, errors)`
Adds a bad row to the collection with error details.

#### `get_bad_rows_batch() -> Optional[pa.RecordBatch]`
Returns bad rows as PyArrow RecordBatch for output processing.

#### `is_threshold_exceeded(total_rows) -> bool`
Checks if bad row percentage exceeds configured threshold.

#### `_infer_field_type(field_name, bad_rows) -> pa.DataType`
Intelligently infers PyArrow data types from collected bad row data, handling mixed types and null values.

### 5. Package Init (`__init__.py`)

**Purpose**: Provides clean package interface and backward compatibility.

**Exports**:
- All configuration classes for easy import
- Main processor class
- Utility classes (ValidationRules, BadRowsHandler)
- Maintains backward compatibility with existing code

## Usage Patterns

### Basic Usage
```python
from forklift.processors.data_validation import DataValidationProcessor, ValidationConfig, FieldValidationRule

# Define validation rules
rules = [
    FieldValidationRule(
        field_name="id",
        required=True,
        unique=True
    ),
    FieldValidationRule(
        field_name="age",
        range_validation=RangeValidation(min_value=0, max_value=120)
    )
]

# Create configuration
config = ValidationConfig(
    field_validations=rules,
    bad_rows_config=BadRowsConfig(enabled=True)
)

# Create processor
processor = DataValidationProcessor(config)

# Process data
clean_batch, validation_results = processor.process_batch(batch)
bad_rows_batch = processor.get_bad_rows_batch()
```

### Pipeline Integration
```python
from forklift.processors import ProcessorPipeline

pipeline = ProcessorPipeline([
    DataValidationProcessor(validation_config),
    SchemaValidator(schema_config),
    DataQualityProcessor(quality_config)
])

processed_batch, all_results = pipeline.process_batch(batch)
```

## Error Handling Strategies

The package fails closed:

1. **Bad Row Collection**: Invalid rows are separated and collected for inspection
   (`BadRowsConfig.include_original_row=False` keeps the original data out of that output; original
   columns whose names start with `_` are kept, and renamed `original_<name>` if they would clash with
   the `_validation_errors` / `_error_count` / `_processed_timestamp` / `_row_number` columns)
2. **Threshold Management**: with `fail_on_exceed_threshold=True` (default) exceeding
   `max_bad_rows_percent` raises `BadRowsThresholdExceededError`; the batch is not emitted. Rejected
   rows are counted even when `BadRowsConfig.enabled=False`
3. **Internal errors raise** `ValidationProcessingError`; an unvalidated batch is never returned
4. **Required columns**: a `required=True` rule for a column the batch does not have raises
   (`"Email"` does not silently match `"email"`)
5. **Detailed Error Reporting**: Each validation failure includes the field and rule. Messages do
   **not** contain the offending value unless `ValidationConfig.include_values_in_errors=True`
6. **Attribution**: every error of a rejected row is a `ValidationResult` with `row_index` (position of
   the row in the batch passed to `process_batch`), `error_code` (`VALIDATION_ERROR`) and `column_name`
   (the field whose rule failed)

## Building the processor from `x-validation`

`forklift.processors.schema_extensions.build_data_validator(schema, *, resolve_column=identity)` turns the
`x-validation` extension of a schema dictionary into a `DataValidationProcessor` (or `None` when no field
has a rule). Supported shape (the one in `schema-standards/20250826-csv.json`):

```json
{
  "x-validation": {
    "badRowsHandling": {"maxBadRowsPercent": 10.0, "failOnExceedThreshold": true},
    "uniquenessHandling": {"strategy": "first_wins"},
    "fieldValidations": {
      "age": {
        "required": false,
        "unique": false,
        "range": {"min": 0, "max": 150, "inclusive": true},
        "stringValidation": {"minLength": 1, "maxLength": 100, "pattern": "^[A-Za-z]+$", "allowEmpty": false},
        "enumValidation": {"allowedValues": ["A", "B"], "caseSensitive": true},
        "dateValidation": {"minDate": "1900-01-01", "maxDate": "2100-12-31", "format": ["%Y-%m-%d"]}
      }
    }
  }
}
```

| `x-validation` | Python |
|---|---|
| `fieldValidations.<f>.required` / `unique` | `FieldValidationRule.required` / `.unique` |
| `range {min, max, inclusive}` | `RangeValidation(min_value, max_value, inclusive)` (`range_validation`) |
| `stringValidation {minLength, maxLength, pattern, allowEmpty}` | `StringValidation(min_length, max_length, pattern, allow_empty)` |
| `enumValidation {allowedValues, caseSensitive}` | `EnumValidation(allowed_values, case_sensitive)` |
| `dateValidation {minDate, maxDate, format}` | `DateValidation(min_date, max_date, formats)` (`format` is a string or a list) |
| `uniquenessHandling.strategy` | `ValidationConfig.uniqueness_strategy` (`first_wins`, `last_wins`, `fail_on_duplicate`, `mark_all_duplicates`) |
| `badRowsHandling.maxBadRowsPercent` / `failOnExceedThreshold` | `BadRowsConfig.max_bad_rows_percent` / `.fail_on_exceed_threshold` (defaults 10.0 / true) |
| `badRowsHandling.thresholdMode` (`end_of_file` or `early`) | `BadRowsConfig.threshold_check`; the loader's default is `end_of_file`, the class's own default is `early` (the previous behaviour). With `end_of_file` the verdict is given by `DataValidationProcessor.check_threshold()` after the last batch |

The processor that is built **does not write files and does not keep the rejected rows**
(`BadRowsConfig(enabled=False)`): the caller removes/writes the rejected rows itself using the
`ValidationResult`s, and the handler only counts them, so memory stays constant however many rows are
rejected while the `maxBadRowsPercent` threshold keeps working. `badRowsHandling.outputPath`,
`fileFormat`, `includeOriginalRow` and `includeValidationErrors` are ignored, as are
`fieldValidations.<f>.onViolation` (a violation always rejects the row), `crossFieldValidations` and
`globalValidations`; `unsupported_extension_keys(schema)` lists them (they do not raise, the shipped
standard contains them). Invalid configuration (wrong types, `min` greater than `max`, an unknown
strategy, an invalid or unsafe regular expression, ...) raises `ValueError("x-validation....: ...")`.
Field names go through `resolve_column` (header name -> output name).

### Use by `import_csv`

`import_csv` (CSV only) builds this processor with `build_data_validator` and runs it on every batch after
`x-columnMapping`, `x-calculatedColumns` and `x-dataQuality` and before the key and constraint checks, so the
rules use the *output* column names and can name calculated columns. The rows it rejects are written by the
engine to `bad_rows.parquet` (the input's column names, all strings) with the reason
`VALIDATION_ERROR:<column>` in the `_rejection_reason` column, and counted in
`ProcessingResults.validation_summary`; the processor writes no file. The share of rejected rows is
compared with `maxBadRowsPercent` against **all rows that reached the validator** (rows already rejected by type
conversion or `required` never get here): with the default `thresholdMode` (`end_of_file`) the whole input is
checked first and the import then raises `BadRowsThresholdExceededError` (a `RuntimeError`) if more than 10 % of
those rows were rejected, with the findings by rule in the message (the import discards the data file, keeps `bad_rows.parquet` and names it in the error); with `early` the
comparison is made after every batch on the rows seen so far and the import stops at the first batch over the
limit. A rule for a column that is declared in `properties` but absent
from the file is skipped with a warning; a rule for a name that is nowhere raises `ValueError`.

## Validation semantics

- **Regular expressions** (`StringValidation.pattern`) are *unanchored searches* (JSON Schema
  semantics): anchor with `^...$` to require the whole value to match. A trailing `$` does not accept a
  trailing newline. Patterns are compiled when `StringValidation` is created; invalid patterns, patterns
  longer than 2000 characters, and patterns with nested unbounded quantifiers such as `(a+)+`
  (catastrophic backtracking) raise `ValueError` unless `allow_unsafe_regex=True`.
- **NULL and empty values**: `None` skips every rule except `required`. Empty and whitespace-only
  strings are values: `min_length`, `pattern`, enum, range and date rules apply to them, and
  `StringValidation(allow_empty=False)` rejects them. `required=True` rejects both `None` and blanks.
- **Uniqueness** (`uniqueness_strategy`): `first_wins` keeps the first row of a key; `fail_on_duplicate`
  does the same with a "violates uniqueness constraint" message; `last_wins` keeps the *last* valid row of
  a key within a batch; `mark_all_duplicates` rejects every row of a key that occurs more than once. Rows
  already emitted by earlier batches cannot be retracted, so a later duplicate of such a key is rejected
  under every strategy. A row claims its keys only when it passes all rules, so a row rejected for another
  reason never makes a later valid row look like a duplicate. NULL/blank values are not keys.
- **Range rules** compare exact decimals (`min_value=0.01` accepts `Decimal("0.01")`; numeric strings keep
  their precision), reject NaN, and also work for dates: bounds and values may be `date`, `datetime` or
  ISO strings (a date-only bound compares calendar dates). Invalid bounds raise `ValueError` when the
  processor is created.
- **Date rules** parse strings with `DateValidation.formats` (default `["%Y-%m-%d"]`; when `formats` is
  given only those formats are accepted). `min_date` / `max_date` are ISO dates (or any of `formats`).

## Integration Points

- **Input**: Works with PyArrow RecordBatch objects from Forklift's streaming readers
- **Output**: Produces clean data batches and separate bad row batches
- **Pipeline**: Integrates with ProcessorPipeline for complex workflows
- **Validation Results**: Returns structured ValidationResult objects for error tracking
- **Metadata**: Provides processing summaries and statistics

## Performance Characteristics

- **Streaming**: Processes data in batches for memory efficiency
- **Type Safety**: Uses PyArrow's typed arrays for performance
- **Minimal Copying**: Efficient row filtering without full data copying
- **Configurable**: Validation overhead scales with number of active rules
- **Memory Management**: Bad row collection respects configured limits; with
  `BadRowsConfig(enabled=False)` rejected rows are only counted

This package provides the foundation for robust data validation in Forklift's data processing pipeline, ensuring data quality while maintaining high performance and flexibility.
