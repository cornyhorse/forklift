# x-constraintHandling Documentation

## Overview
The `x-constraintHandling` extension provides comprehensive configuration for handling constraint violations and data quality issues during processing. This feature enables fine-grained control over how the system responds to various data quality problems, including primary key violations, unique constraint violations, and null constraint violations.

## Schema Structure
```json
{
  "x-constraintHandling": {
    "description": "Configuration for handling constraint violations and data quality issues",
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
    },
    "validationOptions": {
      "continueOnError": true,
      "collectAllErrors": true,
      "maxErrorsPerRow": 10
    }
  }
}
```

## Configuration Properties

### `errorMode` (required)
- **Type**: String
- **Description**: Global error handling strategy
- **Values**:
  - `"bad_rows"`: Route violating rows to bad rows output
  - `"fail_fast"`: Stop processing on first constraint violation
  - `"ignore"`: Continue processing, log warnings only
  - `"transform"`: Attempt to fix violations automatically
- **Default**: `"bad_rows"`

### `primaryKeyViolations` (optional)
Configuration for handling primary key constraint violations.

#### `duplicates`
- **Type**: String
- **Description**: How to handle duplicate primary key values
- **Values**: `"bad_rows"`, `"fail_fast"`, `"ignore"`, `"keep_first"`, `"keep_last"`
- **Implementation**:
  - `"keep_first"`: Keep first occurrence, discard duplicates
  - `"keep_last"`: Keep last occurrence, discard earlier duplicates

#### `nulls`
- **Type**: String
- **Description**: How to handle NULL values in primary key columns
- **Values**: `"bad_rows"`, `"fail_fast"`, `"ignore"`, `"generate_id"`
- **Implementation**:
  - `"generate_id"`: Automatically generate unique IDs for NULL primary keys

### `uniqueConstraintViolations` (optional)
- **Type**: String
- **Description**: How to handle unique constraint violations
- **Values**: `"bad_rows"`, `"fail_fast"`, `"ignore"`, `"deduplicate"`

### `notNullViolations` (optional)
- **Type**: String
- **Description**: How to handle NOT NULL constraint violations
- **Values**: `"bad_rows"`, `"fail_fast"`, `"ignore"`, `"fill_default"`

### `badRowsOutput` (optional)
Configuration for bad rows output when `errorMode` is `"bad_rows"`.

#### `enabled`
- **Type**: Boolean
- **Description**: Enable bad rows output generation
- **Default**: `true`

#### `format`
- **Type**: String
- **Description**: Output format for bad rows
- **Values**: `"parquet"`, `"json"`, `"csv"`
- **Default**: `"parquet"`

#### `includeOriginalData`
- **Type**: Boolean
- **Description**: Include original row data in bad rows output
- **Default**: `true`

#### `includeErrorDetails`
- **Type**: Boolean
- **Description**: Include detailed error information for each violation
- **Default**: `true`

#### `maxBadRows`
- **Type**: Integer or null
- **Description**: Maximum number of bad rows to collect (null = unlimited)
- **Default**: `null`

#### `createSummary`
- **Type**: Boolean
- **Description**: Generate summary report of constraint violations
- **Default**: `true`

### `validationOptions` (optional)
Advanced validation behavior configuration.

#### `continueOnError`
- **Type**: Boolean
- **Description**: Continue processing after encountering constraint violations
- **Default**: `true`

#### `collectAllErrors`
- **Type**: Boolean
- **Description**: Collect all constraint violations per row (vs. stopping at first)
- **Default**: `true`

#### `maxErrorsPerRow`
- **Type**: Integer
- **Description**: Maximum number of errors to collect per row
- **Default**: `10`

## Bad Rows Output Structure

When constraint violations occur and `errorMode` is `"bad_rows"`, the system generates a bad rows file with this structure:

```json
{
  "original_data": {
    "id": null,
    "name": "John Doe",
    "email": "john@example.com"
  },
  "errors": [
    {
      "constraint_type": "primary_key",
      "constraint_name": "pk_customers",
      "column": "id",
      "violation_type": "null_value",
      "message": "Primary key column 'id' cannot be null",
      "severity": "error"
    }
  ],
  "row_number": 1247,
  "source_file": "customers.csv",
  "processing_timestamp": "2024-08-26T10:30:00Z"
}
```

## Implementation Details

### Constraint Validation Pipeline
1. **Primary Key Validation**: Check uniqueness and null constraints
2. **Unique Constraint Validation**: Validate additional unique constraints
3. **Not Null Validation**: Verify required fields are populated
4. **Error Collection**: Aggregate all violations per row
5. **Routing Decision**: Route to main output or bad rows based on configuration

### Memory Management
- Constraint tracking uses memory-efficient data structures
- Configurable limits prevent memory exhaustion
- Streaming validation for large datasets

### Error Reporting
- Detailed violation descriptions with context
- Row-level and file-level error summaries
- Integration with logging systems

## Usage Examples

### Strict Validation (Fail Fast)
```json
{
  "x-constraintHandling": {
    "errorMode": "fail_fast",
    "primaryKeyViolations": {
      "duplicates": "fail_fast",
      "nulls": "fail_fast"
    },
    "validationOptions": {
      "continueOnError": false
    }
  }
}
```

### Permissive Processing (Continue with Warnings)
```json
{
  "x-constraintHandling": {
    "errorMode": "ignore",
    "primaryKeyViolations": {
      "duplicates": "keep_first",
      "nulls": "generate_id"
    },
    "validationOptions": {
      "continueOnError": true,
      "collectAllErrors": false
    }
  }
}
```

### Comprehensive Bad Rows Collection
```json
{
  "x-constraintHandling": {
    "errorMode": "bad_rows",
    "badRowsOutput": {
      "enabled": true,
      "format": "parquet",
      "includeOriginalData": true,
      "includeErrorDetails": true,
      "maxBadRows": 10000,
      "createSummary": true
    },
    "validationOptions": {
      "collectAllErrors": true,
      "maxErrorsPerRow": 5
    }
  }
}
```

### Data Cleaning Mode
```json
{
  "x-constraintHandling": {
    "errorMode": "transform",
    "primaryKeyViolations": {
      "duplicates": "keep_last",
      "nulls": "generate_id"
    },
    "notNullViolations": "fill_default",
    "uniqueConstraintViolations": "deduplicate"
  }
}
```

## Integration with Other Features

### Primary Key Integration
- Works with `x-primaryKey` configuration
- Enforces primary key constraints defined in schema
- Provides detailed violation reporting

### Unique Constraints Integration
- Works with `x-uniqueConstraints` definitions
- Supports multiple unique constraint validation
- Custom violation handling per constraint

### Metadata Integration
- Constraint violation statistics included in metadata
- Data quality metrics generation
- Historical violation tracking

## Performance Considerations

1. **Memory Usage**: Constraint tracking requires memory proportional to unique values
2. **Processing Speed**: Validation adds overhead, especially for complex constraints
3. **Bad Rows Storage**: Large numbers of violations can create significant output files
4. **Sampling**: Consider sampling for initial data quality assessment

## Best Practices

1. **Start Permissive**: Use `"ignore"` mode for initial data exploration
2. **Graduate to Strict**: Move to `"bad_rows"` or `"fail_fast"` for production
3. **Monitor Bad Rows**: Set up alerts for high violation rates
4. **Review Patterns**: Analyze bad rows to improve data sources
5. **Configure Limits**: Set `maxBadRows` to prevent runaway bad rows files
6. **Test Configurations**: Validate constraint handling with sample data
