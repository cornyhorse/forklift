# x-primaryKey Documentation

## Overview
The `x-primaryKey` extension provides comprehensive primary key configuration for data uniqueness and referential integrity in Forklift schemas. This feature enables enforcement of primary key constraints during data processing with customizable behavior for violations.

## Schema Structure
```json
{
  "x-primaryKey": {
    "description": "Primary key configuration for data uniqueness and referential integrity",
    "columns": ["id"],
    "type": "single",
    "enforceUniqueness": true,
    "allowNulls": false,
    "description_detail": "Single column primary key on 'id' field ensuring row uniqueness"
  }
}
```

## Configuration Properties

### `columns` (required)
- **Type**: Array of strings
- **Description**: List of column names that comprise the primary key
- **Examples**:
  - Single column: `["id"]`
  - Composite key: `["customer_id", "order_date"]`

### `type` (required)
- **Type**: String
- **Description**: Specifies the type of primary key
- **Values**:
  - `"single"`: Single column primary key
  - `"composite"`: Multi-column composite primary key
- **Example**: `"single"`

### `enforceUniqueness` (required)
- **Type**: Boolean
- **Description**: Controls whether uniqueness violations should be enforced
- **Implementation**: 
  - When `true`: Duplicate primary key values result in constraint violations
  - When `false`: Allows duplicate primary key values (warning only)
- **Default**: `true`
- **Example**: `true`

### `allowNulls` (required)
- **Type**: Boolean
- **Description**: Controls whether NULL values are permitted in primary key columns
- **Implementation**:
  - When `false`: NULL values in primary key columns result in constraint violations
  - When `true`: Allows NULL values in primary key columns
- **Default**: `false`
- **Example**: `false`

### `description` (optional)
- **Type**: String
- **Description**: Human-readable description of the primary key configuration
- **Example**: `"Primary key configuration for data uniqueness and referential integrity"`

### `description_detail` (optional)
- **Type**: String
- **Description**: Detailed explanation of the specific primary key implementation
- **Example**: `"Single column primary key on 'id' field ensuring row uniqueness"`

## Implementation Details

### Uniqueness Enforcement
When `enforceUniqueness` is `true`, the system:
1. Tracks all primary key values during processing
2. Identifies duplicate primary key combinations
3. Routes duplicate rows to bad rows output (when configured)
4. Generates constraint violation reports

### Null Handling
When `allowNulls` is `false`, the system:
1. Validates that no primary key columns contain NULL values
2. Routes rows with NULL primary keys to bad rows output
3. Tracks null violations in constraint reports

### Error Handling Integration
Primary key violations integrate with the `x-constraintHandling` extension:
- Duplicate primary keys trigger `primaryKeyViolations.duplicates` behavior
- NULL primary keys trigger `primaryKeyViolations.nulls` behavior
- Can be configured to continue processing or halt on violations

## Usage Examples

### Single Column Primary Key
```json
{
  "x-primaryKey": {
    "columns": ["customer_id"],
    "type": "single",
    "enforceUniqueness": true,
    "allowNulls": false,
    "description": "Customer ID is the primary key"
  }
}
```

### Composite Primary Key
```json
{
  "x-primaryKey": {
    "columns": ["order_id", "line_item"],
    "type": "composite",
    "enforceUniqueness": true,
    "allowNulls": false,
    "description": "Composite key ensures unique order line items"
  }
}
```

### Permissive Primary Key (Development/Testing)
```json
{
  "x-primaryKey": {
    "columns": ["id"],
    "type": "single",
    "enforceUniqueness": false,
    "allowNulls": true,
    "description": "Relaxed primary key for development data"
  }
}
```

## Related Features
- **x-constraintHandling**: Configures behavior when primary key violations occur
- **x-uniqueConstraints**: Defines additional unique constraints beyond primary key
- **x-metadata-generation**: Can include primary key statistics in generated metadata

## Performance Considerations
- Primary key validation requires tracking all key values in memory
- Large datasets with composite keys may require additional memory allocation
- Consider using sampling for initial data quality assessment on very large files

## Best Practices
1. Always set `allowNulls: false` for true primary keys
2. Use meaningful column names in the `columns` array
3. Document business logic in `description` and `description_detail`
4. Test primary key constraints with sample data before production use
5. Consider performance implications for composite keys with high cardinality
