# x-primaryKey Documentation

## Overview
The `x-primaryKey` extension provides primary key configuration for data uniqueness in Forklift schemas. `import_csv` enforces it while it reads the file: the first row of a key is kept, later rows with the same key (and rows with a NULL key) are rejected to `bad_rows.parquet`, or the import fails, depending on `x-constraintHandling.errorMode`.

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
- **Type**: Array of strings (non-empty, no name twice)
- **Description**: List of column names that comprise the primary key
- **Examples**:
  - Single column: `["id"]`
  - Composite key: `["customer_id", "order_date"]`
- **Names**: the output names of the columns, that is the names after `x-columnMapping` (a header name that was renamed is accepted and resolved). A column that is not in the file stops the import with a `ValueError` before any output is written

### `type` (optional)
- **Type**: String
- **Description**: Documents the type of primary key; it is only checked against the number of columns
- **Values**:
  - `"single"`: exactly one column
  - `"composite"`: at least two columns
- **Example**: `"single"`
- Any other value, or a value that disagrees with `columns`, raises a `ValueError`

### `enforceUniqueness` (optional)
- **Type**: Boolean
- **Description**: Controls whether duplicate keys are violations
- **Implementation**:
  - When `true`: the second and every later row with the same key (a tuple for a composite key) is a violation (`UNIQUE_VIOLATION:<first key column>`); the first row is kept
  - When `false`: duplicate keys are allowed and nothing is reported
- **Default**: `true`
- **Example**: `true`

### `allowNulls` (optional)
- **Type**: Boolean
- **Description**: Controls whether NULL values are permitted in primary key columns
- **Implementation**:
  - When `false`: a row with a NULL in any key column is a violation (`NULL_VIOLATION:<that column>`)
  - When `true`: NULL keys are accepted; a key with a NULL part is never compared, so such rows are never duplicates of each other
- **Default**: `false`
- **Example**: `false`
- An empty cell is NULL for number, date and boolean columns. In a `string` column it is the empty string (a value) unless `x-csv.nulls` lists `""`

### `description` (optional)
- **Type**: String
- **Description**: Human-readable description of the primary key configuration
- **Example**: `"Primary key configuration for data uniqueness and referential integrity"`

### `description_detail` (optional)
- **Type**: String
- **Description**: Detailed explanation of the specific primary key implementation
- **Example**: `"Single column primary key on 'id' field ensuring row uniqueness"`

Any other key is ignored and reported in `results.warnings` (`x-primaryKey.<key> is not supported and is ignored`).

## In `import_csv`

| Setting | Effect | Reason in `bad_rows.parquet` / `validation_summary` |
| --- | --- | --- |
| `enforceUniqueness: true` (default) | first row of a key wins, later rows are rejected | `UNIQUE_VIOLATION:<first key column>` |
| `allowNulls: false` (default) | rows with a NULL key column are rejected | `NULL_VIOLATION:<null column>` |
| `x-constraintHandling.errorMode` | `bad_rows` (default), `fail_fast`, `fail_complete` | - |

With `enforceUniqueness: false` and `allowNulls: true` the extension checks nothing (the key columns must still exist in the file).

Stage: the key is checked after `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality` and `x-validation`, together with the other constraints, on typed values. A row that was rejected earlier (type conversion, `required`, `x-validation`) or by another constraint of the same batch does not claim its key, so a later row with that key is kept. The set of keys seen so far is kept for the whole file (also across batches).

### Example

```
id,name,age
1,Ann,30
2,Bob,41
2,Bo,22
,Cy,50
3,Di,60
```

with `{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "age": {"type": "integer"}}, "x-primaryKey": {"columns": ["id"]}}` gives

| `data.parquet` | id | name | age |
| --- | --- | --- | --- |
| | 1 | Ann | 30 |
| | 2 | Bob | 41 |
| | 3 | Di | 60 |

| `bad_rows.parquet` | id | name | age | _rejection_reason |
| --- | --- | --- | --- | --- |
| | 2 | Bo | 22 | UNIQUE_VIOLATION:id |
| | (null) | Cy | 50 | NULL_VIOLATION:id |

`results.validation_summary` is `{'NULL_VIOLATION:id': 1, 'UNIQUE_VIOLATION:id': 1}`. With `"x-constraintHandling": {"errorMode": "fail_fast"}` the import raises a `ValueError` at the first violation and writes no output file.

## Implementation Details

### Uniqueness Enforcement
When `enforceUniqueness` is `true`, the system:
1. Tracks all primary key values seen so far (in memory, across batches)
2. Identifies duplicate primary key combinations
3. Rejects the duplicate rows (or fails, see `errorMode`)
4. Counts each violation in `validation_summary`

### Null Handling
When `allowNulls` is `false`, the system:
1. Validates that no primary key column contains NULL values
2. Rejects rows with NULL primary keys
3. Counts each violation in `validation_summary`

### Error Handling Integration
Primary key violations follow `x-constraintHandling.errorMode`, the same mode as for `x-uniqueConstraints` and the per-property constraints. There are no separate settings for duplicates and NULLs (`primaryKeyViolations` is not implemented, see [x-constraintHandling](./X_CONSTRAINT_HANDLING_DOCUMENTATION.md)).

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
Only the combination must be unique; the reason of a duplicate names the first key column (`UNIQUE_VIOLATION:order_id`).

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
This checks nothing; it only documents the key.

## Related Features
- **x-constraintHandling**: `errorMode` decides what happens when a primary key is violated
- **x-uniqueConstraints**: Defines additional unique constraints beyond primary key (the same key written twice is checked once)
- **x-metadata-generation**: The output metadata is collected from the rows that passed

## Performance Considerations
- Primary key validation keeps every distinct key in memory for the whole import
- Large datasets with composite keys need correspondingly more memory
- The violations kept in memory are bounded (the first 1000); `validation_summary` counts stay exact

## Best Practices
1. Always keep `allowNulls: false` for true primary keys
2. Use meaningful column names in the `columns` array
3. Document business logic in `description` and `description_detail`
4. Test primary key constraints with sample data before production use
5. Consider memory for composite keys with high cardinality
