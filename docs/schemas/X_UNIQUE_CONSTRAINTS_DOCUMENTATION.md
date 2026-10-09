# x-uniqueConstraints Documentation

## Overview
The `x-uniqueConstraints` extension provides configuration for additional unique constraints beyond the primary key. This feature enables enforcement of business rules requiring unique combinations of columns. `import_csv` enforces each constraint while it reads the file: the first row with a key is kept, later rows with the same key are rejected to `bad_rows.parquet` (or the import fails, see `x-constraintHandling.errorMode`).

## Schema Structure
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email_address"],
      "description": "Email addresses must be unique across all customers"
    },
    {
      "name": "unique_customer_order",
      "columns": ["customer_id", "order_date", "order_number"],
      "description": "Each customer can only have one order with the same number on the same date"
    }
  ]
}
```

## Configuration Properties

### Constraint Definition

#### `name` (optional)
- **Type**: String
- **Description**: Identifier for the constraint; informational. Two constraints may not have the same name (`ValueError`)
- **Naming Convention**: Descriptive name indicating purpose (e.g., "unique_email", "unique_customer_product")

#### `columns` (required)
- **Type**: Array of strings (non-empty, no name twice)
- **Description**: List of column names that must be unique together
- **Implementation**: The combination of values across all listed columns must be unique. A column that is not in the file stops the import with a `ValueError` before any output is written. Names are output names (after `x-columnMapping`; a renamed header name is accepted)
- **Examples**:
  - Single column: `["email"]`
  - Multi-column: `["customer_id", "product_id", "order_date"]`

#### `description` (optional)
- **Type**: String
- **Description**: Human-readable explanation of the business rule

### Not implemented (ignored with a warning)

These keys are accepted in a schema but no processor reads them. Each one produces a line in `results.warnings` (and on the CLI's stderr), and the constraint is enforced without it:

| Key | What the engine does instead |
| --- | --- |
| `condition` | the constraint applies to every row |
| `ignoreNulls: false` | NULL keys are never compared (as if `ignoreNulls` were true); `ignoreNulls: true` is what happens anyway and is not reported |
| `caseSensitive: false` | values are compared case-sensitively (`Ann` and `ann` are different); `caseSensitive: true` is not reported |

## In `import_csv`

- **NULL keys**: a key with a NULL in any of its columns is never compared, so any number of rows may have a NULL there (SQL semantics). In a `string` column an empty cell is the empty string (a value, and so can be a duplicate) unless `x-csv.nulls` lists `""`
- **Which row is kept**: the first one in the file; a row only claims its keys if it passes every check, so a row rejected for another reason (type conversion, `required`, `x-validation`, another constraint) never makes a later row look like a duplicate
- **Across batches**: the keys seen so far are kept for the whole file
- **Same key twice**: a constraint that repeats the primary key, another constraint (in any column order) or a per-property `x-unique` is checked once
- **Stage**: after `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality` and `x-validation`, on typed values (`1` and `1.0` are the same number)
- **Reason**: `UNIQUE_VIOLATION:<first column of the constraint>` in `_rejection_reason` and in `validation_summary`. A row that breaks several constraints lists each reason, joined by `; `

### Example

```
id,name,age
1,Ann,30
2,Ann,30
3,Bob,
4,Bob,
5,Cy,7
6,Di,7
```

with

```json
{
  "type": "object",
  "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "age": {"type": "integer"}},
  "x-uniqueConstraints": [
    {"name": "u_name_age", "columns": ["name", "age"]},
    {"name": "u_age", "columns": ["age"]}
  ]
}
```

gives

| `data.parquet` | id | name | age |
| --- | --- | --- | --- |
| | 1 | Ann | 30 |
| | 3 | Bob | (null) |
| | 4 | Bob | (null) |
| | 5 | Cy | 7 |

| `bad_rows.parquet` | id | name | age | _rejection_reason |
| --- | --- | --- | --- | --- |
| | 2 | Ann | 30 | UNIQUE_VIOLATION:name; UNIQUE_VIOLATION:age |
| | 6 | Di | 7 | UNIQUE_VIOLATION:age |

Rows 3 and 4 both have a NULL age, so neither constraint compares them. `results.validation_summary` is `{'UNIQUE_VIOLATION:name': 1, 'UNIQUE_VIOLATION:age': 2}`.

## Constraint Types

### Single Column Constraints
Enforce uniqueness on individual columns.

```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email_address"],
      "description": "Email addresses must be unique"
    },
    {
      "name": "unique_employee_id",
      "columns": ["employee_id"],
      "description": "Employee IDs must be unique"
    }
  ]
}
```
A per-property `"x-unique": true` in `properties` does the same for one column.

### Multi-Column Constraints
Enforce uniqueness on combinations of columns.

```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_order_line",
      "columns": ["order_id", "line_number"],
      "description": "Each order can only have one line with the same line number"
    },
    {
      "name": "unique_employee_department_role",
      "columns": ["employee_id", "department", "role"],
      "description": "Employee can only have one role per department"
    }
  ]
}
```

### Conditional and case-insensitive constraints
Not implemented (`condition`, `caseSensitive: false`, see above). To enforce a rule such as "usernames are unique regardless of case", add a calculated column (`lower(username)`, see [x-calculatedColumns](./X_CALCULATED_COLUMNS_DOCUMENTATION.md)) and put the constraint on it (the extra column is then part of `data.parquet`), or lower-case the column itself with a `string_cleaning` transformation. A rule that only applies to some rows has no equivalent: check it after the import.

## Implementation Details

### Validation Process
1. **Constraint Registration**: the constraint definitions are read when the import starts; invalid ones stop it before any output is written
2. **Data Collection**: the keys of the rows seen so far are kept in a set
3. **Uniqueness Checking**: each batch is checked against that set and against itself
4. **Violation Detection**: the rows whose key was seen before are the violations
5. **Error Handling**: the rows are rejected or the import fails according to `x-constraintHandling.errorMode`

### Memory Management
- **Hash-based Tracking**: one set of keys per distinct constraint; memory grows with the number of distinct keys
- **Violations**: only the first 1000 violations are kept in memory (`ConstraintConfig.max_retained_violations`), their counts stay exact, and no cell values are stored
- **Streaming Validation**: the data itself is processed batch by batch

## Error Handling Integration

### With x-constraintHandling
Unique constraint violations follow `errorMode` (`bad_rows`, `fail_fast`, `fail_complete`), the same as the primary key and the per-property constraints. There is no separate `uniqueConstraintViolations` setting.

```json
{
  "x-constraintHandling": {
    "errorMode": "bad_rows"
  },
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email_address"]
    }
  ]
}
```

### Violation Output Format
A rejected row is written to `bad_rows.parquet` in the shape of the input file, with the reason as an extra last column:

| id | email_address | status | _rejection_reason |
| --- | --- | --- | --- |
| 1001 | john@example.com | active | UNIQUE_VIOLATION:email_address |

The reason never contains the offending value. With `fail_fast` the `ValueError` names the violated constraint (an internal name such as `a+b_unique`, built from the columns, not the schema `name`) and the 0-based position of the row in its batch; with `fail_complete` it reports the number of violations after the whole file was checked. In both cases no output file is left behind.

## Usage Examples

### E-commerce System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_customer_email",
      "columns": ["email_address"],
      "description": "Customer email addresses must be unique"
    },
    {
      "name": "unique_order_line_item",
      "columns": ["order_id", "product_id", "variant_id"],
      "description": "Each order can contain each product variant only once"
    }
  ]
}
```

### HR Management System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_employee_badge",
      "columns": ["badge_number"],
      "description": "Badge numbers must be unique; employees without a badge (NULL) are not compared"
    },
    {
      "name": "unique_employee_position",
      "columns": ["employee_id", "position_title"],
      "description": "An employee can hold each position title only once"
    }
  ]
}
```

### Financial System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_account_number",
      "columns": ["account_number"],
      "description": "Account numbers must be globally unique"
    },
    {
      "name": "unique_daily_transaction",
      "columns": ["account_id", "transaction_date", "reference_number"],
      "description": "Each account can only have one transaction per reference per day"
    }
  ]
}
```

### Multi-Tenant System
```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_tenant_username",
      "columns": ["tenant_id", "username"],
      "description": "Usernames must be unique within each tenant (case-sensitive)"
    },
    {
      "name": "unique_tenant_resource",
      "columns": ["tenant_id", "resource_name", "resource_type"],
      "description": "Resource names must be unique per type within tenant"
    }
  ]
}
```

## Best Practices

### Constraint Design
1. **Business Logic First**: Design constraints based on actual business rules
2. **Performance Impact**: Consider the memory cost of multi-column constraints with many distinct keys
3. **Null Handling**: Remember that a key with a NULL part is never compared
4. **Case Sensitivity**: Values are compared exactly; normalise them first (`x-transformations`, e.g. `string_cleaning` with `case_transform`) if case or whitespace must not matter

### Naming Conventions
1. **Descriptive Names**: Use names that clearly indicate the constraint purpose
2. **Consistent Prefixing**: Use consistent prefixes like "unique_" for all unique constraints
3. **Column Indication**: Include key column names in constraint names when helpful

### Error Handling Strategy
1. **Appropriate Actions**: Choose the right `errorMode` (`bad_rows` to keep going, `fail_fast` / `fail_complete` to stop)
2. **Monitoring**: Watch `results.validation_summary` for `UNIQUE_VIOLATION:<column>` counts
3. **Business Impact**: Consider that the first row of a key wins, whichever it is

## Integration Considerations

### With Primary Keys
- Unique constraints complement primary key constraints
- A constraint on the primary key columns is checked once together with the key
- Consider composite constraints that include primary key columns

### With Transformations
- Transformations run first (on the text of the file), so the constraint sees the cleaned values
- Consider how transformations might affect uniqueness (for example `case_transform` makes `Ann` and `ann` equal)

### With Column Mapping
- Use the output names; a renamed header name is accepted too

## Monitoring and Maintenance

### Constraint Violation Tracking
1. **Counts**: `results.validation_summary` (also in `metadata.json` and printed by the CLI)
2. **Rows**: `bad_rows.parquet` with `_rejection_reason`
3. **Trends**: compare the counts between runs
