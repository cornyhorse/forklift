# x-columnMapping Documentation

## Overview
The `x-columnMapping` extension renames the columns of the data (and can drop columns). `import_csv` applies it to every batch right after the type conversion; every extension that runs after it (`x-calculatedColumns`, `x-dataQuality`, `x-validation`, the keys and constraints, `x-rowHash`) and the output file use the new names.

## Schema Structure
```json
{
  "x-columnMapping": {
    "description": "Column name mapping and standardization configuration",
    "explicitMappings": {
      "FirstName": "first_name",
      "DOB": "birth_date",
      "Email": "email_address"
    },
    "namingConvention": "snake_case",
    "caseSensitive": true,
    "allowUnmapped": true,
    "dropUnmapped": false
  }
}
```

## Configuration Properties

### `explicitMappings`
- **Type**: Object (source name -> target name)
- **Description**: Direct column renames. Keys are the names in the file header, targets are non-empty strings
- **Use Cases**:
  - Legacy system abbreviations
  - Standardizing common field names
  - Correcting typos in source systems
- A column that is not in the file is simply not renamed

```json
{
  "explicitMappings": {
    "emp_id": "employee_id",
    "fname": "first_name",
    "lname": "last_name",
    "dob": "birth_date"
  }
}
```

### `namingConvention`
- **Type**: String
- **Description**: Naming convention applied to **every** column name after the explicit mapping (to the target of an explicit mapping as well: `FirstName` -> `GivenName` -> `given_name` with `snake_case`)
- **Values** (anything else raises a `ValueError`):
  - `"snake_case"`, `"camelCase"`, `"PascalCase"`, `"lowercase"`, `"UPPERCASE"`
- **Default**: none (names are kept)

| Header | `snake_case` | `camelCase` | `PascalCase` | `lowercase` | `UPPERCASE` |
| --- | --- | --- | --- | --- | --- |
| `Last Name` | `last_name` | `lastName` | `LastName` | `last name` | `LAST NAME` |
| `customerName` | `customer_name` | `customerName` | `CustomerName` | `customername` | `CUSTOMERNAME` |
| `HTTPServer` | `http_server` | `httpServer` | `HttpServer` | `httpserver` | `HTTPSERVER` |
| `order-id` | `order_id` | `orderId` | `OrderId` | `order-id` | `ORDER-ID` |
| `ID` | `id` | `id` | `Id` | `id` | `ID` |

`snake_case`, `camelCase` and `PascalCase` split words at spaces, hyphens, underscores and case changes and treat non-ASCII letters as separators (`Années` becomes `ann_es`); `lowercase` and `UPPERCASE` only change the case.

### `caseSensitive`
- **Type**: Boolean
- **Description**: Whether the keys of `explicitMappings` are matched case-sensitively against the header names
- **Default**: `true`. With `false`, `FirstName` also renames a header `FIRSTNAME`; two keys that differ only in case but map to different names are an error

### `allowUnmapped`
- **Type**: Boolean
- **Default**: `true`
- **Description**: With `false` the columns without an entry in `explicitMappings` are dropped (same effect as `dropUnmapped: true`)

### `dropUnmapped`
- **Type**: Boolean
- **Default**: `false`
- **Description**: Drop the columns that have no entry in `explicitMappings`. A column counts as mapped when it matches an entry, even an identity entry such as `"A": "A"`; a naming convention does not make a column mapped

### `description` (optional)
- **Type**: String
- **Description**: Free text, ignored

### Not implemented (ignored with a warning)

The earlier versions of this page described more keys. No code reads them; each one is reported in `results.warnings` (`x-columnMapping.<key> is not supported and is ignored`) and the mapping works without it:

| Key | Use instead |
| --- | --- |
| `globalMappings` | `explicitMappings` |
| `tableMappings` | a schema per file (an `x-columnMapping` has no table context) |
| `patternMappings` | `explicitMappings` for the columns concerned, or `namingConvention` for systematic changes |
| `standardization` (`caseConversion`, `removeSpecialChars`, `maxLength`, `reservedWords`, `reservedWordSuffix`) and `standardizationRules` | `namingConvention` |
| `validation` (`requireMapping`, `allowUnmapped`, `logUnmapped`, `duplicateHandling`) | `allowUnmapped` / `dropUnmapped` at the top level of `x-columnMapping`; two columns with the same output name are always an error |

## In `import_csv`

### Naming rule
`properties` (types, constraints), `required`, `x-csv` (`nulls`, `parquetTypeMapping`) and `x-transformations` run **before** the rename and use the names in the file header. Everything after the rename uses the output names. A header name that was renamed is also accepted by `x-validation`, `x-dataQuality`, `x-primaryKey`, `x-uniqueConstraints` and the per-property constraints (it is resolved to its output name); `x-calculatedColumns` expressions and `x-rowHash.includeColumns` / `excludeColumns` need the output names.

A `properties` entry that is declared under the *new* name does not apply to the renamed column (the type, the constraints and `required` are matched by header name). The import warns: `column 'Score' is renamed to 'score' by x-columnMapping, but the schema properties are matched by the names in the file: the definition of 'score' is not applied to it (declare the property as 'Score')`.

### Collisions and errors
- Two columns that end up with the same output name (after the mapping and the naming convention) stop the import with `Column mapping creates duplicate output column names: ...`, before any output is written
- An invalid configuration (unknown `namingConvention`, wrong types) raises a `ValueError` naming the key
- `bad_rows.parquet` keeps the **input** names (it is in the shape of the file); the reasons in it use the output names, for example `VALIDATION_ERROR:age`

### Example

```
FirstName,Last Name,Years,Notes
Ann,Lee,30,x
Bob,Ray,41,y
```

with

```json
{
  "type": "object",
  "properties": {"Years": {"type": "integer"}},
  "x-columnMapping": {
    "explicitMappings": {"FirstName": "given_name", "Years": "age"},
    "namingConvention": "snake_case"
  }
}
```

`data.parquet` has the columns `given_name`, `last_name`, `age`, `notes` (`age` is an integer because the property `Years` was applied under the header name):

| given_name | last_name | age | notes |
| --- | --- | --- | --- |
| Ann | Lee | 30 | x |
| Bob | Ray | 41 | y |

With `"dropUnmapped": true` instead of the naming convention the columns are `given_name` and `age` only. If the schema also has `"x-validation": {"badRowsHandling": {"maxBadRowsPercent": 100}, "fieldValidations": {"age": {"range": {"max": 40}}}}`, the row of Bob goes to `bad_rows.parquet` with the columns `FirstName`, `Last Name`, `Years`, `Notes` and the reason `VALIDATION_ERROR:age`.

## Implementation Details

### Processing Order
1. **Input Column Detection**: the header names of the file
2. **Explicit Mappings**: `explicitMappings` (case-insensitive lookup with `caseSensitive: false`)
3. **Naming Convention**: applied to the result of step 2
4. **Dropping**: columns without an explicit entry are removed when `dropUnmapped` is true or `allowUnmapped` is false
5. **Collision check**: two remaining columns with the same name raise an error

The new names are worked out once from the header (`ColumnMapper.output_names`), before the first batch, so a mistake is reported before any output is written.

## Usage Examples

### Legacy System Integration
```json
{
  "x-columnMapping": {
    "explicitMappings": {
      "EMPNO": "employee_id",
      "ENAME": "employee_name",
      "JOB": "job_title",
      "MGR": "manager_id",
      "HIREDATE": "hire_date",
      "SAL": "salary",
      "COMM": "commission",
      "DEPTNO": "department_id"
    },
    "namingConvention": "snake_case"
  }
}
```

### Keep Only the Mapped Columns
```json
{
  "x-columnMapping": {
    "explicitMappings": {
      "cust_id": "customer_id",
      "cust_name": "customer_name"
    },
    "dropUnmapped": true
  }
}
```

### Database Migration
```json
{
  "x-columnMapping": {
    "explicitMappings": {
      "CreatedOn": "created_at",
      "ModifiedOn": "updated_at",
      "CreatedBy": "created_by_user_id",
      "ModifiedBy": "updated_by_user_id"
    },
    "namingConvention": "snake_case"
  }
}
```

## Integration with Other Features

### With Transformations
`x-transformations` runs before the rename, so it uses the header names:
```json
{
  "x-columnMapping": {
    "explicitMappings": {
      "emp_name": "employee_name"
    }
  },
  "x-transformations": {
    "column_transformations": {
      "emp_name": {
        "string_cleaning": {"enabled": true, "strip_whitespace": true}
      }
    }
  }
}
```

### With Special Types
`x-special-type` formats the column under the header name (as part of the transformations), so declare the property under the header name:
```json
{
  "x-columnMapping": {
    "explicitMappings": {
      "ssn_num": "social_security_number",
      "email_addr": "email_address"
    }
  },
  "properties": {
    "ssn_num": {
      "type": "string",
      "x-special-type": "ssn"
    },
    "email_addr": {
      "type": "string",
      "x-special-type": "email"
    }
  }
}
```

### With Keys and Validation
Use the new names (or the header names) in `x-primaryKey`, `x-uniqueConstraints`, `x-validation` and `x-dataQuality`:
```json
{
  "x-columnMapping": {"explicitMappings": {"Id": "id"}},
  "x-primaryKey": {"columns": ["id"]}
}
```

### With PII Handling
`x-pii` is documentation only (it is not read), so it can use any of the names.

## Best Practices

### Mapping Design
1. **Consistent Naming**: Establish and follow consistent naming conventions
2. **Business Terminology**: Use business-friendly column names
3. **Declare properties under the header name**: types, `required` and constraints are matched before the rename
4. **Documentation**: Document the business meaning of mapped column names

### Maintenance Strategy
1. **Version Control**: Track changes to mapping configurations
2. **Testing**: Test mappings with representative data samples
3. **Review the warnings**: `results.warnings` lists ignored keys and renames onto declared properties

## Troubleshooting

### Common Issues
1. **`duplicate output column names`**: two columns map to the same name; rename one of them explicitly
2. **A property is not applied after a rename**: declare it under the header name (see the warning text above)
3. **A calculated column or a hash column cannot find a column**: use the output name there

### Debugging Tips
1. **Check `data.parquet`'s columns** against the names you used in the later extensions
2. **Read `results.warnings`** for ignored keys
3. **Use `ColumnMapper.output_names(header)`** (`forklift.processors.schema_extensions.build_column_mapper(schema)`) to see the renaming without data
