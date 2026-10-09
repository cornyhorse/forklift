# x-calculatedColumns Documentation

## Overview
The `x-calculatedColumns` extension provides powerful capabilities for adding calculated columns including constants, expressions, and computed fields during data processing. This feature enables data enrichment, derived values, and metadata addition without requiring separate post-processing steps.

> **Status:** `import_csv` (and `read_csv`, `forklift ingest --input-kind csv`) runs `x-calculatedColumns` on every batch,
> right after `x-columnMapping`; see [In `import_csv`](#in-import_csv). Excel, SQL and fixed-width imports do not.
> Outside the engine the extension is executed by `forklift.processors.CalculatedColumnsProcessor`
> (`create_calculated_columns_processor_from_schema`).

## Schema Structure
```json
{
  "x-calculatedColumns": {
    "description": "Configuration for adding calculated columns including constants, expressions, and computed fields",
    "constants": [
      {
        "name": "data_source",
        "value": "customer_import",
        "dataType": "string",
        "description": "Source identifier for data lineage"
      }
    ],
    "expressions": [
      {
        "name": "full_name",
        "expression": "first_name + ' ' + last_name",
        "dataType": "string",
        "dependencies": ["first_name", "last_name"],
        "description": "Concatenated full name"
      }
    ],
    "calculated": [
      {
        "name": "age_years",
        "expression": "year(today()) - year(birth_date)",
        "dependencies": ["birth_date"],
        "dataType": "int32",
        "description": "Approximate age in years (calendar year difference)"
      }
    ],
    "failOnError": true,
    "validateDependencies": true
  }
}
```

## Column Types

### Constants
Static values added to every row during processing.

#### Configuration Properties
- **`name`** (required): Column name for the constant value
- **`value`** (required): Static value to assign to every row
- **`dataType`** (recommended): Parquet data type for the column; inferred from the value when omitted
- **`description`** (optional): Human-readable description of the constant

#### Supported Data Types
- `string`: Text values
- `int32`, `int64`: Integer values
- `float32`, `double`: Floating-point values
- `bool`: Boolean values
- `date32` (or `date`): Date values; ISO text such as `"2024-08-26"` is accepted as the value
- `timestamp[us]`: Timestamp values; ISO text (`"2024-08-26T10:30:00"`, `"2024-08-26 10:30:00"`); a value with `Z` or an offset is converted to UTC and the wall time is kept. Use `timestamp[us, tz=UTC]` to keep the time zone
- Any other type string the schema tooling knows (`decimal(10,2)`, `list<string>`, ...); an unknown one raises `ValueError`. When `dataType` is left out the type is inferred from the value

#### Use Cases
- **Data Lineage**: Source system identification
- **Batch Tracking**: Load date/time stamping
- **Versioning**: Schema or process version tracking
- **Partitioning**: Static partition keys
- **Compliance**: Regulatory or audit markers

#### Examples
```json
{
  "constants": [
    {
      "name": "source_system",
      "value": "CRM_v2.1",
      "dataType": "string",
      "description": "Source system and version"
    },
    {
      "name": "load_timestamp",
      "value": "2024-08-26T10:30:00Z",
      "dataType": "timestamp[us, tz=UTC]",
      "description": "Data load timestamp"
    },
    {
      "name": "is_production",
      "value": true,
      "dataType": "bool",
      "description": "Production environment flag"
    }
  ]
}
```

### Expressions
Expressions combine or transform existing column values. They use a **small, safe subset of
Python expression syntax** (not SQL). Expressions come from schema files, so they are parsed into
a syntax tree and run by a whitelist interpreter: there is no `eval`, no attribute access
(`a.b`), no lambdas, comprehensions, indexing or imports. Anything outside the whitelist is
rejected when the processor is created, even with `failOnError: false`.

#### Configuration Properties
- **`name`** (required): Column name for the expression result. It must not collide with an
  input column or another calculated column (a collision raises `ValueError`).
- **`expression`** (required): Expression to evaluate (see below)
- **`dataType`**: Result data type, for example `string`, `int`, `int32`, `int64`,
  `double`, `decimal(10,2)`, `date`, `timestamp[us]`. Unknown types raise `ValueError`. **The default is
  `string`**, so set it whenever the expression does not return text (a number cannot be stored in a
  `string` column: the import fails with `cannot be converted to string`).
- **`dependencies`**: Array of the columns used in the expression. It is needed to order calculated
  columns that use other calculated columns (and for the circular-dependency check); without it the
  columns are calculated in the order constants, `expressions`, `calculated`, and an expression that
  uses a column calculated later fails with `Unknown name`. Listing the columns of the file is good
  documentation but has no other effect
- **`description`** (optional): Description of the expression logic

#### Syntax
- **Column references**: the bare column name (a valid identifier, for example `order_total`)
- **Literals**: numbers, `'strings'`, `True`/`False`/`None`; the constants `PI`, `E`, `TRUE`,
  `FALSE`, `NULL` are predefined
- **Arithmetic**: `+ - * / // % **` and unary `-`; `+` also concatenates strings
- **Comparison**: `== != < <= > >=`
- **Logical**: `and`, `or`, `not` (lower case)
- **Conditional**: `a if condition else b`, or `if_then_else(condition, a, b)`
- **Calls**: `function(arg, ...)` using the functions below. Function names are lower case and
  case sensitive; SQL spellings such as `UPPER()` or `CASE WHEN ... END` are not supported.

**NULL handling:** arithmetic and ordering comparisons (`< <= > >=`) with a NULL operand give
NULL (SQL semantics), whatever the column is called. `==` and `!=` and the logical operators keep
Python semantics. Use `coalesce(x, default)`, `isnull(x)` and `nullif(x, y)` to handle NULLs
explicitly.

**Limits:** at most 4096 characters, 512 syntax nodes and 64 call arguments per expression;
exponents up to 10 000; integer results up to 65 536 bits; strings/sequences up to 1 000 000
elements. Larger results raise an error.

#### Built-in Functions
- **Arithmetic**: `add`, `subtract`, `multiply`, `divide`, `power`, `mod`, `abs`, `round`
  (Python rounding: halves go to the nearest even number), `floor`, `ceil`, `sqrt`, `log`,
  `log10`, `sin`, `cos`, `tan`
- **String**: `concat`, `upper`, `lower`, `trim`, `length`, `substring(x, start, length=None)`,
  `replace(x, old, new)`, `left(x, n)`, `right(x, n)`
- **Conditional / NULL**: `if_then_else`, `coalesce`, `nullif`, `isnull`, `isnotnull`
- **Conversion**: `to_string`, `to_int`, `to_float`, `to_bool` (understands `'true'/'false'`,
  `'yes'/'no'`, `'1'/'0'`; other strings raise)
- **Date/time**: `now()`, `today()` (one snapshot per batch), `year`, `month`, `day`, `weekday`
- **Comparison / logic**: `equals`, `not_equals`, `greater_than`, `less_than`, `greater_equal`,
  `less_equal`, `not`. `and` and `or` are written as operators (`a > 1 and b < 2`); a call such as
  `and(a, b)` is not valid expression syntax
- **Aggregation over arguments**: `min`, `max`, `sum`, `avg` (NULL arguments are ignored; all
  NULL gives NULL)

#### Examples
```json
{
  "expressions": [
    {
      "name": "full_address",
      "expression": "street + ', ' + city + ', ' + state + ' ' + zip_code",
      "dataType": "string",
      "dependencies": ["street", "city", "state", "zip_code"],
      "description": "Complete formatted address"
    },
    {
      "name": "discount_amount",
      "expression": "order_total * (discount_percent / 100.0)",
      "dataType": "double",
      "dependencies": ["order_total", "discount_percent"],
      "description": "Calculated discount amount (NULL when either input is NULL)"
    },
    {
      "name": "customer_tier",
      "expression": "'Gold' if annual_spend >= 10000 else ('Silver' if annual_spend >= 5000 else 'Bronze')",
      "dataType": "string",
      "dependencies": ["annual_spend"],
      "description": "Customer tier based on annual spending"
    },
    {
      "name": "years_since_signup",
      "expression": "year(today()) - year(signup_date)",
      "dataType": "int32",
      "dependencies": ["signup_date"],
      "description": "Calendar years since customer signup"
    },
    {
      "name": "display_name",
      "expression": "coalesce(nickname, first_name)",
      "dataType": "string",
      "dependencies": ["nickname", "first_name"],
      "description": "Nickname when present, otherwise first name"
    }
  ]
}
```

### Calculated Fields
`calculated` entries are expressions too: they take the same `expression` key, the same expression
syntax and the same built-in functions as above. For backward compatibility the key may also be called
`function`, which is only an alias for `expression` (if both are present, `expression` wins).
**There are no separate pre-built functions** (earlier versions of this document listed
names such as `years_from_date`, `years_from_timestamp`, `string_length` or `extract_domain`; they were never
implemented: write `year(today()) - year(born)`, `length(name)`, ...).

#### Configuration Properties
- **`name`** (required): Column name for the calculated result
- **`expression`** (required; `function` is accepted as an alias): Expression to evaluate
- **`dependencies`**: Array of input column names (see Expressions)
- **`dataType`**: Expected result data type (default `string`, see Expressions)
- **`description`** (optional): Description of the calculation

#### Examples
```json
{
  "calculated": [
    {
      "name": "name_length",
      "expression": "length(trim(first_name))",
      "dependencies": ["first_name"],
      "dataType": "int32",
      "description": "Length of the trimmed first name"
    },
    {
      "name": "signup_month",
      "expression": "month(signup_date)",
      "dependencies": ["signup_date"],
      "dataType": "int32",
      "description": "Month number of the signup date"
    }
  ]
}
```

## Configuration Options

These keys sit next to `constants`, `expressions` and `calculated` in the `x-calculatedColumns`
object.

### `failOnError`
- **Type**: Boolean
- **Default**: `true`
- **Description**: When `true`, a failure while calculating a column raises `ValueError`
  (the batch is never returned without its calculated columns; in `import_csv` the import stops and
  leaves no output). When `false`, a row whose expression fails gets NULL, and when the results of a
  column cannot be converted to its `dataType` the whole column is NULL and a `CALCULATION_ERROR`
  validation result is returned (counted as `CALCULATION_ERROR:<column>` in `validation_summary`).
  Unsafe or malformed expressions always raise when the processor is created.

### `validateDependencies`
- **Type**: Boolean
- **Default**: `true`
- **Description**: Check for circular dependencies between calculated columns (a cycle raises `ValueError`)

### `addMetadata`
- **Type**: Boolean
- **Default**: `false`
- **Description**: Return a `CALCULATION_SUCCESS` validation result for every calculated column.
  `import_csv` only counts failures, so the option has no visible effect there

### `partitionColumns`
- **Type**: Array of strings
- **Description**: Columns recorded as partitioning hints in the processor configuration. **Recorded only**: `import_csv` does not partition the output and warns (`x-calculatedColumns.partitionColumns is recorded only: the output is not partitioned`)
- **Example**: `["data_source", "load_date", "customer_tier"]`

### Not implemented (ignored with a warning)
- `indexColumns`: not read (`x-calculatedColumns.indexColumns is not read and is ignored`)
- `options` (for example `{"options": {"failOnError": false}}`): not read; write `failOnError`, `addMetadata` and `validateDependencies` at the top level of `x-calculatedColumns`

## In `import_csv`

| Key | Supported | Notes |
| --- | --- | --- |
| `constants[]` | `name`, `value`, `dataType`, `description` | one value for every row |
| `expressions[]` | `name`, `expression`, `dataType` (default `string`), `dependencies`, `description` | |
| `calculated[]` | same as `expressions[]`; `function` is an alias of `expression` | |
| `failOnError`, `validateDependencies`, `addMetadata` | yes | top level of `x-calculatedColumns` |
| `partitionColumns` | recorded only | warning |
| `indexColumns`, `options` | no | warning |

- **Stage**: after the type conversion and `x-columnMapping`, before `x-dataQuality`, `x-validation`, the constraints and `x-rowHash`. Expressions see the **typed** values (a column typed `integer` is a number; a date column is a date) and the **output** column names (after `x-columnMapping`; a header name that was renamed is unknown there). Only names that are valid identifiers can be used in an expression
- **Output**: the new columns are appended after the columns of the file, constants first, then the expressions and calculated entries. A column that uses another calculated column comes after it (columns are added in rounds: every column whose calculated dependencies already exist, in the listed order, then the next round)
- **Name clashes**: a calculated column named like a column of the file (output name) or like another calculated column stops the import before any output is written (`x-calculatedColumns would overwrite existing column(s) [...]`)
- **Columns the file lacks**: list the columns an expression uses in `dependencies`. A column whose `dependencies` include a column that is not in the file is left out with a warning when `properties` declares that column (as is every column that depends on it); a dependency that nothing declares raises `ValueError` before any output is written
- **Dates and timestamps**: a constant or an expression result for a `date32` / `date` / `timestamp[us]` column may be ISO text (`"2024-08-26"`); a timestamp text with `Z` or an offset is stored as its UTC wall time (use a `tz=UTC` type to keep the zone)
- **Rejected rows** do not get calculated columns: `bad_rows.parquet` has the columns of the input file only
- **NULL**: `salary * 2` is NULL where `salary` is NULL. An empty cell in a `string` column is the empty string, not NULL, unless `x-csv.nulls` lists `""`; use `length(x) == 0` or `nullif(x, '')`

### Example

```
name,age,born
Ann,30,1990-05-01
Bob,17,2008-01-31
,41,
```

with

```json
{
  "type": "object",
  "properties": {
    "name": {"type": "string"},
    "age": {"type": "integer"},
    "born": {"type": "string", "format": "date"}
  },
  "x-calculatedColumns": {
    "constants": [
      {"name": "source", "value": "csv", "dataType": "string"},
      {"name": "loaded", "value": "2024-08-26", "dataType": "date32"}
    ],
    "expressions": [
      {"name": "age_group", "expression": "'minor' if age < 18 else 'adult'", "dataType": "string", "dependencies": ["age"]},
      {"name": "next_age", "expression": "age + 1", "dataType": "int64", "dependencies": ["age"]},
      {"name": "age_in_two", "expression": "next_age + 1", "dataType": "int64", "dependencies": ["next_age"]}
    ],
    "calculated": [
      {"name": "born_year", "expression": "year(born)", "dataType": "int32", "dependencies": ["born"]}
    ]
  }
}
```

gives

| name | age | born | source | loaded | age_group | next_age | born_year | age_in_two |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Ann | 30 | 1990-05-01 | csv | 2024-08-26 | adult | 31 | 1990 | 32 |
| Bob | 17 | 2008-01-31 | csv | 2024-08-26 | minor | 18 | 2008 | 19 |
| (empty) | 41 | (null) | csv | 2024-08-26 | adult | 42 | (null) | 43 |

## Implementation Details

### Processing Order
1. **Compilation**: every expression is parsed and checked when the processor is created
2. **Dependency analysis**: the `dependencies` between calculated columns decide the order; a cycle raises `ValueError`
3. **Rounds**: constants and every column whose calculated dependencies exist are added first, dependents in the following rounds
4. **Post-processing**: partitioning hints are recorded in the configuration, nothing else

### Error Handling
- **Configuration errors**: syntax errors, unsafe constructs, unknown functions, unknown
  `dataType`, duplicate column names and circular dependencies raise `ValueError` when the
  processor is created (in `import_csv`: before any output is written)
- **Column collisions**: a calculated column named like an existing input column raises
  `ValueError` (in `import_csv`: before any output is written)
- **Evaluation errors** (type mismatches, limits exceeded, invalid `to_bool` input, an unknown
  column name, ...): raise with `failOnError: true`; with `false` the row gets NULL (and a result that
  does not fit `dataType` makes the whole column NULL with a `CALCULATION_ERROR` result).
  Division by zero in `divide()` and `mod()` yields NULL.
- **NULL inputs**: arithmetic and ordering comparisons give NULL (see Syntax above)
- Error messages name the column and the construct involved, never cell values

### Performance Considerations
- **Per-row evaluation**: expressions are parsed once per processor and then evaluated row by
  row, so very large batches with many expressions are slower than Arrow compute kernels
- **Memory Usage**: results are bounded by the limits listed above
- **Dependency Chains**: long chains of dependent calculations increase processing time

## Usage Examples

### Data Lineage and Auditing
```json
{
  "x-calculatedColumns": {
    "constants": [
      {
        "name": "ingestion_batch_id",
        "value": "batch_20240826_001",
        "dataType": "string"
      },
      {
        "name": "schema_version",
        "value": "v2.1",
        "dataType": "string"
      },
      {
        "name": "processed_at",
        "value": "2024-08-26T10:30:00Z",
        "dataType": "timestamp[us, tz=UTC]"
      }
    ]
  }
}
```

### Customer Analytics
```json
{
  "x-calculatedColumns": {
    "expressions": [
      {
        "name": "customer_lifetime_value",
        "expression": "total_orders * average_order_value * estimated_lifetime_months",
        "dataType": "double",
        "dependencies": ["total_orders", "average_order_value", "estimated_lifetime_months"]
      }
    ],
    "calculated": [
      {
        "name": "account_age_years",
        "expression": "year(today()) - year(signup_date)",
        "dependencies": ["signup_date"],
        "dataType": "int32"
      }
    ]
  }
}
```

### Address Standardization
```json
{
  "x-calculatedColumns": {
    "expressions": [
      {
        "name": "normalized_address",
        "expression": "upper(trim(street_address)) + ', ' + upper(city) + ', ' + state_code + ' ' + zip_code",
        "dataType": "string",
        "dependencies": ["street_address", "city", "state_code", "zip_code"]
      },
      {
        "name": "zip_is_five_digits",
        "expression": "length(trim(zip_code)) == 5",
        "dataType": "bool",
        "dependencies": ["zip_code"]
      }
    ]
  }
}
```

## Best Practices

1. **Plan Dependencies**: Map out column dependencies before configuration
2. **Test Expressions**: Validate complex expressions with sample data
3. **Performance Testing**: Benchmark calculated column performance impact
4. **Document Business Logic**: Clearly explain calculation rationale
5. **Error Handling**: Configure appropriate null and error handling
6. **Partitioning**: a calculated column can hold a partition key (for example a constant `load_date`), but the output is not partitioned by the engine (`partitionColumns` is recorded only)
7. **Avoid Over-calculation**: Only add columns that provide clear value
8. **Version Control**: Track changes to calculated column definitions

## Integration with Other Features

- **Pipeline**: `import_csv` and the CLI run the calculated columns on every batch, after
  `x-columnMapping` (see [In `import_csv`](#in-import_csv))
- **x-validation, x-dataQuality, keys and constraints**: they run after the calculated columns, so a
  calculated column can be validated, be a key, or carry a unique constraint; rejected rows go to
  `bad_rows.parquet` without the calculated columns
- **x-rowHash**: the row hash covers the calculated columns (list them in `excludeColumns` to leave them out)
- **x-metadata-generation**: value statistics are computed from the output data and appear only
  when `include_value_statistics` is enabled
- **x-pii**: documentation only; mark a calculated column as PII in your own schema if it derives from sensitive data
