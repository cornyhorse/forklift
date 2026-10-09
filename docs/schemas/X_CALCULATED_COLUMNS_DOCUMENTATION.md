# x-calculatedColumns Documentation

## Overview
The `x-calculatedColumns` extension provides powerful capabilities for adding calculated columns including constants, expressions, and computed fields during data processing. This feature enables data enrichment, derived values, and metadata addition without requiring separate post-processing steps.

> **Status:** `x-calculatedColumns` is parsed by the schema importers and executed by
> `forklift.processors.CalculatedColumnsProcessor` (see
> `create_calculated_columns_processor_from_schema`). The `import_csv` pipeline and the CLI do
> **not** run it automatically; apply the processor to your record batches yourself.

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
        "function": "year(today()) - year(birth_date)",
        "dependencies": ["birth_date"],
        "dataType": "int32",
        "description": "Approximate age in years (calendar year difference)"
      }
    ],
    "failOnError": true,
    "validateDependencies": true,
    "partitionColumns": ["data_source", "load_date"]
  }
}
```

## Column Types

### Constants
Static values added to every row during processing.

#### Configuration Properties
- **`name`** (required): Column name for the constant value
- **`value`** (required): Static value to assign to every row
- **`dataType`** (required): Parquet data type for the column
- **`description`** (optional): Human-readable description of the constant

#### Supported Data Types
- `string`: Text values
- `int32`, `int64`: Integer values
- `float32`, `double`: Floating-point values
- `bool`: Boolean values
- `date32`: Date values (YYYY-MM-DD format)
- `timestamp[us]`: Timestamp values

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
      "dataType": "timestamp[us]",
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
- **`dataType`** (required): Result data type, for example `string`, `int`, `int32`, `int64`,
  `double`, `decimal(10,2)`, `date`, `timestamp[us]`. Unknown types raise `ValueError`.
- **`dependencies`** (required): Array of column names used in the expression
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
  `less_equal`, `and`, `or`, `not`
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
`calculated` entries use a `function` key. For backward compatibility `function` is simply an
alias for `expression`: it takes the same expression syntax and the same built-in functions as
above. **There are no separate pre-built functions** (earlier versions of this document listed
names such as `years_from_date` or `extract_domain`; they were never implemented).

#### Configuration Properties
- **`name`** (required): Column name for the calculated result
- **`function`** (required): Expression to evaluate (alias of `expression`)
- **`dependencies`** (required): Array of input column names
- **`dataType`** (required): Expected result data type
- **`description`** (optional): Description of the calculation

#### Examples
```json
{
  "calculated": [
    {
      "name": "name_length",
      "function": "length(trim(first_name))",
      "dependencies": ["first_name"],
      "dataType": "int32",
      "description": "Length of the trimmed first name"
    },
    {
      "name": "signup_month",
      "function": "month(signup_date)",
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
  (the batch is never returned without its calculated columns). When `false`, the failing column
  is filled with NULLs and a `CALCULATION_ERROR` validation result is returned.
  Unsafe or malformed expressions always raise when the processor is created.

### `validateDependencies`
- **Type**: Boolean
- **Default**: `true`
- **Description**: Check for circular dependencies between calculated columns

### `addMetadata`
- **Type**: Boolean
- **Default**: `false`
- **Description**: Return a `CALCULATION_SUCCESS` validation result for every calculated column

### `partitionColumns`
- **Type**: Array of strings
- **Description**: Columns recorded as partitioning hints in the processor configuration
- **Example**: `["data_source", "load_date", "customer_tier"]`

## Implementation Details

### Processing Order
1. **Dependency Analysis**: Build dependency graph to determine calculation order
2. **Validation**: Verify all required columns exist and types match
3. **Constants**: Add constant values first (no dependencies)
4. **Expressions & Calculated**: Process in dependency order
5. **Post-processing**: Partitioning hints are recorded in the configuration

### Error Handling
- **Configuration errors**: syntax errors, unsafe constructs, unknown functions, unknown
  `dataType`, duplicate column names and circular dependencies raise `ValueError` when the
  processor is created
- **Column collisions**: a calculated column named like an existing input column raises
  `ValueError` when a batch is processed
- **Evaluation errors** (type mismatches, limits exceeded, invalid `to_bool` input, ...): raise
  with `failOnError: true`; with `false` the column is NULL and a `CALCULATION_ERROR` result is
  returned. Division by zero in `divide()` and `mod()` yields NULL.
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
        "dataType": "timestamp[us]"
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
        "function": "year(today()) - year(signup_date)",
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
6. **Partition Strategy**: Use calculated columns for effective partitioning
7. **Avoid Over-calculation**: Only add columns that provide clear value
8. **Version Control**: Track changes to calculated column definitions

## Integration with Other Features

- **Pipeline**: calculated columns are not part of `import_csv`/the CLI yet; run
  `CalculatedColumnsProcessor.process_batch` on your batches (see Status above)
- **x-metadata-generation**: value statistics are computed from the output data and appear only
  when `include_value_statistics` is enabled
- **x-pii**: mark a calculated column as PII in your own schema if it derives from sensitive data
