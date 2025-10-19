# x-calculatedColumns Documentation

## Overview
The `x-calculatedColumns` extension provides powerful capabilities for adding calculated columns including constants, expressions, and computed fields during data processing. This feature enables data enrichment, derived values, and metadata addition without requiring separate post-processing steps.

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
        "function": "years_from_date",
        "dependencies": ["birth_date"],
        "dataType": "int32",
        "description": "Age in years calculated from birth date"
      }
    ],
    "partitionColumns": ["data_source", "load_date"],
    "indexColumns": ["customer_id", "status"],
    "options": {
      "skipIfExists": false,
      "validateDependencies": true,
      "allowNullDependencies": false
    }
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
SQL-like expressions that combine or transform existing column values.

#### Configuration Properties
- **`name`** (required): Column name for the expression result
- **`expression`** (required): SQL-like expression to evaluate
- **`dataType`** (required): Expected result data type
- **`dependencies`** (required): Array of column names used in expression
- **`description`** (optional): Description of the expression logic

#### Supported Expression Operations
- **String Operations**: Concatenation (+), substring, length, case conversion
- **Arithmetic**: +, -, *, /, % (modulo)
- **Logical**: AND, OR, NOT
- **Comparison**: =, !=, <, >, <=, >=
- **Conditional**: CASE WHEN ... THEN ... ELSE ... END
- **Functions**: Built-in functions for common operations

#### Built-in Functions
- **String Functions**: `UPPER()`, `LOWER()`, `TRIM()`, `SUBSTRING()`, `LENGTH()`
- **Date Functions**: `YEAR()`, `MONTH()`, `DAY()`, `DATEADD()`, `DATEDIFF()`
- **Math Functions**: `ABS()`, `ROUND()`, `CEILING()`, `FLOOR()`
- **Conditional**: `COALESCE()`, `NULLIF()`, `ISNULL()`

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
      "description": "Calculated discount amount"
    },
    {
      "name": "customer_tier",
      "expression": "CASE WHEN annual_spend >= 10000 THEN 'Gold' WHEN annual_spend >= 5000 THEN 'Silver' ELSE 'Bronze' END",
      "dataType": "string",
      "dependencies": ["annual_spend"],
      "description": "Customer tier based on annual spending"
    },
    {
      "name": "days_since_signup",
      "expression": "DATEDIFF(CURRENT_DATE, signup_date)",
      "dataType": "int32",
      "dependencies": ["signup_date"],
      "description": "Number of days since customer signup"
    }
  ]
}
```

### Calculated Fields
Pre-built functions for common calculations and transformations.

#### Configuration Properties
- **`name`** (required): Column name for the calculated result
- **`function`** (required): Pre-built function name
- **`dependencies`** (required): Array of input column names
- **`dataType`** (required): Expected result data type
- **`parameters`** (optional): Function-specific parameters
- **`description`** (optional): Description of the calculation

#### Available Functions

##### Date/Time Functions
- **`years_from_date`**: Calculate age/years from a date field
- **`months_from_date`**: Calculate months from a date field
- **`days_from_date`**: Calculate days from a date field
- **`date_part`**: Extract part of date (year, month, day, etc.)
- **`format_date`**: Format date according to specified pattern

##### String Functions
- **`string_length`**: Calculate string length
- **`extract_domain`**: Extract domain from email address
- **`extract_extension`**: Extract file extension from filename
- **`clean_phone`**: Standardize phone number format
- **`mask_ssn`**: Mask SSN for privacy
- **`extract_initials`**: Extract initials from name

##### Numeric Functions
- **`calculate_percentage`**: Calculate percentage of total
- **`round_currency`**: Round to currency precision
- **`normalize_score`**: Normalize values to 0-1 range
- **`calculate_variance`**: Calculate variance from mean

##### Geospatial Functions
- **`extract_state`**: Extract state from address
- **`validate_zip`**: Validate ZIP code format
- **`distance_between`**: Calculate distance between coordinates

#### Examples
```json
{
  "calculated": [
    {
      "name": "customer_age",
      "function": "years_from_date",
      "dependencies": ["birth_date"],
      "dataType": "int32",
      "description": "Customer age in years"
    },
    {
      "name": "email_domain",
      "function": "extract_domain",
      "dependencies": ["email_address"],
      "dataType": "string",
      "description": "Domain portion of email address"
    },
    {
      "name": "account_tenure_months",
      "function": "months_from_date",
      "dependencies": ["account_created_date"],
      "dataType": "int32",
      "parameters": {
        "reference_date": "current_date"
      },
      "description": "Account tenure in months"
    },
    {
      "name": "name_initials",
      "function": "extract_initials",
      "dependencies": ["first_name", "last_name"],
      "dataType": "string",
      "description": "Customer initials"
    }
  ]
}
```

## Configuration Options

### `partitionColumns`
- **Type**: Array of strings
- **Description**: Columns to use for Parquet partitioning
- **Implementation**: These columns are marked for partition-based storage optimization
- **Example**: `["data_source", "load_date", "customer_tier"]`

### `indexColumns`
- **Type**: Array of strings
- **Description**: Columns to create indexes on for query optimization
- **Implementation**: Metadata hints for downstream systems
- **Example**: `["customer_id", "order_id", "timestamp"]`

### `options`
Advanced configuration for calculated column behavior.

#### `skipIfExists`
- **Type**: Boolean
- **Description**: Skip calculation if column already exists in data
- **Default**: `false`

#### `validateDependencies`
- **Type**: Boolean
- **Description**: Validate that all dependency columns exist before processing
- **Default**: `true`

#### `allowNullDependencies`
- **Type**: Boolean
- **Description**: Allow calculations when dependency columns contain null values
- **Default**: `false`
- **Implementation**: When `false`, rows with null dependencies are excluded from calculation

## Implementation Details

### Processing Order
1. **Dependency Analysis**: Build dependency graph to determine calculation order
2. **Validation**: Verify all required columns exist and types match
3. **Constants**: Add constant values first (no dependencies)
4. **Expressions & Calculated**: Process in dependency order
5. **Post-processing**: Apply partitioning and indexing hints

### Error Handling
- **Missing Dependencies**: Routes to bad rows if required columns missing
- **Expression Errors**: Division by zero, type mismatches, etc.
- **Function Errors**: Invalid parameters or unexpected input values
- **Null Handling**: Configurable behavior for null input values

### Performance Considerations
- **Memory Usage**: Complex expressions may require additional memory
- **Processing Time**: Each calculated column adds processing overhead
- **Dependency Chains**: Long chains of dependent calculations increase complexity
- **Vectorization**: Simple operations can be vectorized for performance

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
        "name": "account_age_days",
        "function": "days_from_date",
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
    "calculated": [
      {
        "name": "state_code",
        "function": "extract_state",
        "dependencies": ["full_address"],
        "dataType": "string"
      },
      {
        "name": "zip_valid",
        "function": "validate_zip",
        "dependencies": ["zip_code"],
        "dataType": "bool"
      }
    ],
    "expressions": [
      {
        "name": "normalized_address",
        "expression": "UPPER(TRIM(street_address)) + ', ' + UPPER(city) + ', ' + state_code + ' ' + zip_code",
        "dataType": "string",
        "dependencies": ["street_address", "city", "state_code", "zip_code"]
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

- **x-transformations**: Applied before calculated columns
- **x-constraintHandling**: Calculated column errors handled by constraint system
- **x-metadata-generation**: Statistics generated for calculated columns
- **x-pii**: Calculated columns can be marked as PII if they contain sensitive derived data
