# x-columnMapping Documentation

## Overview
The `x-columnMapping` extension provides comprehensive column name mapping and standardization capabilities for data processing. This feature enables automatic column name transformation, legacy system integration, and standardization of column names across different data sources with varying naming conventions.

## Schema Structure
```json
{
  "x-columnMapping": {
    "description": "Column name mapping and standardization configuration",
    "globalMappings": {
      "emp_id": "employee_id",
      "fname": "first_name",
      "lname": "last_name",
      "dob": "birth_date",
      "addr": "address",
      "ph_num": "phone_number"
    },
    "tableMappings": {
      "employees": {
        "emp_num": "employee_id",
        "dept": "department_code"
      },
      "customers": {
        "cust_id": "customer_id",
        "cust_name": "customer_name"
      }
    },
    "patternMappings": [
      {
        "pattern": "^(.+)_dt$",
        "replacement": "${1}_date",
        "description": "Convert _dt suffix to _date"
      },
      {
        "pattern": "^(.+)_amt$",
        "replacement": "${1}_amount",
        "description": "Convert _amt suffix to _amount"
      }
    ],
    "standardization": {
      "caseConversion": "snake_case",
      "removeSpecialChars": true,
      "maxLength": 63,
      "reservedWords": ["user", "order", "group"],
      "reservedWordSuffix": "_field"
    },
    "validation": {
      "requireMapping": false,
      "allowUnmapped": true,
      "logUnmapped": true,
      "duplicateHandling": "suffix"
    }
  }
}
```

## Configuration Properties

### Global Mappings

#### `globalMappings`
- **Type**: Object (key-value pairs)
- **Description**: Direct column name mappings applied across all tables/files
- **Implementation**: Simple string replacement from source to target column names
- **Use Cases**:
  - Legacy system abbreviations
  - Standardizing common field names
  - Correcting typos in source systems

```json
{
  "globalMappings": {
    "emp_id": "employee_id",
    "fname": "first_name",
    "lname": "last_name",
    "dob": "birth_date",
    "ssn": "social_security_number",
    "addr": "address",
    "ph_num": "phone_number",
    "email_addr": "email_address",
    "hire_dt": "hire_date",
    "term_dt": "termination_date",
    "sal": "salary",
    "dept": "department",
    "mgr_id": "manager_id"
  }
}
```

### Table-Specific Mappings

#### `tableMappings`
- **Type**: Object with table names as keys
- **Description**: Column mappings that apply only to specific tables
- **Implementation**: Overrides global mappings for specific tables
- **Priority**: Table-specific mappings take precedence over global mappings

```json
{
  "tableMappings": {
    "employees": {
      "emp_num": "employee_id",
      "dept_code": "department_code",
      "pos_title": "position_title",
      "sal_grade": "salary_grade"
    },
    "customers": {
      "cust_id": "customer_id",
      "cust_name": "customer_name",
      "acct_num": "account_number",
      "cred_limit": "credit_limit"
    },
    "orders": {
      "ord_id": "order_id",
      "ord_dt": "order_date",
      "ship_dt": "ship_date",
      "tot_amt": "total_amount"
    }
  }
}
```

### Pattern-Based Mappings

#### `patternMappings`
- **Type**: Array of pattern objects
- **Description**: Regular expression-based column name transformations
- **Implementation**: Applied after direct mappings, enables flexible transformations

#### Pattern Object Properties

##### `pattern` (required)
- **Type**: String (regular expression)
- **Description**: Regex pattern to match column names
- **Capture Groups**: Use parentheses to capture parts for replacement

##### `replacement` (required)
- **Type**: String
- **Description**: Replacement pattern using captured groups
- **Syntax**: Use `${1}`, `${2}`, etc. for captured groups

##### `description` (optional)
- **Type**: String
- **Description**: Human-readable explanation of the transformation

```json
{
  "patternMappings": [
    {
      "pattern": "^(.+)_dt$",
      "replacement": "${1}_date",
      "description": "Convert _dt suffix to _date"
    },
    {
      "pattern": "^(.+)_amt$",
      "replacement": "${1}_amount",
      "description": "Convert _amt suffix to _amount"
    },
    {
      "pattern": "^(.+)_num$",
      "replacement": "${1}_number",
      "description": "Convert _num suffix to _number"
    },
    {
      "pattern": "^(.+)_cd$",
      "replacement": "${1}_code",
      "description": "Convert _cd suffix to _code"
    },
    {
      "pattern": "^(.+)_desc$",
      "replacement": "${1}_description",
      "description": "Convert _desc suffix to _description"
    }
  ]
}
```

### Standardization Rules

#### `standardization`
Configuration for automatic column name standardization.

##### `caseConversion`
- **Type**: String
- **Description**: Case conversion strategy for column names
- **Values**:
  - `"snake_case"`: Convert to snake_case (recommended for most databases)
  - `"camelCase"`: Convert to camelCase
  - `"PascalCase"`: Convert to PascalCase
  - `"kebab-case"`: Convert to kebab-case
  - `"UPPER_CASE"`: Convert to UPPER_CASE
  - `"lower_case"`: Convert to lowercase
  - `"none"`: No case conversion

##### `removeSpecialChars`
- **Type**: Boolean
- **Description**: Remove special characters from column names
- **Implementation**: Removes characters not allowed in target system
- **Default**: `true`

##### `maxLength`
- **Type**: Integer
- **Description**: Maximum allowed length for column names
- **Implementation**: Truncates or abbreviates names exceeding limit
- **Common Values**: 63 (PostgreSQL), 30 (Oracle), 128 (SQL Server)

##### `reservedWords`
- **Type**: Array of strings
- **Description**: Database reserved words that cannot be used as column names
- **Implementation**: Automatically modifies names that conflict with reserved words

##### `reservedWordSuffix`
- **Type**: String
- **Description**: Suffix to append to reserved words
- **Default**: `"_field"`
- **Example**: "user" becomes "user_field"

### Validation Options

#### `validation`
Configuration for mapping validation and error handling.

##### `requireMapping`
- **Type**: Boolean
- **Description**: Whether all columns must have explicit mappings
- **Default**: `false`
- **Implementation**: When `true`, unmapped columns cause errors

##### `allowUnmapped`
- **Type**: Boolean
- **Description**: Whether to allow columns without mappings to pass through
- **Default**: `true`
- **Implementation**: When `false`, unmapped columns are dropped

##### `logUnmapped`
- **Type**: Boolean
- **Description**: Whether to log warnings for unmapped columns
- **Default**: `true`
- **Use**: Helps identify potential mapping issues

##### `duplicateHandling`
- **Type**: String
- **Description**: Strategy for handling duplicate column names after mapping
- **Values**:
  - `"suffix"`: Add numeric suffix (name_1, name_2)
  - `"prefix"`: Add numeric prefix (1_name, 2_name)
  - `"error"`: Raise error on duplicates
  - `"ignore"`: Keep first occurrence

## Implementation Details

### Processing Order
1. **Input Column Detection**: Identify all column names in source data
2. **Table-Specific Mappings**: Apply table-specific mappings first
3. **Global Mappings**: Apply global mappings to unmapped columns
4. **Pattern Mappings**: Apply regex-based transformations
5. **Standardization**: Apply case conversion and character cleanup
6. **Validation**: Check for duplicates and reserved words
7. **Final Mapping**: Generate final column name mapping

### Mapping Priority
1. **Table-Specific Mappings** (highest priority)
2. **Global Mappings**
3. **Pattern Mappings**
4. **Standardization Rules** (lowest priority)

### Conflict Resolution
- **Multiple Mappings**: First matching mapping wins
- **Duplicate Results**: Handled according to `duplicateHandling` setting
- **Reserved Words**: Automatically modified with suffix
- **Length Limits**: Intelligent truncation preserving meaning

## Usage Examples

### Legacy System Integration
```json
{
  "x-columnMapping": {
    "globalMappings": {
      "EMPNO": "employee_id",
      "ENAME": "employee_name",
      "JOB": "job_title",
      "MGR": "manager_id",
      "HIREDATE": "hire_date",
      "SAL": "salary",
      "COMM": "commission",
      "DEPTNO": "department_id"
    },
    "standardization": {
      "caseConversion": "snake_case",
      "removeSpecialChars": true
    }
  }
}
```

### Multi-Source Data Harmonization
```json
{
  "x-columnMapping": {
    "tableMappings": {
      "system_a_customers": {
        "cust_num": "customer_id",
        "cust_nm": "customer_name",
        "addr1": "address_line_1",
        "addr2": "address_line_2"
      },
      "system_b_clients": {
        "client_id": "customer_id", 
        "client_name": "customer_name",
        "street_addr": "address_line_1",
        "suite_num": "address_line_2"
      }
    },
    "patternMappings": [
      {
        "pattern": "^(.+)_nm$",
        "replacement": "${1}_name",
        "description": "Standardize _nm to _name"
      }
    ]
  }
}
```

### Database Migration
```json
{
  "x-columnMapping": {
    "globalMappings": {
      "CreatedOn": "created_at",
      "ModifiedOn": "updated_at",
      "CreatedBy": "created_by_user_id",
      "ModifiedBy": "updated_by_user_id"
    },
    "patternMappings": [
      {
        "pattern": "^(.+)ID$",
        "replacement": "${1}_id",
        "description": "Convert CamelCase ID suffix to snake_case"
      }
    ],
    "standardization": {
      "caseConversion": "snake_case",
      "maxLength": 63,
      "reservedWords": ["user", "order", "group", "index"]
    }
  }
}
```

### Financial Data Standardization
```json
{
  "x-columnMapping": {
    "globalMappings": {
      "acct_no": "account_number",
      "bal": "balance",
      "avail_bal": "available_balance",
      "tx_amt": "transaction_amount",
      "tx_dt": "transaction_date"
    },
    "patternMappings": [
      {
        "pattern": "^(.+)_bal$",
        "replacement": "${1}_balance",
        "description": "Expand balance abbreviations"
      },
      {
        "pattern": "^tx_(.+)$",
        "replacement": "transaction_${1}",
        "description": "Expand transaction abbreviations"
      }
    ]
  }
}
```

## Integration with Other Features

### With Transformations
```json
{
  "x-columnMapping": {
    "globalMappings": {
      "emp_name": "employee_name"
    }
  },
  "x-transformations": {
    "fieldSpecific": {
      "employee_name": {
        "transformations": ["stringCleaning", "caseTransformation"]
      }
    }
  }
}
```

### With Special Types
```json
{
  "x-columnMapping": {
    "globalMappings": {
      "ssn_num": "social_security_number",
      "email_addr": "email_address"
    }
  },
  "properties": {
    "social_security_number": {
      "type": "string",
      "x-special-type": "ssn"
    },
    "email_address": {
      "type": "string",
      "x-special-type": "email"
    }
  }
}
```

### With PII Handling
```json
{
  "x-columnMapping": {
    "globalMappings": {
      "ssn": "social_security_number",
      "sin": "social_insurance_number"
    }
  },
  "x-pii": {
    "fields": {
      "social_security_number": {
        "isPII": true,
        "category": "direct_identifier"
      },
      "social_insurance_number": {
        "isPII": true,
        "category": "direct_identifier"
      }
    }
  }
}
```

## Best Practices

### Mapping Design
1. **Consistent Naming**: Establish and follow consistent naming conventions
2. **Business Terminology**: Use business-friendly column names
3. **Future-Proofing**: Design mappings that accommodate future changes
4. **Documentation**: Document the business meaning of mapped column names

### Performance Considerations
1. **Mapping Complexity**: Complex regex patterns can impact performance
2. **Large Mappings**: Many mappings can slow processing
3. **Caching**: Mapping results are cached for performance
4. **Memory Usage**: Large mapping tables consume memory

### Maintenance Strategy
1. **Version Control**: Track changes to mapping configurations
2. **Testing**: Test mappings with representative data samples
3. **Monitoring**: Monitor for unmapped columns in production
4. **Regular Review**: Periodically review and update mappings

### Error Handling
1. **Validation**: Enable appropriate validation for your use case
2. **Logging**: Use logging to identify mapping issues
3. **Fallback**: Design fallback strategies for unmapped columns
4. **Alerts**: Set up alerts for high rates of unmapped columns

## Common Patterns

### Abbreviation Expansion
```json
{
  "globalMappings": {
    "addr": "address",
    "qty": "quantity",
    "amt": "amount",
    "desc": "description",
    "num": "number",
    "dt": "date",
    "tm": "time"
  }
}
```

### System Prefix Removal
```json
{
  "patternMappings": [
    {
      "pattern": "^sys_(.+)$",
      "replacement": "${1}",
      "description": "Remove system prefix"
    },
    {
      "pattern": "^app_(.+)$",
      "replacement": "${1}",
      "description": "Remove application prefix"
    }
  ]
}
```

### Unit Suffix Addition
```json
{
  "patternMappings": [
    {
      "pattern": "^(.+)_weight$",
      "replacement": "${1}_weight_kg",
      "description": "Add weight unit"
    },
    {
      "pattern": "^(.+)_distance$",
      "replacement": "${1}_distance_km",
      "description": "Add distance unit"
    }
  ]
}
```

## Troubleshooting

### Common Issues
1. **Mapping Conflicts**: Multiple mappings for same source column
2. **Circular Mappings**: Mappings that reference each other
3. **Reserved Word Conflicts**: Mapped names conflict with database reserved words
4. **Length Violations**: Mapped names exceed database limits

### Debugging Tips
1. **Enable Logging**: Use `logUnmapped: true` to identify issues
2. **Test Incrementally**: Test mappings with small data samples
3. **Validate Results**: Check final column names match expectations
4. **Monitor Performance**: Track mapping processing time
