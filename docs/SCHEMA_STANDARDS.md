# Forklift Schema Standards

Forklift uses JSON Schema as the foundation for data validation and processing configuration, with custom extensions to support advanced data processing features.

## Table of Contents

- [Base JSON Schema Structure](#base-json-schema-structure)
- [Forklift Extensions](#forklift-extensions)
- [File Format Configurations](#file-format-configurations)
- [Validation Configuration](#validation-configuration)
- [Examples](#examples)

## Base JSON Schema Structure

Forklift schemas follow the JSON Schema Draft 2020-12 specification:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/csv-example.json",
  "title": "Forklift CSV Schema - Generated",
  "description": "Schema for customer data processing",
  "type": "object",
  "properties": {
    "customer_id": {
      "type": "integer",
      "description": "Unique customer identifier"
    },
    "name": {
      "type": "string",
      "maxLength": 100
    },
    "email": {
      "type": "string",
      "format": "email"
    },
    "signup_date": {
      "type": "string",
      "format": "date"
    },
    "status": {
      "type": "string",
      "enum": ["active", "inactive", "pending"]
    }
  },
  "required": ["customer_id", "name", "email"]
}
```

## Forklift Extensions

Forklift extends JSON Schema with custom properties prefixed with `x-` to configure data processing behavior.

### Primary Key Configuration (`x-primaryKey`)

Defines primary key constraints for the data:

```json
{
  "x-primaryKey": {
    "description": "Customer ID is the primary key",
    "columns": ["customer_id"],
    "type": "single",
    "enforceUniqueness": true,
    "allowNulls": false
  }
}
```

**Properties:**
- `columns`: Array of column names that form the primary key
- `type`: `"single"` or `"composite"` 
- `enforceUniqueness`: Boolean, enforce uniqueness constraint
- `allowNulls`: Boolean, allow null values in primary key columns

### Unique Constraints (`x-uniqueConstraints`)

Define additional unique constraints:

```json
{
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email"],
      "description": "Email addresses must be unique"
    },
    {
      "name": "unique_name_company",
      "columns": ["name", "company_id"],
      "description": "Name must be unique within company"
    }
  ]
}
```

### Metadata Information (`x-metadata`)

Rich metadata about each field (generated during schema inference):

```json
{
  "x-metadata": {
    "customer_id": {
      "distinct_count": 1247,
      "null_count": 0,
      "min_value": 1,
      "max_value": 1247,
      "is_potentially_unique": true
    },
    "status": {
      "distinct_count": 3,
      "null_count": 12,
      "value_counts": {
        "active": 890,
        "inactive": 245,
        "pending": 100
      },
      "suggested_enum": true
    },
    "signup_date": {
      "distinct_count": 456,
      "null_count": 0,
      "min_value": "2020-01-15",
      "max_value": "2024-12-30",
      "date_formats_detected": ["YYYY-MM-DD"]
    }
  }
}
```

## File Format Configurations

### CSV Configuration (`x-csv`)

Comprehensive CSV processing configuration:

```json
{
  "x-csv": {
    "encodingPriority": ["utf-8", "utf-8-sig", "latin-1"],
    "delimiter": ",",
    "quotechar": "\"",
    "escapechar": "\\",
    "header": {
      "mode": "present",
      "row": 0
    },
    "nulls": {
      "global": ["", "NA", "NULL", "null", "None"],
      "perColumn": {
        "salary": ["", "0.00", "N/A"],
        "comments": ["", "No comment", "-"]
      }
    },
    "dataTypes": {
      "customer_id": "int64",
      "name": "string",
      "salary": "double",
      "signup_date": "date32"
    },
    "validation": {
      "enabled": true,
      "onError": "log",
      "maxErrors": 1000,
      "badRowsPath": "./bad_rows/"
    },
    "preprocessing": {
      "stringCleaning": {
        "enabled": true,
        "trimWhitespace": true,
        "normalizeCase": false
      },
      "dateStandardization": {
        "enabled": true,
        "inferFormats": true,
        "targetFormat": "YYYY-MM-DD"
      }
    }
  }
}
```

**Key Properties:**
- `encodingPriority`: Array of encodings to try in order
- `delimiter`: Field separator character
- `header.mode`: `"present"`, `"absent"`, or `"auto"`
- `nulls.global`: Global null value representations
- `nulls.perColumn`: Column-specific null values
- `dataTypes`: PyArrow data type mappings
- `validation`: Validation behavior configuration

### Excel Configuration (`x-excel`)

Excel-specific processing options:

```json
{
  "x-excel": {
    "sheet": "CustomerData",
    "header": {
      "mode": "present",
      "row": 0
    },
    "skipRows": 2,
    "maxRows": 10000,
    "nulls": {
      "global": ["", "NA", "NULL"]
    },
    "dataTypes": {
      "customer_id": "int64",
      "name": "string",
      "signup_date": "date32"
    },
    "validation": {
      "enabled": true,
      "onError": "log"
    }
  }
}
```

### Fixed-Width File Configuration (`x-fwf`)

Configuration for fixed-width file processing:

```json
{
  "x-fwf": {
    "encoding": "utf-8",
    "recordTypes": {
      "customer": {
        "flag": {
          "column": "record_type",
          "position": {"start": 1, "length": 1},
          "value": "C"
        },
        "fields": [
          {
            "name": "record_type",
            "start": 1,
            "length": 1,
            "type": "string"
          },
          {
            "name": "customer_id",
            "start": 2,
            "length": 8,
            "type": "integer"
          },
          {
            "name": "name",
            "start": 10,
            "length": 30,
            "type": "string"
          }
        ]
      },
      "transaction": {
        "flag": {
          "column": "record_type",
          "position": {"start": 1, "length": 1},
          "value": "T"
        },
        "fields": [
          {
            "name": "record_type",
            "start": 1,
            "length": 1,
            "type": "string"
          },
          {
            "name": "transaction_id",
            "start": 2,
            "length": 10,
            "type": "integer"
          },
          {
            "name": "amount",
            "start": 12,
            "length": 12,
            "type": "decimal"
          }
        ]
      }
    }
  }
}
```

## Validation Configuration

### Constraint Validation (`x-constraints`)

Define various data constraints:

```json
{
  "x-constraints": {
    "fieldValidation": {
      "customer_id": [
        {
          "type": "range",
          "min": 1,
          "max": 999999,
          "message": "Customer ID must be between 1 and 999999"
        }
      ],
      "email": [
        {
          "type": "regex",
          "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$",
          "message": "Invalid email format"
        }
      ],
      "status": [
        {
          "type": "enum",
          "values": ["active", "inactive", "pending"],
          "message": "Status must be active, inactive, or pending"
        }
      ]
    },
    "crossFieldValidation": [
      {
        "type": "conditional",
        "condition": "status == 'active'",
        "requirement": "email IS NOT NULL",
        "message": "Active customers must have an email address"
      }
    ]
  }
}
```

### Processing Configuration (`x-processing`)

Configure data transformation and processing:

```json
{
  "x-processing": {
    "calculatedColumns": [
      {
        "name": "full_name",
        "type": "expression",
        "expression": "CONCAT(first_name, ' ', last_name)",
        "dataType": "string"
      },
      {
        "name": "process_date",
        "type": "constant",
        "value": "2024-01-15",
        "dataType": "date32"
      }
    ],
    "columnMapping": {
      "customer_name": "name",
      "cust_id": "customer_id"
    },
    "stringCleaning": {
      "columns": ["name", "address"],
      "operations": ["trim", "normalize_whitespace", "title_case"]
    }
  }
}
```

## Examples

### Complete Customer Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/schemas/customers.json",
  "title": "Customer Data Schema",
  "description": "Schema for customer data import and validation",
  "type": "object",
  "properties": {
    "customer_id": {
      "type": "integer",
      "description": "Unique customer identifier"
    },
    "name": {
      "type": "string",
      "maxLength": 100,
      "description": "Customer full name"
    },
    "email": {
      "type": "string",
      "format": "email",
      "description": "Customer email address"
    },
    "signup_date": {
      "type": "string",
      "format": "date",
      "description": "Date customer signed up"
    },
    "status": {
      "type": "string",
      "enum": ["active", "inactive", "pending"],
      "description": "Customer status"
    }
  },
  "required": ["customer_id", "name", "email"],
  "x-primaryKey": {
    "columns": ["customer_id"],
    "type": "single",
    "enforceUniqueness": true,
    "allowNulls": false
  },
  "x-uniqueConstraints": [
    {
      "name": "unique_email",
      "columns": ["email"]
    }
  ],
  "x-csv": {
    "encodingPriority": ["utf-8", "utf-8-sig"],
    "delimiter": ",",
    "header": {"mode": "present"},
    "nulls": {
      "global": ["", "NA", "NULL"]
    },
    "dataTypes": {
      "customer_id": "int64",
      "name": "string",
      "email": "string",
      "signup_date": "date32",
      "status": "string"
    },
    "validation": {
      "enabled": true,
      "onError": "bad_rows",
      "badRowsPath": "./validation_errors/"
    }
  },
  "x-constraints": {
    "fieldValidation": {
      "customer_id": [
        {
          "type": "range",
          "min": 1,
          "message": "Customer ID must be positive"
        }
      ]
    }
  }
}
```

### Multi-Record FWF Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Multi-Record Fixed Width File Schema",
  "description": "Schema for processing files with multiple record types",
  "x-fwf": {
    "encoding": "utf-8",
    "recordTypes": {
      "header": {
        "flag": {
          "column": "record_type",
          "position": {"start": 1, "length": 1},
          "value": "H"
        },
        "fields": [
          {"name": "record_type", "start": 1, "length": 1, "type": "string"},
          {"name": "file_date", "start": 2, "length": 8, "type": "string"},
          {"name": "batch_id", "start": 10, "length": 10, "type": "string"}
        ]
      },
      "detail": {
        "flag": {
          "column": "record_type",
          "position": {"start": 1, "length": 1},
          "value": "D"
        },
        "fields": [
          {"name": "record_type", "start": 1, "length": 1, "type": "string"},
          {"name": "customer_id", "start": 2, "length": 8, "type": "integer"},
          {"name": "amount", "start": 10, "length": 12, "type": "decimal"},
          {"name": "transaction_date", "start": 22, "length": 8, "type": "string"}
        ]
      }
    }
  }
}
```

