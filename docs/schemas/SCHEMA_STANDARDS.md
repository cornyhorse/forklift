# Forklift Schema Standards

Forklift uses JSON Schema as the foundation for data validation and processing configuration, with custom extensions to support advanced data processing features.

## Table of Contents

- [Base JSON Schema Structure](#base-json-schema-structure)
- [Forklift Extensions](#forklift-extensions)
- [File Format Configurations](#file-format-configurations)
- [Data Type Transformations](#data-type-transformations)
- [Validation Configuration](#validation-configuration)
- [Processing Configuration](#processing-configuration)
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

**Unique CSV Features:**
- **Encoding Detection**: Automatic detection with fallback priority list
- **Flexible Delimiter Detection**: Support for comma, tab, pipe, semicolon delimiters
- **Smart Header Detection**: Automatic detection of header presence and location
- **Per-Column Null Values**: Different null representations per column
- **Bad Rows Handling**: Configurable error handling with bad row collection

```json
{
  "x-csv": {
    "encodingPriority": ["utf-8", "utf-8-sig", "latin-1", "cp1252"],
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

### Excel Configuration (`x-excel`)

**Unique Excel Features:**
- **Multi-Sheet Support**: Specify worksheet by name or index
- **Skip Rows**: Handle files with metadata or headers above data
- **Cell Range Specification**: Process specific cell ranges
- **Formula Evaluation**: Option to evaluate Excel formulas

```json
{
  "x-excel": {
    "sheet": "CustomerData",
    "sheetIndex": 0,
    "header": {
      "mode": "present",
      "row": 0
    },
    "skipRows": 2,
    "maxRows": 10000,
    "usecols": "A:E",
    "evaluateFormulas": true,
    "nulls": {
      "global": ["", "NA", "NULL", "#N/A"]
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

**Unique FWF Features:**
- **Multi-Record Type Support**: Handle files with different record structures
- **Position-Based Field Definition**: Precise field positioning with start/length
- **Record Type Flags**: Automatic record type detection based on flag fields
- **Hierarchical Data Processing**: Support for header/detail/trailer record patterns

```json
{
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
          {
            "name": "record_type",
            "start": 1,
            "length": 1,
            "type": "string"
          },
          {
            "name": "file_date",
            "start": 2,
            "length": 8,
            "type": "string"
          },
          {
            "name": "batch_id",
            "start": 10,
            "length": 10,
            "type": "string"
          }
        ]
      },
      "detail": {
        "flag": {
          "column": "record_type",
          "position": {"start": 1, "length": 1},
          "value": "D"
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
            "name": "amount",
            "start": 10,
            "length": 12,
            "type": "decimal"
          },
          {
            "name": "transaction_date",
            "start": 22,
            "length": 8,
            "type": "string"
          }
        ]
      }
    }
  }
}
```

### JSON Configuration (`x-json`)

**Unique JSON Features:**
- **Nested Object Handling**: Flatten or preserve nested structures
- **Array Processing**: Handle arrays within JSON documents
- **JSON Lines Support**: Process line-delimited JSON files
- **Schema Inference**: Automatic schema detection from JSON structure

```json
{
  "x-json": {
    "mode": "lines",
    "flattenNested": true,
    "arrayHandling": "expand",
    "maxNestingLevel": 5,
    "nulls": {
      "global": [null, "", "null"]
    },
    "validation": {
      "enabled": true,
      "strictMode": false
    }
  }
}
```

### Parquet Configuration (`x-parquet`)

**Unique Parquet Features:**
- **Column Subset Reading**: Read only specified columns for performance
- **Predicate Pushdown**: Filter data at the file level
- **Schema Evolution**: Handle schema changes over time
- **Compression Options**: Support for different compression algorithms

```json
{
  "x-parquet": {
    "columns": ["customer_id", "name", "email"],
    "filters": [
      ["status", "=", "active"],
      ["signup_date", ">=", "2024-01-01"]
    ],
    "useThreads": true,
    "batchSize": 65536,
    "validation": {
      "enabled": true,
      "validateSchema": true
    }
  }
}
```

## Data Type Transformations

Forklift provides comprehensive data transformation capabilities through the `x-transformations` property.

### String Transformations

**Comprehensive String Cleaning:**

```json
{
  "x-transformations": {
    "name": {
      "type": "string_cleaning",
      "config": {
        "normalize_quotes": true,
        "normalize_dashes": true,
        "normalize_spaces": true,
        "collapse_whitespace": true,
        "strip_whitespace": true,
        "remove_zero_width": true,
        "remove_control_chars": true,
        "unicode_normalize": "NFKC",
        "case_transform": "proper",
        "title_case_exceptions": ["of", "the", "and"],
        "custom_case_mapping": {"california": "CA"},
        "acronyms": ["NASA", "API", "CEO"],
        "remove_accents": false,
        "fix_encoding_errors": true
      }
    }
  }
}
```

**String Operations:**

```json
{
  "x-transformations": {
    "product_code": {
      "type": "string_padding",
      "config": {
        "width": 10,
        "fillchar": "0",
        "side": "left"
      }
    },
    "description": {
      "type": "regex_replace",
      "config": {
        "pattern": "\\s+",
        "replacement": " ",
        "flags": 0
      }
    }
  }
}
```

### Numeric Transformations

**Money Type Conversion:**

```json
{
  "x-transformations": {
    "price": {
      "type": "money_conversion",
      "config": {
        "currency_symbols": ["$", "€", "£"],
        "thousands_separator": ",",
        "decimal_separator": ".",
        "parentheses_negative": true,
        "strip_whitespace": true
      }
    }
  }
}
```

**Numeric Cleaning:**

```json
{
  "x-transformations": {
    "quantity": {
      "type": "numeric_cleaning",
      "config": {
        "thousands_separator": ",",
        "decimal_separator": ".",
        "allow_nan": true,
        "nan_values": ["", "N/A", "NULL"],
        "target_type": "int64"
      }
    }
  }
}
```

### DateTime Transformations

**Advanced DateTime Processing:**

```json
{
  "x-transformations": {
    "event_date": {
      "type": "datetime",
      "config": {
        "mode": "common_formats",
        "allow_fuzzy": false,
        "from_epoch": false,
        "target_type": "datetime",
        "timezone": "UTC",
        "output_format": "YYYY-MM-DD HH:mm:ss"
      }
    },
    "timestamp": {
      "type": "datetime",
      "config": {
        "mode": "enforce",
        "format": "%Y-%m-%d %H:%M:%S",
        "to_epoch": "seconds"
      }
    }
  }
}
```

### Format-Specific Transformations

**Social Security Number (SSN) Formatting:**

```json
{
  "x-transformations": {
    "ssn": {
      "type": "ssn_formatting",
      "config": {
        "format": "dashed",
        "mask": false,
        "mask_char": "X",
        "validate": true,
        "strict": false
      }
    }
  }
}
```

**ZIP Code Formatting:**

```json
{
  "x-transformations": {
    "zip_code": {
      "type": "zip_formatting",
      "config": {
        "format": "zip5",
        "validate": true,
        "pad_zeros": true
      }
    }
  }
}
```

**Phone Number Formatting:**

```json
{
  "x-transformations": {
    "phone": {
      "type": "phone_formatting",
      "config": {
        "format": "national",
        "country_code": "US",
        "validate": true,
        "strict": false
      }
    }
  }
}
```

**Email Formatting:**

```json
{
  "x-transformations": {
    "email": {
      "type": "email_formatting",
      "config": {
        "normalize_case": true,
        "validate": true,
        "strict": false
      }
    }
  }
}
```

**Network Address Formatting:**

```json
{
  "x-transformations": {
    "ip_address": {
      "type": "ip_formatting",
      "config": {
        "version": "auto",
        "compress_ipv6": true,
        "validate": true
      }
    },
    "mac_address": {
      "type": "mac_formatting",
      "config": {
        "format": "colon",
        "uppercase": true,
        "validate": true
      }
    }
  }
}
```

### HTML/XML Transformations

**HTML/XML Content Cleaning:**

```json
{
  "x-transformations": {
    "description": {
      "type": "html_xml_cleaning",
      "config": {
        "strip_tags": true,
        "decode_entities": true,
        "preserve_whitespace": false
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
      ],
      "age": [
        {
          "type": "numeric_range",
          "min": 0,
          "max": 150,
          "message": "Age must be between 0 and 150"
        }
      ],
      "url": [
        {
          "type": "url",
          "schemes": ["http", "https"],
          "message": "Must be a valid HTTP/HTTPS URL"
        }
      ]
    },
    "crossFieldValidation": [
      {
        "type": "conditional",
        "condition": "status == 'active'",
        "requirement": "email IS NOT NULL",
        "message": "Active customers must have an email address"
      },
      {
        "type": "date_comparison",
        "field1": "start_date",
        "field2": "end_date",
        "operator": "<=",
        "message": "Start date must be before or equal to end date"
      }
    ]
  }
}
```

## Processing Configuration

### Enhanced Processing (`x-processing`)

Configure comprehensive data transformation and processing:

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
      },
      {
        "name": "age_group",
        "type": "conditional",
        "conditions": [
          {"if": "age < 18", "then": "'Minor'"},
          {"if": "age >= 18 AND age < 65", "then": "'Adult'"},
          {"else": "'Senior'"}
        ],
        "dataType": "string"
      },
      {
        "name": "row_hash",
        "type": "hash",
        "algorithm": "sha256",
        "columns": ["customer_id", "name", "email"],
        "dataType": "string"
      }
    ],
    "columnMapping": {
      "customer_name": "name",
      "cust_id": "customer_id",
      "email_addr": "email"
    },
    "dataQuality": {
      "enabled": true,
      "completenessThreshold": 0.95,
      "uniquenessChecks": ["customer_id", "email"],
      "validityChecks": {
        "email": "email_format",
        "phone": "phone_format"
      }
    },
    "deduplication": {
      "enabled": true,
      "strategy": "keep_first",
      "columns": ["customer_id"],
      "fuzzyMatching": {
        "enabled": true,
        "threshold": 0.85,
        "algorithm": "levenshtein"
      }
    }
  }
}
```

### Row Hash Configuration

Generate unique identifiers for data lineage and change detection:

```json
{
  "x-processing": {
    "rowHash": {
      "enabled": true,
      "algorithm": "sha256",
      "columns": ["customer_id", "name", "email"],
      "includeAllColumns": false,
      "excludeColumns": ["created_date", "modified_date"],
      "outputColumn": "row_hash"
    }
  }
}
```

## Examples

### Complete Customer Schema with All Features

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/schemas/customers-comprehensive.json",
  "title": "Comprehensive Customer Data Schema",
  "description": "Advanced schema demonstrating all Forklift features",
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
    "phone": {
      "type": "string",
      "description": "Customer phone number"
    },
    "ssn": {
      "type": "string",
      "description": "Social Security Number"
    },
    "salary": {
      "type": "string",
      "description": "Annual salary (currency format)"
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
    },
    {
      "name": "unique_ssn",
      "columns": ["ssn"]
    }
  ],
  
  "x-csv": {
    "encodingPriority": ["utf-8", "utf-8-sig", "latin-1"],
    "delimiter": ",",
    "header": {"mode": "present"},
    "nulls": {
      "global": ["", "NA", "NULL"],
      "perColumn": {
        "salary": ["", "0.00", "N/A"]
      }
    },
    "dataTypes": {
      "customer_id": "int64",
      "name": "string",
      "email": "string",
      "phone": "string",
      "ssn": "string",
      "salary": "string",
      "signup_date": "date32",
      "status": "string"
    },
    "validation": {
      "enabled": true,
      "onError": "bad_rows",
      "badRowsPath": "./validation_errors/"
    }
  },
  
  "x-transformations": {
    "name": {
      "type": "string_cleaning",
      "config": {
        "case_transform": "proper",
        "normalize_quotes": true,
        "strip_whitespace": true
      }
    },
    "email": {
      "type": "email_formatting",
      "config": {
        "normalize_case": true,
        "validate": true
      }
    },
    "phone": {
      "type": "phone_formatting",
      "config": {
        "format": "national",
        "country_code": "US",
        "validate": true
      }
    },
    "ssn": {
      "type": "ssn_formatting",
      "config": {
        "format": "dashed",
        "validate": true
      }
    },
    "salary": {
      "type": "money_conversion",
      "config": {
        "currency_symbols": ["$"],
        "thousands_separator": ",",
        "decimal_separator": "."
      }
    },
    "signup_date": {
      "type": "datetime",
      "config": {
        "mode": "common_formats",
        "target_type": "date"
      }
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
      ],
      "email": [
        {
          "type": "regex",
          "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$",
          "message": "Invalid email format"
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
  },
  
  "x-processing": {
    "calculatedColumns": [
      {
        "name": "full_name_upper",
        "type": "expression",
        "expression": "UPPER(name)",
        "dataType": "string"
      },
      {
        "name": "process_timestamp",
        "type": "constant",
        "value": "2024-01-15T10:00:00Z",
        "dataType": "timestamp"
      },
      {
        "name": "customer_hash",
        "type": "hash",
        "algorithm": "sha256",
        "columns": ["customer_id", "name", "email"],
        "dataType": "string"
      }
    ],
    "columnMapping": {
      "cust_id": "customer_id",
      "customer_name": "name"
    },
    "deduplication": {
      "enabled": true,
      "strategy": "keep_first",
      "columns": ["customer_id"]
    }
  }
}
```

### Multi-Format Processing Schema

This example demonstrates how different file formats can be processed with format-specific configurations:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Multi-Format Transaction Schema",
  "description": "Schema supporting CSV, Excel, and JSON formats",
  
  "x-csv": {
    "delimiter": ",",
    "header": {"mode": "present"},
    "validation": {"enabled": true}
  },
  
  "x-excel": {
    "sheet": "Transactions",
    "skipRows": 1,
    "validation": {"enabled": true}
  },
  
  "x-json": {
    "mode": "lines",
    "flattenNested": true,
    "validation": {"enabled": true}
  },
  
  "x-transformations": {
    "amount": {
      "type": "money_conversion",
      "config": {
        "currency_symbols": ["$", "€", "£"]
      }
    },
    "transaction_date": {
      "type": "datetime",
      "config": {
        "mode": "common_formats",
        "target_type": "date"
      }
    }
  }
}
```

This comprehensive documentation covers all available features in Forklift, highlighting the unique capabilities of each file format and data transformation type. The schema standards provide a complete reference for implementing data processing pipelines with full validation, transformation, and quality control capabilities.
