# Forklift Schema Standards

Forklift uses JSON Schema as the foundation for data validation and processing configuration, with custom extensions to support advanced data processing features.

> **Status of the examples.** This page is an overview of the schema vocabulary. The extensions that
> have their own page (`x-primaryKey`, `x-uniqueConstraints`, `x-constraintHandling`, `x-csv`, `x-fwf`,
> `x-special-type`, `x-transformations`, `x-calculatedColumns`, `x-rowHash`, `x-pii`, `x-columnMapping`,
> `x-dataQuality`, `x-metadata-generation`) are specified there, and each of those pages says what the
> code does with it; start from [README.md](./README.md#what-the-engine-applies-today). The sections
> below on `x-json`, `x-parquet`, `x-constraints` and `x-processing` are illustrative designs: no code in
> this version reads those four extensions. `import_csv` itself applies the property types, `required`,
> `x-csv.parquetTypeMapping`, `x-csv.nulls` and `x-metadata-generation`.

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

Property definitions may declare `type` as a string, as a nullable array (`["integer", "null"]`) or
through a nullable `anyOf` / `oneOf` union (`{"anyOf": [{"type": "string"}, {"type": "null"}]}`). `required`
must be an array of strings that name defined properties; the schema importers collect every problem
they find (with its location, for example `Sheet 0 column 1 invalid type 'foo'` or
`required[2] refers to unknown property 'x'`) into one `SchemaValidationError`.

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

Column-level metadata written into a schema by schema generation (`generate_schema_from_*`, `forklift generate-schema`):

```json
{
  "x-metadata": {
    "analysis_config": {"rows_analyzed": 1247, "include_value_statistics": false},
    "table_metadata": {"row_count": 1247, "column_count": 3, "source_file": "customers.csv"},
    "column_metadata": {
      "customer_id": {
        "name": "customer_id",
        "type": "int64",
        "parquet_type": "int64",
        "null_count": 0,
        "distinct_count": 1247,
        "uniqueness_ratio": 1.0,
        "mean": 624.0,
        "std_dev": 360.1,
        "variance": 129688.0
      },
      "status": {
        "name": "status",
        "type": "string",
        "null_count": 12,
        "distinct_count": 3,
        "uniqueness_ratio": 0.0024,
        "min_length": 6,
        "max_length": 8
      }
    },
    "enum_suggestions": {
      "status": {"is_enum_candidate": true, "confidence": "high", "distinct_count": 3}
    }
  }
}
```

By default the metadata contains **no cell values**: counts, null counts, distinct counts, uniqueness
ratios, mean / standard deviation / variance, outlier counts, string-length statistics and enum
*candidates* without their values. The statistics that copy values from the data appear only when
schema generation is run with `include_value_statistics=True` (CLI `--include-value-stats`), because
those values can be personal data:

```json
{
  "x-metadata": {
    "column_metadata": {
      "customer_id": {"min_value": 1.0, "max_value": 1247.0, "median": 624.0, "quantiles": {"quantile_25": 312.5, "quantile_99_5": 1240.8},
                      "top_values": [{"value": "1", "count": 1, "percentage": 0.08}]},
      "status": {"top_values": [{"value": "active", "count": 890, "percentage": 71.4}]}
    },
    "enum_suggestions": {
      "status": {"suggested_enum_values": ["active", "inactive", "pending"]}
    }
  }
}
```

Quantile keys are the exact percentage (`0.995` is `quantile_99_5`); each requested quantile must be
within 0..1. `source_file` is the file name, never the directory. The separate output metadata file
written by `import_csv` has its own structure; see the [metadata generation
documentation](./X_METADATA_GENERATION_DOCUMENTATION.md).

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
      "mode": "present"
    },
    "footer": {"mode": "regex", "pattern": "^(total|summary)\\b"},
    "nulls": {
      "global": ["", "NA", "NULL", "null", "None"],
      "perColumn": {
        "salary": ["", "0.00", "N/A"],
        "comments": ["", "No comment", "-"]
      }
    },
    "parquetTypeMapping": {
      "customer_id": "int64",
      "name": "string",
      "salary": "double",
      "signup_date": "date32"
    }
  }
}
```

See the [x-csv documentation](./X_CSV_DOCUMENTATION.md) for the exact values (`header.mode` is `present`,
`absent`, `auto` or `stability_scan`; `footer.mode` is `regex` or `blank_line`) and for which parts the
engine applies: the column types (`parquetTypeMapping`, falling back to each property's `type` /
`format`) and `nulls`. Bad rows are always written to `bad_rows.parquet` in the output directory; there
is no `validation` or `preprocessing` block.

### Excel Configuration (`x-excel`)

**Unique Excel Features:**
- **Multi-Sheet Support**: Select sheets by name, 0-based index or regex
- **Header and Data Range**: `header.row` (0-based), `dataStartRow` / `dataEndRow` (1-based, inclusive), `header.mode` (`present`, `absent`, `auto`)
- **Column Mapping**: Pick columns by letter or number and cast them with `parquetType`
- **Cached Formula Values**: `valuesOnly` (default true) reads the values Excel stored, formulas without a cached value are null
- **Resource limits**: `.xlsx` archives and sheets are read with size, row and cell limits (`ExcelInputConfig`)

```json
{
  "x-excel": {
    "valuesOnly": true,
    "dateSystem": "1900",
    "sheets": [
      {
        "select": {"name": "CustomerData"},
        "header": {"mode": "present", "row": 0},
        "dataStartRow": 2,
        "skipBlankRows": true,
        "nameOverride": "customers",
        "columns": [
          {"name": "customer_id", "position": "A", "parquetType": "int64"},
          {"name": "name", "position": "B"},
          {"name": "signup_date", "position": "C", "parquetType": "date32"}
        ]
      }
    ],
    "nulls": {
      "global": ["", "NA", "N/A", "#N/A"]
    }
  }
}
```

A single sheet can also be written as `"x-excel": {"sheet": "CustomerData", "header": {"row": 0}}`
(this is the form the schema generator emits; `sheet` is a name or a 0-based index). Giving both
`sheet` and `sheets` is an error. By default only empty and whitespace-only cells are null; `NA`
stays text unless it is listed under `nulls`. `schema-standards/20250826-excel.json` contains a complete example `x-excel`
block.

### Fixed-Width File Configuration (`x-fwf`)

**Unique FWF Features:**
- **Multi-Record Type Support**: Handle files with different record structures (`conditionalSchemas`)
- **Position-Based Field Definition**: Precise field positioning with start/length
- **Record Type Flags**: Record type detection based on a flag column
- **Recorded problems**: unconvertible values and rejected lines are listed, not dropped silently (see the [x-fwf documentation](./X_FWF_DOCUMENTATION.md))

```json
{
  "x-fwf": {
    "encoding": "utf-8",
    "conditionalSchemas": {
      "flagColumn": {"name": "record_type", "start": 1, "length": 1, "parquetType": "string"},
      "schemas": [
        {
          "flagValue": "H",
          "description": "Header record",
          "fields": [
            {"name": "file_date", "start": 2, "length": 8, "parquetType": "string"},
            {"name": "batch_id", "start": 10, "length": 10, "parquetType": "string"}
          ]
        },
        {
          "flagValue": "D",
          "description": "Detail record",
          "fields": [
            {"name": "customer_id", "start": 2, "length": 8, "align": "right", "pad": "0", "parquetType": "int64"},
            {"name": "amount", "start": 10, "length": 12, "align": "right", "parquetType": "double"},
            {"name": "transaction_date", "start": 22, "length": 8, "parquetType": "string"}
          ]
        }
      ]
    }
  }
}
```

Single-layout files use a plain `"fields": [...]` list instead. Fixed-width import is not wired into
`import_fwf()` yet; use `FwfInputHandler` directly.

### JSON Configuration (`x-json`)

> Illustrative only: JSON input is not implemented and no code reads `x-json`.

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

> Illustrative only: no code reads `x-parquet`. Parquet is read for schema generation (`generate_schema_from_parquet`) and is the output format of the importers.

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

> **Reading the examples in this section.** Each example is written as `"<column>": {"type": "<transformation>", "config": {...}}` for brevity. The form the processor (`SchemaBasedTransformer`) reads is `"x-transformations": {"column_transformations": {"<column>": {"<transformation>": {"enabled": true, ...config}}}}`, and the `config` options are exactly the fields of the matching config class in `forklift.utils.transformations.configs` (an unknown option raises `ValueError`). Transformation names: `string_cleaning`, `regex_replace`, `string_replace`, `string_padding`, `string_trimming`, `html_xml_cleaning`, `money_conversion`, `numeric_cleaning`, `datetime`, `ssn_formatting`, `zip_code_formatting`, `phone_number_formatting`, `email_formatting`, `ip_address_formatting`, `mac_address_formatting`. `import_csv` does not execute these transformations; see [X_TRANSFORMATIONS_DOCUMENTATION.md](./X_TRANSFORMATIONS_DOCUMENTATION.md).

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
        "format_with_dashes": true,
        "zero_pad": true,
        "validate": true,
        "allow_invalid": false
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
      "type": "zip_code_formatting",
      "config": {
        "zip_type": "zip-5",
        "validate": true,
        "zero_pad": true
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
      "type": "phone_number_formatting",
      "config": {
        "format_style": "us-standard",
        "validate": true,
        "allow_invalid": false
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
        "validate_format": true,
        "allow_invalid": false
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
      "type": "ip_address_formatting",
      "config": {
        "ip_version": "both",
        "compress_ipv6": true,
        "validate": true
      }
    },
    "mac_address": {
      "type": "mac_address_formatting",
      "config": {
        "format_style": "colon",
        "case_style": "upper",
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

HTML/XML cleaning is text extraction, not a security sanitizer: tags are stripped first, entities are decoded once afterwards (`5 &lt; 6` becomes `5 < 6`) and `<script>`/`<style>` content is dropped. Escape the result for the place where you use it.

## Validation Configuration

### Constraint Validation (`x-constraints`)

> Illustrative only: the constraint extensions that exist are `x-primaryKey`, `x-uniqueConstraints`, `x-constraintHandling` and `x-dataQuality`; no code reads `x-constraints`.

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

> Illustrative only: no code reads `x-processing`. The extensions that exist are `x-calculatedColumns`, `x-rowHash`, `x-columnMapping` and `x-dataQuality`.

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
