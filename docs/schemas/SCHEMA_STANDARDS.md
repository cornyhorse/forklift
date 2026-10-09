# Forklift Schema Standards

Forklift uses JSON Schema as the foundation for data validation and processing configuration, with custom extensions to support advanced data processing features.

> **Status of the examples.** This page is an overview of the schema vocabulary. The extensions that
> have their own page (`x-primaryKey`, `x-uniqueConstraints`, `x-constraintHandling`, `x-validation`,
> `x-csv`, `x-fwf`, `x-special-type`, `x-transformations`, `x-calculatedColumns`, `x-rowHash`, `x-pii`,
> `x-columnMapping`, `x-dataQuality`, `x-metadata-generation`) are specified there, and each of those
> pages says what the code does with it; start from
> [README.md](./README.md#what-the-engine-applies-today). The sections below on `x-json`, `x-parquet`,
> `x-constraints` and `x-processing` are illustrative designs: no code in this version reads those four
> extensions (`import_csv` ignores them silently).
>
> **What `import_csv` applies.** The CSV engine (`import_csv`, `read_csv`, `forklift ingest --input-kind csv`)
> applies the property types, `required`, `x-csv.parquetTypeMapping`, `x-csv.nulls` and
> `x-metadata-generation`, and also runs `x-transformations` (`column_transformations`), `x-special-type`,
> `x-columnMapping`, `x-calculatedColumns`, `x-dataQuality`, `x-validation`, `x-primaryKey`,
> `x-uniqueConstraints`, the per-property constraints (`minimum`, `maximum`, `minLength`, `maxLength`,
> `pattern`, `enum`, `x-unique`), `x-constraintHandling.errorMode` and `x-rowHash` on every batch, in the
> order given in the README. Rejected rows go to `bad_rows.parquet` with a `_rejection_reason`. Schema
> content that no processor reads (`x-pii`, unknown keys) is reported in `results.warnings`. Excel, SQL
> and fixed-width imports do not apply these extensions. `apply_schema_extensions=False` /
> `--no-schema-extensions` switches them off.

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
- `columns` (required): Array of column names that form the primary key
- `type` (optional): `"single"` or `"composite"`; only checked against the number of columns
- `enforceUniqueness` (optional, default `true`): Boolean, enforce uniqueness constraint
- `allowNulls` (optional, default `false`): Boolean, allow null values in primary key columns

`import_csv` keeps the first row of a key and rejects later duplicates and NULL keys to `bad_rows.parquet` (see [X_PRIMARY_KEY_DOCUMENTATION.md](./X_PRIMARY_KEY_DOCUMENTATION.md)).

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

A key with a NULL part is never compared; `condition`, `ignoreNulls: false` and `caseSensitive: false` are not implemented (see [X_UNIQUE_CONSTRAINTS_DOCUMENTATION.md](./X_UNIQUE_CONSTRAINTS_DOCUMENTATION.md)).

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
engine reads from `x-csv`: the column types (`parquetTypeMapping`, falling back to each property's
`type` / `format`) and `nulls` (the other `x-...` extensions of the schema are applied as described
above). Bad rows are always written to `bad_rows.parquet` in the output directory; there is no
`validation` or `preprocessing` block in `x-csv` (row validation is the separate `x-validation`
extension).

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

> **Reading the examples in this section.** The form the code reads is `"x-transformations": {"column_transformations": {"<column>": {"<transformation>": {"enabled": true, ...config}}}}`, and the config options are exactly the fields of the matching config class in `forklift.utils.transformations.configs` (an unknown option raises `ValueError`). Transformation names: `string_cleaning`, `regex_replace`, `string_replace`, `string_padding`, `string_trimming`, `html_xml_cleaning`, `money_conversion`, `numeric_cleaning`, `datetime`, `ssn_formatting`, `zip_code_formatting`, `phone_number_formatting`, `email_formatting`, `ip_address_formatting`, `mac_address_formatting`. `import_csv` runs these transformations on the text of the file before the types are applied (column names are the header names); other `x-transformations` keys are ignored with a warning. See [X_TRANSFORMATIONS_DOCUMENTATION.md](./X_TRANSFORMATIONS_DOCUMENTATION.md).

### String Transformations

**Comprehensive String Cleaning:**

```json
{
  "x-transformations": {
    "column_transformations": {
      "name": {
        "string_cleaning": {
          "enabled": true,
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
          "custom_case_mapping": {
            "california": "CA"
          },
          "acronyms": ["NASA", "API", "CEO"],
          "remove_accents": false,
          "fix_encoding_errors": true
        }
      }
    }
  }
}
```

**String Operations:**

```json
{
  "x-transformations": {
    "column_transformations": {
      "product_code": {
        "string_padding": {
          "enabled": true,
          "width": 10,
          "fillchar": "0",
          "side": "left"
        }
      },
      "description": {
        "regex_replace": {
          "enabled": true,
          "pattern": "\\s+",
          "replacement": " ",
          "flags": 0
        }
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
    "column_transformations": {
      "price": {
        "money_conversion": {
          "enabled": true,
          "currency_symbols": ["$", "€", "£"],
          "thousands_separator": ",",
          "decimal_separator": ".",
          "parentheses_negative": true,
          "strip_whitespace": true
        }
      }
    }
  }
}
```

**Numeric Cleaning:**

```json
{
  "x-transformations": {
    "column_transformations": {
      "quantity": {
        "numeric_cleaning": {
          "enabled": true,
          "thousands_separator": ",",
          "decimal_separator": ".",
          "allow_nan": true,
          "nan_values": ["", "N/A", "NULL"],
          "target_type": "int64"
        }
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
    "column_transformations": {
      "event_date": {
        "datetime": {
          "enabled": true,
          "mode": "common_formats",
          "allow_fuzzy": false,
          "from_epoch": false,
          "target_type": "string",
          "timezone": "UTC",
          "output_format": "%Y-%m-%d %H:%M:%S"
        }
      },
      "timestamp": {
        "datetime": {
          "enabled": true,
          "mode": "enforce",
          "format": "%Y-%m-%d %H:%M:%S",
          "to_epoch": "seconds"
        }
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
    "column_transformations": {
      "ssn": {
        "ssn_formatting": {
          "enabled": true,
          "format_with_dashes": true,
          "zero_pad": true,
          "validate": true,
          "allow_invalid": false
        }
      }
    }
  }
}
```

**ZIP Code Formatting:**

```json
{
  "x-transformations": {
    "column_transformations": {
      "zip_code": {
        "zip_code_formatting": {
          "enabled": true,
          "zip_type": "zip-5",
          "validate": true,
          "zero_pad": true
        }
      }
    }
  }
}
```

**Phone Number Formatting:**

```json
{
  "x-transformations": {
    "column_transformations": {
      "phone": {
        "phone_number_formatting": {
          "enabled": true,
          "format_style": "us-standard",
          "validate": true,
          "allow_invalid": false
        }
      }
    }
  }
}
```

**Email Formatting:**

```json
{
  "x-transformations": {
    "column_transformations": {
      "email": {
        "email_formatting": {
          "enabled": true,
          "normalize_case": true,
          "validate_format": true,
          "allow_invalid": false
        }
      }
    }
  }
}
```

**Network Address Formatting:**

```json
{
  "x-transformations": {
    "column_transformations": {
      "ip_address": {
        "ip_address_formatting": {
          "enabled": true,
          "ip_version": "both",
          "compress_ipv6": true,
          "validate": true
        }
      },
      "mac_address": {
        "mac_address_formatting": {
          "enabled": true,
          "format_style": "colon",
          "case_style": "upper",
          "validate": true
        }
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
    "column_transformations": {
      "description": {
        "html_xml_cleaning": {
          "enabled": true,
          "strip_tags": true,
          "decode_entities": true,
          "preserve_whitespace": false
        }
      }
    }
  }
}
```

HTML/XML cleaning is text extraction, not a security sanitizer: tags are stripped first, entities are decoded once afterwards (`5 &lt; 6` becomes `5 < 6`) and `<script>`/`<style>` content is dropped. Escape the result for the place where you use it.

## Validation Configuration

### Constraint Validation (`x-constraints`)

> Illustrative only: the validation extensions that exist are `x-validation` (rules that reject rows), `x-primaryKey`, `x-uniqueConstraints`, `x-constraintHandling`, the per-property constraints (`minimum`, `maxLength`, `pattern`, `enum`, ...) and `x-dataQuality` (report only); no code reads `x-constraints`. See [X_VALIDATION_DOCUMENTATION.md](./X_VALIDATION_DOCUMENTATION.md).

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

> Illustrative only: no code reads `x-processing`. The extensions that exist are the top-level `x-calculatedColumns` (`constants`, `expressions`; expressions are Python-like, not SQL such as `CONCAT` or `UPPER`), `x-rowHash`, `x-columnMapping` (`explicitMappings`, `namingConvention`) and `x-dataQuality` (`fieldSpecificRules`); there is no deduplication extension (use `x-primaryKey` / `x-uniqueConstraints`).

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

Generate unique identifiers for data lineage and change detection (the real extension is the top-level `x-rowHash`, see [X_ROW_HASH_DOCUMENTATION.md](./X_ROW_HASH_DOCUMENTATION.md); `import_csv` applies it):

```json
{
  "x-rowHash": {
    "enabled": true,
    "algorithm": "sha256",
    "includeColumns": ["customer_id", "name", "email"],
    "columnName": "row_hash"
  }
}
```

## Examples

### Complete Customer Schema with All Features

A schema that uses the extensions the CSV engine applies (it runs as it is with `import_csv`):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/cornyhorse/forklift/schema-standards/customers-comprehensive.json",
  "title": "Comprehensive Customer Data Schema",
  "description": "Advanced schema demonstrating the Forklift features the CSV engine applies",
  "type": "object",
  "properties": {
    "customer_id": {"type": "integer", "description": "Unique customer identifier"},
    "name": {"type": "string", "maxLength": 100, "description": "Customer full name"},
    "email": {"type": "string", "x-special-type": "email", "description": "Customer email address"},
    "phone": {"type": "string", "x-special-type": "phone", "description": "Customer phone number"},
    "ssn": {"type": "string", "x-special-type": "ssn", "description": "Social Security Number"},
    "salary": {"type": "number", "description": "Annual salary"},
    "signup_date": {"type": "string", "format": "date", "description": "Date customer signed up"},
    "status": {"type": "string", "enum": ["active", "inactive", "pending"], "description": "Customer status"}
  },
  "required": ["customer_id", "name", "email"],

  "x-primaryKey": {
    "columns": ["customer_id"],
    "type": "single",
    "enforceUniqueness": true,
    "allowNulls": false
  },

  "x-uniqueConstraints": [
    {"name": "unique_email", "columns": ["email"]},
    {"name": "unique_ssn", "columns": ["ssn"]}
  ],

  "x-csv": {
    "encodingPriority": ["utf-8", "utf-8-sig", "latin-1"],
    "delimiter": ",",
    "header": {"mode": "present"},
    "nulls": {
      "global": ["", "NA", "NULL"],
      "perColumn": {"salary": ["", "0.00", "N/A"]}
    },
    "parquetTypeMapping": {
      "customer_id": "int64",
      "salary": "double",
      "signup_date": "date32"
    }
  },

  "x-transformations": {
    "column_transformations": {
      "name": {
        "string_cleaning": {"enabled": true, "case_transform": "title", "normalize_quotes": true, "strip_whitespace": true}
      },
      "salary": {
        "money_conversion": {"enabled": true, "currency_symbols": ["$"], "thousands_separator": ",", "decimal_separator": "."}
      },
      "signup_date": {
        "datetime": {"enabled": true, "mode": "common_formats", "target_type": "date"}
      }
    }
  },

  "x-columnMapping": {
    "explicitMappings": {"phone": "phone_number"}
  },

  "x-validation": {
    "badRowsHandling": {"maxBadRowsPercent": 50},
    "fieldValidations": {
      "customer_id": {"range": {"min": 1}},
      "email": {"stringValidation": {"pattern": "^[\\w.-]+@[\\w.-]+\\.[a-zA-Z]{2,}$"}}
    }
  },

  "x-calculatedColumns": {
    "constants": [
      {"name": "process_timestamp", "value": "2024-01-15T10:00:00Z", "dataType": "timestamp[us, tz=UTC]"}
    ],
    "expressions": [
      {"name": "name_upper", "expression": "upper(name)", "dataType": "string", "dependencies": ["name"]}
    ]
  },

  "x-rowHash": {
    "enabled": true,
    "columnName": "customer_hash",
    "includeColumns": ["customer_id", "name", "email"]
  }
}
```

With this input:

```
customer_id,name,email,phone,ssn,salary,signup_date,status
1,"  jane   o'neil ",Jane@Example.com,5551234567,123456789,"$85,000.00",2024-01-05,active
2,JOHN SMITH,john@example.com,(555) 123-4567,987-65-4321,"$72,500.00",03/15/2023,pending
3,Jim Roe,jane@example.com,555-123-4567,not-an-ssn,N/A,2022-11-30,inactive
4,Sue Poe,sue@example.com,5559876543,NA,"$61,000.00",2021-07-01,archived
```

`data.parquet` has the rows 1 and 2, with the columns `customer_id`, `name`, `email`, `phone_number` (renamed from `phone`), `ssn`, `salary`, `signup_date`, `status`, then `process_timestamp`, `name_upper` and `customer_hash`:

| customer_id | name | email | phone_number | ssn | salary | signup_date | name_upper |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Jane O'Neil | jane@example.com | (555) 123-4567 | 123-45-6789 | 85000.0 | 2024-01-05 | JANE O'NEIL |
| 2 | John Smith | john@example.com | (555) 123-4567 | 987-65-4321 | 72500.0 | 2023-03-15 | JOHN SMITH |

`bad_rows.parquet` has the rows 3 (`UNIQUE_VIOLATION:email`: the cleaned e-mail of row 1 is the same) and 4 (`ENUM_VIOLATION:status`: `archived` is not in the list), in the shape of the input file. `results.validation_summary` is `{'INVALID_SPECIAL_VALUE:ssn': 1, 'ENUM_VIOLATION:status': 1, 'UNIQUE_VIOLATION:email': 1}` (the invalid SSN of row 3 became NULL, the ordinary null marker `NA` of row 4 is not a finding) and `results.warnings` is empty.

### Multi-Format Processing Schema

A schema can carry the settings of several file formats side by side; each importer reads its own block (`x-csv` for `import_csv`, `x-excel` for `import_excel`). The shared parts (`properties`, `required`) are used by both. The processing extensions (`x-transformations`, `x-validation`, `x-primaryKey`, ...) are applied by the CSV engine only; `import_excel` ignores them. `x-json` is illustrative (JSON input is not implemented).

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Multi-Format Transaction Schema",
  "description": "Schema supporting CSV and Excel (and, illustratively, JSON) formats",
  "type": "object",
  "properties": {
    "amount": {"type": "number"},
    "transaction_date": {"type": "string", "format": "date"}
  },

  "x-csv": {
    "delimiter": ",",
    "header": {"mode": "present"}
  },

  "x-excel": {
    "sheet": "Transactions",
    "header": {"row": 1}
  },

  "x-json": {
    "mode": "lines",
    "flattenNested": true
  },

  "x-transformations": {
    "column_transformations": {
      "amount": {
        "money_conversion": {"enabled": true, "currency_symbols": ["$", "€", "£"]}
      },
      "transaction_date": {
        "datetime": {"enabled": true, "mode": "common_formats", "target_type": "date"}
      }
    }
  }
}
```

This documentation gives an overview of the schema vocabulary; the per-extension pages listed in [README.md](./README.md) specify each extension and what the engine does with it.
