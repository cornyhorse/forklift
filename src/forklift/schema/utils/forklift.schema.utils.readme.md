# forklift.schema.utils

## Overview

The `forklift.schema.utils` subpackage provides essential utility functions and helper classes that support schema generation, formatting, and validation operations across the Forklift framework. It includes formatting utilities, error handling, and common helper functions used throughout the schema processing pipeline.

## Key Components

### SchemaFormatter
A comprehensive formatting utility that handles the presentation and structure of generated schemas:
- **Base Schema Creation**: Generates foundational schema structures with proper JSON Schema compliance
- **Metadata Integration**: Adds generation timestamps, version information, and processing metadata
- **Format Standardization**: Ensures consistent formatting across different schema outputs
- **Extension Handling**: Manages custom schema extensions and x-prefixed properties
- **Output Formatting**: Pretty-printed strict JSON (`allow_nan=False`; non-finite floats are written as `null`)
- **Source Attribution**: `x-generation.source_file` holds the file name only, never the directory

### SchemaValidationError
Exception raised by `validate_schema_structure()` in `helpers.py` when a schema dictionary lacks `$schema`, `type` or `properties`, or when `properties` is not a dictionary. It is a plain `Exception` subclass carrying a message. (The CSV, Excel, SQL and FWF schema importers define their own `SchemaValidationError` classes, which collect every problem found, with its location, into one message.)

## Utility Functions

### Type String Helpers (helpers.py)
- **`get_parquet_type_string(arrow_type)`**: converts an Arrow type to the type string used in generated schemas (`x-csv.dataTypes`, metadata `parquet_type`). Precision, scale, unit and time zone are preserved (`decimal128(18,4)`, `timestamp[us, tz=UTC]`, `duration[ns]`, `list<decimal128(5,2)>`, `dictionary<values=string, indices=int8>`). Types the schema importers cannot express map to the closest accepted type: `float16` -> `float32`, `fixed_size_list`/`large_list` -> `list<T>`, `map` -> `list<struct>`, and `time32`/`time64`/`decimal256` -> `string`
- **`parquet_type_string_to_arrow(type_string)`**: the inverse, used for round-trip checks
- **`validate_quantiles`**, **`quantile_label`**: quantiles must be within 0..1; labels are exact (`0.29` -> `29`, `0.995` -> `99_5`)
- **`split_name_tokens`**: splits column names into lower-case word tokens (snake_case, camelCase, digits) for whole-word name matching
- **`to_json_safe`**: converts values to strict-JSON-safe ones (NaN/infinity -> `null`, dates -> ISO strings, decimals -> strings, bytes -> base64)
- **`source_basename`**: reduces a local, Windows or `s3://` path to its file name

### Other helpers
- **`validate_schema_structure(schema)`**: raises `SchemaValidationError` if `$schema`, `type` or `properties` is missing
- **`SchemaFormatter.create_base_schema(file_type)`**: the JSON Schema 2020-12 skeleton (`$schema`, `$id` under `https://github.com/cornyhorse/forklift/schema-standards/`, `title`, `type: object`, empty `properties` / `required`)
- **`SchemaFormatter.add_generation_metadata(schema, source_file, rows_analyzed)`**: adds `x-generation` (timestamp, file name, rows analysed, generator version)
- **`SchemaFormatter.format_schema_json(schema, indent=2)`**: strict JSON text

## Usage Patterns

### Basic Schema Formatting
```python
from forklift.schema.utils import SchemaFormatter

base_schema = SchemaFormatter.create_base_schema("csv")
schema = SchemaFormatter.add_generation_metadata(base_schema, "/data/customers.csv", 1000)
print(schema["x-generation"]["source_file"])   # customers.csv  (file name only)
print(SchemaFormatter.format_schema_json(schema))
```

### Error Handling
```python
from forklift.schema.utils.helpers import validate_schema_structure, SchemaValidationError

try:
    validate_schema_structure({"properties": {}})
except SchemaValidationError as e:
    print(f"Validation failed: {e}")   # Missing required field: $schema
```

## Integration Points

### Internal Dependencies
- `forklift.schema.generator.*` - Core schema generation
- `forklift.schema.processors.*` - Schema processing components
- `forklift.schema.types.*` - Type system integration

### External Dependencies
- **PyArrow**: type objects for `get_parquet_type_string()`
- Standard library only otherwise (`json`, `re`, `decimal`, `base64`); pandas is not used
