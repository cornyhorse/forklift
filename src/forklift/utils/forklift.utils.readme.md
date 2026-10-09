# Forklift Utilities

## Overview

The `forklift.utils` module provides a comprehensive collection of utility functions and classes that support the core functionality of the Forklift data processing ecosystem. These utilities handle common data processing tasks, provide specialized functionality for data transformation and validation, and serve as building blocks for higher-level Forklift components.

## Role in Forklift Ecosystem

The utils module is foundational to Forklift's architecture, providing:

- **Data Processing Utilities**: Core functions for data manipulation and cleaning
- **Schema Support**: Utilities for working with data schemas and validation
- **File Handling**: Encoding detection and file processing capabilities
- **Column Management**: Tools for managing column names and structures
- **Transformation Infrastructure**: Foundation for data transformation pipelines
- **Date Processing**: Comprehensive date and datetime parsing capabilities

## Core Module Files

### `column_name_utilities.py`
**Column Name Management**

Provides utilities for handling column name conflicts and standardization:
- **Deduplication**: Ensures unique column names by adding numeric suffixes; always terminates, also for empty names and names that already look generated (`["a", "a", "a_1"]`)
- **Multiple Methods**: Supports suffix, prefix, and error-based deduplication strategies; `max_length` (at least 16) keeps the generated names within a limit, which `postgres` standardization uses for its 63-character limit
- **Postgres standardization**: `standardize_postgres_column_name()` lower-cases, transliterates accents (`Café` -> `cafe`), replaces other characters with `_` and cuts to 63 characters; a header with only non-Latin letters becomes `col_<n>` / `col_<hash>` instead of an empty name
- **Conflict Resolution**: Handles duplicate column names in data imports
- **Schema Compliance**: Ensures column names meet schema requirements
- **Use Cases**: CSV imports, data merging, schema validation

### `detect_encoding.py`
**File Encoding Detection**

Handles automatic detection and handling of text file encodings:
- **`detect_encoding(path)`**: a byte order mark decides immediately; otherwise the guess of `chardet` (or `charset-normalizer`) is tried first, then `utf-8`, `cp1252` and `latin-1`. A candidate is accepted only if the **whole file** decodes with it (a detector sees only the first 64 KiB)
- **`verify_encoding(path, encoding)`**: streams the file through an incremental decoder (bounded memory)
- **`open_text_auto(path, encodings=None)`**: opens the file with the first candidate that decodes it completely; on total failure falls back to UTF-8 with `errors="replace"`. The handle is opened with `newline=""`, so line endings (CRLF) are preserved as stored
- **Defensive Processing**: Prevents crashes from encoding issues
- **Use Cases**: File imports, data readers, international data processing

### `date_parser/` (package)
`forklift.utils.date_parser` is a package (see below). Its public functions `parse_date`, `coerce_date` and `coerce_datetime` keep the original signatures; there is no separate `date_parser.py` module.

### `row_validation.py`
**Deprecated Row Validation**

Legacy module that has been replaced by the type_coercion preprocessor:
- **Removal Notice**: Provides clear error messages for deprecated functionality
- **Migration Guidance**: Directs users to new type_coercion preprocessor
- **Legacy Cleanup**: Maintains module for backwards compatibility
- **Error Handling**: Helpful error messages for migration
- **Deprecation Management**: Clean removal of obsolete functionality

### `sql_include.py`
**SQL Schema Processing**

Utilities for processing SQL-related schema configurations:
- **Table Extraction**: Derives SQL table lists from schema configurations
- **Schema Processing**: Handles x-sql schema extensions
- **Table Mapping**: Maps schema definitions to SQL table structures
- **Configuration Parsing**: Processes SQL-specific schema directives
- **Use Cases**: SQL data sources, database integrations, schema validation

## Sub-Modules

### `transformations/`
**Data Transformation Framework**

A comprehensive transformation system providing data cleaning, formatting, and standardization:
- **String Transformations**: Text cleaning, case conversion, regex operations
- **DateTime Processing**: Date parsing, formatting, timezone handling
- **Numeric Operations**: Number formatting, validation, type conversion
- **Format Standardization**: Email, phone, postal code, SSN formatting
- **HTML/XML Processing**: Markup handling and content extraction

*See [Transformations README](transformations/forklift.utils.transformations.readme.md) for detailed documentation.*

### `date_parser/`
**Advanced Date Parsing**

Modular date and datetime parsing system with intelligent format detection:
- **Format Detection**: Automatic recognition of date formats
- **Multiple Formats**: Support for international and custom date formats
- **Epoch Handling**: Unix timestamp and epoch conversion
- **Validation**: Robust date validation and error handling
- **Performance**: Optimized for large dataset processing

*See [Date Parser README](date_parser/forklift.utils.date_parser.readme.md) for detailed documentation.*

## Usage Examples

### Column Name Deduplication
```python
from forklift.utils.column_name_utilities import dedupe_column_names

# Handle duplicate column names
columns = ["id", "name", "name", "amount", "name"]
unique_columns = dedupe_column_names(columns)
# Returns: ["id", "name", "name_1", "amount", "name_2"]
```

### Encoding Detection
```python
from forklift.utils.detect_encoding import detect_encoding, open_text_auto

encoding = detect_encoding("data.csv")   # e.g. "ascii", "utf-8", "cp1252" or "utf-8-sig"; verified against the whole file

# Open file with automatic encoding detection
with open_text_auto("data.csv") as file:
    content = file.read()
```

### SQL Schema Processing
```python
from forklift.utils.sql_include import derive_sql_table_list

# Extract table list from schema
schema = {...}  # Your schema configuration
tables = derive_sql_table_list(schema)
# Returns list of (schema_name, table_name, output_name) tuples
```

### Date Parsing
```python
from forklift.utils.date_parser import parse_date, coerce_date

is_valid = parse_date("2023-12-25")
iso_date = coerce_date("12/25/2023")          # "2023-12-25"
iso_date = coerce_date("03-04-2024")          # "2024-04-03": day first by default
iso_date = coerce_date("03-04-2024", dayfirst=False)   # "2024-03-04"
```

## Integration with Forklift

The utils module integrates throughout the Forklift ecosystem:

1. **Data Readers**: Encoding detection, column management, date parsing
2. **Processors**: Transformation utilities, validation support
3. **Schema System**: SQL processing, validation utilities
4. **Output Writers**: Data formatting, transformation support
5. **Engine Components**: Core utilities used across the processing pipeline

## Performance Considerations

- **Memory Efficiency**: Utilities designed for large dataset processing (encoding verification streams the file)
- **Error Handling**: Graceful degradation without performance penalties
- **PyArrow Integration**: Optimized for columnar data processing

## Migration and Deprecation

The utils module manages several deprecated components:

- **row_validation**: Replaced by type_coercion preprocessor
- **Format transformations**: Consolidated into transformations/ module

Migration guidance is provided through clear error messages and documentation.

## Error Handling

Robust error handling across all utilities:
- **Graceful Degradation**: Continues processing when possible
- **Clear Error Messages**: Helpful guidance for troubleshooting
- **Logging Integration**: Comprehensive logging for debugging
- **Validation Feedback**: Clear feedback on data issues
- **Recovery Strategies**: Multiple approaches for handling edge cases

## Extension Points

The utils module provides several extension points:
- **Custom Transformations**: Easy addition of new transformation types
- **Format Support**: Extensible format detection and handling
- **Encoding Support**: Additional encoding detection strategies
- **Column Strategies**: Custom column name handling approaches
- **Validation Rules**: Custom validation and coercion logic
