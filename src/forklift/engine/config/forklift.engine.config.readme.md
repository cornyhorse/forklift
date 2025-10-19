# Forklift Engine Configuration Module

The configuration module provides essential classes and enums for configuring data import operations in the Forklift engine. It handles CSV processing settings, validation options, and output preferences.

## Overview

This module contains the core configuration components:

- **ImportConfig**: Main configuration class for data import operations
- **ProcessingResults**: Results tracking for completed operations
- **HeaderMode**: Enumeration for header detection strategies
- **ExcessColumnMode**: Enumeration for handling extra columns

## Components

### ImportConfig

The primary configuration class that controls all aspects of data import processing.

**Key Features:**
- File path configuration (input/output)
- CSV parsing options (delimiter, encoding, quotes)
- Header detection and processing
- Schema validation settings
- Output file generation options
- Error handling preferences

### ProcessingResults

Tracks the outcomes of data processing operations, including:
- Row counts (total, valid, invalid)
- Generated file paths
- Execution metrics
- Error collection

### Enums

#### HeaderMode
Controls header detection behavior:
- `PRESENT`: File contains headers to use
- `ABSENT`: No headers, use schema or defaults
- `AUTO`: Automatically detect header location

#### ExcessColumnMode
Handles rows with more columns than expected:
- `TRUNCATE`: Remove extra columns, keep row (default)
- `REJECT`: Discard entire row with excess columns
- `PASSTHROUGH`: Keep all columns, including those not in schema

**Important**: When using `TRUNCATE` mode with a schema that specifies only a subset of columns, Forklift will only keep the first N columns (where N is the number of columns in your schema) and discard all additional columns. This is positional truncation, not selective column filtering by name.

**Example**: If your CSV has 5 columns (`Name,Age,City,Country,Phone`) but your schema only defines 3 columns (`Name,Age,City`), the `Country` and `Phone` columns will be completely discarded in TRUNCATE mode.

**PASSTHROUGH Mode**: When using `PASSTHROUGH` mode, all columns from the input file are preserved in the output, even if they're not defined in your schema. Extra columns beyond the schema are automatically assigned default names like `col_4`, `col_5`, etc. This is useful when you want to capture all data from variable-width files while still applying schema validation to the known columns.

**PASSTHROUGH Example**: 
- CSV file has columns: `Name,Age,City,Country,Phone`
- Schema defines only: `Name,Age,City` (3 columns)
- Result: All columns are kept with names: `Name,Age,City,col_4,col_5`

**Implementation**: This functionality is implemented in the `BatchProcessor` class located at `src/forklift/engine/processors/batch_processor.py`. The logic is applied during CSV row processing where:
- For `TRUNCATE` mode: Excess columns are removed using `row[:expected_columns]` 
- For `REJECT` mode: The entire row is skipped when excess columns are detected
- For `PASSTHROUGH` mode: All columns are preserved and extra columns get auto-generated names
- The implementation also handles insufficient columns by padding with empty strings regardless of mode

**Testing**: The functionality is validated by unit tests in `tests/test_batch_processor.py`:
- `test_create_s3_csv_batches_excess_columns_truncate()` - Tests that excess columns are properly removed while preserving the row
- `test_create_s3_csv_batches_excess_columns_reject()` - Tests that rows with excess columns are completely discarded
- `test_create_s3_csv_batches_excess_columns_passthrough()` - Tests that all columns are preserved with auto-generated names for extras

## Usage Examples

### Basic Configuration

```python
from forklift.engine.config import ImportConfig, HeaderMode

config = ImportConfig(
    input_path="data/input.csv",
    output_path="data/output/",
    header_mode=HeaderMode.PRESENT,
    batch_size=5000
)
```

### Advanced Configuration with Schema Validation

```python
from forklift.engine.config import ImportConfig, ExcessColumnMode

config = ImportConfig(
    input_path="data/complex.csv",
    output_path="data/processed/",
    schema_file="schemas/data_schema.json",
    delimiter="|",
    encoding="utf-8",
    header_mode=HeaderMode.AUTO,
    header_search_rows=5,
    excess_column_mode=ExcessColumnMode.REJECT,
    validate_schema=True,
    max_validation_errors=100,
    create_manifest=True,
    compression="gzip"
)
```

### Processing Results Usage

```python
from forklift.engine.config import ProcessingResults

# After processing operation
results = ProcessingResults(
    total_rows=10000,
    valid_rows=9850,
    invalid_rows=150,
    output_files=["output_001.parquet", "output_002.parquet"],
    execution_time=45.2
)

print(f"Success rate: {results.valid_rows / results.total_rows * 100:.2f}%")
```

## Configuration Parameters

### File Handling
- `input_path`: Source file location
- `output_path`: Destination directory
- `schema_file`: Optional JSON schema for validation
- `encoding`: Text encoding (default: utf-8)

### CSV Processing
- `delimiter`: Field separator (default: comma)
- `quote_char`: Quote character (default: double quote)
- `escape_char`: Escape character for special chars
- `skip_blank_lines`: Skip empty rows (default: True)

### Header Processing
- `header_mode`: Header detection strategy
- `header_search_rows`: Max rows to scan for headers (default: 10)
- `comment_rows`: Regex patterns for comment detection

### Validation & Error Handling
- `validate_schema`: Enable schema validation (default: True)
- `max_validation_errors`: Error threshold before stopping (default: 1000)
- `excess_column_mode`: Strategy for extra columns

### Output Options
- `batch_size`: Rows per processing batch (default: 10000)
- `create_manifest`: Generate manifest file (default: True)
- `create_metadata`: Generate metadata file (default: True)
- `compression`: Output compression type (default: snappy)

## Error Handling

The module provides robust error handling through:
- Configurable validation error limits
- Multiple strategies for malformed data
- Comprehensive error collection in ProcessingResults
- Graceful handling of schema mismatches

## Performance Considerations

- **Batch Size**: Larger batches improve throughput but use more memory
- **Header Search**: Limit `header_search_rows` for large files
- **Validation**: Disable schema validation for trusted data sources
- **Compression**: Choose appropriate compression for your use case

## Integration

This configuration module integrates with the broader Forklift engine:

```python
from forklift.engine.config import ImportConfig
from forklift.engine import DataProcessor

config = ImportConfig(input_path="data.csv", output_path="output/")
processor = DataProcessor(config)
results = processor.process()
```
