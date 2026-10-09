# forklift.schema.processors

## Overview

The `forklift.schema.processors` subpackage provides specialized processing components for transforming data analysis results into structured JSON schemas. It handles the conversion from raw data types to JSON Schema specifications, configuration generation, and metadata extraction.

## Key Components

### JSONSchemaProcessor
The core processor responsible for converting PyArrow table structures into JSON Schema format:
- **Property Generation**: Converts Arrow data types to JSON Schema property definitions
- **Required Field Detection**: Analyzes data to determine which fields should be required based on null value presence
- **Type Mapping**: Handles complex type conversions including nested structures, dates, and custom formats
- **Schema Extensions**: Generates file-format specific extensions (x-csv, x-excel)
- **Sample Data Integration**: Embeds the first rows as JSON-safe samples (dates and timestamps as ISO strings, decimals as strings, NaN/infinity as null) when `include_sample_data` is set

### ConfigurationParser
Generates configuration objects and extensions for data processing pipelines:
- **Primary Key Configuration**: Infers a primary key only from columns that are exactly unique and null-free in the analysed rows (the key is written with `enforceUniqueness: true`) and whose name contains a whole `id`, `key`, `pk`, `uuid` or `guid` token (`user_id`, `userId`, not `width` or `paid`)
- **Transformation Extensions**: Creates transformation rule templates based on data characteristics
- **Processing Hints**: Generates optimization suggestions for large datasets
- **Validation Rules**: Constructs field-level validation constraints
- **Column Analysis**: Provides detailed column-level processing recommendations

### MetadataGenerator
Extracts and formats metadata from data sources using `pyarrow.compute` (no pandas):
- **Privacy**: raw cell values are only embedded when the caller sets `include_value_statistics=True` (`top_values`, `bottom_values`, `suggested_enum_values`, `min_value`, `max_value`, `median`, `range`, `quantiles`). Without it the metadata keeps counts, null/NaN statistics, type information, distinct counts, mean/standard deviation/variance, outlier counts and string-length statistics. Only the source file name is recorded
- **Robustness**: list, struct, map and other unhashable columns get null/type statistics but no distinct-value statistics; dictionary columns are analysed on their decoded values; undefined statistics (such as the standard deviation of one row) are `null` and the result is strict JSON
- **Quantiles**: configured quantiles must be within 0..1 (`ValueError` otherwise) and are keyed by their percentage, for example `quantile_29` and `quantile_99_5`
- **Statistical Analysis**: Numeric columns get count, null and NaN counts, mean, standard deviation, variance, coefficient of variation and IQR outlier counts; quartiles, median, range and the configured quantiles only with `include_value_statistics`
- **Data Profiling**: Generates column profiles including uniqueness, null rates and string-length statistics; value distributions (`top_values`, `bottom_values`) only with `include_value_statistics`
- **Enum suggestions**: A column is an enum candidate when its uniqueness ratio is at most `enum_threshold` (default 0.1), it has at most 50 distinct values and it is below `uniqueness_threshold`; the suggestion carries `suggested_enum_values` only with `include_value_statistics`
- **Source**: `table_metadata.source_file` is the file name, not the path

## Processing Capabilities

### Data Type Conversion
- **Primitive Types**: String, number, boolean, null handling
- **Temporal Types**: Date, datetime, time format detection and conversion
- **Complex Types**: Array, object, and nested structure processing
- **Special Types**: Email, phone, SSN, and other pattern-based type detection
- **Custom Types**: Extensible type system for domain-specific data

### Schema Enhancement
- **Format Specifications**: Adds JSON Schema format constraints (date, email, uri, etc.)
- **Pattern Matching**: Generates regex patterns for string validation
- **Range Constraints**: Determines min/max values for numeric fields
- **Enum Detection**: Identifies categorical fields and generates enum constraints
- **Null Handling**: Configures nullable field specifications

### File Format Processing
- **CSV Extensions**: `x-csv` with encoding priority, delimiter, quote/escape characters, null markers and the Parquet type of each column
- **Excel Extensions**: `x-excel` with the sheet (a name or 0-based index), header and null markers

## Configuration Generation

### Primary Key Analysis
- **Uniqueness Detection**: A column is only a candidate if every analysed value is distinct (uniqueness ratio 1.0), it has no nulls or NaN, and its name contains an `id`, `key`, `pk`, `uuid` or `guid` word token
- **Single column only**: composite keys are not inferred; a composite key can be supplied through `user_specified_primary_key`

### Transformation Templates
- **Data Cleaning**: Generates rules for common data quality issues
- **Format Standardization**: Creates transformation rules for consistent formatting
- **Type Coercion**: Suggests safe type conversion strategies
- **Validation Rules**: Implements business rule validation templates
- **Default Values**: Recommends default value strategies for missing data

## Metadata Extraction

### Statistical Profiling
- **Descriptive Statistics**: Mean, standard deviation and variance for numeric data; median, range and min/max only with `include_value_statistics`
- **Distribution Analysis**: Percentiles (opt-in) and IQR outlier counts
- **Categorical Analysis**: Cardinality metrics always; value counts and frequency distributions (`top_values`) only with `include_value_statistics`
- **Temporal columns**: type, null and distinct-count statistics (no min/max dates are computed)

### Data Quality Assessment
- **Completeness Metrics**: Null value analysis and missing data patterns
- **Consistency Checks**: Format consistency and pattern compliance
- **Accuracy Indicators**: Range validation and constraint compliance
- **Uniqueness Analysis**: Duplicate detection and identifier quality assessment

## Usage Patterns

### Basic JSON Schema Generation
```python
from forklift.schema.processors import JSONSchemaProcessor
import pyarrow as pa

processor = JSONSchemaProcessor()
properties = processor.generate_properties_from_table(table)
required_fields = processor.determine_required_fields(table)
```

### Configuration Generation
```python
from forklift.schema.processors import ConfigurationParser

parser = ConfigurationParser()
primary_key_config = parser.generate_primary_key_config(table, config)
transformation_config = parser.generate_transformation_extension(table)
```

### Metadata Extraction
```python
from forklift.schema.processors import MetadataGenerator

generator = MetadataGenerator()
metadata = generator.generate_metadata(
    table,
    {"enum_threshold": 0.1, "quantiles": [0.25, 0.5, 0.75], "include_value_statistics": False},
)
```

## Integration Points

### Internal Dependencies
- `forklift.schema.types.*` - Data type conversion and detection
- `forklift.schema.utils.*` - Formatting and helper utilities
- `forklift.io` - File I/O operations

### External Dependencies
- **PyArrow**: Core data processing and type system
- **PyArrow compute**: Statistical analysis (no pandas or NumPy required)

## Performance Optimizations

- **Vectorised statistics**: all statistics use `pyarrow.compute` kernels on the sampled table
- **Sampling**: the caller chooses the sample with `nrows` before the table reaches these processors
