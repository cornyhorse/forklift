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
- **Sample Data Integration**: Embeds representative data samples in schema definitions

### ConfigurationParser
Generates configuration objects and extensions for data processing pipelines:
- **Primary Key Configuration**: Analyzes data uniqueness and cardinality to suggest primary key candidates
- **Transformation Extensions**: Creates transformation rule templates based on data characteristics
- **Processing Hints**: Generates optimization suggestions for large datasets
- **Validation Rules**: Constructs field-level validation constraints
- **Column Analysis**: Provides detailed column-level processing recommendations

### MetadataGenerator
Extracts and formats comprehensive metadata from data sources:
- **Statistical Analysis**: Calculates distributions, quartiles, and data quality metrics
- **Data Profiling**: Generates column profiles including uniqueness, null rates, and value distributions
- **Type Confidence**: Provides confidence scores for type inference decisions
- **Data Quality Metrics**: Identifies potential data quality issues and anomalies
- **Schema Versioning**: Tracks schema evolution and compatibility information

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
- **CSV Extensions**: Delimiter, encoding, and parsing configuration
- **Excel Extensions**: Sheet references, cell formatting, and range specifications
- **Parquet Extensions**: Column statistics, compression settings, and schema metadata
- **Fixed-Width Extensions**: Field positions, padding, and alignment specifications

## Configuration Generation

### Primary Key Analysis
- **Uniqueness Detection**: Identifies columns with high uniqueness ratios
- **Composite Key Analysis**: Suggests multi-column primary key combinations
- **Data Distribution**: Analyzes value distributions to assess key quality
- **Performance Optimization**: Recommends indexing strategies based on key characteristics

### Transformation Templates
- **Data Cleaning**: Generates rules for common data quality issues
- **Format Standardization**: Creates transformation rules for consistent formatting
- **Type Coercion**: Suggests safe type conversion strategies
- **Validation Rules**: Implements business rule validation templates
- **Default Values**: Recommends default value strategies for missing data

## Metadata Extraction

### Statistical Profiling
- **Descriptive Statistics**: Mean, median, mode, standard deviation for numeric data
- **Distribution Analysis**: Histograms, percentiles, and outlier detection
- **Categorical Analysis**: Value counts, frequency distributions, and cardinality metrics
- **Temporal Analysis**: Date ranges, seasonal patterns, and time series characteristics

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
metadata = generator.generate_metadata(table, config)
profile = generator.generate_column_profiles(table)
```

## Integration Points

### Internal Dependencies
- `forklift.schema.types.*` - Data type conversion and detection
- `forklift.schema.utils.*` - Formatting and helper utilities
- `forklift.io` - File I/O operations

### External Dependencies
- **PyArrow**: Core data processing and type system
- **Pandas**: Statistical analysis and data manipulation
- **NumPy**: Numerical computations and array operations

## Performance Optimizations

- **Lazy Evaluation**: Deferred computation for large datasets
- **Sampling Strategies**: Statistical sampling for performance-critical operations
- **Memory Management**: Efficient processing of large tables
- **Parallel Processing**: Multi-threaded analysis for independent operations
- **Caching**: Result caching for repeated operations
