# Forklift Metadata Package

## Overview

The `forklift.metadata` package provides comprehensive data profiling and metadata collection capabilities for the Forklift data processing pipeline. This package is a critical component of Forklift's data quality and observability features, automatically collecting statistics, quality metrics, and profiling information during data processing operations.

## Package Architecture

The metadata package integrates seamlessly into Forklift's streaming data processing pipeline, collecting statistics and quality metrics in real-time without significant performance overhead. It operates on PyArrow RecordBatch objects during the streaming process, enabling efficient analysis of large datasets that don't fit in memory.

### Integration Points

- **Processing Pipeline**: Automatically collects metadata during CSV, Excel, FWF, and SQL data imports
- **Output Generation**: Generates comprehensive metadata files alongside processed data outputs
- **Schema Processing**: Works with Forklift's schema validation and inference systems
- **Quality Assurance**: Provides data quality metrics for monitoring and validation

## Components

### 1. OutputMetadataCollector

**File**: `output_metadata_collector.py`

The core component responsible for collecting and aggregating metadata from streaming data batches.

#### Key Features

- **Real-time Collection**: Processes data batches as they stream through the pipeline
- **Memory Efficient**: Uses sampling and limits to handle large datasets without memory issues
- **Comprehensive Statistics**: Collects descriptive statistics, data quality metrics, and profiling information
- **Type-Aware Analysis**: Handles numeric, string, temporal, and categorical data types appropriately
- **Configurable Thresholds**: Allows customization of categorical detection and uniqueness analysis

#### Configuration Parameters

```python
OutputMetadataCollector(
    enabled: bool = True,                    # Enable/disable metadata collection
    enum_threshold: float = 0.1,             # Uniqueness ratio threshold for categorical detection
    uniqueness_threshold: float = 0.95,      # Threshold for detecting too-unique columns
    top_n_values: int = 10,                  # Number of top values to track for categorical columns
    quantiles: List[float] = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99],  # Each in [0, 1], else ValueError
    include_value_statistics: bool = False,  # Opt in to statistics that expose real cell values
    max_distinct_tracked: int = 10_000,      # Per-column cap for exact distinct tracking
    sample_size: int = 10_000,               # Reservoir size used for quantiles
)
```

**Privacy:** by default the metadata contains no cell values: only counts, null counts, distinct
counts, types, string length statistics and the aggregate mean / standard deviation / variance of
numeric columns. `top_values`, numeric/temporal `min_value`/`max_value`, `median`, `mode` and
`quantiles` are only written with `include_value_statistics=True`.

**Statistics honesty:** distinct values are tracked exactly up to `max_distinct_tracked`; beyond
that `distinct_count_is_lower_bound` is `true` and `uniqueness_ratio`, `likely_categorical` and
`too_unique` are `null`. Count/min/max/mean/variance are exact (finite values only; NaN/inf are
counted in `non_finite_count`). Median and quantiles come from a seeded reservoir sample of
`sample_size` values (exact while the column has no more values than that, otherwise
`quantiles_are_estimated` is `true`). NaN/inf never appear in the JSON.

#### Data Collection Process

1. **Batch Processing**: Receives PyArrow RecordBatch objects from the streaming pipeline
2. **Schema Analysis**: Analyzes data types and initializes appropriate statistics tracking
3. **Statistical Accumulation**: Updates running statistics for each column across batches
4. **Value Sampling**: Maintains samples of unique values and numeric data for analysis
5. **Quality Assessment**: Tracks null counts, data completeness, and quality indicators

#### Statistics Collected

**Per Column:**
- Data type and nullability information
- Null counts and percentages
- Unique value counts and uniqueness ratios
- Min/max values (numeric, temporal; opt-in) and string length (`min_length`/`max_length`)
- Top N most frequent values for categorical columns (opt-in)
- Numeric statistics: mean, std dev, variance; median, mode and quantiles (opt-in)

**Dataset Level:**
- Total row and column counts
- Overall data completeness scores
- Data quality metrics and problem identification
- Schema information and field metadata

#### Output Metadata Structure

The generated metadata follows a structured format:

```json
{
  "generation_timestamp": "2025-10-19T10:30:00",
  "source_info": {
    "output_path": "/path/to/output",
    "filename": "processed_data.parquet",
    "generation_method": "output_metadata_collector"
  },
  "data_summary": {
    "total_rows": 1000000,
    "total_columns": 15,
    "batches_processed": 100,
    "schema": {...}
  },
  "column_statistics": {
    "column_name": {
      "data_type": "int64",
      "total_values": 1000000,
      "null_count": 50,
      "null_percentage": 0.005,
      "unique_values_count": 950000,
      "uniqueness_ratio": 0.95,
      "likely_categorical": false,
      "too_unique": true,
      "min_value": 1,
      "max_value": 1000000,
      "numeric_statistics": {
        "mean": 500000.5,
        "median": 500000,
        "standard_deviation": 288675.1345,
        "quantiles": {...}
      }
    }
  },
  "data_quality": {
    "overall_null_percentage": 2.5,
    "data_completeness_score": 97.5,
    "high_null_columns": [...],
    "too_unique_columns": [...],
    "likely_categorical_columns": [...]
  }
}
```

### 2. Package Initialization

**File**: `__init__.py`

Provides clean public API access to the metadata collection functionality.

**Exports:**
- `OutputMetadataCollector`: Main metadata collection class

## Integration with Forklift Pipeline

### Automatic Integration

The metadata package is automatically integrated into Forklift's data processing pipeline:

1. **Initialization**: `CSVProcessor` creates an `OutputMetadataCollector` during setup
2. **Configuration**: Collector settings are derived from schema metadata configuration
3. **Collection**: Each valid batch processed through the pipeline is automatically analyzed
4. **Generation**: Metadata is generated and saved alongside output files

### Configuration Sources

Metadata collector configuration can come from multiple sources:

1. **Schema Metadata**: JSON schema files can include metadata collection settings
2. **Default Values**: Sensible defaults are applied when no configuration is provided
3. **Runtime Parameters**: Configuration can be modified programmatically

Example schema metadata configuration:
```json
{
  "metadata": {
    "enabled": true,
    "enum_detection": {
      "uniqueness_threshold": 0.15
    },
    "statistics": {
      "categorical": {
        "top_n_values": 15
      },
      "numeric": {
        "quantiles": [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
      }
    }
  }
}
```

### Output Files

When metadata collection is enabled, the following files are generated:

- **Primary Output**: Main processed data file (e.g., `data.parquet`)
- **Metadata File**: Comprehensive metadata JSON (e.g., `output_metadata.json`)
- **Manifest Files**: Processing manifests and logs (generated by other components)

## Use Cases

### Data Quality Monitoring

- **Completeness Assessment**: Track null percentages and data completeness scores
- **Anomaly Detection**: Identify columns with unexpected uniqueness patterns
- **Type Validation**: Verify data types match expectations

### Data Profiling

- **Statistical Analysis**: Understand data distributions and characteristics
- **Categorical Analysis**: Identify categorical columns and their value distributions
- **Uniqueness Analysis**: Detect potential primary keys and overly unique columns

### Processing Optimization

- **Memory Planning**: Use cardinality information for processing optimization
- **Schema Evolution**: Track data characteristics over time
- **Quality Reporting**: Generate data quality reports for stakeholders

## Performance Considerations

### Memory Management

- **Sampling Limits**: Limits unique value tracking to prevent memory exhaustion
- **Batch Processing**: Processes data in streaming batches rather than loading entire datasets
- **Reservoir Sampling**: Quantiles use a fixed-size, seeded reservoir sample (`sample_size`)

### Processing Overhead

- **Minimal Impact**: Designed for minimal processing overhead during streaming operations
- **Configurable**: Can be disabled entirely when metadata collection is not needed
- **Efficient Algorithms**: Uses efficient statistical algorithms and data structures

## Best Practices

### Configuration

1. **Enable by Default**: Leave metadata collection enabled unless performance is critical
2. **Adjust Thresholds**: Tune categorical detection thresholds based on your data characteristics
3. **Monitor Output Size**: Be aware that metadata files can become large for wide datasets

### Analysis

1. **Review Quality Metrics**: Always examine data quality metrics before proceeding with analysis
2. **Validate Expectations**: Compare collected statistics against expected data characteristics
3. **Track Over Time**: Use metadata to monitor data quality trends across processing runs

### Integration

1. **Automated Workflows**: Integrate metadata validation into automated data pipelines
2. **Alert Thresholds**: Set up alerts based on data quality score thresholds
3. **Documentation**: Use generated metadata as data documentation for downstream consumers

## Error Handling

The metadata collector is designed to be resilient:

- **Graceful Degradation**: Per-column statistic errors are skipped (and logged at debug level) without aborting collection
- **Write Failures Are Raised**: `save_metadata` raises `MetadataWriteError` (logged, not printed) when the file cannot be written; `s3://` destinations are written through the S3 I/O handler. The caller decides whether that is fatal
- **Config Validation**: Quantiles outside [0, 1] raise `ValueError` when the collector is created
- **Type Safety**: Handles type conversion errors gracefully
- **Memory Protection**: Implements safeguards against excessive memory usage
- **Validation**: Validates generated metadata before serialization

## Future Enhancements

The metadata package is designed for extensibility:

- **Custom Metrics**: Framework allows for addition of custom statistical measures
- **Export Formats**: Additional output formats beyond JSON
- **Real-time Monitoring**: Integration with monitoring and alerting systems
- **Advanced Profiling**: Enhanced profiling capabilities for complex data types
