# Forklift Metadata Package

## Overview

The `forklift.metadata` package provides comprehensive data profiling and metadata collection capabilities for the Forklift data processing pipeline. This package is a critical component of Forklift's data quality and observability features, automatically collecting statistics, quality metrics, and profiling information during data processing operations.

## Package Architecture

The metadata package integrates seamlessly into Forklift's streaming data processing pipeline, collecting statistics and quality metrics in real-time without significant performance overhead. It operates on PyArrow RecordBatch objects during the streaming process, enabling efficient analysis of large datasets that don't fit in memory.

### Integration Points

- **Processing Pipeline**: Collects metadata during CSV imports (`CSVProcessor`). The Excel and SQL importers write Parquet without it, and the engine does not import fixed-width files yet
- **Output Generation**: Writes `output_data_metadata.json` next to the processed data (local directory or `s3://` prefix)
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
counts, types, string length statistics (`min_length`/`max_length`, which are lengths, not values)
and the aggregate mean / standard deviation / variance of numeric columns. `top_values`,
numeric/temporal `min_value`/`max_value`, `median`, `mode` and `quantiles` copy real cell values into
the file, which is usually stored with weaker access control than the data, so they are only
written with `include_value_statistics=True`. Through the engine that is
`ImportConfig(include_value_statistics=True)` (or `import_csv(..., include_value_statistics=True)`);
on the command line it is `forklift ingest --include-value-stats`. Even with the default, a mean or
variance over a column with only one or two non-null values still reveals those values.

**Statistics honesty:** distinct values are tracked exactly up to `max_distinct_tracked`; beyond
that `distinct_count_is_lower_bound` is `true` and `uniqueness_ratio`, `likely_categorical` and
`too_unique` are `null`. Count/min/max/mean/variance are exact (finite values only; NaN/inf are
counted in `non_finite_count`). Median and quantiles come from a seeded reservoir sample of
`sample_size` values (exact while the column has no more values than that, otherwise
`quantiles_are_estimated` is `true`); the quantile keys are `p25`, `p50`, `p99.9`, ... NaN/inf never
appear in the JSON.

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

The generated metadata follows a structured format. This is what a default run (without
`include_value_statistics`) writes; `source_info` is whatever the caller passes (the CSV processor
records base names only, never directories):

```json
{
  "generation_timestamp": "2025-10-19T10:30:00",
  "source_info": {
    "input_path": "sales.csv",
    "processing_type": "csv_processing",
    "schema_file": "sales_schema.json",
    "total_batches_processed": "streaming",
    "final_output_files": ["data.parquet"]
  },
  "data_summary": {
    "total_rows": 1000000,
    "total_columns": 15,
    "batches_processed": 100,
    "schema": {"fields": [{"name": "order_id", "type": "int64", "nullable": true}, "..."]}
  },
  "column_statistics": {
    "order_id": {
      "data_type": "int64",
      "total_values": 1000000,
      "null_count": 50,
      "non_null_count": 999950,
      "null_percentage": 0.01,
      "unique_values_count": 950000,
      "distinct_count_is_lower_bound": false,
      "uniqueness_ratio": 0.95,
      "likely_categorical": false,
      "too_unique": true,
      "numeric_statistics": {
        "mean": 500000.5,
        "standard_deviation": 288675.1345,
        "variance": 83333416666.6667
      }
    },
    "customer_name": {
      "data_type": "string",
      "total_values": 1000000,
      "null_count": 0,
      "non_null_count": 1000000,
      "null_percentage": 0.0,
      "unique_values_count": 10000,
      "distinct_count_is_lower_bound": true,
      "uniqueness_ratio": null,
      "likely_categorical": null,
      "too_unique": null,
      "min_length": 2,
      "max_length": 64
    }
  },
  "data_quality": {
    "overall_null_percentage": 2.5,
    "columns_with_nulls": 3,
    "columns_with_nulls_percentage": 20.0,
    "data_completeness_score": 97.5,
    "high_null_columns": [...],
    "too_unique_columns": [...],
    "likely_categorical_columns": [...]
  },
  "profiling_config": {
    "enum_threshold": 0.1,
    "uniqueness_threshold": 0.95,
    "top_n_values": 10,
    "quantiles": [0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
    "include_value_statistics": false,
    "max_distinct_tracked": 10000,
    "sample_size": 10000
  }
}
```

`customer_name` shows a column that reached `max_distinct_tracked`: its distinct count is a lower
bound, so the ratios are `null` instead of a misleading number. String columns report
`min_length`/`max_length` (never the smallest/largest string).

With `include_value_statistics=True` the entries additionally contain `top_values`
(`value`/`count`/`percentage`, only while the distinct count is exact; otherwise
`top_values_unavailable` explains why), `min_value`/`max_value` for numeric and temporal columns,
and, inside `numeric_statistics`, `median`, `mode`, `quantiles`, `quantiles_are_estimated` and
`sample_size`:

```json
"numeric_statistics": {
  "mean": 500000.5,
  "standard_deviation": 288675.1345,
  "variance": 83333416666.6667,
  "median": 500000,
  "quantiles": {"p25": 250000, "p50": 500000, "p75": 750000},
  "quantiles_are_estimated": true,
  "sample_size": 10000
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

The schema file configures it through the `x-metadata-generation` extension. The CSV processor reads
`enabled`, `enum_detection.uniqueness_threshold`, `statistics.categorical.top_n_values` and
`statistics.numeric.quantiles`; the remaining keys of that extension are not used by the collector.
Whether value statistics are written is deliberately not a schema setting: it is the
`include_value_statistics` option of `ImportConfig` (or the collector), so a schema file cannot
switch on PII-bearing statistics by itself.

Example schema metadata configuration:
```json
{
  "x-metadata-generation": {
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
- **Metadata File**: Comprehensive metadata JSON (`output_data_metadata.json` in the CSV pipeline; `save_metadata` defaults to `output_metadata.json`)
- **Manifest and Processing Metadata**: `manifest.json` and `metadata.json` (generated by the CSV processor, not by the collector)

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
