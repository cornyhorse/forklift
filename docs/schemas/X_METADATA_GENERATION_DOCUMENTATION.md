# x-metadata-generation Documentation

## Overview
The `x-metadata-generation` extension provides comprehensive metadata file generation during data processing. This feature automatically analyzes data characteristics, generates statistics, detects potential enums, and creates detailed metadata files to support data quality assessment and schema evolution.

### What is implemented

Two components produce metadata, and they read different parts of this extension:

- **Output metadata during `import_csv`** (`OutputMetadataCollector`, written to `output_data_metadata.json` next to `data.parquet`). The CSV processor reads only `enabled`, `enum_detection.uniqueness_threshold`, `statistics.categorical.top_n_values` and `statistics.numeric.quantiles` from the schema; every other key of the extension below is part of the schema standard but is **not read** by the code at present (`output_path`, `enum_detection.enabled`/`max_distinct_values`, `include_quantiles`, `include_outlier_detection`, the `string` block, `include_frequency_analysis` and the whole `performance` block). The metadata file is skipped when no row was accepted.
- **Schema generation** (`generate_schema_from_*`, `forklift generate-schema`) writes an `x-metadata` section into the generated schema and, with `--metadata-output`, a separate file. It is configured through `SchemaGenerationConfig` / CLI options (`enum_threshold`, `uniqueness_threshold`, `top_n_values`, `quantiles`), not through this extension.

### Value statistics are opt-in

Statistics that copy real cell values into the metadata (top and bottom values, enum value lists, min/max, median, mode, quantiles) are **off by default**, because cell values can be personal data and the metadata file is usually stored with weaker access control than the data. They are enabled with `include_value_statistics=True` (`ImportConfig` / `import_csv(...)`, `SchemaGenerationConfig` / `generate_schema_from_*`, CLI `--include-value-stats`), never by the schema file alone. Without it the metadata still has counts, null counts, distinct counts, types, string length statistics (`min_length` / `max_length`, never the shortest/longest string itself) and aggregate mean / standard deviation / variance. Provenance paths (`source_file`, `input_path`, `schema_file`, output files) are recorded as base names, never as directories.

## Schema Structure
```json
{
  "x-metadata-generation": {
    "description": "Configuration for metadata file generation during processing",
    "enabled": true,
    "output_path": "auto",
    "enum_detection": {
      "enabled": true,
      "uniqueness_threshold": 0.1,
      "max_distinct_values": 50
    },
    "statistics": {
      "numeric": {
        "enabled": true,
        "include_quantiles": true,
        "quantiles": [0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
        "include_outlier_detection": true
      },
      "string": {
        "enabled": true,
        "include_length_stats": true,
        "include_pattern_analysis": true
      },
      "categorical": {
        "enabled": true,
        "top_n_values": 10,
        "include_frequency_analysis": true
      }
    },
    "performance": {
      "skip_if_too_large": true,
      "max_rows_for_full_analysis": 1000000,
      "sample_size_for_large_files": 10000
    }
  }
}
```

## Configuration Properties

### `enabled` (required)
- **Type**: Boolean
- **Description**: Controls whether metadata generation is active
- **Implementation**: When `false`, no metadata files are generated
- **Default**: `true`

### `output_path` (optional)
- **Type**: String
- **Description**: Path for metadata output files
- **Values**:
  - `"auto"`: Automatically determine output path based on input file
  - Custom path: Specify exact location for metadata files
- **Default**: `"auto"`

### `enum_detection` (optional)
Configuration for automatic enum detection in data.

#### `enabled`
- **Type**: Boolean
- **Description**: Enable automatic enum detection for categorical data
- **Default**: `true`

#### `uniqueness_threshold`
- **Type**: Number (0.0 to 1.0)
- **Description**: Threshold for uniqueness ratio to suggest enum
- **Implementation**: If unique_values/total_values <= threshold, suggest as enum
- **Default**: `0.1` (10% or fewer unique values)
- **Note**: A column is only reported as a likely enum if its distinct count is also small (at most 50 values in schema generation) and its uniqueness ratio is below the "too unique" threshold (`0.95`). The values themselves are listed only with `include_value_statistics`

#### `max_distinct_values`
- **Type**: Integer
- **Description**: Maximum number of distinct values to consider for enum
- **Implementation**: Columns with more distinct values won't be flagged as enums
- **Default**: `50`

### `statistics` (optional)
Configuration for generating statistical analysis of data.

#### `numeric` - Numeric Statistics
- **`enabled`**: Enable numeric statistics generation
- **`include_quantiles`**: Calculate statistical quantiles
- **`quantiles`**: Array of quantile values to calculate (0.0 to 1.0)
- **`include_outlier_detection`**: Detect and report statistical outliers

Generated numeric statistics include:
- Count, null count, distinct count
- Mean, standard deviation and variance (NaN and infinite values are excluded from them; the output metadata reports them as `non_finite_count`, schema generation as `nan_count`)
- Outlier counts using the IQR method (schema generation)
- With `include_value_statistics`: min, max, median, mode and the specified quantiles (quartiles, percentiles). Quantile keys are `quantile_25`, `quantile_99_5` ... in generated schemas and `p25`, `p99.9` ... in `output_data_metadata.json`. Each quantile must be within 0..1, otherwise a `ValueError` is raised when the collector or generator is created

#### `string` - String Statistics
- **`enabled`**: Enable string statistics generation
- **`include_length_stats`**: Calculate string length statistics
- **`include_pattern_analysis`**: Analyze common patterns in string data

Generated string statistics include:
- Count, null count, distinct count
- Min/max/average string length (`min_length`, `max_length`, `avg_length`)
- Character set analysis (ASCII / non-ASCII counts, whitespace, numbers, special characters)

#### `categorical` - Categorical Statistics
- **`enabled`**: Enable categorical data analysis
- **`top_n_values`**: Number of top values to include in frequency analysis
- **`include_frequency_analysis`**: Include detailed frequency distributions

Generated categorical statistics include:
- Cardinality analysis (distinct count, uniqueness ratio)
- Enum suggestions (`is_enum_candidate`, confidence, distribution balance)
- With `include_value_statistics`: value frequency distributions (`top_values`, `bottom_values`) and the suggested enum values

### `performance` (optional)
Performance optimization settings for large datasets.

#### `skip_if_too_large`
- **Type**: Boolean
- **Description**: Skip metadata generation for very large files
- **Default**: `true`

#### `max_rows_for_full_analysis`
- **Type**: Integer
- **Description**: Maximum rows to process for full metadata analysis
- **Default**: `1000000`

#### `sample_size_for_large_files`
- **Type**: Integer
- **Description**: Sample size when file exceeds max_rows_for_full_analysis
- **Default**: `10000`

## Generated Metadata Output

### Output metadata of `import_csv` (`output_data_metadata.json`)

Default run (no value statistics):

```json
{
  "generation_timestamp": "2025-10-19T10:30:00",
  "source_info": {
    "input_path": "customers.csv",
    "processing_type": "csv_processing",
    "schema_file": "customers_schema.json",
    "final_output_files": ["data.parquet"]
  },
  "data_summary": {"total_rows": 1247, "total_columns": 3, "batches_processed": 1, "schema": {"fields": ["..."]}},
  "column_statistics": {
    "customer_id": {
      "data_type": "int64",
      "total_values": 1247,
      "null_count": 0,
      "non_null_count": 1247,
      "null_percentage": 0.0,
      "unique_values_count": 1247,
      "distinct_count_is_lower_bound": false,
      "uniqueness_ratio": 1.0,
      "likely_categorical": false,
      "too_unique": true,
      "numeric_statistics": {"mean": 624.0, "standard_deviation": 360.1, "variance": 129688.0}
    },
    "email": {
      "data_type": "string",
      "total_values": 1247,
      "null_count": 47,
      "non_null_count": 1200,
      "null_percentage": 3.77,
      "unique_values_count": 1200,
      "distinct_count_is_lower_bound": false,
      "uniqueness_ratio": 1.0,
      "likely_categorical": false,
      "too_unique": true,
      "min_length": 8,
      "max_length": 54
    }
  },
  "data_quality": {"overall_null_percentage": 1.25, "data_completeness_score": 98.75, "...": "..."},
  "profiling_config": {"include_value_statistics": false, "max_distinct_tracked": 10000, "...": "..."}
}
```

`distinct_count_is_lower_bound` becomes `true` when a column has more distinct values than `max_distinct_tracked` (10,000): the ratios and `likely_categorical` / `too_unique` are then `null` rather than a misleading number. String columns only report lengths. With `include_value_statistics=True` each column additionally gets `top_values` (while the distinct count is exact), `min_value` / `max_value` for numeric and temporal columns, and `numeric_statistics` gains `median`, `mode`, `quantiles`, `quantiles_are_estimated` (`true` when the column has more finite values than the reservoir `sample_size`, 10,000, so the quantiles come from a seeded sample) and `sample_size`.

### `x-metadata` in a generated schema

```json
{
  "x-metadata": {
    "description": "Column-level metadata analysis for data profiling and enum type suggestions",
    "analysis_config": {"rows_analyzed": 1247, "include_value_statistics": false, "...": "..."},
    "table_metadata": {"row_count": 1247, "column_count": 3, "source_file": "customers.csv"},
    "column_metadata": {
      "status": {
        "name": "status",
        "type": "string",
        "parquet_type": "string",
        "null_count": 12,
        "distinct_count": 3,
        "uniqueness_ratio": 0.0024,
        "min_length": 6,
        "max_length": 8
      }
    },
    "enum_suggestions": {
      "status": {
        "is_enum_candidate": true,
        "confidence": "high",
        "distinct_count": 3,
        "uniqueness_ratio": 0.0024,
        "distribution_balance": "skewed",
        "top_value_dominance_percentage": 71.4,
        "recommendation": "Column 'status' appears to be categorical with 3 distinct values. Consider using enum type (enable include_value_statistics to list the values)"
      }
    }
  }
}
```

With `include_value_statistics=True` the column entries also contain `top_values` / `bottom_values` (`value`, `count`, `percentage`), the numeric ones `min_value`, `max_value`, `median`, `range` and `quantiles`, and each enum suggestion a `suggested_enum_values` list (for the `status` column: `["active", "inactive", "pending"]`). `source_file` is the base name of the analysed file.

## Implementation Details

### Data Scanning
- The collector (`OutputMetadataCollector`) consumes the accepted batches while they are written (streaming) and keeps running counts; schema generation analyses the sampled table with `pyarrow.compute`.
- Distinct values are tracked exactly up to `max_distinct_tracked` per column; mean, variance, min and max are exact running values.
- Quantiles come from a fixed-seed reservoir sample, so repeated runs on the same file give the same estimates.
- NaN and infinity never appear in the JSON output (they become `null` or are reported as counts).

### Writing
- The file is written once, after the data files are complete. If it cannot be written, `import_csv` still returns the finished run and records the failure in `ProcessingResults.errors`.
- The destination can be a local directory or an `s3://` prefix.

## Usage Examples

### Basic Metadata Generation
```json
{
  "x-metadata-generation": {
    "enabled": true,
    "output_path": "auto"
  }
}
```

### Detailed Analysis Configuration
```json
{
  "x-metadata-generation": {
    "enabled": true,
    "output_path": "/output/metadata/",
    "enum_detection": {
      "enabled": true,
      "uniqueness_threshold": 0.05,
      "max_distinct_values": 100
    },
    "statistics": {
      "numeric": {
        "enabled": true,
        "include_quantiles": true,
        "quantiles": [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
        "include_outlier_detection": true
      }
    }
  }
}
```

### Performance-Optimized Configuration
```json
{
  "x-metadata-generation": {
    "enabled": true,
    "performance": {
      "skip_if_too_large": true,
      "max_rows_for_full_analysis": 500000,
      "sample_size_for_large_files": 5000
    }
  }
}
```

## Integration with Schema Evolution

The generated metadata can be used to:
1. **Refine Schema Definitions**: Update data types based on actual data
2. **Add Enum Constraints**: Convert high-frequency categorical data to enums
3. **Optimize Parquet Types**: Choose appropriate precision for numeric types
4. **Detect Data Quality Issues**: Identify patterns suggesting data problems

## Best Practices

1. **Enable for New Data Sources**: Always generate metadata for unknown data
2. **Review Enum Suggestions**: Manually verify suggested enum values
3. **Monitor Performance**: Adjust sampling for very large datasets
4. **Archive Metadata**: Keep metadata files for schema versioning
5. **Use for Validation**: Compare new data against historical metadata patterns
6. **Leave value statistics off for personal data**: enable `include_value_statistics` only when the metadata file is protected like the data itself
