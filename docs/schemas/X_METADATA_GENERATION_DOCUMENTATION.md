# x-metadata-generation Documentation

## Overview
The `x-metadata-generation` extension provides comprehensive metadata file generation during data processing. This feature automatically analyzes data characteristics, generates statistics, detects potential enums, and creates detailed metadata files to support data quality assessment and schema evolution.

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
- Min, max, mean, median, standard deviation
- Specified quantiles (quartiles, percentiles)
- Outlier detection using IQR method
- Data type recommendations

#### `string` - String Statistics
- **`enabled`**: Enable string statistics generation
- **`include_length_stats`**: Calculate string length statistics
- **`include_pattern_analysis`**: Analyze common patterns in string data

Generated string statistics include:
- Count, null count, distinct count
- Min/max/average string length
- Character set analysis
- Common patterns and formats detected
- Potential data quality issues

#### `categorical` - Categorical Statistics
- **`enabled`**: Enable categorical data analysis
- **`top_n_values`**: Number of top values to include in frequency analysis
- **`include_frequency_analysis`**: Include detailed frequency distributions

Generated categorical statistics include:
- Value frequency distributions
- Top N most common values
- Rare value detection
- Cardinality analysis
- Enum suggestions

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

The metadata generation produces a comprehensive JSON file with structure like:

```json
{
  "x-metadata": {
    "customer_id": {
      "distinct_count": 1247,
      "null_count": 0,
      "min_value": 1,
      "max_value": 1247,
      "is_potentially_unique": true,
      "data_type_detected": "integer",
      "recommended_type": "int32"
    },
    "status": {
      "distinct_count": 3,
      "null_count": 12,
      "value_counts": {
        "active": 890,
        "inactive": 245,
        "pending": 100
      },
      "suggested_enum": true,
      "enum_values": ["active", "inactive", "pending"]
    },
    "signup_date": {
      "distinct_count": 456,
      "null_count": 0,
      "min_value": "2020-01-15",
      "max_value": "2024-12-30",
      "date_formats_detected": ["YYYY-MM-DD"],
      "recommended_type": "date32"
    },
    "email": {
      "distinct_count": 1200,
      "null_count": 47,
      "pattern_analysis": {
        "email_format": 0.96,
        "invalid_emails": 0.04
      },
      "length_stats": {
        "min_length": 8,
        "max_length": 54,
        "avg_length": 24.3
      }
    }
  }
}
```

## Implementation Details

### Data Scanning Process
1. **Initial Pass**: Counts, nulls, basic type detection
2. **Statistical Pass**: Detailed statistics based on detected types
3. **Pattern Analysis**: String patterns, date formats, enum detection
4. **Report Generation**: Consolidate findings into metadata JSON

### Memory Management
- Streaming processing for large files
- Sampling strategies for performance optimization
- Configurable memory limits for distinct value tracking

### Type Detection
- Automatic data type inference from content
- Pattern-based format detection (dates, emails, phones, etc.)
- Parquet type recommendations

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
